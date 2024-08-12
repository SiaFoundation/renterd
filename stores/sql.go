package stores

import (
	"context"
	"fmt"
	"math"
	"os"
	"sync"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/alerts"
	"go.sia.tech/renterd/stores/sql"
	"go.sia.tech/renterd/stores/sql/mysql"
	"go.sia.tech/renterd/stores/sql/sqlite"
	"go.uber.org/zap"
	gmysql "gorm.io/driver/mysql"
	gsqlite "gorm.io/driver/sqlite"
	"gorm.io/gorm"
	glogger "gorm.io/gorm/logger"
)

type (
	// Model defines the common fields of every table. Same as Model
	// but excludes soft deletion since it breaks cascading deletes.
	Model struct {
		ID        uint `gorm:"primarykey"`
		CreatedAt time.Time
	}

	// Config contains all params for creating a SQLStore
	Config struct {
		Conn                          gorm.Dialector
		DBMetrics                     sql.MetricsDatabase
		Alerts                        alerts.Alerter
		PartialSlabDir                string
		Migrate                       bool
		AnnouncementMaxAge            time.Duration
		WalletAddress                 types.Address
		SlabBufferCompletionThreshold int64
		Logger                        *zap.SugaredLogger
		GormLogger                    glogger.Interface
		RetryTransactionIntervals     []time.Duration
		LongQueryDuration             time.Duration
		LongTxDuration                time.Duration
	}

	// SQLStore is a helper type for interacting with a SQL-based backend.
	SQLStore struct {
		alerts    alerts.Alerter
		db        sql.Database
		dbMetrics sql.MetricsDatabase
		logger    *zap.SugaredLogger

		walletAddress types.Address

		// ObjectDB related fields
		slabBufferMgr *SlabBufferManager

		// SettingsDB related fields
		settingsMu sync.Mutex
		settings   map[string]string

		retryTransactionIntervals []time.Duration

		shutdownCtx       context.Context
		shutdownCtxCancel context.CancelFunc

		slabPruneSigChan chan struct{}
		wg               sync.WaitGroup

		mu           sync.Mutex
		lastPrunedAt time.Time
		closed       bool

		gormDB *gorm.DB // deprecated: don't use
	}
)

// NewEphemeralSQLiteConnection creates a connection to an in-memory SQLite DB.
// NOTE: Use simple names such as a random hex identifier or the filepath.Base
// of a test's name. Certain symbols will break the cfg string and cause a file
// to be created on disk.
//
//	mode: set to memory for in-memory database
//	cache: set to shared which is required for in-memory databases
//	_foreign_keys: enforce foreign_key relations
func NewEphemeralSQLiteConnection(name string) gorm.Dialector {
	return gsqlite.Open(fmt.Sprintf("file:%s?mode=memory&cache=shared&_foreign_keys=1", name))
}

// NewSQLiteConnection opens a sqlite db at the given path.
//
//	_busy_timeout: set to prevent concurrent transactions from failing and
//	  instead have them block
//	_foreign_keys: enforce foreign_key relations
//	_journal_mode: set to WAL instead of delete since it's usually the fastest.
//	  Only downside is that the db won't work on network drives. In that case this
//	  should be made configurable and set to TRUNCATE or any of the other options.
//	  For reference see https://github.com/mattn/go-sqlite3#connection-string.
func NewSQLiteConnection(path string) gorm.Dialector {
	return gsqlite.Open(fmt.Sprintf("file:%s?_busy_timeout=30000&_foreign_keys=1&_journal_mode=WAL&_secure_delete=false&_cache_size=65536", path))
}

// NewMetricsSQLiteConnection opens a sqlite db at the given path similarly to
// NewSQLiteConnection but with weaker consistency guarantees since it's
// optimised for recording metrics.
func NewMetricsSQLiteConnection(path string) gorm.Dialector {
	return gsqlite.Open(fmt.Sprintf("file:%s?_busy_timeout=30000&_foreign_keys=1&_journal_mode=WAL&_synchronous=NORMAL", path))
}

// NewMySQLConnection creates a connection to a MySQL database.
func NewMySQLConnection(user, password, addr, dbName string) gorm.Dialector {
	return gmysql.Open(fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8mb4&parseTime=True&loc=Local&multiStatements=true", user, password, addr, dbName))
}

// NewSQLStore uses a given Dialector to connect to a SQL database.  NOTE: Only
// pass migrate=true for the first instance of SQLHostDB if you connect via the
// same Dialector multiple times.
func NewSQLStore(cfg Config) (*SQLStore, error) {
	if err := os.MkdirAll(cfg.PartialSlabDir, 0700); err != nil {
		return nil, fmt.Errorf("failed to create partial slab dir '%s': %v", cfg.PartialSlabDir, err)
	}
	db, err := gorm.Open(cfg.Conn, &gorm.Config{
		Logger:                   cfg.GormLogger, // custom logger
		SkipDefaultTransaction:   true,
		DisableNestedTransaction: true,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to open SQL db")
	}
	l := cfg.Logger.Named("sql")

	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("failed to fetch db: %v", err)
	}

	// Print DB version
	var dbMain sql.Database
	dbMetrics := cfg.DBMetrics
	var mainErr error
	if cfg.Conn.Name() == "sqlite" {
		dbMain, mainErr = sqlite.NewMainDatabase(sqlDB, l, cfg.LongQueryDuration, cfg.LongTxDuration)
	} else {
		dbMain, mainErr = mysql.NewMainDatabase(sqlDB, l, cfg.LongQueryDuration, cfg.LongTxDuration)
	}
	if mainErr != nil {
		return nil, fmt.Errorf("failed to create main database: %v", mainErr)
	}

	dbName, dbVersion, err := dbMain.Version(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to fetch db version: %v", err)
	}
	l.Infof("Using %s version %s", dbName, dbVersion)

	// Perform migrations.
	if cfg.Migrate {
		if err := dbMain.Migrate(context.Background()); err != nil {
			return nil, fmt.Errorf("failed to perform migrations: %v", err)
		} else if err := dbMetrics.Migrate(context.Background()); err != nil {
			return nil, fmt.Errorf("failed to perform migrations for metrics db: %v", err)
		}
	}

	shutdownCtx, shutdownCtxCancel := context.WithCancel(context.Background())
	ss := &SQLStore{
		alerts:    cfg.Alerts,
		gormDB:    db,
		db:        dbMain,
		dbMetrics: dbMetrics,
		logger:    l,

		settings:      make(map[string]string),
		walletAddress: cfg.WalletAddress,

		slabPruneSigChan:          make(chan struct{}, 1),
		lastPrunedAt:              time.Now(),
		retryTransactionIntervals: cfg.RetryTransactionIntervals,

		shutdownCtx:       shutdownCtx,
		shutdownCtxCancel: shutdownCtxCancel,
	}

	ss.slabBufferMgr, err = newSlabBufferManager(shutdownCtx, cfg.Alerts, dbMain, l.Named("slabbuffers"), cfg.SlabBufferCompletionThreshold, cfg.PartialSlabDir)
	if err != nil {
		return nil, err
	}
	if err := ss.initSlabPruning(); err != nil {
		return nil, err
	}
	return ss, nil
}

func isSQLite(db *gorm.DB) bool {
	switch db.Dialector.(type) {
	case *gsqlite.Dialector:
		return true
	case *gmysql.Dialector:
		return false
	default:
		panic(fmt.Sprintf("unknown dialector: %t", db.Dialector))
	}
}

func (s *SQLStore) initSlabPruning() error {
	// start pruning loop
	s.wg.Add(1)
	go func() {
		s.pruneSlabsLoop()
		s.wg.Done()
	}()

	// prune once to guarantee consistency on startup
	return s.db.Transaction(s.shutdownCtx, func(tx sql.DatabaseTx) error {
		_, err := tx.PruneSlabs(s.shutdownCtx, math.MaxInt64)
		return err
	})
}

// Close closes the underlying database connection of the store.
func (s *SQLStore) Close() error {
	s.shutdownCtxCancel()

	err := s.slabBufferMgr.Close()
	if err != nil {
		return err
	}

	err = s.db.Close()
	if err != nil {
		return err
	}
	err = s.dbMetrics.Close()
	if err != nil {
		return err
	}

	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	return nil
}
