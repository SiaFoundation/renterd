package stores

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"sync"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/syncer"
	"go.sia.tech/renterd/alerts"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/utils"
	"go.sia.tech/renterd/stores/sql"
	"go.sia.tech/renterd/stores/sql/mysql"
	"go.sia.tech/renterd/stores/sql/sqlite"
	"go.uber.org/zap"
	gmysql "gorm.io/driver/mysql"
	gsqlite "gorm.io/driver/sqlite"
	"gorm.io/gorm"
	glogger "gorm.io/gorm/logger"
)

const (
	// maxSQLVars is the maximum number of variables in an sql query. This
	// number matches the sqlite default of 32766 rounded down to the nearest
	// 1000. This is also lower than the mysql default of 65535.
	maxSQLVars = 32000
)

var (
	exprTRUE = gorm.Expr("TRUE")
)

var (
	errNoSuchTable    = errors.New("no such table")
	errDuplicateEntry = errors.New("Duplicate entry")
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
		ConnMetrics                   gorm.Dialector
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
		db        *gorm.DB
		dbMetrics *gorm.DB
		bMain     sql.Database
		bMetrics  sql.MetricsDatabase
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
		hasAllowlist bool
		hasBlocklist bool
		lastPrunedAt time.Time
		closed       bool
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
	dbMetrics, err := gorm.Open(cfg.ConnMetrics, &gorm.Config{
		Logger: cfg.GormLogger, // custom logger
	})
	if err != nil {
		return nil, fmt.Errorf("failed to open metrics db")
	}
	l := cfg.Logger.Named("sql")

	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("failed to fetch db: %v", err)
	}
	sqlDBMetrics, err := dbMetrics.DB()
	if err != nil {
		return nil, fmt.Errorf("failed to fetch metrics db: %v", err)
	}

	// Print DB version
	var dbMain sql.Database
	var bMetrics sql.MetricsDatabase
	var mainErr, metricsErr error
	if cfg.Conn.Name() == "sqlite" {
		dbMain, mainErr = sqlite.NewMainDatabase(sqlDB, l, cfg.LongQueryDuration, cfg.LongTxDuration)
		bMetrics, metricsErr = sqlite.NewMetricsDatabase(sqlDBMetrics, l, cfg.LongQueryDuration, cfg.LongTxDuration)
	} else {
		dbMain, mainErr = mysql.NewMainDatabase(sqlDB, l, cfg.LongQueryDuration, cfg.LongTxDuration)
		bMetrics, metricsErr = mysql.NewMetricsDatabase(sqlDBMetrics, l, cfg.LongQueryDuration, cfg.LongTxDuration)
	}
	if mainErr != nil {
		return nil, fmt.Errorf("failed to create main database: %v", mainErr)
	} else if metricsErr != nil {
		return nil, fmt.Errorf("failed to create metrics database: %v", metricsErr)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	dbName, dbVersion, err := dbMain.Version(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch db version: %v", err)
	}
	l.Infof("Using %s version %s", dbName, dbVersion)

	// Perform migrations.
	if cfg.Migrate {
		if err := dbMain.Migrate(context.Background()); err != nil {
			return nil, fmt.Errorf("failed to perform migrations: %v", err)
		} else if err := bMetrics.Migrate(context.Background()); err != nil {
			return nil, fmt.Errorf("failed to perform migrations for metrics db: %v", err)
		}
	}

	// Check allowlist and blocklist counts
	allowlistCnt, err := tableCount(db, &dbAllowlistEntry{})
	if err != nil {
		return nil, err
	}
	blocklistCnt, err := tableCount(db, &dbBlocklistEntry{})
	if err != nil {
		return nil, err
	}

	shutdownCtx, shutdownCtxCancel := context.WithCancel(context.Background())
	ss := &SQLStore{
		alerts:    cfg.Alerts,
		db:        db,
		dbMetrics: dbMetrics,
		bMain:     dbMain,
		bMetrics:  bMetrics,
		logger:    l,

		settings:      make(map[string]string),
		hasAllowlist:  allowlistCnt > 0,
		hasBlocklist:  blocklistCnt > 0,
		walletAddress: cfg.WalletAddress,

		slabPruneSigChan:          make(chan struct{}, 1),
		lastPrunedAt:              time.Now(),
		retryTransactionIntervals: cfg.RetryTransactionIntervals,

		shutdownCtx:       shutdownCtx,
		shutdownCtxCancel: shutdownCtxCancel,
	}

	ss.slabBufferMgr, err = newSlabBufferManager(ss, cfg.SlabBufferCompletionThreshold, cfg.PartialSlabDir)
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
	return s.bMain.Transaction(s.shutdownCtx, func(tx sql.DatabaseTx) error {
		_, err := tx.PruneSlabs(s.shutdownCtx, math.MaxInt64)
		return err
	})
}

func (ss *SQLStore) updateHasAllowlist(err *error) {
	if *err != nil {
		return
	}

	cnt, cErr := tableCount(ss.db, &dbAllowlistEntry{})
	if cErr != nil {
		*err = cErr
		return
	}

	ss.mu.Lock()
	ss.hasAllowlist = cnt > 0
	ss.mu.Unlock()
}

func (ss *SQLStore) updateHasBlocklist(err *error) {
	if *err != nil {
		return
	}

	cnt, cErr := tableCount(ss.db, &dbBlocklistEntry{})
	if cErr != nil {
		*err = cErr
		return
	}

	ss.mu.Lock()
	ss.hasBlocklist = cnt > 0
	ss.mu.Unlock()
}

func tableCount(db *gorm.DB, model interface{}) (cnt int64, err error) {
	err = db.Model(model).Count(&cnt).Error
	return
}

// Close closes the underlying database connection of the store.
func (s *SQLStore) Close() error {
	s.shutdownCtxCancel()

	err := s.slabBufferMgr.Close()
	if err != nil {
		return err
	}

	err = s.bMain.Close()
	if err != nil {
		return err
	}
	err = s.bMetrics.Close()
	if err != nil {
		return err
	}

	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	return nil
}

// ChainIndex returns the last stored chain index.
func (ss *SQLStore) ChainIndex(ctx context.Context) (types.ChainIndex, error) {
	var ci dbConsensusInfo
	if err := ss.db.
		WithContext(ctx).
		Where(&dbConsensusInfo{Model: Model{ID: consensusInfoID}}).
		FirstOrCreate(&ci).
		Error; err != nil {
		return types.ChainIndex{}, err
	}
	return types.ChainIndex{
		Height: ci.Height,
		ID:     types.BlockID(ci.BlockID),
	}, nil
}

func (s *SQLStore) retryTransaction(ctx context.Context, fc func(tx *gorm.DB) error) error {
	return retryTransaction(ctx, s.db, s.logger, s.retryTransactionIntervals, fc, s.retryAbortFn)
}

func (s *SQLStore) retryAbortFn(err error) bool {
	return err == nil ||
		utils.IsErr(err, context.Canceled) ||
		utils.IsErr(err, context.DeadlineExceeded) ||
		utils.IsErr(err, gorm.ErrRecordNotFound) ||
		utils.IsErr(err, api.ErrContractNotFound) ||
		utils.IsErr(err, api.ErrObjectNotFound) ||
		utils.IsErr(err, api.ErrObjectCorrupted) ||
		utils.IsErr(err, api.ErrBucketExists) ||
		utils.IsErr(err, api.ErrBucketNotFound) ||
		utils.IsErr(err, api.ErrBucketNotEmpty) ||
		utils.IsErr(err, api.ErrMultipartUploadNotFound) ||
		utils.IsErr(err, api.ErrObjectExists) ||
		utils.IsErr(err, errNoSuchTable) ||
		utils.IsErr(err, api.ErrPartNotFound) ||
		utils.IsErr(err, api.ErrSlabNotFound) ||
		utils.IsErr(err, syncer.ErrPeerNotFound) ||
		utils.IsErr(err, errDuplicateEntry)
}

func retryTransaction(ctx context.Context, db *gorm.DB, logger *zap.SugaredLogger, intervals []time.Duration, fn func(tx *gorm.DB) error, abortFn func(error) bool) error {
	var err error
	attempts := len(intervals) + 1
	for i := 0; i < attempts; i++ {
		// execute the transaction
		err = db.WithContext(ctx).Transaction(fn)
		if abortFn(err) {
			return err
		}

		// if this was the last attempt, return the error
		if i == len(intervals) {
			logger.Warn(fmt.Sprintf("transaction attempt %d/%d failed, err: %v", i+1, attempts, err))
			return err
		}

		// log the failed attempt and sleep before retrying
		interval := intervals[i]
		logger.Warn(fmt.Sprintf("transaction attempt %d/%d failed, retry in %v,  err: %v", i+1, attempts, interval, err))
		time.Sleep(interval)
	}
	return fmt.Errorf("retryTransaction failed: %w", err)
}
