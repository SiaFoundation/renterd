package stores

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/renterd/alerts"
	"go.sia.tech/renterd/api"
	"go.uber.org/zap"
	"gorm.io/driver/mysql"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	glogger "gorm.io/gorm/logger"
)

const (
	// maxSQLVars is the maximum number of variables in an sql query. This
	// number matches the sqlite default of 32766 rounded down to the nearest
	// 1000. This is also lower than the mysql default of 65535.
	maxSQLVars = 32000
)

//go:embed all:migrations/*
var migrations embed.FS

var (
	exprTRUE = gorm.Expr("TRUE")
)

var (
	_ wallet.SingleAddressStore = (*SQLStore)(nil)
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
		PersistInterval               time.Duration
		WalletAddress                 types.Address
		SlabBufferCompletionThreshold int64
		Logger                        *zap.SugaredLogger
		GormLogger                    glogger.Interface
		RetryTransactionIntervals     []time.Duration
	}

	// SQLStore is a helper type for interacting with a SQL-based backend.
	SQLStore struct {
		alerts    alerts.Alerter
		cs        *chainSubscriber
		db        *gorm.DB
		dbMetrics *gorm.DB
		logger    *zap.SugaredLogger

		// HostDB related fields
		announcementMaxAge time.Duration

		// ObjectDB related fields
		slabBufferMgr    *SlabBufferManager
		slabPruneSigChan chan struct{}

		// SettingsDB related fields
		settingsMu sync.Mutex
		settings   map[string]string

		// WalletDB related fields.
		walletAddress types.Address

		retryTransactionIntervals []time.Duration

		shutdownCtx       context.Context
		shutdownCtxCancel context.CancelFunc

		mu           sync.Mutex
		hasAllowlist bool
		hasBlocklist bool
		closed       bool
	}

	revisionUpdate struct {
		height uint64
		number uint64
		size   uint64
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
	return sqlite.Open(fmt.Sprintf("file:%s?mode=memory&cache=shared&_foreign_keys=1", name))
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
	return sqlite.Open(fmt.Sprintf("file:%s?_busy_timeout=30000&_foreign_keys=1&_journal_mode=WAL", path))
}

// NewMetricsSQLiteConnection opens a sqlite db at the given path similarly to
// NewSQLiteConnection but with weaker consistency guarantees since it's
// optimised for recording metrics.
func NewMetricsSQLiteConnection(path string) gorm.Dialector {
	return sqlite.Open(fmt.Sprintf("file:%s?_busy_timeout=30000&_foreign_keys=1&_journal_mode=WAL&_synchronous=NORMAL", path))
}

// NewMySQLConnection creates a connection to a MySQL database.
func NewMySQLConnection(user, password, addr, dbName string) gorm.Dialector {
	return mysql.Open(fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8mb4&parseTime=True&loc=Local&multiStatements=true", user, password, addr, dbName))
}

func DBConfigFromEnv() (uri, user, password, dbName string) {
	uri = os.Getenv("RENTERD_DB_URI")
	user = os.Getenv("RENTERD_DB_USER")
	password = os.Getenv("RENTERD_DB_PASSWORD")
	dbName = os.Getenv("RENTERD_DB_NAME")
	return
}

// NewSQLStore uses a given Dialector to connect to a SQL database.  NOTE: Only
// pass migrate=true for the first instance of SQLHostDB if you connect via the
// same Dialector multiple times.
func NewSQLStore(cfg Config) (*SQLStore, error) {
	// Sanity check announcement max age.
	if cfg.AnnouncementMaxAge == 0 {
		return nil, errors.New("announcementMaxAge must be non-zero")
	}

	if err := os.MkdirAll(cfg.PartialSlabDir, 0700); err != nil {
		return nil, fmt.Errorf("failed to create partial slab dir: %v", err)
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

	// Print SQLite version
	var dbName string
	var dbVersion string
	if isSQLite(db) {
		err = db.Raw("select sqlite_version()").Scan(&dbVersion).Error
		dbName = "SQLite"
	} else {
		err = db.Raw("select version()").Scan(&dbVersion).Error
		dbName = "MySQL"
	}
	if err != nil {
		return nil, fmt.Errorf("failed to fetch db version: %v", err)
	}
	l.Infof("Using %s version %s", dbName, dbVersion)

	// Perform migrations.
	if cfg.Migrate {
		if err := performMigrations(db, l); err != nil {
			return nil, fmt.Errorf("failed to perform migrations: %v", err)
		}
		if err := performMetricsMigrations(dbMetrics, l); err != nil {
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

	// Create chain subscriber
	cs, err := NewChainSubscriber(db, cfg.Logger, cfg.RetryTransactionIntervals, cfg.PersistInterval, cfg.WalletAddress, cfg.AnnouncementMaxAge)
	if err != nil {
		return nil, err
	}

	shutdownCtx, shutdownCtxCancel := context.WithCancel(context.Background())
	ss := &SQLStore{
		alerts:           cfg.Alerts,
		cs:               cs,
		db:               db,
		dbMetrics:        dbMetrics,
		logger:           l,
		hasAllowlist:     allowlistCnt > 0,
		hasBlocklist:     blocklistCnt > 0,
		settings:         make(map[string]string),
		slabPruneSigChan: make(chan struct{}, 1),
		walletAddress:    cfg.WalletAddress,

		announcementMaxAge: cfg.AnnouncementMaxAge,

		retryTransactionIntervals: cfg.RetryTransactionIntervals,

		shutdownCtx:       shutdownCtx,
		shutdownCtxCancel: shutdownCtxCancel,
	}

	ss.slabBufferMgr, err = newSlabBufferManager(ss, cfg.SlabBufferCompletionThreshold, cfg.PartialSlabDir)
	if err != nil {
		return nil, err
	}
	return ss, nil
}

func isSQLite(db *gorm.DB) bool {
	switch db.Dialector.(type) {
	case *sqlite.Dialector:
		return true
	case *mysql.Dialector:
		return false
	default:
		panic(fmt.Sprintf("unknown dialector: %t", db.Dialector))
	}
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

	db, err := s.db.DB()
	if err != nil {
		return err
	}
	dbMetrics, err := s.dbMetrics.DB()
	if err != nil {
		return err
	}

	err = s.cs.Close()
	if err != nil {
		return err
	}

	err = db.Close()
	if err != nil {
		return err
	}
	err = dbMetrics.Close()
	if err != nil {
		return err
	}

	err = s.slabBufferMgr.Close()
	if err != nil {
		return err
	}

	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	return nil
}

// ProcessChainApplyUpdate implements chain.Subscriber.
func (s *SQLStore) ProcessChainApplyUpdate(cau *chain.ApplyUpdate, mayCommit bool) error {
	return s.cs.ProcessChainApplyUpdate(cau, mayCommit)
}

// ProcessChainRevertUpdate implements chain.Subscriber.
func (s *SQLStore) ProcessChainRevertUpdate(cru *chain.RevertUpdate) error {
	return s.cs.ProcessChainRevertUpdate(cru)
}

func retryTransaction(db *gorm.DB, logger *zap.SugaredLogger, fc func(tx *gorm.DB) error, intervals []time.Duration, opts ...*sql.TxOptions) error {
	abortRetry := func(err error) bool {
		if err == nil ||
			errors.Is(err, gorm.ErrRecordNotFound) ||
			errors.Is(err, errInvalidNumberOfShards) ||
			errors.Is(err, errShardRootChanged) ||
			errors.Is(err, api.ErrContractNotFound) ||
			errors.Is(err, api.ErrObjectNotFound) ||
			errors.Is(err, api.ErrObjectCorrupted) ||
			errors.Is(err, api.ErrBucketExists) ||
			errors.Is(err, api.ErrBucketNotFound) ||
			errors.Is(err, api.ErrBucketNotEmpty) ||
			errors.Is(err, api.ErrContractNotFound) ||
			errors.Is(err, api.ErrMultipartUploadNotFound) ||
			errors.Is(err, api.ErrObjectExists) ||
			strings.Contains(err.Error(), "no such table") ||
			strings.Contains(err.Error(), "Duplicate entry") ||
			errors.Is(err, api.ErrPartNotFound) ||
			errors.Is(err, api.ErrSlabNotFound) ||
			errors.Is(err, ErrPeerNotFound) {
			return true
		}
		return false
	}
	var err error
	for i := 0; i < len(intervals); i++ {
		err = db.Transaction(fc, opts...)
		if abortRetry(err) {
			return err
		}
		logger.Warn(fmt.Sprintf("transaction attempt %d/%d failed, retry in %v,  err: %v", i+1, len(intervals), intervals[i], err))
		time.Sleep(intervals[i])
	}
	return fmt.Errorf("retryTransaction failed: %w", err)
}

func (s *SQLStore) retryTransaction(fc func(tx *gorm.DB) error, opts ...*sql.TxOptions) error {
	return retryTransaction(s.db, s.logger, fc, s.retryTransactionIntervals, opts...)
}
