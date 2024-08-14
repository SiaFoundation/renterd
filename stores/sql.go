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
	"go.uber.org/zap"
)

type (
	// Config contains all params for creating a SQLStore
	Config struct {
		DB                            sql.Database
		DBMetrics                     sql.MetricsDatabase
		Alerts                        alerts.Alerter
		PartialSlabDir                string
		Migrate                       bool
		AnnouncementMaxAge            time.Duration
		WalletAddress                 types.Address
		SlabBufferCompletionThreshold int64
		Logger                        *zap.SugaredLogger
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
	}
)

// NewSQLStore uses a given Dialector to connect to a SQL database.  NOTE: Only
// pass migrate=true for the first instance of SQLHostDB if you connect via the
// same Dialector multiple times.
func NewSQLStore(cfg Config) (*SQLStore, error) {
	if err := os.MkdirAll(cfg.PartialSlabDir, 0700); err != nil {
		return nil, fmt.Errorf("failed to create partial slab dir '%s': %v", cfg.PartialSlabDir, err)
	}
	l := cfg.Logger.Named("sql")
	dbMain := cfg.DB
	dbMetrics := cfg.DBMetrics

	// Print DB version
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
