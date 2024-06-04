package node

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/renterd/alerts"
	"go.sia.tech/renterd/autopilot"
	"go.sia.tech/renterd/bus"
	"go.sia.tech/renterd/config"
	ibus "go.sia.tech/renterd/internal/bus"
	"go.sia.tech/renterd/internal/chain"
	"go.sia.tech/renterd/stores"
	"go.sia.tech/renterd/webhooks"
	"go.sia.tech/renterd/worker"
	"go.sia.tech/renterd/worker/s3"
	"go.uber.org/zap"
	"golang.org/x/crypto/blake2b"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"moul.io/zapgorm2"
)

// TODOs:
// - add wallet metrics
// - add UPNP support

type Bus interface {
	worker.Bus
	s3.Bus
}

type BusConfig struct {
	config.Bus
	Database                    config.Database
	DatabaseLog                 config.DatabaseLog
	Genesis                     types.Block
	Logger                      *zap.Logger
	Network                     *consensus.Network
	RetryTxIntervals            []time.Duration
	SlabPruningInterval         time.Duration
	SyncerSyncInterval          time.Duration
	SyncerPeerDiscoveryInterval time.Duration
}

type AutopilotConfig struct {
	config.Autopilot
	ID string
}

type (
	RunFn      = func() error
	ShutdownFn = func(context.Context) error
)

func NewBus(cfg BusConfig, dir string, seed types.PrivateKey, logger *zap.Logger) (http.Handler, ShutdownFn, *chain.Manager, *chain.ChainSubscriber, error) {
	// create consensus directory
	consensusDir := filepath.Join(dir, "consensus")
	if err := os.MkdirAll(consensusDir, 0700); err != nil {
		return nil, nil, nil, nil, err
	}
	bdb, err := coreutils.OpenBoltChainDB(filepath.Join(consensusDir, "chain.db"))
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("failed to open chain database: %w", err)
	}

	// create database connections
	var dbConn, dbMetricsConn gorm.Dialector
	if cfg.Database.MySQL.URI != "" {
		// create MySQL connections
		dbConn = stores.NewMySQLConnection(
			cfg.Database.MySQL.User,
			cfg.Database.MySQL.Password,
			cfg.Database.MySQL.URI,
			cfg.Database.MySQL.Database,
		)
		dbMetricsConn = stores.NewMySQLConnection(
			cfg.Database.MySQL.User,
			cfg.Database.MySQL.Password,
			cfg.Database.MySQL.URI,
			cfg.Database.MySQL.MetricsDatabase,
		)
	} else {
		// create database directory
		dbDir := filepath.Join(dir, "db")
		if err := os.MkdirAll(dbDir, 0700); err != nil {
			return nil, nil, nil, nil, err
		}

		// create SQLite connections
		dbConn = stores.NewSQLiteConnection(filepath.Join(dbDir, "db.sqlite"))
		dbMetricsConn = stores.NewSQLiteConnection(filepath.Join(dbDir, "metrics.sqlite"))
	}

	// create database logger
	dbLogger := zapgorm2.Logger{
		ZapLogger:                 cfg.Logger.Named("SQL"),
		LogLevel:                  gormLogLevel(cfg.DatabaseLog),
		SlowThreshold:             cfg.DatabaseLog.SlowThreshold,
		SkipCallerLookup:          false,
		IgnoreRecordNotFoundError: cfg.DatabaseLog.IgnoreRecordNotFoundError,
		Context:                   nil,
	}

	alertsMgr := alerts.NewManager()
	sqlStoreDir := filepath.Join(dir, "partial_slabs")
	sqlStore, err := stores.NewSQLStore(stores.Config{
		Conn:                          dbConn,
		ConnMetrics:                   dbMetricsConn,
		Alerts:                        alerts.WithOrigin(alertsMgr, "bus"),
		PartialSlabDir:                sqlStoreDir,
		Migrate:                       true,
		SlabBufferCompletionThreshold: cfg.SlabBufferCompletionThreshold,
		Logger:                        logger.Sugar(),
		GormLogger:                    dbLogger,
		RetryTransactionIntervals:     cfg.RetryTxIntervals,
		WalletAddress:                 types.StandardUnlockHash(seed.PublicKey()),
		LongQueryDuration:             cfg.DatabaseLog.SlowThreshold,
		LongTxDuration:                cfg.DatabaseLog.SlowThreshold,
	})
	if err != nil {
		return nil, nil, nil, nil, err
	}

	// create webhooks manager
	wh, err := webhooks.NewManager(sqlStore, logger)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	// hookup webhooks <-> alerts
	alertsMgr.RegisterWebhookBroadcaster(wh)

	// create event broadcaster
	events := ibus.NewEventBroadcaster(wh, logger)

	// create chain manager
	store, state, err := chain.NewDBStore(bdb, cfg.Network, cfg.Genesis)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	cm := chain.NewManager(store, state)

	// create chain subscriber
	cs, err := chain.NewChainSubscriber(cm, sqlStore, events, types.StandardUnlockHash(seed.PublicKey()), time.Duration(cfg.AnnouncementMaxAgeHours)*time.Hour, logger.Named("chainsubscriber"))
	if err != nil {
		return nil, nil, nil, nil, err
	}

	// create wallet
	w, err := wallet.NewSingleAddressWallet(seed, cm, sqlStore, wallet.WithReservationDuration(cfg.UsedUTXOExpiry))
	if err != nil {
		return nil, nil, nil, nil, err
	}

	// create syncer
	s, err := NewSyncer(cfg, cm, sqlStore, logger)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	b, err := bus.New(alertsMgr, wh, cm, sqlStore, s, w, sqlStore, sqlStore, sqlStore, sqlStore, sqlStore, sqlStore, logger)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	shutdownFn := func(ctx context.Context) error {
		return errors.Join(
			cs.Close(),
			s.Close(),
			w.Close(),
			b.Shutdown(ctx),
			sqlStore.Close(),
			bdb.Close(),
		)
	}
	return b.Handler(), shutdownFn, cm, cs, nil
}

func NewWorker(cfg config.Worker, s3Opts s3.Opts, b Bus, seed types.PrivateKey, l *zap.Logger) (http.Handler, http.Handler, ShutdownFn, error) {
	workerKey := blake2b.Sum256(append([]byte("worker"), seed...))
	w, err := worker.New(workerKey, cfg.ID, b, cfg.ContractLockTimeout, cfg.BusFlushInterval, cfg.DownloadOverdriveTimeout, cfg.UploadOverdriveTimeout, cfg.DownloadMaxOverdrive, cfg.UploadMaxOverdrive, cfg.DownloadMaxMemory, cfg.UploadMaxMemory, cfg.AllowPrivateIPs, l)
	if err != nil {
		return nil, nil, nil, err
	}
	s3Handler, err := s3.New(b, w, l.Named("s3").Sugar(), s3Opts)
	if err != nil {
		err = errors.Join(err, w.Shutdown(context.Background()))
		return nil, nil, nil, fmt.Errorf("failed to create s3 handler: %w", err)
	}
	return w.Handler(), s3Handler, w.Shutdown, nil
}

func NewAutopilot(cfg AutopilotConfig, b autopilot.Bus, workers []autopilot.Worker, l *zap.Logger) (http.Handler, RunFn, ShutdownFn, error) {
	ap, err := autopilot.New(cfg.ID, b, workers, l, cfg.Heartbeat, cfg.ScannerInterval, cfg.ScannerBatchSize, cfg.ScannerNumThreads, cfg.MigrationHealthCutoff, cfg.AccountsRefillInterval, cfg.RevisionSubmissionBuffer, cfg.MigratorParallelSlabsPerWorker, cfg.RevisionBroadcastInterval)
	if err != nil {
		return nil, nil, nil, err
	}
	return ap.Handler(), ap.Run, ap.Shutdown, nil
}

func gormLogLevel(cfg config.DatabaseLog) logger.LogLevel {
	level := logger.Silent
	if cfg.Enabled {
		switch strings.ToLower(cfg.Level) {
		case "":
			level = logger.Warn // default to 'warn' if not set
		case "error":
			level = logger.Error
		case "warn":
			level = logger.Warn
		case "info":
			level = logger.Info
		case "debug":
			level = logger.Info
		default:
			log.Fatalf("invalid log level %q, options are: silent, error, warn, info", cfg.Level)
		}
	}
	return level
}
