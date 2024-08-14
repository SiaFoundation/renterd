package node

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/renterd/alerts"
	"go.sia.tech/renterd/autopilot"
	"go.sia.tech/renterd/bus"
	"go.sia.tech/renterd/config"
	"go.sia.tech/renterd/internal/chain"
	"go.sia.tech/renterd/internal/utils"
	"go.sia.tech/renterd/stores"
	"go.sia.tech/renterd/stores/sql"
	"go.sia.tech/renterd/stores/sql/mysql"
	"go.sia.tech/renterd/stores/sql/sqlite"
	"go.sia.tech/renterd/webhooks"
	"go.sia.tech/renterd/worker"
	"go.sia.tech/renterd/worker/s3"
	"go.uber.org/zap"
	"golang.org/x/crypto/blake2b"
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
	SyncerSyncInterval          time.Duration
	SyncerPeerDiscoveryInterval time.Duration
}

type AutopilotConfig struct {
	config.Autopilot
	ID string
}

type (
	RunFn         = func() error
	BusSetupFn    = func()
	WorkerSetupFn = func(context.Context, string, string) error
	ShutdownFn    = func(context.Context) error
)

var NoopFn = func(context.Context) error { return nil }

func NewBus(cfg BusConfig, dir string, seed types.PrivateKey, logger *zap.Logger) (http.Handler, BusSetupFn, ShutdownFn, *chain.Manager, *chain.ChainSubscriber, error) {
	// ensure we don't hang indefinitely
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// create database connections
	var dbMain sql.Database
	var dbMetrics sql.MetricsDatabase
	if cfg.Database.MySQL.URI != "" {
		// create MySQL connections
		connMain, err := mysql.Open(
			cfg.Database.MySQL.User,
			cfg.Database.MySQL.Password,
			cfg.Database.MySQL.URI,
			cfg.Database.MySQL.Database,
		)
		if err != nil {
			return nil, nil, nil, nil, nil, fmt.Errorf("failed to open MySQL main database: %w", err)
		}
		connMetrics, err := mysql.Open(
			cfg.Database.MySQL.User,
			cfg.Database.MySQL.Password,
			cfg.Database.MySQL.URI,
			cfg.Database.MySQL.MetricsDatabase,
		)
		if err != nil {
			return nil, nil, nil, nil, nil, fmt.Errorf("failed to open MySQL metrics database: %w", err)
		}
		dbMain, err = mysql.NewMainDatabase(connMain, logger.Named("main").Sugar(), cfg.DatabaseLog.SlowThreshold, cfg.DatabaseLog.SlowThreshold)
		if err != nil {
			return nil, nil, nil, nil, nil, fmt.Errorf("failed to create MySQL main database: %w", err)
		}
		dbMetrics, err = mysql.NewMetricsDatabase(connMetrics, logger.Named("metrics").Sugar(), cfg.DatabaseLog.SlowThreshold, cfg.DatabaseLog.SlowThreshold)
		if err != nil {
			return nil, nil, nil, nil, nil, fmt.Errorf("failed to create MySQL metrics database: %w", err)
		}
	} else {
		// create database directory
		dbDir := filepath.Join(dir, "db")
		if err := os.MkdirAll(dbDir, 0700); err != nil {
			return nil, nil, nil, nil, nil, err
		}

		// create SQLite connections
		db, err := sqlite.Open(filepath.Join(dbDir, "db.sqlite"))
		if err != nil {
			return nil, nil, nil, nil, nil, fmt.Errorf("failed to open SQLite main database: %w", err)
		}
		dbMain, err = sqlite.NewMainDatabase(db, logger.Named("main").Sugar(), cfg.DatabaseLog.SlowThreshold, cfg.DatabaseLog.SlowThreshold)
		if err != nil {
			return nil, nil, nil, nil, nil, fmt.Errorf("failed to create SQLite main database: %w", err)
		}

		dbm, err := sqlite.Open(filepath.Join(dbDir, "metrics.sqlite"))
		if err != nil {
			return nil, nil, nil, nil, nil, fmt.Errorf("failed to open SQLite metrics database: %w", err)
		}
		dbMetrics, err = sqlite.NewMetricsDatabase(dbm, logger.Named("metrics").Sugar(), cfg.DatabaseLog.SlowThreshold, cfg.DatabaseLog.SlowThreshold)
		if err != nil {
			return nil, nil, nil, nil, nil, fmt.Errorf("failed to create SQLite metrics database: %w", err)
		}
	}

	alertsMgr := alerts.NewManager()
	sqlStoreDir := filepath.Join(dir, "partial_slabs")
	sqlStore, err := stores.NewSQLStore(stores.Config{
		Alerts:                        alerts.WithOrigin(alertsMgr, "bus"),
		DB:                            dbMain,
		DBMetrics:                     dbMetrics,
		PartialSlabDir:                sqlStoreDir,
		Migrate:                       true,
		SlabBufferCompletionThreshold: cfg.SlabBufferCompletionThreshold,
		Logger:                        logger.Sugar(),
		RetryTransactionIntervals:     cfg.RetryTxIntervals,
		WalletAddress:                 types.StandardUnlockHash(seed.PublicKey()),
		LongQueryDuration:             cfg.DatabaseLog.SlowThreshold,
		LongTxDuration:                cfg.DatabaseLog.SlowThreshold,
	})
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}

	// create webhooks manager
	wh, err := webhooks.NewManager(sqlStore, logger)
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}

	// hookup webhooks <-> alerts
	alertsMgr.RegisterWebhookBroadcaster(wh)

	// create consensus directory
	consensusDir := filepath.Join(dir, "consensus")
	if err := os.MkdirAll(consensusDir, 0700); err != nil {
		return nil, nil, nil, nil, nil, err
	}

	// migrate consensus database
	oldConsensus, err := os.Stat(filepath.Join(consensusDir, "consensus.db"))
	if err != nil && !os.IsNotExist(err) {
		return nil, nil, nil, nil, nil, err
	} else if err == nil {
		logger.Warn("found old consensus.db, indicating a migration is necessary")

		// reset chain state
		logger.Warn("Resetting chain state...")
		if err := sqlStore.ResetChainState(ctx); err != nil {
			return nil, nil, nil, nil, nil, err
		}
		logger.Warn("Chain state was successfully reset.")

		// remove consensus.db and consensus.log file
		logger.Warn("Removing consensus database...")
		_ = os.RemoveAll(filepath.Join(consensusDir, "consensus.log")) // ignore error
		if err := os.Remove(filepath.Join(consensusDir, "consensus.db")); err != nil {
			return nil, nil, nil, nil, nil, err
		}
		logger.Warn(fmt.Sprintf("Old 'consensus.db' was successfully removed, reclaimed %v of disk space.", utils.HumanReadableSize(int(oldConsensus.Size()))))
		logger.Warn("ATTENTION: consensus will now resync from scratch, this process may take several hours to complete")
	}

	// reset chain state if blockchain.db does not exist to make sure deleting
	// it forces a resync
	chainPath := filepath.Join(consensusDir, "blockchain.db")
	if _, err := os.Stat(chainPath); os.IsNotExist(err) {
		if err := sqlStore.ResetChainState(context.Background()); err != nil {
			return nil, nil, nil, nil, nil, err
		}
	}

	// create chain database
	bdb, err := coreutils.OpenBoltChainDB(chainPath)
	if err != nil {
		return nil, nil, nil, nil, nil, fmt.Errorf("failed to open chain database: %w", err)
	}

	// create chain manager
	store, state, err := chain.NewDBStore(bdb, cfg.Network, cfg.Genesis)
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}
	cm := chain.NewManager(store, state)

	// create wallet
	w, err := wallet.NewSingleAddressWallet(seed, cm, sqlStore, wallet.WithReservationDuration(cfg.UsedUTXOExpiry))
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}

	// create chain subscriber
	cs, err := chain.NewChainSubscriber(wh, cm, sqlStore, w, time.Duration(cfg.AnnouncementMaxAgeHours)*time.Hour, logger)
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}

	// create syncer
	s, err := NewSyncer(cfg, cm, sqlStore, logger)
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}

	b, err := bus.New(ctx, alertsMgr, wh, cm, sqlStore, s, w, sqlStore, sqlStore, sqlStore, sqlStore, sqlStore, sqlStore, logger)
	if err != nil {
		return nil, nil, nil, nil, nil, err
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
	return b.Handler(), b.Setup, shutdownFn, cm, cs, nil
}

func NewWorker(cfg config.Worker, s3Opts s3.Opts, b Bus, seed types.PrivateKey, l *zap.Logger) (http.Handler, http.Handler, WorkerSetupFn, ShutdownFn, error) {
	workerKey := blake2b.Sum256(append([]byte("worker"), seed...))
	w, err := worker.New(workerKey, cfg.ID, b, cfg.ContractLockTimeout, cfg.BusFlushInterval, cfg.DownloadOverdriveTimeout, cfg.UploadOverdriveTimeout, cfg.DownloadMaxOverdrive, cfg.UploadMaxOverdrive, cfg.DownloadMaxMemory, cfg.UploadMaxMemory, cfg.AllowPrivateIPs, l)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	s3Handler, err := s3.New(b, w, l.Named("s3").Sugar(), s3Opts)
	if err != nil {
		err = errors.Join(err, w.Shutdown(context.Background()))
		return nil, nil, nil, nil, fmt.Errorf("failed to create s3 handler: %w", err)
	}
	return w.Handler(), s3Handler, w.Setup, w.Shutdown, nil
}

func NewAutopilot(cfg AutopilotConfig, b autopilot.Bus, workers []autopilot.Worker, l *zap.Logger) (http.Handler, RunFn, ShutdownFn, error) {
	ap, err := autopilot.New(cfg.ID, b, workers, l, cfg.Heartbeat, cfg.ScannerInterval, cfg.ScannerBatchSize, cfg.ScannerNumThreads, cfg.MigrationHealthCutoff, cfg.AccountsRefillInterval, cfg.RevisionSubmissionBuffer, cfg.MigratorParallelSlabsPerWorker, cfg.RevisionBroadcastInterval)
	if err != nil {
		return nil, nil, nil, err
	}
	return ap.Handler(), ap.Run, ap.Shutdown, nil
}
