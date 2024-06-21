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
	"go.sia.tech/renterd/alerts"
	"go.sia.tech/renterd/autopilot"
	"go.sia.tech/renterd/bus"
	"go.sia.tech/renterd/config"
	"go.sia.tech/renterd/stores"
	"go.sia.tech/renterd/stores/sql"
	"go.sia.tech/renterd/stores/sql/mysql"
	"go.sia.tech/renterd/stores/sql/sqlite"
	"go.sia.tech/renterd/wallet"
	"go.sia.tech/renterd/webhooks"
	"go.sia.tech/renterd/worker"
	"go.sia.tech/renterd/worker/s3"
	"go.sia.tech/siad/modules"
	mconsensus "go.sia.tech/siad/modules/consensus"
	"go.sia.tech/siad/modules/gateway"
	"go.sia.tech/siad/modules/transactionpool"
	"go.sia.tech/siad/sync"
	"go.uber.org/zap"
	"golang.org/x/crypto/blake2b"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"moul.io/zapgorm2"
)

type Bus interface {
	worker.Bus
	s3.Bus
}

type BusConfig struct {
	config.Bus
	Database    config.Database
	DatabaseLog config.DatabaseLog
	Network     *consensus.Network
	Logger      *zap.Logger
	Miner       *Miner
}

type AutopilotConfig struct {
	config.Autopilot
	ID string
}

type (
	RunFn         = func() error
	BusSetupFn    = func(context.Context) error
	WorkerSetupFn = func(context.Context, string, string) error
	ShutdownFn    = func(context.Context) error
)

var NoopFn = func(context.Context) error { return nil }

func NewBus(cfg BusConfig, dir string, seed types.PrivateKey, l *zap.Logger) (http.Handler, BusSetupFn, ShutdownFn, error) {
	gatewayDir := filepath.Join(dir, "gateway")
	if err := os.MkdirAll(gatewayDir, 0700); err != nil {
		return nil, nil, nil, err
	}
	g, err := gateway.New(cfg.GatewayAddr, cfg.Bootstrap, gatewayDir)
	if err != nil {
		return nil, nil, nil, err
	}
	consensusDir := filepath.Join(dir, "consensus")
	if err := os.MkdirAll(consensusDir, 0700); err != nil {
		return nil, nil, nil, err
	}
	cs, errCh := mconsensus.New(g, cfg.Bootstrap, consensusDir)
	select {
	case err := <-errCh:
		if err != nil {
			return nil, nil, nil, err
		}
	default:
		go func() {
			if err := <-errCh; err != nil {
				log.Println("WARNING: consensus initialization returned an error:", err)
			}
		}()
	}
	tpoolDir := filepath.Join(dir, "transactionpool")
	if err := os.MkdirAll(tpoolDir, 0700); err != nil {
		return nil, nil, nil, err
	}
	tp, err := transactionpool.New(cs, g, tpoolDir)
	if err != nil {
		return nil, nil, nil, err
	}

	// create database connections
	var dbConn gorm.Dialector
	var dbMetrics sql.MetricsDatabase
	if cfg.Database.MySQL.URI != "" {
		// create MySQL connections
		dbConn = stores.NewMySQLConnection(
			cfg.Database.MySQL.User,
			cfg.Database.MySQL.Password,
			cfg.Database.MySQL.URI,
			cfg.Database.MySQL.Database,
		)
		dbm, err := mysql.Open(
			cfg.Database.MySQL.User,
			cfg.Database.MySQL.Password,
			cfg.Database.MySQL.URI,
			cfg.Database.MySQL.MetricsDatabase,
		)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("failed to open MySQL metrics database: %w", err)
		}
		dbMetrics, err = mysql.NewMetricsDatabase(dbm, l.Named("metrics").Sugar(), cfg.DatabaseLog.SlowThreshold, cfg.DatabaseLog.SlowThreshold)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("failed to create MySQL metrics database: %w", err)
		}
	} else {
		// create database directory
		dbDir := filepath.Join(dir, "db")
		if err := os.MkdirAll(dbDir, 0700); err != nil {
			return nil, nil, nil, err
		}

		// create SQLite connections
		dbConn = stores.NewSQLiteConnection(filepath.Join(dbDir, "db.sqlite"))

		dbm, err := sqlite.Open(filepath.Join(dbDir, "metrics.sqlite"))
		if err != nil {
			return nil, nil, nil, fmt.Errorf("failed to open SQLite metrics database: %w", err)
		}
		dbMetrics, err = sqlite.NewMetricsDatabase(dbm, l.Named("metrics").Sugar(), cfg.DatabaseLog.SlowThreshold, cfg.DatabaseLog.SlowThreshold)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("failed to create SQLite metrics database: %w", err)
		}
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
	walletAddr := wallet.StandardAddress(seed.PublicKey())
	sqlStoreDir := filepath.Join(dir, "partial_slabs")
	announcementMaxAge := time.Duration(cfg.AnnouncementMaxAgeHours) * time.Hour
	sqlStore, ccid, err := stores.NewSQLStore(stores.Config{
		Conn:                          dbConn,
		Alerts:                        alerts.WithOrigin(alertsMgr, "bus"),
		DBMetrics:                     dbMetrics,
		PartialSlabDir:                sqlStoreDir,
		Migrate:                       true,
		AnnouncementMaxAge:            announcementMaxAge,
		PersistInterval:               cfg.PersistInterval,
		WalletAddress:                 walletAddr,
		SlabBufferCompletionThreshold: cfg.SlabBufferCompletionThreshold,
		Logger:                        l.Sugar(),
		GormLogger:                    dbLogger,
		RetryTransactionIntervals:     []time.Duration{200 * time.Millisecond, 500 * time.Millisecond, time.Second, 3 * time.Second, 10 * time.Second, 10 * time.Second},
		LongQueryDuration:             cfg.DatabaseLog.SlowThreshold,
		LongTxDuration:                cfg.DatabaseLog.SlowThreshold,
	})
	if err != nil {
		return nil, nil, nil, err
	}
	hooksMgr, err := webhooks.NewManager(l.Named("webhooks").Sugar(), sqlStore)
	if err != nil {
		return nil, nil, nil, err
	}

	// Hook up webhooks to alerts.
	alertsMgr.RegisterWebhookBroadcaster(hooksMgr)

	cancelSubscribe := make(chan struct{})
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()

		subscribeErr := cs.ConsensusSetSubscribe(sqlStore, ccid, cancelSubscribe)
		if errors.Is(subscribeErr, modules.ErrInvalidConsensusChangeID) {
			l.Warn("Invalid consensus change ID detected - resyncing consensus")
			// Reset the consensus state within the database and rescan.
			if err := sqlStore.ResetConsensusSubscription(ctx); err != nil {
				l.Fatal(fmt.Sprintf("Failed to reset consensus subscription of SQLStore: %v", err))
				return
			}
			// Subscribe from the beginning.
			subscribeErr = cs.ConsensusSetSubscribe(sqlStore, modules.ConsensusChangeBeginning, cancelSubscribe)
		}
		if subscribeErr != nil && !errors.Is(subscribeErr, sync.ErrStopped) {
			l.Fatal(fmt.Sprintf("ConsensusSetSubscribe returned an error: %v", err))
		}
	}()

	w := wallet.NewSingleAddressWallet(seed, sqlStore, cfg.UsedUTXOExpiry, zap.NewNop().Sugar())
	tp.TransactionPoolSubscribe(w)
	if err := cs.ConsensusSetSubscribe(w, modules.ConsensusChangeRecent, nil); err != nil {
		return nil, nil, nil, err
	}

	if m := cfg.Miner; m != nil {
		if err := cs.ConsensusSetSubscribe(m, ccid, nil); err != nil {
			return nil, nil, nil, err
		}
		tp.TransactionPoolSubscribe(m)
	}

	cm, err := NewChainManager(cs, NewTransactionPool(tp), cfg.Network)
	if err != nil {
		return nil, nil, nil, err
	}

	b, err := bus.New(syncer{g, tp}, alertsMgr, hooksMgr, cm, NewTransactionPool(tp), w, sqlStore, sqlStore, sqlStore, sqlStore, sqlStore, sqlStore, l)
	if err != nil {
		return nil, nil, nil, err
	}

	shutdownFn := func(ctx context.Context) error {
		close(cancelSubscribe)
		return errors.Join(
			g.Close(),
			cs.Close(),
			tp.Close(),
			b.Shutdown(ctx),
			sqlStore.Close(),
		)
	}
	return b.Handler(), b.Setup, shutdownFn, nil
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
