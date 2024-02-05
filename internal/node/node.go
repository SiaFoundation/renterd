package node

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/gateway"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/syncer"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/renterd/alerts"
	"go.sia.tech/renterd/autopilot"
	"go.sia.tech/renterd/bus"
	"go.sia.tech/renterd/config"
	"go.sia.tech/renterd/stores"
	"go.sia.tech/renterd/webhooks"
	"go.sia.tech/renterd/worker"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/crypto/blake2b"
	"gorm.io/gorm"
)

type BusConfig struct {
	config.Bus
	Network             *consensus.Network
	Genesis             types.Block
	Miner               *Miner
	DBLoggerConfig      stores.LoggerConfig
	DBDialector         gorm.Dialector
	DBMetricsDialector  gorm.Dialector
	SlabPruningInterval time.Duration
	SlabPruningCooldown time.Duration
}

type AutopilotConfig struct {
	config.Autopilot
	ID string
}

type (
	RunFn      = func() error
	ShutdownFn = func(context.Context) error
)

func NewBus(cfg BusConfig, dir string, seed types.PrivateKey, l *zap.Logger) (http.Handler, ShutdownFn, error) {
	// If no DB dialector was provided, use SQLite.
	dbConn := cfg.DBDialector
	if dbConn == nil {
		dbDir := filepath.Join(dir, "db")
		if err := os.MkdirAll(dbDir, 0700); err != nil {
			return nil, nil, err
		}
		dbConn = stores.NewSQLiteConnection(filepath.Join(dbDir, "db.sqlite"))
	}
	dbMetricsConn := cfg.DBMetricsDialector
	if dbMetricsConn == nil {
		dbDir := filepath.Join(dir, "db")
		if err := os.MkdirAll(dbDir, 0700); err != nil {
			return nil, nil, err
		}
		dbMetricsConn = stores.NewSQLiteConnection(filepath.Join(dbDir, "metrics.sqlite"))
	}

	alertsMgr := alerts.NewManager()
	sqlLogger := stores.NewSQLLogger(l.Named("db"), cfg.DBLoggerConfig)
	walletAddr := types.StandardUnlockHash(seed.PublicKey())
	sqlStoreDir := filepath.Join(dir, "partial_slabs")
	announcementMaxAge := time.Duration(cfg.AnnouncementMaxAgeHours) * time.Hour
	sqlStore, _, err := stores.NewSQLStore(stores.Config{
		Conn:                          dbConn,
		ConnMetrics:                   dbMetricsConn,
		Alerts:                        alerts.WithOrigin(alertsMgr, "bus"),
		PartialSlabDir:                sqlStoreDir,
		Migrate:                       true,
		AnnouncementMaxAge:            announcementMaxAge,
		PersistInterval:               cfg.PersistInterval,
		WalletAddress:                 walletAddr,
		SlabBufferCompletionThreshold: cfg.SlabBufferCompletionThreshold,
		Logger:                        l.Sugar(),
		GormLogger:                    sqlLogger,
		RetryTransactionIntervals:     []time.Duration{200 * time.Millisecond, 500 * time.Millisecond, time.Second, 3 * time.Second, 10 * time.Second, 10 * time.Second},
	})
	if err != nil {
		return nil, nil, err
	}
	hooksMgr, err := webhooks.NewManager(l.Named("webhooks").Sugar(), sqlStore)
	if err != nil {
		return nil, nil, err
	}

	// Hook up webhooks to alerts.
	alertsMgr.RegisterWebhookBroadcaster(hooksMgr)

	// TODO: should we have something similar to ResetConsensusSubscription?
	// TODO: should we get rid of modules.ConsensusChangeID all together?

	// create chain manager
	chainDB := chain.NewMemDB()
	store, state, err := chain.NewDBStore(chainDB, cfg.Network, cfg.Genesis)
	if err != nil {
		return nil, nil, err
	}
	cm := chain.NewManager(store, state)

	// TODO: we stopped recording wallet metrics,

	// create wallet
	w, err := wallet.NewSingleAddressWallet(seed, cm, sqlStore, wallet.WithReservationDuration(cfg.UsedUTXOExpiry))
	if err != nil {
		return nil, nil, err
	}

	// create syncer
	gwl, err := net.Listen("tcp", cfg.GatewayAddr)
	if err != nil {
		return nil, nil, err
	}

	// TODO: implement peer store
	// TODO: not sure what to pass as header here
	// TODO: reuse the gateway dir? create new peer dir?
	s := syncer.New(gwl, cm, NewPeerStore(), gateway.Header{
		UniqueID:   gateway.GenerateUniqueID(),
		GenesisID:  cfg.Genesis.ID(),
		NetAddress: cfg.GatewayAddr,
	})

	b, err := bus.New(alertsMgr, hooksMgr, cm, s, w, sqlStore, sqlStore, sqlStore, sqlStore, sqlStore, sqlStore, l)
	if err != nil {
		return nil, nil, err
	}

	shutdownFn := func(ctx context.Context) error {
		return errors.Join(
			gwl.Close(),
			b.Shutdown(ctx),
			sqlStore.Close(),
		)
	}
	return b.Handler(), shutdownFn, nil
}

func NewWorker(cfg config.Worker, b worker.Bus, seed types.PrivateKey, l *zap.Logger) (http.Handler, ShutdownFn, error) {
	workerKey := blake2b.Sum256(append([]byte("worker"), seed...))
	w, err := worker.New(workerKey, cfg.ID, b, cfg.ContractLockTimeout, cfg.BusFlushInterval, cfg.DownloadOverdriveTimeout, cfg.UploadOverdriveTimeout, cfg.DownloadMaxOverdrive, cfg.DownloadMaxMemory, cfg.UploadMaxMemory, cfg.UploadMaxOverdrive, cfg.AllowPrivateIPs, l)
	if err != nil {
		return nil, nil, err
	}

	return w.Handler(), w.Shutdown, nil
}

func NewAutopilot(cfg AutopilotConfig, b autopilot.Bus, workers []autopilot.Worker, l *zap.Logger) (http.Handler, RunFn, ShutdownFn, error) {
	ap, err := autopilot.New(cfg.ID, b, workers, l, cfg.Heartbeat, cfg.ScannerInterval, cfg.ScannerBatchSize, cfg.ScannerNumThreads, cfg.MigrationHealthCutoff, cfg.AccountsRefillInterval, cfg.RevisionSubmissionBuffer, cfg.MigratorParallelSlabsPerWorker, cfg.RevisionBroadcastInterval)
	if err != nil {
		return nil, nil, nil, err
	}
	return ap.Handler(), ap.Run, ap.Shutdown, nil
}

func NewLogger(path string) (*zap.Logger, func(context.Context) error, error) {
	writer, closeFn, err := zap.Open(path)
	if err != nil {
		return nil, nil, err
	}

	// console
	config := zap.NewProductionEncoderConfig()
	config.EncodeTime = zapcore.RFC3339TimeEncoder
	config.EncodeLevel = zapcore.CapitalColorLevelEncoder
	config.StacktraceKey = ""
	consoleEncoder := zapcore.NewConsoleEncoder(config)

	// file
	config = zap.NewProductionEncoderConfig()
	config.EncodeTime = zapcore.RFC3339TimeEncoder
	config.CallerKey = ""     // hide
	config.StacktraceKey = "" // hide
	config.NameKey = "component"
	config.TimeKey = "date"
	fileEncoder := zapcore.NewJSONEncoder(config)

	core := zapcore.NewTee(
		zapcore.NewCore(fileEncoder, writer, zapcore.DebugLevel),
		zapcore.NewCore(consoleEncoder, zapcore.AddSync(os.Stdout), zapcore.DebugLevel),
	)

	logger := zap.New(
		core,
		zap.AddCaller(),
		zap.AddStacktrace(zapcore.ErrorLevel),
	)

	return logger, func(_ context.Context) error {
		_ = logger.Sync() // ignore Error
		closeFn()
		return nil
	}, nil
}
