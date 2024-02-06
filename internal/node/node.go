package node

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/gateway"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils"
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

// RHP4 TODOs:
// - get rid of dbConsensusInfo
// - get rid of returned chain manager in bus constructor
// - all wallet metrics support
// - add UPNP support

type BusConfig struct {
	config.Bus
	Network             *consensus.Network
	Genesis             types.Block
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

func NewBus(cfg BusConfig, dir string, seed types.PrivateKey, logger *zap.Logger) (http.Handler, ShutdownFn, *chain.Manager, error) {
	// If no DB dialector was provided, use SQLite.
	dbConn := cfg.DBDialector
	if dbConn == nil {
		dbDir := filepath.Join(dir, "db")
		if err := os.MkdirAll(dbDir, 0700); err != nil {
			return nil, nil, nil, err
		}
		dbConn = stores.NewSQLiteConnection(filepath.Join(dbDir, "db.sqlite"))
	}
	dbMetricsConn := cfg.DBMetricsDialector
	if dbMetricsConn == nil {
		dbDir := filepath.Join(dir, "db")
		if err := os.MkdirAll(dbDir, 0700); err != nil {
			return nil, nil, nil, err
		}
		dbMetricsConn = stores.NewSQLiteConnection(filepath.Join(dbDir, "metrics.sqlite"))
	}

	consensusDir := filepath.Join(dir, "consensus")
	if err := os.MkdirAll(consensusDir, 0700); err != nil {
		return nil, nil, nil, err
	}
	bdb, err := coreutils.OpenBoltChainDB(filepath.Join(dir, "consensus.db"))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to open consensus database: %w", err)
	}

	alertsMgr := alerts.NewManager()
	sqlLogger := stores.NewSQLLogger(logger.Named("db"), cfg.DBLoggerConfig)
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
		Logger:                        logger.Sugar(),
		GormLogger:                    sqlLogger,
		RetryTransactionIntervals:     []time.Duration{200 * time.Millisecond, 500 * time.Millisecond, time.Second, 3 * time.Second, 10 * time.Second, 10 * time.Second},
	})
	if err != nil {
		return nil, nil, nil, err
	}
	wh, err := webhooks.NewManager(logger.Named("webhooks").Sugar(), sqlStore)
	if err != nil {
		return nil, nil, nil, err
	}

	// Hook up webhooks to alerts.
	alertsMgr.RegisterWebhookBroadcaster(wh)

	// create chain manager
	store, state, err := chain.NewDBStore(bdb, cfg.Network, cfg.Genesis)
	if err != nil {
		return nil, nil, nil, err
	}
	cm := chain.NewManager(store, state)

	// create wallet
	w, err := wallet.NewSingleAddressWallet(seed, cm, sqlStore, wallet.WithReservationDuration(cfg.UsedUTXOExpiry))
	if err != nil {
		return nil, nil, nil, err
	}

	// create syncer
	l, err := net.Listen("tcp", cfg.GatewayAddr)
	if err != nil {
		return nil, nil, nil, err
	}
	syncerAddr := l.Addr().String()

	// peers will reject us if our hostname is empty or unspecified, so use loopback
	host, port, _ := net.SplitHostPort(syncerAddr)
	if ip := net.ParseIP(host); ip == nil || ip.IsUnspecified() {
		syncerAddr = net.JoinHostPort("127.0.0.1", port)
	}

	header := gateway.Header{
		GenesisID:  cfg.Genesis.ID(),
		UniqueID:   gateway.GenerateUniqueID(),
		NetAddress: syncerAddr,
	}
	s := syncer.New(l, cm, sqlStore, header)

	b, err := bus.New(alertsMgr, wh, cm, s, w, sqlStore, sqlStore, sqlStore, sqlStore, sqlStore, sqlStore, logger)
	if err != nil {
		return nil, nil, nil, err
	}

	// bootstrap the syncer
	if cfg.Bootstrap {
		if cfg.Network == nil {
			return nil, nil, nil, errors.New("cannot bootstrap without a network")
		}
		bootstrap(*cfg.Network, sqlStore)
	}

	// start the syncer
	go s.Run()

	// subscribe the store to the chain manager
	err = cm.AddSubscriber(sqlStore, types.ChainIndex{})
	if err != nil {
		return nil, nil, nil, err
	}

	shutdownFn := func(ctx context.Context) error {
		return errors.Join(
			l.Close(),
			b.Shutdown(ctx),
			sqlStore.Close(),
			bdb.Close(),
		)
	}
	return b.Handler(), shutdownFn, cm, nil
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
