package node

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/alerts"
	"go.sia.tech/renterd/autopilot"
	"go.sia.tech/renterd/bus"
	"go.sia.tech/renterd/config"
	"go.sia.tech/renterd/stores"
	"go.sia.tech/renterd/wallet"
	"go.sia.tech/renterd/webhooks"
	"go.sia.tech/renterd/worker"
	"go.sia.tech/siad/modules"
	mconsensus "go.sia.tech/siad/modules/consensus"
	"go.sia.tech/siad/modules/gateway"
	"go.sia.tech/siad/modules/transactionpool"
	"go.sia.tech/siad/sync"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/crypto/blake2b"
	"gorm.io/gorm"
)

type BusConfig struct {
	config.Bus
	Network        *consensus.Network
	Miner          *Miner
	DBLoggerConfig stores.LoggerConfig
	DBDialector    gorm.Dialector
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
	gatewayDir := filepath.Join(dir, "gateway")
	if err := os.MkdirAll(gatewayDir, 0700); err != nil {
		return nil, nil, err
	}
	g, err := gateway.New(cfg.GatewayAddr, cfg.Bootstrap, gatewayDir)
	if err != nil {
		return nil, nil, err
	}
	consensusDir := filepath.Join(dir, "consensus")
	if err := os.MkdirAll(consensusDir, 0700); err != nil {
		return nil, nil, err
	}
	cs, errCh := mconsensus.New(g, cfg.Bootstrap, consensusDir)
	select {
	case err := <-errCh:
		if err != nil {
			return nil, nil, err
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
		return nil, nil, err
	}
	tp, err := transactionpool.New(cs, g, tpoolDir)
	if err != nil {
		return nil, nil, err
	}

	// If no DB dialector was provided, use SQLite.
	dbConn := cfg.DBDialector
	if dbConn == nil {
		dbDir := filepath.Join(dir, "db")
		if err := os.MkdirAll(dbDir, 0700); err != nil {
			return nil, nil, err
		}
		dbConn = stores.NewSQLiteConnection(filepath.Join(dbDir, "db.sqlite"))
	}

	alertsMgr := alerts.NewManager()
	sqlLogger := stores.NewSQLLogger(l.Named("db"), cfg.DBLoggerConfig)
	walletAddr := wallet.StandardAddress(seed.PublicKey())
	sqlStoreDir := filepath.Join(dir, "partial_slabs")
	announcementMaxAge := time.Duration(cfg.AnnouncementMaxAgeHours) * time.Hour
	sqlStore, ccid, err := stores.NewSQLStore(dbConn, alerts.WithOrigin(alertsMgr, "bus"), sqlStoreDir, true, announcementMaxAge, cfg.PersistInterval, walletAddr, cfg.SlabBufferCompletionThreshold, l.Sugar(), sqlLogger)
	if err != nil {
		return nil, nil, err
	}
	hooksMgr, err := webhooks.NewManager(l.Named("webhooks").Sugar(), sqlStore)
	if err != nil {
		return nil, nil, err
	}

	// Hook up webhooks to alerts.
	alertsMgr.RegisterWebhookBroadcaster(hooksMgr)

	cancelSubscribe := make(chan struct{})
	go func() {
		subscribeErr := cs.ConsensusSetSubscribe(sqlStore, ccid, cancelSubscribe)
		if errors.Is(subscribeErr, modules.ErrInvalidConsensusChangeID) {
			l.Warn("Invalid consensus change ID detected - resyncing consensus")
			// Reset the consensus state within the database and rescan.
			if err := sqlStore.ResetConsensusSubscription(); err != nil {
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

	if m := cfg.Miner; m != nil {
		if err := cs.ConsensusSetSubscribe(m, ccid, nil); err != nil {
			return nil, nil, err
		}
		tp.TransactionPoolSubscribe(m)
	}

	cm, err := NewChainManager(cs, cfg.Network)
	if err != nil {
		return nil, nil, err
	}

	b, err := bus.New(syncer{g, tp}, alertsMgr, hooksMgr, cm, NewTransactionPool(tp), w, sqlStore, sqlStore, sqlStore, sqlStore, sqlStore, l)
	if err != nil {
		return nil, nil, err
	}

	shutdownFn := func(ctx context.Context) error {
		return errors.Join(
			func() error {
				close(cancelSubscribe)
				return nil
			}(),
			g.Close(),
			cs.Close(),
			tp.Close(),
			b.Shutdown(ctx),
			sqlStore.Close(),
		)
	}
	return b.Handler(), shutdownFn, nil
}

func NewWorker(cfg config.Worker, b worker.Bus, seed types.PrivateKey, l *zap.Logger) (http.Handler, ShutdownFn, error) {
	workerKey := blake2b.Sum256(append([]byte("worker"), seed...))
	w, err := worker.New(workerKey, cfg.ID, b, cfg.ContractLockTimeout, cfg.BusFlushInterval, cfg.DownloadOverdriveTimeout, cfg.UploadOverdriveTimeout, cfg.DownloadMaxOverdrive, cfg.UploadMaxOverdrive, cfg.AllowPrivateIPs, l)
	if err != nil {
		return nil, nil, err
	}

	return w.Handler(), w.Shutdown, nil
}

func NewAutopilot(cfg AutopilotConfig, b autopilot.Bus, workers []autopilot.Worker, l *zap.Logger) (http.Handler, RunFn, ShutdownFn, error) {
	ap, err := autopilot.New(cfg.ID, b, workers, l, cfg.Heartbeat, cfg.ScannerInterval, cfg.ScannerBatchSize, cfg.ScannerMinRecentFailures, cfg.ScannerNumThreads, cfg.MigrationHealthCutoff, cfg.AccountsRefillInterval, cfg.RevisionSubmissionBuffer, cfg.MigratorParallelSlabsPerWorker, cfg.RevisionBroadcastInterval)
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
