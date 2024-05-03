package node

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
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
	Database            config.Database
	DatabaseLog         config.DatabaseLog
	Genesis             types.Block
	Logger              *zap.Logger
	Network             *consensus.Network
	SlabPruningInterval time.Duration
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
	// create consensus directory
	consensusDir := filepath.Join(dir, "consensus")
	if err := os.MkdirAll(consensusDir, 0700); err != nil {
		return nil, nil, nil, err
	}
	bdb, err := coreutils.OpenBoltChainDB(filepath.Join(dir, "chain.db"))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to open chain database: %w", err)
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
			return nil, nil, nil, err
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
	walletAddr := types.StandardUnlockHash(seed.PublicKey())
	sqlStoreDir := filepath.Join(dir, "partial_slabs")
	announcementMaxAge := time.Duration(cfg.AnnouncementMaxAgeHours) * time.Hour
	sqlStore, err := stores.NewSQLStore(stores.Config{
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
		GormLogger:                    dbLogger,
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
	s := syncer.New(l, cm, sqlStore, header, syncer.WithSyncInterval(100*time.Millisecond), syncer.WithLogger(logger.Named("syncer")))

	b, err := bus.New(alertsMgr, wh, cm, s, w, sqlStore, sqlStore, sqlStore, sqlStore, sqlStore, sqlStore, logger)
	if err != nil {
		return nil, nil, nil, err
	}

	// bootstrap the syncer
	if cfg.Bootstrap {
		if cfg.Network == nil {
			return nil, nil, nil, errors.New("cannot bootstrap without a network")
		}

		var bootstrapPeers []string
		switch cfg.Network.Name {
		case "mainnet":
			bootstrapPeers = syncer.MainnetBootstrapPeers
		case "zen":
			bootstrapPeers = syncer.ZenBootstrapPeers
		case "anagami":
			bootstrapPeers = syncer.AnagamiBootstrapPeers
		default:
			return nil, nil, nil, fmt.Errorf("no available bootstrap peers for unknown network '%s'", cfg.Network.Name)
		}

		for _, addr := range bootstrapPeers {
			if err := sqlStore.AddPeer(addr); err != nil {
				return nil, nil, nil, fmt.Errorf("%w: failed to add bootstrap peer '%s'", err, addr)
			}
		}
	}

	// start the syncer
	go s.Run()

	// fetch chain index
	ci, err := sqlStore.ChainIndex()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("%w: failed to fetch chain index", err)
	}

	// subscribe the store to the chain manager
	err = cm.AddSubscriber(sqlStore, ci)
	if err != nil {
		return nil, nil, nil, err
	}

	shutdownFn := func(ctx context.Context) error {
		return errors.Join(
			l.Close(),
			w.Close(),
			b.Shutdown(ctx),
			sqlStore.Close(),
			store.Close(),
			bdb.Close(),
		)
	}
	return b.Handler(), shutdownFn, cm, nil
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
