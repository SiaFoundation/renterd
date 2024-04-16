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
	"go.sia.tech/coreutils/syncer"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/renterd/alerts"
	"go.sia.tech/renterd/autopilot"
	"go.sia.tech/renterd/bus"
	"go.sia.tech/renterd/chain"
	"go.sia.tech/renterd/config"
	"go.sia.tech/renterd/stores"
	"go.sia.tech/renterd/webhooks"
	"go.sia.tech/renterd/worker"
	"go.uber.org/zap"
	"golang.org/x/crypto/blake2b"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// TODOs:
// - add wallet metrics
// - add UPNP support

type BusConfig struct {
	config.Bus
	Network             *consensus.Network
	Genesis             types.Block
	DBLogger            logger.Interface
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

var (
	retryTxIntervals = []time.Duration{
		200 * time.Millisecond,
		500 * time.Millisecond,
		time.Second,
		3 * time.Second,
		10 * time.Second,
		10 * time.Second,
	}
)

func NewBus(cfg BusConfig, dir string, seed types.PrivateKey, logger *zap.Logger) (http.Handler, ShutdownFn, *chain.Manager, *chain.Subscriber, error) {
	// If no DB dialector was provided, use SQLite.
	dbConn := cfg.DBDialector
	if dbConn == nil {
		dbDir := filepath.Join(dir, "db")
		if err := os.MkdirAll(dbDir, 0700); err != nil {
			return nil, nil, nil, nil, err
		}
		dbConn = stores.NewSQLiteConnection(filepath.Join(dbDir, "db.sqlite"))
	}
	dbMetricsConn := cfg.DBMetricsDialector
	if dbMetricsConn == nil {
		dbDir := filepath.Join(dir, "db")
		if err := os.MkdirAll(dbDir, 0700); err != nil {
			return nil, nil, nil, nil, err
		}
		dbMetricsConn = stores.NewSQLiteConnection(filepath.Join(dbDir, "metrics.sqlite"))
	}

	consensusDir := filepath.Join(dir, "consensus")
	if err := os.MkdirAll(consensusDir, 0700); err != nil {
		return nil, nil, nil, nil, err
	}
	bdb, err := coreutils.OpenBoltChainDB(filepath.Join(dir, "chain.db"))
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("failed to open chain database: %w", err)
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
		GormLogger:                    cfg.DBLogger,
		RetryTransactionIntervals:     retryTxIntervals,
		WalletAddress:                 types.StandardUnlockHash(seed.PublicKey()),
	})
	if err != nil {
		return nil, nil, nil, nil, err
	}
	wh, err := webhooks.NewManager(logger.Named("webhooks").Sugar(), sqlStore)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	// Hook up webhooks to alerts.
	alertsMgr.RegisterWebhookBroadcaster(wh)

	// create chain manager
	store, state, err := chain.NewDBStore(bdb, cfg.Network, cfg.Genesis)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	cm := chain.NewManager(store, state)

	// create wallet
	w, err := wallet.NewSingleAddressWallet(seed, cm, sqlStore, wallet.WithReservationDuration(cfg.UsedUTXOExpiry))
	if err != nil {
		return nil, nil, nil, nil, err
	}

	// create syncer
	l, err := net.Listen("tcp", cfg.GatewayAddr)
	if err != nil {
		return nil, nil, nil, nil, err
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
		return nil, nil, nil, nil, err
	}

	cs, err := chain.NewSubscriber(cm, sqlStore, sqlStore, types.StandardUnlockHash(seed.PublicKey()), time.Duration(cfg.AnnouncementMaxAgeHours)*time.Hour, retryTxIntervals, logger.Named("chainsubscriber"))
	if err != nil {
		return nil, nil, nil, nil, err
	}

	// bootstrap the syncer
	if cfg.Bootstrap {
		if cfg.Network == nil {
			return nil, nil, nil, nil, errors.New("cannot bootstrap without a network")
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
			return nil, nil, nil, nil, fmt.Errorf("no available bootstrap peers for unknown network '%s'", cfg.Network.Name)
		}

		for _, addr := range bootstrapPeers {
			if err := sqlStore.AddPeer(addr); err != nil {
				return nil, nil, nil, nil, fmt.Errorf("%w: failed to add bootstrap peer '%s'", err, addr)
			}
		}
	}

	// start the syncer
	go s.Run()

	// start the subscriber
	unsubscribeFn, err := cs.Run()
	if err != nil {
		return nil, nil, nil, nil, err
	}

	shutdownFn := func(ctx context.Context) error {
		unsubscribeFn()
		return errors.Join(
			cs.Close(),
			l.Close(),
			w.Close(),
			b.Shutdown(ctx),
			sqlStore.Close(),
			bdb.Close(),
		)
	}
	return b.Handler(), shutdownFn, cm, cs, nil
}

func NewWorker(cfg config.Worker, b worker.Bus, seed types.PrivateKey, l *zap.Logger) (http.Handler, ShutdownFn, error) {
	workerKey := blake2b.Sum256(append([]byte("worker"), seed...))
	w, err := worker.New(workerKey, cfg.ID, b, cfg.ContractLockTimeout, cfg.BusFlushInterval, cfg.DownloadOverdriveTimeout, cfg.UploadOverdriveTimeout, cfg.DownloadMaxOverdrive, cfg.UploadMaxOverdrive, cfg.DownloadMaxMemory, cfg.UploadMaxMemory, cfg.AllowPrivateIPs, l)
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
