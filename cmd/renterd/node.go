package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/gateway"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/syncer"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/jape"
	"go.sia.tech/renterd/alerts"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/autopilot"
	"go.sia.tech/renterd/build"
	"go.sia.tech/renterd/bus"
	"go.sia.tech/renterd/config"
	"go.sia.tech/renterd/internal/utils"
	"go.sia.tech/renterd/stores"
	"go.sia.tech/renterd/stores/sql"
	"go.sia.tech/renterd/stores/sql/mysql"
	"go.sia.tech/renterd/stores/sql/sqlite"
	"go.sia.tech/renterd/webhooks"
	"go.sia.tech/renterd/worker"
	"go.sia.tech/renterd/worker/s3"
	"go.sia.tech/web/renterd"
	"go.uber.org/zap"
	"golang.org/x/crypto/blake2b"
	"golang.org/x/sys/cpu"
)

type (
	node struct {
		cfg config.Config

		apiSrv      *http.Server
		apiListener net.Listener

		s3Srv      *http.Server
		s3Listener net.Listener

		setupFns    []fn
		shutdownFns []fn

		bus    *bus.Client
		logger *zap.SugaredLogger
	}

	fn struct {
		name string
		fn   func(context.Context) error
	}
)

func newNode(cfg config.Config, network *consensus.Network, genesis types.Block) (*node, error) {
	var setupFns, shutdownFns []fn

	// validate config
	if cfg.Bus.RemoteAddr != "" && !cfg.Worker.Enabled && !cfg.Autopilot.Enabled {
		return nil, errors.New("remote bus, remote worker, and no autopilot -- nothing to do!")
	}
	if cfg.Worker.Enabled && cfg.Bus.RemoteAddr != "" && cfg.Worker.ExternalAddress == "" {
		return nil, errors.New("can't enable the worker using a remote bus, without configuring the worker's external address")
	}
	if cfg.Autopilot.Enabled && !cfg.Worker.Enabled && len(cfg.Worker.Remotes) == 0 {
		return nil, errors.New("can't enable autopilot without providing either workers to connect to or creating a worker")
	}

	// initialise directory
	err := os.MkdirAll(cfg.Directory, 0700)
	if err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	// initialise logger
	logger, closeFn, err := NewLogger(cfg.Directory, "renterd.log", cfg.Log)
	if err != nil {
		return nil, fmt.Errorf("failed to create logger: %w", err)
	}
	shutdownFns = append(shutdownFns, fn{
		name: "Logger",
		fn:   closeFn,
	})

	// print network and version
	logger.Info("renterd", zap.String("version", build.Version()), zap.String("network", network.Name), zap.String("commit", build.Commit()), zap.Time("buildDate", build.BuildTime()))
	if runtime.GOARCH == "amd64" && !cpu.X86.HasAVX2 {
		logger.Warn("renterd is running on a system without AVX2 support, performance may be degraded")
	}

	// initialise a listener and override the HTTP address, we have to do this
	// first so we know the actual api address if the user specifies port :0
	l, err := utils.ListenTCP(cfg.HTTP.Address, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create listener: %w", err)
	}
	cfg.HTTP.Address = "http://" + l.Addr().String()

	// initialise a web server
	mux := &utils.TreeMux{Sub: make(map[string]utils.TreeMux)}
	srv := &http.Server{Handler: mux}
	shutdownFns = append(shutdownFns, fn{
		name: "HTTP Server",
		fn:   srv.Shutdown,
	})

	// initialise auth handler
	auth := jape.BasicAuth(cfg.HTTP.Password)

	// generate private key from seed
	var pk types.PrivateKey
	if cfg.Seed != "" {
		var rawSeed [32]byte
		err := wallet.SeedFromPhrase(&rawSeed, cfg.Seed)
		if err != nil {
			return nil, fmt.Errorf("failed to load wallet: %w", err)
		}
		pk = wallet.KeyFromSeed(&rawSeed, 0)
	}

	// initialise bus
	busAddr, busPassword := cfg.Bus.RemoteAddr, cfg.Bus.RemotePassword
	if cfg.Bus.RemoteAddr == "" {
		// ensure we don't hang indefinitely
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		// create bus
		b, shutdownFn, err := newBus(ctx, cfg, pk, network, genesis, logger)
		if err != nil {
			return nil, err
		}

		shutdownFns = append(shutdownFns, fn{
			name: "Bus",
			fn:   shutdownFn,
		})

		mux.Sub["/api/bus"] = utils.TreeMux{Handler: auth(b.Handler())}
		busAddr = cfg.HTTP.Address + "/api/bus"
		busPassword = cfg.HTTP.Password

		// only serve the UI if a bus is created
		mux.Handler = renterd.Handler()
	} else {
		logger.Info("connecting to remote bus at " + busAddr)
	}
	bc := bus.NewClient(busAddr, busPassword)

	// initialise workers
	var s3Srv *http.Server
	var s3Listener net.Listener
	var workers []autopilot.Worker
	if len(cfg.Worker.Remotes) == 0 {
		if cfg.Worker.Enabled {
			workerAddr := cfg.HTTP.Address + "/api/worker"
			var workerExternAddr string
			if cfg.Bus.RemoteAddr != "" {
				workerExternAddr = cfg.Worker.ExternalAddress
			} else {
				workerExternAddr = workerAddr
			}

			workerKey := blake2b.Sum256(append([]byte("worker"), pk...))
			w, err := worker.New(cfg.Worker, workerKey, bc, logger)
			if err != nil {
				logger.Fatal("failed to create worker: " + err.Error())
			}
			setupFns = append(setupFns, fn{
				name: "Worker",
				fn: func(ctx context.Context) error {
					return w.Setup(ctx, workerExternAddr, cfg.HTTP.Password)
				},
			})
			shutdownFns = append(shutdownFns, fn{
				name: "Worker",
				fn:   w.Shutdown,
			})

			mux.Sub["/api/worker"] = utils.TreeMux{Handler: utils.Auth(cfg.HTTP.Password, cfg.Worker.AllowUnauthenticatedDownloads)(w.Handler())}
			wc := worker.NewClient(workerAddr, cfg.HTTP.Password)
			workers = append(workers, wc)

			if cfg.S3.Enabled {
				s3Handler, err := s3.New(bc, w, logger, s3.Opts{
					AuthDisabled:      cfg.S3.DisableAuth,
					HostBucketBases:   cfg.S3.HostBucketBases,
					HostBucketEnabled: cfg.S3.HostBucketEnabled,
				})
				if err != nil {
					err = errors.Join(err, w.Shutdown(context.Background()))
					logger.Fatal("failed to create s3 handler: " + err.Error())
				}

				s3Srv = &http.Server{
					Addr:    cfg.S3.Address,
					Handler: s3Handler,
				}
				s3Listener, err = utils.ListenTCP(cfg.S3.Address, logger)
				if err != nil {
					logger.Fatal("failed to create listener: " + err.Error())
				}
				shutdownFns = append(shutdownFns, fn{
					name: "S3",
					fn:   s3Srv.Shutdown,
				})
			}
		}
	} else {
		for _, remote := range cfg.Worker.Remotes {
			workers = append(workers, worker.NewClient(remote.Address, remote.Password))
			logger.Info("connecting to remote worker at " + remote.Address)
		}
	}

	// initialise autopilot
	if cfg.Autopilot.Enabled {
		ap, err := autopilot.New(cfg.Autopilot, bc, workers, logger)
		if err != nil {
			logger.Fatal("failed to create autopilot: " + err.Error())
		}
		setupFns = append(setupFns, fn{
			name: "Autopilot",
			fn:   func(_ context.Context) error { ap.Run(); return nil },
		})
		shutdownFns = append(shutdownFns, fn{
			name: "Autopilot",
			fn:   ap.Shutdown,
		})

		mux.Sub["/api/autopilot"] = utils.TreeMux{Handler: auth(ap.Handler())}
	}

	return &node{
		apiSrv:      srv,
		apiListener: l,

		s3Srv:      s3Srv,
		s3Listener: s3Listener,

		setupFns:    setupFns,
		shutdownFns: shutdownFns,

		cfg: cfg,

		logger: logger.Sugar(),
	}, nil
}

func newBus(ctx context.Context, cfg config.Config, pk types.PrivateKey, network *consensus.Network, genesis types.Block, logger *zap.Logger) (*bus.Bus, func(ctx context.Context) error, error) {
	// create store
	alertsMgr := alerts.NewManager()
	storeCfg, err := buildStoreConfig(alertsMgr, cfg, pk, logger)
	if err != nil {
		return nil, nil, err
	}
	sqlStore, err := stores.NewSQLStore(storeCfg)
	if err != nil {
		return nil, nil, err
	}

	// create webhooks manager
	wh, err := webhooks.NewManager(sqlStore, logger)
	if err != nil {
		return nil, nil, err
	}

	// hookup webhooks <-> alerts
	alertsMgr.RegisterWebhookBroadcaster(wh)

	// create consensus directory
	consensusDir := filepath.Join(cfg.Directory, "consensus")
	if err := os.MkdirAll(consensusDir, 0700); err != nil {
		return nil, nil, err
	}

	// migrate consensus database if necessary
	migrateConsensusDatabase(ctx, sqlStore, consensusDir, logger)

	// reset chain state if blockchain.db does not exist to make sure deleting
	// it forces a resync
	chainPath := filepath.Join(consensusDir, "blockchain.db")
	if _, err := os.Stat(chainPath); os.IsNotExist(err) {
		if err := sqlStore.ResetChainState(context.Background()); err != nil {
			return nil, nil, err
		}
	}

	// create chain database
	bdb, err := coreutils.OpenBoltChainDB(chainPath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open chain database: %w", err)
	}

	// create chain manager
	store, state, err := chain.NewDBStore(bdb, network, genesis)
	if err != nil {
		return nil, nil, err
	}
	cm := chain.NewManager(store, state)

	// create wallet
	w, err := wallet.NewSingleAddressWallet(pk, cm, sqlStore, wallet.WithReservationDuration(cfg.Bus.UsedUTXOExpiry))
	if err != nil {
		return nil, nil, err
	}

	// bootstrap the syncer
	if cfg.Bus.Bootstrap {
		var peers []string
		switch network.Name {
		case "mainnet":
			peers = syncer.MainnetBootstrapPeers
		case "zen":
			peers = syncer.ZenBootstrapPeers
		case "anagami":
			peers = syncer.AnagamiBootstrapPeers
		default:
			return nil, nil, fmt.Errorf("no available bootstrap peers for unknown network '%s'", network.Name)
		}
		for _, addr := range peers {
			if err := sqlStore.AddPeer(addr); err != nil {
				return nil, nil, fmt.Errorf("%w: failed to add bootstrap peer '%s'", err, addr)
			}
		}
	}

	// create syncer, peers will reject us if our hostname is empty or
	// unspecified, so use loopback
	l, err := net.Listen("tcp", cfg.Bus.GatewayAddr)
	if err != nil {
		return nil, nil, err
	}
	syncerAddr := l.Addr().String()
	host, port, _ := net.SplitHostPort(syncerAddr)
	if ip := net.ParseIP(host); ip == nil || ip.IsUnspecified() {
		syncerAddr = net.JoinHostPort("127.0.0.1", port)
	}

	// create header
	header := gateway.Header{
		GenesisID:  genesis.ID(),
		UniqueID:   gateway.GenerateUniqueID(),
		NetAddress: syncerAddr,
	}

	// create the syncer
	s := syncer.New(l, cm, sqlStore, header, syncer.WithLogger(logger.Named("syncer")), syncer.WithSendBlocksTimeout(time.Minute))

	// start syncer
	errChan := make(chan error, 1)
	go func() {
		errChan <- s.Run(context.Background())
		close(errChan)
	}()

	// create a helper function to wait for syncer to wind down on shutdown
	syncerShutdown := func(ctx context.Context) error {
		select {
		case err := <-errChan:
			return err
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	}

	// create master key - we currently derive the same key used by the workers
	// to ensure contracts formed by the bus can be renewed by the autopilot
	masterKey := blake2b.Sum256(append([]byte("worker"), pk...))

	// create bus
	announcementMaxAgeHours := time.Duration(cfg.Bus.AnnouncementMaxAgeHours) * time.Hour
	b, err := bus.New(ctx, masterKey, alertsMgr, wh, cm, s, w, sqlStore, announcementMaxAgeHours, logger)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create bus: %w", err)
	}

	return b, func(ctx context.Context) error {
		return errors.Join(
			s.Close(),
			w.Close(),
			b.Shutdown(ctx),
			sqlStore.Close(),
			bdb.Close(),
			syncerShutdown(ctx),
		)
	}, nil
}

func (n *node) Run() error {
	// start server
	go n.apiSrv.Serve(n.apiListener)
	n.logger.Info("api: Listening on " + n.apiListener.Addr().String())

	// execute run functions
	for _, fn := range n.setupFns {
		if err := fn.fn(context.Background()); err != nil {
			return fmt.Errorf("failed to run %v: %w", fn.name, err)
		}
	}

	// set initial S3 keys
	if n.cfg.S3.Enabled && !n.cfg.S3.DisableAuth {
		as, err := n.bus.S3AuthenticationSettings(context.Background())
		if err != nil && !strings.Contains(err.Error(), api.ErrSettingNotFound.Error()) {
			return fmt.Errorf("failed to fetch S3 authentication settings: %w", err)
		} else if as.V4Keypairs == nil {
			as.V4Keypairs = make(map[string]string)
		}

		// S3 key pair validation was broken at one point, we need to remove the
		// invalid key pairs here to ensure we don't fail when we update the
		// setting below.
		for k, v := range as.V4Keypairs {
			if err := (api.S3AuthenticationSettings{V4Keypairs: map[string]string{k: v}}).Validate(); err != nil {
				n.logger.Infof("removing invalid S3 keypair for AccessKeyID %s, reason: %v", k, err)
				delete(as.V4Keypairs, k)
			}
		}

		// merge keys
		for k, v := range n.cfg.S3.KeypairsV4 {
			as.V4Keypairs[k] = v
		}
		// update settings
		if err := n.bus.UpdateSetting(context.Background(), api.SettingS3Authentication, as); err != nil {
			return fmt.Errorf("failed to update S3 authentication settings: %w", err)
		}
	}

	// start S3 server
	if n.s3Srv != nil {
		go n.s3Srv.Serve(n.s3Listener)
		n.logger.Info("s3: Listening on " + n.s3Listener.Addr().String())
	}

	// fetch the syncer address
	syncerAddress, err := n.bus.SyncerAddress(context.Background())
	if err != nil {
		return fmt.Errorf("failed to fetch syncer address: %w", err)
	}
	n.logger.Info("bus: Listening on " + syncerAddress)

	// open the web UI if enabled
	if n.cfg.AutoOpenWebUI {
		time.Sleep(time.Millisecond) // give the web server a chance to start
		_, port, err := net.SplitHostPort(n.apiListener.Addr().String())
		if err != nil {
			n.logger.Debug("failed to parse API address", zap.Error(err))
		} else if err := utils.OpenBrowser(fmt.Sprintf("http://127.0.0.1:%s", port)); err != nil {
			n.logger.Debug("failed to open browser", zap.Error(err))
		}
	}
	return nil
}

func (n *node) Shutdown() error {
	n.logger.Info("Shutting down...")

	// give each service a fraction of the total shutdown timeout. One service
	// timing out shouldn't prevent the others from attempting a shutdown.
	timeout := n.cfg.ShutdownTimeout / time.Duration(len(n.shutdownFns))
	shutdown := func(fn func(ctx context.Context) error) error {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		return fn(ctx)
	}

	// shut down the services in reverse order
	var errs []error
	for i := len(n.shutdownFns) - 1; i >= 0; i-- {
		if err := shutdown(n.shutdownFns[i].fn); err != nil {
			n.logger.Errorf("failed to shut down %v: %v", n.shutdownFns[i].name, err)
			errs = append(errs, err)
		} else {
			n.logger.Infof("%v shut down successfully", n.shutdownFns[i].name)
		}
	}

	return errors.Join(errs...)
}

// TODO: needs a better spot
func buildStoreConfig(am alerts.Alerter, cfg config.Config, pk types.PrivateKey, logger *zap.Logger) (stores.Config, error) {
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
			return stores.Config{}, fmt.Errorf("failed to open MySQL main database: %w", err)
		}
		connMetrics, err := mysql.Open(
			cfg.Database.MySQL.User,
			cfg.Database.MySQL.Password,
			cfg.Database.MySQL.URI,
			cfg.Database.MySQL.MetricsDatabase,
		)
		if err != nil {
			return stores.Config{}, fmt.Errorf("failed to open MySQL metrics database: %w", err)
		}
		dbMain, err = mysql.NewMainDatabase(connMain, logger, cfg.Log.Database.SlowThreshold, cfg.Log.Database.SlowThreshold)
		if err != nil {
			return stores.Config{}, fmt.Errorf("failed to create MySQL main database: %w", err)
		}
		dbMetrics, err = mysql.NewMetricsDatabase(connMetrics, logger, cfg.Log.Database.SlowThreshold, cfg.Log.Database.SlowThreshold)
		if err != nil {
			return stores.Config{}, fmt.Errorf("failed to create MySQL metrics database: %w", err)
		}
	} else {
		// create database directory
		dbDir := filepath.Join(cfg.Directory, "db")
		if err := os.MkdirAll(dbDir, 0700); err != nil {
			return stores.Config{}, err
		}

		// create SQLite connections
		db, err := sqlite.Open(filepath.Join(dbDir, "db.sqlite"))
		if err != nil {
			return stores.Config{}, fmt.Errorf("failed to open SQLite main database: %w", err)
		}
		dbMain, err = sqlite.NewMainDatabase(db, logger, cfg.Log.Database.SlowThreshold, cfg.Log.Database.SlowThreshold)
		if err != nil {
			return stores.Config{}, fmt.Errorf("failed to create SQLite main database: %w", err)
		}

		dbm, err := sqlite.Open(filepath.Join(dbDir, "metrics.sqlite"))
		if err != nil {
			return stores.Config{}, fmt.Errorf("failed to open SQLite metrics database: %w", err)
		}
		dbMetrics, err = sqlite.NewMetricsDatabase(dbm, logger, cfg.Log.Database.SlowThreshold, cfg.Log.Database.SlowThreshold)
		if err != nil {
			return stores.Config{}, fmt.Errorf("failed to create SQLite metrics database: %w", err)
		}
	}

	return stores.Config{
		Alerts:                        alerts.WithOrigin(am, "bus"),
		DB:                            dbMain,
		DBMetrics:                     dbMetrics,
		PartialSlabDir:                filepath.Join(cfg.Directory, "partial_slabs"),
		Migrate:                       true,
		SlabBufferCompletionThreshold: cfg.Bus.SlabBufferCompletionThreshold,
		Logger:                        logger,
		RetryTransactionIntervals: []time.Duration{
			200 * time.Millisecond,
			500 * time.Millisecond,
			time.Second,
			3 * time.Second,
			10 * time.Second,
			10 * time.Second,
		},
		WalletAddress:     types.StandardUnlockHash(pk.PublicKey()),
		LongQueryDuration: cfg.Log.Database.SlowThreshold,
		LongTxDuration:    cfg.Log.Database.SlowThreshold,
	}, nil
}

func migrateConsensusDatabase(ctx context.Context, store *stores.SQLStore, consensusDir string, logger *zap.Logger) error {
	oldConsensus, err := os.Stat(filepath.Join(consensusDir, "consensus.db"))
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	}

	logger.Warn("found old consensus.db, indicating a migration is necessary")

	// reset chain state
	logger.Warn("Resetting chain state...")
	if err := store.ResetChainState(ctx); err != nil {
		return err
	}
	logger.Warn("Chain state was successfully reset.")

	// remove consensus.db and consensus.log file
	logger.Warn("Removing consensus database...")
	_ = os.RemoveAll(filepath.Join(consensusDir, "consensus.log")) // ignore error
	if err := os.Remove(filepath.Join(consensusDir, "consensus.db")); err != nil {
		return err
	}

	logger.Warn(fmt.Sprintf("Old 'consensus.db' was successfully removed, reclaimed %v of disk space.", utils.HumanReadableSize(int(oldConsensus.Size()))))
	logger.Warn("ATTENTION: consensus will now resync from scratch, this process may take several hours to complete")
	return nil
}
