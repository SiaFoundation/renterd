package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
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
	cwallet "go.sia.tech/coreutils/wallet"
	"go.sia.tech/renterd/v2/alerts"
	"go.sia.tech/renterd/v2/api"
	"go.sia.tech/renterd/v2/autopilot"
	"go.sia.tech/renterd/v2/autopilot/contractor"
	"go.sia.tech/renterd/v2/autopilot/migrator"
	"go.sia.tech/renterd/v2/autopilot/pruner"
	"go.sia.tech/renterd/v2/autopilot/scanner"
	"go.sia.tech/renterd/v2/autopilot/walletmaintainer"
	"go.sia.tech/renterd/v2/build"
	"go.sia.tech/renterd/v2/bus"
	"go.sia.tech/renterd/v2/config"
	"go.sia.tech/renterd/v2/stores"
	"go.sia.tech/renterd/v2/stores/sql"
	"go.sia.tech/renterd/v2/stores/sql/mysql"
	"go.sia.tech/renterd/v2/stores/sql/sqlite"
	"go.sia.tech/renterd/v2/worker"
	"go.sia.tech/renterd/v2/worker/s3"
	"go.sia.tech/web/renterd"
	"go.uber.org/zap"
	"golang.org/x/crypto/blake2b"
	"golang.org/x/sys/cpu"
)

var (
	errNoSuchHost = errors.New("no such host")
)

type (
	Autopilot interface {
		Handler() http.Handler
		Run()
		Shutdown(context.Context) error
	}

	node struct {
		cfg config.Config

		apiSrv      *http.Server
		apiListener net.Listener

		s3Srv      *http.Server
		s3Listener net.Listener

		setupFns    []fn
		shutdownFns []fn

		bus           *bus.Client
		logger        *zap.SugaredLogger
		loggerCloseFn func(context.Context) error
	}

	fn struct {
		name string
		fn   func(context.Context) error
	}
)

func newNode(cfg config.Config, configPath string, network *consensus.Network, genesis types.Block) (*node, error) {
	var setupFns, shutdownFns []fn

	// validate config
	if cfg.Bus.RemoteAddr != "" && !cfg.Worker.Enabled && !cfg.Autopilot.Enabled {
		return nil, errors.New("remote bus, remote worker, and no autopilot -- nothing to do!")
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

	// print network and version
	header := logger.With(
		zap.String("version", build.Version()),
		zap.String("network", network.Name),
		zap.String("commit", build.Commit()),
		zap.Time("buildDate", build.BuildTime()),
	)
	if configPath != "" {
		header = header.With(zap.String("config", configPath))
	}
	header.Info("renterd")
	if runtime.GOARCH == "amd64" && !cpu.X86.HasAVX2 {
		header.Warn("renterd is running on a system without AVX2 support, performance may be degraded")
	}

	// initialise a listener and override the HTTP address, we have to do this
	// first so we know the actual api address if the user specifies port :0
	l, err := listenTCP(cfg.HTTP.Address, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create listener: %w", err)
	}
	cfg.HTTP.Address = "http://" + l.Addr().String()

	// initialise a web server
	mux := &api.TreeMux{Sub: make(map[string]api.TreeMux)}
	srv := &http.Server{Handler: mux}
	shutdownFns = append(shutdownFns, fn{
		name: "HTTP Server",
		fn:   srv.Shutdown,
	})

	// initialise auth handler
	tokens := api.NewTokenStore()
	auth := api.Auth(tokens, cfg.HTTP.Password)

	// generate private key from seed
	var pk types.PrivateKey
	if cfg.Seed != "" {
		var rawSeed [32]byte
		err := cwallet.SeedFromPhrase(&rawSeed, cfg.Seed)
		if err != nil {
			return nil, fmt.Errorf("failed to load wallet: %w", err)
		}
		pk = cwallet.KeyFromSeed(&rawSeed, 0)
	}

	// add auth route
	if cfg.HTTP.Password != "" {
		mux.Sub["/api/auth"] = api.TreeMux{Handler: api.AuthHandler(tokens, cfg.HTTP.Password)}
	}

	// initialise bus
	busAddr, busPassword := cfg.Bus.RemoteAddr, cfg.Bus.RemotePassword
	if cfg.Bus.RemoteAddr == "" {
		// create bus
		b, shutdownFn, err := newBus(cfg, pk, network, genesis, logger)
		if err != nil {
			return nil, err
		}

		shutdownFns = append(shutdownFns, fn{
			name: "Bus",
			fn:   shutdownFn,
		})

		mux.Sub["/api/bus"] = api.TreeMux{Handler: auth(b.Handler())}
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
	if cfg.Worker.Enabled {
		workerKey := blake2b.Sum256(append([]byte("worker"), pk...))
		w, err := worker.New(cfg.Worker, workerKey, bc, logger)
		if err != nil {
			return nil, fmt.Errorf("failed to create worker: %v", err)
		}
		shutdownFns = append(shutdownFns, fn{
			name: "Worker",
			fn:   w.Shutdown,
		})

		mux.Sub["/api/worker"] = api.TreeMux{Handler: api.WorkerAuth(tokens, cfg.HTTP.Password, cfg.Worker.AllowUnauthenticatedDownloads)(w.Handler())}

		if cfg.S3.Enabled {
			s3Handler, err := s3.New(bc, w, logger, s3.Opts{
				AuthDisabled:      cfg.S3.DisableAuth,
				HostBucketBases:   cfg.S3.HostBucketBases,
				HostBucketEnabled: cfg.S3.HostBucketEnabled,
			})
			if err != nil {
				err = errors.Join(err, w.Shutdown(context.Background()))
				return nil, fmt.Errorf("failed to create s3 handler: %v", err)
			}

			s3Srv = &http.Server{
				Addr:    cfg.S3.Address,
				Handler: s3Handler,
			}
			s3Listener, err = listenTCP(cfg.S3.Address, logger)
			if err != nil {
				return nil, fmt.Errorf("failed to create listener: %v", err)
			}
			shutdownFns = append(shutdownFns, fn{
				name: "S3",
				fn:   s3Srv.Shutdown,
			})
		}
	}

	// initialise autopilot
	if cfg.Autopilot.Enabled {
		workerKey := blake2b.Sum256(append([]byte("worker"), pk...))
		ap, err := newAutopilot(workerKey, cfg.Autopilot, bc, logger)
		if err != nil {
			return nil, fmt.Errorf("failed to create autopilot: %v", err)
		}
		setupFns = append(setupFns, fn{
			name: "Autopilot",
			fn:   func(_ context.Context) error { go ap.Run(); return nil },
		})
		shutdownFns = append(shutdownFns, fn{
			name: "Autopilot",
			fn:   ap.Shutdown,
		})

		mux.Sub["/api/autopilot"] = api.TreeMux{Handler: auth(ap.Handler())}
	}

	return &node{
		apiSrv:      srv,
		apiListener: l,

		s3Srv:      s3Srv,
		s3Listener: s3Listener,

		setupFns:    setupFns,
		shutdownFns: shutdownFns,

		bus: bc,
		cfg: cfg,

		logger:        logger.Sugar(),
		loggerCloseFn: closeFn,
	}, nil
}

func newAutopilot(masterKey [32]byte, cfg config.Autopilot, bus *bus.Client, l *zap.Logger) (Autopilot, error) {
	a := alerts.WithOrigin(bus, "autopilot")
	l = l.Named("autopilot")

	ctx, cancel := context.WithCancelCause(context.Background())
	m, err := migrator.New(ctx, masterKey, a, bus, bus, cfg.MigratorHealthCutoff, cfg.MigratorNumThreads, cfg.MigratorDownloadMaxOverdrive, cfg.MigratorUploadMaxOverdrive, cfg.MigratorDownloadOverdriveTimeout, cfg.MigratorUploadOverdriveTimeout, cfg.MigratorAccountsRefillInterval, l)
	if err != nil {
		cancel(nil)
		return nil, err
	}

	s, err := scanner.New(bus, cfg.ScannerBatchSize, cfg.ScannerNumThreads, cfg.ScannerInterval, l)
	if err != nil {
		cancel(nil)
		return nil, err
	}

	c := contractor.New(bus, bus, bus, bus, bus, cfg.RevisionSubmissionBuffer, cfg.RevisionBroadcastInterval, cfg.AllowRedundantHostIPs, l)
	p := pruner.New(bus, l)
	w := walletmaintainer.New(a, bus, l)

	return autopilot.New(ctx, cancel, bus, c, m, p, s, w, cfg.Heartbeat, l), nil
}

func newBus(cfg config.Config, pk types.PrivateKey, network *consensus.Network, genesis types.Block, logger *zap.Logger) (*bus.Bus, func(ctx context.Context) error, error) {
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

	// create consensus directory
	consensusDir := filepath.Join(cfg.Directory, "consensus")
	if err := os.MkdirAll(consensusDir, 0700); err != nil {
		return nil, nil, err
	}

	// migrate consensus database if necessary
	migrateConsensusDatabase(sqlStore, consensusDir, logger)

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
	store, state, err := chain.NewDBStore(bdb, network, genesis, chain.NewZapMigrationLogger(logger.Named("chaindb")))
	if err != nil {
		return nil, nil, err
	}
	cm := chain.NewManager(store, state)

	// create wallet
	w, err := cwallet.NewSingleAddressWallet(pk, cm, sqlStore, cwallet.WithReservationDuration(cfg.Bus.UsedUTXOExpiry))
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
		case "erravimus":
			peers = syncer.ErravimusBootstrapPeers
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
		errChan <- s.Run()
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

	// get explorer URL
	var explorerURL string
	if !cfg.Explorer.Disable {
		explorerURL = cfg.Explorer.URL
	}

	// create bus
	b, err := bus.New(cfg.Bus, masterKey, alertsMgr, cm, s, w, sqlStore, explorerURL, logger)
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
			n.logger.Debugw("failed to parse API address", zap.Error(err))
		} else if err := openBrowser(fmt.Sprintf("http://127.0.0.1:%s", port)); err != nil {
			n.logger.Debugw("failed to open browser", zap.Error(err))
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

	errs = append(errs, shutdown(n.loggerCloseFn))
	return errors.Join(errs...)
}

// TODO: needs a better spot
func buildStoreConfig(am alerts.Alerter, cfg config.Config, pk types.PrivateKey, logger *zap.Logger) (stores.Config, error) {
	partialSlabDir := filepath.Join(cfg.Directory, "partial_slabs")

	// create database connections
	var dbMain sql.Database
	var dbMetrics sql.MetricsDatabase
	if cfg.Database.MySQL.URI != "" {
		// check that both main and metrics databases are not the same
		if cfg.Database.MySQL.Database == cfg.Database.MySQL.MetricsDatabase {
			return stores.Config{}, errors.New("main and metrics databases cannot be the same")
		}

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
		dbMain, err = mysql.NewMainDatabase(connMain, logger, cfg.Log.Database.SlowThreshold, cfg.Log.Database.SlowThreshold, partialSlabDir)
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
		dbMain, err = sqlite.NewMainDatabase(db, logger, cfg.Log.Database.SlowThreshold, cfg.Log.Database.SlowThreshold, partialSlabDir)
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
		PartialSlabDir:                partialSlabDir,
		Migrate:                       true,
		SlabBufferCompletionThreshold: cfg.Bus.SlabBufferCompletionThreshold,
		Logger:                        logger,
		WalletAddress:                 types.StandardUnlockHash(pk.PublicKey()),
		LongQueryDuration:             cfg.Log.Database.SlowThreshold,
		LongTxDuration:                cfg.Log.Database.SlowThreshold,
	}, nil
}

func migrateConsensusDatabase(store *stores.SQLStore, consensusDir string, logger *zap.Logger) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

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

	logger.Warn(fmt.Sprintf("Old 'consensus.db' was successfully removed, reclaimed %v of disk space.", humanReadableSize(int(oldConsensus.Size()))))
	logger.Warn("ATTENTION: consensus will now resync from scratch, this process may take several hours to complete")
	return nil
}

func defaultDataDirectory(fp string) string {
	// use the provided path if it's not empty
	if fp != "" {
		return fp
	}

	// check for databases in the current directory
	if _, err := os.Stat("db/db.sqlite"); err == nil {
		return "."
	}

	// default to the operating system's application directory
	switch runtime.GOOS {
	case "windows":
		return filepath.Join(os.Getenv("APPDATA"), "renterd")
	case "darwin":
		return filepath.Join(os.Getenv("HOME"), "Library", "Application Support", "renterd")
	case "linux", "freebsd", "openbsd":
		return filepath.Join(string(filepath.Separator), "var", "lib", "renterd")
	default:
		return "."
	}
}

func humanReadableSize(b int) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB",
		float64(b)/float64(div), "KMGTPE"[exp])
}

func listenTCP(addr string, logger *zap.Logger) (net.Listener, error) {
	l, err := net.Listen("tcp", addr)
	if (errors.Is(err, errNoSuchHost) || err != nil && strings.Contains(err.Error(), errNoSuchHost.Error())) && strings.Contains(addr, "localhost") {
		// fall back to 127.0.0.1 if 'localhost' doesn't work
		_, port, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, err
		}
		fallbackAddr := fmt.Sprintf("127.0.0.1:%s", port)
		logger.Sugar().Warnf("failed to listen on %s, falling back to %s", addr, fallbackAddr)
		return net.Listen("tcp", fallbackAddr)
	} else if err != nil {
		return nil, err
	}
	return l, nil
}

func openBrowser(url string) error {
	switch runtime.GOOS {
	case "linux":
		return exec.Command("xdg-open", url).Start()
	case "windows":
		return exec.Command("rundll32", "url.dll,FileProtocolHandler", url).Start()
	case "darwin":
		return exec.Command("open", url).Start()
	default:
		return fmt.Errorf("unsupported platform %q", runtime.GOOS)
	}
}
