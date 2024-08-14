package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/jape"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/autopilot"
	"go.sia.tech/renterd/build"
	"go.sia.tech/renterd/bus"
	"go.sia.tech/renterd/config"
	"go.sia.tech/renterd/utils"
	"go.sia.tech/renterd/worker"
	"go.sia.tech/renterd/worker/s3"
	"go.sia.tech/web/renterd"
	"go.uber.org/zap"
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
		b, shutdownFn, _, err := bus.NewNode(bus.NodeConfig{
			Bus:         cfg.Bus,
			Database:    cfg.Database,
			DatabaseLog: cfg.Log.Database,
			Logger:      logger,
			Network:     network,
			Genesis:     genesis,
			RetryTxIntervals: []time.Duration{
				200 * time.Millisecond,
				500 * time.Millisecond,
				time.Second,
				3 * time.Second,
				10 * time.Second,
				10 * time.Second,
			},
		}, cfg.Directory, pk, logger)
		if err != nil {
			return nil, fmt.Errorf("failed to create bus: %w", err)
		}

		shutdownFns = append(shutdownFns, fn{
			name: "Bus",
			fn:   shutdownFn,
		})

		mux.Sub["/api/bus"] = utils.TreeMux{Handler: auth(b)}
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
			w, s3Handler, setupFn, shutdownFn, err := worker.NewNode(cfg.Worker, s3.Opts{
				AuthDisabled:      cfg.S3.DisableAuth,
				HostBucketBases:   cfg.S3.HostBucketBases,
				HostBucketEnabled: cfg.S3.HostBucketEnabled,
			}, bc, pk, logger)
			if err != nil {
				logger.Fatal("failed to create worker: " + err.Error())
			}
			var workerExternAddr string
			if cfg.Bus.RemoteAddr != "" {
				workerExternAddr = cfg.Worker.ExternalAddress
			} else {
				workerExternAddr = workerAddr
			}

			setupFns = append(setupFns, fn{
				name: "Worker",
				fn: func(ctx context.Context) error {
					return setupFn(ctx, workerExternAddr, cfg.HTTP.Password)
				},
			})
			shutdownFns = append(shutdownFns, fn{
				name: "Worker",
				fn:   shutdownFn,
			})

			mux.Sub["/api/worker"] = utils.TreeMux{Handler: utils.Auth(cfg.HTTP.Password, cfg.Worker.AllowUnauthenticatedDownloads)(w)}
			wc := worker.NewClient(workerAddr, cfg.HTTP.Password)
			workers = append(workers, wc)

			if cfg.S3.Enabled {
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
		ap, runFn, shutdownFn, err := autopilot.NewNode(cfg.Autopilot, bc, workers, logger)
		if err != nil {
			logger.Fatal("failed to create autopilot: " + err.Error())
		}

		setupFns = append(setupFns, fn{
			name: "Autopilot",
			fn:   func(_ context.Context) error { runFn(); return nil },
		})

		// NOTE: shutdown functions are executed in reverse order, it's
		// important the autopilot is shut down first so we don't needlessly
		// make worker and bus calls while they're shutting down
		shutdownFns = append(shutdownFns, fn{
			name: "Autopilot",
			fn:   shutdownFn,
		})

		mux.Sub["/api/autopilot"] = utils.TreeMux{Handler: auth(ap)}
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

	// run autopilot store migration
	//
	// TODO: we can safely remove this already
	if n.cfg.Autopilot.Enabled {
		autopilotDir := filepath.Join(n.cfg.Directory, n.cfg.Autopilot.ID)
		if err := runCompatMigrateAutopilotJSONToStore(n.bus, "autopilot", autopilotDir); err != nil {
			return fmt.Errorf("failed to migrate autopilot JSON: %w", err)
		}
	}

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

func runCompatMigrateAutopilotJSONToStore(bc *bus.Client, id, dir string) (err error) {
	// check if the file exists
	path := filepath.Join(dir, "autopilot.json")
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil
	}

	// defer autopilot dir cleanup
	defer func() {
		if err == nil {
			log.Println("migration: removing autopilot directory")
			if err = os.RemoveAll(dir); err == nil {
				log.Println("migration: done")
			}
		}
	}()

	// read the json config
	log.Println("migration: reading autopilot.json")
	//nolint:tagliatelle
	var cfg struct {
		Config api.AutopilotConfig `json:"Config"`
	}
	if data, err := os.ReadFile(path); err != nil {
		return err
	} else if err := json.Unmarshal(data, &cfg); err != nil {
		return err
	}

	// make sure we don't hang
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// check if the autopilot already exists, if so we don't need to migrate
	_, err = bc.Autopilot(ctx, api.DefaultAutopilotID)
	if err == nil {
		log.Printf("migration: autopilot already exists in the bus, the autopilot.json won't be migrated\n old config: %+v\n", cfg.Config)
		return nil
	}

	// create an autopilot entry
	log.Println("migration: persisting autopilot to the bus")
	if err := bc.UpdateAutopilot(ctx, api.Autopilot{
		ID:     id,
		Config: cfg.Config,
	}); err != nil {
		return err
	}

	// remove autopilot folder and config
	log.Println("migration: cleaning up autopilot directory")
	if err = os.RemoveAll(dir); err == nil {
		log.Println("migration: done")
	}

	return nil
}
