package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/jape"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/autopilot"
	"go.sia.tech/renterd/build"
	"go.sia.tech/renterd/bus"
	"go.sia.tech/renterd/config"
	"go.sia.tech/renterd/internal/node"
	"go.sia.tech/renterd/s3"
	"go.sia.tech/renterd/stores"
	"go.sia.tech/renterd/tracing"
	"go.sia.tech/renterd/wallet"
	"go.sia.tech/renterd/worker"
	"go.sia.tech/web/renterd"
	"golang.org/x/term"
	"gopkg.in/yaml.v3"
	"gorm.io/gorm/logger"
)

const (
	// accountRefillInterval is the amount of time between refills of ephemeral
	// accounts. If we conservatively assume that a good host charges 500 SC /
	// TiB, we can pay for about 2.2 GiB with 1 SC. Since we want to refill
	// ahead of time at 0.5 SC, that makes 1.1 GiB. Considering a 1 Gbps uplink
	// that is shared across 30 uploads, we upload at around 33 Mbps to each
	// host. That means uploading 1.1 GiB to drain 0.5 SC takes around 5
	// minutes. That's why we assume 10 seconds to be more than frequent enough
	// to refill an account when it's due for another refill.
	defaultAccountRefillInterval = 10 * time.Second
)

var (
	// to be supplied at build time
	githash   = "?"
	builddate = "?"

	cfg = config.Config{
		Directory: ".",
		Seed:      os.Getenv("RENTERD_SEED"),
		HTTP: config.HTTP{
			Address:  build.DefaultAPIAddress,
			Password: os.Getenv("RENTERD_API_PASSWORD"),
		},
		ShutdownTimeout: 5 * time.Minute,
		Tracing: config.Tracing{
			InstanceID: "cluster",
		},
		Database: config.Database{
			Log: config.DatabaseLog{
				IgnoreRecordNotFoundError: true,
				SlowThreshold:             100 * time.Millisecond,
			},
		},
		Log: config.Log{
			Level: "warn",
		},
		Bus: config.Bus{
			AnnouncementMaxAgeHours:       24 * 7 * 52, // 1 year
			Bootstrap:                     true,
			GatewayAddr:                   build.DefaultGatewayAddress,
			PersistInterval:               time.Minute,
			UsedUTXOExpiry:                24 * time.Hour,
			SlabBufferCompletionThreshold: 1 << 12,
		},
		Worker: config.Worker{
			Enabled: true,

			ID:                  "worker",
			ContractLockTimeout: 30 * time.Second,
			BusFlushInterval:    5 * time.Second,

			DownloadMaxOverdrive:     5,
			DownloadOverdriveTimeout: 3 * time.Second,

			UploadMaxOverdrive:     5,
			UploadOverdriveTimeout: 3 * time.Second,
		},
		Autopilot: config.Autopilot{
			Enabled:                        true,
			RevisionSubmissionBuffer:       144,
			AccountsRefillInterval:         defaultAccountRefillInterval,
			Heartbeat:                      30 * time.Minute,
			MigrationHealthCutoff:          0.75,
			RevisionBroadcastInterval:      24 * time.Hour,
			ScannerBatchSize:               1000,
			ScannerInterval:                24 * time.Hour,
			ScannerMinRecentFailures:       10,
			ScannerNumThreads:              100,
			MigratorParallelSlabsPerWorker: 1,
		},
		S3: config.S3{
			Address:     build.DefaultS3Address,
			Enabled:     true,
			DisableAuth: false,
			KeypairsV4:  nil,
		},
	}
	seed types.PrivateKey
)

func check(context string, err error) {
	if err != nil {
		log.Fatalf("%v: %v", context, err)
	}
}

func mustLoadAPIPassword() {
	if len(cfg.HTTP.Password) != 0 {
		return
	}

	fmt.Print("Enter API password: ")
	pw, err := term.ReadPassword(int(os.Stdin.Fd()))
	fmt.Println()
	if err != nil {
		log.Fatal(err)
	}
	cfg.HTTP.Password = string(pw)
}

func getSeed() types.PrivateKey {
	if seed == nil {
		phrase := cfg.Seed
		if phrase == "" {
			fmt.Print("Enter seed: ")
			pw, err := term.ReadPassword(int(os.Stdin.Fd()))
			check("Could not read seed phrase:", err)
			fmt.Println()
			phrase = string(pw)
		}
		key, err := wallet.KeyFromPhrase(phrase)
		if err != nil {
			log.Fatal(err)
		}
		seed = key
	}
	return seed
}

func mustParseWorkers(workers, password string) {
	if workers == "" {
		return
	}
	// if the CLI flag/environment variable is set, overwrite the config file
	cfg.Worker.Remotes = cfg.Worker.Remotes[:0]
	for _, addr := range strings.Split(workers, ";") {
		// note: duplicates the old behavior of all workers sharing the same
		// password
		cfg.Worker.Remotes = append(cfg.Worker.Remotes, config.RemoteWorker{
			Address:  addr,
			Password: password,
		})
	}
}

// tryLoadConfig loads the config file specified by the RENTERD_CONFIG_FILE
// environment variable. If the config file does not exist, it will not be
// loaded.
func tryLoadConfig() {
	configPath := "renterd.yml"
	if str := os.Getenv("RENTERD_CONFIG_FILE"); len(str) != 0 {
		configPath = str
	}

	// If the config file doesn't exist, don't try to load it.
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		return
	}

	f, err := os.Open(configPath)
	if err != nil {
		log.Fatal("failed to open config file:", err)
	}
	defer f.Close()

	dec := yaml.NewDecoder(f)
	dec.KnownFields(true)

	if err := dec.Decode(&cfg); err != nil {
		log.Fatal("failed to decode config file:", err)
	}
}

func parseEnvVar(s string, v interface{}) {
	if env, ok := os.LookupEnv(s); ok {
		if _, err := fmt.Sscan(env, v); err != nil {
			log.Fatalf("failed to parse %s: %v", s, err)
		}
		fmt.Printf("Using %s environment variable\n", s)
	}
}

func main() {
	log.SetFlags(0)

	// load the YAML config first. CLI flags and environment variables will
	// overwrite anything set in the config file.
	tryLoadConfig()

	// TODO: the following flags will be deprecated in v1.0.0 in favor of
	// environment variables to ensure we do not ask the user to pass sensitive
	// information via CLI parameters.
	var depDBPassword string
	var depBusRemotePassword string
	var depBusRemoteAddr string
	var depWorkerRemotePassStr string
	var depWorkerRemoteAddrsStr string
	flag.StringVar(&depDBPassword, "db.password", "", "[DEPRECATED] password for the database to use for the bus - can be overwritten using RENTERD_DB_PASSWORD environment variable")
	flag.StringVar(&depBusRemotePassword, "bus.apiPassword", "", "[DEPRECATED] API password for remote bus service - can be overwritten using RENTERD_BUS_API_PASSWORD environment variable")
	flag.StringVar(&depBusRemoteAddr, "bus.remoteAddr", "", "[DEPRECATED] URL of remote bus service - can be overwritten using RENTERD_BUS_REMOTE_ADDR environment variable")
	flag.StringVar(&depWorkerRemotePassStr, "worker.apiPassword", "", "[DEPRECATED] API password for remote worker service")
	flag.StringVar(&depWorkerRemoteAddrsStr, "worker.remoteAddrs", "", "[DEPRECATED] URL of remote worker service(s). Multiple addresses can be provided by separating them with a semicolon. Can be overwritten using the RENTERD_WORKER_REMOTE_ADDRS environment variable")

	for _, flag := range []struct {
		input    string
		name     string
		env      string
		insecure bool
	}{
		{depDBPassword, "db.password", "RENTERD_DB_PASSWORD", true},
		{depBusRemotePassword, "bus.apiPassword", "RENTERD_BUS_API_PASSWORD", true},
		{depBusRemoteAddr, "bus.remoteAddr", "RENTERD_BUS_REMOTE_ADDR", false},
		{depWorkerRemotePassStr, "worker.apiPassword", "RENTERD_WORKER_API_PASSWORDS", true},
		{depWorkerRemoteAddrsStr, "worker.remoteAddrs", "RENTERD_WORKER_REMOTE_ADDRS", false},
	} {
		if flag.input != "" {
			if flag.insecure {
				log.Printf("WARNING: usage of CLI flag '%s' is considered insecure and will be deprecated in v1.0.0, please use the environment variable '%s' instead\n", flag.name, flag.env)
			} else {
				log.Printf("WARNING: CLI flag '%s' will be deprecated in v1.0.0, please use the environment variable '%s' instead\n", flag.name, flag.env)
			}
		}
	}

	if depDBPassword != "" {
		cfg.Database.MySQL.Password = depDBPassword
	}
	if depBusRemotePassword != "" {
		cfg.Bus.RemotePassword = depBusRemotePassword
	}
	if depBusRemoteAddr != "" {
		cfg.Bus.RemoteAddr = depBusRemoteAddr
	}

	// node
	flag.StringVar(&cfg.HTTP.Address, "http", cfg.HTTP.Address, "address to serve API on")
	flag.StringVar(&cfg.Directory, "dir", cfg.Directory, "directory to store node state in")
	flag.BoolVar(&cfg.Tracing.Enabled, "tracing-enabled", cfg.Tracing.Enabled, "Enables tracing through OpenTelemetry. If RENTERD_TRACING_ENABLED is set, it overwrites the CLI flag's value. Tracing can be configured using the standard OpenTelemetry environment variables. https://github.com/open-telemetry/opentelemetry-specification/blob/v1.8.0/specification/protocol/exporter.md")
	flag.StringVar(&cfg.Tracing.InstanceID, "tracing-service-instance-id", cfg.Tracing.InstanceID, "ID of the service instance used for tracing. If RENTERD_TRACING_SERVICE_INSTANCE_ID is set, it overwrites the CLI flag's value.")
	flag.StringVar(&cfg.Log.Path, "log-path", cfg.Log.Path, "Overwrites the default log location on disk. Alternatively RENTERD_LOG_PATH can be used")

	// db
	flag.StringVar(&cfg.Database.MySQL.URI, "db.uri", cfg.Database.MySQL.URI, "URI of the database to use for the bus - can be overwritten using RENTERD_DB_URI environment variable")
	flag.StringVar(&cfg.Database.MySQL.User, "db.user", cfg.Database.MySQL.User, "username for the database to use for the bus - can be overwritten using RENTERD_DB_USER environment variable")
	flag.StringVar(&cfg.Database.MySQL.Database, "db.name", cfg.Database.MySQL.Database, "name of the database to use for the bus - can be overwritten using RENTERD_DB_NAME environment variable")

	// db logger
	flag.BoolVar(&cfg.Database.Log.IgnoreRecordNotFoundError, "db.logger.ignoreNotFoundError", cfg.Database.Log.IgnoreRecordNotFoundError, "ignore not found error for logger - can be overwritten using RENTERD_DB_LOGGER_IGNORE_NOT_FOUND_ERROR environment variable")
	flag.StringVar(&cfg.Log.Level, "db.logger.logLevel", cfg.Log.Level, "log level for logger - can be overwritten using RENTERD_DB_LOGGER_LOG_LEVEL environment variable")
	flag.DurationVar(&cfg.Database.Log.SlowThreshold, "db.logger.slowThreshold", cfg.Database.Log.SlowThreshold, "slow threshold for logger - can be overwritten using RENTERD_DB_LOGGER_SLOW_THRESHOLD environment variable")

	// bus
	flag.Uint64Var(&cfg.Bus.AnnouncementMaxAgeHours, "bus.announcementMaxAgeHours", cfg.Bus.AnnouncementMaxAgeHours, "announcements older than this are ignored")
	flag.BoolVar(&cfg.Bus.Bootstrap, "bus.bootstrap", cfg.Bus.Bootstrap, "bootstrap the gateway and consensus modules")
	flag.StringVar(&cfg.Bus.GatewayAddr, "bus.gatewayAddr", cfg.Bus.GatewayAddr, "address to listen on for Sia peer connections - can be overwritten using RENTERD_BUS_GATEWAY_ADDR environment variable")
	flag.DurationVar(&cfg.Bus.PersistInterval, "bus.persistInterval", cfg.Bus.PersistInterval, "interval at which to persist the consensus updates")
	flag.DurationVar(&cfg.Bus.UsedUTXOExpiry, "bus.usedUTXOExpiry", cfg.Bus.UsedUTXOExpiry, "time after which a used UTXO that hasn't been included in a transaction becomes spendable again")
	flag.Int64Var(&cfg.Bus.SlabBufferCompletionThreshold, "bus.slabBufferCompletionThreshold", cfg.Bus.SlabBufferCompletionThreshold, "number of remaining bytes in a slab buffer before it is uploaded - can be overwritten using the RENTERD_BUS_SLAB_BUFFER_COMPLETION_THRESHOLD environment variable")

	// worker
	flag.BoolVar(&cfg.Worker.AllowPrivateIPs, "worker.allowPrivateIPs", cfg.Worker.AllowPrivateIPs, "allow hosts with private IPs")
	flag.DurationVar(&cfg.Worker.BusFlushInterval, "worker.busFlushInterval", cfg.Worker.BusFlushInterval, "time after which the worker flushes buffered data to bus for persisting")
	flag.Uint64Var(&cfg.Worker.DownloadMaxOverdrive, "worker.downloadMaxOverdrive", cfg.Worker.DownloadMaxOverdrive, "maximum number of active overdrive workers when downloading a slab")
	flag.StringVar(&cfg.Worker.ID, "worker.id", cfg.Worker.ID, "unique identifier of worker used internally - can be overwritten using the RENTERD_WORKER_ID environment variable")
	flag.DurationVar(&cfg.Worker.DownloadOverdriveTimeout, "worker.downloadOverdriveTimeout", cfg.Worker.DownloadOverdriveTimeout, "timeout applied to slab downloads that decides when we start overdriving")
	flag.Uint64Var(&cfg.Worker.UploadMaxOverdrive, "worker.uploadMaxOverdrive", cfg.Worker.UploadMaxOverdrive, "maximum number of active overdrive workers when uploading a slab")
	flag.DurationVar(&cfg.Worker.UploadOverdriveTimeout, "worker.uploadOverdriveTimeout", cfg.Worker.UploadOverdriveTimeout, "timeout applied to slab uploads that decides when we start overdriving")
	flag.BoolVar(&cfg.Worker.Enabled, "worker.enabled", cfg.Worker.Enabled, "enable/disable creating a worker - can be overwritten using the RENTERD_WORKER_ENABLED environment variable")
	flag.BoolVar(&cfg.Worker.AllowUnauthenticatedDownloads, "worker.unauthenticatedDownloads", cfg.Worker.AllowUnauthenticatedDownloads, "if set to 'true', the worker will allow for downloading from the /objects endpoint without basic authentication. Can be overwritten using the RENTERD_WORKER_UNAUTHENTICATED_DOWNLOADS environment variable")

	// autopilot
	flag.DurationVar(&cfg.Autopilot.AccountsRefillInterval, "autopilot.accountRefillInterval", cfg.Autopilot.AccountsRefillInterval, "interval at which the autopilot checks the workers' accounts balance and refills them if necessary")
	flag.DurationVar(&cfg.Autopilot.Heartbeat, "autopilot.heartbeat", cfg.Autopilot.Heartbeat, "interval at which autopilot loop runs")
	flag.Float64Var(&cfg.Autopilot.MigrationHealthCutoff, "autopilot.migrationHealthCutoff", cfg.Autopilot.MigrationHealthCutoff, "health threshold below which slabs are migrated to new hosts")
	flag.DurationVar(&cfg.Autopilot.RevisionBroadcastInterval, "autopilot.revisionBroadcastInterval", cfg.Autopilot.RevisionBroadcastInterval, "interval at which the autopilot broadcasts contract revisions to be mined - can be overwritten using the RENTERD_AUTOPILOT_REVISION_BROADCAST_INTERVAL environment variable - setting it to 0 will disable this feature")
	flag.Uint64Var(&cfg.Autopilot.ScannerBatchSize, "autopilot.scannerBatchSize", cfg.Autopilot.ScannerBatchSize, "size of the batch with which hosts are scanned")
	flag.DurationVar(&cfg.Autopilot.ScannerInterval, "autopilot.scannerInterval", cfg.Autopilot.ScannerInterval, "interval at which hosts are scanned")
	flag.Uint64Var(&cfg.Autopilot.ScannerMinRecentFailures, "autopilot.scannerMinRecentFailures", cfg.Autopilot.ScannerMinRecentFailures, "minimum amount of consesutive failed scans a host must have before it is removed for exceeding the max downtime")
	flag.Uint64Var(&cfg.Autopilot.ScannerNumThreads, "autopilot.scannerNumThreads", cfg.Autopilot.ScannerNumThreads, "number of threads that scan hosts")
	flag.Uint64Var(&cfg.Autopilot.MigratorParallelSlabsPerWorker, "autopilot.migratorParallelSlabsPerWorker", cfg.Autopilot.MigratorParallelSlabsPerWorker, "number of slabs that the autopilot migrates in parallel per worker. Can be overwritten using the RENTERD_MIGRATOR_PARALLEL_SLABS_PER_WORKER environment variable")
	flag.BoolVar(&cfg.Autopilot.Enabled, "autopilot.enabled", cfg.Autopilot.Enabled, "enable/disable the autopilot - can be overwritten using the RENTERD_AUTOPILOT_ENABLED environment variable")
	flag.DurationVar(&cfg.ShutdownTimeout, "node.shutdownTimeout", cfg.ShutdownTimeout, "the timeout applied to the node shutdown")

	// s3
	flag.StringVar(&cfg.S3.Address, "s3.address", cfg.S3.Address, "address to serve S3 API on - can be overwritten using the RENTERD_S3_ADDRESS environment variable")
	flag.BoolVar(&cfg.S3.DisableAuth, "s3.disableAuth", cfg.S3.DisableAuth, "disables authentication for the S3 API - can be overwritten using the RENTERD_S3_DISABLE_AUTH environment variable")
	flag.BoolVar(&cfg.S3.Enabled, "s3.enabled", cfg.S3.Enabled, "enable/disable the S3 API (only works if worker.enabled is also 'true') - can be overwritten using the RENTERD_S3_ENABLED environment variable")
	flag.BoolVar(&cfg.S3.HostBucketEnabled, "s3.hostBucketEnabled", cfg.S3.HostBucketEnabled, "enables bucket rewriting in the router -  - can be overwritten using the RENTERD_S3_HOST_BUCKET_ENABLED environment variable")

	flag.Parse()

	log.Println("renterd v0.6.0")
	log.Println("Network", build.NetworkName())
	if flag.Arg(0) == "version" {
		log.Println("Commit:", githash)
		log.Println("Build Date:", builddate)
		return
	} else if flag.Arg(0) == "seed" {
		log.Println("Seed phrase:")
		fmt.Println(wallet.NewSeedPhrase())
		return
	}

	// Overwrite flags from environment if set.
	parseEnvVar("RENTERD_LOG_PATH", &cfg.Log.Path)

	parseEnvVar("RENTERD_TRACING_ENABLED", &cfg.Tracing.Enabled)
	parseEnvVar("RENTERD_TRACING_SERVICE_INSTANCE_ID", &cfg.Tracing.InstanceID)

	parseEnvVar("RENTERD_BUS_REMOTE_ADDR", &cfg.Bus.RemoteAddr)
	parseEnvVar("RENTERD_BUS_API_PASSWORD", &cfg.Bus.RemotePassword)
	parseEnvVar("RENTERD_BUS_GATEWAY_ADDR", &cfg.Bus.GatewayAddr)
	parseEnvVar("RENTERD_BUS_SLAB_BUFFER_COMPLETION_THRESHOLD", &cfg.Bus.SlabBufferCompletionThreshold)

	parseEnvVar("RENTERD_DB_URI", &cfg.Database.MySQL.URI)
	parseEnvVar("RENTERD_DB_USER", &cfg.Database.MySQL.User)
	parseEnvVar("RENTERD_DB_PASSWORD", &cfg.Database.MySQL.Password)
	parseEnvVar("RENTERD_DB_NAME", &cfg.Database.MySQL.Database)

	parseEnvVar("RENTERD_DB_LOGGER_IGNORE_NOT_FOUND_ERROR", &cfg.Database.Log.IgnoreRecordNotFoundError)
	parseEnvVar("RENTERD_DB_LOGGER_LOG_LEVEL", &cfg.Log.Level)
	parseEnvVar("RENTERD_DB_LOGGER_SLOW_THRESHOLD", &cfg.Database.Log.SlowThreshold)

	parseEnvVar("RENTERD_WORKER_REMOTE_ADDRS", &depWorkerRemoteAddrsStr)
	parseEnvVar("RENTERD_WORKER_API_PASSWORD", &depWorkerRemotePassStr)
	parseEnvVar("RENTERD_WORKER_ENABLED", &cfg.Worker.Enabled)
	parseEnvVar("RENTERD_WORKER_ID", &cfg.Worker.ID)
	parseEnvVar("RENTERD_WORKER_UNAUTHENTICATED_DOWNLOADS", &cfg.Worker.AllowUnauthenticatedDownloads)

	parseEnvVar("RENTERD_AUTOPILOT_ENABLED", &cfg.Autopilot.Enabled)
	parseEnvVar("RENTERD_AUTOPILOT_REVISION_BROADCAST_INTERVAL", &cfg.Autopilot.RevisionBroadcastInterval)
	parseEnvVar("RENTERD_MIGRATOR_PARALLEL_SLABS_PER_WORKER", &cfg.Autopilot.MigratorParallelSlabsPerWorker)

	parseEnvVar("RENTERD_S3_ADDRESS", &cfg.S3.Address)
	parseEnvVar("RENTERD_S3_ENABLED", &cfg.S3.Enabled)
	parseEnvVar("RENTERD_S3_DISABLE_AUTH", &cfg.S3.DisableAuth)
	parseEnvVar("RENTERD_S3_HOST_BUCKET_ENABLED", &cfg.S3.HostBucketEnabled)

	if cfg.S3.Enabled {
		var keyPairsV4 string
		parseEnvVar("RENTERD_S3_KEYPAIRS_V4", &keyPairsV4)
		if !cfg.S3.DisableAuth && keyPairsV4 != "" {
			var err error
			cfg.S3.KeypairsV4, err = s3.Parsev4AuthKeys(strings.Split(keyPairsV4, ";"))
			if err != nil {
				log.Fatalf("failed to parse keypairs: %v", err)
			}
		}
	}

	mustLoadAPIPassword()
	if depWorkerRemoteAddrsStr != "" && depWorkerRemotePassStr != "" {
		mustParseWorkers(depWorkerRemoteAddrsStr, depWorkerRemotePassStr)
	}

	network, _ := build.Network()
	busCfg := node.BusConfig{
		Bus:     cfg.Bus,
		Network: network,
	}
	// Init db dialector
	if cfg.Database.MySQL.URI != "" {
		busCfg.DBDialector = stores.NewMySQLConnection(
			cfg.Database.MySQL.User,
			cfg.Database.MySQL.Password,
			cfg.Database.MySQL.URI,
			cfg.Database.MySQL.Database,
		)
	}

	var level logger.LogLevel
	switch strings.ToLower(cfg.Log.Level) {
	case "silent":
		level = logger.Silent
	case "error":
		level = logger.Error
	case "warn":
		level = logger.Warn
	case "info":
		level = logger.Info
	default:
		log.Fatalf("invalid log level %q, options are: silent, error, warn, info", cfg.Log.Level)
	}

	// Create logger.
	renterdLog := filepath.Join(cfg.Directory, "renterd.log")
	if cfg.Log.Path != "" {
		renterdLog = cfg.Log.Path
	}
	logger, closeFn, err := node.NewLogger(renterdLog)
	if err != nil {
		log.Fatalln("failed to create logger:", err)
	}
	defer closeFn(context.Background())

	busCfg.DBLoggerConfig = stores.LoggerConfig{
		LogLevel:                  level,
		IgnoreRecordNotFoundError: cfg.Database.Log.IgnoreRecordNotFoundError,
		SlowThreshold:             cfg.Database.Log.SlowThreshold,
	}

	type shutdownFn struct {
		name string
		fn   func(context.Context) error
	}
	var shutdownFns []shutdownFn

	// Init tracing.
	if cfg.Tracing.Enabled {
		fn, err := tracing.Init(cfg.Tracing.InstanceID)
		if err != nil {
			logger.Fatal("failed to init tracing: " + err.Error())
		}
		shutdownFns = append(shutdownFns, shutdownFn{
			name: "Tracing",
			fn:   fn,
		})
	}

	if cfg.Bus.RemoteAddr != "" && len(cfg.Worker.Remotes) != 0 && !cfg.Autopilot.Enabled {
		logger.Fatal("remote bus, remote worker, and no autopilot -- nothing to do!")
	}
	if len(cfg.Worker.Remotes) == 0 && !cfg.Worker.Enabled && cfg.Autopilot.Enabled {
		logger.Fatal("can't enable autopilot without providing either workers to connect to or creating a worker")
	}

	// create listener first, so that we know the actual apiAddr if the user
	// specifies port :0
	l, err := net.Listen("tcp", cfg.HTTP.Address)
	if err != nil {
		logger.Fatal("failed to create listener: " + err.Error())
	}

	// override the address with the actual one
	cfg.HTTP.Address = "http://" + l.Addr().String()

	auth := jape.BasicAuth(cfg.HTTP.Password)
	mux := &treeMux{
		sub: make(map[string]treeMux),
	}

	// Create the webserver.
	srv := &http.Server{Handler: mux}
	shutdownFns = append(shutdownFns, shutdownFn{
		name: "HTTP Server",
		fn:   srv.Shutdown,
	})

	if err := os.MkdirAll(cfg.Directory, 0700); err != nil {
		logger.Fatal("failed to create directory: " + err.Error())
	}

	busAddr, busPassword := cfg.Bus.RemoteAddr, cfg.Bus.RemotePassword
	if cfg.Bus.RemoteAddr == "" {
		b, fn, err := node.NewBus(busCfg, cfg.Directory, getSeed(), logger)
		if err != nil {
			logger.Fatal("failed to create bus, err: " + err.Error())
		}
		shutdownFns = append(shutdownFns, shutdownFn{
			name: "Bus",
			fn:   fn,
		})

		mux.sub["/api/bus"] = treeMux{h: auth(b)}
		busAddr = cfg.HTTP.Address + "/api/bus"
		busPassword = cfg.HTTP.Password

		// only serve the UI if a bus is created
		mux.h = renterd.Handler()
	} else {
		logger.Info("connecting to remote bus at " + busAddr)
	}
	bc := bus.NewClient(busAddr, busPassword)

	var s3Srv *http.Server
	var s3Listener net.Listener
	var workers []autopilot.Worker
	if len(cfg.Worker.Remotes) == 0 {
		if cfg.Worker.Enabled {
			w, fn, err := node.NewWorker(cfg.Worker, bc, getSeed(), logger)
			if err != nil {
				logger.Fatal("failed to create worker: " + err.Error())
			}
			shutdownFns = append(shutdownFns, shutdownFn{
				name: "Worker",
				fn:   fn,
			})

			mux.sub["/api/worker"] = treeMux{h: workerAuth(cfg.HTTP.Password, cfg.Worker.AllowUnauthenticatedDownloads)(w)}
			workerAddr := cfg.HTTP.Address + "/api/worker"
			wc := worker.NewClient(workerAddr, cfg.HTTP.Password)
			workers = append(workers, wc)

			if cfg.S3.Enabled {
				s3Handler, err := s3.New(bc, wc, logger.Sugar(), s3.Opts{
					AuthDisabled:      cfg.S3.DisableAuth,
					HostBucketEnabled: cfg.S3.HostBucketEnabled,
				})
				if err != nil {
					log.Fatal("failed to create s3 client", err)
				}
				s3Srv = &http.Server{
					Addr:    cfg.S3.Address,
					Handler: s3Handler,
				}
				s3Listener, err = net.Listen("tcp", cfg.S3.Address)
				if err != nil {
					logger.Fatal("failed to create listener: " + err.Error())
				}
				shutdownFns = append(shutdownFns, shutdownFn{
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

	autopilotErr := make(chan error, 1)
	autopilotDir := filepath.Join(cfg.Directory, api.DefaultAutopilotID)
	if cfg.Autopilot.Enabled {
		apCfg := node.AutopilotConfig{
			ID:        api.DefaultAutopilotID,
			Autopilot: cfg.Autopilot,
		}
		ap, runFn, fn, err := node.NewAutopilot(apCfg, bc, workers, logger)
		if err != nil {
			logger.Fatal("failed to create autopilot: " + err.Error())
		}

		// NOTE: the autopilot shutdown function needs to be called first.
		shutdownFns = append(shutdownFns, shutdownFn{
			name: "Autopilot",
			fn:   fn,
		})

		go func() { autopilotErr <- runFn() }()
		mux.sub["/api/autopilot"] = treeMux{h: auth(ap)}
	}

	// Start server.
	go srv.Serve(l)

	// Set initial S3 keys.
	if cfg.S3.Enabled && !cfg.S3.DisableAuth {
		as, err := bc.S3AuthenticationSettings(context.Background())
		if err != nil && !strings.Contains(err.Error(), api.ErrSettingNotFound.Error()) {
			logger.Fatal("failed to fetch S3 authentication settings: " + err.Error())
		} else if as.V4Keypairs == nil {
			as.V4Keypairs = make(map[string]string)
		}
		// merge keys
		for k, v := range cfg.S3.KeypairsV4 {
			as.V4Keypairs[k] = v
		}
		// update settings
		if err := bc.UpdateSetting(context.Background(), api.SettingS3Authentication, as); err != nil {
			logger.Fatal("failed to update S3 authentication settings: " + err.Error())
		}
	}

	logger.Info("api: Listening on " + l.Addr().String())

	if s3Srv != nil {
		go s3Srv.Serve(s3Listener)
		logger.Info("s3: Listening on " + s3Listener.Addr().String())
	}

	syncerAddress, err := bc.SyncerAddress(context.Background())
	if err != nil {
		logger.Fatal("failed to fetch syncer address: " + err.Error())
	}
	logger.Info("bus: Listening on " + syncerAddress)

	if cfg.Autopilot.Enabled {
		if err := runCompatMigrateAutopilotJSONToStore(bc, "autopilot", autopilotDir); err != nil {
			logger.Fatal("failed to migrate autopilot JSON: " + err.Error())
		}
	}

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM)
	select {
	case <-signalCh:
		logger.Info("Shutting down...")
	case err := <-autopilotErr:
		logger.Fatal("Fatal autopilot error: " + err.Error())
	}

	// Give each service a fraction of the total shutdown timeout. One service
	// timing out shouldn't prevent the others from attempting a shutdown.
	timeout := cfg.ShutdownTimeout / time.Duration(len(shutdownFns))
	shutdown := func(fn func(ctx context.Context) error) error {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		return fn(ctx)
	}

	// Shut down the autopilot first, then the rest of the services in reverse order and then
	exitCode := 0
	for i := len(shutdownFns) - 1; i >= 0; i-- {
		if err := shutdown(shutdownFns[i].fn); err != nil {
			logger.Sugar().Errorf("Failed to shut down %v: %v", shutdownFns[i].name, err)
			exitCode = 1
		} else {
			logger.Sugar().Infof("%v shut down successfully", shutdownFns[i].name)
		}
	}
	logger.Info("Shutdown complete")
	os.Exit(exitCode)
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

func workerAuth(password string, unauthenticatedDownloads bool) func(http.Handler) http.Handler {
	return func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			if unauthenticatedDownloads && req.Method == http.MethodGet && strings.HasPrefix(req.URL.Path, "/objects/") {
				h.ServeHTTP(w, req)
			} else {
				jape.BasicAuth(password)(h).ServeHTTP(w, req)
			}
		})
	}
}
