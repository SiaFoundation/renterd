package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/jape"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/autopilot"
	"go.sia.tech/renterd/build"
	"go.sia.tech/renterd/bus"
	"go.sia.tech/renterd/config"
	"go.sia.tech/renterd/internal/node"
	"go.sia.tech/renterd/internal/utils"
	iworker "go.sia.tech/renterd/internal/worker"
	"go.sia.tech/renterd/worker"
	"go.sia.tech/renterd/worker/s3"
	"go.sia.tech/web/renterd"
	"go.uber.org/zap"
	"golang.org/x/sys/cpu"
	"gopkg.in/yaml.v3"
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

	// usageHeader is the header for the CLI usage text.
	usageHeader = `
Renterd is the official Sia renter daemon. It provides a REST API for forming
contracts with hosts, uploading data to them and downloading data from them.

There are 3 ways to configure renterd (sorted from lowest to highest precedence):
  - A YAML config file
  - CLI flags
  - Environment variables

Usage:
`
	// usageFooter is the footer for the CLI usage text.
	usageFooter = `
There are 3 commands:
  - version: prints the network as well as build information
  - config: builds a YAML config file through a series of prompts
  - seed: generates a new seed and prints the recovery phrase

See the documentation (https://docs.sia.tech/) for more information and examples
on how to configure and use renterd.
`
)

var (
	cfg = config.Config{
		Directory:     ".",
		Seed:          os.Getenv("RENTERD_SEED"),
		AutoOpenWebUI: true,
		HTTP: config.HTTP{
			Address:  build.DefaultAPIAddress,
			Password: os.Getenv("RENTERD_API_PASSWORD"),
		},
		ShutdownTimeout: 5 * time.Minute,
		Database: config.Database{
			MySQL: config.MySQL{
				User:            "renterd",
				Database:        "renterd",
				MetricsDatabase: "renterd_metrics",
			},
		},
		Log: config.Log{
			Path:  "", // deprecated. included for compatibility.
			Level: "",
			File: config.LogFile{
				Enabled: true,
				Format:  "json",
				Path:    os.Getenv("RENTERD_LOG_FILE"),
			},
			StdOut: config.StdOut{
				Enabled:    true,
				Format:     "human",
				EnableANSI: runtime.GOOS != "windows",
			},
			Database: config.DatabaseLog{
				Enabled:                   true,
				IgnoreRecordNotFoundError: true,
				SlowThreshold:             100 * time.Millisecond,
			},
		},
		Bus: config.Bus{
			AnnouncementMaxAgeHours:       24 * 7 * 52, // 1 year
			Bootstrap:                     true,
			GatewayAddr:                   build.DefaultGatewayAddress,
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

			DownloadMaxMemory:      1 << 30, // 1 GiB
			UploadMaxMemory:        1 << 30, // 1 GiB
			UploadMaxOverdrive:     5,
			UploadOverdriveTimeout: 3 * time.Second,
		},
		Autopilot: config.Autopilot{
			Enabled:                        true,
			RevisionSubmissionBuffer:       144,
			AccountsRefillInterval:         defaultAccountRefillInterval,
			Heartbeat:                      30 * time.Minute,
			MigrationHealthCutoff:          0.75,
			RevisionBroadcastInterval:      7 * 24 * time.Hour,
			ScannerBatchSize:               100,
			ScannerInterval:                4 * time.Hour,
			ScannerNumThreads:              10,
			MigratorParallelSlabsPerWorker: 1,
		},
		S3: config.S3{
			Address:     build.DefaultS3Address,
			Enabled:     true,
			DisableAuth: false,
			KeypairsV4:  nil,
		},
	}
	disableStdin bool
)

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
	if str := os.Getenv("RENTERD_CONFIG_FILE"); str != "" {
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

func listenTCP(logger *zap.Logger, addr string) (net.Listener, error) {
	l, err := net.Listen("tcp", addr)
	if utils.IsErr(err, errors.New("no such host")) && strings.Contains(addr, "localhost") {
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

func main() {
	log.SetFlags(0)

	// load the YAML config first. CLI flags and environment variables will
	// overwrite anything set in the config file.
	tryLoadConfig()

	// deprecated - these go first so that they can be overwritten by the non-deprecated flags
	flag.StringVar(&cfg.Log.Database.Level, "db.logger.logLevel", cfg.Log.Database.Level, "(deprecated) Logger level (overrides with RENTERD_DB_LOGGER_LOG_LEVEL)")
	flag.BoolVar(&cfg.Database.Log.IgnoreRecordNotFoundError, "db.logger.ignoreNotFoundError", cfg.Database.Log.IgnoreRecordNotFoundError, "(deprecated) Ignores 'not found' errors in logger (overrides with RENTERD_DB_LOGGER_IGNORE_NOT_FOUND_ERROR)")
	flag.DurationVar(&cfg.Database.Log.SlowThreshold, "db.logger.slowThreshold", cfg.Database.Log.SlowThreshold, "(deprecated) Threshold for slow queries in logger (overrides with RENTERD_DB_LOGGER_SLOW_THRESHOLD)")
	flag.StringVar(&cfg.Log.Path, "log-path", cfg.Log.Path, "(deprecated) Path to directory for logs (overrides with RENTERD_LOG_PATH)")

	// node
	flag.StringVar(&cfg.HTTP.Address, "http", cfg.HTTP.Address, "Address for serving the API")
	flag.StringVar(&cfg.Directory, "dir", cfg.Directory, "Directory for storing node state")
	flag.BoolVar(&disableStdin, "env", false, "disable stdin prompts for environment variables (default false)")
	flag.BoolVar(&cfg.AutoOpenWebUI, "openui", cfg.AutoOpenWebUI, "automatically open the web UI on startup")

	// logger
	flag.StringVar(&cfg.Log.Level, "log.level", cfg.Log.Level, "Global logger level (debug|info|warn|error). Defaults to 'info' (overrides with RENTERD_LOG_LEVEL)")
	flag.BoolVar(&cfg.Log.File.Enabled, "log.file.enabled", cfg.Log.File.Enabled, "Enables logging to disk. Defaults to 'true'. (overrides with RENTERD_LOG_FILE_ENABLED)")
	flag.StringVar(&cfg.Log.File.Format, "log.file.format", cfg.Log.File.Format, "Format of log file (json|human). Defaults to 'json' (overrides with RENTERD_LOG_FILE_FORMAT)")
	flag.StringVar(&cfg.Log.File.Path, "log.file.path", cfg.Log.File.Path, "Path of log file. Defaults to 'renterd.log' within the renterd directory. (overrides with RENTERD_LOG_FILE_PATH)")
	flag.BoolVar(&cfg.Log.StdOut.Enabled, "log.stdout.enabled", cfg.Log.StdOut.Enabled, "Enables logging to stdout. Defaults to 'true'. (overrides with RENTERD_LOG_STDOUT_ENABLED)")
	flag.StringVar(&cfg.Log.StdOut.Format, "log.stdout.format", cfg.Log.StdOut.Format, "Format of log output (json|human). Defaults to 'human' (overrides with RENTERD_LOG_STDOUT_FORMAT)")
	flag.BoolVar(&cfg.Log.StdOut.EnableANSI, "log.stdout.enableANSI", cfg.Log.StdOut.EnableANSI, "Enables ANSI color codes in log output. Defaults to 'true' on non-Windows systems. (overrides with RENTERD_LOG_STDOUT_ENABLE_ANSI)")
	flag.BoolVar(&cfg.Log.Database.Enabled, "log.database.enabled", cfg.Log.Database.Enabled, "Enable logging database queries. Defaults to 'true' (overrides with RENTERD_LOG_DATABASE_ENABLED)")
	flag.StringVar(&cfg.Log.Database.Level, "log.database.level", cfg.Log.Database.Level, "Logger level for database queries (info|warn|error). Defaults to 'warn' (overrides with RENTERD_LOG_LEVEL and RENTERD_LOG_DATABASE_LEVEL)")
	flag.BoolVar(&cfg.Log.Database.IgnoreRecordNotFoundError, "log.database.ignoreRecordNotFoundError", cfg.Log.Database.IgnoreRecordNotFoundError, "Enable ignoring 'not found' errors resulting from database queries. Defaults to 'true' (overrides with RENTERD_LOG_DATABASE_IGNORE_RECORD_NOT_FOUND_ERROR)")
	flag.DurationVar(&cfg.Log.Database.SlowThreshold, "log.database.slowThreshold", cfg.Log.Database.SlowThreshold, "Threshold for slow queries in logger. Defaults to 100ms (overrides with RENTERD_LOG_DATABASE_SLOW_THRESHOLD)")

	// db
	flag.StringVar(&cfg.Database.MySQL.URI, "db.uri", cfg.Database.MySQL.URI, "Database URI for the bus (overrides with RENTERD_DB_URI)")
	flag.StringVar(&cfg.Database.MySQL.User, "db.user", cfg.Database.MySQL.User, "Database username for the bus (overrides with RENTERD_DB_USER)")
	flag.StringVar(&cfg.Database.MySQL.Database, "db.name", cfg.Database.MySQL.Database, "Database name for the bus (overrides with RENTERD_DB_NAME)")
	flag.StringVar(&cfg.Database.MySQL.MetricsDatabase, "db.metricsName", cfg.Database.MySQL.MetricsDatabase, "Database for metrics (overrides with RENTERD_DB_METRICS_NAME)")

	// bus
	flag.Uint64Var(&cfg.Bus.AnnouncementMaxAgeHours, "bus.announcementMaxAgeHours", cfg.Bus.AnnouncementMaxAgeHours, "Max age for announcements")
	flag.BoolVar(&cfg.Bus.Bootstrap, "bus.bootstrap", cfg.Bus.Bootstrap, "Bootstraps gateway and consensus modules")
	flag.StringVar(&cfg.Bus.GatewayAddr, "bus.gatewayAddr", cfg.Bus.GatewayAddr, "Address for Sia peer connections (overrides with RENTERD_BUS_GATEWAY_ADDR)")
	flag.DurationVar(&cfg.Bus.PersistInterval, "bus.persistInterval", cfg.Bus.PersistInterval, "(deprecated) Interval for persisting consensus updates")
	flag.DurationVar(&cfg.Bus.UsedUTXOExpiry, "bus.usedUTXOExpiry", cfg.Bus.UsedUTXOExpiry, "Expiry for used UTXOs in transactions")
	flag.Int64Var(&cfg.Bus.SlabBufferCompletionThreshold, "bus.slabBufferCompletionThreshold", cfg.Bus.SlabBufferCompletionThreshold, "Threshold for slab buffer upload (overrides with RENTERD_BUS_SLAB_BUFFER_COMPLETION_THRESHOLD)")

	// worker
	flag.BoolVar(&cfg.Worker.AllowPrivateIPs, "worker.allowPrivateIPs", cfg.Worker.AllowPrivateIPs, "Allows hosts with private IPs")
	flag.DurationVar(&cfg.Worker.BusFlushInterval, "worker.busFlushInterval", cfg.Worker.BusFlushInterval, "Interval for flushing data to bus")
	flag.Uint64Var(&cfg.Worker.DownloadMaxMemory, "worker.downloadMaxMemory", cfg.Worker.DownloadMaxMemory, "Max amount of RAM the worker allocates for slabs when downloading (overrides with RENTERD_WORKER_DOWNLOAD_MAX_MEMORY)")
	flag.Uint64Var(&cfg.Worker.DownloadMaxOverdrive, "worker.downloadMaxOverdrive", cfg.Worker.DownloadMaxOverdrive, "Max overdrive workers for downloads")
	flag.StringVar(&cfg.Worker.ID, "worker.id", cfg.Worker.ID, "Unique ID for worker (overrides with RENTERD_WORKER_ID)")
	flag.DurationVar(&cfg.Worker.DownloadOverdriveTimeout, "worker.downloadOverdriveTimeout", cfg.Worker.DownloadOverdriveTimeout, "Timeout for overdriving slab downloads")
	flag.Uint64Var(&cfg.Worker.UploadMaxMemory, "worker.uploadMaxMemory", cfg.Worker.UploadMaxMemory, "Max amount of RAM the worker allocates for slabs when uploading (overrides with RENTERD_WORKER_UPLOAD_MAX_MEMORY)")
	flag.Uint64Var(&cfg.Worker.UploadMaxOverdrive, "worker.uploadMaxOverdrive", cfg.Worker.UploadMaxOverdrive, "Max overdrive workers for uploads")
	flag.DurationVar(&cfg.Worker.UploadOverdriveTimeout, "worker.uploadOverdriveTimeout", cfg.Worker.UploadOverdriveTimeout, "Timeout for overdriving slab uploads")
	flag.BoolVar(&cfg.Worker.Enabled, "worker.enabled", cfg.Worker.Enabled, "Enables/disables worker (overrides with RENTERD_WORKER_ENABLED)")
	flag.BoolVar(&cfg.Worker.AllowUnauthenticatedDownloads, "worker.unauthenticatedDownloads", cfg.Worker.AllowUnauthenticatedDownloads, "Allows unauthenticated downloads (overrides with RENTERD_WORKER_UNAUTHENTICATED_DOWNLOADS)")
	flag.StringVar(&cfg.Worker.ExternalAddress, "worker.externalAddress", cfg.Worker.ExternalAddress, "Address of the worker on the network, only necessary when the bus is remote (overrides with RENTERD_WORKER_EXTERNAL_ADDR)")

	// autopilot
	flag.DurationVar(&cfg.Autopilot.AccountsRefillInterval, "autopilot.accountRefillInterval", cfg.Autopilot.AccountsRefillInterval, "Interval for refilling workers' account balances")
	flag.DurationVar(&cfg.Autopilot.Heartbeat, "autopilot.heartbeat", cfg.Autopilot.Heartbeat, "Interval for autopilot loop execution")
	flag.Float64Var(&cfg.Autopilot.MigrationHealthCutoff, "autopilot.migrationHealthCutoff", cfg.Autopilot.MigrationHealthCutoff, "Threshold for migrating slabs based on health")
	flag.DurationVar(&cfg.Autopilot.RevisionBroadcastInterval, "autopilot.revisionBroadcastInterval", cfg.Autopilot.RevisionBroadcastInterval, "Interval for broadcasting contract revisions (overrides with RENTERD_AUTOPILOT_REVISION_BROADCAST_INTERVAL)")
	flag.Uint64Var(&cfg.Autopilot.ScannerBatchSize, "autopilot.scannerBatchSize", cfg.Autopilot.ScannerBatchSize, "Batch size for host scanning")
	flag.DurationVar(&cfg.Autopilot.ScannerInterval, "autopilot.scannerInterval", cfg.Autopilot.ScannerInterval, "Interval for scanning hosts")
	flag.Uint64Var(&cfg.Autopilot.ScannerNumThreads, "autopilot.scannerNumThreads", cfg.Autopilot.ScannerNumThreads, "Number of threads for scanning hosts")
	flag.Uint64Var(&cfg.Autopilot.MigratorParallelSlabsPerWorker, "autopilot.migratorParallelSlabsPerWorker", cfg.Autopilot.MigratorParallelSlabsPerWorker, "Parallel slab migrations per worker (overrides with RENTERD_MIGRATOR_PARALLEL_SLABS_PER_WORKER)")
	flag.BoolVar(&cfg.Autopilot.Enabled, "autopilot.enabled", cfg.Autopilot.Enabled, "Enables/disables autopilot (overrides with RENTERD_AUTOPILOT_ENABLED)")
	flag.DurationVar(&cfg.ShutdownTimeout, "node.shutdownTimeout", cfg.ShutdownTimeout, "Timeout for node shutdown")

	// s3
	var hostBasesStr string
	flag.StringVar(&cfg.S3.Address, "s3.address", cfg.S3.Address, "Address for serving S3 API (overrides with RENTERD_S3_ADDRESS)")
	flag.BoolVar(&cfg.S3.DisableAuth, "s3.disableAuth", cfg.S3.DisableAuth, "Disables authentication for S3 API (overrides with RENTERD_S3_DISABLE_AUTH)")
	flag.BoolVar(&cfg.S3.Enabled, "s3.enabled", cfg.S3.Enabled, "Enables/disables S3 API (requires worker.enabled to be 'true', overrides with RENTERD_S3_ENABLED)")
	flag.StringVar(&hostBasesStr, "s3.hostBases", "", "Enables bucket rewriting in the router for specific hosts provided via comma-separated list (overrides with RENTERD_S3_HOST_BUCKET_BASES)")
	flag.BoolVar(&cfg.S3.HostBucketEnabled, "s3.hostBucketEnabled", cfg.S3.HostBucketEnabled, "Enables bucket rewriting in the router for all hosts (overrides with RENTERD_S3_HOST_BUCKET_ENABLED)")

	// custom usage
	flag.Usage = func() {
		log.Print(usageHeader)
		flag.PrintDefaults()
		log.Print(usageFooter)
	}

	flag.Parse()

	// NOTE: update the usage header when adding new commands
	if flag.Arg(0) == "version" {
		cmdVersion()
		return
	} else if flag.Arg(0) == "seed" {
		cmdSeed()
		return
	} else if flag.Arg(0) == "config" {
		cmdBuildConfig()
		return
	} else if flag.Arg(0) != "" {
		flag.Usage()
		return
	}

	// Overwrite flags from environment if set.
	parseEnvVar("RENTERD_BUS_REMOTE_ADDR", &cfg.Bus.RemoteAddr)
	parseEnvVar("RENTERD_BUS_API_PASSWORD", &cfg.Bus.RemotePassword)
	parseEnvVar("RENTERD_BUS_GATEWAY_ADDR", &cfg.Bus.GatewayAddr)
	parseEnvVar("RENTERD_BUS_SLAB_BUFFER_COMPLETION_THRESHOLD", &cfg.Bus.SlabBufferCompletionThreshold)

	parseEnvVar("RENTERD_DB_URI", &cfg.Database.MySQL.URI)
	parseEnvVar("RENTERD_DB_USER", &cfg.Database.MySQL.User)
	parseEnvVar("RENTERD_DB_PASSWORD", &cfg.Database.MySQL.Password)
	parseEnvVar("RENTERD_DB_NAME", &cfg.Database.MySQL.Database)
	parseEnvVar("RENTERD_DB_METRICS_NAME", &cfg.Database.MySQL.MetricsDatabase)

	parseEnvVar("RENTERD_DB_LOGGER_IGNORE_NOT_FOUND_ERROR", &cfg.Database.Log.IgnoreRecordNotFoundError)
	parseEnvVar("RENTERD_DB_LOGGER_LOG_LEVEL", &cfg.Log.Level)
	parseEnvVar("RENTERD_DB_LOGGER_SLOW_THRESHOLD", &cfg.Database.Log.SlowThreshold)

	parseEnvVar("RENTERD_WORKER_ENABLED", &cfg.Worker.Enabled)
	parseEnvVar("RENTERD_WORKER_ID", &cfg.Worker.ID)
	parseEnvVar("RENTERD_WORKER_UNAUTHENTICATED_DOWNLOADS", &cfg.Worker.AllowUnauthenticatedDownloads)
	parseEnvVar("RENTERD_WORKER_DOWNLOAD_MAX_MEMORY", &cfg.Worker.DownloadMaxMemory)
	parseEnvVar("RENTERD_WORKER_UPLOAD_MAX_MEMORY", &cfg.Worker.UploadMaxMemory)
	parseEnvVar("RENTERD_WORKER_EXTERNAL_ADDR", &cfg.Worker.ExternalAddress)

	parseEnvVar("RENTERD_AUTOPILOT_ENABLED", &cfg.Autopilot.Enabled)
	parseEnvVar("RENTERD_AUTOPILOT_REVISION_BROADCAST_INTERVAL", &cfg.Autopilot.RevisionBroadcastInterval)
	parseEnvVar("RENTERD_MIGRATOR_PARALLEL_SLABS_PER_WORKER", &cfg.Autopilot.MigratorParallelSlabsPerWorker)

	parseEnvVar("RENTERD_S3_ADDRESS", &cfg.S3.Address)
	parseEnvVar("RENTERD_S3_ENABLED", &cfg.S3.Enabled)
	parseEnvVar("RENTERD_S3_DISABLE_AUTH", &cfg.S3.DisableAuth)
	parseEnvVar("RENTERD_S3_HOST_BUCKET_ENABLED", &cfg.S3.HostBucketEnabled)
	parseEnvVar("RENTERD_S3_HOST_BUCKET_BASES", &cfg.S3.HostBucketBases)

	parseEnvVar("RENTERD_LOG_PATH", &cfg.Log.Path)
	parseEnvVar("RENTERD_LOG_LEVEL", &cfg.Log.Level)
	parseEnvVar("RENTERD_LOG_FILE_ENABLED", &cfg.Log.File.Enabled)
	parseEnvVar("RENTERD_LOG_FILE_FORMAT", &cfg.Log.File.Format)
	parseEnvVar("RENTERD_LOG_FILE_PATH", &cfg.Log.File.Path)
	parseEnvVar("RENTERD_LOG_STDOUT_ENABLED", &cfg.Log.StdOut.Enabled)
	parseEnvVar("RENTERD_LOG_STDOUT_FORMAT", &cfg.Log.StdOut.Format)
	parseEnvVar("RENTERD_LOG_STDOUT_ENABLE_ANSI", &cfg.Log.StdOut.EnableANSI)
	parseEnvVar("RENTERD_LOG_DATABASE_ENABLED", &cfg.Log.Database.Enabled)
	parseEnvVar("RENTERD_LOG_DATABASE_LEVEL", &cfg.Log.Database.Level)
	parseEnvVar("RENTERD_LOG_DATABASE_IGNORE_RECORD_NOT_FOUND_ERROR", &cfg.Log.Database.IgnoreRecordNotFoundError)
	parseEnvVar("RENTERD_LOG_DATABASE_SLOW_THRESHOLD", &cfg.Log.Database.SlowThreshold)

	// parse remotes
	var workerRemotePassStr string
	var workerRemoteAddrsStr string
	parseEnvVar("RENTERD_WORKER_REMOTE_ADDRS", &workerRemoteAddrsStr)
	parseEnvVar("RENTERD_WORKER_API_PASSWORD", &workerRemotePassStr)
	if workerRemoteAddrsStr != "" && workerRemotePassStr != "" {
		mustParseWorkers(workerRemoteAddrsStr, workerRemotePassStr)
	}

	// disable worker if remotes are set
	if len(cfg.Worker.Remotes) > 0 {
		cfg.Worker.Enabled = false
	}

	// combine host bucket bases
	for _, base := range strings.Split(hostBasesStr, ",") {
		if trimmed := strings.TrimSpace(base); trimmed != "" {
			cfg.S3.HostBucketBases = append(cfg.S3.HostBucketBases, base)
		}
	}

	// check that the API password is set
	if cfg.HTTP.Password == "" {
		if disableStdin {
			stdoutFatalError("API password must be set via environment variable or config file when --env flag is set")
			return
		}
	}
	setAPIPassword()

	// check that the seed is set
	if cfg.Seed == "" && (cfg.Worker.Enabled || cfg.Bus.RemoteAddr == "") { // only worker & bus require a seed
		if disableStdin {
			stdoutFatalError("Seed must be set via environment variable or config file when --env flag is set")
			return
		}
		setSeedPhrase()
	}

	// generate private key from seed
	var pk types.PrivateKey
	if cfg.Seed != "" {
		var rawSeed [32]byte
		if err := wallet.SeedFromPhrase(&rawSeed, cfg.Seed); err != nil {
			log.Fatal("failed to load wallet", zap.Error(err))
		}
		pk = wallet.KeyFromSeed(&rawSeed, 0)
	}

	// parse S3 auth keys
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

	// create logger
	if cfg.Log.Level == "" {
		cfg.Log.Level = "info" // default to 'info' if not set
	}
	logger, closeFn, err := NewLogger(cfg.Directory, cfg.Log)
	if err != nil {
		log.Fatalln("failed to create logger:", err)
	}
	defer closeFn(context.Background())

	logger.Info("renterd", zap.String("version", build.Version()), zap.String("network", build.NetworkName()), zap.String("commit", build.Commit()), zap.Time("buildDate", build.BuildTime()))
	if runtime.GOARCH == "amd64" && !cpu.X86.HasAVX2 {
		logger.Warn("renterd is running on a system without AVX2 support, performance may be degraded")
	}

	if cfg.Log.Database.Level == "" {
		cfg.Log.Database.Level = cfg.Log.Level
	}

	network, genesis := build.Network()
	busCfg := node.BusConfig{
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
	}

	type shutdownFnEntry struct {
		name string
		fn   func(context.Context) error
	}
	var shutdownFns []shutdownFnEntry

	if cfg.Bus.RemoteAddr != "" && !cfg.Worker.Enabled && !cfg.Autopilot.Enabled {
		logger.Fatal("remote bus, remote worker, and no autopilot -- nothing to do!")
	}
	if cfg.Worker.Enabled && cfg.Bus.RemoteAddr != "" && cfg.Worker.ExternalAddress == "" {
		logger.Fatal("can't enable the worker using a remote bus, without configuring the worker's external address")
	}
	if cfg.Autopilot.Enabled && !cfg.Worker.Enabled && len(cfg.Worker.Remotes) == 0 {
		logger.Fatal("can't enable autopilot without providing either workers to connect to or creating a worker")
	}

	// create listener first, so that we know the actual apiAddr if the user
	// specifies port :0
	l, err := listenTCP(logger, cfg.HTTP.Address)
	if err != nil {
		logger.Fatal("failed to create listener: " + err.Error())
	}

	// override the address with the actual one
	cfg.HTTP.Address = "http://" + l.Addr().String()

	auth := jape.BasicAuth(cfg.HTTP.Password)
	mux := &utils.TreeMux{
		Sub: make(map[string]utils.TreeMux),
	}

	// Create the webserver.
	srv := &http.Server{Handler: mux}
	shutdownFns = append(shutdownFns, shutdownFnEntry{
		name: "HTTP Server",
		fn:   srv.Shutdown,
	})

	if err := os.MkdirAll(cfg.Directory, 0700); err != nil {
		logger.Fatal("failed to create directory: " + err.Error())
	}

	busAddr, busPassword := cfg.Bus.RemoteAddr, cfg.Bus.RemotePassword
	setupBusFn := node.NoopFn
	if cfg.Bus.RemoteAddr == "" {
		b, setupFn, shutdownFn, _, _, err := node.NewBus(busCfg, cfg.Directory, pk, logger)
		if err != nil {
			logger.Fatal("failed to create bus, err: " + err.Error())
		}
		setupBusFn = setupFn
		shutdownFns = append(shutdownFns, shutdownFnEntry{
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

	var s3Srv *http.Server
	var s3Listener net.Listener
	var workers []autopilot.Worker
	setupWorkerFn := node.NoopFn
	if len(cfg.Worker.Remotes) == 0 {
		if cfg.Worker.Enabled {
			workerAddr := cfg.HTTP.Address + "/api/worker"
			var shutdownFn node.ShutdownFn
			w, s3Handler, setupFn, shutdownFn, err := node.NewWorker(cfg.Worker, s3.Opts{
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
			setupWorkerFn = func(ctx context.Context) error {
				return setupFn(ctx, workerExternAddr, cfg.HTTP.Password)
			}
			shutdownFns = append(shutdownFns, shutdownFnEntry{
				name: "Worker",
				fn:   shutdownFn,
			})

			mux.Sub["/api/worker"] = utils.TreeMux{Handler: iworker.Auth(cfg.HTTP.Password, cfg.Worker.AllowUnauthenticatedDownloads)(w)}
			wc := worker.NewClient(workerAddr, cfg.HTTP.Password)
			workers = append(workers, wc)

			if cfg.S3.Enabled {
				s3Srv = &http.Server{
					Addr:    cfg.S3.Address,
					Handler: s3Handler,
				}
				s3Listener, err = listenTCP(logger, cfg.S3.Address)
				if err != nil {
					logger.Fatal("failed to create listener: " + err.Error())
				}
				shutdownFns = append(shutdownFns, shutdownFnEntry{
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
		shutdownFns = append(shutdownFns, shutdownFnEntry{
			name: "Autopilot",
			fn:   fn,
		})

		go func() { autopilotErr <- runFn() }()
		mux.Sub["/api/autopilot"] = utils.TreeMux{Handler: auth(ap)}
	}

	// Start server.
	go srv.Serve(l)

	// Finish bus setup.
	if err := setupBusFn(context.Background()); err != nil {
		logger.Fatal("failed to setup bus: " + err.Error())
	}

	// Finish worker setup.
	if err := setupWorkerFn(context.Background()); err != nil {
		logger.Fatal("failed to setup worker: " + err.Error())
	}

	// Set initial S3 keys.
	if cfg.S3.Enabled && !cfg.S3.DisableAuth {
		as, err := bc.S3AuthenticationSettings(context.Background())
		if err != nil && !strings.Contains(err.Error(), api.ErrSettingNotFound.Error()) {
			logger.Fatal("failed to fetch S3 authentication settings: " + err.Error())
		} else if as.V4Keypairs == nil {
			as.V4Keypairs = make(map[string]string)
		}

		// S3 key pair validation was broken at one point, we need to remove the
		// invalid key pairs here to ensure we don't fail when we update the
		// setting below.
		for k, v := range as.V4Keypairs {
			if err := (api.S3AuthenticationSettings{V4Keypairs: map[string]string{k: v}}).Validate(); err != nil {
				logger.Sugar().Infof("removing invalid S3 keypair for AccessKeyID %s, reason: %v", k, err)
				delete(as.V4Keypairs, k)
			}
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

	if cfg.AutoOpenWebUI {
		time.Sleep(time.Millisecond) // give the web server a chance to start
		_, port, err := net.SplitHostPort(l.Addr().String())
		if err != nil {
			logger.Debug("failed to parse API address", zap.Error(err))
		} else if err := openBrowser(fmt.Sprintf("http://127.0.0.1:%s", port)); err != nil {
			logger.Debug("failed to open browser", zap.Error(err))
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
