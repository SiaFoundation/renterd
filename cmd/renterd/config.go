package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/renterd/config"
	"golang.org/x/term"
	"gopkg.in/yaml.v3"
)

// TODO: handle RENTERD_S3_HOST_BUCKET_BASES correctly

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
	disableStdin bool
	enableANSI   = runtime.GOOS != "windows"

	hostBasesStr string
)

func defaultConfig() config.Config {
	return config.Config{
		Directory:     ".",
		Seed:          os.Getenv("RENTERD_SEED"),
		AutoOpenWebUI: true,
		Network:       "mainnet",
		HTTP: config.HTTP{
			Address:  "localhost:9980",
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
		Explorer: config.ExplorerData{
			URL: "https://api.siascan.com",
		},
		Log: config.Log{
			Level: "",
			File: config.LogFile{
				Enabled: true,
				Format:  "human",
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
				SlowThreshold:             500 * time.Millisecond,
			},
		},
		Bus: config.Bus{
			AnnouncementMaxAgeHours:       24 * 7 * 52, // 1 year
			Bootstrap:                     true,
			GatewayAddr:                   ":9981",
			UsedUTXOExpiry:                24 * time.Hour,
			SlabBufferCompletionThreshold: 1 << 12,
		},
		Worker: config.Worker{
			Enabled: true,

			ID:                     "",
			AccountsRefillInterval: defaultAccountRefillInterval,
			BusFlushInterval:       5 * time.Second,
			CacheExpiry:            5 * time.Minute,

			DownloadMaxOverdrive:     5,
			DownloadOverdriveTimeout: 3 * time.Second,

			DownloadMaxMemory:      1 << 30, // 1 GiB
			UploadMaxMemory:        1 << 30, // 1 GiB
			UploadMaxOverdrive:     5,
			UploadOverdriveTimeout: 3 * time.Second,
		},
		Autopilot: config.Autopilot{
			Enabled: true,

			Heartbeat: 30 * time.Minute,

			MigratorAccountsRefillInterval:   defaultAccountRefillInterval,
			MigratorHealthCutoff:             0.75,
			MigratorNumThreads:               1,
			MigratorDownloadMaxOverdrive:     5,
			MigratorDownloadOverdriveTimeout: 3 * time.Second,
			MigratorUploadMaxOverdrive:       5,
			MigratorUploadOverdriveTimeout:   3 * time.Second,

			RevisionBroadcastInterval: 7 * 24 * time.Hour,
			RevisionSubmissionBuffer:  150, // 144 + 6 blocks leeway

			ScannerBatchSize:  100,
			ScannerInterval:   4 * time.Hour,
			ScannerNumThreads: 10,
		},
		S3: config.S3{
			Address:     "localhost:8080",
			Enabled:     true,
			DisableAuth: false,
		},
	}
}

func assertWorkerID(cfg *config.Config) error {
	if !cfg.Worker.Enabled {
		// no worker
		return nil
	} else if cfg.Bus.RemoteAddr != "" && cfg.Worker.ID == "" {
		// remote worker
		return errors.New("a unique worker ID must be set in a cluster setup")
	} else if cfg.Worker.ID == "" {
		// local worker
		cfg.Worker.ID = "worker"
	}
	return nil
}

// loadConfig creates a default config and overrides it with the contents of the
// YAML file (specified by the RENTERD_CONFIG_FILE), CLI flags, and environment
// variables, in that order.
func loadConfig() (cfg config.Config, network *consensus.Network, genesis types.Block, err error) {
	cfg = defaultConfig()
	if err = parseYamlConfig(&cfg); err != nil {
		return
	}
	parseCLIFlags(&cfg)
	parseEnvironmentVariables(&cfg)

	// check worker id
	if err = assertWorkerID(&cfg); err != nil {
		return
	}

	// check network
	switch cfg.Network {
	case "anagami":
		network, genesis = chain.TestnetAnagami()
	case "mainnet":
		network, genesis = chain.Mainnet()
	case "zen":
		network, genesis = chain.TestnetZen()
	default:
		err = fmt.Errorf("unknown network '%s'", cfg.Network)
		return
	}

	// check explorer
	if !cfg.Explorer.Disable && cfg.Explorer.URL == "" {
		err = fmt.Errorf("explorer is enabled but no URL is set")
		return
	}

	return
}

func sanitizeConfig(cfg *config.Config) error {
	// combine host bucket bases
	for _, base := range strings.Split(hostBasesStr, ",") {
		if trimmed := strings.TrimSpace(base); trimmed != "" {
			cfg.S3.HostBucketBases = append(cfg.S3.HostBucketBases, base)
		}
	}

	// check that the API password is set
	if cfg.HTTP.Password == "" {
		if disableStdin {
			return errors.New("API password must be set via environment variable or config file when --env flag is set")
		}
	}
	setAPIPassword(cfg)

	// check that the seed is set
	if cfg.Seed == "" && (cfg.Worker.Enabled || cfg.Bus.RemoteAddr == "") { // only worker & bus require a seed
		if disableStdin {
			return errors.New("Seed must be set via environment variable or config file when --env flag is set")
		}
		setSeedPhrase(cfg)
	}

	// validate the seed is valid
	if cfg.Seed != "" {
		var rawSeed [32]byte
		if err := wallet.SeedFromPhrase(&rawSeed, cfg.Seed); err != nil {
			return fmt.Errorf("failed to load wallet: %v", err)
		}
	}

	// default log levels
	if cfg.Log.Level == "" {
		cfg.Log.Level = "info"
	}
	if cfg.Log.Database.Level == "" {
		cfg.Log.Database.Level = cfg.Log.Level
	}

	return nil
}

func parseYamlConfig(cfg *config.Config) error {
	configPath := "renterd.yml"
	if str := os.Getenv("RENTERD_CONFIG_FILE"); str != "" {
		configPath = str
	}

	// If the config file doesn't exist, don't try to load it.
	_, err := os.Stat(configPath)
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	}

	f, err := os.Open(configPath)
	if err != nil {
		return fmt.Errorf("failed to open config file: %w", err)
	}
	defer f.Close()

	dec := yaml.NewDecoder(f)
	dec.KnownFields(true)

	if err := dec.Decode(&cfg); err != nil {
		return fmt.Errorf("failed to decode config file: %w", err)
	}
	return nil
}

func parseCLIFlags(cfg *config.Config) {
	// node
	flag.StringVar(&cfg.HTTP.Address, "http", cfg.HTTP.Address, "Address for serving the API")
	flag.StringVar(&cfg.Directory, "dir", cfg.Directory, "Directory for storing node state")
	flag.BoolVar(&disableStdin, "env", false, "disable stdin prompts for environment variables (default false)")
	flag.BoolVar(&cfg.AutoOpenWebUI, "openui", cfg.AutoOpenWebUI, "automatically open the web UI on startup")
	flag.StringVar(&cfg.Network, "network", cfg.Network, "Network to connect to (mainnet|zen|anagami). Defaults to 'mainnet' (overrides with RENTERD_NETWORK)")

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
	flag.DurationVar(&cfg.Log.Database.SlowThreshold, "log.database.slowThreshold", cfg.Log.Database.SlowThreshold, "Threshold for slow queries in logger. Defaults to 500ms (overrides with RENTERD_LOG_DATABASE_SLOW_THRESHOLD)")

	// db
	flag.StringVar(&cfg.Database.MySQL.URI, "db.uri", cfg.Database.MySQL.URI, "Database URI for the bus (overrides with RENTERD_DB_URI)")
	flag.StringVar(&cfg.Database.MySQL.User, "db.user", cfg.Database.MySQL.User, "Database username for the bus (overrides with RENTERD_DB_USER)")
	flag.StringVar(&cfg.Database.MySQL.Database, "db.name", cfg.Database.MySQL.Database, "Database name for the bus (overrides with RENTERD_DB_NAME)")
	flag.StringVar(&cfg.Database.MySQL.MetricsDatabase, "db.metricsName", cfg.Database.MySQL.MetricsDatabase, "Database for metrics (overrides with RENTERD_DB_METRICS_NAME)")

	// bus
	flag.BoolVar(&cfg.Bus.AllowPrivateIPs, "bus.allowPrivateIPs", cfg.Bus.AllowPrivateIPs, "Allows hosts with private IPs")
	flag.Uint64Var(&cfg.Bus.AnnouncementMaxAgeHours, "bus.announcementMaxAgeHours", cfg.Bus.AnnouncementMaxAgeHours, "Max age for announcements")
	flag.BoolVar(&cfg.Bus.Bootstrap, "bus.bootstrap", cfg.Bus.Bootstrap, "Bootstraps gateway and consensus modules")
	flag.StringVar(&cfg.Bus.GatewayAddr, "bus.gatewayAddr", cfg.Bus.GatewayAddr, "Address for Sia peer connections (overrides with RENTERD_BUS_GATEWAY_ADDR)")
	flag.DurationVar(&cfg.Bus.UsedUTXOExpiry, "bus.usedUTXOExpiry", cfg.Bus.UsedUTXOExpiry, "Expiry for used UTXOs in transactions")
	flag.Int64Var(&cfg.Bus.SlabBufferCompletionThreshold, "bus.slabBufferCompletionThreshold", cfg.Bus.SlabBufferCompletionThreshold, "Threshold for slab buffer upload (overrides with RENTERD_BUS_SLAB_BUFFER_COMPLETION_THRESHOLD)")

	// worker
	flag.DurationVar(&cfg.Worker.AccountsRefillInterval, "worker.accountRefillInterval", cfg.Worker.AccountsRefillInterval, "Interval for refilling workers' account balances")
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

	// autopilot
	flag.DurationVar(&cfg.Autopilot.Heartbeat, "autopilot.heartbeat", cfg.Autopilot.Heartbeat, "Interval for autopilot loop execution")
	flag.DurationVar(&cfg.Autopilot.RevisionBroadcastInterval, "autopilot.revisionBroadcastInterval", cfg.Autopilot.RevisionBroadcastInterval, "Interval for broadcasting contract revisions (overrides with RENTERD_AUTOPILOT_REVISION_BROADCAST_INTERVAL)")
	flag.Uint64Var(&cfg.Autopilot.ScannerBatchSize, "autopilot.scannerBatchSize", cfg.Autopilot.ScannerBatchSize, "Batch size for host scanning")
	flag.DurationVar(&cfg.Autopilot.ScannerInterval, "autopilot.scannerInterval", cfg.Autopilot.ScannerInterval, "Interval for scanning hosts")
	flag.Uint64Var(&cfg.Autopilot.ScannerNumThreads, "autopilot.scannerNumThreads", cfg.Autopilot.ScannerNumThreads, "Number of threads for scanning hosts")
	flag.BoolVar(&cfg.Autopilot.Enabled, "autopilot.enabled", cfg.Autopilot.Enabled, "Enables/disables autopilot (overrides with RENTERD_AUTOPILOT_ENABLED)")
	flag.DurationVar(&cfg.ShutdownTimeout, "node.shutdownTimeout", cfg.ShutdownTimeout, "Timeout for node shutdown")

	flag.DurationVar(&cfg.Autopilot.MigratorAccountsRefillInterval, "autopilot.migratorAccountRefillInterval", cfg.Autopilot.MigratorAccountsRefillInterval, "Interval for refilling migrator' account balances")
	flag.Float64Var(&cfg.Autopilot.MigratorHealthCutoff, "autopilot.migratorHealthCutoff", cfg.Autopilot.MigratorHealthCutoff, "Threshold for migrating slabs based on health")
	flag.Uint64Var(&cfg.Autopilot.MigratorNumThreads, "autopilot.migratorNumThreads", cfg.Autopilot.MigratorNumThreads, "Parallel slab migrations per worker (overrides with RENTERD_MIGRATOR_PARALLEL_SLABS_PER_WORKER)")
	flag.Uint64Var(&cfg.Autopilot.MigratorDownloadMaxOverdrive, "autopilot.migratorDownloadMaxOverdrive", cfg.Autopilot.MigratorDownloadMaxOverdrive, "Max overdrive workers for migration downloads")
	flag.DurationVar(&cfg.Autopilot.MigratorDownloadOverdriveTimeout, "autopilot.migratorDownloadOverdriveTimeout", cfg.Autopilot.MigratorDownloadOverdriveTimeout, "Timeout for overdriving migration downloads")
	flag.Uint64Var(&cfg.Autopilot.MigratorUploadMaxOverdrive, "autopilot.migratorUploadMaxOverdrive", cfg.Autopilot.MigratorUploadMaxOverdrive, "Max overdrive workers for migration uploads")
	flag.DurationVar(&cfg.Autopilot.MigratorUploadOverdriveTimeout, "autopilot.migratorUploadOverdriveTimeout", cfg.Autopilot.MigratorUploadOverdriveTimeout, "Timeout for overdriving migration uploads")

	// s3
	flag.StringVar(&cfg.S3.Address, "s3.address", cfg.S3.Address, "Address for serving S3 API (overrides with RENTERD_S3_ADDRESS)")
	flag.BoolVar(&cfg.S3.DisableAuth, "s3.disableAuth", cfg.S3.DisableAuth, "Disables authentication for S3 API (overrides with RENTERD_S3_DISABLE_AUTH)")
	flag.BoolVar(&cfg.S3.Enabled, "s3.enabled", cfg.S3.Enabled, "Enables/disables S3 API (requires worker.enabled to be 'true', overrides with RENTERD_S3_ENABLED)")
	flag.StringVar(&hostBasesStr, "s3.hostBases", "", "Enables bucket rewriting in the router for specific hosts provided via comma-separated list (overrides with RENTERD_S3_HOST_BUCKET_BASES)")
	flag.BoolVar(&cfg.S3.HostBucketEnabled, "s3.hostBucketEnabled", cfg.S3.HostBucketEnabled, "Enables bucket rewriting in the router for all hosts (overrides with RENTERD_S3_HOST_BUCKET_ENABLED)")

	// explorer
	flag.StringVar(&cfg.Explorer.URL, "explorer.url", cfg.Explorer.URL, "URL of service to retrieve data about the Sia network (overrides with RENTERD_EXPLORER_URL)")
	flag.BoolVar(&cfg.Explorer.Disable, "explorer.disable", cfg.Explorer.Disable, "Disables explorer service (overrides with RENTERD_EXPLORER_DISABLE)")

	// custom usage
	flag.Usage = func() {
		log.Print(usageHeader)
		flag.PrintDefaults()
		log.Print(usageFooter)
	}

	flag.Parse()
}

func parseEnvironmentVariables(cfg *config.Config) {
	// define helper function to parse environment variables
	parseEnvVar := func(s string, v interface{}) {
		if env, ok := os.LookupEnv(s); ok {
			_, err := fmt.Sscan(env, v)
			checkFatalError(fmt.Sprintf("failed to parse %s", s), err)
			fmt.Printf("Using %s environment variable\n", s)
		}
	}

	parseEnvVar("RENTERD_NETWORK", &cfg.Network)

	parseEnvVar("RENTERD_BUS_REMOTE_ADDR", &cfg.Bus.RemoteAddr)
	parseEnvVar("RENTERD_BUS_API_PASSWORD", &cfg.Bus.RemotePassword)
	parseEnvVar("RENTERD_BUS_GATEWAY_ADDR", &cfg.Bus.GatewayAddr)
	parseEnvVar("RENTERD_BUS_SLAB_BUFFER_COMPLETION_THRESHOLD", &cfg.Bus.SlabBufferCompletionThreshold)

	parseEnvVar("RENTERD_DB_URI", &cfg.Database.MySQL.URI)
	parseEnvVar("RENTERD_DB_USER", &cfg.Database.MySQL.User)
	parseEnvVar("RENTERD_DB_PASSWORD", &cfg.Database.MySQL.Password)
	parseEnvVar("RENTERD_DB_NAME", &cfg.Database.MySQL.Database)
	parseEnvVar("RENTERD_DB_METRICS_NAME", &cfg.Database.MySQL.MetricsDatabase)
	parseEnvVar("RENTERD_DB_LOGGER_LOG_LEVEL", &cfg.Log.Level)

	parseEnvVar("RENTERD_WORKER_ENABLED", &cfg.Worker.Enabled)
	parseEnvVar("RENTERD_WORKER_ID", &cfg.Worker.ID)
	parseEnvVar("RENTERD_WORKER_UNAUTHENTICATED_DOWNLOADS", &cfg.Worker.AllowUnauthenticatedDownloads)
	parseEnvVar("RENTERD_WORKER_DOWNLOAD_MAX_MEMORY", &cfg.Worker.DownloadMaxMemory)
	parseEnvVar("RENTERD_WORKER_UPLOAD_MAX_MEMORY", &cfg.Worker.UploadMaxMemory)

	parseEnvVar("RENTERD_AUTOPILOT_ENABLED", &cfg.Autopilot.Enabled)
	parseEnvVar("RENTERD_AUTOPILOT_REVISION_BROADCAST_INTERVAL", &cfg.Autopilot.RevisionBroadcastInterval)

	parseEnvVar("RENTERD_S3_ADDRESS", &cfg.S3.Address)
	parseEnvVar("RENTERD_S3_ENABLED", &cfg.S3.Enabled)
	parseEnvVar("RENTERD_S3_DISABLE_AUTH", &cfg.S3.DisableAuth)
	parseEnvVar("RENTERD_S3_HOST_BUCKET_ENABLED", &cfg.S3.HostBucketEnabled)
	parseEnvVar("RENTERD_S3_HOST_BUCKET_BASES", &cfg.S3.HostBucketBases)

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

	parseEnvVar("RENTERD_EXPLORER_DISABLE", &cfg.Explorer.Disable)
	parseEnvVar("RENTERD_EXPLORER_URL", &cfg.Explorer.URL)
}

// readPasswordInput reads a password from stdin.
func readPasswordInput(context string) string {
	fmt.Printf("%s: ", context)
	input, err := term.ReadPassword(int(os.Stdin.Fd()))
	checkFatalError("Could not read input", err)
	fmt.Println("")
	return string(input)
}

func readInput(context string) string {
	fmt.Printf("%s: ", context)
	r := bufio.NewReader(os.Stdin)
	input, err := r.ReadString('\n')
	checkFatalError("Could not read input", err)
	return strings.TrimSpace(input)
}

// wrapANSI wraps the output in ANSI escape codes if enabled.
// nolint: unparam
func wrapANSI(prefix, output, suffix string) string {
	if enableANSI {
		return prefix + output + suffix
	}
	return output
}

func humanList(s []string, sep string) string {
	if len(s) == 0 {
		return ""
	} else if len(s) == 1 {
		return fmt.Sprintf(`%q`, s[0])
	} else if len(s) == 2 {
		return fmt.Sprintf(`%q %s %q`, s[0], sep, s[1])
	}

	var sb strings.Builder
	for i, v := range s {
		if i != 0 {
			sb.WriteString(", ")
		}
		if i == len(s)-1 {
			sb.WriteString("or ")
		}
		sb.WriteString(`"`)
		sb.WriteString(v)
		sb.WriteString(`"`)
	}
	return sb.String()
}

func promptQuestion(question string, answers []string) string {
	for {
		input := readInput(fmt.Sprintf("%s (%s)", question, strings.Join(answers, "/")))
		for _, answer := range answers {
			if strings.EqualFold(input, answer) {
				return answer
			}
		}
		fmt.Println(wrapANSI("\033[31m", fmt.Sprintf("Answer must be %s", humanList(answers, "or")), "\033[0m"))
	}
}

func promptYesNo(question string) bool {
	answer := promptQuestion(question, []string{"yes", "no"})
	return strings.EqualFold(answer, "yes")
}

// stdoutError prints an error message to stdout
func stdoutError(msg string) {
	if enableANSI {
		fmt.Println(wrapANSI("\033[31m", msg, "\033[0m"))
	} else {
		fmt.Println(msg)
	}
}

func setInputValue(context string, value *string) {
	if *value != "" {
		context = fmt.Sprintf("%s (default: %q)", context, *value)
	}
	if input := readInput(context); input != "" {
		*value = input
	}
}

func setListenAddress(context string, value *string, allowEmpty bool) {
	// will continue to prompt until a valid value is entered
	for {
		input := readInput(fmt.Sprintf("%s (currently %q)", context, *value))
		if input == "" {
			if allowEmpty {
				return
			}
			stdoutError(fmt.Sprintf("Invalid %s %q: must not be empty", context, input))
			continue
		}

		host, port, err := net.SplitHostPort(input)
		if err != nil {
			stdoutError(fmt.Sprintf("Invalid %s port %q: %s", context, input, err.Error()))
			continue
		}

		n, err := strconv.Atoi(port)
		if err != nil {
			stdoutError(fmt.Sprintf("Invalid %s port %q: %s", context, input, err.Error()))
			continue
		} else if n < 0 || n > 65535 {
			stdoutError(fmt.Sprintf("Invalid %s port %q: must be between 0 and 65535", context, input))
			continue
		}
		*value = net.JoinHostPort(host, port)
		return
	}
}

// setSeedPhrase prompts the user to enter a seed phrase if one is not already
// set via environment variable or config file.
func setSeedPhrase(cfg *config.Config) {
	// retry until a valid seed phrase is entered
	for {
		fmt.Println("")
		fmt.Println("Type in your 12-word seed phrase and press enter. If you do not have a seed phrase yet, type 'seed' to generate one.")
		phrase := readPasswordInput("Enter seed phrase")

		if strings.ToLower(strings.TrimSpace(phrase)) == "seed" {
			// generate a new seed phrase
			var seed [32]byte
			phrase = wallet.NewSeedPhrase()
			if err := wallet.SeedFromPhrase(&seed, phrase); err != nil {
				panic(err)
			}
			key := wallet.KeyFromSeed(&seed, 0)
			fmt.Println("")
			fmt.Println("")
			fmt.Println("A new seed phrase has been generated below. " + wrapANSI("\033[1m", "Write it down and keep it safe.", "\033[0m"))
			fmt.Println("Your seed phrase is the only way to recover your Siacoin. If you lose your seed phrase, you will also lose your Siacoin.")
			fmt.Println("")
			fmt.Println(wrapANSI("\033[34;1m", "Seed Phrase:", "\033[0m"), phrase)
			fmt.Println(wrapANSI("\033[34;1m", "Wallet Address:", "\033[0m"), types.StandardUnlockHash(key.PublicKey()))

			// confirm seed phrase
			for {
				fmt.Println("")
				fmt.Println(wrapANSI("\033[1m", "Please confirm your seed phrase to continue.", "\033[0m"))

				confirmPhrase := readPasswordInput("Enter seed phrase")
				if confirmPhrase == phrase {
					cfg.Seed = phrase
					return
				}

				fmt.Println(wrapANSI("\033[31m", "Seed phrases do not match!", "\033[0m"))
				fmt.Println("You entered:", confirmPhrase)
				fmt.Println("Actual phrase:", phrase)
			}
		}

		var seed [32]byte
		if err := wallet.SeedFromPhrase(&seed, phrase); err != nil {
			// invalid seed phrase, retry
			fmt.Println(wrapANSI("\033[31m", "Invalid seed phrase:", "\033[0m"), err)
			fmt.Println("You entered:", phrase)
			continue
		}

		// valid seed phrase
		cfg.Seed = phrase
		return
	}
}

// setAPIPassword prompts the user to enter an API password if one is not
// already set via environment variable or config file.
func setAPIPassword(cfg *config.Config) {
	// return early if the password is already set
	if len(cfg.HTTP.Password) >= 4 {
		return
	}

	// retry until a valid API password is entered
	for len(cfg.HTTP.Password) < 4 {
		fmt.Println("Please choose a password for the renterd admin UI.")
		fmt.Println("This password will be required to access the admin UI in your web browser.")
		fmt.Println("(The password must be at least 4 characters.)")

		cfg.HTTP.Password = readPasswordInput("Enter password")
		if len(cfg.HTTP.Password) < 4 {
			// invalid password, retry
			fmt.Println(wrapANSI("\033[31m", "Password must be at least 4 characters!", "\033[0m"))
			fmt.Println("")
		}
	}
}

func setAdvancedConfig(cfg *config.Config) {
	if !promptYesNo("Would you like to configure advanced settings?") {
		return
	}

	fmt.Println("")
	fmt.Println("Advanced settings are used to configure the renter's behavior.")
	fmt.Println("You can leave these settings blank to use the defaults.")

	// http address
	fmt.Println("")
	fmt.Println("The HTTP address is used to serve the renter's admin API.")
	fmt.Println("The admin API is used to configure the renter.")
	fmt.Println("It should not be exposed to the public internet without setting up a reverse proxy.")
	setListenAddress("HTTP Address", &cfg.HTTP.Address, true)

	// gateway address
	fmt.Println("")
	fmt.Println("The gateway address is used to exchange blocks with other nodes in the Sia network")
	fmt.Println("It should be exposed publicly to improve the renter's connectivity.")
	setListenAddress("Gateway Address", &cfg.Bus.GatewayAddr, true)

	// database
	fmt.Println("")
	fmt.Println("The database is used to store the renter's metadata.")
	fmt.Println("The embedded SQLite database requires no additional configuration and is ideal for testing or demo purposes.")
	fmt.Println("For production usage, we recommend MySQL, which requires a separate MySQL server.")
	setStoreConfig(cfg)
}

func setStoreConfig(cfg *config.Config) {
	store := promptQuestion("Which data store would you like to use?", []string{"mysql", "sqlite"})
	switch store {
	case "mysql":
		fmt.Println("")
		fmt.Println("The MySQL database is used to store the renter metadata.")
		fmt.Println("You will need to set up a MySQL server to connect to.")
		fmt.Println("")
		fmt.Println("You will also need to create two database")
		fmt.Println(" - The first database will be used to store the object metadata.")
		fmt.Println(" - The second database will be used to store metrics.")
		fmt.Println("")
		setListenAddress("MySQL address", &cfg.Database.MySQL.URI, false)

		cfg.Database.MySQL.User = readInput("MySQL username")
		cfg.Database.MySQL.Password = readPasswordInput("MySQL password")
		setInputValue("Object database name", &cfg.Database.MySQL.Database)
		setInputValue("Metrics database name", &cfg.Database.MySQL.MetricsDatabase)
	default:
		cfg.Database.MySQL = config.MySQL{} // omit defaults
		return
	}
}

func setS3Config(cfg *config.Config) {
	if !promptYesNo("Would you like to configure S3 settings?") {
		return
	} else if !promptYesNo("Would you like to enable the S3 gateway?") {
		cfg.S3.Enabled = false
		return
	}

	fmt.Println("")
	fmt.Println("S3 settings are used to configure renterd's S3-compatible gateway.")
	fmt.Println("You can leave these settings blank to use the defaults.")
	fmt.Println("")

	// s3 address
	fmt.Println("The S3 address is used to serve the renter's S3 API.")
	fmt.Println("The S3 API provides an S3-compatible gateway for uploading data to Sia.")
	fmt.Println("It should not be exposed to the public internet without setting up a reverse proxy.")
	setListenAddress("S3 Address", &cfg.S3.Address, true)
}
