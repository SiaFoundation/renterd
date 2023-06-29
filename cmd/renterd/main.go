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
	"go.sia.tech/renterd/internal/node"
	"go.sia.tech/renterd/stores"
	"go.sia.tech/renterd/tracing"
	"go.sia.tech/renterd/wallet"
	"go.sia.tech/renterd/worker"
	"go.sia.tech/web/renterd"
	"golang.org/x/term"
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

	// fetched once, then cached
	apiPassword *string
	seed        *types.PrivateKey
)

func check(context string, err error) {
	if err != nil {
		log.Fatalf("%v: %v", context, err)
	}
}

func getAPIPassword() string {
	if apiPassword == nil {
		pw := os.Getenv("RENTERD_API_PASSWORD")
		if pw != "" {
			fmt.Println("Using RENTERD_API_PASSWORD environment variable.")
			apiPassword = &pw
		} else {
			fmt.Print("Enter API password: ")
			pw, err := term.ReadPassword(int(os.Stdin.Fd()))
			fmt.Println()
			if err != nil {
				log.Fatal(err)
			}
			s := string(pw)
			apiPassword = &s
		}
	}
	return *apiPassword
}

func getSeed() types.PrivateKey {
	if seed == nil {
		phrase := os.Getenv("RENTERD_SEED")
		if phrase != "" {
			fmt.Println("Using RENTERD_SEED environment variable")
		} else {
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
		seed = &key
	}
	return *seed
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

	var nodeCfg struct {
		shutdownTimeout time.Duration
	}

	var busCfg struct {
		remoteAddr  string
		apiPassword string
		node.BusConfig
	}
	busCfg.Network = build.ConsensusNetwork

	var dbCfg struct {
		uri      string
		user     string
		password string
		database string
	}

	var dbLoggerCfg struct {
		ignoreNotFoundError string
		logLevel            string
		slowThreshold       string
	}

	var workerCfg struct {
		enabled     bool
		remoteAddrs string
		apiPassword string
		node.WorkerConfig
	}
	workerCfg.ContractLockTimeout = 30 * time.Second

	var autopilotCfg struct {
		enabled bool
		node.AutopilotConfig
	}
	autopilotCfg.RevisionSubmissionBuffer = api.BlocksPerDay
	// node
	apiAddr := flag.String("http", build.DefaultAPIAddress, "address to serve API on")
	dir := flag.String("dir", ".", "directory to store node state in")
	tracingEnabled := flag.Bool("tracing-enabled", false, "Enables tracing through OpenTelemetry. If RENTERD_TRACING_ENABLED is set, it overwrites the CLI flag's value. Tracing can be configured using the standard OpenTelemetry environment variables. https://github.com/open-telemetry/opentelemetry-specification/blob/v1.8.0/specification/protocol/exporter.md")
	tracingServiceInstanceId := flag.String("tracing-service-instance-id", "cluster", "ID of the service instance used for tracing. If RENTERD_TRACING_SERVICE_INSTANCE_ID is set, it overwrites the CLI flag's value.")

	// db
	flag.StringVar(&dbCfg.uri, "db.uri", "", "URI of the database to use for the bus - can be overwritten using RENTERD_DB_URI environment variable")
	flag.StringVar(&dbCfg.user, "db.user", "", "username for the database to use for the bus - can be overwritten using RENTERD_DB_USER environment variable")
	flag.StringVar(&dbCfg.password, "db.password", "", "password for the database to use for the bus - can be overwritten using RENTERD_DB_PASSWORD environment variable")
	flag.StringVar(&dbCfg.database, "db.name", "", "name of the database to use for the bus - can be overwritten using RENTERD_DB_NAME environment variable")

	// db logger
	flag.StringVar(&dbLoggerCfg.ignoreNotFoundError, "db.logger.ignoreNotFoundError", "true", "ignore not found error for logger - can be overwritten using RENTERD_DB_LOGGER_IGNORE_NOT_FOUND_ERROR environment variable")
	flag.StringVar(&dbLoggerCfg.logLevel, "db.logger.logLevel", "warn", "log level for logger - can be overwritten using RENTERD_DB_LOGGER_LOG_LEVEL environment variable")
	flag.StringVar(&dbLoggerCfg.slowThreshold, "db.logger.slowThreshold", "500ms", "slow threshold for logger - can be overwritten using RENTERD_DB_LOGGER_SLOW_THRESHOLD environment variable")

	// bus
	flag.BoolVar(&busCfg.Bootstrap, "bus.bootstrap", true, "bootstrap the gateway and consensus modules")
	flag.StringVar(&busCfg.GatewayAddr, "bus.gatewayAddr", build.DefaultGatewayAddress, "address to listen on for Sia peer connections - can be overwritten using RENTERD_BUS_GATEWAY_ADDR environment variable")
	flag.DurationVar(&busCfg.PersistInterval, "bus.persistInterval", busCfg.PersistInterval, "interval at which to persist the consensus updates")
	flag.StringVar(&busCfg.apiPassword, "bus.apiPassword", "", "API password for remote bus service - can be overwritten using RENTERD_BUS_API_PASSWORD environment variable")
	flag.StringVar(&busCfg.remoteAddr, "bus.remoteAddr", "", "URL of remote bus service - can be overwritten using RENTERD_BUS_REMOTE_ADDR environment variable")
	flag.DurationVar(&busCfg.UsedUTXOExpiry, "bus.usedUTXOExpiry", 24*time.Hour, "time after which a used UTXO that hasn't been included in a transaction becomes spendable again")

	// worker
	flag.BoolVar(&workerCfg.AllowPrivateIPs, "worker.allowPrivateIPs", false, "allow hosts with private IPs")
	flag.DurationVar(&workerCfg.BusFlushInterval, "worker.busFlushInterval", 5*time.Second, "time after which the worker flushes buffered data to bus for persisting")
	flag.Uint64Var(&workerCfg.DownloadMaxOverdrive, "worker.downloadMaxOverdrive", 5, "maximum number of active overdrive workers when downloading a slab")
	flag.StringVar(&workerCfg.WorkerConfig.ID, "worker.id", "worker", "unique identifier of worker used internally - can be overwritten using the RENTERD_WORKER_ID environment variable")
	flag.DurationVar(&workerCfg.DownloadSectorTimeout, "worker.downloadSectorTimeout", 3*time.Second, "timeout applied to sector downloads when downloading a slab")
	flag.Uint64Var(&workerCfg.UploadMaxOverdrive, "worker.uploadMaxOverdrive", 5, "maximum number of active overdrive workers when uploading a slab")
	flag.DurationVar(&workerCfg.UploadOverdriveTimeout, "worker.uploadOverdriveTimeout", 3*time.Second, "timeout applied to slab uploads when we start overdriving shards")
	flag.StringVar(&workerCfg.apiPassword, "worker.apiPassword", "", "API password for remote worker service")
	flag.BoolVar(&workerCfg.enabled, "worker.enabled", true, "enable/disable creating a worker - can be overwritten using the RENTERD_WORKER_ENABLED environment variable")
	flag.StringVar(&workerCfg.remoteAddrs, "worker.remoteAddrs", "", "URL of remote worker service(s). Multiple addresses can be provided by separating them with a semicolon. Can be overwritten using RENTERD_WORKER_REMOTE_ADDRS environment variable")

	// autopilot
	flag.DurationVar(&autopilotCfg.AccountsRefillInterval, "autopilot.accountRefillInterval", defaultAccountRefillInterval, "interval at which the autopilot checks the workers' accounts balance and refills them if necessary")
	flag.DurationVar(&autopilotCfg.Heartbeat, "autopilot.heartbeat", 10*time.Minute, "interval at which autopilot loop runs")
	flag.Float64Var(&autopilotCfg.MigrationHealthCutoff, "autopilot.migrationHealthCutoff", 0.75, "health threshold below which slabs are migrated to new hosts")
	flag.Uint64Var(&autopilotCfg.ScannerBatchSize, "autopilot.scannerBatchSize", 1000, "size of the batch with which hosts are scanned")
	flag.DurationVar(&autopilotCfg.ScannerInterval, "autopilot.scannerInterval", 24*time.Hour, "interval at which hosts are scanned")
	flag.Uint64Var(&autopilotCfg.ScannerMinRecentFailures, "autopilot.scannerMinRecentFailures", 10, "minimum amount of consesutive failed scans a host must have before it is removed for exceeding the max downtime")
	flag.Uint64Var(&autopilotCfg.ScannerNumThreads, "autopilot.scannerNumThreads", 100, "number of threads that scan hosts")
	flag.BoolVar(&autopilotCfg.enabled, "autopilot.enabled", true, "enable/disable the autopilot - can be overwritten using the RENTERD_AUTOPILOT_ENABLED environment variable")
	flag.DurationVar(&nodeCfg.shutdownTimeout, "node.shutdownTimeout", 5*time.Minute, "the timeout applied to the node shutdown")
	flag.Parse()

	log.Println("renterd v0.1.0")
	log.Println("Network", build.ConsensusNetworkName)
	if flag.Arg(0) == "version" {
		log.Println("Commit:", githash)
		log.Println("Build Date:", builddate)
		return
	} else if flag.Arg(0) == "seed" {
		log.Println("Seed phrase:", wallet.NewSeedPhrase())
		return
	}

	// Overwrite flags from environment if set.
	parseEnvVar("RENTERD_TRACING_ENABLED", tracingEnabled)
	parseEnvVar("RENTERD_TRACING_SERVICE_INSTANCE_ID", tracingServiceInstanceId)

	parseEnvVar("RENTERD_BUS_REMOTE_ADDR", &busCfg.remoteAddr)
	parseEnvVar("RENTERD_BUS_API_PASSWORD", &busCfg.apiPassword)
	parseEnvVar("RENTERD_BUS_GATEWAY_ADDR", &busCfg.GatewayAddr)

	parseEnvVar("RENTERD_DB_URI", &dbCfg.uri)
	parseEnvVar("RENTERD_DB_USER", &dbCfg.user)
	parseEnvVar("RENTERD_DB_PASSWORD", &dbCfg.password)
	parseEnvVar("RENTERD_DB_NAME", &dbCfg.database)

	parseEnvVar("RENTERD_DB_LOGGER_IGNORE_NOT_FOUND_ERROR", &dbLoggerCfg.ignoreNotFoundError)
	parseEnvVar("RENTERD_DB_LOGGER_LOG_LEVEL", &dbLoggerCfg.logLevel)
	parseEnvVar("RENTERD_DB_LOGGER_SLOW_THRESHOLD", &dbLoggerCfg.slowThreshold)

	parseEnvVar("RENTERD_WORKER_REMOTE_ADDRS", &workerCfg.remoteAddrs)
	parseEnvVar("RENTERD_WORKER_API_PASSWORD", &workerCfg.apiPassword)
	parseEnvVar("RENTERD_WORKER_ENABLED", &workerCfg.enabled)
	parseEnvVar("RENTERD_WORKER_ID", &workerCfg.ID)

	parseEnvVar("RENTERD_AUTOPILOT_ENABLED", &autopilotCfg.enabled)

	// Init db dialector
	if dbCfg.uri != "" {
		busCfg.DBDialector = stores.NewMySQLConnection(
			dbCfg.user,
			dbCfg.password,
			dbCfg.uri,
			dbCfg.database,
		)
	}

	// Init db logger config
	if cfg, err := stores.ParseLoggerConfig(dbLoggerCfg.logLevel, dbLoggerCfg.ignoreNotFoundError, dbLoggerCfg.slowThreshold); err != nil {
		log.Fatalf("failed to parse logger config, err: %v", err)
	} else {
		busCfg.DBLoggerConfig = cfg
	}

	var autopilotShutdownFn func(context.Context) error
	var shutdownFns []func(context.Context) error

	// Init tracing.
	if *tracingEnabled {
		shutdownFn, err := tracing.Init(*tracingServiceInstanceId)
		if err != nil {
			log.Fatal("failed to init tracing", err)
		}
		shutdownFns = append(shutdownFns, shutdownFn)
	}

	if busCfg.remoteAddr != "" && workerCfg.remoteAddrs != "" && !autopilotCfg.enabled {
		log.Fatal("remote bus, remote worker, and no autopilot -- nothing to do!")
	}
	if workerCfg.remoteAddrs == "" && !workerCfg.enabled && autopilotCfg.enabled {
		log.Fatal("can't enable autopilot without providing either workers to connect to or creating a worker")
	}

	// create listener first, so that we know the actual apiAddr if the user
	// specifies port :0
	l, err := net.Listen("tcp", *apiAddr)
	if err != nil {
		log.Fatal("failed to create listener", err)
	}
	shutdownFns = append(shutdownFns, func(_ context.Context) error {
		_ = l.Close()
		return nil
	})
	*apiAddr = "http://" + l.Addr().String()

	auth := jape.BasicAuth(getAPIPassword())
	mux := treeMux{
		h:   renterd.Handler(),
		sub: make(map[string]treeMux),
	}

	// Create logger.
	renterdLog := filepath.Join(*dir, "renterd.log")
	logger, closeFn, err := node.NewLogger(renterdLog)
	if err != nil {
		log.Fatal("failed to create logger", err)
	}
	shutdownFns = append(shutdownFns, closeFn)

	busAddr, busPassword := busCfg.remoteAddr, busCfg.apiPassword
	if busAddr == "" {
		b, shutdownFn, err := node.NewBus(busCfg.BusConfig, *dir, getSeed(), logger)
		if err != nil {
			log.Fatal("failed to create bus, err: ", err)
		}
		shutdownFns = append(shutdownFns, shutdownFn)

		mux.sub["/api/bus"] = treeMux{h: auth(b)}
		busAddr = *apiAddr + "/api/bus"
		busPassword = getAPIPassword()
	} else {
		fmt.Println("connecting to remote bus at", busAddr)
	}
	bc := bus.NewClient(busAddr, busPassword)

	var workers []autopilot.Worker
	workerAddrs, workerPassword := workerCfg.remoteAddrs, workerCfg.apiPassword
	if workerAddrs == "" {
		if workerCfg.enabled {
			w, shutdownFn, err := node.NewWorker(workerCfg.WorkerConfig, bc, getSeed(), logger)
			if err != nil {
				log.Fatal("failed to create worker", err)
			}
			shutdownFns = append(shutdownFns, shutdownFn)

			mux.sub["/api/worker"] = treeMux{h: auth(w)}
			workerAddr := *apiAddr + "/api/worker"
			workerPassword = getAPIPassword()
			workers = append(workers, worker.NewClient(workerAddr, workerPassword))
		}
	} else {
		// TODO: all workers use the same password. Figure out a nice way to
		// have individual passwords.
		workerAddrsSplit := strings.Split(workerAddrs, ";")
		for _, workerAddr := range workerAddrsSplit {
			workers = append(workers, worker.NewClient(workerAddr, workerPassword))
			fmt.Println("connecting to remote worker at", workerAddr)
		}
	}

	autopilotErr := make(chan error, 1)
	autopilotDir := filepath.Join(*dir, "autopilot")
	if autopilotCfg.enabled {
		autopilotCfg.AutopilotConfig.ID = api.DefaultAutopilotID // hardcoded
		ap, runFn, shutdownFn, err := node.NewAutopilot(autopilotCfg.AutopilotConfig, bc, workers, logger)
		if err != nil {
			log.Fatal("failed to create autopilot", err)
		}

		// NOTE: the autopilot shutdown function is not added to the shutdown
		// functions array because it needs to be called first
		autopilotShutdownFn = shutdownFn

		go func() { autopilotErr <- runFn() }()
		mux.sub["/api/autopilot"] = treeMux{h: auth(ap)}
	}

	srv := &http.Server{Handler: mux}
	go srv.Serve(l)
	log.Println("api: Listening on", l.Addr())

	syncerAddress, err := bc.SyncerAddress(context.Background())
	if err != nil {
		log.Fatal("failed to fetch syncer address", err)
	}
	log.Println("bus: Listening on", syncerAddress)

	if autopilotCfg.enabled {
		if err := runCompatMigrateAutopilotJSONToStore(bc, autopilotCfg.ID, autopilotDir); err != nil {
			log.Fatal("failed to migrate autopilot JSON", err)
		}
	}

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM)
	select {
	case <-signalCh:
		log.Println("Shutting down...")
		shutdownFns = append(shutdownFns, srv.Shutdown)
	case err := <-autopilotErr:
		log.Fatalln("Fatal autopilot error:", err)
	}

	// Shut down the autopilot first, then the rest of the services in reverse order.
	ctx, cancel := context.WithTimeout(context.Background(), nodeCfg.shutdownTimeout)
	defer cancel()
	if autopilotShutdownFn != nil {
		if err := autopilotShutdownFn(ctx); err != nil {
			log.Fatalf("Failed to shut down autopilot: %v", err)
		}
	}
	for i := len(shutdownFns) - 1; i >= 0; i-- {
		if err := shutdownFns[i](ctx); err != nil {
			log.Fatalf("Shutdown function %v failed: %v", i+1, err)
		}
	}
	log.Println("Shutdown complete")
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

	return nil
}
