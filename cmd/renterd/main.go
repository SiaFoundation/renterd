package main

import (
	"context"
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
	"go.sia.tech/renterd/bus"
	"go.sia.tech/renterd/internal/node"
	"go.sia.tech/renterd/internal/stores"
	"go.sia.tech/renterd/internal/tracing"
	"go.sia.tech/renterd/wallet"
	"go.sia.tech/renterd/worker"
	"golang.org/x/term"
	"gorm.io/gorm"
)

const (
	// accountRefillInterval is the amount of time between refills of ephemeral
	// accounts. If we conservatively assume that a good hosts charges 500 SC /
	// TiB, we can pay for about 2.2 GiB with 1 SC. Since we want to refill
	// ahead of time at 0.5 SC, that makes 1.1 GiB. Considering a 1 Gbps uplink
	// that is shared across 30 uploads, we upload at around 33 Mbps to each
	// host. That means uploading 1.1 GiB to drain 0.5 SC takes around 5
	// minutes.  That's why we assume 30 seconds to be more than frequent enough
	// to refill an account when it's due for another refill.
	defaultAccountRefillInterval = 30 * time.Second
)

var (
	// to be supplied at build time
	githash   = "?"
	builddate = "?"

	// fetched once, then cached
	apiPassword *string
	walletKey   *types.PrivateKey
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

func getWalletKey() types.PrivateKey {
	if walletKey == nil {
		phrase := os.Getenv("RENTERD_WALLET_SEED")
		if phrase != "" {
			fmt.Println("Using RENTERD_WALLET_SEED environment variable")
		} else {
			fmt.Print("Enter wallet seed: ")
			pw, err := term.ReadPassword(int(os.Stdin.Fd()))
			check("Could not read seed phrase:", err)
			fmt.Println()
			phrase = string(pw)
		}
		key, err := wallet.KeyFromPhrase(phrase)
		if err != nil {
			log.Fatal(err)
		}
		walletKey = &key
	}
	return *walletKey
}

type currencyVar types.Currency

func newCurrencyVar(c *types.Currency, d types.Currency) *currencyVar {
	*c = d
	return (*currencyVar)(c)
}

func (c *currencyVar) Set(s string) (err error) {
	*(*types.Currency)(c), err = types.ParseCurrency(s)
	return
}

func (c *currencyVar) String() string {
	return strings.Replace((*types.Currency)(c).String(), " ", "", -1)
}

func flagCurrencyVar(c *types.Currency, name string, d types.Currency, usage string) {
	flag.Var(newCurrencyVar(c, d), name, usage)
}

func getDBDialectorFromEnv() gorm.Dialector {
	uri, user, password, dbName := stores.DBConfigFromEnv()
	if uri == "" {
		return nil
	}
	return stores.NewMySQLConnection(user, password, uri, dbName)
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
	busCfg.PersistInterval = 10 * time.Minute
	busCfg.DBDialector = getDBDialectorFromEnv()

	var workerCfg struct {
		remoteAddr  string
		apiPassword string
		node.WorkerConfig
	}
	var autopilotCfg struct {
		enabled bool
		node.AutopilotConfig
	}

	apiAddr := flag.String("http", "localhost:9980", "address to serve API on")
	tracingEnabled := flag.Bool("tracing-enabled", false, "Enables tracing through OpenTelemetry. If RENTERD_TRACING_ENABLED is set, it overwrites the CLI flag's value. Tracing can be configured using the standard OpenTelemetry environment variables. https://github.com/open-telemetry/opentelemetry-specification/blob/v1.8.0/specification/protocol/exporter.md")
	dir := flag.String("dir", ".", "directory to store node state in")
	flag.StringVar(&busCfg.remoteAddr, "bus.remoteAddr", "", "URL of remote bus service")
	flag.StringVar(&busCfg.apiPassword, "bus.apiPassword", "", "API password for remote bus service")
	flag.BoolVar(&busCfg.Bootstrap, "bus.bootstrap", true, "bootstrap the gateway and consensus modules")
	flag.StringVar(&busCfg.GatewayAddr, "bus.gatewayAddr", ":9981", "address to listen on for Sia peer connections")
	flag.IntVar(&busCfg.MinShards, "bus.minShards", 10, "min amount of shards needed to reconstruct the slab")
	flag.IntVar(&busCfg.TotalShards, "bus.totalShards", 30, "total amount of shards for each slab")
	flagCurrencyVar(&busCfg.MaxRPCPrice, "bus.maxRPCPrice", types.Siacoins(1), "max allowed base price for RPCs")
	flagCurrencyVar(&busCfg.MaxContractPrice, "bus.maxContractPrice", types.Siacoins(1), "max allowed price to form a contract")
	flagCurrencyVar(&busCfg.MaxDownloadPrice, "bus.maxDownloadPrice", types.Siacoins(2500), "max allowed price to download one TiB")
	flagCurrencyVar(&busCfg.MaxUploadPrice, "bus.maxUploadPrice", types.Siacoins(2500), "max allowed price to upload one TiB")
	flagCurrencyVar(&busCfg.MaxStoragePrice, "bus.maxStoragePrice", types.Siacoins(1), "max allowed price to store one byte per block")
	flag.IntVar(&busCfg.HostBlockHeightLeeway, "bus.hostBlockHeightLeeway", 3, "amount of leeway given to host block height before it is considered gouging")
	flag.DurationVar(&workerCfg.BusFlushInterval, "worker.busFlushInterval", 5*time.Second, "time after which the worker flushes buffered data to bus for persisting")
	flag.StringVar(&workerCfg.WorkerConfig.ID, "worker.id", "worker", "unique identifier of worker")
	flag.StringVar(&workerCfg.remoteAddr, "worker.remoteAddr", "", "URL of remote worker service")
	flag.StringVar(&workerCfg.apiPassword, "worker.apiPassword", "", "API password for remote worker service")
	flag.DurationVar(&workerCfg.SessionReconnectTimeout, "worker.sessionReconnectTimeout", 10*time.Second, "the maximum of time reconnecting a session is allowed to take")
	flag.DurationVar(&workerCfg.SessionTTL, "worker.sessionTTL", 2*time.Minute, "the time a host session is valid for before reconnecting")
	flag.DurationVar(&workerCfg.DownloadSectorTimeout, "worker.downloadSectorTimeout", 3*time.Second, "timeout applied to sector downloads when downloading a slab")
	flag.DurationVar(&workerCfg.UploadSectorTimeout, "worker.uploadSectorTimeout", 5*time.Second, "timeout applied to sector uploads when uploading a slab")
	flag.DurationVar(&autopilotCfg.AccountsRefillInterval, "autopilot.accountRefillInterval", defaultAccountRefillInterval, "interval at which the autopilot checks the workers' accounts balance and refills them if necessary")
	flag.BoolVar(&autopilotCfg.enabled, "autopilot.enabled", true, "enable the autopilot")
	flag.DurationVar(&autopilotCfg.Heartbeat, "autopilot.heartbeat", 10*time.Minute, "interval at which autopilot loop runs")
	flag.Float64Var(&autopilotCfg.MigrationHealthCutoff, "autopilot.migrationHealthCutoff", 0.75, "health threshold below which slabs are migrated to new hosts")
	flag.DurationVar(&autopilotCfg.ScannerInterval, "autopilot.scannerInterval", 24*time.Hour, "interval at which hosts are scanned")
	flag.Uint64Var(&autopilotCfg.ScannerBatchSize, "autopilot.scannerBatchSize", 1000, "size of the batch with which hosts are scanned")
	flag.Uint64Var(&autopilotCfg.ScannerNumThreads, "autopilot.scannerNumThreads", 100, "number of threads that scan hosts")
	flag.DurationVar(&nodeCfg.shutdownTimeout, "node.shutdownTimeout", 5*time.Minute, "the timeout applied to the node shutdown")

	flag.Parse()

	log.Println("renterd v0.1.0")
	if flag.Arg(0) == "version" {
		log.Println("Commit:", githash)
		log.Println("Build Date:", builddate)
		return
	} else if flag.Arg(0) == "seed" {
		log.Println("Seed phrase:", wallet.NewSeedPhrase())
		return
	}

	var autopilotShutdownFn func(context.Context) error
	var shutdownFns []func(context.Context) error

	// Alternative way to enable tracing.
	if tracingStr := os.Getenv("RENTERD_TRACING_ENABLED"); tracingStr != "" {
		_, err := fmt.Sscan(tracingStr, tracingEnabled)
		if err != nil {
			log.Fatal("failed to parse RENTERD_TRACING_ENABLED")
		}
	}

	// Init tracing.
	if *tracingEnabled {
		shutdownFn, err := tracing.Init(workerCfg.ID)
		if err != nil {
			log.Fatal("failed to init tracing", err)
		}
		shutdownFns = append(shutdownFns, shutdownFn)
	}

	if busCfg.remoteAddr != "" && workerCfg.remoteAddr != "" && !autopilotCfg.enabled {
		log.Fatal("remote bus, remote worker, and no autopilot -- nothing to do!")
	}
	if err := busCfg.RedundancySettings.Validate(); err != nil {
		log.Fatal(err)
	}

	// create listener first, so that we know the actual apiAddr if the user
	// specifies port :0
	l, err := net.Listen("tcp", *apiAddr)
	if err != nil {
		log.Fatal(err)
	}
	shutdownFns = append(shutdownFns, func(_ context.Context) error { return l.Close() })
	*apiAddr = "http://" + l.Addr().String()

	auth := jape.BasicAuth(getAPIPassword())
	mux := treeMux{
		h:   createUIHandler(),
		sub: make(map[string]treeMux),
	}

	// Create logger.
	renterdLog := filepath.Join(*dir, "renterd.log")
	logger, closeFn, err := node.NewLogger(renterdLog)
	if err != nil {
		log.Fatal(err)
	}
	shutdownFns = append(shutdownFns, closeFn)

	busAddr, busPassword := busCfg.remoteAddr, busCfg.apiPassword
	if busAddr == "" {
		b, shutdownFn, err := node.NewBus(busCfg.BusConfig, *dir, getWalletKey(), logger)
		if err != nil {
			log.Fatal(err)
		}
		shutdownFns = append(shutdownFns, shutdownFn)

		mux.sub["/api/bus"] = treeMux{h: auth(b)}
		busAddr = *apiAddr + "/api/bus"
		busPassword = getAPIPassword()
	}
	bc := bus.NewClient(busAddr, busPassword)

	workerAddr, workerPassword := workerCfg.remoteAddr, workerCfg.apiPassword
	if workerAddr == "" {
		w, shutdownFn, err := node.NewWorker(workerCfg.WorkerConfig, bc, getWalletKey(), logger)
		if err != nil {
			log.Fatal(err)
		}
		shutdownFns = append(shutdownFns, shutdownFn)

		mux.sub["/api/worker"] = treeMux{h: auth(w)}
		workerAddr = *apiAddr + "/api/worker"
		workerPassword = getAPIPassword()
	}
	wc := worker.NewClient(workerAddr, workerPassword)

	autopilotErr := make(chan error, 1)
	if autopilotCfg.enabled {
		autopilotDir := filepath.Join(*dir, "autopilot")
		if err := os.MkdirAll(autopilotDir, 0700); err != nil {
			log.Fatal(err)
		}

		s, err := stores.NewJSONAutopilotStore(autopilotDir)
		if err != nil {
			log.Fatal(err)
		}

		ap, runFn, shutdownFn, err := node.NewAutopilot(autopilotCfg.AutopilotConfig, s, bc, wc, logger)
		if err != nil {
			log.Fatal(err)
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
		log.Fatal(err)
	}
	log.Println("bus: Listening on", syncerAddress)

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
			log.Fatal(err)
		}
	}
	for i := len(shutdownFns) - 1; i >= 0; i-- {
		if err := shutdownFns[i](ctx); err != nil {
			log.Fatal(err)
		}
	}
}
