package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"time"

	"go.sia.tech/jape"
	"go.sia.tech/renterd/autopilot"
	"go.sia.tech/renterd/bus"
	"go.sia.tech/renterd/internal/consensus"
	"go.sia.tech/renterd/internal/node"
	"go.sia.tech/renterd/internal/stores"
	"go.sia.tech/renterd/wallet"
	"go.sia.tech/renterd/worker"
	"go.sia.tech/siad/types"
	"golang.org/x/term"
)

var (
	// to be supplied at build time
	githash   = "?"
	builddate = "?"

	// fetched once, then cached
	apiPassword *string
	walletKey   *consensus.PrivateKey
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

func getWalletKey() consensus.PrivateKey {
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

func (c *currencyVar) Set(s string) error {
	hastings, err := types.ParseCurrency(s)
	if err != nil {
		return errors.New("invalid currency format")
	}
	_, err = fmt.Sscan(hastings, (*types.Currency)(c))
	return err
}

func (c *currencyVar) String() string {
	return strings.Replace((*types.Currency)(c).HumanString(), " ", "", -1)
}

func flagCurrencyVar(c *types.Currency, name string, d types.Currency, usage string) {
	flag.Var(newCurrencyVar(c, d), name, usage)
}

func main() {
	log.SetFlags(0)

	var busCfg struct {
		remoteAddr  string
		apiPassword string
		node.BusConfig
	}
	busCfg.PersistInterval = 10 * time.Minute

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
	dir := flag.String("dir", ".", "directory to store node state in")
	flag.StringVar(&busCfg.remoteAddr, "bus.remoteAddr", "", "URL of remote bus service")
	flag.StringVar(&busCfg.apiPassword, "bus.apiPassword", "", "API password for remote bus service")
	flag.BoolVar(&busCfg.ApplyDefaultBlocklist, "bus.defaultBlocklist", true, "apply the default blocklist: 'siacentral.ddnsfree.com','siacentral.mooo.com',' 51.158.108.244'")
	flag.BoolVar(&busCfg.Bootstrap, "bus.bootstrap", true, "bootstrap the gateway and consensus modules")
	flag.StringVar(&busCfg.GatewayAddr, "bus.gatewayAddr", ":9981", "address to listen on for Sia peer connections")
	flag.Uint64Var(&busCfg.MinShards, "bus.minShards", 10, "min amount of shards needed to reconstruct the slab")
	flag.Uint64Var(&busCfg.TotalShards, "bus.totalShards", 30, "total amount of shards for each slab")
	flagCurrencyVar(&busCfg.MaxRPCPrice, "bus.maxRPCPrice", types.SiacoinPrecision, "max allowed base price for RPCs")
	flagCurrencyVar(&busCfg.MaxContractPrice, "bus.maxContractPrice", types.SiacoinPrecision, "max allowed price to form a contract")
	flagCurrencyVar(&busCfg.MaxDownloadPrice, "bus.maxDownloadPrice", types.SiacoinPrecision.Mul64(2500), "max allowed price to download one TiB")
	flagCurrencyVar(&busCfg.MaxUploadPrice, "bus.maxUploadPrice", types.SiacoinPrecision.Mul64(1000), "max allowed price to upload one TiB")
	flag.StringVar(&workerCfg.remoteAddr, "worker.remoteAddr", "", "URL of remote worker service")
	flag.StringVar(&workerCfg.apiPassword, "worker.apiPassword", "", "API password for remote worker service")
	flag.BoolVar(&autopilotCfg.enabled, "autopilot.enabled", true, "enable the autopilot")
	flag.DurationVar(&autopilotCfg.Heartbeat, "autopilot.heartbeat", time.Minute, "interval at which autopilot loop runs")
	flag.DurationVar(&autopilotCfg.ScannerInterval, "autopilot.scannerInterval", 10*time.Minute, "interval at which hosts are scanned")
	flag.Uint64Var(&autopilotCfg.ScannerBatchSize, "autopilot.scannerBatchSize", 1000, "size of the batch with which hosts are scanned")
	flag.Uint64Var(&autopilotCfg.ScannerNumThreads, "autopilot.scannerNumThreads", 100, "number of threads that scan hosts")

	flag.Parse()

	log.Println("renterd v0.1.0")
	if flag.Arg(0) == "version" {
		log.Println("Commit:", githash)
		log.Println("Build Date:", builddate)
		return
	}

	if busCfg.remoteAddr != "" && workerCfg.remoteAddr != "" && !autopilotCfg.enabled {
		log.Fatal("remote bus, remote worker, and no autopilot -- nothing to do!")
	}

	// create listener first, so that we know the actual apiAddr if the user
	// specifies port :0
	l, err := net.Listen("tcp", *apiAddr)
	if err != nil {
		log.Fatal(err)
	}
	defer l.Close()
	*apiAddr = "http://" + l.Addr().String()

	auth := jape.BasicAuth(getAPIPassword())
	mux := treeMux{
		h:   createUIHandler(),
		sub: make(map[string]treeMux),
	}

	busAddr, busPassword := busCfg.remoteAddr, busCfg.apiPassword
	if busAddr == "" {
		b, cleanup, err := node.NewBus(busCfg.BusConfig, *dir, getWalletKey())
		if err != nil {
			log.Fatal(err)
		}
		defer cleanup()
		mux.sub["/api/bus"] = treeMux{h: auth(b)}
		busAddr = *apiAddr + "/api/bus"
		busPassword = getAPIPassword()
	}
	bc := bus.NewClient(busAddr, busPassword)

	workerAddr, workerPassword := workerCfg.remoteAddr, workerCfg.apiPassword
	if workerAddr == "" {
		w, cleanup, err := node.NewWorker(workerCfg.WorkerConfig, bc, getWalletKey())
		if err != nil {
			log.Fatal(err)
		}
		defer cleanup()
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

		autopilotLog := filepath.Join(autopilotDir, "autopilot.log")
		l, closeFn, err := node.NewLogger(autopilotLog)
		if err != nil {
			log.Fatal(err)
		}

		s, err := stores.NewJSONAutopilotStore(autopilotDir)
		if err != nil {
			log.Fatal(err)
		}

		ap, cleanup, err := node.NewAutopilot(autopilotCfg.AutopilotConfig, s, bc, wc, l)
		if err != nil {
			log.Fatal(err)
		}
		defer func() {
			cleanup()
			_ = l.Sync() // ignore error
			closeFn()
		}()

		go func() { autopilotErr <- ap.Run() }()
		mux.sub["/api/autopilot"] = treeMux{h: auth(autopilot.NewServer(ap))}
	}

	srv := &http.Server{Handler: mux}
	go srv.Serve(l)
	log.Println("api: Listening on", l.Addr())

	syncerAddress, err := bc.SyncerAddress()
	if err != nil {
		log.Fatal(err)
	}
	log.Println("bus: Listening on", syncerAddress)

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt)
	select {
	case <-signalCh:
		log.Println("Shutting down...")
		srv.Shutdown(context.Background())
	case err := <-autopilotErr:
		log.Fatalln("Fatal autopilot error:", err)
	}
}
