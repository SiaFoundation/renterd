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
	"time"

	"go.sia.tech/jape"
	"go.sia.tech/renterd/autopilot"
	"go.sia.tech/renterd/bus"
	"go.sia.tech/renterd/internal/consensus"
	"go.sia.tech/renterd/internal/node"
	"go.sia.tech/renterd/wallet"
	"go.sia.tech/renterd/worker"
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

func main() {
	log.SetFlags(0)

	var busCfg struct {
		remoteAddr  string
		apiPassword string
		node.BusConfig
	}
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
	flag.BoolVar(&busCfg.Bootstrap, "bus.bootstrap", true, "bootstrap the gateway and consensus modules")
	flag.StringVar(&busCfg.GatewayAddr, "bus.gatewayAddr", ":9981", "address to listen on for Sia peer connections")
	flag.StringVar(&workerCfg.remoteAddr, "worker.remoteAddr", "", "URL of remote worker service")
	flag.StringVar(&workerCfg.apiPassword, "worker.apiPassword", "", "API password for remote worker service")
	flag.BoolVar(&autopilotCfg.enabled, "autopilot.enabled", true, "enable the autopilot")
	flag.DurationVar(&autopilotCfg.Heartbeat, "autopilot.heartbeat", time.Minute, "interval at which autopilot loop runs")
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

	var wb worker.Bus
	var ab autopilot.Bus
	if busCfg.remoteAddr != "" {
		b := bus.NewClient(busCfg.remoteAddr, busCfg.apiPassword)
		wb = b
		ab = b
	} else {
		b, cleanup, err := node.NewBus(busCfg.BusConfig, *dir, getWalletKey())
		if err != nil {
			log.Fatal(err)
		}
		defer cleanup()
		log.Println("bus: Listening on", b.GatewayAddress())
		mux.sub["/api/bus"] = treeMux{h: auth(bus.NewServer(b))}
		panic("TODO: unify bus interfaces")
		// wb = b
		// ab = b
	}

	var aw autopilot.Worker
	if workerCfg.remoteAddr != "" {
		w := worker.NewClient(workerCfg.remoteAddr, workerCfg.apiPassword)
		aw = w
	} else {
		w, cleanup, err := node.NewWorker(workerCfg.WorkerConfig, wb, getWalletKey())
		if err != nil {
			log.Fatal(err)
		}
		defer cleanup()
		mux.sub["/api/worker"] = treeMux{h: auth(worker.NewServer(w))}
		panic("TODO: unify worker interfaces")
		// aw = w
	}

	autopilotErr := make(chan error)
	if autopilotCfg.enabled {
		a, cleanup, err := node.NewAutopilot(autopilotCfg.AutopilotConfig, ab, aw, *dir)
		if err != nil {
			log.Fatal(err)
		}
		defer cleanup()
		go func() { autopilotErr <- a.Run() }()
		mux.sub["/api/autopilot"] = treeMux{h: auth(autopilot.NewServer(a))}
	}

	srv := &http.Server{Handler: mux}
	go srv.Serve(l)
	log.Println("api: Listening on", l.Addr())

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
