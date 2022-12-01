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

	var workerCfg workerConfig
	var busCfg busConfig
	var autopilotCfg autopilotConfig

	apiAddr := flag.String("http", "localhost:9980", "address to serve API on")
	dir := flag.String("dir", ".", "directory to store node state in")
	flag.BoolVar(&workerCfg.enabled, "worker.enabled", true, "enable the worker API")
	flag.BoolVar(&busCfg.enabled, "bus.enabled", true, "enable the bus API")
	flag.BoolVar(&busCfg.bootstrap, "bus.bootstrap", true, "bootstrap the gateway and consensus modules")
	flag.StringVar(&busCfg.gatewayAddr, "bus.gatewayAddr", ":9981", "address to listen on for Sia peer connections")
	flag.BoolVar(&autopilotCfg.enabled, "autopilot.enabled", true, "enable the autopilot API")
	flag.DurationVar(&autopilotCfg.heartbeat, "autopilot.heartbeat", time.Minute, "interval at which autopilot loop runs")
	flag.Parse()

	log.Println("renterd v0.1.0")
	if flag.Arg(0) == "version" {
		log.Println("Commit:", githash)
		log.Println("Build Date:", builddate)
		return
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

	if busCfg.enabled {
		b, cleanup, err := newBus(busCfg, *dir, getWalletKey())
		if err != nil {
			log.Fatal(err)
		}
		defer cleanup()
		log.Println("bus: Listening on", b.GatewayAddress())
		mux.sub["/api/store"] = treeMux{h: auth(bus.NewServer(b))}
		autopilotCfg.busAddr = *apiAddr + "/bus/"
		autopilotCfg.busPassword = getAPIPassword()
	}
	if workerCfg.enabled {
		w, cleanup, err := newWorker(workerCfg, getWalletKey())
		if err != nil {
			log.Fatal(err)
		}
		defer cleanup()
		mux.sub["/api/worker"] = treeMux{h: auth(worker.NewServer(w))}
		autopilotCfg.workerAddr = *apiAddr + "/worker/"
		autopilotCfg.workerPassword = getAPIPassword()
	}
	if autopilotCfg.enabled {
		a, cleanup, err := newAutopilot(autopilotCfg, *dir)
		if err != nil {
			log.Fatal(err)
		}
		defer cleanup()
		go func() {
			err := a.Run()
			if err != nil {
				log.Fatalln("Fatal autopilot error:", err)
			}
		}()

		mux.sub["/api/autopilot"] = treeMux{h: auth(autopilot.NewServer(a))}
	}

	srv := &http.Server{Handler: mux}
	go srv.Serve(l)
	log.Println("api: Listening on", l.Addr())

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt)
	<-signalCh
	log.Println("Shutting down...")
	srv.Shutdown(context.Background())
}
