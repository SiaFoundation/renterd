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

	"go.sia.tech/renterd/internal/consensus"
	"go.sia.tech/renterd/node"
	"go.sia.tech/renterd/wallet"
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

	var workerCfg node.WorkerConfig
	var busCfg node.BusConfig
	var autopilotCfg node.AutopilotConfig

	apiAddr := flag.String("http", "localhost:9980", "address to serve API on")
	dir := flag.String("dir", ".", "directory to store node state in")
	flag.BoolVar(&workerCfg.Enabled, "worker.enabled", true, "enable the worker API")
	flag.BoolVar(&busCfg.Enabled, "bus.enabled", true, "enable the bus API")
	flag.BoolVar(&busCfg.Bootstrap, "bus.bootstrap", true, "bootstrap the gateway and consensus modules")
	flag.StringVar(&busCfg.GatewayAddr, "bus.gatewayAddr", ":9981", "address to listen on for Sia peer connections")
	flag.BoolVar(&autopilotCfg.Enabled, "autopilot.enabled", true, "enable the autopilot API")
	flag.DurationVar(&autopilotCfg.LoopInterval, "autopilot.loopInterval", time.Minute, "with which the autopilot loop is triggered")
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

	// All components use the same API password.
	workerCfg.APIPassword = getAPIPassword()
	busCfg.APIPassword = getAPIPassword()
	autopilotCfg.APIPassword = getAPIPassword()

	workerCfg.BusPassword = getAPIPassword()
	autopilotCfg.BusPassword = getAPIPassword()
	autopilotCfg.WorkerPassword = getAPIPassword()

	// All components use the same API address.
	*apiAddr = "http://" + l.Addr().String()
	workerCfg.BusAddr = *apiAddr
	busCfg.GatewayAddr = *apiAddr
	autopilotCfg.BusAddr = *apiAddr
	autopilotCfg.WorkerAddr = *apiAddr

	// All components use the same dir.
	workerCfg.Dir = *dir
	busCfg.Dir = *dir
	autopilotCfg.Dir = *dir

	mux := node.TreeMux{
		H:   createUIHandler(),
		Sub: make(map[string]http.Handler),
	}

	node, cleanup, err := node.NewNode(busCfg, workerCfg, autopilotCfg, getWalletKey())
	if err != nil {
		log.Fatal(err)
	}
	defer cleanup()

	if node.BusSrv != nil {
		log.Println("bus: Listening on", node.Bus.GatewayAddress())
		mux.Sub["/api/store"] = node.BusSrv
	}
	if node.WorkerSrv != nil {
		mux.Sub["/api/worker"] = node.WorkerSrv
	}
	if node.AutopilotSrv != nil {
		mux.Sub["/api/autopilot"] = node.AutopilotSrv
		go func() {
			err := node.Autopilot.Run()
			if err != nil {
				log.Fatalln("Fatal autopilot error:", err)
			}
		}()
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
