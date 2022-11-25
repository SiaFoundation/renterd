package main

import (
	"flag"
	"fmt"
	"log"
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

	c := node.NewCluster(*apiAddr, getAPIPassword(), *dir, createUIHandler(), getWalletKey())
	defer c.Close()

	if busCfg.Enabled {
		if err := c.CreateBus(busCfg.Bootstrap, busCfg.GatewayAddr); err != nil {
			log.Fatal("failed to create bus", err)
		}
		log.Println("bus: Listening on", busCfg.GatewayAddr)
	}
	if workerCfg.Enabled {
		if err := c.CreateWorker(); err != nil {
			log.Fatal("failed to create worker", err)
		}
	}
	if autopilotCfg.Enabled {
		if err := c.CreateAutopilot(autopilotCfg.LoopInterval); err != nil {
			log.Fatal("failed to create autopilot", err)
		}
	}
	log.Println("api: Listening on", c.APIAddress())

	go c.Serve()
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt)
	<-signalCh
	log.Println("Shutting down...")
}
