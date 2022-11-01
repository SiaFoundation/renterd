package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"

	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/autopilot"
	"go.sia.tech/renterd/internal/consensus"
	"go.sia.tech/renterd/internal/stores"
	"go.sia.tech/renterd/wallet"
	"golang.org/x/crypto/blake2b"
	"golang.org/x/term"
)

var (
	// to be supplied at build time
	githash   = "?"
	builddate = "?"
)

func check(context string, err error) {
	if err != nil {
		log.Fatalf("%v: %v", context, err)
	}
}

func getAPIPassword() string {
	apiPassword := os.Getenv("RENTERD_API_PASSWORD")
	if apiPassword != "" {
		fmt.Println("Using RENTERD_API_PASSWORD environment variable.")
	} else {
		fmt.Print("Enter API password: ")
		pw, err := term.ReadPassword(int(os.Stdin.Fd()))
		fmt.Println()
		if err != nil {
			log.Fatal(err)
		}
		apiPassword = string(pw)
	}
	return apiPassword
}

func getWalletKey() consensus.PrivateKey {
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
	return key
}

func main() {
	log.SetFlags(0)
	gatewayAddr := flag.String("addr", ":0", "address to listen on for peer connections")
	apiAddr := flag.String("http", "localhost:9980", "address to serve API on")
	dir := flag.String("dir", ".", "directory to store node state in")
	stateless := flag.Bool("stateless", false, "run in stateless mode")
	bootstrap := flag.Bool("bootstrap", true, "bootstrap the gateway and consensus modules")
	flag.Parse()

	log.Println("renterd v0.1.0")
	if flag.Arg(0) == "version" {
		log.Println("Commit:", githash)
		log.Println("Build Date:", builddate)
		return
	}

	if *stateless {
		apiPassword := getAPIPassword()
		l, err := net.Listen("tcp", *apiAddr)
		if err != nil {
			log.Fatal(err)
		}
		defer l.Close()
		log.Println("api: Listening on", l.Addr())
		go startStatelessWeb(l, apiPassword)

		signalCh := make(chan os.Signal, 1)
		signal.Notify(signalCh, os.Interrupt)
		<-signalCh
		log.Println("Shutting down...")
		return
	}

	apiPassword := getAPIPassword()
	walletKey := getWalletKey()
	n, err := newNode(*gatewayAddr, *dir, *bootstrap, walletKey)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := n.Close(); err != nil {
			log.Println("WARN: error shutting down:", err)
		}
	}()
	log.Println("p2p: Listening on", n.g.Address())

	l, err := net.Listen("tcp", *apiAddr)
	if err != nil {
		log.Fatal(err)
	}
	defer l.Close()
	log.Println("api: Listening on", l.Addr())

	as, _, err := stores.NewJSONAutopilotStore("autopilot")
	if err != nil {
		log.Fatal(err)
	}
	autopilotKey := blake2b.Sum256(append([]byte("autopilot"), walletKey...))
	ap := autopilot.New(as, api.NewClient(l.Addr().String(), apiPassword), autopilotKey)
	defer ap.Stop()
	apErrChan := make(chan error)
	go func() { apErrChan <- ap.Run() }()
	go startWeb(l, n, ap, apiPassword)

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt)
	select {
	case <-signalCh:
		log.Println("Shutting down...")
	case err := <-apErrChan:
		log.Fatalln("Fatal autopilot error:", err)
	}
}
