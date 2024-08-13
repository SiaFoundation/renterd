package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
)

const (
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

func main() {
	log.SetFlags(0)

	// set usage
	flag.Usage = func() {
		log.Print(usageHeader)
		flag.PrintDefaults()
		log.Print(usageFooter)
	}

	// load the config
	cfg := loadConfig()

	// validate the network
	var network *consensus.Network
	var genesis types.Block
	switch cfg.Network {
	case "anagami":
		network, genesis = chain.TestnetAnagami()
	case "mainnet":
		network, genesis = chain.Mainnet()
	case "zen":
		network, genesis = chain.TestnetZen()
	default:
		log.Fatalf("unknown network '%s'", cfg.Network)
	}

	// NOTE: update the usage header when adding new commands
	if flag.Arg(0) == "version" {
		cmdVersion(network.Name)
		return
	} else if flag.Arg(0) == "seed" {
		cmdSeed()
		return
	} else if flag.Arg(0) == "config" {
		cmdBuildConfig(&cfg)
		return
	} else if flag.Arg(0) != "" {
		flag.Usage()
		return
	}

	// create node
	node, err := newNode(cfg, network, genesis)
	if err != nil {
		log.Fatal("failed to create node: " + err.Error())
	}

	// start node
	err = node.Run()
	if err != nil {
		log.Fatal("failed to run node: " + err.Error())
	}

	// wait for interrupt signal
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM)
	<-signalCh

	// shut down the node
	err = node.Shutdown()
	if err != nil {
		os.Exit(1)
		return
	}

	os.Exit(0)
}
