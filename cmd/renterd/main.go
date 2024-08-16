package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
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

	// load the config
	cfg, network, genesis, err := loadConfig()
	if err != nil {
		stdoutFatalError("failed to load config: " + err.Error())
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

	// sanitize the config
	if err := sanitizeConfig(&cfg); err != nil {
		stdoutFatalError("failed to sanitize config: " + err.Error())
	}

	// create node
	node, err := newNode(cfg, network, genesis)
	if err != nil {
		stdoutFatalError("failed to create node: " + err.Error())
	}

	// start node
	err = node.Run()
	if err != nil {
		stdoutFatalError("failed to run node: " + err.Error())
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
