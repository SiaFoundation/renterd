package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
)

const (
	apiPasswordEnvVar = "RENTERD_API_PASSWORD"
	configFileEnvVar  = "RENTERD_CONFIG_FILE"
	dataDirEnvVar     = "RENTERD_DATA_DIR"
	logFileEnvVar     = "RENTERD_LOG_FILE"
	seedEnvVar        = "RENTERD_SEED"
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
There are 4 commands:
  - version: prints the network as well as build information
  - config: builds a YAML config file through a series of prompts
  - seed: generates a new seed and prints the recovery phrase
  - sqlite backup <src> <dest>: backs up the sqlite database at a
    specified source path to the specified destination path
    (safe to use while renterd is running)

See the documentation (https://docs.sia.tech/) for more information and examples
on how to configure and use renterd.
`
)

func main() {
	log.SetFlags(0)

	// load config file
	configPath := tryLoadConfig()

	// default data directory
	cfg.Directory = defaultDataDirectory(cfg.Directory)

	// override config file with CLI flags and/or environment variables
	parseCLIFlags()
	parseEnvironmentVariables()

	// check network
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
		checkFatalError("invalid network settings", fmt.Errorf("unknown network '%s'", cfg.Network))
	}

	// NOTE: update the usage header when adding new commands
	if flag.Arg(0) == "version" {
		cmdVersion(network.Name)
		return
	} else if flag.Arg(0) == "seed" {
		cmdSeed()
		return
	} else if flag.Arg(0) == "config" {
		cmdBuildConfig(configPath)
		return
	} else if flag.Arg(0) == "sqlite" && flag.Arg(1) == "backup" &&
		flag.Arg(2) != "" && flag.Arg(3) != "" {
		cmdBackup()
		return
	} else if flag.Arg(0) != "" {
		flag.Usage()
		return
	}

	// sanitize the config
	checkFatalError(fmt.Sprintf("failed to sanitize config %q", configPath), sanitizeConfig())

	// create node
	node, err := newNode(cfg, configPath, network, genesis)
	checkFatalError("failed to create node", err)

	// start node
	checkFatalError("failed to run node", node.Run())

	// wait for interrupt signal
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM)
	<-signalCh

	// shut down the node
	checkFatalError("failed to shut down", node.Shutdown())

	os.Exit(0)
}

// checkFatalError prints an error message to stderr and exits with a 1 exit
// code. If err is nil, this is a no-op.
func checkFatalError(context string, err error) {
	if err == nil {
		return
	}
	os.Stderr.WriteString(fmt.Sprintf("%s: %s\n", context, err))
	os.Exit(1)
}
