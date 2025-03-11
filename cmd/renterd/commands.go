package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/renterd/v2/build"
	"go.sia.tech/renterd/v2/stores/sql/sqlite"
	"gopkg.in/yaml.v3"
)

func cmdBackup() {
	err := sqlite.Backup(context.Background(), flag.Arg(2), flag.Arg(3))
	checkFatalError("failed to backup sqlite database", err)
}

func cmdBuildConfig(fp string) {
	fmt.Println("renterd Configuration Wizard")
	fmt.Println("This wizard will help you configure renterd for the first time.")
	fmt.Println("You can always change these settings with the config command or by editing the config file.")

	if fp == "" {
		fp = configPath()
	}

	fmt.Println("")
	fmt.Printf("Config Location %q\n", fp)

	if _, err := os.Stat(fp); err == nil {
		if !promptYesNo(fmt.Sprintf("%q already exists. Would you like to overwrite it?", fp)) {
			return
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		checkFatalError("failed to check if config file exists", err)
	} else {
		// ensure the config directory exists
		checkFatalError("failed to create config directory", os.MkdirAll(filepath.Dir(fp), 0700))
	}

	fmt.Println("")
	setDataDirectory()

	fmt.Println("")
	setSeedPhrase()

	fmt.Println("")
	setAPIPassword()

	fmt.Println("")
	setS3Config()

	fmt.Println("")
	setAdvancedConfig()

	f, err := os.Create(fp)
	checkFatalError("Failed to create config file", err)
	defer f.Close()

	enc := yaml.NewEncoder(f)
	defer enc.Close()

	checkFatalError("Failed to encode config file", enc.Encode(cfg))
	checkFatalError("Failed to sync config file", f.Sync())
}

func cmdSeed() {
	var seed [32]byte
	phrase := wallet.NewSeedPhrase()
	checkFatalError("failed to derive seed from phrase", wallet.SeedFromPhrase(&seed, phrase))
	key := wallet.KeyFromSeed(&seed, 0)
	fmt.Println("Recovery Phrase:", phrase)
	fmt.Println("Address", types.StandardUnlockHash(key.PublicKey()))
}

func cmdVersion(network string) {
	fmt.Println("renterd", build.Version())
	fmt.Println("Network", network)
	fmt.Println("Commit:", build.Commit())
	fmt.Println("Build Date:", build.BuildTime())
}
