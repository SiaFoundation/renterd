package main

import (
	"fmt"
	"os"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/renterd/build"
	"gopkg.in/yaml.v3"
)

func cmdBuildConfig() {
	if _, err := os.Stat("renterd.yml"); err == nil {
		if !promptYesNo("renterd.yml already exists. Would you like to overwrite it?") {
			return
		}
	}

	fmt.Println("")
	if cfg.Seed != "" {
		fmt.Println(wrapANSI("\033[33m", "A wallet seed phrase is already set.", "\033[0m"))
		fmt.Println("If you change your wallet seed phrase, your renter will not be able to access Siacoin associated with this wallet.")
		fmt.Println("Ensure that you have backed up your wallet seed phrase before continuing.")
		if promptYesNo("Would you like to change your wallet seed phrase?") {
			setSeedPhrase()
		}
	} else {
		setSeedPhrase()
	}

	fmt.Println("")
	if cfg.HTTP.Password != "" {
		fmt.Println(wrapANSI("\033[33m", "An admin password is already set.", "\033[0m"))
		fmt.Println("If you change your admin password, you will need to update any scripts or applications that use the admin API.")
		if promptYesNo("Would you like to change your admin password?") {
			setAPIPassword()
		}
	} else {
		setAPIPassword()
	}

	fmt.Println("")
	setS3Config()

	fmt.Println("")
	setAdvancedConfig()

	// write the config file
	configPath := "renterd.yml"
	if str := os.Getenv("RENTERD_CONFIG_FILE"); str != "" {
		configPath = str
	}

	f, err := os.Create(configPath)
	if err != nil {
		stdoutFatalError("Failed to create config file: " + err.Error())
		return
	}
	defer f.Close()

	enc := yaml.NewEncoder(f)
	if err := enc.Encode(cfg); err != nil {
		stdoutFatalError("Failed to encode config file: " + err.Error())
		return
	}
}

func cmdSeed() {
	var seed [32]byte
	phrase := wallet.NewSeedPhrase()
	if err := wallet.SeedFromPhrase(&seed, phrase); err != nil {
		println(err.Error())
		os.Exit(1)
	}
	key := wallet.KeyFromSeed(&seed, 0)
	fmt.Println("Recovery Phrase:", phrase)
	fmt.Println("Address", types.StandardUnlockHash(key.PublicKey()))
}

func cmdVersion() {
	fmt.Println("renterd", build.Version())
	fmt.Println("Network", build.NetworkName())
	fmt.Println("Commit:", build.Commit())
	fmt.Println("Build Date:", build.BuildTime())
}
