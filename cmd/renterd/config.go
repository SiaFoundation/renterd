package main

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"

	"go.sia.tech/core/types"
	"go.sia.tech/core/wallet"
	"golang.org/x/term"
	"gopkg.in/yaml.v3"
	"lukechampine.com/frand"
)

var enableANSI = runtime.GOOS != "windows"

// readPasswordInput reads a password from stdin.
func readPasswordInput(context string) (string, error) {
	fmt.Printf("%s: ", context)
	input, err := term.ReadPassword(int(os.Stdin.Fd()))
	fmt.Println()
	return string(input), err
}

func readInput(context string) (string, error) {
	fmt.Printf("%s: ", context)
	r := bufio.NewReader(os.Stdin)
	input, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	fmt.Println()
	return strings.TrimSpace(input), nil
}

// wrapANSI wraps the output in ANSI escape codes if enabled.
func wrapANSI(prefix, output, suffix string) string {
	if enableANSI {
		return prefix + output + suffix
	}
	return output
}

// stdoutFatalError prints an error message to stdout and exits with a 1 exit code.
func stdoutFatalError(msg string) {
	stdoutError(msg)
	os.Exit(1)
}

// stdoutError prints an error message to stdout
func stdoutError(msg string) {
	if enableANSI {
		fmt.Println(wrapANSI("\033[31m", msg, "\033[0m"))
	} else {
		fmt.Println(msg)
	}
}

func setListenAddress(context string, value *string) {
	// will continue to prompt until a valid value is entered
	for {
		input, err := readInput(fmt.Sprintf("%s (currently %q)", context, *value))
		if err != nil {
			stdoutFatalError(fmt.Sprintf("Could not read %s port: %s", context, err.Error()))
			return
		}

		if input == "" {
			return
		}

		host, port, err := net.SplitHostPort(input)
		if err != nil {
			stdoutError(fmt.Sprintf("Invalid %s port %q: %s", context, input, err.Error()))
			continue
		}

		n, err := strconv.Atoi(port)
		if err != nil {
			stdoutError(fmt.Sprintf("Invalid %s port %q: %s", context, input, err.Error()))
			continue
		} else if n < 0 || n > 65535 {
			stdoutError(fmt.Sprintf("Invalid %s port %q: must be between 0 and 65535", context, input))
			continue
		}
		*value = net.JoinHostPort(host, port)
		return
	}
}

// setSeedPhrase prompts the user to enter a seed phrase if one is not already
// set via environment variable or config file.
func setSeedPhrase() {
	// retry until a valid seed phrase is entered
	for {
		fmt.Println("")
		fmt.Println("Type in your 12-word seed phrase and press enter. If you do not have a seed phrase yet, type 'seed' to generate one.")
		phrase, err := readPasswordInput("Enter seed phrase")
		if err != nil {
			stdoutFatalError("Could not read seed phrase: " + err.Error())
		}

		if strings.ToLower(strings.TrimSpace(phrase)) == "seed" {
			// generate a new seed phrase
			var seed [32]byte
			phrase = wallet.NewSeedPhrase()
			if err := wallet.SeedFromPhrase(&seed, phrase); err != nil {
				panic(err)
			}
			key := wallet.KeyFromSeed(&seed, 0)
			fmt.Println("")
			fmt.Println("A new seed phrase has been generated below. " + wrapANSI("\033[1m", "Write it down and keep it safe.", "\033[0m"))
			fmt.Println("Your seed phrase is the only way to recover your Siacoin. If you lose your seed phrase, you will also lose your Siacoin.")
			fmt.Println("")
			fmt.Println(wrapANSI("\033[34;1m", "Seed Phrase:", "\033[0m"), phrase)
			fmt.Println(wrapANSI("\033[34;1m", "Wallet Address:", "\033[0m"), types.StandardUnlockHash(key.PublicKey()))

			// confirm seed phrase
			for {
				fmt.Println("")
				fmt.Println(wrapANSI("\033[1m", "Please confirm your seed phrase to continue.", "\033[0m"))
				confirmPhrase, err := readPasswordInput("Enter seed phrase")
				if err != nil {
					stdoutFatalError("Could not read seed phrase: " + err.Error())
				} else if confirmPhrase == phrase {
					cfg.Seed = phrase
					return
				}

				fmt.Println(wrapANSI("\033[31m", "Seed phrases do not match!", "\033[0m"))
				fmt.Println("You entered:", confirmPhrase)
				fmt.Println("Actual phrase:", phrase)
			}
		}

		var seed [32]byte
		err = wallet.SeedFromPhrase(&seed, phrase)
		if err == nil {
			// valid seed phrase
			cfg.Seed = phrase
			return
		}
		fmt.Println(wrapANSI("\033[31m", "Invalid seed phrase:", "\033[0m"), err)
		fmt.Println("You entered:", phrase)
	}
}

// setAPIPassword prompts the user to enter an API password if one is not
// already set via environment variable or config file.
func setAPIPassword() {
	// retry until a valid API password is entered
	for {
		fmt.Println("Please choose a password for the renterd admin UI.")
		fmt.Println("This password will be required to access the admin UI in your web browser.")
		fmt.Println("(The password must be at least 4 characters.)")
		var err error
		cfg.HTTP.Password, err = readPasswordInput("Enter password")
		if err != nil {
			stdoutFatalError("Could not read password:" + err.Error())
		}

		if len(cfg.HTTP.Password) >= 4 {
			break
		}

		fmt.Println(wrapANSI("\033[31m", "Password must be at least 4 characters!", "\033[0m"))
		fmt.Println("")
	}
}

func setAdvancedConfig() {
	input, err := readInput("Would you like to configure advanced settings? (yes/no)")
	if err != nil {
		stdoutFatalError("Could not read input: " + err.Error())
	} else if !strings.EqualFold(input, "yes") {
		return
	}

	fmt.Println("")
	fmt.Println("Advanced settings are used to configure the renter's behavior.")
	fmt.Println("You can leave these settings blank to use the defaults.")
	fmt.Println("")

	// http address
	fmt.Println("The HTTP address is used to serve the renter's admin API.")
	fmt.Println("The admin API is used to configure the renter.")
	fmt.Println("It should not be exposed to the public internet without setting up a reverse proxy.")
	setListenAddress("HTTP Address", &cfg.HTTP.Address)

	// gateway address
	fmt.Println("")
	fmt.Println("The gateway address is used to exchange blocks with other nodes in the Sia network")
	fmt.Println("It should be exposed publicly to improve the renter's connectivity.")
	setListenAddress("Gateway Address", &cfg.Bus.GatewayAddr)
}

func setS3Config() {
	input, err := readInput("Would you like to configure S3 settings? (yes/no)")
	if err != nil {
		stdoutFatalError("Could not read input: " + err.Error())
	} else if !strings.EqualFold(input, "yes") {
		return
	}

	fmt.Println("")
	fmt.Println("S3 settings are used to configure renterd's S3-compatible gateway.")
	fmt.Println("You can leave these settings blank to use the defaults.")
	fmt.Println("")

	// s3 address
	fmt.Println("The S3 address is used to serve the renter's S3 API.")
	fmt.Println("The S3 API provides an S3-compatible gateway for uploading data to Sia.")
	fmt.Println("It should not be exposed to the public internet without setting up a reverse proxy.")
	setListenAddress("S3 Address", &cfg.S3.Address)

	// s3 access key
	if len(cfg.S3.KeypairsV4) != 0 {
		fmt.Println("")
		fmt.Println("A S3 keypair has already been created.")
		fmt.Println("If you change your S3 key pair, you will need to update any scripts or applications that use the S3 API.")
		input, err := readInput("Would you like to change your S3 key pair? (yes/no)")
		if err != nil {
			stdoutFatalError("Could not read input: " + err.Error())
		} else if strings.EqualFold(input, "no") {
			return
		}
	}

	cfg.S3.KeypairsV4 = make(map[string]string)

	fmt.Println("")
	fmt.Println("Would you like to automatically generate a new S3 key pair or enter your own? (auto/manual)")
	input, err = readInput("Enter auto or manual")
	if err != nil {
		stdoutFatalError("Could not read input: " + err.Error())
	} else if strings.EqualFold(input, "auto") {
		// generate a new key pair
		accessKey := hex.EncodeToString(frand.Bytes(20))
		secretKey := hex.EncodeToString(frand.Bytes(20))
		cfg.S3.KeypairsV4[accessKey] = secretKey
		fmt.Println("")
		fmt.Println("A new S3 key pair has been generated below.")
		fmt.Println(wrapANSI("\033[34;1m", "Access Key:", "\033[0m"), accessKey)
		fmt.Println(wrapANSI("\033[34;1m", "Secret Key:", "\033[0m"), secretKey)
		fmt.Println("")
		return
	}

	var accessKey, secretKey string
	for {
		fmt.Println("")
		fmt.Println("Enter your S3 access key. It must between 16 and 128 characters long.")
		accessKey, err = readInput("Enter access key")
		if err != nil {
			stdoutFatalError("Could not read access key: " + err.Error())
		} else if len(accessKey) >= 16 && len(accessKey) <= 128 {
			break
		}

		fmt.Println(wrapANSI("\033[31m", "Access key must be between 16 and 128 characters!", "\033[0m"))
	}

	for {
		fmt.Println("")
		fmt.Println("Enter your S3 secret key. It must be 40 characters long.")
		secretKey, err = readInput("Enter secret key")
		if err != nil {
			stdoutFatalError("Could not read secret key: " + err.Error())
		} else if len(secretKey) != 40 {
			break
		}

		fmt.Println(wrapANSI("\033[31m", "Secret key must be be 40 characters!", "\033[0m"))
	}
}

func cmdBuildConfig() {
	if _, err := os.Stat("renterd.yml"); err == nil {
		input, err := readInput("renterd.yml already exists. Would you like to overwrite it? (yes/no)")
		if err != nil {
			stdoutFatalError("Could not read input: " + err.Error())
			return
		} else if !strings.EqualFold(input, "yes") {
			return
		}
	}

	if cfg.Seed != "" {
		fmt.Println("")
		fmt.Println(wrapANSI("\033[33m", "A wallet seed phrase is already set.", "\033[0m"))
		fmt.Println("If you change your wallet seed phrase, your renter will not be able to access Siacoin associated with this wallet.")
		fmt.Println("Ensure that you have backed up your wallet seed phrase before continuing.")
		input, err := readInput("Would you like to change your wallet seed phrase? (yes/no)")
		if err != nil {
			stdoutFatalError("Could not read input: " + err.Error())
		} else if strings.EqualFold(input, "yes") {
			setSeedPhrase()
			fmt.Println("")
		}
	} else {
		setSeedPhrase()
		fmt.Println("")
	}

	if cfg.HTTP.Password != "" {
		fmt.Println("")
		fmt.Println(wrapANSI("\033[33m", "An admin password is already set.", "\033[0m"))
		fmt.Println("If you change your admin password, you will need to update any scripts or applications that use the admin API.")
		input, err := readInput("Would you like to change your admin password? (yes/no)")
		if err != nil {
			stdoutFatalError("Could not read input: " + err.Error())
		} else if strings.EqualFold(input, "yes") {
			setAPIPassword()
			fmt.Println("")
		}
	} else {
		setAPIPassword()
		fmt.Println("")
	}

	setS3Config()
	setAdvancedConfig()

	// write the config file
	f, err := os.Create("renterd.yml")
	if err != nil {
		stdoutFatalError("failed to create config file: " + err.Error())
		return
	}
	defer f.Close()

	enc := yaml.NewEncoder(f)
	if err := enc.Encode(cfg); err != nil {
		stdoutFatalError("failed to encode config file: " + err.Error())
		return
	}
}
