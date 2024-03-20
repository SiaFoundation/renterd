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
	"go.sia.tech/coreutils/wallet"
	"golang.org/x/term"
	"gopkg.in/yaml.v3"
	"lukechampine.com/frand"
)

var enableANSI = runtime.GOOS != "windows"

// readPasswordInput reads a password from stdin.
func readPasswordInput(context string) string {
	fmt.Printf("%s: ", context)
	input, err := term.ReadPassword(int(os.Stdin.Fd()))
	if err != nil {
		stdoutFatalError("Could not read input: " + err.Error())
	}
	fmt.Println("")
	return string(input)
}

func readInput(context string) string {
	fmt.Printf("%s: ", context)
	r := bufio.NewReader(os.Stdin)
	input, err := r.ReadString('\n')
	if err != nil {
		stdoutFatalError("Could not read input: " + err.Error())
	}
	return strings.TrimSpace(input)
}

// wrapANSI wraps the output in ANSI escape codes if enabled.
// nolint: unparam
func wrapANSI(prefix, output, suffix string) string {
	if enableANSI {
		return prefix + output + suffix
	}
	return output
}

func humanList(s []string, sep string) string {
	if len(s) == 0 {
		return ""
	} else if len(s) == 1 {
		return fmt.Sprintf(`%q`, s[0])
	} else if len(s) == 2 {
		return fmt.Sprintf(`%q %s %q`, s[0], sep, s[1])
	}

	var sb strings.Builder
	for i, v := range s {
		if i != 0 {
			sb.WriteString(", ")
		}
		if i == len(s)-1 {
			sb.WriteString("or ")
		}
		sb.WriteString(`"`)
		sb.WriteString(v)
		sb.WriteString(`"`)
	}
	return sb.String()
}

func promptQuestion(question string, answers []string) string {
	for {
		input := readInput(fmt.Sprintf("%s (%s)", question, strings.Join(answers, "/")))
		for _, answer := range answers {
			if strings.EqualFold(input, answer) {
				return answer
			}
		}
		fmt.Println(wrapANSI("\033[31m", fmt.Sprintf("Answer must be %s", humanList(answers, "or")), "\033[0m"))
	}
}

func promptYesNo(question string) bool {
	answer := promptQuestion(question, []string{"yes", "no"})
	return strings.EqualFold(answer, "yes")
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

func setListenAddress(context string, value *string, allowEmpty bool) {
	// will continue to prompt until a valid value is entered
	for {
		input := readInput(fmt.Sprintf("%s (currently %q)", context, *value))
		if input == "" {
			if allowEmpty {
				return
			}
			stdoutError(fmt.Sprintf("Invalid %s %q: must not be empty", context, input))
			continue
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
		phrase := readPasswordInput("Enter seed phrase")

		if strings.ToLower(strings.TrimSpace(phrase)) == "seed" {
			// generate a new seed phrase
			var seed [32]byte
			phrase = wallet.NewSeedPhrase()
			if err := wallet.SeedFromPhrase(&seed, phrase); err != nil {
				panic(err)
			}
			key := wallet.KeyFromSeed(&seed, 0)
			fmt.Println("")
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

				confirmPhrase := readPasswordInput("Enter seed phrase")
				if confirmPhrase == phrase {
					cfg.Seed = phrase
					return
				}

				fmt.Println(wrapANSI("\033[31m", "Seed phrases do not match!", "\033[0m"))
				fmt.Println("You entered:", confirmPhrase)
				fmt.Println("Actual phrase:", phrase)
			}
		}

		var seed [32]byte
		if err := wallet.SeedFromPhrase(&seed, phrase); err != nil {
			// invalid seed phrase, retry
			fmt.Println(wrapANSI("\033[31m", "Invalid seed phrase:", "\033[0m"), err)
			fmt.Println("You entered:", phrase)
			continue
		}

		// valid seed phrase
		cfg.Seed = phrase
		return
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

		cfg.HTTP.Password = readPasswordInput("Enter password")
		if len(cfg.HTTP.Password) >= 4 {
			break
		}

		// invalid password, retry
		fmt.Println(wrapANSI("\033[31m", "Password must be at least 4 characters!", "\033[0m"))
		fmt.Println("")
	}
}

func setAdvancedConfig() {
	if !promptYesNo("Would you like to configure advanced settings?") {
		return
	}

	fmt.Println("")
	fmt.Println("Advanced settings are used to configure the renter's behavior.")
	fmt.Println("You can leave these settings blank to use the defaults.")

	// http address
	fmt.Println("")
	fmt.Println("The HTTP address is used to serve the renter's admin API.")
	fmt.Println("The admin API is used to configure the renter.")
	fmt.Println("It should not be exposed to the public internet without setting up a reverse proxy.")
	setListenAddress("HTTP Address", &cfg.HTTP.Address, true)

	// gateway address
	fmt.Println("")
	fmt.Println("The gateway address is used to exchange blocks with other nodes in the Sia network")
	fmt.Println("It should be exposed publicly to improve the renter's connectivity.")
	setListenAddress("Gateway Address", &cfg.Bus.GatewayAddr, true)

	// database
	fmt.Println("")
	fmt.Println("The database is used to store the renter's metadata.")
	fmt.Println("The embedded SQLite database is recommended for small (< 50TB), single-user setups. Choose this for the easiest setup.")
	fmt.Println("MySQL database is recommended for larger (> 50TB) or multi-user setups. MySQL requires a separate MySQL server to connect to.")
	setStoreConfig()
}

func setStoreConfig() {
	store := promptQuestion("Which data store would you like to use?", []string{"mysql", "sqlite"})
	switch store {
	case "mysql":
		fmt.Println("")
		fmt.Println("The MySQL database is used to store the renter metadata.")
		fmt.Println("You will need to set up a MySQL server to connect to.")
		fmt.Println("")
		fmt.Println("You will also need to create two database")
		fmt.Println(" - The first database will be used to store the object metadata.")
		fmt.Println(" - The second database will be used to store metrics.")
		fmt.Println("")
		setListenAddress("MySQL address", &cfg.Database.MySQL.URI, false)

		cfg.Database.MySQL.User = readInput("MySQL username")
		cfg.Database.MySQL.Password = readPasswordInput("MySQL password")
		objectDB := readInput("Object database name (default: renterd)")
		if objectDB != "" {
			cfg.Database.MySQL.Database = objectDB
		}
		metricsDB := readInput("Metrics database name (default: renterd_metrics)")
		if metricsDB != "" {
			cfg.Database.MySQL.MetricsDatabase = metricsDB
		}
	default:
		return
	}
}

func setS3Config() {
	if !promptYesNo("Would you like to configure S3 settings?") {
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
	setListenAddress("S3 Address", &cfg.S3.Address, true)

	// s3 access key
	if len(cfg.S3.KeypairsV4) != 0 {
		fmt.Println("")
		fmt.Println("A S3 keypair has already been created.")
		fmt.Println("If you change your S3 key pair, you will need to update any scripts or applications that use the S3 API.")
		if !promptYesNo("Would you like to change your S3 key pair?") {
			return
		}
	}

	cfg.S3.KeypairsV4 = make(map[string]string)

	fmt.Println("")
	answer := promptQuestion("Would you like to automatically generate a new S3 key pair or set your own?", []string{"auto", "manual"})
	if strings.EqualFold(answer, "auto") {
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
		accessKey = readInput("Enter access key")
		if len(accessKey) >= 16 && len(accessKey) <= 128 {
			break
		}
		fmt.Println(wrapANSI("\033[31m", "Access key must be between 16 and 128 characters!", "\033[0m"))
	}

	for {
		fmt.Println("")
		fmt.Println("Enter your S3 secret key. It must be 40 characters long.")
		secretKey = readInput("Enter secret key")
		if len(secretKey) == 40 {
			break
		}
		fmt.Println(wrapANSI("\033[31m", "Secret key must be be 40 characters!", "\033[0m"))
	}

	cfg.S3.KeypairsV4[accessKey] = secretKey
}

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
