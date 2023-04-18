# Renterd Workshop


Welcome to the renterd workshop, in this workshop you will learn about the Sia Network and how to leverage the `renterd` software to store data on our decentralised cloud storage network called Sia.

:::warning
This page is WIP and will be continuously updated as the `renterd` software evolves
:::

## Introduction

### The Sia Network

The Sia Network is a decentralized cloud storage platform that aims to provide a more secure, private, and affordable alternative to traditional cloud storage services like Amazon S3, Google Cloud, and Microsoft Azure. Launched in 2015 by Nebulous Inc., Sia leverages blockchain technology to create a distributed marketplace where users can rent out their unused hard drive space to others in exchange for Siacoin, the platform's native cryptocurrency.

### The Renter Daemon

The Renter Daemon, or what we call `renterd`, is a piece of software that is meant to continuously run as a background process on your machine. It serves an http API that allows to upload and download files to and from hosts on the Sia network.

In order to perform all of its tasks, it needs to:

- have access to a fully synced consensus set that represents the Sia blockchain
- have access to a (testnet) wallet that is funded with Siacoins
- be properly configured for your particular storage needs

In this workshop we will guide you through the process of setting up a fully working `renterd` node, as well as preconfigure it so you can store files on the Sia network. Note that we will use the Zen Testnet, which is a test network that works exactly the same way as the actual Sia network.

### Useful Links

- [Sia Website](https://sia.tech/learn)
- [Renterd API docs](https://api.sia.tech/renterd)
- [Zen Testnet](https://zen.sia.tech/)

## Getting Started

In this section we walk you through setting up and running an instance of `renterd`.

### Step 0: Prerequisites

- Git:

Git is a tool used to manage your project's source code.
To install, follow the instruction [available here](https://gist.github.com/derhuerst/1b15ff4652a867391f03) or try [Github Desktop](https://desktop.github.com/).

- Golang:

Golang is the language in which the renter software is developed. If you want to build the binary manually you'll need to have Go installed, following the instructions [available here](https://go.dev/doc/install)

### Step 1: Install `renterd`

The first thing to do is to install the renter daemon. There are currently two ways to do this:

- build from source
- download the binary

:::warning
At this time `renterd` binaries are not yet available on the website. But you can download recent binaries from the [build artifacts](https://github.com/SiaFoundation/renterd/suites/12287852932/artifacts/650990876) on GitHub.

You can find all build artifacts [here](https://github.com/SiaFoundation/renterd/actions/workflows/publish%20-%20zen.yml). The "Publish" tab contains mainnet binaries and the "Zen Testnet" tab contains testnet binaries. We generally recommend using recent artifacts from runs marked `stable`.
:::

For the workshop, we will build from source and compile to source code into an executable binary.

```bash
# clone the stable branch of the renterd source code and cd into it
git clone --branch stable https://github.com/SiaFoundation/renterd.git && cd renterd

# build the binary and specify the test network
go build -tags="testnet" -o ./ ./...
```

### Step 2: Generate a wallet seed

The wallet module uses a 12-word BIP-39 (Bitcoin Improvement Proposal 39) seed, which is a widely-used standard for generating mnemonic seed phrases for cryptocurrency wallets. It is designed to make it easier for users to manage and back up their private keys, which are essential for securing access to their cryptocurrency holdings.

You can generate a seed using Ian Coleman's [excellent seed generator](https://iancoleman.io/bip39/), or simply ask `renterd` to generate one for you:

```bash
./renterd seed

# output:
renterd v0.1.0
Network Testnet-Zen
Seed phrase: [[ your seed will be displayed here ]]
```

:::warning
Do not use this seed for any other purpose than this workshop.
:::

### Step 3: Start the renter

Finally we are ready to launch the `renterd` executable. After launching the daemon will ask you for an API password and the wallet seed you just generated. You can choose any API password you want but for the purpose of this workshop we advise you to just use 'test' or something that is easy to remember.

```bash
./renterd

# output:
renterd v0.1.0
Network Testnet-Zen
Enter API password:
Enter wallet seed:
api: Listening on 127.0.0.1:9880
bus: Listening on localhost:9881
```


:::success
Congrats! :tada: You now have a running instance of `renterd` that is connected to the [Zen Testnet](https://zen.sia.tech). Browse to http://localhost:9880 and enter your API password to navigate to the UI.
:::

### Step 4: Wait for consensus to sync

To find out whether consensus is synced, we need to know the current height of the chain, which can be found [here](https://zen.sia.tech). Our own height is displayed in the UI on the [Blockchain Node Tab](http://localhost:9880/node).

You can also follow the process manually by querying the `/bus/consensus/state` endpoint using `curl` like so:

```bash
curl -u ":[[ API PASSWORD HERE ]]" localhost:9880/api/bus/consensus/state
```

Response:
```json
{
	"BlockHeight": 13652,
	"Synced": true
}
```

This process should take less than a couple of minutes.

### Step 5: Fund your wallet

#### Wallet Address

For the purpose of this workshop we need some Siacoins to form file contracts. These contracts are necessary to be able to upload data into. Luckily, there is a testnet faucet we can ask for some funds. For that we first need to know our wallet address, which is displayed in the [Wallet Tab](http://localhost:9880/wallet) in the modal shown when you click "Receive" in the top right corner.

You can also obtain the address manually by querying the `/bus/wallet/address` endpoint using `curl` like so:

```bash
curl -u ":[[ API PASSWORD HERE ]]" localhost:9880/api/bus/wallet/address
```

Response:
```json
"addr:c0d6af1cfc0bc5fb9d53ee58781a9677f572202e3bfa2a53df2ae545115fea843ea9745bd5ea"
```

#### Testnet Faucet

Now that we have our wallet address, head on over to the official [Testnet Faucet](https://zen.sia.tech/faucet) and request 1,000 Siacoins. This process will take your time because the transaction has to be mined, once it is mined and taken up in a block we can query the wallet for its balance. The number returned by the endpoint represents Hastings, the smallest unit of value in the Sia ecosystem. 1 Siacoin equals 10^24 Hastings. The balance will be displayed in the [Wallet Tab](http://localhost:9880/wallet).

You can also obtain the balance manually by querying the `/bus/wallet/balance` endpoint using `curl` like so:

```bash
curl -u ":[[ API PASSWORD HERE ]]" localhost:9880/api/bus/wallet/balance
```

Response:
```json
"1000000000000000000000000000"
```

:::warning
Remove the `addr:` prefix and surrounding quotes if you used the CLI to get your address. Otherwise you'll receive an error.
:::

:::warning
Incoming transactions won't be reflected in the wallet's balance before the consensus state is considered "synced".
:::

## Configuration

### Step 1: Configure your autopilot

The renter daemon has a component that we call the autopilot. It is responsible for keeping the renter operational once it is up an running and should theoretically be able to manage the file contracts and uploaded date for you. For this to work it has to be properly configured to suit your data storage needs. The autopilot can be configured through the UI in the [Autopilot Tab](http://localhost:9880/autopilot).

You can view the current configuration manually by querying the `/autopilot/config` endpoint using `curl` like so:

```bash=
curl -u ":[[ API PASSWORD HERE ]]" localhost:9880/api/autopilot/config
```

Response:

```json
{
	"wallet": {
		"defragThreshold": 1000
	},
	"hosts": {
		"ignoreRedundantIPs": false,
		"maxDowntimeHours": 168,
		"scoreOverrides": {}
	},
	"contracts": {
		"set": "autopilot",
		"amount": 10,
		"allowance": "1000000000000000000000000000",
		"period": 6048,
		"renewWindow": 2016,
		"download": 10000000000,
		"upload": 10000000000,
		"storage": 10000000000
	}
}
```

You can also update the configuration manually by querying the `/autopilot/config` endpoint using `curl` like so:

```bash
curl -u ":[[ API PASSWORD HERE ]]" -X PUT localhost:9880/api/autopilot/config --data '{
    "wallet": {
        "defragThreshold": 1000
    },
    "hosts": {
        "ignoreRedundantIPs": false,
        "maxDowntimeHours": 168,
        "scoreOverrides": {}
    },
    "contracts": {
        "set": "autopilot",
        "amount": 10,
        "allowance": "1000000000000000000000000000",
        "period": 6048,
        "renewWindow": 2016,
        "download": 10000000000,
        "upload": 10000000000,
        "storage": 10000000000
    }
}'
```

The above configuration holds sane settings for use on the Zen testnet.

| Field | Value | Description |
| ------| ------| ------------|
| defragThreshold | 1000 | Siacoin outputs before the wallet is defragmented
| ignoreRedundantIPs | true | Allow hosts from the same subnet (only allow on testnet)
| maxDownTimeHours | 168 | allow one week of downtime before hosts get removed
| scoreOverrides | {} | no manual overrides
| set | autopilot | name of the contract set used by the autopilot
| amount | 10 | keep a set of contracts with 10 unique hosts
| allowance | 1000000000000000000000000000 |  1000 Siacoins (1KS)
| period | 6048 | 6 weeks `(6 * 24 * 7 * 6)` contract periods before they expire
| renewWindow | 2016 | 2 weeks `(6 * 24 * 7 * 2)` before the end of a contract period we start renewing them
| download | 10000000000 | 10 GB of expected downloads per period
| upload | 10000000000 |  10 GB of expected uploads per period
| storage | 10000000000| 10 GB of expected stored data per period

### Step 2: Configure your redundancy

:::warning
Since we only form contracts with 10 hosts, we need to update the redundancy settings for this workshop as the default requires 30 contracts.
:::

The redundancy settings can be updated through the UI in the [Configuration Tab](http://localhost:9880/configuration). For this workshop we recommend to set it to 1-3, where one is the minimum number of shards required to download the file, and three is the total number of shards.

You can also update the redundancy settings manually by querying the `/bus/setting/redundancy` endpoint using `curl` like so:

```bash
curl -u ":[[ API PASSWORD HERE ]]" -X PUT localhost:9880/api/bus/setting/redundancy --data '{
    "minShards": 1,
    "totalShards": 3
}'
```

### Step 3: Wait for contracts to form

After updating the autopilot settings, the autopilot will start forming contracts. Eventually it will have formed 10 contracts. You can check the progress in the UI's [Contracts Tab](http://localhost:9880/contracts).

You can also fetch the active contracts list manually by querying the `/bus/contracts/active` endpoint using `curl` like so:

```bash
curl -u ":[[ API PASSWORD HERE ]]" localhost:9880/api/bus/contracts/active
```

## Storing Data

### Step 1: Create a dummy file

Create a dummy file or choose a file you want to upload:

```bash
echo "Hello World!" > foo.txt
```

### Step 2: Upload the file to the network

The easiest way to upload a file is via the UI's [Files Tab](http://localhost:9880/files) `files` tab.

You can also upload a file manually by uploading it to the `/worker/objects/[[ FILENAME HERE ]]` endpoint using `curl` like so:

```bash
curl -u ":[[ API PASSWORD HERE ]]" -H "Content-Type: application/octet-stream" -X PUT localhost:9880/api/worker/objects/foo.txt --data-binary '@foo.txt'
```

### Step 3: Download the file from the network

The easiest way to download a file is via the UI's [Files Tab](http://localhost:9880/files) `files` tab.

You can also download a file manually by querying the `/worker/objects/[[ FILENAME HERE ]]` endpoint using `curl` like so:

```bash
curl -u ":[[ API PASSWORD HERE ]]" localhost:9880/api/worker/objects/foo.txt
```

Response

```bash
Hello World!%
```

## Docker

We also host [official docker images](https://github.com/SiaFoundation/renterd/pkgs/container/renterd). At this time we automatically build `master`, `stable`, `master-testnet` and `stable-testnet` images.

You can run `renterd` with the following command assuming you already have a seed. See [here](#Step-2-Generate-a-wallet-seed) how to obtain a seed.

```bash
docker run -d --name renterd -e RENTERD_API_PASSWORD="<PASSWORD>" -e RENTERD_SEED="<SEED>" -p 127.0.0.1:9880:9880/tcp -p :9881:9981/tcp ghcr.io/siafoundation/renterd:stable-testnet
```