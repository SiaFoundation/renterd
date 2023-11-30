# [![Sia](https://sia.tech/assets/banners/sia-banner-expanded-renterd.png)](http://sia.tech)
[![GoDoc](https://godoc.org/go.sia.tech/renterd?status.svg)](https://godoc.org/go.sia.tech/renterd)

# Renterd: The Next-Gen Sia Renter

## Overview

`renterd` is an advanced Sia renter engineered by the Sia Foundation. Designed
to cater to both casual users seeking straightforward data storage and
developers requiring a robust API for building apps on Sia.

`renterd` is now in beta, achieving feature parity with siad. It introduces an
enhanced web UI and the autopilot functionality. While it mirrors most siad
capabilities, renterd does not support backwards compatibility with siad
metadata. Consequently, files uploaded via siad cannot currently be migrated to
renterd. Our immediate focus is on refining renterd to enhance its stability,
scalability, and performance, ensuring it serves as a robust foundation for new
Sia applications.

## Useful Links

API documentation can be found [here](https://api.sia.tech/renterd).<br>
Setup guides are available on our [website](https://docs.sia.tech/renting/setting-up-renterd).<br>
A project roadmap is available on [GitHub](https://github.com/orgs/SiaFoundation/projects/5).

## Docker Support

`renterd` includes a `Dockerfile` which can be used for building and running
renterd within a docker container. For the time being it is provided as-is
without any compatibility guarantees as it will change over time and be extended
with more configuration options. The image can also be pulled from
ghcr.io/siafoundation/renterd.

### Docker Compose

```docker
version: "3.9"
services:
  renterd:
    image: ghcr.io/siafoundation/renterd:master-zen
    environment:
      - RENTERD_SEED=put your seed here
      - RENTERD_API_PASSWORD=test
    ports:
      - 9880:9880
      - 9881:9881
      - 7070:7070
    volumes:
      - ./data:/data
    restart: unless-stopped
    stop_grace_period: 5m
```

### Build Image

From within the root of the repo run the following command to build an image of
`renterd` tagged `renterd`.

#### Mainnet

```sh
docker build -t renterd:master -f ./docker/Dockerfile .
```

#### Testnet

```sh
docker build --build-arg BUILD_TAGS='netgo testnet' -t renterd:master-zen -f ./docker/Dockerfile .
```

### Run Container

Run `renterd` in the background as a container named `renterd` that exposes its
API to the host system and the gateway to the world.

#### Mainnet

```bash
docker run -d --name renterd -e RENTERD_API_PASSWORD="<PASSWORD>" -e RENTERD_SEED="<SEED>" -p 127.0.0.1:9980:9980/tcp -p :9981:9981/tcp ghcr.io/siafoundation/renterd:master
```

#### Testnet

```bash
docker run -d --name renterd-testnet -e RENTERD_API_PASSWORD="<PASSWORD>" -e RENTERD_SEED="<SEED>" -p 127.0.0.1:9880:9880/tcp -p :9881:9881/tcp ghcr.io/siafoundation/renterd:master-zen
```

## Architecture

`renterd` distinguishes itself from its predecessor, `siad`, through a unique
architecture comprised of three main components: the autopilot, the bus, and one
or more workers.

Instead of adopting another Electron app bundle, `renterd` incorporates an
embedded web UI. This approach caters to developers and power users who prefer a
streamlined experience. For those who do not require the UI, `renterd` can be
compiled without it, which reduces software bloat and simplifies the build
process.

### Autopilot

The autopilot in `renterd` automates high-level functions like host scanning,
ranking, and the management of contracts and data. This ensures efficient host
selection for data storage. While it offers convenience for most users, those
desiring full control can disable the autopilot via a CLI flag. Remarkably, the
autopilot is designed over the public `renterd` HTTP API, allowing for
straightforward customization and language porting.

### Bus

Serving as the central nervous system of `renterd`, the bus handles data
persistence and connections with Sia's peer-to-peer network. While `renterd`
defaults to an SQLite database, users have the option to configure it for MySQL
use. SQLite is the default choice due to its seamless initial experience, but
switching to MySQL is recommended for enhanced performance.

### Worker(s)

Workers are the direct interface for users, managing tasks such as file
uploads/downloads and contract handling. They depend on the bus for consistent
data persistence.

## Usage

`renterd` can be configured in various ways, through the use of a yaml file, CLI
flags or environment variables. Settings that are configured multiple times will
be evaluated in this order. Use the `help` command to see an overview of all
settings.

```sh
renterd --help
```

The Web UI streamlines the initial setup and configuration for newcomers.
However, if manual configuration is necessary, the subsequent sections outline a
step-by-step guide to achieving a functional `renterd` instance.

### Wallet

Make sure the wallet is funded, it's a good rule of thumb to have at least twice
the allowance in the wallet. Fetch the wallet's address and transfer some money.
Verify the wallet's balance is not zero using the following endpoint:

- `GET /api/bus/wallet`

The autopilot will automatically redistribute the wallet funds over a certain
number of outputs that make sense with regards to the autopilot's configuration.
Contract formation and renewals work best when the autopilot has a good amount
of outputs at its disposal. It's definitely a good idea to verify whether this
is the case because if not it means that it's likely the autopilot is
misconfigured, in which case the logs should be of help.

- `GET /api/bus/wallet/outputs`

### Consensus

In order for the contracts to get formed, your node has to be synced with the
blockchain. If you are not bootstrapping your node this can take a while. Verify
your node's consensus state using the following endpoint:

- `GET /api/bus/consensus/state`

### Config

The configuration can be updated through the UI or by using the following
endpoints:

- `GET /api/autopilot/config`
- `PUT /api/autopilot/config`

The autopilot will not perform any tasks until it is configured. An example
configuration can be found below. Especially the `contracts` section is
important, make sure the `amount` is set to the amount of hosts with which you
want to form a contract. The `allowance` is the amount of money the autopilot
can spend per period, make sure it is not set to zero or contracts won't get
formed.

```json
{
	"wallet": {
		"defragThreshold": 1000
	},
	"hosts": {
		"allowRedundantIPs": false,
		"maxDowntimeHours": 1440,
		"minRecentScanFailures": 20,
		"scoreOverrides": {}
	},
	"contracts": {
		"set": "autopilot",
		"amount": 50,
		"allowance": "10000000000000000000000000000",
		"period": 6048,
		"renewWindow": 2016,
		"download": 1099511627776, // 1TiB
		"upload": 1099511627776, // 1TiB
		"storage": 1099511627776 // 1TiB
	}
}
```

### Contract Set

The contract set settings on the bus allow specifying a default contract set.
This contract set will be returned by the `bus` through the upload parameters,
and decides what contracts data is upload or migrated to by default. This
setting does not have a default value, it can be updated using the settings API:

- `GET /api/bus/setting/contractset`
- `PUT /api/bus/setting/contractset`

```json
{
        "default": "autopilot"
}
```

In most cases the default set should match the set from your autopilot
configuration in order for migrations to work properly. The contract set can be
overriden by passing it as a query string parameter to the worker's upload and
migrate endpoints.

- `PUT /api/worker/objects/foo?contractset=foo`

### Redundancy

The default redundancy on mainnet is 30-10, on testnet it is 6-2. The redunancy
can be updated using the settings API:

- `GET /api/bus/setting/redundancy`
- `PUT /api/bus/setting/redundancy`

The redundancy can also be passed through query string parameters on the upload
endpoint in the worker API:

- `PUT /api/worker/objects/foo?minshards=2&totalshards=5`

### Gouging

The default gouging settings are listed below. The gouging settings can be
updated using the settings API:

- `GET /api/bus/setting/gouging`
- `PUT /api/bus/setting/gouging`

```json
{
	"hostBlockHeightLeeway": 6,                                   // 6 blocks
	"maxContractPrice": "15000000000000000000000000",             // 15 SC per contract
	"maxDownloadPrice": "3000000000000000000000000000",           // 3000 SC per 1 TiB
	"maxRPCPrice": "1000000000000000000000",                      // 1mS per RPC
	"maxStoragePrice": "631593542824",                            // 3000 SC per TiB per month
	"maxUploadPrice": "3000000000000000000000000000",             // 3000 SC per 1 TiB
	"migrationSurchargeMultiplier": 10,                           // overpay up to 10x for sectors migrations on critical slabs
	"minAccountExpiry": 86400000000000,                           // 1 day
	"minMaxCollateral": "10000000000000000000000000",             // at least up to 10 SC per contract
	"minMaxEphemeralAccountBalance": "1000000000000000000000000", // 1 SC
	"minPriceTableValidity": 300000000000                         // 5 minutes
}
```

### Blocklist

Unfortunately the Sia blockchain is subject to hosts that announced themselves
with faulty parameters and/or bad intentions, something which is unavoidable of
course in a decentralized environment. We added a blocklist to give users the
power to block hosts by IP or domain.

- `GET /api/bus/hosts/blocklist`
- `PUT /api/bus/hosts/blocklist`

The Sia Foundation does not ship `renterd` with a default blocklist, the
following entries exclude a decent amount of bad/old/malicious hosts:

- 45.148.30.56
- 51.158.108.244
- siacentral.ddnsfree.com
- siacentral.mooo.com

## Debugging

### Logging

`renterd` has both console and file logging, the logs are stored in
`renterd.log` and contain logs from all of the components that are enabled, e.g.
if only the `bus` and `worker` are enabled it will only contain the logs from
those two components.

### Ephemeral Account Drift

The Autopilot manages a collection of ephemeral accounts, each corresponding to
a specific contract. These accounts facilitate quicker payments to hosts for
various actions, offering advantages over contract payments in terms of speed
and parallel execution. Account balances are periodically synchronized with
hosts, and discrepancies, if any, are detected during this process. renterd
incorporates built-in safeguards to deter host manipulation, discontinuing
interactions with hosts that exhibit excessive account balance drift. In rare
scenarios, issues may arise due to this drift; these can be rectified by
resetting the drift via a specific endpoint:

- `POST   /account/:id/resetdrift`

### Contract Set Contracts

The autopilot forms and manages contracts in the contract set with name
configured in the autopilot's configuration object, by default this is called
the `autopilot` contract set. This contract set should contain the amount of
contracts configured in the contracts section of the configuration.

That means that, if everything is running smoothly, the following curl call
should return that number

```bash
curl -u ":[YOUR_PASSWORD]"  [BASE_URL]/api/bus/contracts/set/autopilot | jq '.|length'
```

### Autopilot Loop Trigger

The autopilot allows triggering its loop using the following endpoint. The UI
triggers this endpoint after the user updates the configuration, but it can be
useful for debugging purposes too.

- `POST /api/autopilot/trigger`
