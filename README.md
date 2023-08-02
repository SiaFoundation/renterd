# [![Sia](https://sia.tech/assets/banners/sia-banner-expanded-renterd.png)](http://sia.tech)

[![GoDoc](https://godoc.org/go.sia.tech/renterd?status.svg)](https://godoc.org/go.sia.tech/renterd)

A renter for Sia.

## Overview

`renterd` is a next-generation Sia renter, developed by the Sia Foundation. It
aims to serve the needs of both everyday users -- who want a simple interface
for storing and retrieving their personal data -- and developers -- who want to
a powerful, flexible, and reliable API for building apps on Sia.

To achieve this, we have carefully architected `renterd` to isolate its
user-friendly functionality, which we call "autopilot," from its low-level APIs.
Autopilot consists of background processes that scan and rank hosts, form and
renew contracts, and automatically migrate files when necessary. Most users want
this functionality, but for those who want absolute control over their contracts
and data, autopilot can easily be disabled with a CLI flag. Even better, the
autopilot code is built directly on top of the public `renterd` HTTP API,
meaning it can be easily forked, modified, or even ported to another language.

`renterd` also ships with an embedded web UI, rather than yet another Electron
app bundle. Developers and power users can even compile `renterd` without a UI
at all, reducing bloat and simplifying the build process.

## Current Status

`renterd` is officially in beta and most of the features known from `siad` have
been implemented and are ready for use. This includes autopilot functionality.
It also comes bundled with a new and improved web UI. However, it lacks
backwards-compatibility with `siad` renter metadata. This means that, while
`renterd` is already capable of serving as the backbone for new Sia
applications, there is currently no way to migrate files uploaded through `siad`
to `renterd`.

Going forward, our immediate priority is continuously improving `renterd` to
become more stable, scalable and performant. However, we also want to make it
painless for existing `siad` users to switch to `renterd`. So before shipping
v1.0, we'll implement compatibility code that will allow you to access all of
your `siad` files with `renterd`.

# DOCKER

Renterd comes with a `Dockerfile` which can be used for building and running
renterd within a docker container. For the time being it is provided as-is
without any compatibility guarantees as it will change over time and be extended
with more configuration options.

## Building Image

From within the root of the repo run the following command to build an image of
`renterd` tagged `renterd`.

`docker build -t renterd .`

## Running Container

Run `renterd` in the background as a container named `renterd` that exposes its
API to the host system and the gateway to the world.

`docker run -d --name renterd -e RENTERD_API_PASSWORD="<PASSWORD>" -e RENTERD_SEED="<SEED>" -p 127.0.0.1:9980:9980/tcp -p :9981:9981/tcp <IMAGE_ID>`

# Usage

## Wallet

Make sure the wallet is funded, it's a good rule of thumb to have at least twice the allowance in the wallet. Fetch the wallet's address and transfer some money. Verify the wallet's balance is not zero using the following endpoints:

- `GET /api/bus/wallet/address`
- `GET /api/bus/wallet/balance`

The autopilot will automatically redistribute the wallet funds over a certain number of outputs that make sense with regards to the autopilot's configuration. Contract formation and renewals work best when the autopilot has a good amount of outputs at its disposal. It's definitely a good idea to verify whether this is the case because if not it means that it's likely the autopilot is misconfigured, in which case the logs should be of help.

- `GET /api/bus/wallet/outputs`

## Consensus

In order for the contracts to get formed, your node has to be synced with the blockchain. If you are not bootstrapping your node this can take a while. Verify your node's consensus state using the following endpoint:

- `GET /api/bus/consensus/state`

## Config

The configuration can be updated through the UI or by using the following endpoints:

- `GET /api/autopilot/config`
- `PUT /api/autopilot/config`

The autopilot will not perform any tasks until it is configured. An example configuration can be found below. Especially the `contracts` section is important, make sure the `amount` is set to the amount of hosts with which you want to form a contract. The `allowance` is the amount of money the autopilot can spend per period, make sure it is not set to zero or contracts won't get formed.

```json
{
	"wallet": {
		"defragThreshold": 1000
	},
	"hosts": {
		"ignoreRedundantIPs": false,
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

## Contract Set

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

## Redundancy

The default redundancy on mainnet is 30-10, on testnet it is 6-2. The redunancy
can be updated using the settings API:

- `GET /api/bus/setting/redundancy`
- `PUT /api/bus/setting/redundancy`

The redundancy can also be passed through query string parameters on the upload
endpoint in the worker API:

- `PUT /api/worker/objects/foo?minshards=2&totalshards=5`

## Gouging

The default gouging settings are listed below. The gouging settings can be updated using the settings API:

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
	"minAccountExpiry": 86400000000000,                           // 1 day
	"minMaxCollateral": "10000000000000000000000000",             // at least up to 10 SC per contract
	"minMaxEphemeralAccountBalance": "1000000000000000000000000", // 1 SC
	"minPriceTableValidity": 300000000000                         // 5 minutes
}
```

## Blocklist

Unfortunately the Sia blockchain contains a large amount of hosts that announced themselves with faulty parameters and/or bad intentions, something which is unavoidable of course in a decentralized environment. To make sure the autopilot does not have to scan/loop through all ~80.000 hosts on every iteration of the loop, we added a blocklist.

- `GET /api/bus/hosts/blocklist`
- `PUT /api/bus/hosts/blocklist`

The Sia Foundation does not ship `renterd` with a default blocklist, the following entries exclude a decent amount of bad/old/malicious hosts:

- 45.148.30.56
- 51.158.108.244
- siacentral.ddnsfree.com
- siacentral.mooo.com

## Logging

`renterd` has both console and file logging, the logs are stored in `renterd.log` and contain logs from all of the components that are enabled, e.g. if only the `bus` and `worker` are enabled it will only contain the logs from those two components.

## Debug

### Contract Set Contracts

The autopilot forms and manages contracts in the contract set with name configured in the autopilot's configuration object, by default this is called the `autopilot` contract set. This contract set should contain the amount of contracts configured in the contracts section of the configuration.

That means that, if everything is running smoothly, the following curl call should return that number:
`curl -u ":[YOUR_PASSWORD]"  [BASE_URL]/api/bus/contracts/set/autopilot | jq '.|length'`

### Autopilot Trigger

For debugging purposes, the autopilot allows triggering the main loop using the following endpoint:

- `POST /api/autopilot/debug/trigger`
