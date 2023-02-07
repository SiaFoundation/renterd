# [![Sia](https://sia.tech/banners/sia-banner-renterd.png)](http://sia.tech)

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

All of the key low-level APIs have been implemented and are ready for use.
However, `renterd` currently does not ship with a UI or autopilot functionality,
and it lacks backwards-compatibility with `siad` renter metadata. This means
that, while `renterd` is already capable of serving as the backbone for new Sia
applications, most users should continue to use `siad`.

Going forward, our immediate priority is to implement autopilot functionality,
which will make `renterd` viable as a standalone renter. In tandem, we'll be
designing and integrating the embedded web UI. At this point, `renterd` will
become the recommended renter for new users. However, we also want to make it
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

`docker run -d --name renterd -e RENTERD_API_PASSWORD="<PASSWORD>" -e RENTERD_WALLET_SEED="<SEED>" -p 127.0.0.1:9980:9980/tcp -p :9981:9981/tcp <IMAGE_ID>`

# Usage

## Wallet

Make sure the wallet is funded. Fetch the wallet's address and transfer some money. Verify the wallet's balance is not zero using the following endpoints:

- `GET /api/bus/wallet/address`
- `GET /api/bus/wallet/balance`

The autopilot will automatically redistribute the wallet funds over a certain of outputs that make sense with regards to the autopilot's configuration. Contract formation and renewals work best when the autopilot has a good amount of outputs at its disposal. It's definitely a good idea to verify whether this is the case because if not it means that it's likely the autopilot is misconfigured, in which case the logs should be of help.

- `GET /api/bus/wallet/outputs`

## Consensus

In order for the contracts to get formed, your node has to be synced with the blockchain. If you are not bootstrapping your node this can take a while. Verify your node's consensus state using the following endpoint:

- `GET /api/bus/consensus/state`

## Config

To have a working autopilot, it must be configured with a sane config. The
autopilot's configuration is configurable through the following endpoints:

- `GET /api/autopilot/config`
- `PUT /api/autopilot/config`

Especially the `contracts` section is important, make sure the `amount` is set to the amount of hosts with which you want to form a contract. The `allowance` is the amount of money the autopilot can spend per period, make sure it is not set to zero or contracts won't get formed.

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
		"download": 1099511627776,
		"upload": 1099511627776,
		"storage": 1099511627776
	}
}
```

## Redundancy

The redundancy is configurable in a couple of ways, the first one is through two CLI options:

- **`bus.minShards`**: min amount of shards needed to reconstruct the slab
- **`bus.totalShards`**: total amount of shards for each slab

The default redundancy is 30-10. The redunancy can be updated using the settings API:

- `GET /api/bus/setting/redundancy`
- `PUT /api/bus/setting/redundancy`

Lastly, the redundancy can be passed through query string parameters on the
upload endpoint in the worker API:

- `PUT /api/worker/objects/foo?minshards=2&totalshards=5`

It is important to note that on every restart of `renterd` the setting is overwritten with the default values, if you want to run `renterd` using a redundancy which is not the default we advise to use the CLI options.

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

The autopilot forms and manages contracts in the contract set with name configured in the autopilot's configuration object, by default this is called the `autopilot` contract set. This contract set should contain the amount of contracts configured in the contracts sectino of the configuration.

That means that, if everything is running smoothly, the following curl call should return that number:
`curl -u ":[YOUR_PASSWORD]"  [BASE_URL]/api/bus/contracts/set/autopilot | jq '.|length'`

### Autopilot Trigger

For debugging purposes, the autopilot allows triggering the main loop using the following endpoint:

- `POST /api/autopilot/debug/trigger`