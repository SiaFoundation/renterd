## 2.9.0 (2026-02-27)

### Features

- Update Go to 1.26.0.

### Fixes

- Ensure all store methods are retry-safe
- Update coreutils from v0.21.0 to v0.21.1

## 2.8.1 (2026-02-06)

### Fixes

- Fixed instant sync in SQLite causing crashes.

## 2.8.0 (2026-02-03)

### Features

#### Add support for instant syncing

New users can sync instantly using `renterd --instant`. When instant syncing, the `renterd` node initializes using a Utreexo-based checkpoint and can immediately validate blocks from that point forward without replaying the whole chain state. The state is extremely compact and committed in block headers, making this initialization both quick and secure. 

[Learn more](https://sia.tech/learn/instant-syncing)

**The wallet is required to only have v2 history to use instant syncing.**

### Fixes

- Reduce memory pressure when fetching contract roots for pruning.
- Update core dependency to v0.19.0 and coreutils dependency v0.20.0.

## 2.7.1 (2025-11-03)

### Fixes

- Reset chain state if consensus.db was deleted.
- Upgrade `core` to v0.18.0 and `coreutils` to v0.18.6

## 2.7.0 (2025-09-29)

### Features

- Make per-sector upload timeout configurable.

### Fixes

- Remove requirement of 64 byte alignment for client side encryption.
- Score hosts according to host's protocol version.

## 2.6.0 (2025-08-25)

### Features

- Add 'Minimum' option to wallet redistribution endpoint.
- Added `[GET] /pinned/*key` to retrieve the raw object metadata suitable for downloading using external tools.
- Rebroadcast transactions periodically

### Fixes

- Avoid marking contract as 'bad' if renew/refresh fails for non-host-related reasons.
- Fix failure to unmarhsal protocol version in host settings.
- Fix race when broadcasting contract revisions and resolutions.
- Increase funding for contracts.
- Increase value of redistributed UTXOs.
- Make sure that SlowThreshold can't be set to 0.
- Perform sector write and append with single siamux transport.

## 2.5.0 (2025-07-07)

### Features

- Remove legacy RHP2 and RHP3 code
- Renamed blockchain.db to consensus.db and moved it out of the consensus subdirectory

### Fixes

- Broadcast formation set to peers
- Broadcast maintenance transactions
- Configure MaxDefragUTXOs on the wallet to ensure we don't defrag as often.
- Only broadcast contract revisions for active contracts
- Reduced locked utxo expiry from 24 hours to 3 hours.

## 2.4.0 (2025-07-01)

### Features

- No longer reset consensus on commitment mismatch

### Fixes

- Be slightly more generous with the host collateral put into refreshed contracts to refresh less often
- Don't resync accounts on every reboot
- Limit maximum number of times we reset chain state in an attempt to sync the chain state'
- Use only confirmed outputs for funding contract transactions

## 2.3.3 (2025-06-25)

### Fixes

- Fix panic when reverting contract creation

## 2.3.2 (2025-06-23)

### Fixes

- Fix index out of range panic when processing chain updates without any blocks that are being applied
- Reset chain state when block is missing instead of on startup.

## 2.3.1 (2025-06-17)

### Fixes

- Update UI from 0.79.0 to 0.80.0.

## 2.3.0 (2025-06-10)

### Features

- Extend store with utxo lock/unlock methods to implement the SingleAddressStore interface

### Fixes

- Check for rhp4 errors returned by hosts in download code.
- Don't attempt to renew v1 contracts with RHP4.
- Fix address filter not working on v2 hosts.
- Update coreutils dependency.
- Use RHP3 to fund accounts from v1 contracts.
- Use RHP3 to prune v1 contracts.

## 2.2.1 (2025-05-29)

### Fixes

- Cap endheight for contract formation of v1 contracts to 1 block before v2 require height.

## 2.2.0 (2025-05-27)

### Features

- Added automatic migration for v2 commitments.

### Fixes

- Add hardening against writing incomplete partial slab data.

## 2.1.0 (2025-04-29)

### Features

- Add dl query param to download endpoint to be able to change the Content-Disposition to "attachment"

### Fixes

- Add compate code to migrate the config from v1.1.1 to v2.0.1
- Add missing descriptions to all routes and schemas
- Include unconfirmed parents of transactions when broadcasting revisions for expired contracts
- Fix listing objects with a slash delimiter and similar paths across buckets.
- Fix missing inferred MimeType for objects uploaded via multipart uploads
- Fix out of bounds panic in alerts manager
- Fix renter expecting host putting up too much collateral for v2 contracts.
- Fixed a deadlock in the migration loop.
- Increase default number of migrator threads
- Redistribute wallet into 100 outputs instead of 10.
- Remove context from bus initialisation
- Split renewContractV2 up into renewContractV2 for renewals and refreshContractV2 for refreshes.
- Update go import path to v2
- Update jape dependency to v0.13.0

## 2.0.1 (2025-03-07)

### Fixes

- Fix default S3 address in README.md
- Fix issue with remaining storage and version scoring for v2 hosts
- Improve logging in v2 migration code and fix scan during migration not timing out

## 2.0.0 (2025-02-18)

This version of `renterd` introduces full v2 support for the renting software. This means it contains all the required changes to support the hardfork later this year which also makes this a major version increase since it introduces breaking changes.

Both the API as well as the config changed in this version. The up-to-date specs for the API can be found in the `openapi.yml` file. As we release the stable version we will update the official api documentation.
If you are using a `renterd.yml` config file you might need to update that before your renter is able to start. Either that or delete the current one and create a new one using `renterd config`.

### Breaking Changes

- API refactor and introduction of an `openapi.yml` spec
- Enforce S3 bucket name validation rules on bucket creation request
- Migrations are now performed by the autopilot without a worker
- Removed the concept of contract sets
- Removed event webhooks
- Contract lifecycles are no longer aligned
- Removed concept of an allowance

### Features

- Full support for RHP4, the new protocol used by renters to communicate with hosts
- Full support for the Utreexo hardfork
- Account drift gets reset automatically at a given rate
- UI was updated from v0.61.0 -> v0.73.0 ([changelogs](https://github.com/SiaFoundation/web/releases))
- `/api/auth` endpoint to create short-lived cookies for API authentication

### Fixes

- Allow worker id to be empty when worker is disabled
- Build image from debian:bookworm-slim rather than scratch to avoid issue with read permissions when accessing SQLite temp dir
- Don't perform full slab prune on startup
- Don't return an error when a v1 contract doesn't contain any sectors to prune
- Log startup errors to stderr
- Fix bug where pruning large amounts of data from a contract would prune good sectors leading to lost sector alerts
- Fix bus configuration commands in issue template
- Fix default settings not being used everywhere if settings are not found in database
- Fix DeleteHostSector deleting a sector from all hosts rather than the given one
- Fix error output in the renterd.log
- Fix hosts that fail the pre-scan checks not having a failed scan recorded
- Fix memory manager for downloads using the uploads config
- Fix migration failing due to locking table getting too big on large nodes
- Fix potential deadlock/panic when forcing a scan
- Fix upload bug that caused failures when uploading to contracts that are being refreshed in the background
- Fix uploads that fail if a contract used in the upload was archived in the meantime
- Fragment the wallet into larger outputs.
- Improve migration output after foreignkey check fails
- Monitor file contracts and unlock their funds when they expire
- Prune host sectors in a background loop in small batches
- Remove unexported type from migrator.New
- Retry MySQL transactions on 'Lock wait timeout exceeded' errors
- Speed up slab pruning query
- Track accumulator state elements for file contracts in a new contract_elements table
- Fix error in pruning code which potentially led to lost sectors
- Reduce log spam for default logging
- Fix an issue where the config evaluation endpoint returns 0 matching hosts
