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
