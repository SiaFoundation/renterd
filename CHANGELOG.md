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
