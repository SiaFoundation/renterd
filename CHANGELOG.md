## 2.0.0 (2025-02-18)

### Breaking Changes

- Enforce S3 bucket name validation rules on bucket creation request
- Fix hosts query, properly take into account the field 'usability_low_max_duration'
- Move migrations to the autopilot
- Remove contract sets
- Remove event webhooks
- Remove partial setting updates

#### Add bus section to openapi spec

Added routes:
- accounts
- alerts
- autopilot
- buckets

#### Extend OpenAPI spec

The following routes were added:
- consensus
- contracts
- contract
- hosts
- host
- metric
- multipart
- objects
- object
- params
- sectors
- settings
- slabbuffers
- slabs
- state
- stats
- syncer
- system
- upload
- txpool
- wallet
- webhooks

#### Move `autopilots` into `autopilot_state`

##1657 by @peterjan

This migrates the `autopilots` to a single `autopilot_state` table, effectively removing the concept of multiple autopilots.

#### Use standard locations for application data

 Uses standard locations for application data instead of the current directory. This brings `renterd` in line with other system services and makes it easier to manage application data.

 #### Linux, FreeBSD, OpenBSD
 - Configuration: `/etc/renterd/renterd.yml`
 - Data directory: `/var/lib/renterd`

 #### macOS
 - Configuration: `~/Library/Application Support/renterd.yml`
 - Data directory: `~/Library/Application Support/renterd`

 #### Windows
 - Configuration: `%APPDATA%\SiaFoundation\renterd.yml`
 - Data directory: `%APPDATA%\SiaFoundation\renterd`

 #### Docker
 - Configuration: `/data/renterd.yml`
 - Data directory: `/data`

### Features

- Add host_sectors table
- Add runtime to prometheus metrics
- Allow for changing the 'Domain' of a cookie by setting the 'X-Forwarded-Host' request header
- Ensure the autopilot adheres to the flag that allows enabling/disabling contract pruning.
- Fixed all instances where created_at is never set.
- Fix bug in evaluate config where host blockheight would not be properly ignored
- Fix 00030_host_sectors migration on DBs that have a host_sectors table
- Increase slow query threshold to 500ms.
- Integrate RHP4 for uploads
- Log startup errors to stderr
- Move hosts allowRedundantIPs config
- Remove current period
- Remove migrationSurchargeMultiplier from the gouging settings.
- Remove prune alerts, they are not actionable.
- Unregister migration failure alerts if there are no slabs to migrate.
- Update UI to v0.68.0

#### Add openapi.yml file with autopilot routes

Added an openapi.yml spec with the specifications for the autopilot routes and a CI step to validate it. The goal is to eventually have a complete spec for the V2 API that we can use to generate API docs as well as making sure that there is always a valid spec for every given commit in the repo.

#### Add worker API to openapi spec

##1733 by @ChrisSchinnerl

#### Allow for bypassing basic auth using a 'renterd_auth' cookie

Added a new `POST /api/auth` endpoint with a single required parameter 'validity'
(ms) which creates a new renterd auth token. The client can set that token as
the value of the 'renterd_auth' cookie to bypass basic auth for the duration of
the token's validity.

#### Implement support for pruning using RHP4

##1711 by @ChrisSchinnerl

Closes https://github.com/SiaFoundation/renterd/issues/1676

#### Remove HostIP and SiamuxAddr from ContractMetadata

##1712 by @chris124567

Implement https://github.com/SiaFoundation/renterd/issues/1691

### Fixes

- Automatically reset account drift using a rate limit
- Add sub packages to autopilot to prepare the move to the bus.
- Allow worker id to be empty when worker is disabled
- Automate changelog generation
- Build image from debian:bookworm-slim rather than scratch to avoid issue with read permissions when accessing SQLite temp dir
- Check for existing migration '00027_remove_directories' and skip both '00020_idx_db_directory' and '00020_remove_directories' if already applied.
- don't perform full slab prune on startup
- Don't return an error when a v1 contract doesn't contain any sectors to prune
- Fix bug where pruning large amounts of data from a contract would prune good sectors leading to lost sector alerts
- Fix bus configuration commands in issue template
- Fix deadlock in test suite
- Fix default settings not being used everywhere if settings are not found in database
- Fix DeleteHostSector deleting a sector from all hosts rather than the given one
- Fix edge case where contract gets refreshed immediately after being formed if host switches from v1 to v2 mid contract maintenance
- Fix error output in the renterd.log
- fix hosts that fail the pre-scan checks not having a failed scan recorded
- fix incorrect conversion of durations in gouging settings
- Fix issue where db migration 'migration_00020_idx_db_directory.sql' would fail due to a column being missing
- Fix memory manager for downloads using the uploads config
- fix migration failing due to locking table getting too big on large nodes
- Fix NDF in TestUsableHosts
- Fix potential deadlock/panic when forcing a scan
- Fix syntax error in migration 34
- Fix NDF in TestScoredHostsRandSelectByScore
- Fix upload bug that caused failures when uploading to contracts that are being refreshed in the background.
- Fix uploads that fail if a contract used in the upload was archived in the meantime
- Fixed data race when fetching a new price table
- Fragment the wallet into larger outputs.
- Have knope bot ignore conventional commits
- Improve migration out after foreignkey check fails
- Monitor file contracts and unlock their funds when they expire
- Move upload manager to internal package
- No longer use global var for token store and pass it into the handlers directly
- Prevent attempted write after closing the log file
- Prune host sectors in a background loop in small batches
- Reduce info level logging spam related to host scanning and pruning
- Remove internal imports in main package
- Remove unexported type from migrator.New
- Retry MySQL transactions on 'Lock wait timeout exceeded' errors
- Speed up slab pruning query
- Track accumulator state elements for file contracts in a new contract_elements table
- UI: Fixed an issue where sorting was not applied when in "all files" mode.
- UI: The total number of files no longer includes uploads in progress.
- Update API docs, fix use of additionalProperties, add more schemas to avoid example which is deprecated in open api 3.1.0
- Update core dependencies
- Update coreutils dependency to v0.10.0
- Update OpenAPI spec
- Update web to 0.72.0

#### Fix TestBlocklist

##1702 by @chris124567

Fix https://github.com/SiaFoundation/renterd/issues/1697

## 1.1.0

### Breaking changes

### Features

### Fixed
