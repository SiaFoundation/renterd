## 2.0.0 (2024-12-19)

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

### Features

- Add host_sectors table
- Fix 00030_host_sectors migration on DBs that have a host_sectors table
- Integrate RHP4 for uploads
- Move hosts allowRedundantIPs config
- Remove current period
- Remove migrationSurchargeMultiplier from the gouging settings.
- Update UI to v0.68.0

#### Add openapi.yml file with autopilot routes

Added an openapi.yml spec with the specifications for the autopilot routes and a CI step to validate it. The goal is to eventually have a complete spec for the V2 API that we can use to generate API docs as well as making sure that there is always a valid spec for every given commit in the repo.

#### Add worker API to openapi spec

##1733 by @ChrisSchinnerl

#### Implement support for pruning using RHP4

##1711 by @ChrisSchinnerl

Closes https://github.com/SiaFoundation/renterd/issues/1676

#### Remove HostIP and SiamuxAddr from ContractMetadata

##1712 by @chris124567

Implement https://github.com/SiaFoundation/renterd/issues/1691

### Fixes

- Automatically reset account drift using a rate limit
- Automate changelog generation
- Check for existing migration '00027_remove_directories' and skip both '00020_idx_db_directory' and '00020_remove_directories' if already applied.
- Fix default settings not being used everywhere if settings are not found in database
- Fix DeleteHostSector deleting a sector from all hosts rather than the given one
- Fix NDF in TestUsableHosts
- Fix NDF in TestScoredHostsRandSelectByScore
- Have knope bot ignore conventional commits
- Improve migration out after foreignkey check fails
- Monitor file contracts and unlock their funds when they expire
- Move upload manager to internal package
- Track accumulator state elements for file contracts in a new contract_elements table
- Update OpenAPI spec

#### Fix TestBlocklist

##1702 by @chris124567

Fix https://github.com/SiaFoundation/renterd/issues/1697

## 1.1.0

### Breaking changes

### Features

### Fixed
