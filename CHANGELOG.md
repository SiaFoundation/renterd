## 2.0.0 (2024-12-19)

### Breaking Changes

- Enforced S3 bucket name validation rules (https://github.com/SiaFoundation/renterd/pull/1722).
- Moved migrations to the autopilot (https://github.com/SiaFoundation/renterd/pull/1735).
- Removed contract sets (https://github.com/SiaFoundation/renterd/pull/1657).
- Removed support for multiple autopilots (https://github.com/SiaFoundation/renterd/pull/1657).
- Removed event webhooks (https://github.com/SiaFoundation/renterd/pull/1699).
- Removed partial setting updates (https://github.com/SiaFoundation/renterd/pull/1688).
- Updated `ContractMetadata` to exclude the host IP and siamux address (https://github.com/SiaFoundation/renterd/pull/1712).

### Features

- Integrated RHP4 support (https://github.com/SiaFoundation/renterd/pull/1723, https://github.com/SiaFoundation/renterd/pull/1710, https://github.com/SiaFoundation/renterd/pull/1711, https://github.com/SiaFoundation/renterd/pull/1680, https://github.com/SiaFoundation/renterd/pull/1693).
- Added OpenAPI documentation and validation to the API (https://github.com/SiaFoundation/renterd/pull/1724, https://github.com/SiaFoundation/renterd/pull/1733, https://github.com/SiaFoundation/renterd/pull/1737).
- Monitors file contracts and unlocks funds upon expiration (https://github.com/SiaFoundation/renterd/pull/1720).
- Removed the "current period" concept from the contractor (https://github.com/SiaFoundation/renterd/pull/1692).
- Updated autopilot configuration by removing the ability to toggle redundant host IPs (https://github.com/SiaFoundation/renterd/pull/1692).
- Updated gouging settings to remove the migration surcharge multiplier (https://github.com/SiaFoundation/renterd/pull/1696).
- Updated UI to version `v0.71.0`.

### Fixes

- Automated changelog generation (https://github.com/SiaFoundation/renterd/pull/1681).
- Automatically reset account drift using rate limiting (https://github.com/SiaFoundation/renterd/pull/1714).
- Fixed a bug where the low max duration was not considered when fetching hosts (https://github.com/SiaFoundation/renterd/pull/1755).
- Fixed default settings not being applied if missing in the database (https://github.com/SiaFoundation/renterd/pull/1761).
- Fixed a bug in host sector deletion (https://github.com/SiaFoundation/renterd/pull/1762).
- Fixed various non-deterministic failures (NDFs) in tests.
- Added `contract_elements` table to track accumulator state elements for file contracts (https://github.com/SiaFoundation/renterd/pull/1703).
- Added `host_sectors` table to track which sectors are stored on which hosts (https://github.com/SiaFoundation/renterd/pull/1704).
