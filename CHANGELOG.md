## 2.0.0 (2024-12-20)

### Breaking Changes

- Enforced S3 bucket name validation rules ([#1722](https://github.com/SiaFoundation/renterd/pull/1722)).
- Moved migrations to the autopilot ([#1735](https://github.com/SiaFoundation/renterd/pull/1735)).
- Removed contract sets ([#1657](https://github.com/SiaFoundation/renterd/pull/1657)).
- Removed support for multiple autopilots ([#1657](https://github.com/SiaFoundation/renterd/pull/1657)).
- Removed event webhooks ([#1699](https://github.com/SiaFoundation/renterd/pull/1699)).
- Removed partial setting updates ([#1688](https://github.com/SiaFoundation/renterd/pull/1688)).
- Updated `ContractMetadata` to exclude the host IP and siamux address ([#1712](https://github.com/SiaFoundation/renterd/pull/1712)).

### Features

- Integrated RHP4 support ([#1723](https://github.com/SiaFoundation/renterd/pull/1723), [#1710](https://github.com/SiaFoundation/renterd/pull/1710), [#1711](https://github.com/SiaFoundation/renterd/pull/1711), [#1680](https://github.com/SiaFoundation/renterd/pull/1680), [#1693](https://github.com/SiaFoundation/renterd/pull/1693)).
- Added OpenAPI documentation and validation to the API ([#1724](https://github.com/SiaFoundation/renterd/pull/1724), [#1733](https://github.com/SiaFoundation/renterd/pull/1733), [#1737](https://github.com/SiaFoundation/renterd/pull/1737)).
- Monitors file contracts and unlocks funds upon expiration ([#1720](https://github.com/SiaFoundation/renterd/pull/1720)).
- Removed the "current period" concept from the contractor ([#1692](https://github.com/SiaFoundation/renterd/pull/1692)).
- Updated autopilot configuration by removing the ability to toggle redundant host IPs ([#1692](https://github.com/SiaFoundation/renterd/pull/1692)).
- Updated gouging settings to remove the migration surcharge multiplier ([#1696](https://github.com/SiaFoundation/renterd/pull/1696)).
- Updated UI to version `v0.72.0`. ([#1764](https://github.com/SiaFoundation/renterd/pull/1764)).

### Fixes

- Automated changelog generation ([#1681](https://github.com/SiaFoundation/renterd/pull/1681)).
- Automatically reset account drift using rate limiting ([#1714](https://github.com/SiaFoundation/renterd/pull/1714)).
- Fixed a bug where the low max duration was not considered when fetching hosts ([#1755](https://github.com/SiaFoundation/renterd/pull/1755)).
- Fixed default settings not being applied if missing in the database ([#1761](https://github.com/SiaFoundation/renterd/pull/1761)).
- Fixed a bug in host sector deletion ([#1762](https://github.com/SiaFoundation/renterd/pull/1762)).
- Fixed various non-deterministic failures (NDFs) in tests.
- Added `contract_elements` table to track accumulator state elements for file contracts ([#1703](https://github.com/SiaFoundation/renterd/pull/1703)).
- Added `host_sectors` table to track which sectors are stored on which hosts ([#1704](https://github.com/SiaFoundation/renterd/pull/1704)).