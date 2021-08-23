# Changelog

## [v0.3.4]

### Added

- add `geometrysearch`, `geojsonsearch` and `xyzsearch` for optimized searches for tiled requets ([#39](https://github.com/stac-utils/pgstac/pull/39))

## [v0.3.3]

## Fixed

- Fixed CQL term to be "id", not "ids" ([#46](https://github.com/stac-utils/pgstac/pull/46))
- Make sure featureCollection response has empty features `[]` not `null` ([#46](https://github.com/stac-utils/pgstac/pull/46))
- Fixed bugs for `sortby` and `pagination` ([#46](https://github.com/stac-utils/pgstac/pull/46))
- Make sure pgtap errors get caught in CI ([#46](https://github.com/stac-utils/pgstac/pull/46))

## [v0.3.2]

## Fixed

- Fixed CQL term to be "collections", not "collection" ([#43](https://github.com/stac-utils/pgstac/pull/43))

## [v0.3.1]

_TODO_

## [v0.2.8]

### Added

- Type hints to pypgstac that pass mypy checks ([#18](https://github.com/stac-utils/pgstac/pull/18))

### Fixed

- Fixed issue with pypgstac loads which caused some writes to fail ([#18](https://github.com/stac-utils/pgstac/pull/18))

[Unreleased]: <https://github.com/stac-utils/pgstac/compare/v0.2.8..main>
[v0.2.8]: <https://github.com/stac-utils/pgstac/compare/v0.2.7..v0.2.8>
