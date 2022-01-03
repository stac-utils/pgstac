# Changelog

## [v0.4.1]

### Changed

- Update `typer` to 0.4.0 to avoid clashes with `click` ([#76](https://github.com/stac-utils/pgstac/pull/76))

### Fixed

- Fix logic in getting settings to make sure that filter-lang set on query is respected. ([#77](https://github.com/stac-utils/pgstac/pull/77))
- Fix for large queries in the query cache. ([#71](https://github.com/stac-utils/pgstac/pull/71))


## [v0.4.0]

### Fixed
- Fixes syntax for IN, BETWEEN, ISNULL, and NOT in CQL 1 ([#69](https://github.com/stac-utils/pgstac/pull/69))

### Added
- Adds support for modifying settings through pgstac_settings table and by passing in 'conf' object in search json to support AWS RDS where custom user configuration settings are not allowed and changing settings on the fly for a given query.
- Adds support for CQL2-JSON ([#67](https://github.com/stac-utils/pgstac/pull/67))
  - Adds tests for all examples in https://github.com/radiantearth/stac-api-spec/blob/f5da775080ff3ff46d454c2888b6e796ee956faf/fragments/filter/README.md
  - filter-lang parameter controls which dialect of CQL to use
    - Adds 'default-filter-lang' setting to control what dialect to use when 'filter-lang' is not present
    - old style stac 'query' object and top level ids, collections, datetime, bbox, and intersects parameters are only available with cql-json

## [v0.3.4]

### Added

- add `geometrysearch`, `geojsonsearch` and `xyzsearch` for optimized searches for tiled requets ([#39](https://github.com/stac-utils/pgstac/pull/39))
- add `create_items` and `upsert_items` methods for bulk insert ([#39](https://github.com/stac-utils/pgstac/pull/39))

## [v0.3.3]

### Fixed

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
