# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/)
and this project adheres to [Semantic Versioning](http://semver.org/).

## [v0.6.5]

### Fixed
- Fix for type casting when using the "in" operator [#122](https://github.com/stac-utils/pgstac/issues/122)
- Fix failure of pypgstac load for large items [#121](https://github.com/stac-utils/pgstac/pull/121)

## [v0.6.4]

### Fixed
- Fixed casts for numeric data when a property is not in the queryables table to use the type from the incoming json filter
- Fixed issue loader grouping an unordered iterable by partition, speeding up loads of items with mixed partitions [#116](https://github.com/stac-utils/pgstac/pull/116)

## [v0.6.3]

### Fixed

- Fixed content_hydrate argument ordering which caused incorrect behavior in database hydration [#115](https://github.com/stac-utils/pgstac/pull/115)

### Added

- Skip partition updates when unnecessary, which can drastically improve large ingest performance into existing partitions. [#114](https://github.com/stac-utils/pgstac/pull/114)

## [v0.6.2]

### Fixed

- Ensure special keys are not in content when loaded [#112](https://github.com/stac-utils/pgstac/pull/112/files)

## [v0.6.1]

### Fixed
- Fix issue where using equality operator against an array was only comparing the first element of the array

## [v0.6.0]

### Fixed
- Fix function signatures for transactional functions (delete_item etc) to make sure that they are marked as volatile
- Fix function for getting start/end dates from a stac item
### Changed
- Update hydration/dehydration logic to make sure that it matches hydration/dehydration in pypgstac
- Update fields logic in pgstac to only use full paths and to match logic in stac-fastapi
- Always include id and collection on features regardless of fields setting
### Added
- Add tests to ensure that pgstac and pypgstac hydration logic is equivalent
- Add conf item to search to allow returning results without hydrating. This allows an application using pgstac to shift the CPU load of rehydrating items from the database onto the application server.
- Add "--dehydrated" option to loader to be able to load a dehydrated file (or iterable) of items such as would be output using pg_dump or postgresql copy.
- Add "--chunksize" option to loader that can split the processing of an iterable or file into chunks of n records at a time

## [v0.5.1]

### Fixed

### Changed

### Added

 - Add conf item to search to allow returning results without hydrating. This allows an application using pgstac to shift the CPU load of rehydrating items from the database onto the application server.

## [v0.5.0]
Version 0.5.0 is a major refactor of how data is stored. It is recommended to start a new database from scratch and to move data over rather than to use the inbuilt migration which will be very slow for larger amounts of data.

### Fixed

### Changed
 - The partition layout has been changed from being hardcoded to a partition to week to using nested partitions. The first level is by collection, for each collection, there is an attribute partition_trunc which can be set to NULL (no temporal partitions), month, or year.

 - CQL1 and Query Code have been refactored to translate to CQL2 to reduce duplicated code in query parsing.

 - Unused functions have been stripped from the project.

 - Pypgstac has been changed to use Fire rather than Typo.

 - Pypgstac has been changed to use Psycopg3 rather than Asyncpg to enable easier use as both sync and async.

 - Indexing has been reworked to eliminate indexes that from logs were not being used. The global json index on properties has been removed. Indexes on individual properties can be added either globally or per collection using the new queryables table.

 - Triggers for maintaining partitions have been updated to reduce lock contention and to reflect the new data layout.

 - The data pager which optimizes "order by datetime" searches has been updated to get time periods from the new partition layout and partition metadata.

 - Tests have been updated to reflect the many changes.

### Added

 - On ingest, the content in an item is compared to the metadata available at the collection level and duplicate information is stripped out (this is primarily data in the item_assets property). Logic is added in to merge this data back in on data usage.

## [v0.4.5]

### Fixed
 - Fixes support for using the intersects parameter at the base of a search (regression from changes in 0.4.4)
 - Fixes issue where results for a search on id returned [None] rather than [] (regression from changes in 0.4.4)


### Changed
- Changes requirement for PostgreSQL to 13+, the triggers used to main partitions are not available to be used on partitions prior to 13 ([#90](https://github.com/stac-utils/pgstac/pull/90))
- Bump requirement for asyncpg to 0.25.0 ([#82](https://github.com/stac-utils/pgstac/pull/82))

### Added
- Added more tests.

## [v0.4.4]

### Added
 - Adds support for using ids, collections, datetime, bbox, and intersects parameters separated from the filter-lang (Fixes #85)
   - Previously use of these parameters was translated into cql-json and then to SQL, so was not available when using cql2-json
   - The deprecated query parameter is still only available when filter-lang is set to cql-json


### Changed
- Add PLPGSQL for item lookups by id so that the query plan for the simple query can be cached
  - Use item_by_id function when looking up records used for paging filters
  - Add a short circuit to search to use item_by_id lookup when using the ids parameter
    - This short circuit avoids using the query cache for this simple case
    - Ordering when using the ids parameter is hard coded to return results in the same order as the array passed in (this avoids the overhead of full parsing and additional overhead to sort)

### Fixed
- Fix to make sure that filtering on the search_wheres table leverages the functional index on the hash of the query rather than on the query itself.

## [v0.4.3]

### Fixed

- Fix for optimization when using equals with json properties. Allow optimization for both "eq" and "=" (was only previously enabled for "eq")

## [v0.4.2]

### Changed

- Add support for updated CQL2 spec to use timestamp or interval key

### Fixed

- Fix for 0.3.4 -> 0.3.5 migration making sure that partitions get renamed correctly

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

[unreleased]: https://github.com/stac-utils/pgstac/compare/v0.6.5...HEAD
[v0.6.5]: https://github.com//stac-utils/pgstac/compare/v0.6.4...v0.6.5
[v0.6.4]: https://github.com//stac-utils/pgstac/compare/v0.6.3...v0.6.4
[v0.6.3]: https://github.com//stac-utils/pgstac/compare/v0.6.2...v0.6.3
[v0.6.2]: https://github.com//stac-utils/pgstac/compare/v0.6.1...v0.6.2
[v0.6.1]: https://github.com//stac-utils/pgstac/compare/v0.6.0...v0.6.1
[v0.6.0]: https://github.com//stac-utils/pgstac/compare/v0.5.1...v0.6.0
[v0.5.1]: https://github.com//stac-utils/pgstac/compare/v0.5.0...v0.5.1
[v0.5.0]: https://github.com//stac-utils/pgstac/compare/v0.4.5...v0.5.0
[v0.4.5]: https://github.com//stac-utils/pgstac/compare/v0.4.4...v0.4.5
[v0.4.4]: https://github.com//stac-utils/pgstac/compare/v0.4.3...v0.4.4
[v0.4.3]: https://github.com//stac-utils/pgstac/compare/v0.4.2...v0.4.3
[v0.4.2]: https://github.com//stac-utils/pgstac/compare/v0.4.1...v0.4.2
[v0.4.1]: https://github.com//stac-utils/pgstac/compare/v0.4.0...v0.4.1
[v0.4.0]: https://github.com//stac-utils/pgstac/compare/v0.3.4...v0.4.0
[v0.3.4]: https://github.com//stac-utils/pgstac/compare/v0.3.3...v0.3.4
[v0.3.3]: https://github.com//stac-utils/pgstac/compare/v0.3.2...v0.3.3
[v0.3.2]: https://github.com//stac-utils/pgstac/compare/v0.3.1...v0.3.2
[v0.3.1]: https://github.com//stac-utils/pgstac/compare/v0.3.0...v0.3.1
[v0.3.0]: https://github.com//stac-utils/pgstac/compare/v0.2.8...v0.3.0
[v0.2.8]: https://github.com//stac-utils/pgstac/compare/ff02c9cee7bbb0a2de21530b0aeb34e823f2e95c...v0.2.8
