# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/)
and this project adheres to [Semantic Versioning](http://semver.org/).

## [Unreleased]

## [v0.9.6]

### Added

- Add `load_queryables` function to pypgstac for loading queryables from a JSON file
- Add support for specifying collection IDs when loading queryables

### Fixed
- Added missing 0.8.6-0.9.0 migration script

## [v0.9.5]

### Changed

 - Pin to `plpygic>=0.5.0` and use `geom.ewkb` instead of `geom.wkt` when formatting items in `Loader.format_item`. Fixes (#357)

## [v0.9.4]

### Changed
 - Relax pypgstac dependencies

## [v0.9.3]

### Fixed

- Fix CI issue with tests not running
- Fix for issue with nulls in title or keywords for free text search

### Changed

- Replace hardcoded org name in CI

## [v0.9.2]

### Added

- Add limited support for free-text search in the search functions. (Fixes #293)
  - the `q` parameter is converted from the
    [OGC API - Features syntax](https://docs.ogc.org/DRAFTS/24-031.html) into a `tsquery`
    statement which is used to compare to the description, title, and keywords fields in items or collection_search
  - the text search is un-indexed and will be very slow for item-level searches!
  - Add support for Postgres 17
  - Support for adding data to the private field using the pypgstac loader

### Fixed

- Add `open=True` in `psycopg.ConnectionPool` to avoid future behavior change
- Switch from postgres `server_version` to `server_version_num` to get PG version (Fixes #300)
- Allow read-only replicas work even when the context extension is enabled (Fixes #300)
- Consistently ensure use of instantiated postgres fields when addressing with 'properties.' prefix

### Changed
- Move rust hydration to a separate repo

## [v0.9.1]

### Fixed

- Fixed double nested extent when using trigger based update collection extent. (Fixes #274)
- Fix time formatting (Fixes #275)
- Relaxes smart-open dependency check (Fixes #273)
- Switch to uv for docker image

## [v0.9.0]

### Breaking Changes

- Context Extension has been deprecated. Context is now reported using OGC Features compliant numberMatched and numberReturned
- Paging return from search using prev/next properties has been deprecated. Paging is now available in the spec compliant Links

### Added

- Add support for Casei and Accenti (Fixes #237). (Also, requires the addition of the unaccent extension)
- Add numberReturned and numberMatched fields for ItemCollection. BREAKING CHANGE: As the context extension is deprecated, this also removes the "context" item from results.
- Updated docs on automated updates of collection extents. (CLOSES #247)
- stac search now returns paging information using standards compliant links rather than prev/next properties (Fixes #265)

### Fixed

- Fixes issue when there is a None rather than an empty dictionary in hydration.
- Use "debug" log level rather than "log" to prevent growth in log messages due to differences in how client_min_messages and log_min_messages treat log levels. (Fixes #242)
- Refactor search_query and search_where functions to eliminate race condition when running identical queries. (Fixes #233)
- Fixes CQL2 Parser for Between operator (Fixes #251)
- Update PyO3 for rust hydration performance improvements.

## [v0.8.6]

### Fixed

 - Relax version requirement for smart-open (Fixes #273)
 - Use uv pip in docker build

## [v0.8.5]

### Fixed

- Fix issue when installing or migrating pgstac using a non superuser (particularly when using the default role found on RDS). (FIXES #239). Backports fix into migrations for 0.8.2, 0.8.3, and 0.8.4.
- Adds fixes/updates to documentation
- Fixes issue when using geometry with the strict queryables setting set.

## [v0.8.4]

### Fixed

- Make release deployment use postgres images without plrust
- Update versions of plrust in dockerfile (used for development, there is no plrust code yet)
- Update incremental migration tests to start at v0.3.0 rather than v0.1.9 due to a breaking change in pg_partman at version 5 that has no ability to pin a version. Migrating from prior to v0.3.0 should still work fine as long as pg_partman has not been updated on the database.

## [v0.8.3]

### Added

- Add support for arm64 to Docker images

### Fixed

- Fixes a critical bug when using the ingest_staging_upsert table or the upsert_item/upsert_items functions to update records with existing data where the existing row would get deleted, but the new row would not get added.

## [v0.8.2]

### Added

- Add support functions and tests for Collection Search
- Add configuration parameter for base_url to be able to generate absolute links
- With this release, this is only used to create links for paging in collection_search
- Adds read only mode to allow use of pgstac on read replicas
- Note: Turning on romode disables any caching (particularly when context is turned on) and does not allow to store q query hash that can be used with geometry_search.
- Add option to pypgstac loader "--usequeue" that forces use of the query queue for the loading process
- Add "pypgstac runqueue" command to run any commands that are set in the query queue

### Fixed

- Fix bug with end_datetime constraint management leading to inability to add data outside of constraints
- Fix bugs dealing with table ownership to ensure that all pgstac tables are owned by the pgstac_admin role
- Fixes issues with errors/warnings caused when doing index maintenance
- Fixes issues with errors/warnings caused with partition management
- Make sure that pgstac_ingest role always has read/write permissions on all tables
- Remove call to create_table_constraints from check_partition function. create_table_constraints was being called twice as it also gets called from update_partition_stats
- Add NOT NULL constraint to collections table (FIXES #224)
- Fix issue with indexes not getting created as the pg_admin role using SECURITY DEFINER

### Changed

- Revert pydantic requirement back to '>=1.7' and use basesettings conditionally from pydantic or pydantic.v1 to allow compatibility with pydantic 2 as well as with stac-fastapi that requires pydantic <2

## [v0.8.1]

### Fixed

- Fix issue with CI building/pushing docker images

## [v0.8.0]

### Fixed

- Revert an optimisation which limited the number of results from a search query to the number of item IDs specified in the query.
This fixes an issue where items with the same ID that are in multiple collections could be left out of search results.

### Changed

- update `pydantic` requirement to `~=2.0`
- update docker and ci workflows to build binary wheels for rust additions to pypgstac
- split docker into database service and python/rust container
- Modify scripts to auto-generate unreleased migration
- Add pre commit tasks to generate migration and to rebuild and compile pypgstac with maturin for rust
- Add private jsonb column to items and collections table to hold private metadata that should not be returned as part of a stac item
- Add generated columns to collections with the bounding box as a geometry and the datetime and end_datetime from the extents (this is to help with forthcoming work on collections search)
- Add PLRust to the Docker postgres image for forthcoming work to add optional PLRust functions for expensive json manipulation (including hydration)
- Remove default queryable for eo:cloud_cover

## [v0.7.10]

### Fixed

- Return an empty jsonb array from all_collections() when the collections table is empty, instead of NULL. Fixes #186.
- Add delete trigger to collections to clean up partition_stats records and remove any partitions. Fixes #185
- Fixes boolean casting in get_setting_bool function

## [v0.7.9]

### Fixed

- Update docker image to use postgis 3.3.3

## [v0.7.8]

### Fixed

- Fix issue with search_query not returning all fields on first use of a query. Fixes #182

## [v0.7.7]

### Fixed

- Fix migrations for 0.7.4->0.7.5 and 0.7.5->0.7.6 to use the partition_view rather than the materialized view to avoid issue with refreshing the materialized view when run in the same statement that is accessing the view. Fixes #180.

### Added

- Add a short cirucit for id searches that sets the limit to be no more than the number of ids in the filter.
- Add 'timing' configuration variable that adds a "timing" element to the return object with the amount of time that it took to return a search.
- Reduce locking when updating statistics in the search table. Use skip locked to skip updating last_used and count when there is a lock being held.

## [v0.7.6]

### Fixed

- Fix issue with checking for existing collections in queryable trigger function that prevented adding scoped queryable entries.

## [v0.7.5]

### Fixed

- Default sort not getting set when sortby not included in query with token (Fixes [#177](https://github.com/stac-utils/pgstac/issues/177))
- Fixes regression in performance between with changes for partition structure at v0.7.0. Changes the normal view for partitions and partition_steps into indexed materialized views. Adds refreshing of the views to existing triggers to make sure they stay up to date.

## [v0.7.4]

### Added

- Add --v and --vv options to scripts/test to change logging to notice / log when running tests.
- Add framework for option to cache expensive item formatting/hydrating calls. Note: this only provides functionality to add and read from the cached calls, but does not have any wiring to remove any entries from the cache.
- Update the costs for json formatting functions to 5000 to help the query planner choose to prefer using indexes on json fields.

### Fixed

- Fix bug in foreign key and unique collection detection in queryables trigger function, update tests to catch.
- Add collection id to tokens to ensure uniqueness and improve speed when looking up token values. Update tests to use the new keys. Old item id only tokens are still valid, but new results will all contain the new keys.
- Improve performance when looking for whether next/prev links should be added.
- Update Search function to remove the use of cursors and temp tables.
- Update get_token_filter to remove the use of temp tables.

## [v0.7.3]

### Fixed

- Use IF EXISTS when dropping constraints to avoid race conditions
- Rework function that finds indexes that need to be added to be added and to find functionally identical indexes better.

## [v0.7.2]

### Fixed

- Use version_parser for parsing versions in pypgstac
- Fix issue with dropping functions/procedures in 0.6.13->0.7.0 migrations
- Fix issue with CREATE OR REPLACE TRIGGER on PG 13
- Fix issue identifying duplicate indexes in maintain_partition_queries function
- Ensure that pgstac_read role has read permissions to all partitions
- Fix issue (and add tests) caused by bug in psycopg datetime types not being able to translate 'infinity', '-infinity'

## [v0.7.1]

### Fixed

- Fix permission issue when running incremental migrations.
- Make sure that pypgstac migrate runs in a single transaction
- Don't try to use concurrently when building indexes by default (this was tripping things up when using with pg_cron)
- Don't short circuit search for requests with ids (Fixes #159)
- Fix for issue with pagination when sorting by columns with nulls (Fixes #161 Fixes #152)
- Fixes issue where duplicate datetime,end_datetime index was being built.
- Fix bug in pypgstac loader when using delsert option

### Added

- Add trigger to detect duplicate configurations for name/collection combination in queryables
- Add trigger to ensure collections added to queryables exist
- Add tests for queryables triggers
- Add more tests for different pagination scenarios

## [v0.7.0]

### Added

- Reorganize code base to create clearer separation between pgstac sql code and pypgstac.
- Move Python tooling to use hatch with all python project configuration in pyproject.toml
- Rework testing framework to not rely on pypgstac or migrations. This allows to run tests on any code updates without creating a version first. If a new version has been staged, the tests will still run through all incremental migrations to make sure they pass as well.
- Add pre-commit to run formatting as well as the tests appropriate for which files have changed.
- Add a query queue to allow for deferred processing of steps that do not change the ability to get results, but enhance performance. The query queue allows to use pg_cron or similar to run tasks that are placed in the queue.
- Modify triggers to allow the use of the query queue for building indexes, adding constraints that are used solely for constraint exclusion, and updating partition and collection spatial and temporal extents. The use of the queue is controlled by the new configuration parameter "use_queue" which can be set as the pgstac.use_queue GUC or by setting in the pgstac_settings table.
- Reorganize how partitions are created and updated to maintain more metadata about partition extents and better tie the constraints to the actual temporal extent of a partition.
- Add "partitions" view that shows stats about number of records, the partition range, constraint ranges, actual date range and spatial extent of each partition.
- Add ability to automatically update the extent object on a collection using the partition metadata via triggers. This is controlled by the new configuration parameter "update_collection_extent" which can be set as the pgstac.update_collection_extent GUC or by setting in the pgstac_settings table. This can be combined with "use_queue" to defer the processing.
- Add many new tests.
- Migrations now make sure that all objects in the pgstac schema are owned by the pgstac_admin role. Functions marked as "SECURITY DEFINER" have been moved to the lower level functions responsible for creating/altering partitions and adding records to the search/search_wheres tables. This should open the door for approaches to using Row Level Security.
- Allow pypgstac loader to load data on pgstac databases that have the same major version even if minor version differs. [162] (<https://github.com/stac-utils/pgstac/issues/162>) Cherry picked from <https://github.com/stac-utils/pgstac/pull/164>.

### Fixed

- Allow empty strings in datetime intervals
- Set search_path and application_name upon connection rather than as kwargs for compatibility with RDS [156] (<https://github.com/stac-utils/pgstac/issues/156>)

## [v0.6.13]

### Fixed

- Fix issue with sorting and paging where in some circumstances the aggregation of data changed the expected order

## [v0.6.12]

### Added

- Add ability to merge enum, min, and max from queryables where collections have different values.
- Add tooling in pypgstac and pgstac to add stac_extension definitions to the database.
- Modify missing_queryables function to try to use stac_extension definitions to populate queryable definitions from the stac_extension schemas.
- Add validate_constraints procedure
- Add analyze_items procedure
- Add check_pgstac_settings function to check system and pgstac settings.

### Fixed

- Fix issue with upserts in the trigger for using the items_staging tables
- Fix for generating token query for sorting. [152] (<https://github.com/stac-utils/pgstac/pull/152>)

## [v0.6.11]

### Fixed

- update pypgstac requirements to support python 3.11 [142](https://github.com/stac-utils/pgstac/pull/142)
- rename pgstac setting `default-filter-lang` to `default_filter_lang` to allow pgstac on postgresql>=14

## [v0.6.10]

### Fixed

- Makes sure that passing in a non-existing collection does not return a queryable object.

## [v0.6.9]

### Fixed

- Set cursor_tuple_fraction to 1 in search function to let query planner know to expect the entire table result within the search function to be returned. The default cursor_tuple_fraction of .1 within that function was at times creating bad query plans leading to slow queries.

## [v0.6.8]

### Added

- Add get_queryables function to return a composite queryables json for either a single collection (text), a list of collections(text[]), or for the full repository (null::text).
- Add missing_queryables(collection text, tablesample int) function to help identify if there are any properties in a collection without entries in the queryables table. The tablesample parameter is an int <=100 that is the approximate percentage of the collection to scan to look for missing queryables rather than reading every item.
- Add missing_queryables(tablesample int) function that scans all collections using a sample of records to identify missing queryables.

## [v0.6.7]

### Added

- Add get_queryables function to return a composite queryables json for either a single collection (text), a list of collections(text[]), or for the full repository (null::text).
- Add missing_queryables(collection text, tablesample int) function to help identify if there are any properties in a collection without entries in the queryables table. The tablesample parameter is an int <=100 that is the approximate percentage of the collection to scan to look for missing queryables rather than reading every item.
- Add missing_queryables(tablesample int) function that scans all collections using a sample of records to identify missing queryables.

## [v0.6.6]

### Added

- Add support for array operators in CQL2 (a_equals, a_contains, a_contained_by, a_overlaps).
- Add check in loader to make sure that pypgstac and pgstac versions match before loading data [#123](https://github.com/stac-utils/pgstac/issues/123)

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
  - Adds tests for all examples in <https://github.com/radiantearth/stac-api-spec/blob/f5da775080ff3ff46d454c2888b6e796ee956faf/fragments/filter/README.md>
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

[v0.9.6]: https://github.com/stac-utils/pgstac/compare/v0.9.5...v0.9.6
[v0.9.5]: https://github.com/stac-utils/pgstac/compare/v0.9.4...v0.9.5
[v0.9.4]: https://github.com/stac-utils/pgstac/compare/v0.9.3...v0.9.4
[v0.9.3]: https://github.com/stac-utils/pgstac/compare/v0.9.2...v0.9.3
[v0.9.2]: https://github.com/stac-utils/pgstac/compare/v0.9.1...v0.9.2
[v0.9.1]: https://github.com/stac-utils/pgstac/compare/v0.9.0...v0.9.1
[v0.9.0]: https://github.com/stac-utils/pgstac/compare/v0.8.5...v0.9.0
[v0.8.5]: https://github.com/stac-utils/pgstac/compare/v0.8.4...v0.8.5
[v0.8.4]: https://github.com/stac-utils/pgstac/compare/v0.8.3...v0.8.4
[v0.8.3]: https://github.com/stac-utils/pgstac/compare/v0.8.2...v0.8.3
[v0.8.2]: https://github.com/stac-utils/pgstac/compare/v0.8.1...v0.8.2
[v0.8.1]: https://github.com/stac-utils/pgstac/compare/v0.8.0...v0.8.1
[v0.8.0]: https://github.com/stac-utils/pgstac/compare/v0.7.10...v0.8.0
[v0.7.10]: https://github.com/stac-utils/pgstac/compare/v0.7.9...v0.7.10
[v0.7.9]: https://github.com/stac-utils/pgstac/compare/v0.7.8...v0.7.9
[v0.7.8]: https://github.com/stac-utils/pgstac/compare/v0.7.7...v0.7.8
[v0.7.7]: https://github.com/stac-utils/pgstac/compare/v0.7.6...v0.7.7
[v0.7.6]: https://github.com/stac-utils/pgstac/compare/v0.7.5...v0.7.6
[v0.7.5]: https://github.com/stac-utils/pgstac/compare/v0.7.4...v0.7.5
[v0.7.4]: https://github.com/stac-utils/pgstac/compare/v0.7.3...v0.7.4
[v0.7.3]: https://github.com/stac-utils/pgstac/compare/v0.7.2...v0.7.3
[v0.7.2]: https://github.com/stac-utils/pgstac/compare/v0.7.1...v0.7.2
[v0.7.1]: https://github.com/stac-utils/pgstac/compare/v0.7.0...v0.7.1
[v0.7.0]: https://github.com/stac-utils/pgstac/compare/v0.6.13...v0.7.0
[v0.6.13]: https://github.com/stac-utils/pgstac/compare/v0.6.12...v0.6.13
[v0.6.12]: https://github.com/stac-utils/pgstac/compare/v0.6.11...v0.6.12
[v0.6.11]: https://github.com/stac-utils/pgstac/compare/v0.6.10...v0.6.11
[v0.6.10]: https://github.com/stac-utils/pgstac/compare/v0.6.9...v0.6.10
[v0.6.9]: https://github.com/stac-utils/pgstac/compare/v0.6.8...v0.6.9
[v0.6.8]: https://github.com/stac-utils/pgstac/compare/v0.6.7...v0.6.8
[v0.6.7]: https://github.com/stac-utils/pgstac/compare/v0.6.6...v0.6.7
[v0.6.6]: https://github.com/stac-utils/pgstac/compare/v0.6.5...v0.6.6
[v0.6.5]: https://github.com/stac-utils/pgstac/compare/v0.6.4...v0.6.5
[v0.6.4]: https://github.com/stac-utils/pgstac/compare/v0.6.3...v0.6.4
[v0.6.3]: https://github.com/stac-utils/pgstac/compare/v0.6.2...v0.6.3
[v0.6.2]: https://github.com/stac-utils/pgstac/compare/v0.6.1...v0.6.2
[v0.6.1]: https://github.com/stac-utils/pgstac/compare/v0.6.0...v0.6.1
[v0.6.0]: https://github.com/stac-utils/pgstac/compare/v0.5.1...v0.6.0
[v0.5.1]: https://github.com/stac-utils/pgstac/compare/v0.5.0...v0.5.1
[v0.5.0]: https://github.com/stac-utils/pgstac/compare/v0.4.5...v0.5.0
[v0.4.5]: https://github.com/stac-utils/pgstac/compare/v0.4.4...v0.4.5
[v0.4.4]: https://github.com/stac-utils/pgstac/compare/v0.4.3...v0.4.4
[v0.4.3]: https://github.com/stac-utils/pgstac/compare/v0.4.2...v0.4.3
[v0.4.2]: https://github.com/stac-utils/pgstac/compare/v0.4.1...v0.4.2
[v0.4.1]: https://github.com/stac-utils/pgstac/compare/v0.4.0...v0.4.1
[v0.4.0]: https://github.com/stac-utils/pgstac/compare/v0.3.4...v0.4.0
[v0.3.4]: https://github.com/stac-utils/pgstac/compare/v0.3.3...v0.3.4
[v0.3.3]: https://github.com/stac-utils/pgstac/compare/v0.3.2...v0.3.3
[v0.3.2]: https://github.com/stac-utils/pgstac/compare/v0.3.1...v0.3.2
[v0.3.1]: https://github.com/stac-utils/pgstac/compare/v0.3.0...v0.3.1
[v0.2.8]: https://github.com/stac-utils/pgstac/compare/ff02c9cee7bbb0a2de21530b0aeb34e823f2e95c...v0.2.8
