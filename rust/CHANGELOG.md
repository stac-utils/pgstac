# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.4.9](https://github.com/stac-utils/rustac/compare/pgstac-v0.4.8...pgstac-v0.4.9) - 2026-04-07

### Added

- add generic search client traits and adapters ([#994](https://github.com/stac-utils/rustac/pull/994))

### Fixed

- more permissive deserialization, arrow v58 ([#985](https://github.com/stac-utils/rustac/pull/985))

### Other

- *(deps)* update geojson requirement from 0.24.1 to 1.0.0 ([#993](https://github.com/stac-utils/rustac/pull/993))

## [0.4.8](https://github.com/stac-utils/rustac/compare/pgstac-v0.4.7...pgstac-v0.4.8) - 2026-03-02

### Other

- updated the following local packages: stac

## [0.4.7](https://github.com/stac-utils/rustac/compare/pgstac-v0.4.6...pgstac-v0.4.7) - 2026-02-18

### Other

- create traits for clients ([#949](https://github.com/stac-utils/rustac/pull/949))

## [0.4.6](https://github.com/stac-utils/rustac/compare/pgstac-v0.4.5...pgstac-v0.4.6) - 2026-02-12

### Other

- updated the following local packages: stac

## [0.4.5](https://github.com/stac-utils/rustac/compare/pgstac-v0.4.4...pgstac-v0.4.5) - 2026-02-03

### Other

- updated the following local packages: stac

## [0.4.4](https://github.com/stac-utils/rustac/compare/pgstac-v0.4.3...pgstac-v0.4.4) - 2026-01-20

### Other

- updated the following local packages: stac

## [0.4.3](https://github.com/stac-utils/rustac/compare/pgstac-v0.4.2...pgstac-v0.4.3) - 2026-01-14

### Added

- search directly from pgstac ([#933](https://github.com/stac-utils/rustac/pull/933))

## [0.4.2](https://github.com/stac-utils/rustac/compare/pgstac-v0.4.1...pgstac-v0.4.2) - 2026-01-05

### Other

- updated the following local packages: stac

## [0.4.1](https://github.com/stac-utils/rustac/compare/pgstac-v0.4.0...pgstac-v0.4.1) - 2025-12-15

### Other

- switch to release-plz ([#911](https://github.com/stac-utils/rustac/pull/911))
- update releasing to be much simpler ([#899](https://github.com/stac-utils/rustac/pull/899))

## [0.4.0](https://github.com/stac-utils/rustac/compare/pgstac-v0.3.2...pgstac-v0.4.0) (2025-12-01)


### ⚠ BREAKING CHANGES

* move stac_api crate into stac crate ([#869](https://github.com/stac-utils/rustac/issues/869))
* remove unused error enums ([#868](https://github.com/stac-utils/rustac/issues/868))

### Bug Fixes

* remove unused error enums ([#868](https://github.com/stac-utils/rustac/issues/868)) ([cf0e815](https://github.com/stac-utils/rustac/commit/cf0e815e03433e8ef219a79a67161174f3e99e84))


### Code Refactoring

* move stac_api crate into stac crate ([#869](https://github.com/stac-utils/rustac/issues/869)) ([d0f7405](https://github.com/stac-utils/rustac/commit/d0f7405a811dd2c3b044404b4a6a48cf07926a89))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * stac bumped from 0.14.0 to 0.15.0

## [0.3.2] - 2025-11-14

Update **stac** dependency.

## [0.3.1] - 2025-09-19

Update **stac** dependency.

## [0.3.0] - 2025-01-14

### Added

- `Pgstac` trait ([#551](https://github.com/stac-utils/rustac/pull/551))
- `python` feature ([#558](https://github.com/stac-utils/rustac/pull/558))
- `readonly` ([#570](https://github.com/stac-utils/rustac/pull/570))
- `update_collection_extents` ([#574](https://github.com/stac-utils/rustac/pull/574))

### Changed

- Return JSON, not STAC ([#550](https://github.com/stac-utils/rustac/pull/550))

### Removed

- `Client` ([#551](https://github.com/stac-utils/rustac/pull/551))

## [0.2.2] - 2024-11-12

Bump dependencies.

## [0.2.1] - 2024-09-19

### Changed

- Bump **stac** to v0.10.0, **stac-api** to v0.6.0

## [0.2.0] - 2024-09-16

### Added

- Unverified tls provider ([#383](https://github.com/stac-utils/rustac/pull/383))

## [0.1.2] - 2024-09-05

### Changed

- Bump **stac** version to v0.9
- Bump **stac-api** version to v0.5

## [0.1.1] - 2024-08-12

### Changed

- Bump **pgstac** version to v0.8.6

## [0.1.0] - 2024-04-29

### Changed

- Moved from <https://github.com/stac-utils/pgrustac> to the <https://github.com/stac-utils/rustac> monorepo ([#246](https://github.com/stac-utils/rustac/pull/246))

## [0.0.6] - 2024-04-20

- Bump **stac** version to v0.6
- Bump **pgstac** version to v0.8.5

## [0.0.5] - 2023-09-25

- Bump **stac-api** version to v0.3.0

## [0.0.4] - 2023-07-07

- Bump **stac** version to v0.5
- Bump **pgstac** version to v0.6.13 ([#2](https://github.com/stac-utils/pgrustac/pull/2))

## [0.0.3] - 2023-01-08

### Changed

- `Client` now takes a reference to a generic client, instead of owning it

### Removed

- `Client::into_inner`

## [0.0.2] - 2023-01-08

### Changed

- Make `Error`, `Result`, and `Context` publicly visible

## [0.0.1] - 2023-01-07

Initial release

[unreleased]: https://github.com/stac-utils/rustac/compare/pgstac-v0.3.2...HEAD
[0.3.2]: https://github.com/stac-utils/rustac/compare/pgstac-v0.3.1..pgstac-v0.3.2
[0.3.1]: https://github.com/stac-utils/rustac/compare/pgstac-v0.3.0..pgstac-v0.3.1
[0.3.0]: https://github.com/stac-utils/rustac/compare/pgstac-v0.2.2..pgstac-v0.3.0
[0.2.2]: https://github.com/stac-utils/rustac/compare/pgstac-v0.2.1..pgstac-v0.2.2
[0.2.1]: https://github.com/stac-utils/rustac/compare/pgstac-v0.2.0..pgstac-v0.2.1
[0.2.0]: https://github.com/stac-utils/rustac/compare/pgstac-v0.1.2..pgstac-v0.2.0
[0.1.2]: https://github.com/stac-utils/rustac/compare/pgstac-v0.1.1..pgstac-v0.1.2
[0.1.1]: https://github.com/stac-utils/rustac/compare/pgstac-v0.1.0..pgstac-v0.1.1
[0.1.0]: https://github.com/stac-utils/rustac/releases/tag/pgstac-v0.1.0
[0.0.6]: https://github.com/stac-utils/pgrustac/compare/v0.0.5...v0.0.6
[0.0.5]: https://github.com/stac-utils/pgrustac/compare/v0.0.4...v0.0.5
[0.0.4]: https://github.com/stac-utils/pgrustac/compare/v0.0.3...v0.0.4
[0.0.3]: https://github.com/stac-utils/pgrustac/compare/v0.0.2...v0.0.3
[0.0.2]: https://github.com/stac-utils/pgrustac/compare/v0.0.1...v0.0.2
[0.0.1]: https://github.com/stac-utils/pgrustac/tree/v0.0.1

<!-- markdownlint-disable-file MD024 -->
