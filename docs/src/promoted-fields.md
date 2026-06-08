# Promoted Fields

!!! note "This document is for pgstac developers, not operators or API users"
    The promoted field set is part of pgstac's internal schema and is only changed by pgstac maintainers as part of a **major release**. Operators and API users do not need to read or act on this document.

    **You do not need to do anything special to use non-promoted fields.** Every STAC property — including fields from extensions not listed here, vendor-specific properties, and future extension versions — is stored in the `properties` JSONB column and is fully queryable via CQL2. Promotion is purely a performance optimisation for a curated set of well-known, high-cardinality fields: it moves them into typed native columns so that index-only scans are possible. Unpromoted fields work correctly in all circumstances; they just rely on JSONB extraction at query time rather than a native column comparison.

    The promoted field list is reviewed and updated **at major pgstac releases**, aligned with the latest stable versions of the STAC core spec and the extensions listed below. Minor or patch releases do not change the column set.

## How Promotion Works

At ingest, `content_dehydrate()` reads each property out of `item.properties`, casts it to the target SQL type, and writes it into the matching column. At read time, `promoted_properties_from_item()` places the column values back into the STAC JSON. The residual properties (everything _not_ promoted) remain in the `properties` jsonb column.

Promoted fields are automatically registered as queryables, so CQL2 filters on any promoted field resolve to a direct column comparison rather than a JSONB path extraction. No manual `queryables` insert is needed when adding a new promoted field; `promoted_queryables_defaults()` generates the seed rows at install and upgrade time.

### COLUMN LIST SYNC CONTRACT

Adding or removing a promoted field requires updating **all six** of these locations in [`src/pgstac/sql/003a_items.sql`](../../src/pgstac/sql/003a_items.sql) and [`src/pgstac/sql/002a_queryables.sql`](../../src/pgstac/sql/002a_queryables.sql) together:

1. `items` TABLE DDL — the column declaration itself
2. `content_dehydrate()` — extracts the value from JSONB at ingest
3. `promoted_item_property_defs()` — the metadata registry (name → column mapping)
4. `promoted_properties_from_item()` — rehydrates promoted columns back to JSONB
5. `promoted_items_column_list()` — ordered array; drives auto-derived logic
6. `items_staging_dehydrate()` — the enriched SELECT for all staging branches

`strip_promoted_properties()` and queryable seeding auto-derive from `promoted_item_property_defs()` and require no manual edit.

---

## STAC Core Common Metadata

**Spec:** [STAC Core — Common Metadata](https://github.com/radiantearth/stac-spec/blob/master/item-spec/common-metadata.md)
**Spec version:** STAC 1.1

| STAC property | Column | SQL type | Description |
|---|---|---|---|
| `created` | `created` | `timestamptz` | Metadata creation timestamp |
| `updated` | `updated` | `timestamptz` | Metadata last-update timestamp |
| `platform` | `platform` | `text` | Platform (satellite/sensor) name |
| `instruments` | `instruments` | `text[]` | Instrument name(s) |
| `constellation` | `constellation` | `text` | Constellation name |
| `mission` | `mission` | `text` | Mission name |
| `gsd` | `gsd` | `float8` | Ground sample distance (metres) |
| `bands` | `bands` | `jsonb` | Bands array (STAC 1.1 core; successor of the deprecated `eo:bands`) |

---

## EO Extension

**Extension:** [`eo`](https://github.com/stac-extensions/eo)
**Extension version:** 2.0.0
**Spec URL:** https://stac-extensions.github.io/eo/v2.0.0/schema.json

> **Note:** `eo:bands` was removed from this extension in v2.0.0 and replaced by `bands` in STAC core common metadata. Do not re-add `eo:bands` as a promoted field.

| STAC property | Column | SQL type | Description |
|---|---|---|---|
| `eo:cloud_cover` | `eo_cloud_cover` | `float8` | Cloud cover percentage (0–100) |
| `eo:snow_cover` | `eo_snow_cover` | `float8` | Snow and ice cover percentage (0–100) |

---

## Projection Extension

**Extension:** [`proj`](https://github.com/stac-extensions/projection)
**Extension version:** 2.0.0
**Spec URL:** https://stac-extensions.github.io/projection/v2.0.0/schema.json

> **Note:** `proj:epsg` (integer) was removed in v2.0.0 and replaced by `proj:code` (authority+code string, e.g. `"EPSG:32659"`). Do not re-add `proj:epsg`.

| STAC property | Column | SQL type | Description |
|---|---|---|---|
| `proj:code` | `proj_code` | `text` | CRS authority and code (e.g. `"EPSG:32659"`) |
| `proj:geometry` | `proj_geometry` | `jsonb` | Item footprint in its native CRS |
| `proj:wkt2` | `proj_wkt2` | `text` | WKT2 CRS definition string |
| `proj:projjson` | `proj_projjson` | `jsonb` | PROJJSON CRS definition object |
| `proj:bbox` | `proj_bbox` | `jsonb` | Bounding box in the native CRS (`[xmin, ymin, xmax, ymax]`) |
| `proj:centroid` | `proj_centroid` | `jsonb` | Centroid in the native CRS (`{"lat":…,"lon":…}`) |
| `proj:shape` | `proj_shape` | `jsonb` | Number of pixels in Y and X directions (`[rows, cols]`) |
| `proj:transform` | `proj_transform` | `jsonb` | Affine transform coefficients (9-element array) |

---

## Scientific Citation Extension

**Extension:** [`sci`](https://github.com/stac-extensions/scientific)
**Extension version:** 1.0.0
**Spec URL:** https://stac-extensions.github.io/scientific/v1.0.0/schema.json

| STAC property | Column | SQL type | Description |
|---|---|---|---|
| `sci:doi` | `sci_doi` | `text` | Digital Object Identifier |
| `sci:citation` | `sci_citation` | `text` | Recommended human-readable citation |
| `sci:publications` | `sci_publications` | `jsonb` | Array of publication objects |

---

## View Geometry Extension

**Extension:** [`view`](https://github.com/stac-extensions/view)
**Extension version:** 1.1.0
**Spec URL:** https://stac-extensions.github.io/view/v1.1.0/schema.json

| STAC property | Column | SQL type | Description |
|---|---|---|---|
| `view:off_nadir` | `view_off_nadir` | `float8` | Off-nadir angle (degrees, 0–90) |
| `view:incidence_angle` | `view_incidence_angle` | `float8` | Incidence angle (degrees, 0–90) |
| `view:azimuth` | `view_azimuth` | `float8` | Viewing direction azimuth (degrees, 0–360) |
| `view:sun_azimuth` | `view_sun_azimuth` | `float8` | Sun azimuth (degrees, 0–360) |
| `view:sun_elevation` | `view_sun_elevation` | `float8` | Sun elevation (degrees, −90–90) |
| `view:moon_azimuth` | `view_moon_azimuth` | `float8` | Moon azimuth (degrees, 0–360) |
| `view:moon_elevation` | `view_moon_elevation` | `float8` | Moon elevation (degrees, −90–90) |

---

## File Info Extension

**Extension:** [`file`](https://github.com/stac-extensions/file)
**Extension version:** 2.1.0
**Spec URL:** https://stac-extensions.github.io/file/v2.1.0/schema.json

| STAC property | Column | SQL type | Description |
|---|---|---|---|
| `file:size` | `file_size` | `bigint` | File size in bytes |
| `file:header_size` | `file_header_size` | `bigint` | Header size in bytes |
| `file:checksum` | `file_checksum` | `text` | Multihash checksum string |
| `file:byte_order` | `file_byte_order` | `text` | Byte order (`big-endian` or `little-endian`) |

---

## SAR Satellites Extension

**Extension:** [`sat`](https://github.com/stac-extensions/sat)
**Extension version:** 1.2.0
**Spec URL:** https://stac-extensions.github.io/sat/v1.2.0/schema.json

| STAC property | Column | SQL type | Description |
|---|---|---|---|
| `sat:orbit_state` | `sat_orbit_state` | `text` | Orbit state (`ascending` or `descending`) |
| `sat:relative_orbit` | `sat_relative_orbit` | `integer` | Relative orbit number |
| `sat:absolute_orbit` | `sat_absolute_orbit` | `integer` | Absolute orbit number |
| `sat:platform_international_designator` | `sat_platform_international_designator` | `text` | COSPAR International Designator |
| `sat:anx_datetime` | `sat_anx_datetime` | `timestamptz` | Ascending node crossing datetime |

---

## Machine-Readable Field Registry

The YAML block below is the canonical reference for tooling and AI-assisted code changes. When updating the promoted field set, update both this YAML block and the SQL source files in [`src/pgstac/sql/`](../../src/pgstac/sql/) per the [COLUMN LIST SYNC CONTRACT](#column-list-sync-contract).

```yaml
# promoted-fields registry
# stac_property: the JSON property key in item.properties (or item root for top-level)
# column:        PostgreSQL column name on the items table
# pg_type:       PostgreSQL column type
# nullable:      always true for promoted fields
# source:        stac_core or the extension name
# source_version: STAC spec or extension version this field was introduced/last aligned to
# spec_url:      canonical JSON Schema URL for the source spec
# notes:         backward-compatibility or migration notes

fields:

  # ── STAC core common metadata (STAC 1.1) ──────────────────────────────────
  - stac_property:    created
    column:           created
    pg_type:          timestamptz
    nullable:         true
    source:           stac_core
    source_version:   "1.1"
    spec_url:         https://github.com/radiantearth/stac-spec/blob/master/item-spec/common-metadata.md

  - stac_property:    updated
    column:           updated
    pg_type:          timestamptz
    nullable:         true
    source:           stac_core
    source_version:   "1.1"
    spec_url:         https://github.com/radiantearth/stac-spec/blob/master/item-spec/common-metadata.md

  - stac_property:    platform
    column:           platform
    pg_type:          text
    nullable:         true
    source:           stac_core
    source_version:   "1.1"
    spec_url:         https://github.com/radiantearth/stac-spec/blob/master/item-spec/common-metadata.md

  - stac_property:    instruments
    column:           instruments
    pg_type:          "text[]"
    nullable:         true
    source:           stac_core
    source_version:   "1.1"
    spec_url:         https://github.com/radiantearth/stac-spec/blob/master/item-spec/common-metadata.md

  - stac_property:    constellation
    column:           constellation
    pg_type:          text
    nullable:         true
    source:           stac_core
    source_version:   "1.1"
    spec_url:         https://github.com/radiantearth/stac-spec/blob/master/item-spec/common-metadata.md

  - stac_property:    mission
    column:           mission
    pg_type:          text
    nullable:         true
    source:           stac_core
    source_version:   "1.1"
    spec_url:         https://github.com/radiantearth/stac-spec/blob/master/item-spec/common-metadata.md

  - stac_property:    gsd
    column:           gsd
    pg_type:          float8
    nullable:         true
    source:           stac_core
    source_version:   "1.1"
    spec_url:         https://github.com/radiantearth/stac-spec/blob/master/item-spec/common-metadata.md

  - stac_property:    bands
    column:           bands
    pg_type:          jsonb
    nullable:         true
    source:           stac_core
    source_version:   "1.1"
    spec_url:         https://github.com/radiantearth/stac-spec/blob/master/item-spec/common-metadata.md
    notes: >
      Successor of eo:bands from the EO extension. Moved to STAC core in v1.1.
      eo:bands is no longer a promoted field.

  # ── EO Extension v2.0.0 ───────────────────────────────────────────────────
  - stac_property:    "eo:cloud_cover"
    column:           eo_cloud_cover
    pg_type:          float8
    nullable:         true
    source:           eo
    source_version:   "2.0.0"
    spec_url:         https://stac-extensions.github.io/eo/v2.0.0/schema.json

  - stac_property:    "eo:snow_cover"
    column:           eo_snow_cover
    pg_type:          float8
    nullable:         true
    source:           eo
    source_version:   "2.0.0"
    spec_url:         https://stac-extensions.github.io/eo/v2.0.0/schema.json

  # ── Projection Extension v2.0.0 ───────────────────────────────────────────
  - stac_property:    "proj:code"
    column:           proj_code
    pg_type:          text
    nullable:         true
    source:           proj
    source_version:   "2.0.0"
    spec_url:         https://stac-extensions.github.io/projection/v2.0.0/schema.json
    notes: >
      Replaced proj:epsg (integer) in v2.0.0. Stores authority+code string
      (e.g. "EPSG:32659"). proj:epsg is no longer a promoted field.

  - stac_property:    "proj:geometry"
    column:           proj_geometry
    pg_type:          jsonb
    nullable:         true
    source:           proj
    source_version:   "2.0.0"
    spec_url:         https://stac-extensions.github.io/projection/v2.0.0/schema.json

  - stac_property:    "proj:wkt2"
    column:           proj_wkt2
    pg_type:          text
    nullable:         true
    source:           proj
    source_version:   "2.0.0"
    spec_url:         https://stac-extensions.github.io/projection/v2.0.0/schema.json

  - stac_property:    "proj:projjson"
    column:           proj_projjson
    pg_type:          jsonb
    nullable:         true
    source:           proj
    source_version:   "2.0.0"
    spec_url:         https://stac-extensions.github.io/projection/v2.0.0/schema.json

  - stac_property:    "proj:bbox"
    column:           proj_bbox
    pg_type:          jsonb
    nullable:         true
    source:           proj
    source_version:   "2.0.0"
    spec_url:         https://stac-extensions.github.io/projection/v2.0.0/schema.json

  - stac_property:    "proj:centroid"
    column:           proj_centroid
    pg_type:          jsonb
    nullable:         true
    source:           proj
    source_version:   "2.0.0"
    spec_url:         https://stac-extensions.github.io/projection/v2.0.0/schema.json

  - stac_property:    "proj:shape"
    column:           proj_shape
    pg_type:          jsonb
    nullable:         true
    source:           proj
    source_version:   "2.0.0"
    spec_url:         https://stac-extensions.github.io/projection/v2.0.0/schema.json

  - stac_property:    "proj:transform"
    column:           proj_transform
    pg_type:          jsonb
    nullable:         true
    source:           proj
    source_version:   "2.0.0"
    spec_url:         https://stac-extensions.github.io/projection/v2.0.0/schema.json

  # ── Scientific Citation Extension v1.0.0 ──────────────────────────────────
  - stac_property:    "sci:doi"
    column:           sci_doi
    pg_type:          text
    nullable:         true
    source:           sci
    source_version:   "1.0.0"
    spec_url:         https://stac-extensions.github.io/scientific/v1.0.0/schema.json

  - stac_property:    "sci:citation"
    column:           sci_citation
    pg_type:          text
    nullable:         true
    source:           sci
    source_version:   "1.0.0"
    spec_url:         https://stac-extensions.github.io/scientific/v1.0.0/schema.json

  - stac_property:    "sci:publications"
    column:           sci_publications
    pg_type:          jsonb
    nullable:         true
    source:           sci
    source_version:   "1.0.0"
    spec_url:         https://stac-extensions.github.io/scientific/v1.0.0/schema.json

  # ── View Geometry Extension v1.1.0 ────────────────────────────────────────
  - stac_property:    "view:off_nadir"
    column:           view_off_nadir
    pg_type:          float8
    nullable:         true
    source:           view
    source_version:   "1.1.0"
    spec_url:         https://stac-extensions.github.io/view/v1.1.0/schema.json

  - stac_property:    "view:incidence_angle"
    column:           view_incidence_angle
    pg_type:          float8
    nullable:         true
    source:           view
    source_version:   "1.1.0"
    spec_url:         https://stac-extensions.github.io/view/v1.1.0/schema.json

  - stac_property:    "view:azimuth"
    column:           view_azimuth
    pg_type:          float8
    nullable:         true
    source:           view
    source_version:   "1.1.0"
    spec_url:         https://stac-extensions.github.io/view/v1.1.0/schema.json

  - stac_property:    "view:sun_azimuth"
    column:           view_sun_azimuth
    pg_type:          float8
    nullable:         true
    source:           view
    source_version:   "1.1.0"
    spec_url:         https://stac-extensions.github.io/view/v1.1.0/schema.json

  - stac_property:    "view:sun_elevation"
    column:           view_sun_elevation
    pg_type:          float8
    nullable:         true
    source:           view
    source_version:   "1.1.0"
    spec_url:         https://stac-extensions.github.io/view/v1.1.0/schema.json

  - stac_property:    "view:moon_azimuth"
    column:           view_moon_azimuth
    pg_type:          float8
    nullable:         true
    source:           view
    source_version:   "1.1.0"
    spec_url:         https://stac-extensions.github.io/view/v1.1.0/schema.json

  - stac_property:    "view:moon_elevation"
    column:           view_moon_elevation
    pg_type:          float8
    nullable:         true
    source:           view
    source_version:   "1.1.0"
    spec_url:         https://stac-extensions.github.io/view/v1.1.0/schema.json

  # ── File Info Extension v2.1.0 ────────────────────────────────────────────
  - stac_property:    "file:size"
    column:           file_size
    pg_type:          bigint
    nullable:         true
    source:           file
    source_version:   "2.1.0"
    spec_url:         https://stac-extensions.github.io/file/v2.1.0/schema.json

  - stac_property:    "file:header_size"
    column:           file_header_size
    pg_type:          bigint
    nullable:         true
    source:           file
    source_version:   "2.1.0"
    spec_url:         https://stac-extensions.github.io/file/v2.1.0/schema.json

  - stac_property:    "file:checksum"
    column:           file_checksum
    pg_type:          text
    nullable:         true
    source:           file
    source_version:   "2.1.0"
    spec_url:         https://stac-extensions.github.io/file/v2.1.0/schema.json

  - stac_property:    "file:byte_order"
    column:           file_byte_order
    pg_type:          text
    nullable:         true
    source:           file
    source_version:   "2.1.0"
    spec_url:         https://stac-extensions.github.io/file/v2.1.0/schema.json

  # ── SAR Satellites Extension v1.2.0 ───────────────────────────────────────
  - stac_property:    "sat:orbit_state"
    column:           sat_orbit_state
    pg_type:          text
    nullable:         true
    source:           sat
    source_version:   "1.2.0"
    spec_url:         https://stac-extensions.github.io/sat/v1.2.0/schema.json

  - stac_property:    "sat:relative_orbit"
    column:           sat_relative_orbit
    pg_type:          integer
    nullable:         true
    source:           sat
    source_version:   "1.2.0"
    spec_url:         https://stac-extensions.github.io/sat/v1.2.0/schema.json

  - stac_property:    "sat:absolute_orbit"
    column:           sat_absolute_orbit
    pg_type:          integer
    nullable:         true
    source:           sat
    source_version:   "1.2.0"
    spec_url:         https://stac-extensions.github.io/sat/v1.2.0/schema.json

  - stac_property:    "sat:platform_international_designator"
    column:           sat_platform_international_designator
    pg_type:          text
    nullable:         true
    source:           sat
    source_version:   "1.2.0"
    spec_url:         https://stac-extensions.github.io/sat/v1.2.0/schema.json

  - stac_property:    "sat:anx_datetime"
    column:           sat_anx_datetime
    pg_type:          timestamptz
    nullable:         true
    source:           sat
    source_version:   "1.2.0"
    spec_url:         https://stac-extensions.github.io/sat/v1.2.0/schema.json

removed_fields:
  # Fields that were once promoted and have been removed. Do not re-add.
  - stac_property:    "proj:epsg"
    removed_in_pgstac: unreleased
    replaced_by:      "proj:code"
    reason: >
      Projection extension v2.0.0 replaced the integer EPSG code with a
      authority+code string (proj:code) to support non-EPSG CRS authorities.

  - stac_property:    "eo:bands"
    removed_in_pgstac: unreleased
    replaced_by:      bands
    reason: >
      STAC 1.1 moved bands to core common metadata. eo:bands is deprecated
      in EO extension v2.0.0.

  - stac_property:    "file:values_regex"
    removed_in_pgstac: unreleased
    replaced_by:      ~
    reason: Not a field in file extension v2.1.0.
```

---

## Adding or Removing a Promoted Field

> **Developers only.** This section applies only to pgstac maintainers preparing a major release. Operators should not modify the promoted column set; doing so would put the schema out of sync with pgstac's internal functions and break migrations.
>
> Promotion is a performance decision, not a correctness one. A field that is not promoted still works — it is stored in `properties` and is fully accessible via CQL2 filters and the STAC API. Only promote a field when it is stable (unlikely to be renamed or removed), widely used across collections, and expected to benefit significantly from indexed column access.

Changes to the promoted field set are made **only at major pgstac releases**, aligned with breaking extension spec changes. Do not promote fields mid-release-cycle; incremental migrations that add or drop columns require coordinated schema and function changes across many locations.

When a STAC extension releases a new version that adds, removes, or renames a field you want to promote:

1. Update this document — both the human-readable table for the relevant extension section and the YAML registry (add to `fields` or move to `removed_fields`).
2. Apply the [COLUMN LIST SYNC CONTRACT](#column-list-sync-contract) — all six SQL locations.
3. For a removal, add an `ALTER TABLE items DROP COLUMN <column>` to the incremental migration.
4. For an addition, add `ALTER TABLE items ADD COLUMN <column> <type>` to the incremental migration, then a `UPDATE items SET <column> = ...` backfill if needed.
5. Update `CHANGELOG.md` under `## [Unreleased]` and mirror to `docs/src/release-notes.md`.
6. Run `scripts/test` to verify.
