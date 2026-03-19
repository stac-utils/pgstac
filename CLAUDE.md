# PgSTAC Development Instructions

## Project Overview

PgSTAC is a PostgreSQL extension (SQL functions + schema) for Spatio-Temporal Asset Catalogs (STAC), paired with pypgstac, a Python package for database migrations and bulk data ingestion.

- **Repository**: stac-utils/pgstac
- **License**: MIT
- **Docs**: https://stac-utils.github.io/pgstac/

## Architecture

```
src/pgstac/sql/          ← ALL SQL source files (edit ONLY here)
src/pgstac/pgstac.sql    ← Assembled output (DO NOT edit directly)
src/pgstac/migrations/   ← Base + incremental migration files
src/pgstac/tests/        ← PGTap and basic SQL tests
src/pypgstac/src/pypgstac/ ← Python package source
src/pypgstac/tests/        ← pytest tests
docker/pypgstac/bin/     ← Build/test/utility scripts (pgstac_restore, test, etc.)
```

### Documentation Files

- **`CHANGELOG.md`** — the single source of truth for release notes
- **`docs/src/release-notes.md`** — a **manual copy** of `CHANGELOG.md`, served by mkdocs. Keep them identical; update both when changing either.

### Database Roles

- **pgstac_admin** – schema owner, migrations
- **pgstac_ingest** – read/write, execute functions
- **pgstac_read** – SELECT only

## Critical Rules

### SQL Changes

**ONLY edit files in `src/pgstac/sql/`**. Never edit `pgstac.sql` directly — it is assembled by concatenating all `sql/*.sql` files in alphabetical order during `stageversion`.

File execution order (alphabetical by prefix):
`000_idempotent_pre` → `001_core` → `001a_jsonutils` → `001s_stacutils` → `002_collections` → `002a_queryables` → `002b_cql` → `003a_items` → `003b_partitions` → `004_search` → `004a_collectionsearch` → `005_tileutils` → `006_tilesearch` → `997_maintenance` → `998_idempotent_post` → `999_version`

Prefix ranges: `000-001` setup/core, `002` collections, `003` items/partitions, `004-006` search/tiles, `997-998` maintenance/post, `999` version (auto-generated).

### Idempotency

`000_idempotent_pre.sql` and `998_idempotent_post.sql` are included in both base installs and incremental migrations. Use `IF NOT EXISTS`, `CREATE OR REPLACE`, `ON CONFLICT DO NOTHING`.

### Partitioning

Items partitioned by `LIST(collection)`, optionally sub-partitioned by `RANGE(datetime)` (year/month via `collections.partition_trunc`). Naming: `_items_{key}[_{YYYY|YYYYMM}]`.

Key functions: `check_partition()` (create/update), `update_partition_stats()` (recalculate constraints), `partition_sys_meta` (live VIEW — always current), `partitions` (MATERIALIZED VIEW — stale between refreshes).

### Search Path

PgSTAC installs into the `pgstac` schema. All connections must have `search_path` set to `pgstac, public`.

### pg_dump / pg_restore Compatibility

PgSTAC functions reference PostGIS functions (e.g. `st_makeenvelope`, `st_geomfromgeojson`) **without schema qualification** because PostGIS may be installed in either `public` or `postgis` schema. `pg_dump` clears `search_path` during restore, breaking these references.

**Rules to maintain dump/restore compatibility:**

- **Do NOT schema-qualify PostGIS function calls** in PgSTAC SQL
- **Avoid cross-function dependencies in SQL functions used by GENERATED columns** — pg_dump orders functions alphabetically, so `func_a` calling `func_b` may be created before `func_b` exists. Inline the logic instead.
- Use `pgstac_restore` (in `docker/pypgstac/bin/`) to restore dumps — it installs a temporary event trigger that sets the correct `search_path` before each DDL command
- Test with `scripts/test --pgdump`

## Development Workflow

### Setup

```bash
scripts/setup          # Build Docker images, start database
scripts/server         # Start database (use --detach for background)
```

### Running Tests

```bash
scripts/test                    # All test suites
scripts/test --pypgstac         # pytest only
scripts/test --pgtap            # PGTap SQL tests
scripts/test --basicsql         # SQL output comparison tests
scripts/test --migrations       # Full migration chain test
scripts/test --formatting       # ruff + mypy
scripts/test --pgdump           # pg_dump/pg_restore round-trip test
```

All tests run inside Docker via `scripts/runinpypgstac`. Use `--build` to rebuild images first.

### Docker Architecture

- **pgstac** container: PostgreSQL 17 + PostGIS 3 + extensions, port 5439→5432
- **pypgstac** container: Python + Rust build tools, runs scripts
- Credentials: `username` / `password`, database: `postgis`

## Migration Process

### Creating Migrations (Release)

```bash
scripts/stageversion 0.9.11
```

This runs inside Docker and:
1. Removes old `*unreleased*` migration files
2. Writes `SELECT set_version('0.9.11');` to `999_version.sql`
3. Concatenates all `sql/*.sql` → `migrations/pgstac.0.9.11.sql` (base migration)
4. Copies the base migration to `pgstac.sql`
5. Updates `version.py` and `pyproject.toml` version strings
6. Runs `makemigration -f 0.9.10 -t 0.9.11` to generate incremental migration

### How makemigration Works

`makemigration` (in `docker/pypgstac/bin/makemigration`) generates incremental migrations by diffing schemas:

1. Creates two temp databases: `migra_from`, `migra_to`
2. Loads old base migration into `migra_from`
3. Loads new base migration into `migra_to`
4. Runs `migra --schema pgstac --unsafe` to calculate the SQL diff
5. Wraps the diff with `000_idempotent_pre.sql`, `998_idempotent_post.sql`, and `set_version()`
6. Output: `migrations/pgstac.0.9.10-0.9.11.sql`

**Important**: The generated migration is created with a `.staged` suffix. You MUST:
1. Review the `.staged` file for correctness
2. Remove the `.staged` suffix to enable it
3. Run `scripts/test --migrations` to validate

### Running Migrations

```bash
pypgstac migrate                    # Migrate to current pypgstac version
pypgstac migrate --toversion 0.9.10 # Migrate to specific version
```

The `Migrate` class (in `migrate.py`) builds a directed graph of all available migration files and uses BFS to find the shortest path from the current DB version to the target.

## Testing Details

### Test Database Setup

Tests create `pgstac_test_db_template` from `pgstac.sql`, then clone it per test suite:
- `pgstac_test_pgtap` – PGTap tests
- `pgstac_test_basicsql` – basic SQL tests
- `pgstac_test_pypgstac` – pytest (function-scoped fixture creates fresh DB per test)

### Test Types

1. **PGTap**: SQL assertions in `src/pgstac/tests/pgtap.sql`
2. **Basic SQL**: `.sql` files in `src/pgstac/tests/basic/`, output compared to `.sql.out`
3. **Pytest**: `src/pypgstac/tests/test_load.py`, `test_benchmark.py`, `test_queryables.py`, `hydration/`
4. **Migration**: Installs v0.3.0, migrates to latest, runs all test suites against migrated DB
5. **pg_dump**: Dumps a database with sample data, restores via `pgstac_restore`, verifies counts match

### Pytest Fixtures (conftest.py)

- `db` – function-scoped `PgstacDB` connected to fresh test DB
- `loader` – `Loader(db)` instance

## PR Checklist

1. Changes only in `src/pgstac/sql/` for SQL, `src/pypgstac/` for Python
2. Tests added if appropriate
3. `CHANGELOG.md` updated under `## [UNRELEASED]`
4. `docs/src/release-notes.md` updated to match `CHANGELOG.md` (they must stay identical)
5. Docs updated if needed
6. All tests pass: `scripts/test` (or `scripts/runinpypgstac --build test --pypgstac`)

## Release Checklist

1. `scripts/stageversion VERSION`
2. Review `.staged` migration, remove suffix
3. `scripts/test --migrations`
4. Move CHANGELOG "Unreleased" → new version
5. Copy updated `CHANGELOG.md` to `docs/src/release-notes.md` (keep identical)
6. Create PR, merge
7. `git tag vVERSION && git push origin vVERSION`
8. CI publishes to PyPI + ghcr.io

## Common Patterns

### Adding a new SQL function

1. Edit the appropriate file in `src/pgstac/sql/` (use `CREATE OR REPLACE FUNCTION`)
2. Add `SECURITY DEFINER` if the function modifies tables
3. Grant execute in `998_idempotent_post.sql` if needed
4. Add PGTap or basic SQL tests

### Adding a new queryable

```sql
INSERT INTO queryables (name, definition, property_wrapper, property_index_type)
VALUES ('prop_name', '{"$ref": "..."}', 'to_int', 'BTREE')
ON CONFLICT DO NOTHING;
```

### Loading test data

```bash
scripts/runinpypgstac --build loadsampledata
```
