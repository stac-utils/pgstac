# pyPgSTAC has been removed

The legacy `pypgstac` Python package (migrations, bulk loading, queryables, and
the pure-Python hydration helpers) has been removed from this repository. Every
capability it provided now lives in one of its replacements:

- **`pgstac-migrate`** — the migration runtime (planning + apply), published to
  PyPI as [`pgstac-migrate`](https://pypi.org/project/pgstac-migrate/).
- **`pgstac`** — the Rust CLI (built from `src/pgstac-rs` with the `cli` feature)
  for loading, searching, queryables, extensions, and maintenance.
- **`pypgstac-rs`** — the Rust-backed Python extension (module name
  `pgstac`) exposing the read/write pool API for `stac-fastapi-pgstac`,
  published to PyPI as
  [`pypgstac-rs`](https://pypi.org/project/pypgstac-rs/).

Previously published `pypgstac` releases remain available on PyPI; no new
versions are published.

## Command and API mapping

| Old `pypgstac` capability | Replacement |
|---|---|
| `pypgstac migrate --toversion X` | `pgstac-migrate migrate --to X` |
| `pypgstac version` (DB-installed version) | `pgstac-migrate current` |
| `pypgstac load items file --method upsert` | `pgstac load file --policy upsert` |
| `pypgstac load items --method insert_ignore` | `pgstac load file --policy ignore` |
| `pypgstac search '<query>'` | `pgstac search '<query>'` |
| `pypgstac load_queryables q.json` | `pgstac load-queryables q.json` |
| `pypgstac loadextensions` | `pgstac load-extensions` |
| `pypgstac runqueue` | `pgstac runqueue` |
| `pypgstac pgready` | `pg_isready` (or a connect-retry loop) |
| `pypgstac.hydration.hydrate` | `hydraters.hydrate`, or the `pgstac` read API (server-side hydration) |
| `pypgstac.hydration.dehydrate` | the Rust loader in `pgstac load` / `pgstac` (dehydration runs in Rust at ingest) |
| `pypgstac.migrate.MigrationPath` | `pgstac_migrate.compat.MigrationPath` |
| `pypgstac.__version__` | `pgstac_migrate.__version__` (or the `pypgstac-rs` wheel version) |

## Behavior changes to note

- **Conflict-mode default changed.** `pypgstac load` defaulted to `insert`
  (fail on duplicate). `pgstac load` defaults to `upsert`. The Rust
  `--policy` values are `upsert` (default), `ignore`, and `error`; `delsert`
  semantics are handled by the loader.
- **Flag renames.** `--method` → `--policy`; `--chunksize` → `--batch-size`.
  The table is auto-detected from the input, so there is no `items`/`collections`
  positional argument.
- **Search engine.** `pgstac search` drives the Rust keyset engine
  (`search_plan`, client-side band-stepping/hydration/token minting) rather than
  calling the SQL `search()` function. It gains NDJSON / ItemCollection /
  geoparquet output formats.
- **Client-side version gate removed.** The old `Loader.check_version`
  major/minor pre-check is gone; ingesting against a mismatched schema now fails
  at the SQL layer instead.
- **Legacy dehydrated input format dropped.** The tab-delimited dehydrated
  ndjson (`.pgcopy`/`.txt`) `pypgstac load` accepted is not a `pgstac load`
  input. The Rust loader dehydrates hydrated input in-process; dump/restore uses
  stac-geoparquet.

## Migrations

`pgstac-migrate` owns runtime migration planning and apply logic.

```bash
pgstac-migrate migrate --help
pgstac-migrate migrate            # migrate to the latest bundled version
pgstac-migrate migrate --to 0.9.11
pgstac-migrate current            # print the version installed in the DB
pgstac-migrate plan
pgstac-migrate versions
pgstac-migrate info
```

Python API example:

```python
from pgstac_migrate.api import migrate

result = migrate(target=None, conninfo="postgresql://...")
print(result.final_version)
```

Use `target=None` for latest, or set `target="<version>"`.

`pgstac-migrate` reads the standard PG environment variables (`PGHOST`,
`PGPORT`, `PGUSER`, `PGDATABASE`, `PGPASSWORD`) and also accepts a
`--dsn "postgresql://..."`.

Migration filenames use canonical PostgreSQL extension naming:

- **Base migrations:** `pgstac--<version>.sql`
- **Incremental migrations:** `pgstac--<from>--<to>.sql`

### Bootstrapping an Empty Database

Running `pgstac-migrate migrate` against an empty database as a privileged user
(such as `postgres`) creates the required extensions (postgis, btree_gist,
unaccent), the roles (`pgstac_admin`, `pgstac_read`, `pgstac_ingest`), and the
`pgstac` schema.

In production, assign those roles to your application user rather than using the
`postgres` superuser:

```sql
GRANT pgstac_read TO your_app_user;
GRANT pgstac_ingest TO your_app_user;
GRANT pgstac_admin TO your_app_user;
ALTER USER your_app_user SET search_path TO pgstac, public;
```

## Bulk Data Loading

Use the `pgstac` Rust CLI (built from `src/pgstac-rs` with the `cli` feature).
It loads stac-geoparquet, NDJSON, or JSON through the Rust loader (dehydration,
fragment splitting, and the binary COPY all run in Rust). Collections are loaded
before items.

```bash
pgstac load items.ndjson                 # default policy: upsert
pgstac load items.ndjson --policy ignore # skip conflicting ids
pgstac load items.ndjson --policy error  # fail on duplicate ids
```

## Loading Queryables

Queryables let clients discover which terms are available for CQL2 filter
expressions. Load them from a JSON schema file:

```bash
pgstac load-queryables queryables.json
pgstac load-queryables queryables.json --collection-ids collection1,collection2
pgstac load-queryables queryables.json --delete-missing
pgstac load-queryables queryables.json --index-fields field1,field2
```

By default no indexes are created. Use `--index-fields` to selectively index the
fields you filter on frequently; over-indexing degrades write performance.

The JSON file should follow the queryables schema described in the
[STAC API - Filter Extension](https://github.com/stac-api-extensions/filter#queryables).

## Automated Collection Extent Updates

Setting `pgstac.update_collection_extent` to `true` enables a trigger that
adjusts collection spatial/temporal extents as items are ingested. To reduce
load-transaction overhead, combine it with `pgstac.use_queue`, which defers the
work to a queue drained by:

```bash
pgstac runqueue          # runs CALL run_queued_queries()
```

Schedule `pgstac runqueue` (or `CALL run_queued_queries();` via `pg_cron`) to
process the queue asynchronously.
