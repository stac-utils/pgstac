# Rust client & CLI (`pgstac-rs`)

`src/pgstac-rs` is a Rust crate (`pgstac`) and a command-line tool (`pgstac`) that talk to a PgSTAC
database. It provides:

- a **read engine** that drives `search_plan` / `collection_search_plan` and does the band stepping,
  hydration (EWKB→GeoJSON, fragment merge), keyset token minting, and the STAC `fields` include/exclude
  projection **in Rust** — page-equivalent to SQL `search()` but with flat memory under streaming and
  the CPU moved off the database. When a search's `fields` need no shared fragment, `search_plan` nulls
  `fragment_id` in the projection it returns, so the engine skips the per-row `item_fragments` lookup;
- a **connection pool** (`PgstacPool`, the `pool` feature) with the async read API;
- a **dump/export** library + the `pgstac` **CLI** (`export` / `cli` features);
- a **Python wheel** (`pypgstac_rs`, the `python` feature, built with [maturin](https://github.com/PyO3/maturin)).

## Cargo features

| Feature     | Adds |
| ----------- | ---- |
| *(default)* | The `Pgstac` trait over any `tokio_postgres::GenericClient`. |
| `pool`      | `PgstacPool`: pooled async read API (rustls TLS, PgBouncer-safe). |
| `export`    | The dump library (scan partitions → stac-geoparquet + manifest). |
| `cli`       | The `pgstac` binary (implies `export`). |
| `python`    | The `pypgstac_rs` pyo3 extension module. |

## Environment variables

The library and CLI resolve a connection from a single DSN if one is given, otherwise from the standard
**libpq** environment variables. The connection `search_path` is **always** set to `pgstac, public`.

### Connection

| Variable | Purpose |
| -------- | ------- |
| `PGSTAC_DSN` | Full Postgres connection string (URL or key/value). The CLI `--dsn` flag defaults to this. |
| `DATABASE_URL` | Fallback DSN if `PGSTAC_DSN` is unset. |
| `PGHOST` | Server host. |
| `PGPORT` | Server port. |
| `PGDATABASE` | Database name. |
| `PGUSER` | Username. |
| `PGPASSWORD` | Password. |
| `PGOPTIONS` | Extra startup options. |
| `PGAPPNAME` | `application_name` for the connection. |
| `PGCONNECT_TIMEOUT` | Connection timeout, in seconds. |
| `PGSSLMODE` | TLS mode (`disable`, `prefer`, `require`, `verify-ca`, `verify-full`). |
| `PGSSLROOTCERT` | Path to the CA certificate. |
| `PGSSLCERT` | Path to the client certificate. |
| `PGSSLKEY` | Path to the client key. |

A DSN (`PGSTAC_DSN` / `--dsn`) takes precedence; the individual `PG*` variables fill in any field the
DSN does not set.

### Test fixtures

The integration tests skip (rather than fail) when their database is unreachable. Point them at your
fixtures with:

| Variable | Default | Used by |
| -------- | ------- | ------- |
| `PGSTAC_RS_TEST_DB` | `postgresql://username:password@localhost:5439/postgis` | clean-template tests |
| `PGSTAC_RS_TEST_RICH_DB` | `…/pgstac_rs_test_rich` | search/stream/hydration/collection parity tests |
| `PGSTAC_RS_TEST_TEMPLATE` | `pgstac_rs_test_template` | pool clone-from-template tests |
| `PGSTAC_PARITY_DB_010` | `…/a_parity010` | 0.10 dump end-to-end tests |
| `PGSTAC_PARITY_DB_0911` | `…/a_parity0911` | 0.9.11 dump end-to-end tests |

## Library

```rust,no_run
use pgstac::{ConnectConfig, PgstacPool};
use futures::StreamExt;
use serde_json::json;

# tokio_test::block_on(async {
// Pool from the environment (PGSTAC_DSN or the PG* vars). Create once at startup.
let pool = PgstacPool::connect(ConnectConfig::from_env()).await.unwrap();

// A bounded page, with keyset next/prev tokens.
let page = pool.search_page(&json!({"collections": ["landsat-c2-l2"]}), None, 10).await.unwrap();

// Stream every match with flat memory.
let mut items = Box::pin(pool.search_items(json!({"collections": ["landsat-c2-l2"]}), None, None));
while let Some(item) = items.next().await { let _ = item.unwrap(); }
# })
```

Pool methods: `search_page`, `search_items` (stream), `search_collect_items`, `stream_ndjson`,
`search_matched`, `collection_search`, `get_item`, `get_collection`, `get_queryables`.

## CLI

```text
pgstac <COMMAND>
  dump      Dump a pgstac instance (or a subset) to a directory, tar, S3, or stdout
  search    Stream a search/CQL2 result as NDJSON / ItemCollection / geoparquet (0.10 only)
```

Both subcommands read `--dsn` from `$PGSTAC_DSN` when the flag is omitted.

### `pgstac dump`

Writes fully-hydrated items as stac-geoparquet (one file per partition, ordered by `datetime, id`) plus
`collection.json` / `queryables.json` / `settings.json` and a sha256'd `manifest.json` (written last —
its presence marks a complete dump).

| Option | Default | Description |
| ------ | ------- | ----------- |
| `--dsn <DSN>` | `$PGSTAC_DSN` / local dev | Postgres connection string. |
| `-o, --out <OUT>` | *(required)* | Destination: a directory, `s3://bucket/prefix` (or other object-store URL), a `*.tar` / `*.tar.zst` file, or `-` for stdout. |
| `-c, --collection <ID>` | *(all)* | Restrict to these collection ids (repeatable). |
| `--datetime-start <RFC3339>` | — | Inclusive datetime prefilter start (requires `--datetime-end`). |
| `--datetime-end <RFC3339>` | — | Exclusive datetime prefilter end (requires `--datetime-start`). |
| `--bbox <W> <S> <E> <N>` | — | Bbox prefilter (EPSG:4326). |
| `--compression <CODEC>` | `zstd` | Parquet codec: `zstd`, `snappy`, `uncompressed`. |
| `--skip-errors` | off | Continue past per-item errors, recording skips in the report. |
| `--memory-budget <BYTES>` | ~25% RAM | Memory budget for the buffered geoparquet path. |
| `--concurrency <N>` | `1` | Max partitions dumped concurrently (>1 opens one extra connection per worker). |
| `--consistent` | off | Dump the whole instance under one repeatable-read snapshot (implies a parallel run). |
| `--tar-zstd` | off | Write a compressed `.tar.zst` when `--out` is a `.tar` path. |
| `--dry-run` | off | Report the plan without writing data. |

### `pgstac search`

Streams a 0.10 search off the keyset engine.

| Option | Default | Description |
| ------ | ------- | ----------- |
| `--dsn <DSN>` | `$PGSTAC_DSN` / local dev | Postgres connection string. |
| `-o, --out <OUT>` | `-` (stdout) | A file path or `-` for stdout. |
| `--format <FORMAT>` | `ndjson` | `ndjson` (streams all pages), `itemcollection` (one SQL-faithful page with next/prev + context), or `geoparquet`. |
| `-c, --collection <ID>` | *(all)* | Restrict to these collection ids (repeatable). |
| `--id <ID>` | — | Restrict to these item ids (repeatable). |
| `--bbox <W> <S> <E> <N>` | — | Bbox (EPSG:4326). |
| `--datetime <DT>` | — | Datetime / interval (STAC datetime syntax). |
| `--filter <CQL2>` | — | CQL2-JSON filter (a JSON object). |
| `--limit <N>` | server default | Page size (the keyset limit). |
| `--token <TOKEN>` | — | Continuation token (`next:…` / `prev:…`). With `itemcollection`, returns exactly that one page. |
| `--max-items <N>` | *(unbounded)* | Cap total items streamed (`ndjson` / `geoparquet`). |
| `--compression <CODEC>` | `zstd` | Parquet codec (`geoparquet` only). |

## Python wheel

```sh
maturin build -m src/pgstac-rs/Cargo.toml     # produces the pypgstac_rs wheel
```

```python
import asyncio, orjson, pypgstac_rs

async def main():
    pool = await pypgstac_rs.Pgstac.connect()           # libpq env, or pass a dsn string
    body = orjson.dumps({"collections": ["landsat-c2-l2"], "limit": 10}).decode()
    fc = orjson.loads(await pool.search(body))
    print(fc["numberReturned"])

asyncio.run(main())
```

`Pgstac` methods (each `async`, returning a JSON string): `connect(dsn=None)`, `search(search,
token=None, limit=10)`, `search_collect(search, max_items=None)`, `search_matched(search)`,
`collection_search(search, token=None)`, `get_item(collection_id, item_id)`,
`get_collection(collection_id)`, `get_queryables(collection_id=None)`.
