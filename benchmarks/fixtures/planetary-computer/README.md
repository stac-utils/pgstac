# Planetary Computer benchmark fixtures

This directory defines reproducible benchmark fixtures for PgSTAC load-path benchmarking.

## Collections

- `naip`
- `sentinel-2-l2a`
- `landsat-c2-l2`

Each fixture set materializes:

- one collection document (`collection.json`)
- exactly 1000 STAC items (`items.ndjson`)

## Why fixtures are generated

Committing 3000 raw Planetary Computer items would add a large and frequently-changing payload to the repository.
Instead, this directory commits a deterministic fixture manifest plus a fetch script.

## Generate fixtures

From repository root:

```bash
uv run --no-project --with psycopg[binary] python scripts/benchmark_fetch_pc_fixtures.py \
  --manifest benchmarks/fixtures/planetary-computer/manifest.json \
  --output-dir benchmarks/fixtures/planetary-computer/data
```

## Validate generated fixtures

```bash
uv run --no-project --with psycopg[binary] python scripts/benchmark_fetch_pc_fixtures.py \
  --manifest benchmarks/fixtures/planetary-computer/manifest.json \
  --output-dir benchmarks/fixtures/planetary-computer/data \
  --validate-only
```
