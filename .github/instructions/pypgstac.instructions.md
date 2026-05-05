---
applyTo: "src/pypgstac/**"
---

# pypgstac Python

See CLAUDE.md "pypgstac Loader Internals" for patterns. See AGENTS.md "loader-developer" for critical rules.

- Uses psycopg v3 (not psycopg2), orjson (not json), tenacity, plpygis, fire
- `pypgstac migrate` is a thin wrapper over `pgstac-migrate`; put new migration runtime behavior in `src/pgstac-migrate/`, not `src/pypgstac/src/pypgstac/migrate.py`
- `src/pypgstac/pyproject.toml` keeps a local `[tool.uv.sources]` override for `pgstac-migrate`, while `pgpkg` resolves from PyPI
- In Docker-backed dev runs, `scripts/runinpypgstac` can mount a local `pgpkg` checkout at `/pgpkg` and export `PGPKG_REPO_DIR` so container scripts can force that checkout when needed
- Materialize generators before retry boundaries
- Query `partition_sys_meta` (live VIEW), never `partitions` (stale MATERIALIZED VIEW)
- Test: `scripts/runinpypgstac --build test --pypgstac`
