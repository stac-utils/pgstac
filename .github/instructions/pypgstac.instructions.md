---
applyTo: "src/pypgstac/**"
---

# pypgstac Python

See CLAUDE.md "pypgstac Loader Internals" for patterns. See AGENTS.md "loader-developer" for critical rules.

- Uses psycopg v3 (not psycopg2), orjson (not json), tenacity, plpygis, fire
- Materialize generators before retry boundaries
- Query `partition_sys_meta` (live VIEW), never `partitions` (stale MATERIALIZED VIEW)
- Test: `scripts/runinpypgstac --build test --pypgstac`
