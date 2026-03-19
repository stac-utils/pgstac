# PgSTAC Agents

## sql-developer

PostgreSQL SQL developer for PgSTAC. Works exclusively in `src/pgstac/sql/` files. See CLAUDE.md for full SQL rules, file map, and partition architecture.

### Key Constraints

- NEVER edit `pgstac.sql` — it is auto-generated
- `CREATE OR REPLACE FUNCTION`, `IF NOT EXISTS`, `SECURITY DEFINER` for data-modifying functions
- Grant permissions in `998_idempotent_post.sql`, not inline
- Use `run_or_queue()` for deferrable operations
- Do NOT schema-qualify PostGIS calls (PostGIS may be in `public` or `postgis` schema)
- Avoid cross-function deps in SQL functions used by GENERATED columns — pg_dump orders alphabetically, so inline the logic (see `search_hash` pattern)
- Test: `scripts/runinpypgstac --build test --pgtap --basicsql`

---

## migration-engineer

Migration specialist for PgSTAC. See CLAUDE.md "Migration Process" for full workflow.

### Quick Reference

1. Edit SQL in `src/pgstac/sql/*.sql`
2. `scripts/stageversion VERSION` → generates base + incremental `.staged` migration
3. Review `.staged` file (watch for DROPs, unsafe ALTERs, missing `CREATE OR REPLACE`)
4. Remove `.staged` suffix → `scripts/test --migrations`

### Review Checklist

- No unintended `DROP TABLE/COLUMN`, safe `ALTER TABLE` for large tables
- `CREATE OR REPLACE` (not bare `CREATE`), `IF NOT EXISTS` for indexes
- `000_idempotent_pre.sql` and `998_idempotent_post.sql` included
- `set_version()` called at end

---

## loader-developer

Specialist in pypgstac bulk loading (`src/pypgstac/src/pypgstac/load.py`). See CLAUDE.md "pypgstac Loader Internals" for full details.

### Critical Patterns

- **Materialize generators**: `list(g)` before `load_partition()` — generators can't survive tenacity retries
- **Live view only**: Query `partition_sys_meta` (VIEW), never `partitions` (stale MATERIALIZED VIEW)
- **Retry safety**: `item.pop("partition", None)` with `None` default; `before_sleep` sets `partition.requires_update = True` on `CheckViolation`
- **Retry scope**: `CheckViolation`, `DeadlockDetected`, `SerializationFailure`, `LockNotAvailable`, `ObjectInUse`
- **Load modes**: `insert`, `ignore`/`insert_ignore`, `upsert`, `delsert`
- Test: `scripts/runinpypgstac --build test --pypgstac`
