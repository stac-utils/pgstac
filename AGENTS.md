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
2. `src/pgstac/pyproject.toml` is the `pgpkg` project config for the SQL + migrations tree
3. `uv run --directory src/pgstac-migrate pgstac-migrate info|versions|plan` inspects the baked migration artifact during wrapper work
4. `uv run --directory src/pypgstac pypgstac migrate -- --help` remains a backwards-compatible wrapper over `pgstac-migrate`; put new runtime migration behavior in `src/pgstac-migrate/`, not `src/pypgstac/`
5. `scripts/stageversion VERSION` regenerates canonical `pgstac--VERSION.sql` plus incremental `pgstac--FROM--TO.sql`; set `PGPKG_LOCAL_REPO_DIR` when `stageversion` or `makemigration` should run against a local pgpkg checkout. The Docker-backed flow mounts that override at `/pgpkg` and exports `PGPKG_REPO_DIR` to the container scripts.
6. Review the generated incremental migration (watch for DROPs, unsafe ALTERs, missing `CREATE OR REPLACE`)
7. If you hand-edit the incremental migration, rebuild the baked artifact: `uv run --directory src/pgstac-migrate pgstac-migrate build-artifact`
8. Run `scripts/test --migrations` (or full `scripts/test` gate)
9. Tagged releases publish both `pypgstac` and `pgstac-migrate` to PyPI from `.github/workflows/release.yml`; keep the PyPI trusted publisher registration aligned with the `pypi` environment and workflow path

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
