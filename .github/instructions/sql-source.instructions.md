---
applyTo: "src/pgstac/sql/**"
---

# SQL Source Files

See CLAUDE.md "Critical Rules" for full SQL conventions.

- NEVER edit `pgstac.sql` — it is auto-generated
- `CREATE OR REPLACE FUNCTION`, `IF NOT EXISTS`, `SECURITY DEFINER`
- Grant permissions in `998_idempotent_post.sql`, not inline
- `get_tstz_constraint()` regex must handle fractional seconds (`.` in timestamps)
- Do NOT schema-qualify PostGIS calls — PostGIS may be in `public` or `postgis` schema
- SQL functions used by GENERATED columns must be self-contained (no cross-function deps) — pg_dump orders functions alphabetically and breaks dependency chains
- Test: `scripts/runinpypgstac --build test --pgtap --basicsql`
