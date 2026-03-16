---
applyTo: "src/pgstac/sql/**"
---

# SQL Source Files

See CLAUDE.md "Critical Rules" for full SQL conventions.

- NEVER edit `pgstac.sql` — it is auto-generated
- `CREATE OR REPLACE FUNCTION`, `IF NOT EXISTS`, `SECURITY DEFINER`
- Grant permissions in `998_idempotent_post.sql`, not inline
- `get_tstz_constraint()` regex must handle fractional seconds (`.` in timestamps)
- Test: `scripts/runinpypgstac --build test --pgtap --basicsql`
