---
description: "Add a new SQL function to PgSTAC"
---

Add a new SQL function following PgSTAC conventions:

1. Determine the correct file in `src/pgstac/sql/` based on the prefix ranges in CLAUDE.md
2. Use `CREATE OR REPLACE FUNCTION` in the `pgstac` schema
3. Add `SECURITY DEFINER` if the function modifies tables
4. Add permission grants in `src/pgstac/sql/998_idempotent_post.sql` if the function should be callable by `pgstac_ingest` or `pgstac_read`
5. Add a test in `src/pgstac/tests/pgtap.sql` or a new `.sql`/`.sql.out` pair in `src/pgstac/tests/basic/`

Test with: `scripts/runinpypgstac --build test --pgtap --basicsql`
