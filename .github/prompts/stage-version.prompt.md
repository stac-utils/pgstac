---
description: "Stage a new PgSTAC version and review the migration"
---

Guide me through the PgSTAC release migration process:

1. Confirm all SQL changes are in `src/pgstac/sql/` (never `pgstac.sql` directly)
2. Run `scripts/stageversion {VERSION}` to generate base + incremental migrations
3. Review the `.staged` migration file checking for:
   - Unintended `DROP TABLE` or `DROP COLUMN`
   - Unsafe `ALTER TABLE` for large tables
   - Bare `CREATE` instead of `CREATE OR REPLACE` for functions
   - Missing `IF NOT EXISTS` on indexes
   - Presence of `000_idempotent_pre.sql` and `998_idempotent_post.sql` content
   - `set_version()` called at the end
4. Remove the `.staged` suffix
5. Run `scripts/test --migrations` to validate the full migration chain
6. Update CHANGELOG.md
