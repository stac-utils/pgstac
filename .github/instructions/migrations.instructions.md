---
applyTo: "src/pgstac/migrations/**"
---

# Migration Files

These files are **generated** — see CLAUDE.md "Migration Process" for the full workflow.

- **DO NOT** create, edit, or hand-modify migration files
- Base (`pgstac.X.Y.Z.sql`) = full schema at that version
- Incremental (`pgstac.X.Y.Z-A.B.C.sql`) = upgrade diff
- Staged (`*.sql.staged`) = needs review before removing `.staged` suffix
- Test: `scripts/test --migrations`
