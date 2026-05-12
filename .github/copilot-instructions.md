# Copilot Instructions for PgSTAC

See `CLAUDE.md` for comprehensive project instructions, architecture, and workflows.
See `AGENTS.md` for specialized agent definitions (sql-developer, migration-engineer, loader-developer).

When migration workflows, script entry points, or developer commands change, update `CLAUDE.md`, `AGENTS.md`, and any relevant files under `.github/instructions/` in the same change.
Use `uv` for Python execution, dependency installation, and standalone helper scripts; avoid direct `pip` commands.
