---
applyTo: "scripts/**"
---

# Build Scripts

See CLAUDE.md "Development Workflow" for usage. All scripts require the Docker compose environment.

- `runinpypgstac` is the foundation — most scripts delegate to it
- `loadsampledata` has a host wrapper at `scripts/loadsampledata`; prefer that wrapper over calling `runinpypgstac` directly
- `runinpypgstac` uses the published-package path by default; set `PGPKG_LOCAL_REPO_DIR` to mount a local `pgpkg` checkout at `/pgpkg` when you need an override
- `scripts/container-scripts/` contains the in-container script payload copied into the pypgstac image; keep host wrappers in `scripts/`
- `stageversion` modifies version files AND generates migrations — see CLAUDE.md "Migration Process"
- `stageversion` regenerates `*unreleased*` migrations each run; if you hand-edit incremental SQL, rebuild the baked artifact with `uv run --directory src/pgstac-migrate pgstac-migrate build-artifact` and avoid rerunning `stageversion` unless you intend to overwrite edits
- `scripts/container-scripts/stageversion` and `scripts/container-scripts/makemigration` now shell through `pgpkg` inside the container rather than assembling/diffing SQL directly
- Set `PGPKG_LOCAL_REPO_DIR` on the host when you need to force a local pgpkg checkout for `stageversion`, `makemigration`, or related container-script testing
- Tagged releases run `.github/workflows/release.yml`, which publishes both `pypgstac` and `pgstac-migrate` to PyPI via the GitHub `pypi` environment; PyPI trusted publishers must exist for both projects
- DO NOT run `stageversion` without understanding its side effects
