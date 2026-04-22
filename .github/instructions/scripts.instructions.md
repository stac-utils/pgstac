---
applyTo: "scripts/**"
---

# Build Scripts

See CLAUDE.md "Development Workflow" for usage. All scripts require the Docker compose environment.

- `runinpypgstac` is the foundation — most scripts delegate to it
- `scripts/container-scripts/` contains the in-container script payload copied into the pypgstac image; keep host wrappers in `scripts/`
- `stageversion` modifies version files AND generates migrations — see CLAUDE.md "Migration Process"
- DO NOT run `stageversion` without understanding its side effects
