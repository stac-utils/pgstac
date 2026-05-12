#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "pgpkg>=0.1.1,<0.2",
# ]
# ///
"""Build the local pgstac-migrate baked artifact with the published pgpkg API."""

from __future__ import annotations

from pathlib import Path

from pgpkg.api import bundle_project


def main() -> int:
    package_root = Path(__file__).resolve().parents[1]
    repo_root = package_root.parents[1]
    project_root = repo_root / "src" / "pgstac"
    artifact_path = package_root / "src" / "pgstac_migrate" / "migrations.tar.zst"

    artifact_path = bundle_project(project_root, artifact_path)
    print(f"wrote {artifact_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
