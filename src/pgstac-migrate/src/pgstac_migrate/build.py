"""Source-tree helpers for building and locating the baked PgSTAC artifact."""

from __future__ import annotations

from pathlib import Path

from pgpkg.api import bundle_project
from pgpkg.errors import PgpkgError


def artifact_path() -> Path:
    return Path(__file__).with_name("migrations.tar.zst")


def package_root() -> Path:
    return Path(__file__).resolve().parents[2]


def source_project_root() -> Path:
    project_root = package_root().parents[1] / "src" / "pgstac"
    if not (project_root / "pyproject.toml").is_file():
        raise PgpkgError(
            "Could not find the PgSTAC source tree. `build-artifact` only works from a pgstac checkout.",
            code="E_ARTIFACT",
        )
    return project_root


def build_local_artifact(output_path: Path | None = None) -> Path:
    return bundle_project(source_project_root(), output_path or artifact_path())


def ensure_artifact_path() -> Path:
    """Return the baked artifact path, building it from source when possible."""
    path = artifact_path()
    if path.is_file():
        return path

    try:
        return build_local_artifact(path)
    except PgpkgError as exc:
        raise PgpkgError(
            "Missing baked artifact. Run `uv run --directory src/pgstac-migrate pgstac-migrate build-artifact` first.",
            code="E_ARTIFACT",
        ) from exc
