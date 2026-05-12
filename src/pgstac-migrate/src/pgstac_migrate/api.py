"""Public Python API for PgSTAC migration artifacts."""

from __future__ import annotations

from pathlib import Path

from pgpkg.api import migrate_from_artifact
from pgpkg.executor import ApplyResult

from pgstac_migrate.build import ensure_artifact_path
from pgstac_migrate.version_source import PgstacVersionSource


def normalize_target_version(target: str | None) -> str | None:
    """Map source-tree dev versions to the staged unreleased migration label."""
    if target is None:
        return None
    if target.endswith("-dev"):
        return "unreleased"
    return target


def artifact_path() -> Path:
    """Return the baked artifact path, building it when running from source."""
    return ensure_artifact_path()


def migrate(
    *,
    target: str | None = None,
    dry_run: bool = False,
    conninfo: str | None = None,
    host: str | None = None,
    port: int | str | None = None,
    dbname: str | None = None,
    user: str | None = None,
    password: str | None = None,
) -> ApplyResult:
    """Apply baked PgSTAC migrations to a live database."""
    return migrate_from_artifact(
        str(artifact_path()),
        target=normalize_target_version(target),
        dry_run=dry_run,
        conninfo=conninfo,
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password,
        version_source=PgstacVersionSource(),
    )
