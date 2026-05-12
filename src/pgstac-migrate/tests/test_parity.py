"""Cross-surface migration plan parity tests.

Asserts that the pgstac-migrate artifact catalog and the pypgstac MigrationPath
compatibility helper produce *identical* ordered file sequences for every
(source, target) pair in the parity matrix.

This is the canonical regression test for "both tools would apply exactly the
same SQL in exactly the same order".
"""

from __future__ import annotations

import tempfile
from importlib import import_module
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Shared fixture: load the baked artifact once and extract migration files
# to a temporary directory so MigrationPath can resolve filenames.
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def artifact_catalog_and_migrations_dir():
    """Return (catalog, migrations_dir) using the baked pgstac-migrate artifact."""
    pgpkg_artifact_mod = import_module("pgpkg.artifact")
    cli = import_module("pgstac_migrate.cli")

    artifact = pgpkg_artifact_mod.load_artifact(cli._artifact_path())
    catalog = cli._catalog_from_artifact(artifact)

    tmp_root = Path(tempfile.mkdtemp(prefix="pgstac_parity_"))
    migrations_dir = tmp_root / "migrations"
    migrations_dir.mkdir()
    for name, data in artifact.migrations_files().items():
        (migrations_dir / Path(name).name).write_bytes(data)

    return catalog, str(migrations_dir)


# ---------------------------------------------------------------------------
# Parity cases: (source, target) tuples.
#   source=None means a fresh install (no prior pgstac version).
# ---------------------------------------------------------------------------

PARITY_CASES = [
    # Fresh install
    (None, "0.9.11"),
    (None, "0.9.10"),
    # Single-hop incremental upgrade
    ("0.9.10", "0.9.11"),
    ("0.9.9", "0.9.10"),
    # Multi-hop incremental upgrade
    ("0.9.9", "0.9.11"),
    ("0.9.8", "0.9.11"),
    ("0.8.6", "0.9.11"),
]


@pytest.mark.parametrize("source,target", PARITY_CASES)
def test_plan_parity_across_surfaces(
    artifact_catalog_and_migrations_dir,
    source: str | None,
    target: str,
) -> None:
    """pgstac-migrate catalog plan == pypgstac MigrationPath for every test case."""
    pgpkg_planner = import_module("pgpkg.planner")
    compat = import_module("pgstac_migrate.compat")

    catalog, migrations_dir = artifact_catalog_and_migrations_dir

    # ---- pgstac-migrate catalog path -----------------------------------------
    migration_plan = pgpkg_planner.plan(catalog, source=source, target=target)

    pgpkg_files: list[str] = []
    if migration_plan.bootstrap_base is not None:
        pgpkg_files.append(migration_plan.bootstrap_base.name)
    pgpkg_files.extend(step.file.name for step in migration_plan.steps)

    # ---- pypgstac MigrationPath compat path ----------------------------------
    compat_source = "init" if source is None else source
    compat_files = compat.MigrationPath(
        migrations_dir, compat_source, target
    ).migrations()

    assert pgpkg_files == compat_files, (
        f"Plan mismatch for {source!r} → {target!r}:\n"
        f"  pgstac-migrate catalog: {pgpkg_files}\n"
        f"  pypgstac MigrationPath: {compat_files}"
    )
