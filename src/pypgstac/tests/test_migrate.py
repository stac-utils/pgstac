"""Unit tests for migration filename handling."""

from pathlib import Path

from pypgstac.migrate import (
    MigrationPath,
    base_migration_filename,
    incremental_migration_filename,
)


def test_canonical_migration_filename_helpers() -> None:
    assert base_migration_filename("0.9.11") == "pgstac--0.9.11.sql"
    assert (
        incremental_migration_filename("0.9.10", "0.9.11")
        == "pgstac--0.9.10--0.9.11.sql"
    )


def test_parse_filename_uses_canonical_layout() -> None:
    migration_path = MigrationPath("/tmp", "0.9.10", "0.9.11")

    assert migration_path.parse_filename("/tmp/pgstac--0.9.11.sql") == ["0.9.11"]
    assert migration_path.parse_filename("/tmp/pgstac--0.9.10--0.9.11.sql") == [
        "0.9.10",
        "0.9.11",
    ]


def test_migration_path_returns_canonical_filenames(tmp_path: Path) -> None:
    (tmp_path / "pgstac--0.9.11.sql").write_text("-- base\n")
    (tmp_path / "pgstac--0.9.10.sql").write_text("-- from\n")
    (tmp_path / "pgstac--0.9.10--0.9.11.sql").write_text("-- incremental\n")

    fresh_install = MigrationPath(str(tmp_path), "init", "0.9.11")
    assert fresh_install.migrations() == ["pgstac--0.9.11.sql"]

    upgrade = MigrationPath(str(tmp_path), "0.9.10", "0.9.11")
    assert upgrade.migrations() == ["pgstac--0.9.10--0.9.11.sql"]
