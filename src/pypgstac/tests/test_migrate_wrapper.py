from importlib import import_module
from pathlib import Path
from types import SimpleNamespace

from pypgstac.db import PgstacDB
from pypgstac.migrate import Migrate
from pypgstac.migrate import MigrationPath as PypgstacMigrationPath
from pypgstac.pypgstac import PgstacCLI


def test_run_migration_delegates_to_pgstac_migrate(monkeypatch) -> None:
    migrate_module = import_module("pypgstac.migrate")
    db = PgstacDB(dsn="postgresql:///example")
    captured: dict[str, object] = {}
    disconnect_calls: list[None] = []

    def fake_disconnect() -> None:
        disconnect_calls.append(None)

    def fake_migrate(**kwargs):
        captured.update(kwargs)
        return SimpleNamespace(final_version="0.9.11")

    monkeypatch.setattr(db, "disconnect", fake_disconnect)
    monkeypatch.setattr(
        migrate_module,
        "_pgstac_migrate_api",
        lambda: SimpleNamespace(migrate=fake_migrate),
    )

    final_version = Migrate(db).run_migration("0.9.11-dev")

    assert final_version == "0.9.11"
    assert captured == {
        "target": "0.9.11-dev",
        "conninfo": "postgresql:///example",
    }
    assert len(disconnect_calls) == 2


def test_run_migration_defaults_to_package_version(monkeypatch) -> None:
    migrate_module = import_module("pypgstac.migrate")
    db = PgstacDB(dsn="")
    captured: dict[str, object] = {}

    monkeypatch.setattr(db, "disconnect", lambda: None)
    monkeypatch.setattr(
        migrate_module,
        "_pgstac_migrate_api",
        lambda: SimpleNamespace(
            migrate=lambda **kwargs: (
                captured.update(kwargs) or SimpleNamespace(final_version="unreleased")
            ),
        ),
    )

    final_version = Migrate(db).run_migration()

    assert final_version == "unreleased"
    assert captured == {
        "target": "0.9.11-dev",
        "conninfo": None,
    }


def test_cli_migrate_delegates_to_migrate_wrapper(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_run_migration(self, toversion=None):
        captured["toversion"] = toversion
        return "0.9.11"

    monkeypatch.setattr(Migrate, "run_migration", fake_run_migration)

    result = PgstacCLI(dsn="postgresql:///example").migrate("0.9.11")

    assert result == "0.9.11"
    assert captured == {"toversion": "0.9.11"}


def test_migration_path_matches_pgstac_migrate_compat(tmp_path: Path) -> None:
    compat = import_module("pgstac_migrate.compat")

    (tmp_path / "pgstac--0.9.10.sql").write_text("-- base\n")
    (tmp_path / "pgstac--0.9.11.sql").write_text("-- base\n")
    (tmp_path / "pgstac--0.9.10--0.9.11.sql").write_text("-- step\n")

    left = PypgstacMigrationPath(str(tmp_path), "0.9.10", "0.9.11").migrations()
    right = compat.MigrationPath(str(tmp_path), "0.9.10", "0.9.11").migrations()

    assert left == ["pgstac--0.9.10--0.9.11.sql"]
    assert left == right
