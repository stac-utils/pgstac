from importlib import import_module
from pathlib import Path
from types import SimpleNamespace

import pytest


def run_cli(argv: list[str]) -> int:
    return import_module("pgstac_migrate.cli").main(argv)


@pytest.fixture(scope="module", autouse=True)
def ensure_baked_artifact() -> None:
    package_root = Path(__file__).resolve().parents[1]
    artifact_path = package_root / "src" / "pgstac_migrate" / "migrations.tar.zst"
    if artifact_path.is_file():
        return

    exit_code = run_cli(["build-artifact"])
    if exit_code != 0:
        raise RuntimeError("pgstac-migrate build-artifact failed during test bootstrap")


def test_build_artifact_command_reports_output(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    built = tmp_path / "migrations.tar.zst"
    monkeypatch.setattr(
        import_module("pgstac_migrate.cli"),
        "build_local_artifact",
        lambda: built,
    )

    exit_code = run_cli(["build-artifact"])

    captured = capsys.readouterr()
    assert exit_code == 0
    assert f"wrote {built}" in captured.out


def test_versions_lists_known_versions(capsys) -> None:
    exit_code = run_cli(["versions"])

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "0.1.9" in captured.out.splitlines()
    assert "unreleased" in captured.out.splitlines()


def test_plan_renders_known_incremental_step(capsys) -> None:
    exit_code = run_cli(["plan", "--source", "0.9.10", "--to", "0.9.11"])

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "0.9.10 -> 0.9.11" in captured.out
    assert "pgstac--0.9.10--0.9.11.sql" in captured.out


def test_migrate_delegates_to_api(monkeypatch, capsys) -> None:
    cli_module = import_module("pgstac_migrate.cli")
    captured_kwargs: dict[str, object] = {}

    def fake_migrate_database(**kwargs):
        captured_kwargs.update(kwargs)
        return SimpleNamespace(
            bootstrapped_from="0.9.10",
            applied_steps=[("0.9.10", "0.9.11")],
            final_version="0.9.11",
        )

    monkeypatch.setattr(cli_module, "migrate_database", fake_migrate_database)

    exit_code = run_cli(
        [
            "migrate",
            "--to",
            "0.9.11",
            "--dry-run",
            "--dsn",
            "postgresql:///example",
        ]
    )

    output = capsys.readouterr().out
    assert exit_code == 0
    assert captured_kwargs == {
        "target": "0.9.11",
        "dry_run": True,
        "conninfo": "postgresql:///example",
        "host": None,
        "port": None,
        "dbname": None,
        "user": None,
        "password": None,
    }
    assert "bootstrapped to 0.9.10" in output
    assert "applied 0.9.10 -> 0.9.11" in output
    assert "final version: 0.9.11" in output
    assert "(dry-run: rolled back)" in output
