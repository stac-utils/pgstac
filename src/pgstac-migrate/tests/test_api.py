from importlib import import_module
from pathlib import Path
from types import SimpleNamespace


def test_normalize_target_version_maps_dev_to_unreleased() -> None:
    api = import_module("pgstac_migrate.api")

    assert api.normalize_target_version("0.9.11-dev") == "unreleased"
    assert api.normalize_target_version("0.9.11") == "0.9.11"
    assert api.normalize_target_version(None) is None


def test_artifact_path_builds_from_source_when_missing(
    monkeypatch, tmp_path: Path
) -> None:
    api = import_module("pgstac_migrate.api")
    artifact = tmp_path / "migrations.tar.zst"

    monkeypatch.setattr(api, "ensure_artifact_path", lambda: artifact)

    assert api.artifact_path() == artifact


def test_migrate_uses_artifact_api(monkeypatch, tmp_path: Path) -> None:
    api = import_module("pgstac_migrate.api")
    artifact = tmp_path / "migrations.tar.zst"
    captured: dict[str, object] = {}

    def fake_migrate_from_artifact(path: str, **kwargs):
        captured["path"] = path
        captured.update(kwargs)
        return SimpleNamespace(final_version="0.9.11")

    monkeypatch.setattr(api, "artifact_path", lambda: artifact)
    monkeypatch.setattr(api, "migrate_from_artifact", fake_migrate_from_artifact)

    result = api.migrate(target="0.9.11-dev", conninfo="postgresql:///example")

    assert result.final_version == "0.9.11"
    assert captured["path"] == str(artifact)
    assert captured["target"] == "unreleased"
    assert captured["conninfo"] == "postgresql:///example"
    assert captured["version_source"].__class__.__name__ == "PgstacVersionSource"
