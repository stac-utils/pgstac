from importlib import import_module
from types import SimpleNamespace

import pytest


class FakeCursor:
    def __init__(
        self,
        *,
        fetchone_results: list[tuple[object, ...] | None],
        executed: list[tuple[str, object | None]],
    ):
        self._fetchone_results = fetchone_results
        self._executed = executed

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, query, params=None):
        self._executed.append((str(query), params))

    def fetchone(self):
        if not self._fetchone_results:
            raise AssertionError("No fetchone result queued")
        return self._fetchone_results.pop(0)


class FakeConnection:
    def __init__(self, *, fetchone_results: list[tuple[object, ...] | None]):
        self.executed: list[tuple[str, object | None]] = []
        self._fetchone_results = fetchone_results

    def cursor(self):
        return FakeCursor(
            fetchone_results=self._fetchone_results, executed=self.executed
        )


@pytest.fixture
def version_source_module():
    return import_module("pgstac_migrate.version_source")


def test_record_applied_uses_set_version_when_available(
    monkeypatch, version_source_module
) -> None:
    source = version_source_module.PgstacVersionSource()
    conn = FakeConnection(fetchone_results=[("pgstac.set_version(text)",)])
    config = SimpleNamespace(tracking_schema="pgpkg", tracking_table="migrations")

    monkeypatch.setattr(
        version_source_module,
        "current_tracking_version",
        lambda *args, **kwargs: "0.3.0",
    )
    monkeypatch.setattr(source, "read_live_version", lambda *args, **kwargs: "0.3.0")

    source.record_applied(
        conn,
        config,
        version="0.3.0",
        sha256="ignored",
        filename="pgstac--0.3.0.sql",
    )

    assert conn.executed == [
        ("SELECT to_regprocedure('pgstac.set_version(text)')", None),
        ("SELECT pgstac.set_version(%s)", ("0.3.0",)),
    ]


def test_record_applied_falls_back_to_direct_insert_without_set_version(
    monkeypatch, version_source_module
) -> None:
    source = version_source_module.PgstacVersionSource()
    conn = FakeConnection(fetchone_results=[(None,)])
    config = SimpleNamespace(tracking_schema="pgpkg", tracking_table="migrations")

    monkeypatch.setattr(
        version_source_module,
        "current_tracking_version",
        lambda *args, **kwargs: "0.3.0",
    )
    monkeypatch.setattr(source, "read_live_version", lambda *args, **kwargs: "0.3.0")

    source.record_applied(
        conn,
        config,
        version="0.3.0",
        sha256="ignored",
        filename="pgstac--0.3.0.sql",
    )

    assert conn.executed == [
        ("SELECT to_regprocedure('pgstac.set_version(text)')", None),
        ("INSERT INTO pgstac.migrations (version) VALUES (%s)", ("0.3.0",)),
    ]
