"""Tests for how migration files are read."""

import re
from pathlib import Path

import psycopg
import pytest

from pypgstac.db import PgstacDB
from pypgstac.migrate import Migrate, get_sql, migrations_dir


def _wrapped_files() -> list:
    """Migration files that carry their own BEGIN, in file order."""
    out = []
    for f in sorted(Path(migrations_dir).glob("pgstac.*.sql")):
        text = f.read_text()
        if re.match(r"\A\s*BEGIN\s*;", text, flags=re.IGNORECASE):
            out.append(f.name)
    return out


def test_wrapped_file_loses_both_begin_and_commit() -> None:
    """A file that opens with BEGIN is handed over with neither end.

    Every file of a chain is executed on one connection with autocommit off, so
    an embedded COMMIT would end that transaction and leave the files already
    applied committed when a later one fails.
    """
    wrapped = _wrapped_files()
    assert wrapped, "expected at least one migration to carry its own BEGIN"
    for name in wrapped:
        sql = get_sql(name)
        assert not re.match(r"\A\s*BEGIN\s*;", sql, flags=re.IGNORECASE), name
        assert not re.search(r"COMMIT\s*;\s*\Z", sql, flags=re.IGNORECASE), name


def test_an_unwrapped_file_keeps_its_trailing_commit() -> None:
    """Both ends go or neither.

    Four migrations from 0.2.x put SET SEARCH_PATH before their BEGIN. Removing
    only the trailing COMMIT would leave their BEGIN unmatched inside the
    transaction pypgstac is already running in.
    """
    wrapped = set(_wrapped_files())
    for f in sorted(Path(migrations_dir).glob("pgstac.*.sql")):
        if f.name in wrapped:
            continue
        original = f.read_text()
        if not re.search(r"COMMIT\s*;\s*\Z", original, flags=re.IGNORECASE):
            continue
        assert re.search(
            r"COMMIT\s*;\s*\Z", get_sql(f.name), flags=re.IGNORECASE,
        ), f.name


def test_a_commit_inside_a_body_is_left_alone() -> None:
    """Only the file's own trailing COMMIT is removed."""
    for name in _wrapped_files():
        original = (Path(migrations_dir) / name).read_text()
        inner = len(re.findall(r"^\s+COMMIT\s*;", original, flags=re.MULTILINE))
        if inner:
            assert (
                len(
                    re.findall(
                        r"^\s+COMMIT\s*;", get_sql(name), flags=re.MULTILINE,
                    ),
                )
                == inner
            ), name


TESTDATA = Path(__file__).parent.parent.parent / "pgstac" / "tests" / "testdata"


def _install(db: PgstacDB, version: str) -> None:
    """Replace the test database's schema with a release and the test collections."""
    db.connect().execute("DROP SCHEMA pgstac CASCADE;")
    Migrate(db).run_migration(version)
    conn = db.connect()
    conn.execute("RESET ROLE;")
    for line in (TESTDATA / "collections.ndjson").read_text().splitlines():
        conn.execute("INSERT INTO pgstac.collections (content) VALUES (%s);", [line])


@pytest.mark.parametrize(
    "rows",
    [
        """INSERT INTO pgstac.queryables (name, definition, property_path)
           VALUES ('unreadable', '{"type":"string"}', 'content #> ''{a,b}''');""",
        """INSERT INTO pgstac.queryables (name, collection_ids, definition) VALUES
           ('quality', '{pgstac-test-collection}', '{"type":"string"}'),
           ('properties.quality', '{pgstac-test-collection,pgstac-test-collection2}',
            '{"type":"string"}');""",
        """INSERT INTO pgstac.queryables (name, collection_ids, definition) VALUES
           ('quality', '{pgstac-test-collection,pgstac-test-collection2}',
            '{"type":"string"}'),
           ('properties.quality', NULL, '{"type":"string"}');""",
    ],
    ids=["unreadable-path", "lost-collection", "lost-global"],
)
def test_upgrade_stops_on_queryables_it_would_change(db: PgstacDB, rows: str) -> None:
    """An upgrade from 0.9 stops, changing nothing, rather than lose a queryable."""
    _install(db, "0.9.12")
    db.connect().execute(rows)
    with pytest.raises(psycopg.errors.RaiseException):
        Migrate(db).run_migration()
    db.connect().rollback()
    assert db.version == "0.9.12"


def test_upgrade_waits_for_a_lost_unique_id_index(db: PgstacDB) -> None:
    """An upgrade stops until a partition's lost unique id index is rebuilt."""
    _install(db, "0.10.0")
    conn = db.connect()
    item = (TESTDATA / "items.ndjson").read_text().splitlines()[0]
    conn.execute("INSERT INTO pgstac.items_staging (content) VALUES (%s);", [item])
    unique = """SELECT i.indexrelid::regclass::text FROM pg_index i
                JOIN pg_partition_tree('pgstac.items') t ON t.relid = i.indrelid
                WHERE t.isleaf AND i.indisunique;"""
    index = db.query_one(unique)
    conn.execute(f"DROP INDEX {index};")
    with pytest.raises(psycopg.errors.RaiseException, match="unique id index"):
        Migrate(db).run_migration()
    conn.rollback()
    conn.autocommit = True
    assert db.version == "0.10.0"

    partition = db.query_one(
        "SELECT relname FROM pg_class WHERE relname ~ '^_items_' AND relkind = 'r';",
    )
    conn.execute(f"CREATE UNIQUE INDEX {index} ON pgstac.{partition} (id);")
    assert Migrate(db).run_migration() == db.version != "0.10.0"
