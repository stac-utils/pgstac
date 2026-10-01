"""Tests for how migration files are read."""

import re
from pathlib import Path

from pypgstac.migrate import get_sql, migrations_dir


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
