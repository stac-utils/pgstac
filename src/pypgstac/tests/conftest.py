"""Fixtures for pypgstac tests."""
import os
from typing import Generator

import psycopg
import pytest

from pypgstac.db import PgstacDB
from pypgstac.load import Loader
from pypgstac.migrate import Migrate


@pytest.fixture(scope="function")
def db() -> Generator:
    """Fixture to get a fresh database."""
    origdb: str = os.getenv("PGDATABASE", "")

    with psycopg.connect(autocommit=True) as conn:
        try:
            conn.execute("CREATE DATABASE pgstactestdb;")
        except psycopg.errors.DuplicateDatabase:
            try:
                conn.execute("DROP DATABASE pgstactestdb WITH (FORCE);")
                conn.execute("CREATE DATABASE pgstactestdb;")
            except psycopg.errors.InsufficientPrivilege:
                try:
                    conn.execute("DROP DATABASE pgstactestdb;")
                    conn.execute("CREATE DATABASE pgstactestdb;")
                except Exception:
                    pass

    os.environ["PGDATABASE"] = "pgstactestdb"

    pgdb = PgstacDB()

    yield pgdb

    pgdb.close()
    os.environ["PGDATABASE"] = origdb

    with psycopg.connect(autocommit=True) as conn:
        try:
            conn.execute("DROP DATABASE pgstactestdb WITH (FORCE);")
        except psycopg.errors.InsufficientPrivilege:
            try:
                conn.execute("DROP DATABASE pgstactestdb;")
            except Exception:
                pass


@pytest.fixture(scope="function")
def loader(db: PgstacDB) -> Generator:
    """Fixture to get a loader and an empty pgstac."""
    db.query("DROP SCHEMA IF EXISTS pgstac CASCADE;")
    Migrate(db)
    ldr = Loader(db)
    return ldr
