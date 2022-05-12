"""Fixtures for pypgstac tests."""
from typing import Generator
import pytest
import os
import psycopg
from pypgstac.db import PgstacDB
from pypgstac.migrate import Migrate
from pypgstac.load import Loader


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
                except:
                    pass

    os.environ["PGDATABASE"] = "pgstactestdb"

    pgdb = PgstacDB()

    yield pgdb

    print("Closing Connection and Dropping DB")
    pgdb.close()
    os.environ["PGDATABASE"] = origdb

    with psycopg.connect(autocommit=True) as conn:
        try:
            conn.execute("DROP DATABASE pgstactestdb WITH (FORCE);")
        except psycopg.errors.InsufficientPrivilege:
            try:
                conn.execute("DROP DATABASE pgstactestdb;")
            except:
                pass


@pytest.fixture(scope="function")
def loader(db: PgstacDB) -> Generator:
    """Fixture to get a loader and an empty pgstac."""
    db.query("DROP SCHEMA IF EXISTS pgstac CASCADE;")
    migrator = Migrate(db)
    print(migrator.run_migration())
    ldr = Loader(db)
    yield ldr
