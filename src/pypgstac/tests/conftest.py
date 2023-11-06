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
            conn.execute(
                """
                CREATE DATABASE pypgstactestdb
                TEMPLATE pgstac_test_db_template;
                """,
            )
        except psycopg.errors.DuplicateDatabase:
            try:
                conn.execute(
                    """
                    DROP DATABASE pypgstactestdb WITH (FORCE);
                    """,
                )
                conn.execute(
                    """
                    CREATE DATABASE pypgstactestdb
                    TEMPLATE pgstac_test_db_template;
                    """,
                )
            except psycopg.errors.InsufficientPrivilege:
                try:
                    conn.execute("DROP DATABASE pypgstactestdb;")
                    conn.execute(
                    """
                    CREATE DATABASE pypgstactestdb
                    TEMPLATE pgstac_test_db_template;
                    """,
                )
                except Exception:
                    pass

    os.environ["PGDATABASE"] = "pypgstactestdb"

    pgdb = PgstacDB()

    yield pgdb

    pgdb.close()
    os.environ["PGDATABASE"] = origdb

    with psycopg.connect(autocommit=True) as conn:
        try:
            conn.execute("DROP DATABASE pypgstactestdb WITH (FORCE);")
        except psycopg.errors.InsufficientPrivilege:
            try:
                conn.execute("DROP DATABASE pypgstactestdb;")
            except Exception:
                pass


@pytest.fixture(scope="function")
def loader(db: PgstacDB) -> Generator:
    """Fixture to get a loader and an empty pgstac."""
    if False:
        db.query("DROP SCHEMA IF EXISTS pgstac CASCADE;")
        Migrate(db).run_migration()
    ldr = Loader(db)
    return ldr
