"""Test Hydration in PGStac."""
from .test_hydrate import TestHydrate as THydrate
from typing import Dict, Any
import os
from typing import Generator
from pypgstac.db import PgstacDB
from pypgstac.migrate import Migrate
import psycopg
from contextlib import contextmanager


class TestHydratePG(THydrate):
    """Test hydration using PGStac."""

    @contextmanager
    def db(self) -> Generator[PgstacDB, None, None]:
        """Set up database."""
        print("Setting up db.")
        origdb: str = os.getenv("PGDATABASE", "")
        with psycopg.connect(autocommit=True) as conn:
            try:
                conn.execute("CREATE DATABASE pgstactestdb;")
            except psycopg.errors.DuplicateDatabase:
                pass

        os.environ["PGDATABASE"] = "pgstactestdb"

        pgdb = PgstacDB()
        pgdb.query("DROP SCHEMA IF EXISTS pgstac CASCADE;")
        migrator = Migrate(pgdb)
        print(migrator.run_migration())

        yield pgdb

        print("Closing Connection to DB")
        pgdb.close()
        os.environ["PGDATABASE"] = origdb

    def hydrate(
        self, base_item: Dict[str, Any], item: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Hydrate using pgstac."""
        with self.db() as db:
            return next(db.func("content_hydrate", item, base_item))[0]
