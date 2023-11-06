"""Test Hydration in PGStac."""
import os
from contextlib import contextmanager
from typing import Any, Dict, Generator

import psycopg

from pypgstac.db import PgstacDB
from pypgstac.migrate import Migrate

from .test_hydrate import TestHydrate as THydrate


class TestHydratePG(THydrate):
    """Test hydration using PGStac."""

    @contextmanager
    def db(self) -> Generator[PgstacDB, None, None]:
        """Set up database."""
        origdb: str = os.getenv("PGDATABASE", "")
        with psycopg.connect(autocommit=True) as conn:
            try:
                conn.execute("CREATE DATABASE pgstactestdb;")
            except psycopg.errors.DuplicateDatabase:
                pass

        os.environ["PGDATABASE"] = "pgstactestdb"

        pgdb = PgstacDB()
        with psycopg.connect(autocommit=True) as conn:
            conn.execute("DROP SCHEMA IF EXISTS pgstac CASCADE;")
        Migrate(pgdb).run_migration()

        yield pgdb

        pgdb.close()
        os.environ["PGDATABASE"] = origdb

    def hydrate(
        self, base_item: Dict[str, Any], item: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Hydrate using pgstac."""
        with self.db() as db:
            return next(db.func("content_hydrate", item, base_item))[0]
