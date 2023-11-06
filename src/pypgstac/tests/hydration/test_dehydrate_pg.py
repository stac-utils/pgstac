import os
from contextlib import contextmanager
from typing import Any, Dict, Generator

import psycopg

from pypgstac.db import PgstacDB
from pypgstac.migrate import Migrate

from .test_dehydrate import TestDehydrate as TDehydrate


class TestDehydratePG(TDehydrate):
    """Class to test Dehydration using pgstac."""

    @contextmanager
    def db(self) -> Generator:
        """Set up database connection."""
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

    def dehydrate(
        self, base_item: Dict[str, Any], item: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Dehydrate item using pgstac."""
        with self.db() as db:
            return next(db.func("strip_jsonb", item, base_item))[0]
