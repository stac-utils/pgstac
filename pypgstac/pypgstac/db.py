from dataclasses import dataclass
import time
from typing import Dict, List, Optional, Union
import orjson
import psycopg
from psycopg import Connection, sql
from psycopg_pool import ConnectionPool
import atexit
import os
import logging
import sys


def pg_notice_handler(notice):
    msg = f"{notice.severity} - {notice.message_primary}"
    logging.info(msg)


@dataclass
class settings:
    db_min_conn_size = 0
    db_max_conn_size = 1
    db_max_queries = 5
    db_max_idle = 5
    db_num_workers = 1
    db_retries = 3


class PgstacDB:
    def __init__(
        self,
        dsn: Optional[str] = "",
        pool: Optional[ConnectionPool] = None,
        connection: Optional[Connection] = None,
        commit_on_exit: bool = True,
        debug: bool = True,
    ):
        self.dsn = dsn
        self.pool = pool
        self.connection = connection
        self.commit_on_exit = commit_on_exit
        self.initial_version = "0.1.9"
        self.debug = debug

    def get_pool(self):
        if self.pool is None:
            logging.debug("connecting to pool")
            self.pool = ConnectionPool(
                conninfo=self.dsn,
                min_size=settings.db_min_conn_size,
                max_size=settings.db_max_conn_size,
                max_waiting=settings.db_max_queries,
                max_idle=settings.db_max_idle,
                num_workers=settings.db_num_workers,
                kwargs={
                    "options": "-c search_path=pgstac,public -c application_name=pypgstac"
                },
            )
        return self.pool

    def open(self):
        print("open")
        self.get_pool()

    def close(self):
        print("close")
        if self.pool is not None:
            self.pool.close()

    def connect(self):
        pool = self.get_pool()
        if self.connection is None:
            self.connection = pool.getconn()
            if self.debug:
                self.connection.add_notice_handler(pg_notice_handler)
        atexit.register(self.disconnect)
        return self.connection

    def wait(self):
        print("wait")
        cnt: int = 0
        while cnt < 60:
            try:
                self.connect()
                self.query("SELECT 1;")
                return True
            except psycopg.errors.OperationalError:
                time.sleep(1)
                cnt += 1
        raise psycopg.errors.CannotConnectNow

    def disconnect(self):
        logging.debug('Disconnecting from pool.')
        pool = self.get_pool()
        if self.connection is not None:
            if self.commit_on_exit:
                self.connection.commit()
            pool.putconn(self.connection)
            self.connection = None
            self.pool = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()

    def exec(
        self,
        query: str,
        attempt: int = 1,
        e: psycopg.errors.OperationalError = None,
    ) -> Dict:
        if attempt > settings.db_retries:
            raise e
        conn = self.connect()
        logging.info(f"connection: {conn}")
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, prepare=False)
                logging.debug(f"""
                    {cursor.statusmessage},
                    {cursor.rowcount},
                    {cursor._query}
                """)
        except psycopg.errors.OperationalError as e:
            # If we get an operational error check the pool and retry
            logging.warning(f"OPERATIONAL ERROR: {e}")
            self.pool.check()
            self.exec(query, attempt + 1, e)

    def query(
        self,
        query: str,
        args: Optional[List] = None,
        row_factory: psycopg.rows.BaseRowFactory = psycopg.rows.tuple_row,
        attempt: int = 1,
        e: psycopg.errors.OperationalError = None,
    ) -> Dict:
        if attempt > settings.db_retries:
            raise e
        conn = self.connect()
        logging.debug(f"connection: {conn}")
        try:
            with self.connection.cursor(row_factory=row_factory) as cursor:
                rows = cursor.execute(query, args)
                for row in rows:
                    yield row
        except psycopg.errors.OperationalError as e:
            # If we get an operational error check the pool and retry
            print(f"OPERATIONAL ERROR: {e}")
            self.pool.check()
            for row in self.query(query, args, row_factory, attempt + 1, e):
                yield row
        # finally:
        #     if not self._is_context_manager:
        #         self.disconnect()

    def query_one(self, *args, **kwargs) -> Dict:
        """Return results from a query that returns a single row.
        If the result is a single column, only return the value of that column."""
        r = next(self.query(*args, **kwargs))
        if r is None:
            return None
        if len(r) == 1:
            return r[0]
        return r

    @property
    def version(self) -> str:
        """Get the current version number from a pgstac database."""
        try:
            version = self.query_one(
                """
                SELECT version from pgstac.migrations
                order by datetime desc, version desc limit 1;
                """
            )
        except psycopg.errors.UndefinedTable:
            self.connection.rollback()
            version = None
        return version

    @property
    def pg_version(self) -> str:
        """Get the current pg version number from a pgstac database."""
        version = self.query_one(
            """
            SHOW server_version;
            """
        )
        if int(version.split(".")[0]) < 13:
            raise Exception("PGStac requires PostgreSQL 13+")
        return version


    def func(self, function_name: str, *args):
        placeholders = sql.SQL(', ').join(sql.Placeholder() * len(args))
        func = sql.Identifier(function_name)
        base_query = sql.SQL("SELECT * FROM {}({});").format(func, placeholders)
        return self.query(base_query, args)



    def search(self, query: Union[dict, str, None] = '{}'):
        if isinstance(query, dict):
            query = psycopg.types.json.Jsonb(query)
        return next(self.func("search", query))[0]
