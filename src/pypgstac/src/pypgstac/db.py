"""Base library for database interaction with PgSTAC."""

import contextlib
import logging
import time
from dataclasses import dataclass, field
from types import TracebackType
from typing import Any, Generator, List, Optional, Tuple, Type, Union

import orjson
import psycopg
from psycopg import Connection, sql
from psycopg.types.json import set_json_dumps, set_json_loads
from psycopg_pool import ConnectionPool

try:
    from pydantic.v1 import BaseSettings  # type:ignore
except ImportError:
    from pydantic import BaseSettings  # type:ignore

from tenacity import retry, retry_if_exception_type, stop_after_attempt

logger = logging.getLogger(__name__)


def dumps(data: dict) -> str:
    """Dump dictionary as string."""
    return orjson.dumps(data).decode()


set_json_dumps(dumps)
set_json_loads(orjson.loads)


def pg_notice_handler(notice: psycopg.errors.Diagnostic) -> None:
    """Add PG messages to logging."""
    msg = f"{notice.severity} - {notice.message_primary}"
    logger.info(msg)


class Settings(BaseSettings):
    """Base Settings for Database Connection."""

    db_min_conn_size: int = 0
    db_max_conn_size: int = 1
    db_max_queries: int = 5
    db_max_idle: int = 5
    db_num_workers: int = 1
    db_retries: int = 3

    model_config = {"env_file": ".env", "extra": "ignore"}


settings = Settings()


@dataclass
class PgstacDB:
    """Base class for interacting with PgSTAC Database."""

    dsn: str
    commit_on_exit: bool = True
    debug: bool = False
    use_queue: bool = False

    pool: ConnectionPool = field(default=None)

    initial_version: str = field(init=False, default="0.1.9")

    _pool: ConnectionPool = field(init=False)

    def __post_init__(self):
        if not self.pool:
            self._pool = ConnectionPool(
                conninfo=self.dsn,
                min_size=settings.db_min_conn_size,
                max_size=settings.db_max_conn_size,
                max_waiting=settings.db_max_queries,
                max_idle=settings.db_max_idle,
                num_workers=settings.db_num_workers,
                open=True,
            )

    def get_pool(self) -> ConnectionPool:
        """Get Database Pool."""
        return self.pool or self._pool

    def close(self) -> None:
        """Close database pool connection."""
        if self._pool is not None:
            self._pool.close()

    def __enter__(self) -> Any:
        """Enter used for context."""
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        """Exit used for context."""
        self.close()

    @contextlib.contextmanager
    def connect(self) -> Connection:
        """Return database connection."""
        pool = self.get_pool()
        try:
            conn = pool.getconn()
            conn.autocommit = True
            if self.debug:
                conn.add_notice_handler(pg_notice_handler)
                conn.execute(
                    "SET CLIENT_MIN_MESSAGES TO NOTICE;",
                    prepare=False,
                )
            if self.use_queue:
                conn.execute(
                    "SET pgstac.use_queue TO TRUE;",
                    prepare=False,
                )

            conn.execute(
                """
                    SELECT
                        CASE
                        WHEN
                        current_setting('search_path', false) ~* '\\mpgstac\\M'
                        THEN current_setting('search_path', false)
                        ELSE set_config(
                            'search_path',
                            'pgstac,' || current_setting('search_path', false),
                            false
                            )
                        END
                    ;
                    SET application_name TO 'pgstac';
                """,
                prepare=False,
            )
            with conn:
                yield conn

        finally:
            pool.putconn(conn)

    def wait(self) -> None:
        """Block until database connection is ready."""
        cnt: int = 0
        while cnt < 60:
            try:
                self.query("SELECT 1;")
                return None
            except psycopg.errors.OperationalError:
                time.sleep(1)
                cnt += 1
        raise psycopg.errors.CannotConnectNow

    @retry(
        stop=stop_after_attempt(settings.db_retries),
        retry=retry_if_exception_type(psycopg.errors.OperationalError),
        reraise=True,
    )
    def query(
        self,
        query: Union[str, sql.Composed],
        args: Optional[List[Any]] = None,
        row_factory: psycopg.rows.BaseRowFactory = psycopg.rows.tuple_row,
    ) -> Generator:
        """Query the database with parameters."""
        with self.connect() as conn:
            try:
                with conn.cursor(row_factory=row_factory) as cursor:
                    if args is None:
                        rows = cursor.execute(query, prepare=False)
                    else:
                        rows = cursor.execute(query, args)
                    if rows:
                        for row in rows:
                            yield row
                    else:
                        yield None
            except psycopg.errors.OperationalError as e:
                # If we get an operational error check the pool and retry
                logger.warning(f"OPERATIONAL ERROR: {e}")
                self._pool.check()
                raise e
            except psycopg.errors.DatabaseError as e:
                if conn is not None:
                    conn.rollback()
                raise e

    def query_one(self, *args: Any, **kwargs: Any) -> Union[Tuple, str, None]:
        """Return results from a query that returns a single row."""
        try:
            r = next(self.query(*args, **kwargs))
        except StopIteration:
            return None

        if r is None:
            return None
        if len(r) == 1:
            return r[0]
        return r

    def run_queued(self) -> str:
        try:
            with self.connect() as conn:
                conn.execute("CALL run_queued_queries();")
                return "Ran Queued Queries"
        except Exception as e:
            return f"Error Running Queued Queries: {e}"

    @property
    def version(self) -> Optional[str]:
        """Get the current version number from a pgstac database."""
        try:
            version = self.query_one(
                """
                SELECT version from pgstac.migrations
                order by datetime desc, version desc limit 1;
                """,
            )
            logger.debug(f"VERSION: {version}")
            if isinstance(version, bytes):
                version = version.decode()
            if isinstance(version, str):
                return version
        except psycopg.errors.UndefinedTable:
            logger.debug("PgSTAC is not installed.")
        return None

    @property
    def pg_version(self) -> str:
        """Get the current pg version number from a pgstac database."""
        version = self.query_one(
            """
            SHOW server_version_num;
            """,
        )
        logger.debug(f"PG VERSION: {version}.")
        if isinstance(version, bytes):
            version = version.decode()
        if isinstance(version, str):
            if int(version) < 130000:
                major, minor, patch = tuple(
                    map(int, [version[i : i + 2] for i in range(0, len(version), 2)]),
                )
                raise Exception(
                    f"PgSTAC requires PostgreSQL 13+, current version is: {major}.{minor}.{patch}",
                )  # noqa: E501
            return version
        else:
            raise Exception("Could not find PG version.")

    def func(self, function_name: str, *args: Any) -> Generator:
        """Call a database function."""
        placeholders = sql.SQL(", ").join(sql.Placeholder() * len(args))
        func = sql.Identifier(function_name)
        cleaned_args = []
        for arg in args:
            if isinstance(arg, dict):
                cleaned_args.append(psycopg.types.json.Jsonb(arg))
            else:
                cleaned_args.append(arg)
        base_query = sql.SQL("SELECT * FROM {}({});").format(func, placeholders)
        return self.query(base_query, cleaned_args)

    def search(self, query: Union[dict, str, psycopg.types.json.Jsonb] = "{}") -> str:
        """Search PgSTAC."""
        return dumps(next(self.func("search", query))[0])
