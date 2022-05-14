"""Base library for database interaction with PgStac."""
import time
from types import TracebackType
from typing import Any, List, Optional, Tuple, Type, Union, Generator
import orjson
import psycopg
from psycopg import Connection, sql
from psycopg.types.json import set_json_dumps, set_json_loads
from psycopg_pool import ConnectionPool
import atexit
import logging
from pydantic import BaseSettings
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

    class Config:
        """Use .env file if available."""

        env_file = ".env"


settings = Settings()


class PgstacDB:
    """Base class for interacting with PgStac Database."""

    def __init__(
        self,
        dsn: Optional[str] = "",
        pool: Optional[ConnectionPool] = None,
        connection: Optional[Connection] = None,
        commit_on_exit: bool = True,
        debug: bool = False,
    ) -> None:
        """Initialize Database."""
        self.dsn: str
        if dsn is not None:
            self.dsn = dsn
        else:
            self.dsn = ""
        self.pool = pool
        self.connection = connection
        self.commit_on_exit = commit_on_exit
        self.initial_version = "0.1.9"
        self.debug = debug
        if self.debug:
            logging.basicConfig(level=logging.DEBUG)

    def get_pool(self) -> ConnectionPool:
        """Get Database Pool."""
        if self.pool is None:
            self.pool = ConnectionPool(
                conninfo=self.dsn,
                min_size=settings.db_min_conn_size,
                max_size=settings.db_max_conn_size,
                max_waiting=settings.db_max_queries,
                max_idle=settings.db_max_idle,
                num_workers=settings.db_num_workers,
                kwargs={
                    "options": "-c search_path=pgstac,public"
                    " -c application_name=pypgstac"
                },
            )
        return self.pool

    def open(self) -> None:
        """Open database pool connection."""
        self.get_pool()

    def close(self) -> None:
        """Close database pool connection."""
        if self.pool is not None:
            self.pool.close()

    def connect(self) -> Connection:
        """Return database connection."""
        pool = self.get_pool()
        if self.connection is None:
            self.connection = pool.getconn()
            self.connection.autocommit = True
            if self.debug:
                self.connection.add_notice_handler(pg_notice_handler)
            atexit.register(self.disconnect)
        return self.connection

    def wait(self) -> None:
        """Block until database connection is ready."""
        cnt: int = 0
        while cnt < 60:
            try:
                self.connect()
                self.query("SELECT 1;")
                return None
            except psycopg.errors.OperationalError:
                time.sleep(1)
                cnt += 1
        raise psycopg.errors.CannotConnectNow

    def disconnect(self) -> None:
        """Disconnect from database."""
        try:
            if self.commit_on_exit:
                if self.connection is not None:
                    self.connection.commit()
                if self.connection is not None:
                    self.connection.rollback()
        except:
            pass
        try:
            if self.pool is not None and self.connection is not None:
                self.pool.putconn(self.connection)
        except:
            pass

        self.connection = None
        self.pool = None

    def __enter__(self) -> Any:
        """Enter used for context."""
        self.connect()
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        """Exit used for context."""
        self.disconnect()

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
        conn = self.connect()
        try:
            with conn.cursor(row_factory=row_factory) as cursor:
                if args is None:
                    rows = cursor.execute(query, prepare=False)
                    for row in rows:
                        yield row
                else:
                    rows = cursor.execute(query, args)
                    for row in rows:
                        yield row
        except psycopg.errors.OperationalError as e:
            # If we get an operational error check the pool and retry
            logger.warning(f"OPERATIONAL ERROR: {e}")
            if self.pool is None:
                self.get_pool()
            else:
                self.pool.check()
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

    @property
    def version(self) -> Optional[str]:
        """Get the current version number from a pgstac database."""
        try:
            version = self.query_one(
                """
                SELECT version from pgstac.migrations
                order by datetime desc, version desc limit 1;
                """
            )
            logger.debug(f"VERSION: {version}")
            if isinstance(version, bytes):
                version = version.decode()
            if isinstance(version, str):
                return version
        except psycopg.errors.UndefinedTable:
            logger.debug("PGStac is not installed.")
            if self.connection is not None:
                self.connection.rollback()
        return None

    @property
    def pg_version(self) -> str:
        """Get the current pg version number from a pgstac database."""
        version = self.query_one(
            """
            SHOW server_version;
            """
        )
        logger.debug(f"PG VERSION: {version}.")
        if isinstance(version, bytes):
            version = version.decode()
        if isinstance(version, str):
            if int(version.split(".")[0]) < 13:
                raise Exception("PGStac requires PostgreSQL 13+")
            return version
        else:
            if self.connection is not None:
                self.connection.rollback()
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
        """Search PgStac."""
        return dumps(next(self.func("search", query))[0])
