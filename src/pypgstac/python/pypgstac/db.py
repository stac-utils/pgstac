"""Base library for database interaction with PgSTAC."""

import atexit
import logging
import time
from types import TracebackType
from typing import (
    Any,
    Callable,
    Dict,
    Generator,
    List,
    Optional,
    Sequence,
    Tuple,
    Type,
    Union,
)

import orjson
import psycopg
from cachetools import LRUCache, cachedmethod
from psycopg import Connection, sql
from psycopg.types.json import set_json_dumps, set_json_loads
from psycopg_pool import ConnectionPool

try:
    from pydantic.v1 import BaseSettings  # type:ignore
except ImportError:
    from pydantic import BaseSettings  # type:ignore

import psycopg_infdate
import pyarrow as pa
import shapely
from stac_geoparquet.to_arrow import _process_arrow_table as cleanarrow
from tenacity import retry, retry_if_exception_type, stop_after_attempt
from version_parser import Version as V

from .hydration import hydrate
from .version import __version__ as pypgstac_version

logger = logging.getLogger(__name__)


def dumps(data: dict) -> str:
    """Dump dictionary as string."""
    return orjson.dumps(data).decode()


set_json_dumps(dumps)
set_json_loads(orjson.loads)
psycopg_infdate.register_inf_date_handler(psycopg)


def pg_notice_handler(notice: psycopg.errors.Diagnostic) -> None:
    """Add PG messages to logging."""
    msg = f"{notice.severity} - {notice.message_primary}"
    logger.info(msg)


def _chunks(
    lst: Sequence[Dict[str, Any]],
    n: int,
) -> Generator[Sequence[Dict[str, Any]], None, None]:
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


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


class PgstacDB:
    """Base class for interacting with PgSTAC Database."""

    def __init__(
        self,
        dsn: Optional[str] = "",
        pool: Optional[ConnectionPool] = None,
        connection: Optional[Connection] = None,
        commit_on_exit: bool = True,
        debug: bool = False,
        use_queue: bool = False,
        item_funcs: Optional[List[Callable]] = None,
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
        self.use_queue = use_queue
        self.item_funcs = item_funcs
        if self.debug:
            logging.basicConfig(level=logging.DEBUG)
        self.cache: LRUCache = LRUCache(maxsize=256)

    def check_version(self) -> None:
        db_version = self.version
        if db_version is None:
            raise Exception("Failed to detect the target database version.")

        if db_version != "unreleased":
            v1 = V(db_version)
            v2 = V(pypgstac_version)
            if (v1.get_major_version(), v1.get_minor_version()) != (
                v2.get_major_version(),
                v2.get_minor_version(),
            ):
                raise Exception(
                    f"pypgstac version {pypgstac_version}"
                    " is not compatible with the target"
                    f" database version {self.version}."
                    f" database version {db_version}.",
                )

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
                self.connection.execute(
                    "SET CLIENT_MIN_MESSAGES TO NOTICE;",
                    prepare=False,
                )
            if self.use_queue:
                self.connection.execute(
                    "SET pgstac.use_queue TO TRUE;",
                    prepare=False,
                )
            atexit.register(self.disconnect)
            self.connection.execute(
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
            if self.connection is not None:
                if self.commit_on_exit:
                    self.connection.commit()
                else:
                    self.connection.rollback()
        except Exception:
            pass
        try:
            if self.pool is not None and self.connection is not None:
                self.pool.putconn(self.connection)
        except Exception:
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
            if self.pool is None:
                self.get_pool()
            else:
                self.pool.check()
            raise e
        except psycopg.errors.DatabaseError as e:
            if conn is not None:
                conn.rollback()
            raise e

    def query_one(self, *args: Any, **kwargs: Any) -> Union[Tuple, str, dict, None]:
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
            self.connect().execute("""
                CALL run_queued_queries();
            """)
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
            if self.connection is not None:
                self.connection.rollback()
        return None

    @property
    def pg_version(self) -> str:
        """Get the current pg version number from a pgstac database."""
        version = self.query_one(
            """
            SHOW server_version;
            """,
        )
        logger.debug(f"PG VERSION: {version}.")
        if isinstance(version, bytes):
            version = version.decode()
        if isinstance(version, str):
            if int(version.split(".")[0]) < 13:
                raise Exception("PgSTAC requires PostgreSQL 13+")
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

    @cachedmethod(lambda self: self.cache)
    def collection_baseitem(self, collection_id: str) -> dict:
        """Get collection."""
        base_item = self.query_one(
            "SELECT base_item FROM collections WHERE id=%s",
            (collection_id,),
        )
        if not isinstance(base_item, dict):
            raise Exception(
                f"Collection {collection_id} is not present in the database",
            )
        logger.debug(f"Found {collection_id} with base_item {base_item}")
        return base_item

    def pgstac_row_reader(
        self,
        id,
        collection,
        geometry,
        datetime,
        end_datetime,
        content,
    ):
        """Read pgstac item, hydrate it, and convert to item stac json formatted dict."""
        base_item = self.collection_baseitem(collection)
        content["id"] = id
        content["collection"] = collection
        content["geometry"] = geometry
        if datetime == end_datetime and "datetime" not in content["properties"]:
            content["properties"]["datetime"] = datetime
        elif datetime != end_datetime:
            if "start_datetime" not in content["properties"]:
                content["properties"]["start_datetime"] = datetime
            if "end_datetime" not in content["properties"]:
                content["properties"]["end_datetime"] = end_datetime
        if "bbox" not in content:
            geom = shapely.wkb.loads(geometry)
            content["bbox"] = list(geom.bounds)
        if "type" not in content:
            content["type"] = "Feature"
        content = hydrate(base_item, content)
        if self.item_funcs is not None:
            for func in self.item_funcs:
                content = func(content)
        return content

    def get_table(self, results):
        """Convert pgstac item row results to arrow table."""
        pylist = [self.pgstac_row_reader(*r) for r in results]
        table = pa.Table.from_pylist(pylist)
        table = cleanarrow(table)

        return table

    def search(self, query: Union[dict, str, psycopg.types.json.Jsonb] = "{}") -> str:
        """Search PgSTAC."""

        results = self.query(
            """
            SELECT
                id,
                collection,
                st_asbinary(geometry),
                datetime::text,
                end_datetime::text,
                content
            FROM search_items(%s);
        """,
            (query,),
        )
        return self.get_table(results)
