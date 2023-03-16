"""Utilities to bulk load data into pgstac from json/ndjson."""
import contextlib
import itertools
import logging
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import (
    Any,
    BinaryIO,
    Dict,
    Generator,
    Iterable,
    Iterator,
    Optional,
    TextIO,
    Tuple,
    Union,
)

import orjson
import psycopg
from cachetools.func import lru_cache
from orjson import JSONDecodeError
from plpygis.geometry import Geometry
from psycopg import sql
from smart_open import open
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_random_exponential,
)
from version_parser import Version as V

from .db import PgstacDB
from .hydration import dehydrate
from .version import __version__

logger = logging.getLogger(__name__)


@dataclass
class Partition:
    name: str
    collection: str
    datetime_range_min: str
    datetime_range_max: str
    end_datetime_range_min: str
    end_datetime_range_max: str
    requires_update: bool


def chunked_iterable(iterable: Iterable, size: Optional[int] = 10000) -> Iterable:
    """Chunk an iterable."""
    it = iter(iterable)
    while True:
        chunk = tuple(itertools.islice(it, size))
        if not chunk:
            break
        yield chunk


class Tables(str, Enum):
    """Available tables for loading."""

    items = "items"
    collections = "collections"


class Methods(str, Enum):
    """Available methods for loading data."""

    insert = "insert"
    ignore = "ignore"
    upsert = "upsert"
    delsert = "delsert"
    insert_ignore = "insert_ignore"


@contextlib.contextmanager
def open_std(
    filename: str, mode: str = "r", *args: Any, **kwargs: Any,
) -> Generator[Any, None, None]:
    """Open files and i/o streams transparently."""
    fh: Union[TextIO, BinaryIO]
    if (
        filename is None
        or filename == "-"
        or filename == "stdin"
        or filename == "stdout"
    ):
        stream = sys.stdin if "r" in mode else sys.stdout
        fh = stream.buffer if "b" in mode else stream
        close = False
    else:
        fh = open(filename, mode, *args, **kwargs)
        close = True

    try:
        yield fh
    finally:
        if close:
            try:
                fh.close()
            except AttributeError:
                pass


def read_json(file: Union[Path, str, Iterator[Any]] = "stdin") -> Iterable:
    """Load data from an ndjson or json file."""
    if file is None:
        file = "stdin"
    if isinstance(file, str):
        open_file: Any = open_std(file, "r")
        with open_file as f:
            # Try reading line by line as ndjson
            try:
                for line in f:
                    line = line.strip().replace("\\\\", "\\").replace("\\\\", "\\")
                    yield orjson.loads(line)
            except JSONDecodeError:
                # If reading first line as json fails, try reading entire file
                logger.info("First line could not be parsed as json, trying full file.")
                try:
                    f.seek(0)
                    json = orjson.loads(f.read())
                    if isinstance(json, list):
                        for record in json:
                            yield record
                    else:
                        yield json
                except JSONDecodeError:
                    logger.info("File cannot be read as json")
                    raise
    elif isinstance(file, Iterable):
        for line in file:
            if isinstance(line, Dict):
                yield line
            else:
                yield orjson.loads(line)


class Loader:
    """Utilities for loading data."""

    db: PgstacDB
    _partition_cache: Dict[str, Partition]

    def __init__(self, db: PgstacDB):
        self.db = db
        self._partition_cache: Dict[str, Partition] = {}

    def check_version(self) -> None:
        db_version = self.db.version
        if db_version is None:
            raise Exception("Failed to detect the target database version.")

        v1 = V(db_version)
        v2 = V(__version__)
        if (v1.get_major_version(), v1.get_minor_version()) != (
            v2.get_major_version(),
            v2.get_minor_version(),
        ):
            raise Exception(
                f"pypgstac version {__version__}"
                " is not compatible with the target"
                f" database version {self.db.version}."
                f" database version {db_version}.",
            )

    @lru_cache(maxsize=128)
    def collection_json(self, collection_id: str) -> Tuple[Dict[str, Any], int, str]:
        """Get collection."""
        res = self.db.query_one(
            "SELECT base_item, key, partition_trunc FROM collections WHERE id=%s",
            (collection_id,),
        )
        if isinstance(res, tuple):
            base_item, key, partition_trunc = res
        else:
            raise Exception(f"Error getting info for {collection_id}.")
        if key is None:
            raise Exception(
                f"Collection {collection_id} is not present in the database",
            )
        logger.debug(f"Found {collection_id} with base_item {base_item}")
        return base_item, key, partition_trunc

    def load_collections(
        self,
        file: Union[Path, str, Iterator[Any]] = "stdin",
        insert_mode: Optional[Methods] = Methods.insert,
    ) -> None:
        """Load a collections json or ndjson file."""
        self.check_version()

        if file is None:
            file = "stdin"
        conn = self.db.connect()
        with conn.cursor() as cur:
            with conn.transaction():
                cur.execute(
                    """
                    DROP TABLE IF EXISTS tmp_collections;
                    CREATE TEMP TABLE tmp_collections
                    (content jsonb) ON COMMIT DROP;
                    """,
                )
                with cur.copy("COPY tmp_collections (content) FROM stdin;") as copy:
                    for collection in read_json(file):
                        copy.write_row((orjson.dumps(collection).decode(),))
                if insert_mode in (
                    None,
                    Methods.insert,
                ):
                    cur.execute(
                        """
                        INSERT INTO collections (content)
                        SELECT content FROM tmp_collections;
                        """,
                    )
                    logger.debug(cur.statusmessage)
                    logger.debug(f"Rows affected: {cur.rowcount}")
                elif insert_mode in (
                    Methods.insert_ignore,
                    Methods.ignore,
                ):
                    cur.execute(
                        """
                        INSERT INTO collections (content)
                        SELECT content FROM tmp_collections
                        ON CONFLICT DO NOTHING;
                        """,
                    )
                    logger.debug(cur.statusmessage)
                    logger.debug(f"Rows affected: {cur.rowcount}")
                elif insert_mode == Methods.upsert:
                    cur.execute(
                        """
                        INSERT INTO collections (content)
                        SELECT content FROM tmp_collections
                        ON CONFLICT (id) DO
                        UPDATE SET content=EXCLUDED.content;
                        """,
                    )
                    logger.debug(cur.statusmessage)
                    logger.debug(f"Rows affected: {cur.rowcount}")
                else:
                    raise Exception(
                        "Available modes are insert, ignore, and upsert."
                        f"You entered {insert_mode}.",
                    )

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_random_exponential(multiplier=1, max=120),
        retry=(
            retry_if_exception_type(psycopg.errors.CheckViolation)
            | retry_if_exception_type(psycopg.errors.DeadlockDetected)
        ),
        reraise=True,
    )
    def load_partition(
        self,
        partition: Partition,
        items: Iterable[Dict[str, Any]],
        insert_mode: Optional[Methods] = Methods.insert,
    ) -> None:
        """Load items data for a single partition."""
        conn = self.db.connect()
        t = time.perf_counter()

        logger.debug(f"Loading data for partition: {partition}.")
        with conn.cursor() as cur:
            if partition.requires_update:
                with conn.transaction():
                    cur.execute(
                        """
                        SELECT check_partition(
                            %s,
                            tstzrange(%s, %s, '[]'),
                            tstzrange(%s, %s, '[]')
                        );
                    """,
                        (
                            partition.collection,
                            partition.datetime_range_min,
                            partition.datetime_range_max,
                            partition.end_datetime_range_min,
                            partition.end_datetime_range_max,
                        ),
                    )

                    logger.debug(
                        f"Adding or updating partition {partition.name} "
                        f"took {time.perf_counter() - t}s",
                    )
                partition.requires_update = False
            else:
                logger.debug(f"Partition {partition.name} does not require an update.")

            with conn.transaction():

                t = time.perf_counter()
                if insert_mode in (
                    None,
                    Methods.insert,
                ):
                    with cur.copy(
                        sql.SQL(
                            """
                            COPY {}
                            (id, collection, datetime, end_datetime, geometry, content)
                            FROM stdin;
                            """,
                        ).format(sql.Identifier(partition.name)),
                    ) as copy:
                        for item in items:
                            item.pop("partition")
                            copy.write_row(
                                (
                                    item["id"],
                                    item["collection"],
                                    item["datetime"],
                                    item["end_datetime"],
                                    item["geometry"],
                                    item["content"],
                                ),
                            )
                    logger.debug(cur.statusmessage)
                    logger.debug(f"Rows affected: {cur.rowcount}")
                elif insert_mode in (
                    Methods.insert_ignore,
                    Methods.upsert,
                    Methods.delsert,
                    Methods.ignore,
                ):
                    cur.execute(
                        """
                        DROP TABLE IF EXISTS items_ingest_temp;
                        CREATE TEMP TABLE items_ingest_temp
                        ON COMMIT DROP AS SELECT * FROM items LIMIT 0;
                        """,
                    )
                    with cur.copy(
                        """
                        COPY items_ingest_temp
                        (id, collection, datetime, end_datetime, geometry, content)
                        FROM stdin;
                        """,
                    ) as copy:
                        for item in items:
                            item.pop("partition")
                            copy.write_row(
                                (
                                    item["id"],
                                    item["collection"],
                                    item["datetime"],
                                    item["end_datetime"],
                                    item["geometry"],
                                    item["content"],
                                ),
                            )
                    logger.debug(cur.statusmessage)
                    logger.debug(f"Copied rows: {cur.rowcount}")

                    cur.execute(
                        sql.SQL(
                            """
                                LOCK TABLE ONLY {} IN EXCLUSIVE MODE;
                            """,
                        ).format(sql.Identifier(partition.name)),
                    )
                    if insert_mode in (
                        Methods.ignore,
                        Methods.insert_ignore,
                    ):
                        cur.execute(
                            sql.SQL(
                                """
                                INSERT INTO {}
                                SELECT *
                                FROM items_ingest_temp ON CONFLICT DO NOTHING;
                                """,
                            ).format(sql.Identifier(partition.name)),
                        )
                        logger.debug(cur.statusmessage)
                        logger.debug(f"Rows affected: {cur.rowcount}")
                    elif insert_mode == Methods.upsert:
                        cur.execute(
                            sql.SQL(
                                """
                                INSERT INTO {} AS t SELECT * FROM items_ingest_temp
                                ON CONFLICT (id) DO UPDATE
                                SET
                                    datetime = EXCLUDED.datetime,
                                    end_datetime = EXCLUDED.end_datetime,
                                    geometry = EXCLUDED.geometry,
                                    collection = EXCLUDED.collection,
                                    content = EXCLUDED.content
                                WHERE t IS DISTINCT FROM EXCLUDED
                                ;
                            """,
                            ).format(sql.Identifier(partition.name)),
                        )
                        logger.debug(cur.statusmessage)
                        logger.debug(f"Rows affected: {cur.rowcount}")
                    elif insert_mode == Methods.delsert:
                        cur.execute(
                            sql.SQL(
                                """
                                WITH deletes AS (
                                    DELETE FROM items i USING items_ingest_temp s
                                        WHERE
                                            i.id = s.id
                                            AND i.collection = s.collection
                                )
                                INSERT INTO {} AS t SELECT * FROM items_ingest_temp
                                ON CONFLICT (id) DO UPDATE
                                SET
                                    datetime = EXCLUDED.datetime,
                                    end_datetime = EXCLUDED.end_datetime,
                                    geometry = EXCLUDED.geometry,
                                    collection = EXCLUDED.collection,
                                    content = EXCLUDED.content
                                WHERE t IS DISTINCT FROM EXCLUDED
                                ;
                                """,
                            ).format(sql.Identifier(partition.name)),

                        )
                        logger.debug(cur.statusmessage)
                        logger.debug(f"Rows affected: {cur.rowcount}")
                else:
                    raise Exception(
                        "Available modes are insert, ignore, upsert, and delsert."
                        f"You entered {insert_mode}.",
                    )
                cur.execute("SELECT update_partition_stats_q(%s);",(partition.name,))
        logger.debug(
            f"Copying data for {partition} took {time.perf_counter() - t} seconds",
        )

    def _partition_update(self, item: Dict[str, Any]) -> str:
        """Update the cached partition with the item information and return the name.

        This method will mark the partition as dirty if the bounds of the partition
        need to be updated based on this item.
        """
        p = item.get("partition", None)
        if p is None:
            _, key, partition_trunc = self.collection_json(item["collection"])
            if partition_trunc == "year":
                pd = item["datetime"].replace("-", "")[:4]
                p = f"_items_{key}_{pd}"
            elif partition_trunc == "month":
                pd = item["datetime"].replace("-", "")[:6]
                p = f"_items_{key}_{pd}"
            else:
                p = f"_items_{key}"
            item["partition"] = p

        partition_name: str = p

        partition: Optional[Partition] = None

        if partition_name not in self._partition_cache:
            # Read the partition information from the database if it exists
            db_rows = list(
                self.db.query(
                    """
                    SELECT
                        nullif(lower(constraint_dtrange),'-infinity')
                            as datetime_range_min,
                        nullif(upper(constraint_dtrange),'infinity')
                            as datetime_range_max,
                        nullif(lower(constraint_edtrange),'-infinity')
                            as end_datetime_range_min,
                        nullif(upper(constraint_edtrange),'infinity')
                            as end_datetime_range_max
                    FROM partitions WHERE partition=%s;
                    """,
                    [partition_name],
                ),
            )
            if db_rows:
                datetime_range_min: Optional[datetime] = db_rows[0][0] or datetime.min
                datetime_range_max: Optional[datetime] = db_rows[0][1] or datetime.max
                end_datetime_range_min: Optional[datetime] = (
                    db_rows[0][2] or datetime.min
                )
                end_datetime_range_max: Optional[datetime] = (
                    db_rows[0][3] or datetime.max
                )

                partition = Partition(
                    name=partition_name,
                    collection=item["collection"],
                    datetime_range_min=datetime_range_min.isoformat(),
                    datetime_range_max=datetime_range_max.isoformat(),
                    end_datetime_range_min=end_datetime_range_min.isoformat(),
                    end_datetime_range_max=end_datetime_range_max.isoformat(),
                    requires_update=False,
                )

        else:
            partition = self._partition_cache[partition_name]

        if partition:
            # Only update the partition if the item is outside the current bounds
            if item["datetime"] < partition.datetime_range_min:
                partition.datetime_range_min = item["datetime"]
                partition.requires_update = True
            if item["datetime"] > partition.datetime_range_max:
                partition.datetime_range_max = item["datetime"]
                partition.requires_update = True
            if item["end_datetime"] < partition.end_datetime_range_min:
                partition.end_datetime_range_min = item["end_datetime"]
                partition.requires_update = True
            if item["end_datetime"] > partition.end_datetime_range_max:
                partition.end_datetime_range_max = item["end_datetime"]
                partition.requires_update = True
        else:
            # No partition exists yet; create a new one from item
            partition = Partition(
                name=partition_name,
                collection=item["collection"],
                datetime_range_min=item["datetime"],
                datetime_range_max=item["datetime"],
                end_datetime_range_min=item["end_datetime"],
                end_datetime_range_max=item["end_datetime"],
                requires_update=True,
            )

        self._partition_cache[partition_name] = partition

        return partition_name

    def read_dehydrated(self, file: Union[Path, str] = "stdin") -> Generator:
        if file is None:
            file = "stdin"
        if isinstance(file, str):
            open_file: Any = open_std(file, "r")
            with open_file as f:
                # Note: if 'content' is changed to be anything
                # but the last field, the logic below will break.
                fields = [
                    "id",
                    "geometry",
                    "collection",
                    "datetime",
                    "end_datetime",
                    "content",
                ]

                for line in f:
                    tab_split = line.split("\t")
                    item = {}
                    for i, field in enumerate(fields):
                        if field == "content":
                            # Join the remaining splits in case
                            # there were any tabs in the JSON content.
                            content_value = "\t".join(tab_split[i:])
                            # Replace quote characters that can be
                            # written on export and causes failures.
                            content_value = content_value.replace(r'\\"', r"\"")
                            item[field] = content_value
                        else:
                            item[field] = tab_split[i]
                    item["partition"] = self._partition_update(item)
                    yield item

    def read_hydrated(
        self, file: Union[Path, str, Iterator[Any]] = "stdin",
    ) -> Generator:
        for line in read_json(file):
            item = self.format_item(line)
            item["partition"] = self._partition_update(item)
            yield item

    def load_items(
        self,
        file: Union[Path, str, Iterator[Any]] = "stdin",
        insert_mode: Optional[Methods] = Methods.insert,
        dehydrated: Optional[bool] = False,
        chunksize: Optional[int] = 10000,
    ) -> None:
        """Load items json records."""
        self.check_version()

        if file is None:
            file = "stdin"
        t = time.perf_counter()
        self._partition_cache = {}

        if dehydrated and isinstance(file, str):
            items = self.read_dehydrated(file)
        else:
            items = self.read_hydrated(file)

        for chunk in chunked_iterable(items, chunksize):
            chunk = list(chunk)
            chunk.sort(key=lambda x: x["partition"])
            for k, g in itertools.groupby(chunk, lambda x: x["partition"]):
                self.load_partition(self._partition_cache[k], g, insert_mode)

        logger.debug(f"Adding data to database took {time.perf_counter() - t} seconds.")

    def format_item(self, _item: Union[Path, str, Dict[str, Any]]) -> Dict[str, Any]:
        """Format an item to insert into a record."""
        out: Dict[str, Any] = {}
        item: Dict[str, Any]
        if not isinstance(_item, dict):
            try:
                item = orjson.loads(str(_item).replace("\\\\", "\\"))
            except Exception:
                raise
        else:
            item = _item

        base_item, key, partition_trunc = self.collection_json(item["collection"])

        out["id"] = item.get("id")
        out["collection"] = item.get("collection")
        properties: Dict[str, Any] = item.get("properties", {})

        dt: Optional[str] = properties.get("datetime")
        edt: Optional[str] = properties.get("end_datetime")
        sdt: Optional[str] = properties.get("start_datetime")

        if edt is not None and sdt is not None:
            out["datetime"] = sdt
            out["end_datetime"] = edt
        elif dt is not None:
            out["datetime"] = dt
            out["end_datetime"] = dt
        else:
            raise Exception("Invalid datetime encountered")

        if out["datetime"] is None or out["end_datetime"] is None:
            raise Exception(
                f"Datetime must be set. OUT: {out} Properties: {properties}",
            )

        if partition_trunc == "year":
            pd = out["datetime"].replace("-", "")[:4]
            partition = f"_items_{key}_{pd}"
        elif partition_trunc == "month":
            pd = out["datetime"].replace("-", "")[:6]
            partition = f"_items_{key}_{pd}"
        else:
            partition = f"_items_{key}"

        out["partition"] = partition

        geojson = item.get("geometry")
        if geojson is None:
            geometry = None
        else:
            geom = Geometry.from_geojson(geojson)
            if geom is None:
                raise Exception(f"Invalid geometry encountered: {geojson}")
            geometry = str(geom.wkb)
        out["geometry"] = geometry

        content = dehydrate(base_item, item)

        # Remove keys from the dehydrated item content which are stored directly
        # on the table row.
        content.pop("id", None)
        content.pop("collection", None)
        content.pop("geometry", None)

        out["content"] = orjson.dumps(content).decode()

        return out

    def __hash__(self) -> int:
        """Return hash so that the LRU deocrator can cache without the class."""
        return 0
