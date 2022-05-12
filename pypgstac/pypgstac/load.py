"""Utilities to bulk load data into pgstac from json/ndjson."""
import contextlib
import itertools
import logging
from pathlib import Path
import sys
import time
from dataclasses import dataclass
from functools import lru_cache
from typing import (
    Any,
    BinaryIO,
    Dict,
    Iterable,
    Iterator,
    Optional,
    Tuple,
    Union,
    Generator,
    TextIO,
)
import csv
import orjson
import psycopg
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


from .db import PgstacDB
from .hydration import dehydrate
from enum import Enum

logger = logging.getLogger(__name__)


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
    filename: str, mode: str = "r", *args: Any, **kwargs: Any
) -> Generator[Any, None, None]:
    """Open files and i/o streams transparently."""
    fh: Union[TextIO, BinaryIO]
    if (
        filename is None
        or filename == "-"
        or filename == "stdin"
        or filename == "stdout"
    ):
        if "r" in mode:
            stream = sys.stdin
        else:
            stream = sys.stdout
        if "b" in mode:
            fh = stream.buffer
        else:
            fh = stream
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


@dataclass
class Loader:
    """Utilities for loading data."""

    db: PgstacDB
    _partition_cache: Optional[dict] = None

    @lru_cache
    def collection_json(self, collection_id: str) -> Tuple[dict, int, str]:
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
                f"Collection {collection_id} is not present in the database"
            )
        logger.debug(f"Found {collection_id} with base_item {base_item}")
        return base_item, key, partition_trunc

    def load_collections(
        self,
        file: Union[Path, str, Iterator[Any]] = "stdin",
        insert_mode: Optional[Methods] = Methods.insert,
    ) -> None:
        """Load a collections json or ndjson file."""
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
                    """
                )
                with cur.copy("COPY tmp_collections (content) FROM stdin;") as copy:
                    for collection in read_json(file):
                        copy.write_row((orjson.dumps(collection).decode(),))
                if insert_mode is None or insert_mode == "insert":
                    cur.execute(
                        """
                        INSERT INTO collections (content)
                        SELECT content FROM tmp_collections;
                        """
                    )
                    logger.debug(cur.statusmessage)
                    logger.debug(f"Rows affected: {cur.rowcount}")
                elif insert_mode in ("insert_ignore", "ignore"):
                    cur.execute(
                        """
                        INSERT INTO collections (content)
                        SELECT content FROM tmp_collections
                        ON CONFLICT DO NOTHING;
                        """
                    )
                    logger.debug(cur.statusmessage)
                    logger.debug(f"Rows affected: {cur.rowcount}")
                elif insert_mode == "upsert":
                    cur.execute(
                        """
                        INSERT INTO collections (content)
                        SELECT content FROM tmp_collections
                        ON CONFLICT (id) DO
                        UPDATE SET content=EXCLUDED.content;
                        """
                    )
                    logger.debug(cur.statusmessage)
                    logger.debug(f"Rows affected: {cur.rowcount}")
                else:
                    raise Exception(
                        "Available modes are insert, ignore, and upsert."
                        f"You entered {insert_mode}."
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
        partition: dict,
        items: Iterable,
        insert_mode: Optional[Methods] = Methods.insert,
    ) -> None:
        """Load items data for a single partition."""
        conn = self.db.connect()
        t = time.perf_counter()

        logger.debug(f"Loading data for partition: {partition}.")
        with conn.cursor() as cur:
            with conn.transaction():
                cur.execute(
                    "SELECT * FROM partitions WHERE name = %s FOR UPDATE;",
                    (partition["partition"],),
                )
                cur.execute(
                    """
                    INSERT INTO partitions
                    (collection, datetime_range, end_datetime_range)
                    VALUES
                        (%s, tstzrange(%s, %s, '[]'), tstzrange(%s,%s, '[]'))
                    ON CONFLICT (name) DO UPDATE SET
                        datetime_range = EXCLUDED.datetime_range,
                        end_datetime_range = EXCLUDED.end_datetime_range
                    ;
                """,
                    (
                        partition["collection"],
                        partition["mindt"],
                        partition["maxdt"],
                        partition["minedt"],
                        partition["maxedt"],
                    ),
                )
            with conn.transaction():
                logger.debug(
                    f"Adding partition {partition} took {time.perf_counter() - t}s"
                )
                t = time.perf_counter()
                if insert_mode is None or insert_mode == "insert":
                    with cur.copy(
                        sql.SQL(
                            """
                            COPY {}
                            (id, collection, datetime, end_datetime, geometry, content)
                            FROM stdin;
                            """
                        ).format(sql.Identifier(partition["partition"]))
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
                                )
                            )
                    logger.debug(cur.statusmessage)
                    logger.debug(f"Rows affected: {cur.rowcount}")
                elif insert_mode in (
                    "ignore_dupes",
                    "upsert",
                    "delsert",
                    "ignore",
                ):
                    cur.execute(
                        """
                        DROP TABLE IF EXISTS items_ingest_temp;
                        CREATE TEMP TABLE items_ingest_temp
                        ON COMMIT DROP AS SELECT * FROM items LIMIT 0;
                        """
                    )
                    with cur.copy(
                        """
                        COPY items_ingest_temp
                        (id, collection, datetime, end_datetime, geometry, content)
                        FROM stdin;
                        """
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
                                )
                            )
                    logger.debug(cur.statusmessage)
                    logger.debug(f"Copied rows: {cur.rowcount}")

                    cur.execute(
                        sql.SQL(
                            """
                                LOCK TABLE ONLY {} IN EXCLUSIVE MODE;
                            """
                        ).format(sql.Identifier(partition["partition"]))
                    )
                    if insert_mode in ("ignore", "insert_ignore"):
                        cur.execute(
                            sql.SQL(
                                """
                                INSERT INTO {}
                                SELECT *
                                FROM items_ingest_temp ON CONFLICT DO NOTHING;
                                """
                            ).format(sql.Identifier(partition["partition"]))
                        )
                        logger.debug(cur.statusmessage)
                        logger.debug(f"Rows affected: {cur.rowcount}")
                    elif insert_mode == "upsert":
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
                            """
                            ).format(sql.Identifier(partition["partition"]))
                        )
                        logger.debug(cur.statusmessage)
                        logger.debug(f"Rows affected: {cur.rowcount}")
                    elif insert_mode == "delsert":
                        cur.execute(
                            sql.SQL(
                                """
                                WITH deletes AS (
                                    DELETE FROM items i USING items_ingest_temp s
                                        WHERE
                                            i.id = s.id
                                            AND i.collection = s.collection
                                            AND i IS DISTINCT FROM s
                                    RETURNING i.id, i.collection
                                )
                                INSERT INTO {}
                                SELECT s.* FROM
                                    items_ingest_temp s
                                    JOIN deletes d
                                    USING (id, collection);
                                ;
                            """
                            ).format(sql.Identifier(partition["partition"]))
                        )
                        logger.debug(cur.statusmessage)
                        logger.debug(f"Rows affected: {cur.rowcount}")
                else:
                    raise Exception(
                        "Available modes are insert, ignore, upsert, and delsert."
                        f"You entered {insert_mode}."
                    )
        logger.debug(
            f"Copying data for {partition} took {time.perf_counter() - t} seconds"
        )

    def _partition_update(self, item: dict) -> str:

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

        if self._partition_cache is None:
            self._partition_cache = {}

        partition = self._partition_cache.get(
            item["partition"],
            {
                "partition": None,
                "collection": None,
                "mindt": None,
                "maxdt": None,
                "minedt": None,
                "maxedt": None,
            },
        )

        partition["partition"] = item["partition"]
        partition["collection"] = item["collection"]
        if partition["mindt"] is None or item["datetime"] < partition["mindt"]:
            partition["mindt"] = item["datetime"]

        if partition["maxdt"] is None or item["datetime"] > partition["maxdt"]:
            partition["maxdt"] = item["datetime"]

        if partition["minedt"] is None or item["end_datetime"] < partition["minedt"]:
            partition["minedt"] = item["end_datetime"]

        if partition["maxedt"] is None or item["end_datetime"] > partition["maxedt"]:
            partition["maxedt"] = item["end_datetime"]
        self._partition_cache[item["partition"]] = partition

        return p

    def read_dehydrated(self, file: Union[Path, str] = "stdin") -> Generator:
        if file is None:
            file = "stdin"
        if isinstance(file, str):
            open_file: Any = open_std(file, "r")
            with open_file as f:
                fields = [
                    "id",
                    "geometry",
                    "collection",
                    "datetime",
                    "end_datetime",
                    "content",
                ]
                csvreader = csv.DictReader(f, fields, delimiter="\t")
                for item in csvreader:
                    item["partition"] = self._partition_update(item)
                    yield item

    def read_hydrated(
        self, file: Union[Path, str, Iterator[Any]] = "stdin"
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
        if file is None:
            file = "stdin"
        t = time.perf_counter()
        self._partition_cache = {}

        if dehydrated and isinstance(file, str):
            items = self.read_dehydrated(file)
        else:
            items = self.read_hydrated(file)

        for chunk in chunked_iterable(items, chunksize):
            list(chunk).sort(key=lambda x: x["partition"])
            for k, g in itertools.groupby(chunk, lambda x: x["partition"]):
                self.load_partition(self._partition_cache[k], g, insert_mode)

        logger.debug(f"Adding data to database took {time.perf_counter() - t} seconds.")

    def format_item(self, _item: Union[Path, str, dict]) -> dict:
        """Format an item to insert into a record."""
        out = {}
        item: dict
        if not isinstance(_item, dict):
            try:
                item = orjson.loads(str(_item).replace("\\\\", "\\"))
            except:
                raise Exception(f"Could not load {_item}")
        else:
            item = _item

        base_item, key, partition_trunc = self.collection_json(item["collection"])

        out["id"] = item.pop("id")
        out["collection"] = item.pop("collection")
        properties: dict = item.get("properties", {})

        dt = properties.get("datetime")
        edt = properties.get("end_datetime")
        sdt = properties.get("start_datetime")

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
                f"Datetime must be set. OUT: {out} Properties: {properties}"
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

        bbox = item.pop("bbox")
        geojson = item.pop("geometry")
        if geojson is None and bbox is not None:
            geometry = None
        else:
            geometry = str(Geometry.from_geojson(geojson).wkb)
        out["geometry"] = geometry

        content = dehydrate(base_item, item)

        out["content"] = orjson.dumps(content).decode()

        return out

    def __hash__(self) -> int:
        """Return hash so that the LRU deocrator can cache without the class."""
        return 0
