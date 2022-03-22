"""Utilities to bulk load data into pgstac from json/ndjson."""
import logging
from enum import Enum
from functools import lru_cache
from typing import Any, AsyncGenerator, Dict, Iterable, List, Optional, TypeVar
import sys
import orjson
import typer
from orjson import JSONDecodeError
from plpygis.geometry import Geometry
from pyproj import CRS
from smart_open import open
import contextlib
from dataclasses import dataclass
from .db import PgstacDB


def safeget(d, *keys):
    for key in keys:
        try:
            d = d[key]
        except KeyError:
            return "_"
    return d


from typing import Optional

from attr import define


@contextlib.contextmanager
def open_std(filename: str, mode: str = "r", *args, **kwargs):
    """Open files and i/o streams transparently."""
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
            fh = stream.buffer  # type: IO
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


def name_array_asdict(na: List):
    out = {}
    for i in na:
        out[i["name"]] = i
    return out


def name_array_diff(a: List, b: List):
    diff = dict_minus(name_array_asdict(a), name_array_asdict(b))
    vals = diff.values()
    return [v for v in vals if v != {}]


def dict_minus(a, b):
    out = {}
    for key, value in b.items():
        if isinstance(value, list):
            try:
                arraydiff = name_array_diff(a[key], value)
                if arraydiff is not None and arraydiff != []:
                    out[key] = arraydiff
                continue
            except KeyError:
                pass
            except TypeError:
                pass

        if value is None or value == []:
            continue
        if a is None or key not in a:
            out[key] = value
            continue

        if a.get(key) != value:
            if isinstance(value, dict):
                out[key] = dict_minus(a[key], value)
                continue
            out[key] = value

    return out


@lru_cache
def get_epsg_from_wkt(wkt):
    """Get srid from a wkt string."""
    crs = CRS(wkt)
    if crs:
        auths = crs.list_authority()
        if auths and len(auths) >= 1:
            for auth in auths:
                authcrs = CRS.from_authority(auth.auth_name, auth.code)
                if crs.equals(authcrs):
                    return int(auth.code)


def read_json(file: str) -> None:
    """Load data from an ndjson or json file."""
    open_file: Any = open_std(file, "r")
    with open_file as f:
        # Try reading line by line as ndjson
        try:
            for line in f:
                line = line.strip().replace("////", "//")
                yield orjson.loads(line)
        except JSONDecodeError as e:
            # If reading first line as json fails, try reading entire file
            logging.info(
                f"First line could not be parsed as json, trying full file."
            )
            try:
                f.seek(0)
                json = orjson.loads(f.read())
                if isinstance(json, list):
                    for record in json:
                        yield record
                else:
                    yield json
            except JSONDecodeError as e:
                logging.info("File cannot be read as json")
                raise e


@dataclass
class Loader:
    db: PgstacDB
    minimizeproj: Optional[bool] = False

    @lru_cache
    def collection_json(self, collection_id):
        """Get collection so that we can use it to slim down redundant
        information from the item.
        """
        collection = self.db.query_one(
            "SELECT base_item FROM collections WHERE id=%s",
            (collection_id,),
        )
        if collection is None:
            raise Exception(
                f"Collection {collection_id} is not present in the database"
            )
        return collection

    def load_collections(
        self, file: Optional[str] = "stdin", insert_mode: str = "insert"
    ):
        conn = self.db.connect()
        with conn.cursor() as cur:
            cur.execute(
                "CREATE TEMP TABLE tmp_collections (content jsonb) ON COMMIT DROP;"
            )
            with cur.copy(
                "COPY tmp_collections (content) FROM stdin;"
            ) as copy:
                for collection in read_json(file):
                    copy.write(orjson.dumps(collection))
            if insert_mode == "insert":
                cur.execute(
                    "INSERT INTO collections (content) SELECT content FROM tmp_collections;"
                )
            elif insert_mode == "ignore_dupes":
                cur.execute(
                    "INSERT INTO collections (content) SELECT content FROM tmp_collections ON CONFLICT DO NOTHING;"
                )
            elif insert_mode == "upsert":
                cur.execute(
                    "INSERT INTO collections (content) SELECT content FROM tmp_collections ON CONFLICT (id) DO UPDATE SET content=EXCLUDED.content;"
                )
            else:
                raise Exception(
                    f"Available modes are insert, ignore_dupes, and upsert. You entered {insert_mode}."
                )
        conn.commit()

    def load_items(
        self, file: Optional[str] = "stdin", insert_mode: str = "insert"
    ):
        items = [self.format_item(item).values() for item in read_json(file)]
        logging.debug(f">>>>>>>>>> ITEM\n{items[0]}\n")
        conn = self.db.connect()
        with conn.cursor() as cur:
            if insert_mode == "insert":
                with cur.copy(
                    "COPY items_staging (id, collection, datetime, end_datetime, geometry, content) FROM stdin;"
                ) as copy:
                    for item in items:
                        copy.write_row(item)
            elif insert_mode == "ignore_dupes":
                with cur.copy(
                    "COPY items_staging_ignore (id, collection, datetime, end_datetime, geometry, content) FROM stdin;"
                ) as copy:
                    for item in items:
                        copy.write_row(item)
            elif insert_mode == "upsert":
                with cur.copy(
                    "COPY items_staging_upsert (id, collection, datetime, end_datetime, geometry, content) FROM stdin;"
                ) as copy:
                    for item in items:
                        copy.write_row(item)
            else:
                raise Exception(
                    f"Available modes are insert, ignore_dupes, and upsert. You entered {insert_mode}."
                )
        conn.commit()

    @lru_cache
    def get_epsg(self, wkt):
        """Get srid from a wkt string."""
        crs = CRS(wkt)
        if crs:
            auths = crs.list_authority()
            if auths and len(auths) >= 1:
                for auth in auths:
                    authcrs = CRS.from_authority(auth.auth_name, auth.code)
                    if crs.equals(authcrs):
                        return int(auth.code)

    def format_item(self, item):
        """Format an item to insert into a record."""
        out = {}
        if not isinstance(item, dict):
            try:
                item = orjson.loads(item.replace("\\\\", "\\"))
            except:
                raise Exception(f"Could not load {item}")
        base_item = self.collection_json(item["collection"])

        out["id"] = item.pop("id")
        out["collection"] = item.pop("collection")
        properties = item.get("properties")

        out["datetime"] = properties.get(
            "datetime", properties.get("start_datetime")
        )
        out["end_datetime"] = properties.get(
            "end_datetime", properties.get("datetime")
        )

        bbox = item.pop("bbox")
        geojson = item.pop("geometry")
        if geojson is None and bbox is not None:
            pass
        else:
            geometry = str(Geometry.from_geojson(geojson).wkb)
        out["geometry"] = geometry

        logging.debug(f"BASEITEM: {base_item}")
        logging.debug(f"ITEM: {item}")
        content = dict_minus(base_item, item)

        properties = content["properties"]

        if self.minimizeproj:
            if properties.get("proj:epsg") == 4326:
                properties.pop("proj:bbox")

            if properties.get("proj:wkt2"):
                srid = self.get_epsg(properties.get("proj:wkt2"))
                if srid:
                    properties.pop("proj:wkt2")
                    properties["proj:epsg"] = srid

            if properties.get("proj:epsg"):
                if properties.get("proj:bbox"):
                    properties.pop("proj:bbox")
                if properties.get("proj:transform"):
                    properties.pop("proj:transform")

        out["content"] = orjson.dumps(content).decode()

        return out

    def item_to_copy(self, item):
        return "\t".join(list(self.format_item(item).values()))

    def ndjson_to_pgcopy(
        self, file: Optional[str] = "-", outfile: Optional[str] = "-"
    ):
        with open_std(outfile, "w") as f:
            for line in read_json(file):
                f.write(self.item_to_copy(line))
                f.write("\n")

    def __hash__(self):
        """Returns hash so that the LRU deocrator can cache without the class."""
        return 0
