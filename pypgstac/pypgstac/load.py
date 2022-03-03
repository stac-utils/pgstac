"""Utilities to bulk load data into pgstac from json/ndjson."""
from enum import Enum
from typing import Any, AsyncGenerator, Dict, Iterable, Optional, TypeVar

import asyncpg
import orjson
from orjson import JSONDecodeError

import typer
from asyncpg.connection import Connection
from smart_open import open
from .db import PgstacDB
from itertools import groupby
import logging
from stac_pydantic import Item, Collection
from functools import lru_cache

app = typer.Typer()



def safeget(d, *keys):
    for key in keys:
        try:
            d = d[key]
        except KeyError:
            return '_'
    return d

from attr import define
from typing import Optional

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
        # if key in ["proj:bbox", "proj:transform"]:
        #     continue

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
        if key not in a:
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

@define
class Item:
    id: Optional[str]
    geom: Optional[str]
    collection: Optional[str]
    datetime: Optional[str]
    end_datetime: Optional[str]
    properties: Optional[dict]
    content: Optional[dict]
    _sort: tuple

    @classmethod
    def from_dict(cls, d):
        id = d.get('id')
        geom = d.get('geometry')
        collection = d.get('collection')
        datetime = safeget(d, 'properties', 'datetime')
        end_datetime = safeget(d, 'properties', 'end_datetime')
        properties = safeget(d, 'properties')
        content = d
        _sort = (collection, datetime, id)
        return cls(id, geom, collection, datetime, end_datetime, properties, content, _sort)


    def __lt__(self, other):
        return self._sort < other._sort
    def __le__(self, other):
        return self._sort <= other._sort
    def __gt__(self, other):
        return self._sort > other._sort
    def __ge__(self, other):
        return self._sort >= other._sort



def read_json(
    file: str
) -> None:
    """Load data from an ndjson or json file."""
    open_file: Any = open(file, "rb")
    with open_file as f:
        # Try reading line by line as ndjson
        try:
            for line in f:
                line = line.strip().replace('////','//')
                yield orjson.loads(line)
        except JSONDecodeError as e:
            # If reading first line as json fails, try reading entire file
            logging.info(f'First line could not be parsed as json, trying full file.')
            try:
                f.seek(0)
                json = orjson.loads(f.read())
                if isinstance(json, list):
                    for record in json:
                        yield record
                else:
                    yield json
            except JSONDecodeError as e:
                logging.info('File cannot be read as json')
                raise e


class Loader:
    def __init__(self, db: PgstacDB, schema: str = 'pgstac'):
        print("migrate init")
        self.db = db
        self.schema = schema

    def __hash__(self):
        return self.schema

    @lru_cache
    def get_collection_item(self, collection:str):
        collection = self.db.query_one(
            """
            SELECT item_template FROM collections
            WHERE id = %s
            """,
            collection)
        if collection is None:
            raise Exception(f"Collection {collection} not found.")



    def temp_items(self, iter: T):
        self.db.connect()
        with self.connection.cursor() as cursor:
            cursor.execute("""
            CREATE TEMP TABLE pgstacloader (content jsonb);
            """)
            with cursor.copy("COPY pgstacloader (content) FROM STDIN;"):


    def format_item(self, item, item_template: dict):
        """Format an item to insert into a record."""
        out = {}
        if not isinstance(item, dict):
            try:
                item = orjson.loads(item.replace("\\\\", "\\"))
            except:
                print(f"Could not load {item}")
                return None

        id = item.pop("id")
        out["collection_id"] = item.pop("collection")
        out["datetime"] = item["properties"].pop("datetime")

        item.pop("bbox")
        geojson = item.pop("geometry")
        geometry = Geometry.from_geojson(geojson).wkb
        out["geometry"] = geometry

        content = dict_minus(item_template, item)

        properties = content["properties"]

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
