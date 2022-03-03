import psycopg
import orjson
from psycopg.types.json import Jsonb, set_json_dumps, set_json_loads
from psycopg import Connection, AsyncConnection, Cursor, AsyncCursor
from functools import lru_cache
import asyncio
from dataclasses import dataclass
from os.path import commonprefix
from pyproj import CRS
from typing import List
from plpygis.geometry import Geometry

set_json_dumps(orjson.dumps)
set_json_loads(orjson.loads)


class DB:
    """Database connection context manager."""

    def __init__(self, conninfo, *args, **kwargs) -> None:
        """Initialize DB class."""
        self.conninfo = conninfo
        self.args = args
        self.kwargs = kwargs
        self.connection = None
        self.async_connection = None
        options = self.kwargs.get("options", "")
        options = f"{options} -c search_path=pgstac,public"
        self.kwargs["options"] = options
        if "row_factory" not in self.kwargs:
            kwargs["row_factory"] = psycopg.rows.dict_row

    def logger(msg):
        print(f"{msg.severity} | {msg.message_primary}")

    async def create_async_connection(self) -> AsyncConnection:
        """Create database connection and set search_path."""
        self.async_connection: AsyncConnection = (
            await psycopg.AsyncConnection.connect(
                self.conninfo, *self.args, **self.kwargs
            )
        )
        return self.async_connection

    async def __aenter__(self) -> AsyncConnection:
        """Enter DB Connection."""
        if self.async_connection is None:
            await self.create_async_connection()
        assert self.async_connection is not None
        return self.async_connection

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit DB Connection."""
        if self.async_connection:
            if self.kwargs.get("autocommit", None):
                await self.async_connection.commit()
            await self.async_connection.close()

    def create_connection(self) -> Connection:
        """Create database connection and set search_path."""
        self.connection: Connection = psycopg.connect(
            self.conninfo, *self.args, **self.kwargs
        )
        self.connection.add_notice_handler(self.logger)
        return self.connection

    def __enter__(self) -> Connection:
        """Enter DB Connection."""
        if self.connection is None:
            self.create_connection()
        assert self.connection is not None
        return self.connection

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit DB Connection."""
        if self.connection:
            if self.kwargs.get("autocommit", None):
                self.connection.commit()
            self.connection.close()


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
        if key in ["proj:bbox", "proj:transform"]:
            continue

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


@dataclass
class Loader:
    conn: Connection

    @lru_cache
    def collection_json(self, collection_id):
        """Get collection so that we can use it to slim down redundant
        information from the item.
        """
        cur = self.conn.cursor(row_factory=psycopg.rows.tuple_row)
        cur.execute(
            "SELECT id, base_item FROM pgstac_rework.collections WHERE id=%s",
            (collection_id,),
        )
        collection = cur.fetchone()[1]
        return collection

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
                print(f"Could not load {item}")
                return None
        base_item = self.collection_json(item["collection"])

        out["id"] = item.pop("id")
        out["collection_id"] = item.pop("collection")
        out["datetime"] = item["properties"].pop("datetime")

        item.pop("bbox")
        geojson = item.pop("geometry")
        geometry = Geometry.from_geojson(geojson).wkb
        out["geometry"] = geometry

        content = dict_minus(base_item, item)

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

    def __hash__(self):
        """Returns hash so that the LRU deocrator can cache without the class."""
        return 0
