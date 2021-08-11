"""Utilities to bulk load data into pgstac from json/ndjson."""
from enum import Enum
from typing import Any, AsyncGenerator, Dict, Iterable, Optional, TypeVar

import asyncpg
import orjson
import typer
from asyncpg.connection import Connection
from smart_open import open

app = typer.Typer()


async def con_init(conn: Connection) -> None:
    """Use orjson for json returns."""
    await conn.set_type_codec(
        "json",
        encoder=orjson.dumps,
        decoder=orjson.loads,
        schema="pg_catalog",
    )
    await conn.set_type_codec(
        "jsonb",
        encoder=orjson.dumps,
        decoder=orjson.loads,
        schema="pg_catalog",
    )


class DB:
    """Database connection context manager."""

    pg_connection_string: Optional[str] = None
    connection: Optional[Connection] = None

    def __init__(self, pg_connection_string: Optional[str] = None) -> None:
        """Initialize DB class."""
        self.pg_connection_string = pg_connection_string

    async def create_connection(self) -> Connection:
        """Create database connection and set search_path."""
        connection: Connection = await asyncpg.connect(
            self.pg_connection_string,
            server_settings={
                "search_path": "pgstac,public",
                "application_name": "pypgstac",
            },
        )
        await con_init(connection)
        self.connection = connection
        return self.connection

    async def __aenter__(self) -> Connection:
        """Enter DB Connection."""
        if self.connection is None:
            await self.create_connection()
        assert self.connection is not None
        return self.connection

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit DB Connection."""
        if self.connection:
            await self.connection.close()


class loadopt(str, Enum):
    """Options for how to load data."""

    insert = "insert"
    insert_ignore = "insert_ignore"
    upsert = "upsert"


class tables(str, Enum):
    """Tables available to load data into."""

    items = "items"
    collections = "collections"


# Types of iterable that load_iterator can support
T = TypeVar("T", Iterable[bytes], Iterable[Dict[str, Any]], Iterable[str])


async def aiter(list: T) -> AsyncGenerator[bytes, None]:
    """Async Iterator to convert data to be suitable for pg copy."""
    for item in list:
        item_str: str
        if isinstance(item, bytes):
            item_str = item.decode("utf-8")
        elif isinstance(item, dict):
            item_str = orjson.dumps(item).decode("utf-8")
        elif isinstance(item, str):
            item_str = item
        else:
            raise ValueError(
                f"Cannot load iterator with values of type {type(item)} (value {item})"
            )

        lines = "\n".join(
            [item_str.rstrip().replace(r"\n", r"\\n").replace(r"\t", r"\\t")]
        )
        encoded_lines = (lines + "\n").encode("utf-8")

        yield encoded_lines


async def copy(iter: T, table: tables, conn: asyncpg.Connection) -> None:
    """Directly use copy to load data."""
    bytes_iter = aiter(iter)
    async with conn.transaction():
        if table == "collections":
            await conn.execute(
                """
                CREATE TEMP TABLE pgstactemp (content jsonb)
                ON COMMIT DROP;
            """
            )
            await conn.copy_to_table(
                "pgstactemp",
                source=bytes_iter,
                columns=["content"],
                format="csv",
                quote=chr(27),
                delimiter=chr(31),
            )
            await conn.execute(
                """
                INSERT INTO collections (content)
                SELECT content FROM pgstactemp;
            """
            )
        if table == "items":
            await conn.copy_to_table(
                "items_staging",
                source=bytes_iter,
                columns=["content"],
                format="csv",
                quote=chr(27),
                delimiter=chr(31),
            )


async def copy_ignore_duplicates(
    iter: T, table: tables, conn: asyncpg.Connection
) -> None:
    """Load data first into a temp table to ignore duplicates."""
    bytes_iter = aiter(iter)
    async with conn.transaction():
        if table == "collections":
            await conn.execute(
                """
                CREATE TEMP TABLE pgstactemp (content jsonb)
                ON COMMIT DROP;
            """
            )
            await conn.copy_to_table(
                "pgstactemp",
                source=bytes_iter,
                columns=["content"],
                format="csv",
                quote=chr(27),
                delimiter=chr(31),
            )
            await conn.execute(
                """
                INSERT INTO collections (content)
                SELECT content FROM pgstactemp
                ON CONFLICT DO NOTHING;
            """
            )
        if table == "items":
            await conn.copy_to_table(
                "items_staging_ignore",
                source=bytes_iter,
                columns=["content"],
                format="csv",
                quote=chr(27),
                delimiter=chr(31),
            )


async def copy_upsert(iter: T, table: tables, conn: asyncpg.Connection) -> None:
    """Insert data into a temp table to be able merge data."""
    bytes_iter = aiter(iter)
    async with conn.transaction():
        if table == "collections":
            await conn.execute(
                """
                CREATE TEMP TABLE pgstactemp (content jsonb)
                ON COMMIT DROP;
            """
            )
            await conn.copy_to_table(
                "pgstactemp",
                source=bytes_iter,
                columns=["content"],
                format="csv",
                quote=chr(27),
                delimiter=chr(31),
            )
            await conn.execute(
                """
                INSERT INTO collections (content)
                SELECT content FROM pgstactemp
                ON CONFLICT (id) DO UPDATE
                SET content = EXCLUDED.content
                WHERE collections.content IS DISTINCT FROM EXCLUDED.content;
            """
            )
        if table == "items":
            await conn.copy_to_table(
                "items_staging_upsert",
                source=bytes_iter,
                columns=["content"],
                format="csv",
                quote=chr(27),
                delimiter=chr(31),
            )


async def load_iterator(
    iter: T,
    table: tables,
    conn: asyncpg.Connection,
    method: loadopt = loadopt.insert,
):
    """Use appropriate method to load data from a file like iterator."""
    if method == loadopt.insert:
        await copy(iter, table, conn)
    elif method == loadopt.insert_ignore:
        await copy_ignore_duplicates(iter, table, conn)
    else:
        await copy_upsert(iter, table, conn)


async def load_ndjson(
    file: str, table: tables, method: loadopt = loadopt.insert, dsn: str = None
) -> None:
    """Load data from an ndjson file."""
    typer.echo(f"loading {file} into {table} using {method}")
    open_file: Any = open(file, "rb")

    with open_file as f:
        async with DB(dsn) as conn:
            await load_iterator(f, table, conn, method)
