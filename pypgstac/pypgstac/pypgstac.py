import asyncio
from io import TextIOWrapper
import os
import time
from typing import Any, AsyncGenerator, Dict, Iterable, List, Optional, TypeVar, Union

import asyncpg
from asyncpg.connection import Connection
import typer
import orjson
from smart_open import open
from enum import Enum

from pypgstac import __version__ as version

import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

app = typer.Typer()

dirname = os.path.dirname(__file__)
migrations_dir = os.path.join(dirname, "migrations")


def pglogger(message: str) -> None:
    logging.debug(message)


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
    pg_connection_string: Optional[str] = None
    connection: Optional[Connection] = None

    def __init__(self, pg_connection_string: Optional[str] = None) -> None:
        self.pg_connection_string = pg_connection_string

    async def create_connection(self) -> Connection:
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
        if self.connection is None:
            await self.create_connection()
        assert self.connection is not None
        return self.connection

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if self.connection:
            await self.connection.close()


async def run_migration(dsn: Optional[str] = None) -> str:
    conn = await asyncpg.connect(dsn=dsn)
    async with conn.transaction():
        try:
            oldversion = await conn.fetchval(
                """
                SELECT version FROM pgstac.migrations
                ORDER BY datetime DESC LIMIT 1;
                """
            )
        except asyncpg.exceptions.UndefinedTableError:
            oldversion = None
    logging.debug(
        f"Old Version: {oldversion} New Version: {version} Migrations Dir: {migrations_dir}"
    )
    if oldversion == version:
        logging.debug(f"Target database already at version: {version}")
        return version
    if oldversion is None:
        logging.debug(f"No pgstac version set, installing {version} from scratch")
        migration_file = os.path.join(migrations_dir, f"pgstac.{version}.sql")
    else:
        logging.debug(f"Migrating from {oldversion} to {version}.")
        migration_file = os.path.join(
            migrations_dir, f"pgstac.{oldversion}-{version}.sql"
        )

    if not os.path.exists(migration_file):
        raise Exception(
            f"Pypgstac does not have a migration from {oldversion} to {version} ({migration_file})"
        )

    open_migration_file = open(migration_file)
    if isinstance(open_migration_file, TextIOWrapper):
        with open_migration_file as f:
            migration_sql = f.read()
            logging.debug(migration_sql)
            async with conn.transaction():
                conn.add_log_listener(pglogger)
                await conn.execute(migration_sql)
                await conn.execute(
                    """
                    INSERT INTO pgstac.migrations (version)
                    VALUES ($1);
                    """,
                    version,
                )

        await conn.close()
    else:
        raise IOError(f"Unable to open {migration_file}")
    return version


@app.command()
def migrate(dsn: Optional[str] = None) -> None:
    """Migrate a pgstac database"""
    version = asyncio.run(run_migration(dsn))
    typer.echo(f"pgstac version {version}")


class loadopt(str, Enum):
    insert = "insert"
    insert_ignore = "insert_ignore"
    upsert = "upsert"


class tables(str, Enum):
    items = "items"
    collections = "collections"


# Types of iterable that load_iterator can support
T = TypeVar("T", Iterable[bytes], Iterable[Dict[str, Any]], Iterable[str])


async def aiter(list: T) -> AsyncGenerator[bytes, None]:
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

        line = "\n".join(
            [item_str.rstrip().replace(r"\n", r"\\n").replace(r"\t", r"\\t")]
        ).encode("utf-8")
        yield line


async def copy(iter: T, table: tables, conn: asyncpg.Connection) -> None:
    logger.debug(f"copying to {table} directly")
    logger.debug(f"iter: {iter}")
    bytes_iter = aiter(iter)
    async with conn.transaction():
        logger.debug("Copying data")
        await conn.copy_to_table(
            table,
            source=bytes_iter,
            columns=["content"],
            format="csv",
            quote=chr(27),
            delimiter=chr(31),
        )
        logger.debug("Backfilling partitions")
        await conn.execute(
            f"""
            SELECT backfill_partitions();
        """
        )
        logger.debug("Copy complete")


async def copy_ignore_duplicates(
    iter: T, table: tables, conn: asyncpg.Connection
) -> None:
    logger.debug(f"inserting to {table} ignoring duplicates")
    bytes_iter = aiter(iter)
    async with conn.transaction():
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
        logger.debug("Data Copied")
        await conn.execute(
            """
            SELECT make_partitions(
                min((content->>'datetime')::timestamptz),
                max((content->>'datetime')::timestamptz)
            ) FROM pgstactemp;
        """
        )
        logger.debug("Made Partitions")
        await conn.execute(
            f"""
            INSERT INTO {table} (content)
            SELECT content FROM pgstactemp
            ON CONFLICT DO NOTHING;
        """
        )
        logger.debug("Data Inserted")


async def copy_upsert(iter: T, table: tables, conn: asyncpg.Connection) -> None:
    logger.debug(f"upserting to {table}")
    bytes_iter = aiter(iter)
    async with conn.transaction():
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
        logger.debug("Data Copied")
        if table == "collections":
            await conn.execute(
                f"""
                INSERT INTO collections (content)
                SELECT content FROM pgstactemp
                ON CONFLICT (id) DO UPDATE
                SET content = EXCLUDED.content
                WHERE collections.content IS DISTINCT FROM EXCLUDED.content;
            """
            )
        if table == "items":
            logger.debug("Upserting Data")
            await conn.execute(
                f"""
                SELECT upsert_item(content)
                FROM pgstactemp;
            """
            )


async def load_iterator(
    iter: T, table: tables, conn: asyncpg.Connection, method: loadopt = loadopt.insert
):
    logger.debug(f"Load Iterator Connection: {conn}")
    if method == loadopt.insert:
        await copy(iter, table, conn)
    elif method == loadopt.insert_ignore:
        await copy_ignore_duplicates(iter, table, conn)
    else:
        await copy_upsert(iter, table, conn)


async def load_ndjson(
    file: str, table: tables, method: loadopt = loadopt.insert, dsn: str = None
) -> None:
    print(f"loading {file} into {table} using {method}")
    open_file = open(file, "rb")
    if isinstance(open_file, TextIOWrapper):
        with open_file as f:
            async with DB(dsn) as conn:
                await load_iterator(f, table, conn, method)
    else:
        raise IOError(f"Cannot read {file}")


@app.command()
def load(
    table: tables,
    file: str,
    dsn: str = None,
    method: loadopt = typer.Option("insert", prompt="How to deal conflicting ids"),
) -> None:
    "Load STAC data into a pgstac database."
    typer.echo(asyncio.run(load_ndjson(file=file, table=table, dsn=dsn, method=method)))


@app.command()
def pgready(dsn: Optional[str] = None) -> None:
    """Wait for a pgstac database to accept connections"""

    async def wait_on_connection() -> bool:
        cnt = 0

        print("Waiting for pgstac to come online...", end="", flush=True)
        while True:
            if cnt > 150:
                raise Exception("Unable to connect to database")
            try:
                print(".", end="", flush=True)
                conn = await asyncpg.connect()
                await conn.execute("SELECT 1")
                await conn.close()
                print("success!")
                return True
            except Exception:
                time.sleep(0.1)
                cnt += 1

    asyncio.run(wait_on_connection())


if __name__ == "__main__":
    app()
