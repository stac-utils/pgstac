import asyncio
import os

import asyncpg
import typer
from smart_open import open
from enum import Enum

from pypgstac import __version__ as version

app = typer.Typer()

dirname = os.path.dirname(__file__)
migrations_dir = os.path.join(dirname, "migrations")


def pglogger(conn, message):
    print(message)


async def run_migration(dsn: str = None):
    print(os.getcwd())
    conn = await asyncpg.connect(dsn=dsn)
    async with conn.transaction():
        try:
            oldversion = await conn.fetchval(
                f"""
                SELECT version FROM pgstac.migrations ORDER BY datetime DESC LIMIT 1;
                """
            )
        except asyncpg.exceptions.UndefinedTableError:
            oldversion = None
    print(f"Old Version: {oldversion} Migrations Dir: {migrations_dir}")
    if oldversion is None or oldversion == {version}:
        migration_file = os.path.join(migrations_dir, f"pgstac.{version}.sql")
    else:
        migration_file = os.path.join(
            migrations_dir, f"pgstac.{oldversion}-{version}.sql"
        )

    if not os.path.exists(migration_file):
        raise Exception(
            f"Pypgstac does not have a migration from {oldversion} to {version} ({migration_file})"
        )

    with open(migration_file) as f:
        migration_sql = f.read()
        print(migration_sql)
        async with conn.transaction():
            conn.add_log_listener(pglogger)
            await conn.execute(migration_sql)
            await conn.execute(
                """
                INSERT INTO pgstac.versions (version)
                VALUES ($1);
                """,
                version,
            )

    await conn.close()
    return version


@app.command()
def migrate(dsn: str = None):
    typer.echo(asyncio.run(run_migration(dsn)))


class loadopt(str, Enum):
    insert = "insert"
    insert_ignore = "insert_ignore"
    upsert = "upsert"


class tables(str, Enum):
    items = "items"
    collections = "collections"


async def load_ndjson(
    file: str, table: tables, method: loadopt = "insert", dsn: str = None
):
    with open(file, "rb") as f:
        conn = await asyncpg.connect(dsn=dsn)
        conn.add_log_listener(pglogger)
        if method == "insert":
            print(f"copying to {table} directly")
            async with conn.transaction():
                await conn.copy_to_table(table, source=f, columns=["content"])
                await conn.execute(
                    f"""
                    SELECT backfill_partitions();
                """
                )
        elif method == "insert_ignore":
            print(f"inserting to {table} ignoring duplicates")
            async with conn.transaction():
                await conn.execute(
                    """
                    CREATE TEMP TABLE pgstactemp (content jsonb)
                    ON COMMIT DROP;
                """
                )
                await conn.copy_to_table(
                    "pgstactemp", source=f, columns=["content"]
                )
                await conn.execute(
                    f"""
                    SELECT make_partitions(
                        min((content->>'datetime')::timestamptz),
                        max((content->>'datetime')::timestamptz)
                    ) FROM pgstactemp;
                    INSERT INTO {table} (content)
                    SELECT content FROM pgstactemp
                    ON CONFLICT DO NOTHING;
                """
                )
        else:
            print(f"upserting to {table}")
            async with conn.transaction():
                await conn.execute(
                    """
                    CREATE TEMP TABLE pgstactemp (content jsonb)
                    ON COMMIT DROP;
                """
                )
                await conn.copy_to_table(
                    "pgstactemp", source=f, columns=["content"]
                )
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
                    await conn.execute(
                        f"""
                        SELECT upsert_item(content)
                        FROM pgstactemp;
                    """
                    )

        await conn.close()


@app.command()
def load(
    table: tables,
    file: str,
    dsn: str = None,
    method: loadopt = typer.Option(
        "insert", prompt="How to deal conflicting ids"
    ),
):
    typer.echo(
        asyncio.run(
            load_ndjson(file=file, table=table, dsn=dsn, method=method)
        )
    )


if __name__ == "__main__":
    app()
