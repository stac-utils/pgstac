import asyncio
import os

import asyncpg
import typer
from smart_open import open

from pypgstac import __version__ as version

app = typer.Typer()

dirname = os.path.dirname(__file__)
migrations_dir = os.path.join(dirname, 'migrations')


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
    if oldversion is None or oldversion == {version}:
        migration_file = os.path.join(migrations_dir, f"pgstac.{version}.sql")
    else:
        migration_file = os.path.join(migrations_dir, f"pgstac.{oldversion}-{version}.sql")

    if not os.path.exists(migration_file):
        raise Exception(f"Pypgstac does not have a migration from {oldversion} to {version} ({migration_file})")

    with open(migration_file) as f:
        migration_sql = f.read()
        async with conn.transaction():
            conn.add_log_listener(pglogger)
            await conn.execute(migration_sql)
            await conn.execute(
                """
                INSERT INTO pgstac.versions (version)
                VALUES ($1)
                """,
                version,
            )

    await conn.close()


@app.command()
def migrate(dsn: str = None):
    typer.echo(asyncio.run(run_migration(dsn)))


async def load_ndjson(file: str, table: str, dsn: str = None):
    with open(file, "rb") as f:
        conn = await asyncpg.connect(dsn=dsn)
        async with conn.transaction():
            await conn.execute(
                f"SET client_min_messages to 'notice'; TRUNCATE {table};"
            )
            conn.add_log_listener(pglogger)
            await conn.copy_to_table(table, source=f, columns=["content"])
        await conn.close()


@app.command()
def collections(file: str, dsn: str = None):
    typer.echo(asyncio.run(load_ndjson(file, "collections", dsn)))


@app.command()
def items(file: str, dsn: str = None):
    typer.echo(asyncio.run(load_ndjson(file, "items", dsn)))


if __name__ == "__main__":
    app()
