"""Utilities to help migrate pgstac schema."""
import glob
import os
from collections import defaultdict
from typing import Optional, Dict, List, Iterator, Any

import asyncpg
import typer
from smart_open import open

from pypgstac import __version__ as version

dirname = os.path.dirname(__file__)
migrations_dir = os.path.join(dirname, "migrations")


class MigrationPath:
    """Calculate path from migration files to get from one version to the next."""

    def __init__(self, path: str, f: str, t: str):
        """Initialize MigrationPath."""
        self.path = path
        if f is None:
            f = "init"
        if t is None:
            raise Exception('Must set "to" version')
        if f == t:
            raise Exception("No Migration Necessary")

        self.f = f
        self.t = t

    def parse_filename(self, filename: str) -> List[str]:
        """Get version numbers from filename."""
        filename = os.path.splitext(os.path.basename(filename))[0].replace(
            "pgstac.", ""
        )
        return filename.split("-")

    def get_files(self) -> Iterator[str]:
        """Find all migration files available."""
        path = self.path.rstrip("/")
        return glob.iglob(f"{path}/*.sql")

    def build_graph(self) -> Dict:
        """Build a graph to get from one version to another."""
        graph = defaultdict(list)
        for file in self.get_files():
            parts = self.parse_filename(file)
            if len(parts) == 2:
                graph[parts[0]].append(parts[1])
            else:
                graph["init"].append(parts[0])
        return graph

    def build_path(self) -> Optional[List[str]]:
        """Create the path of ordered files needed to migrate."""
        graph = self.build_graph()
        explored: List = []
        q = [[self.f]]

        while q:
            path = q.pop(0)
            node = path[-1]
            if node not in explored:
                neighbours = graph[node]
                for neighbour in neighbours:
                    new_path = list(path)
                    new_path.append(neighbour)
                    q.append(new_path)
                    if neighbour == self.t:
                        return new_path
                explored.append(node)
        return None

    def migrations(self) -> List[str]:
        """Return the list of migrations needed in order."""
        path = self.build_path()
        if path is None:
            raise Exception(
                "Could not determine path to get f %s to %s.", self.f, self.t
            )
        if len(path) == 1:
            return [f"pgstac.{path[0]}.sql"]
        files = []
        for idx in range(len(path) - 1):
            f = f"pgstac.{path[idx]}-{path[idx+1]}.sql"
            f = f.replace("--init", "")
            files.append(f"pgstac.{path[idx]}-{path[idx+1]}.sql")
        return files


def get_sql(file: str) -> str:
    """Get sql from a file as a string."""
    sqlstrs = []
    fp = os.path.join(migrations_dir, file)
    file_handle: Any = open(fp)

    with file_handle as fd:
        sqlstrs.extend(fd.readlines())
    return "\n".join(sqlstrs)


def get_initial_version() -> str:
    """Get initial version available in migrations."""
    return "0.1.9"


async def get_version(conn: asyncpg.Connection) -> str:
    """Get the current version number from a pgstac database."""
    async with conn.transaction():
        try:
            version = await conn.fetchval(
                """
                SELECT version FROM pgstac.migrations
                ORDER BY datetime DESC LIMIT 1;
                """
            )
        except asyncpg.exceptions.UndefinedTableError:
            version = None
    return version


async def get_version_dsn(dsn: Optional[str] = None) -> str:
    """Get current version from a specified database."""
    conn = await asyncpg.connect(dsn=dsn)
    version = await get_version(conn)
    await conn.close()
    return version


async def run_migration(
    dsn: Optional[str] = None, toversion: Optional[str] = None
) -> str:
    """Migrate a pgstac database to current version."""
    if toversion is None:
        toversion = version
    files = []
    conn = await asyncpg.connect(dsn=dsn)
    oldversion = await get_version(conn)

    if oldversion == toversion:
        typer.echo(f"Target database already at version: {toversion}")
        return toversion
    if oldversion is None:
        typer.echo(f"No pgstac version set, installing {toversion} from scratch.")
        files.append(os.path.join(migrations_dir, f"pgstac.{toversion}.sql"))
    else:
        typer.echo(f"Migrating from {oldversion} to {toversion}.")
        m = MigrationPath(migrations_dir, oldversion, toversion)
        files = m.migrations()

    if len(files) < 1:
        raise Exception("Could not find migration files")

    typer.echo(f"Running migrations for {files}.")

    for file in files:
        migration_sql = get_sql(file)
        async with conn.transaction():
            await conn.execute(migration_sql)
            await conn.execute(
                """
                INSERT INTO pgstac.migrations (version)
                VALUES ($1);
                """,
                toversion,
            )

    newversion = await get_version(conn)
    await conn.close()

    return newversion
