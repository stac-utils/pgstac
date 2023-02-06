"""Utilities to help migrate pgstac schema."""
import glob
import os
from collections import defaultdict
from typing import Optional, Dict, List, Iterator, Any
from smart_open import open
import logging

from .db import PgstacDB
from . import __version__

dirname = os.path.dirname(__file__)
migrations_dir = os.path.join(dirname, "migrations")

logger = logging.getLogger(__name__)


class MigrationPath:
    """Calculate path from migration files to get from one version to the next."""

    def __init__(self, path: str, f: str, t: str) -> None:
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
                f"Could not determine path to get from {self.f} to {self.t}."
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


class Migrate:
    """Utilities for migrating pgstac database."""

    def __init__(self, db: PgstacDB, schema: str = "pgstac"):
        """Prepare for migration."""
        self.db = db
        self.schema = schema

    def run_migration(self, toversion: Optional[str] = None) -> str:
        """Migrate a pgstac database to current version."""
        if toversion is None:
            toversion = __version__
        files = []

        pg_version = self.db.pg_version
        logger.info(f"Migrating PGStac on PostgreSQL Version {pg_version}")
        oldversion = self.db.version
        if oldversion == toversion:
            logger.info(f"Target database already at version: {toversion}")
            return toversion
        if oldversion is None:
            logger.info(f"No pgstac version set, installing {toversion} from scratch.")
            files.append(os.path.join(migrations_dir, f"pgstac.{toversion}.sql"))
        else:
            logger.info(f"Migrating from {oldversion} to {toversion}.")
            m = MigrationPath(migrations_dir, oldversion, toversion)
            files = m.migrations()

        if len(files) < 1:
            raise Exception("Could not find migration files")

        conn = self.db.connect()

        with conn.cursor() as cur:
            for file in files:
                logger.debug(f"Running migration file {file}.")
                migration_sql = get_sql(file)
                cur.execute(migration_sql)
                logger.debug(cur.statusmessage)
                logger.debug(cur.rowcount)

            logger.debug(f"Database migrated to {toversion}")

        newversion = self.db.version
        if conn is not None:
            if newversion == toversion:
                conn.commit()
            else:
                conn.rollback()
                raise Exception(
                    "Migration failed, database rolled back to previous state."
                )

        logger.debug(f"New Version: {newversion}")

        return newversion
