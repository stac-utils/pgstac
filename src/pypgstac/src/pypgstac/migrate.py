"""Utilities to help migrate pgstac schema."""

import glob
import logging
import os
import re
from collections import defaultdict
from typing import Any, Dict, Iterator, List, Optional

from smart_open import open

from . import __version__
from .db import PgstacDB

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
            "pgstac.",
            "",
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
                f"Could not determine path to get from {self.f} to {self.t}.",
            )
        if len(path) == 1:
            return [f"pgstac.{path[0]}.sql"]
        files = []
        for idx in range(len(path) - 1):
            f = f"pgstac.{path[idx]}-{path[idx + 1]}.sql"
            f = f.replace("--init", "")
            files.append(f"pgstac.{path[idx]}-{path[idx + 1]}.sql")
        return files


def get_sql(file: str) -> str:
    """Get sql from a file as a string, without its own transaction control.

    A migration file wraps itself in BEGIN/COMMIT so that applying one directly with
    psql is atomic. Every file of a chain is executed here on one connection with
    autocommit off, and an embedded COMMIT would end that transaction, leaving the
    files already applied committed when a later one fails.
    """
    sqlstrs = []
    file = re.sub("[0-9]+[.][0-9]+[.][0-9]+-dev", "unreleased", file)
    fp = os.path.join(migrations_dir, file)
    file_handle: Any = open(fp)

    with file_handle as fd:
        sqlstrs.extend(fd.readlines())
    sql = "\n".join(sqlstrs)
    # Both or neither. Four migrations from 0.2.x put SET SEARCH_PATH before
    # their BEGIN, so stripping the trailing COMMIT on its own would leave an
    # unmatched BEGIN inside the transaction this runs in.
    stripped, n = re.subn(r"\A\s*BEGIN\s*;", "", sql, flags=re.IGNORECASE)
    if n:
        stripped = re.sub(r"COMMIT\s*;\s*\Z", "", stripped, flags=re.IGNORECASE)
        return stripped
    return sql


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
        if re.search(r"-dev$", toversion):
            logger.info("using unreleased version")
            toversion = "unreleased"

        major, minor, patch = tuple(
            map(
                int,
                [
                    self.db.pg_version[i : i + 2]
                    for i in range(0, len(self.db.pg_version), 2)
                ],
            ),
        )
        logger.info(f"Migrating PgSTAC on PostgreSQL Version {major}.{minor}.{patch}")
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

        # Before the schema moves under them: a queued statement names the function
        # signatures it was queued against. Outside the migration's transaction, so a
        # failure here stops the upgrade rather than rolling back a half-applied one.
        if self.db.queue_length() > 0:
            # Statistics updates are discarded rather than run: 998_idempotent_post
            # queues every partition again once the migration commits, so running them
            # now locks every partition to compute what is about to be recomputed, and
            # a failure would stop the upgrade over work that was going to be redone.
            discarded = self.db.discard_queued_partition_stats()
            if discarded:
                logger.info(
                    f"Discarded {discarded} queued partition statistics updates; "
                    "they are queued again after the migration.",
                )
            if self.db.queue_length() > 0:
                logger.info("Draining the query queue before migrating.")
                self.db.run_queued()

        conn = self.db.connect()

        queued = 0
        with conn.cursor() as cur:
            conn.autocommit = False
            for file in files:
                logger.debug(f"Running migration file {file}.")
                migration_sql = get_sql(file)
                cur.execute(migration_sql)
                logger.debug(cur.statusmessage)
                logger.debug(cur.rowcount)

            logger.debug(f"Database migrated to {toversion}")

            # Inside the migration's transaction: querying after the commit
            # below leaves this connection idle in transaction, holding locks.
            # Two statements, not one guarded by CASE: query_queue does not
            # exist in the versions a chain starts from, and names resolve at
            # parse time, so even an unreachable reference aborts the
            # migration.
            cur.execute("SELECT to_regclass('pgstac.query_queue') IS NOT NULL;")
            exists = cur.fetchone()
            if exists and exists[0]:
                cur.execute("SELECT count(*) FROM pgstac.query_queue;")
                row = cur.fetchone()
                queued = row[0] if row else 0

        newversion = self.db.version
        if conn is not None:
            if newversion == toversion:
                conn.commit()
            else:
                conn.rollback()
                raise Exception(
                    "Migration failed, database rolled back to previous state.",
                )

        conn.autocommit = True

        logger.debug(f"New Version: {newversion}")

        # 998_idempotent_post queues partition maintenance rather than running it in
        # the migration's transaction, so it is drained here: an operator running no
        # queue runner of their own would otherwise be left with stale statistics and
        # constraints and nothing saying so. The schema change has already committed,
        # so a statement that cannot be run is reported, not raised -- the migration
        # itself succeeded.
        if queued > 0:
            logger.info(f"Draining {queued} queued maintenance statements.")
            try:
                self.db.run_queued()
            except RuntimeError as e:
                logger.error(
                    f"{e}\nThe schema is migrated to {newversion}, but partition "
                    "statistics and constraints are stale for the statements above. "
                    "Fix the cause and run 'pypgstac runqueue'.",
                )

        return newversion
