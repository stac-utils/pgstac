"""Command utilities for managing pgstac."""

from typing import Optional
import sys
import fire
from pypgstac.db import PgstacDB
from pypgstac.migrate import Migrate
from pypgstac.load import Loader, Methods, Tables
import logging

# sys.tracebacklimit = 0


class PgstacCLI:
    """CLI for PgStac."""

    def __init__(self, dsn: Optional[str] = "", debug: bool = False):
        """Initialize PgStac CLI."""
        self.dsn = dsn
        self._db = PgstacDB(dsn=dsn, debug=debug)
        if debug:
            logging.basicConfig(level=logging.DEBUG)
            sys.tracebacklimit = 1000

    @property
    def initversion(self) -> str:
        """Return earliest migration version."""
        return "0.1.9"

    @property
    def version(self) -> Optional[str]:
        """Get PGStac version installed on database."""
        return self._db.version

    @property
    def pg_version(self) -> str:
        """Get PostgreSQL server version installed on database."""
        return self._db.pg_version

    def pgready(self) -> None:
        """Wait for a pgstac database to accept connections."""
        self._db.wait()

    def search(self, query: str) -> str:
        """Search PgStac."""
        return self._db.search(query)

    def migrate(self, toversion: Optional[str] = None) -> str:
        """Migrate PgStac Database."""
        migrator = Migrate(self._db)
        return migrator.run_migration(toversion=toversion)

    def load(
        self, table: Tables, file: str, method: Optional[Methods] = Methods.insert
    ) -> None:
        """Load collections or items into PGStac."""
        loader = Loader(db=self._db)
        if table == "collections":
            loader.load_collections(file, method)
        if table == "items":
            loader.load_items(file, method)


def cli() -> fire.Fire:
    """Wrap fire call for CLI."""
    fire.Fire(PgstacCLI)


if __name__ == "__main__":
    fire.Fire(PgstacCLI)
