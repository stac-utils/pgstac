"""Command utilities for managing pgstac."""

import logging
import sys
from typing import Optional

import fire
import orjson
from smart_open import open

from pypgstac.db import PgstacDB
from pypgstac.load import Loader, Methods, Tables, read_json
from pypgstac.migrate import Migrate


class PgstacCLI:
    """CLI for PgSTAC."""

    def __init__(
        self,
        dsn: Optional[str] = "",
        version: bool = False,
        debug: bool = False,
        usequeue: bool = False,
    ):
        """Initialize PgSTAC CLI."""
        if version:
            sys.exit(0)

        self.dsn = dsn
        self._db = PgstacDB(dsn=dsn, debug=debug, use_queue=usequeue)
        if debug:
            logging.basicConfig(level=logging.DEBUG)
            sys.tracebacklimit = 1000

    @property
    def initversion(self) -> str:
        """Return earliest migration version."""
        return "0.1.9"

    @property
    def version(self) -> Optional[str]:
        """Get PgSTAC version installed on database."""
        return self._db.version

    @property
    def pg_version(self) -> str:
        """Get PostgreSQL server version installed on database."""
        return self._db.pg_version

    def pgready(self) -> None:
        """Wait for a pgstac database to accept connections."""
        self._db.wait()

    def search(self, query: str) -> str:
        """Search PgSTAC."""
        return self._db.search(query)

    def migrate(self, toversion: Optional[str] = None) -> str:
        """Migrate PgSTAC Database."""
        migrator = Migrate(self._db)
        return migrator.run_migration(toversion=toversion)

    def load(
        self,
        table: Tables,
        file: str,
        method: Optional[Methods] = Methods.insert,
        dehydrated: Optional[bool] = False,
        chunksize: Optional[int] = 10000,
    ) -> None:
        """Load collections or items into PgSTAC."""
        loader = Loader(db=self._db)
        if table == "collections":
            loader.load_collections(file, method)
        if table == "items":
            loader.load_items(file, method, dehydrated, chunksize)

    def runqueue(self) -> str:
        """Drain the query queue, reporting anything that could not be run."""
        return self._db.run_queued()

    def loadextensions(self) -> None:
        conn = self._db.connect()

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO stac_extensions (url)
                SELECT DISTINCT
                substring(
                    jsonb_array_elements_text(content->'stac_extensions') FROM E'^[^#]*'
                )
                FROM collections
                ON CONFLICT DO NOTHING;
            """,
            )
            conn.commit()

        urls = self._db.query(
            """
                SELECT url FROM stac_extensions WHERE content IS NULL;
            """,
        )
        if urls:
            for u in urls:
                url = u[0]
                try:
                    with open(url, "r") as f:
                        content = f.read()
                        self._db.query(
                            """
                                UPDATE pgstac.stac_extensions
                                SET content=%s
                                WHERE url=%s
                                ;
                            """,
                            [content, url],
                        )
                        conn.commit()
                except Exception:
                    pass

    def load_queryables(
        self,
        file: str,
        collection_ids: Optional[list[str]] = None,
        delete_missing: Optional[bool] = False,
        index_fields: Optional[list[str]] = None,
    ) -> None:
        """Load queryables from a JSON file.

        Args:
            file: Path to the JSON file containing queryables definition
            collection_ids: Comma-separated list of collection IDs to apply the
                            queryables to
            delete_missing: If True, delete properties not present in the file.
                            If collection_ids is specified, only delete properties
                            for those collections.
            index_fields: List of field names to create indexes for. If not provided,
                         no indexes will be created. Creating too many indexes can
                         negatively impact performance.
        """

        # Read the queryables JSON file
        queryables_data = None
        for item in read_json(file):
            queryables_data = item
            break  # We only need the first item

        if not queryables_data:
            raise ValueError(f"No valid JSON data found in {file}")

        # One call: everything about how a property becomes a queryable -- the wrapper,
        # the index method, the properties. prefix and the collection_ids rules --
        # stays in SQL beside the constraints that enforce them.
        self._db.connect().execute(
            "SELECT upsert_queryables(%s::jsonb, %s::text[], %s::text[], %s)",
            [
                orjson.dumps(queryables_data).decode(),
                collection_ids,
                index_fields,
                bool(delete_missing),
            ],
        )


def cli() -> fire.Fire:
    """Wrap fire call for CLI."""
    fire.Fire(PgstacCLI)


if __name__ == "__main__":
    fire.Fire(PgstacCLI)
