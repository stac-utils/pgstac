"""Command utilities for managing pgstac."""

import logging
import sys
from typing import Optional

import fire
from smart_open import open

from pypgstac.db import PgstacDB
from pypgstac.load import Loader, Methods, Tables
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
        collection_ids: Optional[str] = None,
    ) -> None:
        """Load queryables from a JSON file.

        Args:
            file: Path to the JSON file containing queryables definition
            collection_ids: Comma-separated list of collection IDs to apply the
                            queryables
                to
        """
        import orjson

        from pypgstac.load import read_json

        # Parse collection_ids if provided
        coll_ids_array = None
        if collection_ids:
            coll_ids_array = [cid.strip() for cid in collection_ids.split(",")]

        # Read the queryables JSON file
        queryables_data = None
        for item in read_json(file):
            queryables_data = item
            break  # We only need the first item

        if not queryables_data:
            raise ValueError(f"No valid JSON data found in {file}")

        # Extract properties from the queryables definition
        properties = queryables_data.get("properties", {})
        if not properties:
            raise ValueError("No properties found in queryables definition")

        conn = self._db.connect()
        with conn.cursor() as cur:
            with conn.transaction():
                # Insert each property as a queryable
                for name, definition in properties.items():
                    # Skip core fields that are already indexed
                    if name in (
                        "id",
                        "geometry",
                        "datetime",
                        "end_datetime",
                        "collection",
                    ):
                        continue

                    # Determine property wrapper based on type
                    property_wrapper = "to_text"  # default
                    if definition.get("type") == "number":
                        property_wrapper = "to_float"
                    elif definition.get("type") == "integer":
                        property_wrapper = "to_int"
                    elif definition.get("format") == "date-time":
                        property_wrapper = "to_tstz"
                    elif definition.get("type") == "array":
                        property_wrapper = "to_text_array"

                    # Determine index type (default to BTREE)
                    property_index_type = "BTREE"

                    # First delete any existing queryable with the same name
                    if coll_ids_array is None:
                        # If no collection_ids specified, delete queryables
                        # with NULL collection_ids
                        cur.execute(
                            """
                            DELETE FROM queryables
                            WHERE name = %s AND collection_ids IS NULL
                            """,
                            [name],
                        )
                    else:
                        # Delete queryables with matching name and collection_ids
                        cur.execute(
                            """
                            DELETE FROM queryables
                            WHERE name = %s AND collection_ids = %s::text[]
                            """,
                            [name, coll_ids_array],
                        )

                        # Also delete queryables with NULL collection_ids
                        cur.execute(
                            """
                            DELETE FROM queryables
                            WHERE name = %s AND collection_ids IS NULL
                            """,
                            [name],
                        )

                    # Then insert the new queryable
                    cur.execute(
                        """
                        INSERT INTO queryables
                        (name, collection_ids, definition, property_wrapper,
                        property_index_type)
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                        [
                            name,
                            coll_ids_array,
                            orjson.dumps(definition).decode(),
                            property_wrapper,
                            property_index_type,
                        ],
                    )

                # Trigger index creation
                cur.execute("SELECT maintain_partitions();")


def cli() -> fire.Fire:
    """Wrap fire call for CLI."""
    fire.Fire(PgstacCLI)


if __name__ == "__main__":
    fire.Fire(PgstacCLI)
