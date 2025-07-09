"""Command utilities for managing pgstac."""

import logging
import sys
from typing import Optional

import fire
import orjson
from psycopg.rows import tuple_row
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
        self.debug = debug
        self.usequeue = usequeue

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
        with PgstacDB(dsn=self.dsn, debug=self.debug, use_queue=self.usequeue) as db:
            return db.version

    @property
    def pg_version(self) -> str:
        """Get PostgreSQL server version installed on database."""
        with PgstacDB(dsn=self.dsn, debug=self.debug, use_queue=self.usequeue) as db:
            return db.pg_version

    def pgready(self) -> None:
        """Wait for a pgstac database to accept connections."""
        with PgstacDB(dsn=self.dsn, debug=self.debug, use_queue=self.usequeue) as db:
            db.wait()

    def search(self, query: str) -> str:
        """Search PgSTAC."""
        with PgstacDB(dsn=self.dsn, debug=self.debug, use_queue=self.usequeue) as db:
            db.search(query)

    def migrate(self, toversion: Optional[str] = None) -> str:
        """Migrate PgSTAC Database."""
        with PgstacDB(dsn=self.dsn, debug=self.debug, use_queue=self.usequeue) as db:
            return Migrate(db).run_migration(toversion=toversion)

    def load(
        self,
        table: Tables,
        file: str,
        method: Optional[Methods] = Methods.insert,
        dehydrated: Optional[bool] = False,
        chunksize: Optional[int] = 10000,
    ) -> None:
        """Load collections or items into PgSTAC."""
        with PgstacDB(dsn=self.dsn, debug=self.debug, use_queue=self.usequeue) as db:
            loader = Loader(db=db)
            if table == "collections":
                loader.load_collections(file, method)
            if table == "items":
                loader.load_items(file, method, dehydrated, chunksize)

    def runqueue(self) -> str:
        with PgstacDB(dsn=self.dsn, debug=self.debug, use_queue=self.usequeue) as db:
            return db.run_queued()

    def loadextensions(self) -> None:
        with PgstacDB(dsn=self.dsn, debug=self.debug, use_queue=self.usequeue) as db:
            with db.connect() as conn:
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

                with conn.cursor(row_factory=tuple_row) as cur:
                    query = """
                        SELECT url FROM stac_extensions WHERE content IS NULL;
                    """
                    urls = cur.execute(query, prepare=False)
                    for u in urls:
                        url = u[0]
                        try:
                            with open(url, "r") as f:
                                content = f.read()

                            cur.execute(
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

        # Extract properties from the queryables definition
        properties = queryables_data.get("properties", {})
        if not properties:
            raise ValueError("No properties found in queryables definition")

        with PgstacDB(dsn=self.dsn, debug=self.debug, use_queue=self.usequeue) as db:
            with db.connect() as conn:
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

                            # Determine if this field should be indexed
                            property_index_type = None
                            if index_fields and name in index_fields:
                                property_index_type = "BTREE"

                            # First delete any existing queryable with the same name
                            if not collection_ids:
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
                                    [name, collection_ids],
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
                                    collection_ids,
                                    orjson.dumps(definition).decode(),
                                    property_wrapper,
                                    property_index_type,
                                ],
                            )

                        # If delete_missing is True,
                        # delete all queryables that were not in the file
                        if delete_missing:
                            # Get the list of property names from the file
                            property_names = list(properties.keys())

                            # Skip core fields that are already indexed
                            core_fields = [
                                "id",
                                "geometry",
                                "datetime",
                                "end_datetime",
                                "collection",
                            ]
                            property_names = [
                                name
                                for name in property_names
                                if name not in core_fields
                            ]

                            if not property_names:
                                # If no valid properties, don't delete anything
                                pass
                            elif not collection_ids:
                                # If no collection_ids specified,
                                # delete queryables with NULL collection_ids
                                # that are not in the property_names list
                                placeholders = ", ".join(["%s"] * len(property_names))
                                core_placeholders = ", ".join(["%s"] * len(core_fields))

                                # Build the query with proper placeholders
                                query = f"""
                                    DELETE FROM queryables
                                    WHERE collection_ids IS NULL
                                    AND name NOT IN ({placeholders})
                                    AND name NOT IN ({core_placeholders})
                                """

                                # Flatten the parameters
                                params = property_names + core_fields

                                cur.execute(query, params)
                            else:
                                # Delete queryables with matching collection_ids
                                # that are not in the property_names list
                                placeholders = ", ".join(["%s"] * len(property_names))
                                core_placeholders = ", ".join(["%s"] * len(core_fields))

                                # Build the query with proper placeholders
                                query = f"""
                                    DELETE FROM queryables
                                    WHERE collection_ids = %s::text[]
                                    AND name NOT IN ({placeholders})
                                    AND name NOT IN ({core_placeholders})
                                """

                                # Flatten the parameters
                                params = [collection_ids] + property_names + core_fields

                                cur.execute(query, params)

                        # Trigger index creation only if index_fields were provided
                        if index_fields and len(index_fields) > 0:
                            cur.execute("SELECT maintain_partitions();")


def cli() -> fire.Fire:
    """Wrap fire call for CLI."""
    fire.Fire(PgstacCLI)


if __name__ == "__main__":
    fire.Fire(PgstacCLI)
