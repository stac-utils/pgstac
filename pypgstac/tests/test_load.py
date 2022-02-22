"""Tests for pypgstac."""
import asyncio
from pathlib import Path
import unittest

from pypgstac.pypgstac import loadopt, tables
from pypgstac.load import load_json, load_ndjson

HERE = Path(__file__).parent
TEST_DATA_DIR = HERE.parent.parent / "test" / "testdata"
TEST_COLLECTIONS_NDJSON = TEST_DATA_DIR / "collections.ndjson"
TEST_COLLECTION_JSON = TEST_DATA_DIR / "collection.json"
TEST_ITEMS = TEST_DATA_DIR / "items.ndjson"


class LoadTest(unittest.TestCase):
    """Tests pypgstac data loader."""

    def test_load_testdata_succeeds(self) -> None:
        """Test pypgstac data loader."""
        asyncio.run(
            load_ndjson(
                str(TEST_COLLECTIONS_NDJSON),
                table=tables.collections,
                method=loadopt.upsert,
            )
        )
        asyncio.run(
            load_json(
                str(TEST_COLLECTION_JSON),
                table=tables.collections,
                method=loadopt.upsert,
            )
        )
        asyncio.run(
            load_ndjson(str(TEST_ITEMS), table=tables.items, method=loadopt.upsert)
        )
