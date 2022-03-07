"""Tests for pypgstac."""
import asyncio
from pathlib import Path
import unittest

from pypgstac.pypgstac import delete_ndjson, load_ndjson, loadopt, tables
from pypgstac.load import DB

HERE = Path(__file__).parent
TEST_DATA_DIR = HERE.parent.parent / "test" / "testdata"
TEST_COLLECTIONS = TEST_DATA_DIR / "collections.ndjson"
TEST_ITEMS = TEST_DATA_DIR / "items.ndjson"


async def count_items() -> int:
    async with DB() as conn:
        row = await conn.fetchrow("SELECT COUNT(*) FROM items")
        return row[0]


class LoadTest(unittest.TestCase):
    """Tests pypgstac data loader."""

    def test_load_testdata_succeeds(self) -> None:
        """Test pypgstac data loader."""
        asyncio.run(
            load_ndjson(
                str(TEST_COLLECTIONS),
                table=tables.collections,
                method=loadopt.upsert,
            )
        )
        asyncio.run(
            load_ndjson(str(TEST_ITEMS), table=tables.items, method=loadopt.upsert)
        )

    def test_delete_succeeds(self) -> None:
        """Test bulk delete items"""
        starting_item_count = asyncio.run(count_items())
        # Load some items in case there aren't any in the DB yet.
        asyncio.run(
            load_ndjson(str(TEST_ITEMS), table=tables.items, method=loadopt.upsert)
        )
        loaded_item_count = asyncio.run(count_items())
        asyncio.run(delete_ndjson(str(TEST_ITEMS)))
        assert asyncio.run(count_items()) == starting_item_count - loaded_item_count
