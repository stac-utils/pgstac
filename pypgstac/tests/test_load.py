"""Tests for pypgstac."""
import asyncio
from copy import copy
import json
from pathlib import Path
from typing import IO
import unittest
from tempfile import NamedTemporaryFile

from pypgstac.pypgstac import delete_ndjson, load_ndjson, loadopt, tables
from pypgstac.load import DB

HERE = Path(__file__).parent
TEST_DATA_DIR = HERE.parent.parent / "test" / "testdata"
TEST_COLLECTIONS = TEST_DATA_DIR / "collections.ndjson"
TEST_ITEMS = TEST_DATA_DIR / "items.ndjson"

with open(TEST_DATA_DIR / "sample_item.json", "r") as f:
    SAMPLE_ITEM = json.load(f)


async def count_items() -> int:
    async with DB() as conn:
        row = await conn.fetchrow("SELECT COUNT(*) FROM items")
        return row[0]


async def clear_items() -> None:
    async with DB() as conn:
        await conn.fetchrow("DELETE FROM items")


def create_ndjson_file(num_items: int = 1000) -> IO[bytes]:
    f = NamedTemporaryFile()
    for i in range(num_items + 1):
        item = copy(SAMPLE_ITEM)
        item["id"] = f"pgstac-test-item-{i}"
        f.write(str.encode(f"{json.dumps(item)}\n"))
    f.seek(0)
    return f


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
        asyncio.run(clear_items())
        # Load some items in case there aren't any in the DB yet.
        asyncio.run(
            load_ndjson(str(TEST_ITEMS), table=tables.items, method=loadopt.upsert)
        )
        self.assertGreater(asyncio.run(count_items()), 0)
        asyncio.run(delete_ndjson(str(TEST_ITEMS)))
        self.assertEqual(asyncio.run(count_items()), 0)

    def test_delete_high_volume(self) -> None:
        """Bulk deleting a large quantity of items succeeds"""
        asyncio.run(clear_items())
        # Load many items to delete
        ndjson_file = create_ndjson_file(10000)
        asyncio.run(
            load_ndjson(ndjson_file.name, table=tables.items, method=loadopt.upsert)
        )
        self.assertGreater(asyncio.run(count_items()), 0)
        ndjson_file.seek(0)
        asyncio.run(delete_ndjson(ndjson_file.name))
        ndjson_file.close()
        self.assertEqual(asyncio.run(count_items()), 0)
