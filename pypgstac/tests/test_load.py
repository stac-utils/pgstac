import asyncio
from pathlib import Path
import unittest

from pypgstac.pypgstac import load_ndjson, loadopt, tables

HERE = Path(__file__).parent
TEST_DATA_DIR = HERE.parent.parent / "test" / "testdata"
TEST_COLLECTIONS = TEST_DATA_DIR / "collections.ndjson"
TEST_ITEMS = TEST_DATA_DIR / "items.ndjson"


class LoadTest(unittest.TestCase):
    def test_load_testdata_succeeds(self) -> None:
        asyncio.run(
            load_ndjson(
                str(TEST_COLLECTIONS), table=tables.collections, method=loadopt.upsert
            )
        )
        asyncio.run(
            load_ndjson(str(TEST_ITEMS), table=tables.items, method=loadopt.upsert)
        )
