"""Tests for pypgstac."""
from pathlib import Path
import unittest

from pypgstac.db import PgstacDB
from pypgstac.load import Loader, Methods

HERE = Path(__file__).parent
TEST_DATA_DIR = HERE.parent.parent / "test" / "testdata"
TEST_COLLECTIONS_JSON = TEST_DATA_DIR / "collections.json"
TEST_COLLECTIONS = TEST_DATA_DIR / "collections.ndjson"
TEST_ITEMS = TEST_DATA_DIR / "items.ndjson"

db = PgstacDB()
loader = Loader(db)


class LoadTest(unittest.TestCase):
    """Tests pypgstac data loader."""

    def test_load_collections_succeeds(self) -> None:
        """Test pypgstac collections loader."""
        loader.load_collections(
            str(TEST_COLLECTIONS),
            insert_mode=Methods.ignore,
        )

    # def test_load_items_succeeds(self) -> None:
    #     """Test pypgstac items loader."""
    #     loader.load_collections(
    #         str(TEST_COLLECTIONS),
    #         insert_mode=Methods.ignore,
    #     )

    #     loader.load_items(
    #         str(TEST_ITEMS),
    #         insert_mode=Methods.insert,
    #     )

    # def test_load_items_ignore_succeeds(self) -> None:
    #     """Test pypgstac items ignore loader."""
    #     loader.load_collections(
    #         str(TEST_COLLECTIONS),
    #         insert_mode=Methods.ignore,
    #     )

    #     loader.load_items(
    #         str(TEST_ITEMS),
    #         insert_mode=Methods.insert,
    #     )

    #     loader.load_items(
    #         str(TEST_ITEMS),
    #         insert_mode=Methods.ignore,
    #     )
