"""Tests for pypgstac."""
from pathlib import Path
from pypgstac.load import Methods, Loader
from psycopg.errors import UniqueViolation
import pytest

HERE = Path(__file__).parent
TEST_DATA_DIR = HERE.parent.parent / "test" / "testdata"
TEST_COLLECTIONS_JSON = TEST_DATA_DIR / "collections.json"
TEST_COLLECTIONS = TEST_DATA_DIR / "collections.ndjson"
TEST_ITEMS = TEST_DATA_DIR / "items.ndjson"


def test_load_collections_succeeds(loader: Loader) -> None:
    """Test pypgstac collections loader."""
    loader.load_collections(
        str(TEST_COLLECTIONS),
        insert_mode=Methods.insert,
    )


def test_load_collections_json_succeeds(loader: Loader) -> None:
    """Test pypgstac collections loader."""
    loader.load_collections(
        str(TEST_COLLECTIONS_JSON),
        insert_mode=Methods.insert,
    )


def test_load_collections_json_duplicates_fails(loader: Loader) -> None:
    """Test pypgstac collections loader."""
    loader.load_collections(
        str(TEST_COLLECTIONS_JSON),
        insert_mode=Methods.insert,
    )
    with pytest.raises(UniqueViolation):
        loader.load_collections(
            str(TEST_COLLECTIONS_JSON),
            insert_mode=Methods.insert,
        )


def test_load_collections_json_duplicates_with_upsert(loader: Loader) -> None:
    """Test pypgstac collections loader."""
    loader.load_collections(
        str(TEST_COLLECTIONS_JSON),
        insert_mode=Methods.insert,
    )
    loader.load_collections(
        str(TEST_COLLECTIONS_JSON),
        insert_mode=Methods.upsert,
    )


def test_load_collections_json_duplicates_with_ignore(loader: Loader) -> None:
    """Test pypgstac collections loader."""
    loader.load_collections(
        str(TEST_COLLECTIONS_JSON),
        insert_mode=Methods.insert,
    )
    loader.load_collections(
        str(TEST_COLLECTIONS_JSON),
        insert_mode=Methods.ignore,
    )


def test_load_items_duplicates_fails(loader: Loader) -> None:
    """Test pypgstac collections loader."""
    loader.load_collections(
        str(TEST_COLLECTIONS),
        insert_mode=Methods.insert,
    )
    loader.load_items(
        str(TEST_ITEMS),
        insert_mode=Methods.insert,
    )

    with pytest.raises(UniqueViolation):
        loader.load_items(
            str(TEST_ITEMS),
            insert_mode=Methods.insert,
        )


def test_load_items_succeeds(loader: Loader) -> None:
    """Test pypgstac items loader."""
    loader.load_collections(
        str(TEST_COLLECTIONS),
        insert_mode=Methods.upsert,
    )

    loader.load_items(
        str(TEST_ITEMS),
        insert_mode=Methods.insert,
    )


def test_load_items_ignore_succeeds(loader: Loader) -> None:
    """Test pypgstac items ignore loader."""
    loader.load_collections(
        str(TEST_COLLECTIONS),
        insert_mode=Methods.ignore,
    )

    loader.load_items(
        str(TEST_ITEMS),
        insert_mode=Methods.insert,
    )

    loader.load_items(
        str(TEST_ITEMS),
        insert_mode=Methods.ignore,
    )


def test_load_items_upsert_succeeds(loader: Loader) -> None:
    """Test pypgstac items ignore loader."""
    loader.load_collections(
        str(TEST_COLLECTIONS),
        insert_mode=Methods.ignore,
    )

    loader.load_items(
        str(TEST_ITEMS),
        insert_mode=Methods.insert,
    )

    loader.load_items(
        str(TEST_ITEMS),
        insert_mode=Methods.upsert,
    )


def test_load_items_delsert_succeeds(loader: Loader) -> None:
    """Test pypgstac items ignore loader."""
    loader.load_collections(
        str(TEST_COLLECTIONS),
        insert_mode=Methods.ignore,
    )

    loader.load_items(
        str(TEST_ITEMS),
        insert_mode=Methods.insert,
    )

    loader.load_items(
        str(TEST_ITEMS),
        insert_mode=Methods.delsert,
    )


def test_partition_loads_default(loader: Loader) -> None:
    """Test pypgstac items ignore loader."""
    loader.load_collections(
        str(TEST_COLLECTIONS_JSON),
        insert_mode=Methods.ignore,
    )

    loader.load_items(
        str(TEST_ITEMS),
        insert_mode=Methods.insert,
    )

    partitions = loader.db.query_one('''
        SELECT count(*) from partitions;
    ''')

    assert partitions == 1


def test_partition_loads_month(loader: Loader) -> None:
    """Test pypgstac items ignore loader."""
    loader.load_collections(
        str(TEST_COLLECTIONS_JSON),
        insert_mode=Methods.ignore,
    )

    loader.db.connection.execute('''
        UPDATE collections SET partition_trunc='month';
    ''')

    loader.load_items(
        str(TEST_ITEMS),
        insert_mode=Methods.insert,
    )

    partitions = loader.db.query_one('''
        SELECT count(*) from partitions;
    ''')

    assert partitions == 2


def test_partition_loads_year(loader: Loader) -> None:
    """Test pypgstac items ignore loader."""
    loader.load_collections(
        str(TEST_COLLECTIONS_JSON),
        insert_mode=Methods.ignore,
    )

    loader.db.connection.execute('''
        UPDATE collections SET partition_trunc='year';
    ''')

    loader.load_items(
        str(TEST_ITEMS),
        insert_mode=Methods.insert,
    )

    partitions = loader.db.query_one('''
        SELECT count(*) from partitions;
    ''')

    assert partitions == 1
