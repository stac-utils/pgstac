"""Tests for pypgstac."""
import json
from pathlib import Path
from pypgstac.load import Methods, Loader, read_json
from psycopg.errors import UniqueViolation
import pytest
import pystac

HERE = Path(__file__).parent
TEST_DATA_DIR = HERE.parent.parent / "test" / "testdata"
TEST_COLLECTIONS_JSON = TEST_DATA_DIR / "collections.json"
TEST_COLLECTIONS = TEST_DATA_DIR / "collections.ndjson"
TEST_ITEMS = TEST_DATA_DIR / "items.ndjson"
TEST_DEHYDRATED_ITEMS = TEST_DATA_DIR / "items.pgcopy"

S1_GRD_COLLECTION = (
    HERE / "data-files" / "hydration" / "collections" / "sentinel-1-grd.json"
)

S1_GRD_ITEM = (
    HERE
    / "data-files"
    / "hydration"
    / "raw-items"
    / "sentinel-1-grd"
    / "S1A_IW_GRDH_1SDV_20220428T034417_20220428T034442_042968_05213C.json"
)


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

    partitions = loader.db.query_one(
        """
        SELECT count(*) from partitions;
    """
    )

    assert partitions == 1


def test_partition_loads_month(loader: Loader) -> None:
    """Test pypgstac items ignore loader."""
    loader.load_collections(
        str(TEST_COLLECTIONS_JSON),
        insert_mode=Methods.ignore,
    )
    if loader.db.connection is not None:
        loader.db.connection.execute(
            """
            UPDATE collections SET partition_trunc='month';
        """
        )

    loader.load_items(
        str(TEST_ITEMS),
        insert_mode=Methods.insert,
    )

    partitions = loader.db.query_one(
        """
        SELECT count(*) from partitions;
    """
    )

    assert partitions == 2


def test_partition_loads_year(loader: Loader) -> None:
    """Test pypgstac items ignore loader."""
    loader.load_collections(
        str(TEST_COLLECTIONS_JSON),
        insert_mode=Methods.ignore,
    )
    if loader.db.connection is not None:
        loader.db.connection.execute(
            """
            UPDATE collections SET partition_trunc='year';
        """
        )

    loader.load_items(
        str(TEST_ITEMS),
        insert_mode=Methods.insert,
    )

    partitions = loader.db.query_one(
        """
        SELECT count(*) from partitions;
    """
    )

    assert partitions == 1


def test_load_items_dehydrated_ignore_succeeds(loader: Loader) -> None:
    """Test pypgstac items ignore loader."""
    loader.load_collections(
        str(TEST_COLLECTIONS),
        insert_mode=Methods.ignore,
    )

    loader.load_items(
        str(TEST_DEHYDRATED_ITEMS), insert_mode=Methods.insert, dehydrated=True
    )

    loader.load_items(
        str(TEST_DEHYDRATED_ITEMS), insert_mode=Methods.ignore, dehydrated=True
    )


def test_format_items_keys(loader: Loader) -> None:
    """Test pypgstac items ignore loader."""
    loader.load_collections(
        str(TEST_COLLECTIONS_JSON),
        insert_mode=Methods.ignore,
    )

    items_iter = read_json(str(TEST_ITEMS))
    item_json = next(iter(items_iter))
    out = loader.format_item(item_json)

    # Top level keys expected after format
    assert "id" in out
    assert "collection" in out
    assert "geometry" in out
    assert "content" in out

    # Special keys expected not to be in the item content
    content_json = json.loads(out["content"])
    assert "id" not in content_json
    assert "collection" not in content_json
    assert "geometry" not in content_json

    # Ensure bbox is included in content
    assert "bbox" in content_json


def test_s1_grd_load_and_query(loader: Loader) -> None:
    """Test pypgstac items ignore loader."""
    loader.load_collections(
        str(S1_GRD_COLLECTION),
        insert_mode=Methods.ignore,
    )

    loader.load_items(str(S1_GRD_ITEM), insert_mode=Methods.insert)

    search_body = {
        "filter-lang": "cql2-json",
        "filter": {
            "op": "and",
            "args": [
                {
                    "op": "=",
                    "args": [{"property": "collection"}, "sentinel-1-grd"],
                },
                {
                    "op": "=",
                    "args": [
                        {"property": "id"},
                        "S1A_IW_GRDH_1SDV_20220428T034417_20220428T034442_042968_05213C",  # noqa: E501
                    ],
                },
            ],
        },
    }

    res = next(
        loader.db.func(
            "search",
            search_body,
        )
    )[0]
    item = res["features"][0]
    pystac.Item.from_dict(item).validate()
