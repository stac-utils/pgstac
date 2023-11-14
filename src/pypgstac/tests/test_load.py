"""Tests for pypgstac."""
import json
import re
from pathlib import Path
from unittest import mock

import pytest
from psycopg.errors import UniqueViolation
from version_parser import Version as V

from pypgstac.load import Loader, Methods, __version__, read_json

HERE = Path(__file__).parent
TEST_DATA_DIR = HERE.parent.parent / "pgstac" / "tests" / "testdata"
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


def version_increment(source_version: str) -> str:
    source_version = re.sub("-dev$","",source_version)
    version = V(source_version)
    return ".".join(
        map(
            str,
            [
                version.get_major_version(),
                version.get_minor_version(),
                version.get_patch_version() + 1,
            ],
        ),
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
    """,
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
        """,
        )

    loader.load_items(
        str(TEST_ITEMS),
        insert_mode=Methods.insert,
    )

    partitions = loader.db.query_one(
        """
        SELECT count(*) from partitions;
    """,
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
        """,
        )

    loader.load_items(
        str(TEST_ITEMS),
        insert_mode=Methods.insert,
    )

    partitions = loader.db.query_one(
        """
        SELECT count(*) from partitions;
    """,
    )

    assert partitions == 1


def test_load_items_dehydrated_ignore_succeeds(loader: Loader) -> None:
    """Test pypgstac items ignore loader."""
    loader.load_collections(
        str(TEST_COLLECTIONS),
        insert_mode=Methods.ignore,
    )

    loader.load_items(
        str(TEST_DEHYDRATED_ITEMS), insert_mode=Methods.insert, dehydrated=True,
    )

    loader.load_items(
        str(TEST_DEHYDRATED_ITEMS), insert_mode=Methods.ignore, dehydrated=True,
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
        ),
    )[0]
    res["features"][0]


def test_load_dehydrated(loader: Loader) -> None:
    """Test loader for items dumped directly out of item table."""
    collections = [
        HERE / "data-files" / "hydration" / "collections" / "chloris-biomass.json",
    ]

    for collection in collections:
        loader.load_collections(
            str(collection),
            insert_mode=Methods.ignore,
        )

    dehydrated_items = HERE / "data-files" / "load" / "dehydrated.txt"

    loader.load_items(
        str(dehydrated_items), insert_mode=Methods.insert, dehydrated=True,
    )


def test_load_collections_incompatible_version(loader: Loader) -> None:
    """Test pypgstac collections loader raises an exception for incompatible version."""
    with mock.patch(
        "pypgstac.db.PgstacDB.version", new_callable=mock.PropertyMock,
    ) as mock_version:
        mock_version.return_value = "dummy"
        with pytest.raises(ValueError):
            loader.load_collections(
                str(TEST_COLLECTIONS_JSON),
                insert_mode=Methods.insert,
            )


def test_load_items_incompatible_version(loader: Loader) -> None:
    """Test pypgstac items loader raises an exception for incompatible version."""
    loader.load_collections(
        str(TEST_COLLECTIONS_JSON),
        insert_mode=Methods.insert,
    )
    with mock.patch(
        "pypgstac.db.PgstacDB.version", new_callable=mock.PropertyMock,
    ) as mock_version:
        mock_version.return_value = "dummy"
        with pytest.raises(ValueError):
            loader.load_items(
                str(TEST_ITEMS),
                insert_mode=Methods.insert,
            )


def test_load_compatible_major_minor_version(loader: Loader) -> None:
    """Test pypgstac loader doesn't raise an exception."""
    with mock.patch(
        "pypgstac.load.__version__", version_increment(__version__),
    ) as mock_version:
        loader.load_collections(
            str(TEST_COLLECTIONS_JSON),
            insert_mode=Methods.insert,
        )
        loader.load_items(
            str(TEST_ITEMS),
            insert_mode=Methods.insert,
        )
        assert mock_version != loader.db.version


def test_load_items_nopartitionconstraint_succeeds(loader: Loader) -> None:
    """Test pypgstac items loader."""
    loader.load_collections(
        str(TEST_COLLECTIONS),
        insert_mode=Methods.upsert,
    )
    loader.load_items(
        str(TEST_ITEMS),
        insert_mode=Methods.insert,
    )

    cdtmin = loader.db.query_one(
        """
        SELECT lower(constraint_dtrange)::text
        FROM partition_sys_meta WHERE partition = '_items_1';
        """,
    )

    assert cdtmin == "2011-07-31 00:00:00+00"
    with loader.db.connect() as conn:
        conn.execute(
            """
            ALTER TABLE _items_1 DROP CONSTRAINT _items_1_dt;
            """,
        )
    cdtmin = loader.db.query_one(
        """
        SELECT lower(constraint_dtrange)::text
        FROM partition_sys_meta WHERE partition = '_items_1';
        """,
    )
    assert cdtmin == "-infinity"

    loader.load_items(
        str(TEST_ITEMS),
        insert_mode=Methods.upsert,
    )
    cdtmin = loader.db.query_one(
        """
        SELECT lower(constraint_dtrange)::text
        FROM partition_sys_meta WHERE partition = '_items_1';
        """,
    )
    assert cdtmin == "2011-07-31 00:00:00+00"
