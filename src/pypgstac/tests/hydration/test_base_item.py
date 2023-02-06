import json
from pathlib import Path
from typing import Any, Dict, cast

from pypgstac.load import Loader


HERE = Path(__file__).parent
LANDSAT_COLLECTION = (
    HERE / ".." / "data-files" / "hydration" / "collections" / "landsat-c2-l1.json"
)


def test_landsat_c2_l1(loader: Loader) -> None:
    """Test that a base item is created when a collection is loaded and that it
    is equal to the item_assets of the collection"""
    with open(LANDSAT_COLLECTION) as f:
        collection = json.load(f)
    loader.load_collections(str(LANDSAT_COLLECTION))

    base_item = cast(
        Dict[str, Any],
        loader.db.query_one(
            "SELECT base_item FROM collections WHERE id=%s;", (collection["id"],)
        ),
    )

    assert type(base_item) == dict
    assert base_item["collection"] == collection["id"]
    assert base_item["assets"] == collection["item_assets"]
