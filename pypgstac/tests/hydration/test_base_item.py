import json
from pathlib import Path

from pypgstac.load import Loader


HERE = Path(__file__).parent
LANDSAT_COLLECTION = (
    HERE / ".." / "data-files" / "hydration" / "collections" / "landsat-8-c2-l1.json"
)


def test_lansat_c2_l1(loader: Loader) -> None:
    with open(LANDSAT_COLLECTION) as f:
        collection = json.load(f)
    loader.load_collections(str(LANDSAT_COLLECTION))

    base_item = loader.db.query_one("SELECT base_item FROM collections WHERE ;")

    assert base_item["collection"] == collection["id"]
