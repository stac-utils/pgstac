import json
import uuid
from datetime import datetime, timezone
from math import ceil
from typing import Any, Dict, Generator, Tuple

import morecantile
import psycopg
import pytest

from pypgstac.load import Loader, Methods

XMIN, YMIN = 0, 0
AOI_WIDTH = 50
AOI_HEIGHT = 50


ITEM_WIDTHS = [0.5, 0.75, 1, 1.5, 2, 3, 4, 5, 6, 8, 10]
TMS = morecantile.tms.get("WebMercatorQuad")


def generate_items(
    item_size: Tuple[float, float],
    collection_id: str,
) -> Generator[Dict[str, Any], None, None]:
    item_width, item_height = item_size

    cols = ceil(AOI_WIDTH / item_width)
    rows = ceil(AOI_HEIGHT / item_height)

    # generate an item for each grid cell
    for row in range(rows):
        for col in range(cols):
            left = XMIN + (col * item_width)
            bottom = YMIN + (row * item_height)
            right = left + item_width
            top = bottom + item_height

            yield {
                "type": "Feature",
                "stac_version": "1.0.0",
                "id": str(uuid.uuid4()),
                "collection": collection_id,
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [left, bottom],
                            [right, bottom],
                            [right, top],
                            [left, top],
                            [left, bottom],
                        ],
                    ],
                },
                "bbox": [left, bottom, right, top],
                "properties": {
                    "datetime": datetime.now(timezone.utc).isoformat(),
                },
            }


@pytest.fixture(scope="function")
def search_hashes(loader: Loader) -> Dict[float, str]:
    search_hashes = {}
    for item_width in ITEM_WIDTHS:
        collection_id = f"collection-{str(item_width)}"
        collection = {
            "type": "Collection",
            "id": collection_id,
            "stac_version": "1.0.0",
            "description": f"Minimal test collection {collection_id}",
            "license": "proprietary",
            "extent": {
                "spatial": {
                    "bbox": [XMIN, YMIN, XMIN + AOI_WIDTH, YMIN + AOI_HEIGHT],
                },
                "temporal": {
                    "interval": [[datetime.now(timezone.utc).isoformat(), None]],
                },
            },
        }

        loader.load_collections(
            [collection],
            insert_mode=Methods.insert,
        )
        loader.load_items(
            generate_items((item_width, item_width), collection_id),
            insert_mode=Methods.insert,
        )

        with psycopg.connect(autocommit=True) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT * FROM search_query(%s);",
                    (json.dumps({"collections": [collection_id]}),),
                )
                res = cursor.fetchone()
                assert res
                search_hashes[item_width] = res[0]

    return search_hashes


@pytest.mark.benchmark(
    group="xyzsearch",
    min_rounds=3,
    warmup=True,
    warmup_iterations=2,
)
@pytest.mark.parametrize("item_width", ITEM_WIDTHS)
@pytest.mark.parametrize("zoom", range(3, 8 + 1))
def test1(
    benchmark,
    search_hashes: Dict[float, str],
    item_width: float,
    zoom: int,
) -> None:
    # get a tile from the center of the full AOI
    xmid = XMIN + AOI_WIDTH / 2
    ymid = YMIN + AOI_HEIGHT / 2
    tiles = TMS.tiles(xmid, ymid, xmid + 1, ymid + 1, [zoom])
    tile = next(tiles)

    def xyzsearch_test():
        with psycopg.connect(autocommit=True) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT * FROM xyzsearch(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);",
                    (
                        tile.x,
                        tile.y,
                        tile.z,
                        search_hashes[item_width],
                        json.dumps(
                            {
                                "include": ["assets", "id", "bbox", "collection"],
                            },
                        ),  # fields
                        100000,  # scan_limit,
                        100000,  # items limit
                        "5 seconds",
                        True,  # exitwhenfull
                        True,  # skipcovered
                    ),
                )
                _ = cursor.fetchone()[0]

    _ = benchmark(xyzsearch_test)
