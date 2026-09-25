"""Tests for base item versioning through the loader."""

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest

from pypgstac.hydration import hydrate
from pypgstac.load import Loader, Methods

HERE = Path(__file__).parent
COLLECTION_FILE = (
    HERE / "data-files" / "hydration" / "collections" / "sentinel-1-grd.json"
)
ITEM_FILE = (
    HERE
    / "data-files"
    / "hydration"
    / "raw-items"
    / "sentinel-1-grd"
    / "S1A_IW_GRDH_1SDV_20220428T034417_20220428T034442_042968_05213C.json"
)
COLLECTION_ID = "sentinel-1-grd"
TAG = "pgstac:base_item"


def collection_json() -> Dict[str, Any]:
    """Read the test collection."""
    with open(COLLECTION_FILE) as f:
        return json.load(f)


def edited_collection_json() -> Dict[str, Any]:
    """Read the test collection with an edited item_assets."""
    collection = collection_json()
    # A key the raw item does not have, to detect do-not-merge marking.
    collection["item_assets"]["vv"]["gsd"] = 10
    collection["item_assets"]["vv"]["title"] = "VV, edited"
    return collection


def item_json(item_id: str) -> Dict[str, Any]:
    """Read the test item with a given id."""
    with open(ITEM_FILE) as f:
        item = json.load(f)
    item["id"] = item_id
    return item


def stored_content(loader: Loader, item_id: str) -> Dict[str, Any]:
    """Return the dehydrated content stored for an item."""
    return next(  # type: ignore[no-any-return]
        loader.db.query(
            "SELECT content FROM items WHERE id=%s AND collection=%s;",
            [item_id, COLLECTION_ID],
        ),
    )[0]


def get_item(loader: Loader, item_id: str) -> Dict[str, Any]:
    """Return the item hydrated by pgstac."""
    return next(  # type: ignore[no-any-return]
        loader.db.query(
            "SELECT get_item(%s, %s);",
            [item_id, COLLECTION_ID],
        ),
    )[0]


def base_item_ids(loader: Loader) -> List[int]:
    """Return the base item ids recorded for the test collection."""
    return [
        row[0]
        for row in loader.db.query(
            "SELECT id FROM base_items WHERE collection=%s ORDER BY id ASC;",
            [COLLECTION_ID],
        )
    ]


def base_item(loader: Loader, base_item_id: Optional[int]) -> Dict[str, Any]:
    """Return a base item by tag, as an API hydrating in python would."""
    if base_item_id is None:
        row = next(
            loader.db.query("SELECT collection_base_item(%s);", [COLLECTION_ID]),
        )
    else:
        row = next(
            loader.db.query(
                "SELECT collection_base_item(%s, %s);",
                [COLLECTION_ID, base_item_id],
            ),
        )
    return row[0]  # type: ignore[no-any-return]


def nohydrate_features(loader: Loader, ids: List[str]) -> Dict[str, Dict[str, Any]]:
    """Return non hydrated features keyed by item id."""
    res = next(
        loader.db.func(
            "search",
            {
                "ids": ids,
                "collections": [COLLECTION_ID],
                "conf": {"nohydrate": True},
            },
        ),
    )[0]
    return {f["id"]: f for f in res["features"]}


def load_scenario(loader: Loader, insert_mode: Methods) -> None:
    """Load an item, edit the collection, then load a second item."""
    loader.load_collections([collection_json()], insert_mode=Methods.ignore)
    loader.load_items([item_json("item-a")], insert_mode=insert_mode)
    loader.load_collections([edited_collection_json()], insert_mode=Methods.upsert)
    loader.load_items([item_json("item-b")], insert_mode=insert_mode)


@pytest.mark.parametrize(
    "insert_mode",
    [Methods.insert, Methods.upsert, Methods.ignore],
)
def test_collection_edit_does_not_change_existing_items(
    loader: Loader,
    insert_mode: Methods,
) -> None:
    """Items keep hydrating against the base item they were written against."""
    loader.load_collections([collection_json()], insert_mode=Methods.ignore)
    loader.load_items([item_json("item-a")], insert_mode=insert_mode)
    before = get_item(loader, "item-a")

    loader.load_collections([edited_collection_json()], insert_mode=Methods.upsert)
    loader.load_items([item_json("item-b")], insert_mode=insert_mode)

    after = get_item(loader, "item-a")
    assert after == before
    assert "gsd" not in after["assets"]["vv"]

    # Both items come from the same raw item, so both hydrate back to it.
    item_b = get_item(loader, "item-b")
    del after["id"]
    del item_b["id"]
    assert after == item_b


def test_items_tagged_with_the_base_item_they_were_written_against(
    loader: Loader,
) -> None:
    """Only items written after the edit carry a tag, naming the latest row."""
    load_scenario(loader, Methods.insert)

    ids = base_item_ids(loader)
    assert len(ids) == 2

    assert TAG not in stored_content(loader, "item-a")
    assert stored_content(loader, "item-b")[TAG] == ids[1]

    # The untagged lookup returns the base item item-a was written against.
    assert base_item(loader, None) == base_item(loader, ids[0])
    assert base_item(loader, ids[1])["assets"]["vv"]["gsd"] == 10
    assert "gsd" not in base_item(loader, None)["assets"]["vv"]


def test_python_hydration_of_nohydrate_matches_sql(loader: Loader) -> None:
    """The base item travels with the item, so the client needs no second lookup."""
    load_scenario(loader, Methods.insert)

    features = nohydrate_features(loader, ["item-a", "item-b"])
    ids = base_item_ids(loader)
    # Every item carries the base item it was dehydrated against, not a row id:
    # the first one for an item loaded before any edit, the current one for an
    # item loaded after. They differ, so an implementation that handed back the
    # collection's current base item would fail here.
    assert features["item-a"][TAG] == base_item(loader, ids[0])
    assert features["item-b"][TAG] == base_item(loader, ids[1])
    assert features["item-a"][TAG] != features["item-b"][TAG]

    for item_id, feature in features.items():
        carried = feature.pop(TAG)
        base = {k: v for k, v in carried.items() if v is not None}
        assert hydrate(base, feature) == get_item(loader, item_id)


def test_dehydrated_roundtrip_keeps_tag(loader: Loader, tmp_path: Path) -> None:
    """A dehydrated export reloaded into the same database keeps its tag."""
    load_scenario(loader, Methods.insert)
    before = {i: get_item(loader, i) for i in ("item-a", "item-b")}

    export = tmp_path / "dehydrated.txt"
    conn = loader.db.connect()
    with conn.cursor() as cur:
        with cur.copy(
            """
            COPY (
                SELECT id, geometry, collection, datetime, end_datetime, content
                FROM items ORDER BY id
            ) TO stdout;
            """,
        ) as copy:
            export.write_bytes(b"".join(copy))

    loader.load_items(str(export), insert_mode=Methods.upsert, dehydrated=True)

    assert TAG not in stored_content(loader, "item-a")
    assert stored_content(loader, "item-b")[TAG] == base_item_ids(loader)[1]
    assert {i: get_item(loader, i) for i in ("item-a", "item-b")} == before


def test_missing_base_item_hydrates_against_current(loader: Loader) -> None:
    """A tag that names no row warns and hydrates against the current base item."""
    load_scenario(loader, Methods.insert)
    # item-b is tagged with the current base item, so this is what hydrating
    # against collections.base_item yields.
    expected = get_item(loader, "item-b")

    conn = loader.db.connect()
    conn.execute(
        "UPDATE items SET content = content || %s::jsonb WHERE id=%s AND collection=%s",
        [json.dumps({TAG: 999999}), "item-b", COLLECTION_ID],
    )

    # A Diagnostic is only readable inside the handler, so copy what is needed.
    notices: List[str] = []
    conn.add_notice_handler(
        lambda n: notices.append(f"{n.severity_nonlocalized}: {n.message_primary}"),
    )
    assert get_item(loader, "item-b") == expected
    assert any(n.startswith("WARNING:") and "item-b" in n for n in notices)


def test_malformed_base_item_tag_warns(loader: Loader) -> None:
    """A tag that is not a number degrades the row instead of aborting the page."""
    load_scenario(loader, Methods.insert)
    expected = get_item(loader, "item-b")

    conn = loader.db.connect()
    conn.execute(
        "UPDATE items SET content = content || %s::jsonb WHERE id=%s AND collection=%s",
        [json.dumps({TAG: "notanint"}), "item-b", COLLECTION_ID],
    )

    notices: List[str] = []
    conn.add_notice_handler(
        lambda n: notices.append(f"{n.severity_nonlocalized}: {n.message_primary}"),
    )
    # The whole query used to abort on the cast; only this row should be affected.
    assert get_item(loader, "item-b") == expected
    assert any(n.startswith("WARNING:") and "item-b" in n for n in notices)


def test_nohydrate_warns_on_a_tag_that_names_no_row(loader: Loader) -> None:
    """The nohydrate path reports a dangling tag the same way hydration does."""
    load_scenario(loader, Methods.insert)

    conn = loader.db.connect()
    conn.execute(
        "UPDATE items SET content = content || %s::jsonb WHERE id=%s AND collection=%s",
        [json.dumps({TAG: 999999}), "item-b", COLLECTION_ID],
    )

    notices: List[str] = []
    conn.add_notice_handler(
        lambda n: notices.append(f"{n.severity_nonlocalized}: {n.message_primary}"),
    )
    features = nohydrate_features(loader, ["item-b"])
    # It still carries a base item, the collection's current one, and says so.
    assert features["item-b"][TAG] is not None
    assert any(n.startswith("WARNING:") and "item-b" in n for n in notices)


def test_loader_cache_cleared_when_collections_are_loaded(loader: Loader) -> None:
    """A long lived loader does not tag against rows of a deleted collection."""
    load_scenario(loader, Methods.insert)
    assert len(base_item_ids(loader)) == 2

    loader.db.query_one("SELECT delete_collection(%s);", [COLLECTION_ID])
    assert base_item_ids(loader) == []

    # The recreated collection has no base item rows.
    loader.load_collections([collection_json()], insert_mode=Methods.insert)
    loader.load_items([item_json("item-c")], insert_mode=Methods.insert)

    assert TAG not in stored_content(loader, "item-c")
    assert "gsd" not in get_item(loader, "item-c")["assets"]["vv"]
