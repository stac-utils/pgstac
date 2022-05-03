import json
from pathlib import Path
from typing import Any, Dict, cast

from pypgstac import hydration
from pypgstac.hydration import DO_NOT_MERGE_MARKER
from pypgstac.load import Loader


HERE = Path(__file__).parent
LANDSAT_COLLECTION = (
    HERE / ".." / "data-files" / "hydration" / "collections" / "landsat-c2-l1.json"
)
LANDSAT_DEHYDRATED_ITEM = (
    HERE
    / ".."
    / "data-files"
    / "hydration"
    / "dehydrated-items"
    / "landsat-c2-l1"
    / "LM04_L1GS_001001_19830527_02_T2.json"
)

LANDSAT_ITEM = (
    HERE
    / ".."
    / "data-files"
    / "hydration"
    / "raw-items"
    / "landsat-c2-l1"
    / "LM04_L1GS_001001_19830527_02_T2.json"
)


def test_landsat_c2_l1(loader: Loader) -> None:
    """Test that a dehydrated item is is equal to the raw item it was dehydrated
    from, against the base item of the collection"""
    with open(LANDSAT_COLLECTION) as f:
        collection = json.load(f)
    loader.load_collections(str(LANDSAT_COLLECTION))

    with open(LANDSAT_DEHYDRATED_ITEM) as f:
        dehydrated = json.load(f)

    with open(LANDSAT_ITEM) as f:
        raw_item = json.load(f)

    base_item = cast(
        Dict[str, Any],
        loader.db.query_one(
            "SELECT base_item FROM collections WHERE id=%s;", (collection["id"],)
        ),
    )

    assert type(base_item) == dict

    hydrated = hydration.hydrate(base_item, dehydrated)
    assert hydrated == raw_item


def test_full_hydrate() -> None:
    base_item = {"a": "first", "b": "second", "c": "third"}
    dehydrated: Dict[str, Any] = {}

    rehydrated = hydration.hydrate(base_item, dehydrated)
    assert rehydrated == base_item


def test_full_nested() -> None:
    base_item = {"a": "first", "b": "second", "c": {"d": "third"}}
    dehydrated: Dict[str, Any] = {}

    rehydrated = hydration.hydrate(base_item, dehydrated)
    assert rehydrated == base_item


def test_nested_extra_keys() -> None:
    """
    Test that items having nested dicts with keys not in base item preserve
    the additional keys in the dehydrated item.
    """
    base_item = {"a": "first", "b": "second", "c": {"d": "third"}}
    dehydrated = {"c": {"e": "fourth", "f": "fifth"}}
    hydrated = hydration.hydrate(base_item, dehydrated)

    assert hydrated == {
        "a": "first",
        "b": "second",
        "c": {"d": "third", "e": "fourth", "f": "fifth"},
    }


def test_list_of_dicts_extra_keys() -> None:
    """Test that an equal length list of dicts is hydrated correctly"""
    base_item = {"a": [{"b1": 1, "b2": 2}, {"c1": 1, "c2": 2}]}
    dehydrated = {"a": [{"b3": 3}, {"c3": 3}]}

    hydrated = hydration.hydrate(base_item, dehydrated)
    assert hydrated == {"a": [{"b1": 1, "b2": 2, "b3": 3}, {"c1": 1, "c2": 2, "c3": 3}]}


def test_equal_len_list_of_mixed_types() -> None:
    """
    Test that a list of equal length containing matched types at each index dehydrates
    dicts and preserves item-values of other types.
    """
    base_item = {"a": [{"b1": 1, "b2": 2}, "foo", {"c1": 1, "c2": 2}, "bar"]}
    dehydrated = {"a": [{"b3": 3}, "far", {"c3": 3}, "boo"]}

    hydrated = hydration.hydrate(base_item, dehydrated)
    assert hydrated == {
        "a": [{"b1": 1, "b2": 2, "b3": 3}, "far", {"c1": 1, "c2": 2, "c3": 3}, "boo"]
    }


def test_unequal_len_list() -> None:
    """Test that unequal length lists preserve the item value exactly"""
    base_item = {"a": [{"b1": 1}, {"c1": 1}, {"d1": 1}]}
    dehydrated = {"a": [{"b1": 1, "b2": 2}, {"c1": 1, "c2": 2}]}

    hydrated = hydration.hydrate(base_item, dehydrated)
    assert hydrated == dehydrated


def test_marked_non_merged_fields() -> None:
    base_item = {"a": "first", "b": "second", "c": {"d": "third", "e": "fourth"}}
    dehydrated = {"c": {"e": DO_NOT_MERGE_MARKER, "f": "fifth"}}

    hydrated = hydration.hydrate(base_item, dehydrated)
    assert hydrated == {
        "a": "first",
        "b": "second",
        "c": {"d": "third", "f": "fifth"},
    }


def test_marked_non_merged_fields_in_list() -> None:
    base_item = {"a": [{"b": "first", "d": "third"}, {"c": "second", "e": "fourth"}]}
    dehydrated = {
        "a": [
            {"d": DO_NOT_MERGE_MARKER},
            {"e": DO_NOT_MERGE_MARKER, "f": "fifth"},
        ]
    }

    hydrated = hydration.hydrate(base_item, dehydrated)
    assert hydrated == {"a": [{"b": "first"}, {"c": "second", "f": "fifth"}]}


def test_deeply_nested_dict() -> None:
    base_item = {"a": {"b": {"c": {"d": "first", "d1": "second"}}}}
    dehydrated = {"a": {"b": {"c": {"d2": "third"}}}}

    hydrated = hydration.hydrate(base_item, dehydrated)
    assert hydrated == {
        "a": {"b": {"c": {"d": "first", "d1": "second", "d2": "third"}}}
    }


def test_equal_list_of_non_dicts() -> None:
    """Values of lists that match base_item should be dehydrated off"""
    base_item = {"assets": {"thumbnail": {"roles": ["thumbnail"]}}}
    dehydrated = {"assets": {"thumbnail": {"href": "http://foo.com"}}}

    hydrated = hydration.hydrate(base_item, dehydrated)
    assert hydrated == {
        "assets": {"thumbnail": {"roles": ["thumbnail"], "href": "http://foo.com"}}
    }
