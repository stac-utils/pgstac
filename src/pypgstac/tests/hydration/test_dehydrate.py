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
LANDSAT_ITEM = (
    HERE
    / ".."
    / "data-files"
    / "hydration"
    / "raw-items"
    / "landsat-c2-l1"
    / "LM04_L1GS_001001_19830527_02_T2.json"
)


class TestDehydrate:
    def dehydrate(
        self, base_item: Dict[str, Any], item: Dict[str, Any]
    ) -> Dict[str, Any]:
        return hydration.dehydrate(base_item, item)

    def test_landsat_c2_l1(self, loader: Loader) -> None:
        """
        Test that a dehydrated item is created properly from a raw item against a
        base item from a collection
        """
        with open(LANDSAT_COLLECTION) as f:
            collection = json.load(f)
        loader.load_collections(str(LANDSAT_COLLECTION))

        with open(LANDSAT_ITEM) as f:
            item = json.load(f)

        base_item = cast(
            Dict[str, Any],
            loader.db.query_one(
                "SELECT base_item FROM collections WHERE id=%s;", (collection["id"],)
            ),
        )

        assert type(base_item) == dict

        dehydrated = self.dehydrate(base_item, item)

        # Expect certain keys on base and not on dehydrated
        only_base_keys = ["type", "collection", "stac_version"]
        assert all(k in base_item for k in only_base_keys)
        assert not any(k in dehydrated for k in only_base_keys)

        # Expect certain keys on dehydrated and not on base
        only_dehydrated_keys = ["id", "bbox", "geometry", "properties"]
        assert not any(k in base_item for k in only_dehydrated_keys)
        assert all(k in dehydrated for k in only_dehydrated_keys)

        # Properties, links should be exactly the same pre- and post-dehydration
        assert item["properties"] == dehydrated["properties"]
        assert item["links"] == dehydrated["links"]

        # Check specific assets are dehydrated correctly
        thumbnail = dehydrated["assets"]["thumbnail"]
        assert list(thumbnail.keys()) == ["href"]
        assert thumbnail["href"] == item["assets"]["thumbnail"]["href"]

        # Red asset raster bands have additional `scale` and `offset` keys
        red = dehydrated["assets"]["red"]
        assert list(red.keys()) == ["href", "eo:bands", "raster:bands"]
        assert len(red["raster:bands"]) == 1
        assert list(red["raster:bands"][0].keys()) == ["scale", "offset"]
        item_red_rb = item["assets"]["red"]["raster:bands"][0]
        assert red["raster:bands"] == [
            {"scale": item_red_rb["scale"], "offset": item_red_rb["offset"]}
        ]

        # nir09 asset raster bands does not have a `unit` attribute, which is
        # present on base
        nir09 = dehydrated["assets"]["nir09"]
        assert list(nir09.keys()) == ["href", "eo:bands", "raster:bands"]
        assert len(nir09["raster:bands"]) == 1
        assert list(nir09["raster:bands"][0].keys()) == ["unit"]
        assert nir09["raster:bands"] == [{"unit": DO_NOT_MERGE_MARKER}]

    def test_single_depth_equals(self) -> None:
        base_item = {"a": "first", "b": "second", "c": "third"}
        item = {"a": "first", "b": "second", "c": "third"}
        dehydrated = self.dehydrate(base_item, item)
        assert dehydrated == {}

    def test_nested_equals(self) -> None:
        base_item = {"a": "first", "b": "second", "c": {"d": "third"}}
        item = {"a": "first", "b": "second", "c": {"d": "third"}}
        dehydrated = self.dehydrate(base_item, item)
        assert dehydrated == {}

    def test_nested_extra_keys(self) -> None:
        """
        Test that items having nested dicts with keys not in base item preserve
        the additional keys in the dehydrated item.
        """
        base_item = {"a": "first", "b": "second", "c": {"d": "third"}}
        item = {
            "a": "first",
            "b": "second",
            "c": {"d": "third", "e": "fourth", "f": "fifth"},
        }
        dehydrated = self.dehydrate(base_item, item)
        assert dehydrated == {"c": {"e": "fourth", "f": "fifth"}}

    def test_list_of_dicts_extra_keys(self) -> None:
        """Test that an equal length list of dicts is dehydrated correctly"""
        base_item = {"a": [{"b1": 1, "b2": 2}, {"c1": 1, "c2": 2}]}
        item = {"a": [{"b1": 1, "b2": 2, "b3": 3}, {"c1": 1, "c2": 2, "c3": 3}]}

        dehydrated = self.dehydrate(base_item, item)
        assert "a" in dehydrated
        assert dehydrated["a"] == [{"b3": 3}, {"c3": 3}]

    def test_equal_len_list_of_mixed_types(self) -> None:
        """
        Test that a list of equal length containing matched
        types at each index dehydrates
        dicts and preserves item-values of other types.
        """
        base_item = {"a": [{"b1": 1, "b2": 2}, "foo", {"c1": 1, "c2": 2}, "bar"]}
        item = {
            "a": [
                {"b1": 1, "b2": 2, "b3": 3},
                "far",
                {"c1": 1, "c2": 2, "c3": 3},
                "boo",
            ]
        }

        dehydrated = self.dehydrate(base_item, item)
        assert "a" in dehydrated
        assert dehydrated["a"] == [{"b3": 3}, "far", {"c3": 3}, "boo"]

    def test_unequal_len_list(self) -> None:
        """Test that unequal length lists preserve the item value exactly"""
        base_item = {"a": [{"b1": 1}, {"c1": 1}, {"d1": 1}]}
        item = {"a": [{"b1": 1, "b2": 2}, {"c1": 1, "c2": 2}]}

        dehydrated = self.dehydrate(base_item, item)
        assert "a" in dehydrated
        assert dehydrated["a"] == item["a"]

    def test_marked_non_merged_fields(self) -> None:
        base_item = {"a": "first", "b": "second", "c": {"d": "third", "e": "fourth"}}
        item = {
            "a": "first",
            "b": "second",
            "c": {"d": "third", "f": "fifth"},
        }
        dehydrated = self.dehydrate(base_item, item)
        assert dehydrated == {"c": {"e": DO_NOT_MERGE_MARKER, "f": "fifth"}}

    def test_marked_non_merged_fields_in_list(self) -> None:
        base_item = {
            "a": [{"b": "first", "d": "third"}, {"c": "second", "e": "fourth"}]
        }
        item = {"a": [{"b": "first"}, {"c": "second", "f": "fifth"}]}

        dehydrated = self.dehydrate(base_item, item)
        assert dehydrated == {
            "a": [
                {"d": DO_NOT_MERGE_MARKER},
                {"e": DO_NOT_MERGE_MARKER, "f": "fifth"},
            ]
        }

    def test_deeply_nested_dict(self) -> None:
        base_item = {"a": {"b": {"c": {"d": "first", "d1": "second"}}}}
        item = {"a": {"b": {"c": {"d": "first", "d1": "second", "d2": "third"}}}}

        dehydrated = self.dehydrate(base_item, item)
        assert dehydrated == {"a": {"b": {"c": {"d2": "third"}}}}

    def test_equal_list_of_non_dicts(self) -> None:
        """Values of lists that match base_item should be dehydrated off"""
        base_item = {"assets": {"thumbnail": {"roles": ["thumbnail"]}}}
        item = {
            "assets": {"thumbnail": {"roles": ["thumbnail"], "href": "http://foo.com"}}
        }

        dehydrated = self.dehydrate(base_item, item)
        assert dehydrated == {"assets": {"thumbnail": {"href": "http://foo.com"}}}

    def test_invalid_assets_marked(self) -> None:
        """
        Assets can be included on item-assets that are not uniformly included on
        individual items. Ensure that base item asset keys without a matching item
        key are marked do-no-merge after dehydration.
        """
        base_item = {
            "type": "Feature",
            "assets": {
                "asset1": {"name": "Asset one"},
                "asset2": {"name": "Asset two"},
            },
        }
        hydrated = {
            "assets": {"asset1": {"name": "Asset one", "href": "http://foo.com"}}
        }

        dehydrated = self.dehydrate(base_item, hydrated)

        assert dehydrated == {
            "type": DO_NOT_MERGE_MARKER,
            "assets": {
                "asset1": {"href": "http://foo.com"},
                "asset2": DO_NOT_MERGE_MARKER,
            },
        }

    def test_top_level_base_keys_marked(self) -> None:
        """
        Top level keys on the base item not present on the incoming item should
        be marked as do not merge, no matter the nesting level.
        """
        base_item = {
            "single": "Feature",
            "double": {"nested": "value"},
            "triple": {"nested": {"deep": "value"}},
            "included": "value",
        }
        hydrated = {"included": "value", "unique": "value"}

        dehydrated = self.dehydrate(base_item, hydrated)

        assert dehydrated == {
            "single": DO_NOT_MERGE_MARKER,
            "double": DO_NOT_MERGE_MARKER,
            "triple": DO_NOT_MERGE_MARKER,
            "unique": "value",
        }
