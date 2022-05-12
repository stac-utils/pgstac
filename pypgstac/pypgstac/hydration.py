"""Hydrate data in pypgstac rather than on the database."""
from copy import deepcopy
from typing import Any, Dict

# Marker value to indicate that a key should not be rehydrated
DO_NOT_MERGE_MARKER = "𒍟※"


def hydrate(base_item: Dict[str, Any], item: Dict[str, Any]) -> Dict[str, Any]:
    """Hydrate item in-place with base_item properties.

    This will not perform a deep copy; values of the original item will be referenced
    in the return item.
    """
    # Merge will mutate i, but create deep copies of values in the base item
    # This will prevent the base item values from being mutated, e.g. by
    # filtering out fields in `filter_fields`.
    def merge(b: Dict[str, Any], i: Dict[str, Any]) -> None:
        for key in b:
            if key in i:
                if isinstance(b[key], dict) and isinstance(i.get(key), dict):
                    # Recurse on dicts to merge values
                    merge(b[key], i[key])
                elif isinstance(b[key], list) and isinstance(i.get(key), list):
                    # Merge unequal lists, assume uniform types
                    if len(b[key]) == len(i[key]):
                        for bb, ii in zip(b[key], i[key]):
                            # Make sure we're merging two dicts
                            if isinstance(bb, dict) and isinstance(ii, dict):
                                merge(bb, ii)
                    else:
                        # If item has a different length, then just use the item value
                        continue
                else:
                    # Key exists on item but isn't a dict or list, keep item value
                    if i[key] == DO_NOT_MERGE_MARKER:
                        # Key was marked as do-not-merge, drop it from the item
                        del i[key]
                    else:
                        # Keep the item value
                        continue

            else:
                # Keys in base item that are not in item are simply copied over
                i[key] = deepcopy(b[key])

    merge(base_item, item)
    return item


def dehydrate(base_item: Dict[str, Any], full_item: Dict[str, Any]) -> Dict[str, Any]:
    """
    Get a recursive difference between a base item and an incoming item to dehydrate.

    For keys of dicts within items, if the base item contains a key not present
    in the incoming item, then a do-no-merge value is added indicating that the
    key should not be rehydrated with the corresponding base item value. This will allow
    collection item-assets to contain keys that may not be present on individual items.
    """

    def strip(base_value: Dict[str, Any], item_value: Dict[str, Any]) -> Dict[str, Any]:
        out: dict = {}
        for key, value in item_value.items():
            if base_value is None or key not in base_value:
                # Nothing on base; preserve item value in the dehydrated item
                out[key] = value
                continue

            if base_value[key] == value:
                # Equal values; do not include in the dehydrated item
                continue

            if isinstance(base_value[key], list) and isinstance(value, list):
                if len(base_value[key]) == len(value):
                    # Equal length lists dehydrate dicts at each matching index
                    # and use incoming item values for other types
                    out[key] = []
                    for bv, v in zip(base_value[key], value):
                        if isinstance(bv, dict) and isinstance(v, dict):
                            dehydrated = strip(bv, v)
                            apply_marked_keys(bv, v, dehydrated)
                            out[key].append(dehydrated)
                        else:
                            out[key].append(v)
                else:
                    # Unequal length lists are not dehydrated and just use the
                    # incoming item value
                    out[key] = value
                continue

            if value is None or value == []:
                # Don't keep empty values
                continue

            if isinstance(value, dict):
                # After dehdrating a dict, mark any keys that are present on the
                # base item but not in the incoming item as `do-not-merge` during
                # rehydration
                dehydrated = strip(base_value[key], value)
                apply_marked_keys(base_value[key], value, dehydrated)
                out[key] = dehydrated
                continue
            else:
                # Unequal non-dict values are copied over from the incoming item
                out[key] = value

        # Mark any top-level keys from the base_item that are not in the incoming item
        apply_marked_keys(base_value, item_value, out)

        return out

    return strip(base_item, full_item)


def apply_marked_keys(
    base_item: Dict[str, Any],
    full_item: Dict[str, Any],
    dehydrated: Dict[str, Any],
) -> None:
    """Mark keys.

    Mark any keys that are present on the base item but not in the incoming item
    as `do-not-merge` on the dehydrated item. This will prevent they key from
    being rehydrated.

    This modifies the dehydrated item in-place.
    """
    try:
        marked_keys = [key for key in base_item if key not in full_item.keys()]
        marked_dict = {k: DO_NOT_MERGE_MARKER for k in marked_keys}
        dehydrated.update(marked_dict)
    except TypeError:
        pass
