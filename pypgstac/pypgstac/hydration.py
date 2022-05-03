from copy import deepcopy
from typing import Any, Dict


def hydrate(item: Dict[str, Any], base_item: Dict[str, Any]) -> Dict[str, Any]:
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
                        # If item has a different length,
                        # then just use the item value
                        pass
                else:
                    # Key exists on item but isn't a dict or list, keep item value
                    pass

            else:
                # Keys in base item that are not in item are simply copied over
                i[key] = deepcopy(b[key])

    merge(base_item, item)
    return item
