import csv
from functools import lru_cache
import sys


@lru_cache()
def set_csv_field_size_limit() -> None:
    """Set the field size limit to the maximum allowed.

    Otherwise csv can raise _csv.Error: field larger than field limit (131072)
    """
    field_size_limit = sys.maxsize
    while True:
        try:
            csv.field_size_limit(field_size_limit)
            break
        except OverflowError:
            field_size_limit = int(field_size_limit / 10)
