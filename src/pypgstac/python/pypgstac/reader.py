"""Utilities to read data in from json, ndjson, parquet, or as dicts/pystac items."""

from collections import deque
from typing import (
    Iterable,
    Union,
)

import orjson
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pystac
from ciso8601 import parse_rfc3339
from orjson import JSONDecodeError
from pyarrow import json as pajson
from smart_open import open


# Minimal Schema for pulling in stats from a stac-geoparquet layout
# (properties at root level)
pq_stats_schema = pa.schema(
    [
        pa.field("collection", pa.string()),
        pa.field("datetime", pa.timestamp("us", tz="UTC")),
        pa.field("start_datetime", pa.timestamp("us", tz="UTC")),
        pa.field("end_datetime", pa.timestamp("us", tz="UTC")),
    ],
)

# Minimal Schema for pulling in stats from a stac json layout
# (nested properties)
stac_stats_schema = pa.schema(
    [
        pa.field(
            "properties",
            pa.struct(
                [
                    pa.field("datetime", pa.timestamp("us", tz="UTC")),
                    pa.field("start_datetime", pa.timestamp("us", tz="UTC")),
                    pa.field("end_datetime", pa.timestamp("us", tz="UTC")),
                ],
            ),
        ),
        pa.field("collection", pa.string()),
    ],
)


# Reorganize table to have single start/end datetime
# (when using datetime, start/end will be the same)
def arrow_stats(table):
    """
        Reorganize table to have single start/end datetime
        (when using datetime, start/end will be the same)
    """
    table = table.append_column(
        "start_month",
        pc.floor_temporal(
            pc.coalesce(table["datetime"], table["start_datetime"]),
            unit="month",
        ),
    )
    table = table.append_column(
        "start",
        pc.coalesce(table["datetime"], table["start_datetime"]),
    )
    table = table.append_column(
        "end",
        pc.coalesce(table["datetime"], table["end_datetime"]),
    )

    stats = table.group_by(("collection", "start_month")).aggregate(
        [
            ("start", "min"),
            ("start", "max"),
            ("end", "min"),
            ("end", "max"),
            ([], "count_all"),
        ],
    )

    return stats



def pq_stats(file):
    """Calculate stats from a parquet file."""
    pqdataset = ds.dataset(file)
    base_schema = pq_stats_schema
    schema = pa.unify_schemas([pqdataset.schema, base_schema])
    pqdataset = pqdataset.replace_schema(schema)

    table = pqdataset.to_table(
        ["collection", "datetime", "start_datetime", "end_datetime"],
    )
    return arrow_stats(table)


def ndjson_stats(file):
    """Calculate stats from an ndjson file."""
    schema = stac_stats_schema
    table = (
        pajson.read_json(
            file,
            parse_options=pajson.ParseOptions(
                explicit_schema=schema,
                unexpected_field_behavior="ignore",
            ),
        )
        .flatten()
        .rename_columns(["datetime", "start_datetime", "end_datetime", "collection"])
    )

    return arrow_stats(table)


def items_stats(items: Iterable[Union[pystac.item.Item, dict]]):
    """Calculate stats from an iterable of stac items."""
    collections = []
    datetimes = []
    start_datetimes = []
    end_datetimes = []

    def get_ts(item, prop):
        v = item["properties"].get(prop, None)
        if v:
            return parse_rfc3339(v)
        return None

    for item in items:
        if isinstance(item, dict):
            collections.append(item["collection"])
            datetimes.append(get_ts(item, "datetime"))
            start_datetimes.append(get_ts(item, "start_datetime"))
            end_datetimes.append(get_ts(item, "end_datetime"))
        else:
            collections.append(item.collection_id)
            datetimes.append(item.datetime)
            start_datetimes.append(item.properties.get("start_datetime", None))
            end_datetimes.append(item.properties.get("end_datetime", None))

    table = pa.table(
        [collections, datetimes, start_datetimes, end_datetimes], schema=pq_stats_schema,
    )
    return arrow_stats(table)


def isfile(file):
    """Try to determine if input string is a file or a json string."""
    if not isinstance(file, str):
        return False
    if any((file.startswith("{"), file.startswith("["))):
        return False
    try:
        with open(file):
            return True
    except ValueError:
        return False


def iter_arrow(table, batch_size: int = 1000):
    """Use arrow batches to iterate through an arrow table as dict rows."""
    for batch in table.to_batches(batch_size=batch_size):
        for row in batch.to_pylist():
            yield row


class Reader:
    """Utilities for loading data."""

    def __init__(self, input):
        self.input = input
        self.path = None
        self.type = None
        self.iter = None
        self.file_type = None
        self._check_input()

    def read_json(self):
        """Read a json or ndjson file."""
        with open(self.input) as f:
            try:
                # read ndjson
                row = orjson.loads(f.readline().strip())
                self.type = row["type"].lower()
                self.file_type = "ndjson"
                f.seek(0)
                for line in f:
                    yield orjson.loads(line.strip())
            except JSONDecodeError:
                try:
                    f.seek(0)
                    orjson.loads(f.readline().strip())
                    self.type = row["type"].lower()
                    self.file_type = "ndjson"
                    f.seek(0)
                    for line in f:
                        # Data dumped as json using pg copy
                        # gets munged with extra back slashes
                        yield orjson.loads(
                            line.strip().replace("\\\\", "\\").replace("\\\\", "\\"),
                        )
                except JSONDecodeError:
                    f.seek(0)
                    # read full json file that
                    json = orjson.loads(f.read())
                    self.file_type = "json"
                    if isinstance(json, list):
                        self.type = json[0]["type"]
                        for record in json:
                            yield record
                    else:
                        self.type = json["type"].lower()
                        if self.type == "featurecollection":
                            self.type = "item"
                            for record in json["features"]:
                                yield record
                        elif self.type == "itemcollection":
                            self.type = "item"
                            for record in json["items"]:
                                yield record
                        else:
                            yield json

    def _check_input(self):
        input = self.input
        if isfile(input):
            if input.endswith(".parquet"):
                self.file_type = "parquet"
                self.stats = pq_stats(input)
                self.arrow = pq.read_table(input)
                self.iter = iter_arrow(self.arrow)
            else:
                self.iter = deque(self.read_json(), 0)
                if self.type == "feature" and self.file_type == "ndjson":
                    self.stats = ndjson_stats(input)
                else:
                    self.stats = items_stats(self.iter)
