-- Test STAC datetime:null compliance during hydration
SET ROLE pgstac_ingest;

-- Setup: collection with empty base_item
INSERT INTO collections (content) VALUES ('{"id":"pgstactest-hydration"}');

-- Create item with start/end_datetime but no datetime
SELECT create_item('{
    "id": "temporal-range-item",
    "collection": "pgstactest-hydration",
    "geometry": {"type": "Point", "coordinates": [0, 0]},
    "properties": {
        "start_datetime": "2026-01-01T00:00:00Z",
        "end_datetime": "2026-01-31T23:00:00Z"
    }
}');

-- Verify hydrated item has datetime:null (STAC compliance)
SELECT get_item('temporal-range-item', 'pgstactest-hydration')->'properties'->'datetime';