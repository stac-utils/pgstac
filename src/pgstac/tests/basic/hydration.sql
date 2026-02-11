-- Test that JSON null values survive the dehydrate/hydrate round-trip
SET ROLE pgstac_ingest;

-- Setup: collection with empty base_item
INSERT INTO collections (content) VALUES ('{"id":"pgstactest-hydration"}');

-- Create item with explicit datetime:null (STAC-compliant temporal range)
SELECT create_item('{
    "id": "temporal-range-item",
    "collection": "pgstactest-hydration",
    "geometry": {"type": "Point", "coordinates": [0, 0]},
    "properties": {
        "datetime": null,
        "start_datetime": "2026-01-01T00:00:00Z",
        "end_datetime": "2026-01-31T23:00:00Z"
    }
}');

-- Verify datetime:null is preserved in stored content
SELECT content->'properties'->'datetime' FROM items WHERE id='temporal-range-item';

-- Verify datetime:null is preserved after hydration
SELECT get_item('temporal-range-item', 'pgstactest-hydration')->'properties'->'datetime';
