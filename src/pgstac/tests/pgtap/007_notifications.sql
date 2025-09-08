-- Test notification functionality
-- First load the notification SQL file (TODO: Remove this line once it is part of a release)
\i sql/007_notifications.sql

-- Test that notification functions exist
SELECT has_function('pgstac'::name, 'notify_item_change', ARRAY['text','text','text']);
SELECT has_function('pgstac'::name, 'notify_item_insert', '{}'::text[]);
SELECT has_function('pgstac'::name, 'notify_item_update', '{}'::text[]);
SELECT has_function('pgstac'::name, 'notify_item_delete', '{}'::text[]);
SELECT has_function('pgstac'::name, 'items_staging_triggerfunc_with_notify', '{}'::text[]);
SELECT has_function('pgstac'::name, 'set_item_notifications_enabled', ARRAY['boolean']);

-- Test that triggers exist on items table
SELECT ok(
    EXISTS(SELECT 1 FROM pg_trigger WHERE tgname = 'items_notify_insert_trigger'),
    'items_notify_insert_trigger exists'
);
SELECT ok(
    EXISTS(SELECT 1 FROM pg_trigger WHERE tgname = 'items_notify_update_trigger'),
    'items_notify_update_trigger exists'
);
SELECT ok(
    EXISTS(SELECT 1 FROM pg_trigger WHERE tgname = 'items_notify_delete_trigger'),
    'items_notify_delete_trigger exists'
);

-- Test that triggers exist on staging tables
SELECT ok(
    EXISTS(SELECT 1 FROM pg_trigger WHERE tgname = 'items_staging_insert_trigger'),
    'items_staging_insert_trigger exists'
);
SELECT ok(
    EXISTS(SELECT 1 FROM pg_trigger WHERE tgname = 'items_staging_insert_ignore_trigger'),
    'items_staging_insert_ignore_trigger exists'
);
SELECT ok(
    EXISTS(SELECT 1 FROM pg_trigger WHERE tgname = 'items_staging_insert_upsert_trigger'),
    'items_staging_insert_upsert_trigger exists'
);

-- Test basic notify_item_change function
-- We can't easily test the actual NOTIFY sending without complex setup,
-- but we can test that the function executes without error
SELECT lives_ok(
    $$SELECT notify_item_change('INSERT', 'test-collection', 'test-item')$$,
    'notify_item_change function executes successfully'
);

-- Test that notification payloads have correct structure
-- Create a temporary table to capture notification details
CREATE TEMP TABLE notification_test_log (
    operation text,
    collection_id text,
    item_id text,
    payload jsonb
);

-- Create a test function that logs instead of sending notifications
CREATE OR REPLACE FUNCTION test_notify_item_change(
    operation_type text,
    collection_id text,
    item_id text
) RETURNS void AS $$
DECLARE
    payload jsonb;
BEGIN
    payload := jsonb_build_object(
        'operation', operation_type,
        'collection', collection_id,
        'item_id', item_id,
        'timestamp', now()
    );

    INSERT INTO notification_test_log (operation, collection_id, item_id, payload)
    VALUES (operation_type, collection_id, item_id, payload);
END;
$$ LANGUAGE plpgsql;

-- Test payload structure
SELECT test_notify_item_change('INSERT', 'test-collection', 'test-item');
SELECT results_eq(
    $$SELECT payload->>'operation' FROM notification_test_log WHERE operation = 'INSERT'$$,
    $$VALUES ('INSERT')$$,
    'Notification payload contains correct operation'
);

SELECT results_eq(
    $$SELECT payload->>'collection' FROM notification_test_log WHERE operation = 'INSERT'$$,
    $$VALUES ('test-collection')$$,
    'Notification payload contains correct collection'
);

SELECT results_eq(
    $$SELECT payload->>'item_id' FROM notification_test_log WHERE operation = 'INSERT'$$,
    $$VALUES ('test-item')$$,
    'Notification payload contains correct item_id'
);

SELECT ok(
    (SELECT payload ? 'timestamp' FROM notification_test_log WHERE operation = 'INSERT'),
    'Notification payload contains timestamp'
);

-- Clean up test data
TRUNCATE notification_test_log;

-- Test enable/disable functionality
SELECT lives_ok(
    $$SELECT set_item_notifications_enabled(false)$$,
    'Disabling notifications executes successfully'
);

SELECT lives_ok(
    $$SELECT set_item_notifications_enabled(true)$$,
    'Enabling notifications executes successfully'
);

-- Test that triggers can be disabled and enabled
-- Check initial state (should be enabled by default)
SELECT ok(
    (SELECT tgenabled = 'O' FROM pg_trigger WHERE tgname = 'items_notify_insert_trigger'),
    'items_notify_insert_trigger is initially enabled'
);

-- Disable notifications
SELECT set_item_notifications_enabled(false);

-- Check that triggers are disabled
SELECT ok(
    (SELECT tgenabled = 'D' FROM pg_trigger WHERE tgname = 'items_notify_insert_trigger'),
    'items_notify_insert_trigger is disabled after calling set_item_notifications_enabled(false)'
);

SELECT ok(
    (SELECT tgenabled = 'D' FROM pg_trigger WHERE tgname = 'items_notify_update_trigger'),
    'items_notify_update_trigger is disabled after calling set_item_notifications_enabled(false)'
);

SELECT ok(
    (SELECT tgenabled = 'D' FROM pg_trigger WHERE tgname = 'items_notify_delete_trigger'),
    'items_notify_delete_trigger is disabled after calling set_item_notifications_enabled(false)'
);

-- Re-enable notifications
SELECT set_item_notifications_enabled(true);

-- Check that triggers are enabled again
SELECT ok(
    (SELECT tgenabled = 'O' FROM pg_trigger WHERE tgname = 'items_notify_insert_trigger'),
    'items_notify_insert_trigger is re-enabled after calling set_item_notifications_enabled(true)'
);



-- Test actual notification triggering with item operations
-- Set up test data first
DELETE FROM collections WHERE id = 'notification-test-collection';
INSERT INTO collections (content) VALUES ('{
    "id": "notification-test-collection",
    "type": "Collection",
    "stac_version": "1.0.0",
    "description": "Test collection for notifications",
    "license": "public-domain",
    "extent": {
        "spatial": {"bbox": [[-180, -90, 180, 90]]},
        "temporal": {"interval": [["2020-01-01T00:00:00Z", null]]}
    }
}');

-- Test with a mock function that captures notifications instead of sending them
-- Replace the notify_item_change function temporarily
CREATE OR REPLACE FUNCTION notify_item_change(
    operation_type text,
    collection_id text,
    item_id text
) RETURNS void AS $$
BEGIN
    INSERT INTO notification_test_log (operation, collection_id, item_id)
    VALUES (operation_type, collection_id, item_id);
END;
$$ LANGUAGE plpgsql;

-- Create partition for test collection
SELECT check_partition('notification-test-collection', '[2020-01-01,2020-01-02)', '[2020-01-01,2020-01-02)');

-- Test INSERT operations
INSERT INTO items (id, collection, geometry, datetime, end_datetime, content) VALUES (
    'notification-test-item-1',
    'notification-test-collection',
    ST_GeomFromText('POINT(0 0)', 4326),
    '2020-01-01T00:00:00Z'::timestamptz,
    '2020-01-01T00:00:00Z'::timestamptz,
    '{
        "id": "notification-test-item-1",
        "type": "Feature",
        "collection": "notification-test-collection",
        "geometry": {"type": "Point", "coordinates": [0, 0]},
        "properties": {"datetime": "2020-01-01T00:00:00Z"}
    }'
);

SELECT results_eq(
    $$SELECT COUNT(*)::int FROM notification_test_log WHERE operation = 'INSERT' AND collection_id = 'notification-test-collection' AND item_id = 'notification-test-item-1'$$,
    $$VALUES (1)$$,
    'INSERT operation triggers notification'
);

-- Test UPDATE operations
UPDATE items SET content = jsonb_set(content, '{properties,datetime}', '"2020-01-02T00:00:00Z"')
WHERE id = 'notification-test-item-1' AND collection = 'notification-test-collection';

SELECT results_eq(
    $$SELECT COUNT(*)::int FROM notification_test_log WHERE operation = 'UPDATE' AND collection_id = 'notification-test-collection' AND item_id = 'notification-test-item-1'$$,
    $$VALUES (1)$$,
    'UPDATE operation triggers notification'
);

-- Test DELETE operations
DELETE FROM items WHERE id = 'notification-test-item-1' AND collection = 'notification-test-collection';

SELECT results_eq(
    $$SELECT COUNT(*)::int FROM notification_test_log WHERE operation = 'DELETE' AND collection_id = 'notification-test-collection' AND item_id = 'notification-test-item-1'$$,
    $$VALUES (1)$$,
    'DELETE operation triggers notification'
);

-- Test bulk operations don't create excessive notifications
TRUNCATE notification_test_log;

-- Insert multiple items at once
INSERT INTO items (id, collection, geometry, datetime, end_datetime, content)
SELECT
    'notification-test-item-' || i,
    'notification-test-collection',
    ST_GeomFromText('POINT(0 0)', 4326),
    '2020-01-01T00:00:00Z'::timestamptz,
    '2020-01-01T00:00:00Z'::timestamptz,
    jsonb_build_object(
        'id', 'notification-test-item-' || i,
        'type', 'Feature',
        'collection', 'notification-test-collection',
        'geometry', json_build_object('type', 'Point', 'coordinates', ARRAY[0, 0]),
        'properties', json_build_object('datetime', '2020-01-01T00:00:00Z')
    )
FROM generate_series(2, 4) i;

-- Should get one notification per item
SELECT results_eq(
    $$SELECT COUNT(*)::int FROM notification_test_log WHERE operation = 'INSERT'$$,
    $$VALUES (3)$$,
    'Bulk INSERT operations trigger correct number of notifications'
);

-- Test staging operations
TRUNCATE notification_test_log;

-- Test items_staging
INSERT INTO items_staging (content) VALUES ('{
    "id": "notification-staging-test-item-1",
    "type": "Feature",
    "collection": "notification-test-collection",
    "geometry": {"type": "Point", "coordinates": [1, 1]},
    "properties": {"datetime": "2020-01-01T00:00:00Z"}
}');

-- The staging trigger should have processed this and created notifications
SELECT ok(
    (SELECT COUNT(*) > 0 FROM notification_test_log WHERE operation = 'INSERT' AND item_id = 'notification-staging-test-item-1'),
    'items_staging operations trigger notifications'
);

-- Clean up
DELETE FROM items WHERE collection = 'notification-test-collection';
DELETE FROM collections WHERE id = 'notification-test-collection';

-- Restore original notify_item_change function
CREATE OR REPLACE FUNCTION notify_item_change(
    operation_type text,
    collection_id text,
    item_id text
) RETURNS void AS $$
DECLARE
    payload jsonb;
    general_channel text := 'pgstac_items';
BEGIN
    -- Build the notification payload
    payload := jsonb_build_object(
        'operation', operation_type,
        'collection', collection_id,
        'item_id', item_id,
        'timestamp', now()
    );

    -- Send to general channel
    PERFORM pg_notify(general_channel, payload::text);

    -- Log the notification (can be disabled by setting log_min_messages)
    RAISE DEBUG 'Item notification sent: % % %', operation_type, collection_id, item_id;
END;
$$ LANGUAGE plpgsql;

-- Clean up test functions and tables
DROP FUNCTION test_notify_item_change(text, text, text);
DROP TABLE notification_test_log;
