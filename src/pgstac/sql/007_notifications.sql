-- LISTEN/NOTIFY functionality for pgstac
-- Provides real-time notifications for item operations (INSERT, UPDATE, DELETE)

-- Channel names:
-- - pgstac_items: All item operations across all collections

-- Notification payload format (JSON):
-- {
--   "operation": "INSERT|UPDATE|DELETE",
--   "collection": "collection_id",
--   "item_id": "item_id",
--   "timestamp": "2023-01-01T00:00:00Z"
-- }

-- Function to send item notifications
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

-- Trigger function for item INSERT operations
CREATE OR REPLACE FUNCTION notify_item_insert() RETURNS trigger AS $$
BEGIN
    -- For row-level triggers
    IF TG_LEVEL = 'ROW' THEN
        PERFORM notify_item_change('INSERT', NEW.collection, NEW.id);
        RETURN NEW;
    END IF;

    -- For statement-level triggers with NEW TABLE
    IF TG_LEVEL = 'STATEMENT' THEN
        -- Send notification for each inserted row
        PERFORM notify_item_change('INSERT', collection, id)
        FROM newdata;
        RETURN NULL;
    END IF;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Trigger function for item UPDATE operations
CREATE OR REPLACE FUNCTION notify_item_update() RETURNS trigger AS $$
BEGIN
    -- For row-level triggers
    IF TG_LEVEL = 'ROW' THEN
        PERFORM notify_item_change('UPDATE', NEW.collection, NEW.id);
        RETURN NEW;
    END IF;

    -- For statement-level triggers with NEW TABLE
    IF TG_LEVEL = 'STATEMENT' THEN
        -- Send notification for each updated row
        PERFORM notify_item_change('UPDATE', collection, id)
        FROM newdata;
        RETURN NULL;
    END IF;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Trigger function for item DELETE operations
CREATE OR REPLACE FUNCTION notify_item_delete() RETURNS trigger AS $$
BEGIN
    -- For row-level triggers
    IF TG_LEVEL = 'ROW' THEN
        PERFORM notify_item_change('DELETE', OLD.collection, OLD.id);
        RETURN OLD;
    END IF;

    -- For statement-level triggers with OLD TABLE
    IF TG_LEVEL = 'STATEMENT' THEN
        -- Send notification for each deleted row
        PERFORM notify_item_change('DELETE', collection, id)
        FROM olddata;
        RETURN NULL;
    END IF;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Enhanced staging trigger function that includes notifications
CREATE OR REPLACE FUNCTION items_staging_triggerfunc_with_notify() RETURNS TRIGGER AS $$
DECLARE
    p record;
    _partitions text[];
    part text;
    ts timestamptz := clock_timestamp();
    nrows int;
    operation_type text;
BEGIN
    RAISE NOTICE 'Creating Partitions. %', clock_timestamp() - ts;

    FOR part IN WITH t AS (
        SELECT
            n.content->>'collection' as collection,
            stac_daterange(n.content->'properties') as dtr,
            partition_trunc
        FROM newdata n JOIN collections ON (n.content->>'collection'=collections.id)
    ), p AS (
        SELECT
            collection,
            COALESCE(date_trunc(partition_trunc::text, lower(dtr)),'-infinity') as d,
            tstzrange(min(lower(dtr)),max(lower(dtr)),'[]') as dtrange,
            tstzrange(min(upper(dtr)),max(upper(dtr)),'[]') as edtrange
        FROM t
        GROUP BY 1,2
    ) SELECT check_partition(collection, dtrange, edtrange) FROM p LOOP
        RAISE NOTICE 'Partition %', part;
    END LOOP;

    RAISE NOTICE 'Creating temp table with data to be added. %', clock_timestamp() - ts;
    DROP TABLE IF EXISTS tmpdata;
    CREATE TEMP TABLE tmpdata ON COMMIT DROP AS
    SELECT
        (content_dehydrate(content)).*
    FROM newdata;
    GET DIAGNOSTICS nrows = ROW_COUNT;
    RAISE NOTICE 'Added % rows to tmpdata. %', nrows, clock_timestamp() - ts;

    RAISE NOTICE 'Doing the insert. %', clock_timestamp() - ts;
    IF TG_TABLE_NAME = 'items_staging' THEN
        INSERT INTO items
        SELECT * FROM tmpdata;
        GET DIAGNOSTICS nrows = ROW_COUNT;
        RAISE NOTICE 'Inserted % rows to items. %', nrows, clock_timestamp() - ts;

        -- Send INSERT notifications
        PERFORM notify_item_change('INSERT', collection, id) FROM tmpdata;

    ELSIF TG_TABLE_NAME = 'items_staging_ignore' THEN
        INSERT INTO items
        SELECT * FROM tmpdata
        ON CONFLICT DO NOTHING;
        GET DIAGNOSTICS nrows = ROW_COUNT;
        RAISE NOTICE 'Inserted % rows to items. %', nrows, clock_timestamp() - ts;

        -- Send INSERT notifications (only for actually inserted items)
        -- Note: This is approximate since ON CONFLICT DO NOTHING doesn't report which rows were inserted
        PERFORM notify_item_change('INSERT', collection, id) FROM tmpdata;

    ELSIF TG_TABLE_NAME = 'items_staging_upsert' THEN
        -- Handle deletes first (these are updates)
        PERFORM notify_item_change('UPDATE', i.collection, i.id)
        FROM items i
        INNER JOIN tmpdata s ON (i.id = s.id AND i.collection = s.collection AND i IS DISTINCT FROM s);

        DELETE FROM items i USING tmpdata s
            WHERE
                i.id = s.id
                AND i.collection = s.collection
                AND i IS DISTINCT FROM s
        ;
        GET DIAGNOSTICS nrows = ROW_COUNT;
        RAISE NOTICE 'Deleted % rows from items. %', nrows, clock_timestamp() - ts;

        INSERT INTO items AS t
        SELECT * FROM tmpdata
        ON CONFLICT DO NOTHING;
        GET DIAGNOSTICS nrows = ROW_COUNT;
        RAISE NOTICE 'Inserted % rows to items. %', nrows, clock_timestamp() - ts;

        -- Send INSERT notifications for new items
        -- Note: This includes both new inserts and re-inserts after updates
        PERFORM notify_item_change('INSERT', collection, id) FROM tmpdata;
    END IF;

    RAISE NOTICE 'Deleting data from staging table. %', clock_timestamp() - ts;
    DELETE FROM items_staging;
    RAISE NOTICE 'Done. %', clock_timestamp() - ts;

    RETURN NULL;

END;
$$ LANGUAGE PLPGSQL;

-- Drop existing staging triggers and create new ones with notifications
DROP TRIGGER IF EXISTS items_staging_insert_trigger ON items_staging;
DROP TRIGGER IF EXISTS items_staging_insert_ignore_trigger ON items_staging_ignore;
DROP TRIGGER IF EXISTS items_staging_insert_upsert_trigger ON items_staging_upsert;

CREATE TRIGGER items_staging_insert_trigger
    AFTER INSERT ON items_staging
    REFERENCING NEW TABLE AS newdata
    FOR EACH STATEMENT
    EXECUTE FUNCTION items_staging_triggerfunc_with_notify();

CREATE TRIGGER items_staging_insert_ignore_trigger
    AFTER INSERT ON items_staging_ignore
    REFERENCING NEW TABLE AS newdata
    FOR EACH STATEMENT
    EXECUTE FUNCTION items_staging_triggerfunc_with_notify();

CREATE TRIGGER items_staging_insert_upsert_trigger
    AFTER INSERT ON items_staging_upsert
    REFERENCING NEW TABLE AS newdata
    FOR EACH STATEMENT
    EXECUTE FUNCTION items_staging_triggerfunc_with_notify();

-- Create triggers for direct operations on items table
-- These handle cases where items are inserted/updated/deleted directly (not through staging)

CREATE TRIGGER items_notify_insert_trigger
    AFTER INSERT ON items
    REFERENCING NEW TABLE AS newdata
    FOR EACH STATEMENT
    EXECUTE FUNCTION notify_item_insert();

CREATE TRIGGER items_notify_update_trigger
    AFTER UPDATE ON items
    REFERENCING NEW TABLE AS newdata
    FOR EACH STATEMENT
    EXECUTE FUNCTION notify_item_update();

CREATE TRIGGER items_notify_delete_trigger
    AFTER DELETE ON items
    REFERENCING OLD TABLE AS olddata
    FOR EACH STATEMENT
    EXECUTE FUNCTION notify_item_delete();

-- Function to enable/disable notifications
CREATE OR REPLACE FUNCTION set_item_notifications_enabled(enabled boolean) RETURNS void AS $$
BEGIN
    IF enabled THEN
        -- Enable triggers
        ALTER TABLE items ENABLE TRIGGER items_notify_insert_trigger;
        ALTER TABLE items ENABLE TRIGGER items_notify_update_trigger;
        ALTER TABLE items ENABLE TRIGGER items_notify_delete_trigger;

        ALTER TABLE items_staging ENABLE TRIGGER items_staging_insert_trigger;
        ALTER TABLE items_staging_ignore ENABLE TRIGGER items_staging_insert_ignore_trigger;
        ALTER TABLE items_staging_upsert ENABLE TRIGGER items_staging_insert_upsert_trigger;

        RAISE NOTICE 'Item notifications enabled';
    ELSE
        -- Disable triggers
        ALTER TABLE items DISABLE TRIGGER items_notify_insert_trigger;
        ALTER TABLE items DISABLE TRIGGER items_notify_update_trigger;
        ALTER TABLE items DISABLE TRIGGER items_notify_delete_trigger;

        ALTER TABLE items_staging DISABLE TRIGGER items_staging_insert_trigger;
        ALTER TABLE items_staging_ignore DISABLE TRIGGER items_staging_insert_ignore_trigger;
        ALTER TABLE items_staging_upsert DISABLE TRIGGER items_staging_insert_upsert_trigger;

        RAISE NOTICE 'Item notifications disabled';
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Example usage and documentation
COMMENT ON FUNCTION notify_item_change(text, text, text) IS
'Sends NOTIFY messages for item operations. Used internally by triggers.';

COMMENT ON FUNCTION set_item_notifications_enabled(boolean) IS
'Enable or disable item change notifications. Use set_item_notifications_enabled(false) to temporarily disable notifications during bulk operations.';

-- By default, notifications are enabled
-- To disable: SELECT set_item_notifications_enabled(false);
-- To enable:  SELECT set_item_notifications_enabled(true);
