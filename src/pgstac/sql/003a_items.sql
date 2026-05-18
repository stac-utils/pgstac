CREATE TABLE items (
    id text NOT NULL,
    geometry geometry NOT NULL,
    collection text NOT NULL,
    datetime timestamptz NOT NULL,
    end_datetime timestamptz NOT NULL,
    pgstac_updated_at timestamptz NOT NULL DEFAULT now(),
    content_hash text NOT NULL DEFAULT '',
    content JSONB NOT NULL,
    private jsonb
)
PARTITION BY LIST (collection)
;

CREATE TABLE IF NOT EXISTS items_deleted_log (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    item_id text NOT NULL,
    collection text NOT NULL,
    partition text,
    datetime timestamptz,
    end_datetime timestamptz,
    content_hash text NOT NULL DEFAULT '',
    deleted_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS items_deleted_log_deleted_at_idx ON items_deleted_log (deleted_at);

CREATE INDEX "datetime_idx" ON items USING BTREE (datetime DESC, end_datetime ASC);
CREATE INDEX "geometry_idx" ON items USING GIST (geometry);

CREATE STATISTICS datetime_stats (dependencies) on datetime, end_datetime from items;

ALTER TABLE items ADD CONSTRAINT items_collections_fk FOREIGN KEY (collection) REFERENCES collections(id) ON DELETE CASCADE DEFERRABLE;

CREATE OR REPLACE FUNCTION partition_after_triggerfunc() RETURNS TRIGGER AS $$
DECLARE
    p text;
    t timestamptz := clock_timestamp();
BEGIN
    RAISE NOTICE 'Updating partition stats %', t;
    FOR p IN SELECT DISTINCT partition
        FROM newdata n JOIN partition_sys_meta p
        ON (n.collection=p.collection AND n.datetime <@ p.partition_dtrange)
    LOOP
        PERFORM run_or_queue(format('SELECT update_partition_stats(%L, %L);', p, true));
    END LOOP;
    IF TG_OP IN ('DELETE','UPDATE') THEN
        DELETE FROM format_item_cache c USING newdata n WHERE c.collection = n.collection AND c.id = n.id;
    END IF;
    RAISE NOTICE 't: % %', t, clock_timestamp() - t;
    RETURN NULL;
END;
$$ LANGUAGE PLPGSQL SECURITY DEFINER;

CREATE TRIGGER items_after_insert_trigger
AFTER INSERT ON items
REFERENCING NEW TABLE AS newdata
FOR EACH STATEMENT
EXECUTE FUNCTION partition_after_triggerfunc();

CREATE TRIGGER items_after_update_trigger
AFTER DELETE ON items
REFERENCING OLD TABLE AS newdata
FOR EACH STATEMENT
EXECUTE FUNCTION partition_after_triggerfunc();

CREATE TRIGGER items_after_delete_trigger
AFTER UPDATE ON items
REFERENCING NEW TABLE AS newdata
FOR EACH STATEMENT
EXECUTE FUNCTION partition_after_triggerfunc();

CREATE OR REPLACE FUNCTION items_touch_triggerfunc() RETURNS TRIGGER AS $$
BEGIN
    NEW.pgstac_updated_at := now();
    NEW.content_hash := encode(sha256(content_hydrate(NEW)::text::bytea), 'hex');
    RETURN NEW;
END;
$$ LANGUAGE PLPGSQL SECURITY DEFINER;

DROP TRIGGER IF EXISTS items_before_upsert_trigger ON items;
DROP TRIGGER IF EXISTS items_before_update_trigger ON items;
CREATE TRIGGER items_before_update_trigger
BEFORE UPDATE ON items
FOR EACH ROW
EXECUTE FUNCTION items_touch_triggerfunc();

CREATE OR REPLACE FUNCTION items_delete_log_trigger() RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO items_deleted_log (
        item_id,
        collection,
        partition,
        datetime,
        end_datetime,
        content_hash
    )
    SELECT
        old_rows.id,
        old_rows.collection,
        (partition_name(old_rows.collection, old_rows.datetime)).partition_name,
        old_rows.datetime,
        old_rows.end_datetime,
        old_rows.content_hash
    FROM old_rows;

    RETURN NULL;
END;
$$ LANGUAGE PLPGSQL SECURITY DEFINER;

DROP TRIGGER IF EXISTS items_delete_log_after_delete_trigger ON items;
CREATE TRIGGER items_delete_log_after_delete_trigger
    AFTER DELETE ON items
    REFERENCING OLD TABLE AS old_rows
    FOR EACH STATEMENT EXECUTE FUNCTION items_delete_log_trigger();


CREATE OR REPLACE FUNCTION content_dehydrate(content jsonb) RETURNS items AS $$
DECLARE
    out items;
BEGIN
    out.id := content->>'id';
    out.geometry := stac_geom(content);
    out.collection := content->>'collection';
    out.datetime := stac_datetime(content);
    out.end_datetime := stac_end_datetime(content);
    out.pgstac_updated_at := now();
    out.content_hash := encode(sha256(content::text::bytea), 'hex');
    out.content := strip_jsonb(
        content - '{id,geometry,collection,type}'::text[],
        collection_base_item(content->>'collection')
    ) - '{id,geometry,collection,type}'::text[];
    out.private := null;
    RETURN out;
END;
$$ LANGUAGE PLPGSQL STABLE;

CREATE OR REPLACE FUNCTION include_field(f text, fields jsonb DEFAULT '{}'::jsonb) RETURNS boolean AS $$
DECLARE
    includes jsonb := fields->'include';
    excludes jsonb := fields->'exclude';
BEGIN
    IF f IS NULL THEN
        RETURN NULL;
    END IF;


    IF
        jsonb_typeof(excludes) = 'array'
        AND jsonb_array_length(excludes)>0
        AND excludes ? f
    THEN
        RETURN FALSE;
    END IF;

    IF
        (
            jsonb_typeof(includes) = 'array'
            AND jsonb_array_length(includes) > 0
            AND includes ? f
        ) OR
        (
            includes IS NULL
            OR jsonb_typeof(includes) = 'null'
            OR jsonb_array_length(includes) = 0
        )
    THEN
        RETURN TRUE;
    END IF;

    RETURN FALSE;
END;
$$ LANGUAGE PLPGSQL IMMUTABLE;

DROP FUNCTION IF EXISTS content_hydrate(jsonb, jsonb, jsonb);
CREATE OR REPLACE FUNCTION content_hydrate(
    _item jsonb,
    _base_item jsonb,
    fields jsonb DEFAULT '{}'::jsonb
) RETURNS jsonb AS $$
    SELECT merge_jsonb(
            jsonb_fields(_item, fields),
            jsonb_fields(_base_item, fields)
    );
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;



CREATE OR REPLACE FUNCTION content_hydrate(_item items, _collection collections, fields jsonb DEFAULT '{}'::jsonb) RETURNS jsonb AS $$
DECLARE
    geom jsonb;
    bbox jsonb;
    output jsonb;
    content jsonb;
    base_item jsonb := _collection.base_item;
BEGIN
    IF include_field('geometry', fields) THEN
        geom := ST_ASGeoJson(_item.geometry, 20)::jsonb;
    END IF;
    output := content_hydrate(
        jsonb_build_object(
            'id', _item.id,
            'geometry', geom,
            'collection', _item.collection,
            'type', 'Feature'
        ) || _item.content,
        _collection.base_item,
        fields
    );

    RETURN output;
END;
$$ LANGUAGE PLPGSQL STABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION content_nonhydrated(
    _item items,
    fields jsonb DEFAULT '{}'::jsonb
) RETURNS jsonb AS $$
DECLARE
    geom jsonb;
    bbox jsonb;
    output jsonb;
BEGIN
    IF include_field('geometry', fields) THEN
        geom := ST_ASGeoJson(_item.geometry, 20)::jsonb;
    END IF;
    output := jsonb_build_object(
                'id', _item.id,
                'geometry', geom,
                'collection', _item.collection,
                'type', 'Feature'
            ) || _item.content;
    RETURN output;
END;
$$ LANGUAGE PLPGSQL STABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION content_hydrate(_item items, fields jsonb DEFAULT '{}'::jsonb) RETURNS jsonb AS $$
    SELECT content_hydrate(
        _item,
        (SELECT c FROM collections c WHERE id=_item.collection LIMIT 1),
        fields
    );
$$ LANGUAGE SQL STABLE;


CREATE UNLOGGED TABLE items_staging (
    content JSONB NOT NULL
);
CREATE UNLOGGED TABLE items_staging_ignore (
    content JSONB NOT NULL
);
CREATE UNLOGGED TABLE items_staging_upsert (
    content JSONB NOT NULL
);

CREATE OR REPLACE FUNCTION items_staging_triggerfunc() RETURNS TRIGGER AS $$
DECLARE
    p record;
    _partitions text[];
    part text;
    ts timestamptz := clock_timestamp();
    nrows int;
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
    ELSIF TG_TABLE_NAME = 'items_staging_ignore' THEN
        INSERT INTO items
        SELECT * FROM tmpdata
        ON CONFLICT DO NOTHING;
        GET DIAGNOSTICS nrows = ROW_COUNT;
        RAISE NOTICE 'Inserted % rows to items. %', nrows, clock_timestamp() - ts;
    ELSIF TG_TABLE_NAME = 'items_staging_upsert' THEN
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
    END IF;

    RAISE NOTICE 'Deleting data from staging table. %', clock_timestamp() - ts;
    DELETE FROM items_staging;
    RAISE NOTICE 'Done. %', clock_timestamp() - ts;

    RETURN NULL;

END;
$$ LANGUAGE PLPGSQL;


CREATE TRIGGER items_staging_insert_trigger AFTER INSERT ON items_staging REFERENCING NEW TABLE AS newdata
    FOR EACH STATEMENT EXECUTE PROCEDURE items_staging_triggerfunc();

CREATE TRIGGER items_staging_insert_ignore_trigger AFTER INSERT ON items_staging_ignore REFERENCING NEW TABLE AS newdata
    FOR EACH STATEMENT EXECUTE PROCEDURE items_staging_triggerfunc();

CREATE TRIGGER items_staging_insert_upsert_trigger AFTER INSERT ON items_staging_upsert REFERENCING NEW TABLE AS newdata
    FOR EACH STATEMENT EXECUTE PROCEDURE items_staging_triggerfunc();


CREATE OR REPLACE FUNCTION item_by_id(_id text, _collection text DEFAULT NULL) RETURNS items AS
$$
DECLARE
    i items%ROWTYPE;
BEGIN
    SELECT * INTO i FROM items WHERE id=_id AND (_collection IS NULL OR collection=_collection) LIMIT 1;
    RETURN i;
END;
$$ LANGUAGE PLPGSQL STABLE SET SEARCH_PATH TO pgstac, public;

CREATE OR REPLACE FUNCTION get_item(_id text, _collection text DEFAULT NULL) RETURNS jsonb AS $$
    SELECT content_hydrate(items) FROM items WHERE id=_id AND (_collection IS NULL OR collection=_collection);
$$ LANGUAGE SQL STABLE SET SEARCH_PATH TO pgstac, public;

CREATE OR REPLACE FUNCTION delete_item(_id text, _collection text DEFAULT NULL) RETURNS VOID AS $$
DECLARE
out items%ROWTYPE;
BEGIN
    DELETE FROM items WHERE id = _id AND (_collection IS NULL OR collection=_collection) RETURNING * INTO STRICT out;
END;
$$ LANGUAGE PLPGSQL;

--/*
CREATE OR REPLACE FUNCTION create_item(data jsonb) RETURNS VOID AS $$
    INSERT INTO items_staging (content) VALUES (data);
$$ LANGUAGE SQL SET SEARCH_PATH TO pgstac,public;


CREATE OR REPLACE FUNCTION update_item(content jsonb) RETURNS VOID AS $$
DECLARE
    old items %ROWTYPE;
    out items%ROWTYPE;
BEGIN
    PERFORM delete_item(content->>'id', content->>'collection');
    PERFORM create_item(content);
END;
$$ LANGUAGE PLPGSQL SET SEARCH_PATH TO pgstac,public;

CREATE OR REPLACE FUNCTION upsert_item(data jsonb) RETURNS VOID AS $$
    INSERT INTO items_staging_upsert (content) VALUES (data);
$$ LANGUAGE SQL SET SEARCH_PATH TO pgstac,public;

CREATE OR REPLACE FUNCTION create_items(data jsonb) RETURNS VOID AS $$
    INSERT INTO items_staging (content)
    SELECT * FROM jsonb_array_elements(data);
$$ LANGUAGE SQL SET SEARCH_PATH TO pgstac,public;

CREATE OR REPLACE FUNCTION upsert_items(data jsonb) RETURNS VOID AS $$
    INSERT INTO items_staging_upsert (content)
    SELECT * FROM jsonb_array_elements(data);
$$ LANGUAGE SQL SET SEARCH_PATH TO pgstac,public;


CREATE OR REPLACE FUNCTION collection_bbox(id text) RETURNS jsonb AS $$
    SELECT (replace(replace(replace(st_extent(geometry)::text,'BOX(','[['),')',']]'),' ',','))::jsonb
    FROM items WHERE collection=$1;
    ;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE SET SEARCH_PATH TO pgstac, public;

CREATE OR REPLACE FUNCTION collection_temporal_extent(id text) RETURNS jsonb AS $$
    SELECT to_jsonb(array[array[min(datetime), max(datetime)]])
    FROM items WHERE collection=$1;
;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE SET SEARCH_PATH TO pgstac, public;

CREATE OR REPLACE FUNCTION update_collection_extents() RETURNS VOID AS $$
UPDATE collections
    SET content = jsonb_set_lax(
        content,
        '{extent}'::text[],
        collection_extent(id, FALSE),
        true,
        'use_json_null'
    )
;
$$ LANGUAGE SQL;
