CREATE TABLE items (
    id text NOT NULL,
    geometry geometry NOT NULL,
    collection text NOT NULL,
    datetime timestamptz NOT NULL,
    end_datetime timestamptz NOT NULL,
    content JSONB NOT NULL,
    private jsonb
)
PARTITION BY LIST (collection)
;

CREATE INDEX "datetime_idx" ON items USING BTREE (datetime DESC, end_datetime ASC);
CREATE INDEX "geometry_idx" ON items USING GIST (geometry);

-- Never written: it carries the reference index of every indexed queryable, the copies of the
-- indexes above and the unique id index among them (see 002a_queryables).
CREATE TABLE IF NOT EXISTS queryable_index_template (LIKE items INCLUDING INDEXES);
CREATE UNIQUE INDEX IF NOT EXISTS queryable_index_template_id_idx ON queryable_index_template (id);

CREATE STATISTICS datetime_stats (dependencies) on datetime, end_datetime from items;

ALTER TABLE items ADD CONSTRAINT items_collections_fk FOREIGN KEY (collection) REFERENCES collections(id) ON DELETE CASCADE DEFERRABLE;

CREATE OR REPLACE FUNCTION partition_after_triggerfunc() RETURNS TRIGGER AS $$
DECLARE
    p text;
    t timestamptz := clock_timestamp();
BEGIN
    RAISE DEBUG 'Updating partition stats %', t;
    -- Ordered: each iteration holds a partition_stats row lock until commit.
    FOR p IN SELECT DISTINCT partition
        FROM newdata n JOIN partition_stats p
        ON (n.collection=p.collection AND n.datetime <@ p.partition_dtrange)
        ORDER BY 1
    LOOP
        PERFORM run_or_queue(format('SELECT update_partition_stats(%L, %L);', p, true));
    END LOOP;
    IF TG_OP IN ('DELETE','UPDATE') THEN
        DELETE FROM format_item_cache c USING newdata n WHERE c.collection = n.collection AND c.id = n.id;
    END IF;
    RAISE DEBUG 't: % %', t, clock_timestamp() - t;
    RETURN NULL;
END;
$$ LANGUAGE PLPGSQL SET SEARCH_PATH TO pgstac, public;

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


CREATE OR REPLACE FUNCTION content_slim(_item jsonb) RETURNS jsonb AS $$
    SELECT (strip_jsonb(_item - '{id,geometry,collection,type,pgstac:base_item}'::text[], b.base_item)
                - '{id,geometry,collection,type}'::text[])
           || jsonb_strip_nulls(jsonb_build_object('pgstac:base_item', b.base_item_id))
    FROM current_base_item(_item->>'collection') b;
$$ LANGUAGE SQL STABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION content_dehydrate(content jsonb) RETURNS items AS $$
    SELECT
            content->>'id' as id,
            stac_geom(content) as geometry,
            content->>'collection' as collection,
            stac_datetime(content) as datetime,
            stac_end_datetime(content) as end_datetime,
            content_slim(content) as content,
            null::jsonb as private
    ;
$$ LANGUAGE SQL STABLE;

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



CREATE OR REPLACE FUNCTION content_hydrate(_item items, fields jsonb DEFAULT '{}'::jsonb) RETURNS jsonb AS $$
DECLARE
    geom jsonb;
    content jsonb;
    base_item jsonb;
    tag text;
BEGIN
    IF include_field('geometry', fields) THEN
        geom := ST_ASGeoJson(_item.geometry, 20)::jsonb;
    END IF;
    -- The tag is validated rather than cast inside a BEGIN ... EXCEPTION block. A block with an
    -- exception handler opens a subtransaction on EVERY call, and this runs once per returned
    -- item; the guard costs a regex instead. Nine digits always fit in an int, so a tag that
    -- passes cannot overflow the cast. A tag that fails falls through to the warning below.
    tag := _item.content->>'pgstac:base_item';
    IF tag IS NULL OR tag ~ '^\s*\d{1,9}\s*$' THEN
        base_item := collection_base_item(_item.collection, tag::int);
    END IF;
    IF base_item IS NULL THEN
        RAISE WARNING 'Item % in collection % is tagged with base item %, which does not exist; hydrating against the current base item.',
            _item.id, _item.collection, tag;
        SELECT c.base_item INTO base_item FROM collections c WHERE c.id = _item.collection;
    END IF;
    content := jsonb_build_object(
        'id', _item.id,
        'geometry', geom,
        'collection', _item.collection,
        'type', 'Feature'
    ) || (_item.content - 'pgstac:base_item');
    RETURN content_hydrate(content, base_item, fields);
END;
$$ LANGUAGE PLPGSQL STABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION content_nonhydrated(
    _item items,
    fields jsonb DEFAULT '{}'::jsonb
) RETURNS jsonb AS $$
DECLARE
    geom jsonb;
    output jsonb;
    base_item jsonb;
    tag text;
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
    -- The base item itself, not the row id it is stored as, and emitted for every item: an
    -- untagged item hydrates against the collection's FIRST base item, so a client seeing no
    -- key would use the current one and get different content than search() returns.
    tag := output->>'pgstac:base_item';
    IF tag IS NULL OR tag ~ '^\s*\d{1,9}\s*$' THEN
        base_item := collection_base_item(_item.collection, tag::int);
    END IF;
    IF base_item IS NULL AND tag IS NOT NULL THEN
        RAISE WARNING 'Item % in collection % is tagged with base item %, which does not exist; returning the current base item.',
            _item.id, _item.collection, tag;
        SELECT c.base_item INTO base_item FROM collections c WHERE c.id = _item.collection;
    END IF;
    output := output || jsonb_build_object('pgstac:base_item', base_item);
    RETURN output;
END;
$$ LANGUAGE PLPGSQL STABLE PARALLEL SAFE;

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
    part text;
    ts timestamptz := clock_timestamp();
    nrows int;
BEGIN
    RAISE DEBUG 'Creating Partitions. %', clock_timestamp() - ts;

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
    -- Ordered: check_partition holds DDL and row locks until commit.
    ) SELECT check_partition(collection, dtrange, edtrange) FROM (
        SELECT * FROM p ORDER BY collection, d
    ) ordered LOOP
        RAISE DEBUG 'Partition %', part;
    END LOOP;

    RAISE DEBUG 'Creating temp table with data to be added. %', clock_timestamp() - ts;
    DROP TABLE IF EXISTS tmpdata;
    -- LATERAL so content_dehydrate runs once per row.
    CREATE TEMP TABLE tmpdata ON COMMIT DROP AS
    SELECT d.* FROM newdata n, LATERAL content_dehydrate(n.content) d;
    GET DIAGNOSTICS nrows = ROW_COUNT;
    RAISE DEBUG 'Added % rows to tmpdata. %', nrows, clock_timestamp() - ts;

    RAISE DEBUG 'Doing the insert. %', clock_timestamp() - ts;
    IF TG_TABLE_NAME = 'items_staging' THEN
        INSERT INTO items
        SELECT * FROM tmpdata;
        GET DIAGNOSTICS nrows = ROW_COUNT;
        RAISE DEBUG 'Inserted % rows to items. %', nrows, clock_timestamp() - ts;
    ELSIF TG_TABLE_NAME = 'items_staging_ignore' THEN
        INSERT INTO items
        SELECT * FROM tmpdata
        ON CONFLICT DO NOTHING;
        GET DIAGNOSTICS nrows = ROW_COUNT;
        RAISE DEBUG 'Inserted % rows to items. %', nrows, clock_timestamp() - ts;
    ELSIF TG_TABLE_NAME = 'items_staging_upsert' THEN
        -- Locked in a fixed order first, so concurrent upserts over an
        -- overlapping id set cannot deadlock. A bare DELETE gives no ordering;
        -- ORDER BY ... FOR UPDATE does, because LockRows sits above the sort.
        WITH locked AS (
            SELECT o.collection, o.id
            FROM tmpdata s
                JOIN items o ON (o.id = s.id AND o.collection = s.collection)
            WHERE o IS DISTINCT FROM s
            ORDER BY o.collection, o.id
            FOR UPDATE OF o
        )
        DELETE FROM items i
        USING locked l
        WHERE i.collection = l.collection AND i.id = l.id
        ;
        GET DIAGNOSTICS nrows = ROW_COUNT;
        RAISE DEBUG 'Deleted % rows from items. %', nrows, clock_timestamp() - ts;
        INSERT INTO items AS t
        SELECT * FROM tmpdata
        ON CONFLICT DO NOTHING;
        GET DIAGNOSTICS nrows = ROW_COUNT;
        RAISE DEBUG 'Inserted % rows to items. %', nrows, clock_timestamp() - ts;
    END IF;

    RAISE DEBUG 'Deleting data from staging table. %', clock_timestamp() - ts;
    EXECUTE format('DELETE FROM %I', TG_TABLE_NAME);
    RAISE DEBUG 'Done. %', clock_timestamp() - ts;

    RETURN NULL;

END;
-- UTC, matching partition_name: the date_trunc here groups rows for check_partition.
$$ LANGUAGE PLPGSQL SET TIME ZONE 'UTC';


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
