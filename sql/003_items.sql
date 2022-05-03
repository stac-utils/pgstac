CREATE TABLE items (
    id text NOT NULL,
    geometry geometry NOT NULL,
    collection text NOT NULL,
    datetime timestamptz NOT NULL,
    end_datetime timestamptz NOT NULL,
    content JSONB NOT NULL
)
PARTITION BY LIST (collection)
;

CREATE INDEX "datetime_idx" ON items USING BTREE (datetime DESC, end_datetime ASC);
CREATE INDEX "geometry_idx" ON items USING GIST (geometry);

CREATE STATISTICS datetime_stats (dependencies) on datetime, end_datetime from items;


ALTER TABLE items ADD CONSTRAINT items_collections_fk FOREIGN KEY (collection) REFERENCES collections(id) ON DELETE CASCADE DEFERRABLE;


CREATE OR REPLACE FUNCTION content_slim(_item jsonb, _collection jsonb) RETURNS jsonb AS $$
    SELECT
        jsonb_object_agg(
            key,
            CASE
                WHEN
                    jsonb_typeof(c.value) = 'object'
                    AND
                    jsonb_typeof(i.value) = 'object'
                THEN content_slim(i.value, c.value)
                ELSE i.value
            END
        )
    FROM
        jsonb_each(_item) as i
    LEFT JOIN
        jsonb_each(_collection) as c
    USING (key)
    WHERE
        i.value IS DISTINCT FROM c.value
    ;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION content_slim(_item jsonb) RETURNS jsonb AS $$
    SELECT content_slim(_item - '{id,type,collection,geometry,bbox}'::text[], collection_base_item(_item->>'collection'));
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION content_dehydrate(content jsonb) RETURNS items AS $$
    SELECT
            content->>'id' as id,
            stac_geom(content) as geometry,
            content->>'collection' as collection,
            stac_datetime(content) as datetime,
            stac_end_datetime(content) as end_datetime,
            content_slim(content) as content
    ;
$$ LANGUAGE SQL STABLE;


CREATE OR REPLACE FUNCTION include_field(f text, fields jsonb DEFAULT '{}'::jsonb) RETURNS boolean AS $$
DECLARE
    includes jsonb := coalesce(fields->'includes', fields->'include', '[]'::jsonb);
    excludes jsonb := coalesce(fields->'excludes', fields->'exclude', '[]'::jsonb);
BEGIN
    IF f IS NULL THEN
        RETURN NULL;
    ELSIF jsonb_array_length(includes)>0 AND includes ? f THEN
        RETURN TRUE;
    ELSIF jsonb_array_length(excludes)>0 AND excludes ? f THEN
        RETURN FALSE;
    ELSIF jsonb_array_length(includes)>0 AND NOT includes ? f THEN
        RETURN FALSE;
    END IF;
    RETURN TRUE;
END;
$$ LANGUAGE PLPGSQL IMMUTABLE;


CREATE OR REPLACE FUNCTION key_filter(IN k text, IN val jsonb, INOUT kf jsonb, OUT include boolean) AS $$
DECLARE
    includes jsonb := coalesce(kf->'includes', kf->'include', '[]'::jsonb);
    excludes jsonb := coalesce(kf->'excludes', kf->'exclude', '[]'::jsonb);
BEGIN
    RAISE NOTICE '% % %', k, val, kf;

    include := TRUE;
    IF k = 'properties' AND NOT excludes ? 'properties' THEN
        excludes := excludes || '["properties"]';
        include := TRUE;
        RAISE NOTICE 'Prop include %', include;
    ELSIF
        jsonb_array_length(excludes)>0 AND excludes ? k THEN
        include := FALSE;
    ELSIF
        jsonb_array_length(includes)>0 AND NOT includes ? k THEN
        include := FALSE;
    ELSIF
        jsonb_array_length(includes)>0 AND includes ? k THEN
        includes := '[]'::jsonb;
        RAISE NOTICE 'KF: %', kf;
    END IF;
    kf := jsonb_build_object('includes', includes, 'excludes', excludes);
    RETURN;
END;
$$ LANGUAGE PLPGSQL IMMUTABLE PARALLEL SAFE;


CREATE OR REPLACE FUNCTION strip_assets(a jsonb) RETURNS jsonb AS $$
    WITH t AS (SELECT * FROM jsonb_each(a))
    SELECT jsonb_object_agg(key, value) FROM t
    WHERE value ? 'href';
$$ LANGUAGE SQL IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION content_hydrate(
    _item jsonb,
    _collection jsonb,
    fields jsonb DEFAULT '{}'::jsonb
) RETURNS jsonb AS $$
    SELECT
        jsonb_strip_nulls(jsonb_object_agg(
            key,
            CASE
                WHEN key = 'properties' AND include_field('properties', fields) THEN
                    i.value
                WHEN key = 'properties' THEN
                    content_hydrate(i.value, c.value, kf)
                WHEN
                    c.value IS NULL AND key != 'properties'
                THEN i.value
                WHEN
                    key = 'assets'
                    AND
                    jsonb_typeof(c.value) = 'object'
                    AND
                    jsonb_typeof(i.value) = 'object'
                THEN strip_assets(content_hydrate(i.value, c.value, kf))
                WHEN
                    jsonb_typeof(c.value) = 'object'
                    AND
                    jsonb_typeof(i.value) = 'object'
                THEN content_hydrate(i.value, c.value, kf)
                ELSE coalesce(i.value, c.value)
            END
        ))
    FROM
        jsonb_each(coalesce(_item,'{}'::jsonb)) as i
    FULL JOIN
        jsonb_each(coalesce(_collection,'{}'::jsonb)) as c
    USING (key)
    JOIN LATERAL (
        SELECT kf, include FROM key_filter(key, i.value, fields)
    ) as k ON (include)
    ;
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
        geom := ST_ASGeoJson(_item.geometry)::jsonb;
    END IF;
    IF include_field('bbox', fields) THEN
        bbox := geom_bbox(_item.geometry)::jsonb;
    END IF;
    output := content_hydrate(
            jsonb_build_object(
                'id', _item.id,
                'geometry', geom,
                'bbox',bbox,
                'collection', _item.collection
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
        geom := ST_ASGeoJson(_item.geometry)::jsonb;
    END IF;
    IF include_field('bbox', fields) THEN
        bbox := geom_bbox(_item.geometry)::jsonb;
    END IF;
    output := jsonb_build_object(
                'id', _item.id,
                'geometry', geom,
                'bbox',bbox,
                'collection', _item.collection
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
    ts timestamptz := clock_timestamp();
BEGIN
    RAISE NOTICE 'Creating Partitions. %', clock_timestamp() - ts;
    WITH ranges AS (
        SELECT
            n.content->>'collection' as collection,
            stac_daterange(n.content->'properties') as dtr
        FROM newdata n
    ), p AS (
        SELECT
            collection,
            lower(dtr) as datetime,
            upper(dtr) as end_datetime,
            (partition_name(
                collection,
                lower(dtr)
            )).partition_name as name
        FROM ranges
    )
    INSERT INTO partitions (collection, datetime_range, end_datetime_range)
        SELECT
            collection,
            tstzrange(min(datetime), max(datetime), '[]') as datetime_range,
            tstzrange(min(end_datetime), max(end_datetime), '[]') as end_datetime_range
        FROM p
            GROUP BY collection, name
        ON CONFLICT (name) DO UPDATE SET
            datetime_range = EXCLUDED.datetime_range,
            end_datetime_range = EXCLUDED.end_datetime_range
    ;

    RAISE NOTICE 'Doing the insert. %', clock_timestamp() - ts;
    IF TG_TABLE_NAME = 'items_staging' THEN
        INSERT INTO items
        SELECT
            (content_dehydrate(content)).*
        FROM newdata;
        DELETE FROM items_staging;
    ELSIF TG_TABLE_NAME = 'items_staging_ignore' THEN
        INSERT INTO items
        SELECT
            (content_dehydrate(content)).*
        FROM newdata
        ON CONFLICT DO NOTHING;
        DELETE FROM items_staging_ignore;
    ELSIF TG_TABLE_NAME = 'items_staging_upsert' THEN
        WITH staging_formatted AS (
            SELECT (content_dehydrate(content)).* FROM newdata
        ), deletes AS (
            DELETE FROM items i USING staging_formatted s
                WHERE
                    i.id = s.id
                    AND i.collection = s.collection
                    AND i IS DISTINCT FROM s
            RETURNING i.id, i.collection
        )
        INSERT INTO items
        SELECT s.* FROM
            staging_formatted s
            JOIN deletes d
            USING (id, collection);
        DELETE FROM items_staging_upsert;
    END IF;
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
$$ LANGUAGE PLPGSQL STABLE SECURITY DEFINER SET SEARCH_PATH TO pgstac, public;

CREATE OR REPLACE FUNCTION get_item(_id text, _collection text DEFAULT NULL) RETURNS jsonb AS $$
    SELECT content_hydrate(items) FROM items WHERE id=_id AND (_collection IS NULL OR collection=_collection);
$$ LANGUAGE SQL STABLE SECURITY DEFINER SET SEARCH_PATH TO pgstac, public;

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
    SELECT to_jsonb(array[array[min(datetime)::text, max(datetime)::text]])
    FROM items WHERE collection=$1;
;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE SET SEARCH_PATH TO pgstac, public;

CREATE OR REPLACE FUNCTION update_collection_extents() RETURNS VOID AS $$
UPDATE collections SET
    content = content ||
    jsonb_build_object(
        'extent', jsonb_build_object(
            'spatial', jsonb_build_object(
                'bbox', collection_bbox(collections.id)
            ),
            'temporal', jsonb_build_object(
                'interval', collection_temporal_extent(collections.id)
            )
        )
    )
;
$$ LANGUAGE SQL;
