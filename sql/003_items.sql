SET SEARCH_PATH TO pgstac, public;

CREATE OR REPLACE FUNCTION properties_idx(_in jsonb) RETURNS jsonb AS $$
WITH kv AS (
    SELECT key, value FROM jsonb_each(_in) WHERE key = 'datetime' OR jsonb_typeof(value) IN ('object','array')
) SELECT lower((_in - array_agg(key))::text)::jsonb FROM kv;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE TABLE IF NOT EXISTS items (
    id text,
    stac_version text,
    stac_extensions text[],
    geometry geometry NOT NULL,
    properties JSONB,
    assets JSONB,
    collection_id text NOT NULL,
    datetime timestamptz NOT NULL,
    links jsonb,
    extra_fields jsonb,
    PRIMARY KEY (id)
);

CREATE INDEX "datetime_idx" ON items (datetime);
CREATE INDEX "properties_idx" ON items USING GIN ((properties_idx(properties)));


CREATE TYPE item AS (
    id text,
    stac_version text,
    stac_extensions text[],
    geometry geometry,
    properties JSONB,
    assets JSONB,
    collection_id text,
    datetime timestamptz,
    links jsonb,
    extra_fields jsonb
);



/*
Converts single feature into an items row
*/
CREATE OR REPLACE FUNCTION feature_to_item(value jsonb) RETURNS item AS $$
    SELECT
        DISTINCT ON (id)
        value->>'id' as id,
        value->>'stac_version' as stac_version,
        textarr(value->'stac_extensions') as stac_extensions,
        CASE
            WHEN value->>'geometry' IS NOT NULL THEN
                ST_GeomFromGeoJSON(value->>'geometry')
            WHEN value->>'bbox' IS NOT NULL THEN
                ST_MakeEnvelope(
                    (value->'bbox'->>0)::float,
                    (value->'bbox'->>1)::float,
                    (value->'bbox'->>2)::float,
                    (value->'bbox'->>3)::float,
                    4326
                )
            ELSE NULL
        END as geometry,
        value ->'properties' as properties,
        value->'assets' AS assets,
        value->>'collection' as collection_id,
        (value->'properties'->>'datetime')::timestamptz as datetime,
        (value->'links') - '{self, item, parent, collection, root}'::text[] as links,
        value-'{id, stac_version, stac_extensions, type, geometry, bbox, properties, assets, collection, links}'::text[] as exta_fields
;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE SET SEARCH_PATH TO pgstac,public;

/*
Takes a single feature, an array of features, or a feature collection
and returns a set up individual items rows
*/
CREATE OR REPLACE FUNCTION features_to_items(value jsonb) RETURNS SETOF item AS $$
    WITH features AS (
        SELECT
        jsonb_array_elements(
            CASE
                WHEN jsonb_typeof(value) = 'array' THEN value
                WHEN value->>'type' = 'Feature' THEN '[]'::jsonb || value
                WHEN value->>'type' = 'FeatureCollection' THEN value->'features'
                ELSE NULL
            END
        ) as value
    )
    SELECT feature_to_item(value) FROM features
    ;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE SET SEARCH_PATH TO pgstac,public;


/*
create an item from a json feature
*/


CREATE OR REPLACE FUNCTION format_item(_item record) RETURNS jsonb AS $$
DECLARE
BEGIN
return jsonb_build_object(
        'type', 'Feature',
        'id', _item.id,
        'stac_version', _item.stac_version,
        'stac_extensions', _item.stac_extensions,
        'geometry', st_asgeojson(_item.geometry)::jsonb,
        'bbox', ARRAY[st_xmin(_item.geometry), st_ymin(_item.geometry), st_xmax(_item.geometry), st_ymax(_item.geometry)],
        'properties', _item.properties,
        'assets', _item.assets,
        'collection', _item.collection_id,
        'links', coalesce(_item.links, '[]'::jsonb)
    ) || coalesce(_item.extra_fields, '{}'::jsonb)
;
END;
$$ LANGUAGE PLPGSQL SET SEARCH_PATH TO pgstac,public;

CREATE OR REPLACE FUNCTION get_item(_id text) RETURNS jsonb AS $$
SELECT format_item(items) FROM items WHERE id=_id;
$$ LANGUAGE SQL SET SEARCH_PATH TO pgstac,public;

CREATE OR REPLACE FUNCTION create_item(content jsonb) RETURNS jsonb AS $$
    DELETE FROM items WHERE id = content->>'id';
    INSERT INTO items SELECT * FROM feature_to_item(content)
    ON CONFLICT DO NOTHING;
    SELECT format_item(items) FROM items WHERE id = content->>'id';
$$ LANGUAGE SQL SET SEARCH_PATH TO pgstac,public;

CREATE OR REPLACE FUNCTION get_items(_limit int = 10, _offset int = 0, _token varchar = NULL) RETURNS SETOF jsonb AS $$
SELECT get_item(id) FROM items
--WHERE
    --CASE
    --    WHEN _token is NULL THEN TRUE
    --    ELSE id > _token
    --END
ORDER BY id ASC
OFFSET _offset
LIMIT _limit
;
$$ LANGUAGE SQL SET SEARCH_PATH TO pgstac,public;

CREATE OR REPLACE FUNCTION item_collection(_id text, _limit int = 10, _token text = NULL) RETURNS SETOF jsonb AS $$
DECLARE
tok_val text := substr(_token,9);
BEGIN
raise notice 'token value %', tok_val;
IF _token is null THEN
    RETURN QUERY
    SELECT get_item(id) as item FROM items
    WHERE collection_id=_id
    ORDER BY id ASC LIMIT _limit;
ELSIF starts_with(_token, 'minitem:') THEN
    RETURN QUERY
    SELECT get_item(id) as item FROM items
    WHERE collection_id=_id AND id < tok_val
    ORDER BY id DESC LIMIT _limit;
ELSE
    RETURN QUERY
    SELECT get_item(id) as item FROM items
    WHERE collection_id=_id AND id > tok_val
    ORDER BY id ASC LIMIT _limit;
END IF;

END;
$$ LANGUAGE PLPGSQL SET SEARCH_PATH TO pgstac,public;




/*
Staging table and triggers allow ndjson to be upserted into the
items table using the postgresql copy mechanism.
*/
CREATE UNLOGGED TABLE items_staging (data jsonb);
ALTER TABLE items_staging SET (autovacuum_enabled = false);

CREATE  OR REPLACE FUNCTION items_staging_trigger_func()
RETURNS TRIGGER AS $$
DECLARE
cnt integer;
t timestamptz := clock_timestamp();
inc timestamptz := clock_timestamp();
BEGIN
    RAISE NOTICE 'loading raw data %', clock_timestamp()-t;
    CREATE TEMP TABLE items_staging_preload ON COMMIT DROP
    AS
    WITH features AS (
        SELECT features_to_items(data) d FROM newdata
    ) SELECT (d).* FROM features;
    GET DIAGNOSTICS cnt= ROW_COUNT;

    RAISE NOTICE 'Loaded % rows into raw table time: % total_time %', cnt, clock_timestamp()-inc, clock_timestamp()-t;
    inc := clock_timestamp();

    CREATE INDEX ON items_staging_preload (id);
    ANALYZE items_staging_preload(id);

    RAISE NOTICE 'Created index on id on raw table time: % total_time %', clock_timestamp()-inc, clock_timestamp()-t;
    inc := clock_timestamp();

    DELETE FROM items USING items_staging_preload
        WHERE items.id=items_staging_preload.id;
    GET DIAGNOSTICS cnt= ROW_COUNT;

    RAISE NOTICE 'Deleted % rows with existing ids in destination table. time: % total_time %', cnt, clock_timestamp()-inc, clock_timestamp()-t;
    inc := clock_timestamp();

    INSERT INTO items
    SELECT DISTINCT ON (id) * FROM items_staging_preload
    ON CONFLICT DO NOTHING;
    GET DIAGNOSTICS cnt= ROW_COUNT;

    RAISE NOTICE 'Inserted % rows into destination table. time: % total_time %', cnt, clock_timestamp()-inc, clock_timestamp()-t;
    inc := clock_timestamp();
    RETURN NULL;
END;
$$ LANGUAGE PLPGSQL SET SEARCH_PATH TO pgstac,public;

CREATE TRIGGER items_staging_trigger
AFTER INSERT ON items_staging
REFERENCING NEW TABLE as newdata
FOR EACH STATEMENT EXECUTE PROCEDURE items_staging_trigger_func();