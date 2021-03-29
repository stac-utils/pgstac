SET SEARCH_PATH TO pgstac, public;

CREATE TABLE IF NOT EXISTS items (
    id VARCHAR,
    stac_version VARCHAR,
    stac_extensions VARCHAR[],
    geometry geometry NOT NULL,
    properties JSONB,
    assets JSONB,
    collection_id VARCHAR NOT NULL,
    datetime timestamptz NOT NULL,
    PRIMARY KEY (id)
);

/*
Converts single feature into an items row
*/
CREATE OR REPLACE FUNCTION feature_to_items(value jsonb) RETURNS items AS $$
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
        (value->'properties'->>'datetime')::timestamptz as datetime
;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE SET SEARCH_PATH TO pgstac,public;

/*
Takes a single feature, an array of features, or a feature collection
and returns a set up individual items rows
*/
CREATE OR REPLACE FUNCTION features_to_items(value jsonb) RETURNS SETOF items AS $$
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
    ) SELECT feature_to_items(value) FROM features;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE SET SEARCH_PATH TO pgstac,public;


/*
create an item from a json feature
*/
CREATE OR REPLACE FUNCTION create_item(content jsonb) RETURNS VOID AS $$
    DELETE FROM items WHERE id = content->>'id';
    INSERT INTO items SELECT * FROM features_to_items(content) ON CONFLICT DO NOTHING;
$$ LANGUAGE SQL SET SEARCH_PATH TO pgstac,public;


CREATE  FUNCTION get_items(_limit int = 10, _offset int = 0, _token varchar = NULL) RETURNS SETOF jsonb AS $$
SELECT to_jsonb(items) FROM items
WHERE
    CASE
        WHEN _token is NULL THEN TRUE
        ELSE id > _token
    END
ORDER BY id ASC
OFFSET _offset
LIMIT _limit
;
$$ LANGUAGE SQL SET SEARCH_PATH TO pgstac,public;


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