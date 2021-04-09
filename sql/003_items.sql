SET SEARCH_PATH TO pgstac, public;

CREATE OR REPLACE FUNCTION properties_idx(_in jsonb) RETURNS jsonb AS $$
WITH kv AS (
    SELECT key, value FROM jsonb_each(_in) WHERE key = 'datetime' OR jsonb_typeof(value) IN ('object','array')
) SELECT lower((_in - array_agg(key))::text)::jsonb FROM kv;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE TABLE IF NOT EXISTS items (
    id VARCHAR GENERATED ALWAYS AS (content->>'id') STORED PRIMARY KEY,
    content JSONB
);

CREATE TABLE IF NOT EXISTS items_search (
    id text PRIMARY KEY,
    geometry geometry NOT NULL,
    properties jsonb,
    collection_id text NOT NULL,
    datetime timestamptz NOT NULL
);

CREATE INDEX "datetime_idx" ON items_search (datetime);
CREATE INDEX "properties_idx" ON items_search USING GIN (properties);
CREATE INDEX "collection_idx" ON items_search (collection_id);
CREATE INDEX "geometry_idx" ON items_search USING GIST (geometry);


CREATE TYPE item AS (
    id text,
    geometry geometry,
    properties JSONB,
    collection_id text,
    datetime timestamptz
);



/*
Converts single feature into an items row
*/
CREATE OR REPLACE FUNCTION feature_to_item(value jsonb) RETURNS item AS $$
    SELECT
        value->>'id' as id,
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
        properties_idx(value ->'properties') as properties,
        value->>'collection' as collection_id,
        (value->'properties'->>'datetime')::timestamptz as datetime
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


CREATE OR REPLACE FUNCTION get_item(_id text) RETURNS jsonb AS $$
SELECT content FROM items WHERE id=_id;
$$ LANGUAGE SQL SET SEARCH_PATH TO pgstac,public;

CREATE OR REPLACE FUNCTION delete_item(_id text) RETURNS VOID AS $$
    DELETE FROM items WHERE id = _id;
$$ LANGUAGE SQL SET SEARCH_PATH TO pgstac,public;

CREATE OR REPLACE FUNCTION create_item(data jsonb) RETURNS VOID AS $$
    DELETE FROM items WHERE id=data->>'id';
    INSERT INTO items (content) VALUES (data);
$$ LANGUAGE SQL SET SEARCH_PATH TO pgstac,public;

/* Trigger Function to cascade inserts/updates/deletes
from items table to items_search table */
CREATE OR REPLACE FUNCTION items_trigger_func()
RETURNS TRIGGER AS $$
DECLARE
BEGIN
    DELETE FROM items_search WHERE id = NEW.id;
    IF TG_OP IN ('INSERT', 'UPDATE') AND pg_trigger_depth() = 1 THEN
        INSERT INTO items (content) VALUES (NEW.content)
        ON CONFLICT (id) DO UPDATE
            SET content=EXCLUDED.content;
        INSERT INTO items_search SELECT * FROM feature_to_item(NEW.content)
        ON CONFLICT (id) DO UPDATE
            SET
                geometry=EXCLUDED.geometry,
                properties=EXCLUDED.properties,
                collection_id=EXCLUDED.collection_id,
                datetime=EXCLUDED.datetime
        ;
        RETURN NULL;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE PLPGSQL SET SEARCH_PATH TO pgstac,public;

CREATE TRIGGER items_trigger
BEFORE INSERT OR UPDATE OR DELETE ON items
FOR EACH ROW EXECUTE PROCEDURE items_trigger_func();

/*
Staging table and triggers allow ndjson to be upserted into the
items table using the postgresql copy mechanism.
*/
/*
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
    DELETE FROM items_search USING items_staging_preload
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
*/