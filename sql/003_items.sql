SET SEARCH_PATH TO pgstac, public;

CREATE TABLE IF NOT EXISTS items (
    id VARCHAR GENERATED ALWAYS AS (content->>'id') STORED PRIMARY KEY,
    content JSONB
);

CREATE TABLE IF NOT EXISTS items_search (
    id text NOT NULL,
    geometry geometry NOT NULL,
    properties jsonb,
    collection_id text NOT NULL,
    datetime timestamptz NOT NULL
)
PARTITION BY RANGE (datetime)
;

CREATE TABLE IF NOT EXISTS items_search_template (
    LIKE items_search
)
;
ALTER TABLE items_search_template ADD PRIMARY KEY (id);

SELECT partman.create_parent(
    'pgstac.items_search',
    'datetime',
    'native',
    'weekly',
    p_template_table := 'pgstac.items_search_template',
    p_start_partition := '2000-01-01',
    p_premake := 52
);


CREATE INDEX "datetime_id_idx" ON items_search (datetime, id);
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
        ON CONFLICT DO NOTHING
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
View to get a table of available items partitions
with date ranges
*/
DROP VIEW IF EXISTS items_search_partitions;
CREATE VIEW items_search_partitions AS
WITH base AS
(SELECT
    c.oid::pg_catalog.regclass::text as partition,
    pg_catalog.pg_get_expr(c.relpartbound, c.oid) as _constraint,
    regexp_matches(
        pg_catalog.pg_get_expr(c.relpartbound, c.oid),
        E'\\(''\([0-9 :+-]*\)''\\).*\\(''\([0-9 :+-]*\)''\\)'
    ) as t,
    reltuples::bigint as est_cnt
FROM pg_catalog.pg_class c, pg_catalog.pg_inherits i
WHERE c.oid = i.inhrelid AND i.inhparent = 'items_search'::regclass)
SELECT partition, tstzrange(
    t[1]::timestamptz,
    t[2]::timestamptz
), est_cnt
FROM base
WHERE est_cnt >0
ORDER BY 2 desc;

CREATE OR REPLACE FUNCTION collection_bbox(id text) RETURNS jsonb AS $$
SELECT (replace(replace(replace(st_extent(geometry)::text,'BOX(','[['),')',']]'),' ',','))::jsonb
FROM items_search WHERE collection_id=$1;
;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE SET SEARCH_PATH TO pgstac, public;

CREATE OR REPLACE FUNCTION collection_temporal_extent(id text) RETURNS jsonb AS $$
SELECT to_jsonb(array[array[min(datetime)::text, max(datetime)::text]])
FROM items_search WHERE collection_id=$1;
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
$$ LANGUAGE SQL SET SEARCH_PATH TO pgstac, public;
