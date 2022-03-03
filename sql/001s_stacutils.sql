/* looks for a geometry in a stac item first from geometry and falling back to bbox */
CREATE OR REPLACE FUNCTION stac_geom(value jsonb) RETURNS geometry AS $$
SELECT
    CASE
            WHEN value ? 'intersects' THEN
                ST_GeomFromGeoJSON(value->>'intersects')
            WHEN value ? 'geometry' THEN
                ST_GeomFromGeoJSON(value->>'geometry')
            WHEN value ? 'bbox' THEN
                bbox_geom(value->'bbox')
            ELSE NULL
        END as geometry
;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION stac_datetime(value jsonb) RETURNS timestamptz AS $$
SELECT COALESCE(
    (value->'properties'->>'datetime')::timestamptz,
    (value->'properties'->>'start_datetime')::timestamptz
);
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE SET TIMEZONE='UTC';

CREATE OR REPLACE FUNCTION stac_end_datetime(value jsonb) RETURNS timestamptz AS $$
SELECT COALESCE(
    (value->'properties'->>'datetime')::timestamptz,
    (value->'properties'->>'end_datetime')::timestamptz
);
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE SET TIMEZONE='UTC';


CREATE OR REPLACE FUNCTION stac_daterange(value jsonb) RETURNS tstzrange AS $$
    SELECT tstzrange(stac_datetime(value),stac_end_datetime(value));
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE SET TIMEZONE='UTC';


CREATE TABLE stac_extensions(
    name text PRIMARY KEY,
    url text,
    enbabled_by_default boolean NOT NULL DEFAULT TRUE,
    enableable boolean NOT NULL DEFAULT TRUE
);

INSERT INTO stac_extensions (name, url) VALUES
    ('fields', 'https://api.stacspec.org/v1.0.0-beta.5/item-search#fields'),
    ('sort','https://api.stacspec.org/v1.0.0-beta.5/item-search#sort'),
    ('context','https://api.stacspec.org/v1.0.0-beta.5/item-search#context'),
    ('filter', 'https://api.stacspec.org/v1.0.0-beta.5/item-search#filter'),
    ('query', 'https://api.stacspec.org/v1.0.0-beta.5/item-search#query')
ON CONFLICT (name) DO UPDATE SET url=EXCLUDED.url;
