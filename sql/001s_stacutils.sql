/* looks for a geometry in a stac item first from geometry and falling back to bbox */
CREATE OR REPLACE FUNCTION stac_geom(value jsonb) RETURNS geometry AS $$
SELECT
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
