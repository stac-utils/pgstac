/* looks for a geometry in a stac item first from geometry and falling back to bbox */
CREATE OR REPLACE FUNCTION stac_geom(value jsonb) RETURNS geometry AS $$
SELECT
    CASE
            WHEN value ? 'intersects' THEN
                ST_GeomFromGeoJSON(value->>'intersects')
            WHEN value ? 'geometry' THEN
                ST_GeomFromGeoJSON(value->>'geometry')
            WHEN value ? 'bbox' THEN
                pgstac.bbox_geom(value->'bbox')
            ELSE NULL
        END as geometry
;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;



CREATE OR REPLACE FUNCTION stac_daterange(
    value jsonb
) RETURNS tstzrange AS $$
DECLARE
    props jsonb := value;
    dt timestamptz;
    edt timestamptz;
BEGIN
    IF props ? 'properties' THEN
        props := props->'properties';
    END IF;
    IF
        props ? 'start_datetime'
        AND props->>'start_datetime' IS NOT NULL
        AND props ? 'end_datetime'
        AND props->>'end_datetime' IS NOT NULL
    THEN
        dt := props->>'start_datetime';
        edt := props->>'end_datetime';
        IF dt > edt THEN
            RAISE EXCEPTION 'start_datetime must be < end_datetime';
        END IF;
    ELSE
        dt := props->>'datetime';
        edt := props->>'datetime';
    END IF;
    IF dt is NULL OR edt IS NULL THEN
        RAISE NOTICE 'DT: %, EDT: %', dt, edt;
        RAISE EXCEPTION 'Either datetime (%) or both start_datetime (%) and end_datetime (%) must be set.', props->>'datetime',props->>'start_datetime',props->>'end_datetime';
    END IF;
    RETURN tstzrange(dt, edt, '[]');
END;
$$ LANGUAGE PLPGSQL IMMUTABLE PARALLEL SAFE SET TIMEZONE='UTC';

CREATE OR REPLACE FUNCTION stac_datetime(value jsonb) RETURNS timestamptz AS $$
    SELECT lower(stac_daterange(value));
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE SET TIMEZONE='UTC';

CREATE OR REPLACE FUNCTION stac_end_datetime(value jsonb) RETURNS timestamptz AS $$
    SELECT upper(stac_daterange(value));
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE SET TIMEZONE='UTC';


CREATE TABLE stac_extensions(
    url text PRIMARY KEY,
    content jsonb
);

CREATE OR REPLACE FUNCTION update_stac_extension_urls() RETURNS VOID AS $$
    INSERT INTO stac_extensions (url)
    SELECT DISTINCT substring(jsonb_array_elements_text(content->'stac_extensions') FROM E'^[^#]*')
    FROM collections
    ON CONFLICT DO NOTHING;
$$ LANGUAGE SQL;
