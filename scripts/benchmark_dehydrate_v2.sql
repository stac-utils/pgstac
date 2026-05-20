-- benchmark_dehydrate_v2.sql
-- Direct timing benchmarks for content_dehydrate variants.
-- Each variant runs N iterations and we time the total with clock_timestamp().
SET search_path TO pgstac, public;

-- ============================================================
-- Helper: get a sample item as raw JSONB
-- ============================================================
CREATE TEMP TABLE bench_items AS
    SELECT content_hydrate(i) AS content FROM items i;

DO $$ BEGIN RAISE NOTICE 'Benchmark items: %', (SELECT count(*) FROM bench_items); END; $$;

-- ============================================================
-- Variant A: SQL-language (lateral subquery for props)
-- ============================================================
CREATE OR REPLACE FUNCTION bench_a_sql_lateral(content jsonb) RETURNS items AS $$
    SELECT
        content->>'id',
        stac_geom(content),
        content->>'collection',
        stac_datetime(content),
        stac_end_datetime(content),
        content->>'stac_version',
        COALESCE(content->'stac_extensions', '[]'::jsonb),
        now(),
        encode(sha256(content::text::bytea), 'hex'),
        NULL::bigint,
        content->'bbox',
        COALESCE(content->'links',  '[]'::jsonb),
        COALESCE(content->'assets', '{}'::jsonb),
        COALESCE(props, '{}'::jsonb),
        content - '{id,geometry,collection,type,bbox,links,assets,properties,stac_version,stac_extensions}'::text[],
        (props->>'created')::timestamptz,
        (props->>'updated')::timestamptz,
        props->>'platform',
        to_text_array(props->'instruments'),
        props->>'constellation',
        props->>'mission',
        (props->>'eo:cloud_cover')::float8,
        props->'eo:bands',
        (props->>'eo:snow_cover')::float8,
        (props->>'gsd')::float8,
        (props->>'proj:epsg')::integer,
        props->>'proj:wkt2',
        props->'proj:projjson',
        props->'proj:bbox',
        props->'proj:centroid',
        props->'proj:shape',
        props->'proj:transform',
        props->>'sci:doi',
        props->>'sci:citation',
        props->'sci:publications',
        (props->>'view:off_nadir')::float8,
        (props->>'view:incidence_angle')::float8,
        (props->>'view:azimuth')::float8,
        (props->>'view:sun_azimuth')::float8,
        (props->>'view:sun_elevation')::float8,
        (props->>'file:size')::bigint,
        (props->>'file:header_size')::bigint,
        props->>'file:checksum',
        props->>'file:byte_order',
        props->>'file:values_regex',
        props->>'sat:orbit_state',
        (props->>'sat:relative_orbit')::integer,
        (props->>'sat:absolute_orbit')::integer
    FROM (SELECT content->'properties') AS t(props)
$$ LANGUAGE SQL STABLE;

-- ============================================================
-- Variant B: SQL-language without sha256 (isolate hash cost)
-- ============================================================
CREATE OR REPLACE FUNCTION bench_b_sql_nohash(content jsonb) RETURNS items AS $$
    SELECT
        content->>'id',
        stac_geom(content),
        content->>'collection',
        stac_datetime(content),
        stac_end_datetime(content),
        content->>'stac_version',
        COALESCE(content->'stac_extensions', '[]'::jsonb),
        now(),
        ''::text,
        NULL::bigint,
        content->'bbox',
        COALESCE(content->'links',  '[]'::jsonb),
        COALESCE(content->'assets', '{}'::jsonb),
        COALESCE(props, '{}'::jsonb),
        content - '{id,geometry,collection,type,bbox,links,assets,properties,stac_version,stac_extensions}'::text[],
        (props->>'created')::timestamptz,
        (props->>'updated')::timestamptz,
        props->>'platform',
        to_text_array(props->'instruments'),
        props->>'constellation',
        props->>'mission',
        (props->>'eo:cloud_cover')::float8,
        props->'eo:bands',
        (props->>'eo:snow_cover')::float8,
        (props->>'gsd')::float8,
        (props->>'proj:epsg')::integer,
        props->>'proj:wkt2',
        props->'proj:projjson',
        props->'proj:bbox',
        props->'proj:centroid',
        props->'proj:shape',
        props->'proj:transform',
        props->>'sci:doi',
        props->>'sci:citation',
        props->'sci:publications',
        (props->>'view:off_nadir')::float8,
        (props->>'view:incidence_angle')::float8,
        (props->>'view:azimuth')::float8,
        (props->>'view:sun_azimuth')::float8,
        (props->>'view:sun_elevation')::float8,
        (props->>'file:size')::bigint,
        (props->>'file:header_size')::bigint,
        props->>'file:checksum',
        props->>'file:byte_order',
        props->>'file:values_regex',
        props->>'sat:orbit_state',
        (props->>'sat:relative_orbit')::integer,
        (props->>'sat:absolute_orbit')::integer
    FROM (SELECT content->'properties') AS t(props)
$$ LANGUAGE SQL STABLE;

-- ============================================================
-- Variant C: PL/pgSQL with fragment inline (optional, NULL = no-op)
-- ============================================================
CREATE OR REPLACE FUNCTION bench_c_plpgsql_frag(
    content jsonb,
    fragment_config jsonb DEFAULT NULL
) RETURNS items AS $$
DECLARE
    out items;
    props jsonb;
    asset_keys text[];
    prop_keys  text[];
BEGIN
    out.id := content->>'id';
    out.geometry := stac_geom(content);
    out.collection := content->>'collection';
    out.datetime := stac_datetime(content);
    out.end_datetime := stac_end_datetime(content);
    out.stac_version := content->>'stac_version';
    out.stac_extensions := COALESCE(content->'stac_extensions', '[]'::jsonb);
    out.pgstac_updated_at := now();
    out.content_hash := encode(sha256(content::text::bytea), 'hex');
    props := content->'properties';
    out.bbox       := content->'bbox';
    out.links      := COALESCE(content->'links', '[]'::jsonb);
    out.assets     := COALESCE(content->'assets', '{}'::jsonb);
    out.properties := COALESCE(props, '{}'::jsonb);
    out.extra      := content - '{id,geometry,collection,type,bbox,links,assets,properties,stac_version,stac_extensions}'::text[];
    out.created             := (props->>'created')::timestamptz;
    out.updated             := (props->>'updated')::timestamptz;
    out.platform            := props->>'platform';
    out.instruments         := to_text_array(props->'instruments');
    out.constellation       := props->>'constellation';
    out.mission             := props->>'mission';
    out.eo_cloud_cover    := (props->>'eo:cloud_cover')::float8;
    out.eo_bands          := props->'eo:bands';
    out.eo_snow_cover     := (props->>'eo:snow_cover')::float8;
    out.gsd               := (props->>'gsd')::float8;
    out.proj_epsg         := (props->>'proj:epsg')::integer;
    out.proj_wkt2         := props->>'proj:wkt2';
    out.proj_projjson     := props->'proj:projjson';
    out.proj_bbox         := props->'proj:bbox';
    out.proj_centroid     := props->'proj:centroid';
    out.proj_shape        := props->'proj:shape';
    out.proj_transform    := props->'proj:transform';
    out.sci_doi           := props->>'sci:doi';
    out.sci_citation      := props->>'sci:citation';
    out.sci_publications  := props->'sci:publications';
    out.view_off_nadir    := (props->>'view:off_nadir')::float8;
    out.view_incidence_angle := (props->>'view:incidence_angle')::float8;
    out.view_azimuth      := (props->>'view:azimuth')::float8;
    out.view_sun_azimuth  := (props->>'view:sun_azimuth')::float8;
    out.view_sun_elevation := (props->>'view:sun_elevation')::float8;
    out.file_size         := (props->>'file:size')::bigint;
    out.file_header_size  := (props->>'file:header_size')::bigint;
    out.file_checksum     := props->>'file:checksum';
    out.file_byte_order   := props->>'file:byte_order';
    out.file_values_regex := props->>'file:values_regex';
    out.sat_orbit_state   := props->>'sat:orbit_state';
    out.sat_relative_orbit := (props->>'sat:relative_orbit')::integer;
    out.sat_absolute_orbit := (props->>'sat:absolute_orbit')::integer;
    out.fragment_id := NULL;

    IF fragment_config IS NOT NULL THEN
        IF jsonb_typeof(fragment_config->'asset_keys') = 'array' THEN
            asset_keys := ARRAY(SELECT jsonb_array_elements_text(fragment_config->'asset_keys'));
            out.assets := out.assets - asset_keys;
        END IF;
        IF jsonb_typeof(fragment_config->'property_keys') = 'array' THEN
            prop_keys := ARRAY(SELECT jsonb_array_elements_text(fragment_config->'property_keys'));
            out.properties := out.properties - prop_keys;
        END IF;
    END IF;

    RETURN out;
END;
$$ LANGUAGE PLPGSQL STABLE;

-- ============================================================
-- Warm-up round (avoid cold cache effects)
-- ============================================================
DO $$
DECLARE n int;
BEGIN
    SELECT count(*) INTO n FROM (
        SELECT (content_dehydrate(content)).id FROM bench_items
    ) t;
    RAISE NOTICE 'Warmup done: % rows', n;
END;
$$;

-- ============================================================
-- Run benchmarks using clock_timestamp() timing
-- ============================================================
DO $$
DECLARE
    t0  timestamptz;
    n   int := 20;  -- iterations
    i   int;
    cnt int;
    ms  numeric;
BEGIN
    -- Baseline: existing content_dehydrate (PL/pgSQL)
    t0 := clock_timestamp();
    FOR i IN 1..n LOOP
        SELECT count(*) INTO cnt FROM (SELECT (content_dehydrate(content)).id FROM bench_items) t;
    END LOOP;
    ms := round(extract(epoch FROM (clock_timestamp() - t0)) * 1000 / n, 2);
    RAISE NOTICE 'BASELINE plpgsql (content_dehydrate): % ms/iter, % rows', ms, cnt;

    -- Variant A: SQL lateral
    t0 := clock_timestamp();
    FOR i IN 1..n LOOP
        SELECT count(*) INTO cnt FROM (SELECT (bench_a_sql_lateral(content)).id FROM bench_items) t;
    END LOOP;
    ms := round(extract(epoch FROM (clock_timestamp() - t0)) * 1000 / n, 2);
    RAISE NOTICE 'A: SQL + lateral (with hash): % ms/iter, % rows', ms, cnt;

    -- Variant B: SQL no hash
    t0 := clock_timestamp();
    FOR i IN 1..n LOOP
        SELECT count(*) INTO cnt FROM (SELECT (bench_b_sql_nohash(content)).id FROM bench_items) t;
    END LOOP;
    ms := round(extract(epoch FROM (clock_timestamp() - t0)) * 1000 / n, 2);
    RAISE NOTICE 'B: SQL + lateral (no hash): % ms/iter, % rows', ms, cnt;

    -- Variant C-null: plpgsql+frag, fragment_config=NULL (no-op path)
    t0 := clock_timestamp();
    FOR i IN 1..n LOOP
        SELECT count(*) INTO cnt FROM (SELECT (bench_c_plpgsql_frag(content, NULL)).id FROM bench_items) t;
    END LOOP;
    ms := round(extract(epoch FROM (clock_timestamp() - t0)) * 1000 / n, 2);
    RAISE NOTICE 'C-null: plpgsql+frag (config=NULL): % ms/iter, % rows', ms, cnt;

    -- Variant C-config: plpgsql+frag, with a non-null fragment_config
    t0 := clock_timestamp();
    FOR i IN 1..n LOOP
        SELECT count(*) INTO cnt FROM (
            SELECT (bench_c_plpgsql_frag(
                b.content,
                '{"asset_keys":["B01","B02"],"property_keys":["platform","instruments"]}'::jsonb
            )).id
            FROM bench_items b
        ) t;
    END LOOP;
    ms := round(extract(epoch FROM (clock_timestamp() - t0)) * 1000 / n, 2);
    RAISE NOTICE 'C-config: plpgsql+frag (with config): % ms/iter, % rows', ms, cnt;

    -- Isolated: sha256 cost only
    t0 := clock_timestamp();
    FOR i IN 1..n LOOP
        SELECT count(*) INTO cnt FROM (SELECT encode(sha256(content::text::bytea),'hex') FROM bench_items) t;
    END LOOP;
    ms := round(extract(epoch FROM (clock_timestamp() - t0)) * 1000 / n, 2);
    RAISE NOTICE 'ISOLATED sha256 only: % ms/iter, % rows', ms, cnt;

    -- Isolated: stac_geom cost only
    t0 := clock_timestamp();
    FOR i IN 1..n LOOP
        SELECT count(*) INTO cnt FROM (SELECT stac_geom(content) FROM bench_items) t;
    END LOOP;
    ms := round(extract(epoch FROM (clock_timestamp() - t0)) * 1000 / n, 2);
    RAISE NOTICE 'ISOLATED stac_geom only: % ms/iter, % rows', ms, cnt;

    -- Isolated: properties extraction all fields
    t0 := clock_timestamp();
    FOR i IN 1..n LOOP
        SELECT count(*) INTO cnt FROM (
            SELECT
                (content->'properties')->>'platform',
                ((content->'properties')->>'eo:cloud_cover')::float8,
                ((content->'properties')->>'proj:epsg')::integer
            FROM bench_items
        ) t;
    END LOOP;
    ms := round(extract(epoch FROM (clock_timestamp() - t0)) * 1000 / n, 2);
    RAISE NOTICE 'ISOLATED properties extraction: % ms/iter, % rows', ms, cnt;

    RAISE NOTICE '';
    RAISE NOTICE 'Done. % rows per iteration, % iterations each.', cnt, n;
END;
$$;

-- Cleanup
DROP FUNCTION IF EXISTS bench_a_sql_lateral(jsonb);
DROP FUNCTION IF EXISTS bench_b_sql_nohash(jsonb);
DROP FUNCTION IF EXISTS bench_c_plpgsql_frag(jsonb, jsonb);
DROP TABLE IF EXISTS bench_items;
