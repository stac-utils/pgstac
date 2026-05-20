-- benchmark_dehydrate.sql
-- Run inside the pgstac schema to compare content_dehydrate variants.
-- Usage: psql -U username -d postgis -v ON_ERROR_STOP=1 -f benchmark_dehydrate.sql
SET search_path TO pgstac, public;

-- ============================================================
-- Variant definitions
-- ============================================================

-- A: Current PL/pgSQL (baseline)
CREATE OR REPLACE FUNCTION bench_dehydrate_plpgsql(content jsonb) RETURNS items AS $$
DECLARE
    out items;
    props jsonb;
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
    RETURN out;
END;
$$ LANGUAGE PLPGSQL STABLE;

-- B: SQL language (single SELECT, planner can inline/optimise)
-- Note: stac_geom/stac_datetime/stac_end_datetime must remain as function calls.
-- content_hash intentionally omitted here (variant C adds it back) to test cost of sha256.
CREATE OR REPLACE FUNCTION bench_dehydrate_sql_nohash(content jsonb) RETURNS items AS $$
    SELECT
        content->>'id',
        stac_geom(content),
        content->>'collection',
        stac_datetime(content),
        stac_end_datetime(content),
        content->>'stac_version',
        COALESCE(content->'stac_extensions', '[]'::jsonb),
        now(),
        ''::text,                                                   -- content_hash skipped
        NULL::bigint,                                               -- fragment_id
        content->'bbox',
        COALESCE(content->'links',  '[]'::jsonb),
        COALESCE(content->'assets', '{}'::jsonb),
        COALESCE(content->'properties', '{}'::jsonb),
        content - '{id,geometry,collection,type,bbox,links,assets,properties,stac_version,stac_extensions}'::text[],
        -- promoted: common metadata
        ((content->'properties')->>'created')::timestamptz,
        ((content->'properties')->>'updated')::timestamptz,
        (content->'properties')->>'platform',
        to_text_array(content->'properties'->'instruments'),
        (content->'properties')->>'constellation',
        (content->'properties')->>'mission',
        ((content->'properties')->>'eo:cloud_cover')::float8,
        content->'properties'->'eo:bands',
        ((content->'properties')->>'eo:snow_cover')::float8,
        ((content->'properties')->>'gsd')::float8,
        ((content->'properties')->>'proj:epsg')::integer,
        (content->'properties')->>'proj:wkt2',
        content->'properties'->'proj:projjson',
        content->'properties'->'proj:bbox',
        content->'properties'->'proj:centroid',
        content->'properties'->'proj:shape',
        content->'properties'->'proj:transform',
        (content->'properties')->>'sci:doi',
        (content->'properties')->>'sci:citation',
        content->'properties'->'sci:publications',
        ((content->'properties')->>'view:off_nadir')::float8,
        ((content->'properties')->>'view:incidence_angle')::float8,
        ((content->'properties')->>'view:azimuth')::float8,
        ((content->'properties')->>'view:sun_azimuth')::float8,
        ((content->'properties')->>'view:sun_elevation')::float8,
        ((content->'properties')->>'file:size')::bigint,
        ((content->'properties')->>'file:header_size')::bigint,
        (content->'properties')->>'file:checksum',
        (content->'properties')->>'file:byte_order',
        (content->'properties')->>'file:values_regex',
        (content->'properties')->>'sat:orbit_state',
        ((content->'properties')->>'sat:relative_orbit')::integer,
        ((content->'properties')->>'sat:absolute_orbit')::integer
$$ LANGUAGE SQL STABLE;

-- C: SQL language, full (with sha256 hash, same semantics as baseline)
CREATE OR REPLACE FUNCTION bench_dehydrate_sql_full(content jsonb) RETURNS items AS $$
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
        COALESCE(content->'properties', '{}'::jsonb),
        content - '{id,geometry,collection,type,bbox,links,assets,properties,stac_version,stac_extensions}'::text[],
        ((content->'properties')->>'created')::timestamptz,
        ((content->'properties')->>'updated')::timestamptz,
        (content->'properties')->>'platform',
        to_text_array(content->'properties'->'instruments'),
        (content->'properties')->>'constellation',
        (content->'properties')->>'mission',
        ((content->'properties')->>'eo:cloud_cover')::float8,
        content->'properties'->'eo:bands',
        ((content->'properties')->>'eo:snow_cover')::float8,
        ((content->'properties')->>'gsd')::float8,
        ((content->'properties')->>'proj:epsg')::integer,
        (content->'properties')->>'proj:wkt2',
        content->'properties'->'proj:projjson',
        content->'properties'->'proj:bbox',
        content->'properties'->'proj:centroid',
        content->'properties'->'proj:shape',
        content->'properties'->'proj:transform',
        (content->'properties')->>'sci:doi',
        (content->'properties')->>'sci:citation',
        content->'properties'->'sci:publications',
        ((content->'properties')->>'view:off_nadir')::float8,
        ((content->'properties')->>'view:incidence_angle')::float8,
        ((content->'properties')->>'view:azimuth')::float8,
        ((content->'properties')->>'view:sun_azimuth')::float8,
        ((content->'properties')->>'view:sun_elevation')::float8,
        ((content->'properties')->>'file:size')::bigint,
        ((content->'properties')->>'file:header_size')::bigint,
        (content->'properties')->>'file:checksum',
        (content->'properties')->>'file:byte_order',
        (content->'properties')->>'file:values_regex',
        (content->'properties')->>'sat:orbit_state',
        ((content->'properties')->>'sat:relative_orbit')::integer,
        ((content->'properties')->>'sat:absolute_orbit')::integer
$$ LANGUAGE SQL STABLE;

-- D: PL/pgSQL with props extracted once (current approach) but with LATERAL reuse
--    via a CTE-like local variable (already the current code's approach — pure baseline).

-- E: SQL with lateral subquery to extract props once (avoids repeating content->'properties')
CREATE OR REPLACE FUNCTION bench_dehydrate_sql_lateral(content jsonb) RETURNS items AS $$
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

-- F: PL/pgSQL + fragment dehydration inline (with collection join).
--    content_dehydrate_with_fragment(content, fragment_config) — optional overload.
--    fragment_config = NULL → behaves identically to baseline (no fragment work).
CREATE OR REPLACE FUNCTION bench_dehydrate_with_fragment(
    content jsonb,
    fragment_config jsonb DEFAULT NULL
) RETURNS items AS $$
DECLARE
    out items;
    props jsonb;
    frag_content jsonb;
    frag_hash text;
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

    -- Optional inline fragment dehydration:
    -- When fragment_config is supplied, compute the fragment payload and strip
    -- its covered keys from per-item assets/properties.  fragment_id is NOT
    -- assigned here (requires a table INSERT + RETURNING), but we shrink the
    -- per-item payload so callers can batch-insert the fragment themselves.
    IF fragment_config IS NOT NULL THEN
        frag_content := extract_fragment(content, fragment_config);
        IF frag_content IS NOT NULL AND frag_content != '{}'::jsonb THEN
            -- Strip covered asset keys
            IF jsonb_typeof(fragment_config->'asset_keys') = 'array' THEN
                asset_keys := ARRAY(SELECT jsonb_array_elements_text(fragment_config->'asset_keys'));
                out.assets := out.assets - asset_keys;
            END IF;
            -- Strip covered property keys
            IF jsonb_typeof(fragment_config->'property_keys') = 'array' THEN
                prop_keys := ARRAY(SELECT jsonb_array_elements_text(fragment_config->'property_keys'));
                out.properties := out.properties - prop_keys;
            END IF;
        END IF;
    END IF;

    RETURN out;
END;
$$ LANGUAGE PLPGSQL STABLE;


-- ============================================================
-- Warm-up (avoid cold-cache effects on first variant)
-- ============================================================
DO $$
DECLARE n int;
BEGIN
    SELECT count(*) INTO n FROM (
        SELECT (bench_dehydrate_plpgsql(content_hydrate(i))).id FROM items i LIMIT 50
    ) t;
END;
$$;

-- ============================================================
-- Benchmark harness: run each variant N times over all items
-- ============================================================
DO $$
DECLARE
    t0 timestamptz;
    t1 timestamptz;
    n  int := 5;   -- repeat iterations
    i  int;
    cnt int;
    label text;
    results text[] := '{}';

    PROCEDURE run_bench(variant_label text, variant_sql text) AS $$
    DECLARE
        _t0 timestamptz := clock_timestamp();
        _i  int;
        _n  int := 5;
    BEGIN
        FOR _i IN 1.._n LOOP
            EXECUTE variant_sql INTO cnt;
        END LOOP;
        results := results || format('%s: %s ms/iter (%s rows/iter)',
            variant_label,
            round(extract(epoch from (clock_timestamp() - _t0)) * 1000 / _n, 1),
            cnt);
    END;
    $$;
BEGIN
    -- A: PL/pgSQL baseline
    CALL run_bench('A: plpgsql baseline',
        'SELECT count(*) FROM (SELECT (bench_dehydrate_plpgsql(content_hydrate(i))).id FROM items i) t');

    -- B: SQL no hash
    CALL run_bench('B: sql no-hash',
        'SELECT count(*) FROM (SELECT (bench_dehydrate_sql_nohash(content_hydrate(i))).id FROM items i) t');

    -- C: SQL full (with hash)
    CALL run_bench('C: sql full',
        'SELECT count(*) FROM (SELECT (bench_dehydrate_sql_full(content_hydrate(i))).id FROM items i) t');

    -- D: SQL lateral (props extracted once via subquery)
    CALL run_bench('D: sql lateral',
        'SELECT count(*) FROM (SELECT (bench_dehydrate_sql_lateral(content_hydrate(i))).id FROM items i) t');

    -- E: plpgsql + fragment inline, no fragment config (NULL = baseline path)
    CALL run_bench('E: plpgsql+frag NULL config',
        'SELECT count(*) FROM (SELECT (bench_dehydrate_with_fragment(content_hydrate(i), NULL)).id FROM items i) t');

    -- F: plpgsql + fragment inline, with fragment_config from collection
    CALL run_bench('F: plpgsql+frag with config',
        'SELECT count(*) FROM (SELECT (bench_dehydrate_with_fragment(content_hydrate(i), c.fragment_config)).id FROM items i JOIN collections c ON c.id=i.collection) t');

    -- G: Cost of sha256 alone (isolated)
    CALL run_bench('G: sha256 cost only',
        'SELECT count(*) FROM (SELECT encode(sha256(content_hydrate(i)::text::bytea),''hex'') FROM items i) t');

    -- H: Cost of stac_geom alone (isolated)
    CALL run_bench('H: stac_geom cost only',
        'SELECT count(*) FROM (SELECT stac_geom(content_hydrate(i)) FROM items i) t');

    -- Print results
    RAISE NOTICE '';
    RAISE NOTICE '=== content_dehydrate benchmark results ===';
    FOR i IN 1..array_length(results, 1) LOOP
        RAISE NOTICE '%', results[i];
    END LOOP;
    RAISE NOTICE '===========================================';
END;
$$;

-- Cleanup
DROP FUNCTION IF EXISTS bench_dehydrate_plpgsql(jsonb);
DROP FUNCTION IF EXISTS bench_dehydrate_sql_nohash(jsonb);
DROP FUNCTION IF EXISTS bench_dehydrate_sql_full(jsonb);
DROP FUNCTION IF EXISTS bench_dehydrate_sql_lateral(jsonb);
DROP FUNCTION IF EXISTS bench_dehydrate_with_fragment(jsonb, jsonb);
