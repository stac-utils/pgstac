
-- geometrysearch: tile/area feature search. Walks the SAME next_band loop as search_page (server-side
-- adaptive cumulative-count bands over partition_bounds' unified per-month histogram) and hydrates the
-- SAME way (content_hydrate with the needs_fragment skip, json_agg, json output). It differs from
-- search_page only in its per-row action: a greedy coverage filter (st_intersection/union +
-- skipcovered/exitwhenfull) and the scan/limit/time budget that drives early exit. Rows that pass
-- coverage are collected and hydrated once at the end.
DROP FUNCTION IF EXISTS geometrysearch;
CREATE OR REPLACE FUNCTION geometrysearch(
    IN geom geometry,
    IN queryhash text,
    IN fields jsonb DEFAULT NULL,
    IN _scanlimit int DEFAULT 10000,
    IN _limit int DEFAULT 100,
    IN _timelimit interval DEFAULT '5 seconds'::interval,
    IN exitwhenfull boolean DEFAULT TRUE, -- Return as soon as the passed in geometry is fully covered
    IN skipcovered boolean DEFAULT TRUE   -- Skip any items that would show up completely under the previous items
) RETURNS json AS $$
DECLARE
    -- Same adaptive band sizing as search_page: first band ~one page (band_margin headroom), each
    -- subsequent band re-sized from the band's observed spatial hit-rate (band_safety headroom).
    band_margin CONSTANT numeric := 3.0;
    band_safety CONSTANT numeric := 1.5;
    band_cap_months CONSTANT int := 18;
    search searches%ROWTYPE;
    curs refcursor;
    _where text; query text; proj_expr text;
    iter_record items%ROWTYPE;
    page_rows items[] := '{}'::items[];   -- coverage-passing rows, hydrated once at the end
    features json;
    exit_flag boolean := FALSE;
    counter int := 1; scancounter int := 1; remaining_limit int := _scanlimit;
    tilearea float; unionedgeom geometry; clippedgeom geometry;
    unionedgeom_area float := 0; prev_area float := 0;
    _env pred_envelope; bnds record;
    lead_field text; eff_dir text; datetime_leading boolean; is_asc boolean; orderby_str text;
    cursor_idx int; mo interval := interval '1 month'; band record; band_target numeric; obs_sel numeric;
    band_fetched int; guard int := 0; cum_scanned bigint := 0;
BEGIN
    -- If the passed in geometry is not an area, coverage tests are meaningless.
    IF ST_GeometryType(geom) !~* 'polygon' THEN
        skipcovered := FALSE; exitwhenfull := FALSE;
    END IF;
    -- skipcovered implies exitwhenfull (once covered, nothing new can show through).
    IF skipcovered THEN exitwhenfull := TRUE; END IF;

    search := search_fromhash(queryhash);
    IF search IS NULL THEN
        RAISE EXCEPTION 'Search with Query Hash % Not Found', queryhash;
    END IF;

    tilearea := st_area(geom);
    _where := format('%s AND st_intersects(geometry, %L::geometry)', search._where, geom);

    -- Discovery envelope from the registered search AND the tile geometry, then the single
    -- partition_bounds read (unified per-month histogram) that next_band walks.
    _env := search_envelope(search.search);
    _env.geom := CASE
        WHEN _env.geom IS NULL THEN ST_Envelope(geom)
        ELSE ST_Envelope(ST_Intersection(_env.geom, ST_Envelope(geom)))
    END;
    SELECT * INTO bnds FROM partition_bounds(_env);

    -- Leading sort field/direction + the registered forward orderby. Bands are datetime windows walked
    -- newest-first unless the search sorts datetime ascending; within a band rows follow search.orderby.
    SELECT (array_agg(field ORDER BY ord))[1], (array_agg(dir ORDER BY ord))[1]
    INTO lead_field, eff_dir FROM keyset_sortkeys(search.search);
    datetime_leading := (lead_field = 'datetime');
    is_asc := (datetime_leading AND eff_dir = 'ASC');
    orderby_str := search.orderby;

    -- Per-row projection: skip the shared item_fragments lookup when the requested fields are
    -- satisfiable from item columns alone (needs_fragment, evaluated once for the whole query).
    IF needs_fragment(coalesce(fields, '{}'::jsonb), bnds.collections) THEN
        proj_expr := format('content_hydrate(i, %L::jsonb)', coalesce(fields, '{}'::jsonb));
    ELSE
        proj_expr := format('content_hydrate(i, %L::jsonb, true)', coalesce(fields, '{}'::jsonb));
    END IF;

    IF array_length(bnds.months, 1) IS NOT NULL THEN
        -- Walk bands in the SEARCH's datetime direction: newest month first for a descending search,
        -- oldest first when sorting datetime ascending. Mirrors search_page (004_search). Walking the
        -- wrong direction returns older items before newer ones for a descending limit/early-exit search,
        -- and (because band grouping shifts with the partition_stats histogram) makes the result depend on
        -- stats — a bug, since stats must only affect performance, never which rows come back.
        cursor_idx := CASE WHEN is_asc THEN 1 ELSE array_length(bnds.months, 1) END;
        band_target := (_limit + 1) * band_margin;
        <<bands>>
        WHILE NOT exit_flag AND guard < 80 LOOP
            guard := guard + 1;
            SELECT * INTO band FROM next_band(bnds.counts, cursor_idx, band_target, band_cap_months, NOT is_asc);
            -- process a valid band even when next_band also flags done; stop only on no band.
            EXIT bands WHEN band.band_start_idx IS NULL;
            query := format(
                'SELECT * FROM items i WHERE i.collection = ANY(%L::text[]) AND i.datetime >= %L AND i.datetime < %L AND %s ORDER BY %s LIMIT %L',
                bnds.collections, bnds.months[band.band_start_idx], bnds.months[band.band_end_idx] + mo, _where, orderby_str, remaining_limit);
            band_fetched := 0;
            OPEN curs FOR EXECUTE query;
            LOOP
                FETCH curs INTO iter_record;
                EXIT WHEN NOT FOUND;
                band_fetched := band_fetched + 1;
                IF exitwhenfull OR skipcovered THEN -- skip expensive geometry ops when neither is on
                    clippedgeom := st_intersection(geom, iter_record.geometry);
                    IF unionedgeom IS NULL THEN
                        unionedgeom := clippedgeom;
                    ELSE
                        unionedgeom := st_union(unionedgeom, clippedgeom);
                    END IF;
                    unionedgeom_area := st_area(unionedgeom);
                    IF skipcovered AND prev_area = unionedgeom_area THEN
                        scancounter := scancounter + 1;
                        CONTINUE;
                    END IF;
                    prev_area := unionedgeom_area;
                END IF;
                -- Collect the row; hydration happens once at the end (same content_hydrate path as search_page).
                page_rows := page_rows || iter_record;
                IF counter >= _limit
                    OR scancounter > _scanlimit
                    OR ftime() > _timelimit
                    OR (exitwhenfull AND unionedgeom_area >= tilearea)
                THEN
                    exit_flag := TRUE;
                    EXIT;
                END IF;
                counter := counter + 1;
                scancounter := scancounter + 1;
            END LOOP;
            CLOSE curs;
            cum_scanned := cum_scanned + band.scanned;
            cursor_idx := band.next_cursor_idx;
            EXIT bands WHEN exit_flag;
            -- LEARN: size the next band from this band's spatial hit-rate (fetched / rows scanned).
            obs_sel := GREATEST(band_fetched::numeric, 0.5) / GREATEST(cum_scanned, 1);
            band_target := ((_limit + 1 - counter) / obs_sel) * band_safety;
            remaining_limit := _scanlimit - scancounter;
        END LOOP;
    END IF;

    -- Hydrate the collected rows once (content_hydrate + needs_fragment skip, json output -- 1GB text
    -- ceiling, no 256MB jsonb-array limit), exactly like search_page.
    EXECUTE format('SELECT coalesce(json_agg(%s), ''[]''::json) FROM unnest($1::items[]) i', proj_expr)
    USING page_rows INTO features;

    RETURN json_build_object(
        'type', 'FeatureCollection',
        'features', coalesce(features, '[]'::json)
    );
END;
$$ LANGUAGE PLPGSQL;

DROP FUNCTION IF EXISTS geojsonsearch;
CREATE OR REPLACE FUNCTION geojsonsearch(
    IN geojson jsonb,
    IN queryhash text,
    IN fields jsonb DEFAULT NULL,
    IN _scanlimit int DEFAULT 10000,
    IN _limit int DEFAULT 100,
    IN _timelimit interval DEFAULT '5 seconds'::interval,
    IN exitwhenfull boolean DEFAULT TRUE,
    IN skipcovered boolean DEFAULT TRUE
) RETURNS json AS $$
    SELECT * FROM geometrysearch(
        st_geomfromgeojson(geojson),
        queryhash,
        fields,
        _scanlimit,
        _limit,
        _timelimit,
        exitwhenfull,
        skipcovered
    );
$$ LANGUAGE SQL;

DROP FUNCTION IF EXISTS xyzsearch;
CREATE OR REPLACE FUNCTION xyzsearch(
    IN _x int,
    IN _y int,
    IN _z int,
    IN queryhash text,
    IN fields jsonb DEFAULT NULL,
    IN _scanlimit int DEFAULT 10000,
    IN _limit int DEFAULT 100,
    IN _timelimit interval DEFAULT '5 seconds'::interval,
    IN exitwhenfull boolean DEFAULT TRUE,
    IN skipcovered boolean DEFAULT TRUE
) RETURNS json AS $$
    SELECT * FROM geometrysearch(
        st_transform(tileenvelope(_z, _x, _y), 4326),
        queryhash,
        fields,
        _scanlimit,
        _limit,
        _timelimit,
        exitwhenfull,
        skipcovered
    );
$$ LANGUAGE SQL;
