-- pred_envelope: safe over-approximation of the collections/datetime/end_datetime/geometry
-- extent a search's filter covers. Used by partition_bounds() to prune candidate partitions.
-- NULL = unconstrained (all partitions are candidates).

CREATE TYPE pred_envelope AS (
    colls text[],
    dt    tstzmultirange,
    edt   tstzmultirange,
    geom  geometry
);

-- env_full: unconstrained envelope -- all partitions are candidates.
CREATE OR REPLACE FUNCTION env_full() RETURNS pred_envelope LANGUAGE sql IMMUTABLE AS $$
    SELECT (NULL::text[],
            tstzmultirange(tstzrange('-infinity','infinity','[]')),
            tstzmultirange(tstzrange('-infinity','infinity','[]')), NULL)::pred_envelope;
$$;

-- env_and: intersect every axis (tighter -- a row must satisfy both).
-- An empty collection set means no partition matches.
CREATE OR REPLACE FUNCTION env_and(a pred_envelope, b pred_envelope) RETURNS pred_envelope LANGUAGE sql IMMUTABLE AS $$
    SELECT (
            CASE WHEN a.colls IS NULL THEN b.colls WHEN b.colls IS NULL THEN a.colls
                 ELSE array_intersection(a.colls, b.colls) END,
            a.dt * b.dt, a.edt * b.edt,
            CASE WHEN a.geom IS NULL THEN b.geom WHEN b.geom IS NULL THEN a.geom
                 ELSE ST_Envelope(ST_Intersection(a.geom, b.geom)) END)::pred_envelope;
$$;

-- env_or: union every axis (wider -- a row satisfying either is a candidate).
-- If either side is unconstrained (NULL colls), the result is unconstrained.
CREATE OR REPLACE FUNCTION env_or(a pred_envelope, b pred_envelope) RETURNS pred_envelope LANGUAGE sql IMMUTABLE AS $$
    SELECT (
            CASE WHEN a.colls IS NULL OR b.colls IS NULL THEN NULL
                 ELSE ARRAY(SELECT DISTINCT unnest(a.colls || b.colls)) END,
            a.dt + b.dt, a.edt + b.edt,
            CASE WHEN a.geom IS NULL OR b.geom IS NULL THEN NULL
                 ELSE ST_Envelope(ST_Collect(a.geom, b.geom)) END)::pred_envelope;
$$;

-- cql2_envelope: recursively walk a CQL2 filter tree and produce a pred_envelope.
-- and/or are intersected/unioned; temporal operators constrain datetime/end_datetime;
-- spatial operators constrain the geometry bbox. Unsupported operators return env_full().
CREATE OR REPLACE FUNCTION cql2_envelope(j jsonb) RETURNS pred_envelope LANGUAGE plpgsql STABLE AS $$
DECLARE op text; args jsonb; child jsonb; acc pred_envelope; r tstzrange; prop text; v timestamptz; g geometry;
BEGIN
    IF j IS NULL OR jsonb_typeof(j) <> 'object' OR NOT j ? 'op' THEN RETURN env_full(); END IF;
    op := lower(j->>'op'); args := j->'args';
    IF op = 'and' THEN
        acc := env_full();
        FOR child IN SELECT * FROM jsonb_array_elements(args) LOOP acc := env_and(acc, cql2_envelope(child)); END LOOP;
        RETURN acc;
    ELSIF op = 'or' THEN
        acc := NULL;
        FOR child IN SELECT * FROM jsonb_array_elements(args) LOOP
            acc := CASE WHEN acc IS NULL THEN cql2_envelope(child) ELSE env_or(acc, cql2_envelope(child)) END;
        END LOOP;
        RETURN coalesce(acc, env_full());
    ELSIF op = 'not' THEN RETURN env_full();
    ELSIF op ILIKE 't_%' OR op = 'anyinteracts' THEN
        r := parse_dtrange(args->1); acc := env_full();
        IF op IN ('t_intersects','anyinteracts','t_during','t_equals','t_starts','t_finishes','t_overlaps') THEN
            acc.dt  := tstzmultirange(tstzrange('-infinity', upper(r), '(]'));
            acc.edt := tstzmultirange(tstzrange(lower(r), 'infinity', '[)'));
        ELSIF op IN ('t_before','t_meets') THEN
            acc.edt := tstzmultirange(tstzrange('-infinity', lower(r), '()'));
        ELSIF op IN ('t_after','t_metby') THEN
            acc.dt  := tstzmultirange(tstzrange(upper(r), 'infinity', '()'));
        END IF;
        RETURN acc;
    ELSIF op ILIKE 's_%' OR op = 'intersects' THEN
        BEGIN
            g := ST_GeomFromGeoJSON(args->1);
        EXCEPTION WHEN others THEN
            RAISE EXCEPTION 'Invalid GeoJSON geometry: %', args->1 USING ERRCODE = '22P02';
        END;
        acc := env_full(); acc.geom := ST_Envelope(g); RETURN acc;
    ELSIF op IN ('=','<','<=','>','>=','between','eq','lt','lte','gt','gte','in','like','ilike')
          AND jsonb_typeof(args)='array' AND args->0 ? 'property' THEN
        prop := args->0->>'property'; acc := env_full();
        IF prop IN ('datetime','end_datetime') THEN
            IF op IN ('in','like','ilike') THEN RETURN env_full(); END IF;
            IF op = 'between' THEN          r := tstzrange(cql2_ts(args->1), cql2_ts(args->2), '[]');
            ELSIF op IN ('<','<=','lt','lte') THEN r := tstzrange('-infinity', cql2_ts(args->1), '(]');
            ELSIF op IN ('>','>=','gt','gte') THEN r := tstzrange(cql2_ts(args->1), 'infinity', '[)');
            ELSE v := cql2_ts(args->1);     r := tstzrange(v, v, '[]'); END IF;
            IF prop = 'datetime' THEN acc.dt := tstzmultirange(r); ELSE acc.edt := tstzmultirange(r); END IF;
            RETURN acc;
        ELSIF prop = 'collection' THEN
            acc.colls := cql2_collection_set(op, args); RETURN acc;
        ELSE RETURN env_full(); END IF;
    ELSE RETURN env_full();
    END IF;
END;
$$;

-- Full search envelope: top-level collections/datetime/bbox/intersects AND cql2(filter).
CREATE OR REPLACE FUNCTION search_to_cql2(j jsonb) RETURNS jsonb AS $$
DECLARE
    parts jsonb := '[]'::jsonb;
    fil jsonb;
    filterlang text;
    g geometry;
BEGIN
    IF j ? 'ids' THEN
        parts := parts || jsonb_build_object('op', 'in',
            'args', jsonb_build_array(jsonb_build_object('property', 'id'), j->'ids'));
    END IF;
    IF j ? 'collections' THEN
        parts := parts || jsonb_build_object('op', 'in',
            'args', jsonb_build_array(jsonb_build_object('property', 'collection'), j->'collections'));
    END IF;
    IF j ? 'datetime' THEN
        parts := parts || jsonb_build_object('op', 'anyinteracts',
            'args', jsonb_build_array(jsonb_build_object('property', 'datetime'), j->'datetime'));
    END IF;
    g := stac_geom(j);
    IF g IS NOT NULL THEN
        parts := parts || jsonb_build_object('op', 's_intersects',
            'args', jsonb_build_array(jsonb_build_object('property', 'geometry'), ST_AsGeoJSON(g)::jsonb));
    END IF;
    IF j ? 'q' THEN parts := parts || jsonb_build_object('op', 'q', 'args', j->'q'); END IF;

    IF j ? 'query' AND j ? 'filter' THEN
        RAISE EXCEPTION 'Can only use either query or filter at one time.';
    END IF;
    IF j ? 'query' THEN fil := query_to_cql2(j->'query');
    ELSIF j ? 'filter' THEN
        filterlang := COALESCE(j->>'filter-lang', get_setting('default_filter_lang', j->'conf'));
        IF NOT (j->'filter') @? '$.**.op' OR filterlang = 'cql-json' THEN
            fil := cql1_to_cql2(j->'filter');
        ELSE fil := j->'filter';
        END IF;
    END IF;
    IF fil IS NOT NULL THEN parts := parts || fil; END IF;

    IF jsonb_array_length(parts) = 0 THEN RETURN NULL;
    ELSIF jsonb_array_length(parts) = 1 THEN RETURN parts->0;
    END IF;
    RETURN jsonb_build_object('op', 'and', 'args', parts);
END;
$$ LANGUAGE PLPGSQL STABLE;

-- search_envelope: convert a STAC search JSON to a pred_envelope for partition pruning.
-- Used by tilesearch and external callers that have raw search JSON.
CREATE OR REPLACE FUNCTION search_envelope(j jsonb) RETURNS pred_envelope LANGUAGE sql STABLE AS $$
    SELECT cql2_envelope(search_to_cql2(j));
$$;

-- partition_bounds: read partition_stats once using an envelope. Returns the candidate
-- collections, the per-month row-count histogram as aligned ascending arrays (months[] +
-- counts[]), and the total candidate count. next_band walks counts[] by index; callers map
-- indices back to timestamps via months[]. Each candidate partition's row estimate is prorated
-- across the calendar months its [lo,hi) data extent spans (an instant lo=hi puts all n in its
-- month), then summed per month.
CREATE OR REPLACE FUNCTION partition_bounds(
    _env pred_envelope,
    OUT months    timestamptz[],
    OUT counts    bigint[],
    OUT collections text[],
    OUT total_count bigint
) RETURNS record LANGUAGE sql STABLE AS $$
    WITH cand AS (
        SELECT ps.collection,
               lower(ps.dtrange) AS lo,
               upper(ps.dtrange) AS hi,
               coalesce(ps.n, 0) AS n
        FROM partition_stats ps
        WHERE ((_env).colls IS NULL OR ps.collection = ANY((_env).colls))
          AND (_env).dt && COALESCE(ps.dtrange, tstzrange('-infinity','infinity','[]'))
          AND (_env).edt && COALESCE(ps.edtrange, tstzrange('-infinity','infinity','[]'))
          AND ((_env).geom IS NULL OR ps.spatial IS NULL OR ps.spatial && (_env).geom)
    ),
    monthly AS (
        SELECT date_trunc('month', gs) AS month_start,
               CASE
                 WHEN c.hi <= c.lo THEN c.n::numeric
                 ELSE c.n * (
                        extract(epoch FROM (
                            LEAST(c.hi, date_trunc('month', gs) + interval '1 month')
                          - GREATEST(c.lo, date_trunc('month', gs))))
                      / extract(epoch FROM (c.hi - c.lo)))
               END AS pn
        FROM cand c,
             generate_series(date_trunc('month', c.lo),
                             date_trunc('month', GREATEST(c.lo, c.hi - interval '1 microsecond')),
                             interval '1 month') AS gs
    ),
    buckets AS (
        SELECT month_start, round(sum(pn))::bigint AS n
        FROM monthly
        GROUP BY month_start
    )
    SELECT
        (SELECT array_agg(month_start ORDER BY month_start) FROM buckets),
        (SELECT array_agg(n ORDER BY month_start) FROM buckets),
        (SELECT array_agg(DISTINCT collection) FROM cand),
        (SELECT coalesce(sum(n), 0) FROM cand);
$$;

-- next_band: walk a histogram using array indexes. Given per-month counts, a
-- cursor position, and a target row count, returns the next band's index range.
-- The caller converts band indexes back to timestamps for SQL queries. The band
-- index range [band_start_idx, band_end_idx] is always low..high regardless of
-- direction; only the walk order differs. For a descending search (_descending)
-- the cursor starts at the most recent month and walks toward older months, so
-- the most recent items are collected first.
CREATE OR REPLACE FUNCTION next_band(
    _counts bigint[],
    _cursor_idx int,
    _target numeric,
    _cap_months int,
    _descending boolean DEFAULT false,
    OUT band_start_idx int,
    OUT band_end_idx int,
    OUT scanned bigint,
    OUT next_cursor_idx int,
    OUT done boolean
) RETURNS record LANGUAGE plpgsql STABLE AS $$
DECLARE
    idx int;
    n int;
    cumulative bigint := 0;
BEGIN
    done := false; scanned := 0;
    band_start_idx := NULL; band_end_idx := NULL; next_cursor_idx := _cursor_idx;

    IF _counts IS NULL OR array_length(_counts, 1) IS NULL OR _cursor_idx IS NULL THEN
        done := true;  -- no histogram / no cursor => nothing to walk
        RETURN;
    END IF;
    n := array_length(_counts, 1);

    IF _descending THEN
        -- Walk downward (newest -> oldest). The high bound is the cursor (clamped into range).
        IF _cursor_idx < 1 THEN done := true; RETURN; END IF;
        idx := LEAST(_cursor_idx, n);
        FOR i IN REVERSE idx..GREATEST(1, idx - _cap_months + 1) LOOP
            cumulative := cumulative + _counts[i];
            scanned := scanned + _counts[i];
            IF cumulative >= _target THEN
                band_start_idx := i;      -- low (older) month bound
                band_end_idx := idx;      -- high (newer) month bound
                next_cursor_idx := i - 1;
                IF next_cursor_idx < 1 THEN done := true; END IF;
                RETURN;
            END IF;
        END LOOP;
        -- Target not reached within the cap: end the band at the cap boundary.
        band_start_idx := GREATEST(1, idx - _cap_months + 1);
        band_end_idx := idx;
        next_cursor_idx := band_start_idx - 1;
        done := (band_start_idx <= 1);
        RETURN;
    END IF;

    -- Ascending (oldest -> newest).
    idx := NULL;
    FOR i IN 1..n LOOP
        IF i >= _cursor_idx THEN idx := i; EXIT; END IF;
    END LOOP;
    IF idx IS NULL THEN done := true; next_cursor_idx := _cursor_idx; RETURN; END IF;

    FOR i IN idx..n LOOP
        IF i > idx + _cap_months - 1 THEN EXIT; END IF;
        cumulative := cumulative + _counts[i];
        scanned := scanned + _counts[i];
        IF cumulative >= _target THEN
            band_start_idx := idx;
            band_end_idx := i;
            next_cursor_idx := i + 1;
            IF next_cursor_idx > n THEN done := true; END IF;
            RETURN;
        END IF;
    END LOOP;

    -- Target not reached within the cap: end the band at the cap boundary (not the array end),
    -- so the cap actually limits band width. done only when we've consumed the whole histogram.
    band_start_idx := idx;
    band_end_idx := LEAST(idx + _cap_months - 1, n);
    next_cursor_idx := band_end_idx + 1;
    done := (band_end_idx >= n);
END;
$$;
