-- Search hashing

-- pgstac_hash: sha256 of a UTF8-encoded text, returned as hex.
CREATE OR REPLACE FUNCTION pgstac_hash(data text) RETURNS text AS $$
    SELECT encode(sha256(convert_to(data, 'UTF8')), 'hex');
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE STRICT;

-- search_hash_from_where: produce a deterministic hash from a WHERE clause and optional metadata.
CREATE OR REPLACE FUNCTION search_hash_from_where(_where text, _metadata jsonb DEFAULT '{}'::jsonb) RETURNS text AS $$
    SELECT pgstac_hash(format('%s|%s', _where, coalesce(_metadata, '{}'::jsonb)::text));
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

-- search_hash: produce a deterministic hash from a STAC search JSON.
CREATE OR REPLACE FUNCTION search_hash(_search jsonb, _metadata jsonb DEFAULT '{}'::jsonb) RETURNS text AS $$
    SELECT search_hash_from_where(stac_search_to_where(_search), _metadata);
$$ LANGUAGE SQL STABLE PARALLEL SAFE;

-- Searches cache table stores derived search metadata (WHERE clause, ORDER BY,
-- hash) so that repeated equivalent queries re-use the cached context count.
-- metadata IS NULL = anonymous search (short-lived GC).
CREATE TABLE IF NOT EXISTS searches(
    hash text PRIMARY KEY,
    search jsonb NOT NULL,
    _where text,
    orderby text,
    lastused timestamptz DEFAULT now(),
    usecount bigint DEFAULT 0,
    metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamptz DEFAULT now(),
    statslastupdated timestamptz,
    context_count bigint
);
CREATE INDEX IF NOT EXISTS searches_lastused_anon_idx
    ON searches (lastused) WHERE metadata IS NOT NULL;

-- stac_search_to_where: convert a STAC search JSON to a SQL WHERE clause
-- via the unified CQL2 representation (search_to_cql2 -> cql2_query).
-- Returns ' TRUE ' for an empty/unconstrained search.
CREATE OR REPLACE FUNCTION stac_search_to_where(j jsonb) RETURNS text AS $$
DECLARE _where text := cql2_query(search_to_cql2(j));
BEGIN
    IF _where IS NULL OR btrim(_where) = '' THEN RETURN ' TRUE '; END IF;
    RETURN _where;
END;
$$ LANGUAGE PLPGSQL STABLE;

-- where_stats: estimate or count matching rows for a search, cached in the searches
-- table. The inclamp parameter is a sound partition clamp (collection + datetime range)
-- that lets the planner prune partitions for the count query.
CREATE OR REPLACE FUNCTION where_stats(
    inhash text, inwhere text, updatestats boolean default false,
    conf jsonb default null, inclamp text default null
) RETURNS searches AS $$
DECLARE
    t timestamptz; i interval; explain_json jsonb;
    sw searches%ROWTYPE; sw_statslastupdated timestamptz;
    sw_estimated_count bigint; sw_estimated_cost float;
    _context text := lower(context(conf));
    _stats_ttl interval := context_stats_ttl(conf);
    _estimated_cost_threshold float := context_estimated_cost(conf);
    _estimated_count_threshold int := context_estimated_count(conf);
    ro bool := pgstac.readonly(conf);
BEGIN
    IF updatestats THEN _stats_ttl := '0'::interval; END IF;
    IF _context = 'off' THEN RETURN sw; END IF;
    SELECT * INTO sw FROM searches WHERE hash = inhash;
    IF sw IS NULL THEN sw.hash := inhash; sw._where := inwhere; sw_statslastupdated := NULL;
    ELSE sw_statslastupdated := sw.statslastupdated; END IF;
    IF sw IS NOT NULL AND sw.statslastupdated IS NOT NULL AND sw.context_count IS NOT NULL
       AND now() - sw_statslastupdated <= _stats_ttl THEN RETURN sw; END IF;
    t := clock_timestamp();
    EXECUTE format('EXPLAIN (format json) SELECT 1 FROM items i WHERE %s',
                   concat_ws(' AND ', inclamp, inwhere)) INTO explain_json;
    i := clock_timestamp() - t;
    sw_estimated_count := (explain_json->0->'Plan'->>'Plan Rows')::bigint;
    sw_estimated_cost := (explain_json->0->'Plan'->>'Total Cost')::float;
    IF _context = 'auto' AND sw_estimated_count >= _estimated_count_threshold
       AND sw_estimated_cost >= _estimated_cost_threshold THEN
        sw.context_count := sw_estimated_count;
        IF NOT ro THEN
            UPDATE searches SET statslastupdated = now(), context_count = sw.context_count
            WHERE hash = inhash AND statslastupdated IS NOT DISTINCT FROM sw_statslastupdated
            RETURNING * INTO sw;
            IF sw IS NULL THEN SELECT * INTO sw FROM searches WHERE hash = inhash; END IF;
        END IF;
        RETURN sw;
    END IF;
    t := clock_timestamp();
    EXECUTE format('SELECT count(*) FROM items i WHERE %s', concat_ws(' AND ', inclamp, inwhere))
        INTO sw.context_count;
    i := clock_timestamp() - t;
    IF NOT ro THEN
        UPDATE searches SET statslastupdated = now(), context_count = sw.context_count
        WHERE hash = inhash AND statslastupdated IS NOT DISTINCT FROM sw_statslastupdated
        RETURNING * INTO sw;
        IF sw IS NULL THEN SELECT * INTO sw FROM searches WHERE hash = inhash; END IF;
    END IF;
    RETURN sw;
END;
$$ LANGUAGE PLPGSQL SECURITY DEFINER;

-- register_search: persist a pre-derived search row in the searches cache table
-- (hash, _where, orderby, search, metadata already populated). Best-effort and
-- non-blocking: returns the canonical cached row.
CREATE OR REPLACE FUNCTION register_search(search searches) RETURNS searches AS $$
DECLARE cached_search searches%ROWTYPE;
BEGIN
    IF pgstac.readonly() THEN RETURN search; END IF;
    UPDATE searches SET lastused = now(), usecount = searches.usecount + 1
    WHERE ctid = (SELECT ctid FROM searches WHERE hash = search.hash FOR UPDATE SKIP LOCKED LIMIT 1)
    RETURNING * INTO cached_search;
    IF cached_search IS NULL THEN
        IF pg_try_advisory_xact_lock(hashtext(search.hash)) THEN
            INSERT INTO searches (hash, search, _where, orderby, lastused, usecount, metadata)
                VALUES (search.hash, search.search, search._where, search.orderby, now(), 1, search.metadata)
                ON CONFLICT (hash) DO UPDATE SET lastused = EXCLUDED.lastused, usecount = searches.usecount + 1
                RETURNING * INTO cached_search;
        END IF;
        IF cached_search IS NULL THEN
            SELECT * INTO cached_search FROM searches WHERE hash = search.hash;
        END IF;
    END IF;
    IF cached_search IS NOT NULL THEN
        cached_search._where := search._where;
        cached_search.orderby := search.orderby;
        RETURN cached_search;
    END IF;
    RETURN search;
END;
$$ LANGUAGE PLPGSQL SECURITY DEFINER SET search_path TO pgstac, public;

-- search_query: derive hash/where/orderby from a STAC search JSON, register
-- it, and return only the hash and metadata. Used by external callers.
CREATE OR REPLACE FUNCTION search_query(
    _search jsonb = '{}'::jsonb,
    _metadata jsonb = '{}'::jsonb
) RETURNS TABLE(hash text, metadata jsonb) AS $$
DECLARE search searches%ROWTYPE;
BEGIN
    search.search := _search;
    search.metadata := _metadata;
    search._where := stac_search_to_where(_search);
    search.hash := search_hash_from_where(search._where, search.metadata);
    search.orderby := keyset_orderby(_search);
    search.lastused := now();
    search.usecount := 1;
    search := register_search(search);
    RETURN QUERY SELECT search.hash, search.metadata;
END;
$$ LANGUAGE PLPGSQL SECURITY DEFINER SET search_path TO pgstac, public;

-- search_fromhash: lookup a cached search by its hash.
CREATE OR REPLACE FUNCTION search_fromhash(_hash text) RETURNS searches AS $$
    SELECT * FROM searches WHERE hash = _hash LIMIT 1;
$$ LANGUAGE SQL STRICT;

-- search_from_json: same as search_query — derive hash/where/orderby, register,
-- and return hash + metadata. Used internally by search_page and search_plan.
CREATE OR REPLACE FUNCTION search_from_json(
    _search jsonb DEFAULT '{}'::jsonb,
    _metadata jsonb DEFAULT '{}'::jsonb
) RETURNS TABLE(hash text, metadata jsonb) AS $$
DECLARE search searches%ROWTYPE;
BEGIN
    search.search := _search;
    search.metadata := _metadata;
    search._where := stac_search_to_where(_search);
    search.hash := search_hash_from_where(search._where, search.metadata);
    search.orderby := keyset_orderby(_search);
    search.lastused := now();
    search.usecount := 1;
    search := register_search(search);
    RETURN QUERY SELECT search.hash, search.metadata;
END;
$$ LANGUAGE PLPGSQL SECURITY DEFINER SET search_path TO pgstac, public;

-- field_included: STAC fields include/exclude decision over text[] arrays (the array-form used by
-- the column projector fields_to_itemcols). include_field() is the equivalent over a fields jsonb
-- (used by content_hydrate); both apply the same rule: exclude wins, then an explicit include list
-- restricts to its members, otherwise everything is included.
CREATE OR REPLACE FUNCTION field_included(_field text, _includes text[], _excludes text[])
RETURNS boolean LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE
        WHEN _field = ANY(_excludes) THEN false
        WHEN array_length(_includes, 1) IS NOT NULL THEN _field = ANY(_includes)
        ELSE true END;
$$;

-- needs_fragment: determine whether satisfying the requested fields for the
-- query's collections requires the item_fragments row (shared asset/property/link values).
-- Returns false (fragment not needed) only when an explicit include list can be
-- satisfied from item columns alone.
CREATE OR REPLACE FUNCTION needs_fragment(fields jsonb, _colls text[]) RETURNS boolean AS $$
DECLARE
    includes text[] := ARRAY(SELECT jsonb_array_elements_text(fields->'include'));
    promoted text[] := ARRAY['datetime','start_datetime','end_datetime']
                       || ARRAY(SELECT name FROM promoted_item_property_defs());
    inc text; propkey text; prop_paths text[] := ARRAY[]::text[];
BEGIN
    IF array_length(includes, 1) IS NULL THEN RETURN true; END IF;
    IF _colls IS NULL THEN RETURN true; END IF;
    FOREACH inc IN ARRAY includes LOOP
        IF inc IN ('assets','links','stac_version','stac_extensions','properties')
           OR inc LIKE 'assets.%' OR inc LIKE 'links.%' THEN
            RETURN true;
        ELSIF inc LIKE 'properties.%' THEN
            propkey := substring(inc FROM 'properties\\.(.*)');
            IF NOT (propkey = ANY (promoted)) THEN RETURN true; END IF;
            prop_paths := prop_paths || propkey;
        END IF;
    END LOOP;
    IF array_length(prop_paths, 1) IS NOT NULL AND EXISTS (
        SELECT 1 FROM collections c, unnest(c.fragment_config) cfg(p),
             LATERAL (SELECT fragment_path_array(cfg.p) AS parr) z
        WHERE c.id = ANY (_colls)
          AND z.parr[1] = 'properties'
          AND (array_length(z.parr, 1) = 1 OR z.parr[2] = ANY (prop_paths))
    ) THEN RETURN true; END IF;
    RETURN false;
END;
$$ LANGUAGE PLPGSQL STABLE PARALLEL SAFE;

-- fields_to_itemcols: produce a SELECT list of item columns in attnum order.
-- Heavy columns (geometry, bbox, assets, links, properties, extra) are emitted as
-- NULL::type when their controlling field is excluded/not-included.
CREATE OR REPLACE FUNCTION fields_to_itemcols(fields jsonb DEFAULT '{}'::jsonb) RETURNS text AS $$
DECLARE
    includes text[] := ARRAY(SELECT jsonb_array_elements_text(fields->'include'));
    excludes text[] := ARRAY(SELECT jsonb_array_elements_text(fields->'exclude'));
    cols text;
BEGIN
    SELECT string_agg(
        CASE
          WHEN a.attname = ANY (ARRAY['geometry','bbox','assets','links','link_hrefs',
                                      'extra','properties','stac_version','stac_extensions'])
               AND NOT field_included(CASE WHEN a.attname = 'link_hrefs' THEN 'links' ELSE a.attname END,
                                      includes, excludes)
          THEN format('NULL::%s', format_type(a.atttypid, a.atttypmod))
          ELSE format('i.%I', a.attname)
        END, ', ' ORDER BY a.attnum)
    INTO cols
    FROM pg_attribute a
    WHERE a.attrelid = 'items'::regclass AND a.attnum > 0 AND NOT a.attisdropped;
    RETURN cols;
END;
$$ LANGUAGE PLPGSQL STABLE;

-- search_page: server-hydrate page primitive with keyset pagination and
-- adaptive cumulative-count bands over partition_bounds' per-month histogram.
-- Returns typed page components (features json, count, next/prev tokens) so
-- search() can compose the FeatureCollection envelope without re-parsing json.
CREATE OR REPLACE FUNCTION search_page(
    _search jsonb DEFAULT '{}'::jsonb,
    _limit  int   DEFAULT 100,
    _token  text  DEFAULT NULL,
    _prev   boolean DEFAULT false,
    _fields jsonb DEFAULT '{}'::jsonb,
    OUT features        json,
    OUT number_returned integer,
    OUT next_token      text,
    OUT prev_token      text,
    OUT number_matched  bigint
) RETURNS record LANGUAGE plpgsql SECURITY DEFINER SET search_path TO pgstac, public AS $$
DECLARE
    band_margin CONSTANT numeric := 3.0;
    band_safety CONSTANT numeric := 1.5;
    _where text; _hash text; total_count bigint;
    keyset_w text; full_where text; orderby_str text; keys_proj text;
    lead_field text; eff_lead_dir text; datetime_leading boolean;
    acc json := '[]'::json; cnt bigint := 0; has_more boolean := false;
    first_k text[]; last_k text[]; fwd_first_k text[]; fwd_last_k text[];
    have_row boolean := false; next_tok text; prev_tok text;
    next_present boolean; prev_present boolean;
    _env pred_envelope; _cql2 jsonb;
    bnds record; clamp text; clamped_where text;
    band_cap_months CONSTANT int := 18;
    page_rows items[] := '{}'::items[]; chunk_rows items[];
    target int := _limit + 1; got int := 0; got_band int;
    is_asc boolean; cursor_ts timestamptz; band record;
    band_target numeric; obs_sel numeric; band_where text;
    guard int := 0; cum_scanned bigint := 0;
    proj_expr text; mo interval := interval '1 month';
    cursor_idx int;
BEGIN
    -- The requested STAC `fields` live in the search request; honor them over the (defaulted)
    -- parameter so include/exclude projection is actually applied for search().
    _fields := coalesce(_search->'fields', _fields, '{}'::jsonb);
    _cql2 := search_to_cql2(_search);
    _where := cql2_query(_cql2);
    IF _where IS NULL OR btrim(_where) = '' THEN _where := ' TRUE '; END IF;
    _hash := search_hash_from_where(_where, '{}'::jsonb);
    _env := cql2_envelope(_cql2);

    SELECT * INTO bnds FROM partition_bounds(_env);
    IF bnds.collections IS NOT NULL THEN
        clamp := format('i.collection = ANY (%L::text[])', bnds.collections);
    END IF;

    SELECT string_agg(expr||' '||CASE WHEN _prev THEN (CASE dir WHEN 'ASC' THEN 'DESC' ELSE 'ASC' END) ELSE dir END, ', ' ORDER BY ord),
           'ARRAY['||string_agg(format('(%s)::text', expr), ',' ORDER BY ord)||']::text[]',
           (array_agg(field ORDER BY ord))[1],
           (array_agg(CASE WHEN _prev THEN (CASE dir WHEN 'ASC' THEN 'DESC' ELSE 'ASC' END) ELSE dir END ORDER BY ord))[1]
    INTO orderby_str, keys_proj, lead_field, eff_lead_dir
    FROM keyset_sortkeys(_search);
    datetime_leading := (lead_field = 'datetime');
    is_asc := (eff_lead_dir = 'ASC');

    IF context(_search->'conf') <> 'off' THEN
        DECLARE s searches%ROWTYPE; BEGIN
            s.search := _search; s.metadata := '{}'::jsonb; s._where := _where; s.hash := _hash;
            s.orderby := keyset_orderby(_search); s.lastused := now(); s.usecount := 1;
            PERFORM register_search(s);
        END;
        total_count := (where_stats(_hash, _where, false, _search->'conf', clamp)).context_count;
    END IF;

    IF _token IS NOT NULL THEN
        keyset_w := keyset_where(_search, keyset_decode(_token), _prev);
    END IF;
    full_where := concat_ws(' AND ', _where, keyset_w);
    IF full_where IS NULL OR btrim(full_where) = '' THEN full_where := 'TRUE'; END IF;
    clamped_where := concat_ws(' AND ', clamp, full_where);
    IF clamped_where IS NULL OR btrim(clamped_where) = '' THEN clamped_where := 'TRUE'; END IF;

    -- Per-row projection. When the requested fields can be satisfied from item columns alone
    -- (needs_fragment, evaluated once for the whole query), tell content_hydrate to skip the shared
    -- item_fragments lookup entirely; otherwise it fetches and merges the fragment per row.
    IF needs_fragment(_fields, bnds.collections) THEN
        proj_expr := format('content_hydrate(i, %L::jsonb)', _fields);
    ELSE
        proj_expr := format('content_hydrate(i, %L::jsonb, true)', _fields);
    END IF;

    IF datetime_leading AND array_length(bnds.months, 1) IS NOT NULL THEN
        cursor_ts := CASE WHEN is_asc THEN bnds.months[1] ELSE bnds.months[array_length(bnds.months, 1)] + mo END;
        cursor_idx := 1;
        band_target := target * band_margin;
        WHILE got < target AND guard < 80 LOOP
            guard := guard + 1;
            SELECT * INTO band FROM next_band(bnds.counts, cursor_idx, band_target, band_cap_months);
            -- a valid band must be processed even when next_band also flags done (it consumed the
            -- last bucket); only stop when there is no band at all.
            EXIT WHEN band.band_start_idx IS NULL;
            band_where := format('i.datetime >= %L AND i.datetime < %L AND (%s)',
                bnds.months[band.band_start_idx], bnds.months[band.band_end_idx] + mo, full_where);
            EXECUTE format('SELECT array_agg(i ORDER BY %s) FROM (SELECT * FROM items i WHERE %s ORDER BY %s LIMIT %s) i',
                orderby_str, band_where, orderby_str, target - got) INTO chunk_rows;
            got_band := coalesce(array_length(chunk_rows, 1), 0);
            IF chunk_rows IS NOT NULL THEN page_rows := page_rows || chunk_rows; got := got + got_band; END IF;
            cum_scanned := cum_scanned + band.scanned;
            cursor_idx := band.next_cursor_idx;
            EXIT WHEN got >= target;
            obs_sel := GREATEST(got::numeric, 0.5) / GREATEST(cum_scanned, 1);
            band_target := ((target - got) / obs_sel) * band_safety;
        END LOOP;
    ELSE
        EXECUTE format('SELECT array_agg(i ORDER BY %s) FROM (SELECT * FROM items i WHERE %s ORDER BY %s LIMIT %s) i',
            orderby_str, clamped_where, orderby_str, target) INTO page_rows;
    END IF;

    EXECUTE format($q$
        WITH page AS (
            SELECT %1$s AS content,
                   CASE WHEN row_number() OVER (ORDER BY %2$s) IN (1, %3$s) THEN %4$s END AS keys,
                   row_number() OVER (ORDER BY %2$s) AS rn
            FROM unnest($1::items[]) i
        ),
        counts AS (SELECT count(*) AS n FROM page)
        SELECT
            coalesce(json_agg(content ORDER BY rn) FILTER (WHERE rn <= %3$s), '[]'::json),
            (SELECT n FROM counts),
            (SELECT keys FROM page WHERE rn = 1),
            (SELECT keys FROM page WHERE rn = LEAST(%3$s, (SELECT n FROM counts)::int))
        FROM page
    $q$, proj_expr, orderby_str, _limit, keys_proj)
    USING page_rows INTO acc, cnt, first_k, last_k;

    IF acc IS NULL THEN acc := '[]'::json; END IF;
    has_more := cnt > _limit;
    have_row := cnt > 0;

    IF _prev THEN
        acc := (SELECT coalesce(json_agg(e ORDER BY ord DESC), '[]'::json) FROM json_array_elements(acc) WITH ORDINALITY t(e, ord));
        fwd_first_k := last_k; fwd_last_k := first_k;
        next_present := (_token IS NOT NULL);
        prev_present := has_more;
    ELSE
        fwd_first_k := first_k; fwd_last_k := last_k;
        next_present := has_more;
        prev_present := (_token IS NOT NULL);
    END IF;
    IF have_row AND next_present THEN next_tok := keyset_encode(fwd_last_k); END IF;
    IF have_row AND prev_present THEN prev_tok := keyset_encode(fwd_first_k); END IF;

    features := acc;
    number_returned := json_array_length(acc);
    next_token := next_tok;
    prev_token := prev_tok;
    number_matched := total_count;
END;
$$;

-- search: FeatureCollection API wrapper
CREATE OR REPLACE FUNCTION search(_search jsonb DEFAULT '{}'::jsonb) RETURNS json AS $$
DECLARE
    -- caller-provided limit/token come from the search body; default to the configured page size.
    _limit  int := coalesce((_search->>'limit')::int,
                            nullif(get_setting('default_page_size', _search->'conf'), '')::int, 10);
    _token  text := _search->>'token';
    -- The keyset is the token minus its next/prev prefix; an empty token means no keyset (first
    -- page). A non-empty keyset that does not decode raises in keyset_decode downstream.
    keyset  text := nullif(regexp_replace(coalesce(_token,''), '^(next|prev):', ''), '');
    is_prev boolean := (_token LIKE 'prev:%') AND keyset IS NOT NULL;
    pg record;
    burl text := rtrim(coalesce(base_url(_search->'conf'), ''), '/');
    links jsonb := '[]'::jsonb;
    out json;
BEGIN
    SELECT * INTO pg FROM search_page(_search, _limit, keyset, is_prev);
    links := links
      || jsonb_build_object('rel','root','type','application/json','href', burl)
      || jsonb_build_object('rel','self','type','application/json','href',burl||'/search');
    IF pg.next_token IS NOT NULL THEN
        links := links || jsonb_build_object('rel','next','type','application/geo+json','method','GET',
            'href', burl||'/search?token=next:'||pg.next_token);
    END IF;
    IF pg.prev_token IS NOT NULL THEN
        links := links || jsonb_build_object('rel','prev','type','application/geo+json','method','GET',
            'href', burl||'/search?token=prev:'||pg.prev_token);
    END IF;
    IF pg.number_matched IS NOT NULL THEN
        out := json_build_object(
            'type','FeatureCollection',
            'features', coalesce(pg.features,'[]'::json),
            'links', links,
            'numberReturned', pg.number_returned,
            'numberMatched', pg.number_matched);
    ELSE
        out := json_build_object(
            'type','FeatureCollection',
            'features', coalesce(pg.features,'[]'::json),
            'links', links,
            'numberReturned', pg.number_returned);
    END IF;
    RETURN out;
END;
$$ LANGUAGE PLPGSQL;

-- search_plan: client-streaming entry point
CREATE OR REPLACE FUNCTION search_plan(
    _search jsonb DEFAULT '{}'::jsonb,
    _token  text  DEFAULT NULL,
    _limit  int   DEFAULT NULL,
    OUT query        text,
    OUT histogram    jsonb,
    OUT min_datetime timestamptz,
    OUT max_datetime timestamptz,
    OUT max_count    bigint,
    OUT lead_desc    boolean,
    OUT ctx_query    text,
    OUT datetime_leading boolean,
    OUT context_count    bigint
) RETURNS record LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path TO pgstac, public AS $$
DECLARE
    _cql2   jsonb := search_to_cql2(_search);
    _where  text  := cql2_query(_cql2);
    is_prev boolean := _token LIKE 'prev:%';
    keyset  text  := nullif(regexp_replace(coalesce(_token, ''), '^(next|prev):', ''), '');
    keyset_w text; full_where text; orderby_str text; collist text;
    lead_field text; eff_dir text; _env pred_envelope; _hash text;
    bnds record; coll_clamp text := ''; clamp text;
BEGIN
    IF _where IS NULL OR btrim(_where) = '' THEN _where := ' TRUE '; END IF;
    collist := fields_to_itemcols(coalesce(_search->'fields', '{}'::jsonb));
    orderby_str := keyset_orderby(_search, is_prev);
    SELECT (array_agg(field ORDER BY ord))[1],
           (array_agg(CASE WHEN is_prev THEN (CASE dir WHEN 'ASC' THEN 'DESC' ELSE 'ASC' END) ELSE dir END ORDER BY ord))[1]
    INTO lead_field, eff_dir FROM keyset_sortkeys(_search);
    datetime_leading := (lead_field = 'datetime');
    lead_desc := (eff_dir = 'DESC');

    IF keyset IS NOT NULL THEN
        keyset_w := keyset_where(_search, keyset_decode(keyset), is_prev);
    END IF;
    full_where := concat_ws(' AND ', _where, keyset_w);
    IF full_where IS NULL OR btrim(full_where) = '' THEN full_where := 'TRUE'; END IF;

    _env := cql2_envelope(_cql2);
    SELECT * INTO bnds FROM partition_bounds(_env);
    min_datetime := bnds.months[1];
    max_datetime := bnds.months[array_length(bnds.months, 1)];
    max_count := bnds.total_count;
    IF bnds.collections IS NOT NULL THEN
        clamp := format('i.collection = ANY(%L::text[])', bnds.collections);
    END IF;
    IF bnds.collections IS NOT NULL THEN
        coll_clamp := format('i.collection = ANY(%L::text[]) AND ', bnds.collections);
    END IF;

    IF datetime_leading AND array_length(bnds.months, 1) IS NOT NULL THEN
        query := format(
            'SELECT %s FROM items i WHERE i.collection = ANY($4) AND i.datetime >= $1 AND i.datetime < $2 AND (%s) ORDER BY %s LIMIT $3',
            collist, full_where, orderby_str);
        -- serialize the per-month histogram (months[]/counts[]) to jsonb for the streaming client
        histogram := (
            SELECT jsonb_agg(jsonb_build_object('m', m, 'n', n) ORDER BY ord)
            FROM unnest(bnds.months, bnds.counts) WITH ORDINALITY AS h(m, n, ord)
        );
    ELSE
        DECLARE dt_clamp text := ''; BEGIN
            IF array_length(bnds.months, 1) IS NOT NULL THEN
                dt_clamp := format('i.datetime >= %L AND i.datetime <= %L AND ', bnds.months[1], bnds.months[array_length(bnds.months, 1)]);
            END IF;
            query := format(
                'SELECT %s FROM items i WHERE %s%s(%s) ORDER BY %s LIMIT $1',
                collist, coll_clamp, dt_clamp, full_where, orderby_str);
            histogram := NULL;
        END;
    END IF;

    IF context(_search->'conf') <> 'off' THEN
        _hash := search_hash_from_where(_where, '{}'::jsonb);
        DECLARE s searches%ROWTYPE; BEGIN
            s.search := _search; s.metadata := '{}'::jsonb; s._where := _where; s.hash := _hash;
            s.orderby := keyset_orderby(_search); s.lastused := now(); s.usecount := 1;
            PERFORM register_search(s);
        END;
        -- Inline the cached count when stats are fresh (same rule as where_stats), so the client
        -- can skip ctx_query on a cache hit. NULL => miss/stale => client races ctx_query.
        SELECT s2.context_count INTO context_count
        FROM searches s2
        WHERE s2.hash = _hash
          AND s2.statslastupdated IS NOT NULL
          AND s2.context_count IS NOT NULL
          AND now() - s2.statslastupdated <= context_stats_ttl(_search->'conf');
        ctx_query := format('SELECT (where_stats(%L, %L, false, %L, %L)).context_count',
                            _hash, _where, _search->'conf', clamp);
    ELSE
        ctx_query := NULL;
        context_count := NULL;
    END IF;
END;
$$;
