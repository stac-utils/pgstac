
-- chunker (v0.10 discovery): prune partition_stats candidates by collection + the
-- predicate envelope (temporal multiranges + spatial bbox), then build merged disjoint
-- datetime bands from the candidates' partition boundaries (contiguous → clean band set).
-- Pure indexed SELECT on partition_stats -- no EXPLAIN and no materialized view.
-- The envelope is a safe over-approximation, so this returns a SUPERSET of touched bands;
-- exact correctness is enforced by each band query's WHERE. NULL data-extents (a freshly
-- created, not-yet-analyzed partition) are treated as unbounded → always included.
CREATE OR REPLACE FUNCTION chunker(
    _env pred_envelope,
    OUT s timestamptz,
    OUT e timestamptz
) RETURNS SETOF record LANGUAGE sql STABLE AS $$
    WITH cand AS (
        SELECT lower(ps.partition_dtrange) AS part_lo, upper(ps.partition_dtrange) AS part_hi
        FROM partition_stats ps
        WHERE ((_env).colls IS NULL OR ps.collection = ANY ((_env).colls))
          AND (_env).dt  && COALESCE(ps.dtrange,  tstzrange('-infinity', 'infinity', '[]'))
          AND (_env).edt && COALESCE(ps.edtrange, tstzrange('-infinity', 'infinity', '[]'))
          AND ((_env).geom IS NULL OR ps.spatial IS NULL OR ps.spatial && (_env).geom)
    ),
    bnds AS (
        SELECT part_lo AS t FROM cand WHERE part_lo IS NOT NULL
        UNION SELECT part_hi FROM cand WHERE part_hi IS NOT NULL
    ),
    uniq AS (SELECT DISTINCT t FROM bnds),
    bands AS (SELECT t AS s, lead(t) OVER (ORDER BY t) AS e FROM uniq)
    SELECT b.s, b.e FROM bands b WHERE b.e IS NOT NULL;
$$;

CREATE OR REPLACE FUNCTION q_to_tsquery (jinput jsonb)
    RETURNS tsquery
    AS $$
DECLARE
    input text;
    processed_text text;
    temp_text text;
    quote_array text[];
    placeholder text := '@QUOTE@';
BEGIN
    IF jsonb_typeof(jinput) = 'string' THEN
        input := jinput->>0;
    ELSIF jsonb_typeof(jinput) = 'array' THEN
        input := array_to_string(
            array(select jsonb_array_elements_text(jinput)),
            ' OR '
        );
    ELSE
        RAISE EXCEPTION 'Input must be a string or an array of strings.';
    END IF;
    -- Extract all quoted phrases and store in array
    quote_array := regexp_matches(input, '"[^"]*"', 'g');

    -- Replace each quoted part with a unique placeholder if there are any quoted phrases
    IF array_length(quote_array, 1) IS NOT NULL THEN
        processed_text := input;
        FOR i IN array_lower(quote_array, 1) .. array_upper(quote_array, 1) LOOP
            processed_text := replace(processed_text, quote_array[i], placeholder || i || placeholder);
        END LOOP;
    ELSE
        processed_text := input;
    END IF;

    -- Replace non-quoted text using regular expressions

    -- , -> |
    processed_text := regexp_replace(processed_text, ',(?=(?:[^"]*"[^"]*")*[^"]*$)', ' | ', 'g');

    -- and -> &
    processed_text := regexp_replace(processed_text, '\s+AND\s+', ' & ', 'gi');

    -- or -> |
    processed_text := regexp_replace(processed_text, '\s+OR\s+', ' | ', 'gi');

    -- + ->
    processed_text := regexp_replace(processed_text, '^\s*\+([a-zA-Z0-9_]+)', '\1', 'g'); -- +term at start
    processed_text := regexp_replace(processed_text, '\s*\+([a-zA-Z0-9_]+)', ' & \1', 'g'); -- +term elsewhere

    -- - ->  !
    processed_text := regexp_replace(processed_text, '^\s*\-([a-zA-Z0-9_]+)', '! \1', 'g'); -- -term at start
    processed_text := regexp_replace(processed_text, '\s*\-([a-zA-Z0-9_]+)', ' & ! \1', 'g'); -- -term elsewhere

    -- terms separated with spaces are assumed to represent adjacent terms. loop through these
    -- occurrences and replace them with the adjacency operator (<->)
    LOOP
        temp_text := regexp_replace(processed_text, '([a-zA-Z0-9_]+)\s+([a-zA-Z0-9_]+)(?!\s*[&|<>])', '\1 <-> \2', 'g');
        IF temp_text = processed_text THEN
            EXIT; -- No more replacements were made
        END IF;
        processed_text := temp_text;
    END LOOP;


    -- Replace placeholders back with quoted phrases if there were any
    IF array_length(quote_array, 1) IS NOT NULL THEN
        FOR i IN array_lower(quote_array, 1) .. array_upper(quote_array, 1) LOOP
            processed_text := replace(processed_text, placeholder || i || placeholder, '''' || substring(quote_array[i] from 2 for length(quote_array[i]) - 2) || '''');
        END LOOP;
    END IF;

    RETURN to_tsquery('english', processed_text);
END;
$$
LANGUAGE plpgsql;


-- Build the SQL WHERE clause for a STAC search. Every parameter (ids, collections,
-- datetime, bbox/intersects, q, query, filter) is normalized into one CQL2 filter by
-- search_to_cql2() and translated by cql2_query() -- the same single representation
-- that drives partition pruning via cql2_envelope(). Returns ' TRUE ' for a match-all
-- (empty) search.
CREATE OR REPLACE FUNCTION stac_search_to_where(j jsonb) RETURNS text AS $$
DECLARE
    _where text := cql2_query(search_to_cql2(j));
BEGIN
    IF _where IS NULL OR BTRIM(_where) = '' THEN
        RETURN ' TRUE ';
    END IF;
    RETURN _where;
END;
$$ LANGUAGE PLPGSQL STABLE;


CREATE OR REPLACE FUNCTION parse_sort_dir(_dir text, reverse boolean default false) RETURNS text AS $$
    WITH t AS (
        SELECT COALESCE(upper(_dir), 'ASC') as d
    ) SELECT
        CASE
            WHEN NOT reverse THEN d
            WHEN d = 'ASC' THEN 'DESC'
            WHEN d = 'DESC' THEN 'ASC'
        END
    FROM t;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION sort_dir_to_op(_dir text, prev boolean default false) RETURNS text AS $$
    WITH t AS (
        SELECT COALESCE(upper(_dir), 'ASC') as d
    ) SELECT
        CASE
            WHEN d = 'ASC' AND prev THEN '<='
            WHEN d = 'DESC' AND prev THEN '>='
            WHEN d = 'ASC' THEN '>='
            WHEN d = 'DESC' THEN '<='
        END
    FROM t;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;


CREATE OR REPLACE FUNCTION sort_sqlorderby(
    _search jsonb DEFAULT NULL,
    reverse boolean DEFAULT FALSE
) RETURNS text AS $$
    WITH sortby AS (
        SELECT coalesce(_search->'sortby','[{"field":"datetime", "direction":"desc"}]') as sort
    ), withid AS (
        SELECT CASE
            WHEN sort @? '$[*] ? (@.field == "id")' THEN sort
            ELSE sort || '[{"field":"id", "direction":"desc"}]'::jsonb
            END as sort
        FROM sortby
    ), withid_rows AS (
        SELECT jsonb_array_elements(sort) as value FROM withid
    ),sorts AS (
        SELECT
            coalesce(
                (queryable(value->>'field')).expression
            ) as key,
            parse_sort_dir(value->>'direction', reverse) as dir
        FROM withid_rows
    )
    SELECT array_to_string(
        array_agg(concat(key, ' ', dir)),
        ', '
    ) FROM sorts;
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION get_sort_dir(sort_item jsonb) RETURNS text AS $$
    SELECT CASE WHEN sort_item->>'direction' ILIKE 'desc%' THEN 'DESC' ELSE 'ASC' END;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;


CREATE OR REPLACE FUNCTION  get_token_val_str(
    _field text,
    _item items
) RETURNS text AS $$
DECLARE
    q text;
    literal text;
BEGIN
    q := format($q$ SELECT quote_literal(%s) FROM (SELECT $1.*) as r;$q$, _field);
    EXECUTE q INTO literal USING _item;
    RETURN literal;
END;
$$ LANGUAGE PLPGSQL;



CREATE OR REPLACE FUNCTION get_token_record(IN _token text, OUT prev BOOLEAN, OUT item items) RETURNS RECORD AS $$
DECLARE
    _itemid text := _token;
    _collectionid text;
BEGIN
    IF _token IS NULL THEN
        RETURN;
    END IF;
    RAISE NOTICE 'Looking for token: %', _token;
    prev := FALSE;
    IF _token ILIKE 'prev:%' THEN
        _itemid := replace(_token, 'prev:','');
        prev := TRUE;
    ELSIF _token ILIKE 'next:%' THEN
        _itemid := replace(_token, 'next:', '');
    END IF;
    SELECT id INTO _collectionid FROM collections WHERE _itemid LIKE concat(id,':%');
    IF FOUND THEN
        _itemid := replace(_itemid, concat(_collectionid,':'), '');
        SELECT * INTO item FROM items WHERE id=_itemid AND collection=_collectionid;
    ELSE
        SELECT * INTO item FROM items WHERE id=_itemid;
    END IF;
    IF item IS NULL THEN
        RAISE EXCEPTION 'Could not find item using token: % item: % collection: %', _token, _itemid, _collectionid;
    END IF;
    RETURN;
END;
$$ LANGUAGE PLPGSQL STABLE STRICT;


CREATE OR REPLACE FUNCTION get_token_filter(
    _sortby jsonb DEFAULT '[{"field":"datetime","direction":"desc"}]'::jsonb,
    token_item items DEFAULT NULL,
    prev boolean DEFAULT FALSE,
    inclusive boolean DEFAULT FALSE
) RETURNS text AS $$
DECLARE
    ltop text := '<';
    gtop text := '>';
    dir text;
    sort record;
    orfilter text := '';
    orfilters text[] := '{}'::text[];
    andfilters text[] := '{}'::text[];
    output text;
    token_where text;
BEGIN
    IF _sortby IS NULL OR _sortby = '[]'::jsonb THEN
        _sortby := '[{"field":"datetime","direction":"desc"}]'::jsonb;
    END IF;
    _sortby := _sortby || jsonb_build_object('field','id','direction',_sortby->0->>'direction');
    RAISE NOTICE 'Getting Token Filter. % %', _sortby, token_item;
    IF inclusive THEN
        orfilters := orfilters || format('( id=%L AND collection=%L )' , token_item.id, token_item.collection);
    END IF;

    FOR sort IN
        WITH s1 AS (
            SELECT
                _row,
                (queryable(value->>'field')).expression as _field,
                (value->>'field' = 'id') as _isid,
                get_sort_dir(value) as _dir
            FROM jsonb_array_elements(_sortby)
            WITH ORDINALITY AS t(value, _row)
        )
        SELECT
            _row,
            _field,
            _dir,
            get_token_val_str(_field, token_item) as _val
        FROM s1
        WHERE _row <= (SELECT min(_row) FROM s1 WHERE _isid)
    LOOP
        orfilter := NULL;
        RAISE NOTICE 'SORT: %', sort;
        IF sort._val IS NOT NULL AND  ((prev AND sort._dir = 'ASC') OR (NOT prev AND sort._dir = 'DESC')) THEN
            orfilter := format($f$(
                (%s %s %s) OR (%s IS NULL)
            )$f$,
            sort._field,
            ltop,
            sort._val,
            sort._val
            );
        ELSIF sort._val IS NULL AND  ((prev AND sort._dir = 'ASC') OR (NOT prev AND sort._dir = 'DESC')) THEN
            RAISE NOTICE '< but null';
            orfilter := format('%s IS NOT NULL', sort._field);
        ELSIF sort._val IS NULL THEN
            RAISE NOTICE '> but null';
        ELSE
            orfilter := format($f$(
                (%s %s %s) OR (%s IS NULL)
            )$f$,
            sort._field,
            gtop,
            sort._val,
            sort._field
            );
        END IF;
        RAISE NOTICE 'ORFILTER: %', orfilter;

        IF orfilter IS NOT NULL THEN
            IF sort._row = 1 THEN
                orfilters := orfilters || orfilter;
            ELSE
                orfilters := orfilters || format('(%s AND %s)', array_to_string(andfilters, ' AND '), orfilter);
            END IF;
        END IF;
        IF sort._val IS NOT NULL THEN
            andfilters := andfilters || format('%s = %s', sort._field, sort._val);
        ELSE
            andfilters := andfilters || format('%s IS NULL', sort._field);
        END IF;
    END LOOP;

    output := array_to_string(orfilters, ' OR ');

    token_where := concat('(',coalesce(output,'true'),')');
    IF trim(token_where) = '' THEN
        token_where := NULL;
    END IF;
    RAISE NOTICE 'TOKEN_WHERE: %',token_where;
    RETURN token_where;
    END;
$$ LANGUAGE PLPGSQL SET transform_null_equals TO TRUE
;

-- ============================================================================
-- Search Hashing
-- ============================================================================

CREATE OR REPLACE FUNCTION pgstac_hash(data text) RETURNS text AS $$
    SELECT encode(sha256(convert_to(data, 'UTF8')), 'hex');
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE STRICT;

-- Central hash helper: one canonical where-clause + metadata payload to hash.
CREATE OR REPLACE FUNCTION search_hash_from_where(_where text, _metadata jsonb DEFAULT '{}'::jsonb) RETURNS text AS $$
    SELECT pgstac_hash(
        format(
            '%s|%s',
            _where,
            coalesce(_metadata, '{}'::jsonb)::text
        )
    );
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION search_hash(_search jsonb, _metadata jsonb DEFAULT '{}'::jsonb) RETURNS text AS $$
    SELECT search_hash_from_where(
        stac_search_to_where(_search),
        _metadata
    );
$$ LANGUAGE SQL STABLE PARALLEL SAFE;

-- ============================================================================
-- Search Cache Table
-- ============================================================================

-- Search lifecycle and context cache now live on searches; search_wheres is retired.
CREATE TABLE IF NOT EXISTS searches(
    hash text PRIMARY KEY,
    name text UNIQUE,
    search jsonb NOT NULL,
    _where text,
    orderby text,
    lastused timestamptz DEFAULT now(),
    usecount bigint DEFAULT 0,
    metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
    pinned boolean NOT NULL DEFAULT false,
    created_at timestamptz DEFAULT now(),
    statslastupdated timestamptz,
    context_count bigint
);
CREATE INDEX IF NOT EXISTS searches_lastused_anon_idx
    ON searches (lastused) WHERE name IS NULL AND NOT pinned;

DROP TABLE IF EXISTS search_wheres;

-- ============================================================================
-- Context Stats (estimate/count/TTL)
-- ============================================================================

CREATE OR REPLACE FUNCTION where_stats(
    inhash text,
    inwhere text,
    updatestats boolean default false,
    conf jsonb default null
) RETURNS searches AS $$
DECLARE
    t timestamptz;
    i interval;
    explain_json jsonb;
    sw searches%ROWTYPE;
    sw_statslastupdated timestamptz;
    sw_estimated_count bigint;
    sw_estimated_cost float;
    _context text := lower(context(conf));
    _stats_ttl interval := context_stats_ttl(conf);
    _estimated_cost_threshold float := context_estimated_cost(conf);
    _estimated_count_threshold int := context_estimated_count(conf);
    ro bool := pgstac.readonly(conf);
BEGIN
    -- If updatestats is true then set ttl to 0
    IF updatestats THEN
        RAISE DEBUG 'Updatestats set to TRUE, setting TTL to 0';
        _stats_ttl := '0'::interval;
    END IF;

    -- If we don't need to calculate context, just return
    IF _context = 'off' THEN
        RETURN sw;
    END IF;

    -- Read current stats state without holding row locks during expensive
    -- estimate/count operations.
    SELECT * INTO sw FROM searches WHERE hash = inhash;

    IF sw IS NULL THEN
        -- In read-only mode, searches may not be persisted. Continue with
        -- non-persistent estimate/count calculation so context can still be
        -- returned to callers.
        sw.hash := inhash;
        sw._where := inwhere;
        sw_statslastupdated := NULL;
    ELSE
        sw_statslastupdated := sw.statslastupdated;
    END IF;

    -- If there is a cached row, figure out if we need to update
    IF
        sw IS NOT NULL
        AND sw.statslastupdated IS NOT NULL
        AND sw.context_count IS NOT NULL
        AND now() - sw.statslastupdated <= _stats_ttl
    THEN
        -- We have a cached row with data that is within our ttl.
        RAISE DEBUG 'Stats present in table and lastupdated within ttl: %', sw;
        RAISE DEBUG 'Returning cached counts. %', sw;
        RETURN sw;
    END IF;

    -- Calculate estimated cost and rows
    -- Use explain to get estimated count/cost
    RAISE DEBUG 'Calculating estimated stats';
    t := clock_timestamp();
    EXECUTE format('EXPLAIN (format json) SELECT 1 FROM items WHERE %s', inwhere)
        INTO explain_json;
    RAISE DEBUG 'Time for just the explain: %', clock_timestamp() - t;
    i := clock_timestamp() - t;

    sw_estimated_count := (explain_json->0->'Plan'->>'Plan Rows')::bigint;
    sw_estimated_cost := (explain_json->0->'Plan'->>'Total Cost')::float;

    RAISE DEBUG 'ESTIMATED_COUNT: %, THRESHOLD %', sw_estimated_count, _estimated_count_threshold;
    RAISE DEBUG 'ESTIMATED_COST: %, THRESHOLD %', sw_estimated_cost, _estimated_cost_threshold;

    -- If context is set to auto and the costs are within the threshold return the estimated costs
    IF
        _context = 'auto'
        AND sw_estimated_count >= _estimated_count_threshold
        AND sw_estimated_cost >= _estimated_cost_threshold
    THEN
        sw.context_count := sw_estimated_count;
        IF NOT ro THEN
            UPDATE searches SET
                statslastupdated = now(),
                context_count = sw.context_count
            WHERE
                hash = inhash
                AND statslastupdated IS NOT DISTINCT FROM sw_statslastupdated
            RETURNING * INTO sw;

            IF sw IS NULL THEN
                SELECT * INTO sw FROM searches WHERE hash = inhash;
            END IF;
        END IF;
        RAISE DEBUG 'Estimates are within thresholds, returning estimates. %', sw;
        RETURN sw;
    END IF;

    -- Calculate Actual Count
    t := clock_timestamp();
    RAISE NOTICE 'Calculating actual count...';
    EXECUTE format(
        'SELECT count(*) FROM items WHERE %s',
        inwhere
    ) INTO sw.context_count;
    i := clock_timestamp() - t;
    RAISE NOTICE 'Actual Count: % -- %', sw.context_count, i;

    IF NOT ro THEN
        UPDATE searches SET
            statslastupdated = now(),
            context_count = sw.context_count
        WHERE
            hash = inhash
            AND statslastupdated IS NOT DISTINCT FROM sw_statslastupdated
        RETURNING * INTO sw;

        IF sw IS NULL THEN
            SELECT * INTO sw FROM searches WHERE hash = inhash;
        END IF;
    END IF;
    RAISE DEBUG 'Returning with actual count. %', sw;
    RETURN sw;
END;
$$ LANGUAGE PLPGSQL SECURITY DEFINER;


-- ============================================================================
-- Search Cache Lifecycle (create, name, pin, GC)
-- ============================================================================

DROP FUNCTION IF EXISTS search_query(jsonb, boolean, jsonb);

CREATE OR REPLACE FUNCTION search_query(
    _search jsonb = '{}'::jsonb,
    updatestats boolean = false,
    _metadata jsonb = '{}'::jsonb
) RETURNS searches AS $$
DECLARE
    search searches%ROWTYPE;
    cached_search searches%ROWTYPE;
    search_where searches%ROWTYPE;
    ro boolean := pgstac.readonly();
BEGIN
    RAISE NOTICE 'SEARCH: %', _search;
    -- Calculate hash, where clause, and order by statement
    search.search := _search;
    search.metadata := _metadata;
    search._where := stac_search_to_where(_search);
    search.hash := search_hash_from_where(search._where, search.metadata);
    search.orderby := sort_sqlorderby(_search);
    search.lastused := now();
    search.usecount := 1;

    -- If we are in read only mode, directly return search
    IF ro THEN
        RETURN search;
    END IF;

    -- Cache bookkeeping is best-effort and non-blocking. We always return
    -- canonical hash + where, even if cache touch cannot be acquired quickly.
    UPDATE searches
    SET
        lastused = now(),
        usecount = searches.usecount + 1
    WHERE ctid = (
        SELECT ctid
        FROM searches
        WHERE hash = search.hash
        FOR UPDATE SKIP LOCKED
        LIMIT 1
    )
    RETURNING * INTO cached_search;

    IF cached_search IS NULL THEN
        IF pg_try_advisory_xact_lock(hashtext(search.hash)) THEN
            INSERT INTO searches (hash, search, _where, orderby, lastused, usecount, metadata)
                VALUES (search.hash, search.search, search._where, search.orderby, now(), 1, search.metadata)
                ON CONFLICT (hash) DO UPDATE SET
                    lastused = EXCLUDED.lastused,
                    usecount = searches.usecount + 1
                RETURNING * INTO cached_search;
        END IF;

        IF cached_search IS NULL THEN
            SELECT * INTO cached_search FROM searches WHERE hash = search.hash;
        END IF;
    END IF;

    IF cached_search IS NOT NULL THEN
        cached_search._where = search._where;
        cached_search.orderby = search.orderby;
        IF updatestats THEN
            search_where := where_stats(
                cached_search.hash,
                cached_search._where,
                true,
                _search->'conf'
            );
            cached_search.context_count := search_where.context_count;
            cached_search.statslastupdated := search_where.statslastupdated;
        END IF;
        RETURN cached_search;
    END IF;

    IF updatestats THEN
        search_where := where_stats(
            search.hash,
            search._where,
            true,
            _search->'conf'
        );
        search.context_count := search_where.context_count;
        search.statslastupdated := search_where.statslastupdated;
    END IF;

    RETURN search;

END;
$$ LANGUAGE PLPGSQL SECURITY DEFINER;

CREATE OR REPLACE FUNCTION search_fromhash(
    _hash text
) RETURNS searches AS $$
    SELECT * FROM searches WHERE hash = _hash LIMIT 1;
$$ LANGUAGE SQL STRICT;

CREATE OR REPLACE FUNCTION name_search(
    _search jsonb,
    _name text,
    _metadata jsonb DEFAULT '{}'::jsonb
) RETURNS searches AS $$
DECLARE
    named searches%ROWTYPE;
BEGIN
    named := search_query(_search, false, _metadata);
    UPDATE searches
    SET
        name = _name,
        lastused = now(),
        usecount = searches.usecount + 1
    WHERE hash = named.hash
    RETURNING * INTO named;

    IF named IS NULL THEN
        RAISE EXCEPTION 'Could not name search for input: %', _search;
    END IF;

    RETURN named;
END;
$$ LANGUAGE PLPGSQL SECURITY DEFINER;

CREATE OR REPLACE FUNCTION rename_search(_old_name text, _new_name text) RETURNS searches AS $$
DECLARE
    renamed searches%ROWTYPE;
BEGIN
    -- Serialize rename-pair operations to avoid deadlocks on concurrent name swaps.
    PERFORM pg_advisory_xact_lock(
        hashtext(
            least(_old_name, _new_name)
            || '|'
            || greatest(_old_name, _new_name)
        )
    );

    UPDATE searches
    SET
        name = _new_name,
        lastused = now(),
        usecount = searches.usecount + 1
    WHERE name = _old_name
    RETURNING * INTO renamed;

    IF renamed IS NULL THEN
        RAISE EXCEPTION 'Named search % not found', _old_name;
    END IF;

    RETURN renamed;
END;
$$ LANGUAGE PLPGSQL SECURITY DEFINER;

CREATE OR REPLACE FUNCTION unname_search(_name text) RETURNS searches AS $$
DECLARE
    unnamed searches%ROWTYPE;
BEGIN
    UPDATE searches
    SET
        name = NULL,
        pinned = false,
        lastused = now(),
        usecount = searches.usecount + 1
    WHERE name = _name
    RETURNING * INTO unnamed;

    IF unnamed IS NULL THEN
        RAISE EXCEPTION 'Named search % not found', _name;
    END IF;

    RETURN unnamed;
END;
$$ LANGUAGE PLPGSQL SECURITY DEFINER;

CREATE OR REPLACE FUNCTION pin_search(_name text) RETURNS searches AS $$
DECLARE
    pinned_search searches%ROWTYPE;
BEGIN
    UPDATE searches
    SET
        pinned = true,
        lastused = now(),
        usecount = searches.usecount + 1
    WHERE name = _name
    RETURNING * INTO pinned_search;

    IF pinned_search IS NULL THEN
        RAISE EXCEPTION 'Named search % not found', _name;
    END IF;

    RETURN pinned_search;
END;
$$ LANGUAGE PLPGSQL SECURITY DEFINER;

CREATE OR REPLACE FUNCTION unpin_search(_name text) RETURNS searches AS $$
DECLARE
    unpinned_search searches%ROWTYPE;
BEGIN
    UPDATE searches
    SET
        pinned = false,
        lastused = now(),
        usecount = searches.usecount + 1
    WHERE name = _name
    RETURNING * INTO unpinned_search;

    IF unpinned_search IS NULL THEN
        RAISE EXCEPTION 'Named search % not found', _name;
    END IF;

    RETURN unpinned_search;
END;
$$ LANGUAGE PLPGSQL SECURITY DEFINER;

CREATE OR REPLACE FUNCTION gc_anonymous_searches(retention_interval interval DEFAULT NULL, conf jsonb DEFAULT NULL) RETURNS bigint AS $$
    WITH effective_retention AS (
        SELECT COALESCE(
            retention_interval,
            search_gc_retention_interval(conf)
        ) AS i
    ),
    deleted AS (
        DELETE FROM searches
        USING effective_retention
        WHERE
            name IS NULL
            AND NOT pinned
            AND lastused < now() - effective_retention.i
        RETURNING 1
    )
    SELECT count(*)::bigint FROM deleted;
$$ LANGUAGE SQL SECURITY DEFINER;

CREATE OR REPLACE FUNCTION gc_search_caches(retention_interval interval DEFAULT NULL, conf jsonb DEFAULT NULL) RETURNS jsonb AS $$
    SELECT jsonb_build_object(
        'removed_searches',
        gc_anonymous_searches(retention_interval, conf)
    );
$$ LANGUAGE SQL SECURITY DEFINER;



-- fields_to_columns: Map a STAC `fields` include/exclude spec to a SQL projection
-- string for use in `row`-mode streaming queries.
--
-- The returned string is a comma-separated column list suitable for embedding
-- directly in a SELECT.  Control columns (id, collection, datetime,
-- end_datetime, fragment_id, item_hash, pgstac_updated_at, datetime_is_range,
-- private) are always included regardless of the fields spec because the client
-- needs them for rehydration, token minting, and ordering.
--
-- When no `include` list is supplied the function returns `i.*` plus correlated
-- subqueries for the fragment columns (frag_content, frag_links_template).
-- Fragment columns use correlated subqueries rather than JOIN aliases so the
-- returned projection string is self-contained: search_sql can use it in a
-- plain `FROM items i` query without adding item_fragments to the outer FROM
-- clause, which would cause column-ambiguity errors (both tables have `id` and
-- `collection`) in the WHERE clause generated by stac_search_to_where.
CREATE OR REPLACE FUNCTION fields_to_columns(fields jsonb DEFAULT '{}'::jsonb) RETURNS text AS $$
DECLARE
    includes text[];
    need_frag boolean;
    cols text[];
BEGIN
    includes := ARRAY(SELECT jsonb_array_elements_text(fields->'include'));

    -- Fast path: no include list → return everything.
    IF array_length(includes, 1) IS NULL THEN
        RETURN $proj$i.*,
            (SELECT content FROM item_fragments WHERE id = i.fragment_id) AS frag_content,
            (SELECT links_template FROM item_fragments WHERE id = i.fragment_id) AS frag_links_template$proj$;
    END IF;

    -- Control columns are always projected.
    cols := ARRAY[
        'i.id', 'i.collection', 'i.datetime', 'i.end_datetime',
        'i.fragment_id', 'i.item_hash', 'i.pgstac_updated_at',
        'i.datetime_is_range', 'i.private'
    ];

    -- Fragment is needed when any asset, link, or fragment-stored-property field
    -- is requested.  Over-including is safe; under-including breaks rehydration.
    need_frag := 'assets'      = ANY(includes)
              OR 'links'       = ANY(includes)
              OR 'properties'  = ANY(includes)
              OR EXISTS (
                    SELECT 1 FROM unnest(includes) AS inc
                    WHERE inc LIKE 'properties.%'
                 );

    -- Use array_append for single-element additions to avoid type-resolution
    -- ambiguity: `cols || 'literal'` with cols text[] causes PostgreSQL to
    -- attempt parsing the literal as an array literal, which fails.
    IF 'geometry'         = ANY(includes) THEN cols := array_append(cols, 'i.geometry'); END IF;
    IF 'bbox'             = ANY(includes) THEN cols := array_append(cols, 'i.bbox'); END IF;
    IF 'links'            = ANY(includes) THEN cols := cols || ARRAY['i.links', 'i.link_hrefs']; END IF;
    IF 'assets'           = ANY(includes) THEN cols := array_append(cols, 'i.assets'); END IF;
    IF 'extra'            = ANY(includes) THEN cols := array_append(cols, 'i.extra'); END IF;
    IF 'stac_version'     = ANY(includes) THEN cols := array_append(cols, 'i.stac_version'); END IF;
    IF 'stac_extensions'  = ANY(includes) THEN cols := array_append(cols, 'i.stac_extensions'); END IF;

    -- `properties` or `properties.<x>`: include the jsonb blob and all
    -- promoted scalar columns so the client can reassemble properties.
    IF 'properties' = ANY(includes)
    OR EXISTS (SELECT 1 FROM unnest(includes) AS inc WHERE inc LIKE 'properties.%')
    THEN
        cols := array_append(cols, 'i.properties');
        cols := cols || ARRAY(
            SELECT format('i.%I', p.property_path)
            FROM promoted_item_property_defs() p
        );
    ELSE
        -- Pull only the promoted columns explicitly requested as properties.*
        cols := cols || ARRAY(
            SELECT format('i.%I', p.property_path)
            FROM promoted_item_property_defs() p
            WHERE format('properties.%s', p.name) = ANY(includes)
        );
    END IF;

    IF need_frag THEN
        cols := cols || ARRAY[
            $fc$(SELECT content FROM item_fragments WHERE id = i.fragment_id) AS frag_content$fc$,
            $fl$(SELECT links_template FROM item_fragments WHERE id = i.fragment_id) AS frag_links_template$fl$
        ];
    END IF;

    RETURN array_to_string(cols, ', ');
END;
$$ LANGUAGE PLPGSQL STABLE PARALLEL SAFE;


-- collection_fragments_properties: does this collection's fragment_config fragment
-- any `properties.*` path? Used by fields_to_rowjsonb to decide whether a property
-- request needs the shared fragment. Conservative (TRUE) for an unknown / multi-
-- collection search (NULL arg) since we cannot prove the fragment is unneeded. The
-- default fragment_config fragments assets only, so this is FALSE for typical
-- collections — property requests then skip the fragment entirely.
CREATE OR REPLACE FUNCTION collection_fragments_properties(_collection text) RETURNS boolean AS $$
    SELECT CASE WHEN _collection IS NULL THEN true ELSE EXISTS (
        SELECT 1 FROM collections c, unnest(c.fragment_config) p
        WHERE c.id = _collection AND (fragment_path_array(p))[1] = 'properties'
    ) END;
$$ LANGUAGE sql STABLE;


-- fields_to_rowjsonb: row-mode jsonb projection that HONORS STAC `fields`
-- include/exclude, so a thin item does not drag heavy columns (geometry, assets,
-- properties, links) or the shared fragment over the wire. Used by search_page
-- row mode. Rules:
--   - Always ship the small control scalars the client needs to hydrate/serialize
--     and resume: id, collection, datetime, end_datetime, datetime_is_range,
--     fragment_id. (Keyset tokens come from search_page's separate `keys`, not
--     from this content.)
--   - Add a heavy column only when its field is requested (include) / not excluded.
--   - Fetch the inline `_fragment` ONLY when assets/links/properties/root-keys are
--     requested — those can live in the shared fragment (per fragment_config). This
--     is the deliberate "overfetch a little" (whole fragment) that saves a per-key
--     join, but it is skipped entirely for fields that never touch the fragment.
--   - Keys are raw column names; the client maps them to STAC exactly as
--     promoted_item_property_defs() / content_hydrate do. jsonb_strip_nulls keeps
--     null columns off the wire.
--   - Fragment-config precision for a SPECIFIC `properties.<x>` request: a promoted
--     property ships just its own column (not all ~37 promoted + the properties
--     jsonb); a non-promoted property pulls the per-item `properties` jsonb but
--     fetches the fragment ONLY if the collection's fragment_config actually
--     fragments properties (the default fragments assets only). The config lookup
--     runs at most once per search (not per row) and only for the property-request
--     case, so thinning never adds per-row work — see the timing in the 10M bench.
--     (item_field_registry is sample-based discovery, not an exhaustive existence
--     index, so it is NOT used to drop fields — that would risk losing real data.)
CREATE OR REPLACE FUNCTION fields_to_rowjsonb(
    fields jsonb DEFAULT '{}'::jsonb,
    _collections text[] DEFAULT NULL
) RETURNS text AS $$
DECLARE
    includes text[] := ARRAY(SELECT jsonb_array_elements_text(fields->'include'));
    excludes text[] := ARRAY(SELECT jsonb_array_elements_text(fields->'exclude'));
    has_inc boolean := array_length(includes, 1) IS NOT NULL;
    single_coll text := CASE WHEN array_length(_collections, 1) = 1 THEN _collections[1] ELSE NULL END;
    whole_props boolean;
    prop_reqs text[];          -- specific 'properties.<x>' names requested (x only)
    np_reqs text[];            -- of those, the non-promoted ones (need properties jsonb)
    props_fragmented boolean;
    parts text[] := ARRAY[
        $p$'id', i.id$p$, $p$'collection', i.collection$p$,
        $p$'datetime', i.datetime$p$, $p$'end_datetime', i.end_datetime$p$,
        $p$'datetime_is_range', i.datetime_is_range$p$, $p$'fragment_id', i.fragment_id$p$
    ];
    need_frag boolean := false;
BEGIN
    -- NOTE: text[] || 'literal' makes PostgreSQL parse the literal as an array
    -- literal (fails); use array_append for single elements, ARRAY[...] for pairs.
    -- geometry as clean STAC GeoJSON. NB: to_jsonb(i.geometry) emits PostGIS GeoJSON
    -- WITH a non-standard `crs` member (invalid for STAC); ST_AsGeoJson is the same
    -- server conversion cost but standards-clean, so the client gets a valid item
    -- directly. (A future WKB/typed-column row wire could offload this to the client;
    -- benchmark before switching.)
    IF (CASE WHEN has_inc THEN 'geometry' = ANY(includes) ELSE NOT ('geometry' = ANY(excludes)) END)
        THEN parts := array_append(parts, $p$'geometry', ST_AsGeoJson(i.geometry)::jsonb$p$); END IF;
    IF (CASE WHEN has_inc THEN 'bbox' = ANY(includes) ELSE NOT ('bbox' = ANY(excludes)) END)
        THEN parts := array_append(parts, $p$'bbox', i.bbox$p$); END IF;
    IF (CASE WHEN has_inc THEN 'assets' = ANY(includes) ELSE NOT ('assets' = ANY(excludes)) END)
        THEN parts := array_append(parts, $p$'assets', i.assets$p$); need_frag := true; END IF;
    IF (CASE WHEN has_inc THEN 'links' = ANY(includes) ELSE NOT ('links' = ANY(excludes)) END)
        THEN parts := parts || ARRAY[$p$'links', i.links$p$, $p$'link_hrefs', i.link_hrefs$p$]; need_frag := true; END IF;
    IF (CASE WHEN has_inc THEN 'stac_version' = ANY(includes) ELSE NOT ('stac_version' = ANY(excludes)) END)
        THEN parts := array_append(parts, $p$'stac_version', i.stac_version$p$); need_frag := true; END IF;
    IF (CASE WHEN has_inc THEN 'stac_extensions' = ANY(includes) ELSE NOT ('stac_extensions' = ANY(excludes)) END)
        THEN parts := array_append(parts, $p$'stac_extensions', i.stac_extensions$p$); need_frag := true; END IF;
    IF (CASE WHEN has_inc THEN 'extra' = ANY(includes) ELSE NOT ('extra' = ANY(excludes)) END)
        THEN parts := array_append(parts, $p$'extra', i.extra$p$); END IF;
    -- Properties. Three cases, increasingly precise:
    whole_props := (CASE WHEN has_inc THEN 'properties' = ANY(includes)
                                      ELSE NOT ('properties' = ANY(excludes)) END);
    prop_reqs := ARRAY(SELECT substring(x FROM 12) FROM unnest(includes) x WHERE x LIKE 'properties.%');

    IF whole_props THEN
        -- whole properties: per-item jsonb + every promoted column; fragment only if
        -- this collection fragments properties (single-coll) else conservative.
        parts := array_append(parts, $p$'properties', i.properties$p$);
        parts := parts || ARRAY(
            SELECT format('%L, i.%I', p.property_path, p.property_path)
            FROM promoted_item_property_defs() p);
        need_frag := need_frag OR collection_fragments_properties(single_coll);
    ELSIF array_length(prop_reqs, 1) IS NOT NULL THEN
        -- specific properties.<x>: ship only the promoted columns asked for ...
        parts := parts || ARRAY(
            SELECT format('%L, i.%I', p.property_path, p.property_path)
            FROM promoted_item_property_defs() p
            WHERE p.name = ANY(prop_reqs));
        -- ... and the per-item properties jsonb only if a NON-promoted prop is asked
        -- for (dropping any that the registry says the collection does not have).
        np_reqs := ARRAY(
            SELECT x FROM unnest(prop_reqs) x
            WHERE NOT EXISTS (SELECT 1 FROM promoted_item_property_defs() p WHERE p.name = x));
        -- NB: item_field_registry is a *sample-based* discovery index, not an
        -- exhaustive existence index (absence != nonexistent), so it is NOT used to
        -- drop a requested property — that would risk discarding real data. The
        -- authoritative precision signal is the collection's fragment_config below.
        IF array_length(np_reqs, 1) IS NOT NULL THEN
            parts := array_append(parts, $p$'properties', i.properties$p$);
            need_frag := need_frag OR collection_fragments_properties(single_coll);
        END IF;
    END IF;
    IF need_frag THEN
        parts := array_append(parts, $p$'_fragment', (SELECT to_jsonb(f) FROM item_fragments f WHERE f.id = i.fragment_id)$p$);
    END IF;
    -- jsonb_build_object caps at 100 args (50 key/value parts); the full-row case
    -- (all promoted columns) exceeds that, so emit chunks of <=40 parts and concat.
    DECLARE expr text := ''; lo int;
    BEGIN
        FOR lo IN 1 .. array_length(parts, 1) BY 40 LOOP
            expr := expr
                || CASE WHEN expr = '' THEN '' ELSE ' || ' END
                || 'jsonb_build_object('
                || array_to_string(parts[lo : LEAST(lo + 39, array_length(parts, 1))], ', ')
                || ')';
        END LOOP;
        RETURN 'jsonb_strip_nulls(' || expr || ')';
    END;
END;
$$ LANGUAGE PLPGSQL STABLE PARALLEL SAFE;


-- search_sql: Core streaming primitive — returns a SQL query string for the
-- given search, ready to be executed under a server-side cursor.
--
-- mode = 'item' (default): each projected row is a fully-hydrated STAC Feature
--   jsonb, computed by content_hydrate(i, f, fields).  The fragment join feeds
--   hydration without a per-row correlated subquery.
--
-- mode = 'row': each projected row is the raw split-storage columns plus inline
--   fragment content (fields_to_columns projection).  The client reassembles
--   items itself, offloading hydration CPU from the server.
--
-- The returned SQL MUST be consumed via a server-side cursor (DECLARE … FETCH),
-- never materialized with jsonb_agg or a plain SELECT … FROM (…).  A fast-start
-- cursor (cursor_tuple_fraction = 0) chooses Nested Loop + Merge Append instead
-- of Hash + Sort, enabling constant-memory streaming. (EXPLAIN of the whole query
-- without the cursor is misleading: it will show the Hash + Sort plan instead.)
CREATE OR REPLACE FUNCTION search_sql(
    _search jsonb DEFAULT '{}'::jsonb,
    mode text DEFAULT 'item'
) RETURNS text AS $$
DECLARE
    searches   searches%ROWTYPE;
    _where     text;
    _orderby   text;
    _fields    jsonb;
    token      record;
    token_where text;
    full_where text;
    projection text;
BEGIN
    searches  := search_query(_search);
    _where    := searches._where;
    _orderby  := searches.orderby;
    _fields   := coalesce(_search->'fields', '{}'::jsonb);

    -- Apply pagination token to WHERE and (for prev-token) reverse ORDER BY.
    token := get_token_record(_search->>'token');
    IF token IS NOT NULL THEN
        IF token.prev THEN
            _orderby := sort_sqlorderby(_search, TRUE);
        END IF;
        token_where := get_token_filter(_search->'sortby', token.item, token.prev, FALSE);
    END IF;

    full_where := concat_ws(' AND ', _where, token_where);
    IF full_where IS NULL OR trim(full_where) = '' THEN
        full_where := 'TRUE';
    END IF;

    IF mode = 'item' THEN
        -- Use a correlated subquery for the fragment so the outer FROM clause
        -- stays `FROM items i` only.  This avoids column-ambiguity errors:
        -- item_fragments also has `id` and `collection`, so a LEFT JOIN in
        -- the outer FROM would make the unqualified WHERE/ORDER BY references
        -- generated by stac_search_to_where and sort_sqlorderby ambiguous.
        projection := format(
            $p$content_hydrate(i, %L::jsonb, (SELECT f FROM item_fragments f WHERE f.id = i.fragment_id))$p$,
            _fields
        );
    ELSE
        -- row mode: fields_to_columns already generates correlated subqueries
        -- for frag_content / frag_links_template, so no JOIN needed here either.
        projection := fields_to_columns(_fields);
    END IF;

    RETURN format(
        $sql$
        SELECT %s
        FROM items i
        WHERE %s
        ORDER BY %s
        $sql$,
        projection,
        full_where,
        _orderby
    );
END;
$$ LANGUAGE PLPGSQL STABLE SECURITY DEFINER SET SEARCH_PATH TO pgstac, public;




-- search_cursor: Open a server-side cursor for streaming search results.
--
-- Usage (within a transaction):
--   SELECT search_cursor('{"collections":["sentinel-2"],"limit":1000}'::jsonb);
--   FETCH 1000 FROM pgstac_stream;
--   CLOSE pgstac_stream;  -- or just COMMIT
--
-- mode = 'item' (default): each FETCH row is a hydrated STAC Feature jsonb.
-- mode = 'row':            each FETCH row is raw split-storage columns + fragment.
-- cur:  cursor name, defaults to 'pgstac_stream'.
--
-- Opens a server-side cursor over the SQL text from search_sql(_search, mode).
DROP FUNCTION IF EXISTS search_cursor(jsonb);

CREATE OR REPLACE FUNCTION search_cursor(
    _search jsonb DEFAULT '{}'::jsonb,
    mode text DEFAULT 'item',
    cur refcursor DEFAULT 'pgstac_stream'
) RETURNS refcursor AS $$
BEGIN
    OPEN cur NO SCROLL FOR EXECUTE search_sql(_search, mode);
    RETURN cur;
END;
$$ LANGUAGE PLPGSQL SECURITY DEFINER SET SEARCH_PATH TO pgstac, public;


-- ============================================================================
-- v0.10 streaming search engine: keyset pagination + late-materialized walk.
-- search_page is the streaming entry point; search() (below) is re-derived onto it,
-- and geometrysearch()/xyzsearch() share the same discovery: all use chunker()
-- (above) to walk partition_stats bands from the predicate envelope -- no per-query
-- EXPLAIN and no materialized-view refresh on the hot path.
-- ============================================================================

-- NULL-aware encode/decode (override the simple versions).
CREATE OR REPLACE FUNCTION keyset_encode(vals text[]) RETURNS text AS $$
    SELECT encode(convert_to(array_to_string(vals, chr(31), chr(30)), 'UTF8'), 'base64');
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION keyset_decode(token text) RETURNS text[] AS $$
    SELECT array_replace(
        string_to_array(convert_from(decode(token,'base64'),'UTF8'), chr(31)),
        chr(30), NULL);
$$ LANGUAGE sql IMMUTABLE;

-- Ordered sort keys = sortby + id + collection tiebreaks for a unique total order
-- (no overlapping pages), de-duplicated by field (first occurrence wins), each
-- resolved to its SQL expression.
CREATE OR REPLACE FUNCTION keyset_sortkeys(_search jsonb)
RETURNS TABLE(ord int, field text, expr text, dir text, notnull boolean) AS $$
    WITH base AS (
        SELECT coalesce(_search->'sortby','[{"field":"datetime","direction":"desc"}]'::jsonb) AS s
    ),
    app AS (
        SELECT s
               || jsonb_build_object('field','id','direction', s->0->>'direction')
               || jsonb_build_object('field','collection','direction', s->0->>'direction') AS s
        FROM base
    ),
    rows AS (
        SELECT value->>'field' AS field, get_sort_dir(value) AS dir, o
        FROM app, jsonb_array_elements(s) WITH ORDINALITY AS t(value, o)
    ),
    firsts AS (  -- keep first occurrence of each field
        SELECT DISTINCT ON (field) field, dir, o FROM rows ORDER BY field, o
    )
    -- id/collection/datetime/end_datetime are NOT NULL on items, so keyset_where can
    -- skip the NULL-handling branch for them (the common sort + tiebreaks).
    SELECT (row_number() OVER (ORDER BY o))::int, field, (queryable(field)).expression, dir,
           field IN ('id', 'collection', 'datetime', 'end_datetime')
    FROM firsts ORDER BY o;
$$ LANGUAGE sql STABLE;

-- Multi-level keyset seek predicate from token values (matches get_token_filter's
-- branch logic incl. NULLS FIRST/LAST, generalized to N levels & arbitrary fields).
CREATE OR REPLACE FUNCTION keyset_where(_search jsonb, _values text[], prev boolean DEFAULT false)
RETURNS text AS $$
DECLARE
    k record; vlit text; orterm text;
    andfilters text[] := '{}'::text[];
    orfilters  text[] := '{}'::text[];
BEGIN
    IF _values IS NULL THEN RETURN NULL; END IF;
    FOR k IN SELECT * FROM keyset_sortkeys(_search) ORDER BY ord LOOP
        vlit := CASE WHEN _values[k.ord] IS NULL THEN NULL ELSE quote_literal(_values[k.ord]) END;
        orterm := NULL;
        IF vlit IS NOT NULL AND ((prev AND k.dir='ASC') OR (NOT prev AND k.dir='DESC')) THEN
            orterm := format('(%s < %s)', k.expr, vlit);                          -- DESC fwd: nulls already passed
        ELSIF vlit IS NULL AND ((prev AND k.dir='ASC') OR (NOT prev AND k.dir='DESC')) THEN
            orterm := format('(%s IS NOT NULL)', k.expr);                         -- after a null (NULLS FIRST)
        ELSIF vlit IS NULL THEN
            orterm := NULL;                                                       -- after a null (NULLS LAST) → none
        ELSIF k.notnull THEN
            orterm := format('(%s > %s)', k.expr, vlit);                          -- NOT NULL column: no trailing nulls
        ELSE
            orterm := format('((%s > %s) OR (%s IS NULL))', k.expr, vlit, k.expr);-- ASC fwd: include trailing nulls
        END IF;
        IF orterm IS NOT NULL THEN
            IF array_length(andfilters,1) IS NULL THEN
                orfilters := orfilters || orterm;
            ELSE
                orfilters := orfilters || format('(%s AND %s)', array_to_string(andfilters,' AND '), orterm);
            END IF;
        END IF;
        andfilters := andfilters || CASE WHEN vlit IS NULL
            THEN format('(%s IS NULL)', k.expr) ELSE format('(%s = %s)', k.expr, vlit) END;
    END LOOP;
    IF array_length(orfilters,1) IS NULL THEN RETURN NULL; END IF;
    RETURN '(' || array_to_string(orfilters, ' OR ') || ')';
END;
$$ LANGUAGE plpgsql STABLE;

CREATE OR REPLACE FUNCTION search_page(
    _search jsonb DEFAULT '{}'::jsonb,
    _limit  int   DEFAULT 10,
    _token  text  DEFAULT NULL,
    _prev   boolean DEFAULT false,
    _mode   text  DEFAULT 'item'
) RETURNS jsonb AS $$
DECLARE
    _where text; _fields jsonb := coalesce(_search->'fields','{}'::jsonb);
    -- Per-row projection: 'item' hydrates server-side (FeatureCollection contract);
    -- 'row' ships the raw split-storage row + inline fragment for client-side
    -- hydration (the fast-start, low-memory streaming path the Rust client uses).
    -- Both modes share the SAME chunker / keyset / early-exit page machinery below,
    -- so 'row' inherits the newest-first early exit (fast TTFB, one page in memory).
    proj_expr text := CASE _mode
        WHEN 'row' THEN
            -- field-aware: only the columns the requested fields need + fragment-when-needed,
            -- with registry/fragment-config precision for property requests (collection-scoped).
            fields_to_rowjsonb(
                coalesce(_search->'fields','{}'::jsonb),
                ARRAY(SELECT jsonb_array_elements_text(_search->'collections')))
        ELSE
            format('content_hydrate(i, %L::jsonb, (SELECT f FROM item_fragments f WHERE f.id = i.fragment_id))',
                   coalesce(_search->'fields','{}'::jsonb))
    END;
    _hash text; ctx searches%ROWTYPE; total_count bigint;
    keyset_w text; full_where text;
    orderby_str text; keys_proj text; lead_field text; eff_lead_dir text;
    datetime_leading boolean;
    remaining int := _limit + 1; acc jsonb := '[]'::jsonb; cnt int := 0;
    r_count bigint := 0;
    has_more boolean := false;
    sdate timestamptz; edate timestamptz; q text; r record;
    first_k text[]; last_k text[]; fwd_first_k text[]; fwd_last_k text[];
    have_row boolean := false; nbands int := 0;
    band_pred text; next_tok text; prev_tok text; result jsonb;
    next_present boolean; prev_present boolean;
    _env pred_envelope;
BEGIN
    _where := stac_search_to_where(_search);                       -- PURE where/orderby derivation
    _hash  := search_hash_from_where(_where, '{}'::jsonb);
    -- discovery envelope: safe over-approximation of the search's (collections, datetime,
    -- end_datetime, geometry) extent, used by chunker() to prune partition_stats candidates.
    _env := search_envelope(_search);
    -- numberMatched (unchanged on/off/auto semantics). where_stats only persists and
    -- returns the count for a *registered* search, so register (best-effort, via
    -- search_query) only when context is actually wanted — keeping the default
    -- context='off' read path free of the cache write.
    IF context(_search->'conf') <> 'off' THEN
        PERFORM search_query(_search);
        total_count := (where_stats(_hash, _where, false, _search->'conf')).context_count;
    END IF;

    -- sort keys → EFFECTIVE ORDER BY (reversed for _prev), key projection, leading field/dir.
    -- For _prev we fetch the rows just *before* the token in reverse order, then flip.
    SELECT string_agg(expr||' '||CASE WHEN _prev THEN (CASE dir WHEN 'ASC' THEN 'DESC' ELSE 'ASC' END) ELSE dir END, ', ' ORDER BY ord),
           'ARRAY['||string_agg(format('(%s)::text', expr), ',' ORDER BY ord)||']::text[]',
           (array_agg(field ORDER BY ord))[1],
           (array_agg(CASE WHEN _prev THEN (CASE dir WHEN 'ASC' THEN 'DESC' ELSE 'ASC' END) ELSE dir END ORDER BY ord))[1]
    INTO orderby_str, keys_proj, lead_field, eff_lead_dir
    FROM keyset_sortkeys(_search);
    datetime_leading := (lead_field = 'datetime');

    -- keyset seek from token values (NO item lookup), arbitrary multi-level, direction-aware
    IF _token IS NOT NULL THEN
        keyset_w := keyset_where(_search, keyset_decode(_token), _prev);
    END IF;
    full_where := concat_ws(' AND ', _where, keyset_w);
    IF full_where IS NULL OR btrim(full_where) = '' THEN full_where := 'TRUE'; END IF;

    -- Fetch up to _limit+1 rows (the +1 tells us whether a further page exists in the
    -- walk direction) in EFFECTIVE order into a session temp table. Rows are appended
    -- SET-BASED (one INSERT per band / one for the direct query) and the page is built
    -- with a single jsonb_agg below -- this is O(n), unlike per-row jsonb `||` which is
    -- O(n^2) and explodes at high limits. The `ord` column carries the global effective
    -- order across bands (offset by the running count `cnt`).
    CREATE TEMP TABLE IF NOT EXISTS _search_page_rows (ord bigint, content jsonb, keys text[]);
    TRUNCATE _search_page_rows;
    IF datetime_leading THEN
        <<walk>>
        FOR sdate, edate IN
            EXECUTE format('SELECT s, e FROM chunker($1::pred_envelope) ORDER BY s %s',
                           CASE WHEN eff_lead_dir='ASC' THEN 'ASC' ELSE 'DESC' END)
            USING _env
        LOOP
            nbands := nbands + 1;
            band_pred := format('i.datetime >= %L AND i.datetime < %L', sdate, edate);
            EXECUTE format($q$
                INSERT INTO _search_page_rows (ord, content, keys)
                SELECT %s + row_number() OVER (ORDER BY %s),
                       %s,
                       %s
                FROM (SELECT * FROM items i WHERE (%s) AND (%s) ORDER BY %s LIMIT %s) i $q$,
                cnt, orderby_str, proj_expr, keys_proj, band_pred, full_where, orderby_str, remaining);
            GET DIAGNOSTICS r_count = ROW_COUNT;
            cnt := cnt + r_count;
            remaining := remaining - r_count;
            EXIT walk WHEN remaining <= 0;
        END LOOP;
    ELSE
        -- non-datetime-leading: single direct late-mat query (planner sorts, effective order)
        EXECUTE format($q$
            INSERT INTO _search_page_rows (ord, content, keys)
            SELECT row_number() OVER (ORDER BY %s),
                   %s,
                   %s
            FROM (SELECT * FROM items i WHERE (%s) ORDER BY %s LIMIT %s) i $q$,
            orderby_str, proj_expr, keys_proj, full_where, orderby_str, remaining);
        GET DIAGNOSTICS cnt = ROW_COUNT;
    END IF;

    -- Build the page (first _limit rows, effective order) with one O(n) jsonb_agg;
    -- derive the +1 has-more flag and the page's end keys from the materialized rows.
    -- (first_k/last_k are read as separate scalars -- array_agg of a text[] column makes
    -- a 2-D array whose single-subscript element is NULL, so it can't extract a key row.)
    has_more := cnt > _limit;
    have_row := cnt > 0;
    SELECT coalesce(jsonb_agg(content ORDER BY ord), '[]'::jsonb) INTO acc
    FROM (SELECT content, ord FROM _search_page_rows ORDER BY ord LIMIT _limit) z;
    IF have_row THEN
        SELECT keys INTO first_k FROM _search_page_rows ORDER BY ord LIMIT 1;
        SELECT keys INTO last_k  FROM _search_page_rows ORDER BY ord OFFSET (LEAST(_limit, cnt) - 1) LIMIT 1;
    END IF;

    -- Flip to forward sort order for _prev, and identify the page's forward end-rows.
    IF _prev THEN
        acc := flip_jsonb_array(acc);
        fwd_first_k := last_k; fwd_last_k := first_k;
        next_present := (_token IS NOT NULL);   -- a prev page always has the origin ahead
        prev_present := has_more;                -- ... and a further-back page iff more remain
    ELSE
        fwd_first_k := first_k; fwd_last_k := last_k;
        next_present := has_more;                -- a further page iff the +1 row existed
        prev_present := (_token IS NOT NULL);    -- a previous page iff we navigated off page 1
    END IF;

    IF have_row AND next_present THEN next_tok := keyset_encode(fwd_last_k); END IF;
    IF have_row AND prev_present THEN prev_tok := keyset_encode(fwd_first_k); END IF;

    result := jsonb_build_object(
        'type','FeatureCollection', 'features', acc,
        'numberReturned', jsonb_array_length(acc),
        'next', next_tok, 'prev', prev_tok, 'bands_scanned', nbands);
    IF total_count IS NOT NULL THEN
        result := result || jsonb_build_object('numberMatched', total_count);
    END IF;
    RETURN result;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER SET search_path TO pgstac, public;

-- search_rows: binary counterpart of search_page's 'row' mode. Returns the raw
-- split-storage item rows (SETOF items -- native columns, geometry as EWKB on the wire,
-- no jsonb serialization) for one page, applying the SAME where + chunker discovery +
-- keyset seek + effective order + limit as a single streaming query (no temp table).
-- The client batch-loads fragments by fragment_id and hydrates client-side, and builds
-- the next/prev token from the sort-key values of the last/first row it received
-- (request _limit+1 to detect a further page). This is the fast, allocation-light path
-- the Rust client uses; search_page('...', 'row') is the jsonb equivalent.
CREATE OR REPLACE FUNCTION search_rows(
    _search jsonb DEFAULT '{}'::jsonb,
    _limit  int   DEFAULT NULL,
    _token  text  DEFAULT NULL,
    _prev   boolean DEFAULT false
) RETURNS SETOF items AS $$
DECLARE
    _where text; keyset_w text; full_where text;
    orderby_str text; lead_field text; datetime_leading boolean;
    _env pred_envelope;
    lim int := coalesce(_limit, nullif(get_setting('default_page_size', _search->'conf'), '')::int, 250);
BEGIN
    _where := stac_search_to_where(_search);
    _env := search_envelope(_search);
    SELECT string_agg(expr || ' ' || CASE WHEN _prev THEN (CASE dir WHEN 'ASC' THEN 'DESC' ELSE 'ASC' END) ELSE dir END, ', ' ORDER BY ord),
           (array_agg(field ORDER BY ord))[1]
    INTO orderby_str, lead_field
    FROM keyset_sortkeys(_search);
    datetime_leading := (lead_field = 'datetime');
    IF _token IS NOT NULL THEN
        keyset_w := keyset_where(_search, keyset_decode(_token), _prev);
    END IF;
    full_where := concat_ws(' AND ', _where, keyset_w);
    IF full_where IS NULL OR btrim(full_where) = '' THEN full_where := 'TRUE'; END IF;

    IF datetime_leading THEN
        -- per-band late-materialized fetch + outer merge (Merge Append), fast start.
        RETURN QUERY EXECUTE format($q$
            SELECT i.* FROM chunker($1::pred_envelope) AS bands(s, e)
            CROSS JOIN LATERAL (
                SELECT * FROM items i
                WHERE i.datetime >= bands.s AND i.datetime < bands.e AND (%s)
                ORDER BY %s LIMIT %s
            ) i
            ORDER BY %s LIMIT %s
        $q$, full_where, orderby_str, lim, orderby_str, lim) USING _env;
    ELSE
        RETURN QUERY EXECUTE format($q$
            SELECT * FROM items i WHERE (%s) ORDER BY %s LIMIT %s
        $q$, full_where, orderby_str, lim);
    END IF;
END;
$$ LANGUAGE plpgsql STABLE;

-- search() re-derived onto the streaming engine (search_page) with KEYSET tokens.
-- Preserves the FeatureCollection + links + numberMatched/numberReturned contract;
-- only the token VALUES in next/prev hrefs change (now opaque keyset, not collection:id).
-- Overrides the legacy search() in 004_search.sql (slated for removal in the cleanup step).
CREATE OR REPLACE FUNCTION search(_search jsonb DEFAULT '{}'::jsonb) RETURNS jsonb AS $$
DECLARE
    -- caller-provided limit is the max rows; default to the configurable target page size.
    _limit  int := coalesce((_search->>'limit')::int,
                            nullif(get_setting('default_page_size', _search->'conf'), '')::int, 250);
    _token  text := _search->>'token';
    is_prev boolean := _token LIKE 'prev:%';
    keyset  text := nullif(regexp_replace(coalesce(_token,''), '^(next|prev):', ''), '');
    page    jsonb;
    burl    text := rtrim(coalesce(base_url(_search->'conf'), ''), '/');
    links   jsonb := '[]'::jsonb;
    nt text; pt text; out jsonb;
BEGIN
    page := search_page(_search, _limit, keyset, is_prev);
    links := links
      || jsonb_build_object('rel','root','type','application/json','href', burl)
      || jsonb_build_object('rel','self','type','application/json','href', burl||'/search');
    nt := page->>'next'; pt := page->>'prev';
    IF nt IS NOT NULL THEN
        links := links || jsonb_build_object('rel','next','type','application/geo+json','method','GET',
            'href', burl||'/search?token=next:'||nt);
    END IF;
    IF pt IS NOT NULL THEN
        links := links || jsonb_build_object('rel','prev','type','application/geo+json','method','GET',
            'href', burl||'/search?token=prev:'||pt);
    END IF;
    out := jsonb_build_object(
        'type','FeatureCollection',
        'features', coalesce(page->'features','[]'::jsonb),
        'links', links,
        'numberReturned', page->'numberReturned');
    IF page ? 'numberMatched' THEN out := out || jsonb_build_object('numberMatched', page->'numberMatched'); END IF;
    RETURN out;
END;
$$ LANGUAGE plpgsql;
