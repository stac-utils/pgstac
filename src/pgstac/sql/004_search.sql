
CREATE OR REPLACE FUNCTION chunker(
    IN _where text,
    OUT s timestamptz,
    OUT e timestamptz
) RETURNS SETOF RECORD AS $$
DECLARE
    explain jsonb;
BEGIN
    IF _where IS NULL THEN
        _where := ' TRUE ';
    END IF;
    EXECUTE format('EXPLAIN (format json) SELECT 1 FROM items WHERE %s;', _where)
    INTO explain;
    RAISE DEBUG 'EXPLAIN: %', explain;

    RETURN QUERY
    WITH t AS (
        SELECT j->>0 as p FROM
            jsonb_path_query(
                explain,
                'strict $.**."Relation Name" ? (@ != null)'
            ) j
    ),
    parts AS (
        -- = ANY(array) uses the partition_stats primary key; the planner
        -- cannot estimate jsonb_path_query, so a join plans as a full scan.
        SELECT
            date_trunc('month', lower(partition_dtrange)) as sdate,
            date_trunc('month', upper(partition_dtrange)) + '1 month'::interval as edate
        FROM partition_stats
        WHERE
            partition = ANY (ARRAY(SELECT p FROM t))
            AND partition_dtrange IS NOT NULL
            AND partition_dtrange != 'empty'::tstzrange
    ),
    times AS (
        SELECT sdate FROM parts
        UNION
        SELECT edate FROM parts
    ),
    uniq AS (
        SELECT DISTINCT sdate FROM times ORDER BY sdate
    ),
    last AS (
    SELECT sdate, lead(sdate, 1) over () as edate FROM uniq
    )
    SELECT sdate, edate FROM last WHERE edate IS NOT NULL;
END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION partition_queries(
    IN _where text DEFAULT 'TRUE',
    IN _orderby text DEFAULT 'datetime DESC, collection DESC, id DESC',
    IN partitions text[] DEFAULT NULL
) RETURNS SETOF text AS $$
DECLARE
    query text;
    sdate timestamptz;
    edate timestamptz;
BEGIN
IF _where IS NULL OR trim(_where) = '' THEN
    _where = ' TRUE ';
END IF;
RAISE DEBUG 'Getting chunks for % %', _where, _orderby;
IF _orderby ILIKE 'datetime d%' THEN
    FOR sdate, edate IN SELECT * FROM chunker(_where) ORDER BY 1 DESC LOOP
        RETURN NEXT format($q$
            SELECT * FROM items
            WHERE
            datetime >= %L AND datetime < %L
            AND (%s)
            ORDER BY %s
            $q$,
            sdate,
            edate,
            _where,
            _orderby
        );
    END LOOP;
ELSIF _orderby ILIKE 'datetime a%' THEN
    FOR sdate, edate IN SELECT * FROM chunker(_where) ORDER BY 1 ASC LOOP
        RETURN NEXT format($q$
            SELECT * FROM items
            WHERE
            datetime >= %L AND datetime < %L
            AND (%s)
            ORDER BY %s
            $q$,
            sdate,
            edate,
            _where,
            _orderby
        );
    END LOOP;
ELSE
    query := format($q$
        SELECT * FROM items
        WHERE %s
        ORDER BY %s
    $q$, _where, _orderby
    );

    RETURN NEXT query;
    RETURN;
END IF;

RETURN;
END;
$$ LANGUAGE PLPGSQL SET SEARCH_PATH TO pgstac,public;

-- Volatile like partition_queries, which reads the partitions as they stand.
CREATE OR REPLACE FUNCTION partition_query_view(
    IN _where text DEFAULT 'TRUE',
    IN _orderby text DEFAULT 'datetime DESC, collection DESC, id DESC',
    IN _limit int DEFAULT 10
) RETURNS text AS $$
    WITH p AS (
        SELECT * FROM partition_queries(_where, _orderby) p
    )
    SELECT
        CASE WHEN EXISTS (SELECT 1 FROM p) THEN
            (SELECT format($q$
                SELECT * FROM (
                    %s
                ) total LIMIT %s
                $q$,
                string_agg(
                    format($q$ SELECT * FROM ( %s ) AS sub $q$, p),
                    '
                    UNION ALL
                    '
                ),
                _limit
            ))
        ELSE NULL
        END FROM p;
$$ LANGUAGE SQL;


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
    -- The placeholder has to be made of term characters, so it cannot be made unspellable.
    -- An input that contains it would be substituted for a phrase it never wrote.
    IF position(placeholder in input) > 0 THEN
        RAISE EXCEPTION 'Free text query may not contain %', placeholder;
    END IF;

    -- Extract all quoted phrases and store in array. ARRAY(...) because regexp_matches with
    -- the g flag returns a set, and assigning a set to a scalar fails on the second match.
    quote_array := ARRAY(SELECT m[1] FROM regexp_matches(input, '"[^"]*"', 'g') m);

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
    processed_text := regexp_replace(processed_text, '^\s*\+([a-zA-Z0-9_@]+)', '\1', 'g'); -- +term at start
    processed_text := regexp_replace(processed_text, '\s+\+([a-zA-Z0-9_@]+)', ' & \1', 'g'); -- +term elsewhere, whitespace required so that foo+bar stays one word

    -- - ->  !
    processed_text := regexp_replace(processed_text, '^\s*\-([a-zA-Z0-9_@]+)', '! \1', 'g'); -- -term at start
    processed_text := regexp_replace(processed_text, '\s+\-([a-zA-Z0-9_@]+)', ' & ! \1', 'g'); -- -term elsewhere, whitespace required so that foo-bar stays one word

    -- a +/- term following an operator would otherwise double the operator
    processed_text := regexp_replace(processed_text, '([&|])\s*&\s*(!?)', '\1 \2', 'g');

    -- terms separated with spaces are assumed to represent adjacent terms. loop through these
    -- occurrences and replace them with the adjacency operator (<->)
    LOOP
        -- The placeholder standing in for a quoted phrase counts as a term here, or no adjacency
        -- operator is inserted beside it and the result is not a valid tsquery.
        temp_text := regexp_replace(processed_text, '([a-zA-Z0-9_@]+)\s+([a-zA-Z0-9_@]+)(?!\s*[&|<>])', '\1 <-> \2', 'g');
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


CREATE OR REPLACE FUNCTION stac_search_to_where(j jsonb) RETURNS text AS $$
DECLARE
    where_segments text[];
    _where text;
    dtrange tstzrange;
    collections text[];
    geom geometry;
    sdate timestamptz;
    edate timestamptz;
    filterlang text;
    filter jsonb := j->'filter';
    ft_query tsquery;
BEGIN
    IF j ? 'ids' THEN
        where_segments := where_segments || format('id = ANY (%L) ', to_text_array(j->'ids'));
    END IF;

    IF j ? 'collections' THEN
        collections := to_text_array(j->'collections');
        where_segments := where_segments || format('collection = ANY (%L) ', collections);
    END IF;

    IF j ? 'datetime' THEN
        dtrange := parse_dtrange(j->'datetime');
        sdate := lower(dtrange);
        edate := upper(dtrange);

        where_segments := where_segments || format(' datetime %s %L::timestamptz AND end_datetime >= %L::timestamptz ',
            CASE WHEN upper_inc(dtrange) THEN '<=' ELSE '<' END,
            edate,
            sdate
        );
    END IF;

    IF j ? 'q' THEN
        ft_query := q_to_tsquery(j->'q');
        where_segments := where_segments || format(
            $quote$
            (
                to_tsvector('english', content->'properties'->>'description') ||
                to_tsvector('english', coalesce(content->'properties'->>'title', '')) ||
                to_tsvector('english', coalesce(content->'properties'->>'keywords', ''))
            ) @@ %L
            $quote$,
            ft_query
        );
    END IF;

    geom := stac_geom(j);
    IF geom IS NOT NULL THEN
        where_segments := where_segments || format('st_intersects(geometry, %L)',geom);
    END IF;

    filterlang := COALESCE(
        j->>'filter-lang',
        get_setting('default_filter_lang', j->'conf')
    );
    IF NOT filter @? '$.**.op' THEN
        filterlang := 'cql-json';
    END IF;

    IF filterlang NOT IN ('cql-json','cql2-json') AND j ? 'filter' THEN
        RAISE EXCEPTION '% is not a supported filter-lang. Please use cql-json or cql2-json.', filterlang;
    END IF;

    IF j ? 'query' AND j ? 'filter' THEN
        RAISE EXCEPTION 'Can only use either query or filter at one time.';
    END IF;

    IF j ? 'query' THEN
        filter := query_to_cql2(j->'query');
    ELSIF filterlang = 'cql-json' THEN
        filter := cql1_to_cql2(filter);
    END IF;
    RAISE DEBUG 'FILTER: %', filter;
    where_segments := where_segments || cql2_query(filter, NULL, collections);
    IF cardinality(where_segments) < 1 THEN
        RETURN ' TRUE ';
    END IF;

    _where := array_to_string(array_remove(where_segments, NULL), ' AND ');

    IF _where IS NULL OR BTRIM(_where) = '' THEN
        RETURN ' TRUE ';
    END IF;
    RETURN _where;

END;
$$ LANGUAGE PLPGSQL STABLE;


CREATE OR REPLACE FUNCTION parse_sort_dir(_dir text, reverse boolean default false) RETURNS text AS $$
DECLARE
    d text := btrim(coalesce(_dir, ''));
BEGIN
    -- The whole word, not a prefix: 'desc%' accepts anything merely beginning with desc. An
    -- unrecognised direction raises rather than reading as ASC, where a typo silently reverses
    -- half a result set.
    IF d <> '' AND d !~* '^(asc|desc)(ending)?$' THEN
        RAISE EXCEPTION 'Invalid sortby direction %: must be asc or desc', _dir;
    END IF;
    -- boolean <> is xor: reverse flips whichever direction was asked for
    RETURN CASE WHEN (d ILIKE 'desc%') <> reverse THEN 'DESC' ELSE 'ASC' END;
END;
$$ LANGUAGE PLPGSQL IMMUTABLE PARALLEL SAFE;


CREATE OR REPLACE FUNCTION sortby_with_tiebreakers(
    _sortby jsonb,
    _keys text[] DEFAULT '{collection,id}'
) RETURNS jsonb AS $$
DECLARE
    -- A missing or empty sortby is datetime DESC and a lone object is a one
    -- element array. The key columns of the searched relation (items unless
    -- given) are appended in the first direction for a total order.
    sort jsonb := CASE
        WHEN _sortby IS NULL OR jsonb_typeof(_sortby) = 'null' OR _sortby = '[]'::jsonb THEN '[{"field":"datetime","direction":"desc"}]'::jsonb
        WHEN jsonb_typeof(_sortby) = 'object' THEN jsonb_build_array(_sortby)
        ELSE _sortby
    END;
BEGIN
    IF jsonb_typeof(sort) != 'array' OR EXISTS (
        SELECT 1 FROM jsonb_array_elements(sort) e
        WHERE jsonb_typeof(e) != 'object'
           OR jsonb_typeof(e->'field') IS DISTINCT FROM 'string'
           -- An empty field resolves to nothing, leaving a bare direction in the ORDER BY:
           -- a syntax error raised from deep inside search_rows.
           OR btrim(coalesce(strip_properties_prefix(e->>'field'), '')) = ''
    ) THEN
        RAISE EXCEPTION 'Invalid sortby %: must be an array of {"field": text, "direction": text} objects', _sortby;
    END IF;
    RETURN sort || coalesce(
        (
            SELECT jsonb_agg(jsonb_build_object('field', f, 'direction', sort->0->>'direction') ORDER BY n)
            FROM unnest(_keys) WITH ORDINALITY AS t(f, n)
            WHERE NOT jsonb_path_exists(sort, '$[*] ? (@.field == $f)', jsonb_build_object('f', f))
        ),
        '[]'::jsonb
    );
END;
$$ LANGUAGE PLPGSQL STABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION sort_sqlorderby(
    _search jsonb DEFAULT NULL,
    reverse boolean DEFAULT FALSE,
    _keys text[] DEFAULT '{collection,id}',
    _collection_ids text[] DEFAULT NULL
) RETURNS text AS $$
    WITH sorts AS (
        SELECT
            (queryable(value->>'field', _collection_ids)).expression as key,
            parse_sort_dir(value->>'direction', reverse) as dir
        FROM jsonb_array_elements(sortby_with_tiebreakers(_search->'sortby', _keys)) AS t(value)
    )
    SELECT array_to_string(
        array_agg(concat(key, ' ', dir)),
        ', '
    ) FROM sorts;
$$ LANGUAGE SQL;


CREATE OR REPLACE FUNCTION  get_token_val_str(
    _field text,
    _item items
) RETURNS text AS $$
DECLARE
    q text;
    literal text;
BEGIN
    q := format($q$ SELECT quote_literal((%s)::text) FROM (SELECT $1.*) as r;$q$, _field);
    EXECUTE q INTO literal USING _item;
    RETURN literal;
END;
$$ LANGUAGE PLPGSQL;



-- The <collection>:<id> half of a paging token, each side the hex of its UTF-8 bytes so
-- neither can contain the separator. Hex digits are RFC 3986 unreserved, so the token crosses
-- a query string unchanged. The caller prefixes the direction to make a whole token.
CREATE OR REPLACE FUNCTION page_token(_collection text, _id text) RETURNS text AS $$
    SELECT encode(convert_to(_collection, 'UTF8'), 'hex')
        || ':' || encode(convert_to(_id, 'UTF8'), 'hex');
$$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION get_token_record(IN _token text, OUT prev BOOLEAN, OUT item items) RETURNS RECORD AS $$
DECLARE
    _parts text[];
BEGIN
    RAISE DEBUG 'Looking for token: %', _token;

    -- <direction>:<collection>:<id>, the collection and id hex encoded so neither can hold a
    -- separator. The direction is required: a token is only ever handed out as part of a
    -- next or prev link. Digits in pairs, or decode meets an odd-length string and rejects it.
    -- Lowered whole: hex is case insensitive to decode, so this costs nothing and makes the
    -- direction match without case folding each part.
    _parts := string_to_array(lower(_token), ':');
    IF cardinality(_parts) <> 3
        OR _parts[1] NOT IN ('next', 'prev')
        OR _parts[2] !~ '^([0-9a-f]{2})+$'
        OR _parts[3] !~ '^([0-9a-f]{2})+$'
    THEN
        RAISE EXCEPTION 'Invalid paging token: %', _token;
    END IF;
    prev := _parts[1] = 'prev';

    SELECT * INTO item FROM items
        WHERE collection = convert_from(decode(_parts[2], 'hex'), 'UTF8')
          AND id = convert_from(decode(_parts[3], 'hex'), 'UTF8');

    IF item IS NULL THEN
        RAISE EXCEPTION 'Could not find item using token: %', _token;
    END IF;
    RETURN;
END;
$$ LANGUAGE PLPGSQL STABLE STRICT;


CREATE OR REPLACE FUNCTION get_token_filter(
    _sortby jsonb DEFAULT NULL,
    token_item items DEFAULT NULL,
    prev boolean DEFAULT FALSE,
    inclusive boolean DEFAULT FALSE,
    _collection_ids text[] DEFAULT NULL
) RETURNS text AS $$
DECLARE
    ltop text := '<';
    gtop text := '>';
    sort record;
    orfilter text := '';
    orfilters text[] := '{}'::text[];
    andfilters text[] := '{}'::text[];
    output text;
    token_where text;
BEGIN
    _sortby := sortby_with_tiebreakers(_sortby);
    IF inclusive THEN
        orfilters := orfilters || format('( id=%L AND collection=%L )' , token_item.id, token_item.collection);
    END IF;

    FOR sort IN
        WITH s1 AS (
            SELECT
                _row,
                (queryable(value->>'field', _collection_ids)).expression as _field,
                (value->>'field' = 'id') as _isid,
                (value->>'field' = 'collection') as _iscollection,
                parse_sort_dir(value->>'direction') as _dir
            FROM jsonb_array_elements(_sortby)
            WITH ORDINALITY AS t(value, _row)
        )
        SELECT
            _row,
            _field,
            _dir,
            get_token_val_str(_field, token_item) as _val
        FROM s1
        WHERE _row <= (SELECT greatest(min(_row) FILTER (WHERE _isid), min(_row) FILTER (WHERE _iscollection)) FROM s1)
        ORDER BY _row ASC
    LOOP
        orfilter := NULL;
        RAISE DEBUG 'SORT: %', sort;
        IF sort._val IS NOT NULL AND  ((prev AND sort._dir = 'ASC') OR (NOT prev AND sort._dir = 'DESC')) THEN
            orfilter := format('(%s %s %s)', sort._field, ltop, sort._val);
        ELSIF sort._val IS NULL AND  ((prev AND sort._dir = 'ASC') OR (NOT prev AND sort._dir = 'DESC')) THEN
            RAISE DEBUG '< but null';
            orfilter := format('%s IS NOT NULL', sort._field);
        ELSIF sort._val IS NULL THEN
            RAISE DEBUG '> but null';
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
        RAISE DEBUG 'ORFILTER: %', orfilter;

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
    RAISE DEBUG 'TOKEN_WHERE: %',token_where;
    RETURN token_where;
    END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION search_hash(jsonb, jsonb) RETURNS text AS $$
    SELECT md5(concat(($1 - '{token,limit,context,includes,excludes}'::text[])::text,$2::text));
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;
DROP FUNCTION IF EXISTS search_tohash(jsonb);

CREATE TABLE IF NOT EXISTS searches(
    hash text GENERATED ALWAYS AS (search_hash(search, metadata)) STORED PRIMARY KEY,
    search jsonb NOT NULL,
    _where text,
    orderby text,
    lastused timestamptz DEFAULT now(),
    usecount bigint DEFAULT 0,
    metadata jsonb DEFAULT '{}'::jsonb NOT NULL
);

CREATE TABLE IF NOT EXISTS search_wheres(
    id bigint generated always as identity primary key,
    _where text NOT NULL,
    lastused timestamptz DEFAULT now(),
    usecount bigint DEFAULT 0,
    statslastupdated timestamptz,
    estimated_count bigint,
    estimated_cost float,
    time_to_estimate float,
    total_count bigint,
    time_to_count float,
    partitions text[]
);

CREATE INDEX IF NOT EXISTS search_wheres_partitions ON search_wheres USING GIN (partitions);
CREATE UNIQUE INDEX IF NOT EXISTS search_wheres_where ON search_wheres ((md5(_where)));

CREATE OR REPLACE FUNCTION where_stats(
    inwhere text,
    updatestats boolean default false,
    conf jsonb default null
) RETURNS search_wheres AS $$
DECLARE
    t timestamptz;
    i interval;
    explain_json jsonb;
    sw search_wheres%ROWTYPE;
    inwhere_hash text := md5(inwhere);
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
        sw._where = inwhere;
        RETURN sw;
    END IF;

    -- Unlocked read. A fresh hit only bumps bookkeeping counters, and that is
    -- the common case when identical searches run concurrently.
    SELECT * INTO sw FROM search_wheres WHERE md5(_where)=inwhere_hash;

    -- Within ttl: bump usage counters and return. The bump skips locked rows so
    -- identical searches do not serialize; a missed increment is harmless.
    -- sw.id, not "sw IS NOT NULL": a composite is only IS NOT NULL when every
    -- field is, and search_wheres.partitions is never populated.
    IF
        sw.id IS NOT NULL
        AND sw.statslastupdated IS NOT NULL
        AND sw.total_count IS NOT NULL
        AND now() - sw.statslastupdated <= _stats_ttl
    THEN
        RAISE DEBUG 'Stats present in table and lastupdated within ttl: %', sw;
        IF NOT ro THEN
            UPDATE search_wheres SET
                lastused = now(),
                usecount = search_wheres.usecount + 1
            WHERE id = (
                SELECT id FROM search_wheres
                WHERE md5(_where) = inwhere_hash
                FOR UPDATE SKIP LOCKED
            );
        END IF;
        RAISE DEBUG 'Returning cached counts. %', sw;
        RETURN sw;
    END IF;

    -- Missing or stale, so lock the row to compute once, then re-check
    -- freshness in case another session finished while we waited.
    IF NOT ro THEN
        SELECT * INTO sw FROM search_wheres WHERE md5(_where)=inwhere_hash FOR UPDATE;
        IF
            sw.statslastupdated IS NOT NULL
            AND sw.total_count IS NOT NULL
            AND now() - sw.statslastupdated <= _stats_ttl
        THEN
            RAISE DEBUG 'Another process refreshed stats while we waited: %', sw;
            UPDATE search_wheres SET
                lastused = now(),
                usecount = search_wheres.usecount + 1
            WHERE md5(_where) = inwhere_hash
            RETURNING * INTO sw;
            RETURN sw;
        END IF;
    END IF;

    -- Calculate estimated cost and rows
    -- Use explain to get estimated count/cost
    IF sw.estimated_count IS NULL OR sw.estimated_cost IS NULL THEN
        RAISE DEBUG 'Calculating estimated stats';
        t := clock_timestamp();
        EXECUTE format('EXPLAIN (format json) SELECT 1 FROM items WHERE %s', inwhere)
            INTO explain_json;
        RAISE DEBUG 'Time for just the explain: %', clock_timestamp() - t;
        i := clock_timestamp() - t;

        sw.estimated_count := (explain_json->0->'Plan'->>'Plan Rows')::bigint;
        sw.estimated_cost := (explain_json->0->'Plan'->>'Total Cost')::float;
        sw.time_to_estimate := extract(epoch from i);
    END IF;

    RAISE DEBUG 'ESTIMATED_COUNT: %, THRESHOLD %', sw.estimated_count, _estimated_count_threshold;
    RAISE DEBUG 'ESTIMATED_COST: %, THRESHOLD %', sw.estimated_cost, _estimated_cost_threshold;

    -- If context is set to auto and the costs are within the threshold return the estimated costs
    IF
        _context = 'auto'
        AND sw.estimated_count >= _estimated_count_threshold
        AND sw.estimated_cost >= _estimated_cost_threshold
    THEN
        IF NOT ro THEN
            INSERT INTO search_wheres (
                _where,
                lastused,
                usecount,
                statslastupdated,
                estimated_count,
                estimated_cost,
                time_to_estimate,
                total_count,
                time_to_count
            ) VALUES (
                inwhere,
                now(),
                1,
                now(),
                sw.estimated_count,
                sw.estimated_cost,
                sw.time_to_estimate,
                null,
                null
            ) ON CONFLICT ((md5(_where)))
            DO UPDATE SET
                lastused = EXCLUDED.lastused,
                usecount = search_wheres.usecount + 1,
                statslastupdated = EXCLUDED.statslastupdated,
                estimated_count = EXCLUDED.estimated_count,
                estimated_cost = EXCLUDED.estimated_cost,
                time_to_estimate = EXCLUDED.time_to_estimate,
                total_count = EXCLUDED.total_count,
                time_to_count = EXCLUDED.time_to_count
            RETURNING * INTO sw;
        END IF;
        RAISE DEBUG 'Estimates are within thresholds, returning estimates. %', sw;
        RETURN sw;
    END IF;

    -- Calculate Actual Count
    t := clock_timestamp();
    RAISE DEBUG 'Calculating actual count...';
    EXECUTE format(
        'SELECT count(*) FROM items WHERE %s',
        inwhere
    ) INTO sw.total_count;
    i := clock_timestamp() - t;
    RAISE DEBUG 'Actual Count: % -- %', sw.total_count, i;
    sw.time_to_count := extract(epoch FROM i);

    IF NOT ro THEN
        INSERT INTO search_wheres (
            _where,
            lastused,
            usecount,
            statslastupdated,
            estimated_count,
            estimated_cost,
            time_to_estimate,
            total_count,
            time_to_count
        ) VALUES (
            inwhere,
            now(),
            1,
            now(),
            sw.estimated_count,
            sw.estimated_cost,
            sw.time_to_estimate,
            sw.total_count,
            sw.time_to_count
        ) ON CONFLICT ((md5(_where)))
        DO UPDATE SET
            lastused = EXCLUDED.lastused,
            usecount = search_wheres.usecount + 1,
            statslastupdated = EXCLUDED.statslastupdated,
            estimated_count = EXCLUDED.estimated_count,
            estimated_cost = EXCLUDED.estimated_cost,
            time_to_estimate = EXCLUDED.time_to_estimate,
            total_count = EXCLUDED.total_count,
            time_to_count = EXCLUDED.time_to_count
        RETURNING * INTO sw;
    END IF;
    RAISE DEBUG 'Returning with actual count. %', sw;
    RETURN sw;
END;
$$ LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION search_query(
    _search jsonb = '{}'::jsonb,
    updatestats boolean = false,
    _metadata jsonb = '{}'::jsonb
) RETURNS searches AS $$
DECLARE
    search searches%ROWTYPE;
    cached_search searches%ROWTYPE;
    pexplain jsonb;
    t timestamptz;
    i interval;
    doupdate boolean := FALSE;
    insertfound boolean := FALSE;
    ro boolean := pgstac.readonly();
    found_search text;
BEGIN
    -- Calculate hash, where clause, and order by statement
    search.search := _search;
    search.metadata := _metadata;
    search.hash := search_hash(_search, _metadata);
    search._where := stac_search_to_where(_search);
    search.orderby := sort_sqlorderby(_search, FALSE, '{collection,id}', to_text_array(_search->'collections'));
    search.lastused := now();
    search.usecount := 1;

    -- If we are in read only mode, directly return search
    IF ro THEN
        RETURN search;
    END IF;

    -- Update statistics for times used and and when last used
    -- If the entry is locked, rather than waiting, skip updating the stats
    INSERT INTO searches (search, lastused, usecount, metadata)
        VALUES (search.search, now(), 1, search.metadata)
        ON CONFLICT DO NOTHING
        RETURNING * INTO cached_search
    ;

    IF NOT FOUND OR cached_search IS NULL THEN
        UPDATE searches SET
            lastused = now(),
            usecount = searches.usecount + 1
        WHERE hash = (
            SELECT hash FROM searches WHERE hash=search.hash FOR UPDATE SKIP LOCKED
        )
        RETURNING * INTO cached_search
        ;
    END IF;

    IF cached_search IS NOT NULL THEN
        cached_search._where = search._where;
        cached_search.orderby = search.orderby;
        RETURN cached_search;
    END IF;
    RETURN search;

END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION search_fromhash(
    _hash text
) RETURNS searches AS $$
DECLARE
    _search jsonb;
BEGIN
    SELECT search INTO _search FROM searches WHERE hash=_hash LIMIT 1;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Search with Query Hash % Not Found', _hash;
    END IF;
    RETURN search_query(_search);
END;
$$ LANGUAGE PLPGSQL STRICT;

CREATE OR REPLACE FUNCTION search_rows(
    IN _where text DEFAULT 'TRUE',
    IN _orderby text DEFAULT 'datetime DESC, collection DESC, id DESC',
    IN partitions text[] DEFAULT NULL,
    IN _limit int DEFAULT 10
) RETURNS SETOF items AS $$
DECLARE
    base_query text;
    query text;
    sdate timestamptz;
    edate timestamptz;
    n int;
    records_left int := _limit;
    timer timestamptz := clock_timestamp();
    full_timer timestamptz := clock_timestamp();
BEGIN
IF _where IS NULL OR trim(_where) = '' THEN
    _where = ' TRUE ';
END IF;
RAISE DEBUG 'Getting chunks for % %', _where, _orderby;

base_query := $q$
    SELECT * FROM items
    WHERE
    datetime >= %L AND datetime < %L
    AND (%s)
    ORDER BY %s
    LIMIT %L
$q$;

IF _orderby ILIKE 'datetime d%' THEN
    FOR sdate, edate IN SELECT * FROM chunker(_where) ORDER BY 1 DESC LOOP
        RAISE DEBUG 'Running Query for % to %. %', sdate, edate, age_ms(full_timer);
        query := format(
            base_query,
            sdate,
            edate,
            _where,
            _orderby,
            records_left
        );
        RAISE DEBUG 'QUERY: %', query;
        timer := clock_timestamp();
        RETURN QUERY EXECUTE query;

        GET DIAGNOSTICS n = ROW_COUNT;
        records_left := records_left - n;
        RAISE DEBUG 'Returned %/% Rows From % to %. % to go. Time: %ms', n, _limit, sdate, edate, records_left, age_ms(timer);
        timer := clock_timestamp();
        IF records_left <= 0 THEN
            RAISE DEBUG 'SEARCH_ROWS TOOK %ms', age_ms(full_timer);
            RETURN;
        END IF;
    END LOOP;
ELSIF _orderby ILIKE 'datetime a%' THEN
    FOR sdate, edate IN SELECT * FROM chunker(_where) ORDER BY 1 ASC LOOP
        RAISE DEBUG 'Running Query for % to %. %', sdate, edate, age_ms(full_timer);
        query := format(
            base_query,
            sdate,
            edate,
            _where,
            _orderby,
            records_left
        );
        RAISE DEBUG 'QUERY: %', query;
        timer := clock_timestamp();
        RETURN QUERY EXECUTE query;

        GET DIAGNOSTICS n = ROW_COUNT;
        records_left := records_left - n;
        RAISE DEBUG 'Returned %/% Rows From % to %. % to go. Time: %ms', n, _limit, sdate, edate, records_left, age_ms(timer);
        timer := clock_timestamp();
        IF records_left <= 0 THEN
            RAISE DEBUG 'SEARCH_ROWS TOOK %ms', age_ms(full_timer);
            RETURN;
        END IF;
    END LOOP;
ELSE
    query := format($q$
        SELECT * FROM items
        WHERE %s
        ORDER BY %s
        LIMIT %L
    $q$, _where, _orderby, _limit
    );
    RAISE DEBUG 'QUERY: %', query;
    timer := clock_timestamp();
    RETURN QUERY EXECUTE query;
    RAISE DEBUG 'FULL QUERY TOOK %ms', age_ms(timer);
END IF;
RAISE DEBUG 'SEARCH_ROWS TOOK %ms', age_ms(full_timer);
RETURN;
END;
$$ LANGUAGE PLPGSQL SET SEARCH_PATH TO pgstac,public;


CREATE UNLOGGED TABLE format_item_cache(
    id text,
    collection text,
    fields text,
    hydrated bool,
    output jsonb,
    lastused timestamptz DEFAULT now(),
    usecount int DEFAULT 1,
    timetoformat float,
    PRIMARY KEY (collection, id, fields, hydrated)
);
CREATE INDEX ON format_item_cache (lastused);

CREATE OR REPLACE FUNCTION format_item(_item items, _fields jsonb DEFAULT '{}', _hydrated bool DEFAULT TRUE) RETURNS jsonb AS $$
DECLARE
    cache bool := get_setting_bool('format_cache');
    _output jsonb := null;
    t timestamptz := clock_timestamp();
BEGIN
    IF cache THEN
        SELECT output INTO _output FROM format_item_cache
        WHERE id=_item.id AND collection=_item.collection AND fields=_fields::text AND hydrated=_hydrated;
    END IF;
    IF _output IS NULL THEN
        IF _hydrated THEN
            _output := content_hydrate(_item, _fields);
        ELSE
            _output := content_nonhydrated(_item, _fields);
        END IF;
    END IF;
    IF cache THEN
        INSERT INTO format_item_cache (id, collection, fields, hydrated, output, timetoformat)
            VALUES (_item.id, _item.collection, _fields::text, _hydrated, _output, age_ms(t))
            ON CONFLICT(collection, id, fields, hydrated) DO
                UPDATE
                    SET lastused=now(), usecount = format_item_cache.usecount + 1
        ;
    END IF;
    RETURN _output;

END;
$$ LANGUAGE PLPGSQL;


-- A non-negative integer from a jsonb member, named for the message. Read as numeric, so a
-- value of the right shape but too large is refused here rather than surfacing as a bare
-- integer overflow from the cast.
CREATE OR REPLACE FUNCTION check_int(_value jsonb, _name text) RETURNS int AS $$
DECLARE
    v text := btrim(_value#>>'{}');
BEGIN
    IF v IS NULL THEN
        RETURN NULL;
    END IF;
    IF v !~ '^\d+$' OR v::numeric > 2147483647 THEN
        RAISE EXCEPTION 'Invalid % %: must be a non-negative integer', _name, _value;
    END IF;
    RETURN v::int;
END;
$$ LANGUAGE PLPGSQL IMMUTABLE PARALLEL SAFE;

-- One reading of offset for both collection_search and collection_search_rows: two would let
-- the rows returned and the links offered to reach them disagree.
CREATE OR REPLACE FUNCTION search_offset(_search jsonb, _default int DEFAULT 0) RETURNS int AS $$
    SELECT COALESCE(pgstac.check_int(_search->'offset', 'offset'), _default);
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION search_limit(_search jsonb, _default int DEFAULT 10) RETURNS int AS $$
    SELECT COALESCE(pgstac.check_int(_search->'limit', 'limit'), _default);
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION search(_search jsonb = '{}'::jsonb) RETURNS jsonb AS $$
DECLARE
    searches searches%ROWTYPE;
    _where text;
    orderby text;
    search_where search_wheres%ROWTYPE;
    total_count bigint;
    token record;
    token_prev boolean;
    token_item items%ROWTYPE;
    token_where text;
    full_where text;
    init_ts timestamptz := clock_timestamp();
    timer timestamptz := clock_timestamp();
    hydrate bool := NOT (_search->'conf'->>'nohydrate' IS NOT NULL AND (_search->'conf'->>'nohydrate')::boolean = true);
    prev text;
    next text;
    collection jsonb;
    out_records jsonb;
    -- The (collection, id) of each row in out_records, in the same order. The tokens are
    -- built from these rather than from the formatted features, which fields.exclude can
    -- strip of exactly the two members a token needs.
    out_keys jsonb;
    out_len int;
    _limit int := search_limit(_search);
    _querylimit int;
    _fields jsonb := coalesce(_search->'fields', '{}'::jsonb);
    has_prev boolean := FALSE;
    has_next boolean := FALSE;
    links jsonb := '[]'::jsonb;
    base_url text:= concat(rtrim(base_url(_search->'conf'),'/'));
BEGIN
    searches := search_query(_search);
    _where := searches._where;
    orderby := searches.orderby;
    search_where := where_stats(_where, false, _search->'conf');
    total_count := coalesce(search_where.total_count, search_where.estimated_count);
    RAISE DEBUG 'SEARCH:TOKEN: %', _search->>'token';
    token := get_token_record(_search->>'token');
    _querylimit := _limit + 1;
    IF token IS NOT NULL THEN
        token_prev := token.prev;
        token_item := token.item;
        token_where := get_token_filter(_search->'sortby', token_item, token_prev, FALSE, to_text_array(_search->'collections'));
        RAISE DEBUG 'TOKEN_WHERE: % (%ms from search start)', token_where, age_ms(timer);
        IF token_prev THEN -- if we are using a prev token, we know has_next is true
            RAISE DEBUG 'There is a previous token, so automatically setting has_next to true';
            has_next := TRUE;
            orderby := sort_sqlorderby(_search, TRUE, '{collection,id}', to_text_array(_search->'collections'));
        ELSE
            RAISE DEBUG 'There is a next token, so automatically setting has_prev to true';
            has_prev := TRUE;

        END IF;
    ELSE -- if there was no token, we know there is no prev
        RAISE DEBUG 'There is no token, so we know there is no prev. setting has_prev to false';
        has_prev := FALSE;
    END IF;

    full_where := concat_ws(' AND ', _where, token_where);
    RAISE DEBUG 'FULL WHERE CLAUSE: %', full_where;
    RAISE DEBUG 'Time to get counts and build query %', age_ms(timer);
    timer := clock_timestamp();

    IF hydrate THEN
        RAISE DEBUG 'Getting hydrated data.';
    ELSE
        RAISE DEBUG 'Getting non-hydrated data.';
    END IF;
    RAISE DEBUG 'CACHE SET TO %', get_setting_bool('format_cache');
    RAISE DEBUG 'Time to set hydration/formatting %', age_ms(timer);
    timer := clock_timestamp();
    SELECT
        jsonb_agg(format_item(i, _fields, hydrate)),
        jsonb_agg(jsonb_build_array(i.collection, i.id))
    INTO out_records, out_keys
    FROM search_rows(
        full_where,
        orderby,
        search_where.partitions,
        _querylimit
    ) as i;

    RAISE DEBUG 'Time to fetch rows %', age_ms(timer);
    timer := clock_timestamp();


    IF token_prev THEN
        out_records := flip_jsonb_array(out_records);
        out_keys := flip_jsonb_array(out_keys);
    END IF;

    RAISE DEBUG 'Query returned % records.', jsonb_array_length(out_records);
    RAISE DEBUG 'TOKEN:   % %', token_item.id, token_item.collection;
    RAISE DEBUG 'RECORD_1: % %', out_keys->0->>1, out_keys->0->>0;
    RAISE DEBUG 'RECORD-1: % %', out_keys->-1->>1, out_keys->-1->>0;

    -- REMOVE records that were from our token
    IF out_keys->0->>0 = token_item.collection AND out_keys->0->>1 = token_item.id THEN
        out_records := out_records - 0;
        out_keys := out_keys - 0;
    ELSIF out_keys->-1->>0 = token_item.collection AND out_keys->-1->>1 = token_item.id THEN
        out_records := out_records - -1;
        out_keys := out_keys - -1;
    END IF;

    IF jsonb_array_length(out_records) = _limit + 1 THEN
        IF token_prev THEN
            has_prev := TRUE;
            out_records := out_records - 0;
            out_keys := out_keys - 0;
        ELSE
            has_next := TRUE;
            out_records := out_records - -1;
            out_keys := out_keys - -1;
        END IF;
    END IF;
    out_len := coalesce(jsonb_array_length(out_records), 0);

    links := links || jsonb_build_object(
        'rel', 'root',
        'type', 'application/json',
        'href', base_url
    ) || jsonb_build_object(
        'rel', 'self',
        'type', 'application/json',
        'href', concat(base_url, '/search')
    );

    -- An empty page still anchors on the token that produced it, so the caller has a way back
    -- instead of a dead end. A link identical to the incoming token is dropped: following it
    -- would return this same page forever.
    IF has_next AND out_len > 0 THEN
        next := page_token(out_keys->-1->>0, out_keys->-1->>1);
        RAISE DEBUG 'HAS NEXT | %', next;
        links := links || jsonb_build_object(
            'rel', 'next',
            'type', 'application/geo+json',
            'method', 'GET',
            'href', concat(base_url, '/search?token=next:', next)
        );
    END IF;

    IF has_prev AND (out_len > 0 OR _limit = 0) THEN
        prev := CASE WHEN out_len > 0
            THEN page_token(out_keys->0->>0, out_keys->0->>1)
            ELSE page_token(token_item.collection, token_item.id)
        END;
        -- Never hand back the token that produced this page: limit 0 reached by a prev token
        -- anchored the prev link on that same token, so a client following it never advanced.
        -- Compared lowered, as get_token_record reads the token, so PREV: cannot reopen the
        -- loop this guard closes.
        IF lower(concat('prev:', prev)) IS DISTINCT FROM lower(_search->>'token') THEN
            RAISE DEBUG 'HAS PREV | %', prev;
            links := links || jsonb_build_object(
                'rel', 'prev',
                'type', 'application/geo+json',
                'method', 'GET',
                'href', concat(base_url, '/search?token=prev:', prev)
            );
        ELSE
            prev := NULL;
        END IF;
    END IF;

    RAISE DEBUG 'Time to get prev/next %', age_ms(timer);
    timer := clock_timestamp();


    collection := jsonb_build_object(
        'type', 'FeatureCollection',
        'features', coalesce(out_records, '[]'::jsonb),
        'links', links
    );



    collection := collection || jsonb_build_object('numberReturned', out_len);
    IF context(_search->'conf') != 'off' THEN
        collection := collection || jsonb_strip_nulls(jsonb_build_object('numberMatched', total_count));
    END IF;

    IF get_setting_bool('timing', _search->'conf') THEN
        collection = collection || jsonb_build_object('timing', age_ms(init_ts));
    END IF;

    RAISE DEBUG 'Time to build final json %', age_ms(timer);
    timer := clock_timestamp();

    RAISE DEBUG 'Total Time: %', age_ms(current_timestamp);
    RAISE DEBUG 'RETURNING % records. NEXT: %. PREV: %', collection->>'numberReturned', collection->>'next', collection->>'prev';
    RETURN collection;
END;
$$ LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION search_cursor(_search jsonb = '{}'::jsonb) RETURNS refcursor AS $$
DECLARE
    curs refcursor;
    searches searches%ROWTYPE;
    _where text;
    _orderby text;
    q text;

BEGIN
    searches := search_query(_search);
    _where := searches._where;
    _orderby := searches.orderby;

    OPEN curs FOR
        WITH p AS (
            SELECT * FROM partition_queries(_where, _orderby) p
        )
        SELECT
            CASE WHEN EXISTS (SELECT 1 FROM p) THEN
                (SELECT format($q$
                    SELECT * FROM (
                        %s
                    ) total
                    $q$,
                    string_agg(
                        format($q$ SELECT * FROM ( %s ) AS sub $q$, p),
                        '
                        UNION ALL
                        '
                    )
                ))
            ELSE NULL
            END FROM p;
    RETURN curs;
END;
$$ LANGUAGE PLPGSQL;
