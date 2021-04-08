SET SEARCH_PATH TO pgstac, public;
/*

WORK IN PROGRESS SEARCH NOT FUNCTIONAL YET

*/
CREATE OR REPLACE FUNCTION split_stac_path(IN path text, OUT col text, OUT dotpath text, OUT jspath text, OUT jspathtext text) AS $$
WITH col AS (
    SELECT
        CASE WHEN
            split_part(path, '.', 1) IN ('id', 'stac_version', 'stac_extensions','geometry','properties','assets','collection_id','datetime','links', 'extra_fields') THEN split_part(path, '.', 1)
        ELSE 'properties'
        END AS col
),
dp AS (
    SELECT
        col, ltrim(replace(path, col , ''),'.') as dotpath
    FROM col
),
paths AS (
SELECT
    col, dotpath,
    regexp_split_to_table(dotpath,E'\\.') as path FROM dp
) SELECT
    col,
    btrim(concat(col,'.',dotpath),'.'),
    CASE WHEN btrim(concat(col,'.',dotpath),'.') != col THEN concat(col,'->',string_agg(concat('''',path,''''),'->')) ELSE col END,
    regexp_replace(
        CASE WHEN btrim(concat(col,'.',dotpath),'.') != col THEN concat(col,'->',string_agg(concat('''',path,''''),'->')) ELSE col END,
        E'>([^>]*)$','>>\1'
    )
FROM paths group by col, dotpath;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;


/* Functions for searching items */
CREATE OR REPLACE FUNCTION sort_base(
    IN _sort jsonb = '[{"field":"datetime","direction":"asc"}]',
    OUT key text,
    OUT col text,
    OUT dir text,
    OUT rdir text,
    OUT sort text,
    OUT rsort text
) RETURNS SETOF RECORD AS $$
WITH cols AS (
    SELECT key as name, value as col  FROM jsonb_each_text(get_config('sort_columns'))
),
sorts AS (
    SELECT
        value->>'field' as key,
        (split_stac_path(value->>'field')).jspathtext as col,
        upper(value->>'direction') as dir
    FROM jsonb_array_elements('[]'::jsonb || _sort)
),
joined AS (
    SELECT
        key,
        col,
        dir
    FROM sorts LEFT JOIN cols using (col)
)
SELECT
    key,
    col,
    dir,
    CASE dir WHEN 'DESC' THEN 'ASC' ELSE 'ASC' END as rdir,
    concat(col, ' ', dir, ' NULLS LAST ') AS sort,
    concat(col,' ', CASE dir WHEN 'DESC' THEN 'ASC' ELSE 'ASC' END, ' NULLS LAST ') AS rsort
FROM joined
UNION ALL
SELECT 'id', 'id', 'ASC', 'DESC', 'id ASC', 'id DESC'
;
$$ LANGUAGE SQL;


CREATE OR REPLACE FUNCTION sort(_sort jsonb) RETURNS text AS $$
SELECT string_agg(sort,', ') FROM sort_base(_sort);
$$ LANGUAGE SQL PARALLEL SAFE;


CREATE OR REPLACE FUNCTION rsort(_sort jsonb) RETURNS text AS $$
SELECT string_agg(rsort,', ') FROM sort_base(_sort);
$$ LANGUAGE SQL PARALLEL SAFE;


CREATE OR REPLACE FUNCTION bbox_geom(_bbox jsonb) RETURNS box3d AS $$
SELECT CASE jsonb_array_length(_bbox)
    WHEN 4 THEN
        ST_SetSRID(ST_MakeEnvelope(
            (_bbox->>0)::float,
            (_bbox->>1)::float,
            (_bbox->>2)::float,
            (_bbox->>3)::float
        ),4326)
    WHEN 6 THEN
    ST_SetSRID(ST_3DMakeBox(
        ST_MakePoint(
            (_bbox->>0)::float,
            (_bbox->>1)::float,
            (_bbox->>2)::float
        ),
        ST_MakePoint(
            (_bbox->>3)::float,
            (_bbox->>4)::float,
            (_bbox->>5)::float
        )
    ),4326)
    ELSE null END;
;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION in_array_q(col text, arr jsonb) RETURNS text AS $$
SELECT CASE jsonb_typeof(arr) WHEN 'array' THEN format('%I = ANY(textarr(%L))', col, arr) ELSE format('%I = %L', col, arr) END;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION count_by_delim(text, text) RETURNS int AS $$
SELECT count(*) FROM regexp_split_to_table($1,$2);
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;



CREATE OR REPLACE FUNCTION stac_query_op(att text, _op text, val jsonb) RETURNS text AS $$
DECLARE
ret text := '';
idx text;
idx_func text;
idx_typ text;
op text;
jp text;
att_parts RECORD;
BEGIN
val := lower(val::text)::jsonb;

att_parts := split_stac_path(att);

op := CASE _op
    WHEN 'eq' THEN '='
    WHEN 'ge' THEN '>='
    WHEN 'gt' THEN '>'
    WHEN 'le' THEN '<='
    WHEN 'lt' THEN '<'
    WHEN 'ne' THEN '!='
    ELSE _op
END;
RAISE NOTICE 'att_parts: % %', att_parts, count_by_delim(att_parts.dotpath,'\.');
IF
    op = '='
    AND att_parts.col = 'properties'
    AND count_by_delim(att_parts.dotpath,'\.') = 2
THEN
    -- use jsonpath query to leverage index for eqaulity tests on single level deep properties
    jp := btrim(format($jp$ $.%I[*] ? ( @ == %s ) $jp$, replace(att_parts.dotpath, 'properties.',''), val));
    raise notice 'jp: %', jp;
    ret := format($q$ properties_idx(properties) @? %L $q$, jp);
ELSIF jsonb_typeof(val) = 'number' THEN
    ret := format('(%s)::numeric %s %s', att_parts.jspathtext, op, val);
ELSE
    ret := format('lower(%s) %s %L', att_parts.jspathtext, op, val);
END IF;
RAISE NOTICE 'Op Query: %', ret;
/*
IF op
SELECT
    indexdef,
    (regexp_match(indexdef, E'btree \\((.*)\\)$'))[1],
    (regexp_match(indexdef, E'::([\\w ]*)\\)*$'))[1]
INTO idx, idx_func, idx_typ FROM pg_indexes
WHERE
    schemaname='pgstac'
    AND tablename='items'
    AND indexdef ~* format(E'properties ->>? %L', att)
    AND indexdef ilike format('%%btree%%', att)
;

IF FOUND THEN
    RAISE NOTICE 'Found index: % % %', idx, idx_func, idx_typ;
    -- USE queries that use the BTREE INDEX
    IF op = '==' THEN
        op := '=';
    END IF;

    ret := format('%s %s %L::%s', idx_func, op, val, idx_typ);
ELSE
    jp := format($jp$ $.%I[*] ? ( @ %s %s ) $jp$, att, op, val);
    raise notice 'jp: %', jp;
    ret := format($q$ properties_idx(properties) @? %L  $q$, jp);
END IF;
*/
return ret;
END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION stac_query(_query jsonb) RETURNS TEXT[] AS $$
DECLARE
qa text[];
att text;
ops jsonb;
op text;
val jsonb;
BEGIN
FOR att, ops IN SELECT key, value FROM jsonb_each(_query)
LOOP
    FOR op, val IN SELECT key, value FROM jsonb_each(ops)
    LOOP
        qa := array_append(qa, stac_query_op(att,op, val));
        RAISE NOTICE '% % %', att, op, val;
    END LOOP;
END LOOP;
RETURN qa;
END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION filter_by_order(item_id text, _sort jsonb, _type text) RETURNS text AS $$
DECLARE
sorts RECORD;
item items%ROWTYPE;
filts text[];
itemval text;
op text;
idop text;
ret text;
BEGIN

SELECT * INTO item FROM items WHERE id=item_id;
FOR sorts IN SELECT * FROM sort_base(_sort) LOOP

    op := CASE
        WHEN _type='prev' AND sorts.col='id' THEN '<'
        WHEN _type='first' AND sorts.col='id' THEN '<='
        WHEN _type='last' AND sorts.col='id' THEN '>='
        WHEN _type='next' AND sorts.col='id' THEN '>'
        WHEN _type in ('prev','first') AND sorts.dir = 'ASC' THEN '>='
        WHEN _type in ('last','next') AND sorts.dir = 'ASC' THEN '<='
        WHEN _type in ('prev','first') AND sorts.dir = 'DESC' THEN '<='
        WHEN _type in ('last','next') AND sorts.dir = 'DESC' THEN '>='
    END;
    EXECUTE format($q$ SELECT ($1.%s)::text; $q$, sorts.col) INTO itemval USING (item);
    IF itemval IS NOT NULL THEN
        filts = array_append(filts, format('%s %s %L', sorts.col, op, itemval));
    END IF;
END LOOP;
ret := coalesce(array_to_string(filts,' AND '), 'TRUE');
RETURN ret;
END;
$$ LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION search(_search jsonb = '{}'::jsonb) RETURNS SETOF jsonb AS $$
DECLARE
_sort text := '';
_limit int := 10;
_geom geometry;
qa text[];
pq text[];
query text;
pq_prop record;
pq_op record;
prev_id text := NULL;
next_id text := NULL;
whereq text := 'TRUE';
links jsonb := '[]'::jsonb;
token text;
tok_val text;
tok_q text := 'TRUE';
minid text;
maxid text;
sort text;
rsort text;
dt text[];
startdt timestamptz;
enddt timestamptz;
item items%ROWTYPE;
BEGIN
sort := sort(_search->'sortby');
rsort := reverse_sort(_search->'sortby');
IF _search ? 'token' THEN
    token := _search->>'token';
    tok_val := substr(token,6);
    IF starts_with(token, 'prev:') THEN
        tok_q := filter_by_order(tok_val,  _search->'sortby', 'last');

        sort := reverse_sort(_search->'sortby');
        rsort := sort(_search->'sortby');
    ELSIF starts_with(token, 'next:') THEN
       tok_q := filter_by_order(tok_val,  _search->'sortby', 'first');
    END IF;
END IF;
RAISE NOTICE 'tok_q: %', tok_q;

IF _search ? 'ids' THEN
    RAISE NOTICE 'searching solely based on ids... %',_search;
    qa := array_append(qa, in_array_q('id', _search->'ids'));
ELSE
    IF _search ? 'intersects' THEN
        _geom := ST_SetSRID(ST_GeomFromGeoJSON(_search->>'intersects'), 4326);
    ELSIF _search ? 'bbox' THEN
        _geom := bbox_geom(_search->'bbox');
    END IF;

    IF _geom IS NOT NULL THEN
        qa := array_append(qa, format('st_intersects(geometry, %L::geometry)',_geom));
    END IF;

    IF _search ? 'collections' THEN
        qa := array_append(qa, in_array_q('collection_id', _search->'collections'));
    END IF;

    IF _search ? 'collections' THEN
        qa := array_append(qa, in_array_q('collection_id', _search->'collections'));
    END IF;

    IF _search ? 'datetime' THEN
        IF jsonb_typeof(_search->'datetime') = 'array' THEN
            dt := textarr(_search->'datetime');
        ELSE
            dt := regexp_split_to_array(_search->>'datetime','/');
        END IF;
        IF array_upper(dt,1) = 1 THEN
            qa := array_append(qa, format('datetime = %L::timestamptz', dt[1]));
        ELSE
            IF dt[1] != '..' THEN
                qa := array_append(qa, format('datetime >= %L::timestamptz', dt[1]));
            END IF;
            IF dt[2] != '..' THEN
                qa := array_append(qa, format('datetime <= %L::timestamptz', dt[2]));
            END IF;
        END IF;
    END IF;

    IF _search ? 'query' THEN
        qa := array_cat(qa,
            stac_query(_search->'query')
        );
    END IF;
END IF;

IF _search ? 'limit' THEN
    _limit := (_search->>'limit')::int;
END IF;


whereq := COALESCE(array_to_string(qa,' AND '),' TRUE ');

query := format($q$
    CREATE TEMP VIEW results_temp_view
    AS
    SELECT *
    FROM items
    WHERE %s
    ORDER BY %s
    $q$,
    whereq,
    sort
);
RAISE NOTICE 'QUERY: %', query;
EXECUTE query;

query := format($q$
    CREATE TEMP TABLE results_page
    ON COMMIT DROP AS
    SELECT format_item(results_temp_view) as item, id
    FROM results_temp_view
    WHERE %s
    LIMIT %s
    $q$,
    tok_q,
    _limit
);
RAISE NOTICE 'QUERY: %', query;
EXECUTE query;

SELECT INTO minid, maxid first_value(id) OVER (), last_value(id) OVER () FROM results_page;
RAISE NOTICE 'minid: %, maxid %', minid, maxid;

query := format($q$
    SELECT id
    FROM results_temp_view
    WHERE %s
    LIMIT 1
    $q$,
    filter_by_order(maxid,  _search->'sortby', 'next')
);
RAISE NOTICE 'next query: %', query;
EXECUTE query into next_id;
RAISE NOTICE 'next_id: %', next_id;

query := format($q$
    SELECT id
    FROM results_temp_view
    WHERE %s
    LIMIT 1
    $q$,
    filter_by_order(minid, _search->'sortby', 'prev')
);
RAISE NOTICE 'prev query: %', query;
EXECUTE query into prev_id;
RAISE NOTICE 'prev_id: %', prev_id;

DROP VIEW results_temp_view;

RETURN QUERY
WITH i AS (
SELECT r.item FROM results_page r LIMIT _limit
),
features AS (
    SELECT jsonb_agg(i.item) features FROM i
)
SELECT jsonb_build_object(
    'type', 'FeatureCollection',
    'features', coalesce(features,'[]'::jsonb),
    'links', links,
    'timeStamp', now(),
    'next', next_id,
    'prev', prev_id
)
FROM features
;


END;
$$ LANGUAGE PLPGSQL;
