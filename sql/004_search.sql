SET SEARCH_PATH TO pgstac, public;

/*
View to get a table of available items partitions
with date ranges
*/
DROP VIEW IF EXISTS items_search_partitions;
CREATE VIEW items_search_partitions AS
WITH base AS
(SELECT
    c.oid::pg_catalog.regclass::text as partition,
    pg_catalog.pg_get_expr(c.relpartbound, c.oid) as _constraint,
    regexp_matches(
        pg_catalog.pg_get_expr(c.relpartbound, c.oid),
        E'\\(''\([0-9 :+-]*\)''\\).*\\(''\([0-9 :+-]*\)''\\)'
    ) as t,
    reltuples::bigint as est_cnt
FROM pg_catalog.pg_class c, pg_catalog.pg_inherits i
WHERE c.oid = i.inhrelid AND i.inhparent = 'items_search'::regclass)
SELECT partition, tstzrange(
    t[1]::timestamptz,
    t[2]::timestamptz
), est_cnt
FROM base
WHERE est_cnt >0
ORDER BY 2 desc;


CREATE OR REPLACE FUNCTION items_by_partition(
    IN _where text DEFAULT 'TRUE',
    IN _dtrange tstzrange DEFAULT tstzrange('-infinity','infinity'),
    IN _orderby text DEFAULT 'datetime DESC, id DESC',
    IN _limit int DEFAULT 10
) RETURNS SETOF text AS $$
DECLARE
partition_query text;
main_query text;
batchcount int;
counter int := 0;
p record;
BEGIN
IF _orderby ILIKE 'datetime d%' THEN
    partition_query := format($q$
        SELECT partition
        FROM items_search_partitions
        WHERE tstzrange && $1
        ORDER BY tstzrange DESC;
    $q$);
ELSIF _orderby ILIKE 'datetime a%' THEN
    partition_query := format($q$
        SELECT partition
        FROM items_search_partitions
        WHERE tstzrange && $1
        ORDER BY tstzrange ASC;
    $q$);
ELSE
    partition_query := format($q$
        SELECT 'items_search' as partition WHERE $1 IS NOT NULL;
    $q$);
END IF;

FOR p IN
    EXECUTE partition_query USING (_dtrange)
LOOP
    IF lower(_dtrange)::timestamptz > '-infinity' THEN
        _where := concat(_where,format(' AND datetime >= %L',lower(_dtrange)::timestamptz::text));
    END IF;
    IF upper(_dtrange)::timestamptz < 'infinity' THEN
        _where := concat(_where,format(' AND datetime <= %L',upper(_dtrange)::timestamptz::text));
    END IF;

    main_query := format($q$
        SELECT id FROM %I
        WHERE %s
        ORDER BY %s
        LIMIT %s - $1
    $q$, p.partition::text, _where, _orderby, _limit
    );
    RAISE NOTICE 'Partition Query %', main_query;
    RAISE NOTICE '%', counter;
    RETURN QUERY EXECUTE main_query USING counter;

    GET DIAGNOSTICS batchcount = ROW_COUNT;
    counter := counter + batchcount;
    RAISE NOTICE 'FOUND %', batchcount;
    IF counter >= _limit THEN
        EXIT;
    END IF;
    RAISE NOTICE 'ADDED % FOR A TOTAL OF %', batchcount, counter;
END LOOP;
RETURN;
END;
$$ LANGUAGE PLPGSQL;


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
    IN _sort jsonb DEFAULT '[{"field":"datetime","direction":"desc"}]',
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
        coalesce(upper(value->>'direction'),'ASC') as dir
    FROM jsonb_array_elements('[]'::jsonb || coalesce(_sort,'[{"field":"datetime","direction":"desc"}]') )
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
SELECT 'id', 'id', 'DESC', 'ASC', 'id DESC', 'id ASC'
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
    ret := format($q$ properties @? %L $q$, jp);
ELSIF jsonb_typeof(val) = 'number' THEN
    ret := format('(%s)::numeric %s %s', att_parts.jspathtext, op, val);
ELSE
    ret := format('lower(%s) %s %L', att_parts.jspathtext, op, val);
END IF;
RAISE NOTICE 'Op Query: %', ret;

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
item item;
BEGIN
SELECT * INTO item FROM items_search WHERE id=item_id;
RETURN filter_by_order(item, _sort, _type);
END;
$$ LANGUAGE PLPGSQL;

-- Used to create filters used for paging using the items id from the token
CREATE OR REPLACE FUNCTION filter_by_order(_item item, _sort jsonb, _type text) RETURNS text AS $$
DECLARE
sorts RECORD;
filts text[];
itemval text;
op text;
idop text;
ret text;
eq_flag text;
_item_j jsonb := to_jsonb(_item);
BEGIN
FOR sorts IN SELECT * FROM sort_base(_sort) LOOP
    IF sorts.col = 'datetime' THEN
        CONTINUE;
    END IF;
    IF sorts.col='id' AND _type IN ('prev','next') THEN
        eq_flag := '';
    ELSE
        eq_flag := '=';
    END IF;

    op := concat(
        CASE
            WHEN _type in ('prev','first') AND sorts.dir = 'ASC' THEN '<'
            WHEN _type in ('last','next') AND sorts.dir = 'ASC' THEN '>'
            WHEN _type in ('prev','first') AND sorts.dir = 'DESC' THEN '>'
            WHEN _type in ('last','next') AND sorts.dir = 'DESC' THEN '<'
        END,
        eq_flag
    );

    IF _item_j ? sorts.col THEN
        filts = array_append(filts, format('%s %s %L', sorts.col, op, _item_j->>sorts.col));
    END IF;
END LOOP;
ret := coalesce(array_to_string(filts,' AND '), 'TRUE');
RAISE NOTICE 'Order Filter %', ret;
RETURN ret;
END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION search_dtrange(IN _indate jsonb, OUT _tstzrange tstzrange) AS
$$
WITH t AS (
    SELECT CASE
        WHEN jsonb_typeof(_indate) = 'array' THEN
            textarr(_indate)
        ELSE
            regexp_split_to_array(
                btrim(_indate::text,'"'),
                '/'
            )
        END AS arr
)
, t1 AS (
    SELECT
        CASE
            WHEN arr[1] = '..' THEN '-infinity'::timestamptz
            ELSE arr[1]::timestamptz
        END AS st,
        CASE
            WHEN array_upper(arr,1) = 1 THEN null
            WHEN arr[2] = '..' THEN 'infinity'::timestamptz
            ELSE arr[2]::timestamptz
        END AS et
    FROM t
)
SELECT
    CASE
        WHEN et IS NULL
            THEN tstzrange(st, st, '()')
        ELSE
            tstzrange(st, et)
    END AS _tstzrange
FROM t1;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;


CREATE OR REPLACE FUNCTION search(_search jsonb = '{}'::jsonb) RETURNS SETOF jsonb AS $$
DECLARE
qstart timestamptz := clock_timestamp();
_sort text := '';
_rsort text := '';
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
tok_sort text;
first_id text;
first_dt timestamptz;
last_id text;
sort text;
rsort text;
dt text[];
dqa text[];
dq text;
mq_where text;
startdt timestamptz;
enddt timestamptz;
item items_search%ROWTYPE;
counter int := 0;
batchcount int;
month timestamptz;
m record;
_dtrange tstzrange := tstzrange('-infinity','infinity');
_dtsort text;
_token_dtrange tstzrange := tstzrange('-infinity','infinity');
_token_record items_search%ROWTYPE;
is_prev boolean := false;
BEGIN
-- Create table from sort query of items to sort
CREATE TEMP TABLE pgstac_tmp_sorts ON COMMIT DROP AS SELECT * FROM sort_base(_search->'sortby');

-- Get the datetime sort direction, necessary for efficient cycling through partitions
SELECT INTO _dtsort dir FROM pgstac_tmp_sorts WHERE key='datetime';
RAISE NOTICE '_dtsort: %',_dtsort;

SELECT INTO _sort string_agg(s.sort,', ') FROM pgstac_tmp_sorts s;
SELECT INTO _rsort string_agg(s.rsort,', ') FROM pgstac_tmp_sorts s;
tok_sort := _sort;


-- Get datetime from query as a tstzrange
IF _search ? 'datetime' THEN
    _dtrange := search_dtrange(_search->'datetime');
    _token_dtrange := _dtrange;
END IF;

-- Get the paging token
IF _search ? 'token' THEN
    token := _search->>'token';
    tok_val := substr(token,6);
    IF starts_with(token, 'prev:') THEN
        is_prev := true;
    END IF;
    SELECT INTO _token_record * FROM items_search WHERE id=tok_val;
    IF
        (is_prev AND _dtsort = 'DESC')
        OR
        (not is_prev AND _dtsort = 'ASC')
    THEN
        _token_dtrange := tstzrange(_token_record.datetime, 'infinity');
    ELSIF
        _dtsort IS NOT NULL
    THEN
        _token_dtrange := tstzrange('-infinity',_token_record.datetime);
    END IF;
    IF is_prev THEN
        tok_q := filter_by_order(tok_val,  _search->'sortby', 'first');
        _sort := _rsort;
    ELSIF starts_with(token, 'next:') THEN
       tok_q := filter_by_order(tok_val,  _search->'sortby', 'last');
    END IF;
END IF;
RAISE NOTICE 'timing: %', age(clock_timestamp(), qstart);
RAISE NOTICE 'tok_q: % _token_dtrange: %', tok_q, _token_dtrange;

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
dq := COALESCE(array_to_string(dqa,' AND '),' TRUE ');
RAISE NOTICE 'timing before temp table: %', age(clock_timestamp(), qstart);

CREATE TEMP TABLE results_page ON COMMIT DROP AS
SELECT items_by_partition(
    concat(whereq, ' AND ', tok_q),
    _token_dtrange,
    _sort,
    _limit + 1
) as id;
RAISE NOTICE 'timing after temp table: %', age(clock_timestamp(), qstart);

RAISE NOTICE 'timing before min/max: %', age(clock_timestamp(), qstart);

IF is_prev THEN
    SELECT INTO last_id, first_id
        first_value(id) OVER (),
        last_value(id) OVER ()
    FROM results_page;
ELSE
    SELECT INTO first_id, last_id
        first_value(id) OVER (),
        last_value(id) OVER ()
    FROM results_page;
END IF;
RAISE NOTICE 'firstid: %, lastid %', first_id, last_id;
RAISE NOTICE 'timing after min/max: %', age(clock_timestamp(), qstart);





next_id := last_id;
RAISE NOTICE 'next_id: %', next_id;

IF tok_q = 'TRUE' THEN
    RAISE NOTICE 'Not a paging query, no previous item';
ELSE
    RAISE NOTICE 'Getting previous item id';
    RAISE NOTICE 'timing: %', age(clock_timestamp(), qstart);
    SELECT INTO _token_record * FROM items_search WHERE id=first_id;
    IF
        _dtsort = 'DESC'
    THEN
        _token_dtrange := _dtrange * tstzrange(_token_record.datetime, 'infinity');
    ELSE
        _token_dtrange := _dtrange * tstzrange('-infinity',_token_record.datetime);
    END IF;
    RAISE NOTICE '% %', _token_dtrange, _dtrange;
    SELECT INTO prev_id items_by_partition(
        concat(whereq, ' AND ', filter_by_order(first_id, _search->'sortby', 'prev')),
        _token_dtrange,
        _rsort,
        1
    ) as id;
    RAISE NOTICE 'timing: %', age(clock_timestamp(), qstart);

    RAISE NOTICE 'prev_id: %', prev_id;
END IF;


RETURN QUERY
WITH features AS (
    SELECT jsonb_agg(content) features
    FROM items WHERE id IN (SELECT id FROM results_page LIMIT _limit)
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
