SET SEARCH_PATH TO pgstac, public;
BEGIN;

CREATE OR REPLACE FUNCTION stac_query_op(att text, _op text, val jsonb) RETURNS text AS $$
DECLARE
ret text := '';
op text;
jp text;
att_parts RECORD;
val_str text;
prop_path text;
BEGIN
val_str := lower(jsonb_build_object('a',val)->>'a');
RAISE NOTICE 'val_str %', val_str;

att_parts := split_stac_path(att);
prop_path := replace(att_parts.dotpath, 'properties.', '');

op := CASE _op
    WHEN 'eq' THEN '='
    WHEN 'gte' THEN '>='
    WHEN 'gt' THEN '>'
    WHEN 'lte' THEN '<='
    WHEN 'lt' THEN '<'
    WHEN 'ne' THEN '!='
    WHEN 'neq' THEN '!='
    WHEN 'startsWith' THEN 'LIKE'
    WHEN 'endsWith' THEN 'LIKE'
    WHEN 'contains' THEN 'LIKE'
    ELSE _op
END;

val_str := CASE _op
    WHEN 'startsWith' THEN concat(val_str, '%')
    WHEN 'endsWith' THEN concat('%', val_str)
    WHEN 'contains' THEN concat('%',val_str,'%')
    ELSE val_str
END;


RAISE NOTICE 'att_parts: % %', att_parts, count_by_delim(att_parts.dotpath,'\.');
IF
    op = '='
    AND att_parts.col = 'properties'
    --AND count_by_delim(att_parts.dotpath,'\.') = 2
THEN
    -- use jsonpath query to leverage index for eqaulity tests on single level deep properties
    jp := btrim(format($jp$ $.%I[*] ? ( @ == %s ) $jp$, replace(att_parts.dotpath, 'properties.',''), lower(val::text)::jsonb));
    raise notice 'jp: %', jp;
    ret := format($q$ properties @? %L $q$, jp);
ELSIF jsonb_typeof(val) = 'number' THEN
    ret := format('properties ? %L AND (%s)::numeric %s %s', prop_path, att_parts.jspathtext, op, val);
ELSE
    ret := format('properties ? %L AND %s %s %L', prop_path ,att_parts.jspathtext, op, val_str);
END IF;
RAISE NOTICE 'Op Query: %', ret;

return ret;
END;
$$ LANGUAGE PLPGSQL;


DROP FUNCTION IF EXISTS bbox_geom;
CREATE OR REPLACE FUNCTION bbox_geom(_bbox jsonb) RETURNS geometry AS $$
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
item items%ROWTYPE;
counter int := 0;
batchcount int;
month timestamptz;
m record;
_dtrange tstzrange := tstzrange('-infinity','infinity');
_dtsort text;
_token_dtrange tstzrange := tstzrange('-infinity','infinity');
_token_record items%ROWTYPE;
is_prev boolean := false;
includes text[];
excludes text[];
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
    SELECT INTO _token_record * FROM items WHERE id=tok_val;
    IF
        (is_prev AND _dtsort = 'DESC')
        OR
        (not is_prev AND _dtsort = 'ASC')
    THEN
        _token_dtrange := _dtrange * tstzrange(_token_record.datetime, 'infinity');
    ELSIF
        _dtsort IS NOT NULL
    THEN
        _token_dtrange := _dtrange * tstzrange('-infinity',_token_record.datetime);
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

IF _search ? 'fields' THEN
    IF _search->'fields' ? 'exclude' THEN
        excludes=textarr(_search->'fields'->'exclude');
    END IF;
    IF _search->'fields' ? 'include' THEN
        includes=textarr(_search->'fields'->'include');
        IF array_length(includes, 1)>0 AND NOT 'id' = ANY (includes) THEN
            includes = includes || '{id}';
        END IF;
    END IF;
    RAISE NOTICE 'Includes: %, Excludes: %', includes, excludes;
END IF;

whereq := COALESCE(array_to_string(qa,' AND '),' TRUE ');
dq := COALESCE(array_to_string(dqa,' AND '),' TRUE ');
RAISE NOTICE 'timing before temp table: %', age(clock_timestamp(), qstart);

CREATE TEMP TABLE results_page ON COMMIT DROP AS
SELECT * FROM items_by_partition(
    concat(whereq, ' AND ', tok_q),
    _token_dtrange,
    _sort,
    _limit + 1
);
RAISE NOTICE 'timing after temp table: %', age(clock_timestamp(), qstart);

RAISE NOTICE 'timing before min/max: %', age(clock_timestamp(), qstart);

IF is_prev THEN
    SELECT INTO last_id, first_id, counter
        first_value(id) OVER (),
        last_value(id) OVER (),
        count(*) OVER ()
    FROM results_page;
ELSE
    SELECT INTO first_id, last_id, counter
        first_value(id) OVER (),
        last_value(id) OVER (),
        count(*) OVER ()
    FROM results_page;
END IF;
RAISE NOTICE 'firstid: %, lastid %', first_id, last_id;
RAISE NOTICE 'timing after min/max: %', age(clock_timestamp(), qstart);




IF counter > _limit THEN
    next_id := last_id;
    RAISE NOTICE 'next_id: %', next_id;
ELSE
    RAISE NOTICE 'No more next';
END IF;

IF tok_q = 'TRUE' THEN
    RAISE NOTICE 'Not a paging query, no previous item';
ELSE
    RAISE NOTICE 'Getting previous item id';
    RAISE NOTICE 'timing: %', age(clock_timestamp(), qstart);
    SELECT INTO _token_record * FROM items WHERE id=first_id;
    IF
        _dtsort = 'DESC'
    THEN
        _token_dtrange := _dtrange * tstzrange(_token_record.datetime, 'infinity');
    ELSE
        _token_dtrange := _dtrange * tstzrange('-infinity',_token_record.datetime);
    END IF;
    RAISE NOTICE '% %', _token_dtrange, _dtrange;
    SELECT id INTO prev_id FROM items_by_partition(
        concat(whereq, ' AND ', filter_by_order(first_id, _search->'sortby', 'prev')),
        _token_dtrange,
        _rsort,
        1
    );
    RAISE NOTICE 'timing: %', age(clock_timestamp(), qstart);

    RAISE NOTICE 'prev_id: %', prev_id;
END IF;


RETURN QUERY
WITH features AS (
    SELECT filter_jsonb(content, includes, excludes) as content
    FROM results_page LIMIT _limit
),
j AS (SELECT jsonb_agg(content) as feature_arr FROM features)
SELECT jsonb_build_object(
    'type', 'FeatureCollection',
    'features', coalesce (
        CASE WHEN is_prev THEN flip_jsonb_array(feature_arr) ELSE feature_arr END
        ,'[]'::jsonb),
    'links', links,
    'timeStamp', now(),
    'next', next_id,
    'prev', prev_id
)
FROM j
;


END;
$$ LANGUAGE PLPGSQL SET SEARCH_PATH TO pgstac,public;
INSERT INTO migrations (version) VALUES ('0.2.7');

COMMIT;
