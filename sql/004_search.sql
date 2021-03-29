SET SEARCH_PATH TO pgstac, public;

/* Functions for searching items */
CREATE OR REPLACE FUNCTION sort(_sort jsonb) RETURNS text AS $$
WITH cols AS (
    SELECT * FROM jsonb_each(get_config('sort_columns'))
),
sort_statements AS (
    SELECT key, cols.value::text as sortstr, sort.value->>'direction' as direction FROM
        jsonb_array_elements(_sort) sort JOIN cols ON (cols.key=sort.value->>'field')
    UNION
    SELECT 'id', 'id', 'asc'
)
SELECT CASE WHEN _sort IS NULL THEN 'datetime desc' ELSE string_agg(concat(sortstr, ' ', direction),', ') END FROM sort_statements;
;
$$ LANGUAGE SQL PARALLEL SAFE;

CREATE OR REPLACE FUNCTION bbox_geom(_bbox jsonb) RETURNS box3d AS $$
SELECT CASE jsonb_array_length(_bbox)
    WHEN 4 THEN
        ST_MakeEnvelope(
            (_bbox->>0)::float,
            (_bbox->>1)::float,
            (_bbox->>2)::float,
            (_bbox->>3)::float
        )
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
SELECT format('%I = ANY(textarr(%L::jsonb))', col, arr);
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION geom_q(in_geom jsonb = NULL, in_box jsonb = NULL) RETURNS text AS $$
SELECT CASE
    WHEN in_geom IS NULL THEN
        format('ST_Intersects(geometry, ST_GeomFromGeoJSON(%L))', in_geom::text)
    WHEN in_box IS NULL THEN
        format('ST_Intersects(geometry, ST_GeomFromGeoJSON(%L))', in_geom::text)
END;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;


CREATE OR REPLACE FUNCTION search(_search jsonb = '{}'::jsonb) RETURNS SETOF jsonb AS $$
DECLARE
_sort text := '';
_limit int := 100;
_geom geometry;
qa text[];
pq text[];
query text;
pq_prop record;
pq_op record;
BEGIN
IF _search ? 'intersects' THEN
    _geom := ST_GeomFromGeoJSON(_search->>'intersects');
ELSIF _search ? 'bbox' THEN
    _geom := bbox_geom(_search->'bbox');
END IF;

IF _geom IS NOT NULL THEN
    qa := array_append(qa, format('st_intersects(geometry, %L::geometry)',_geom));
END IF;

IF _search ? 'collections' THEN
    qa := array_append(qa, format('collections_id = ANY(textarr(%L::jsonb))',_search->'collections'));
END IF;

IF _search ? 'items' THEN
    qa := array_append(qa, format('id = ANY(textarr(%L::jsonb))',_search->'items'));
END IF;


IF _search ? 'query' THEN
    qa := array_append(qa,
        stac_query(_search->'query')
    );
END IF;

IF _search ? 'limit' THEN
    _limit := (_search->>'limit')::int;
END IF;





query := format('
    WITH t AS (
    SELECT %s
    FROM _items
    WHERE %s
    ORDER BY %s
    LIMIT $1
    ) SELECT to_jsonb(t) FROM t;
    ',
    CASE WHEN _search ? 'fields' THEN array_idents(_search->'fields') ELSE '*' END,
    COALESCE(array_to_string(qa,' AND '),' TRUE '),
    sort(_search->'sortby')
);
RAISE NOTICE 'query: %', query;

RETURN QUERY EXECUTE query USING _limit;


END;
$$ LANGUAGE PLPGSQL;
