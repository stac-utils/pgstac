-- CREATE fixtures for testing search - as tests are run within a transaction, these will not persist

\copy items_staging (content) FROM 'tests/testdata/items.ndjson'

SET pgstac.context TO 'on';
SET pgstac."default_filter_lang" TO 'cql-json';

SELECT has_function('pgstac'::name, 'parse_dtrange', ARRAY['jsonb','timestamptz']);


SELECT results_eq($$ SELECT parse_dtrange('["2020-01-01","2021-01-01"]'::jsonb) $$, $$ SELECT '["2020-01-01 00:00:00+00","2021-01-01 00:00:00+00")'::tstzrange $$, 'daterange passed as array range');


SELECT results_eq($$ SELECT parse_dtrange('"2020-01-01/2021-01-01"'::jsonb) $$, $$ SELECT '["2020-01-01 00:00:00+00","2021-01-01 00:00:00+00")'::tstzrange $$, 'date range passed as string range');


SELECT results_eq($$ SELECT parse_dtrange('"2020-01-01/.."'::jsonb) $$, $$ SELECT '["2020-01-01 00:00:00+00",infinity)'::tstzrange $$, 'date range passed as string range');


SELECT results_eq($$ SELECT parse_dtrange('"2020-01-01/"'::jsonb) $$, $$ SELECT '["2020-01-01 00:00:00+00",infinity)'::tstzrange $$, 'date range passed as string range');


SELECT results_eq($$ SELECT parse_dtrange('"../2020-01-01"'::jsonb) $$, $$ SELECT '[-infinity,"2020-01-01 00:00:00+00")'::tstzrange $$, 'date range passed as string range');


SELECT results_eq($$ SELECT parse_dtrange('"/2020-01-01"'::jsonb) $$, $$ SELECT '[-infinity,"2020-01-01 00:00:00+00")'::tstzrange $$, 'date range passed as string range');


SELECT has_function('pgstac'::name, 'bbox_geom', ARRAY['jsonb']);


SELECT results_eq($$ SELECT bbox_geom('[0,1,2,3]') $$, $$ SELECT 'SRID=4326;POLYGON((0 1,0 3,2 3,2 1,0 1))'::geometry $$, '2d bbox');


SELECT results_eq($$ SELECT bbox_geom('[0,1,2,3,4,5]'::jsonb) $$, $$ SELECT '010F0000A0E610000006000000010300008001000000050000000000000000000000000000000000F03F00000000000000400000000000000000000000000000104000000000000000400000000000000840000000000000104000000000000000400000000000000840000000000000F03F00000000000000400000000000000000000000000000F03F0000000000000040010300008001000000050000000000000000000000000000000000F03F00000000000014400000000000000840000000000000F03F00000000000014400000000000000840000000000000104000000000000014400000000000000000000000000000104000000000000014400000000000000000000000000000F03F0000000000001440010300008001000000050000000000000000000000000000000000F03F00000000000000400000000000000000000000000000F03F00000000000014400000000000000000000000000000104000000000000014400000000000000000000000000000104000000000000000400000000000000000000000000000F03F0000000000000040010300008001000000050000000000000000000840000000000000F03F00000000000000400000000000000840000000000000104000000000000000400000000000000840000000000000104000000000000014400000000000000840000000000000F03F00000000000014400000000000000840000000000000F03F0000000000000040010300008001000000050000000000000000000000000000000000F03F00000000000000400000000000000840000000000000F03F00000000000000400000000000000840000000000000F03F00000000000014400000000000000000000000000000F03F00000000000014400000000000000000000000000000F03F000000000000004001030000800100000005000000000000000000000000000000000010400000000000000040000000000000000000000000000010400000000000001440000000000000084000000000000010400000000000001440000000000000084000000000000010400000000000000040000000000000000000000000000010400000000000000040'::geometry $$, '3d bbox');



SELECT has_function('pgstac'::name, 'keyset_orderby', ARRAY['jsonb','boolean']);

SELECT results_eq($$
    SELECT keyset_orderby('{"sortby":[{"field":"datetime","direction":"desc"},{"field":"eo:cloud_cover","direction":"asc"}]}'::jsonb);
    $$,$$
    SELECT 'datetime DESC, eo_cloud_cover ASC, id DESC, collection DESC';
    $$,
    'Test creation of sort sql'
);


SELECT results_eq($$
    SELECT keyset_orderby('{"sortby":[{"field":"datetime","direction":"desc"},{"field":"eo:cloud_cover","direction":"asc"}]}'::jsonb, true);
    $$,$$
    SELECT 'datetime ASC, eo_cloud_cover DESC, id ASC, collection ASC';
    $$,
    'Test creation of reverse sort sql'
);


SELECT has_function('pgstac'::name, 'search', ARRAY['jsonb']);


-- Follow the real keyset next-token to page 2, then page 2's prev-token back: the result must be
-- identical to a fresh no-token first read, including a null prev link (issue #140).
SELECT results_eq($$
    WITH base AS (SELECT '{"collections": ["pgstac-test-collection"], "limit": 10, "sortby":[{"field":"id","direction":"asc"}]}'::jsonb AS s),
    p1 AS (SELECT search((SELECT s FROM base))::jsonb AS j),
    nt AS (SELECT split_part(jsonb_path_query_first((SELECT j FROM p1), '$.links[*] ? (@.rel == "next").href')->>0, 'token=', 2) AS t),
    p2 AS (SELECT search((SELECT s FROM base) || jsonb_build_object('token', (SELECT t FROM nt)))::jsonb AS j),
    pt AS (SELECT split_part(jsonb_path_query_first((SELECT j FROM p2), '$.links[*] ? (@.rel == "prev").href')->>0, 'token=', 2) AS t)
    SELECT search((SELECT s FROM base) || jsonb_build_object('token', (SELECT t FROM pt)))::jsonb
    $$,$$
    SELECT search('{"collections": ["pgstac-test-collection"], "limit": 10, "sortby":[{"field":"id","direction":"asc"}]}')::jsonb
    $$,
    'prev token from page 2 returns the first page, identical to a no-token read (issue #140)'
);

-- A non-empty token that does not decode to a keyset is a client error, not a silent first page.
SELECT throws_ok($$
    SELECT search('{"collections": ["pgstac-test-collection"], "token": "next:pgstac-test-item-0010"}')
    $$, '22P02', NULL, 'a malformed pagination token raises rather than returning the first page');

-- The fields extension actually projects the result (regression guard: search must pass the
-- requested fields through to hydration, not return the whole item).
SELECT results_eq($$
    SELECT array_agg(k ORDER BY k) FROM jsonb_object_keys(jsonb_path_query_first(
        search('{"ids":["pgstac-test-item-0097"],"fields":{"include":["id"]}}')::jsonb, '$.features[0]')) k
    $$, $$ SELECT ARRAY['collection','id'] $$,
    'fields include:[id] returns only id + forced collection');

SELECT results_eq($$
    SELECT jsonb_path_query_first(
        search('{"ids":["pgstac-test-item-0097"],"fields":{"include":["id","properties.datetime"]}}')::jsonb,
        '$.features[0].properties') ?& array['datetime']
       AND NOT jsonb_path_query_first(
        search('{"ids":["pgstac-test-item-0097"],"fields":{"include":["id","properties.datetime"]}}')::jsonb,
        '$.features[0].properties') ? 'eo:cloud_cover'
    $$, $$ SELECT true $$,
    'fields include:[properties.datetime] narrows properties to the requested key');

SELECT results_eq($$
    SELECT NOT jsonb_path_query_first(
        search('{"ids":["pgstac-test-item-0097"],"fields":{"exclude":["assets"]}}')::jsonb,
        '$.features[0]') ? 'assets'
    $$, $$ SELECT true $$,
    'fields exclude:[assets] drops assets from the result');

-- include and exclude naming the same field: exclude wins (field_included and include_field agree).
SELECT results_eq($$
    SELECT NOT jsonb_path_query_first(
        search('{"ids":["pgstac-test-item-0097"],"fields":{"include":["id","assets"],"exclude":["assets"]}}')::jsonb,
        '$.features[0]') ? 'assets'
    $$, $$ SELECT true $$,
    'fields with a key in both include and exclude: exclude wins');


SELECT has_function('pgstac'::name, 'search_query', ARRAY['jsonb','jsonb']);

SELECT ok(
    to_regclass('pgstac.search_wheres') IS NULL,
    'search_wheres table removed'
);
SELECT ok(
    EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE
            table_schema = 'pgstac'
            AND table_name = 'searches'
            AND column_name = 'context_count'
    ),
    'searches table stores context_count cache'
);
SELECT ok(
    EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE
            table_schema = 'pgstac'
            AND table_name = 'searches'
            AND column_name = 'statslastupdated'
    ),
    'searches table stores statslastupdated for TTL'
);
SELECT results_eq(
    $$
        SELECT to_jsonb(array_agg(column_name ORDER BY column_name))
        FROM information_schema.columns
        WHERE table_schema = 'pgstac' AND table_name = 'searches'
    $$,
    $$
        SELECT to_jsonb(ARRAY[
            '_where',
            'context_count',
            'created_at',
            'hash',
            'lastused',
            'metadata',
            'orderby',
            'search',
            'statslastupdated',
            'usecount'
        ]::text[])
    $$,
    'searches table has only expected columns'
);

SELECT results_eq(
    $$
        SELECT search_hash(
            '{"collections":["pgstac-test-collection"],"limit":10,"token":"next:abc","context":"on","sortby":[{"field":"id","direction":"asc"}]}'::jsonb,
            '{}'::jsonb
        )
    $$,
    $$
        SELECT search_hash(
            '{"collections":["pgstac-test-collection"],"limit":1,"token":"prev:def","context":"off","sortby":[{"field":"datetime","direction":"desc"}]}'::jsonb,
            '{}'::jsonb
        )
    $$,
    'search_hash ignores pagination, token, context, and sort fields'
);


SELECT results_eq($$
    SELECT BTRIM(stac_search_to_where($q$
        {
            "intersects":
                {
                    "type": "Polygon",
                    "coordinates": [[
                        [-77.0824, 38.7886], [-77.0189, 38.7886],
                        [-77.0189, 38.8351], [-77.0824, 38.8351],
                        [-77.0824, 38.7886]
                    ]]
                }
        }
    $q$),E' \n');
    $$, $$
    SELECT BTRIM($r$
    st_intersects(geometry, '0103000020E61000000100000005000000304CA60A464553C014D044D8F06443403E7958A8354153C014D044D8F06443403E7958A8354153C0DE718A8EE46A4340304CA60A464553C0DE718A8EE46A4340304CA60A464553C014D044D8F0644340'::geometry)
    $r$,E' \n');
    $$, 'Make sure that intersects returns valid query'
);

-- CQL 2 Tests from examples at https://github.com/radiantearth/stac-api-spec/blob/f5da775080ff3ff46d454c2888b6e796ee956faf/fragments/filter/README.md

SET pgstac."default_filter_lang" TO 'cql2-json';

SELECT results_eq($$
    SELECT BTRIM(stac_search_to_where($q$
        {
            "filter": {
                "op" : "and",
                "args": [
                {
                    "op": "=",
                    "args": [ { "property": "id" }, "LC08_L1TP_060247_20180905_20180912_01_T1_L1TP" ]
                },
                {
                    "op": "=",
                    "args" : [ { "property": "collection" }, "landsat8_l1tp" ]
                }
                ]
            }
        }
    $q$),E' \n');
    $$, $$
    SELECT BTRIM($r$
    (id = 'LC08_L1TP_060247_20180905_20180912_01_T1_L1TP' AND collection = 'landsat8_l1tp')
    $r$,E' \n');
    $$, 'Test Example 1'
);


SELECT results_eq($$
    SELECT BTRIM(stac_search_to_where($q$
        {
            "filter-lang": "cql2-json",
            "filter": {
                "op": "and",
                "args": [
                {
                    "op": "=",
                    "args": [ { "property": "collection" }, "landsat8_l1tp" ]
                },
                {
                    "op": "<=",
                    "args": [ { "property": "eo:cloud_cover" }, "10" ]
                },
                {
                    "op": ">=",
                    "args": [ { "property": "datetime" }, {"timestamp": "2021-04-08T04:39:23Z"} ]
                },
                {
                    "op": "s_intersects",
                    "args": [
                    {
                        "property": "geometry"
                    },
                    {
                        "type": "Polygon",
                        "coordinates": [
                        [
                            [43.5845, -79.5442],
                            [43.6079, -79.4893],
                            [43.5677, -79.4632],
                            [43.6129, -79.3925],
                            [43.6223, -79.3238],
                            [43.6576, -79.3163],
                            [43.7945, -79.1178],
                            [43.8144, -79.1542],
                            [43.8555, -79.1714],
                            [43.7509, -79.6390],
                            [43.5845, -79.5442]
                        ]
                        ]
                    }
                    ]
                }
                ]
            }
            }
    $q$),E' \n');
    $$, $$
    SELECT BTRIM($r$
    (collection = 'landsat8_l1tp' AND eo_cloud_cover <= to_float('"10"') AND datetime >= '2021-04-08 04:39:23+00'::timestamptz AND st_intersects(geometry, '0103000020E6100000010000000B000000894160E5D0CA4540ED9E3C2CD4E253C0849ECDAACFCD4540B37BF2B050DF53C038F8C264AAC8454076E09C11A5DD53C0F5DBD78173CE454085EB51B81ED953C08126C286A7CF4540789CA223B9D453C0C0EC9E3C2CD4454063EE5A423ED453C004560E2DB2E5454001DE02098AC753C063EE5A423EE84540C442AD69DEC953C02FDD240681ED454034A2B437F8CA53C08048BF7D1DE0454037894160E5E853C0894160E5D0CA4540ED9E3C2CD4E253C0'::geometry))
    $r$,E' \n');
    $$, 'Test Example 2'
);


SELECT results_eq($$
    SELECT BTRIM(stac_search_to_where($q$
        {
            "filter-lang": "cql2-json",
            "filter": {
                "op": "and",
                "args": [
                {
                    "op": ">",
                    "args": [ { "property": "sentinel:data_coverage" }, "50" ]
                },
                {
                    "op": "<",
                    "args": [ { "property": "eo:cloud_cover" }, 10 ]
                }
                ]
            }
        }
    $q$),E' \n');
    $$, $$
    SELECT BTRIM($r$
    (to_text(properties->'sentinel:data_coverage') > to_text('"50"') AND eo_cloud_cover < to_float('10'))
    $r$,E' \n');
    $$, 'Test Example 3'
);



SELECT results_eq($$
    SELECT BTRIM(stac_search_to_where($q$
        {
            "filter-lang": "cql2-json",
            "filter": {
                "op": "or",
                "args": [
                {
                    "op": ">",
                    "args": [ { "property": "sentinel:data_coverage" }, 50 ]
                },
                {
                    "op": "<",
                    "args": [ { "property": "eo:cloud_cover" }, 10 ]
                }
                ]
            }
        }
    $q$),E' \n');
    $$, $$
    SELECT BTRIM($r$
    (to_float(properties->'sentinel:data_coverage') > to_float('50') OR eo_cloud_cover < to_float('10'))
    $r$,E' \n');
    $$, 'Test Example 4'
);



SELECT results_eq($$
    SELECT BTRIM(stac_search_to_where($q$
        {
            "filter-lang": "cql2-json",
            "filter": {
                "op": "eq",
                "args": [
                { "property": "prop1" },
                { "property": "prop2" }
                ]
            }
        }
    $q$),E' \n');
    $$, $$
    SELECT BTRIM($r$
    to_text(properties->'prop1') = to_text(properties->'prop2')
    $r$,E' \n');
    $$, 'Test Example 5'
);


SELECT results_eq($$
    SELECT BTRIM(stac_search_to_where($q$
       {
            "filter-lang": "cql2-json",
            "filter": {
                "op": "t_intersects",
                "args": [
                { "property": "datetime" },
                { "interval": [ "2020-11-11T00:00:00Z", "2020-11-12T00:00:00Z"] }
                ]
            }
        }
    $q$),E' \n');
    $$, $$
    SELECT BTRIM($r$
    (datetime <= '2020-11-12 00:00:00+00'::timestamptz AND end_datetime >= '2020-11-11 00:00:00+00'::timestamptz)
    $r$,E' \n');
    $$, 'Test Example 6'
);



SELECT results_eq($$
    SELECT BTRIM(stac_search_to_where($q$
        {
            "filter-lang": "cql2-json",
            "filter": {
                "op": "s_intersects",
                "args": [
                { "property": "geometry" } ,
                {
                    "type": "Polygon",
                    "coordinates": [[
                        [-77.0824, 38.7886], [-77.0189, 38.7886],
                        [-77.0189, 38.8351], [-77.0824, 38.8351],
                        [-77.0824, 38.7886]
                    ]]
                }
                ]
            }
        }
    $q$),E' \n');
    $$, $$
    SELECT BTRIM($r$
    st_intersects(geometry, '0103000020E61000000100000005000000304CA60A464553C014D044D8F06443403E7958A8354153C014D044D8F06443403E7958A8354153C0DE718A8EE46A4340304CA60A464553C0DE718A8EE46A4340304CA60A464553C014D044D8F0644340'::geometry)
    $r$,E' \n');
    $$, 'Test Example 7'
);



SELECT results_eq($$
    SELECT BTRIM(stac_search_to_where($q$
        {
            "filter": {
                "op": "or" ,
                "args": [
                {
                    "op": "s_intersects",
                    "args": [
                    { "property": "geometry" } ,
                    {
                        "type": "Polygon",
                        "coordinates": [[
                        [-77.0824, 38.7886], [-77.0189, 38.7886],
                        [-77.0189, 38.8351], [-77.0824, 38.8351],
                        [-77.0824, 38.7886]
                        ]]
                    }
                    ]
                },
                {
                    "op": "s_intersects",
                    "args": [
                    { "property": "geometry" } ,
                    {
                        "type": "Polygon",
                        "coordinates": [[
                        [-79.0935, 38.7886], [-79.0290, 38.7886],
                        [-79.0290, 38.8351], [-79.0935, 38.8351],
                        [-79.0935, 38.7886]
                        ]]
                    }
                    ]
                }
                ]
            }
        }
    $q$),E' \n');
    $$, $$
    SELECT BTRIM($r$
    (st_intersects(geometry, '0103000020E61000000100000005000000304CA60A464553C014D044D8F06443403E7958A8354153C014D044D8F06443403E7958A8354153C0DE718A8EE46A4340304CA60A464553C0DE718A8EE46A4340304CA60A464553C014D044D8F0644340'::geometry) OR st_intersects(geometry, '0103000020E61000000100000005000000448B6CE7FBC553C014D044D8F064434060E5D022DBC153C014D044D8F064434060E5D022DBC153C0DE718A8EE46A4340448B6CE7FBC553C0DE718A8EE46A4340448B6CE7FBC553C014D044D8F0644340'::geometry))
    $r$,E' \n');
    $$, 'Test Example 8'
);


SELECT results_eq($$
    SELECT BTRIM(stac_search_to_where($q$
        {
            "filter-lang": "cql2-json",
            "filter": {
                "op": "or",
                "args": [
                {
                    "op": ">=",
                    "args": [ { "property": "sentinel:data_coverage" }, 50 ]
                },
                {
                    "op": ">=",
                    "args": [ { "property": "landsat:coverage_percent" }, 50 ]
                },
                {
                    "op": "and",
                    "args": [
                    {
                        "op": "isNull",
                        "args": { "property": "sentinel:data_coverage" }
                    },
                    {
                        "op": "isNull",
                        "args": { "property": "landsat:coverage_percent" }
                    }
                    ]
                }
                ]
            }
        }
    $q$),E' \n');
    $$, $$
    SELECT BTRIM($r$
    (to_float(properties->'sentinel:data_coverage') >= to_float('50') OR to_float(properties->'landsat:coverage_percent') >= to_float('50') OR (to_text(properties->'sentinel:data_coverage') IS NULL AND to_text(properties->'landsat:coverage_percent') IS NULL))
    $r$,E' \n');
    $$, 'Test Example 9'
);


SELECT results_eq($$
    SELECT BTRIM(stac_search_to_where($q$
    {
        "filter-lang": "cql2-json",
        "filter": {
            "op": "between",
            "args": [
            { "property": "eo:cloud_cover" },
            0, 50
            ]
        }
    }
    $q$),E' \n');
    $$, $$
    SELECT BTRIM($r$
    eo_cloud_cover BETWEEN to_float('0') AND to_float('50')
    $r$,E' \n');
    $$, 'Test Example 10'
);


SELECT results_eq($$
    SELECT BTRIM(stac_search_to_where($q$
    {
        "filter-lang": "cql2-json",
        "filter": {
            "op": "like",
            "args": [
            { "property": "mission" },
            "sentinel%"
            ]
        }
    }
    $q$),E' \n');
    $$, $$
        SELECT BTRIM($r$
        mission LIKE to_text('"sentinel%"')
    $r$,E' \n');
    $$, 'Test Example 11'
);

SELECT results_eq($$
    SELECT BTRIM(stac_search_to_where($q$
    {
        "filter-lang": "cql2-json",
        "filter": {
            "op": "eq",
            "args": [
            {"upper": { "property": "mission" }},
            {"upper": "sentinel"}
            ]
        }
    }
    $q$),E' \n');
    $$, $$
        SELECT BTRIM($r$
        upper(mission) = upper(to_text('"sentinel"'))
    $r$,E' \n');
    $$, 'Test upper'
);

SELECT results_eq($$
    SELECT BTRIM(stac_search_to_where($q$
    {
        "filter-lang": "cql2-json",
        "filter": {
            "op": "eq",
            "args": [
            {"lower": { "property": "mission" }},
            {"lower": "sentinel"}
            ]
        }
    }
    $q$),E' \n');
    $$, $$
        SELECT BTRIM($r$
        lower(mission) = lower(to_text('"sentinel"'))
    $r$,E' \n');
    $$, 'Test lower'
);

SELECT results_eq($$
    SELECT BTRIM(stac_search_to_where($q$
    {
        "filter-lang": "cql2-json",
        "filter": {
            "op": "eq",
            "args": [
            {"op": "casei", "args":[{ "property": "mission" }]},
            {"op": "casei", "args":["sentinel"]}
            ]
        }
    }
    $q$),E' \n');
    $$, $$
        SELECT BTRIM($r$
        upper(mission) = upper(to_text('"sentinel"'))
    $r$,E' \n');
    $$, 'Test casei'
);

SELECT results_eq($$
    SELECT BTRIM(stac_search_to_where($q$
    {
        "filter-lang": "cql2-json",
        "filter": {
            "op": "eq",
            "args": [
            {"op": "accenti", "args":[{ "property": "mission" }]},
            {"op": "accenti", "args":["sentinel"]}
            ]
        }
    }
    $q$),E' \n');
    $$, $$
        SELECT BTRIM($r$
        unaccent(mission) = unaccent(to_text('"sentinel"'))
    $r$,E' \n');
    $$, 'Test accenti'
);



/* template
SELECT results_eq($$

    $$,$$

    $$,
    'Test that ...'
);
*/

CREATE OR REPLACE FUNCTION pg_temp.isnull(j jsonb) RETURNS boolean AS $$
    SELECT nullif(j, 'null'::jsonb) IS NULL;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION pg_temp.isnull(t text) RETURNS boolean AS $$
    SELECT t IS NULL;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION pg_temp.prev(j jsonb) RETURNS text AS $$
    SELECT split_part(jsonb_path_query_first(j, '$.links[*] ? (@.rel == "prev") .href')->>0, 'token=', 2);
$$ LANGUAGE SQL IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION pg_temp.next(j jsonb) RETURNS text AS $$
    SELECT split_part(jsonb_path_query_first(j, '$.links[*] ? (@.rel == "next") .href')->>0, 'token=', 2);
$$ LANGUAGE SQL IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION pg_temp.testpaging(testsortdir text, iddir text) RETURNS SETOF TEXT LANGUAGE plpgsql AS $$
DECLARE
    searchfilter jsonb;
    searchresult jsonb;
    offsetids text;
    searchresultids text;
    page int := 0;
    token text;
BEGIN
    RAISE NOTICE 'Testing % %', testsortdir, iddir;
    -- Create collection with items that have a field with nulls and duplicate values
    DELETE FROM items WHERE collection = 'pgstac-test-collection2';

    INSERT INTO collections (content) VALUES ('{"id":"pgstac-test-collection2"}'::jsonb) ON CONFLICT DO NOTHING;
    PERFORM check_partition('pgstac-test-collection2', '[2011-01-01,2012-01-01)', '[2011-01-01,2012-01-01)');

    INSERT INTO items (id, collection, datetime, end_datetime, geometry, bbox, links, assets, properties, extra, item_hash)
        SELECT concat(id, '_2'), 'pgstac-test-collection2', datetime, end_datetime, geometry, bbox, links, assets, properties, extra, item_hash
        FROM items WHERE collection='pgstac-test-collection';

    UPDATE items SET properties = properties || '{"testsort":1}'::jsonb
        WHERE collection = 'pgstac-test-collection2' AND
        id <= 'pgstac-test-item-0005_2';
    UPDATE items SET properties = properties || '{"testsort":2}'::jsonb
        WHERE collection = 'pgstac-test-collection2' AND
        id > 'pgstac-test-item-0005_2' and id <= 'pgstac-test-item-0010_2';
    UPDATE items SET properties = properties || '{"testsort":3}'::jsonb
        WHERE collection = 'pgstac-test-collection2' AND
        id > 'pgstac-test-item-0010' and id <= 'pgstac-test-item-0015_2';

    RETURN NEXT results_eq(
        $q$
        SELECT count(*) FROM items WHERE collection = 'pgstac-test-collection2';
        $q$, $q$
        SELECT 100::bigint;
        $q$,
        'pgstac-test-collection2 has 100 items'
    );

    searchfilter := '{"collections":["pgstac-test-collection2"],"fields":{"include":["id","properties.datetime","properties.testsort"]},"sortby":[{"field":"testsort","direction":null},{"field":"id","direction":null}]}'::jsonb;

    searchfilter := jsonb_set(searchfilter, '{sortby,0,direction}'::text[], to_jsonb(testsortdir));
    searchfilter := jsonb_set(searchfilter, '{sortby,1,direction}'::text[], to_jsonb(iddir));

    RAISE NOTICE 'SORTBY: %', searchfilter->>'sortby';

    searchresult := search(searchfilter);

    RETURN NEXT ok(pg_temp.isnull(pg_temp.prev(searchresult)), 'first prev is null');

    -- page up
    WHILE page <= 100 LOOP
        EXECUTE format($q$
                WITH t AS (
                SELECT id
                FROM items
                WHERE collection='pgstac-test-collection2'
                ORDER BY properties->>'testsort' %s, id %s
                OFFSET %L LIMIT 10
                ) SELECT string_agg(id, ',') FROM t
                $q$,
                testsortdir,
                iddir,
                page
            ) INTO offsetids;
        EXECUTE format($q$
            SELECT string_agg(q->>0, ',') FROM jsonb_path_query(%L, '$.features[*].id') as q;
            $q$, searchresult) INTO searchresultids;
        RAISE NOTICE 'O: %', offsetids;
        RAISE NOTICE 'S: %', searchresultids;
        RETURN NEXT results_eq(
            format($q$
                SELECT id
                FROM items
                WHERE collection='pgstac-test-collection2'
                ORDER BY properties->>'testsort' %s, id %s
                OFFSET %L LIMIT 10
                $q$,
                testsortdir,
                iddir,
                page
            ),
            format($q$
            SELECT q->>0 FROM jsonb_path_query(%L, '$.features[*].id') as q;
            $q$, searchresult),
            format('Going up %s/%s page:%s results match using offset', testsortdir, iddir, page)
        );

        IF pg_temp.isnull(pg_temp.next(searchresult)) THEN
            EXIT;
        END IF;
        searchfilter := searchfilter || jsonb_build_object('token', pg_temp.next(searchresult));
        RAISE NOTICE 'SEARCHFILTER: %', searchfilter;
        searchresult := search(searchfilter);
        RAISE NOTICE 'SEARCHRESULT: %', searchresult;
        RAISE NOTICE 'PAGE:% TOKEN:% LINKS:%', page, searchfilter->>'token', searchresult->'links';
        page := page + 10;
    END LOOP;

    RETURN NEXT ok(pg_temp.isnull(pg_temp.next(searchresult)), 'last next is null');
    RETURN NEXT ok(page=90, 'last page going up is 90');
    -- page down
    WHILE page >= 0 LOOP
        IF page < 10 THEN
            EXIT;
        END IF;
        page := page - 10;
        searchfilter := searchfilter || jsonb_build_object('token', pg_temp.prev(searchresult));
        RAISE NOTICE 'SEARCHFILTER: %', searchfilter;
        searchresult := search(searchfilter);
        RAISE NOTICE 'SEARCHRESULT: %', searchresult;
        RAISE NOTICE 'PAGE:% TOKEN:% LINKS:%', page, searchfilter->>'token', searchresult->>'links';
        EXECUTE format($q$
                WITH t AS (
                SELECT id
                FROM items
                WHERE collection='pgstac-test-collection2'
                ORDER BY properties->>'testsort' %s, id %s
                OFFSET %L LIMIT 10
                ) SELECT string_agg(id, ',') FROM t
                $q$,
                testsortdir,
                iddir,
                page
            ) INTO offsetids;
        EXECUTE format($q$
            SELECT string_agg(q->>0, ',') FROM jsonb_path_query(%L, '$.features[*].id') as q;
            $q$, searchresult) INTO searchresultids;
        RAISE NOTICE 'O: %', offsetids;
        RAISE NOTICE 'S: %', searchresultids;
        RETURN NEXT results_eq(
            format($q$
                SELECT id
                FROM items
                WHERE collection='pgstac-test-collection2'
                ORDER BY properties->>'testsort' %s, id %s
                OFFSET %L LIMIT 10
                $q$,
                testsortdir,
                iddir,
                page
            ),
            format($q$
            SELECT q->>0 FROM jsonb_path_query(%L, '$.features[*].id') as q;
            $q$, searchresult),
            format('Going down %s/%s page:%s results match using offset', testsortdir, iddir, page)
        );

        IF pg_temp.isnull(pg_temp.prev(searchresult)) THEN
            EXIT;
        END IF;
    END LOOP;
    RETURN NEXT ok(pg_temp.isnull(pg_temp.prev(searchresult)), 'last prev is null');
    RETURN NEXT ok(page=0, 'last page going down is 0');
END;
$$;

SELECT * FROM pg_temp.testpaging('asc','asc');
SELECT * FROM pg_temp.testpaging('asc','desc');
SELECT * FROM pg_temp.testpaging('desc','desc');
SELECT * FROM pg_temp.testpaging('desc','asc');

\copy items_staging (content) FROM 'tests/testdata/items_duplicate_ids.ndjson'

SELECT is(
    (SELECT json_array_length(search('{"ids": ["pgstac-test-item-duplicated"]}')->'features')),
    '2',
    'Make sure all matching items are returned when items with the same ID are in multiple collections, no collections specified. #192'
);

SELECT is(
    (SELECT json_array_length(search('{"ids": ["pgstac-test-item-duplicated"], "collections": ["pgstac-test-collection"]}')->'features')),
    '1',
    'Make sure all matching items are returned when items with the same ID are in multiple collections, some collections specified. #192'
);

SELECT is(
    (SELECT json_array_length(search('{"ids": ["pgstac-test-item-duplicated"], "collections": ["pgstac-test-collection", "pgstac-test-collection2"]}')->'features')),
    '2',
    'Make sure all matching items are returned when items with the same ID are in multiple collections, all collections specified. #192'
);
