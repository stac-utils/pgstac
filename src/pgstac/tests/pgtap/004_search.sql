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



SELECT has_function('pgstac'::name, 'sort_sqlorderby', ARRAY['jsonb','boolean']);

SELECT results_eq($$
    SELECT sort_sqlorderby('{"sortby":[{"field":"datetime","direction":"desc"},{"field":"eo:cloud_cover","direction":"asc"}]}'::jsonb);
    $$,$$
    SELECT 'datetime DESC, eo_cloud_cover ASC, id DESC';
    $$,
    'Test creation of sort sql'
);


SELECT results_eq($$
    SELECT sort_sqlorderby('{"sortby":[{"field":"datetime","direction":"desc"},{"field":"eo:cloud_cover","direction":"asc"}]}'::jsonb, true);
    $$,$$
    SELECT 'datetime ASC, eo_cloud_cover DESC, id ASC';
    $$,
    'Test creation of reverse sort sql'
);


SELECT has_function('pgstac'::name, 'search', ARRAY['jsonb']);


-- v0.10: tokens are opaque keyset encodings of the sort-key values (was collection:id).
-- Build the equivalent prev token for item-0011 (sortby id → keyset is [id, collection]).
SELECT results_eq($$
    SELECT search(
        '{"collections": ["pgstac-test-collection"], "limit": 10, "sortby":[{"field":"id","direction":"asc"}]}'::jsonb
        || jsonb_build_object('token', 'prev:' || keyset_encode(ARRAY['pgstac-test-item-0011','pgstac-test-collection']))
    )
    $$,$$
    SELECT search('{"collections": ["pgstac-test-collection"], "limit": 10, "sortby":[{"field":"id","direction":"asc"}]}')
    $$,
    'Test prev token when reading first token_type=prev (https://github.com/stac-utils/pgstac/issues/140)'
);


SELECT has_function('pgstac'::name, 'search_query', ARRAY['jsonb','boolean','jsonb']);
SELECT has_function('pgstac'::name, 'name_search', ARRAY['jsonb','text','jsonb']);
SELECT has_function('pgstac'::name, 'rename_search', ARRAY['text','text']);
SELECT has_function('pgstac'::name, 'unname_search', ARRAY['text']);
SELECT has_function('pgstac'::name, 'pin_search', ARRAY['text']);
SELECT has_function('pgstac'::name, 'unpin_search', ARRAY['text']);
SELECT has_function('pgstac'::name, 'search_gc_retention_interval', ARRAY['jsonb']);
SELECT has_function('pgstac'::name, 'gc_anonymous_searches', ARRAY['interval','jsonb']);
SELECT has_function('pgstac'::name, 'gc_search_caches', ARRAY['interval','jsonb']);

SELECT results_eq(
    $$ SELECT (name_search('{"collections":["pgstac-test-collection"]}'::jsonb, 'pgstac-test-named-search')).name $$,
    $$ SELECT 'pgstac-test-named-search'::text $$,
    'name_search assigns a stable name'
);
SELECT results_eq(
    $$ SELECT (rename_search('pgstac-test-named-search', 'pgstac-test-renamed-search')).name $$,
    $$ SELECT 'pgstac-test-renamed-search'::text $$,
    'rename_search renames an existing named search'
);
SELECT results_eq(
    $$ SELECT (pin_search('pgstac-test-renamed-search')).pinned $$,
    $$ SELECT TRUE $$,
    'pin_search sets pinned=true'
);
SELECT results_eq(
    $$ SELECT (unpin_search('pgstac-test-renamed-search')).pinned $$,
    $$ SELECT FALSE $$,
    'unpin_search sets pinned=false'
);
SELECT results_eq(
    $$ SELECT (unname_search('pgstac-test-renamed-search')).name IS NULL $$,
    $$ SELECT TRUE $$,
    'unname_search clears search name'
);
SELECT results_eq(
    $$ SELECT search_gc_retention_interval('{"search_gc_retention_interval":"3 days"}'::jsonb) $$,
    $$ SELECT '3 days'::interval $$,
    'GC retention interval honors conf override'
);
SELECT lives_ok(
    $$
        INSERT INTO searches (
            hash,
            search,
            _where,
            orderby,
            metadata,
            lastused,
            usecount,
            pinned,
            name
        ) VALUES (
            pgstac_hash('gc-test-row-' || clock_timestamp()::text),
            '{}'::jsonb,
            'TRUE',
            'datetime DESC, id DESC',
            '{}'::jsonb,
            now() - '2 days'::interval,
            1,
            false,
            NULL
        )
    $$,
    'Seed an old anonymous search row for GC test'
);
SELECT results_eq(
    $$ SELECT gc_anonymous_searches(NULL, '{"search_gc_retention_interval":"1 day"}'::jsonb) > 0 $$,
    $$ SELECT TRUE $$,
    'gc_anonymous_searches uses retention from conf when interval arg is null'
);

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
            'name',
            'orderby',
            'pinned',
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
    st_intersects(geometry, '0103000020E61000000100000005000000304CA60A464553C014D044D8F06443403E7958A8354153C014D044D8F06443403E7958A8354153C0DE718A8EE46A4340304CA60A464553C0DE718A8EE46A4340304CA60A464553C014D044D8F0644340')
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
    (SELECT jsonb_array_length(search('{"ids": ["pgstac-test-item-duplicated"]}')->'features')),
    '2',
    'Make sure all matching items are returned when items with the same ID are in multiple collections, no collections specified. #192'
);

SELECT is(
    (SELECT jsonb_array_length(search('{"ids": ["pgstac-test-item-duplicated"], "collections": ["pgstac-test-collection"]}')->'features')),
    '1',
    'Make sure all matching items are returned when items with the same ID are in multiple collections, some collections specified. #192'
);

SELECT is(
    (SELECT jsonb_array_length(search('{"ids": ["pgstac-test-item-duplicated"], "collections": ["pgstac-test-collection", "pgstac-test-collection2"]}')->'features')),
    '2',
    'Make sure all matching items are returned when items with the same ID are in multiple collections, all collections specified. #192'
);

-- ============================================================
-- fields_to_columns tests
-- ============================================================

SELECT has_function('pgstac'::name, 'fields_to_columns', ARRAY['jsonb']);

-- Empty fields spec: return everything including fragment columns via correlated subqueries
SELECT ok(
    fields_to_columns('{}') LIKE '%i.*%',
    'fields_to_columns({}): includes i.*'
);
SELECT ok(
    fields_to_columns('{}') LIKE '%frag_content%',
    'fields_to_columns({}): includes frag_content'
);
SELECT ok(
    fields_to_columns('{}') LIKE '%frag_links_template%',
    'fields_to_columns({}): includes frag_links_template'
);
SELECT ok(
    fields_to_columns('{}') LIKE '%item_fragments%',
    'fields_to_columns({}): references item_fragments via correlated subquery'
);

-- include id and datetime: those columns appear, geometry does not
SELECT ok(
    fields_to_columns('{"include":["id","datetime"]}') LIKE '%i.id%',
    'fields_to_columns include id: i.id present'
);

SELECT ok(
    fields_to_columns('{"include":["id","datetime"]}') LIKE '%i.datetime%',
    'fields_to_columns include datetime: i.datetime present'
);

SELECT ok(
    fields_to_columns('{"include":["id","datetime"]}') NOT LIKE '%i.geometry%',
    'fields_to_columns include id,datetime: i.geometry absent'
);

-- include geometry and assets: geometry column present, fragment needed for assets
SELECT ok(
    fields_to_columns('{"include":["geometry","assets"]}') LIKE '%i.geometry%',
    'fields_to_columns include geometry,assets: i.geometry present'
);

SELECT ok(
    fields_to_columns('{"include":["geometry","assets"]}') LIKE '%frag_content%',
    'fields_to_columns include geometry,assets: frag_content present (fragment needed for assets)'
);

-- include bbox only: bbox column present, geometry absent, no frag_content
SELECT ok(
    fields_to_columns('{"include":["bbox"]}') LIKE '%i.bbox%',
    'fields_to_columns include bbox: i.bbox present'
);

SELECT ok(
    fields_to_columns('{"include":["bbox"]}') NOT LIKE '%i.geometry%',
    'fields_to_columns include bbox: i.geometry absent'
);

SELECT ok(
    fields_to_columns('{"include":["bbox"]}') NOT LIKE '%frag_content%',
    'fields_to_columns include bbox: frag_content absent'
);

-- ============================================================
-- search_sql tests
-- ============================================================

SELECT has_function('pgstac'::name, 'search_sql', ARRAY['jsonb','text']);

-- item mode (default): FROM items i (no outer JOIN), content_hydrate with correlated fragment subquery
SELECT ok(
    search_sql('{}') LIKE '%FROM items i%',
    'search_sql item mode: contains FROM items i'
);

SELECT ok(
    search_sql('{}') NOT LIKE '%LEFT JOIN%',
    'search_sql item mode: no outer LEFT JOIN (fragment via correlated subquery)'
);

SELECT ok(
    search_sql('{}') LIKE '%content_hydrate%',
    'search_sql item mode: contains content_hydrate'
);

SELECT ok(
    search_sql('{}') LIKE '%item_fragments%',
    'search_sql item mode: references item_fragments (via correlated subquery)'
);

-- row mode: FROM items i, frag_content via correlated subquery, NO content_hydrate
SELECT ok(
    search_sql('{}', 'row') LIKE '%FROM items i%',
    'search_sql row mode: contains FROM items i'
);

SELECT ok(
    search_sql('{}', 'row') NOT LIKE '%LEFT JOIN%',
    'search_sql row mode: no outer LEFT JOIN (fragment via correlated subquery)'
);

SELECT ok(
    search_sql('{}', 'row') LIKE '%frag_content%',
    'search_sql row mode: contains frag_content'
);

SELECT ok(
    search_sql('{}', 'row') NOT LIKE '%content_hydrate%',
    'search_sql row mode: does NOT contain content_hydrate'
);

-- collection filter propagated into SQL
SELECT ok(
    search_sql('{"collections":["pgstac-test-collection"]}') LIKE '%pgstac-test-collection%',
    'search_sql with collections filter: collection name present in SQL'
);

-- default mode is item
SELECT is(
    search_sql('{}'),
    search_sql('{}', 'item'),
    'search_sql default mode equals explicit item mode'
);

-- ============================================================
-- search_cursor tests
-- ============================================================

SELECT has_function('pgstac'::name, 'search_cursor', ARRAY['jsonb','text','refcursor']);

-- item mode: cursor opens and first FETCH is a Feature
DO $$
DECLARE
    cur refcursor;
    row1 jsonb;
BEGIN
    cur := search_cursor('{"collections":["pgstac-test-collection"],"limit":3}'::jsonb, 'item', 'test_stream_item');
    FETCH NEXT FROM cur INTO row1;
    IF (row1->>'type') IS DISTINCT FROM 'Feature' THEN
        RAISE EXCEPTION 'Expected Feature, got %', row1->>'type';
    END IF;
    CLOSE cur;
END;
$$;
SELECT ok(true, 'search_cursor item mode: first FETCH row has type = Feature');

-- item mode: first fetched row has an id field
DO $$
DECLARE
    cur refcursor;
    row1 jsonb;
BEGIN
    cur := search_cursor('{"collections":["pgstac-test-collection"],"limit":3}'::jsonb, 'item', 'test_stream_id');
    FETCH NEXT FROM cur INTO row1;
    IF row1->>'id' IS NULL THEN
        RAISE EXCEPTION 'Expected id field, got null';
    END IF;
    CLOSE cur;
END;
$$;
SELECT ok(true, 'search_cursor item mode: first FETCH row has non-null id');

-- row mode: cursor opens without error (FETCH NEXT without INTO is ambiguous in
-- PL/pgSQL blocks; just verify the cursor is non-null and can be opened)
SELECT ok(
    search_cursor(
        '{"collections":["pgstac-test-collection"]}'::jsonb,
        'row',
        'test_stream_row2'
    ) IS NOT NULL,
    'search_cursor row mode: returns a non-null refcursor'
);

-- ============================================================
-- content_hydrate 3-arg / 2-arg parity
-- ============================================================

SELECT results_eq(
    $$ SELECT content_hydrate(i, f, '{}'::jsonb) FROM items i LEFT JOIN item_fragments f ON f.id = i.fragment_id WHERE i.collection = 'pgstac-test-collection' ORDER BY i.id LIMIT 1 $$,
    $$ SELECT content_hydrate(i, '{}'::jsonb) FROM items i WHERE i.collection = 'pgstac-test-collection' ORDER BY i.id LIMIT 1 $$,
    '3-arg content_hydrate matches 2-arg form for pgstac-test-collection'
);

-- ============================================================
-- Row-mode wire contract: full-corpus parity
--
-- The Rust client consumes `row` mode (raw split-storage columns + the inline
-- fragment shipped as the loose columns frag_content / frag_links_template) and
-- rehydrates client-side. That client output MUST equal `item` mode (server-side
-- content_hydrate). The fragment travels on the wire as exactly three fields:
-- id, content, links_template (see fields_to_columns / item_fragments). So the
-- faithful conformance check is: rebuild an item_fragments from ONLY those three
-- wire fields, hydrate, and require byte-identical output to item mode for EVERY
-- item. If content_hydrate ever starts reading another fragment column (e.g.
-- hash) that row mode does not project, or row mode drops a needed column, this
-- fails. Covers the whole loaded corpus (every item shape), not a single row.
-- ============================================================

-- Add link-bearing items (the base corpus has none) so the link reconstruction
-- path (links_template fragment + per-item link_hrefs -> stac_links_hydrate) is
-- exercised by the parity check below. The two share a link/asset shape, so the
-- template dedups into one fragment and hrefs stay per-item.
INSERT INTO items_staging (content) VALUES
('{"type":"Feature","id":"rowmode-links-1","collection":"pgstac-test-collection","stac_version":"1.0.0","stac_extensions":[],"geometry":{"type":"Point","coordinates":[0,0]},"bbox":[0,0,0,0],"properties":{"datetime":"2021-01-01T00:00:00Z"},"assets":{"data":{"href":"https://example.com/1.tif","type":"image/tiff","roles":["data"]}},"links":[{"rel":"self","href":"https://example.com/items/rowmode-links-1","type":"application/geo+json"},{"rel":"collection","href":"https://example.com/collections/pgstac-test-collection","type":"application/json"}]}'),
('{"type":"Feature","id":"rowmode-links-2","collection":"pgstac-test-collection","stac_version":"1.0.0","stac_extensions":[],"geometry":{"type":"Point","coordinates":[1,1]},"bbox":[1,1,1,1],"properties":{"datetime":"2021-01-02T00:00:00Z"},"assets":{"data":{"href":"https://example.com/2.tif","type":"image/tiff","roles":["data"]}},"links":[{"rel":"self","href":"https://example.com/items/rowmode-links-2","type":"application/geo+json"},{"rel":"collection","href":"https://example.com/collections/pgstac-test-collection","type":"application/json"}]}');

SELECT cmp_ok(
    (SELECT count(*)::int FROM items WHERE collection = 'pgstac-test-collection'),
    '>=', 100,
    'row-mode parity: corpus loaded (parity check is non-vacuous)'
);

SELECT cmp_ok(
    (SELECT count(*)::int FROM items
     WHERE id IN ('rowmode-links-1','rowmode-links-2') AND link_hrefs IS NOT NULL),
    '>=', 2,
    'row-mode parity: link-bearing fixtures loaded with link_hrefs populated'
);

SELECT cmp_ok(
    (SELECT count(*)::int FROM items i
     WHERE i.id IN ('rowmode-links-1','rowmode-links-2')
       AND content_hydrate(i, (SELECT f FROM item_fragments f WHERE f.id = i.fragment_id), '{}'::jsonb) ? 'links'),
    '>=', 2,
    'row-mode parity: link-bearing items hydrate with a reconstructed links array'
);

-- CORE: rebuild fragment from wire fields only -> must equal item mode for all items.
SELECT is(
    (
        SELECT count(*)::int
        FROM items i
        WHERE content_hydrate(
                  i,
                  (SELECT f FROM item_fragments f WHERE f.id = i.fragment_id),
                  '{}'::jsonb
              )
          IS DISTINCT FROM
              content_hydrate(
                  i,
                  jsonb_populate_record(
                      NULL::item_fragments,
                      jsonb_build_object(
                          'id',             i.fragment_id,
                          'content',        (SELECT content        FROM item_fragments WHERE id = i.fragment_id),
                          'links_template', (SELECT links_template FROM item_fragments WHERE id = i.fragment_id)
                      )
                  ),
                  '{}'::jsonb
              )
    ),
    0,
    'row-mode parity: fragment rebuilt from wire fields (id, content, links_template) hydrates identically to item mode for every item'
);

-- Parity also holds under a fields include projection (fragment reconstruction is
-- lossless across the field filter, not just for the full document).
SELECT is(
    (
        SELECT count(*)::int
        FROM items i
        WHERE content_hydrate(
                  i,
                  (SELECT f FROM item_fragments f WHERE f.id = i.fragment_id),
                  '{"include":["id","datetime","assets","properties"]}'::jsonb
              )
          IS DISTINCT FROM
              content_hydrate(
                  i,
                  jsonb_populate_record(
                      NULL::item_fragments,
                      jsonb_build_object(
                          'id',             i.fragment_id,
                          'content',        (SELECT content        FROM item_fragments WHERE id = i.fragment_id),
                          'links_template', (SELECT links_template FROM item_fragments WHERE id = i.fragment_id)
                      )
                  ),
                  '{"include":["id","datetime","assets","properties"]}'::jsonb
              )
    ),
    0,
    'row-mode parity: holds under a fields include projection'
);

-- ============================================================
-- Keyset pagination coverage
--
-- Tokens are opaque keyset encodings of the sort-key values. The Rust client
-- drives pagination, so lock: encode/decode round-trips (incl. NULL sort values),
-- keyset_sortkeys tiebreak/dedup/direction rules, keyset_where direction-awareness,
-- and a full prev -> next -> prev round-trip that returns the original page.
-- ============================================================

-- encode/decode round-trips opaque values (spaces, colons, base64-special bytes).
SELECT is(
    keyset_decode(keyset_encode(ARRAY['2021-01-01 00:00:00+00','pgstac-test-item-0011','pgstac-test-collection'])),
    ARRAY['2021-01-01 00:00:00+00','pgstac-test-item-0011','pgstac-test-collection'],
    'keyset encode/decode: round-trips multi-element values with spaces and colons'
);

-- NULL sort-key values survive the round-trip (chr(30) sentinel).
SELECT is(
    keyset_decode(keyset_encode(ARRAY['a',NULL,'c'])),
    ARRAY['a',NULL,'c'],
    'keyset encode/decode: preserves NULL sort-key values'
);

-- Default (no sortby): datetime DESC, with id + collection appended as unique tiebreaks.
SELECT is(
    (SELECT array_agg(field||':'||dir ORDER BY ord) FROM keyset_sortkeys('{}'::jsonb)),
    ARRAY['datetime:DESC','id:DESC','collection:DESC'],
    'keyset_sortkeys: default appends id+collection tiebreaks (datetime desc)'
);

-- A field already present in sortby is not duplicated by the tiebreak append.
SELECT is(
    (SELECT array_agg(field ORDER BY ord) FROM keyset_sortkeys('{"sortby":[{"field":"id","direction":"asc"}]}'::jsonb)),
    ARRAY['id','collection'],
    'keyset_sortkeys: dedups a field already present in sortby (id not duplicated)'
);

-- Mixed directions preserved; appended tiebreaks inherit the leading direction.
SELECT is(
    (SELECT array_agg(field||':'||dir ORDER BY ord)
     FROM keyset_sortkeys('{"sortby":[{"field":"datetime","direction":"desc"},{"field":"eo:cloud_cover","direction":"asc"}]}'::jsonb)),
    ARRAY['datetime:DESC','eo:cloud_cover:ASC','id:DESC','collection:DESC'],
    'keyset_sortkeys: preserves mixed directions, tiebreaks inherit leading dir'
);

-- Direction-aware seek: DESC forward uses <, ASC forward uses >.
SELECT ok(
    keyset_where('{}'::jsonb,
        ARRAY['2021-01-01 00:00:00+00','pgstac-test-item-0011','pgstac-test-collection'], false) LIKE '%<%',
    'keyset_where: default (datetime desc) forward seek uses <'
);
SELECT ok(
    keyset_where('{"sortby":[{"field":"datetime","direction":"asc"}]}'::jsonb,
        ARRAY['2021-01-01 00:00:00+00','pgstac-test-item-0011','pgstac-test-collection'], false) LIKE '%>%',
    'keyset_where: ascending forward seek uses >'
);

-- Full round-trip: page 1 -> follow next -> follow that page's prev -> original page.
SELECT is(
    (
        WITH p1 AS (
            SELECT search('{"limit":5,"sortby":[{"field":"datetime","direction":"desc"}]}'::jsonb) AS j
        ),
        nt AS (
            SELECT substring(
                (SELECT l->>'href' FROM p1, jsonb_array_elements((p1.j)->'links') l WHERE l->>'rel'='next')
                FROM 'token=(.*)$') AS t
        ),
        p2 AS (
            SELECT search(jsonb_build_object(
                'limit', 5,
                'sortby', jsonb_build_array(jsonb_build_object('field','datetime','direction','desc')),
                'token', (SELECT t FROM nt))) AS j
        ),
        pv AS (
            SELECT substring(
                (SELECT l->>'href' FROM p2, jsonb_array_elements((p2.j)->'links') l WHERE l->>'rel'='prev')
                FROM 'token=(.*)$') AS t
        ),
        p1b AS (
            SELECT search(jsonb_build_object(
                'limit', 5,
                'sortby', jsonb_build_array(jsonb_build_object('field','datetime','direction','desc')),
                'token', (SELECT t FROM pv))) AS j
        )
        SELECT (SELECT j->'features' FROM p1b)
    ),
    (SELECT search('{"limit":5,"sortby":[{"field":"datetime","direction":"desc"}]}'::jsonb)->'features'),
    'pagination: prev -> next -> prev round-trip returns the original page'
);

-- ============================================================
-- fields pruning: item-mode output + row-mode projection
--
-- item mode: hydration honors include/exclude (geometry conversion is guarded by
-- include_field inside content_hydrate, so excluding it skips ST_AsGeoJson and
-- omits the key). row mode: fields_to_columns projects only what's needed, so a
-- control-only field set fetches no fragment and a properties set pulls the
-- promoted columns.
-- ============================================================

SELECT ok(
    NOT ((search('{"limit":1,"fields":{"exclude":["geometry"]}}'::jsonb)->'features'->0) ? 'geometry'),
    'fields exclude geometry: hydrated feature omits geometry'
);

SELECT ok(
    NOT ((search('{"limit":1,"fields":{"include":["id","datetime"]}}'::jsonb)->'features'->0) ? 'assets'),
    'fields include [id,datetime]: hydrated feature omits assets'
);

SELECT ok(
    fields_to_columns('{"include":["id","datetime"]}') NOT LIKE '%frag_content%',
    'row mode include [id,datetime]: control-only projection fetches no fragment'
);

SELECT ok(
    fields_to_columns('{"include":["properties"]}') LIKE '%eo_cloud_cover%'
    AND fields_to_columns('{"include":["properties"]}') LIKE '%frag_content%',
    'row mode include [properties]: projects promoted property columns and the fragment'
);

-- ============================================================
-- search_page row mode: fast-start, low-memory paged primitive for the streaming
-- client (Rust iterator). Same chunker/keyset/early-exit engine as item mode, but
-- ships raw split-storage rows + inline _fragment for client-side hydration.
-- (Fast-start TTFB is a scale property, measured in benchmarks/results/chunker/10m;
-- here we lock the contract: shape, parity of count, and keyset iteration.)
-- ============================================================

-- row mode ships raw rows (geometry + inline _fragment), NOT hydrated Features.
SELECT ok(
    (search_page('{"collections":["pgstac-test-collection"],"sortby":[{"field":"datetime","direction":"desc"}]}'::jsonb, 5, NULL, false, 'row')
       -> 'features' -> 0) ? '_fragment',
    'search_page row mode: each row carries the inline _fragment'
);
SELECT ok(
    (search_page('{"collections":["pgstac-test-collection"]}'::jsonb, 5, NULL, false, 'row')
       -> 'features' -> 0 ->> 'type') IS DISTINCT FROM 'Feature',
    'search_page row mode: rows are raw split-storage, not hydrated Features'
);

-- item mode (default) is unchanged: hydrated Features.
SELECT is(
    (search_page('{"collections":["pgstac-test-collection"]}'::jsonb, 5, NULL, false, 'item')
       -> 'features' -> 0 ->> 'type'),
    'Feature',
    'search_page item mode (default): returns hydrated Features'
);

-- row and item modes page the SAME rows (same count, same paging envelope).
SELECT is(
    (search_page('{"collections":["pgstac-test-collection"]}'::jsonb, 5, NULL, false, 'row')->>'numberReturned'),
    (search_page('{"collections":["pgstac-test-collection"]}'::jsonb, 5, NULL, false, 'item')->>'numberReturned'),
    'search_page row/item modes return the same page size'
);

-- keyset iteration works in row mode: page 2 (via next token) is distinct from page 1.
SELECT isnt(
    (search_page(
        '{"collections":["pgstac-test-collection"],"sortby":[{"field":"datetime","direction":"desc"}]}'::jsonb,
        5,
        (search_page('{"collections":["pgstac-test-collection"],"sortby":[{"field":"datetime","direction":"desc"}]}'::jsonb, 5, NULL, false, 'row')->>'next'),
        false, 'row')
      -> 'features' -> 0 ->> 'id'),
    (search_page('{"collections":["pgstac-test-collection"],"sortby":[{"field":"datetime","direction":"desc"}]}'::jsonb, 5, NULL, false, 'row')
      -> 'features' -> 0 ->> 'id'),
    'search_page row mode: next-token iteration advances to a distinct page'
);

-- ============================================================
-- Field-aware row-mode thinning (fields_to_rowjsonb): don't ship heavy columns or
-- the fragment when the requested fields don't need them. Big wire/CPU win for
-- thin items; "overfetch a little" (whole fragment) only when assets/links/props
-- are actually requested.
-- ============================================================

-- full row (no fields): heavy columns + inline fragment present.
SELECT ok(
    (SELECT (search_page('{"collections":["pgstac-test-collection"]}'::jsonb, 1, NULL, false, 'row')->'features'->0) AS r)
        ? 'geometry'
    AND (search_page('{"collections":["pgstac-test-collection"]}'::jsonb, 1, NULL, false, 'row')->'features'->0) ? 'assets'
    AND (search_page('{"collections":["pgstac-test-collection"]}'::jsonb, 1, NULL, false, 'row')->'features'->0) ? '_fragment',
    'row mode (no fields): full row carries geometry, assets, and _fragment'
);

-- thin item include [id,datetime]: NO geometry, NO assets, NO fragment.
SELECT ok(
    NOT ((search_page('{"collections":["pgstac-test-collection"],"fields":{"include":["id","datetime"]}}'::jsonb, 1, NULL, false, 'row')->'features'->0) ? 'geometry')
    AND NOT ((search_page('{"collections":["pgstac-test-collection"],"fields":{"include":["id","datetime"]}}'::jsonb, 1, NULL, false, 'row')->'features'->0) ? 'assets')
    AND NOT ((search_page('{"collections":["pgstac-test-collection"],"fields":{"include":["id","datetime"]}}'::jsonb, 1, NULL, false, 'row')->'features'->0) ? '_fragment'),
    'row mode include [id,datetime]: thin item omits geometry, assets, and the fragment'
);

-- thin item still carries the control scalars the client needs (id, datetime).
SELECT ok(
    (search_page('{"collections":["pgstac-test-collection"],"fields":{"include":["id","datetime"]}}'::jsonb, 1, NULL, false, 'row')->'features'->0) ? 'id'
    AND (search_page('{"collections":["pgstac-test-collection"],"fields":{"include":["id","datetime"]}}'::jsonb, 1, NULL, false, 'row')->'features'->0) ? 'datetime',
    'row mode include [id,datetime]: thin item keeps control scalars (id, datetime)'
);

-- exclude geometry: geometry dropped, but assets (+fragment) still present.
SELECT ok(
    NOT ((search_page('{"collections":["pgstac-test-collection"],"fields":{"exclude":["geometry"]}}'::jsonb, 1, NULL, false, 'row')->'features'->0) ? 'geometry')
    AND (search_page('{"collections":["pgstac-test-collection"],"fields":{"exclude":["geometry"]}}'::jsonb, 1, NULL, false, 'row')->'features'->0) ? 'assets',
    'row mode exclude [geometry]: drops geometry, keeps assets'
);

-- a thin row is materially smaller than a full row.
SELECT cmp_ok(
    octet_length((search_page('{"collections":["pgstac-test-collection"],"fields":{"include":["id","datetime"]}}'::jsonb, 5, NULL, false, 'row'))::text),
    '<',
    octet_length((search_page('{"collections":["pgstac-test-collection"]}'::jsonb, 5, NULL, false, 'row'))::text),
    'row mode: a thin page is smaller on the wire than a full page'
);

-- ============================================================
-- Property precision (fields_to_rowjsonb). Asserted on the generated projection
-- string (deterministic). NB: the inline-fragment key is `_fragment`; check it
-- with strpos, NOT `LIKE '%_fragment%'` — `_` is a LIKE wildcard and would match
-- the `fragment_id` control column. The pgstac-test-collection fixture has a
-- default fragment_config (assets only — properties are NOT fragmented).
-- ============================================================

-- promoted property request -> ONLY that promoted column; no properties jsonb, no
-- inline fragment, and NOT every promoted column (e.g. platform absent).
SELECT ok(
    fields_to_rowjsonb('{"include":["id","properties.eo:cloud_cover"]}'::jsonb, ARRAY['pgstac-test-collection']) LIKE '%eo_cloud_cover%'
    AND fields_to_rowjsonb('{"include":["id","properties.eo:cloud_cover"]}'::jsonb, ARRAY['pgstac-test-collection']) NOT LIKE '%i.properties%'
    AND strpos(fields_to_rowjsonb('{"include":["id","properties.eo:cloud_cover"]}'::jsonb, ARRAY['pgstac-test-collection']), '_fragment') = 0
    AND fields_to_rowjsonb('{"include":["id","properties.eo:cloud_cover"]}'::jsonb, ARRAY['pgstac-test-collection']) NOT LIKE '%i.platform%',
    'precision: promoted property ships only its column (no properties jsonb, no fragment, not all promoted)'
);

-- non-promoted property request -> per-item properties jsonb, but NO inline fragment
-- because this collection''s fragment_config fragments assets, not properties.
SELECT ok(
    fields_to_rowjsonb('{"include":["id","properties.naip:state"]}'::jsonb, ARRAY['pgstac-test-collection']) LIKE '%i.properties%'
    AND strpos(fields_to_rowjsonb('{"include":["id","properties.naip:state"]}'::jsonb, ARRAY['pgstac-test-collection']), '_fragment') = 0,
    'precision: non-promoted property pulls properties jsonb but not the fragment (assets-only fragment_config)'
);

-- this collection does not fragment properties.
SELECT ok(
    NOT collection_fragments_properties('pgstac-test-collection'),
    'collection_fragments_properties: false for default (assets-only) fragment_config'
);

-- unknown / multi-collection -> conservative: non-promoted property pulls the fragment.
SELECT ok(
    collection_fragments_properties(NULL)
    AND strpos(fields_to_rowjsonb('{"include":["id","properties.naip:state"]}'::jsonb, NULL), '_fragment') > 0,
    'precision: unknown/multi-collection stays conservative (fetches fragment for non-promoted property)'
);

-- ============================================================
-- Exhaustive field registry: the staging-ingest trigger now walks every item's
-- raw paths (statement-level, append-only), so the registry actually reflects the
-- collection's fields. The pgstac-test-collection fixture is loaded via staging.
-- ============================================================

SELECT ok(
    EXISTS (SELECT 1 FROM item_field_registry
            WHERE collection = 'pgstac-test-collection' AND path LIKE 'properties.%'),
    'field registry is exhaustive after ingest: properties.* paths are present'
);
SELECT cmp_ok(
    (SELECT count(*)::int FROM item_field_registry
     WHERE collection = 'pgstac-test-collection' AND path LIKE 'properties.%'),
    '>=', 5,
    'field registry captured multiple property paths from real items'
);
SELECT cmp_ok(
    (SELECT count(*)::int FROM item_field_registry
     WHERE collection = 'pgstac-test-collection' AND path LIKE 'assets.%'),
    '>=', 1,
    'field registry captured asset paths too'
);

-- register_field_paths (the Rust seam): append a precomputed path, no walk.
SELECT is(
    register_field_paths('pgstac-test-collection',
        '[{"path":"properties.__pgtap_x","is_leaf":true,"value_kinds":["string"]}]'::jsonb),
    1,
    'register_field_paths: upserts one precomputed path'
);
SELECT ok(
    EXISTS (SELECT 1 FROM item_field_registry
            WHERE collection = 'pgstac-test-collection' AND path = 'properties.__pgtap_x'),
    'register_field_paths: the supplied path is now registered'
);

-- row-mode geometry is clean STAC GeoJSON (no PostGIS `crs` member) and matches
-- item mode — the Rust client gets a valid item without PostGIS-specific fixups.
SELECT ok(
    NOT ((search_page('{"collections":["pgstac-test-collection"],"fields":{"include":["id","geometry"]}}'::jsonb, 1, NULL, false, 'row')
         -> 'features' -> 0 -> 'geometry') ? 'crs'),
    'row mode geometry: clean STAC GeoJSON, no PostGIS crs member'
);
SELECT is(
    (search_page('{"collections":["pgstac-test-collection"],"sortby":[{"field":"id","direction":"asc"}],"fields":{"include":["id","geometry"]}}'::jsonb, 1, NULL, false, 'row')
       -> 'features' -> 0 -> 'geometry'),
    (search_page('{"collections":["pgstac-test-collection"],"sortby":[{"field":"id","direction":"asc"}],"fields":{"include":["id","geometry"]}}'::jsonb, 1, NULL, false, 'item')
       -> 'features' -> 0 -> 'geometry'),
    'row mode geometry equals item mode geometry'
);

-- partition_hashes: the per-partition content fingerprint for GeoParquet sync.
SELECT is(
    (SELECT sum(item_count)::bigint FROM partition_hashes('pgstac-test-collection')),
    (SELECT count(*)::bigint FROM items WHERE collection = 'pgstac-test-collection'),
    'partition_hashes: item counts sum to the collection total'
);
SELECT ok(
    (SELECT bool_and(content_hash ~ '^[0-9a-f]{64}$') FROM partition_hashes('pgstac-test-collection')),
    'partition_hashes: content_hash is a 64-char sha256 hex'
);
SELECT is(
    (SELECT content_hash FROM partition_hashes('pgstac-test-collection') ORDER BY partition LIMIT 1),
    (SELECT content_hash FROM partition_hashes('pgstac-test-collection') ORDER BY partition LIMIT 1),
    'partition_hashes: content_hash is deterministic'
);
