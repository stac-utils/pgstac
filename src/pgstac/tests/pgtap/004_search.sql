-- CREATE fixtures for testing search - as tests are run within a transaction, these will not persist

\copy items_staging (content) FROM 'tests/testdata/items.ndjson'

SET pgstac.context TO 'on';
SET pgstac."default_filter_lang" TO 'cql-json';

SELECT has_function('pgstac'::name, 'parse_dtrange', ARRAY['jsonb','timestamptz']);


SELECT results_eq($$ SELECT parse_dtrange('["2020-01-01","2021-01-01"]'::jsonb) $$, $$ SELECT '["2020-01-01 00:00:00+00","2021-01-02 00:00:00+00")'::tstzrange $$, 'daterange passed as array range runs to the end of its last day');


SELECT results_eq($$ SELECT parse_dtrange('"2020-01-01/2021-01-01"'::jsonb) $$, $$ SELECT '["2020-01-01 00:00:00+00","2021-01-02 00:00:00+00")'::tstzrange $$, 'date range passed as string range runs to the end of its last day');

SELECT results_eq($$ SELECT parse_dtrange('"2020-01-01T00:00:00Z/2021-01-01T00:00:00Z"'::jsonb) $$, $$ SELECT '["2020-01-01 00:00:00+00","2021-01-01 00:00:00+00"]'::tstzrange $$, 'a timestamp ending a range is included as that instant');

SELECT results_eq($$ SELECT parse_dtrange('"P1D/2021-01-01"'::jsonb) $$, $$ SELECT '["2021-01-01 00:00:00+00","2021-01-02 00:00:00+00")'::tstzrange $$, 'a duration runs back from the end of the day that ends it');


SELECT results_eq($$ SELECT parse_dtrange('"2020-01-01/.."'::jsonb) $$, $$ SELECT '["2020-01-01 00:00:00+00",infinity)'::tstzrange $$, 'date range passed as string range');


SELECT results_eq($$ SELECT parse_dtrange('"2020-01-01/"'::jsonb) $$, $$ SELECT '["2020-01-01 00:00:00+00",infinity)'::tstzrange $$, 'date range with an empty end passed as string range');


SELECT results_eq($$ SELECT parse_dtrange('"../2020-01-01"'::jsonb) $$, $$ SELECT '[-infinity,"2020-01-02 00:00:00+00")'::tstzrange $$, 'an open-start range ending on a date runs to the end of that day');


SELECT results_eq($$ SELECT parse_dtrange('"/2020-01-01"'::jsonb) $$, $$ SELECT '[-infinity,"2020-01-02 00:00:00+00")'::tstzrange $$, 'an empty-start range ending on a date runs to the end of that day');


SELECT has_function('pgstac'::name, 'bbox_geom', ARRAY['jsonb']);


SELECT results_eq($$ SELECT bbox_geom('[0,1,2,3]') $$, $$ SELECT 'SRID=4326;POLYGON((0 1,0 3,2 3,2 1,0 1))'::geometry $$, '2d bbox');


SELECT results_eq($$ SELECT bbox_geom('[0,1,2,3,4,5]'::jsonb) $$, $$ SELECT '010F0000A0E610000006000000010300008001000000050000000000000000000000000000000000F03F00000000000000400000000000000000000000000000104000000000000000400000000000000840000000000000104000000000000000400000000000000840000000000000F03F00000000000000400000000000000000000000000000F03F0000000000000040010300008001000000050000000000000000000000000000000000F03F00000000000014400000000000000840000000000000F03F00000000000014400000000000000840000000000000104000000000000014400000000000000000000000000000104000000000000014400000000000000000000000000000F03F0000000000001440010300008001000000050000000000000000000000000000000000F03F00000000000000400000000000000000000000000000F03F00000000000014400000000000000000000000000000104000000000000014400000000000000000000000000000104000000000000000400000000000000000000000000000F03F0000000000000040010300008001000000050000000000000000000840000000000000F03F00000000000000400000000000000840000000000000104000000000000000400000000000000840000000000000104000000000000014400000000000000840000000000000F03F00000000000014400000000000000840000000000000F03F0000000000000040010300008001000000050000000000000000000000000000000000F03F00000000000000400000000000000840000000000000F03F00000000000000400000000000000840000000000000F03F00000000000014400000000000000000000000000000F03F00000000000014400000000000000000000000000000F03F000000000000004001030000800100000005000000000000000000000000000000000010400000000000000040000000000000000000000000000010400000000000001440000000000000084000000000000010400000000000001440000000000000084000000000000010400000000000000040000000000000000000000000000010400000000000000040'::geometry $$, '3d bbox');



SELECT has_function('pgstac'::name, 'sort_sqlorderby', ARRAY['jsonb','boolean','text[]','text[]']);

SELECT results_eq($$
    SELECT sort_sqlorderby('{"sortby":[{"field":"datetime","direction":"desc"},{"field":"eo:cloud_cover","direction":"asc"}]}'::jsonb);
    $$,$$
    SELECT 'datetime DESC, to_int(content->''properties''->''eo:cloud_cover'') ASC, collection DESC, id DESC';
    $$,
    'Test creation of sort sql'
);


SELECT results_eq($$
    SELECT sort_sqlorderby('{"sortby":[{"field":"datetime","direction":"desc"},{"field":"eo:cloud_cover","direction":"asc"}]}'::jsonb, true);
    $$,$$
    SELECT 'datetime ASC, to_int(content->''properties''->''eo:cloud_cover'') DESC, collection ASC, id ASC';
    $$,
    'Test creation of reverse sort sql'
);

-- A direction that is neither asc nor desc is refused outright rather than read as ASC, so a
-- crafted one cannot reach the ORDER BY at all, let alone be spliced into it.
SELECT throws_ok(
    $$ SELECT sort_sqlorderby('{"sortby":[{"field":"datetime","direction":"desc; select pg_sleep(1) --"}]}'::jsonb) $$,
    NULL,
    'Invalid sortby direction desc; select pg_sleep(1) --: must be asc or desc',
    'a sortby direction that is not asc or desc is refused'
);

SELECT matches(
    sort_sqlorderby('{"sortby":[{"field":"datetime","direction":"descending"}]}'::jsonb),
    '^(\S+ (ASC|DESC)(, |$))+$',
    'a spelled out direction still renders as a bare ASC or DESC token'
);

SELECT is(
    sortby_with_tiebreakers('[{"field":"eo:cloud_cover","direction":"desc"}]'::jsonb),
    '[{"field":"eo:cloud_cover","direction":"desc"},{"field":"collection","direction":"desc"},{"field":"id","direction":"desc"}]'::jsonb,
    'collection then id are appended as tie-breakers in the first sort direction'
);

SELECT is(
    sortby_with_tiebreakers('[{"field":"id","direction":"asc"},{"field":"eo:cloud_cover","direction":"desc"}]'::jsonb),
    '[{"field":"id","direction":"asc"},{"field":"eo:cloud_cover","direction":"desc"},{"field":"collection","direction":"asc"}]'::jsonb,
    'a key already in the sortby is not appended again'
);

SELECT is(
    sort_sqlorderby('{}'::jsonb),
    'datetime DESC, collection DESC, id DESC',
    'a missing sortby is datetime DESC with the item key appended in the same direction'
);

SELECT is(
    sort_sqlorderby('{"sortby":[]}'::jsonb),
    sort_sqlorderby('{}'::jsonb),
    'an empty sortby is the default sort'
);

SELECT is(
    sort_sqlorderby('{"sortby":null}'::jsonb),
    sort_sqlorderby('{}'::jsonb),
    'a JSON null sortby is the default sort'
);

SELECT is(
    sort_sqlorderby('{}'::jsonb, FALSE, '{id}'),
    'datetime DESC, id DESC',
    'the tie-break keys are those of the searched relation'
);

SELECT is(
    sort_sqlorderby('{"sortby":[{"field":"datetime","direction":" desc"}]}'::jsonb),
    sort_sqlorderby('{"sortby":[{"field":"datetime","direction":"desc"}]}'::jsonb),
    'whitespace around a direction is ignored'
);

-- b is the newer collection, so datetime DESC puts it first where an id sort would not
SELECT create_collection('{"id":"pgstac-test-sort-a","type":"Collection","stac_version":"1.0.0","description":"a","license":"proprietary","links":[],"extent":{"spatial":{"bbox":[[-180,-90,180,90]]},"temporal":{"interval":[["2020-01-01T00:00:00Z","2020-12-31T00:00:00Z"]]}}}');
SELECT create_collection('{"id":"pgstac-test-sort-b","type":"Collection","stac_version":"1.0.0","description":"b","license":"proprietary","links":[],"extent":{"spatial":{"bbox":[[-180,-90,180,90]]},"temporal":{"interval":[["2021-01-01T00:00:00Z","2021-12-31T00:00:00Z"]]}}}');

SELECT is(
    (SELECT jsonb_agg(c->>'id') FROM jsonb_array_elements(collection_search('{"ids":["pgstac-test-sort-a","pgstac-test-sort-b"]}')->'collections') c),
    '["pgstac-test-sort-b","pgstac-test-sort-a"]'::jsonb,
    'collection_search without a sortby orders by datetime DESC'
);

SELECT is(
    collection_search('{"ids":["pgstac-test-sort-a","pgstac-test-sort-b"],"sortby":[]}'),
    collection_search('{"ids":["pgstac-test-sort-a","pgstac-test-sort-b"]}'),
    'an empty collection_search sortby is the default sort'
);

SELECT delete_collection('pgstac-test-sort-a');
SELECT delete_collection('pgstac-test-sort-b');


SELECT has_function('pgstac'::name, 'search', ARRAY['jsonb']);


SELECT results_eq($$
    SELECT search('{"collections": ["pgstac-test-collection"], "limit": 10, "sortby":[{"field":"id","direction":"asc"}]}'::jsonb
        || jsonb_build_object('token', 'prev:' || page_token('pgstac-test-collection', 'pgstac-test-item-0011')))
    $$,$$
    SELECT search('{"collections": ["pgstac-test-collection"], "limit": 10, "sortby":[{"field":"id","direction":"asc"}]}')
    $$,
    'Test prev token when reading first token_type=prev (https://github.com/stac-utils/pgstac/issues/140)'
);

SELECT is(
    (SELECT search('{"limit":1}'::jsonb || jsonb_build_object('token',
            'next:' || page_token('pgstac-test-collection', 'pgstac-test-item-0003')))
            ->'features'->0->>'id'),
    'pgstac-test-item-0002',
    'a next token returns the row that follows its anchor under the default sort'
);

SELECT is(
    get_token_filter(token_item => (SELECT i FROM items i WHERE collection = 'pgstac-test-collection' AND id = 'pgstac-test-item-0003')),
    get_token_filter('[{"field":"datetime","direction":"desc"}]', (SELECT i FROM items i WHERE collection = 'pgstac-test-collection' AND id = 'pgstac-test-item-0003')),
    'a missing sortby gives the token filter for the default sort'
);

SELECT lives_ok($$
    SELECT search('{"sortby":[{"field":"geometry","direction":"asc"}],"limit":10}'::jsonb
        || jsonb_build_object('token',
            'next:' || page_token('pgstac-test-collection', 'pgstac-test-item-0010')));
$$, 'a token can be built from a geometry sort value');

SET pgstac.context TO 'off';
SELECT ok(
    search('{"limit":1,"conf":{"context":"on"}}') ? 'numberMatched',
    'a per-request conf.context on counts matches even when the global context is off'
);
SET pgstac.context TO 'on';


SELECT has_function('pgstac'::name, 'search_query', ARRAY['jsonb','boolean','jsonb']);


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
    (collection = 'landsat8_l1tp' AND to_int(content->'properties'->'eo:cloud_cover') <= to_int('"10"') AND datetime >= '2021-04-08 04:39:23+00'::timestamptz AND st_intersects(geometry, '0103000020E6100000010000000B000000894160E5D0CA4540ED9E3C2CD4E253C0849ECDAACFCD4540B37BF2B050DF53C038F8C264AAC8454076E09C11A5DD53C0F5DBD78173CE454085EB51B81ED953C08126C286A7CF4540789CA223B9D453C0C0EC9E3C2CD4454063EE5A423ED453C004560E2DB2E5454001DE02098AC753C063EE5A423EE84540C442AD69DEC953C02FDD240681ED454034A2B437F8CA53C08048BF7D1DE0454037894160E5E853C0894160E5D0CA4540ED9E3C2CD4E253C0'::geometry))
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
    (to_text(content->'properties'->'sentinel:data_coverage') > to_text('"50"') AND to_int(content->'properties'->'eo:cloud_cover') < to_int('10'))
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
    (to_float(content->'properties'->'sentinel:data_coverage') > to_float('50') OR to_int(content->'properties'->'eo:cloud_cover') < to_int('10'))
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
    to_text(content->'properties'->'prop1') = to_text(content->'properties'->'prop2')
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
    (datetime <= '2020-11-12 00:00:00+00'::timestamptz AND datetime >= '2020-11-11 00:00:00+00'::timestamptz)
    $r$,E' \n');
    $$, 'Test Example 6'
);

SELECT results_eq($$
    SELECT BTRIM(stac_search_to_where($q$
       {
            "filter-lang": "cql2-json",
            "filter": {
                "op": "t_overlaps",
                "args": [
                { "property": "datetime" },
                { "interval": [ "2020-11-11T00:00:00Z", "2020-11-12T00:00:00Z"] }
                ]
            }
        }
    $q$),E' \n');
    $$, $$
    SELECT BTRIM($r$
    (datetime < '2020-11-11 00:00:00+00'::timestamptz AND datetime > '2020-11-11 00:00:00+00'::timestamptz AND datetime < '2020-11-12 00:00:00+00'::timestamptz)
    $r$,E' \n');
    $$, 't_overlaps expands to three explicit conjuncts'
);

SELECT results_eq($$
    SELECT BTRIM(stac_search_to_where($q$
       {
            "filter-lang": "cql2-json",
            "filter": {
                "op": "t_overlappedby",
                "args": [
                { "property": "datetime" },
                { "interval": [ "2020-11-11T00:00:00Z", "2020-11-12T00:00:00Z"] }
                ]
            }
        }
    $q$),E' \n');
    $$, $$
    SELECT BTRIM($r$
    (datetime > '2020-11-11 00:00:00+00'::timestamptz AND datetime < '2020-11-12 00:00:00+00'::timestamptz AND datetime > '2020-11-12 00:00:00+00'::timestamptz)
    $r$,E' \n');
    $$, 't_overlappedby expands to three explicit conjuncts'
);

SELECT lives_ok($$
    SELECT search('{"filter":{"op":"t_overlaps","args":[{"property":"datetime"},"2011-08-16T00:00:00Z/2011-08-18T00:00:00Z"]},"fields":{"include":["id"]}}');
$$, 't_overlaps search executes without a syntax error');

SELECT throws_ok($$
    SELECT temporal_op_query('t_bogus', '[{"property":"datetime"},"2020-11-11T00:00:00Z"]'::jsonb);
$$, 'Temporal operator t_bogus is not supported.',
    'unsupported temporal operator is rejected'
);

SELECT results_eq($$
    SELECT BTRIM(stac_search_to_where($q$
       {
            "filter-lang": "cql2-json",
            "filter": {
                "op": "t_intersects",
                "args": [
                { "property": "test:created" },
                { "interval": [ "2020-11-11T00:00:00Z", "2020-11-12T00:00:00Z"] }
                ]
            }
        }
    $q$),E' \n');
    $$, $$
    SELECT BTRIM($r$
    (to_tstz(content->'properties'->'test:created') <= '2020-11-12 00:00:00+00'::timestamptz AND to_tstz(content->'properties'->'test:created') >= '2020-11-11 00:00:00+00'::timestamptz)
    $r$,E' \n');
    $$, 'a temporal operator targets the property the filter names'
);

SELECT is(
    temporal_op_query('t_intersects', '[{"property":"test:dt"},{"interval":["2020-11-11T00:00:00Z","2020-11-12T00:00:00Z"]}]'::jsonb),
    $$(to_tstz(content->'properties'->'test:dt') <= '2020-11-12 00:00:00+00'::timestamptz AND to_tstz(content->'properties'->'test:dt') >= '2020-11-11 00:00:00+00'::timestamptz)$$,
    'a date-time queryable is wrapped the way its index is'
);

SELECT is(
    temporal_op_query('t_during', '[{"interval":["2020-11-11T00:00:00Z","2020-11-12T00:00:00Z"]},{"interval":["2020-11-01T00:00:00Z","2020-12-01T00:00:00Z"]}]'::jsonb),
    $$('2020-11-11 00:00:00+00'::timestamptz > '2020-11-01 00:00:00+00'::timestamptz AND '2020-11-12 00:00:00+00'::timestamptz < '2020-12-01 00:00:00+00'::timestamptz)$$,
    'an interval literal is usable as the first operand'
);

SELECT is(
    temporal_op_query('t_intersects', '[{"property":"test:created"},"2020-11-11T00:00:00Z"]'::jsonb),
    $$(to_tstz(content->'properties'->'test:created') <= '2020-11-11 00:00:00+00'::timestamptz AND to_tstz(content->'properties'->'test:created') >= '2020-11-11 00:00:00+00'::timestamptz)$$,
    'an instant literal as a direct operand is that instant'
);

SELECT is(
    temporal_op_query('t_intersects', '[{"property":"datetime"},"2020-11-11T00:00:00Z"]'::jsonb),
    $$(datetime <= '2020-11-11 00:00:00+00'::timestamptz AND datetime >= '2020-11-11 00:00:00+00'::timestamptz)$$,
    'a column intersects an instant only at that instant'
);

SELECT is(
    temporal_op_query('t_before', '[{"property":"datetime"},{"interval":["2020-11-11T00:00:00Z","2020-11-12T00:00:00Z"]}]'::jsonb),
    $$(datetime < '2020-11-11 00:00:00+00'::timestamptz)$$,
    't_before compares the bare column against the interval start'
);

SELECT is(
    temporal_op_query('t_intersects', '[{"property":"datetime"},{"property":"end_datetime"}]'::jsonb),
    $$(datetime <= end_datetime AND datetime >= end_datetime)$$,
    'two column instants intersect only when equal'
);

SELECT is(
    temporal_op_query('t_intersects', '[{"property":"test:created"},{"interval":["..","2020-11-12T00:00:00Z"]}]'::jsonb),
    $$(to_tstz(content->'properties'->'test:created') <= '2020-11-12 00:00:00+00'::timestamptz AND to_tstz(content->'properties'->'test:created') >= '-infinity'::timestamptz)$$,
    'an open interval start becomes -infinity'
);

SELECT is(
    temporal_op_query('t_before', '[{"property":"test:created"},{"property":"test:updated"}]'::jsonb),
    $$(to_tstz(content->'properties'->'test:created') < to_tstz(content->'properties'->'test:updated'))$$,
    'both operands may be properties'
);

SELECT ok(
    NOT EXISTS (
        SELECT FROM
            unnest('{t_before,t_after,t_meets,t_metby,t_overlaps,t_overlappedby,t_starts,t_startedby,t_during,t_contains,t_finishes,t_finishedby,t_equals,t_disjoint,t_intersects,anyinteracts}'::text[]) op,
            unnest('{datetime,end_datetime}'::text[]) col,
            unnest(ARRAY['"2020-11-11T00:00:00Z"', '"2020-11-11"', '{"interval":["2020-11-11T00:00:00Z","2020-11-12T00:00:00Z"]}', '{"property":"end_datetime"}']) other
        WHERE temporal_op_query(op, jsonb_build_array(jsonb_build_object('property', col), other::jsonb)) ~ '(datetime|end_datetime) [+-] interval'
    ),
    'no temporal predicate adds or subtracts an interval on the datetime or end_datetime column'
);

SELECT throws_ok($$
    SELECT temporal_op_query('t_before', '[{"property":"id"},"2020-11-11T00:00:00Z"]'::jsonb);
$$, 'Property id is not temporal.',
    'a non-temporal property is rejected'
);

SELECT throws_ok($$
    SELECT temporal_op_query('t_intersects', '["2011-08-16T00:00:00Z/2011-08-17T00:00:00Z"]'::jsonb);
$$, 'Temporal operator t_intersects requires two operands.',
    'a temporal predicate needs two operands'
);

SELECT results_eq($$
    SELECT (search($s${"filter":{"op":"t_intersects","args":[{"property":"test:created"},{"interval":["2011-08-16T00:00:00Z","2011-08-18T00:00:00Z"]}]},"fields":{"include":["id"]}}$s$)->>'numberReturned')::int;
    $$, $$ SELECT 0; $$,
    'a temporal filter on a property no item carries matches nothing'
);

SELECT is(
    temporal_op_query('t_intersects', '[{"property":"test:created"},"2020-11-11"]'::jsonb),
    $$(to_tstz(content->'properties'->'test:created') <= '2020-11-11 23:59:59.999999+00'::timestamptz AND to_tstz(content->'properties'->'test:created') >= '2020-11-11 00:00:00+00'::timestamptz)$$,
    'a date-only literal is the whole day it names, to its last microsecond'
);

SELECT is(
    temporal_op_query('t_during', '[{"interval":[{"property":"test:created"},{"property":"test:updated"}]},{"interval":["2020-11-01T00:00:00Z","2020-12-01T00:00:00Z"]}]'::jsonb),
    $$(to_tstz(content->'properties'->'test:created') > '2020-11-01 00:00:00+00'::timestamptz AND to_tstz(content->'properties'->'test:updated') < '2020-12-01 00:00:00+00'::timestamptz)$$,
    'an interval built from two properties resolves each end independently'
);

SELECT is(
    temporal_op_query('t_intersects', '[{"property":"end_datetime"},{"interval":["2020-11-11T00:00:00Z","2020-11-12T00:00:00Z"]}]'::jsonb),
    $$(end_datetime <= '2020-11-12 00:00:00+00'::timestamptz AND end_datetime >= '2020-11-11 00:00:00+00'::timestamptz)$$,
    'a direct end_datetime operand is the instant at the item end, not the item span'
);

SELECT is(
    temporal_op_query('t_during', '[{"interval":[{"property":"end_datetime"},"2020-11-12T00:00:00Z"]},{"interval":["2020-11-01T00:00:00Z","2020-12-01T00:00:00Z"]}]'::jsonb),
    $$(end_datetime > '2020-11-01 00:00:00+00'::timestamptz AND '2020-11-12 00:00:00+00'::timestamptz < '2020-12-01 00:00:00+00'::timestamptz)$$,
    'end_datetime starting an interval block is just that instant'
);

SELECT is(
    temporal_op_query('t_during', '[{"interval":[{"property":"test:created"},"2020-11-12T00:00:00Z"]},{"interval":["2020-11-01T00:00:00Z","2020-12-01T00:00:00Z"]}]'::jsonb),
    $$(to_tstz(content->'properties'->'test:created') > '2020-11-01 00:00:00+00'::timestamptz AND '2020-11-12 00:00:00+00'::timestamptz < '2020-12-01 00:00:00+00'::timestamptz)$$,
    'an unpromoted date-time queryable inside an interval block is just that instant'
);

SELECT is(
    temporal_op_query('t_overlaps', '["2020-11-11T00:00:00Z",{"property":"test:created"}]'::jsonb),
    $$('2020-11-11 00:00:00+00'::timestamptz < to_tstz(content->'properties'->'test:created') AND '2020-11-11 00:00:00+00'::timestamptz > to_tstz(content->'properties'->'test:created') AND '2020-11-11 00:00:00+00'::timestamptz < to_tstz(content->'properties'->'test:created'))$$,
    'a literal instant given to t_overlaps is compared as an instant, not an error'
);

SELECT throws_ok($$
    SELECT temporal_op_query('t_intersects', '[{"property":"datetime"},{"interval":[{"interval":["2020-11-11T00:00:00Z","2020-11-12T00:00:00Z"]},"2020-11-13T00:00:00Z"]}]'::jsonb);
$$, 'An interval end must be a timestamp, a date or a property.',
    'a nested interval is rejected'
);

SELECT throws_ok($$
    SELECT temporal_op_query('t_intersects', '[{"property":"datetime"},{"interval":["2020-11-11T00:00:00Z",{"op":"now","args":[]}]}]'::jsonb);
$$, 'Temporal operand {"op": "now", "args": []} is not a timestamp, a date, an interval or a property.',
    'an unknown object as a temporal operand is rejected'
);

SELECT is(
    temporal_op_query('t_intersects', '[{"property":"start_datetime"},{"interval":["2020-11-11T00:00:00Z","2020-11-12T00:00:00Z"]}]'::jsonb),
    temporal_op_query('t_intersects', '[{"property":"datetime"},{"interval":["2020-11-11T00:00:00Z","2020-11-12T00:00:00Z"]}]'::jsonb),
    'start_datetime resolves the same as datetime as a temporal operand'
);

SELECT results_eq($$
    SELECT BTRIM(stac_search_to_where('{"datetime":"2020-11-11T00:00:00Z/2020-11-12T00:00:00Z"}'::jsonb),E' \n');
    $$, $$
    SELECT BTRIM($r$
    datetime <= '2020-11-12 00:00:00+00'::timestamptz AND end_datetime >= '2020-11-11 00:00:00+00'::timestamptz
    $r$,E' \n');
    $$, 'the search datetime argument still means the item span, not an instant'
);

SELECT lives_ok(
    $$ SELECT upsert_queryable('test:datedprop', definition => '{"type":"string","format":"date"}'::jsonb, property_index_type => 'BTREE'); $$,
    'Can register an indexed queryable whose format is date.'
);

SELECT results_eq($$
    SELECT BTRIM(stac_search_to_where($q$
        {
            "filter-lang": "cql2-json",
            "filter": {
                "op": "t_intersects",
                "args": [
                { "property": "test:datedprop" },
                { "interval": [ "2020-11-11T00:00:00Z", "2020-11-12T00:00:00Z"] }
                ]
            }
        }
    $q$),E' \n');
    $$, $$
    SELECT BTRIM($r$
    (to_tstz(content->'properties'->'test:datedprop') <= '2020-11-12 00:00:00+00'::timestamptz AND ((to_tstz(content->'properties'->'test:datedprop') AT TIME ZONE 'UTC' + interval '1 day' - interval '1 microsecond') AT TIME ZONE 'UTC') >= '2020-11-11 00:00:00+00'::timestamptz)
    $r$,E' \n');
    $$, 'a format:date property as a direct operand is the whole day it names'
);

SELECT results_eq($$
    SELECT temporal_op_query('t_during', '[{"interval":[{"property":"test:datedprop"},"2020-11-12T00:00:00Z"]},{"interval":["2020-11-01T00:00:00Z","2020-12-01T00:00:00Z"]}]'::jsonb);
    $$, $$
    SELECT $r$(to_tstz(content->'properties'->'test:datedprop') > '2020-11-01 00:00:00+00'::timestamptz AND '2020-11-12 00:00:00+00'::timestamptz < '2020-12-01 00:00:00+00'::timestamptz)$r$::text;
    $$, 'a format:date property starting an interval block is the start of its day'
);

-- an interval ending on a date includes that whole day, so the high end is its last microsecond.
SELECT results_eq($$
    SELECT temporal_op_query('t_during', '[{"interval":["2020-11-01T00:00:00Z",{"property":"test:datedprop"}]},{"interval":["2020-10-01T00:00:00Z","2020-12-01T00:00:00Z"]}]'::jsonb);
    $$, $$
    SELECT $r$('2020-11-01 00:00:00+00'::timestamptz > '2020-10-01 00:00:00+00'::timestamptz AND ((to_tstz(content->'properties'->'test:datedprop') AT TIME ZONE 'UTC' + interval '1 day' - interval '1 microsecond') AT TIME ZONE 'UTC') < '2020-12-01 00:00:00+00'::timestamptz)$r$::text;
    $$, 'a format:date property ending an interval block runs to the end of its day'
);

SELECT results_eq($$
    SELECT temporal_op_query('t_during', '[{"interval":["2020-11-01T00:00:00Z","2020-11-12"]},{"interval":["2020-10-01T00:00:00Z","2020-12-01T00:00:00Z"]}]'::jsonb);
    $$, $$
    SELECT $r$('2020-11-01 00:00:00+00'::timestamptz > '2020-10-01 00:00:00+00'::timestamptz AND '2020-11-12 23:59:59.999999+00'::timestamptz < '2020-12-01 00:00:00+00'::timestamptz)$r$::text;
    $$, 'a date-only literal ending an interval block runs to the end of its day'
);

CREATE OR REPLACE FUNCTION pg_temp.explain_where(_where text) RETURNS text AS $$
DECLARE
    plan jsonb;
BEGIN
    EXECUTE format('EXPLAIN (FORMAT JSON) SELECT 1 FROM items WHERE %s', _where) INTO plan;
    RETURN plan::text;
END;
$$ LANGUAGE plpgsql SET enable_seqscan TO off;

SELECT matches(
    pg_temp.explain_where(temporal_op_query('t_intersects', '[{"property":"test:datedprop"},"2020-11-11"]'::jsonb)),
    '"Index Name": "_items_[0-9]+_to_tstz_idx"',
    'a temporal predicate on a format:date property uses its to_tstz index'
);

-- The index name alone would match a full index scan with the predicate applied as a filter;
-- enable_seqscan is only a cost penalty, so it does not rule that out. An Index Cond does.
SELECT matches(
    pg_temp.explain_where(temporal_op_query('t_intersects', '[{"property":"test:datedprop"},"2020-11-11"]'::jsonb)),
    '"Index Cond"',
    'the predicate is an index condition rather than a filter over a full index scan'
);

SELECT lives_ok($$
    SELECT search($s${"filter":{"op":"t_intersects","args":[{"property":"test:datedprop"},{"interval":["2011-08-16T00:00:00Z","2011-08-18T00:00:00Z"]}]},"fields":{"include":["id"]}}$s$);
$$, 'a search filtered on a format:date property runs end to end');

SELECT lives_ok($$
    SELECT create_item('{"id":"pgstac-test-dated","type":"Feature","stac_version":"1.0.0","collection":"pgstac-test-collection","bbox":[0,0,1,1],"links":[],"assets":{},"geometry":{"type":"Polygon","coordinates":[[[0,0],[0,1],[1,1],[1,0],[0,0]]]},"properties":{"datetime":"2011-08-16T12:00:00Z","test:datedprop":"2011-08-16"}}');
$$, 'Can create an item carrying a format:date property');

SELECT results_eq($$
    SELECT search($s${"filter":{"op":"t_equals","args":[{"property":"test:datedprop"},"2011-08-16"]},"fields":{"include":["id"]}}$s$)->'features'->0->>'id';
    $$, $$ SELECT 'pgstac-test-dated'; $$,
    't_equals on a format:date property matches the item whose date it names'
);

SET LOCAL TIME ZONE 'America/Chicago';
SELECT results_eq($$
    SELECT search($s${"filter":{"op":"t_equals","args":[{"property":"test:datedprop"},"2011-08-16"]},"fields":{"include":["id"]}}$s$)->'features'->0->>'id';
    $$, $$ SELECT 'pgstac-test-dated'; $$,
    'a format:date property matches the same item whatever the session time zone'
);
SET LOCAL TIME ZONE DEFAULT;

SELECT lives_ok(
    $$ SELECT delete_item('pgstac-test-dated', 'pgstac-test-collection'); $$,
    'Can remove the dated item again.'
);

SELECT lives_ok(
    $$ SELECT delete_queryable('test:datedprop'); $$,
    'Can remove the format:date queryable again.'
);

SELECT throws_ok($$
    SELECT temporal_op_query('t_intersects', '[{"property":"datetime"},{"timestamp":null}]'::jsonb);
$$, 'Temporal operand {"timestamp": null} is not a timestamp, a date, an interval or a property.',
    'a null timestamp block is rejected'
);

SELECT throws_ok($$
    SELECT temporal_op_query('t_intersects', '[{"property":"datetime"},null]'::jsonb);
$$, 'Temporal operand null is not a timestamp, a date, an interval or a property.',
    'a null operand is rejected'
);

SELECT throws_ok($$
    SELECT temporal_op_query('t_intersects', '[null,{"property":"datetime"}]'::jsonb);
$$, 'Temporal operand null is not a timestamp, a date, an interval or a property.',
    'a null first operand is rejected'
);

SELECT throws_ok($$
    SELECT temporal_op_query('t_intersects', '[{"property":"datetime"},{"property":null}]'::jsonb);
$$, 'Temporal operand {"property": null} is not a timestamp, a date, an interval or a property.',
    'a null property is rejected'
);

SELECT throws_ok($$
    SELECT temporal_op_query('t_intersects', '[{"property":"datetime"},{"interval":null}]'::jsonb);
$$, 'Temporal interval {"interval": null} must have exactly two ends.',
    'a null interval block is rejected'
);

SELECT throws_ok($$
    SELECT temporal_op_query('t_intersects', '[{"property":"datetime"},{"interval":["2020-11-11T00:00:00Z"]}]'::jsonb);
$$, 'Temporal interval {"interval": ["2020-11-11T00:00:00Z"]} must have exactly two ends.',
    'a one-ended interval block is rejected'
);

SELECT throws_ok($$
    SELECT temporal_op_query('t_intersects', '[{"property":"datetime"},{"interval":["2020-11-11T00:00:00Z","P1D"]}]'::jsonb);
$$, 'An interval end must be a timestamp, a date or a property.',
    'a duration is not an interval block end'
);

SELECT is(
    temporal_op_query('t_intersects', '[{"property":"datetime"},"2020-11-11T00:00:00Z/P1D"]'::jsonb),
    temporal_op_query('t_intersects', '[{"property":"datetime"},"2020-11-11T00:00:00Z/2020-11-12T00:00:00Z"]'::jsonb),
    'a duration ending a slash interval runs from its start'
);

SELECT is(
    temporal_op_query('t_intersects', '[{"property":"datetime"},"P1D/2020-11-12T00:00:00Z"]'::jsonb),
    temporal_op_query('t_intersects', '[{"property":"datetime"},"2020-11-11T00:00:00Z/2020-11-12T00:00:00Z"]'::jsonb),
    'a duration starting a slash interval runs back from its end'
);

SELECT throws_ok($$
    SELECT temporal_op_query('t_intersects', '[{"property":"datetime"},"P1D/.."]'::jsonb);
$$, 'A duration must be paired with a timestamp or a date.',
    'a duration against an open end is rejected'
);

SELECT is(
    temporal_op_query('t_intersects', '[{"property":"datetime"},{"interval":["2020-11-11","2020-11-12"]}]'::jsonb),
    $$(datetime <= '2020-11-12 23:59:59.999999+00'::timestamptz AND datetime >= '2020-11-11 00:00:00+00'::timestamptz)$$,
    'a date ending an interval block runs to the end of its day'
);

-- End to end on midnight-stamped daily items: a day is closed at both ends.
SELECT create_collection('{"id":"pgstac-test-daily","type":"Collection","stac_version":"1.0.0","description":"daily","license":"proprietary","links":[],"extent":{"spatial":{"bbox":[[-180,-90,180,90]]},"temporal":{"interval":[["2020-11-10T00:00:00Z","2020-11-13T00:00:00Z"]]}}}');
SELECT create_items(jsonb_agg(jsonb_build_object(
    'id', concat('day-', d::date), 'type', 'Feature', 'stac_version', '1.0.0', 'collection', 'pgstac-test-daily',
    'bbox', '[0,0,1,1]'::jsonb, 'links', '[]'::jsonb, 'assets', '{}'::jsonb,
    'geometry', '{"type":"Polygon","coordinates":[[[0,0],[0,1],[1,1],[1,0],[0,0]]]}'::jsonb,
    'properties', jsonb_build_object('datetime', to_char(d, 'YYYY-MM-DD"T00:00:00Z"'))
))) FROM generate_series('2020-11-10'::date, '2020-11-13'::date, '1 day') d;
SELECT create_item('{"id":"day-2020-11-12-noon","type":"Feature","stac_version":"1.0.0","collection":"pgstac-test-daily","bbox":[0,0,1,1],"links":[],"assets":{},"geometry":{"type":"Polygon","coordinates":[[[0,0],[0,1],[1,1],[1,0],[0,0]]]},"properties":{"datetime":"2020-11-12T12:00:00Z"}}');

SELECT is(
    jsonb_path_query_array(search('{"collections":["pgstac-test-daily"],"filter":{"op":"t_intersects","args":[{"property":"datetime"},"2020-11-11"]}}'), '$.features[*].id'),
    '["day-2020-11-11"]'::jsonb,
    'a date literal matches only the items within that day'
);

SELECT is(
    jsonb_path_query_array(search('{"collections":["pgstac-test-daily"],"filter":{"op":"t_intersects","args":[{"property":"datetime"},"2020-11-11/2020-11-12"]}}'), '$.features[*].id'),
    '["day-2020-11-12-noon", "day-2020-11-12", "day-2020-11-11"]'::jsonb,
    'a date interval matches the items within those days'
);

SELECT is(
    jsonb_path_query_array(search('{"collections":["pgstac-test-daily"],"filter":{"op":"t_disjoint","args":[{"property":"datetime"},"2020-11-11"]}}'), '$.features[*].id'),
    '["day-2020-11-13", "day-2020-11-12-noon", "day-2020-11-12", "day-2020-11-10"]'::jsonb,
    't_disjoint on a date literal matches every item outside that day'
);

SELECT is(
    jsonb_path_query_array(search('{"collections":["pgstac-test-daily"],"datetime":"2020-11-11/2020-11-12"}'), '$.features[*].id'),
    '["day-2020-11-12-noon", "day-2020-11-12", "day-2020-11-11"]'::jsonb,
    'the search datetime argument ending on a date includes that whole day and nothing after it'
);

SELECT is(
    jsonb_path_query_array(search('{"collections":["pgstac-test-daily"],"datetime":"2020-11-11/2020-11-12T00:00:00Z"}'), '$.features[*].id'),
    '["day-2020-11-12", "day-2020-11-11"]'::jsonb,
    'the search datetime argument ending on a timestamp is closed at that instant'
);

SELECT results_eq($$
    SELECT BTRIM(stac_search_to_where('{"datetime":"2020-11-11/2020-11-12"}'::jsonb),E' \n');
    $$, $$
    SELECT BTRIM($r$
    datetime < '2020-11-13 00:00:00+00'::timestamptz AND end_datetime >= '2020-11-11 00:00:00+00'::timestamptz
    $r$,E' \n');
    $$, 'the search datetime argument reads a date-only end as open at the next midnight'
);

SELECT delete_collection('pgstac-test-daily');

SELECT is(
    temporal_op_query('t_intersects', '[{"property":"datetime"},"2020-11-11/2020-11-12"]'::jsonb),
    temporal_op_query('t_intersects', '[{"property":"datetime"},{"interval":["2020-11-11","2020-11-12"]}]'::jsonb),
    'the slash spelling of an interval resolves the same as the block'
);

SELECT is(
    temporal_op_query('t_intersects', '[{"property":"datetime"},["2020-11-11","2020-11-12"]]'::jsonb),
    temporal_op_query('t_intersects', '[{"property":"datetime"},{"interval":["2020-11-11","2020-11-12"]}]'::jsonb),
    'the array spelling of an interval resolves the same as the block'
);

SELECT is(
    temporal_op_query('t_intersects', '[{"property":"datetime"},"2020-11-11/.."]'::jsonb),
    temporal_op_query('t_intersects', '[{"property":"datetime"},"2020-11-11/"]'::jsonb),
    'an open slash interval end is spelled .. or nothing'
);

SELECT throws_ok($$
    SELECT temporal_op_query('t_intersects', '[{"property":"datetime"},{"interval":["2020-11-12T00:00:00Z","2020-11-11T00:00:00Z"]}]'::jsonb);
$$, 'Temporal interval {"interval": ["2020-11-12T00:00:00Z", "2020-11-11T00:00:00Z"]} ends before it starts.',
    'a reversed interval block is rejected'
);

SELECT throws_ok($$
    SELECT temporal_op_query('t_intersects', '[{"property":"datetime"},"2020-11-12T00:00:00Z/2020-11-11T00:00:00Z"]'::jsonb);
$$, 'Temporal interval "2020-11-12T00:00:00Z/2020-11-11T00:00:00Z" ends before it starts.',
    'a reversed slash interval is rejected'
);

SELECT throws_ok($$
    SELECT temporal_op_query('t_intersects', '[{"property":"eo:cloud_cover"},"2020-11-11"]'::jsonb);
$$, 'Property eo:cloud_cover is not temporal.',
    'a queryable with a numeric wrapper is rejected as a temporal operand'
);

SELECT hasnt_function('pgstac', 'paging_collections', ARRAY['jsonb'], 'paging_collections is gone');
SELECT hasnt_function('pgstac', 'paging_dtrange', ARRAY['jsonb'], 'paging_dtrange is gone');
SELECT hasnt_function('pgstac', 'queryable_signature', ARRAY['text', 'text[]'], 'queryable_signature is gone');
SELECT is(
    (SELECT count(*)::int FROM pg_proc WHERE pronamespace = 'pgstac'::regnamespace
        AND proname IN ('search_rows', 'partition_queries', 'partition_query_view')
        AND pg_get_function_arguments(oid) LIKE '%_orderby text DEFAULT ''datetime DESC, collection DESC, id DESC''::text%'),
    3,
    'search_rows, partition_queries and partition_query_view default to the tie-broken order'
);


SELECT is(
    cql2_query('{"op":"eq","args":[{"property":"id"},"x"]}'::jsonb),
    $$id = 'x'$$,
    'a supported cql2 operator resolves by exact name'
);

SELECT throws_like($$
    SELECT cql2_query('{"op":"%","args":[{"property":"eo:cloud_cover"},10]}'::jsonb);
$$, '% Not Supported.',
    'an unsupported cql2 operator is not resolved as a LIKE pattern'
);

SELECT throws_ok($$
    SELECT cql2_query('{"op":"t_intersects"}'::jsonb);
$$, 'The t_intersects operator requires an array of args.',
    'an operator without args raises instead of matching everything'
);

SELECT throws_ok($$
    SELECT cql2_query('{"op":"isNull","args":{"property":"id"}}'::jsonb);
$$, 'The isnull operator requires an array of args.',
    'an operator whose args is not an array raises'
);

SELECT throws_ok($$
    SELECT spatial_op_query('s_intersects', '[{"type":"Point","coordinates":[0,0]},{"property":"geometry"}]'::jsonb);
$$, 'Spatial operand {"property": "geometry"} is not a GeoJSON geometry or a bbox.',
    'a property in the second position of a spatial predicate raises'
);

SELECT throws_like($$
    SELECT spatial_op_query('s_intersects', '[{"property":"geometry"},"POINT(0 0)"]'::jsonb);
$$, 'Spatial operand % is not a GeoJSON geometry or a bbox.',
    'a WKT spatial operand raises'
);

SELECT throws_like($$
    SELECT spatial_op_query('s_intersects', '[{"property":"geometry"},[0,1,2]]'::jsonb);
$$, 'Spatial operand % is not a GeoJSON geometry or a bbox.',
    'a malformed bbox raises'
);

SELECT is(
    cql1_to_cql2('{"intersects":[{"property":"geometry"},{"type":"Point","coordinates":[0,0]}]}'::jsonb),
    '{"op":"intersects","args":[{"property":"geometry"},{"type":"Point","coordinates":[0,0]}]}'::jsonb,
    'cql-json passes a GeoJSON geometry through as a literal'
);

SELECT is(
    cql2_query(cql1_to_cql2('{"intersects":[{"property":"geometry"},{"type":"Point","coordinates":[0,0]}]}'::jsonb)),
    cql2_query('{"op":"s_intersects","args":[{"property":"geometry"},{"type":"Point","coordinates":[0,0]}]}'::jsonb),
    'a cql-json intersects with a Point resolves to the same SQL as s_intersects'
);

SELECT is(
    cql1_to_cql2('{"eq":[{"property":"id"},"x"],"lt":[{"property":"eo:cloud_cover"},10]}'::jsonb),
    '{"op":"and","args":[{"op":"eq","args":[{"property":"id"},"x"]},{"op":"lt","args":[{"property":"eo:cloud_cover"},10]}]}'::jsonb,
    'a two-key cql-json object is an implicit and'
);

SELECT is(
    cql1_to_cql2('{"filter":{"eq":[{"property":"id"},"x"]}}'::jsonb),
    '{"op":"eq","args":[{"property":"id"},"x"]}'::jsonb,
    'a filter wrapper is unwrapped'
);

SELECT is(
    cql2_query('{"upper":{"property":"id"}}'::jsonb),
    cql2_query('{"op":"upper","args":[{"property":"id"}]}'::jsonb),
    'the upper shorthand wraps its argument'
);

SELECT is(
    cql1_to_cql2('{"not":{"eq":[{"property":"id"},"x"]}}'::jsonb),
    '{"op":"not","args":[{"op":"eq","args":[{"property":"id"},"x"]}]}'::jsonb,
    'a unary cql-json operator gets an array of args'
);

SELECT is(
    cql1_to_cql2('{"eq":[{"property":"test:flag"},true]}'::jsonb),
    '{"op":"eq","args":[{"property":"test:flag"},true]}'::jsonb,
    'a boolean literal survives cql-json conversion'
);

SET pgstac.additional_properties TO 'false';

SELECT lives_ok(
    $$ SELECT upsert_queryable('test:strprop', definition => '{"type":"string"}'::jsonb); $$,
    'Can register a string queryable with no wrapper.'
);

SELECT lives_ok(
    $$ SELECT cql2_query('{"op":"eq","args":[{"property":"test:strprop"},"x"]}'::jsonb); $$,
    'a registered string queryable with no wrapper is accepted when additional_properties is false'
);

SELECT throws_ok(
    $$ SELECT cql2_query('{"op":"eq","args":[{"property":"test:unregistered"},"x"]}'::jsonb); $$,
    'Term test:unregistered is not found in queryables.',
    'an unregistered term still raises when additional_properties is false'
);

SELECT lives_ok(
    $$ SELECT delete_queryable('test:strprop'); $$,
    'Can remove the string queryable again.'
);

RESET pgstac.additional_properties;


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
                        "args": [ { "property": "sentinel:data_coverage" } ]
                    },
                    {
                        "op": "isNull",
                        "args": [ { "property": "landsat:coverage_percent" } ]
                    }
                    ]
                }
                ]
            }
        }
    $q$),E' \n');
    $$, $$
    SELECT BTRIM($r$
    (to_float(content->'properties'->'sentinel:data_coverage') >= to_float('50') OR to_float(content->'properties'->'landsat:coverage_percent') >= to_float('50') OR (to_text(content->'properties'->'sentinel:data_coverage') IS NULL AND to_text(content->'properties'->'landsat:coverage_percent') IS NULL))
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
    to_int(content->'properties'->'eo:cloud_cover') BETWEEN to_int('0') AND to_int('50')
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
    to_text(content->'properties'->'mission') LIKE to_text('"sentinel%"')
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
    upper(to_text(content->'properties'->'mission')) = upper(to_text('"sentinel"'))
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
    lower(to_text(content->'properties'->'mission')) = lower(to_text('"sentinel"'))
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
    upper(to_text(content->'properties'->'mission')) = upper(to_text('"sentinel"'))
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
    unaccent(to_text(content->'properties'->'mission')) = unaccent(to_text('"sentinel"'))
    $r$,E' \n');
    $$, 'Test accenti'
);

SELECT results_eq($$
    SELECT BTRIM(stac_search_to_where($q$
    {
        "filter-lang": "cql2-json",
        "filter": {
            "op": "gte",
            "args": [
            { "property": "start_datetime" },
            "2020-11-11T00:00:00Z"
            ]
        }
    }
    $q$),E' \n');
    $$, $$
    SELECT BTRIM($r$
    datetime >= to_tstz('"2020-11-11T00:00:00Z"')
    $r$,E' \n');
    $$, 'start_datetime resolves to the instantiated datetime column'
);

SELECT results_eq($$
    SELECT BTRIM(stac_search_to_where($q$
    {
        "filter-lang": "cql2-json",
        "filter": {
            "op": "gte",
            "args": [
            { "property": "properties.start_datetime" },
            "2020-11-11T00:00:00Z"
            ]
        }
    }
    $q$),E' \n');
    $$, $$
    SELECT BTRIM($r$
    datetime >= to_tstz('"2020-11-11T00:00:00Z"')
    $r$,E' \n');
    $$, 'properties.start_datetime resolves the same as start_datetime'
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

    INSERT INTO items (id, collection, datetime, end_datetime, geometry, content)
        SELECT concat(id, '_2'), 'pgstac-test-collection2', datetime, end_datetime, geometry, content FROM items WHERE collection='pgstac-test-collection';

    UPDATE items SET content = '{"properties":{"testsort":1}}'::jsonb
        WHERE collection = 'pgstac-test-collection2' AND
        id <= 'pgstac-test-item-0005_2';
    UPDATE items SET content = '{"properties":{"testsort":2}}'::jsonb
        WHERE collection = 'pgstac-test-collection2' AND
        id > 'pgstac-test-item-0005_2' and id <= 'pgstac-test-item-0010_2';
    UPDATE items SET content = '{"properties":{"testsort":3}}'::jsonb
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
                ORDER BY content->'properties'->>'testsort' %s, id %s
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
                ORDER BY content->'properties'->>'testsort' %s, id %s
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
                ORDER BY content->'properties'->>'testsort' %s, id %s
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
                ORDER BY content->'properties'->>'testsort' %s, id %s
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

-- Returns NULL rather than aborting the test file when a token cannot be resolved.
CREATE OR REPLACE FUNCTION pg_temp.dupsearch(_token text DEFAULT NULL) RETURNS jsonb AS $$
BEGIN
    RETURN search(
        '{"ids": ["pgstac-test-item-duplicated"], "limit": 1}'::jsonb
        || jsonb_build_object('token', _token)
    );
EXCEPTION WHEN OTHERS THEN
    RETURN NULL;
END;
$$ LANGUAGE PLPGSQL;

SELECT is(
    (SELECT jsonb_array_length(pg_temp.dupsearch(pg_temp.next(pg_temp.dupsearch()))->'features')),
    1,
    'Paging past an item whose id is duplicated in another collection returns the sibling. #392'
);

SELECT is(
    (SELECT pg_temp.dupsearch(pg_temp.next(pg_temp.dupsearch()))->'features'->0->>'collection'),
    'pgstac-test-collection',
    'Page 2 of a duplicated id search is the other collection. #392'
);

SELECT isnt(
    (SELECT (item).id FROM get_token_record(
        (SELECT pg_temp.prev(pg_temp.dupsearch(pg_temp.next(pg_temp.dupsearch())))))),
    NULL,
    'Page 2 of a duplicated id search carries a prev token that resolves to an item. #392'
);

SELECT is(
    (SELECT pg_temp.dupsearch(pg_temp.prev(pg_temp.dupsearch(pg_temp.next(pg_temp.dupsearch()))))->'features'->0->>'collection'),
    'pgstac-test-collection2',
    'Following the prev link from page 2 of a duplicated id search returns page 1. #392'
);

SELECT ok(
    pg_temp.isnull(pg_temp.prev(search('{"ids": ["pgstac-test-item-duplicated"], "limit": 2}'::jsonb
        || jsonb_build_object('token',
            'next:' || page_token('pgstac-test-collection', 'pgstac-test-item-duplicated'))))),
    'A page that returns no items carries no prev link. #392'
);

INSERT INTO items (id, collection, datetime, end_datetime, geometry, content)
    SELECT 'pgstac-test-collection:pgstac-test-item-0001', collection, datetime, end_datetime, geometry, content
    FROM items WHERE collection = 'pgstac-test-collection' AND id = 'pgstac-test-item-0001';

-- A bare date is the whole of that day wherever it appears. At either end of an interval it
-- already was; alone it was the instant of midnight, so the same string meant two things.
SELECT results_eq(
    $$ SELECT parse_dtrange('"2020-01-01"'::jsonb) $$,
    $$ SELECT '["2020-01-01 00:00:00+00","2020-01-02 00:00:00+00")'::tstzrange $$,
    'a bare date on its own is the whole day, as it already is inside an interval'
);

-- A timestamp with no offset means UTC whatever the session is set to, so this filter must select
-- the same rows in both. Features, not numberMatched: where_stats memoises the count against the
-- where clause, which is identical here, so comparing counts reads the cache and never exercises
-- the zone.
CREATE TEMP TABLE utc_dt_features AS
SELECT search('{"filter":{"op":"lt","args":[{"property":"datetime"},"2011-08-16"]},"limit":500,"fields":{"include":["id"]}}')->'features' AS f;
SET TIME ZONE 'America/New_York';
SELECT is(
    search('{"filter":{"op":"lt","args":[{"property":"datetime"},"2011-08-16"]},"limit":500,"fields":{"include":["id"]}}')->'features',
    (SELECT f FROM utc_dt_features),
    'a datetime filter selects the same rows whatever the session timezone'
);
RESET TIME ZONE;
DROP TABLE utc_dt_features;

-- An explicit timestamp spelling keeps meaning the instant it names; every other spelling of a
-- bare date is the whole day. Nothing covered this, and it silently became a day plus a
-- microsecond when the lone-date rule changed.
SELECT results_eq(
    $$ SELECT low_ts, high_ts FROM temporal_operand('{"timestamp":"2020-01-01"}'::jsonb) $$,
    $$ VALUES ('2020-01-01 00:00:00+00'::timestamptz, '2020-01-01 00:00:00+00'::timestamptz) $$,
    'an explicit timestamp spelling of a bare date stays the instant it names'
);
SELECT results_eq(
    $$ SELECT low_ts, high_ts FROM temporal_operand('"2020-01-01"'::jsonb) $$,
    $$ VALUES ('2020-01-01 00:00:00+00'::timestamptz, '2020-01-01 23:59:59.999999+00'::timestamptz) $$,
    'a bare date as an operand is the whole day, ending at its last microsecond'
);

-- A quoted phrase used to be usable only on its own: beside a bare term no adjacency operator
-- was inserted next to it, and a second phrase aborted the call outright.
SELECT is(
    q_to_tsquery(to_jsonb('landsat "sea ice"'::text))::text,
    $q$'landsat' <-> ( 'sea' <-> 'ice' )$q$,
    'a quoted phrase beside a bare term is joined to it'
);
SELECT is(
    q_to_tsquery(to_jsonb('"sea ice" AND "snow cover"'::text))::text,
    $q$'sea' <-> 'ice' & 'snow' <-> 'cover'$q$,
    'two quoted phrases in one query are both extracted'
);

-- The token is carried in a query string, so it can only use characters a client will not
-- rewrite: hex digits and the tilde separator are all RFC 3986 unreserved.
SELECT matches(page_token('pgstac-test-collection', 'item-1'),
    '^[0-9a-f]+:[0-9a-f]+$',
    'a token names the row as two hex halves');
SELECT is(page_token('a:b', 'x:y'),
    '613a62:783a79',
    'a colon in either half is encoded, so it cannot be mistaken for a separator');
SELECT is(
    (SELECT (item).id FROM get_token_record(
        'next:' || page_token('pgstac-test-collection', 'pgstac-test-item-0011'))),
    'pgstac-test-item-0011',
    'a token page_token wrote round-trips through get_token_record'
);
SELECT throws_ok(
    $$ SELECT get_token_record('6162:6364') $$,
    'P0001', 'Invalid paging token: 6162:6364',
    'a token with no direction is refused'
);
SELECT throws_ok(
    $$ SELECT get_token_record('sideways:6162:6364') $$,
    'P0001', 'Invalid paging token: sideways:6162:6364',
    'a token whose direction is neither next nor prev is refused'
);
SELECT throws_ok(
    $$ SELECT get_token_record('next:pgstac-test-collection:pgstac-test-item-0011') $$,
    'P0001', 'Invalid paging token: next:pgstac-test-collection:pgstac-test-item-0011',
    'an unencoded token is refused'
);

SELECT is(
    (SELECT (item).id FROM get_token_record(
        'next:' || page_token('pgstac-test-collection', 'pgstac-test-collection:pgstac-test-item-0001'))),
    'pgstac-test-collection:pgstac-test-item-0001',
    'A token for an item id that repeats its collection prefix resolves to that item. #392'
);

DELETE FROM items WHERE collection = 'pgstac-test-collection' AND id = 'pgstac-test-collection:pgstac-test-item-0001';

-- collection a holds items b:c and d while a:b is also a collection. Encoding both halves
-- is what keeps a token for (a, b:c) from reading as (a:b, c); a:b is created first so a
-- prefix match that takes the first row found would land on it.
SELECT create_collection('{"id":"a:b","type":"Collection","stac_version":"1.0.0","description":"a:b","license":"proprietary","links":[],"extent":{"spatial":{"bbox":[[-180,-90,180,90]]},"temporal":{"interval":[["2020-01-01T00:00:00Z","2020-12-31T00:00:00Z"]]}}}');
SELECT create_collection('{"id":"a","type":"Collection","stac_version":"1.0.0","description":"a","license":"proprietary","links":[],"extent":{"spatial":{"bbox":[[-180,-90,180,90]]},"temporal":{"interval":[["2020-01-01T00:00:00Z","2020-12-31T00:00:00Z"]]}}}');
SELECT create_items(jsonb_agg(jsonb_build_object(
    'id', i.id, 'type', 'Feature', 'stac_version', '1.0.0', 'collection', 'a',
    'bbox', '[0,0,1,1]'::jsonb, 'links', '[]'::jsonb, 'assets', '{}'::jsonb,
    'geometry', '{"type":"Polygon","coordinates":[[[0,0],[0,1],[1,1],[1,0],[0,0]]]}'::jsonb,
    'properties', jsonb_build_object('datetime', i.dt)
))) FROM (VALUES ('b:c', '2020-06-02T00:00:00Z'), ('d', '2020-06-01T00:00:00Z')) AS i(id, dt);

SELECT is(
    search('{"collections":["a"],"limit":1}'::jsonb || jsonb_build_object('token',
        'next:' || page_token('a', 'b:c')))->'features'->0->>'id',
    'd',
    'a token whose collection prefix is also a longer collection id resolves'
);

SELECT is(
    pg_temp.next(search('{"collections":["a"],"limit":1}')),
    'next:' || page_token('a', 'b:c'),
    'the next token pgstac emits for that item is the one it resolves'
);

SELECT delete_collection('a:b');
SELECT delete_collection('a');

SELECT results_eq($$
    SELECT (get_token_record(t)).prev FROM unnest(ARRAY[
        'prev:' || page_token('pgstac-test-collection', 'pgstac-test-item-0011'),
        'next:' || page_token('pgstac-test-collection', 'pgstac-test-item-0011')]) t
    $$, $$ VALUES (true), (false) $$,
    'only a prev: direction marks a token as prev'
);

-- Catches the tsquery syntax errors the unfixed doubled-operator inputs raise.
CREATE OR REPLACE FUNCTION pg_temp.tsq(j jsonb) RETURNS text AS $$
BEGIN
    RETURN q_to_tsquery(j)::text;
EXCEPTION WHEN others THEN
    RETURN 'ERROR: ' || SQLERRM;
END;
$$ LANGUAGE plpgsql;

SELECT is(
    pg_temp.tsq('"first-generation"'::jsonb),
    $q$'first-gener' <-> 'first' <-> 'generat'$q$,
    'A hyphen inside a word is not an exclusion operator. #459'
);

SELECT ok(
    to_tsvector('english', 'a first-generation sensor') @@ q_to_tsquery('"first-generation"'::jsonb),
    'A document containing a hyphenated word matches a search for that word. #459'
);

SELECT is(
    pg_temp.tsq('"landsat-8 OR sentinel-2"'::jsonb),
    $q$'landsat' <-> '-8' | 'sentinel' <-> '-2'$q$,
    'A hyphen before a digit inside a word is not an exclusion operator. #459'
);

SELECT is(
    pg_temp.tsq('"model+data"'::jsonb),
    $q$'model' <-> 'data'$q$,
    'A plus inside a word is not an and operator. #459'
);

SELECT is(
    pg_temp.tsq('"bear -stranger"'::jsonb),
    $q$'bear' & !'stranger'$q$,
    'A term prefixed with a hyphen after whitespace is still excluded. #459'
);

SELECT is(
    pg_temp.tsq('"-stranger"'::jsonb),
    $q$!'stranger'$q$,
    'A term prefixed with a hyphen at the start is still excluded. #459'
);

SELECT is(
    pg_temp.tsq('"bear AND -stranger"'::jsonb),
    $q$'bear' & !'stranger'$q$,
    'An excluded term after AND does not double the operator. #459'
);

SELECT is(
    pg_temp.tsq('["bear", "-stranger"]'::jsonb),
    $q$'bear' | !'stranger'$q$,
    'An excluded term in an array of q values does not double the operator. #459'
);

SELECT is(
    pg_temp.tsq('"bear AND +stranger"'::jsonb),
    $q$'bear' & 'stranger'$q$,
    'An included term after AND does not double the operator. #459'
);

SELECT ok(
    (SELECT prosrc FROM pg_proc WHERE proname = 'q_to_tsquery') NOT LIKE '%RAISE NOTICE%',
    'q_to_tsquery does not emit a debug NOTICE. #459'
);

-- sortby is validated once, before any SQL is built
SELECT throws_ok($$
    SELECT search('{"sortby": "datetime"}')
$$, 'P0001', 'Invalid sortby "datetime": must be an array of {"field": text, "direction": text} objects', 'A sortby string raises.');

SELECT throws_ok($$
    SELECT search('{"sortby": [{"direction": "asc"}]}')
$$, 'P0001', 'Invalid sortby [{"direction": "asc"}]: must be an array of {"field": text, "direction": text} objects', 'A sortby object without a field raises.');

SELECT throws_ok($$
    SELECT search('{"sortby": [1]}')
$$, 'P0001', 'Invalid sortby [1]: must be an array of {"field": text, "direction": text} objects', 'A sortby array of non-objects raises.');

-- limit: non-integer or negative raises; 0 is an empty page
SELECT throws_ok($$ SELECT search('{"limit": 1.5}') $$, 'P0001', 'Invalid limit 1.5: must be a non-negative integer', 'search: a non-integer limit raises.');
SELECT throws_ok($$ SELECT search('{"limit": -1}') $$, 'P0001', 'Invalid limit -1: must be a non-negative integer', 'search: a negative limit raises.');
SELECT throws_ok($$ SELECT search('{"limit": "ten"}') $$, 'P0001', 'Invalid limit "ten": must be a non-negative integer', 'search: a non-numeric limit raises.');
SELECT throws_ok($$ SELECT collection_search('{"limit": 1.5}') $$, 'P0001', 'Invalid limit 1.5: must be a non-negative integer', 'collection_search: a non-integer limit raises.');
SELECT throws_ok($$ SELECT collection_search('{"limit": -1}') $$, 'P0001', 'Invalid limit -1: must be a non-negative integer', 'collection_search: a negative limit raises.');

SELECT is(
    search('{"collections": ["pgstac-test-collection"], "limit": 0}') - 'links' - 'numberMatched',
    '{"type": "FeatureCollection", "features": [], "numberReturned": 0}'::jsonb,
    'search: limit 0 returns an empty page.'
);
SELECT ok(
    pg_temp.isnull(pg_temp.next(search('{"collections": ["pgstac-test-collection"], "limit": 0}'))),
    'search: limit 0 has no next link.'
);
SELECT ok(
    pg_temp.isnull(pg_temp.prev(search('{"collections": ["pgstac-test-collection"], "limit": 0}'))),
    'search: limit 0 without a token has no prev link.'
);
SELECT is(
    pg_temp.prev(search('{"collections": ["pgstac-test-collection"], "limit": 0}'::jsonb
        || jsonb_build_object('token',
            'next:' || page_token('pgstac-test-collection', 'pgstac-test-item-0002')))),
    'prev:' || page_token('pgstac-test-collection', 'pgstac-test-item-0002'),
    'search: limit 0 reached by token has a prev link and no next link.'
);
SELECT ok(
    pg_temp.isnull(pg_temp.next(search('{"collections": ["pgstac-test-collection"], "limit": 0}'::jsonb
        || jsonb_build_object('token',
            'next:' || page_token('pgstac-test-collection', 'pgstac-test-item-0002'))))),
    'search: limit 0 reached by token has no next link.'
);

SELECT is(
    collection_search('{"limit": 0}') - 'links' - 'numberMatched',
    '{"collections": [], "numberReturned": 0}'::jsonb,
    'collection_search: limit 0 returns an empty page.'
);
SELECT is(
    (SELECT array_agg(l->>'rel' ORDER BY l->>'rel') FROM jsonb_array_elements(collection_search('{"limit": 0}')->'links') l),
    NULL,
    'collection_search: limit 0 without an offset has no prev or next link.'
);
SELECT is(
    (SELECT array_agg(l->>'rel' ORDER BY l->>'rel') FROM jsonb_array_elements(collection_search('{"limit": 0, "offset": 1}')->'links') l),
    '{prev}'::text[],
    'collection_search: limit 0 with an offset has only a prev link.'
);
SELECT is(
    (SELECT array_agg(l->>'rel' ORDER BY l->>'rel') FROM jsonb_array_elements(collection_search('{"limit": 1, "offset": 100000}')->'links') l),
    '{prev}'::text[],
    'collection_search: an offset past the end has only a prev link.'
);
SELECT is(
    collection_search('{"limit": 1, "offset": 100000}')->'numberReturned',
    '0'::jsonb,
    'collection_search: an offset past the end returns an empty page.'
);

SELECT throws_ok($$
    SELECT geometrysearch(st_makeenvelope(0, 0, 1, 1, 4326), 'nosuchhash')
$$, 'P0001', 'Search with Query Hash nosuchhash Not Found', 'geometrysearch raises on an unknown query hash.');

SELECT throws_ok($$
    SELECT search_fromhash('nonexistent')
$$, 'P0001', 'Search with Query Hash nonexistent Not Found', 'search_fromhash raises on an unknown query hash.');


-- The between operator, and the arity every operator is held to.

SELECT is(
    cql2_query('{"op":"between","args":[{"property":"datetime"},"2020-01-01T00:00:00Z","2020-06-01T00:00:00Z"]}'::jsonb),
    $q$datetime BETWEEN to_tstz('"2020-01-01T00:00:00Z"') AND to_tstz('"2020-06-01T00:00:00Z"')$q$,
    'between reads a datetime column through to_tstz'
);

SELECT is(
    cql2_query('{"op":"between","args":[{"property":"id"},"a","z"]}'::jsonb),
    $q$id BETWEEN 'a' AND 'z'$q$,
    'between reads a text column as itself'
);

SELECT throws_ok($$
    SELECT cql2_query('{"op":"between","args":[{"property":"datetime"},"2020-01-01T00:00:00Z"]}'::jsonb)
$$, 'P0001', 'The between operator takes a value, a lower bound and an upper bound.',
   'between refuses two arguments');

SELECT throws_ok($$
    SELECT cql2_query('{"op":"between","args":[{"property":"datetime"},"2020-01-01T00:00:00Z","2020-06-01T00:00:00Z","2020-09-01T00:00:00Z"]}'::jsonb)
$$, 'P0001', 'The between operator takes a value, a lower bound and an upper bound.',
   'between refuses four arguments');

SELECT throws_ok($$
    SELECT cql2_query('{"op":"in","args":[{"property":"id"},"notanarray"]}'::jsonb)
$$, 'P0001', 'The in operator takes a value and an array of values.',
   'in refuses a second argument that is not an array');

SELECT throws_ok($$
    SELECT cql2_query('{"op":"isnull","args":[{"property":"id"},1,2]}'::jsonb)
$$, 'P0001', 'The isnull operator was given 3 arguments, more than it takes.',
   'an operator refuses more arguments than its template takes');

-- like compares text, and says which half of the reason it refused for.

SELECT upsert_queryable(
    name => 'test:likeint', definition => '{"type":"integer"}'::jsonb, property_wrapper => 'to_int');

SELECT throws_ok($$
    SELECT cql2_query('{"op":"like","args":[{"property":"test:likeint"},"a%"]}'::jsonb)
$$, 'P0001', 'The like operator compares text, but its operand is read with to_int.',
   'like refuses an operand read through a non-text wrapper');

SELECT delete_queryable('test:likeint');

SELECT throws_ok($$
    SELECT cql2_query('{"op":"like","args":[{"property":"geometry"},"a%"]}'::jsonb)
$$, 'P0001', 'The like operator compares text, but its operand is not a text column.',
   'like refuses a column of items that is not text');

-- A property that names nothing is refused where the cause is visible, not in the executor.

SELECT throws_ok($$
    SELECT cql2_query('{"op":"eq","args":[{"property":""},1]}'::jsonb)
$$, 'P0001', 'A property name is required.',
   'cql2_query refuses an empty property name');

SELECT throws_ok($$
    SELECT cql2_query('{"op":"eq","args":[{"property":"properties"},1]}'::jsonb)
$$, 'P0001', 'A property name is required.',
   'cql2_query refuses a property named only properties');

SELECT throws_ok($$
    SELECT cql2_query('{"op":"t_intersects","args":[{"property":""},"2020-01-01T00:00:00Z"]}'::jsonb)
$$, 'P0001', 'A property name is required.',
   'a temporal operand refuses an empty property name');

-- An interval that selects nothing is refused rather than returning no rows silently.

SELECT throws_ok($$
    SELECT parse_dtrange('"P0D/2020-01-02"'::jsonb)
$$, 'P0001', 'Datetime range "P0D/2020-01-02" is empty: its duration is zero.',
   'parse_dtrange refuses a zero duration');

SELECT throws_ok($$
    SELECT parse_dtrange('"2020-01-05/2020-01-01"'::jsonb)
$$, 'P0001', 'Datetime range "2020-01-05/2020-01-01" is empty: it ends before it starts.',
   'parse_dtrange refuses an interval that ends before it starts');

SELECT throws_ok($$
    SELECT q_to_tsquery('"cloud @QUOTE@ rain"'::jsonb)
$$, 'P0001', 'Free text query may not contain @QUOTE@',
   'free text refuses the placeholder it substitutes phrases with');

-- The two token guards the existing tests above do not reach: an odd number of hex digits,
-- which is what decode itself would reject, and a token whose shape is right but names nothing.

SELECT throws_ok($$
    SELECT get_token_record('next:616:6161')
$$, 'P0001', 'Invalid paging token: next:616:6161',
   'a token with an odd number of hex digits is refused before decode sees it');

SELECT throws_ok($$
    SELECT get_token_record('next:6e6f70653a:6e6f70653a')
$$, 'P0001', 'Could not find item using token: next:6e6f70653a:6e6f70653a',
   'a well formed token naming no item is refused');
