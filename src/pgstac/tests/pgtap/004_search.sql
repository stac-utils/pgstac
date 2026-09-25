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



SELECT has_function('pgstac'::name, 'sort_sqlorderby', ARRAY['jsonb','boolean']);

SELECT results_eq($$
    SELECT sort_sqlorderby('{"sortby":[{"field":"datetime","direction":"desc"},{"field":"eo:cloud_cover","direction":"asc"}]}'::jsonb);
    $$,$$
    SELECT 'datetime DESC, to_int(content->''properties''->''eo:cloud_cover'') ASC, id DESC';
    $$,
    'Test creation of sort sql'
);


SELECT results_eq($$
    SELECT sort_sqlorderby('{"sortby":[{"field":"datetime","direction":"desc"},{"field":"eo:cloud_cover","direction":"asc"}]}'::jsonb, true);
    $$,$$
    SELECT 'datetime ASC, to_int(content->''properties''->''eo:cloud_cover'') DESC, id ASC';
    $$,
    'Test creation of reverse sort sql'
);


SELECT has_function('pgstac'::name, 'search', ARRAY['jsonb']);


SELECT results_eq($$
    SELECT search('{"collections": ["pgstac-test-collection"], "limit": 10, "sortby":[{"field":"id","direction":"asc"}], "token": "prev:pgstac-test-item-0011"}')
    $$,$$
    SELECT search('{"collections": ["pgstac-test-collection"], "limit": 10, "sortby":[{"field":"id","direction":"asc"}]}')
    $$,
    'Test prev token when reading first token_type=prev (https://github.com/stac-utils/pgstac/issues/140)'
);


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
