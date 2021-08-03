-- CREATE fixtures for testing search - as tests are run within a transaction, these will not persist
\copy collections (content) FROM 'test/testdata/collections.ndjson'
\copy items (content) FROM 'test/testdata/items.ndjson'


SELECT has_function('pgstac'::name, 'parse_dtrange', ARRAY['jsonb']);


SELECT results_eq($$ SELECT parse_dtrange('["2020-01-01","2021-01-01"]') $$, $$ SELECT '["2020-01-01 00:00:00+00","2021-01-01 00:00:00+00")'::tstzrange $$, 'daterange passed as array range');


SELECT results_eq($$ SELECT parse_dtrange('"2020-01-01/2021-01-01"') $$, $$ SELECT '["2020-01-01 00:00:00+00","2021-01-01 00:00:00+00")'::tstzrange $$, 'date range passed as string range');


SELECT has_function('pgstac'::name, 'bbox_geom', ARRAY['jsonb']);


SELECT results_eq($$ SELECT bbox_geom('[0,1,2,3]') $$, $$ SELECT 'SRID=4326;POLYGON((0 1,0 3,2 3,2 1,0 1))'::geometry $$, '2d bbox');


SELECT results_eq($$ SELECT bbox_geom('[0,1,2,3,4,5]'::jsonb) $$, $$ SELECT '010F0000A0E610000006000000010300008001000000050000000000000000000000000000000000F03F00000000000000400000000000000000000000000000104000000000000000400000000000000840000000000000104000000000000000400000000000000840000000000000F03F00000000000000400000000000000000000000000000F03F0000000000000040010300008001000000050000000000000000000000000000000000F03F00000000000014400000000000000840000000000000F03F00000000000014400000000000000840000000000000104000000000000014400000000000000000000000000000104000000000000014400000000000000000000000000000F03F0000000000001440010300008001000000050000000000000000000000000000000000F03F00000000000000400000000000000000000000000000F03F00000000000014400000000000000000000000000000104000000000000014400000000000000000000000000000104000000000000000400000000000000000000000000000F03F0000000000000040010300008001000000050000000000000000000840000000000000F03F00000000000000400000000000000840000000000000104000000000000000400000000000000840000000000000104000000000000014400000000000000840000000000000F03F00000000000014400000000000000840000000000000F03F0000000000000040010300008001000000050000000000000000000000000000000000F03F00000000000000400000000000000840000000000000F03F00000000000000400000000000000840000000000000F03F00000000000014400000000000000000000000000000F03F00000000000014400000000000000000000000000000F03F000000000000004001030000800100000005000000000000000000000000000000000010400000000000000040000000000000000000000000000010400000000000001440000000000000084000000000000010400000000000001440000000000000084000000000000010400000000000000040000000000000000000000000000010400000000000000040'::geometry $$, '3d bbox');


SELECT has_function('pgstac'::name, 'add_filters_to_cql', ARRAY['jsonb']);

SELECT results_eq($$
    SELECT add_filters_to_cql('{"id":["a","b"]}'::jsonb);
    $$,$$
    SELECT '{"filter":{"and": [{"in": [{"property": "id"}, ["a", "b"]]}]}}'::jsonb;
    $$,
    'Test that id gets added to cql filter when cql filter does not exist'
);

SELECT results_eq($$
    SELECT add_filters_to_cql('{"id":["a","b"],"filter":{"and":[{"eq":[1,1]}]}}'::jsonb);
    $$,$$
    SELECT '{"filter":{"and": [{"and": [{"eq": [1, 1]}]}, {"and": [{"in": [{"property": "id"}, ["a", "b"]]}]}]}}'::jsonb;
    $$,
    'Test that id gets added to cql filter when cql filter does exist'
);

SELECT has_function('pgstac'::name, 'cql_and_append', ARRAY['jsonb','jsonb']);

SELECT has_function('pgstac'::name, 'query_to_cqlfilter', ARRAY['jsonb']);

SELECT results_eq($$
    SELECT query_to_cqlfilter('{"query":{"a":{"gt":0,"lte":10},"b":"test"}}');
    $$,$$
    SELECT '{"filter":{"and": [{"gt": [{"property": "a"}, 0]}, {"lte": [{"property": "a"}, 10]}, {"eq": [{"property": "b"}, "test"]}]}}'::jsonb;
    $$,
    'Test that query_to_cqlfilter appropriately converts old style query items to cql filters'
);


SELECT has_function('pgstac'::name, 'sort_sqlorderby', ARRAY['jsonb','boolean']);

SELECT results_eq($$
    SELECT sort_sqlorderby('{"sort":[{"field":"datetime","direction":"desc"},{"field":"eo:cloudcover","direction":"asc"}]}'::jsonb);
    $$,$$
    SELECT 'datetime DESC, items.properties->''eo:cloudcover'' ASC, id DESC';
    $$,
    'Test creation of sort sql'
);


SELECT results_eq($$
    SELECT sort_sqlorderby('{"sort":[{"field":"datetime","direction":"desc"},{"field":"eo:cloudcover","direction":"asc"}]}'::jsonb, true);
    $$,$$
    SELECT 'datetime ASC, items.properties->''eo:cloudcover'' DESC, id ASC';
    $$,
    'Test creation of reverse sort sql'
);

SELECT results_eq($$
    select s from search('{"fields":{"include":["id","datetime","eo:cloud_cover"]},"sort":[{"field":"datetime","direction":"desc"},{"field":"id","direction":"asc"}]}') s;
    $$,$$
    select '{"next": "pgstac-test-item-0010", "prev": null, "type": "FeatureCollection", "context": {"limit": 10, "returned": 10}, "features": [{"id": "pgstac-test-item-0001", "properties": {"datetime": "2011-08-25T00:00:00Z", "eo:cloud_cover": 89}}, {"id": "pgstac-test-item-0002", "properties": {"datetime": "2011-08-25T00:00:00Z", "eo:cloud_cover": 33}}, {"id": "pgstac-test-item-0003", "properties": {"datetime": "2011-08-25T00:00:00Z", "eo:cloud_cover": 28}}, {"id": "pgstac-test-item-0004", "properties": {"datetime": "2011-08-24T00:00:00Z", "eo:cloud_cover": 23}}, {"id": "pgstac-test-item-0005", "properties": {"datetime": "2011-08-24T00:00:00Z", "eo:cloud_cover": 3}}, {"id": "pgstac-test-item-0006", "properties": {"datetime": "2011-08-24T00:00:00Z", "eo:cloud_cover": 100}}, {"id": "pgstac-test-item-0007", "properties": {"datetime": "2011-08-17T00:00:00Z", "eo:cloud_cover": 59}}, {"id": "pgstac-test-item-0008", "properties": {"datetime": "2011-08-17T00:00:00Z", "eo:cloud_cover": 64}}, {"id": "pgstac-test-item-0009", "properties": {"datetime": "2011-08-17T00:00:00Z", "eo:cloud_cover": 61}}, {"id": "pgstac-test-item-0010", "properties": {"datetime": "2011-08-17T00:00:00Z", "eo:cloud_cover": 31}}]}'::jsonb
    $$,
    'Test basic search with fields and sort extension'
);

SELECT results_eq($$
    select s from search('{"token":"next:pgstac-test-item-0010", "fields":{"include":["id","datetime","eo:cloud_cover"]},"sort":[{"field":"datetime","direction":"desc"},{"field":"id","direction":"asc"}]}') s;
    $$,$$
    select '{"next": "pgstac-test-item-0020", "prev": "pgstac-test-item-0011", "type": "FeatureCollection", "context": {"limit": 10, "returned": 10}, "features": [{"id": "pgstac-test-item-0011", "properties": {"datetime": "2011-08-17T00:00:00Z", "eo:cloud_cover": 41}}, {"id": "pgstac-test-item-0012", "properties": {"datetime": "2011-08-17T00:00:00Z", "eo:cloud_cover": 4}}, {"id": "pgstac-test-item-0013", "properties": {"datetime": "2011-08-17T00:00:00Z", "eo:cloud_cover": 2}}, {"id": "pgstac-test-item-0014", "properties": {"datetime": "2011-08-16T00:00:00Z", "eo:cloud_cover": 17}}, {"id": "pgstac-test-item-0015", "properties": {"datetime": "2011-08-16T00:00:00Z", "eo:cloud_cover": 54}}, {"id": "pgstac-test-item-0016", "properties": {"datetime": "2011-08-16T00:00:00Z", "eo:cloud_cover": 13}}, {"id": "pgstac-test-item-0017", "properties": {"datetime": "2011-08-16T00:00:00Z", "eo:cloud_cover": 59}}, {"id": "pgstac-test-item-0018", "properties": {"datetime": "2011-08-16T00:00:00Z", "eo:cloud_cover": 29}}, {"id": "pgstac-test-item-0019", "properties": {"datetime": "2011-08-16T00:00:00Z", "eo:cloud_cover": 52}}, {"id": "pgstac-test-item-0020", "properties": {"datetime": "2011-08-16T00:00:00Z", "eo:cloud_cover": 39}}]}'::jsonb
    $$,
    'Test basic search with fields and sort extension and next token'
);

SELECT results_eq($$
    select s from search('{"token":"prev:pgstac-test-item-0011", "fields":{"include":["id","datetime","eo:cloud_cover"]},"sort":[{"field":"datetime","direction":"desc"},{"field":"id","direction":"asc"}]}') s;
    $$,$$ -- should be the same result as the first base query
    select s from search('{"fields":{"include":["id","datetime","eo:cloud_cover"]},"sort":[{"field":"datetime","direction":"desc"},{"field":"id","direction":"asc"}]}') s;
    $$,
    'Test basic search with fields and sort extension and prev token'
);


/* template
SELECT results_eq($$

    $$,$$

    $$,
    'Test that ...'
);
*/
