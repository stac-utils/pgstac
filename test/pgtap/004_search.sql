-- CREATE fixtures for testing search - as tests are run within a transaction, these will not persist
DELETE FROM collections WHERE id = 'pgstac-test-collection';
\copy collections (content) FROM 'test/testdata/collections.ndjson'
\copy items_staging (content) FROM 'test/testdata/items.ndjson'

SET pgstac.context TO 'on';
SET pgstac."default-filter-lang" TO 'cql-json';

SELECT has_function('pgstac'::name, 'parse_dtrange', ARRAY['jsonb']);


SELECT results_eq($$ SELECT parse_dtrange('["2020-01-01","2021-01-01"]') $$, $$ SELECT '["2020-01-01 00:00:00+00","2021-01-01 00:00:00+00")'::tstzrange $$, 'daterange passed as array range');


SELECT results_eq($$ SELECT parse_dtrange('"2020-01-01/2021-01-01"') $$, $$ SELECT '["2020-01-01 00:00:00+00","2021-01-01 00:00:00+00")'::tstzrange $$, 'date range passed as string range');


SELECT has_function('pgstac'::name, 'bbox_geom', ARRAY['jsonb']);


SELECT results_eq($$ SELECT bbox_geom('[0,1,2,3]') $$, $$ SELECT 'SRID=4326;POLYGON((0 1,0 3,2 3,2 1,0 1))'::geometry $$, '2d bbox');


SELECT results_eq($$ SELECT bbox_geom('[0,1,2,3,4,5]'::jsonb) $$, $$ SELECT '010F0000A0E610000006000000010300008001000000050000000000000000000000000000000000F03F00000000000000400000000000000000000000000000104000000000000000400000000000000840000000000000104000000000000000400000000000000840000000000000F03F00000000000000400000000000000000000000000000F03F0000000000000040010300008001000000050000000000000000000000000000000000F03F00000000000014400000000000000840000000000000F03F00000000000014400000000000000840000000000000104000000000000014400000000000000000000000000000104000000000000014400000000000000000000000000000F03F0000000000001440010300008001000000050000000000000000000000000000000000F03F00000000000000400000000000000000000000000000F03F00000000000014400000000000000000000000000000104000000000000014400000000000000000000000000000104000000000000000400000000000000000000000000000F03F0000000000000040010300008001000000050000000000000000000840000000000000F03F00000000000000400000000000000840000000000000104000000000000000400000000000000840000000000000104000000000000014400000000000000840000000000000F03F00000000000014400000000000000840000000000000F03F0000000000000040010300008001000000050000000000000000000000000000000000F03F00000000000000400000000000000840000000000000F03F00000000000000400000000000000840000000000000F03F00000000000014400000000000000000000000000000F03F00000000000014400000000000000000000000000000F03F000000000000004001030000800100000005000000000000000000000000000000000010400000000000000040000000000000000000000000000010400000000000001440000000000000084000000000000010400000000000001440000000000000084000000000000010400000000000000040000000000000000000000000000010400000000000000040'::geometry $$, '3d bbox');


SELECT has_function('pgstac'::name, 'add_filters_to_cql', ARRAY['jsonb']);

SELECT results_eq($$
    SELECT add_filters_to_cql('{"ids":["a","b"]}'::jsonb);
    $$,$$
    SELECT '{"filter":{"and": [{"in": [{"property": "id"}, ["a", "b"]]}]}}'::jsonb;
    $$,
    'Test that id gets added to cql filter when cql filter does not exist'
);

SELECT results_eq($$
    SELECT add_filters_to_cql('{"ids":["a","b"],"filter":{"and":[{"eq":[1,1]}]}}'::jsonb);
    $$,$$
    SELECT '{"filter":{"and": [{"and": [{"eq": [1, 1]}]}, {"and": [{"in": [{"property": "id"}, ["a", "b"]]}]}]}}'::jsonb;
    $$,
    'Test that id gets added to cql filter when cql filter does exist'
);

SELECT results_eq($$
    SELECT add_filters_to_cql('{"collections":["a","b"]}'::jsonb);
    $$,$$
    SELECT '{"filter":{"and": [{"in": [{"property": "collection"}, ["a", "b"]]}]}}'::jsonb;
    $$,
    'Test that collections gets added to cql filter when cql filter does not exist'
);

SELECT results_eq($$
    SELECT add_filters_to_cql('{"collection":["a","b"]}'::jsonb);
    $$,$$
    SELECT '{"collection": ["a", "b"]}'::jsonb;
    $$,
    'Test that collection are not added to cql filter'
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
    SELECT sort_sqlorderby('{"sortby":[{"field":"datetime","direction":"desc"},{"field":"eo:cloudcover","direction":"asc"}]}'::jsonb);
    $$,$$
    SELECT 'datetime DESC,  properties->''eo:cloudcover''  ASC, id DESC';
    $$,
    'Test creation of sort sql'
);


SELECT results_eq($$
    SELECT sort_sqlorderby('{"sortby":[{"field":"datetime","direction":"desc"},{"field":"eo:cloudcover","direction":"asc"}]}'::jsonb, true);
    $$,$$
    SELECT 'datetime ASC,  properties->''eo:cloudcover''  DESC, id ASC';
    $$,
    'Test creation of reverse sort sql'
);

SELECT results_eq($$
    select s from search('{"fields":{"include":["id","datetime","eo:cloud_cover"]},"sortby":[{"field":"datetime","direction":"desc"},{"field":"id","direction":"asc"}]}') s;
    $$,$$
    select '{"next": "pgstac-test-item-0010", "prev": null, "type": "FeatureCollection", "context": {"limit": 10, "matched": 100, "returned": 10}, "features": [{"id": "pgstac-test-item-0001", "properties": {"datetime": "2011-08-25T00:00:00Z", "eo:cloud_cover": 89}}, {"id": "pgstac-test-item-0002", "properties": {"datetime": "2011-08-25T00:00:00Z", "eo:cloud_cover": 33}}, {"id": "pgstac-test-item-0003", "properties": {"datetime": "2011-08-25T00:00:00Z", "eo:cloud_cover": 28}}, {"id": "pgstac-test-item-0004", "properties": {"datetime": "2011-08-24T00:00:00Z", "eo:cloud_cover": 23}}, {"id": "pgstac-test-item-0005", "properties": {"datetime": "2011-08-24T00:00:00Z", "eo:cloud_cover": 3}}, {"id": "pgstac-test-item-0006", "properties": {"datetime": "2011-08-24T00:00:00Z", "eo:cloud_cover": 100}}, {"id": "pgstac-test-item-0007", "properties": {"datetime": "2011-08-17T00:00:00Z", "eo:cloud_cover": 59}}, {"id": "pgstac-test-item-0008", "properties": {"datetime": "2011-08-17T00:00:00Z", "eo:cloud_cover": 64}}, {"id": "pgstac-test-item-0009", "properties": {"datetime": "2011-08-17T00:00:00Z", "eo:cloud_cover": 61}}, {"id": "pgstac-test-item-0010", "properties": {"datetime": "2011-08-17T00:00:00Z", "eo:cloud_cover": 31}}]}'::jsonb
    $$,
    'Test basic search with fields and sort extension'
);

SELECT results_eq($$
    select s from search('{"token":"next:pgstac-test-item-0010", "fields":{"include":["id","datetime","eo:cloud_cover"]},"sortby":[{"field":"datetime","direction":"desc"},{"field":"id","direction":"asc"}]}') s;
    $$,$$
    select '{"next": "pgstac-test-item-0020", "prev": "pgstac-test-item-0011", "type": "FeatureCollection", "context": {"limit": 10, "matched": 100, "returned": 10}, "features": [{"id": "pgstac-test-item-0011", "properties": {"datetime": "2011-08-17T00:00:00Z", "eo:cloud_cover": 41}}, {"id": "pgstac-test-item-0012", "properties": {"datetime": "2011-08-17T00:00:00Z", "eo:cloud_cover": 4}}, {"id": "pgstac-test-item-0013", "properties": {"datetime": "2011-08-17T00:00:00Z", "eo:cloud_cover": 2}}, {"id": "pgstac-test-item-0014", "properties": {"datetime": "2011-08-16T00:00:00Z", "eo:cloud_cover": 17}}, {"id": "pgstac-test-item-0015", "properties": {"datetime": "2011-08-16T00:00:00Z", "eo:cloud_cover": 54}}, {"id": "pgstac-test-item-0016", "properties": {"datetime": "2011-08-16T00:00:00Z", "eo:cloud_cover": 13}}, {"id": "pgstac-test-item-0017", "properties": {"datetime": "2011-08-16T00:00:00Z", "eo:cloud_cover": 59}}, {"id": "pgstac-test-item-0018", "properties": {"datetime": "2011-08-16T00:00:00Z", "eo:cloud_cover": 29}}, {"id": "pgstac-test-item-0019", "properties": {"datetime": "2011-08-16T00:00:00Z", "eo:cloud_cover": 52}}, {"id": "pgstac-test-item-0020", "properties": {"datetime": "2011-08-16T00:00:00Z", "eo:cloud_cover": 39}}]}'::jsonb
    $$,
    'Test basic search with fields and sort extension and next token'
);

SELECT results_eq($$
    select s from search('{"token":"prev:pgstac-test-item-0011", "fields":{"include":["id","datetime","eo:cloud_cover"]},"sortby":[{"field":"datetime","direction":"desc"},{"field":"id","direction":"asc"}]}') s;
    $$,$$ -- should be the same result as the first base query
    select '{"next": "pgstac-test-item-0010", "prev": null, "type": "FeatureCollection", "context": {"limit": 10, "matched": 100, "returned": 10}, "features": [{"id": "pgstac-test-item-0001", "properties": {"datetime": "2011-08-25T00:00:00Z", "eo:cloud_cover": 89}}, {"id": "pgstac-test-item-0002", "properties": {"datetime": "2011-08-25T00:00:00Z", "eo:cloud_cover": 33}}, {"id": "pgstac-test-item-0003", "properties": {"datetime": "2011-08-25T00:00:00Z", "eo:cloud_cover": 28}}, {"id": "pgstac-test-item-0004", "properties": {"datetime": "2011-08-24T00:00:00Z", "eo:cloud_cover": 23}}, {"id": "pgstac-test-item-0005", "properties": {"datetime": "2011-08-24T00:00:00Z", "eo:cloud_cover": 3}}, {"id": "pgstac-test-item-0006", "properties": {"datetime": "2011-08-24T00:00:00Z", "eo:cloud_cover": 100}}, {"id": "pgstac-test-item-0007", "properties": {"datetime": "2011-08-17T00:00:00Z", "eo:cloud_cover": 59}}, {"id": "pgstac-test-item-0008", "properties": {"datetime": "2011-08-17T00:00:00Z", "eo:cloud_cover": 64}}, {"id": "pgstac-test-item-0009", "properties": {"datetime": "2011-08-17T00:00:00Z", "eo:cloud_cover": 61}}, {"id": "pgstac-test-item-0010", "properties": {"datetime": "2011-08-17T00:00:00Z", "eo:cloud_cover": 31}}]}'::jsonb
    $$,
    'Test basic search with fields and sort extension and prev token'
);

SELECT results_eq($$
    select s from search('{"datetime":"2011-08-16T00:00:00Z/2011-08-17T00:00:00Z", "fields":{"include":["id","datetime","eo:cloud_cover"]},"sortby":[{"field":"datetime","direction":"desc"},{"field":"id","direction":"asc"}]}') s;
    $$,$$
    select '{"next": "pgstac-test-item-0016", "prev": null, "type": "FeatureCollection", "context": {"limit": 10, "matched": 57, "returned": 10}, "features": [{"id": "pgstac-test-item-0007", "properties": {"datetime": "2011-08-17T00:00:00Z", "eo:cloud_cover": 59}}, {"id": "pgstac-test-item-0008", "properties": {"datetime": "2011-08-17T00:00:00Z", "eo:cloud_cover": 64}}, {"id": "pgstac-test-item-0009", "properties": {"datetime": "2011-08-17T00:00:00Z", "eo:cloud_cover": 61}}, {"id": "pgstac-test-item-0010", "properties": {"datetime": "2011-08-17T00:00:00Z", "eo:cloud_cover": 31}}, {"id": "pgstac-test-item-0011", "properties": {"datetime": "2011-08-17T00:00:00Z", "eo:cloud_cover": 41}}, {"id": "pgstac-test-item-0012", "properties": {"datetime": "2011-08-17T00:00:00Z", "eo:cloud_cover": 4}}, {"id": "pgstac-test-item-0013", "properties": {"datetime": "2011-08-17T00:00:00Z", "eo:cloud_cover": 2}}, {"id": "pgstac-test-item-0014", "properties": {"datetime": "2011-08-16T00:00:00Z", "eo:cloud_cover": 17}}, {"id": "pgstac-test-item-0015", "properties": {"datetime": "2011-08-16T00:00:00Z", "eo:cloud_cover": 54}}, {"id": "pgstac-test-item-0016", "properties": {"datetime": "2011-08-16T00:00:00Z", "eo:cloud_cover": 13}}]}'::jsonb
    $$,
    'Test datetime search with datetime as / separated string'
);


SELECT results_eq($$
    select s from search('{"datetime":["2011-08-16T00:00:00Z","2011-08-17T00:00:00Z"], "fields":{"include":["id","datetime","eo:cloud_cover"]},"sortby":[{"field":"datetime","direction":"desc"},{"field":"id","direction":"asc"}]}') s;
    $$,$$
    select '{"next": "pgstac-test-item-0016", "prev": null, "type": "FeatureCollection", "context": {"limit": 10, "matched": 57, "returned": 10}, "features": [{"id": "pgstac-test-item-0007", "properties": {"datetime": "2011-08-17T00:00:00Z", "eo:cloud_cover": 59}}, {"id": "pgstac-test-item-0008", "properties": {"datetime": "2011-08-17T00:00:00Z", "eo:cloud_cover": 64}}, {"id": "pgstac-test-item-0009", "properties": {"datetime": "2011-08-17T00:00:00Z", "eo:cloud_cover": 61}}, {"id": "pgstac-test-item-0010", "properties": {"datetime": "2011-08-17T00:00:00Z", "eo:cloud_cover": 31}}, {"id": "pgstac-test-item-0011", "properties": {"datetime": "2011-08-17T00:00:00Z", "eo:cloud_cover": 41}}, {"id": "pgstac-test-item-0012", "properties": {"datetime": "2011-08-17T00:00:00Z", "eo:cloud_cover": 4}}, {"id": "pgstac-test-item-0013", "properties": {"datetime": "2011-08-17T00:00:00Z", "eo:cloud_cover": 2}}, {"id": "pgstac-test-item-0014", "properties": {"datetime": "2011-08-16T00:00:00Z", "eo:cloud_cover": 17}}, {"id": "pgstac-test-item-0015", "properties": {"datetime": "2011-08-16T00:00:00Z", "eo:cloud_cover": 54}}, {"id": "pgstac-test-item-0016", "properties": {"datetime": "2011-08-16T00:00:00Z", "eo:cloud_cover": 13}}]}'::jsonb
    $$,
    'Test datetime search with datetime as array'
);

SELECT results_eq($$
    select s from search('{"filter":{"anyinteracts":[{"property":"datetime"},["2011-08-16T00:00:00Z","2011-08-17T00:00:00Z"]]}, "fields":{"include":["id","datetime","eo:cloud_cover"]},"sortby":[{"field":"datetime","direction":"desc"},{"field":"id","direction":"asc"}]}') s;
    $$,$$
    select '{"next": "pgstac-test-item-0016", "prev": null, "type": "FeatureCollection", "context": {"limit": 10, "matched": 57, "returned": 10}, "features": [{"id": "pgstac-test-item-0007", "properties": {"datetime": "2011-08-17T00:00:00Z", "eo:cloud_cover": 59}}, {"id": "pgstac-test-item-0008", "properties": {"datetime": "2011-08-17T00:00:00Z", "eo:cloud_cover": 64}}, {"id": "pgstac-test-item-0009", "properties": {"datetime": "2011-08-17T00:00:00Z", "eo:cloud_cover": 61}}, {"id": "pgstac-test-item-0010", "properties": {"datetime": "2011-08-17T00:00:00Z", "eo:cloud_cover": 31}}, {"id": "pgstac-test-item-0011", "properties": {"datetime": "2011-08-17T00:00:00Z", "eo:cloud_cover": 41}}, {"id": "pgstac-test-item-0012", "properties": {"datetime": "2011-08-17T00:00:00Z", "eo:cloud_cover": 4}}, {"id": "pgstac-test-item-0013", "properties": {"datetime": "2011-08-17T00:00:00Z", "eo:cloud_cover": 2}}, {"id": "pgstac-test-item-0014", "properties": {"datetime": "2011-08-16T00:00:00Z", "eo:cloud_cover": 17}}, {"id": "pgstac-test-item-0015", "properties": {"datetime": "2011-08-16T00:00:00Z", "eo:cloud_cover": 54}}, {"id": "pgstac-test-item-0016", "properties": {"datetime": "2011-08-16T00:00:00Z", "eo:cloud_cover": 13}}]}'::jsonb
    $$,
    'Test datetime as an anyinteracts filter'
);

SELECT results_eq($$
    select s from search('{"filter":{"eq":[{"property":"eo:cloud_cover"},36]}, "fields":{"include":["id","datetime","eo:cloud_cover"]},"sortby":[{"field":"datetime","direction":"desc"},{"field":"id","direction":"asc"}]}') s;
    $$,$$
    select '{"next": null, "prev": null, "type": "FeatureCollection", "context": {"limit": 10, "matched": 2, "returned": 2}, "features": [{"id": "pgstac-test-item-0087", "properties": {"datetime": "2011-08-01T00:00:00Z", "eo:cloud_cover": 36}}, {"id": "pgstac-test-item-0089", "properties": {"datetime": "2011-07-31T00:00:00Z", "eo:cloud_cover": 36}}]}'::jsonb
    $$,
    'Test equality as a filter on a numeric field'
);

SELECT results_eq($$
    select s from search('{"filter":{"lt":[{"property":"eo:cloud_cover"},25]}, "fields":{"include":["id","datetime","eo:cloud_cover"]},"sortby":[{"field":"eo:cloud_cover","direction":"asc"}]}') s;
    $$,$$
    select '{"next": "pgstac-test-item-0012", "prev": null, "type": "FeatureCollection", "context": {"limit": 10, "matched": 31, "returned": 10}, "features": [{"id": "pgstac-test-item-0097", "properties": {"datetime": "2011-07-31T00:00:00Z", "eo:cloud_cover": 1}}, {"id": "pgstac-test-item-0063", "properties": {"datetime": "2011-08-16T00:00:00Z", "eo:cloud_cover": 2}}, {"id": "pgstac-test-item-0013", "properties": {"datetime": "2011-08-17T00:00:00Z", "eo:cloud_cover": 2}}, {"id": "pgstac-test-item-0085", "properties": {"datetime": "2011-08-01T00:00:00Z", "eo:cloud_cover": 3}}, {"id": "pgstac-test-item-0073", "properties": {"datetime": "2011-08-15T00:00:00Z", "eo:cloud_cover": 3}}, {"id": "pgstac-test-item-0041", "properties": {"datetime": "2011-08-16T00:00:00Z", "eo:cloud_cover": 3}}, {"id": "pgstac-test-item-0034", "properties": {"datetime": "2011-08-16T00:00:00Z", "eo:cloud_cover": 3}}, {"id": "pgstac-test-item-0005", "properties": {"datetime": "2011-08-24T00:00:00Z", "eo:cloud_cover": 3}}, {"id": "pgstac-test-item-0048", "properties": {"datetime": "2011-08-16T00:00:00Z", "eo:cloud_cover": 4}}, {"id": "pgstac-test-item-0012", "properties": {"datetime": "2011-08-17T00:00:00Z", "eo:cloud_cover": 4}}]}'::jsonb
    $$,
    'Test lt as a filter on a numeric field with order by'
);

SELECT results_eq($$
    select s from search('{"ids":["pgstac-test-item-0097"],"fields":{"include":["id"]}}') s;
    $$,$$
    select '{"next": null, "prev": null, "type": "FeatureCollection", "context": {"limit": 10, "matched": 1, "returned": 1}, "features": [{"id": "pgstac-test-item-0097"}]}'::jsonb
    $$,
    'Test ids search single'
);

SELECT results_eq($$
    select s from search('{"ids":["pgstac-test-item-0097","pgstac-test-item-0003"],"fields":{"include":["id"]}}') s;
    $$,$$
    select '{"next": null, "prev": null, "type": "FeatureCollection", "context": {"limit": 10, "matched": 2, "returned": 2}, "features": [{"id": "pgstac-test-item-0003"},{"id": "pgstac-test-item-0097"}]}'::jsonb
    $$,
    'Test ids search multi'
);


SELECT results_eq($$
    select s from search('{"collections":["pgstac-test-collection"],"fields":{"include":["id"]}, "limit": 1}') s;
    $$,$$
    select '{"next": "pgstac-test-item-0003", "prev": null, "type": "FeatureCollection", "context": {"limit": 1, "matched": 100, "returned": 1}, "features": [{"id": "pgstac-test-item-0003"}]}'::jsonb
    $$,
    'Test collections search'
);

SELECT results_eq($$
    select s from search('{"collections":["something"]}') s;
    $$,$$
    select '{"next": null, "prev": null, "type": "FeatureCollection", "context": {"limit": 10, "matched": 0, "returned": 0}, "features": []}'::jsonb
    $$,
    'Test collections search with unknow collection'
);

SELECT results_eq($$
    select s from search('{"collections":["something"],"fields":{"include":["id"]}}') s;
    $$,$$
    select '{"next": null, "prev": null, "type": "FeatureCollection", "context": {"limit": 10, "matched": 0, "returned": 0}, "features": []}'::jsonb
    $$,
    'Test collections search return empty feature not null'
);

SELECT has_function('pgstac'::name, 'search_query', ARRAY['jsonb','boolean','jsonb']);

SELECT results_eq($$
    select hash from search_query('{"collections":["pgstac-test-collection"]}') s;
    $$,$$
    select '2bbae9a0ef0bbb5ffaca06603ce621d7'
    $$,
    'Test search_query to return valid hash'
);

SELECT results_eq($$
    select search from search_query('{"collections":["pgstac-test-collection"]}') s;
    $$,$$
    select '{"collections":["pgstac-test-collection"]}'::jsonb
    $$,
    'Test search_query to return valid search'
);

-- CQL 2 Tests from examples at https://github.com/radiantearth/stac-api-spec/blob/f5da775080ff3ff46d454c2888b6e796ee956faf/fragments/filter/README.md

SET pgstac."default-filter-lang" TO 'cql2-json';

SELECT results_eq($$
    SELECT BTRIM(cql2_query($q$
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
    ( (id = 'LC08_L1TP_060247_20180905_20180912_01_T1_L1TP') and (collection_id = 'landsat8_l1tp') )
    $r$,E' \n');
    $$, 'Test Example 1'
);


SELECT results_eq($$
    SELECT BTRIM(cql2_query($q$
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
                    "args": [ { "property": "datetime" }, "2021-04-08T04:39:23Z" ]
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
    ( (collection_id = 'landsat8_l1tp') and ( properties->>'eo:cloud_cover'  <= '10') and (datetime >= '2021-04-08T04:39:23Z') and st_intersects(geometry, '0103000020E6100000010000000B000000894160E5D0CA4540ED9E3C2CD4E253C0849ECDAACFCD4540B37BF2B050DF53C038F8C264AAC8454076E09C11A5DD53C0F5DBD78173CE454085EB51B81ED953C08126C286A7CF4540789CA223B9D453C0C0EC9E3C2CD4454063EE5A423ED453C004560E2DB2E5454001DE02098AC753C063EE5A423EE84540C442AD69DEC953C02FDD240681ED454034A2B437F8CA53C08048BF7D1DE0454037894160E5E853C0894160E5D0CA4540ED9E3C2CD4E253C0'::geometry) )
    $r$,E' \n');
    $$, 'Test Example 2'
);


SELECT results_eq($$
    SELECT BTRIM(cql2_query($q$
        {
            "filter-lang": "cql2-json",
            "filter": {
                "op": "and",
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
    ( ( properties->>'sentinel:data_coverage'  > '50'::numeric) and ( properties->>'eo:cloud_cover'  < '10'::numeric) )
    $r$,E' \n');
    $$, 'Test Example 3'
);



SELECT results_eq($$
    SELECT BTRIM(cql2_query($q$
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
    ( ( properties->>'sentinel:data_coverage'  > '50'::numeric) or ( properties->>'eo:cloud_cover'  < '10'::numeric) )
    $r$,E' \n');
    $$, 'Test Example 4'
);



SELECT results_eq($$
    SELECT BTRIM(cql2_query($q$
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
    ( properties->>'prop1'  =  properties->>'prop2' )
    $r$,E' \n');
    $$, 'Test Example 5'
);


SELECT results_eq($$
    SELECT BTRIM(cql2_query($q$
       {
            "filter-lang": "cql2-json",
            "filter": {
                "op": "t_intersects",
                "args": [
                { "property": "datetime" },
                [ "2020-11-11T00:00:00Z", "2020-11-12T00:00:00Z"]
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
    SELECT BTRIM(cql2_query($q$
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
    SELECT BTRIM(cql2_query($q$
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
    ( st_intersects(geometry, '0103000020E61000000100000005000000304CA60A464553C014D044D8F06443403E7958A8354153C014D044D8F06443403E7958A8354153C0DE718A8EE46A4340304CA60A464553C0DE718A8EE46A4340304CA60A464553C014D044D8F0644340'::geometry) or st_intersects(geometry, '0103000020E61000000100000005000000448B6CE7FBC553C014D044D8F064434060E5D022DBC153C014D044D8F064434060E5D022DBC153C0DE718A8EE46A4340448B6CE7FBC553C0DE718A8EE46A4340448B6CE7FBC553C014D044D8F0644340'::geometry) )
    $r$,E' \n');
    $$, 'Test Example 8'
);



SELECT results_eq($$
    SELECT BTRIM(cql2_query($q$
        {
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
    ( ( properties->>'sentinel:data_coverage'  >= '50'::numeric) or ( properties->>'landsat:coverage_percent'  >= '50'::numeric) or  ( ( properties->>'sentinel:data_coverage'  IS NULL) and ( properties->>'landsat:coverage_percent'  IS NULL) )  )
    $r$,E' \n');
    $$, 'Test Example 9'
);


/* template
SELECT results_eq($$

    $$,$$

    $$,
    'Test that ...'
);
*/
