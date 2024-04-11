-- CREATE fixtures for testing search - as tests are run within a transaction, these will not persist

SET pgstac.context TO 'on';
SET pgstac."default_filter_lang" TO 'cql2-json';

WITH t AS (
    SELECT
        row_number() over () as id,
        x,
        y
    FROM
        generate_series(-180, 170, 10) as x,
        generate_series(-90, 80, 10) as y
), t1 AS (
    SELECT
        concat('testcollection_', id) as id,
        x as minx,
        y as miny,
        x+10 as maxx,
        y+10 as maxy,
        '2000-01-01'::timestamptz + (concat(id, ' weeks'))::interval as sdt,
        '2000-01-01'::timestamptz + (concat(id, ' weeks'))::interval  + ('2 months')::interval as edt
    FROM t
)
SELECT
    create_collection(format($q$
        {
            "id": "%s",
            "type": "Collection",
            "title": "My Test Collection.",
            "description": "Description of my test collection.",
            "extent": {
                "spatial": {"bbox": [[%s, %s, %s, %s]]},
                "temporal": {"interval": [[%I, %I]]}
            },
            "stac_extensions":[]
        }
        $q$,
        id, minx, miny, maxx, maxy, sdt, edt
    )::jsonb)
FROM t1;

SELECT has_function('pgstac'::name, 'collection_search', ARRAY['jsonb']);


SELECT results_eq($$
    select collection_search('{"ids":["testcollection_1","testcollection_2"],"limit":1, "sortby":[{"field":"id","direction":"asc"}]}');
    $$,$$
    SELECT '{"links": [{"rel": "next", "body": {"offset": 1}, "href": "./collections", "type": "application/json", "merge": true, "method": "GET"}], "numberMatched": 2, "numberReturned": 1, "collections": [{"id": "testcollection_1", "type": "Collection", "title": "My Test Collection.", "extent": {"spatial": {"bbox": [[-180, -90, -170, -80]]}, "temporal": {"interval": [["2000-01-08 00:00:00+00", "2000-03-08 00:00:00+00"]]}}, "description": "Description of my test collection.", "stac_extensions": []}]}'::jsonb
    $$,
    'Test search passing in collection ids'
);


SELECT results_eq($$
    select collection_search('{"ids":["testcollection_1","testcollection_2"],"limit":1, "sortby":[{"field":"id","direction":"desc"}]}');
    $$,$$
    SELECT '{"links": [{"rel": "next", "body": {"offset": 1}, "href": "./collections", "type": "application/json", "merge": true, "method": "GET"}], "numberMatched": 2, "numberReturned": 1, "collections": [{"id": "testcollection_2", "type": "Collection", "title": "My Test Collection.", "extent": {"spatial": {"bbox": [[-170, -90, -160, -80]]}, "temporal": {"interval": [["2000-01-15 00:00:00+00", "2000-03-15 00:00:00+00"]]}}, "description": "Description of my test collection.", "stac_extensions": []}]}'::jsonb
    $$,
    'Test search passing in collection ids with descending sort'
);

SET pgstac.base_url='https://test.com/';

SELECT results_eq($$
    select collection_search('{"ids":["testcollection_1","testcollection_2"],"limit":1, "sortby":[{"field":"id","direction":"asc"}]}');
    $$,$$
    SELECT '{"links": [{"rel": "next", "body": {"offset": 1}, "href": "https://test.com/collections", "type": "application/json", "merge": true, "method": "GET"}], "numberMatched": 2, "numberReturned": 1, "collections": [{"id": "testcollection_1", "type": "Collection", "title": "My Test Collection.", "extent": {"spatial": {"bbox": [[-180, -90, -170, -80]]}, "temporal": {"interval": [["2000-01-08 00:00:00+00", "2000-03-08 00:00:00+00"]]}}, "description": "Description of my test collection.", "stac_extensions": []}]}'::jsonb
    $$,
    'Test search passing in collection ids with base_url set'
);
