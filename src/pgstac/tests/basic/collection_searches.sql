
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

select collection_search('{"ids":["testcollection_1","testcollection_2"],"limit":10, "sortby":[{"field":"id","direction":"desc"}]}');

select collection_search('{"ids":["testcollection_1","testcollection_2"],"limit":10, "sortby":[{"field":"id","direction":"asc"}]}');

select collection_search('{"ids":["testcollection_1","testcollection_2","testcollection_3"],"limit":1, "sortby":[{"field":"id","direction":"desc"}]}');

select collection_search('{"ids":["testcollection_1","testcollection_2"],"limit":1, "offset":10, "sortby":[{"field":"datetime","direction":"desc"}]}');

select collection_search('{"filter":{"op":"eq", "args":[{"property":"title"},"My Test Collection."]},"limit":10, "sortby":[{"field":"datetime","direction":"desc"}]}');

select collection_search('{"datetime":["2012-01-01","2012-01-02"], "filter":{"op":"eq", "args":[{"property":"title"},"My Test Collection."]},"limit":10, "sortby":[{"field":"datetime","direction":"desc"}]}');

select collection_search('{"ids":["testcollection_1","testcollection_2"], "fields": {"include": ["title"]}}');
