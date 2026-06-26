
SET pgstac.context TO 'on';
SET pgstac."default_filter_lang" TO 'cql2-json';

-- collwalk: page collection_search forward `npages` times, each step following the real keyset
-- `next` token from the previous page's links; returns each page's collection ids. Exercises real
-- keyset tokens for collection paging (no hard-coded token values).
CREATE OR REPLACE FUNCTION pg_temp.collwalk(basesearch jsonb, npages int)
    RETURNS TABLE(page int, ids jsonb) AS $fn$
DECLARE s jsonb := basesearch; res jsonb; tok text; i int;
BEGIN
    FOR i IN 1..npages LOOP
        res := collection_search(s)::jsonb;
        page := i;
        ids := jsonb_path_query_array(res, '$.collections[*].id');
        RETURN NEXT;
        tok := split_part(jsonb_path_query_first(res, '$.links[*] ? (@.rel == "next").href')->>0, 'token=', 2);
        EXIT WHEN tok IS NULL OR tok = '';
        s := basesearch || jsonb_build_object('token', tok);
    END LOOP;
END; $fn$ LANGUAGE plpgsql;

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
-- count(*) wrapper so the 648 void create_collection() results don't emit 648 noise rows.
SELECT count(*) AS collections_created FROM (
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
    FROM t1
) _created;

select collection_search('{"ids":["testcollection_1","testcollection_2"],"limit":10, "sortby":[{"field":"id","direction":"desc"}]}');

select collection_search('{"ids":["testcollection_1","testcollection_2"],"limit":10, "sortby":[{"field":"id","direction":"asc"}]}');

select collection_search('{"ids":["testcollection_1","testcollection_2","testcollection_3"],"limit":1, "sortby":[{"field":"id","direction":"desc"}]}');

-- keyset paging (limit 1) across the two ids, following the real next-token each step.
SELECT page, ids FROM pg_temp.collwalk('{"ids":["testcollection_1","testcollection_2"],"limit":1, "sortby":[{"field":"datetime","direction":"desc"}]}'::jsonb, 3);

select collection_search('{"filter":{"op":"eq", "args":[{"property":"title"},"My Test Collection."]},"limit":10, "sortby":[{"field":"datetime","direction":"desc"}]}');

select collection_search('{"datetime":["2012-01-01","2012-01-02"], "filter":{"op":"eq", "args":[{"property":"title"},"My Test Collection."]},"limit":10, "sortby":[{"field":"datetime","direction":"desc"}]}');

select collection_search('{"ids":["testcollection_1","testcollection_2"], "fields": {"include": ["title"]}}');
