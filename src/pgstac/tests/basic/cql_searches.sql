SET pgstac."default_filter_lang" TO 'cql-json';

-- pagewalk: page forward `npages` times, each step following the real keyset `next` token taken
-- from the previous page's links; returns each page's feature ids. prev_roundtrip: fetch page 1,
-- follow its `next` token to page 2, then follow page 2's `prev` token and confirm it returns
-- page 1's ids. These exercise real keyset tokens end to end (no hard-coded token values).
CREATE OR REPLACE FUNCTION pg_temp.pagewalk(basesearch jsonb, npages int)
    RETURNS TABLE(page int, ids jsonb) AS $fn$
DECLARE s jsonb := basesearch; res jsonb; tok text; i int;
BEGIN
    FOR i IN 1..npages LOOP
        res := search(s)::jsonb;
        page := i;
        ids := jsonb_path_query_array(res, '$.features[*].id');
        RETURN NEXT;
        tok := split_part(jsonb_path_query_first(res, '$.links[*] ? (@.rel == "next").href')->>0, 'token=', 2);
        EXIT WHEN tok IS NULL OR tok = '';
        s := basesearch || jsonb_build_object('token', tok);
    END LOOP;
END; $fn$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pg_temp.prev_roundtrip(basesearch jsonb) RETURNS boolean AS $fn$
DECLARE p1 jsonb; p2 jsonb; pback jsonb; nexttok text; prevtok text;
BEGIN
    p1 := search(basesearch)::jsonb;
    nexttok := split_part(jsonb_path_query_first(p1, '$.links[*] ? (@.rel == "next").href')->>0, 'token=', 2);
    p2 := search(basesearch || jsonb_build_object('token', nexttok))::jsonb;
    prevtok := split_part(jsonb_path_query_first(p2, '$.links[*] ? (@.rel == "prev").href')->>0, 'token=', 2);
    pback := search(basesearch || jsonb_build_object('token', prevtok))::jsonb;
    RETURN jsonb_path_query_array(pback, '$.features[*].id') = jsonb_path_query_array(p1, '$.features[*].id');
END; $fn$ LANGUAGE plpgsql;

SELECT search('{"fields":{"include":["id","properties.datetime","properties.eo:cloud_cover"]},"sortby":[{"field":"datetime","direction":"desc"},{"field":"id","direction":"asc"}]}');

-- Test Paging: forward walk with real keyset next-tokens (limit 3), then a prev round-trip.
SELECT page, ids FROM pg_temp.pagewalk('{"limit":3,"fields":{"include":["id"]},"sortby":[{"field":"datetime","direction":"desc"},{"field":"id","direction":"asc"}]}'::jsonb, 4);

SELECT pg_temp.prev_roundtrip('{"limit":3,"fields":{"include":["id"]},"sortby":[{"field":"datetime","direction":"desc"},{"field":"id","direction":"asc"}]}'::jsonb) AS prev_returns_to_page1;
--

SELECT search('{"datetime":"2011-08-16T00:00:00Z/2011-08-17T00:00:00Z", "fields":{"include":["id","properties.datetime"]},"sortby":[{"field":"datetime","direction":"desc"},{"field":"id","direction":"asc"}]}');

SELECT search('{"datetime":["2011-08-16T00:00:00Z","2011-08-17T00:00:00Z"], "fields":{"include":["id","properties.datetime"]},"sortby":[{"field":"datetime","direction":"desc"},{"field":"id","direction":"asc"}]}');

SELECT search('{"filter":{"anyinteracts":[{"property":"datetime"},["2011-08-16T00:00:00Z","2011-08-17T00:00:00Z"]]}, "fields":{"include":["id","properties.datetime"]},"sortby":[{"field":"datetime","direction":"desc"},{"field":"id","direction":"asc"}]}');

SELECT search('{"filter":{"eq":[{"property":"eo:cloud_cover"},36]}, "fields":{"include":["id","properties.datetime","properties.eo:cloud_cover"]},"sortby":[{"field":"datetime","direction":"desc"},{"field":"id","direction":"asc"}]}');

SELECT search('{"filter":{"lt":[{"property":"eo:cloud_cover"},25]}, "fields":{"include":["id","properties.datetime","properties.eo:cloud_cover"]},"sortby":[{"field":"eo:cloud_cover","direction":"asc"}]}');

SELECT search('{"ids":["pgstac-test-item-0097"],"fields":{"include":["id"]}}');

SELECT search('{"ids":["pgstac-test-item-0097","pgstac-test-item-0003"],"fields":{"include":["id"]}}');

SELECT search('{"ids":["bogusid"],"fields":{"include":["id"]}}');

SELECT search('{"collections":["pgstac-test-collection"],"fields":{"include":["id"]}, "limit": 1}');

SELECT search('{"collections":["something"]}');

SELECT search('{"collections":["something"],"fields":{"include":["id"]}}');

SELECT s.usecount IS NOT NULL and s.usecount > 0 AND s.lastused IS NOT NULL AND s.lastused < clock_timestamp() FROM search_query(jsonb_build_object('collections',ARRAY[random()::text])) q JOIN searches s ON s.hash = q.hash;

SELECT s.hash, s.search, s._where, s.orderby, s.metadata from search_query('{"collections":["pgstac-test-collection"]}'::jsonb, _metadata=>'{"meta":"value"}'::jsonb) q JOIN searches s ON s.hash = q.hash;

SELECT s.hash, s.search, s._where, s.orderby, s.metadata from search_query('{"collections":["pgstac-test-collection"]}'::jsonb, _metadata=>'{"meta":"value"}'::jsonb) q JOIN searches s ON s.hash = q.hash;

SELECT s.usecount IS NOT NULL and s.usecount > 0 AND s.lastused IS NOT NULL AND s.lastused < clock_timestamp() FROM search_query('{"collections":["pgstac-test-collection"]}') q JOIN searches s ON s.hash = q.hash;
