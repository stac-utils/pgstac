SET ROLE pgstac_read;
SET pgstac."default_filter_lang" TO 'cql2-json';

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

SELECT search('{"ids":["pgstac-test-item-0097"],"fields":{"include":["id"]}}');

SELECT search('{"ids":["pgstac-test-item-0097","pgstac-test-item-0003"],"fields":{"include":["id"]}}');


SELECT search('{"collections":["pgstac-test-collection"],"fields":{"include":["id"]}, "limit": 1}');

SELECT search('{"collections":["something"]}');

SELECT search('{"collections":["something"],"fields":{"include":["id"]}}');

SELECT search('{"filter":{"op":"t_intersects", "args":[{"property":"datetime"},"2011-08-16T00:00:00Z/2011-08-17T00:00:00Z"]}, "fields":{"include":["id","properties.datetime","properties.eo:cloud_cover"]},"sortby":[{"field":"datetime","direction":"desc"},{"field":"id","direction":"asc"}]}');

SELECT search('{"filter":{"op":"t_intersects", "args":[{"property":"datetime"},"2011-08-16T00:00:00Z/2011-08-17T00:00:00Z"]}, "fields":{"include":["id","properties.datetime","properties.eo:cloud_cover"]},"sortby":[{"field":"datetime","direction":"desc"},{"field":"id","direction":"asc"}]}');

SELECT search('{"filter":{"op":"t_intersects", "args":[{"property":"datetime"},"2011-08-16T00:00:00Z/2011-08-17T00:00:00Z"]}, "fields":{"include":["id","properties.datetime","properties.eo:cloud_cover"]},"sortby":[{"field":"datetime","direction":"desc"},{"field":"id","direction":"asc"}]}');

SELECT search('{"filter":{"op":"eq", "args":[{"property":"eo:cloud_cover"},36]}, "fields":{"include":["id","properties.datetime","properties.eo:cloud_cover"]},"sortby":[{"field":"datetime","direction":"desc"},{"field":"id","direction":"asc"}]}');

SELECT search('{"filter":{"op":"lt", "args":[{"property":"eo:cloud_cover"},25]}, "fields":{"include":["id","properties.datetime","properties.eo:cloud_cover"]},"sortby":[{"field":"eo:cloud_cover","direction":"asc"}]}');

SELECT search('{"filter":{"op":"in","args":[{"property":"id"},["pgstac-test-item-0097"]]},"fields":{"include":["id"]}}');


SELECT search('{"filter":{"op":"in","args":[{"property":"id"},["pgstac-test-item-0097","pgstac-test-item-0003"]]},"fields":{"include":["id"]}}');

SELECT search('{"filter":{"op":"in","args":[{"property":"collection"},["pgstac-test-collection"]]},"fields":{"include":["id"]}, "limit": 1}');

SELECT search('{"filter":{"op":"in","args":[{"property":"collection"},["nonexistent"]]}}');

SELECT search('{"filter":{"op":"in","args":[{"property":"collection"},["nonexistent"]]}, "conf":{"context":"off"}}');

SELECT search('{"conf": {"nohydrate": true}, "limit": 2}');

SELECT search('{"filter":{"op":"in","args":[{"property":"naip:state"},["zz","xx"]]},"fields":{"include":["id"]}}');

SELECT search('{"filter":{"op":"in","args":[{"property":"naip:year"},[2012,2013]]},"fields":{"include":["id"]}}');

SELECT search('{"filter":{"op":"a_equals","args":[{"property":"proj:bbox"},[654842, 3423507, 661516, 3431125]]},"fields":{"include":["id"]}}');

SELECT search('{"filter":{"op":"a_equals","args":[[654842, 3423507, 661516, 3431125],{"property":"proj:bbox"}]},"fields":{"include":["id"]}}');

SELECT search('{"filter":{"op":"a_equals","args":[[654842, 3423507, 661516],{"property":"proj:bbox"}]},"fields":{"include":["id"]}}');

SELECT search('{"filter":{"op":"a_overlaps","args":[{"property":"proj:bbox"},[654842, 3423507, 661516, 12345]]},"fields":{"include":["id"]}}');

SELECT search('{"filter":{"op":"a_overlaps","args":[{"property":"proj:bbox"},[12345]]},"fields":{"include":["id"]}}');

SELECT search('{"filter":{"op":"a_contains","args":[{"property":"proj:bbox"},[654842, 3423507, 661516]]},"fields":{"include":["id"]}}');

SELECT search('{"filter":{"op":"a_contains","args":[{"property":"proj:bbox"},[654842, 3423507, 661516, 12345]]},"fields":{"include":["id"]}}');

SELECT search('{"filter":{"op":"a_contained_by","args":[{"property":"proj:bbox"},[654842, 3423507, 661516]]},"fields":{"include":["id"]}}');

SELECT search('{"filter":{"op":"a_contained_by","args":[{"property":"proj:bbox"},[654842, 3423507, 661516, 3431125, 234324]]},"fields":{"include":["id"]}}');

-- Test Paging (with fields include), 3-key sort: forward walk with real keyset next-tokens, then
-- a prev round-trip.
SELECT page, ids FROM pg_temp.pagewalk('{"limit":3,"fields":{"include":["id","properties.datetime","properties.eo:cloud_cover"]},"sortby":[{"field":"properties.eo:cloud_cover","direction":"asc"},{"field":"datetime","direction":"desc"},{"field":"id","direction":"asc"}]}'::jsonb, 4);

SELECT pg_temp.prev_roundtrip('{"limit":3,"fields":{"include":["id","properties.datetime","properties.eo:cloud_cover"]},"sortby":[{"field":"properties.eo:cloud_cover","direction":"asc"},{"field":"datetime","direction":"desc"},{"field":"id","direction":"asc"}]}'::jsonb) AS prev_returns_to_page1;

-- Test paging without the fields extension: same walk, no fields filter (ids extracted from the
-- full hydrated items by pagewalk).
SELECT page, ids FROM pg_temp.pagewalk('{"limit":3,"sortby":[{"field":"properties.eo:cloud_cover","direction":"asc"},{"field":"datetime","direction":"desc"},{"field":"id","direction":"asc"}]}'::jsonb, 4);

SELECT search('{"collections": ["pgstac-test-collection"], "limit": 1}');
-- limit 1 paging: walk forward following the real keyset next-token.
SELECT page, ids FROM pg_temp.pagewalk('{"collections":["pgstac-test-collection"],"limit":1}'::jsonb, 3);
