SET pgstac."default_filter_lang" TO 'cql2-json';

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

-- Test Paging
SELECT search('{"fields":{"include":["id","properties.datetime","properties.eo:cloud_cover"]},"sortby":[{"field":"properties.eo:cloud_cover","direction":"asc"},{"field":"datetime","direction":"desc"},{"field":"id","direction":"asc"}]}');

SELECT search('{"token":"next:pgstac-test-item-0048", "fields":{"include":["id","properties.datetime","properties.eo:cloud_cover"]},"sortby":[{"field":"properties.eo:cloud_cover","direction":"asc"},{"field":"datetime","direction":"desc"},{"field":"id","direction":"asc"}]}');

SELECT search('{"token":"next:pgstac-test-item-0016", "fields":{"include":["id","properties.datetime","properties.eo:cloud_cover"]},"sortby":[{"field":"properties.eo:cloud_cover","direction":"asc"},{"field":"datetime","direction":"desc"},{"field":"id","direction":"asc"}]}');

SELECT search('{"token":"prev:pgstac-test-item-0030", "fields":{"include":["id","properties.datetime","properties.eo:cloud_cover"]},"sortby":[{"field":"properties.eo:cloud_cover","direction":"asc"},{"field":"datetime","direction":"desc"},{"field":"id","direction":"asc"}]}');

SELECT search('{"token":"prev:pgstac-test-item-0054", "fields":{"include":["id","properties.datetime","properties.eo:cloud_cover"]},"sortby":[{"field":"properties.eo:cloud_cover","direction":"asc"},{"field":"datetime","direction":"desc"},{"field":"id","direction":"asc"}]}');

-- Test paging without fields extension
SELECT jsonb_path_query(search('{"fields":{"include":["id","properties.datetime","properties.eo:cloud_cover"]},"sortby":[{"field":"properties.eo:cloud_cover","direction":"asc"},{"field":"datetime","direction":"desc"},{"field":"id","direction":"asc"}]}'), '$.features[*].id');

SELECT jsonb_path_query(search('{"token":"next:pgstac-test-item-0048","sortby":[{"field":"properties.eo:cloud_cover","direction":"asc"},{"field":"datetime","direction":"desc"},{"field":"id","direction":"asc"}]}'), '$.features[*].id');

SELECT jsonb_path_query(search('{"token":"next:pgstac-test-item-0016","sortby":[{"field":"properties.eo:cloud_cover","direction":"asc"},{"field":"datetime","direction":"desc"},{"field":"id","direction":"asc"}]}'), '$.features[*].id');

SELECT jsonb_path_query(search('{"token":"prev:pgstac-test-item-0030","sortby":[{"field":"properties.eo:cloud_cover","direction":"asc"},{"field":"datetime","direction":"desc"},{"field":"id","direction":"asc"}]}'), '$.features[*].id');

SELECT jsonb_path_query(search('{"token":"prev:pgstac-test-item-0054","sortby":[{"field":"properties.eo:cloud_cover","direction":"asc"},{"field":"datetime","direction":"desc"},{"field":"id","direction":"asc"}]}'), '$.features[*].id');

SELECT search('{"collections": ["pgstac-test-collection"], "limit": 1}');
SELECT search('{"collections": ["pgstac-test-collection"], "limit": 1, "token": "next:pgstac-test-item-0001"}');
