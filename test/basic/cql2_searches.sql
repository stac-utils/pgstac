SET pgstac."default-filter-lang" TO 'cql2-json';

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

SELECT search('{"filter":{"op":"aequals","args":[{"property":"proj:bbox"},[654842, 3423507, 661516, 3431125]]},"fields":{"include":["id"]}}');

SELECT search('{"filter":{"op":"aequals","args":[[654842, 3423507, 661516, 3431125],{"property":"proj:bbox"}]},"fields":{"include":["id"]}}');

SELECT search('{"filter":{"op":"aoverlaps","args":[{"property":"proj:bbox"},[654842, 3423507, 661516, 12345]]},"fields":{"include":["id"]}}');

SELECT search('{"filter":{"op":"acontains","args":[{"property":"proj:bbox"},[654842, 3423507, 661516]]},"fields":{"include":["id"]}}');

SELECT search('{"filter":{"op":"contained by","args":[{"property":"proj:bbox"},[654842, 3423507, 661516]]},"fields":{"include":["id"]}}');

SELECT search('{"filter":{"op":"contained by","args":[{"property":"proj:bbox"},[654842, 3423507, 661516, 3431125, 234324]]},"fields":{"include":["id"]}}');
