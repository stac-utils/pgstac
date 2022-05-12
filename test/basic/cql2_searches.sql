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
