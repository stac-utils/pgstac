SET pgstac."default-filter-lang" TO 'cql-json';

SELECT search('{"fields":{"include":["id","properties.datetime","properties.eo:cloud_cover"]},"sortby":[{"field":"datetime","direction":"desc"},{"field":"id","direction":"asc"}]}');

-- Test Paging
SELECT search('{"fields":{"include":["id"]},"sortby":[{"field":"datetime","direction":"desc"},{"field":"id","direction":"asc"}]}');

SELECT search('{"token":"next:pgstac-test-item-0010", "fields":{"include":["id","properties.datetime","properties.eo:cloud_cover"]},"sortby":[{"field":"datetime","direction":"desc"},{"field":"id","direction":"asc"}]}');

SELECT search('{"token":"next:pgstac-test-item-0020", "fields":{"include":["id","properties.datetime","properties.eo:cloud_cover"]},"sortby":[{"field":"datetime","direction":"desc"},{"field":"id","direction":"asc"}]}');

SELECT search('{"token":"prev:pgstac-test-item-0021", "fields":{"include":["id","properties.datetime","properties.eo:cloud_cover"]},"sortby":[{"field":"datetime","direction":"desc"},{"field":"id","direction":"asc"}]}');

SELECT search('{"token":"next:pgstac-test-item-0011", "fields":{"include":["id","properties.datetime","properties.eo:cloud_cover"]},"sortby":[{"field":"datetime","direction":"desc"},{"field":"id","direction":"asc"}]}');
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

SELECT hash from search_query('{"collections":["pgstac-test-collection"]}');

SELECT search from search_query('{"collections":["pgstac-test-collection"]}');
