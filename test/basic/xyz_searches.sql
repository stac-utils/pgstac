SET pgstac."default-filter-lang" TO 'cql-json';

SELECT hash from search_query('{"collections":["pgstac-test-collection"]}');

SELECT xyzsearch(8615, 13418, 15, '2bbae9a0ef0bbb5ffaca06603ce621d7', '{"include":["id"]}'::jsonb);

SELECT xyzsearch(1048, 1682, 12, '2bbae9a0ef0bbb5ffaca06603ce621d7', '{"include":["id"]}'::jsonb);

SELECT xyzsearch(1048, 1682, 12, '2bbae9a0ef0bbb5ffaca06603ce621d7', '{"include":["id"]}'::jsonb, NULL, 1);

SELECT xyzsearch(16792, 26892, 16, '2bbae9a0ef0bbb5ffaca06603ce621d7', '{"include":["id"]}'::jsonb, exitwhenfull => true);

SELECT xyzsearch(16792, 26892, 16, '2bbae9a0ef0bbb5ffaca06603ce621d7', '{"include":["id"]}'::jsonb, exitwhenfull => false, skipcovered => false);
