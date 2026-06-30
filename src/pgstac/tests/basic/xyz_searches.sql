SET pgstac."default_filter_lang" TO 'cql-json';

SELECT hash from search_query('{"collections":["pgstac-test-collection"]}');

SELECT hash, search, metadata FROM search_fromhash('c3b21deccc8f8f20d4319c36dbaaf26dc679094a531fc4f6f27367a3a2524c1a');

SELECT xyzsearch(8615, 13418, 15, 'c3b21deccc8f8f20d4319c36dbaaf26dc679094a531fc4f6f27367a3a2524c1a', '{"include":["id"]}'::jsonb);

SELECT xyzsearch(1048, 1682, 12, 'c3b21deccc8f8f20d4319c36dbaaf26dc679094a531fc4f6f27367a3a2524c1a', '{"include":["id"]}'::jsonb);

SELECT xyzsearch(1048, 1682, 12, 'c3b21deccc8f8f20d4319c36dbaaf26dc679094a531fc4f6f27367a3a2524c1a', '{"include":["id"]}'::jsonb, NULL, 1);

SELECT xyzsearch(16792, 26892, 16, 'c3b21deccc8f8f20d4319c36dbaaf26dc679094a531fc4f6f27367a3a2524c1a', '{"include":["id"]}'::jsonb, exitwhenfull => true);

SELECT xyzsearch(16792, 26892, 16, 'c3b21deccc8f8f20d4319c36dbaaf26dc679094a531fc4f6f27367a3a2524c1a', '{"include":["id"]}'::jsonb, exitwhenfull => false, skipcovered => false);

SELECT geojsonsearch('{"type": "Point","coordinates": [-87.75608539581299,30.692471153735646]}', 'c3b21deccc8f8f20d4319c36dbaaf26dc679094a531fc4f6f27367a3a2524c1a', '{"include":["id"]}'::jsonb, exitwhenfull => true, skipcovered => true);

SELECT geojsonsearch('{"type": "Point","coordinates": [-87.75608539581299,30.692471153735646]}', 'c3b21deccc8f8f20d4319c36dbaaf26dc679094a531fc4f6f27367a3a2524c1a', '{"include":["id"]}'::jsonb, exitwhenfull => false, skipcovered => false) s;
