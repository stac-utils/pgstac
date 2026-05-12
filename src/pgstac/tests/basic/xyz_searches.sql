SET pgstac."default_filter_lang" TO 'cql-json';

SELECT hash from search_query('{"collections":["pgstac-test-collection"]}');

SELECT hash, search, metadata FROM search_fromhash('fd8daf2e208762fc3eedb83e8a9213421c7372bbd23723f31f51d18330f0bec7');

SELECT xyzsearch(8615, 13418, 15, 'fd8daf2e208762fc3eedb83e8a9213421c7372bbd23723f31f51d18330f0bec7', '{"include":["id"]}'::jsonb);

SELECT xyzsearch(1048, 1682, 12, 'fd8daf2e208762fc3eedb83e8a9213421c7372bbd23723f31f51d18330f0bec7', '{"include":["id"]}'::jsonb);

SELECT xyzsearch(1048, 1682, 12, 'fd8daf2e208762fc3eedb83e8a9213421c7372bbd23723f31f51d18330f0bec7', '{"include":["id"]}'::jsonb, NULL, 1);

SELECT xyzsearch(16792, 26892, 16, 'fd8daf2e208762fc3eedb83e8a9213421c7372bbd23723f31f51d18330f0bec7', '{"include":["id"]}'::jsonb, exitwhenfull => true);

SELECT xyzsearch(16792, 26892, 16, 'fd8daf2e208762fc3eedb83e8a9213421c7372bbd23723f31f51d18330f0bec7', '{"include":["id"]}'::jsonb, exitwhenfull => false, skipcovered => false);

SELECT geojsonsearch('{"type": "Point","coordinates": [-87.75608539581299,30.692471153735646]}', 'fd8daf2e208762fc3eedb83e8a9213421c7372bbd23723f31f51d18330f0bec7', '{"include":["id"]}'::jsonb, exitwhenfull => true, skipcovered => true);

SELECT geojsonsearch('{"type": "Point","coordinates": [-87.75608539581299,30.692471153735646]}', 'fd8daf2e208762fc3eedb83e8a9213421c7372bbd23723f31f51d18330f0bec7', '{"include":["id"]}'::jsonb, exitwhenfull => false, skipcovered => false) s;
