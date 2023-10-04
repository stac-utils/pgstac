SELECT results_eq(
    $$ SELECT sort_sqlorderby('{"sortby":{"field":"properties.eo:cloud_cover"}}'); $$,
    $$ SELECT sort_sqlorderby('{"sortby":{"field":"eo:cloud_cover"}}'); $$,
    'Make sure that sortby with/without properties prefix return the same sort statement.'
);

SET pgstac."default_filter_lang" TO 'cql2-json';

SELECT results_eq(
    $$ SELECT stac_search_to_where('{"filter":{"op":"eq","args":[{"property":"eo:cloud_cover"},0]}}'); $$,
    $$ SELECT stac_search_to_where('{"filter":{"op":"eq","args":[{"property":"properties.eo:cloud_cover"},0]}}'); $$,
    'Make sure that CQL2 filter works the same with/without properties prefix.'
);

SET pgstac."default_filter_lang" TO 'cql-json';

SELECT results_eq(
    $$ SELECT stac_search_to_where('{"filter":{"eq":[{"property":"eo:cloud_cover"},0]}}'); $$,
    $$ SELECT stac_search_to_where('{"filter":{"eq":[{"property":"properties.eo:cloud_cover"},0]}}'); $$,
    'Make sure that CQL filter works the same with/without properties prefix.'
);

DELETE FROM collections WHERE id in ('pgstac-test-collection', 'pgstac-test-collection2');

SELECT results_eq(
    $$ SELECT all_collections(); $$,
    $$ SELECT '[]'::jsonb; $$,
    'Make sure all_collections returns an empty array when the collection table is empty.'
);

\copy collections (content) FROM 'tests/testdata/collections.ndjson';

SELECT results_eq(
    $$ SELECT get_queryables('pgstac-test-collection') -> 'properties' ? 'datetime'; $$,
    $$ SELECT true; $$,
    'Make sure valid schema object is returned for a existing collection.'
);

SELECT results_eq(
    $$ SELECT get_queryables('foo'); $$,
    $$ SELECT NULL::jsonb; $$,
    'Make sure null is returned for a non-existant collection.'
);

SELECT lives_ok(
    $$ DELETE FROM queryables WHERE name IN ('testqueryable', 'testqueryable2', 'testqueryable3'); $$,
    'Make sure test queryable does not exist.'
);

SELECT lives_ok(
    $$ INSERT INTO queryables (name, collection_ids) VALUES ('testqueryable', null); $$,
    'Can add a new queryable that applies to all collections.'
);

select is(
    (SELECT count(*) from collections where id = 'pgstac-test-collection'),
    '1',
    'Make sure test collection exists.'
);

SELECT lives_ok(
    $$ INSERT INTO queryables (name, collection_ids) VALUES ('testqueryable3', '{pgstac-test-collection}'); $$,
    'Can add a new queryable to a specific existing collection.'
);

SELECT throws_ok(
    $$ INSERT INTO queryables (name, collection_ids) VALUES ('testqueryable2', '{nonexistent}'); $$,
    '23503'
);

SELECT throws_ok(
    $$ INSERT INTO queryables (name, collection_ids) VALUES ('testqueryable', '{pgstac-test-collection}'); $$,
    '23505'
);

SELECT lives_ok(
    $$ UPDATE queryables SET collection_ids = '{pgstac-test-collection}' WHERE name='testqueryable'; $$,
    'Can update a queryable from null to a single collection.'
);

SET pgstac.additional_properties to 'false';

SELECT results_eq(
    $$ SELECT pgstac.additional_properties(); $$,
    $$ SELECT FALSE; $$,
    'Make sure additional_properties is set to false'
);

SELECT throws_ok(
    $$ SELECT search('{"filter": {"eq": [{"property": "xyzzy"}, "dummy"]}}'); $$,
    'Term xyzzy is not found in queryables.',
    'Make sure a term not present in the list of queryables cannot be used in a filter'
);

SELECT lives_ok(
    $$ SELECT search('{"filter": {"eq": [{"property": "datetime"}, "2020-11-11T00:00:00Z"]}}'); $$,
    'Make sure a term present in the list of queryables can be used in a filter'
);

SELECT lives_ok(
    $$ SELECT search('{"filter": {"and": [{"t_after": [{"property": "datetime"}, "2020-11-11T00:00:00"]}, {"t_before": [{"property": "datetime"}, "2022-11-11T00:00:00"]}]}}'); $$,
    'Make sure that only arguments that are properties are cheked'
);

SELECT throws_ok(
    $$ SELECT search('{"filter": {"and": [{"t_after": [{"property": "datetime"}, "2020-11-11T00:00:00"]}, {"eq": [{"property": "xyzzy"}, "dummy"]}]}}'); $$,
    'Term xyzzy is not found in queryables.',
    'Make sure a term not present in the list of queryables cannot be used in a filter with nested arguments'
);

SET pgstac.additional_properties to 'true';

SELECT results_eq(
    $$ SELECT pgstac.additional_properties(); $$,
    $$ SELECT TRUE; $$,
    'Make sure additional_properties is set to true'
);

SELECT lives_ok(
    $$ SELECT search('{"filter": {"eq": [{"property": "xyzzy"}, "dummy"]}}'); $$,
    'Make sure a term not present in the list of queryables can be used in a filter'
);

SELECT lives_ok(
    $$ SELECT search('{"filter": {"eq": [{"property": "datetime"}, "2020-11-11T00:00:00Z"]}}'); $$,
    'Make sure a term present in the list of queryables can be used in a filter'
);

RESET pgstac.additional_properties;
