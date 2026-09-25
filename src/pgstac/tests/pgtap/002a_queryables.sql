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

SELECT is(
    get_queryables(NULL::text),
    get_queryables('{pgstac-test-collection,pgstac-test-collection2}'::text[]),
    'a NULL collection reads every collection'
);

SELECT upsert_queryable('test:range', definition => '{"type":"integer","minimum":1,"maximum":5}'::jsonb, collection_ids => '{pgstac-test-collection}');
SELECT upsert_queryable('test:range', definition => '{"type":"integer","minimum":2,"maximum":9}'::jsonb, collection_ids => '{pgstac-test-collection2}');
SELECT is(
    get_queryables(NULL::text[]) -> 'properties' -> 'test:range',
    '{"type":"integer","minimum":1,"maximum":9}'::jsonb,
    'get_queryables merges minimum and maximum across the rows of a name.'
);
DELETE FROM queryables WHERE name = 'test:range';

DELETE FROM queryables WHERE name IN ('testqueryable', 'testqueryable2', 'testqueryable3');

SELECT lives_ok(
    $$ SELECT upsert_queryable('testqueryable'); $$,
    'Can add a new queryable that applies to all collections.'
);

select is(
    (SELECT count(*) from collections where id = 'pgstac-test-collection'),
    '1',
    'Make sure test collection exists.'
);

SELECT lives_ok(
    $$ SELECT upsert_queryable('testqueryable3', collection_ids => '{pgstac-test-collection}'); $$,
    'Can add a new queryable to a specific existing collection.'
);

SELECT throws_ok(
    $$ SELECT upsert_queryable('testqueryable2', collection_ids => '{nonexistent}'); $$,
    '23503'
);

SELECT lives_ok(
    $$ SELECT upsert_queryable('testqueryable', collection_ids => '{pgstac-test-collection}'); $$,
    'The row passed to upsert_queryable wins over a global row of the same name.'
);

SELECT has_function('pgstac'::name, 'upsert_queryable', ARRAY['text', 'jsonb', 'text', 'text', 'text[]', 'text[]']);
SELECT has_function('pgstac'::name, 'delete_queryable', ARRAY['text', 'text[]']);

SELECT lives_ok(
    $$ SELECT upsert_queryable('testqueryable4', property_wrapper => 'to_text'); $$,
    'upsert_queryable creates a new queryable.'
);

SELECT results_eq(
    $$ SELECT count(*)::int, min(property_wrapper) FROM queryables WHERE name = 'testqueryable4'; $$,
    $$ SELECT 1, 'to_text'::text; $$,
    'upsert_queryable created exactly one row with the given wrapper.'
);

SELECT lives_ok(
    $$ SELECT upsert_queryable('testqueryable4', property_wrapper => 'to_int'); $$,
    'upsert_queryable replaces an existing queryable.'
);

SELECT results_eq(
    $$ SELECT count(*)::int, min(property_wrapper) FROM queryables WHERE name = 'testqueryable4'; $$,
    $$ SELECT 1, 'to_int'::text; $$,
    'upsert_queryable replaced the row rather than adding one.'
);

-- The row passed to upsert_queryable replaces every row of the name it would conflict with.
SELECT lives_ok(
    $$ SELECT upsert_queryable('testqueryable4', collection_ids => '{pgstac-test-collection}'); $$,
    'a per-collection upsert replaces a global queryable of the same name.'
);

SELECT results_eq(
    $$ SELECT collection_ids FROM queryables WHERE name = 'testqueryable4'; $$,
    $$ SELECT '{pgstac-test-collection}'::text[]; $$,
    'only the per-collection row is left.'
);

SELECT lives_ok(
    $$ SELECT upsert_queryable('testqueryable4', collection_ids => '{pgstac-test-collection,pgstac-test-collection2}'); $$,
    'a per-collection upsert replaces an overlapping per-collection row.'
);

SELECT results_eq(
    $$ SELECT collection_ids FROM queryables WHERE name = 'testqueryable4'; $$,
    $$ SELECT '{pgstac-test-collection,pgstac-test-collection2}'::text[]; $$,
    'one row holds both collections.'
);

SELECT lives_ok(
    $$ SELECT upsert_queryable('testqueryable4'); $$,
    'a global upsert replaces the per-collection rows.'
);

SELECT lives_ok(
    $$ SELECT delete_queryable('testqueryable4'); $$,
    'delete_queryable removes a queryable.'
);

SELECT is(
    (SELECT count(*)::int FROM queryables WHERE name = 'testqueryable4'),
    0,
    'delete_queryable left no rows behind.'
);

SELECT throws_ok(
    $$ SELECT delete_queryable('testqueryable4'); $$,
    'P0002',
    'Queryable testqueryable4 for collections <NULL> does not exist',
    'delete_queryable raises no_data_found when nothing matched.'
);

SELECT lives_ok(
    $$ SELECT delete_queryable('testqueryable3', '{pgstac-test-collection}'); $$,
    'delete_queryable removes a per-collection queryable.'
);

SELECT has_function('pgstac'::name, 'add_collection_to_queryable', ARRAY['text', 'text']);
SELECT has_function('pgstac'::name, 'remove_collection_from_queryable', ARRAY['text', 'text']);

SELECT lives_ok(
    $$ SELECT upsert_queryable('test:member', collection_ids => '{pgstac-test-collection}');
       SELECT add_collection_to_queryable('test:member', 'pgstac-test-collection2'); $$,
    'add_collection_to_queryable adds a collection to a per-collection queryable.'
);

SELECT is(
    (SELECT collection_ids FROM queryables WHERE name = 'test:member'),
    '{pgstac-test-collection,pgstac-test-collection2}'::text[],
    'the queryable reads back with both collections.'
);

SELECT lives_ok(
    $$ SELECT add_collection_to_queryable('test:member', 'pgstac-test-collection2'); $$,
    'adding a collection that is already a member is a no-op.'
);

SELECT is(
    (SELECT collection_ids FROM queryables WHERE name = 'test:member'),
    '{pgstac-test-collection,pgstac-test-collection2}'::text[],
    'the no-op left the collections unchanged.'
);

SELECT upsert_queryable('test:global');
SELECT throws_ok(
    $$ SELECT add_collection_to_queryable('test:global', 'pgstac-test-collection'); $$,
    '23505',
    'Queryable test:global is global and already covers every collection',
    'a global queryable already covers every collection.'
);

SELECT upsert_queryable('test:ambiguous', collection_ids => '{pgstac-test-collection}');
SELECT upsert_queryable('test:ambiguous', collection_ids => '{pgstac-test-collection2}');
SELECT throws_ok(
    $$ SELECT add_collection_to_queryable('test:ambiguous', 'pgstac-test-collection'); $$,
    'P0003',
    'Queryable test:ambiguous has several per-collection rows, use upsert_queryable with the full list',
    'a name with several per-collection rows is ambiguous.'
);

SELECT lives_ok(
    $$ SELECT upsert_queryable('test:ambiguous', collection_ids => '{pgstac-test-collection,pgstac-test-collection2}'); $$,
    'upsert_queryable with the full list replaces disjoint per-collection rows.'
);

SELECT results_eq(
    $$ SELECT collection_ids FROM queryables WHERE name = 'test:ambiguous'; $$,
    $$ SELECT '{pgstac-test-collection,pgstac-test-collection2}'::text[]; $$,
    'one row of either spelling holds both collections.'
);

SELECT throws_ok(
    $$ SELECT add_collection_to_queryable('test:unknown', 'pgstac-test-collection'); $$,
    'P0002',
    'Queryable test:unknown does not exist, use upsert_queryable to create it',
    'add_collection_to_queryable raises no_data_found for an unknown queryable.'
);

SELECT throws_ok(
    $$ SELECT add_collection_to_queryable('test:member', 'nonexistent'); $$,
    '23503',
    NULL,
    'the queryables trigger rejects a collection that does not exist.'
);

SELECT lives_ok(
    $$ SELECT remove_collection_from_queryable('test:member', 'pgstac-test-collection'); $$,
    'remove_collection_from_queryable removes a collection.'
);

SELECT is(
    (SELECT collection_ids FROM queryables WHERE name = 'test:member'),
    '{pgstac-test-collection2}'::text[],
    'the removed collection is gone from the queryable.'
);

SELECT throws_ok(
    $$ SELECT remove_collection_from_queryable('test:member', 'pgstac-test-collection'); $$,
    'P0002',
    'No queryable test:member includes collection pgstac-test-collection',
    'remove_collection_from_queryable raises no_data_found for a collection that is not a member.'
);

SELECT lives_ok(
    $$ SELECT remove_collection_from_queryable('test:member', 'pgstac-test-collection2'); $$,
    'removing the last collection succeeds.'
);

SELECT is(
    (SELECT count(*)::int FROM queryables WHERE name = 'test:member'),
    0,
    'removing the last collection deleted the queryable.'
);

DELETE FROM queryables WHERE name IN ('test:global', 'test:ambiguous');

-- A name is stored without the properties. prefix and the helpers resolve either spelling.
SELECT upsert_queryable('properties.spell:x', collection_ids => '{pgstac-test-collection}');
SELECT is(
    (SELECT count(*)::int FROM queryables WHERE name = 'spell:x'),
    1,
    'a name inserted with the properties. prefix is stored without it.'
);
SELECT lives_ok(
    $$ SELECT add_collection_to_queryable('properties.spell:x', 'pgstac-test-collection2'); $$,
    'add_collection_to_queryable resolves a name spelled with the properties. prefix.'
);
SELECT is(
    (SELECT collection_ids FROM queryables WHERE name = 'spell:x'),
    '{pgstac-test-collection,pgstac-test-collection2}'::text[],
    'the collection was added to the row.'
);
SELECT lives_ok(
    $$ SELECT remove_collection_from_queryable('properties.spell:x', 'pgstac-test-collection2'); $$,
    'remove_collection_from_queryable resolves a name spelled with the properties. prefix.'
);
SELECT lives_ok(
    $$ SELECT delete_queryable('properties.spell:x', '{pgstac-test-collection}'); $$,
    'delete_queryable resolves a name spelled with the properties. prefix.'
);
SELECT is(
    (SELECT count(*)::int FROM queryables WHERE name = 'spell:x'),
    0,
    'the queryable is gone.'
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
    $$ SELECT search('{"filter": {"s_intersects": [{"property": "geometry"}, {"type": "Point", "coordinates": [0, 0]}]}}'); $$,
    'Make sure the geometry column can be used in a spatial filter'
);

SELECT lives_ok(
    $$ SELECT search('{"filter": {"and": [{"t_after": [{"property": "datetime"}, "2020-11-11T00:00:00"]}, {"t_before": [{"property": "datetime"}, "2022-11-11T00:00:00"]}]}}'); $$,
    'Make sure that only arguments that are properties are checked'
);

SELECT lives_ok(
    $$ SELECT search('{"filter": {"eq": [{"property": "start_datetime"}, "2020-11-11T00:00:00Z"]}}'); $$,
    'start_datetime is an instantiated column and is accepted when additional_properties is false'
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
    'Make sure a term present in the list of queryables can still be used in a filter with additional_properties on'
);

SELECT lives_ok(
    $$ SELECT search('{"filter": {"eq": [{"property": "start_datetime"}, "2020-11-11T00:00:00Z"]}}'); $$,
    'A start_datetime filter runs against the instantiated datetime column'
);

RESET pgstac.additional_properties;

SELECT has_function('pgstac'::name, 'queryable_path_elements'::name, ARRAY['text']);

-- Every indexed queryable has one reference index on queryable_index_template, the seeds the
-- copies of the indexes items carries, the rest built by maintain_reference_index from the row.
SELECT has_table('pgstac'::name, 'queryable_index_template'::name);
SELECT is(
    (SELECT count(*)::int FROM queryable_index_template),
    0,
    'the index template holds no rows'
);
SELECT results_eq(
    $$ SELECT name, indexdef_unnamed(pg_get_indexdef(reference_index(q)), 't') FROM queryables q WHERE name IN ('id', 'datetime', 'geometry') ORDER BY name; $$,
    $$ VALUES ('datetime', 'CREATE INDEX ON t USING btree (datetime DESC, end_datetime)'),
              ('geometry', 'CREATE INDEX ON t USING gist (geometry)'),
              ('id', 'CREATE UNIQUE INDEX ON t USING btree (id)'); $$,
    'the seed queryables reference the copies of the indexes items carries and the unique id index'
);
CREATE FUNCTION pg_temp.reference_indexdef(qname text) RETURNS text AS $$
    SELECT indexdef_unnamed(pg_get_indexdef(reference_index(q)), 't') FROM queryables q WHERE name = qname;
$$ LANGUAGE SQL;
SELECT upsert_queryable('test:ref', property_index_type => 'BTREE');
SELECT is(
    pg_temp.reference_indexdef('test:ref'),
    $i$CREATE INDEX ON t USING btree (to_text(((content -> 'properties'::text) -> 'test:ref'::text)))$i$,
    'the reference index appears on insert, on the wrapper of the name under properties'
);
SELECT is(
    (SELECT count(*)::int FROM pg_indexes WHERE tablename = 'queryable_index_template' AND indexname = (SELECT reference_index_name(q) FROM queryables q WHERE name = 'test:ref')),
    1,
    'the reference index is named after the queryable id and a hash of its definition'
);
SELECT lives_ok(
    $$ UPDATE queryables SET definition = '{"type":"number"}' WHERE name = 'test:ref'; $$,
    'a definition change is accepted'
);
SELECT is(
    pg_temp.reference_indexdef('test:ref'),
    $i$CREATE INDEX ON t USING btree (to_float(((content -> 'properties'::text) -> 'test:ref'::text)))$i$,
    'the reference index is replaced when the inferred wrapper changes'
);
UPDATE queryables SET property_index_type = 'HASH', property_wrapper = 'to_text' WHERE name = 'test:ref';
SELECT is(
    pg_temp.reference_indexdef('test:ref'),
    $i$CREATE INDEX ON t USING hash (to_text(((content -> 'properties'::text) -> 'test:ref'::text)))$i$,
    'the reference index is replaced when the index type or wrapper changes'
);
UPDATE queryables SET name = 'properties.assets.thumbnail.href' WHERE name = 'test:ref';
SELECT is(
    pg_temp.reference_indexdef('assets.thumbnail.href'),
    $i$CREATE INDEX ON t USING hash (to_text((((content -> 'assets'::text) -> 'thumbnail'::text) -> 'href'::text)))$i$,
    'a renamed queryable gets the reference index of its new keys, a STAC top-level member at the root'
);
SELECT is(
    (SELECT count(*)::int FROM pg_indexes WHERE tablename = 'queryable_index_template' AND indexname ~ '^q\d+_[0-9a-f]{8}$' AND indexname <> (SELECT reference_index_name(q) FROM queryables q WHERE name = 'eo:cloud_cover')),
    1,
    'a replaced reference index leaves nothing behind'
);
UPDATE queryables SET property_index_type = NULL WHERE name = 'assets.thumbnail.href';
SELECT is(
    pg_temp.reference_indexdef('assets.thumbnail.href'),
    NULL,
    'a queryable that no longer asks for an index has no reference index'
);
UPDATE queryables SET property_index_type = 'BTREE' WHERE name = 'assets.thumbnail.href';
SELECT delete_queryable('assets.thumbnail.href');
SELECT is(
    (SELECT count(*)::int FROM pg_indexes WHERE tablename = 'queryable_index_template' AND indexname ~ '^q\d+_[0-9a-f]{8}$'),
    1,
    'the reference index is gone on delete'
);

-- A partition has to exist for any of the queryable_indexes assertions below to mean anything:
-- it derives every row from the leaves of the partition tree, so with none it returns the empty
-- set whatever the implementation does, and is_empty passes against anything.
SELECT check_partition(
    'pgstac-test-collection',
    tstzrange('2011-08-01'::timestamptz, '2011-08-31'::timestamptz, '[]'),
    tstzrange('2011-08-01'::timestamptz, '2011-08-31'::timestamptz, '[]')
);
SELECT isnt_empty(
    $$ SELECT * FROM pg_partition_tree('items') WHERE isleaf $$,
    'the partition the reference index assertions are measured against exists'
);

-- The reference indexes are derived from the rows, so maintain_partitions repairs them first, and
-- a row whose index cannot be built is skipped with a warning by the whole-table walk.
CREATE FUNCTION pg_temp.reference_indexes() RETURNS SETOF text AS $$
    SELECT indexname::text FROM pg_indexes
    WHERE tablename = 'queryable_index_template' AND indexname ~ '^q\d+_[0-9a-f]{8}$' ORDER BY 1;
$$ LANGUAGE SQL;
ALTER TABLE queryables DISABLE TRIGGER USER;
INSERT INTO queryables (name, property_wrapper, property_index_type) VALUES ('test:legacy_gin', 'to_int', 'GIN');
UPDATE queryables SET property_wrapper = 'to_float' WHERE name = 'eo:cloud_cover';
ALTER TABLE queryables ENABLE TRIGGER USER;
SELECT is(
    (SELECT count(*)::int FROM pg_temp.reference_indexes()),
    1,
    'with the triggers off the reference indexes fall behind the rows'
);
SELECT lives_ok(
    $$ SELECT maintain_reference_index(); $$,
    'the whole-table walk survives a row whose index cannot be built'
);
SELECT results_eq(
    $$ SELECT * FROM pg_temp.reference_indexes(); $$,
    $$ SELECT reference_index_name(q) FROM queryables q WHERE name = 'eo:cloud_cover'; $$,
    'the walk replaces the stale reference index and leaves the unbuildable row un-indexed'
);
SELECT throws_ok(
    $$ UPDATE queryables SET definition = '{"type":"integer"}' WHERE name = 'test:legacy_gin'; $$,
    '42704',
    NULL,
    'a write to the unbuildable row still fails'
);
DELETE FROM queryables WHERE name = 'test:legacy_gin';
-- The walk maintains the template, not the partitions. With a partition in the tree what is
-- pending is exactly the partition side of the change: the index the rewritten queryable now
-- wants, and the one it wanted before, left behind as an orphan.
SELECT results_eq(
    $$ SELECT field, queryable_id IS NOT NULL AS wanted
       FROM queryable_indexes('items', true) ORDER BY wanted $$,
    $$ VALUES (NULL::text, false), ('eo:cloud_cover'::text, true) $$,
    'the walk fixes the template only, leaving the partition an index to build and one to drop'
);
UPDATE queryables SET property_wrapper = 'to_int' WHERE name = 'eo:cloud_cover';
SELECT is(
    (SELECT count(*)::int FROM pg_temp.reference_indexes()),
    1,
    'the row trigger replaces the reference index in place'
);
DO $$ BEGIN EXECUTE format('DROP INDEX %I', (SELECT * FROM pg_temp.reference_indexes())); END $$;
DO $$ BEGIN PERFORM * FROM queryable_indexes('items', true); END $$;
SELECT is(
    (SELECT count(*)::int FROM pg_temp.reference_indexes()),
    0,
    'reading queryable_indexes does not rebuild a dropped reference index'
);
SELECT maintain_partitions();
SELECT is_empty(
    $$ SELECT * FROM queryable_indexes('items', true); $$,
    'a dropped reference index is rebuilt by maintain_partitions before the partitions are compared'
);
SELECT is(
    (SELECT count(*)::int FROM pg_temp.reference_indexes()),
    1,
    'the reference index is back'
);

SELECT throws_ok(
    $$ SELECT upsert_queryable('datetime', property_index_type => 'BTREE'); $$,
    '23514',
    'datetime is read as a column of items, which items indexes itself.',
    'a queryable named for a column of items cannot ask for an index: items carries it'
);

SELECT lives_ok(
    $$ SELECT upsert_queryable('test:num', definition => '{"type":"number"}'::jsonb); $$,
    'Can register a number queryable that carries no explicit wrapper.'
);

SELECT results_eq(
    $$ SELECT (queryable('test:num')).expression; $$,
    $$ SELECT $i$to_float(content->'properties'->'test:num')$i$::text; $$,
    'the filter expression uses the same wrapper indexdef now builds the index with'
);

SELECT results_eq(
    $$ SELECT (queryable('properties.test:num')).definition; $$,
    $$ SELECT '{"type":"number"}'::jsonb; $$,
    'queryable() returns the definition of the queryable it matched'
);

SELECT results_eq(
    $$ SELECT (queryable('datetime')).definition, (queryable('test:unregistered')).definition; $$,
    $$ SELECT NULL::jsonb, NULL::jsonb; $$,
    'queryable() returns no definition for an items column or an unregistered term'
);

SELECT results_eq(
    $$ SELECT (queryable('test:num')).registered, (queryable('datetime')).registered, (queryable('test:unregistered')).registered; $$,
    $$ SELECT true, true, false; $$,
    'queryable() reports a queryables row or an items column as registered'
);

SELECT delete_queryable('test:num');

SELECT upsert_queryable('test:date', definition => '{"type":"string","format":"date"}'::jsonb);
SELECT is(
    (SELECT (queryable('test:date')).wrapper),
    'to_tstz',
    'a date-format definition is read through to_tstz like a date-time one'
);
SELECT delete_queryable('test:date');

SELECT is(
    queryable_wrapper(NULL, '{"type":"integer"}'),
    'to_int',
    'an integer definition is read through to_int'
);
SELECT is(
    queryable_wrapper(NULL, '{"type":["number","null"]}'),
    'to_float',
    'a nullable type list resolves by its non-null member'
);
SELECT is(
    (queryable('bbox')).path,
    $p$content->'bbox'$p$,
    'bbox is read from the item root, not from properties'
);

-- PostgreSQL validates the index type by building the reference index; the error names the queryable.
SELECT throws_ok(
    $$ SELECT upsert_queryable('test:hostile', property_index_type => 'btree (id)); create table pwned(); --'); $$,
    '42704',
    'test:hostile cannot be indexed: access method "btree (id)); create table pwned(); --" does not exist',
    'a property_index_type that is not an index access method is rejected'
);
SELECT ok(to_regclass('pwned') IS NULL, 'the hostile index type did not execute');

SELECT throws_ok(
    $$ SELECT upsert_queryable('test:hostile', property_wrapper => 'pg_sleep'); $$,
    '23514',
    'pg_sleep is not in queryable_wrappers.',
    'a property_wrapper outside the pgstac wrappers is rejected'
);

SELECT set_eq(
    $$ SELECT name FROM queryable_wrappers; $$,
    ARRAY['to_int', 'to_float', 'to_tstz', 'to_text', 'to_text_array'],
    'the five pgstac wrappers are registered'
);

SELECT lives_ok(
    $$ SELECT upsert_queryable('test:wrapper_' || name, property_wrapper => name) FROM queryable_wrappers; $$,
    'the five pgstac wrappers are accepted'
);

-- property_path aliases a queryable to another list of keys under content.
SELECT lives_ok(
    $$ SELECT upsert_queryable('alias:x', property_path => '{a,"b:c"}'); $$,
    'a list of keys is accepted as a property_path'
);
SELECT is(
    (queryable('alias:x')).path,
    $p$content->'a'->'b:c'$p$,
    'the filter reads the aliased keys under content'
);
SELECT lives_ok(
    $$ UPDATE queryables SET property_path = ARRAY['properties', $k$test:it's$k$] WHERE name = 'alias:x'; $$,
    'a key containing a quote is accepted on update'
);
SELECT is(
    (queryable('alias:x')).path,
    $p$content->'properties'->'test:it''s'$p$,
    'a key containing a quote is quoted for the filter'
);
SELECT throws_ok(
    format($$ UPDATE queryables SET property_path = %L WHERE name = 'alias:x'; $$, p),
    '23514',
    NULL,
    format('%s is rejected as a property_path', p)
)
FROM unnest(ARRAY['{}', '{a,""}', '{a,NULL}', '{{a,b}}']) p;
SELECT delete_queryable('alias:x');

SELECT is_empty(
    $$ SELECT * FROM missing_queryables('pgstac-test-collection2'); $$,
    'missing_queryables returns nothing for a collection that has no partition yet'
);

-- An item gives pgstac-test-collection a partition, so the accepted index types are proven by building them.
SELECT create_item('{"id": "pgstac-test-queryables-item", "type": "Feature", "stac_version": "1.0.0", "collection": "pgstac-test-collection", "bbox": [0, 0, 1, 1], "links": [], "geometry": {"type": "Polygon", "coordinates": [[[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]]}, "assets": {}, "properties": {"datetime": "2011-06-01T00:00:00Z", "test:astext": "a", "test:back\\slash": "bs"}, "stac_extensions": []}');
SELECT format('_items_%s', key) AS partition FROM collections WHERE id = 'pgstac-test-collection' \gset

SELECT lives_ok(
    $$ SELECT upsert_queryable('test:idx_' || t, property_index_type => t, collection_ids => '{pgstac-test-collection}')
       FROM unnest(ARRAY['BTREE', 'BRIN', 'HASH']) t;
       SELECT maintain_partitions(); $$,
    'BTREE, BRIN and HASH are accepted on to_text and their indexes build'
);

SELECT is(
    (SELECT count(*)::int FROM pgstac_indexes WHERE tablename = :'partition' AND field LIKE 'test:idx_%'),
    3,
    'each accepted index type built one index on the partition'
);

SELECT throws_ok(
    $$ SELECT upsert_queryable('test:idx_gin_text', property_index_type => 'GIN'); $$,
    '42704',
    'test:idx_gin_text cannot be indexed: data type text has no default operator class for access method "gin"',
    'GIN on to_text is rejected with PostgreSQL''s operator class error'
);

SELECT lives_ok(
    $$ SELECT upsert_queryable('test:idx_gin_array', property_wrapper => 'to_text_array', property_index_type => 'GIN', collection_ids => '{pgstac-test-collection}');
       SELECT maintain_partitions(); $$,
    'GIN on to_text_array is accepted and its index builds'
);

SELECT is(
    (SELECT count(*)::int FROM pgstac_indexes_stats WHERE tablename = :'partition' AND field = 'test:idx_gin_array' AND indexdef ~ 'USING gin'),
    1,
    'the GIN queryable has exactly one gin index'
);

-- An aliased queryable is indexed on its property_path, and that index pairs with it.
SELECT upsert_queryable('alias:idx', property_index_type => 'BTREE', collection_ids => '{pgstac-test-collection}', property_path => '{properties,"alias:target"}');
SELECT is(
    (SELECT count(*)::int FROM pgstac_indexes WHERE tablename = :'partition' AND field = 'alias:idx' AND idx LIKE $l$%'alias:target'::text%$l$),
    1,
    'the aliased queryable is indexed on its property_path'
);
SELECT is_empty(
    format($$ SELECT * FROM maintain_partition_queries(%L); $$, :'partition'),
    'the index built on the aliased path pairs with its queryable'
);

-- An index built from the filter expression is the index the queryable wants.
SELECT format('CREATE INDEX prebuilt_idx ON %I USING btree (%s)', :'partition', (queryable('assets.thumbnail.href')).expression) \gexec
SELECT upsert_queryable('assets.thumbnail.href', property_index_type => 'BTREE', collection_ids => '{pgstac-test-collection}');
SELECT is(
    (SELECT field FROM pgstac_indexes WHERE indexname = 'prebuilt_idx'),
    'assets.thumbnail.href',
    'the queryable() expression deparses to the reference index, so an index built from it pairs'
);
SELECT is(
    (SELECT count(*)::int FROM pgstac_indexes WHERE tablename = :'partition' AND field = 'assets.thumbnail.href'),
    1,
    'and no second index was built'
);
SELECT delete_queryable('assets.thumbnail.href', '{pgstac-test-collection}');
DROP INDEX prebuilt_idx;

-- Keys holding a backslash, a quote or hostile text are data in the filter and in the index alike.
SELECT lives_ok(
    $$ SELECT upsert_queryable('key:bs', property_index_type => 'BTREE', collection_ids => '{pgstac-test-collection}', property_path => ARRAY['properties', E'test:back\\slash']);
       SELECT upsert_queryable($n$key:it's$n$, property_index_type => 'BTREE', collection_ids => '{pgstac-test-collection}');
       SELECT upsert_queryable($n$key:a\');drop table x; --$n$, property_index_type => 'BTREE', collection_ids => '{pgstac-test-collection}');
       SELECT maintain_partitions(); $$,
    'keys ending in a backslash, holding a quote or hostile index'
);
SELECT is(
    (queryable('key:bs')).path,
    $p$content->'properties'->E'test:back\\slash'$p$,
    'the filter reads the backslash key through quote_literal'
);
SELECT is_empty(
    $$ SELECT * FROM queryable_indexes('items', true); $$,
    'nothing is pending after maintain_partitions'
);
DO $$ BEGIN EXECUTE format('DROP INDEX %I', (SELECT reference_index_name(q) FROM queryables q WHERE name = 'key:bs')); END $$;
SELECT is(
    (SELECT count(*)::int FROM queryable_indexes('items', true) WHERE partition = :'partition' AND (queryable_id IS NULL OR field = 'key:bs')),
    1,
    'a hand-dropped reference index leaves the partition index unpaired: an orphan with no missing counterpart'
);
SELECT maintain_partitions();
SELECT is_empty(
    $$ SELECT * FROM queryable_indexes('items', true); $$,
    'maintain_partitions rebuilds the reference index and pairs the partition index again'
);
SELECT ok(to_regclass('x') IS NULL, 'the hostile key did not execute');
SELECT is(
    search('{"collections": ["pgstac-test-collection"], "filter": {"eq": [{"property": "key:bs"}, "bs"]}}')->'features'->0->>'id',
    'pgstac-test-queryables-item',
    'a filter on the backslash key finds the item'
);
SELECT is(
    search('{"collections": ["pgstac-test-collection"], "filter": {"eq": [{"property": "key:bs"}, "nope"]}}')->'features'->0->>'id',
    NULL,
    'a filter on the backslash key misses as it should'
);
SELECT is(
    (SELECT count(*)::int FROM pg_indexes WHERE tablename = :'partition' AND indexdef LIKE $l$%'key:a\\'');drop table x; --'::text%$l$),
    1,
    'the hostile key is indexed as a key'
);
SELECT delete_queryable(n, '{pgstac-test-collection}') FROM unnest(ARRAY['key:bs', $n$key:it's$n$, $n$key:a\');drop table x; --$n$]) n;
SELECT is(
    (SELECT count(*)::int FROM queryable_indexes('items', true) WHERE partition = :'partition' AND queryable_id IS NULL),
    3,
    'the indexes of deleted queryables are orphans'
);
SELECT maintain_partitions(dropindexes => true);
SELECT is_empty(
    $$ SELECT * FROM queryable_indexes('items', true); $$,
    'dropindexes removes the orphans'
);

-- The indexes items itself carries belong to every partition and are not derived from any
-- queryables row. Narrowing the id queryable to one collection must not make the unique id
-- index of every other partition an orphan -- dropping it would admit duplicate item ids.
SELECT upsert_queryable('id', definition => (SELECT definition FROM queryables WHERE name = 'id'),
                        collection_ids => '{pgstac-test-collection}');
SELECT is_empty(
    $$ SELECT * FROM queryable_indexes('items', true); $$,
    'narrowing the id queryable leaves no partition index unpaired'
);
SELECT maintain_partitions(dropindexes => true);
SELECT is(
    (SELECT count(*)::int FROM pg_indexes
      WHERE schemaname = 'pgstac' AND tablename LIKE '\_items\_%' AND indexname LIKE '%\_pk'),
    (SELECT count(*)::int FROM pg_partition_tree('items') WHERE isleaf),
    'every partition still has its unique id index after dropindexes'
);
SELECT upsert_queryable('id', definition => (SELECT definition FROM queryables WHERE name = 'id'));

-- Two queryables that resolve to the same index -- same keys, same wrapper -- must not produce
-- byte-identical duplicates, which pair with one another and never read as orphans.
-- Both rows go in before the partition exists. Against an existing partition the first builds the
-- index and the second pairs with it, so the assertion would pass with or without the dedupe.
SELECT upsert_queryable('test:dup_a', definition => '{"type":"string"}'::jsonb,
                        property_index_type => 'BTREE', property_path => ARRAY['properties','shared_key']);
SELECT upsert_queryable('test:dup_b', definition => '{"type":"string"}'::jsonb,
                        property_index_type => 'BTREE', property_path => ARRAY['properties','shared_key']);
SELECT create_collection('{"id": "pgstac-test-dupidx", "type": "Collection", "stac_version": "1.0.0", "description": "shared index definition", "license": "proprietary", "extent": {"spatial": {"bbox": [[0, 0, 1, 1]]}, "temporal": {"interval": [["2020-01-01T00:00:00Z", null]]}}, "links": []}');
SELECT check_partition(
    'pgstac-test-dupidx',
    tstzrange('2020-01-01'::timestamptz, '2020-01-31'::timestamptz, '[]'),
    tstzrange('2020-01-01'::timestamptz, '2020-01-31'::timestamptz, '[]')
);
SELECT is(
    (SELECT count(*)::int FROM pg_indexes
      WHERE schemaname = 'pgstac' AND tablename = (
          SELECT '_items_' || key FROM collections WHERE id = 'pgstac-test-dupidx')
        AND indexdef LIKE '%shared\_key%'),
    1,
    'two queryables resolving to one index definition build a single index on a new partition'
);
SELECT delete_collection('pgstac-test-dupidx');
SELECT delete_queryable('test:dup_a');
SELECT delete_queryable('test:dup_b');
SELECT maintain_partitions('items', dropindexes => true);

-- Names whose index statement, deparsed definition and readback must all agree.
CREATE FUNCTION pg_temp.hostile_names() RETURNS text[] AS $$
    SELECT ARRAY[
        E'test:back\\slash', 'properties.properties.test:double', 'pct%s', 'pct%%s', 'x%Iy',
        'paren(x)', 'x(id)', 'two  spaces', 'pgstac.x', 'USING btree (id)', $n$test:it's$n$
    ];
$$ LANGUAGE SQL IMMUTABLE;

SELECT lives_ok(
    $$ SELECT upsert_queryable(n, property_index_type => 'BTREE', collection_ids => '{pgstac-test-collection}')
       FROM unnest(pg_temp.hostile_names()) n;
       SELECT maintain_partitions();
       SELECT maintain_partitions(); $$,
    'every hostile name is accepted and its index builds'
);

SELECT is_empty(
    $$ SELECT * FROM queryable_indexes('items', true); $$,
    'every hostile name compares equal to its deparsed index, so nothing is pending after two runs'
);

SELECT is_empty(
    format($$ SELECT n FROM unnest(pg_temp.hostile_names()) n
              WHERE (SELECT count(*) FROM pgstac_indexes WHERE tablename = %L AND field = strip_properties_prefix(n)) <> 1 $$, :'partition'),
    'each hostile name is indexed exactly once however often maintain_partitions runs'
);

SELECT is(
    (SELECT count(*)::int FROM queryables WHERE name = 'test:double'),
    1,
    'a name inserted as properties.properties.x is stored as x'
);

SELECT is(
    (SELECT count(*)::int FROM pg_indexes WHERE tablename = :'partition' AND indexdef LIKE '%''pct\%s''::text%'),
    1,
    'the pct%s index is on that key, not the one pct%%s built'
);

SELECT is(
    (SELECT count(*)::int FROM pg_indexes WHERE tablename = :'partition' AND indexdef ~ 'USING btree \(id\)$'),
    1,
    'no duplicate id index'
);

SELECT format('CREATE TABLE public.%I (id text); CREATE INDEX shadow_id_idx ON public.%I (id);', :'partition', :'partition') \gexec
SELECT lives_ok(
    $$ SELECT maintain_partitions(); $$,
    'a same-named table in another schema is ignored'
);
SELECT is_empty(
    $$ SELECT indexname FROM queryable_indexes('items', true) WHERE indexname = 'shadow_id_idx'; $$,
    'the index of the same-named table is not attributed to the partition'
);
SELECT format('DROP TABLE public.%I;', :'partition') \gexec

SELECT upsert_queryable('test:arr', '{"type":"array"}', property_index_type => 'GIN', collection_ids => '{pgstac-test-collection2}');
SELECT throws_ok(
    $$ UPDATE queryables SET definition = '{"type":"string"}' WHERE name = 'test:arr'; $$,
    '42704',
    NULL,
    'a definition change that invalidates the index type is rejected'
);

-- Both spellings of a name are one property, so the second replaces the first.
SELECT upsert_queryable('flip:x', property_wrapper => 'to_int', property_index_type => 'BTREE', collection_ids => '{pgstac-test-collection}');
SELECT upsert_queryable('properties.flip:x', property_wrapper => 'to_text', property_index_type => 'BTREE', collection_ids => '{pgstac-test-collection}');
SELECT is(
    (SELECT count(*)::int FROM queryables WHERE name LIKE '%flip:x'),
    1,
    'a properties. spelling of a name replaces the bare one'
);
SELECT maintain_partitions();
SELECT is(
    (SELECT count(*)::int FROM pgstac_indexes WHERE tablename = :'partition' AND field = 'flip:x' AND idx LIKE '%to_text(%'),
    1,
    'the replacing row built its index exactly once'
);
SELECT is_empty(
    format($$ SELECT * FROM maintain_partition_queries(%L); $$, :'partition'),
    'nothing is pending after the flip: the index of the replaced row is an orphan until dropindexes'
);
SELECT is(
    (SELECT count(*)::int FROM pgstac_indexes WHERE tablename = :'partition' AND field IS NULL AND idx LIKE '%to_int(%flip:x%'),
    1,
    'the index of the replaced row is reported with no field'
);
SELECT throws_ok(
    $$ INSERT INTO queryables (name) VALUES ('flip:x'); $$,
    '23505',
    NULL,
    'a bare name cannot be inserted next to its properties. spelling'
);

-- collection_ids is stored in one spelling: sorted, deduplicated, never empty.
SELECT upsert_queryable('dup:x', property_index_type => 'BTREE', collection_ids => '{pgstac-test-collection,pgstac-test-collection}');
SELECT is(
    (SELECT collection_ids FROM queryables WHERE name = 'dup:x'),
    '{pgstac-test-collection}'::text[],
    'a repeated collection is stored once'
);
SELECT is(
    (SELECT count(*)::int FROM pgstac_indexes WHERE tablename = :'partition' AND field = 'dup:x'),
    1,
    'a repeated collection builds one index'
);
SELECT upsert_queryable('ord:x', collection_ids => '{pgstac-test-collection2,pgstac-test-collection}');
SELECT lives_ok(
    $$ SELECT delete_queryable('ord:x', '{pgstac-test-collection2,pgstac-test-collection}'); $$,
    'delete_queryable matches the scope however it is ordered'
);
SELECT upsert_queryable('glob:x');
SELECT throws_ok(
    $$ SELECT remove_collection_from_queryable('glob:x', 'pgstac-test-collection'); $$,
    '22023',
    'Queryable glob:x is global; use delete_queryable',
    'a collection cannot be removed from a global queryable'
);
SELECT upsert_queryable('empty:x', collection_ids => '{}');
SELECT is(
    (SELECT collection_ids FROM queryables WHERE name = 'empty:x'),
    NULL,
    'an empty collection_ids argument to upsert_queryable means every collection'
);
SELECT throws_ok(
    $$ INSERT INTO queryables (name, collection_ids) VALUES ('empty:y', '{}'); $$,
    '23514',
    NULL,
    'an empty scope cannot be stored'
);
SELECT throws_ok(
    $$ UPDATE queryables SET collection_ids = '{}' WHERE name = 'dup:x'; $$,
    '23514',
    NULL,
    'an update cannot promote a row to every collection through an empty scope'
);

-- The write path of pgstac_ingest: the definer helper builds the reference index; the template
-- itself is out of reach.
SET ROLE pgstac_ingest;
SELECT lives_ok(
    $$ SELECT upsert_queryable('ingest:x', property_index_type => 'BTREE', collection_ids => '{pgstac-test-collection}'); $$,
    'pgstac_ingest can register an indexed queryable'
);
SELECT is(
    (SELECT count(*)::int FROM pgstac_indexes WHERE tablename = :'partition' AND field = 'ingest:x'),
    1,
    'the index pgstac_ingest asked for is built through the definer helpers'
);
SELECT throws_ok(
    $$ INSERT INTO queryable_index_template (id, geometry, collection, datetime, end_datetime, content) VALUES ('x', 'POINT(0 0)', 'x', now(), now(), '{}'); $$,
    '42501',
    NULL,
    'pgstac_ingest cannot write the index template'
);
SELECT throws_ok(
    $$ CREATE INDEX ingest_idx ON queryable_index_template (id); $$,
    '42501',
    NULL,
    'pgstac_ingest cannot index the template but through maintain_reference_index'
);
RESET ROLE;
SELECT delete_queryable('ingest:x', '{pgstac-test-collection}');

-- Deleting a collection removes it from the collection_ids that name it, never widening them.
INSERT INTO collections (content) VALUES ('{"id": "pgstac-test-colldel"}');
SELECT upsert_queryable('coll:two', collection_ids => '{pgstac-test-collection,pgstac-test-colldel}');
SELECT upsert_queryable('coll:one', collection_ids => '{pgstac-test-colldel}');
DELETE FROM collections WHERE id = 'pgstac-test-colldel';
SELECT is(
    (SELECT collection_ids FROM queryables WHERE name = 'coll:two'),
    '{pgstac-test-collection}'::text[],
    'a deleted collection is removed from a two-collection queryable'
);
SELECT is_empty(
    $$ SELECT * FROM queryables WHERE name = 'coll:one'; $$,
    'a queryable scoped to the deleted collection alone is deleted'
);
DELETE FROM queryables WHERE name = 'coll:two';

-- Registering a queryable named after the key a ->> index reads must leave that index alone.
SELECT format('CREATE INDEX test_astext_idx ON %I ((content->''properties''->>''test:astext''))', :'partition') \gexec
SELECT upsert_queryable('properties', property_index_type => 'BTREE', collection_ids => '{pgstac-test-collection}');
SELECT maintain_partitions();
SELECT has_index('pgstac'::name, :'partition'::name, 'test_astext_idx'::name, 'an index on a ->> expression survives maintain_partitions');

-- maintain_index runs elevated, so it may only touch indexes of the partition it is given.
SET ROLE pgstac_ingest;
SELECT throws_ok(
    format($$ SELECT maintain_index(%L, 'queryables_name_idx', NULL, true); $$, :'partition'),
    '22023',
    NULL,
    'maintain_index refuses an index that is not on the partition'
);
RESET ROLE;
SELECT has_index('pgstac'::name, 'queryables'::name, 'queryables_name_idx'::name, 'the index on another table survived');

-- A custom wrapper is registered by adding its name to queryable_wrappers; the function alone is not enough.
-- CREATE INDEX runs with search_path pg_catalog, so the body qualifies what it calls.
CREATE FUNCTION pgstac.to_upper_text(j jsonb) RETURNS text AS $$
    SELECT upper(pgstac.to_text(j));
$$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;
SELECT throws_ok(
    $$ SELECT upsert_queryable('test:astext', property_wrapper => 'to_upper_text'); $$,
    '23514',
    'to_upper_text is not in queryable_wrappers.',
    'a wrapper name not in queryable_wrappers is rejected even though its function exists'
);
INSERT INTO queryable_wrappers (name) VALUES ('to_upper_text'), ('to_missing');
SELECT throws_ok(
    $$ SELECT upsert_queryable('test:astext', property_wrapper => 'to_missing'); $$,
    '42883',
    'to_missing is registered in queryable_wrappers but pgstac.to_missing(jsonb) does not exist.',
    'a registered wrapper without its function is rejected'
);
SELECT lives_ok(
    $$ SELECT upsert_queryable('test:astext', property_wrapper => 'to_upper_text', property_index_type => 'BTREE', collection_ids => '{pgstac-test-collection}'); $$,
    'a registered custom wrapper is accepted with a BTREE index'
);
SELECT is(
    (SELECT count(*)::int FROM pgstac_indexes WHERE tablename = :'partition' AND field = 'test:astext' AND idx LIKE '%to_upper_text(%'),
    1,
    'the custom wrapper index built on the partition'
);
SELECT is(
    search('{"collections": ["pgstac-test-collection"], "filter": {"eq": [{"property": "test:astext"}, "A"]}}')->'features'->0->>'id',
    'pgstac-test-queryables-item',
    'a filter reads the property through the custom wrapper'
);
CREATE FUNCTION pg_temp.explain_where(partition text, wher text) RETURNS SETOF text AS $$
BEGIN
    RETURN QUERY EXECUTE format('EXPLAIN SELECT id FROM %I WHERE %s', partition, wher);
END;
$$ LANGUAGE PLPGSQL;
SET LOCAL enable_seqscan TO off;
SELECT matches(
    (SELECT string_agg(line, E'\n') FROM pg_temp.explain_where(:'partition', stac_search_to_where('{"filter": {"eq": [{"property": "test:astext"}, "A"]}}')) line),
    (SELECT indexname FROM pgstac_indexes WHERE tablename = :'partition' AND field = 'test:astext'),
    'the filter uses the custom wrapper index'
);
RESET enable_seqscan;
SELECT delete_queryable('test:astext', '{pgstac-test-collection}');
DELETE FROM queryable_wrappers WHERE name IN ('to_upper_text', 'to_missing');
SELECT format('DROP INDEX %I;', indexname) FROM pgstac_indexes WHERE tablename = :'partition' AND idx LIKE '%to_upper_text(%' \gexec
DROP FUNCTION pgstac.to_upper_text(jsonb);

SELECT delete_item('pgstac-test-queryables-item', 'pgstac-test-collection');
DELETE FROM queryables WHERE name IN (SELECT strip_properties_prefix(n) FROM unnest(pg_temp.hostile_names()) n)
    OR name IN ('properties', 'test:arr', 'flip:x', 'dup:x', 'glob:x', 'empty:x');

SELECT throws_ok(
    $$ UPDATE queryables SET property_wrapper = 'pg_sleep' WHERE name = 'test:wrapper_to_int'; $$,
    '23514',
    NULL,
    'an update to a bad property_wrapper is rejected too'
);

SELECT upsert_queryable('test:rename', collection_ids => '{pgstac-test-collection}');
SELECT throws_ok(
    $$ UPDATE queryables SET name = 'test:wrapper_to_int' WHERE name = 'test:rename'; $$,
    '23505',
    NULL,
    'renaming a per-collection queryable onto a name with a global row is rejected'
);

SELECT lives_ok(
    $$ UPDATE queryables SET name = 'test:renamed' WHERE name = 'test:rename'; $$,
    'renaming to an unused name is accepted'
);

DELETE FROM queryables WHERE name LIKE 'test:wrapper_%' OR name LIKE 'test:idx_%' OR name LIKE 'test:rename%';

-- The queryables trigger maintains only the partitions of the collections the affected rows name.
-- B is left missing an index a global queryable wants, which only a walk of its partition would rebuild.
SELECT create_item('{"id": "pgstac-test-queryables-item2", "type": "Feature", "stac_version": "1.0.0", "collection": "pgstac-test-collection2", "bbox": [0, 0, 1, 1], "links": [], "geometry": {"type": "Polygon", "coordinates": [[[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]]}, "assets": {}, "properties": {"datetime": "2011-06-01T00:00:00Z"}, "stac_extensions": []}');
SELECT format('_items_%s', key) AS partition2 FROM collections WHERE id = 'pgstac-test-collection2' \gset
INSERT INTO collections (content) VALUES ('{"id": "pgstac-test-collection3"}');
SELECT upsert_queryable('test:scope', property_index_type => 'BTREE');
SELECT format('DROP INDEX %I;', indexname) FROM pgstac_indexes WHERE tablename = :'partition2' AND field = 'test:scope' \gexec
CREATE FUNCTION pg_temp.index_count(partition text, field text) RETURNS int AS $$
    SELECT count(*)::int FROM pgstac_indexes i WHERE i.tablename = partition AND i.field = index_count.field;
$$ LANGUAGE SQL;
SELECT is(pg_temp.index_count(:'partition2', 'test:scope'), 0, 'B is missing the index of a global queryable');

SELECT upsert_queryable('test:scoped', property_index_type => 'BTREE', collection_ids => '{pgstac-test-collection3}');
SELECT lives_ok(
    $$ SELECT add_collection_to_queryable('test:scoped', 'pgstac-test-collection'); $$,
    'a collection whose partition exists is added next to one that has none yet'
);
SELECT is(pg_temp.index_count(:'partition', 'test:scoped'), 1, 'the per-collection add built the index on A');
SELECT is(pg_temp.index_count(:'partition2', 'test:scope'), 0, 'the per-collection add left B''s partition untouched');

CREATE TEMP TABLE a_indexes AS SELECT indexrelid FROM pg_index WHERE indrelid = partition_oid(:'partition');
SELECT ctid AS scoped_ctid FROM queryables WHERE name = 'test:scoped' \gset
SELECT add_collection_to_queryable('test:scoped', 'pgstac-test-collection');
SELECT is(
    (SELECT ctid FROM queryables WHERE name = 'test:scoped'),
    :'scoped_ctid'::tid,
    'adding a collection that is already a member returns before writing'
);
SELECT set_eq(
    format($$ SELECT indexrelid FROM pg_index WHERE indrelid = partition_oid(%L) $$, :'partition'),
    $$ SELECT indexrelid FROM a_indexes $$,
    'the repeated add rebuilt no index on A'
);
SELECT is(pg_temp.index_count(:'partition2', 'test:scope'), 0, 'the repeated add left B''s partition untouched');

SELECT upsert_queryable('test:scope', property_index_type => 'BTREE');
SELECT is(pg_temp.index_count(:'partition2', 'test:scope'), 1, 'a global row still walks the whole tree');

DELETE FROM queryables WHERE name IN ('test:scope', 'test:scoped');
SELECT delete_item('pgstac-test-queryables-item2', 'pgstac-test-collection2');
DELETE FROM collections WHERE id = 'pgstac-test-collection3';

-- canonicalize_queryables repairs rows the triggers now refuse, so those rows go in with the
-- triggers off, as a legacy database's rows arrive.
CREATE TEMP TABLE canonical_queryables AS SELECT * FROM queryables;
SELECT canonicalize_queryables();
SELECT set_eq(
    $$ SELECT * FROM queryables $$,
    $$ SELECT * FROM canonical_queryables $$,
    'canonicalize_queryables leaves a canonical database alone'
);

ALTER TABLE queryables DISABLE TRIGGER USER;
INSERT INTO queryables (name, collection_ids) VALUES
    ('test:canon_empty', '{}'),
    ('properties.test:canon_twin', NULL),
    ('test:canon_twin', NULL);
INSERT INTO queryables (name, property_index_type) VALUES ('end_datetime', 'BTREE');
ALTER TABLE queryables ENABLE TRIGGER USER;
SELECT id AS twin_id FROM queryables WHERE name = 'properties.test:canon_twin' \gset

SELECT canonicalize_queryables();

SELECT is(
    (SELECT collection_ids FROM queryables WHERE name = 'test:canon_empty'),
    NULL::text[],
    'an empty collection_ids becomes the global row it meant'
);
SELECT results_eq(
    $$ SELECT id, name FROM queryables WHERE name = 'test:canon_twin' $$,
    format($$ VALUES (%s::bigint, 'test:canon_twin'::text) $$, :'twin_id'),
    'of two rows that differ only by the properties. prefix the older survives'
);
SELECT is(
    (SELECT property_index_type FROM queryables WHERE name = 'end_datetime'),
    NULL::text,
    'an index type on a column of items is cleared'
);

ALTER TABLE queryables DISABLE TRIGGER USER;
UPDATE queryables SET property_wrapper = 'to_unregistered' WHERE name = 'test:canon_empty';
ALTER TABLE queryables ENABLE TRIGGER USER;
SELECT throws_ok(
    $$ SELECT canonicalize_queryables(); $$,
    '23514',
    NULL,
    'a wrapper that is not registered stops canonicalize_queryables'
);
-- Named, so an operator does not have to grep the table to find the row. This is why the check
-- runs before the rewrite: the rewrite fires the trigger, whose message names only the wrapper.
SELECT throws_like(
    $$ SELECT canonicalize_queryables(); $$,
    '%test:canon_empty%to_unregistered%',
    'the unregistered wrapper error names the queryable, even in a legacy spelling'
);
ALTER TABLE queryables DISABLE TRIGGER USER;
UPDATE queryables SET property_wrapper = NULL WHERE name = 'test:canon_empty';
ALTER TABLE queryables ENABLE TRIGGER USER;

DELETE FROM queryables WHERE name IN ('test:canon_empty', 'test:canon_twin', 'end_datetime');

-- Two rows of one name can both carry collection_ids '{}': '{}' && '{}' is false and neither is
-- NULL, so the conflict check accepts them. Rewriting one before collection_ids is canonical
-- hands the trigger a row it refuses, aborting the whole migration.
ALTER TABLE queryables DISABLE TRIGGER USER;
INSERT INTO queryables (name, collection_ids, definition, property_wrapper) VALUES
    ('test:canon_pair', '{}', NULL, NULL),
    ('test:canon_pair', '{}', '{"type":"number"}'::jsonb, NULL);
INSERT INTO queryables (name, collection_ids, property_index_type) VALUES
    ('test:canon_pair', '{}', 'BTREE');
ALTER TABLE queryables ENABLE TRIGGER USER;
SELECT lives_ok(
    $$ SELECT canonicalize_queryables(); $$,
    'two legacy rows of one name scoped to the empty array do not abort canonicalize_queryables'
);
SELECT results_eq(
    $$ SELECT count(*)::int, bool_and(collection_ids IS NULL) FROM queryables WHERE name = 'test:canon_pair' $$,
    $$ VALUES (1, true) $$,
    'they reduce to a single global row'
);
-- Field by field across every row removed, not from whichever one happened to sort first.
SELECT results_eq(
    $$ SELECT definition, property_index_type FROM queryables WHERE name = 'test:canon_pair' $$,
    $$ VALUES ('{"type": "number"}'::jsonb, 'BTREE'::text) $$,
    'the survivor inherits each field from whichever removed row had it'
);
DELETE FROM queryables WHERE name = 'test:canon_pair';

-- Anchored only on the oldest row, two rows that conflict with each other but not with it both
-- survive, and the rewrite then trips the trigger. The dedupe repeats until none is left.
ALTER TABLE queryables DISABLE TRIGGER USER;
INSERT INTO queryables (name, collection_ids) VALUES
    ('test:canon_chain', '{pgstac-test-collection}'),
    ('test:canon_chain', '{pgstac-test-collection2}'),
    ('properties.test:canon_chain', '{pgstac-test-collection2}');
ALTER TABLE queryables ENABLE TRIGGER USER;
SELECT lives_ok(
    $$ SELECT canonicalize_queryables(); $$,
    'rows that conflict only with each other, not with the oldest, still resolve'
);
SELECT is(
    (SELECT count(*)::int FROM queryables WHERE name = 'test:canon_chain'),
    2,
    'the two disjoint scopes survive and the overlapping one does not'
);
DELETE FROM queryables WHERE name = 'test:canon_chain';

DROP TABLE canonical_queryables;

-- The index method follows the wrapper the definition implies: an array is read through
-- to_text_array and matched with @> and &&, which btree cannot serve.
SELECT is(default_index_type('{"type":"array"}'::jsonb), 'GIN',
    'an array definition defaults to a GIN index');
SELECT is(default_index_type('{"type":"string"}'::jsonb), 'BTREE',
    'a string definition defaults to a BTREE index');
SELECT is(default_index_type('{"type":"integer"}'::jsonb), 'BTREE',
    'an integer definition defaults to a BTREE index');
SELECT is(default_index_type('{"type":["array","null"]}'::jsonb), 'GIN',
    'an array in a type list still defaults to GIN');

-- delete_missing_queryables prunes one collection's queryables to the names it is given. Given
-- its own collection here: with none it would prune every queryable that applies to all of them,
-- which is what it is for.
SELECT create_collection('{"id": "pgstac-test-prune", "type": "Collection", "stac_version": "1.0.0", "description": "prune scope", "license": "proprietary", "links": [], "extent": {"spatial": {"bbox": [[0, 0, 1, 1]]}, "temporal": {"interval": [["2020-01-01T00:00:00Z", null]]}}}');
SELECT upsert_queryable('test:prune_keep', '{"type":"string"}'::jsonb,
    collection_ids => '{pgstac-test-prune}');
SELECT upsert_queryable('test:prune_drop', '{"type":"string"}'::jsonb,
    collection_ids => '{pgstac-test-prune}');
SELECT upsert_queryable('test:prune_global', '{"type":"string"}'::jsonb);

SELECT is(
    delete_missing_queryables(ARRAY['properties.test:prune_keep'], '{pgstac-test-prune}'),
    1,
    'it removes only the rows of that scope the list omits, counting them'
);
SELECT is_empty(
    $$ SELECT name FROM queryables WHERE name = 'test:prune_drop' $$,
    'the unlisted row is gone'
);
SELECT isnt_empty(
    $$ SELECT name FROM queryables WHERE name = 'test:prune_keep' $$,
    'the listed row survives, matched with its properties. prefix stripped'
);
SELECT isnt_empty(
    $$ SELECT name FROM queryables WHERE name = 'test:prune_global' AND collection_ids IS NULL $$,
    'a row in another scope is left alone'
);
DELETE FROM queryables WHERE name LIKE 'test:prune_%';
SELECT delete_collection('pgstac-test-prune');

-- upsert_queryables loads a whole document: the wrapper and index method come from each
-- property's own definition, and the fields items carries as columns are skipped.
SELECT create_collection('{"id": "pgstac-test-load", "type": "Collection", "stac_version": "1.0.0", "description": "document load", "license": "proprietary", "links": [], "extent": {"spatial": {"bbox": [[0, 0, 1, 1]]}, "temporal": {"interval": [["2020-01-01T00:00:00Z", null]]}}}');

SELECT is(
    upsert_queryables(
        '{"properties": {
            "id": {"type": "string"},
            "datetime": {"type": "string", "format": "date-time"},
            "test:load_str": {"type": "string"},
            "test:load_arr": {"type": "array"},
            "test:load_num": {"type": "number"}
        }}'::jsonb,
        '{pgstac-test-load}',
        '{test:load_str,test:load_arr}'
    ),
    3,
    'the fields items carries as columns are not loaded'
);
SELECT results_eq(
    $$ SELECT name, property_wrapper, property_index_type FROM queryables
       WHERE collection_ids = '{pgstac-test-load}' ORDER BY name $$,
    $$ VALUES ('test:load_arr'::text, NULL::text, 'GIN'::text),
              ('test:load_num', NULL, NULL),
              ('test:load_str', NULL, 'BTREE') $$,
    'each property is indexed only if asked for, by the method its definition implies'
);

-- Loading the document again without a property removes it only when asked.
SELECT is(
    upsert_queryables('{"properties": {"test:load_str": {"type": "string"}}}'::jsonb,
                      '{pgstac-test-load}'),
    1,
    'a second load without delete_missing reports what it loaded'
);
SELECT is(
    (SELECT count(*)::int FROM queryables WHERE collection_ids = '{pgstac-test-load}'),
    3,
    'and leaves the properties it did not name'
);
SELECT is(
    upsert_queryables('{"properties": {"test:load_str": {"type": "string"}}}'::jsonb,
                      '{pgstac-test-load}', NULL, TRUE),
    1,
    'with delete_missing it loads the same one'
);
SELECT results_eq(
    $$ SELECT name FROM queryables WHERE collection_ids = '{pgstac-test-load}' $$,
    $$ VALUES ('test:load_str'::text) $$,
    'and removes the properties the document no longer names'
);
SELECT throws_ok(
    $$ SELECT upsert_queryables('{"title": "no properties here"}'::jsonb) $$,
    NULL, NULL,
    'a document with no properties is refused'
);

DELETE FROM queryables WHERE collection_ids = '{pgstac-test-load}';
SELECT delete_collection('pgstac-test-load');

-- An indexed queryable reaches the partitions, and one that asked for no index builds nothing.
-- The statement trigger on queryables maintains the partitions of the collections the written
-- rows name, so loading a document does not also walk every other collection.
SELECT upsert_queryables(
    '{"properties": {"test:mp_idx": {"type": "string"}, "test:mp_plain": {"type": "string"}}}'::jsonb,
    '{pgstac-test-collection}',
    '{test:mp_idx}'
);
SELECT isnt_empty(
    $$ SELECT indexname FROM pg_indexes
       WHERE schemaname = 'pgstac' AND tablename <> 'queryable_index_template'
         AND indexdef LIKE '%test:mp_idx%' $$,
    'a queryable asked to be indexed reaches the partitions'
);
SELECT is_empty(
    $$ SELECT indexname FROM pg_indexes
       WHERE schemaname = 'pgstac' AND indexdef LIKE '%test:mp_plain%' $$,
    'one that was not is stored without an index anywhere'
);
SELECT delete_queryable(n, '{pgstac-test-collection}')
FROM unnest(ARRAY['test:mp_idx', 'test:mp_plain']) n;
