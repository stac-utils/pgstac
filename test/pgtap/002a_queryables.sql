SELECT results_eq(
    $$ SELECT property_wrapper FROM queryables WHERE name='eo:cloud_cover'; $$,
    $$ SELECT 'to_int'; $$,
    'Make sure that cloud_cover is set to to_int wrapper.'
);


SELECT results_eq(
    $$ SELECT sort_sqlorderby('{"sortby":{"field":"properties.eo:cloud_cover"}}'); $$,
    $$ SELECT sort_sqlorderby('{"sortby":{"field":"eo:cloud_cover"}}'); $$,
    'Make sure that sortby with/without properties prefix return the same sort statement.'
);

SET pgstac."default-filter-lang" TO 'cql2-json';

SELECT results_eq(
    $$ SELECT stac_search_to_where('{"filter":{"op":"eq","args":[{"property":"eo:cloud_cover"},0]}}'); $$,
    $$ SELECT stac_search_to_where('{"filter":{"op":"eq","args":[{"property":"properties.eo:cloud_cover"},0]}}'); $$,
    'Make sure that CQL2 filter works the same with/without properties prefix.'
);

SET pgstac."default-filter-lang" TO 'cql-json';

SELECT results_eq(
    $$ SELECT stac_search_to_where('{"filter":{"eq":[{"property":"eo:cloud_cover"},0]}}'); $$,
    $$ SELECT stac_search_to_where('{"filter":{"eq":[{"property":"properties.eo:cloud_cover"},0]}}'); $$,
    'Make sure that CQL filter works the same with/without properties prefix.'
);
