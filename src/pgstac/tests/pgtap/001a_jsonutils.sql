SELECT has_function('pgstac'::name, 'to_text_array', ARRAY['jsonb']);
SELECT has_function('pgstac'::name, 'jsonb_leaf_rows', ARRAY['jsonb', 'text']);
SELECT has_function('pgstac'::name, 'jsonb_common_values', ARRAY['jsonb', 'jsonb']);

SELECT results_eq(
    $$ SELECT to_text_array('["a","b","c"]'::jsonb) $$,
    $$ SELECT '{a,b,c}'::text[] $$,
    'textarr returns text[] from jsonb array'
);

SELECT results_eq(
    $$
        SELECT path, value
        FROM pgstac.jsonb_leaf_rows(
            '{"a":1,"b":{"c":2},"d":[1,2],"e":{},"f":null}'::jsonb,
            ''::text
        )
        ORDER BY path
    $$,
    $$
        VALUES
            ('a'::text, '1'::jsonb),
            ('b.c'::text, '2'::jsonb),
            ('d'::text, '[1,2]'::jsonb),
            ('e'::text, '{}'::jsonb),
            ('f'::text, 'null'::jsonb)
    $$,
    'jsonb_leaf_rows flattens object leaves and keeps arrays, nulls, and empty objects as atomic values'
);

SELECT is(
    pgstac.jsonb_common_values(
        '{"a":1,"b":{"c":2,"d":3},"e":[1,2],"f":null}'::jsonb,
        '{"a":1,"b":{"c":9,"d":3},"e":[1,2],"f":null,"g":5}'::jsonb
    )::text,
    '{"a": 1, "b": {"d": 3}, "e": [1, 2], "f": null}'::jsonb::text,
    'jsonb_common_values preserves only values that are identical in both JSONB documents'
);

SELECT results_eq(
    $$
        SELECT pgstac.jsonb_common_paths_agg(content)
        FROM (
            VALUES
                ('{"a":1,"b":{"c":2,"d":3},"e":[1,2],"f":null}'::jsonb),
                ('{"a":1,"b":{"c":9,"d":3},"e":[1,2],"f":null,"g":5}'::jsonb),
                ('{"a":1,"b":{"d":3},"e":[1,2],"f":null}'::jsonb)
        ) AS docs(content)
    $$,
    $$ SELECT ARRAY['a','b.d','e','f']::text[] $$,
    'jsonb_common_paths_agg returns sorted paths whose leaf values are identical across all inputs'
);

SELECT results_eq(
    $$
        SELECT pgstac.jsonb_common_paths_agg(content)
        FROM (
            VALUES
                ('{"a":1}'::jsonb),
                ('{"a":2}'::jsonb)
        ) AS docs(content)
    $$,
    $$ SELECT '{}'::text[] $$,
    'jsonb_common_paths_agg returns an empty array when no leaf paths are shared'
);

SELECT results_eq(
    $$
        SELECT pgstac.jsonb_common_paths_agg(content)
        FROM (
            SELECT NULL::jsonb AS content WHERE FALSE
        ) AS docs
    $$,
    $$ SELECT '{}'::text[] $$,
    'jsonb_common_paths_agg returns an empty array for empty input'
);

-- jsonb_merge_recursive: the hydrate merge. Verify the disjoint fast-path matches
-- a full recursive descent for the cases the split-storage strip produces.
SELECT results_eq(
    $$ SELECT pgstac.jsonb_merge_recursive('{"a":{"type":"x","roles":["data"]}}'::jsonb, '{"a":{"href":"u"}}'::jsonb) $$,
    $$ SELECT '{"a":{"type":"x","roles":["data"],"href":"u"}}'::jsonb $$,
    'jsonb_merge_recursive shallow-concats disjoint sub-objects (asset metadata + per-item href)'
);

SELECT results_eq(
    $$ SELECT pgstac.jsonb_merge_recursive('{"a":{"b":{"c":1}}}'::jsonb, '{"a":{"b":{"d":2}}}'::jsonb) $$,
    $$ SELECT '{"a":{"b":{"c":1,"d":2}}}'::jsonb $$,
    'jsonb_merge_recursive recurses into deep (depth-4) overlapping objects without losing keys'
);

SELECT results_eq(
    $$ SELECT pgstac.jsonb_merge_recursive('{"a":{"x":1,"y":9}}'::jsonb, '{"a":{"x":2}}'::jsonb) $$,
    $$ SELECT '{"a":{"x":2,"y":9}}'::jsonb $$,
    'jsonb_merge_recursive gives the per-item value precedence on a scalar key collision'
);

SELECT results_eq(
    $$ SELECT pgstac.jsonb_merge_recursive(NULL::jsonb, '{"a":1}'::jsonb) $$,
    $$ SELECT '{"a":1}'::jsonb $$,
    'jsonb_merge_recursive returns the item when the fragment is NULL'
);

SELECT results_eq(
    $$ SELECT pgstac.jsonb_merge_recursive('{"a":1}'::jsonb, '{}'::jsonb) $$,
    $$ SELECT '{"a":1}'::jsonb $$,
    'jsonb_merge_recursive returns the fragment when the item is empty'
);

-- jsonb_canonical / pgstac_item_hash: RFC 8785-aligned, externally reproducible.
SELECT has_function('pgstac'::name, 'jsonb_canonical', ARRAY['jsonb']);

SELECT results_eq(
    $$ SELECT pgstac.jsonb_canonical('{"id":1,"bbox":2}'::jsonb) $$,
    $$ SELECT '{"bbox":2,"id":1}'::text $$,
    'jsonb_canonical sorts object keys by code point (alphabetical), not jsonb length-then-byte order'
);

SELECT results_eq(
    $$ SELECT pgstac.jsonb_canonical('{"b":1.0,"a":[3,2,1],"c":{"y":0.10,"x":true},"d":null,"id":"x"}'::jsonb) $$,
    $$ SELECT '{"a":[3,2,1],"b":1,"c":{"x":true,"y":0.1},"d":null,"id":"x"}'::text $$,
    'jsonb_canonical: nested key sort, array order preserved, numbers as shortest round-trip doubles'
);

SELECT results_eq(
    $$ SELECT pgstac.pgstac_item_hash('{"b":1.0,"a":[3,2,1],"c":{"y":0.10,"x":true},"d":null,"id":"x"}'::jsonb) $$,
    $$ SELECT '77f18c0a2c2c9f9e4836045bae644ba3d00c0308c9d2c0bd024624c22d532bf7'::text $$,
    'pgstac_item_hash matches an externally-computed sha256 of the canonical form'
);
