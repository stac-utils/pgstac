SELECT has_function('pgstac'::name, 'env_full', ARRAY[]::text[]);
SELECT has_function('pgstac'::name, 'env_and', ARRAY['pred_envelope','pred_envelope']);
SELECT has_function('pgstac'::name, 'env_or', ARRAY['pred_envelope','pred_envelope']);
SELECT has_function('pgstac'::name, 'search_envelope', ARRAY['jsonb']);
SELECT has_function('pgstac'::name, 'partition_bounds', ARRAY['pred_envelope']);
SELECT has_function('pgstac'::name, 'next_band', ARRAY['bigint[]','integer','numeric','integer','boolean']);

SELECT results_eq($$ SELECT (env_full()).colls IS NULL $$, $$ SELECT true $$, 'env_full: colls NULL');
SELECT results_eq($$ SELECT (env_full()).dt IS NOT NULL $$, $$ SELECT true $$, 'env_full: dt not NULL');
SELECT results_eq($$
    SELECT (env_and(
        (ARRAY['a','b'], tstzmultirange(tstzrange('-infinity','infinity','[]')), tstzmultirange(tstzrange('-infinity','infinity','[]')), NULL)::pred_envelope,
        (ARRAY['b','c'], tstzmultirange(tstzrange('-infinity','infinity','[]')), tstzmultirange(tstzrange('-infinity','infinity','[]')), NULL)::pred_envelope
    )).colls $$, $$ SELECT ARRAY['b']::text[] $$, 'env_and: intersect');
SELECT results_eq($$
    SELECT (env_or(
        (ARRAY['a'], tstzmultirange(tstzrange('-infinity','infinity','[]')), tstzmultirange(tstzrange('-infinity','infinity','[]')), NULL)::pred_envelope,
        (ARRAY['b'], tstzmultirange(tstzrange('-infinity','infinity','[]')), tstzmultirange(tstzrange('-infinity','infinity','[]')), NULL)::pred_envelope
    )).colls $$, $$ SELECT ARRAY['a','b']::text[] $$, 'env_or: union');
SELECT results_eq($$ SELECT (nb).band_start_idx FROM next_band(ARRAY[10,20,30,40]::bigint[], 1, 25, 4) nb $$, $$ SELECT 1 $$, 'next_band: start');
SELECT results_eq($$ SELECT (nb).band_end_idx FROM next_band(ARRAY[10,20,30,40]::bigint[], 1, 25, 4) nb $$, $$ SELECT 2 $$, 'next_band: end');
SELECT results_eq($$ SELECT (nb).scanned FROM next_band(ARRAY[10,20,30,40]::bigint[], 1, 25, 4) nb $$, $$ SELECT 30::bigint $$, 'next_band: scanned');
SELECT results_eq($$ SELECT (nb).done FROM next_band(ARRAY[10,20,30,40]::bigint[], 1, 25, 4) nb $$, $$ SELECT false $$, 'next_band: not done');
SELECT results_eq($$ SELECT (nb).done FROM next_band(ARRAY[10,20]::bigint[], 1, 100, 4) nb $$, $$ SELECT true $$, 'next_band: done when exceeds');
SELECT results_eq($$ SELECT (nb).band_end_idx FROM next_band(ARRAY[10,10,10,10,10,10]::bigint[], 1, 100, 2) nb $$, $$ SELECT 2 $$, 'next_band: cap');
SELECT results_eq($$ SELECT (nb).done FROM next_band(NULL::bigint[], 1, 10, 4) nb $$, $$ SELECT true $$, 'next_band: NULL done');
-- Descending walk (the desc band-order fix): starting at the most recent band and walking toward
-- older months. Cursor at index 4, target 50 over counts 40+30 reaches the target at index 3.
SELECT results_eq($$ SELECT (nb).band_start_idx FROM next_band(ARRAY[10,20,30,40]::bigint[], 4, 50, 4, true) nb $$, $$ SELECT 3 $$, 'next_band desc: start (older bound)');
SELECT results_eq($$ SELECT (nb).band_end_idx FROM next_band(ARRAY[10,20,30,40]::bigint[], 4, 50, 4, true) nb $$, $$ SELECT 4 $$, 'next_band desc: end (newer bound)');
SELECT results_eq($$ SELECT (nb).next_cursor_idx FROM next_band(ARRAY[10,20,30,40]::bigint[], 4, 50, 4, true) nb $$, $$ SELECT 2 $$, 'next_band desc: cursor moves toward older');
SELECT results_eq($$ SELECT (nb).done FROM next_band(ARRAY[10,20,30,40]::bigint[], 2, 1000, 4, true) nb $$, $$ SELECT true $$, 'next_band desc: done at oldest band');
SELECT throws_ok($$
    SELECT cql2_envelope('{"op":"s_intersects","args":[{"property":"geometry"},{"type":"Invalid"}]}')
    $$, '22P02', NULL, 'cql2_envelope: bad GeoJSON raises');
SELECT results_eq($$
    SELECT (cql2_envelope('{"op":"q","args":"test"}'::jsonb)).colls IS NULL
    $$, $$ SELECT true $$, 'cql2_envelope: q returns unconstrained');
