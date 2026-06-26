SET transaction_read_only TO 'on';

SELECT results_eq(
    $$ SHOW transaction_read_only; $$,
    $$ SELECT 'on'; $$,
    'Transaction set to read only'
);

SELECT throws_ok(
    $$ SELECT search('{}'); $$,
    '25006'
);

SET pgstac.readonly to 'true';
SELECT results_eq(
    $$ SELECT pgstac.readonly(); $$,
    $$ SELECT TRUE; $$,
    'Readonly is set to true'
);

SELECT lives_ok(
    $$ SELECT search('{}'); $$,
    'Search works with readonly mode set to on in readonly mode.'
);

SET pgstac.context TO 'on';
SELECT lives_ok(
    $$ SELECT search('{}'); $$,
    'Search works with readonly mode set to on in readonly mode and the context extension enabled.'
);
SELECT results_eq(
    $$ SELECT (search('{}')->>'numberMatched') IS NOT NULL; $$,
    $$ SELECT TRUE; $$,
    'Readonly search with context on returns numberMatched without requiring cache writes.'
);
SELECT throws_ok(
    $$ SELECT gc_deleted_items_log('1 second'::interval); $$,
    '25006'
);
SELECT throws_ok(
    $$ SELECT gc_deleted_items_log('1 second'::interval, 1); $$,
    '25006'
);
SELECT throws_ok(
    $$ SELECT gc_deleted_items_log_batch('1 second'::interval, 1); $$,
    '25006'
);
RESET pgstac.readonly;
