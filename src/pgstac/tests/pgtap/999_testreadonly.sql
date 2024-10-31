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
