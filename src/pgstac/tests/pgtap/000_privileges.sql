-- Privilege wall (INV-1): pgstac_ingest may READ the wall-protected tables but may MUTATE them only through
-- the SECURITY DEFINER write functions — no inherited grant lets it write directly.

SELECT ok(
    has_table_privilege('pgstac_ingest', 'pgstac.items', 'SELECT'),
    'pgstac_ingest may SELECT items'
);

SELECT ok(
    NOT has_table_privilege('pgstac_ingest', 'pgstac.items', 'INSERT'),
    'pgstac_ingest may not directly INSERT items (privilege wall)'
);

SELECT ok(
    NOT has_table_privilege('pgstac_ingest', 'pgstac.partition_stats', 'UPDATE'),
    'pgstac_ingest may not directly UPDATE partition_stats (privilege wall)'
);

SELECT ok(
    NOT has_table_privilege('pgstac_ingest', 'pgstac.item_fragments', 'DELETE'),
    'pgstac_ingest may not directly DELETE item_fragments (privilege wall)'
);
