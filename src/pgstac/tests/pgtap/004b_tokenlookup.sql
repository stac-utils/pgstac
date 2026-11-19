-- A paging token is looked up by the id index, in the partitions of its own collection only.
-- The lookup stops at its first match. The decoy collection's partition sorts before the
-- target's, so a lookup that does not prune by collection reads the decoy first.
SELECT create_collection(format(
    '{"id":"%s","type":"Collection","stac_version":"1.0.0","description":"token lookup","license":"proprietary","links":[],"extent":{"spatial":{"bbox":[[-180,-90,180,90]]},"temporal":{"interval":[["2020-01-01T00:00:00Z","2020-03-31T00:00:00Z"]]}}}',
    c)::jsonb) FROM unnest(ARRAY['pgstac-test-token-decoy', 'pgstac-test-token-target']) c;
UPDATE collections SET partition_trunc = 'month' WHERE id = 'pgstac-test-token-target';
SELECT create_items(jsonb_agg(jsonb_build_object(
    'id', format('%s-%s', c, m), 'type', 'Feature', 'stac_version', '1.0.0', 'collection', c,
    'bbox', '[0,0,1,1]'::jsonb, 'links', '[]'::jsonb, 'assets', '{}'::jsonb,
    'geometry', '{"type":"Polygon","coordinates":[[[0,0],[0,1],[1,1],[1,0],[0,0]]]}'::jsonb,
    'properties', jsonb_build_object('datetime', format('2020-%s-15T00:00:00Z', m))
)))
FROM unnest(ARRAY['pgstac-test-token-decoy', 'pgstac-test-token-target']) c,
    unnest(ARRAY['01', '02', '03']) m;

-- Off so partitions this small still use the id index.
SET enable_seqscan TO off;
CREATE TEMP TABLE scans_before AS
    SELECT relid, seq_scan, coalesce(idx_scan, 0) AS idx_scan FROM pg_stat_xact_user_tables
    WHERE relid IN (SELECT relid FROM pg_partition_tree('items') WHERE isleaf);
SELECT is(
    (SELECT (item).id FROM get_token_record(
        'next:' || page_token('pgstac-test-token-target', 'pgstac-test-token-target-02'))),
    'pgstac-test-token-target-02',
    'a token on a month-partitioned collection resolves to its item'
);
RESET enable_seqscan;

-- pg_stat_xact_user_tables counts this transaction's scans; the difference is the lookup above.
CREATE TEMP TABLE token_scans AS
    SELECT get_partition_name(a.relid::regclass) AS partition,
        a.seq_scan - b.seq_scan AS seq_scans, coalesce(a.idx_scan, 0) - b.idx_scan AS idx_scans
    FROM pg_stat_xact_user_tables a JOIN scans_before b USING (relid)
    WHERE a.seq_scan > b.seq_scan OR coalesce(a.idx_scan, 0) > b.idx_scan;

SELECT is(
    ARRAY(SELECT partition FROM token_scans
        EXCEPT SELECT partition FROM partition_sys_meta WHERE collection = 'pgstac-test-token-target'
        ORDER BY 1),
    '{}'::text[],
    'a token lookup reads no partition of another collection'
);
SELECT is(
    ARRAY(SELECT partition FROM token_scans WHERE seq_scans > 0 ORDER BY 1),
    '{}'::text[],
    'a token lookup reads no partition by sequential scan'
);
-- Also fails when nothing was counted, which would let both checks above pass.
SELECT ok(
    EXISTS (SELECT FROM token_scans WHERE idx_scans > 0),
    'a token lookup reads its collection by index'
);
DROP TABLE scans_before, token_scans;
