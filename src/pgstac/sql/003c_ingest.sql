-- ---------------------------------------------------------------------------
-- Rust-first ingest: SECURITY DEFINER staging + flush + fragment helpers.
--
-- These are the server-side seam for the Rust loader's binary-COPY path. The connecting role never
-- writes the real tables directly; it binary-COPYs fully-dehydrated rows into a session-local TEMP
-- table (make_binary_staging) and calls flush_items_staging_binary, which is the only thing that writes
-- `items`. Partition existence + stats (extent + n) and registry/fragment coverage are established BEFORE
-- the load in their own transactions (prepare_partition_for_load / ensure_fragments); the flush writes only
-- `items` and never touches partition_stats.
-- ---------------------------------------------------------------------------

-- ensure_fragments: upsert per-collection fragment payloads and return, for each input position, the
-- item_fragments id the Rust loader stamps into items.fragment_id.
--
-- The caller (the Rust loader) deduplicates fragments locally and sends only the DISTINCT set (one input
-- element per unique fragment), then maps each item -> its fragment via the returned `ord`. Sending one
-- fragment per item would ship millions of duplicates when a collection has only a handful of distinct
-- fragments. The function is nonetheless dup-safe (a DISTINCT ON guards the insert), so a caller that
-- passes duplicates still gets the correct id for every position.
--
-- Each input element is {"content": <overlay or null>, "links_template": <array or null>}; the canonical
-- hash matches pgstac_hash_fragment (so it dedups identically to the SQL ingest path). ON CONFLICT keeps
-- existing rows; pre-existing ids come from item_fragments (snapshot), new ids from the INSERT's RETURNING.
CREATE OR REPLACE FUNCTION ensure_fragments(
    _collection text,
    _fragments jsonb[]
) RETURNS TABLE (ord int, frag_id bigint) AS $$
    WITH input AS (
        SELECT
            o::int AS ord,
            pgstac_hash_fragment(
                jsonb_strip_nulls(jsonb_build_object(
                    'content', NULLIF(f->'content', '{}'::jsonb),
                    'links_template', f->'links_template'
                ))
            ) AS hash,
            COALESCE(f->'content', '{}'::jsonb) AS content,
            f->'links_template' AS links_template
        FROM unnest(_fragments) WITH ORDINALITY AS u(f, o)
    ),
    distinct_hashes AS (
        SELECT DISTINCT ON (hash) hash, content, links_template
        FROM input
        ORDER BY hash
    ),
    upserted AS (
        INSERT INTO item_fragments (collection, hash, content, links_template)
        SELECT _collection, hash, content, links_template FROM distinct_hashes
        -- DO UPDATE (a no-op write) rather than DO NOTHING: it locks + RETURNS the conflicting row even when
        -- a CONCURRENT transaction committed the same (collection, hash) mid-statement. DO NOTHING returns
        -- nothing on conflict, and the old "UNION the existing rows via a separate SELECT" ran on the
        -- statement-start snapshot, so it could MISS such a concurrent insert -> that hash dropped out of the
        -- final join and the item was stamped with NO fragment_id. DO UPDATE returns every distinct hash
        -- exactly once (newly inserted or pre-existing), making the stamp concurrency-safe.
        ON CONFLICT (collection, hash) DO UPDATE SET content = item_fragments.content
        RETURNING id, hash
    )
    SELECT input.ord, upserted.id
    FROM input JOIN upserted ON input.hash = upserted.hash
    ORDER BY input.ord;
$$ LANGUAGE SQL SECURITY DEFINER;


-- ensure_partitions: create + widen every partition a batch will land in, BEFORE the load (widen-now).
-- The Rust loader passes parallel arrays of (collection, datetime, end_datetime) — one element per item.
-- Grouping happens HERE, server-side, so the month/year truncation uses the server's date_trunc (correct
-- timezone) rather than re-deriving partition boundaries in the client. One call per batch replaces the
-- loader's per-item check_partition round trips: it groups by (collection, partition window) and calls
-- check_partition once per distinct partition with that group's datetime range. Runs as its own committed
-- statement before the load transaction, so partitions exist + their stats cover the batch by the time
-- the binary COPY + flush run.
CREATE OR REPLACE FUNCTION ensure_partitions(
    _collections text[],
    _datetimes timestamptz[],
    _end_datetimes timestamptz[]
) RETURNS void AS $$
DECLARE
    r RECORD;
BEGIN
    -- Call check_partition per distinct (collection, partition window) in a DETERMINISTIC order: each
    -- check_partition takes that partition's advisory lock, so locking in a consistent order prevents two
    -- concurrent multi-partition batches from advisory-deadlocking on the locks. (flush locks in the same
    -- sorted order.) A PLPGSQL FOR loop guarantees the order; a set-returning SELECT would not.
    FOR r IN
        WITH t AS (
            SELECT
                unnest(_collections) AS collection,
                unnest(_datetimes) AS dt,
                unnest(_end_datetimes) AS edt
        ),
        j AS (
            SELECT t.collection, t.dt, t.edt, c.partition_trunc
            FROM t JOIN pgstac.collections c ON t.collection = c.id
        )
        SELECT
            collection,
            tstzrange(min(dt), max(dt), '[]') AS dtrange,
            tstzrange(min(edt), max(edt), '[]') AS edtrange
        FROM j
        GROUP BY collection, COALESCE(date_trunc(partition_trunc::text, dt), '-infinity'::timestamptz)
        ORDER BY collection, COALESCE(date_trunc(partition_trunc::text, dt), '-infinity'::timestamptz)
    LOOP
        PERFORM pgstac.check_partition(r.collection, r.dtrange, r.edtrange);
    END LOOP;
END;
$$ LANGUAGE PLPGSQL SECURITY DEFINER;


-- prepare_partition_for_load: per-partition metadata for the Rust direct / precheck load paths. ONE small
-- self-contained txn per partition, run BEFORE any COPY: create + widen the partition to cover this batch
-- (dt + edt + the real SPATIAL envelope, unlike ensure_partitions which passes NULL) and bump n, so
-- partition_stats is at least as wide as the data (golden rule) and search treats the partition as non-empty
-- before the data lands. Returns the partition name + the pre-load row count (n BEFORE the bump) so the
-- loader can choose its adaptive precheck path: empty -> skip the precheck; batch > n -> pull the partition's
-- (id,item_hash) to the client; n >= batch -> COPY the batch (id,hash) to a temp table + JOIN this partition.
CREATE OR REPLACE FUNCTION prepare_partition_for_load(
    _collection text,
    _dt_lo timestamptz, _dt_hi timestamptz,
    _edt_lo timestamptz, _edt_hi timestamptz,
    _xmin float8, _ymin float8, _xmax float8, _ymax float8,
    _n_add bigint,
    OUT partition_name text,
    OUT pre_load_n bigint
) AS $$
BEGIN
    partition_name := pgstac.check_partition(
        _collection,
        tstzrange(_dt_lo, _dt_hi, '[]'),
        tstzrange(_edt_lo, _edt_hi, '[]'),
        st_setsrid(st_makeenvelope(_xmin, _ymin, _xmax, _ymax), 4326)
    );
    SELECT COALESCE(n, 0) INTO pre_load_n
        FROM pgstac.partition_stats WHERE partition = partition_name;
    -- Over-estimating n is the safe direction; the async tightener computes the exact count + extent off the
    -- hot path. Single-row atomic UPDATE, so concurrent loads into the same partition serialize on the row.
    UPDATE pgstac.partition_stats
        SET n = COALESCE(n, 0) + _n_add, dirty = true, last_updated = now()
        WHERE partition = partition_name;
END;
$$ LANGUAGE PLPGSQL SECURITY DEFINER;


-- make_binary_staging: create a session-local TEMP staging table shaped exactly like `items`
-- (ON COMMIT DROP) and grant INSERT to pgstac_ingest so the connecting role can binary-COPY into it.
-- Created inside this SECURITY DEFINER function, so the temp table is owned by pgstac_admin (which also
-- owns `items`), letting flush_items_staging_binary read it. Returns the generated table name.
CREATE OR REPLACE FUNCTION make_binary_staging() RETURNS text AS $$
DECLARE
    _name text := format('_staging_%s', replace(gen_random_uuid()::text, '-', ''));
BEGIN
    EXECUTE format('CREATE TEMP TABLE %I (LIKE items) ON COMMIT DROP', _name);
    EXECUTE format('GRANT INSERT ON pg_temp.%I TO pgstac_ingest', _name);
    RETURN _name;
END;
$$ LANGUAGE PLPGSQL SECURITY DEFINER;


-- flush_items_staging_binary: move fully-dehydrated rows from a TEMP staging table into `items` with the
-- conflict policy. partition_stats (extent + n + dirty) is set entirely by prepare_partition_for_load in
-- the preflight, so this writes only `items`:
--        ignore  -> ON CONFLICT DO NOTHING (idempotent; orphans the old row on a cross-partition move)
--        upsert  -> delete a changed row IN THE PARTITION IT ROUTES TO (window-pruned), then insert. A
--                   datetime change that stays in the partition is applied; one that moves the item to a
--                   different partition orphans the old row (use 'delsert').
--        delsert -> delete a changed row CROSS-partition (by collection+id, move-safe), then insert
--        error   -> plain INSERT (raises on any duplicate)
-- Returns the number of rows inserted.
CREATE OR REPLACE FUNCTION flush_items_staging_binary(
    _staging text,
    _policy text DEFAULT 'ignore'
) RETURNS bigint AS $$
DECLARE
    nrows bigint;
BEGIN
    IF _policy = 'ignore' THEN
        EXECUTE format('INSERT INTO items SELECT * FROM %1$I ON CONFLICT DO NOTHING', _staging);
    ELSIF _policy = 'error' THEN
        EXECUTE format('INSERT INTO items SELECT * FROM %1$I', _staging);
    ELSIF _policy = 'upsert' THEN
        -- Fast SAME-partition upsert: delete a changed row only in the partition the incoming row routes to.
        -- The window range [date_trunc(trunc, s.datetime), + 1 trunc) is derived per staged row, so the
        -- planner runtime-prunes `items` to that one partition (no cross-partition scan). A datetime change
        -- within the partition is applied; one that MOVES the item to another partition isn't seen here, so
        -- the old row orphans (use 'delsert'). NULL partition_trunc => the collection's single partition.
        EXECUTE format($q$
            DELETE FROM items i USING %1$I s JOIN collections c ON c.id = s.collection
            WHERE i.collection = s.collection AND i.id = s.id
              AND i.datetime >= (CASE WHEN c.partition_trunc IS NULL THEN '-infinity'::timestamptz
                                      ELSE date_trunc(c.partition_trunc::text, s.datetime) END)
              AND i.datetime <  (CASE WHEN c.partition_trunc IS NULL THEN 'infinity'::timestamptz
                                      ELSE date_trunc(c.partition_trunc::text, s.datetime)
                                           + ('1 ' || c.partition_trunc::text)::interval END)
              AND ( %2$s )
        $q$, _staging, items_content_distinct_sql('i', 's'));
        EXECUTE format('INSERT INTO items SELECT * FROM %1$I ON CONFLICT DO NOTHING', _staging);
    ELSIF _policy = 'delsert' THEN
        -- Move-safe CROSS-partition upsert: delete the old row wherever it lives (by collection+id, no
        -- datetime bound, so it probes every partition), then insert.
        EXECUTE format($q$
            DELETE FROM items i USING %1$I s
            WHERE i.id = s.id AND i.collection = s.collection AND ( %2$s )
        $q$, _staging, items_content_distinct_sql('i', 's'));
        EXECUTE format('INSERT INTO items SELECT * FROM %1$I ON CONFLICT DO NOTHING', _staging);
    ELSE
        RAISE EXCEPTION 'unknown conflict policy % (expected ignore | upsert | delsert | error)', _policy;
    END IF;
    GET DIAGNOSTICS nrows = ROW_COUNT;
    RETURN nrows;
END;
$$ LANGUAGE PLPGSQL SECURITY DEFINER;
