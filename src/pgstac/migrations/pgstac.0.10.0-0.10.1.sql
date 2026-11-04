BEGIN;
SET client_min_messages TO WARNING;
SET SEARCH_PATH to pgstac, public;
RESET ROLE;

-- A lost unique id index can't be rebuilt here without blocking writes, so stop first.
DO $check$
DECLARE
    _missing text;
BEGIN
    IF to_regclass('pgstac.items') IS NULL THEN
        RETURN;
    END IF;
    SELECT string_agg(c.relname, ', ' ORDER BY c.relname) INTO _missing
    FROM pg_class c
    WHERE c.relkind = 'r' AND c.relispartition
        AND pg_partition_root(c.oid) = to_regclass('pgstac.items')
        AND NOT EXISTS (
            SELECT FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = i.indkey[0]
            WHERE i.indrelid = c.oid AND i.indisunique AND i.indisvalid AND i.indnkeyatts = 1
                AND i.indpred IS NULL AND i.indexprs IS NULL AND a.attname = 'id'
        );
    IF _missing IS NOT NULL THEN
        RAISE EXCEPTION 'Partitions without a unique id index: %', _missing
            USING HINT = 'For each, remove any duplicate ids, run CREATE UNIQUE INDEX CONCURRENTLY <partition>_pk ON pgstac.<partition> (id), and migrate again. Drop any invalid index a failed build leaves.';
    END IF;
END
$check$;
DO $$
DECLARE
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname='postgis') THEN
    CREATE EXTENSION IF NOT EXISTS postgis;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname='btree_gist') THEN
    CREATE EXTENSION IF NOT EXISTS btree_gist;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname='unaccent') THEN
    CREATE EXTENSION IF NOT EXISTS unaccent;
  END IF;
END;
$$ LANGUAGE PLPGSQL;

DO $$
  BEGIN
    CREATE ROLE pgstac_admin;
  EXCEPTION WHEN duplicate_object THEN
    RAISE NOTICE '%, skipping', SQLERRM USING ERRCODE = SQLSTATE;
  END
$$;

DO $$
  BEGIN
    CREATE ROLE pgstac_read;
  EXCEPTION WHEN duplicate_object THEN
    RAISE NOTICE '%, skipping', SQLERRM USING ERRCODE = SQLSTATE;
  END
$$;

DO $$
  BEGIN
    CREATE ROLE pgstac_ingest;
  EXCEPTION WHEN duplicate_object THEN
    RAISE NOTICE '%, skipping', SQLERRM USING ERRCODE = SQLSTATE;
  END
$$;


GRANT pgstac_admin TO current_user;

-- Function to make sure pgstac_admin is the owner of items
CREATE OR REPLACE FUNCTION pgstac_admin_owns() RETURNS VOID AS $$
DECLARE
  f RECORD;
BEGIN
  FOR f IN (
    SELECT
      concat(
        oid::regproc::text,
        '(',
        coalesce(pg_get_function_identity_arguments(oid),''),
        ')'
      ) AS name,
      CASE prokind WHEN 'f' THEN 'FUNCTION' WHEN 'p' THEN 'PROCEDURE' WHEN 'a' THEN 'AGGREGATE' END as typ
    FROM pg_proc
    WHERE
      pronamespace=to_regnamespace('pgstac')
      AND proowner != to_regrole('pgstac_admin')
      AND proname NOT LIKE 'pg_stat%'
  )
  LOOP
    BEGIN
      EXECUTE format('ALTER %s %s OWNER TO pgstac_admin;', f.typ, f.name);
    EXCEPTION WHEN others THEN
      RAISE NOTICE '%, skipping', SQLERRM USING ERRCODE = SQLSTATE;
    END;
  END LOOP;
  FOR f IN (
    SELECT
      oid::regclass::text as name,
      CASE relkind
        WHEN 'i' THEN 'INDEX'
        WHEN 'I' THEN 'INDEX'
        WHEN 'p' THEN 'TABLE'
        WHEN 'r' THEN 'TABLE'
        WHEN 'v' THEN 'VIEW'
        WHEN 'S' THEN 'SEQUENCE'
        ELSE NULL
      END as typ
    FROM pg_class
    WHERE relnamespace=to_regnamespace('pgstac') and relowner != to_regrole('pgstac_admin') AND relkind IN ('r','p','v','S') AND relname NOT LIKE 'pg_stat'
  )
  LOOP
    BEGIN
      EXECUTE format('ALTER %s %s OWNER TO pgstac_admin;', f.typ, f.name);
    EXCEPTION WHEN others THEN
      RAISE NOTICE '%, skipping', SQLERRM USING ERRCODE = SQLSTATE;
    END;
  END LOOP;
  RETURN;
END;
$$ LANGUAGE PLPGSQL;
SELECT pgstac_admin_owns();

CREATE SCHEMA IF NOT EXISTS pgstac AUTHORIZATION pgstac_admin;

GRANT ALL ON ALL FUNCTIONS IN SCHEMA pgstac to pgstac_admin;
GRANT ALL ON ALL TABLES IN SCHEMA pgstac to pgstac_admin;
GRANT ALL ON ALL SEQUENCES IN SCHEMA pgstac to pgstac_admin;

ALTER ROLE pgstac_admin SET SEARCH_PATH TO pgstac, public;
ALTER ROLE pgstac_read SET SEARCH_PATH TO pgstac, public;
ALTER ROLE pgstac_ingest SET SEARCH_PATH TO pgstac, public;

GRANT USAGE ON SCHEMA pgstac to pgstac_read;
ALTER DEFAULT PRIVILEGES IN SCHEMA pgstac GRANT SELECT ON TABLES TO pgstac_read;
ALTER DEFAULT PRIVILEGES IN SCHEMA pgstac GRANT USAGE ON TYPES TO pgstac_read;
ALTER DEFAULT PRIVILEGES IN SCHEMA pgstac GRANT ALL ON SEQUENCES TO pgstac_read;

GRANT pgstac_read TO pgstac_ingest;
GRANT ALL ON SCHEMA pgstac TO pgstac_ingest;
ALTER DEFAULT PRIVILEGES IN SCHEMA pgstac GRANT ALL ON TABLES TO pgstac_ingest;
ALTER DEFAULT PRIVILEGES IN SCHEMA pgstac GRANT ALL ON FUNCTIONS TO pgstac_ingest;

SET ROLE pgstac_admin;
ALTER DEFAULT PRIVILEGES FOR ROLE pgstac_admin IN SCHEMA pgstac GRANT SELECT ON TABLES TO pgstac_read;
ALTER DEFAULT PRIVILEGES FOR ROLE pgstac_admin IN SCHEMA pgstac GRANT USAGE ON TYPES TO pgstac_read;
ALTER DEFAULT PRIVILEGES FOR ROLE pgstac_admin IN SCHEMA pgstac GRANT ALL ON SEQUENCES TO pgstac_read;
ALTER DEFAULT PRIVILEGES FOR ROLE pgstac_admin IN SCHEMA pgstac GRANT ALL ON TABLES TO pgstac_ingest;
ALTER DEFAULT PRIVILEGES FOR ROLE pgstac_admin IN SCHEMA pgstac GRANT ALL ON FUNCTIONS TO pgstac_ingest;
RESET ROLE;

SET ROLE pgstac_ingest;
ALTER DEFAULT PRIVILEGES FOR ROLE pgstac_ingest IN SCHEMA pgstac GRANT SELECT ON TABLES TO pgstac_read;
ALTER DEFAULT PRIVILEGES FOR ROLE pgstac_ingest IN SCHEMA pgstac GRANT USAGE ON TYPES TO pgstac_read;
ALTER DEFAULT PRIVILEGES FOR ROLE pgstac_ingest IN SCHEMA pgstac GRANT ALL ON SEQUENCES TO pgstac_read;
ALTER DEFAULT PRIVILEGES FOR ROLE pgstac_ingest IN SCHEMA pgstac GRANT ALL ON TABLES TO pgstac_ingest;
ALTER DEFAULT PRIVILEGES FOR ROLE pgstac_ingest IN SCHEMA pgstac GRANT ALL ON FUNCTIONS TO pgstac_ingest;
RESET ROLE;

SET SEARCH_PATH TO pgstac, public;

-- PgSTAC references PostGIS without schema qualification and runs with
-- search_path pinned to "pgstac, public", so PostGIS has to be reachable from
-- there. Fail here rather than part way through creating the schema.
DO $$
  BEGIN
    IF to_regtype('geometry') IS NULL THEN
      RAISE EXCEPTION 'PostGIS is not reachable from search_path "pgstac, public"'
        USING HINT = 'PgSTAC requires the postgis extension in the public schema.';
    END IF;
  END
$$;

SET ROLE pgstac_admin;

DO $$
  BEGIN
    DROP FUNCTION IF EXISTS analyze_items;
  EXCEPTION WHEN others THEN
    RAISE NOTICE '%, skipping', SQLERRM USING ERRCODE = SQLSTATE;
  END
$$;
DO $$
  BEGIN
    DROP FUNCTION IF EXISTS validate_constraints;
  EXCEPTION WHEN others THEN
    RAISE NOTICE '%, skipping', SQLERRM USING ERRCODE = SQLSTATE;
  END
$$;
-- The argument list differs from the installed signature, which CREATE OR REPLACE cannot
-- change, so both would otherwise exist at once. 998_idempotent_post addresses maintain_index by
-- bare name and would fail with "function name is not unique" if the other overload survived.
DROP FUNCTION IF EXISTS maintain_index(text, text, bigint, boolean, boolean, boolean);

-- Install these idempotently as migrations do not put them before trying to modify the collections table


CREATE OR REPLACE FUNCTION collection_geom(content jsonb)
RETURNS geometry AS $$
    WITH box AS (SELECT content->'extent'->'spatial'->'bbox'->0 as box)
    SELECT
        st_makeenvelope(
            (box->>0)::float,
            (box->>1)::float,
            (box->>2)::float,
            (box->>3)::float,
            4326
        )
    FROM box;
$$ LANGUAGE SQL IMMUTABLE STRICT SET search_path TO pgstac, public;

CREATE OR REPLACE FUNCTION collection_datetime(content jsonb)
RETURNS timestamptz AS $$
    SELECT
        CASE
            WHEN
                (content->'extent'->'temporal'->'interval'->0->>0) IS NULL
            THEN '-infinity'::timestamptz
            ELSE
                (content->'extent'->'temporal'->'interval'->0->>0)::timestamptz
        END
    ;
$$ LANGUAGE SQL IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION collection_enddatetime(content jsonb)
RETURNS timestamptz AS $$
    SELECT
        CASE
            WHEN
                (content->'extent'->'temporal'->'interval'->0->>1) IS NULL
            THEN 'infinity'::timestamptz
            ELSE
                (content->'extent'->'temporal'->'interval'->0->>1)::timestamptz
        END
    ;
$$ LANGUAGE SQL IMMUTABLE STRICT;

-- The keys a content->'a'->'b' chain names, in order, NULL for anything else; a doubled quote
-- stands for one quote in a key. Read by the conversion below and dropped with it, so it never
-- joins the schema the diff that follows is calculated against. A path this cannot read becomes
-- NULL, and the conversion below stops on those rows.
CREATE OR REPLACE FUNCTION content_keys(_path text) RETURNS text[] AS $fn$
    SELECT array_agg(replace(m[1], '''''', '''') ORDER BY o)
    FROM regexp_matches(_path, $re$->\s*'((?:[^']|'')*)'$re$, 'g') WITH ORDINALITY AS u(m, o)
    WHERE _path ~ $re$^\s*content(\s*->\s*'(?:[^']|'')*')+\s*$$re$;
$fn$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;

-- property_path held the SQL path expression a queryable was read through; it now holds the keys
-- that expression named. The plain cast the migration makes of the column fails on a legacy value,
-- so convert here, before it runs.
DO $conv$
  DECLARE
    _lost text;
  BEGIN
    -- Guarded because a plain ALTER fails on the second run, and because content_keys takes the
    -- column's old type: once converted, the name no longer resolves.
    IF EXISTS (
      SELECT FROM pg_attribute
      WHERE attrelid = to_regclass('pgstac.queryables')
        AND attname = 'property_path'
        AND atttypid = 'text'::regtype
    ) THEN
      -- A value this cannot read would re-point the queryable at the keys of its own name.
      SELECT string_agg(format('%s (%s)', name, property_path), ', ' ORDER BY id) INTO _lost
      FROM queryables
      WHERE property_path IS NOT NULL AND content_keys(property_path) IS NULL;
      IF _lost IS NOT NULL THEN
        RAISE EXCEPTION 'Queryable property_path cannot be converted: %', _lost
          USING HINT = 'Rewrite each as content->''key'' steps, or set it to NULL to use the queryable''s name.';
      END IF;
      ALTER TABLE queryables ALTER COLUMN property_path TYPE text[] USING content_keys(property_path);
    END IF;
  END
$conv$;
DROP FUNCTION content_keys(text);
-- BEGIN migra calculated SQL
set check_function_bodies = off;

CREATE OR REPLACE FUNCTION pgstac.canonicalize_queryables()
 RETURNS void
 LANGUAGE plpgsql
 SET search_path TO 'pgstac', 'public'
AS $function$
DECLARE
    r record;
    _lost text;
BEGIN
    -- Nothing here may UPDATE a row before the table is canonical: the constraint trigger fires
    -- on every UPDATE and checks the row against rows the rest of this function has not fixed
    -- yet, so a legacy pair collides and the whole migration aborts. Everything below is
    -- therefore a DELETE until the single rewrite at the end.

    -- collection_ids naming a collection that no longer exists is the ordinary state of a
    -- long-lived catalog, not corruption. An empty array means every collection, not a row that
    -- applies to none, so it survives and becomes NULL in the rewrite. A row that names only
    -- collections which are gone does not.
    DELETE FROM queryables
      WHERE collection_ids IS NOT NULL
        AND collection_ids <> '{}'
        AND NOT EXISTS (
            SELECT FROM unnest(collection_ids) c WHERE EXISTS (SELECT FROM collections WHERE id = c));

    -- Taken before the dedupe so a removed row's configuration can still be merged into the row
    -- that survives it: a bare stub from missing_queryables is usually older than the row an
    -- operator configured by hand, and discarding the latter silently downgrades the wrapper and
    -- orphans its index. pg_temp so the name cannot resolve to a real table through search_path.
    DROP TABLE IF EXISTS pg_temp._canon_snapshot;
    CREATE TEMP TABLE _canon_snapshot ON COMMIT DROP AS SELECT * FROM queryables;

    -- Oldest first, dropping a row only when a row that has already been KEPT conflicts with it.
    -- Deleting the newest conflicting row per name instead would drop a row whose only conflict
    -- is with something removed in a later round: of {A}, {A,B}, {B} the middle row goes and
    -- both ends must stay, or collection B silently loses the queryable altogether.
    FOR r IN SELECT id FROM queryables ORDER BY id LOOP
        DELETE FROM queryables a
        WHERE a.id = r.id
          AND EXISTS (
              SELECT FROM queryables b
              WHERE b.id < a.id
                AND strip_properties_prefix(b.name) = strip_properties_prefix(a.name)
                AND (canonical_collection_ids(a.collection_ids) IS NULL
                  OR canonical_collection_ids(b.collection_ids) IS NULL
                  OR a.collection_ids && b.collection_ids));
    END LOOP;

    -- Stop if the dedupe left a collection, or every collection, without a queryable it had.
    SELECT string_agg(DISTINCT format('%s (%s)', s.name, coalesce(c.id, 'all collections')), ', ') INTO _lost
    FROM _canon_snapshot s
    LEFT JOIN collections c ON c.id = ANY (s.collection_ids)
    WHERE (canonical_collection_ids(s.collection_ids) IS NULL OR c.id IS NOT NULL)
        AND NOT EXISTS (
            SELECT FROM queryables q
            WHERE strip_properties_prefix(q.name) = strip_properties_prefix(s.name)
                AND (canonical_collection_ids(q.collection_ids) IS NULL OR c.id = ANY (q.collection_ids)));
    IF _lost IS NOT NULL THEN
        RAISE EXCEPTION 'Merging duplicate queryables would leave collections without a queryable: %', _lost
            USING HINT = 'Delete or narrow one of each duplicate, or give the listed collections their own row.';
    END IF;

    -- What each SURVIVING ROW inherits, field by field, from the removed rows that actually
    -- conflicted with IT. Keyed per row rather than per name: two rows of one name can survive
    -- naming different collections, and handing one of them the other's wrapper would build an
    -- index and read a property through a wrapper that collection never asked for.
    DROP TABLE IF EXISTS pg_temp._canon_merge;
    CREATE TEMP TABLE _canon_merge ON COMMIT DROP AS
    WITH gone AS (
        SELECT s.* FROM _canon_snapshot s
        WHERE NOT EXISTS (SELECT FROM queryables q WHERE q.id = s.id)
    )
    SELECT q.id AS winner,
           first_notnull(g.definition ORDER BY g.id) AS definition,
           first_notnull(g.property_wrapper ORDER BY g.id) AS property_wrapper,
           first_notnull(g.property_index_type ORDER BY g.id) AS property_index_type,
           first_notnull(g.property_path ORDER BY g.id) AS property_path
    FROM queryables q
    JOIN gone g
      ON strip_properties_prefix(g.name) = strip_properties_prefix(q.name)
     AND (canonical_collection_ids(g.collection_ids) IS NULL
       OR canonical_collection_ids(q.collection_ids) IS NULL
       OR g.collection_ids && q.collection_ids)
    GROUP BY q.id;

    -- Checked here, before the rewrite below can fire the trigger's barer message. A custom
    -- wrapper may legitimately be registered, so name the row and say how to register it rather
    -- than failing with a wrapper name alone.
    FOR r IN
        SELECT q.name,
               queryable_wrapper(
                   coalesce(q.property_wrapper, m.property_wrapper),
                   coalesce(q.definition, m.definition)) AS wrapper
        FROM queryables q
        LEFT JOIN _canon_merge m ON m.winner = q.id
    LOOP
        IF NOT EXISTS (SELECT FROM queryable_wrappers WHERE name = r.wrapper) THEN
            RAISE check_violation USING
                MESSAGE = format('Queryable %s uses the property_wrapper %s, which is not registered.',
                                 r.name, r.wrapper),
                HINT = format(
                    'Register it with INSERT INTO queryable_wrappers (name) VALUES (%L); pgstac.%I(jsonb) must exist.',
                    r.wrapper, r.wrapper);
        END IF;
    END LOOP;

    -- The one rewrite: name, collection_ids, the merged configuration and the index type items
    -- reserves for itself, all at once, so the trigger only ever sees a finished row.
    UPDATE queryables q SET
        name = strip_properties_prefix(q.name),
        collection_ids = m.cids,
        definition = coalesce(q.definition, m.definition),
        property_wrapper = coalesce(q.property_wrapper, m.property_wrapper),
        property_path = coalesce(q.property_path, m.property_path),
        property_index_type = CASE
            WHEN queryable_column(strip_properties_prefix(q.name)) IS NULL
            THEN coalesce(q.property_index_type, m.property_index_type) END
    FROM (SELECT q2.id, cm.definition, cm.property_wrapper, cm.property_index_type,
                 cm.property_path,
                 canonical_collection_ids(
                     CASE WHEN q2.collection_ids IS NULL THEN NULL ELSE ARRAY(
                         SELECT c FROM unnest(q2.collection_ids) c
                         WHERE EXISTS (SELECT FROM collections WHERE id = c)) END) AS cids
          FROM queryables q2 LEFT JOIN _canon_merge cm ON cm.winner = q2.id) m
    WHERE m.id = q.id
      -- Only rows that actually change, so an unchanged indexed row does not fire the reference
      -- index trigger and rebuild an index the upgrade was supposed to leave alone.
      AND (q.name IS DISTINCT FROM strip_properties_prefix(q.name)
        OR q.collection_ids IS DISTINCT FROM m.cids
        OR (q.property_index_type IS NOT NULL
            AND queryable_column(strip_properties_prefix(q.name)) IS NOT NULL)
        OR m.definition IS NOT NULL OR m.property_wrapper IS NOT NULL
        OR m.property_index_type IS NOT NULL OR m.property_path IS NOT NULL);

    -- Build every indexed row's reference index here, where a failure names the row and stops the
    -- upgrade. The whole-table walk in 998 only warns, and the row trigger above fires only for
    -- rows this function happened to rewrite, so without this whether an unbuildable index type
    -- halts the migration depends on whether the row's name needed stripping.
    PERFORM maintain_reference_index(id) FROM queryables WHERE property_index_type IS NOT NULL;
END;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.collection_search(_search jsonb DEFAULT '{}'::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE PARALLEL SAFE
AS $function$
DECLARE
    out_records jsonb;
    number_matched bigint := collection_search_matched(_search);
    number_returned bigint;
    -- bigint so _offset + _limit cannot overflow.
    _limit bigint := search_limit(_search);
    _offset bigint := search_offset(_search);
    links jsonb := '[]';
    ret jsonb;
    base_url text:= concat(rtrim(base_url(_search->'conf'),'/'), '/collections');
    prevoffset int;
    nextoffset int;
BEGIN
    SELECT
        coalesce(jsonb_agg(c), '[]')
    INTO out_records
    FROM collection_search_rows(_search) c;

    number_returned := jsonb_array_length(out_records);
    RAISE DEBUG 'nm: %, nr: %, l:%, o:%', number_matched, number_returned, _limit, _offset;



    -- a prev link depends only on being past the first page, a next link only on rows remaining
    IF _offset > 0 THEN
        -- Stepped back from the last offset that can hold rows, so an offset past the end does
        -- not walk back one empty page at a time, and by at least one so a limit of 0 does not
        -- produce a prev link pointing at the page that produced it.
        prevoffset := greatest(least(_offset, number_matched) - greatest(_limit, 1), 0);
        links := links || jsonb_build_object(
                'rel', 'prev',
                'type', 'application/json',
                'method', 'GET' ,
                'href', base_url,
                'body', jsonb_build_object('offset', prevoffset),
                'merge', TRUE
            );
    END IF;

    IF _limit > 0 AND _offset + _limit < number_matched THEN
        nextoffset := _offset + _limit;
        links := links || jsonb_build_object(
                'rel', 'next',
                'type', 'application/json',
                'method', 'GET' ,
                'href', base_url,
                'body', jsonb_build_object('offset', nextoffset),
                'merge', TRUE
            );
    END IF;

    ret := jsonb_build_object(
        'collections', out_records,
        'numberMatched', number_matched,
        'numberReturned', number_returned,
        'links', links
    );
    RETURN ret;

END;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.maintain_index(_partition text, _indexname text, _queryable_id bigint, dropindexes boolean DEFAULT false, rebuildindexes boolean DEFAULT false)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'pgstac', 'public'
AS $function$
DECLARE
    _queryable_idx text;
BEGIN
    -- Runs elevated, so it may only touch partitions of items.
    IF NOT EXISTS (SELECT 1 FROM partition_catalog_meta(_partition)) THEN
        RETURN;
    END IF;
    IF _indexname IS NOT NULL AND NOT EXISTS (
        SELECT 1 FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
        WHERE c.relnamespace = 'pgstac'::regnamespace
            AND c.relname = _indexname
            AND i.indrelid = partition_oid(_partition)
    ) THEN
        RAISE EXCEPTION '% is not an index on %', _indexname, _partition
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF _indexname IS NULL THEN
        SELECT indexdef_unnamed(pg_get_indexdef(reference_index(q)), _partition) INTO _queryable_idx
        FROM queryables q WHERE q.id = _queryable_id;
        IF _queryable_idx IS NOT NULL THEN
            EXECUTE _queryable_idx;
        END IF;
    -- Drops only what queryable_indexes reports as an orphan; a NULL _queryable_id is not trusted.
    ELSIF _queryable_id IS NULL AND dropindexes AND EXISTS (
        SELECT FROM queryable_indexes(_partition, true) qi
        WHERE qi.indexname = _indexname AND qi.queryable_id IS NULL
    ) THEN
        EXECUTE format('DROP INDEX IF EXISTS %I;', _indexname);
    ELSIF rebuildindexes THEN
        EXECUTE format('REINDEX INDEX %I;', _indexname);
    ELSIF _queryable_id IS NULL AND dropindexes THEN
        RAISE EXCEPTION '% on % is not an unpaired index', _indexname, _partition
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
END;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.queryable_indexes(treeroot text DEFAULT 'items'::text, changes boolean DEFAULT false, OUT collection text, OUT partition text, OUT field text, OUT indexname text, OUT existing_idx text, OUT queryable_idx text, OUT queryable_id bigint)
 RETURNS SETOF record
 LANGUAGE sql
AS $function$
WITH p AS (
        SELECT
            relid::text as partition,
            partition_collection(
                CASE WHEN parentrelid::regclass::text='items' THEN c.oid ELSE parent.oid END
            ) AS collection
        FROM pg_partition_tree(treeroot)
        JOIN pg_class c ON (relid::regclass = c.oid)
        JOIN pg_class parent ON (parentrelid::regclass = parent.oid AND isleaf)
    ), i AS (
        SELECT partition, indexname, indexdef_unnamed(indexdef, partition) AS iidx
        FROM pg_indexes JOIN p ON (tablename = partition)
        WHERE schemaname = 'pgstac'
    ), r AS (
        SELECT q.id, q.name, q.collection_ids, pg_get_indexdef(ri.oid) AS refdef
        FROM queryables q
        CROSS JOIN LATERAL (SELECT reference_index(q) AS oid) ri
        WHERE ri.oid IS NOT NULL
    ), structural AS (
        -- The indexes items itself carries, which every partition has and no queryables row
        -- derives, so they would read as orphans. Matched against items' own indexes rather than
        -- by excluding what a queryables row points at: reference_index matches a column-named
        -- queryable whatever collections it names, so narrowing the id queryable to one would
        -- leave every other partition's _items_N_pk open to dropindexes. Normalised to one fixed
        -- name, which makes the comparison independent of the partition.
        SELECT indexdef_unnamed(pg_get_indexdef(t.indexrelid), 'x') AS sdx
        FROM pg_index t
        WHERE t.indrelid = to_regclass('pgstac.queryable_index_template')
          AND EXISTS (
              SELECT FROM pg_index ii
              WHERE ii.indrelid = to_regclass('pgstac.items')
                AND indexdef_unnamed(pg_get_indexdef(ii.indexrelid), 'x')
                    = indexdef_unnamed(pg_get_indexdef(t.indexrelid), 'x'))
    ), wanted AS (
        SELECT p.collection, p.partition, r.name AS field, indexdef_unnamed(r.refdef, p.partition) AS qidx, r.id AS qid
        FROM r JOIN p ON (r.collection_ids IS NULL)
        UNION ALL
        SELECT p.collection, p.partition, r.name, indexdef_unnamed(r.refdef, p.partition), r.id
        FROM r CROSS JOIN LATERAL unnest(r.collection_ids) c JOIN p ON (p.collection = c)
    ), q AS (
        -- One index per distinct definition on a partition. Two queryables may legitimately
        -- resolve to the same index -- the same keys through the same wrapper -- and without this
        -- each asks for its own, so the partition ends up with byte-identical duplicates that
        -- pair with one another and are never reported as orphans.
        SELECT DISTINCT ON (partition, qidx) collection, partition, field, qidx, qid
        FROM wanted
        ORDER BY partition, qidx, qid
    )
    SELECT
        collection,
        COALESCE(i.partition, q.partition),
        field,
        indexname,
        iidx,
        qidx,
        qid
    FROM i FULL JOIN q ON (i.partition = q.partition AND i.iidx = q.qidx)
    WHERE NOT changes
        OR indexname IS NULL
        -- Never an orphan: a structural index, or a unique index, which is what stops duplicate ids.
        OR (qid IS NULL AND NOT i.iidx ^@ 'CREATE UNIQUE ' AND NOT EXISTS (
                SELECT FROM structural
                WHERE structural.sdx = indexdef_unnamed(i.iidx, 'x')));
$function$
;

CREATE OR REPLACE FUNCTION pgstac.search(_search jsonb DEFAULT '{}'::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
AS $function$
DECLARE
    searches searches%ROWTYPE;
    _where text;
    orderby text;
    search_where search_wheres%ROWTYPE;
    total_count bigint;
    token record;
    token_prev boolean;
    token_item items%ROWTYPE;
    token_where text;
    full_where text;
    init_ts timestamptz := clock_timestamp();
    timer timestamptz := clock_timestamp();
    hydrate bool := NOT (_search->'conf'->>'nohydrate' IS NOT NULL AND (_search->'conf'->>'nohydrate')::boolean = true);
    prev text;
    next text;
    collection jsonb;
    out_records jsonb;
    -- The (collection, id) of each row in out_records, in the same order. The tokens are
    -- built from these rather than from the formatted features, which fields.exclude can
    -- strip of exactly the two members a token needs.
    out_keys jsonb;
    out_len int;
    _limit int := search_limit(_search);
    _querylimit int;
    _fields jsonb := coalesce(_search->'fields', '{}'::jsonb);
    has_prev boolean := FALSE;
    has_next boolean := FALSE;
    links jsonb := '[]'::jsonb;
    base_url text:= concat(rtrim(base_url(_search->'conf'),'/'));
BEGIN
    searches := search_query(_search);
    _where := searches._where;
    orderby := searches.orderby;
    search_where := where_stats(_where, false, _search->'conf');
    total_count := coalesce(search_where.total_count, search_where.estimated_count);
    RAISE DEBUG 'SEARCH:TOKEN: %', _search->>'token';
    token := get_token_record(_search->>'token');
    -- One row past the page shows whether there is another page; clamped so it cannot overflow.
    _querylimit := least(_limit, 2147483646) + 1;
    IF token IS NOT NULL THEN
        token_prev := token.prev;
        token_item := token.item;
        token_where := get_token_filter(_search->'sortby', token_item, token_prev, FALSE, to_text_array(_search->'collections'));
        RAISE DEBUG 'TOKEN_WHERE: % (%ms from search start)', token_where, age_ms(timer);
        IF token_prev THEN -- if we are using a prev token, we know has_next is true
            RAISE DEBUG 'There is a previous token, so automatically setting has_next to true';
            has_next := TRUE;
            orderby := sort_sqlorderby(_search, TRUE, '{collection,id}', to_text_array(_search->'collections'));
        ELSE
            RAISE DEBUG 'There is a next token, so automatically setting has_prev to true';
            has_prev := TRUE;

        END IF;
    ELSE -- if there was no token, we know there is no prev
        RAISE DEBUG 'There is no token, so we know there is no prev. setting has_prev to false';
        has_prev := FALSE;
    END IF;

    full_where := concat_ws(' AND ', _where, token_where);
    RAISE DEBUG 'FULL WHERE CLAUSE: %', full_where;
    RAISE DEBUG 'Time to get counts and build query %', age_ms(timer);
    timer := clock_timestamp();

    IF hydrate THEN
        RAISE DEBUG 'Getting hydrated data.';
    ELSE
        RAISE DEBUG 'Getting non-hydrated data.';
    END IF;
    RAISE DEBUG 'CACHE SET TO %', get_setting_bool('format_cache');
    RAISE DEBUG 'Time to set hydration/formatting %', age_ms(timer);
    timer := clock_timestamp();
    SELECT
        jsonb_agg(format_item(i, _fields, hydrate)),
        jsonb_agg(jsonb_build_array(i.collection, i.id))
    INTO out_records, out_keys
    FROM search_rows(
        full_where,
        orderby,
        search_where.partitions,
        _querylimit
    ) as i;

    RAISE DEBUG 'Time to fetch rows %', age_ms(timer);
    timer := clock_timestamp();


    IF token_prev THEN
        out_records := flip_jsonb_array(out_records);
        out_keys := flip_jsonb_array(out_keys);
    END IF;

    RAISE DEBUG 'Query returned % records.', jsonb_array_length(out_records);
    RAISE DEBUG 'TOKEN:   % %', token_item.id, token_item.collection;
    RAISE DEBUG 'RECORD_1: % %', out_keys->0->>1, out_keys->0->>0;
    RAISE DEBUG 'RECORD-1: % %', out_keys->-1->>1, out_keys->-1->>0;

    -- REMOVE records that were from our token
    IF out_keys->0->>0 = token_item.collection AND out_keys->0->>1 = token_item.id THEN
        out_records := out_records - 0;
        out_keys := out_keys - 0;
    ELSIF out_keys->-1->>0 = token_item.collection AND out_keys->-1->>1 = token_item.id THEN
        out_records := out_records - -1;
        out_keys := out_keys - -1;
    END IF;

    IF jsonb_array_length(out_records) = _querylimit THEN
        IF token_prev THEN
            has_prev := TRUE;
            out_records := out_records - 0;
            out_keys := out_keys - 0;
        ELSE
            has_next := TRUE;
            out_records := out_records - -1;
            out_keys := out_keys - -1;
        END IF;
    END IF;
    out_len := coalesce(jsonb_array_length(out_records), 0);

    links := links || jsonb_build_object(
        'rel', 'root',
        'type', 'application/json',
        'href', base_url
    ) || jsonb_build_object(
        'rel', 'self',
        'type', 'application/json',
        'href', concat(base_url, '/search')
    );

    -- An empty page still anchors on the token that produced it, so the caller has a way back
    -- instead of a dead end. A link identical to the incoming token is dropped: following it
    -- would return this same page forever.
    IF has_next AND out_len > 0 THEN
        next := page_token(out_keys->-1->>0, out_keys->-1->>1);
        RAISE DEBUG 'HAS NEXT | %', next;
        links := links || jsonb_build_object(
            'rel', 'next',
            'type', 'application/geo+json',
            'method', 'GET',
            'href', concat(base_url, '/search?token=next:', next)
        );
    END IF;

    IF has_prev AND (out_len > 0 OR _limit = 0) THEN
        prev := CASE WHEN out_len > 0
            THEN page_token(out_keys->0->>0, out_keys->0->>1)
            ELSE page_token(token_item.collection, token_item.id)
        END;
        -- Never hand back the token that produced this page: limit 0 reached by a prev token
        -- anchored the prev link on that same token, so a client following it never advanced.
        -- Compared lowered, as get_token_record reads the token, so PREV: cannot reopen the
        -- loop this guard closes.
        IF lower(concat('prev:', prev)) IS DISTINCT FROM lower(_search->>'token') THEN
            RAISE DEBUG 'HAS PREV | %', prev;
            links := links || jsonb_build_object(
                'rel', 'prev',
                'type', 'application/geo+json',
                'method', 'GET',
                'href', concat(base_url, '/search?token=prev:', prev)
            );
        ELSE
            prev := NULL;
        END IF;
    END IF;

    RAISE DEBUG 'Time to get prev/next %', age_ms(timer);
    timer := clock_timestamp();


    collection := jsonb_build_object(
        'type', 'FeatureCollection',
        'features', coalesce(out_records, '[]'::jsonb),
        'links', links
    );



    collection := collection || jsonb_build_object('numberReturned', out_len);
    IF context(_search->'conf') != 'off' THEN
        collection := collection || jsonb_strip_nulls(jsonb_build_object('numberMatched', total_count));
    END IF;

    IF get_setting_bool('timing', _search->'conf') THEN
        collection = collection || jsonb_build_object('timing', age_ms(init_ts));
    END IF;

    RAISE DEBUG 'Time to build final json %', age_ms(timer);
    timer := clock_timestamp();

    RAISE DEBUG 'Total Time: %', age_ms(current_timestamp);
    RAISE DEBUG 'RETURNING % records. NEXT: %. PREV: %', collection->>'numberReturned', collection->>'next', collection->>'prev';
    RETURN collection;
END;
$function$
;


-- END migra calculated SQL
-- Before the queryables below: their trigger accepts only a registered wrapper.
INSERT INTO queryable_wrappers (name) VALUES
  ('to_int'), ('to_float'), ('to_tstz'), ('to_text'), ('to_text_array')
ON CONFLICT DO NOTHING;

INSERT INTO queryables (name, definition)
  SELECT * FROM (VALUES
    ('id', '{"title": "Item ID","description": "Item identifier","$ref": "https://schemas.stacspec.org/v1.0.0/item-spec/json-schema/item.json#/definitions/core/allOf/2/properties/id"}'::jsonb),
    ('geometry', '{"title": "Item Geometry","description": "Item Geometry","$ref": "https://geojson.org/schema/Feature.json#/properties/geometry"}'),
    ('datetime', '{"description": "Datetime","type": "string","title": "Acquired","format": "date-time","pattern": "(\\+00:00|Z)$"}')
  ) v (name, definition)
  WHERE NOT EXISTS (SELECT FROM queryables WHERE name = v.name);

-- Rewrites rows an older release stored in another spelling; a no-op otherwise.
SELECT canonicalize_queryables();

-- Point the geometry queryable at the Feature's geometry if it is set to the whole Feature.
UPDATE queryables
SET definition = '{"title": "Item Geometry","description": "Item Geometry","$ref": "https://geojson.org/schema/Feature.json#/properties/geometry"}'
WHERE name = 'geometry' AND collection_ids IS NULL
  AND definition = '{"title": "Item Geometry","description": "Item Geometry","$ref": "https://geojson.org/schema/Feature.json"}'::jsonb;

-- Reference indexes for rows that predate them; a row whose index cannot be built is warned about.
SELECT maintain_reference_index();


INSERT INTO pgstac_settings (name, value) VALUES
  ('context', 'off'),
  ('context_estimated_count', '100000'),
  ('context_estimated_cost', '100000'),
  ('context_stats_ttl', '1 day'),
  ('default_filter_lang', 'cql2-json'),
  ('additional_properties', 'true'),
  ('use_queue', 'false'),
  ('queue_timeout', '10 minutes'),
  ('queue_retries', '3'),
  ('update_collection_extent', 'false'),
  ('format_cache', 'false'),
  ('readonly', 'false')
ON CONFLICT DO NOTHING
;


INSERT INTO cql2_ops (op, template) VALUES
    ('eq', '%s = %s'),
    ('neq', '%s != %s'),
    ('ne', '%s != %s'),
    ('!=', '%s != %s'),
    ('<>', '%s != %s'),
    ('lt', '%s < %s'),
    ('lte', '%s <= %s'),
    ('gt', '%s > %s'),
    ('gte', '%s >= %s'),
    ('le', '%s <= %s'),
    ('ge', '%s >= %s'),
    ('=', '%s = %s'),
    ('<', '%s < %s'),
    ('<=', '%s <= %s'),
    ('>', '%s > %s'),
    ('>=', '%s >= %s'),
    ('like', '%s LIKE %s'),
    ('ilike', '%s ILIKE %s'),
    ('+', '%s + %s'),
    ('-', '%s - %s'),
    ('*', '%s * %s'),
    ('/', '%s / %s'),
    ('not', 'NOT (%s)'),
    ('between', '%s BETWEEN %s AND %s'),
    ('isnull', '%s IS NULL'),
    ('upper', 'upper(%s)'),
    ('lower', 'lower(%s)'),
    ('casei', 'upper(%s)'),
    ('accenti', 'unaccent(%s)')
ON CONFLICT (op) DO UPDATE
    SET
        template = EXCLUDED.template
;


ALTER FUNCTION to_text COST 5000;
ALTER FUNCTION to_float COST 5000;
ALTER FUNCTION to_int COST 5000;
ALTER FUNCTION to_tstz COST 5000;
ALTER FUNCTION to_text_array COST 5000;

ALTER FUNCTION drop_table_constraints SECURITY DEFINER;
ALTER FUNCTION create_table_constraints SECURITY DEFINER;
ALTER FUNCTION check_partition SECURITY DEFINER;
ALTER FUNCTION repartition SECURITY DEFINER;
ALTER FUNCTION maintain_index SECURITY DEFINER;
ALTER FUNCTION maintain_reference_index SECURITY DEFINER;
ALTER FUNCTION collection_delete_trigger_func SECURITY DEFINER;

-- Created SECURITY INVOKER; these reset databases migrated from a release
-- that created them SECURITY DEFINER.
ALTER FUNCTION where_stats SECURITY INVOKER;
ALTER FUNCTION search_query SECURITY INVOKER;
ALTER FUNCTION format_item SECURITY INVOKER;

GRANT USAGE ON SCHEMA pgstac to pgstac_read;
GRANT ALL ON SCHEMA pgstac to pgstac_ingest;
GRANT ALL ON SCHEMA pgstac to pgstac_admin;

-- pgstac_read role limited to using function apis
GRANT EXECUTE ON FUNCTION search TO pgstac_read;
GRANT EXECUTE ON FUNCTION search_query TO pgstac_read;
GRANT EXECUTE ON FUNCTION item_by_id TO pgstac_read;
GRANT EXECUTE ON FUNCTION get_item TO pgstac_read;
GRANT SELECT ON ALL TABLES IN SCHEMA pgstac TO pgstac_read;

-- The caches maintained by where_stats, search_query and format_item.
GRANT SELECT, INSERT, UPDATE ON search_wheres TO pgstac_read;
GRANT SELECT, INSERT, UPDATE ON searches TO pgstac_read;
GRANT SELECT, INSERT, UPDATE ON format_item_cache TO pgstac_read;


GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA pgstac to pgstac_ingest;
GRANT ALL ON ALL TABLES IN SCHEMA pgstac to pgstac_ingest;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA pgstac to pgstac_ingest;

-- Registering a wrapper is an admin action, and the index template is written by nobody;
-- pgstac_ingest keeps SELECT on both through pgstac_read.
REVOKE ALL ON queryable_wrappers, queryable_index_template FROM pgstac_ingest;

REVOKE ALL PRIVILEGES ON PROCEDURE run_queued_queries FROM public;
GRANT ALL ON PROCEDURE run_queued_queries TO pgstac_admin;

REVOKE ALL PRIVILEGES ON FUNCTION run_queued_queries_intransaction FROM public;
GRANT ALL ON FUNCTION run_queued_queries_intransaction TO pgstac_admin;

REVOKE ALL PRIVILEGES ON FUNCTION run_queued_query FROM public;
GRANT ALL ON FUNCTION run_queued_query TO pgstac_admin;

-- Deletes from the queue, so it is admin-only like the runners that call it.
REVOKE ALL PRIVILEGES ON FUNCTION retire_queued_queries FROM public;
GRANT ALL ON FUNCTION retire_queued_queries TO pgstac_admin;

-- PostgreSQL grants EXECUTE to PUBLIC on every new function, so each definer
-- is revoked and granted back to the roles that need it. Keep in step with the
-- ALTER FUNCTION ... SECURITY DEFINER statements above; the pgtap suite fails
-- if a definer is left PUBLIC executable.
REVOKE ALL PRIVILEGES ON FUNCTION
    drop_table_constraints,
    create_table_constraints,
    check_partition,
    repartition,
    maintain_index,
    maintain_reference_index,
    delete_collection,
    collection_delete_trigger_func
FROM public;

-- A role that can execute a definer trigger function can attach it to its own
-- table. Only the owner's trigger on collections runs this one: CREATE TRIGGER
-- checks EXECUTE, firing does not.
REVOKE ALL PRIVILEGES ON FUNCTION collection_delete_trigger_func FROM pgstac_ingest, pgstac_read;

RESET ROLE;

SET ROLE pgstac_ingest;

-- Search finds partitions through partition_stats, so this must be synchronous
-- rather than queued.
SELECT sync_partition_stats();

-- Repairs observed ranges and CHECK constraints for every partition, ordered
-- by partition as every other writer of these rows is. Queued whatever use_queue
-- says: run inline, each partition's SHARE UPDATE EXCLUSIVE lock is held to the
-- end of the migration, where it deadlocks against autovacuum's ANALYZE and takes
-- the whole upgrade with it. Queued, each is its own short transaction, retried on
-- failure. pypgstac migrate drains the queue once the schema change has committed.
SET pgstac.use_queue TO TRUE;
SELECT update_partition_stats_q(partition) FROM partitions_view ORDER BY partition;
RESET pgstac.use_queue;
SELECT set_version('0.10.1');
COMMIT;
