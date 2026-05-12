RESET ROLE;
DO $$
DECLARE
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname='postgis') THEN
    CREATE EXTENSION IF NOT EXISTS postgis;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname='btree_gist') THEN
    CREATE EXTENSION IF NOT EXISTS btree_gist;
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

SET SEARCH_PATH TO pgstac, public;

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
SET client_min_messages TO WARNING;
SET SEARCH_PATH to pgstac, public;
-- BEGIN migra calculated SQL
set check_function_bodies = off;

CREATE OR REPLACE FUNCTION pgstac.maintain_partition_queries(part text DEFAULT 'items'::text, dropindexes boolean DEFAULT false, rebuildindexes boolean DEFAULT false, idxconcurrently boolean DEFAULT false)
 RETURNS SETOF text
 LANGUAGE plpgsql
AS $function$
DECLARE
    parent text;
    level int;
    isleaf bool;
    collection collections%ROWTYPE;
    subpart text;
    baseidx text;
    queryable_name text;
    queryable_property_index_type text;
    queryable_property_wrapper text;
    queryable_parsed RECORD;
    deletedidx pg_indexes%ROWTYPE;
    q text;
    idx text;
    collection_partition bigint;
    _concurrently text := '';
    idxname text;
BEGIN
    RAISE NOTICE 'Maintaining partition: %', part;
    IF idxconcurrently THEN
        _concurrently='CONCURRENTLY';
    END IF;

    -- Get root partition
    SELECT parentrelid::text, pt.isleaf, pt.level
        INTO parent, isleaf, level
    FROM pg_partition_tree('items') pt
    WHERE relid::text = part;
    IF NOT FOUND THEN
        RAISE NOTICE 'Partition % Does Not Exist In Partition Tree', part;
        RETURN;
    END IF;

    -- If this is a parent partition, recurse to leaves
    IF NOT isleaf THEN
        FOR subpart IN
            SELECT relid::text
            FROM pg_partition_tree(part)
            WHERE relid::text != part
        LOOP
            RAISE NOTICE 'Recursing to %', subpart;
            RETURN QUERY SELECT * FROM maintain_partition_queries(subpart, dropindexes, rebuildindexes);
        END LOOP;
        RETURN; -- Don't continue since not an end leaf
    END IF;


    -- Get collection
    collection_partition := ((regexp_match(part, E'^_items_([0-9]+)'))[1])::bigint;
    RAISE NOTICE 'COLLECTION PARTITION: %', collection_partition;
    SELECT * INTO STRICT collection
    FROM collections
    WHERE key = collection_partition;
    RAISE NOTICE 'COLLECTION ID: %s', collection.id;


    -- Create temp table with existing indexes
    CREATE TEMP TABLE existing_indexes ON COMMIT DROP AS
    SELECT *
    FROM pg_indexes
    WHERE schemaname='pgstac' AND tablename=part;


    -- Check if index exists for each queryable.
    FOR
        queryable_name,
        queryable_property_index_type,
        queryable_property_wrapper
    IN
        SELECT
            name,
            COALESCE(property_index_type, 'BTREE'),
            COALESCE(property_wrapper, 'to_text')
        FROM queryables
        WHERE
            name NOT in ('id', 'datetime', 'geometry')
            AND (
                collection_ids IS NULL
                OR collection_ids = '{}'::text[]
                OR collection.id = ANY (collection_ids)
            )
    LOOP
        baseidx := format(
            $q$ ON %I USING %s (%s(((content -> 'properties'::text) -> %L::text)))$q$,
            part,
            queryable_property_index_type,
            queryable_property_wrapper,
            queryable_name
        );
        RAISE NOTICE 'BASEIDX: %', baseidx;
        RAISE NOTICE 'IDXSEARCH: %', format($q$[(']%s[')]$q$, queryable_name);
        -- If index already exists, delete it from existing indexes type table
        FOR deletedidx IN
            DELETE FROM existing_indexes
            WHERE indexdef ~* format($q$[(']%s[')]$q$, queryable_name)
            RETURNING *
        LOOP
            RAISE NOTICE 'EXISTING INDEX: %', deletedidx;
            IF NOT FOUND THEN -- index did not exist, create it
                RETURN NEXT format('CREATE INDEX %s %s;', _concurrently, baseidx);
            ELSIF rebuildindexes THEN
                RETURN NEXT format('REINDEX %I %s;', deletedidx.indexname, _concurrently);
            END IF;
        END LOOP;
        IF NOT FOUND THEN
            RAISE NOTICE 'CREATING INDEX for %', queryable_name;
            RETURN NEXT format('CREATE INDEX %s %s;', _concurrently, baseidx);
        END IF;
    END LOOP;
    IF NOT EXISTS (SELECT * FROM pg_indexes WHERE indexname::text = concat(part, '_datetime_end_datetime_idx')) THEN
        RETURN NEXT format(
            $f$CREATE INDEX IF NOT EXISTS %I ON %I USING BTREE (datetime DESC, end_datetime ASC);$f$,
            concat(part, '_datetime_end_datetime_idx'),
            part
            );
    END IF;

    IF NOT EXISTS (SELECT * FROM pg_indexes WHERE indexname::text = concat(part, '_geometry_idx')) THEN
        RETURN NEXT format(
            $f$CREATE INDEX IF NOT EXISTS %I ON %I USING GIST (geometry);$f$,
            concat(part, '_geometry_idx'),
            part
            );
    END IF;

    FOR idxname IN
        SELECT indexname::text FROM pg_indexes
        WHERE tablename::text = part AND indexdef ILIKE 'CREATE UNIQUE INDEX % USING btree(id)'
    LOOP
        IF idxname != concat(part, '_pk') AND NOT EXISTS (SELECT * FROM pg_indexes WHERE indexname::text = concat(part,'_pk')) THEN
            RETURN NEXT format(
                $f$ALTER INDEX IF EXISTS %I RENAME TO %I;$f$,
                idxname,
                concat(part, '_pk')
            );
        ELSIF EXISTS (SELECT * FROM pg_indexes WHERE indexname::text = concat(part,'_pk')) AND dropindexes THEN
            RETURN NEXT format(
                    $f$DROP INDEX IF EXISTS %I;$f$,
                    idxname
                );
        END IF;
    END LOOP;

    IF NOT EXISTS (
        SELECT indexname::text FROM pg_indexes
        WHERE tablename::text = part AND indexdef ILIKE 'CREATE UNIQUE INDEX % USING btree (id)'
        ) THEN
        RETURN NEXT format(
            $f$CREATE UNIQUE INDEX IF NOT EXISTS %I ON %I USING BTREE(id);$f$,
            concat(part, '_pk'),
            part
            );
    END IF;

    DELETE FROM existing_indexes WHERE indexname::text IN (
        concat(part, '_datetime_end_datetime_idx'),
        concat(part, '_geometry_idx'),
        concat(part, '_pk')
    );
    -- Remove indexes that were not expected
    FOR idx IN SELECT indexname::text FROM existing_indexes
    LOOP
        RAISE WARNING 'Index: % is not defined by queryables.', idx;
        IF dropindexes THEN
            RETURN NEXT format('DROP INDEX IF EXISTS %I;', idx);
        END IF;
    END LOOP;

    DROP TABLE existing_indexes;
    RAISE NOTICE 'Returning from maintain_partition_queries.';
    RETURN;

END;
$function$
;



-- END migra calculated SQL
DO $$
  BEGIN
    INSERT INTO queryables (name, definition, property_wrapper, property_index_type) VALUES
    ('id', '{"title": "Item ID","description": "Item identifier","$ref": "https://schemas.stacspec.org/v1.0.0/item-spec/json-schema/item.json#/definitions/core/allOf/2/properties/id"}', null, null);
  EXCEPTION WHEN unique_violation THEN
    RAISE NOTICE '%', SQLERRM USING ERRCODE = SQLSTATE;
  END
$$;

DO $$
  BEGIN
    INSERT INTO queryables (name, definition, property_wrapper, property_index_type) VALUES
    ('geometry', '{"title": "Item Geometry","description": "Item Geometry","$ref": "https://geojson.org/schema/Feature.json"}', null, null);
  EXCEPTION WHEN unique_violation THEN
    RAISE NOTICE '%', SQLERRM USING ERRCODE = SQLSTATE;
  END
$$;

DO $$
  BEGIN
    INSERT INTO queryables (name, definition, property_wrapper, property_index_type) VALUES
    ('datetime','{"description": "Datetime","type": "string","title": "Acquired","format": "date-time","pattern": "(\\+00:00|Z)$"}', null, null);
  EXCEPTION WHEN unique_violation THEN
    RAISE NOTICE '%', SQLERRM USING ERRCODE = SQLSTATE;
  END
$$;

DO $$
  BEGIN
    INSERT INTO queryables (name, definition, property_wrapper, property_index_type) VALUES
    ('eo:cloud_cover','{"$ref": "https://stac-extensions.github.io/eo/v1.0.0/schema.json#/definitions/fieldsproperties/eo:cloud_cover"}','to_int','BTREE');
  EXCEPTION WHEN unique_violation THEN
    RAISE NOTICE '%', SQLERRM USING ERRCODE = SQLSTATE;
  END
$$;

DELETE FROM queryables a USING queryables b
  WHERE a.name = b.name AND a.collection_ids IS NOT DISTINCT FROM b.collection_ids AND a.id > b.id;


INSERT INTO pgstac_settings (name, value) VALUES
  ('context', 'off'),
  ('context_estimated_count', '100000'),
  ('context_estimated_cost', '100000'),
  ('context_stats_ttl', '1 day'),
  ('default_filter_lang', 'cql2-json'),
  ('additional_properties', 'true'),
  ('use_queue', 'false'),
  ('queue_timeout', '10 minutes'),
  ('update_collection_extent', 'false')
ON CONFLICT DO NOTHING
;


GRANT USAGE ON SCHEMA pgstac to pgstac_read;
GRANT ALL ON SCHEMA pgstac to pgstac_ingest;
GRANT ALL ON SCHEMA pgstac to pgstac_admin;

-- pgstac_read role limited to using function apis
GRANT EXECUTE ON FUNCTION search TO pgstac_read;
GRANT EXECUTE ON FUNCTION search_query TO pgstac_read;
GRANT EXECUTE ON FUNCTION item_by_id TO pgstac_read;
GRANT EXECUTE ON FUNCTION get_item TO pgstac_read;
GRANT SELECT ON ALL TABLES IN SCHEMA pgstac TO pgstac_read;


GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA pgstac to pgstac_ingest;
GRANT ALL ON ALL TABLES IN SCHEMA pgstac to pgstac_ingest;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA pgstac to pgstac_ingest;

SELECT update_partition_stats_q(partition) FROM partitions;
SELECT set_version('0.7.2');
