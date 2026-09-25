CREATE TABLE queryables (
    id bigint GENERATED ALWAYS AS identity PRIMARY KEY,
    name text NOT NULL,
    collection_ids text[], -- NULL: every collection
    definition jsonb,
    property_path text[], -- the keys under content the queryable reads, when not those of its name
    property_wrapper text,
    property_index_type text
);
CREATE INDEX queryables_name_idx ON queryables (name);
CREATE INDEX queryables_collection_idx ON queryables USING GIN (collection_ids);
CREATE INDEX queryables_property_wrapper_idx ON queryables (property_wrapper);

-- The property_wrapper names a queryable may use, each the name of a pgstac.<name>(jsonb)
-- function. A custom wrapper is registered by adding its row; seeded in 998_idempotent_post.
CREATE TABLE IF NOT EXISTS queryable_wrappers (name text PRIMARY KEY);

-- Strips every leading "properties." so a property name resolves the same however it was spelled.
CREATE OR REPLACE FUNCTION strip_properties_prefix(dotpath text) RETURNS text AS $$
    SELECT regexp_replace(dotpath, $r$^(properties\.)+$r$, '');
$$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;

-- The one spelling of collection_ids: sorted, deduplicated, and NULL (every collection) when empty.
CREATE OR REPLACE FUNCTION canonical_collection_ids(_collection_ids text[]) RETURNS text[] AS $$
    SELECT array_agg(DISTINCT c ORDER BY c) FROM unnest(_collection_ids) c;
$$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;

-- The rows a queryable of this name and these collections cannot coexist with. NULL
-- collection_ids covers every collection, so it conflicts with any other row of the name.
CREATE OR REPLACE FUNCTION conflicting_queryables(_name text, _collection_ids text[]) RETURNS SETOF queryables AS $$
    SELECT * FROM queryables q
    WHERE
        q.name = strip_properties_prefix(_name)
        AND (
            q.collection_ids IS NULL
            OR canonical_collection_ids(_collection_ids) IS NULL
            OR q.collection_ids && _collection_ids
        );
$$ LANGUAGE SQL STABLE;

-- Only a registered name is resolved to a function, so a row cannot steer the lookup at an arbitrary one.
CREATE OR REPLACE FUNCTION check_queryable_wrapper(wrapper text) RETURNS VOID AS $$
BEGIN
    IF NOT EXISTS (SELECT FROM queryable_wrappers WHERE name = wrapper) THEN
        RAISE check_violation USING MESSAGE = format('%s is not in queryable_wrappers.', wrapper);
    END IF;
    IF to_regprocedure(format('pgstac.%I(jsonb)', wrapper)) IS NULL THEN
        RAISE undefined_function USING MESSAGE = format(
            '%s is registered in queryable_wrappers but pgstac.%I(jsonb) does not exist.', wrapper, wrapper
        );
    END IF;
END;
$$ LANGUAGE PLPGSQL STABLE;

CREATE OR REPLACE FUNCTION queryables_constraint_triggerfunc() RETURNS TRIGGER AS $$
DECLARE
    conflicts json;
BEGIN
    -- Only the parameters of upsert_queryable and delete_queryable read empty collection_ids
    -- as every collection.
    IF NEW.collection_ids = '{}' THEN
        RAISE check_violation USING MESSAGE = 'collection_ids is empty; a queryable for every collection has NULL collection_ids.';
    END IF;
    -- Names and collection_ids are stored in one spelling, so equality is enough to find a row.
    NEW.name := strip_properties_prefix(NEW.name);
    NEW.collection_ids := canonical_collection_ids(NEW.collection_ids);
    -- Serializes writers of one property so the conflict check cannot race.
    PERFORM pg_advisory_xact_lock(hashtext(NEW.name));
    SELECT json_agg(row_to_json(q)) INTO conflicts
    FROM conflicting_queryables(NEW.name, NEW.collection_ids) q
    WHERE q.id IS DISTINCT FROM NEW.id;
    IF conflicts IS NOT NULL THEN
        RAISE unique_violation USING MESSAGE = format(
            'There is already a queryable for %s for a collection in %s: %s', NEW.name, NEW.collection_ids, conflicts
        );
    END IF;
    IF EXISTS (SELECT FROM unnest(NEW.collection_ids) c WHERE NOT EXISTS (SELECT FROM collections WHERE id = c)) THEN
        RAISE foreign_key_violation USING MESSAGE = format('One or more collections in %s do not exist.', NEW.collection_ids);
    END IF;
    PERFORM check_queryable_wrapper(queryable_wrapper(NEW.property_wrapper, NEW.definition));
    IF NEW.property_path IS NOT NULL AND (
        coalesce(array_ndims(NEW.property_path), 0) <> 1
        OR EXISTS (SELECT FROM unnest(NEW.property_path) k WHERE k IS NULL OR k = '')
    ) THEN
        RAISE check_violation USING MESSAGE = format('property_path %s is not a list of keys.', NEW.property_path);
    END IF;
    -- A column of items has the index items itself carries, so nothing to ask for.
    IF NEW.property_path IS NOT NULL AND queryable_column(NEW.name) IS NOT NULL THEN
        RAISE check_violation USING
            MESSAGE = format('%s names a column of items, which is read as itself, so property_path would be ignored.', NEW.name);
    END IF;
    IF NEW.property_index_type IS NOT NULL AND queryable_column(NEW.name) IS NOT NULL THEN
        RAISE check_violation USING MESSAGE = format('%s is read as a column of items, which items indexes itself.', NEW.name);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE PLPGSQL;

CREATE TRIGGER queryables_constraint_insert_trigger
BEFORE INSERT ON queryables
FOR EACH ROW EXECUTE PROCEDURE queryables_constraint_triggerfunc();

CREATE TRIGGER queryables_constraint_update_trigger
BEFORE UPDATE ON queryables
FOR EACH ROW
WHEN (OLD.* IS DISTINCT FROM NEW.*)
EXECUTE PROCEDURE queryables_constraint_triggerfunc();

-- The row passed wins: every row of the name it would conflict with is replaced by it. Delete and
-- insert rather than update so the insert trigger checks the row the same way it does a new one.
-- An empty collection_ids means every collection.
CREATE OR REPLACE FUNCTION upsert_queryable(
    name text,
    definition jsonb DEFAULT NULL,
    property_wrapper text DEFAULT NULL,
    property_index_type text DEFAULT NULL,
    collection_ids text[] DEFAULT NULL,
    property_path text[] DEFAULT NULL
) RETURNS VOID AS $$
    SELECT pg_advisory_xact_lock(hashtext(strip_properties_prefix(upsert_queryable.name)));

    DELETE FROM queryables q
    USING conflicting_queryables(upsert_queryable.name, upsert_queryable.collection_ids) c
    WHERE q.id = c.id
    ;

    INSERT INTO queryables (name, definition, property_wrapper, property_index_type, collection_ids, property_path)
    VALUES (
        upsert_queryable.name,
        upsert_queryable.definition,
        upsert_queryable.property_wrapper,
        upsert_queryable.property_index_type,
        canonical_collection_ids(upsert_queryable.collection_ids),
        upsert_queryable.property_path
    )
    ;
$$ LANGUAGE SQL SET SEARCH_PATH TO pgstac,public;

-- Loads a whole queryables document: one row per property under "properties", the wrapper and
-- the index method both inferred from each property's own definition. The fields items carries
-- as columns are already indexed, so they are neither loaded nor treated as missing. With
-- _delete_missing, queryables of these collections that the document does not name are removed.
-- Returns the number of properties loaded.
CREATE OR REPLACE FUNCTION upsert_queryables(
    _definitions jsonb,
    _collection_ids text[] DEFAULT NULL,
    _index_fields text[] DEFAULT NULL,
    _delete_missing boolean DEFAULT FALSE
) RETURNS int AS $$
DECLARE
    _properties jsonb := _definitions->'properties';
    _name text;
    _definition jsonb;
    _loaded int := 0;
BEGIN
    IF jsonb_typeof(_properties) IS DISTINCT FROM 'object' OR _properties = '{}'::jsonb THEN
        RAISE EXCEPTION 'No properties found in the queryables definition'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    FOR _name, _definition IN SELECT key, value FROM jsonb_each(_properties) LOOP
        CONTINUE WHEN queryable_column(_name) IS NOT NULL;
        -- The wrapper is left NULL so queryable_wrapper infers it, and default_index_type
        -- picks the method from the same definition, keeping both rules in one place.
        PERFORM upsert_queryable(
            _name,
            _definition,
            NULL,
            CASE WHEN _name = ANY (coalesce(_index_fields, '{}'::text[]))
                 THEN default_index_type(_definition) END,
            _collection_ids
        );
        _loaded := _loaded + 1;
    END LOOP;

    IF _delete_missing AND _loaded > 0 THEN
        PERFORM delete_missing_queryables(
            ARRAY(SELECT jsonb_object_keys(_properties)),
            _collection_ids
        );
    END IF;

    RETURN _loaded;
END;
$$ LANGUAGE PLPGSQL SET SEARCH_PATH TO pgstac, public;

-- Removes the queryables of these collections that _names does not list, and returns how many
-- went. delete_queryable takes one name and raises when it matches nothing, which is the
-- ordinary outcome here. Empty _collection_ids means every collection, as it does elsewhere.
CREATE OR REPLACE FUNCTION delete_missing_queryables(
    _names text[],
    _collection_ids text[] DEFAULT NULL
) RETURNS int AS $$
DECLARE
    _deleted int;
BEGIN
    DELETE FROM queryables q
    WHERE q.collection_ids IS NOT DISTINCT FROM canonical_collection_ids(_collection_ids)
      -- A queryable named for a column of items is read as that column; a document that does
      -- not mention it is not asking for it to be removed.
      AND queryable_column(q.name) IS NULL
      AND q.name <> ALL (
          SELECT strip_properties_prefix(n) FROM unnest(coalesce(_names, '{}')) n
      );
    GET DIAGNOSTICS _deleted = ROW_COUNT;
    RETURN _deleted;
END;
$$ LANGUAGE PLPGSQL SET SEARCH_PATH TO pgstac,public;

CREATE OR REPLACE FUNCTION delete_queryable(
    name text,
    collection_ids text[] DEFAULT NULL
) RETURNS VOID AS $$
BEGIN
    DELETE FROM queryables q
    WHERE
        q.name = strip_properties_prefix(delete_queryable.name)
        AND q.collection_ids IS NOT DISTINCT FROM canonical_collection_ids(delete_queryable.collection_ids)
    ;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Queryable % for collections % does not exist',
            delete_queryable.name, delete_queryable.collection_ids
            USING ERRCODE = 'no_data_found';
    END IF;
END;
$$ LANGUAGE PLPGSQL SET SEARCH_PATH TO pgstac,public;

-- Updated in place so the update trigger validates the new member and builds its indexes.
-- A global queryable is the only row of its name, so several rows are all per-collection.
CREATE OR REPLACE FUNCTION add_collection_to_queryable(name text, collection_id text) RETURNS VOID AS $$
DECLARE
    q record;
BEGIN
    -- Same lock upsert_queryable takes, held for the read as well as the
    -- write: without it a concurrent upsert deletes and reinserts the row
    -- between the SELECT and the UPDATE, and this silently changes nothing.
    PERFORM pg_advisory_xact_lock(hashtext(strip_properties_prefix(add_collection_to_queryable.name)));

    SELECT id, collection_ids, count(*) OVER () AS n INTO q
    FROM queryables WHERE queryables.name = strip_properties_prefix(add_collection_to_queryable.name);
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Queryable % does not exist, use upsert_queryable to create it', name
            USING ERRCODE = 'no_data_found';
    ELSIF q.collection_ids IS NULL THEN
        RAISE EXCEPTION 'Queryable % is global and already covers every collection', name
            USING ERRCODE = 'unique_violation';
    ELSIF q.n > 1 THEN
        RAISE EXCEPTION 'Queryable % has several per-collection rows, use upsert_queryable with the full list', name
            USING ERRCODE = 'too_many_rows';
    ELSIF collection_id = ANY(q.collection_ids) THEN
        RETURN;
    END IF;
    UPDATE queryables SET collection_ids = collection_ids || collection_id WHERE id = q.id;
END;
$$ LANGUAGE PLPGSQL SET SEARCH_PATH TO pgstac,public;

CREATE OR REPLACE FUNCTION remove_collection_from_queryable(name text, collection_id text) RETURNS VOID AS $$
DECLARE
    q record;
BEGIN
    -- The same lock, for the same reason, as add_collection_to_queryable.
    PERFORM pg_advisory_xact_lock(hashtext(strip_properties_prefix(remove_collection_from_queryable.name)));

    SELECT id, collection_ids, array_remove(collection_ids, collection_id) AS remaining INTO q
    FROM queryables
    WHERE queryables.name = strip_properties_prefix(remove_collection_from_queryable.name)
        AND (collection_ids IS NULL OR collection_id = ANY(collection_ids));
    IF NOT FOUND THEN
        RAISE EXCEPTION 'No queryable % includes collection %', name, collection_id
            USING ERRCODE = 'no_data_found';
    ELSIF q.collection_ids IS NULL THEN
        RAISE EXCEPTION 'Queryable % is global; use delete_queryable', name
            USING ERRCODE = 'invalid_parameter_value';
    ELSIF q.remaining = '{}' THEN
        DELETE FROM queryables WHERE id = q.id;
    ELSE
        UPDATE queryables SET collection_ids = q.remaining WHERE id = q.id;
    END IF;
END;
$$ LANGUAGE PLPGSQL SET SEARCH_PATH TO pgstac,public;

-- What the trigger does to one row on write, done to every row: the repair for a database whose
-- queryables were edited by hand, and a no-op on one already in the stored spelling.
CREATE OR REPLACE FUNCTION canonicalize_queryables() RETURNS VOID AS $$
DECLARE
    r record;
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
$$ LANGUAGE PLPGSQL SET SEARCH_PATH TO pgstac,public;


-- The one rule turning a queryable name into the jsonb keys under content, used by the
-- filter and the index: STAC top-level members sit at the root, anything else under properties.
CREATE OR REPLACE FUNCTION queryable_path_elements(dotpath text) RETURNS text[] AS $$
    SELECT CASE
        WHEN e[1] IN ('assets', 'links', 'bbox', 'stac_version', 'stac_extensions', 'properties') THEN e
        ELSE 'properties'::text || e
    END
    FROM string_to_array(strip_properties_prefix(dotpath), '.') e;
$$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;

-- The keys a queryable reads: its property_path when it has one, else those of its name.
CREATE OR REPLACE FUNCTION queryable_keys(name text, property_path text[]) RETURNS text[] AS $$
    SELECT COALESCE(property_path, queryable_path_elements(name));
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

-- The one rendering of a key chain under content, for filter SQL and the reference index alike.
-- 000_idempotent_pre reads this spelling back, so a change here has to reach its content_keys.
CREATE OR REPLACE FUNCTION content_path(keys text[]) RETURNS text AS $$
    SELECT 'content->' || string_agg(quote_literal(k), '->' ORDER BY o)
    FROM unnest(keys) WITH ORDINALITY AS u(k, o);
$$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;

-- The items column a property name maps to, or NULL. Not STRICT, so the planner inlines its CASE body.
CREATE OR REPLACE FUNCTION queryable_column(dotpath text) RETURNS text AS $$
    SELECT CASE strip_properties_prefix(dotpath)
        WHEN 'start_datetime' THEN 'datetime'
        WHEN 'id' THEN 'id'
        WHEN 'geometry' THEN 'geometry'
        WHEN 'datetime' THEN 'datetime'
        WHEN 'end_datetime' THEN 'end_datetime'
        WHEN 'collection' THEN 'collection'
    END;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

-- The wrapper a queryable is read through: its own, or one inferred from its definition.
-- jsonb ? matches a scalar type as well as a member of a list such as ["number", "null"].
CREATE OR REPLACE FUNCTION queryable_wrapper(property_wrapper text, definition jsonb) RETURNS text AS $$
    SELECT COALESCE(
        property_wrapper,
        CASE
            WHEN definition->'type' ? 'integer' THEN 'to_int'
            WHEN definition->'type' ? 'number' THEN 'to_float'
            WHEN definition->'type' ? 'array' THEN 'to_text_array'
            WHEN definition->>'format' IN ('date-time', 'date') THEN 'to_tstz'
            ELSE 'to_text'
        END
    );
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION queryable(
    IN dotpath text,
    IN _collection_ids text[] DEFAULT NULL,
    OUT path text,
    OUT expression text,
    OUT wrapper text,
    OUT nulled_wrapper text,
    OUT definition jsonb,
    OUT registered boolean
) AS $$
    -- An items column is read as itself, anything else through its wrapper; nulled_wrapper is
    -- NULL for the to_text default. Only rows the search can reach: one for every collection
    -- always, a per-collection row when the search names one of its collections. The row for
    -- every collection wins, and per-collection rows that disagree widen to to_text -- one WHERE
    -- clause cannot read a property as a float for one collection and text for another.
    WITH n AS (
        -- An empty name is not a property: it resolves to content->'properties', so a filter
        -- on it would silently compare the entire properties object.
        SELECT strip_properties_prefix(dotpath) AS name, queryable_column(dotpath) AS col
        -- The stripped name, because that is what is resolved: 'properties.' strips to nothing.
        -- 'properties' alone is not stripped at all and named the whole properties object, which
        -- is the same silent whole-object comparison by another spelling.
        WHERE btrim(coalesce(strip_properties_prefix(dotpath), '')) <> ''
          AND btrim(dotpath) <> 'properties'
    ), applicable AS (
        SELECT q.id, q.definition, q.property_wrapper, q.property_path,
               q.collection_ids IS NULL AS global,
               queryable_wrapper(q.property_wrapper, q.definition) AS w,
               queryable_keys(q.name, q.property_path) AS keys
        FROM queryables q, n
        WHERE n.col IS NULL
          AND q.name = n.name
          AND (q.collection_ids IS NULL
            -- '{}' means every collection here as it does in get_queryables and the write
            -- functions; search() produces it for "collections": [].
            OR nullif(_collection_ids, '{}') IS NULL
            OR q.collection_ids && nullif(_collection_ids, '{}'))
    ), pick AS (
        -- One scan of applicable, not three: the same rows answer all three questions.
        SELECT count(DISTINCT a.w) AS wrappers,
               count(DISTINCT a.keys::text) AS paths,
               (array_agg(a.id ORDER BY a.global DESC, a.id))[1] AS id
        FROM applicable a
    ), best AS (
        SELECT a.*, p.wrappers, p.paths
        FROM pick p LEFT JOIN applicable a ON a.id = p.id
    )
    SELECT
        pth.path,
        CASE WHEN n.col IS NULL THEN format('%I(%s)', wr.wrapper, pth.path) ELSE pth.path END,
        CASE WHEN n.col IS NULL THEN wr.wrapper END,
        CASE WHEN n.col IS NULL AND (
                 b.property_wrapper IS NOT NULL
                 OR wr.wrapper <> 'to_text'
                 -- A widened result is a decision, not the to_text default: reported as NULL it
                 -- would let cql2_query infer a wrapper of its own from the literal.
                 OR (b.id IS NOT NULL AND NOT b.global AND b.wrappers > 1)
             ) THEN wr.wrapper END,
        b.definition,
        n.col IS NOT NULL OR b.id IS NOT NULL
    FROM n
    LEFT JOIN best b ON TRUE
    CROSS JOIN LATERAL (
        SELECT CASE
            WHEN b.id IS NULL OR b.global OR b.wrappers = 1
            THEN queryable_wrapper(b.property_wrapper, b.definition)
            ELSE 'to_text'
        END AS wrapper
    ) wr
    CROSS JOIN LATERAL (
        SELECT COALESCE(n.col, content_path(
            CASE
                WHEN b.id IS NULL OR b.global OR b.paths = 1
                THEN queryable_keys(n.name, b.property_path)
                ELSE queryable_path_elements(n.name)
            END)) AS path
    ) pth;
$$ LANGUAGE SQL STABLE;


-- Every indexed queryable has one reference index on queryable_index_template, an empty copy of
-- items: for a queryable named after a column of items the copy of the index items carries,
-- otherwise the index maintain_reference_index builds from the row. Partitions are indexed by
-- rewriting the reference index to them.
CREATE OR REPLACE FUNCTION reference_index_method(q queryables) RETURNS text AS $$
    SELECT format(
        'USING %I (%I(%s))',
        lower(q.property_index_type),
        queryable_wrapper(q.property_wrapper, q.definition),
        content_path(queryable_keys(q.name, q.property_path))
    ) WHERE q.property_index_type IS NOT NULL;
$$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;

-- The index method for a queryable the caller asked to index without naming one. An array is
-- read through to_text_array and matched with @> and &&, which btree cannot serve.
CREATE OR REPLACE FUNCTION default_index_type(definition jsonb) RETURNS text AS $$
    SELECT CASE WHEN pgstac.queryable_wrapper(NULL, definition) = 'to_text_array'
                THEN 'GIN' ELSE 'BTREE' END;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

-- q<id>_<hash of the method>, so a reference index that no longer matches its row is told by
-- name; NULL for a row that asks for no index.
CREATE OR REPLACE FUNCTION reference_index_name(q queryables) RETURNS text AS $$
    SELECT 'q' || q.id || '_' || left(md5(reference_index_method(q)), 8);
$$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION reference_index(q queryables) RETURNS regclass AS $$
    SELECT i.indexrelid
    FROM pg_index i
    JOIN pg_class c ON c.oid = i.indexrelid
    LEFT JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = i.indkey[0]
    WHERE i.indrelid = to_regclass('pgstac.queryable_index_template')
        AND (c.relname = reference_index_name(q) OR a.attname = q.name);
$$ LANGUAGE SQL STABLE;

-- Brings the reference indexes of one queryable, or of all, in line with the rows. Nothing runs
-- at steady state. The read paths do no DDL, so until maintain_partitions runs they show a stale
-- reference index as unpaired. An index PostgreSQL cannot build fails the row's own write.
-- SECURITY DEFINER: pgstac_ingest owns no table, and the statement is built from the stored row,
-- never from an argument.
CREATE OR REPLACE FUNCTION maintain_reference_index(_queryable_id bigint DEFAULT NULL) RETURNS VOID AS $$
DECLARE
    q queryables;
    stale text;
BEGIN
    FOR stale IN
        SELECT c.relname
        FROM pg_index i
        JOIN pg_class c ON c.oid = i.indexrelid
        CROSS JOIN LATERAL (SELECT substring(c.relname FROM '^q(\d+)_')::bigint AS id) n
        LEFT JOIN queryables r ON r.id = n.id AND r.property_index_type IS NOT NULL
        WHERE i.indrelid = to_regclass('pgstac.queryable_index_template')
            AND c.relname ~ '^q\d+_[0-9a-f]{8}$'
            AND n.id = COALESCE(_queryable_id, n.id)
            AND c.relname IS DISTINCT FROM reference_index_name(r)
    LOOP
        EXECUTE format('DROP INDEX pgstac.%I', stale);
    END LOOP;
    FOR q IN
        SELECT r.* FROM queryables r
        WHERE r.property_index_type IS NOT NULL
            AND r.id = COALESCE(_queryable_id, r.id)
            AND to_regclass(format('pgstac.%I', reference_index_name(r))) IS NULL
    LOOP
        -- CREATE INDEX is where an unusable access method or wrapper type is caught.
        BEGIN
            EXECUTE format(
                'CREATE INDEX %I ON queryable_index_template %s',
                reference_index_name(q), reference_index_method(q)
            );
        EXCEPTION WHEN OTHERS THEN
            IF _queryable_id IS NOT NULL THEN
                RAISE EXCEPTION '% cannot be indexed: %', q.name, SQLERRM USING ERRCODE = SQLSTATE;
            END IF;
            RAISE WARNING '% cannot be indexed: %', q.name, SQLERRM;
        END;
    END LOOP;
END;
$$ LANGUAGE PLPGSQL SECURITY DEFINER SET SEARCH_PATH TO pgstac, public;

CREATE OR REPLACE FUNCTION queryables_reference_index_triggerfunc() RETURNS TRIGGER AS $$
BEGIN
    -- Two sessions writing different queryable names can deadlock on the shared template.
    -- Retryable, and not serialised here: a lock wide enough to prevent it is held for every
    -- loader transaction.
    PERFORM maintain_reference_index(COALESCE(NEW.id, OLD.id));
    RETURN NULL;
END;
$$ LANGUAGE PLPGSQL;

-- Row level, so these run before the statement level triggers below build the partition indexes.
CREATE TRIGGER queryables_reference_index_insert_trigger AFTER INSERT ON queryables
FOR EACH ROW WHEN (NEW.property_index_type IS NOT NULL)
EXECUTE FUNCTION queryables_reference_index_triggerfunc();

CREATE TRIGGER queryables_reference_index_update_trigger AFTER UPDATE ON queryables
FOR EACH ROW WHEN (
    (OLD.name, OLD.definition, OLD.property_path, OLD.property_wrapper, OLD.property_index_type)
    IS DISTINCT FROM (NEW.name, NEW.definition, NEW.property_path, NEW.property_wrapper, NEW.property_index_type)
)
EXECUTE FUNCTION queryables_reference_index_triggerfunc();

CREATE TRIGGER queryables_reference_index_delete_trigger AFTER DELETE ON queryables
FOR EACH ROW WHEN (OLD.property_index_type IS NOT NULL)
EXECUTE FUNCTION queryables_reference_index_triggerfunc();

-- A deparsed index definition with its name and table replaced by the table given, so a
-- partition's indexes compare with the reference index rewritten to it, and the result runs.
-- Only the header is rewritten; the expression may hold anything, including the index name.
CREATE OR REPLACE FUNCTION indexdef_unnamed(_indexdef text, _tablename text) RETURNS text AS $$
    SELECT format(
        'CREATE %sINDEX ON %I%s',
        CASE WHEN _indexdef ^@ 'CREATE UNIQUE ' THEN 'UNIQUE ' END,
        _tablename,
        substr(_indexdef, strpos(_indexdef, ' USING '))
    );
$$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;

-- Pairs the indexes of the partitions under treeroot with the queryables that want them, by
-- partition and exact definition: both sides are pg_get_indexdef output of one session. An index
-- no queryable pairs with is an orphan (queryable_id NULL); a queryable no index pairs with is
-- missing one (indexname NULL).
CREATE OR REPLACE FUNCTION queryable_indexes(
    IN treeroot text DEFAULT 'items',
    IN changes boolean DEFAULT FALSE,
    OUT collection text,
    OUT partition text,
    OUT field text,
    OUT indexname text,
    OUT existing_idx text,
    OUT queryable_idx text,
    OUT queryable_id bigint
) RETURNS SETOF RECORD AS $$
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
        -- An unpaired index is only an orphan if it is not one of the structural ones.
        OR (qid IS NULL AND NOT EXISTS (
                SELECT FROM structural
                WHERE structural.sdx = indexdef_unnamed(i.iidx, 'x')));
$$ LANGUAGE SQL;

DROP VIEW IF EXISTS pgstac_indexes_stats;
DROP VIEW IF EXISTS pgstac_indexes;
CREATE VIEW pgstac_indexes AS
SELECT
    'pgstac'::name AS schemaname,
    partition AS tablename,
    indexname,
    pg_get_indexdef(indexname::regclass) AS indexdef,
    existing_idx AS idx,
    field,
    pg_table_size(indexname::text) AS index_size,
    pg_size_pretty(pg_table_size(indexname::text)) AS index_size_pretty
FROM queryable_indexes('items')
WHERE indexname IS NOT NULL;

CREATE VIEW pgstac_indexes_stats AS
SELECT
    i.*,
    n_distinct,
    most_common_vals::text::text[],
    most_common_freqs::text::text[],
    histogram_bounds::text::text[],
    correlation
FROM pgstac_indexes i
LEFT JOIN pg_stats s ON (s.schemaname = i.schemaname AND s.tablename = i.indexname);

-- SECURITY DEFINER, so the index statement is built here from the queryables
-- row; a caller supplied one would run with the privileges of the schema owner.
CREATE OR REPLACE FUNCTION maintain_index(
    _partition text,
    _indexname text,
    _queryable_id bigint,
    dropindexes boolean DEFAULT FALSE,
    rebuildindexes boolean DEFAULT FALSE
) RETURNS VOID AS $$
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
    ELSIF _queryable_id IS NULL AND dropindexes THEN
        -- The caller's word that this index is unpaired is not taken on trust. This function is
        -- SECURITY DEFINER and pgstac_ingest can execute it, so a caller that simply passed a
        -- NULL queryable id could drop any index on any partition -- including the unique id
        -- index, which is what stops duplicate item ids. Asked of the pairing itself instead.
        IF NOT EXISTS (
            SELECT FROM queryable_indexes(_partition, true) qi
            WHERE qi.indexname = _indexname AND qi.queryable_id IS NULL
        ) THEN
            RAISE EXCEPTION '% on % is not an unpaired index', _indexname, _partition
                USING ERRCODE = 'invalid_parameter_value';
        END IF;
        EXECUTE format('DROP INDEX IF EXISTS %I;', _indexname);
    ELSIF rebuildindexes THEN
        EXECUTE format('REINDEX INDEX %I;', _indexname);
    END IF;
END;
$$ LANGUAGE PLPGSQL SECURITY DEFINER SET SEARCH_PATH TO pgstac, public;


-- A plain run builds missing indexes only; an orphan stays until dropindexes.
CREATE OR REPLACE FUNCTION maintain_partition_queries(
    part text DEFAULT 'items',
    dropindexes boolean DEFAULT FALSE,
    rebuildindexes boolean DEFAULT FALSE
) RETURNS SETOF text AS $$
    SELECT format(
        'SELECT maintain_index(%L,%L,%L,%L,%L);',
        partition, indexname, queryable_id, dropindexes, rebuildindexes
    )
    FROM queryable_indexes(part, NOT rebuildindexes)
    WHERE queryable_id IS NOT NULL OR dropindexes OR rebuildindexes;
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION maintain_partitions(
    part text DEFAULT 'items',
    dropindexes boolean DEFAULT FALSE,
    rebuildindexes boolean DEFAULT FALSE
) RETURNS VOID AS $$
    SELECT maintain_reference_index();
    WITH t AS (
        SELECT run_or_queue(q) FROM maintain_partition_queries(part, dropindexes, rebuildindexes) q
    ) SELECT count(*) FROM t;
$$ LANGUAGE SQL;


-- Maintains the partitions of the collections the affected rows name; a global row
-- reaches every collection, so it walks the whole tree.
CREATE OR REPLACE FUNCTION queryables_trigger_func() RETURNS TRIGGER AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM new_rows WHERE collection_ids IS NULL) THEN
        PERFORM maintain_partitions();
    ELSE
        PERFORM maintain_partitions(format('_items_%s', key))
        FROM collections
        WHERE id IN (SELECT unnest(collection_ids) FROM new_rows)
            AND to_regclass(format('pgstac._items_%s', key)) IS NOT NULL;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE PLPGSQL;

-- A transition table is allowed for one event only, hence two triggers.
CREATE TRIGGER queryables_insert_trigger AFTER INSERT ON queryables
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE PROCEDURE queryables_trigger_func();

CREATE TRIGGER queryables_update_trigger AFTER UPDATE ON queryables
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE PROCEDURE queryables_trigger_func();


-- The queryables of these collections, or of all when NULL, merged by name; NULL when none of the
-- collections exists.
CREATE OR REPLACE FUNCTION get_queryables(_collection_ids text[] DEFAULT NULL) RETURNS jsonb AS $$
    -- nullif because an empty array means every collection here as it does everywhere else in
    -- this file: upsert_queryable and delete_queryable both read '{}' that way.
    WITH g AS (
        SELECT
            name,
            -- Without the three merged keys: they are re-applied below at their widest, and
            -- leaving them here let the lowest-id row's own constraints through unchanged.
            first_notnull(definition - '{enum,minimum,maximum}'::text[] ORDER BY id) AS definition,
            -- A row that omits a constraint permits everything, so the merge has to omit it as
            -- well. These aggregates skip NULLs, which would answer with the one row that
            -- happened to carry the constraint and reject values another collection allows.
            CASE WHEN count(*) = count(definition->'enum')
                 THEN jsonb_array_unique_merge(definition->'enum' ORDER BY id) END AS enum,
            CASE WHEN count(*) = count(definition->'minimum')
                 THEN jsonb_min(definition->'minimum' ORDER BY id) END AS minimum,
            CASE WHEN count(*) = count(definition->'maximum')
                 THEN jsonb_max(definition->'maximum' ORDER BY id) END AS maximum
        FROM (
            SELECT id, name, coalesce(definition, '{"type":"string"}'::jsonb) AS definition
            FROM queryables
            WHERE collection_ids IS NULL
               OR nullif(_collection_ids, '{}') IS NULL
               OR collection_ids && nullif(_collection_ids, '{}')
        ) q
        GROUP BY name
    )
    SELECT CASE WHEN EXISTS (
        SELECT FROM collections
        WHERE nullif(_collection_ids, '{}') IS NULL
           OR id = ANY(nullif(_collection_ids, '{}'))) THEN
        jsonb_build_object(
            '$schema', 'http://json-schema.org/draft-07/schema#',
            '$id', '',
            'type', 'object',
            'title', 'STAC Queryables.',
            'properties', jsonb_object_agg(
                name,
                definition || jsonb_strip_nulls(jsonb_build_object('enum', enum, 'minimum', minimum, 'maximum', maximum))
            ),
            'additionalProperties', additional_properties()
        )
    END
    FROM g;
$$ LANGUAGE SQL STABLE;

CREATE OR REPLACE FUNCTION get_queryables(_collection text DEFAULT NULL) RETURNS jsonb AS $$
    SELECT get_queryables(CASE WHEN _collection IS NULL THEN NULL ELSE ARRAY[_collection] END);
$$ LANGUAGE SQL STABLE;

CREATE OR REPLACE FUNCTION get_queryables() RETURNS jsonb AS $$
    SELECT get_queryables(NULL::text[]);
$$ LANGUAGE SQL STABLE;

CREATE OR REPLACE FUNCTION schema_qualify_refs(url text, j jsonb) returns jsonb as $$
    SELECT replace(j::text, '"$ref": "#', '"$ref": "' || url || '#')::jsonb;
$$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;


CREATE OR REPLACE VIEW stac_extension_queryables AS
SELECT DISTINCT key as name, schema_qualify_refs(e.url, j.value) as definition FROM stac_extensions e, jsonb_each(e.content->'definitions'->'fields'->'properties') j;


CREATE OR REPLACE FUNCTION missing_queryables(_collection text, _tablesample float DEFAULT 5, minrows float DEFAULT 10) RETURNS TABLE(collection text, name text, definition jsonb, property_wrapper text) AS $$
DECLARE
    q text;
    _partition text;
    explain_json json;
    psize float;
BEGIN
    SELECT format('_items_%s', key) INTO _partition FROM collections WHERE id=_collection;
    IF to_regclass(_partition) IS NULL THEN
        RETURN;
    END IF;

    EXECUTE format('EXPLAIN (format json) SELECT 1 FROM %I;', _partition)
    INTO explain_json;
    psize := explain_json->0->'Plan'->'Plan Rows';
    -- Widens the sample until it is expected to hold at least minrows rows.
    _tablesample := least(100, greatest(_tablesample, minrows * 100 / greatest(psize, 1)));
    RAISE DEBUG 'Using tablesample % to find missing queryables from % % that has ~% rows', _tablesample, _collection, _partition, psize;

    q := format(
        $q$
            WITH q AS (
                SELECT * FROM queryables
                WHERE
                    collection_ids IS NULL
                    OR %L = ANY(collection_ids)
            ), t AS (
                SELECT
                    content->'properties' AS properties
                FROM
                    %I
                TABLESAMPLE SYSTEM(%L)
            ), p AS (
                SELECT DISTINCT ON (key)
                    key,
                    COALESCE(s.definition, jsonb_build_object('type', jsonb_typeof(value))) AS definition
                FROM t
                JOIN LATERAL jsonb_each(properties) ON TRUE
                LEFT JOIN q ON (q.name=key)
                LEFT JOIN stac_extension_queryables s ON (s.name=key)
                -- Registered at all, not merely defined: a row with a NULL definition is still a
                -- queryable, and reporting it again feeds a loader a wrapper contradicting its own.
                WHERE q.name IS NULL
            )
            SELECT
                %L,
                key,
                definition,
                queryable_wrapper(NULL, definition)
            FROM p;
        $q$,
        _collection,
        _partition,
        _tablesample,
        _collection
    );
    RETURN QUERY EXECUTE q;
END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION missing_queryables(_tablesample float DEFAULT 5) RETURNS TABLE(collection_ids text[], name text, definition jsonb, property_wrapper text) AS $$
    SELECT
        array_agg(collection),
        name,
        definition,
        property_wrapper
    FROM
        collections
        JOIN LATERAL
        missing_queryables(id, _tablesample) c
        ON TRUE
    GROUP BY
        2,3,4
    ORDER BY 2,1
    ;
$$ LANGUAGE SQL;
