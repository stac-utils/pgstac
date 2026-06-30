-- Encode sort key values into a base64 keyset token for pagination.
CREATE OR REPLACE FUNCTION keyset_encode(vals text[]) RETURNS text AS $$
    SELECT encode(convert_to(array_to_string(vals, chr(31), chr(30)), 'UTF8'), 'base64');
$$ LANGUAGE sql IMMUTABLE;

-- Decode a base64 keyset token back to sort key values. An empty/NULL token returns NULL
-- ("no keyset" => first page). A non-empty token that is not a valid base64 keyset (e.g. a
-- stale/old-style token) raises 22P02 rather than silently returning the first page.
CREATE OR REPLACE FUNCTION keyset_decode(token text) RETURNS text[] AS $$
BEGIN
    IF token IS NULL OR token = '' THEN RETURN NULL; END IF;
    RETURN array_replace(
        string_to_array(convert_from(decode(token,'base64'),'UTF8'), chr(31)),
        chr(30), NULL);
EXCEPTION WHEN others THEN
    -- A non-empty token that does not decode is a client error, not an empty page.
    RAISE EXCEPTION 'Invalid pagination token: %', token USING ERRCODE = '22P02';
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Resolve sortby + id/collection tiebreaks into ordered sort keys with SQL
-- expressions and directions for a unique total row order.
CREATE OR REPLACE FUNCTION keyset_sortkeys(_search jsonb)
RETURNS TABLE(ord int, field text, expr text, dir text, notnull boolean) AS $$
    WITH base AS (
        SELECT coalesce(_search->'sortby','[{"field":"datetime","direction":"desc"}]'::jsonb) AS s
    ),
    app AS (
        SELECT s
               || jsonb_build_object('field','id','direction', s->0->>'direction')
               || jsonb_build_object('field','collection','direction', s->0->>'direction') AS s
        FROM base
    ),
    rows AS (
        SELECT value->>'field' AS field, get_sort_dir(value) AS dir, o
        FROM app, jsonb_array_elements(s) WITH ORDINALITY AS t(value, o)
    ),
    firsts AS (
        SELECT DISTINCT ON (field) field, dir, o FROM rows ORDER BY field, o
    )
    SELECT (row_number() OVER (ORDER BY o))::int, field, (queryable(field)).expression, dir,
           field IN ('id', 'collection', 'datetime', 'end_datetime')
    FROM firsts ORDER BY o;
$$ LANGUAGE sql STABLE;

-- Build the canonical ORDER BY string from keyset sort keys. _prev flips
-- every key's direction for reverse pagination.
CREATE OR REPLACE FUNCTION keyset_orderby(_search jsonb, _prev boolean DEFAULT false) RETURNS text AS $$
    SELECT string_agg(
             expr || ' ' || CASE WHEN _prev
                 THEN (CASE dir WHEN 'ASC' THEN 'DESC' ELSE 'ASC' END) ELSE dir END,
             ', ' ORDER BY ord)
    FROM keyset_sortkeys(_search);
$$ LANGUAGE sql STABLE;

-- Build a multi-level WHERE clause for keyset seek from token values.
-- Handles NULLS FIRST/LAST and direction-aware comparisons.
CREATE OR REPLACE FUNCTION keyset_where(_search jsonb, _values text[], prev boolean DEFAULT false)
RETURNS text AS $$
DECLARE
    k record; vlit text; orterm text;
    andfilters text[] := '{}'::text[];
    orfilters  text[] := '{}'::text[];
BEGIN
    IF _values IS NULL THEN RETURN NULL; END IF;
    FOR k IN SELECT * FROM keyset_sortkeys(_search) ORDER BY ord LOOP
        vlit := CASE WHEN _values[k.ord] IS NULL THEN NULL ELSE quote_literal(_values[k.ord]) END;
        orterm := NULL;
        IF vlit IS NOT NULL AND ((prev AND k.dir='ASC') OR (NOT prev AND k.dir='DESC')) THEN
            orterm := format('(%s < %s)', k.expr, vlit);
        ELSIF vlit IS NULL AND ((prev AND k.dir='ASC') OR (NOT prev AND k.dir='DESC')) THEN
            orterm := format('(%s IS NOT NULL)', k.expr);
        ELSIF vlit IS NULL THEN
            orterm := NULL;
        ELSIF k.notnull THEN
            orterm := format('(%s > %s)', k.expr, vlit);
        ELSE
            orterm := format('((%s > %s) OR (%s IS NULL))', k.expr, vlit, k.expr);
        END IF;
        IF orterm IS NOT NULL THEN
            IF array_length(andfilters,1) IS NULL THEN
                orfilters := orfilters || orterm;
            ELSE
                orfilters := orfilters || format('(%s AND %s)', array_to_string(andfilters,' AND '), orterm);
            END IF;
        END IF;
        andfilters := andfilters || CASE WHEN vlit IS NULL
            THEN format('(%s IS NULL)', k.expr) ELSE format('(%s = %s)', k.expr, vlit) END;
    END LOOP;
    IF array_length(orfilters,1) IS NULL THEN RETURN NULL; END IF;
    RETURN '(' || array_to_string(orfilters, ' OR ') || ')';
END;
$$ LANGUAGE PLPGSQL STABLE;

-- Extract sort direction (ASC/DESC) from a sortby JSON element.
CREATE OR REPLACE FUNCTION get_sort_dir(sort_item jsonb) RETURNS text AS $$
    SELECT CASE WHEN sort_item->>'direction' ILIKE 'desc%' THEN 'DESC' ELSE 'ASC' END;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;
