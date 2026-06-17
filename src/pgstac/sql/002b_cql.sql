CREATE OR REPLACE FUNCTION parse_dtrange(
    _indate jsonb,
    relative_base timestamptz DEFAULT date_trunc('hour', CURRENT_TIMESTAMP)
) RETURNS tstzrange AS $$
DECLARE
    timestrs text[];
    s timestamptz;
    e timestamptz;
BEGIN
    timestrs :=
    CASE
        WHEN _indate ? 'timestamp' THEN
            ARRAY[_indate->>'timestamp']
        WHEN _indate ? 'interval' THEN
            to_text_array(_indate->'interval')
        WHEN jsonb_typeof(_indate) = 'array' THEN
            to_text_array(_indate)
        ELSE
            regexp_split_to_array(
                _indate->>0,
                '/'
            )
    END;
    RAISE NOTICE 'TIMESTRS %', timestrs;
    IF cardinality(timestrs) = 1 THEN
        IF timestrs[1] ILIKE 'P%' THEN
            RETURN tstzrange(relative_base - upper(timestrs[1])::interval, relative_base, '[)');
        END IF;
        s := timestrs[1]::timestamptz;
        RETURN tstzrange(s, s, '[]');
    END IF;

    IF cardinality(timestrs) != 2 THEN
        RAISE EXCEPTION 'Timestamp cannot have more than 2 values';
    END IF;

    IF timestrs[1] = '..' OR timestrs[1] = '' THEN
        s := '-infinity'::timestamptz;
        e := timestrs[2]::timestamptz;
        RETURN tstzrange(s,e,'[)');
    END IF;

    IF timestrs[2] = '..' OR timestrs[2] = '' THEN
        s := timestrs[1]::timestamptz;
        e := 'infinity'::timestamptz;
        RETURN tstzrange(s,e,'[)');
    END IF;

    IF timestrs[1] ILIKE 'P%' AND timestrs[2] NOT ILIKE 'P%' THEN
        e := timestrs[2]::timestamptz;
        s := e - upper(timestrs[1])::interval;
        RETURN tstzrange(s,e,'[)');
    END IF;

    IF timestrs[2] ILIKE 'P%' AND timestrs[1] NOT ILIKE 'P%' THEN
        s := timestrs[1]::timestamptz;
        e := s + upper(timestrs[2])::interval;
        RETURN tstzrange(s,e,'[)');
    END IF;

    s := timestrs[1]::timestamptz;
    e := timestrs[2]::timestamptz;

    RETURN tstzrange(s,e,'[)');

    RETURN NULL;

END;
$$ LANGUAGE PLPGSQL STABLE STRICT PARALLEL SAFE SET TIME ZONE 'UTC';

CREATE OR REPLACE FUNCTION parse_dtrange(
    _indate text,
    relative_base timestamptz DEFAULT CURRENT_TIMESTAMP
) RETURNS tstzrange AS $$
    SELECT parse_dtrange(to_jsonb(_indate), relative_base);
$$ LANGUAGE SQL STABLE STRICT PARALLEL SAFE;


CREATE OR REPLACE FUNCTION temporal_op_query(op text, args jsonb) RETURNS text AS $$
DECLARE
    ll text := 'datetime';
    lh text := 'end_datetime';
    rrange tstzrange;
    rl text;
    rh text;
    outq text;
BEGIN
    rrange := parse_dtrange(args->1);
    op := lower(op);
    rl := format('%L::timestamptz', lower(rrange));
    rh := format('%L::timestamptz', upper(rrange));
    outq := CASE op
        WHEN 't_before'       THEN 'lh < rl'
        WHEN 't_after'        THEN 'll > rh'
        WHEN 't_meets'        THEN 'lh = rl'
        WHEN 't_metby'        THEN 'll = rh'
        WHEN 't_overlaps'     THEN 'll < rl AND rl < lh < rh'
        WHEN 't_overlappedby' THEN 'rl < ll < rh AND lh > rh'
        WHEN 't_starts'       THEN 'll = rl AND lh < rh'
        WHEN 't_startedby'    THEN 'll = rl AND lh > rh'
        WHEN 't_during'       THEN 'll > rl AND lh < rh'
        WHEN 't_contains'     THEN 'll < rl AND lh > rh'
        WHEN 't_finishes'     THEN 'll > rl AND lh = rh'
        WHEN 't_finishedby'   THEN 'll < rl AND lh = rh'
        WHEN 't_equals'       THEN 'll = rl AND lh = rh'
        WHEN 't_disjoint'     THEN 'NOT (ll <= rh AND lh >= rl)'
        WHEN 't_intersects'   THEN 'll <= rh AND lh >= rl'
        WHEN 'anyinteracts'   THEN 'll <= rh AND lh >= rl'
    END;
    outq := regexp_replace(outq, '\mll\M', ll);
    outq := regexp_replace(outq, '\mlh\M', lh);
    outq := regexp_replace(outq, '\mrl\M', rl);
    outq := regexp_replace(outq, '\mrh\M', rh);
    outq := format('(%s)', outq);
    RETURN outq;
END;
$$ LANGUAGE PLPGSQL STABLE STRICT;



CREATE OR REPLACE FUNCTION spatial_op_query(op text, args jsonb) RETURNS text AS $$
DECLARE
    geom text;
    j jsonb := args->1;
BEGIN
    op := lower(op);
    IF op NOT IN ('s_equals','s_disjoint','s_touches','s_within','s_overlaps','s_crosses','s_intersects','intersects','s_contains') THEN
        RAISE EXCEPTION 'Spatial Operator % Not Supported', op;
    END IF;
    op := regexp_replace(op, '^s_', 'st_');
    IF op = 'intersects' THEN
        op := 'st_intersects';
    END IF;
    -- Convert geometry to WKB string
    IF j ? 'type' AND j ? 'coordinates' THEN
        geom := st_geomfromgeojson(j)::text;
    ELSIF jsonb_typeof(j) = 'array' THEN
        geom := bbox_geom(j)::text;
    END IF;

    RETURN format('%s(geometry, %L::geometry)', op, geom);
END;
$$ LANGUAGE PLPGSQL;

-- q_op_query: SQL predicate for the pgstac `q` full-text operator. args is the search
-- term(s) (a string or array of strings, as the STAC `q` parameter); it is matched
-- against a tsvector built from the item's description/title/keywords. Modeled on
-- spatial_op_query / temporal_op_query so full-text is a first-class CQL2 op and the
-- whole search predicate has a single CQL2 representation.
CREATE OR REPLACE FUNCTION q_op_query(args jsonb) RETURNS text AS $$
    SELECT format(
        $q$(
            to_tsvector('english', coalesce(properties->>'description', '')) ||
            to_tsvector('english', coalesce(properties->>'title', '')) ||
            to_tsvector('english', coalesce(properties->>'keywords', ''))
        ) @@ %L$q$,
        q_to_tsquery(args)
    );
$$ LANGUAGE SQL STABLE;

CREATE OR REPLACE FUNCTION query_to_cql2(q jsonb) RETURNS jsonb AS $$
-- Translates anything passed in through the deprecated "query" into equivalent CQL2
WITH t AS (
    SELECT key as property, value as ops
        FROM jsonb_each(q)
), t2 AS (
    SELECT property, (jsonb_each(ops)).*
        FROM t WHERE jsonb_typeof(ops) = 'object'
    UNION ALL
    SELECT property, 'eq', ops
        FROM t WHERE jsonb_typeof(ops) != 'object'
)
SELECT
    jsonb_strip_nulls(jsonb_build_object(
        'op', 'and',
        'args', jsonb_agg(
            jsonb_build_object(
                'op', key,
                'args', jsonb_build_array(
                    jsonb_build_object('property',property),
                    value
                )
            )
        )
    )
) as qcql FROM t2
;
$$ LANGUAGE SQL IMMUTABLE STRICT;


CREATE OR REPLACE FUNCTION cql1_to_cql2(j jsonb) RETURNS jsonb AS $$
DECLARE
    args jsonb;
    ret jsonb;
BEGIN
    RAISE NOTICE 'CQL1_TO_CQL2: %', j;
    IF j ? 'filter' THEN
        RETURN cql1_to_cql2(j->'filter');
    END IF;
    IF j ? 'property' THEN
        RETURN j;
    END IF;
    IF jsonb_typeof(j) = 'array' THEN
        SELECT jsonb_agg(cql1_to_cql2(el)) INTO args FROM jsonb_array_elements(j) el;
        RETURN args;
    END IF;
    IF jsonb_typeof(j) = 'number' THEN
        RETURN j;
    END IF;
    IF jsonb_typeof(j) = 'string' THEN
        RETURN j;
    END IF;

    IF jsonb_typeof(j) = 'object' THEN
        SELECT jsonb_build_object(
                'op', key,
                'args', cql1_to_cql2(value)
            ) INTO ret
        FROM jsonb_each(j)
        WHERE j IS NOT NULL;
        RETURN ret;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE PLPGSQL IMMUTABLE STRICT;

CREATE TABLE cql2_ops (
    op text PRIMARY KEY,
    template text,
    types text[]
);
INSERT INTO cql2_ops (op, template, types) VALUES
    ('eq', '%s = %s', NULL),
    ('neq', '%s != %s', NULL),
    ('ne', '%s != %s', NULL),
    ('!=', '%s != %s', NULL),
    ('<>', '%s != %s', NULL),
    ('lt', '%s < %s', NULL),
    ('lte', '%s <= %s', NULL),
    ('gt', '%s > %s', NULL),
    ('gte', '%s >= %s', NULL),
    ('le', '%s <= %s', NULL),
    ('ge', '%s >= %s', NULL),
    ('=', '%s = %s', NULL),
    ('<', '%s < %s', NULL),
    ('<=', '%s <= %s', NULL),
    ('>', '%s > %s', NULL),
    ('>=', '%s >= %s', NULL),
    ('like', '%s LIKE %s', NULL),
    ('ilike', '%s ILIKE %s', NULL),
    ('+', '%s + %s', NULL),
    ('-', '%s - %s', NULL),
    ('*', '%s * %s', NULL),
    ('/', '%s / %s', NULL),
    ('not', 'NOT (%s)', NULL),
    ('between', '%s BETWEEN %s AND %s', NULL),
    ('isnull', '%s IS NULL', NULL),
    ('upper', 'upper(%s)', NULL),
    ('lower', 'lower(%s)', NULL),
    ('casei', 'upper(%s)', NULL),
    ('accenti', 'unaccent(%s)', NULL)
ON CONFLICT (op) DO UPDATE
    SET
        template = EXCLUDED.template
;


CREATE OR REPLACE FUNCTION cql2_query(j jsonb, wrapper text DEFAULT NULL) RETURNS text AS $$
#variable_conflict use_variable
DECLARE
    args jsonb := j->'args';
    arg jsonb;
    op text := lower(j->>'op');
    cql2op RECORD;
    literal text;
    _wrapper text;
    leftarg text;
    rightarg text;
    prop text;
    extra_props bool := pgstac.additional_properties();
    queryable_row RECORD;
BEGIN
    IF j IS NULL OR (op IS NOT NULL AND args IS NULL) THEN
        RETURN NULL;
    END IF;

    -- check if all properties are represented in the queryables
    IF NOT extra_props THEN
        FOR prop IN
            SELECT DISTINCT p->>0
            FROM jsonb_path_query(j, 'strict $.**.property') p
            WHERE p->>0 NOT IN ('id', 'datetime', 'geometry', 'end_datetime', 'collection')
        LOOP
            IF (queryable(prop)).nulled_wrapper IS NULL THEN
                RAISE EXCEPTION 'Term % is not found in queryables.', prop;
            END IF;
        END LOOP;
    END IF;

    IF j ? 'filter' THEN
        RETURN cql2_query(j->'filter');
    END IF;

    IF j ? 'upper' THEN
        RETURN  cql2_query(jsonb_build_object('op', 'upper', 'args', j->'upper'));
    END IF;

    IF j ? 'lower' THEN
        RETURN  cql2_query(jsonb_build_object('op', 'lower', 'args', j->'lower'));
    END IF;

    -- Temporal Query
    IF op ilike 't_%' or op = 'anyinteracts' THEN
        RETURN temporal_op_query(op, args);
    END IF;

    -- If property is a timestamp convert it to text to use with
    -- general operators
    IF j ? 'timestamp' THEN
        RETURN format('%L::timestamptz', to_tstz(j->'timestamp'));
    END IF;
    IF j ? 'interval' THEN
        RAISE EXCEPTION 'Please use temporal operators when using intervals.';
    END IF;

    -- Spatial Query
    IF op ilike 's_%' or op = 'intersects' THEN
        RETURN spatial_op_query(op, args);
    END IF;

    -- Full-text Query (pgstac `q` operator)
    IF op = 'q' THEN
        RETURN q_op_query(args);
    END IF;

    IF op IN ('a_equals','a_contains','a_contained_by','a_overlaps') THEN
        IF args->0 ? 'property' THEN
            leftarg := format('to_text_array(%s)', (queryable(args->0->>'property')).path);
        END IF;
        IF args->1 ? 'property' THEN
            rightarg := format('to_text_array(%s)', (queryable(args->1->>'property')).path);
        END IF;
        RETURN FORMAT(
            '%s %s %s',
            COALESCE(leftarg, quote_literal(to_text_array(args->0))),
            CASE op
                WHEN 'a_equals' THEN '='
                WHEN 'a_contains' THEN '@>'
                WHEN 'a_contained_by' THEN '<@'
                WHEN 'a_overlaps' THEN '&&'
            END,
            COALESCE(rightarg, quote_literal(to_text_array(args->1)))
        );
    END IF;

    IF op = 'in' THEN
        RAISE NOTICE 'IN : % % %', args, jsonb_build_array(args->0), args->1;
        args := jsonb_build_array(args->0) || (args->1);
        RAISE NOTICE 'IN2 : %', args;
    END IF;



    IF op = 'between' THEN
        args = jsonb_build_array(
            args->0,
            args->1,
            args->2
        );
    END IF;

    -- Make sure that args is an array and run cql2_query on
    -- each element of the array
    RAISE NOTICE 'ARGS PRE: %', args;
    IF j ? 'args' THEN
        IF jsonb_typeof(args) != 'array' THEN
            args := jsonb_build_array(args);
        END IF;

        IF jsonb_path_exists(args, '$[*] ? (@.property == "id" || @.property == "datetime" || @.property == "end_datetime" || @.property == "collection")') THEN
            wrapper := NULL;
        ELSE
            -- if any of the arguments are a property, try to get the property_wrapper
            FOR arg IN SELECT jsonb_path_query(args, '$[*] ? (@.property != null)') LOOP
                RAISE NOTICE 'Arg: %', arg;
                wrapper := (queryable(arg->>'property')).nulled_wrapper;
                RAISE NOTICE 'Property: %, Wrapper: %', arg, wrapper;
                IF wrapper IS NOT NULL THEN
                    EXIT;
                END IF;
            END LOOP;

            -- if the property was not in queryables, see if any args were numbers
            IF
                wrapper IS NULL
                AND jsonb_path_exists(args, '$[*] ? (@.type()=="number")')
            THEN
                wrapper := 'to_float';
            END IF;
            wrapper := coalesce(wrapper, 'to_text');
        END IF;

        SELECT jsonb_agg(cql2_query(a, wrapper))
            INTO args
        FROM jsonb_array_elements(args) a;
    END IF;
    RAISE NOTICE 'ARGS: %', args;

    IF op IN ('and', 'or') THEN
        RETURN
            format(
                '(%s)',
                array_to_string(to_text_array(args), format(' %s ', upper(op)))
            );
    END IF;

    IF op = 'in' THEN
        RAISE NOTICE 'IN --  % %', args->0, to_text(args->0);
        RETURN format(
            '%s IN (%s)',
            to_text(args->0),
            array_to_string((to_text_array(args))[2:], ',')
        );
    END IF;

    -- Look up template from cql2_ops
    IF j ? 'op' THEN
        SELECT * INTO cql2op FROM cql2_ops WHERE  cql2_ops.op ilike op;
        IF FOUND THEN
            -- If specific index set in queryables for a property cast other arguments to that type

            RETURN format(
                cql2op.template,
                VARIADIC (to_text_array(args))
            );
        ELSE
            RAISE EXCEPTION 'Operator % Not Supported.', op;
        END IF;
    END IF;


    IF wrapper IS NOT NULL THEN
        RAISE NOTICE 'Wrapping % with %', j, wrapper;
        IF j ? 'property' THEN
            SELECT * INTO queryable_row FROM queryable(j->>'property');
            -- For native promoted columns (expression = path, no JSONB extraction),
            -- the column's type already matches; applying a cast wrapper like to_int()
            -- is redundant and prevents index-only scans.  Return the bare expression.
            IF
                wrapper = ANY (ARRAY['to_int', 'to_float', 'to_tstz', 'to_text', 'to_text_array'])
                AND queryable_row.expression = queryable_row.path
            THEN
                RETURN queryable_row.expression;
            END IF;
            RETURN format('%I(%s)', wrapper, queryable_row.path);
        ELSE
            RETURN format('%I(%L)', wrapper, j);
        END IF;
    ELSIF j ? 'property' THEN
        RETURN quote_ident(j->>'property');
    END IF;

    RETURN quote_literal(to_text(j));
END;
$$ LANGUAGE PLPGSQL STABLE;


CREATE OR REPLACE FUNCTION paging_dtrange(
    j jsonb
) RETURNS tstzrange AS $$
DECLARE
    op text;
    filter jsonb := j->'filter';
    dtrange tstzrange := tstzrange('-infinity'::timestamptz,'infinity'::timestamptz);
    sdate timestamptz := '-infinity'::timestamptz;
    edate timestamptz := 'infinity'::timestamptz;
    jpitem jsonb;
BEGIN

    IF j ? 'datetime' THEN
        dtrange := parse_dtrange(j->'datetime');
        sdate := lower(dtrange);
        edate := upper(dtrange);
    END IF;
    IF NOT (filter  @? '$.**.op ? (@ == "or" || @ == "not")') THEN
        FOR jpitem IN SELECT jpval FROM jsonb_path_query(filter,'strict $.** ? (@.args[*].property == "datetime")'::jsonpath) AS jp(jpval) LOOP
            op := lower(jpitem->>'op');
            dtrange := parse_dtrange(jpitem->'args'->1);
            IF op IN ('<=', 'lt', 'lte', '<', 'le', 't_before') THEN
                sdate := greatest(sdate,'-infinity');
                edate := least(edate, upper(dtrange));
            ELSIF op IN ('>=', '>', 'gt', 'gte', 'ge', 't_after') THEN
                edate := least(edate, 'infinity');
                sdate := greatest(sdate, lower(dtrange));
            ELSIF op IN ('=', 'eq') THEN
                edate := least(edate, upper(dtrange));
                sdate := greatest(sdate, lower(dtrange));
            END IF;
            RAISE NOTICE '2 OP: %, ARGS: %, DTRANGE: %, SDATE: %, EDATE: %', op, jpitem->'args'->1, dtrange, sdate, edate;
        END LOOP;
    END IF;
    IF sdate > edate THEN
        RETURN 'empty'::tstzrange;
    END IF;
    RETURN tstzrange(sdate,edate, '[]');
END;
$$ LANGUAGE PLPGSQL STABLE STRICT SET TIME ZONE 'UTC';

-- ============================================================================
-- Predicate envelope: a safe over-approximation of the (datetime, end_datetime,
-- geometry) any matching row must fall within, derived from the FULL search
-- predicate. Used by chunker() (004_search.sql) to prune partition candidates by an
-- indexed range query. tstzmultirange keeps OR of disjoint ranges tight;
-- AND = intersect, OR = hull/union, NOT/unknown = full (never a false negative).
-- ============================================================================
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname='pred_envelope'
                   AND typnamespace=(SELECT oid FROM pg_namespace WHERE nspname='pgstac')) THEN
        -- colls: allowed collection set (NULL = all, '{}' = none); dt/edt: allowed
        -- item datetime/end_datetime; geom: spatial bbox bound (NULL = unconstrained).
        CREATE TYPE pred_envelope AS (colls text[], dt tstzmultirange, edt tstzmultirange, geom geometry);
    END IF;
END $$;

CREATE OR REPLACE FUNCTION env_full() RETURNS pred_envelope LANGUAGE sql IMMUTABLE AS $$
    SELECT (NULL::text[],
            tstzmultirange(tstzrange('-infinity','infinity','[]')),
            tstzmultirange(tstzrange('-infinity','infinity','[]')), NULL)::pred_envelope;
$$;

-- AND: intersect every axis (tighter is safe -- a row must satisfy both). For colls,
-- NULL means unconstrained, so AND with NULL keeps the other side; two sets intersect
-- (an empty result '{}' means no collection can match -> no partitions).
CREATE OR REPLACE FUNCTION env_and(a pred_envelope, b pred_envelope) RETURNS pred_envelope LANGUAGE sql IMMUTABLE AS $$
    SELECT (
            CASE WHEN a.colls IS NULL THEN b.colls WHEN b.colls IS NULL THEN a.colls
                 ELSE array_intersection(a.colls, b.colls) END,
            a.dt * b.dt, a.edt * b.edt,
            CASE WHEN a.geom IS NULL THEN b.geom WHEN b.geom IS NULL THEN a.geom
                 ELSE ST_Envelope(ST_Intersection(a.geom, b.geom)) END)::pred_envelope;
$$;

-- OR: union every axis (wider). For colls, if either side is unconstrained (NULL) the
-- union is unconstrained; otherwise it is the de-duplicated union of the two sets.
CREATE OR REPLACE FUNCTION env_or(a pred_envelope, b pred_envelope) RETURNS pred_envelope LANGUAGE sql IMMUTABLE AS $$
    SELECT (
            CASE WHEN a.colls IS NULL OR b.colls IS NULL THEN NULL
                 ELSE ARRAY(SELECT DISTINCT unnest(a.colls || b.colls)) END,
            a.dt + b.dt, a.edt + b.edt,
            CASE WHEN a.geom IS NULL OR b.geom IS NULL THEN NULL
                 ELSE ST_Envelope(ST_Collect(a.geom, b.geom)) END)::pred_envelope;
$$;

-- coerce a cql2 scalar (string | {"timestamp":..} | {"date":..}) to timestamptz
CREATE OR REPLACE FUNCTION cql2_ts(v jsonb) RETURNS timestamptz LANGUAGE sql IMMUTABLE AS $$
    SELECT coalesce(v->>'timestamp', v->>'date', v#>>'{}')::timestamptz;
$$;

-- Resolve a cql2 predicate on the `collection` property to an allowed collection SET.
-- =/in are exact; </<=/>/>=/between/like/ilike are resolved against the live collections
-- table (an empty match returns '{}' = no collection qualifies). NULL = could not bound
-- (treated as unconstrained). STABLE (reads collections).
CREATE OR REPLACE FUNCTION cql2_collection_set(op text, args jsonb) RETURNS text[] LANGUAGE plpgsql STABLE AS $$
DECLARE lst jsonb;
BEGIN
    IF op IN ('=','eq') AND jsonb_typeof(args->1) = 'string' THEN
        RETURN ARRAY[args->>1];
    ELSIF op = 'in' THEN
        lst := CASE WHEN jsonb_typeof(args->1)='object' AND args->1 ? 'list' THEN args->1->'list' ELSE args->1 END;
        IF jsonb_typeof(lst) <> 'array' THEN RETURN NULL; END IF;
        RETURN to_text_array(lst);
    ELSIF jsonb_typeof(args->1) = 'string' THEN
        IF    op IN ('<','lt')   THEN RETURN coalesce((SELECT array_agg(id) FROM collections WHERE id <  (args->>1)), '{}');
        ELSIF op IN ('<=','lte') THEN RETURN coalesce((SELECT array_agg(id) FROM collections WHERE id <= (args->>1)), '{}');
        ELSIF op IN ('>','gt')   THEN RETURN coalesce((SELECT array_agg(id) FROM collections WHERE id >  (args->>1)), '{}');
        ELSIF op IN ('>=','gte') THEN RETURN coalesce((SELECT array_agg(id) FROM collections WHERE id >= (args->>1)), '{}');
        ELSIF op IN ('like')     THEN RETURN coalesce((SELECT array_agg(id) FROM collections WHERE id LIKE  (args->>1)), '{}');
        ELSIF op IN ('ilike')    THEN RETURN coalesce((SELECT array_agg(id) FROM collections WHERE id ILIKE (args->>1)), '{}');
        END IF;
    ELSIF op = 'between' AND jsonb_typeof(args->1)='string' AND jsonb_typeof(args->2)='string' THEN
        RETURN coalesce((SELECT array_agg(id) FROM collections WHERE id BETWEEN (args->>1) AND (args->>2)), '{}');
    END IF;
    RETURN NULL;        -- unsupported shape -> unconstrained
END;
$$;

-- Recursive envelope of a normalized cql2 filter tree.
-- STABLE (not IMMUTABLE): parse_dtrange + cql2_collection_set are STABLE.
CREATE OR REPLACE FUNCTION cql2_envelope(j jsonb) RETURNS pred_envelope LANGUAGE plpgsql STABLE AS $$
DECLARE op text; args jsonb; child jsonb; acc pred_envelope; r tstzrange; prop text; v timestamptz; g geometry;
BEGIN
    IF j IS NULL OR jsonb_typeof(j) <> 'object' OR NOT j ? 'op' THEN RETURN env_full(); END IF;
    op := lower(j->>'op'); args := j->'args';
    IF op = 'and' THEN
        acc := env_full();
        FOR child IN SELECT * FROM jsonb_array_elements(args) LOOP acc := env_and(acc, cql2_envelope(child)); END LOOP;
        RETURN acc;
    ELSIF op = 'or' THEN
        acc := NULL;
        FOR child IN SELECT * FROM jsonb_array_elements(args) LOOP
            acc := CASE WHEN acc IS NULL THEN cql2_envelope(child) ELSE env_or(acc, cql2_envelope(child)) END;
        END LOOP;
        RETURN coalesce(acc, env_full());
    ELSIF op = 'not' THEN
        RETURN env_full();                                       -- negation can't be safely tightened
    ELSIF op ILIKE 't_%' OR op = 'anyinteracts' THEN
        r := parse_dtrange(args->1); acc := env_full();
        IF op IN ('t_intersects','anyinteracts','t_during','t_equals','t_starts','t_finishes','t_overlaps') THEN
            acc.dt  := tstzmultirange(tstzrange('-infinity', upper(r), '(]'));   -- datetime <= rh
            acc.edt := tstzmultirange(tstzrange(lower(r), 'infinity', '[)'));    -- end_datetime >= rl
        ELSIF op IN ('t_before','t_meets') THEN
            acc.edt := tstzmultirange(tstzrange('-infinity', lower(r), '()'));
        ELSIF op IN ('t_after','t_metby') THEN
            acc.dt  := tstzmultirange(tstzrange(upper(r), 'infinity', '()'));
        END IF;
        RETURN acc;
    ELSIF op ILIKE 's_%' OR op = 'intersects' THEN
        BEGIN g := ST_GeomFromGeoJSON(args->1); EXCEPTION WHEN others THEN g := NULL; END;
        acc := env_full(); acc.geom := ST_Envelope(g); RETURN acc;
    ELSIF op IN ('=','<','<=','>','>=','between','eq','lt','lte','gt','gte','in','like','ilike')
          AND jsonb_typeof(args)='array' AND args->0 ? 'property' THEN
        prop := args->0->>'property';
        acc := env_full();
        IF prop IN ('datetime','end_datetime') THEN
            IF op IN ('in','like','ilike') THEN RETURN env_full(); END IF;   -- not a simple temporal bound
            IF op = 'between' THEN          r := tstzrange(cql2_ts(args->1), cql2_ts(args->2), '[]');
            ELSIF op IN ('<','<=','lt','lte') THEN r := tstzrange('-infinity', cql2_ts(args->1), '(]');
            ELSIF op IN ('>','>=','gt','gte') THEN r := tstzrange(cql2_ts(args->1), 'infinity', '[)');
            ELSE v := cql2_ts(args->1);     r := tstzrange(v, v, '[]'); END IF;
            IF prop = 'datetime' THEN acc.dt := tstzmultirange(r); ELSE acc.edt := tstzmultirange(r); END IF;
            RETURN acc;
        ELSIF prop = 'collection' THEN
            acc.colls := cql2_collection_set(op, args);          -- =/in/</>/between/like → collection set
            RETURN acc;
        ELSE
            RETURN env_full();                                   -- other property filters: no partition bound
        END IF;
    ELSE
        RETURN env_full();
    END IF;
END;
$$;

-- Full search envelope: top-level collections/datetime/bbox/intersects AND cql2(filter).
-- Collections are taken from the top-level `collections` arg AND from any `collection`
-- predicate anywhere in the cql2 tree (combined with the same AND/OR logic). `ids` do not
-- constrain partitions. If filter extraction fails, the top-level bounds still apply (the
-- filter part falls back to unconstrained) -- the envelope can only over-approximate;
-- correctness is enforced by each band query's WHERE.
-- search_to_cql2: normalize an entire STAC search into ONE CQL2 filter, so the WHERE
-- clause (cql2_query) and the partition-pruning envelope (cql2_envelope) are both
-- derived from a single representation -- no per-parameter parsing duplicated across
-- functions. Each top-level parameter maps to a CQL2 operator:
--   ids         -> in(id, [...])
--   collections -> in(collection, [...])
--   datetime    -> anyinteracts(datetime, <interval>)   (item interval overlaps query)
--   bbox/intersects -> s_intersects(geometry, <geojson>)
--   q           -> q(<terms>)                            (full-text)
--   query       -> query_to_cql2(...)                    (legacy)
--   filter      -> cql2 (cql-json translated to cql2-json)
-- All present parts are AND-ed. Returns NULL for an empty (match-all) search.
CREATE OR REPLACE FUNCTION search_to_cql2(j jsonb) RETURNS jsonb AS $$
DECLARE
    parts jsonb := '[]'::jsonb;
    fil jsonb;
    filterlang text;
    g geometry;
BEGIN
    IF j ? 'ids' THEN
        parts := parts || jsonb_build_object('op', 'in',
            'args', jsonb_build_array(jsonb_build_object('property', 'id'), j->'ids'));
    END IF;
    IF j ? 'collections' THEN
        parts := parts || jsonb_build_object('op', 'in',
            'args', jsonb_build_array(jsonb_build_object('property', 'collection'), j->'collections'));
    END IF;
    IF j ? 'datetime' THEN
        parts := parts || jsonb_build_object('op', 'anyinteracts',
            'args', jsonb_build_array(jsonb_build_object('property', 'datetime'), j->'datetime'));
    END IF;
    g := stac_geom(j);
    IF g IS NOT NULL THEN
        parts := parts || jsonb_build_object('op', 's_intersects',
            'args', jsonb_build_array(jsonb_build_object('property', 'geometry'), ST_AsGeoJSON(g)::jsonb));
    END IF;
    IF j ? 'q' THEN
        parts := parts || jsonb_build_object('op', 'q', 'args', j->'q');
    END IF;

    IF j ? 'query' AND j ? 'filter' THEN
        RAISE EXCEPTION 'Can only use either query or filter at one time.';
    END IF;
    IF j ? 'query' THEN
        fil := query_to_cql2(j->'query');
    ELSIF j ? 'filter' THEN
        filterlang := COALESCE(j->>'filter-lang', get_setting('default_filter_lang', j->'conf'));
        IF NOT (j->'filter') @? '$.**.op' OR filterlang = 'cql-json' THEN
            fil := cql1_to_cql2(j->'filter');
        ELSE
            fil := j->'filter';
        END IF;
    END IF;
    IF fil IS NOT NULL THEN
        parts := parts || fil;
    END IF;

    IF jsonb_array_length(parts) = 0 THEN
        RETURN NULL;
    ELSIF jsonb_array_length(parts) = 1 THEN
        RETURN parts->0;
    END IF;
    RETURN jsonb_build_object('op', 'and', 'args', parts);
END;
$$ LANGUAGE PLPGSQL STABLE;

-- The partition-pruning envelope is the envelope of the unified CQL2 filter. On any
-- extraction failure fall back to env_full() (scan all partitions) -- never a false
-- negative; exact correctness is always enforced by the WHERE clause.
CREATE OR REPLACE FUNCTION search_envelope(j jsonb) RETURNS pred_envelope LANGUAGE plpgsql STABLE AS $$
BEGIN
    RETURN cql2_envelope(search_to_cql2(j));
EXCEPTION WHEN others THEN
    RETURN env_full();
END;
$$;
