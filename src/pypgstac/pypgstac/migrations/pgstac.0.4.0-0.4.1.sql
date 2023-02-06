SET SEARCH_PATH to pgstac, public;
alter table "pgstac"."search_wheres" drop constraint "search_wheres_pkey";

drop index if exists "pgstac"."search_wheres_pkey";

alter table "pgstac"."search_wheres" add column "id" bigint generated always as identity not null;

CREATE UNIQUE INDEX search_wheres_where ON pgstac.search_wheres USING btree (md5(_where));

CREATE UNIQUE INDEX search_wheres_pkey ON pgstac.search_wheres USING btree (id);

alter table "pgstac"."search_wheres" add constraint "search_wheres_pkey" PRIMARY KEY using index "search_wheres_pkey";

set check_function_bodies = off;

CREATE OR REPLACE FUNCTION pgstac.cql_to_where(_search jsonb DEFAULT '{}'::jsonb)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
DECLARE
filterlang text;
search jsonb := _search;
_where text;
BEGIN

RAISE NOTICE 'SEARCH CQL Final: %', search;
filterlang := COALESCE(
    search->>'filter-lang',
    get_setting('default-filter-lang', _search->'conf')
);

IF filterlang = 'cql-json' THEN
    search := query_to_cqlfilter(search);
    search := add_filters_to_cql(search);
    _where := cql_query_op(search->'filter');
ELSE
    _where := cql2_query(search->'filter');
END IF;

IF trim(_where) = '' THEN
    _where := NULL;
END IF;
_where := coalesce(_where, ' TRUE ');
RETURN _where;
END;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.where_stats(inwhere text, updatestats boolean DEFAULT false, conf jsonb DEFAULT NULL::jsonb)
 RETURNS search_wheres
 LANGUAGE plpgsql
AS $function$
DECLARE
    t timestamptz;
    i interval;
    explain_json jsonb;
    partitions text[];
    sw search_wheres%ROWTYPE;
BEGIN
    SELECT * INTO sw FROM search_wheres WHERE _where=inwhere FOR UPDATE;

    -- Update statistics if explicitly set, if statistics do not exist, or statistics ttl has expired
    IF NOT updatestats THEN
        RAISE NOTICE 'Checking if update is needed.';
        RAISE NOTICE 'Stats Last Updated: %', sw.statslastupdated;
        RAISE NOTICE 'TTL: %, Age: %', context_stats_ttl(conf), now() - sw.statslastupdated;
        RAISE NOTICE 'Context: %, Existing Total: %', context(conf), sw.total_count;
        IF
            sw.statslastupdated IS NULL
            OR (now() - sw.statslastupdated) > context_stats_ttl(conf)
            OR (context(conf) != 'off' AND sw.total_count IS NULL)
        THEN
            updatestats := TRUE;
        END IF;
    END IF;

    sw._where := inwhere;
    sw.lastused := now();
    sw.usecount := coalesce(sw.usecount,0) + 1;

    IF NOT updatestats THEN
        UPDATE search_wheres SET
            lastused = sw.lastused,
            usecount = sw.usecount
        WHERE _where = inwhere
        RETURNING * INTO sw
        ;
        RETURN sw;
    END IF;
    -- Use explain to get estimated count/cost and a list of the partitions that would be hit by the query
    t := clock_timestamp();
    EXECUTE format('EXPLAIN (format json) SELECT 1 FROM items WHERE %s', inwhere)
    INTO explain_json;
    RAISE NOTICE 'Time for just the explain: %', clock_timestamp() - t;
    WITH t AS (
        SELECT j->>0 as p FROM
            jsonb_path_query(
                explain_json,
                'strict $.**."Relation Name" ? (@ != null)'
            ) j
    ), ordered AS (
        SELECT p FROM t ORDER BY p DESC
        -- SELECT p FROM t JOIN items_partitions
        --     ON (t.p = items_partitions.partition)
        -- ORDER BY pstart DESC
    )
    SELECT array_agg(p) INTO partitions FROM ordered;
    i := clock_timestamp() - t;
    RAISE NOTICE 'Time for explain + join: %', clock_timestamp() - t;



    sw.statslastupdated := now();
    sw.estimated_count := explain_json->0->'Plan'->'Plan Rows';
    sw.estimated_cost := explain_json->0->'Plan'->'Total Cost';
    sw.time_to_estimate := extract(epoch from i);
    sw.partitions := partitions;

    -- Do a full count of rows if context is set to on or if auto is set and estimates are low enough
    IF
        context(conf) = 'on'
        OR
        ( context(conf) = 'auto' AND
            (
                sw.estimated_count < context_estimated_count(conf)
                OR
                sw.estimated_cost < context_estimated_cost(conf)
            )
        )
    THEN
        t := clock_timestamp();
        RAISE NOTICE 'Calculating actual count...';
        EXECUTE format(
            'SELECT count(*) FROM items WHERE %s',
            inwhere
        ) INTO sw.total_count;
        i := clock_timestamp() - t;
        RAISE NOTICE 'Actual Count: % -- %', sw.total_count, i;
        sw.time_to_count := extract(epoch FROM i);
    ELSE
        sw.total_count := NULL;
        sw.time_to_count := NULL;
    END IF;


    INSERT INTO search_wheres
        (_where, lastused, usecount, statslastupdated, estimated_count, estimated_cost, time_to_estimate, partitions, total_count, time_to_count)
    SELECT sw._where, sw.lastused, sw.usecount, sw.statslastupdated, sw.estimated_count, sw.estimated_cost, sw.time_to_estimate, sw.partitions, sw.total_count, sw.time_to_count
    ON CONFLICT ((md5(_where)))
    DO UPDATE
        SET
            lastused = sw.lastused,
            usecount = sw.usecount,
            statslastupdated = sw.statslastupdated,
            estimated_count = sw.estimated_count,
            estimated_cost = sw.estimated_cost,
            time_to_estimate = sw.time_to_estimate,
            partitions = sw.partitions,
            total_count = sw.total_count,
            time_to_count = sw.time_to_count
    ;
    RETURN sw;
END;
$function$
;



SELECT set_version('0.4.1');
