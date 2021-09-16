SET SEARCH_PATH to pgstac, public;

CREATE TEMP TABLE temp_migrations AS SELECT version, max(datetime) as datetime from migrations group by 1;
TRUNCATE pgstac.migrations;
INSERT INTO pgstac.migrations SELECT * FROM temp_migrations;

drop function if exists "pgstac"."partition_queries"(_where text, _orderby text);

drop function if exists "pgstac"."search_hash"(jsonb);

drop function if exists "pgstac"."search_query"(_search jsonb, updatestats boolean);

drop view if exists "pgstac"."items_partitions";

drop view if exists "pgstac"."all_items_partitions";

create table "pgstac"."search_wheres" (
    "_where" text not null,
    "lastused" timestamp with time zone default now(),
    "usecount" bigint default 0,
    "statslastupdated" timestamp with time zone,
    "estimated_count" bigint,
    "estimated_cost" double precision,
    "time_to_estimate" double precision,
    "total_count" bigint,
    "time_to_count" double precision,
    "partitions" text[]
);


alter table "pgstac"."migrations" alter column "datetime" set default clock_timestamp();

alter table "pgstac"."migrations" alter column "version" set not null;

alter table "pgstac"."searches" drop column "estimated_count";

alter table "pgstac"."searches" drop column "statslastupdated";

alter table "pgstac"."searches" drop column "total_count";

alter table "pgstac"."searches" add column "metadata" jsonb not null default '{}'::jsonb;

--alter table "pgstac"."searches" alter column "hash" set default pgstac.search_hash(search, metadata);


CREATE OR REPLACE FUNCTION pgstac.search_hash(jsonb, jsonb)
 RETURNS text
 LANGUAGE sql
 IMMUTABLE PARALLEL SAFE
AS $function$
    SELECT md5(concat(search_tohash($1)::text,$2::text));
$function$
;

alter table "pgstac"."searches" add column "hash" text generated always as ("pgstac".search_hash(search, metadata)) stored primary key;

CREATE UNIQUE INDEX migrations_pkey ON pgstac.migrations USING btree (version);

CREATE UNIQUE INDEX search_wheres_pkey ON pgstac.search_wheres USING btree (_where);

alter table "pgstac"."migrations" add constraint "migrations_pkey" PRIMARY KEY using index "migrations_pkey";

alter table "pgstac"."search_wheres" add constraint "search_wheres_pkey" PRIMARY KEY using index "search_wheres_pkey";

set check_function_bodies = off;

CREATE OR REPLACE FUNCTION pgstac.array_reverse(anyarray)
 RETURNS anyarray
 LANGUAGE sql
 IMMUTABLE STRICT
AS $function$
SELECT ARRAY(
    SELECT $1[i]
    FROM generate_subscripts($1,1) AS s(i)
    ORDER BY i DESC
);
$function$
;

CREATE OR REPLACE FUNCTION pgstac.context()
 RETURNS text
 LANGUAGE sql
AS $function$
  SELECT get_setting('pgstac.context','off'::text);
$function$
;

CREATE OR REPLACE FUNCTION pgstac.context_estimated_cost()
 RETURNS double precision
 LANGUAGE sql
AS $function$
  SELECT get_setting('pgstac.context_estimated_cost', 1000000::float);
$function$
;

CREATE OR REPLACE FUNCTION pgstac.context_estimated_count()
 RETURNS integer
 LANGUAGE sql
AS $function$
  SELECT get_setting('pgstac.context_estimated_count', 100000::int);
$function$
;

CREATE OR REPLACE FUNCTION pgstac.context_stats_ttl()
 RETURNS interval
 LANGUAGE sql
AS $function$
  SELECT get_setting('pgstac.context_stats_ttl', '1 day'::interval);
$function$
;

CREATE OR REPLACE FUNCTION pgstac.drop_partition_constraints(partition text)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
DECLARE
    q text;
    end_datetime_constraint text := concat(partition, '_end_datetime_constraint');
    collections_constraint text := concat(partition, '_collections_constraint');
BEGIN
    q := format($q$
            ALTER TABLE %I DROP CONSTRAINT IF EXISTS %I;
            ALTER TABLE %I DROP CONSTRAINT IF EXISTS %I;
        $q$,
        partition,
        end_datetime_constraint,
        partition,
        collections_constraint
    );

    EXECUTE q;
    RETURN;

END;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.field_orderby(p text)
 RETURNS text
 LANGUAGE sql
AS $function$
WITH t AS (
    SELECT
        replace(trim(substring(indexdef from 'btree \((.*)\)')),' ','')as s
    FROM pg_indexes WHERE schemaname='pgstac' AND tablename='items' AND indexdef ~* 'btree' AND indexdef ~* 'properties'
) SELECT s FROM t WHERE strpos(s, lower(trim(p)))>0;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.get_setting(setting text, INOUT _default anynonarray DEFAULT NULL::text)
 RETURNS anynonarray
 LANGUAGE plpgsql
AS $function$
DECLARE
_type text;
BEGIN
  SELECT pg_typeof(_default) INTO _type;
  IF _type = 'unknown' THEN _type='text'; END IF;
  EXECUTE format($q$
    SELECT COALESCE(
      CAST(current_setting($1,TRUE) AS %s),
      $2
    )
    $q$, _type)
    INTO _default
    USING setting, _default
  ;
  RETURN;
END;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.get_version()
 RETURNS text
 LANGUAGE sql
 SET search_path TO 'pgstac', 'public'
AS $function$
  SELECT version FROM migrations ORDER BY datetime DESC, version DESC LIMIT 1;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.items_count(_where text)
 RETURNS bigint
 LANGUAGE plpgsql
AS $function$
DECLARE
cnt bigint;
BEGIN
EXECUTE format('SELECT count(*) FROM items WHERE %s', _where) INTO cnt;
RETURN cnt;
END;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.partition_checks(partition text, OUT min_datetime timestamp with time zone, OUT max_datetime timestamp with time zone, OUT min_end_datetime timestamp with time zone, OUT max_end_datetime timestamp with time zone, OUT collections text[], OUT cnt bigint)
 RETURNS record
 LANGUAGE plpgsql
AS $function$
DECLARE
q text;
end_datetime_constraint text := concat(partition, '_end_datetime_constraint');
collections_constraint text := concat(partition, '_collections_constraint');
BEGIN
RAISE NOTICE 'CREATING CONSTRAINTS FOR %', partition;
q := format($q$
        SELECT
            min(datetime),
            max(datetime),
            min(end_datetime),
            max(end_datetime),
            array_agg(DISTINCT collection_id),
            count(*)
        FROM %I;
    $q$,
    partition
);
EXECUTE q INTO min_datetime, max_datetime, min_end_datetime, max_end_datetime, collections, cnt;
RAISE NOTICE '% % % % % %', min_datetime, max_datetime, min_end_datetime, max_end_datetime, collections, cnt;
IF cnt IS NULL or cnt = 0 THEN
    RAISE NOTICE 'Partition % is empty, removing...', partition;
    q := format($q$
        DROP TABLE IF EXISTS %I;
        $q$, partition
    );
    EXECUTE q;
    RETURN;
END IF;
q := format($q$
        ALTER TABLE %I DROP CONSTRAINT IF EXISTS %I;
        ALTER TABLE %I ADD CONSTRAINT %I
            check((end_datetime >= %L) AND (end_datetime <= %L));
        ALTER TABLE %I DROP CONSTRAINT IF EXISTS %I;
        ALTER TABLE %I ADD CONSTRAINT %I
            check((collection_id = ANY(%L)));
        ANALYZE %I;
    $q$,
    partition,
    end_datetime_constraint,
    partition,
    end_datetime_constraint,
    min_end_datetime,
    max_end_datetime,
    partition,
    collections_constraint,
    partition,
    collections_constraint,
    collections,
    partition
);

EXECUTE q;
RETURN;

END;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.partition_queries(_where text DEFAULT 'TRUE'::text, _orderby text DEFAULT 'datetime DESC, id DESC'::text, partitions text[] DEFAULT '{items}'::text[])
 RETURNS SETOF text
 LANGUAGE plpgsql
 SET search_path TO 'pgstac', 'public'
AS $function$
DECLARE
    partition_query text;
    query text;
    p text;
    cursors refcursor;
    dstart timestamptz;
    dend timestamptz;
    step interval := '10 weeks'::interval;
BEGIN

IF _orderby ILIKE 'datetime d%' THEN
    partitions := partitions;
ELSIF _orderby ILIKE 'datetime a%' THEN
    partitions := array_reverse(partitions);
ELSE
    query := format($q$
        SELECT * FROM items
        WHERE %s
        ORDER BY %s
    $q$, _where, _orderby
    );

    RETURN NEXT query;
    RETURN;
END IF;
RAISE NOTICE 'PARTITIONS ---> %',partitions;
IF cardinality(partitions) > 0 THEN
    FOREACH p IN ARRAY partitions
        --EXECUTE partition_query
    LOOP
        query := format($q$
            SELECT * FROM %I
            WHERE %s
            ORDER BY %s
            $q$,
            p,
            _where,
            _orderby
        );
        RETURN NEXT query;
    END LOOP;
END IF;
RETURN;
END;
$function$
;


CREATE OR REPLACE FUNCTION pgstac.search_query(_search jsonb DEFAULT '{}'::jsonb, updatestats boolean DEFAULT false, _metadata jsonb DEFAULT '{}'::jsonb)
 RETURNS pgstac.searches
 LANGUAGE plpgsql
AS $function$
DECLARE
    search searches%ROWTYPE;
    pexplain jsonb;
    t timestamptz;
    i interval;
BEGIN
SELECT * INTO search FROM searches
WHERE hash=search_hash(_search, _metadata) FOR UPDATE;

-- Calculate the where clause if not already calculated
IF search._where IS NULL THEN
    search._where := cql_to_where(_search);
END IF;

-- Calculate the order by clause if not already calculated
IF search.orderby IS NULL THEN
    search.orderby := sort_sqlorderby(_search);
END IF;

PERFORM where_stats(search._where, updatestats);

search.lastused := now();
search.usecount := coalesce(search.usecount, 0) + 1;
INSERT INTO searches (search, _where, orderby, lastused, usecount, metadata)
VALUES (_search, search._where, search.orderby, search.lastused, search.usecount, _metadata)
ON CONFLICT (hash) DO
UPDATE SET
    _where = EXCLUDED._where,
    orderby = EXCLUDED.orderby,
    lastused = EXCLUDED.lastused,
    usecount = EXCLUDED.usecount,
    metadata = EXCLUDED.metadata
RETURNING * INTO search
;
RETURN search;

END;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.set_version(text)
 RETURNS text
 LANGUAGE sql
 SET search_path TO 'pgstac', 'public'
AS $function$
  INSERT INTO migrations (version) VALUES ($1)
  ON CONFLICT DO NOTHING
  RETURNING version;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.where_stats(inwhere text, updatestats boolean DEFAULT false)
 RETURNS pgstac.search_wheres
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
        RAISE NOTICE 'TTL: %, Age: %', context_stats_ttl(), now() - sw.statslastupdated;
        RAISE NOTICE 'Context: %, Existing Total: %', context(), sw.total_count;
        IF
            sw.statslastupdated IS NULL
            OR (now() - sw.statslastupdated) > context_stats_ttl()
            OR (context() != 'off' AND sw.total_count IS NULL)
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
        context() = 'on'
        OR
        ( context() = 'auto' AND
            (
                sw.estimated_count < context_estimated_count()
                OR
                sw.estimated_cost < context_estimated_cost()
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


    INSERT INTO search_wheres SELECT sw.*
    ON CONFLICT (_where)
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

create or replace view "pgstac"."all_items_partitions" as  WITH base AS (
         SELECT ((c.oid)::regclass)::text AS partition,
            pg_get_expr(c.relpartbound, c.oid) AS _constraint,
            regexp_matches(pg_get_expr(c.relpartbound, c.oid), '\(''([0-9 :+-]*)''\).*\(''([0-9 :+-]*)''\)'::text) AS t,
            (c.reltuples)::bigint AS est_cnt
           FROM pg_class c,
            pg_inherits i
          WHERE ((c.oid = i.inhrelid) AND (i.inhparent = ('pgstac.items'::regclass)::oid))
        )
 SELECT base.partition,
    tstzrange((base.t[1])::timestamp with time zone, (base.t[2])::timestamp with time zone) AS tstzrange,
    (base.t[1])::timestamp with time zone AS pstart,
    (base.t[2])::timestamp with time zone AS pend,
    base.est_cnt
   FROM base
  ORDER BY (tstzrange((base.t[1])::timestamp with time zone, (base.t[2])::timestamp with time zone)) DESC;


CREATE OR REPLACE FUNCTION pgstac.items_partition_name(timestamp with time zone)
 RETURNS text
 LANGUAGE sql
 IMMUTABLE PARALLEL SAFE
AS $function$
    SELECT to_char($1, '"items_p"IYYY"w"IW');
$function$
;

create or replace view "pgstac"."items_partitions" as  SELECT all_items_partitions.partition,
    all_items_partitions.tstzrange,
    all_items_partitions.pstart,
    all_items_partitions.pend,
    all_items_partitions.est_cnt
   FROM pgstac.all_items_partitions
  WHERE (all_items_partitions.est_cnt > 0);


CREATE OR REPLACE FUNCTION pgstac.items_path(dotpath text, OUT field text, OUT path text, OUT path_txt text, OUT jsonpath text, OUT eq text)
 RETURNS record
 LANGUAGE plpgsql
 IMMUTABLE PARALLEL SAFE
AS $function$
DECLARE
path_elements text[];
last_element text;
BEGIN
dotpath := replace(trim(dotpath), 'properties.', '');

IF dotpath = '' THEN
    RETURN;
END IF;

path_elements := string_to_array(dotpath, '.');
jsonpath := NULL;

IF path_elements[1] IN ('id','geometry','datetime') THEN
    field := path_elements[1];
    path_elements := path_elements[2:];
ELSIF path_elements[1] = 'collection' THEN
    field := 'collection_id';
    path_elements := path_elements[2:];
ELSIF path_elements[1] IN ('links', 'assets', 'stac_version', 'stac_extensions') THEN
    field := 'content';
ELSE
    field := 'content';
    path_elements := '{properties}'::text[] || path_elements;
END IF;
IF cardinality(path_elements)<1 THEN
    path := field;
    path_txt := field;
    jsonpath := '$';
    eq := NULL;
    RETURN;
END IF;


last_element := path_elements[cardinality(path_elements)];
path_elements := path_elements[1:cardinality(path_elements)-1];
jsonpath := concat(array_to_string('{$}'::text[] || array_map_ident(path_elements), '.'), '.', quote_ident(last_element));
path_elements := array_map_literal(path_elements);
path     := format($F$ properties->%s $F$, quote_literal(dotpath));
path_txt := format($F$ properties->>%s $F$, quote_literal(dotpath));
eq := format($F$ properties @? '$.%s[*] ? (@ == %%s) '$F$, quote_ident(dotpath));

RAISE NOTICE 'ITEMS PATH -- % % % % %', field, path, path_txt, jsonpath, eq;
RETURN;
END;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.items_staging_ignore_insert_triggerfunc()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
DECLARE
    p record;
BEGIN
    CREATE TEMP TABLE new_partitions ON COMMIT DROP AS
    SELECT
        items_partition_name(stac_datetime(content)) as partition,
        date_trunc('week', min(stac_datetime(content))) as partition_start
    FROM newdata
    GROUP BY 1;

    FOR p IN SELECT new_partitions.partition, new_partitions.partition_start, new_partitions.partition_start + '1 week'::interval as partition_end FROM new_partitions
    LOOP
        RAISE NOTICE 'Getting partition % ready.', p.partition;
        IF NOT items_partition_exists(p.partition) THEN
            RAISE NOTICE 'Creating partition %.', p.partition;
            PERFORM items_partition_create_worker(p.partition, p.partition_start, p.partition_end);
        END IF;
        PERFORM drop_partition_constraints(p.partition);
    END LOOP;

    INSERT INTO items (id, geometry, collection_id, datetime, end_datetime, properties, content)
        SELECT
            content->>'id',
            stac_geom(content),
            content->>'collection',
            stac_datetime(content),
            stac_end_datetime(content),
            properties_idx(content),
            content
        FROM newdata
        ON CONFLICT (datetime, id) DO NOTHING
    ;
    DELETE FROM items_staging;


    FOR p IN SELECT new_partitions.partition FROM new_partitions
    LOOP
        RAISE NOTICE 'Setting constraints for partition %.', p.partition;
        PERFORM partition_checks(p.partition);
    END LOOP;
    DROP TABLE IF EXISTS new_partitions;
    RETURN NULL;
END;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.items_staging_insert_triggerfunc()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
DECLARE
    mindate timestamptz;
    maxdate timestamptz;
    partition text;
    p record;
BEGIN
    CREATE TEMP TABLE new_partitions ON COMMIT DROP AS
    SELECT
        items_partition_name(stac_datetime(content)) as partition,
        date_trunc('week', min(stac_datetime(content))) as partition_start
    FROM newdata
    GROUP BY 1;

    FOR p IN SELECT new_partitions.partition, new_partitions.partition_start, new_partitions.partition_start + '1 week'::interval as partition_end FROM new_partitions
    LOOP
        RAISE NOTICE 'Getting partition % ready.', p.partition;
        IF NOT items_partition_exists(p.partition) THEN
            RAISE NOTICE 'Creating partition %.', p.partition;
            PERFORM items_partition_create_worker(p.partition, p.partition_start, p.partition_end);
        END IF;
        PERFORM drop_partition_constraints(p.partition);
    END LOOP;

    INSERT INTO items (id, geometry, collection_id, datetime, end_datetime, properties, content)
        SELECT
            content->>'id',
            stac_geom(content),
            content->>'collection',
            stac_datetime(content),
            stac_end_datetime(content),
            properties_idx(content),
            content
        FROM newdata
    ;
    DELETE FROM items_staging;


    FOR p IN SELECT new_partitions.partition FROM new_partitions
    LOOP
        RAISE NOTICE 'Setting constraints for partition %.', p.partition;
        PERFORM partition_checks(p.partition);
    END LOOP;
    DROP TABLE IF EXISTS new_partitions;
    RETURN NULL;
END;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.items_staging_upsert_insert_triggerfunc()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
DECLARE
    p record;
BEGIN
    CREATE TEMP TABLE new_partitions ON COMMIT DROP AS
    SELECT
        items_partition_name(stac_datetime(content)) as partition,
        date_trunc('week', min(stac_datetime(content))) as partition_start
    FROM newdata
    GROUP BY 1;

    FOR p IN SELECT new_partitions.partition, new_partitions.partition_start, new_partitions.partition_start + '1 week'::interval as partition_end FROM new_partitions
    LOOP
        RAISE NOTICE 'Getting partition % ready.', p.partition;
        IF NOT items_partition_exists(p.partition) THEN
            RAISE NOTICE 'Creating partition %.', p.partition;
            PERFORM items_partition_create_worker(p.partition, p.partition_start, p.partition_end);
        END IF;
        PERFORM drop_partition_constraints(p.partition);
    END LOOP;

    INSERT INTO items (id, geometry, collection_id, datetime, end_datetime, properties, content)
        SELECT
            content->>'id',
            stac_geom(content),
            content->>'collection',
            stac_datetime(content),
            stac_end_datetime(content),
            properties_idx(content),
            content
        FROM newdata
        ON CONFLICT (datetime, id) DO UPDATE SET
            content = EXCLUDED.content
            WHERE items.content IS DISTINCT FROM EXCLUDED.content
        ;
    DELETE FROM items_staging;


    FOR p IN SELECT new_partitions.partition FROM new_partitions
    LOOP
        RAISE NOTICE 'Setting constraints for partition %.', p.partition;
        PERFORM partition_checks(p.partition);
    END LOOP;
    DROP TABLE IF EXISTS new_partitions;
    RETURN NULL;
END;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.partition_cursor(_where text DEFAULT 'TRUE'::text, _orderby text DEFAULT 'datetime DESC, id DESC'::text)
 RETURNS SETOF refcursor
 LANGUAGE plpgsql
 SET search_path TO 'pgstac', 'public'
AS $function$
DECLARE
    partition_query text;
    query text;
    p record;
    cursors refcursor;
BEGIN
FOR query IN SELECT * FROM partition_queries(_where, _orderby) LOOP
    RETURN NEXT create_cursor(query);
END LOOP;
RETURN;
END;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.search(_search jsonb DEFAULT '{}'::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET jit TO 'off'
AS $function$
DECLARE
    searches searches%ROWTYPE;
    _where text;
    token_where text;
    full_where text;
    orderby text;
    query text;
    token_type text := substr(_search->>'token',1,4);
    _limit int := coalesce((_search->>'limit')::int, 10);
    curs refcursor;
    cntr int := 0;
    iter_record items%ROWTYPE;
    first_record items%ROWTYPE;
    last_record items%ROWTYPE;
    out_records jsonb := '[]'::jsonb;
    prev_query text;
    next text;
    prev_id text;
    has_next boolean := false;
    has_prev boolean := false;
    prev text;
    total_count bigint;
    context jsonb;
    collection jsonb;
    includes text[];
    excludes text[];
    exit_flag boolean := FALSE;
    batches int := 0;
    timer timestamptz := clock_timestamp();
    pstart timestamptz;
    pend timestamptz;
    pcurs refcursor;
    search_where search_wheres%ROWTYPE;
BEGIN
searches := search_query(_search);
_where := searches._where;
orderby := searches.orderby;
search_where := where_stats(_where);
total_count := coalesce(search_where.total_count, search_where.estimated_count);


IF token_type='prev' THEN
    token_where := get_token_filter(_search, null::jsonb);
    orderby := sort_sqlorderby(_search, TRUE);
END IF;
IF token_type='next' THEN
    token_where := get_token_filter(_search, null::jsonb);
END IF;

full_where := concat_ws(' AND ', _where, token_where);
RAISE NOTICE 'FULL QUERY % %', full_where, clock_timestamp()-timer;
timer := clock_timestamp();

FOR query IN SELECT partition_queries(full_where, orderby, search_where.partitions) LOOP
    timer := clock_timestamp();
    query := format('%s LIMIT %s', query, _limit + 1);
    RAISE NOTICE 'Partition Query: %', query;
    batches := batches + 1;
    curs = create_cursor(query);
    --OPEN curs
    LOOP
        FETCH curs into iter_record;
        EXIT WHEN NOT FOUND;
        cntr := cntr + 1;
        last_record := iter_record;
        IF cntr = 1 THEN
            first_record := last_record;
        END IF;
        IF cntr <= _limit THEN
            out_records := out_records || last_record.content;
        ELSIF cntr > _limit THEN
            has_next := true;
            exit_flag := true;
            EXIT;
        END IF;
    END LOOP;
    RAISE NOTICE 'Query took %. Total Time %', clock_timestamp()-timer, ftime();
    timer := clock_timestamp();
    EXIT WHEN exit_flag;
END LOOP;
RAISE NOTICE 'Scanned through % partitions.', batches;


-- Flip things around if this was the result of a prev token query
IF token_type='prev' THEN
    out_records := flip_jsonb_array(out_records);
    first_record := last_record;
END IF;

-- If this query has a token, see if there is data before the first record
IF _search ? 'token' THEN
    prev_query := format(
        'SELECT 1 FROM items WHERE %s LIMIT 1',
        concat_ws(
            ' AND ',
            _where,
            trim(get_token_filter(_search, to_jsonb(first_record)))
        )
    );
    RAISE NOTICE 'Query to get previous record: % --- %', prev_query, first_record;
    EXECUTE prev_query INTO has_prev;
    IF FOUND and has_prev IS NOT NULL THEN
        RAISE NOTICE 'Query results from prev query: %', has_prev;
        has_prev := TRUE;
    END IF;
END IF;
has_prev := COALESCE(has_prev, FALSE);

RAISE NOTICE 'token_type: %, has_next: %, has_prev: %', token_type, has_next, has_prev;
IF has_prev THEN
    prev := out_records->0->>'id';
END IF;
IF has_next OR token_type='prev' THEN
    next := out_records->-1->>'id';
END IF;


-- include/exclude any fields following fields extension
IF _search ? 'fields' THEN
    IF _search->'fields' ? 'exclude' THEN
        excludes=textarr(_search->'fields'->'exclude');
    END IF;
    IF _search->'fields' ? 'include' THEN
        includes=textarr(_search->'fields'->'include');
        IF array_length(includes, 1)>0 AND NOT 'id' = ANY (includes) THEN
            includes = includes || '{id}';
        END IF;
    END IF;
    SELECT jsonb_agg(filter_jsonb(row, includes, excludes)) INTO out_records FROM jsonb_array_elements(out_records) row;
END IF;

IF context() != 'off' THEN
    context := jsonb_strip_nulls(jsonb_build_object(
        'limit', _limit,
        'matched', total_count,
        'returned', coalesce(jsonb_array_length(out_records), 0)
    ));
ELSE
    context := jsonb_strip_nulls(jsonb_build_object(
        'limit', _limit,
        'returned', coalesce(jsonb_array_length(out_records), 0)
    ));
END IF;

collection := jsonb_build_object(
    'type', 'FeatureCollection',
    'features', coalesce(out_records, '[]'::jsonb),
    'next', next,
    'prev', prev,
    'context', context
);

RETURN collection;
END;
$function$
;

CREATE OR REPLACE FUNCTION pgstac.sort_sqlorderby(_search jsonb DEFAULT NULL::jsonb, reverse boolean DEFAULT false)
 RETURNS text
 LANGUAGE sql
AS $function$
WITH sortby AS (
    SELECT coalesce(_search->'sortby','[{"field":"datetime", "direction":"desc"}]') as sort
), withid AS (
    SELECT CASE
        WHEN sort @? '$[*] ? (@.field == "id")' THEN sort
        ELSE sort || '[{"field":"id", "direction":"desc"}]'::jsonb
        END as sort
    FROM sortby
), withid_rows AS (
    SELECT jsonb_array_elements(sort) as value FROM withid
),sorts AS (
    SELECT
        coalesce(field_orderby((items_path(value->>'field')).path_txt), (items_path(value->>'field')).path) as key,
        parse_sort_dir(value->>'direction', reverse) as dir
    FROM withid_rows
)
SELECT array_to_string(
    array_agg(concat(key, ' ', dir)),
    ', '
) FROM sorts;
$function$
;

SELECT partition_checks(partition) FROM all_items_partitions;

SELECT set_version('0.3.5');
