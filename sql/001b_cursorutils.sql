/* Functions to create an iterable of cursors over partitions. */
CREATE OR REPLACE FUNCTION create_cursor(q text) RETURNS refcursor AS $$
DECLARE
    curs refcursor;
BEGIN
    OPEN curs FOR EXECUTE q;
    RETURN curs;
END;
$$ LANGUAGE PLPGSQL;

DROP FUNCTION IF EXISTS partition_queries;
CREATE OR REPLACE FUNCTION partition_queries(
    IN _where text DEFAULT 'TRUE',
    IN _orderby text DEFAULT 'datetime DESC, id DESC',
    IN partitions text[] DEFAULT '{items}'
) RETURNS SETOF text AS $$
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
$$ LANGUAGE PLPGSQL SET SEARCH_PATH TO pgstac,public;

CREATE OR REPLACE FUNCTION partition_cursor(
    IN _where text DEFAULT 'TRUE',
    IN _orderby text DEFAULT 'datetime DESC, id DESC'
) RETURNS SETOF refcursor AS $$
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
$$ LANGUAGE PLPGSQL SET SEARCH_PATH TO pgstac,public;

CREATE OR REPLACE FUNCTION partition_count(
    IN _where text DEFAULT 'TRUE'
) RETURNS bigint AS $$
DECLARE
    partition_query text;
    query text;
    p record;
    subtotal bigint;
    total bigint := 0;
BEGIN
partition_query := format($q$
    SELECT partition, tstzrange
    FROM items_partitions
    ORDER BY tstzrange DESC;
$q$);
RAISE NOTICE 'Partition Query: %', partition_query;
FOR p IN
    EXECUTE partition_query
LOOP
    query := format($q$
        SELECT count(*) FROM items
        WHERE datetime BETWEEN %L AND %L AND %s
    $q$, lower(p.tstzrange), upper(p.tstzrange), _where
    );
    RAISE NOTICE 'Query %', query;
    RAISE NOTICE 'Partition %, Count %, Total %',p.partition, subtotal, total;
    EXECUTE query INTO subtotal;
    total := subtotal + total;
END LOOP;
RETURN total;
END;
$$ LANGUAGE PLPGSQL SET SEARCH_PATH TO pgstac,public;


CREATE OR REPLACE FUNCTION drop_partition_constraints(IN partition text) RETURNS VOID AS $$
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
$$ LANGUAGE PLPGSQL;

DROP FUNCTION IF EXISTS partition_checks;
CREATE OR REPLACE FUNCTION partition_checks(
    IN partition text,
    OUT min_datetime timestamptz,
    OUT max_datetime timestamptz,
    OUT min_end_datetime timestamptz,
    OUT max_end_datetime timestamptz,
    OUT collections text[],
    OUT cnt bigint
) RETURNS RECORD AS $$
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
$$ LANGUAGE PLPGSQL;
