/* Functions to create an iterable of cursors over partitions. */
CREATE OR REPLACE FUNCTION create_cursor(q text) RETURNS refcursor AS $$
DECLARE
    curs refcursor;
BEGIN
    OPEN curs FOR EXECUTE q;
    RETURN curs;
END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION partition_queries(
    IN _where text DEFAULT 'TRUE',
    IN _orderby text DEFAULT 'datetime DESC, id DESC'
) RETURNS SETOF text AS $$
DECLARE
    partition_query text;
    query text;
    p record;
    cursors refcursor;
BEGIN
IF _orderby ILIKE 'datetime d%' THEN
    partition_query := format($q$
        SELECT partition, tstzrange
        FROM items_partitions
        ORDER BY tstzrange DESC;
    $q$);
ELSIF _orderby ILIKE 'datetime a%' THEN
    partition_query := format($q$
        SELECT partition, tstzrange
        FROM items_partitions
        ORDER BY tstzrange ASC
        ;
    $q$);
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
--RAISE NOTICE 'Partition Query: %', partition_query;
FOR p IN
    EXECUTE partition_query
LOOP
    query := format($q$
        SELECT * FROM items
        WHERE datetime >= %L AND datetime < %L AND %s
        ORDER BY %s
    $q$, lower(p.tstzrange), upper(p.tstzrange), _where, _orderby
    );
    --RAISE NOTICE 'query: %', query;
    RETURN NEXT query;
END LOOP;
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
FOR query IN SELECT * FROM partion_queries(_where, _orderby) LOOP
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
