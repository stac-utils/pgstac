/* Functions to create an iterable of cursors over partitions. */
CREATE OR REPLACE FUNCTION create_cursor(q text) RETURNS refcursor AS $$
DECLARE
    curs refcursor;
BEGIN
    OPEN curs FOR EXECUTE q;
    RETURN curs;
END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION partition_cursor(
    IN _where text DEFAULT 'TRUE',
    IN _orderby text DEFAULT 'datetime DESC, id DESC',
    IN _dtrange tstzrange DEFAULT tstzrange('-infinity','infinity')
) RETURNS SETOF refcursor AS $$
DECLARE
    partition_query text;
    main_query text;
    batchcount int;
    counter int := 0;
    p record;
    cursors refcursor;
BEGIN
IF _orderby ILIKE 'datetime d%' THEN
    partition_query := format($q$
        SELECT partition
        FROM items_partitions
        WHERE tstzrange && $1
        ORDER BY tstzrange DESC;
    $q$);
ELSIF _orderby ILIKE 'datetime a%' THEN
    partition_query := format($q$
        SELECT partition
        FROM items_partitions
        WHERE tstzrange && $1
        ORDER BY tstzrange ASC
        ;
    $q$);
ELSE
    partition_query := format($q$
        SELECT 'items' as partition WHERE $1 IS NOT NULL;
    $q$);
END IF;
RAISE NOTICE 'Partition Query: %', partition_query;
FOR p IN
    EXECUTE partition_query USING (_dtrange)
LOOP
    IF lower(_dtrange)::timestamptz > '-infinity' THEN
        _where := concat(_where,format(' AND datetime >= %L',lower(_dtrange)::timestamptz::text));
    END IF;
    IF upper(_dtrange)::timestamptz < 'infinity' THEN
        _where := concat(_where,format(' AND datetime <= %L',upper(_dtrange)::timestamptz::text));
    END IF;

    main_query := format($q$
        SELECT * FROM %I items
        WHERE %s
        ORDER BY %s
    $q$, p.partition::text, _where, _orderby
    );

    RETURN NEXT create_cursor(main_query);
END LOOP;
RETURN;
END;
$$ LANGUAGE PLPGSQL SET SEARCH_PATH TO pgstac,public;
