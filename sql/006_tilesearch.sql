CREATE OR REPLACE FUNCTION xyzsearch(
    IN _x int,
    IN _y int,
    IN _z int,
    IN queryhash text,
    IN _scanlimit int DEFAULT 10000,
    IN _limit int DEFAULT 100,
    IN _timelimit interval DEFAULT '5 seconds'::interval,
    IN skipcovered boolean DEFAULT TRUE
) RETURNS setof items AS $$
DECLARE
    search searches%ROWTYPE;
    curs refcursor;
    _where text;
    query text;
    iter_record items%ROWTYPE;
    exit_flag boolean := FALSE;
    counter int := 1;
    scancounter int := 1;
    remaining_limit int := _scanlimit;
    tile geometry;
    tilearea float;
    unionedgeom geometry;
    clippedgeom geometry;
    unionedgeom_area float := 0;
    prev_area float := 0;

BEGIN
    SELECT * INTO search FROM searches WHERE hash=queryhash;
    tile := st_transform(tileenvelope(_z, _x, _y), 4326);
    tilearea := st_area(tile);
    _where := format('%s AND st_intersects(geometry, %L::geometry)', search._where, tile);
    FOR query IN SELECT * FROM partition_queries(_where, search.orderby) LOOP
        query := format('%s LIMIT %L', query, remaining_limit);
        RAISE NOTICE '%', query;
        curs = create_cursor(query);
        LOOP
            FETCH curs INTO iter_record;
            EXIT WHEN NOT FOUND;

            clippedgeom := st_intersection(tile, iter_record.geometry);

            IF unionedgeom IS NULL THEN
                unionedgeom := clippedgeom;
            ELSE
                unionedgeom := st_union(unionedgeom, clippedgeom);
            END IF;

            unionedgeom_area := st_area(unionedgeom);

            IF skipcovered AND prev_area = unionedgeom_area THEN
                scancounter := scancounter + 1;
                CONTINUE;
            END IF;

            prev_area := unionedgeom_area;



            RAISE NOTICE '% % % %', st_area(unionedgeom)/tilearea, counter, scancounter, ftime();
            RETURN NEXT iter_record;


            IF counter > _limit
                OR scancounter > _scanlimit
                OR ftime() > _timelimit
                OR unionedgeom_area >= tilearea
            THEN
                exit_flag := TRUE;
                EXIT;
            END IF;
            counter := counter + 1;
            scancounter := scancounter + 1;

        END LOOP;
        EXIT WHEN exit_flag;
        remaining_limit := _scanlimit - scancounter;
    END LOOP;
RETURN;
END;
$$ LANGUAGE PLPGSQL;
