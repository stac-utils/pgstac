SET SEARCH_PATH to pgstac, public;

DROP FUNCTION IF EXISTS geometrysearch;
CREATE OR REPLACE FUNCTION geometrysearch(
    IN geom geometry,
    IN queryhash text,
    IN fields jsonb DEFAULT NULL,
    IN _scanlimit int DEFAULT 10000,
    IN _limit int DEFAULT 100,
    IN _timelimit interval DEFAULT '5 seconds'::interval,
    IN skipcovered boolean DEFAULT TRUE
) RETURNS jsonb AS $$
DECLARE
    search searches%ROWTYPE;
    curs refcursor;
    _where text;
    query text;
    iter_record items%ROWTYPE;
    out_records jsonb[] := '{}'::jsonb[];
    exit_flag boolean := FALSE;
    counter int := 1;
    scancounter int := 1;
    remaining_limit int := _scanlimit;
    tilearea float;
    unionedgeom geometry;
    clippedgeom geometry;
    unionedgeom_area float := 0;
    prev_area float := 0;
    excludes text[];
    includes text[];

BEGIN
    SELECT * INTO search FROM searches WHERE hash=queryhash;
    tilearea := st_area(geom);
    _where := format('%s AND st_intersects(geometry, %L::geometry)', search._where, geom);

    IF fields IS NOT NULL THEN
        IF fields ? 'fields' THEN
            fields := fields->'fields';
        END IF;
        IF fields ? 'exclude' THEN
            excludes=textarr(fields->'exclude');
        END IF;
        IF fields ? 'include' THEN
            includes=textarr(fields->'include');
            IF array_length(includes, 1)>0 AND NOT 'id' = ANY (includes) THEN
                includes = includes || '{id}';
            END IF;
        END IF;
    END IF;
    RAISE NOTICE 'fields: %, includes: %, excludes: %', fields, includes, excludes;

    FOR query IN SELECT * FROM partition_queries(_where, search.orderby) LOOP
        query := format('%s LIMIT %L', query, remaining_limit);
        RAISE NOTICE '%', query;
        curs = create_cursor(query);
        LOOP
            FETCH curs INTO iter_record;
            EXIT WHEN NOT FOUND;

            clippedgeom := st_intersection(geom, iter_record.geometry);

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

            RAISE NOTICE '% % % %', unionedgeom_area/tilearea, counter, scancounter, ftime();
            IF fields IS NOT NULL THEN
                out_records := out_records || filter_jsonb(iter_record.content, includes, excludes);
            ELSE
                out_records := out_records || iter_record.content;
            END IF;

            IF counter >= _limit
                OR scancounter > _scanlimit
                OR ftime() > _timelimit
                OR (skipcovered AND unionedgeom_area >= tilearea)
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

    RETURN jsonb_build_object(
        'type', 'FeatureCollection',
        'features', array_to_json(out_records)::jsonb
    );
END;
$$ LANGUAGE PLPGSQL;

DROP FUNCTION IF EXISTS geojsonsearch;
CREATE OR REPLACE FUNCTION geojsonsearch(
    IN geojson jsonb,
    IN queryhash text,
    IN fields jsonb DEFAULT NULL,
    IN _scanlimit int DEFAULT 10000,
    IN _limit int DEFAULT 100,
    IN _timelimit interval DEFAULT '5 seconds'::interval,
    IN skipcovered boolean DEFAULT TRUE
) RETURNS jsonb AS $$
    SELECT * FROM geometrysearch(
        st_geomfromgeojson(geojson),
        queryhash,
        fields,
        _scanlimit,
        _limit,
        _timelimit,
        skipcovered
    );
$$ LANGUAGE SQL;

DROP FUNCTION IF EXISTS xyzsearch;
CREATE OR REPLACE FUNCTION xyzsearch(
    IN _x int,
    IN _y int,
    IN _z int,
    IN queryhash text,
    IN fields jsonb DEFAULT NULL,
    IN _scanlimit int DEFAULT 10000,
    IN _limit int DEFAULT 100,
    IN _timelimit interval DEFAULT '5 seconds'::interval,
    IN skipcovered boolean DEFAULT TRUE
) RETURNS jsonb AS $$
    SELECT * FROM geometrysearch(
        st_transform(tileenvelope(_z, _x, _y), 4326),
        queryhash,
        fields,
        _scanlimit,
        _limit,
        _timelimit,
        skipcovered
    );
$$ LANGUAGE SQL;
