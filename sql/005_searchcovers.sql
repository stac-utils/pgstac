CREATE OR REPLACE FUNCTION quadkey(IN tx int, ty int, zoom int) RETURNS text AS $$
DECLARE
i int;
digit int;
quadkey text := '';
mask int;
BEGIN
--ty := (ty ^ zoom) - ty;
FOR i IN REVERSE zoom..1 LOOP
    digit := 0;
    mask := 1 << (i-1);
    IF (tx & mask) != 0 THEN
        digit :=  digit + 1;
    END IF;
    IF (ty & mask) != 0 THEN
        digit :=  digit + 2;
    END IF;
    quadkey := concat(quadkey, digit::text);
END LOOP;
RETURN quadkey;
END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION quadkeytotile(IN quadkey text, OUT zoom int, OUT x int, OUT y int) RETURNS RECORD AS $$
DECLARE
mask int;
c text;
BEGIN
x := 0;
y:= 0;
zoom := length(quadkey);
FOR i IN REVERSE zoom..1 LOOP
    mask := 1 << (i-1);
    c := substr(quadkey, zoom - i + 1, 1);
    IF c = '1' THEN
        x := x | mask;
    END IF;
    IF c = '2' THEN
        y := y | mask;
    END IF;
    IF c = '3' THEN
        x := x | mask;
        y := y | mask;
    END IF;
END LOOP;
RETURN;
END;
$$ LANGUAGE PLPGSQL IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION tileenvelope(zoom int, x int, y int) RETURNS geometry AS $$
WITH t AS (
    SELECT
        20037508.3427892 as merc_max,
        -20037508.3427892 as merc_min,
        (2 * 20037508.3427892) / (2 ^ zoom) as tile_size
)
SELECT st_makeenvelope(
    merc_min + (tile_size * x),
    merc_max - (tile_size * (y + 1)),
    merc_min + (tile_size * (x + 1)),
    merc_max - (tile_size * y),
    3857
) FROM t;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

DROP FUNCTION IF EXISTS mercgrid;
CREATE OR REPLACE FUNCTION mercgrid(IN _zoom int, IN _x int, IN _y int, IN levels int DEFAULT 4, OUT quadkey text, OUT geom geometry) RETURNS SETOF record AS $$
DECLARE
quadkey text;
x int := _x;
y int := _y;
zoom int := _zoom;
merc_max float := 20037508.3427892;
merc_min float := - merc_max;
tile_size float;
initx float;
finalx float;
inity float;
finaly float;
BEGIN
RAISE NOTICE 'x: % y: % zoom: % levels: % quadkey: %', x, y, zoom, levels, quadkey;
IF levels < 0 THEN
    quadkey := quadkey(x, y, zoom);
    RAISE NOTICE 'quadkey: %', quadkey;
    quadkey := substr(quadkey,1,length(quadkey)+levels);
    RAISE NOTICE 'quadkey adjusted: %', quadkey;
    SELECT * INTO zoom, x, y FROM quadkeytotile(quadkey);
    levels := - levels;
END IF;
RAISE NOTICE 'x: % y: % zoom: % levels: % quadkey: %', x, y, zoom, levels, quadkey;
tile_size := (2 * merc_max) / (2 ^ (zoom + levels));
initx := 2^levels * x;
finalx := (2^levels * (x+1))-1;
inity := 2^levels * y;
finaly := (2^levels * (y+1))-1;
RAISE NOTICE 'initx: %, inity: %, finalx: %, finaly: %', initx, inity, finalx, finaly;
RETURN QUERY
SELECT
    quadkey(dx, dy, zoom + levels),
    st_makeenvelope(
        merc_min + (tile_size * dx),
        merc_max - (tile_size * (dy + 1)),
        merc_min + (tile_size * (dx + 1)),
        merc_max - (tile_size * dy),
        3857
    )
FROM
    generate_series(initx::int, finalx::int, 1) dx,
    generate_series(inity::int, finaly::int, 1) dy
;

RETURN;

END;
$$ LANGUAGE PLPGSQL IMMUTABLE PARALLEL SAFE;



DROP FUNCTION IF EXISTS mercgrid_fromgeom;
CREATE OR REPLACE FUNCTION mercgrid_fromgeom(IN _zoom int, IN _geom geometry, OUT quadkey text, OUT geom geometry) RETURNS SETOF record AS $$
DECLARE
quadkey text;
-- x int := _x;
-- y int := _y;
zoom int := _zoom;
merc_max float := 20037508.3427892;
merc_min float := - merc_max;
tile_size float;
initx float;
finalx float;
inity float;
finaly float;
geom geometry := st_transform(_geom, 3857);
BEGIN
tile_size := (2 * merc_max) / (2 ^ zoom);
initx := floor( (st_xmin(geom) + merc_max) / tile_size );
finalx := ceil( (st_xmax(geom) + merc_max) / tile_size );
inity := floor( (st_ymin(geom) + merc_max) / tile_size );
finaly := ceil( (st_ymax(geom) + merc_max) / tile_size );
-- RAISE NOTICE 'tile_size: %, initx: %, inity: %, finalx: %, finaly: %', tile_size, initx, inity, finalx, finaly;
RETURN QUERY
SELECT
    quadkey(dx, dy, zoom),
    st_makeenvelope(
        merc_min + (tile_size * initx),
        merc_min + (tile_size * inity),
        merc_min + (tile_size * (initx + 1 )),
        merc_min + (tile_size * (inity + 1 )),
        3857
    )
FROM
    generate_series(initx::int, finalx::int, 1) dx,
    generate_series(inity::int, finaly::int, 1) dy
;

RETURN;

END;
$$ LANGUAGE PLPGSQL IMMUTABLE PARALLEL SAFE;



CREATE OR REPLACE FUNCTION ftime() RETURNS text as $$
SELECT age(clock_timestamp(), transaction_timestamp())::text;
$$ LANGUAGE SQL;


DROP FUNCTION IF EXISTS searchcovers;
CREATE OR REPLACE FUNCTION searchcovers(
    IN _zoom int,
    -- IN _x int,
    -- IN _y int,
    -- IN _level int,
    IN _where text DEFAULT 'TRUE',
    IN _dtrange tstzrange DEFAULT tstzrange('-infinity','infinity'),
    IN _orderby text DEFAULT 'datetime DESC, id DESC',
    IN _scan_limit int DEFAULT 1000,
    IN _perquadkey_limit int DEFAULT 1000,
    IN _time_limit interval DEFAULT '5 seconds'::interval,
    OUT quadkey text,
    OUT ids text[],
    OUT geom geometry,
    OUT uniongeom geometry,
    OUT covered boolean
) RETURNS SETOF RECORD AS $$
DECLARE
partition_query text;
main_query text;
batchcount int;
counter int := 0;
p record;
clippedgeom geometry := NULL;
uniongeom geometry := NULL;
diffgeom geometry := NULL;
r record;
exitflag boolean := FALSE;
-- _bbox geometry := st_transform(tileenvelope(_zoom, _x, _y), 4326);
_bbox geometry;
gridcursor refcursor;
rec_ids text[];
rec_uniongeom geometry;
rec_covered boolean;
rowgeom geometry;
rec record;
total_calc interval := 0;
total_update interval := 0;
st timestamptz := clock_timestamp();
ts timestamptz := clock_timestamp();
curs refcursor;
BEGIN
CREATE TEMP TABLE outgrid (
    quadkey text not null primary key,
    uniongeom geometry,
    gridgeom geometry not null,
    ids text[],
    covered boolean default false not null
) ON COMMIT DROP;

--INSERT INTO outgrid (quadkey, gridgeom) SELECT * FROM mercgrid(_zoom, _x, _y, _level);
--RAISE NOTICE 'Created outgrid %', ftime();

CREATE INDEX ON outgrid USING GIST(gridgeom) WHERE not covered;
CREATE INDEX ON outgrid (covered);
ANALYZE outgrid;
RAISE NOTICE 'Indexed outgrid %', ftime();

-- SELECT st_transform(st_setsrid(st_extent(gridgeom), 3857), 4326) INTO _bbox FROM outgrid;
-- RAISE NOTICE 'BBOX: % ( % )', st_asewkt(_bbox), st_asewkt(st_transform(_bbox, 3857));
st := clock_timestamp();

FOR curs IN select * from partition_cursor(_where, _dtrange, _orderby) LOOP
    RAISE NOTICE 'New Partition... %', ftime();
    LOOP
        FETCH curs INTO r;
        EXIT WHEN NOT FOUND;
        -- RAISE NOTICE 'Adding Grid...';
        INSERT INTO outgrid (quadkey, gridgeom)
            SELECT * FROM mercgrid_fromgeom(_zoom, st_transform(r.geometry, 3857))
            ON CONFLICT DO NOTHING;

        SELECT st_transform(st_setsrid(st_extent(gridgeom), 3857), 4326) INTO _bbox FROM outgrid;
        -- RAISE NOTICE 'BBOX: % ( % )', st_asewkt(_bbox), st_asewkt(st_transform(_bbox, 3857));
        IF st_geometrytype(r.geometry) IN  ('ST_Polygon', 'ST_MultiPolygon') THEN
                rowgeom := st_transform(st_intersection(_bbox, r.geometry), 3857);
        ELSE
            rowgeom := st_buffer(st_transform(st_intersection(_bbox, r.geometry), 3857), 6000);
        END IF;

        OPEN gridcursor FOR
            SELECT *, st_area(outgrid.gridgeom) as gridarea
            FROM outgrid
            WHERE
                    NOT outgrid.covered
                AND
                    st_intersects(outgrid.gridgeom, rowgeom);

        LOOP
            FETCH gridcursor INTO rec;
            EXIT WHEN NOT FOUND;
            ts := clock_timestamp();
            rec_ids := array_append(rec.ids, r.id::text);
            IF rec.uniongeom IS NULL THEN
                rec_uniongeom := rowgeom;
            ELSIF _st_covers(rec.uniongeom, rowgeom) THEN
                EXIT;
            ELSE
                rec_uniongeom := st_union(rec.uniongeom, st_intersection(rowgeom, rec.gridgeom));
            END IF;
            IF st_area(rec_uniongeom) >= rec.gridarea THEN
                rec_covered := TRUE;
            ELSE
                rec_covered := FALSE;
            END IF;
            --    rec_
            -- rec_covered := st_covers(st_buffer(rec_uniongeom, 1), rec.gridgeom);
            total_calc := total_calc + age(clock_timestamp(), ts);
            ts := clock_timestamp();
            UPDATE outgrid SET ids = rec_ids, uniongeom = rec_uniongeom, covered = rec_covered WHERE CURRENT OF gridcursor;
            -- RAISE NOTICE 'UPDATE outgrid took %', clock_timestamp() - ts;
            total_update := total_update + age(clock_timestamp(), ts);
        END LOOP;
        CLOSE gridcursor;
        counter := counter + 1;
        RAISE NOTICE 'counter: %, thisloop: %, duration: %, covered: %', counter, clock_timestamp() - ts, clock_timestamp() - st, (SELECT count(*) FROM outgrid WHERE outgrid.covered);
        IF
            (counter >= _scan_limit)
             or (NOT EXISTS (SELECT 1 FROM outgrid WHERE outgrid.covered = false))
            OR (clock_timestamp() - st > _time_limit)
        THEN
            exitflag := TRUE;
            exit;
        END IF;

    END LOOP;
    IF exitflag THEN
        exit;
    END IF;
END LOOP;
RAISE NOTICE 'After looping through partitions %', ftime();
RAISE NOTICE 'counter: %, coveredcells: %', counter, (select count(*) from outgrid where outgrid.covered);
RAISE NOTICE 'total_calc: %, total_update: %', total_calc, total_update;
RETURN QUERY
SELECT
    outgrid.quadkey, outgrid.ids, st_setsrid(outgrid.gridgeom, 3857) geom, outgrid.uniongeom, outgrid.covered
FROM outgrid WHERE outgrid.ids is not null;

RETURN;
END;
$$ LANGUAGE PLPGSQL SET SEARCH_PATH TO pgstac,public;
/*
DROP FUNCTION searchcovers_rast;
CREATE OR REPLACE FUNCTION searchcovers_rast(
    IN _bbox geometry,
    IN _where text DEFAULT 'TRUE',
    IN _dtrange tstzrange DEFAULT tstzrange('-infinity','infinity'),
    IN _orderby text DEFAULT 'datetime DESC, id DESC',
    IN _limit int DEFAULT 1000,
    OUT _id text,
    OUT _counter int,
    OUT _geom geometry
) RETURNS SETOF RECORD AS $$
DECLARE
partition_query text;
main_query text;
batchcount int;
counter int := 1;
p record;
clippedgeom geometry := NULL;
uniongeom geometry := NULL;
diffgeom geometry := NULL;
r record;
coveredby boolean := FALSE;
base_rast raster := st_addband(st_makeemptyraster(20, 20, -93::float, 45::float, .1::float, .1::float, 0, 0, 4326), '8BUI'::text, 1::float, 0::float);
base_extent geometry := st_makeenvelope(-93, 43, -91, 45, 4326);
row_rast raster := NULL;
total_rast raster := st_addband(st_makeemptyraster(20, 20, -93::float, 45::float, .1::float, .1::float, 0, 0, 4326), '8BUI'::text, 0::float, 0::float);
pixelcount int := 0;
st timestamptz := clock_timestamp();
rt timestamptz := clock_timestamp();
BEGIN
CREATE TEMP TABLE lookup (
    id text,
    cnt int
)  ON COMMIT DROP ;


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
        SELECT * FROM %I
        WHERE %s
        AND st_intersects($1, geometry)
        ORDER BY %s
        LIMIT %s - $2
    $q$, p.partition::text, _where, _orderby, _limit
    );
    RAISE NOTICE 'Partition Query %', main_query;
    RAISE NOTICE '%', counter;
    FOR r IN EXECUTE main_query USING _bbox, counter LOOP

            --rt := clock_timestamp();
            --row_rast := st_asraster(r.geometry, base_rast, '8BUI', counter , 0, true);
            --RAISE NOTICE 'CREATING row_rast took % --- total %', age(clock_timestamp(), rt), age(clock_timestamp(), st);
            rt := clock_timestamp();
            --IF st_intersects(base_extent, r.geometry) THEN
                IF st_geometrytype(r.geometry) IN ('ST_Polygon', 'ST_MultiPolygon') THEN
                    row_rast := st_clip(base_rast, r.geometry);
                    --RAISE NOTICE 'CREATING row_rast took % --- total %', age(clock_timestamp(), rt), age(clock_timestamp(), st);
                ELSE
                    row_rast := st_asraster(r.geometry, base_rast, '8BUI', 1 , 0, true);
                    --RAISE NOTICE 'CREATING row_rast took % --- total %', age(clock_timestamp(), rt), age(clock_timestamp(), st);
                END IF;

                rt := clock_timestamp();
                --total_rast := st_mapalgebra(total_rast, row_rast, '[rast1]', null, 'FIRST', '[rast2]', '[rast1]', 0);
                total_rast := st_mapalgebra(total_rast, row_rast, '[rast1]', null, 'FIRST', format('%s::integer', counter) , '[rast1]', 0);
                --RAISE NOTICE 'UPDATING total_rast took % --- total %', age(clock_timestamp(), rt), age(clock_timestamp(), st);
                --RAISE NOTICE 'SUMMARY STATS %', st_summarystats(total_rast);

                rt := clock_timestamp();
                pixelcount := st_count(total_rast, true);
                --RAISE NOTICE 'GETTING Pixel Count took % --- total %', age(clock_timestamp(), rt), age(clock_timestamp(), st);
                RAISE NOTICE 'cnt: %, id: %, pix_cnt: %', counter, r.id, pixelcount;
                INSERT INTO lookup (id, cnt) VALUES (r.id, counter);
            --ELSE
            --    RAISE NOTICE 'skipped % -- no intersect', r.id;
            --END IF;

            counter := counter + 1;

            IF pixelcount >= 400 OR counter >= _limit THEN
                EXIT;
            END IF;
    END LOOP;

    IF pixelcount >= 400 OR counter >= _limit THEN
        EXIT;
    END IF;
END LOOP;
rt := clock_timestamp();

RETURN QUERY
WITH dp AS (
    SELECT val, geom FROM ST_DumpAsPolygons(total_rast)
) SELECT id, cnt, geom FROM lookup JOIN dp ON (dp.val=lookup.cnt);
RAISE NOTICE 'RETURNING QUERY took % --- total %', age(clock_timestamp(), rt), age(clock_timestamp(), st);

RETURN;
END;
$$ LANGUAGE PLPGSQL SET SEARCH_PATH TO pgstac,public;

CREATE UNLOGGED TABLE search_tile_cache_lock(
    id bigint generated always as identity,
    querykey text,
    quadkey text
);

CREATE UNLOGGED TABLE search_tile_cache (
    querykey text,
    metaquadkey text,
    quadkey text,
    ids text[],
    geom geometry,
    hitcount int,
    lasthit timestamptz DEFAULT now()
);
*/
/*
1) Get id from lock if it exists using insert / on conflict update
2) Take out advisory lock on lock table id



DROP FUNCTION searchcovers_tile;
CREATE OR REPLACE FUNCTION searchcovers_tile(
    IN _zoom int,
    IN _x int,
    IN _y int,
    IN _level int,
    IN _where text DEFAULT 'TRUE',
    IN _dtrange tstzrange DEFAULT tstzrange('-infinity','infinity'),
    IN _orderby text DEFAULT 'datetime DESC, id DESC',
    IN _limit int DEFAULT 1000,
    OUT quadkey text,
    OUT ids text[],
    OUT geom geometry,
    OUT uniongeom geometry,
    OUT covered boolean
) RETURNS SETOF RECORD AS $$
DECLARE

BEGIN


END;
$$ LANGUAGE PLPGSQL;
*/
/*
DROP FUNCTION IF EXISTS search_tile;
CREATE OR REPLACE FUNCTION search_tile(
    IN _zoom int,
    IN _x int,
    IN _y int,
    IN _where text DEFAULT 'TRUE',
    IN _dtrange tstzrange DEFAULT tstzrange('-infinity','infinity'),
    IN _orderby text DEFAULT 'datetime DESC, id DESC',
    IN _limit int DEFAULT 100,
    IN _tlimit int DEFAULT 5,
    IN point_line_buffer float DEFAULT  60000 -- Buffer to use for point or line features
) RETURNS text[] AS $$
DECLARE
r record;
_bbox geometry := tileenvelope(_zoom, _x, _y);
_bbox_area float;
rec_ids text[];
rec_uniongeom geometry;
rowgeom geometry;
ts timestamptz := clock_timestamp();
curs refcursor;
BEGIN
_bbox_area := st_area(_bbox);
-- Add the bounding box for the tile to the WHERE statement
_where := format('%L AND st_intersects(geometry, st_transform(%L::geometry, 4326)) ', _where, _bbox::text);

FOR curs IN select * from partition_cursor(_where, _dtrange, _orderby) LOOP
    RAISE NOTICE 'New Partition... %', ftime();
    LOOP
        FETCH curs INTO r;
        EXIT WHEN NOT FOUND;

        IF st_geometrytype(r.geometry) IN  ('ST_Polygon', 'ST_MultiPolygon') THEN
            rowgeom := st_intersection(_bbox, st_transform(r.geometry, 3857));
        ELSE
            -- If the record geometry is a line or a point, use a buffer around the point or line to get an area
            rowgeom := st_intersection(_bbox, st_buffer(st_transform(r.geometry, 3857), point_line_buffer));
        END IF;

        IF rec_ids IS NULL THEN
            -- first record
            rec_ids := ARRAY[r.id::text];
            rec_uniongeom := rowgeom;
        ELSE
            -- if stac record geometry is not already covered by existing extent, add it
            IF NOT _st_covers(rec_uniongeom, rowgeom) THEN
                rec_uniongeom := st_union(rec_uniongeom, rowgeom);
                rec_ids := array_append(rec_ids, r.id::text);
                IF
                    (st_area(rec_uniongeom) >= _bbox_area)
                    OR
                    (clock_timestamp() - ts > concat(_tlimit::text, ' seconds')::interval)
                    OR
                    (cardinality(rec_ids) > _limit)
                THEN
                    RETURN rec_ids;
                END IF;
            END IF;
        END IF;
    END LOOP;
END LOOP;

END;
$$ LANGUAGE PLPGSQL SET SEARCH_PATH TO pgstac,public;
*/
--SELECT search_tile(1,1,1);
