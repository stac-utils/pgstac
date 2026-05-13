#!/usr/bin/env python3
"""Benchmark cached partition-stat pruning for CQL2-like PgSTAC filters.

The script runs inside the repository's pypgstac Docker image via
run_benchmark.sh. It creates an isolated database, installs PgSTAC, generates a
month-partitioned STAC fixture with CQL2-filterable properties, materializes a
benchmark-only cached partition-stat table, and compares that table against the
current EXPLAIN/constraint-pruning path.
"""

from __future__ import annotations

import argparse
import calendar
import json
import os
import re
import statistics
import subprocess
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql
from pypgstac.db import PgstacDB
from pypgstac.load import Loader, Methods

DEFAULT_CONFIG = {
    "database": "pgstac_zone_map_bench",
    "collections": 3,
    "months": 24,
    "items_per_partition": 40,
    "iterations": 7,
    "warmup_iterations": 2,
    "query_windows": [1, 3, 6],
    "spatial_window_degrees": 4.0,
    "cloud_cover_threshold": 35,
    "platform": "sentinel-2a",
    "chunk_limit": 3,
    "result_limit": 25,
    "output": "/bench/results/zone_map_partition_pruning_results.json",
}


def load_config(path: str | None) -> dict[str, Any]:
    config = DEFAULT_CONFIG.copy()
    if path:
        with open(path, encoding="utf-8") as f:
            config.update(json.load(f))
    return config


def connect(dbname: str | None = None) -> psycopg.Connection:
    kwargs: dict[str, Any] = {"autocommit": True}
    if dbname:
        kwargs["dbname"] = dbname
    return psycopg.connect(**kwargs)


def reset_database(dbname: str) -> None:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", dbname):
        raise ValueError(f"Unsafe benchmark database name: {dbname!r}")
    with connect("postgres") as conn:
        conn.execute(
            sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE);").format(
                sql.Identifier(dbname),
            ),
        )
        conn.execute(sql.SQL("CREATE DATABASE {};").format(sql.Identifier(dbname)))
        conn.execute(
            sql.SQL("ALTER DATABASE {} SET SEARCH_PATH TO pgstac, public;").format(
                sql.Identifier(dbname),
            ),
        )
        conn.execute(
            sql.SQL("ALTER DATABASE {} SET CLIENT_MIN_MESSAGES TO WARNING;").format(
                sql.Identifier(dbname),
            ),
        )
    env = os.environ.copy()
    env["PGDATABASE"] = dbname
    try:
        subprocess.run(
            [
                "psql",
                "-X",
                "-q",
                "-v",
                "ON_ERROR_STOP=1",
                "-f",
                "/opt/src/pgstac/pgstac.sql",
            ],
            check=True,
            env=env,
        )
    except subprocess.CalledProcessError as exc:
        msg = f"Failed to install PgSTAC into benchmark database {dbname!r}."
        raise RuntimeError(msg) from exc


def collection_doc(collection_id: str) -> dict[str, Any]:
    return {
        "type": "Collection",
        "id": collection_id,
        "stac_version": "1.0.0",
        "description": f"CQL2 partition-stat pruning benchmark collection {collection_id}",
        "license": "proprietary",
        "extent": {
            "spatial": {"bbox": [[-180, -90, 180, 90]]},
            "temporal": {"interval": [["2020-01-01T00:00:00Z", None]]},
        },
    }


def item_doc(collection_id: str, month: int, item_no: int, collection_no: int) -> dict[str, Any]:
    year = 2020 + (month // 12)
    month_of_year = (month % 12) + 1
    days_in_month = calendar.monthrange(year, month_of_year)[1]
    day = (item_no % days_in_month) + 1
    dt = datetime(year, month_of_year, day, tzinfo=timezone.utc)
    base_x = -120 + month * 4 + collection_no * 8
    base_y = -45 + (month % 12) * 3 + collection_no * 4
    jitter = (item_no % 10) * 0.1
    left = base_x + jitter
    bottom = base_y + jitter
    right = left + 0.5
    top = bottom + 0.5
    cloud_cover = (month * 11 + item_no * 7 + collection_no * 13) % 100
    platform = "sentinel-2a" if (month + item_no + collection_no) % 3 != 0 else "landsat-8"
    return {
        "type": "Feature",
        "stac_version": "1.0.0",
        "id": f"{collection_id}-{month:03d}-{item_no:04d}-{uuid.uuid4().hex[:8]}",
        "collection": collection_id,
        "geometry": {
            "type": "Polygon",
            "coordinates": [
                [[left, bottom], [right, bottom], [right, top], [left, top], [left, bottom]],
            ],
        },
        "bbox": [left, bottom, right, top],
        "properties": {
            "datetime": dt.isoformat().replace("+00:00", "Z"),
            "eo:cloud_cover": cloud_cover,
            "platform": platform,
        },
    }


def prepare_fixture(dbname: str, config: dict[str, Any]) -> list[str]:
    os.environ["PGDATABASE"] = dbname
    db = PgstacDB(dsn=f"dbname={dbname}")
    loader = Loader(db)
    try:
        collection_ids = [f"cql2-zone-map-bench-{i}" for i in range(int(config["collections"]))]
        loader.load_collections(
            (collection_doc(cid) for cid in collection_ids),
            insert_mode=Methods.insert,
        )
        with connect(dbname) as conn:
            conn.execute("UPDATE pgstac.collections SET partition_trunc = 'month';")
        for collection_no, collection_id in enumerate(collection_ids):
            items = (
                item_doc(collection_id, month, item_no, collection_no)
                for month in range(int(config["months"]))
                for item_no in range(int(config["items_per_partition"]))
            )
            loader.load_items(items, insert_mode=Methods.insert)
    finally:
        db.close()
    with connect(dbname) as conn:
        conn.execute(
            "SELECT update_partition_stats(partition, true) FROM pgstac.partition_sys_meta;",
        )
        create_cached_partition_stats(conn)
    return collection_ids


def create_cached_partition_stats(conn: psycopg.Connection) -> None:
    conn.execute("DROP TABLE IF EXISTS bench_partition_cql2_stats;")
    conn.execute(
        """
        -- Benchmark-only cache table. UNLOGGED reduces benchmark I/O noise; the
        -- table is rebuilt for each run and is not intended to survive crash
        -- recovery.
        CREATE UNLOGGED TABLE bench_partition_cql2_stats (
            partition text PRIMARY KEY,
            collection text NOT NULL,
            dtrange tstzrange,
            edtrange tstzrange,
            spatial geometry,
            cloud_cover_range numrange,
            platforms text[],
            row_count bigint NOT NULL
        );
        """
    )
    rows = conn.execute(
        """
        SELECT
            p.partition,
            p.collection,
            COALESCE(s.dtrange, p.constraint_dtrange) AS dtrange,
            COALESCE(s.edtrange, p.constraint_edtrange) AS edtrange,
            s.spatial
        FROM pgstac.partition_sys_meta p
        LEFT JOIN pgstac.partition_stats s USING (partition)
        ORDER BY p.partition;
        """
    ).fetchall()
    for partition, collection, dtrange, edtrange, spatial in rows:
        conn.execute(
            sql.SQL(
                """
                INSERT INTO bench_partition_cql2_stats (
                    partition, collection, dtrange, edtrange, spatial,
                    cloud_cover_range, platforms, row_count
                )
                SELECT %s, %s, %s, %s, %s,
                       CASE
                           -- Empty means a partition has no values for this
                           -- queryable, so a cloud-cover predicate should not
                           -- keep it as a candidate.
                           WHEN count(*) FILTER (
                               WHERE content->'properties' ? 'eo:cloud_cover'
                           ) = 0
                           THEN 'empty'::numrange
                           ELSE numrange(
                               min((content->'properties'->>'eo:cloud_cover')::numeric)
                                   FILTER (
                                       WHERE content->'properties' ? 'eo:cloud_cover'
                                   ),
                               max((content->'properties'->>'eo:cloud_cover')::numeric)
                                   FILTER (
                                       WHERE content->'properties' ? 'eo:cloud_cover'
                                   ),
                               '[]'
                           )
                       END,
                       array_agg(DISTINCT content->'properties'->>'platform')
                           FILTER (WHERE content->'properties' ? 'platform'),
                       count(*)
                FROM {};
                """
            ).format(sql.Identifier("pgstac", partition)),
            (partition, collection, dtrange, edtrange, spatial),
        )
    conn.execute(
        """
        CREATE INDEX bench_partition_cql2_stats_dtrange_gist
        ON bench_partition_cql2_stats USING GIST (dtrange);
        """,
    )
    conn.execute(
        """
        CREATE INDEX bench_partition_cql2_stats_spatial_gist
        ON bench_partition_cql2_stats USING GIST (spatial);
        """,
    )
    conn.execute(
        """
        CREATE INDEX bench_partition_cql2_stats_cloud_gist
        ON bench_partition_cql2_stats USING GIST (cloud_cover_range);
        """,
    )
    conn.execute(
        """
        CREATE INDEX bench_partition_cql2_stats_platforms_gin
        ON bench_partition_cql2_stats USING GIN (platforms);
        """,
    )
    conn.execute("ANALYZE bench_partition_cql2_stats;")


def month_range(start_month: int, width: int) -> str:
    start_year = 2020 + (start_month // 12)
    start_month_of_year = (start_month % 12) + 1
    end_month = start_month + width
    end_year = 2020 + (end_month // 12)
    end_month_of_year = (end_month % 12) + 1
    return (
        f"[{start_year}-{start_month_of_year:02d}-01 UTC,"
        f"{end_year}-{end_month_of_year:02d}-01 UTC)"
    )


def spatial_wkt(month: int, window: float) -> str:
    center_x = -120 + month * 4 + 0.25
    center_y = -45 + (month % 12) * 3 + 0.25
    half = window / 2
    return (
        "POLYGON(("
        f"{center_x - half} {center_y - half},"
        f"{center_x + half} {center_y - half},"
        f"{center_x + half} {center_y + half},"
        f"{center_x - half} {center_y + half},"
        f"{center_x - half} {center_y - half}"
        "))"
    )


def candidate_predicate() -> str:
    # Keep the bbox operator before ST_Intersects to model a cheap stats-table
    # prefilter followed by the exact cached-extent check.
    return """
        collection = ANY(%(collections)s)
        AND dtrange && %(dtrange)s::tstzrange
        AND edtrange && %(dtrange)s::tstzrange
        AND spatial && ST_GeomFromText(%(geom_wkt)s, 4326)
        AND ST_Intersects(spatial, ST_GeomFromText(%(geom_wkt)s, 4326))
        AND cloud_cover_range && numrange(NULL, %(cloud_cover_threshold)s::numeric, '[]')
        AND platforms @> ARRAY[%(platform)s]::text[]
    """


def variants() -> dict[str, str]:
    pred = candidate_predicate()
    return {
        "baseline_explain_constraint_pruning": "SELECT count(*) FROM pgstac.chunker(%(where)s)",
        "cached_cql2_stats_candidates": f"""
            SELECT count(*)
            FROM bench_partition_cql2_stats
            WHERE {pred}
        """,
        "cached_cql2_stats_candidate_rows": f"""
            SELECT COALESCE(sum(row_count), 0)
            FROM bench_partition_cql2_stats
            WHERE {pred}
        """,
        "cached_cql2_stats_datetime_desc_chunks": f"""
            SELECT count(*)
            FROM (
                SELECT partition
                FROM bench_partition_cql2_stats
                WHERE {pred}
                ORDER BY upper(dtrange) DESC NULLS LAST, partition DESC
                LIMIT %(chunk_limit)s
            ) chunks
        """,
        "cached_cql2_stats_datetime_asc_chunks": f"""
            SELECT count(*)
            FROM (
                SELECT partition
                FROM bench_partition_cql2_stats
                WHERE {pred}
                ORDER BY lower(dtrange) ASC NULLS LAST, partition ASC
                LIMIT %(chunk_limit)s
            ) chunks
        """,
        "cached_partition_set_for_cql2": f"""
            WITH key AS (
                SELECT md5(jsonb_build_object(
                    'cql2', %(cql2_filter)s::jsonb,
                    'dtrange', %(dtrange)s,
                    'geom_wkt', %(geom_wkt)s,
                    'collections', to_jsonb((
                        SELECT array_agg(collection ORDER BY collection)
                        FROM unnest(%(collections)s::text[]) AS collection
                    )),
                    'order', %(orderby)s
                )::text) AS cache_key
            ), cached AS (
                SELECT partitions FROM pg_temp.partition_prune_cache c JOIN key USING (cache_key)
            ), inserted AS (
                INSERT INTO pg_temp.partition_prune_cache(cache_key, partitions)
                SELECT key.cache_key,
                       array_agg(
                           partition
                           ORDER BY upper(dtrange) DESC NULLS LAST, partition DESC
                       )
                FROM key, bench_partition_cql2_stats
                WHERE NOT EXISTS (SELECT 1 FROM cached)
                  AND {pred}
                GROUP BY key.cache_key
                ON CONFLICT (cache_key) DO NOTHING
                RETURNING partitions
            )
            SELECT COALESCE(cardinality((SELECT partitions FROM cached LIMIT 1)),
                            cardinality((SELECT partitions FROM inserted LIMIT 1)), 0)
        """,
    }


def time_sql(
    conn: psycopg.Connection,
    sql_text: str,
    params: dict[str, Any],
    warmups: int,
    iterations: int,
) -> dict[str, Any]:
    timings: list[float] = []
    last_value: Any = None
    for i in range(warmups + iterations):
        start = time.perf_counter()
        with conn.cursor() as cur:
            cur.execute(sql_text, params)
            row = cur.fetchone()
        elapsed_ms = (time.perf_counter() - start) * 1000
        last_value = row[0] if row else None
        if i >= warmups:
            timings.append(elapsed_ms)
    return {
        "value": str(last_value),
        "min_ms": min(timings),
        "median_ms": statistics.median(timings),
        "mean_ms": statistics.mean(timings),
        "max_ms": max(timings),
        "runs_ms": timings,
    }


def cql2_filter(config: dict[str, Any]) -> dict[str, Any]:
    return {
        "op": "and",
        "args": [
            {
                "op": "<=",
                "args": [
                    {"property": "eo:cloud_cover"},
                    int(config["cloud_cover_threshold"]),
                ],
            },
            {"op": "=", "args": [{"property": "platform"}, str(config["platform"])]},
        ],
    }


def run_benchmarks(
    dbname: str,
    collection_ids: list[str],
    config: dict[str, Any],
) -> dict[str, Any]:
    results: dict[str, Any] = {"config": config, "scenarios": []}
    warmups = int(config["warmup_iterations"])
    iterations = int(config["iterations"])
    with connect(dbname) as conn:
        conn.execute(
            """
            -- Benchmark-scope cache. It is truncated before each scenario so
            -- cold and warm candidate-set timings are scenario-local.
            CREATE TEMP TABLE partition_prune_cache(
                cache_key text PRIMARY KEY,
                partitions text[]
            ) ON COMMIT PRESERVE ROWS;
            """,
        )
        for width in config["query_windows"]:
            start_month = max(0, int(config["months"]) // 2 - int(width) // 2)
            dtrange = month_range(start_month, int(width))
            geom_wkt = spatial_wkt(start_month, float(config["spatial_window_degrees"]))
            cloud_cover_threshold = int(config["cloud_cover_threshold"])
            platform = str(config["platform"])
            cql2 = cql2_filter(config)
            # Benchmark-only baseline SQL: this intentionally mirrors the
            # current string-based EXPLAIN/chunker path. Production code should
            # keep using parameterized SQL for user-provided values.
            where = (
                f"collection = ANY ('{{{','.join(collection_ids)}}}'::text[]) "
                f"AND datetime < upper('{dtrange}'::tstzrange) "
                f"AND end_datetime >= lower('{dtrange}'::tstzrange) "
                f"AND st_intersects(geometry, ST_GeomFromText('{geom_wkt}', 4326)) "
                f"AND (content->'properties'->>'eo:cloud_cover')::numeric "
                f"<= {cloud_cover_threshold} "
                f"AND content->'properties'->>'platform' = '{platform}'"
            )
            params = {
                "where": where,
                "collections": collection_ids,
                "dtrange": dtrange,
                "geom_wkt": geom_wkt,
                "cloud_cover_threshold": cloud_cover_threshold,
                "platform": platform,
                "chunk_limit": int(config["chunk_limit"]),
                "orderby": "datetime DESC, id DESC",
                "cql2_filter": json.dumps(cql2, sort_keys=True),
            }
            scenario = {
                "window_months": width,
                "dtrange": dtrange,
                "geom_wkt": geom_wkt,
                "cql2_filter": cql2,
                "variants": {},
            }
            conn.execute("TRUNCATE pg_temp.partition_prune_cache;")
            for name, sql_text in variants().items():
                scenario["variants"][name] = time_sql(conn, sql_text, params, warmups, iterations)
            results["scenarios"].append(scenario)
    return results


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", help="Path to benchmark_config.json")
    args = parser.parse_args()
    config = load_config(args.config)
    dbname = str(config["database"])
    reset_database(dbname)
    collections = prepare_fixture(dbname, config)
    results = run_benchmarks(dbname, collections, config)
    output = Path(str(config["output"]))
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(results, indent=2, sort_keys=True), encoding="utf-8")
    print(f"Wrote benchmark results to {output}")


if __name__ == "__main__":
    main()
