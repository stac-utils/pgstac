#!/usr/bin/env python3
"""Benchmark candidate zone-map partition pruning strategies for PgSTAC.

The script is designed to run inside the repository's pypgstac Docker image via
run_benchmark.sh. It creates an isolated database, installs PgSTAC, generates a
small month-partitioned STAC fixture, and times pruning-only SQL variants.
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
        conn.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE);").format(sql.Identifier(dbname)))
        conn.execute(sql.SQL("CREATE DATABASE {};").format(sql.Identifier(dbname)))
        conn.execute(sql.SQL("ALTER DATABASE {} SET SEARCH_PATH TO pgstac, public;").format(sql.Identifier(dbname)))
        conn.execute(sql.SQL("ALTER DATABASE {} SET CLIENT_MIN_MESSAGES TO WARNING;").format(sql.Identifier(dbname)))
    env = os.environ.copy()
    env["PGDATABASE"] = dbname
    subprocess.run(
        ["psql", "-X", "-q", "-v", "ON_ERROR_STOP=1", "-f", "/opt/src/pgstac/pgstac.sql"],
        check=True,
        env=env,
    )


def collection_doc(collection_id: str) -> dict[str, Any]:
    return {
        "type": "Collection",
        "id": collection_id,
        "stac_version": "1.0.0",
        "description": f"Zone-map pruning benchmark collection {collection_id}",
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
    return {
        "type": "Feature",
        "stac_version": "1.0.0",
        "id": f"{collection_id}-{month:03d}-{item_no:04d}-{uuid.uuid4().hex[:8]}",
        "collection": collection_id,
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[left, bottom], [right, bottom], [right, top], [left, top], [left, bottom]]],
        },
        "bbox": [left, bottom, right, top],
        "properties": {"datetime": dt.isoformat().replace("+00:00", "Z")},
    }


def prepare_fixture(dbname: str, config: dict[str, Any]) -> list[str]:
    os.environ["PGDATABASE"] = dbname
    db = PgstacDB(dsn=f"dbname={dbname}")
    loader = Loader(db)
    try:
        collection_ids = [f"zone-map-bench-{i}" for i in range(int(config["collections"]))]
        loader.load_collections((collection_doc(cid) for cid in collection_ids), insert_mode=Methods.insert)
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
        conn.execute("SELECT update_partition_stats(partition, false) FROM pgstac.partition_sys_meta;")
        conn.execute("CREATE INDEX IF NOT EXISTS partition_stats_spatial_gist ON pgstac.partition_stats USING GIST (spatial);")
        conn.execute("CREATE INDEX IF NOT EXISTS partition_stats_dtrange_gist ON pgstac.partition_stats USING GIST (dtrange);")
        conn.execute("ANALYZE pgstac.partition_stats;")
    return collection_ids


def month_range(start_month: int, width: int) -> str:
    start_year = 2020 + (start_month // 12)
    start_month_of_year = (start_month % 12) + 1
    end_month = start_month + width
    end_year = 2020 + (end_month // 12)
    end_month_of_year = (end_month % 12) + 1
    return f"[{start_year}-{start_month_of_year:02d}-01 UTC,{end_year}-{end_month_of_year:02d}-01 UTC)"


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


def variants() -> dict[str, str]:
    # These benchmark variants need accurate zone-map metadata. `partitions_view`
    # is live and joins catalog metadata to partition_stats; avoid the stale
    # `partitions` materialized view here.
    return {
        "baseline_explain_chunker": "SELECT count(*) FROM pgstac.chunker(%(where)s)",
        "direct_constraint_temporal": """
            SELECT count(*)
            FROM pgstac.partition_sys_meta
            WHERE collection = ANY(%(collections)s)
              AND constraint_dtrange && %(dtrange)s::tstzrange
        """,
        "partition_stats_temporal": """
            SELECT count(*)
            FROM pgstac.partitions_view
            WHERE collection = ANY(%(collections)s)
              AND COALESCE(dtrange, constraint_dtrange) && %(dtrange)s::tstzrange
        """,
        "partition_stats_temporal_spatial": """
            SELECT count(*)
            FROM pgstac.partitions_view
            WHERE collection = ANY(%(collections)s)
              AND COALESCE(dtrange, constraint_dtrange) && %(dtrange)s::tstzrange
              AND spatial && ST_GeomFromText(%(geom_wkt)s, 4326)
              AND ST_Intersects(spatial, ST_GeomFromText(%(geom_wkt)s, 4326))
        """,
        "cached_partition_set": """
            WITH key AS (
                SELECT md5(jsonb_build_object(
                    'where', %(where)s,
                    'geom_wkt', %(geom_wkt)s,
                    'dtrange', %(dtrange)s,
                    'collections', to_jsonb((
                        SELECT array_agg(collection ORDER BY collection)
                        FROM unnest(%(collections)s::text[]) AS collection
                    ))
                )::text) AS cache_key
            ), cached AS (
                SELECT partitions FROM pg_temp.partition_prune_cache c JOIN key USING (cache_key)
            ), inserted AS (
                INSERT INTO pg_temp.partition_prune_cache(cache_key, partitions)
                SELECT key.cache_key, array_agg(partition ORDER BY partition)
                FROM key, pgstac.partitions_view
                WHERE NOT EXISTS (SELECT 1 FROM cached)
                  AND collection = ANY(%(collections)s)
                  AND COALESCE(dtrange, constraint_dtrange) && %(dtrange)s::tstzrange
                  AND spatial && ST_GeomFromText(%(geom_wkt)s, 4326)
                GROUP BY key.cache_key
                ON CONFLICT (cache_key) DO NOTHING
                RETURNING partitions
            )
            SELECT COALESCE(cardinality((SELECT partitions FROM cached LIMIT 1)),
                            cardinality((SELECT partitions FROM inserted LIMIT 1)), 0)
        """,
    }


def time_sql(conn: psycopg.Connection, sql: str, params: dict[str, Any], warmups: int, iterations: int) -> dict[str, Any]:
    timings: list[float] = []
    last_value: Any = None
    for i in range(warmups + iterations):
        start = time.perf_counter()
        with conn.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
        elapsed_ms = (time.perf_counter() - start) * 1000
        last_value = row[0] if row else None
        if i >= warmups:
            timings.append(elapsed_ms)
    return {
        "count": last_value,
        "min_ms": min(timings),
        "median_ms": statistics.median(timings),
        "mean_ms": statistics.mean(timings),
        "max_ms": max(timings),
        "runs_ms": timings,
    }


def run_benchmarks(dbname: str, collection_ids: list[str], config: dict[str, Any]) -> dict[str, Any]:
    results: dict[str, Any] = {"config": config, "scenarios": []}
    warmups = int(config["warmup_iterations"])
    iterations = int(config["iterations"])
    with connect(dbname) as conn:
        conn.execute("CREATE TEMP TABLE partition_prune_cache(cache_key text PRIMARY KEY, partitions text[]) ON COMMIT PRESERVE ROWS;")
        for width in config["query_windows"]:
            start_month = max(0, int(config["months"]) // 2 - int(width) // 2)
            dtrange = month_range(start_month, int(width))
            geom_wkt = spatial_wkt(start_month, float(config["spatial_window_degrees"]))
            where = (
                f"collection = ANY ('{{{','.join(collection_ids)}}}'::text[]) "
                f"AND datetime < upper('{dtrange}'::tstzrange) "
                f"AND end_datetime >= lower('{dtrange}'::tstzrange) "
                f"AND st_intersects(geometry, ST_GeomFromText('{geom_wkt}', 4326))"
            )
            params = {
                "where": where,
                "collections": collection_ids,
                "dtrange": dtrange,
                "geom_wkt": geom_wkt,
            }
            scenario = {"window_months": width, "dtrange": dtrange, "geom_wkt": geom_wkt, "variants": {}}
            conn.execute("TRUNCATE pg_temp.partition_prune_cache;")
            for name, sql in variants().items():
                scenario["variants"][name] = time_sql(conn, sql, params, warmups, iterations)
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
