#!/usr/bin/env python3
"""Run PgSTAC ingest/hydrate/storage benchmarks from fixture files."""

from __future__ import annotations

import argparse
import csv
import json
import os
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import psycopg


@dataclass
class FixtureCollection:
    collection_id: str
    collection_path: Path
    items_path: Path


def run(cmd: list[str], cwd: Path) -> None:
    subprocess.run(cmd, cwd=str(cwd), check=True)


def get_dsn() -> str:
    dsn = os.getenv("PG_DSN")
    if dsn:
        return dsn
    if any(os.getenv(name) for name in ("PGHOST", "PGDATABASE", "PGUSER", "PGPORT")):
        return ""
    raise RuntimeError(
        "Database connection is not configured. Set PG_DSN or PGHOST/PGDATABASE/PGUSER/PGPORT.",
    )


def reset_schema(repo_root: Path, pypgstac_dir: Path) -> None:
    dsn = get_dsn()
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute("DROP SCHEMA IF EXISTS pgstac CASCADE;")
    if not pypgstac_dir.exists():
        raise RuntimeError(f"pypgstac directory does not exist: {pypgstac_dir}")
    run(["uv", "run", "--directory", str(pypgstac_dir), "pypgstac", "migrate"], cwd=repo_root)


def discover_fixture_collections(fixtures_dir: Path) -> list[FixtureCollection]:
    collections: list[FixtureCollection] = []
    for collection_dir in sorted(fixtures_dir.iterdir()):
        if not collection_dir.is_dir():
            continue
        collection_path = collection_dir / "collection.json"
        items_path = collection_dir / "items.ndjson"
        if not collection_path.exists() or not items_path.exists():
            continue
        collections.append(
            FixtureCollection(
                collection_id=collection_dir.name,
                collection_path=collection_path,
                items_path=items_path,
            ),
        )
    if not collections:
        raise RuntimeError(f"No fixture collections found in {fixtures_dir}")
    return collections


def insert_collection(cur: psycopg.Cursor[Any], collection_path: Path) -> None:
    content = collection_path.read_text(encoding="utf-8")
    cur.execute("INSERT INTO collections (content) VALUES (%s::jsonb);", (content,))


def ingest_collection(cur: psycopg.Cursor[Any], fixture: FixtureCollection) -> tuple[int, float]:
    started = time.perf_counter()
    rows = 0
    with cur.copy("COPY items_staging (content) FROM stdin") as copy:
        with fixture.items_path.open("r", encoding="utf-8") as src:
            for line in src:
                line = line.strip()
                if not line:
                    continue
                copy.write_row((line,))
                rows += 1
    duration_ms = (time.perf_counter() - started) * 1000.0
    return rows, duration_ms


def hydrate_collection(cur: psycopg.Cursor[Any], collection_id: str, iterations: int) -> tuple[int, float]:
    total_ms = 0.0
    rows = 0
    for _ in range(iterations):
        started = time.perf_counter()
        cur.execute(
            """
            SELECT count(*)
            FROM (
                SELECT content_hydrate(i)
                FROM items i
                WHERE i.collection = %s
            ) hydrated
            """,
            (collection_id,),
        )
        rows = int(cur.fetchone()[0])
        total_ms += (time.perf_counter() - started) * 1000.0
    return rows, total_ms / iterations


def get_partition_key(cur: psycopg.Cursor[Any], collection_id: str) -> str:
    cur.execute("SELECT key FROM collections WHERE id = %s", (collection_id,))
    row = cur.fetchone()
    if not row or not row[0]:
        raise RuntimeError(f"Unable to find partition key for {collection_id}")
    return str(row[0])


def get_collection_storage(cur: psycopg.Cursor[Any], partition_key: str) -> dict[str, int]:
    cur.execute(
        """
        SELECT
            COALESCE(SUM(pg_relation_size(c.oid)), 0) AS table_bytes,
            COALESCE(SUM(pg_indexes_size(c.oid)), 0) AS index_bytes,
            COALESCE(SUM(pg_total_relation_size(c.oid)), 0) AS total_bytes
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'pgstac'
          AND c.relkind = 'r'
          AND c.relname LIKE %s
        """,
        (f"_items_{partition_key}%",),
    )
    row = cur.fetchone()
    return {
        "table_bytes": int(row[0]),
        "index_bytes": int(row[1]),
        "total_bytes": int(row[2]),
    }


def get_global_storage(cur: psycopg.Cursor[Any]) -> dict[str, int]:
    cur.execute(
        """
        SELECT COALESCE(SUM(pg_total_relation_size(c.oid)), 0)
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'pgstac'
          AND c.relkind IN ('r', 'm')
        """,
    )
    schema_total = int(cur.fetchone()[0])

    cur.execute("SELECT to_regclass('pgstac.item_fragments')")
    has_fragments = cur.fetchone()[0] is not None
    fragment_bytes = 0
    if has_fragments:
        cur.execute("SELECT pg_total_relation_size('pgstac.item_fragments'::regclass)")
        fragment_bytes = int(cur.fetchone()[0])

    return {
        "schema_total_bytes": schema_total,
        "item_fragments_total_bytes": fragment_bytes,
    }


def per_item(value: float, rows: int) -> float | None:
    if rows <= 0:
        return None
    return value / rows


def flatten_metrics(report: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    label = report["label"]

    global_storage = report["global_storage"]
    rows.append(
        {
            "label": label,
            "collection": "_all",
            "suite": "storage",
            "metric": "schema_total_bytes",
            "value": global_storage["schema_total_bytes"],
            "unit": "bytes",
        },
    )
    rows.append(
        {
            "label": label,
            "collection": "_all",
            "suite": "storage",
            "metric": "item_fragments_total_bytes",
            "value": global_storage["item_fragments_total_bytes"],
            "unit": "bytes",
        },
    )

    for result in report["collections"]:
        collection_id = result["collection_id"]
        rows.extend(
            [
                {
                    "label": label,
                    "collection": collection_id,
                    "suite": "ingest",
                    "metric": "rows",
                    "value": result["ingest"]["rows"],
                    "unit": "count",
                },
                {
                    "label": label,
                    "collection": collection_id,
                    "suite": "ingest",
                    "metric": "ingest_total_ms",
                    "value": result["ingest"]["duration_ms"],
                    "unit": "ms",
                },
                {
                    "label": label,
                    "collection": collection_id,
                    "suite": "ingest",
                    "metric": "ingest_ms_per_item",
                    "value": result["ingest"]["ms_per_item"],
                    "unit": "ms_per_item",
                },
                {
                    "label": label,
                    "collection": collection_id,
                    "suite": "hydrate",
                    "metric": "hydrate_rows",
                    "value": result["hydrate"]["rows"],
                    "unit": "count",
                },
                {
                    "label": label,
                    "collection": collection_id,
                    "suite": "hydrate",
                    "metric": "hydrate_avg_ms",
                    "value": result["hydrate"]["avg_ms"],
                    "unit": "ms",
                },
                {
                    "label": label,
                    "collection": collection_id,
                    "suite": "hydrate",
                    "metric": "hydrate_ms_per_item",
                    "value": result["hydrate"]["ms_per_item"],
                    "unit": "ms_per_item",
                },
                {
                    "label": label,
                    "collection": collection_id,
                    "suite": "storage",
                    "metric": "collection_table_bytes",
                    "value": result["storage"]["table_bytes"],
                    "unit": "bytes",
                },
                {
                    "label": label,
                    "collection": collection_id,
                    "suite": "storage",
                    "metric": "collection_index_bytes",
                    "value": result["storage"]["index_bytes"],
                    "unit": "bytes",
                },
                {
                    "label": label,
                    "collection": collection_id,
                    "suite": "storage",
                    "metric": "collection_total_bytes",
                    "value": result["storage"]["total_bytes"],
                    "unit": "bytes",
                },
                {
                    "label": label,
                    "collection": collection_id,
                    "suite": "storage",
                    "metric": "collection_bytes_per_item",
                    "value": result["storage"]["bytes_per_item"],
                    "unit": "bytes_per_item",
                },
            ],
        )
    return rows


def write_csv(rows: list[dict[str, Any]], path: Path) -> None:
    fieldnames = ["label", "collection", "suite", "metric", "value", "unit"]
    with path.open("w", newline="", encoding="utf-8") as dst:
        writer = csv.DictWriter(dst, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_markdown(report: dict[str, Any], path: Path) -> None:
    lines = [
        f"# Benchmark results ({report['label']})",
        "",
        f"- Fixture directory: `{report['fixtures_dir']}`",
        f"- Repo root: `{report['repo_root']}`",
        f"- Hydrate iterations: `{report['hydrate_iterations']}`",
        "",
        "## Collection summary",
        "",
        "| Collection | Rows | Ingest ms/item | Hydrate ms/item | Storage bytes/item | Total bytes |",
        "|---|---:|---:|---:|---:|---:|",
    ]

    for result in report["collections"]:
        ingest_ms_per_item = result["ingest"]["ms_per_item"]
        hydrate_ms_per_item = result["hydrate"]["ms_per_item"]
        storage_bytes_per_item = result["storage"]["bytes_per_item"]
        lines.append(
            "| {collection_id} | {rows} | {ingest} | {hydrate} | {bytes_per_item} | {total_bytes} |".format(
                collection_id=result["collection_id"],
                rows=result["ingest"]["rows"],
                ingest=f"{ingest_ms_per_item:.3f}" if ingest_ms_per_item is not None else "n/a",
                hydrate=f"{hydrate_ms_per_item:.6f}"
                if hydrate_ms_per_item is not None
                else "n/a",
                bytes_per_item=f"{storage_bytes_per_item:.2f}"
                if storage_bytes_per_item is not None
                else "n/a",
                total_bytes=result["storage"]["total_bytes"],
            ),
        )

    lines.extend(
        [
            "",
            "## Global storage",
            "",
            f"- `schema_total_bytes`: {report['global_storage']['schema_total_bytes']}",
            f"- `item_fragments_total_bytes`: {report['global_storage']['item_fragments_total_bytes']}",
            "",
        ],
    )

    path.write_text("\n".join(lines), encoding="utf-8")


def run_benchmark(
    fixtures_dir: Path,
    repo_root: Path,
    pypgstac_dir: Path,
    label: str,
    hydrate_iterations: int,
) -> dict[str, Any]:
    reset_schema(repo_root, pypgstac_dir)

    collections = discover_fixture_collections(fixtures_dir)

    report: dict[str, Any] = {
        "label": label,
        "repo_root": str(repo_root),
        "fixtures_dir": str(fixtures_dir),
        "hydrate_iterations": hydrate_iterations,
        "collections": [],
    }

    dsn = get_dsn()
    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("SET search_path TO pgstac, public;")

            for fixture in collections:
                insert_collection(cur, fixture.collection_path)

            for fixture in collections:
                rows, ingest_duration_ms = ingest_collection(cur, fixture)
                hydrate_rows, hydrate_avg_ms = hydrate_collection(
                    cur,
                    fixture.collection_id,
                    hydrate_iterations,
                )
                partition_key = get_partition_key(cur, fixture.collection_id)
                storage = get_collection_storage(cur, partition_key)

                report["collections"].append(
                    {
                        "collection_id": fixture.collection_id,
                        "ingest": {
                            "rows": rows,
                            "duration_ms": ingest_duration_ms,
                            "ms_per_item": per_item(ingest_duration_ms, rows),
                        },
                        "hydrate": {
                            "rows": hydrate_rows,
                            "avg_ms": hydrate_avg_ms,
                            "ms_per_item": per_item(hydrate_avg_ms, hydrate_rows),
                        },
                        "storage": {
                            **storage,
                            "bytes_per_item": per_item(float(storage["total_bytes"]), rows),
                        },
                    },
                )

            report["global_storage"] = get_global_storage(cur)

    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--fixtures-dir",
        type=Path,
        required=True,
        help="Directory containing per-collection fixture folders.",
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        required=True,
        help="Repository root whose migration stack should be benchmarked.",
    )
    parser.add_argument(
        "--pypgstac-dir",
        type=Path,
        default=Path("src/pypgstac"),
        help="Path to the pypgstac project (relative to --repo-root if not absolute).",
    )
    parser.add_argument("--label", type=str, required=True)
    parser.add_argument("--hydrate-iterations", type=int, default=5)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)

    pypgstac_dir = args.pypgstac_dir
    if not pypgstac_dir.is_absolute():
        pypgstac_dir = args.repo_root / pypgstac_dir

    report = run_benchmark(
        fixtures_dir=args.fixtures_dir,
        repo_root=args.repo_root,
        pypgstac_dir=pypgstac_dir,
        label=args.label,
        hydrate_iterations=args.hydrate_iterations,
    )

    json_path = args.output_dir / f"{args.label}.json"
    csv_path = args.output_dir / f"{args.label}.csv"
    md_path = args.output_dir / f"{args.label}.md"

    rows = flatten_metrics(report)

    json_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    write_csv(rows, csv_path)
    write_markdown(report, md_path)

    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
