#!/usr/bin/env python3
"""Compare two benchmark JSON reports and emit markdown/CSV deltas."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any

TRACKED_METRICS = (
    ("ingest", "ms_per_item"),
    ("hydrate", "ms_per_item"),
    ("storage", "bytes_per_item"),
)


def load_report(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as src:
        return json.load(src)


def index_by_collection(report: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {entry["collection_id"]: entry for entry in report.get("collections", [])}


def compare(base: dict[str, Any], head: dict[str, Any]) -> list[dict[str, Any]]:
    base_by_collection = index_by_collection(base)
    head_by_collection = index_by_collection(head)

    rows: list[dict[str, Any]] = []
    for collection_id in sorted(set(base_by_collection) & set(head_by_collection)):
        base_entry = base_by_collection[collection_id]
        head_entry = head_by_collection[collection_id]
        for section, metric in TRACKED_METRICS:
            base_raw = base_entry[section][metric]
            head_raw = head_entry[section][metric]
            if base_raw is None or head_raw is None:
                continue
            base_value = float(base_raw)
            head_value = float(head_raw)
            delta = head_value - base_value
            delta_pct = (delta / base_value * 100.0) if base_value else None
            rows.append(
                {
                    "collection": collection_id,
                    "metric": f"{section}.{metric}",
                    "base": base_value,
                    "head": head_value,
                    "delta": delta,
                    "delta_pct": delta_pct,
                },
            )

    rows.append(
        {
            "collection": "_all",
            "metric": "global.item_fragments_total_bytes",
            "base": float(base["global_storage"].get("item_fragments_total_bytes", 0)),
            "head": float(head["global_storage"].get("item_fragments_total_bytes", 0)),
            "delta": float(head["global_storage"].get("item_fragments_total_bytes", 0))
            - float(base["global_storage"].get("item_fragments_total_bytes", 0)),
            "delta_pct": None,
        },
    )
    return rows


def write_csv(rows: list[dict[str, Any]], path: Path) -> None:
    fieldnames = ["collection", "metric", "base", "head", "delta", "delta_pct"]
    with path.open("w", newline="", encoding="utf-8") as dst:
        writer = csv.DictWriter(dst, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_markdown(
    rows: list[dict[str, Any]],
    base_label: str,
    head_label: str,
    output_path: Path,
) -> None:
    lines = [
        f"# Benchmark comparison: {base_label} → {head_label}",
        "",
        "| Collection | Metric | Base | Head | Delta | Delta % |",
        "|---|---|---:|---:|---:|---:|",
    ]

    for row in rows:
        delta_pct = "n/a" if row["delta_pct"] is None else f"{row['delta_pct']:.2f}%"
        lines.append(
            f"| {row['collection']} | {row['metric']} | {row['base']:.6f} | "
            f"{row['head']:.6f} | {row['delta']:.6f} | {delta_pct} |",
        )

    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", type=Path, required=True)
    parser.add_argument("--head", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)

    base_report = load_report(args.base)
    head_report = load_report(args.head)
    rows = compare(base_report, head_report)

    csv_path = args.output_dir / "comparison.csv"
    json_path = args.output_dir / "comparison.json"
    md_path = args.output_dir / "comparison.md"

    write_csv(rows, csv_path)
    json_path.write_text(json.dumps(rows, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    write_markdown(rows, base_report["label"], head_report["label"], md_path)
    print(json.dumps(rows, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
