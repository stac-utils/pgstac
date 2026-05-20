#!/usr/bin/env python3
"""Fetch deterministic Planetary Computer fixture snapshots for benchmarking."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin
from urllib.request import Request, urlopen


def request_json(url: str, method: str = "GET", body: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = None
    headers = {"Accept": "application/json"}
    if body is not None:
        payload = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"

    req = Request(url=url, data=payload, method=method, headers=headers)
    with urlopen(req, timeout=120) as response:
        return json.load(response)


def hash_file(path: Path) -> str:
    sha = hashlib.sha256()
    with path.open("rb") as src:
        for chunk in iter(lambda: src.read(65536), b""):
            sha.update(chunk)
    return sha.hexdigest()


def find_next_link(page: dict[str, Any]) -> dict[str, Any] | None:
    for link in page.get("links", []):
        if link.get("rel") == "next":
            return link
    return None


def normalize_item(item: dict[str, Any], collection_id: str) -> dict[str, Any]:
    if item.get("collection") != collection_id:
        raise ValueError(
            f"Item {item.get('id')} does not belong to {collection_id}: {item.get('collection')}",
        )
    return item


def fetch_collection(
    api_url: str,
    collection_cfg: dict[str, Any],
    item_count: int,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    collection_id = collection_cfg["id"]
    collection_url = urljoin(f"{api_url.rstrip('/')}/", f"collections/{collection_id}")
    collection_doc = request_json(collection_url)

    search_url = urljoin(f"{api_url.rstrip('/')}/", "search")
    base_body: dict[str, Any] = {
        "collections": [collection_id],
        "limit": min(250, item_count),
        "sortby": collection_cfg.get(
            "sortby",
            [{"field": "id", "direction": "asc"}],
        ),
    }
    if "datetime" in collection_cfg:
        base_body["datetime"] = collection_cfg["datetime"]

    page = request_json(search_url, method="POST", body=base_body)
    features: list[dict[str, Any]] = []

    while True:
        page_features = page.get("features", [])
        features.extend(normalize_item(item, collection_id) for item in page_features)
        if len(features) >= item_count:
            break

        next_link = find_next_link(page)
        if not next_link:
            break

        href = next_link.get("href")
        if href is None:
            break

        method = str(next_link.get("method", "GET")).upper()
        if method == "POST":
            body = next_link.get("body")
            if not isinstance(body, dict):
                body = base_body
            page = request_json(href, method="POST", body=body)
        else:
            page = request_json(href)

    if len(features) < item_count:
        raise RuntimeError(
            f"Only fetched {len(features)} items for {collection_id}; expected {item_count}",
        )

    features = features[:item_count]
    ids = [item.get("id") for item in features]
    if len(ids) != len(set(ids)):
        raise RuntimeError(f"Duplicate item ids detected for {collection_id}")

    return collection_doc, features


def validate_fixture(output_dir: Path, collection_id: str, item_count: int) -> None:
    collection_path = output_dir / collection_id / "collection.json"
    items_path = output_dir / collection_id / "items.ndjson"

    if not collection_path.exists() or not items_path.exists():
        raise FileNotFoundError(f"Missing fixture files for {collection_id} in {output_dir}")

    with collection_path.open("r", encoding="utf-8") as src:
        collection_doc = json.load(src)
    if collection_doc.get("id") != collection_id:
        raise ValueError(
            f"collection.json id mismatch for {collection_id}: {collection_doc.get('id')}",
        )

    seen: set[str] = set()
    count = 0
    with items_path.open("r", encoding="utf-8") as src:
        for line in src:
            line = line.strip()
            if not line:
                continue
            item = json.loads(line)
            if item.get("collection") != collection_id:
                raise ValueError(
                    f"Item {item.get('id')} has wrong collection {item.get('collection')}",
                )
            item_id = item.get("id")
            if not isinstance(item_id, str):
                raise ValueError("Item id must be a string")
            if item_id in seen:
                raise ValueError(f"Duplicate item id in fixture for {collection_id}: {item_id}")
            seen.add(item_id)
            count += 1

    if count != item_count:
        raise ValueError(
            f"Fixture for {collection_id} has {count} items, expected {item_count}",
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--manifest",
        type=Path,
        default=Path("benchmarks/fixtures/planetary-computer/manifest.json"),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("benchmarks/fixtures/planetary-computer/data"),
    )
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--validate-only", action="store_true")
    args = parser.parse_args()

    with args.manifest.open("r", encoding="utf-8") as src:
        manifest = json.load(src)

    api_url = manifest["api_url"]
    item_count = int(manifest["item_count"])
    collections = manifest["collections"]

    args.output_dir.mkdir(parents=True, exist_ok=True)

    if args.validate_only:
        for collection_cfg in collections:
            validate_fixture(args.output_dir, collection_cfg["id"], item_count)
        print("Fixture validation successful.")
        return 0

    summary: dict[str, Any] = {
        "api_url": api_url,
        "item_count": item_count,
        "collections": {},
    }

    for collection_cfg in collections:
        collection_id = collection_cfg["id"]
        target_dir = args.output_dir / collection_id
        target_dir.mkdir(parents=True, exist_ok=True)

        collection_path = target_dir / "collection.json"
        items_path = target_dir / "items.ndjson"

        if not args.overwrite and collection_path.exists() and items_path.exists():
            validate_fixture(args.output_dir, collection_id, item_count)
            summary["collections"][collection_id] = {
                "collection_sha256": hash_file(collection_path),
                "items_sha256": hash_file(items_path),
                "reused_existing": True,
            }
            continue

        print(f"Fetching {collection_id} from {api_url} ...", file=sys.stderr)
        collection_doc, features = fetch_collection(api_url, collection_cfg, item_count)

        with collection_path.open("w", encoding="utf-8") as dst:
            json.dump(collection_doc, dst, indent=2, sort_keys=True)
            dst.write("\n")

        with items_path.open("w", encoding="utf-8") as dst:
            for item in features:
                dst.write(json.dumps(item, separators=(",", ":"), sort_keys=True))
                dst.write("\n")

        validate_fixture(args.output_dir, collection_id, item_count)
        summary["collections"][collection_id] = {
            "collection_sha256": hash_file(collection_path),
            "items_sha256": hash_file(items_path),
            "reused_existing": False,
        }

    summary_path = args.output_dir / "fixture-summary.json"
    with summary_path.open("w", encoding="utf-8") as dst:
        json.dump(summary, dst, indent=2, sort_keys=True)
        dst.write("\n")

    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (HTTPError, URLError, TimeoutError) as exc:
        print(f"Fixture fetch failed: {exc}", file=sys.stderr)
        raise SystemExit(2)
