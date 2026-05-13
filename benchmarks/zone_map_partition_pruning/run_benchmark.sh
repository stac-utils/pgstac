#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)
REPO_ROOT=$(cd -- "$SCRIPT_DIR/../.." >/dev/null 2>&1 && pwd)
CONFIG=${1:-$SCRIPT_DIR/benchmark_config.json}
RESULTS_DIR=${RESULTS_DIR:-$SCRIPT_DIR/results}
mkdir -p "$RESULTS_DIR"
CONFIG_DIR=$(cd -- "$(dirname -- "$CONFIG")" >/dev/null 2>&1 && pwd)
CONFIG_FILE=$(basename -- "$CONFIG")
CONFIG_PATH="$CONFIG_DIR/$CONFIG_FILE"
VOLUME_ARGS=(-v "$SCRIPT_DIR:/bench:ro")
if [[ "$CONFIG_PATH" == "$SCRIPT_DIR/"* ]]; then
  CONTAINER_CONFIG="/bench/${CONFIG_PATH#"$SCRIPT_DIR/"}"
else
  VOLUME_ARGS+=(-v "$CONFIG_DIR:/bench_config:ro")
  CONTAINER_CONFIG="/bench_config/$CONFIG_FILE"
fi

cd "$REPO_ROOT"
if [[ ! -f .env && -f .env.example ]]; then
  cp .env.example .env
fi

docker compose build pgstac pypgstac
docker compose up -d pgstac

docker compose run -T --rm \
  "${VOLUME_ARGS[@]}" \
  -v "$RESULTS_DIR:/bench/results" \
  pypgstac \
  uv run --directory /opt/src/pypgstac --group dev --group test \
  python /bench/benchmark_zone_map_pruning.py --config "$CONTAINER_CONFIG"
