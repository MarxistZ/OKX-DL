#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="${BIN:-$ROOT_DIR/target/release/okx-lob}"

SYMBOL_ARGS=(
  --symbol
  BTC-USDT
  ETH-USDT
  BTC-USDT-SWAP
  ETH-USDT-SWAP
)

DATE_ARGS=(
  --start 2024-07-01
  --end 2024-07-02
)

RUN_ARGS=(
  --workers 4
  --dl-concurrency 2
  --dl-retries 5
)

PATH_ARGS=(
  --raw-root "$ROOT_DIR/data/raw"
  --parquet-root "$ROOT_DIR/data/parquet"
)

LIMIT_ARGS=(
  --raw-max-gb 70
  --raw-check-interval-secs 5
  --retry-delay-secs 60
)

"$BIN" \
  "${SYMBOL_ARGS[@]}" \
  "${DATE_ARGS[@]}" \
  "${RUN_ARGS[@]}" \
  "${PATH_ARGS[@]}" \
  "${LIMIT_ARGS[@]}"
