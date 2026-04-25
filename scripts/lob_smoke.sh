#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="${BIN:-$ROOT_DIR/target/release/okx-lob}"
BUILD="${BUILD:-1}"
LOG_DIR="${LOG_DIR:-$ROOT_DIR/logs}"

START="${START:-2024-07-01}"
END="${END:-2024-07-01}"
SYMBOLS="${SYMBOLS:-BTC-USDT-SWAP}"
WORKERS="${WORKERS:-1}"
DL_CONCURRENCY="${DL_CONCURRENCY:-1}"
DL_RETRIES="${DL_RETRIES:-1}"
RAW_ROOT="${RAW_ROOT:-$ROOT_DIR/data/raw}"
PARQUET_ROOT="${PARQUET_ROOT:-$ROOT_DIR/data/parquet}"
RAW_MAX_GB="${RAW_MAX_GB:-20}"
RAW_CHECK_INTERVAL_SECS="${RAW_CHECK_INTERVAL_SECS:-5}"
RETRY_DELAY_SECS="${RETRY_DELAY_SECS:-30}"

if [[ "$BUILD" == "1" ]]; then
  cargo build --release --manifest-path "$ROOT_DIR/Cargo.toml"
fi

if [[ ! -x "$BIN" ]]; then
  echo "missing executable binary: $BIN" >&2
  exit 66
fi

mkdir -p "$LOG_DIR"
log_file="$LOG_DIR/pipeline-smoke-$(date -u +%Y%m%dT%H%M%SZ).log"

symbol_args=(--symbol)
read -r -a symbol_values <<<"$SYMBOLS"
symbol_args+=("${symbol_values[@]}")

cmd=(
  "$BIN"
  "${symbol_args[@]}"
  --start "$START"
  --end "$END"
  --workers "$WORKERS"
  --dl-concurrency "$DL_CONCURRENCY"
  --dl-retries "$DL_RETRIES"
  --raw-root "$RAW_ROOT"
  --parquet-root "$PARQUET_ROOT"
  --raw-max-gb "$RAW_MAX_GB"
  --raw-check-interval-secs "$RAW_CHECK_INTERVAL_SECS"
  --retry-delay-secs "$RETRY_DELAY_SECS"
)

echo "log: $log_file"
set +e
"${cmd[@]}" 2>&1 | tee "$log_file"
status=${PIPESTATUS[0]}
set -e
exit "$status"
