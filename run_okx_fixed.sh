#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -ne 0 ]]; then
  echo "run_okx_fixed.sh does not accept arguments" >&2
  exit 64
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

BUILD=1 \
START=2025-04-01 \
END=2026-04-01 \
SYMBOLS="BTC-USDT BTC-USDT-SWAP XRP-USDT XRP-USDT-SWAP AVAX-USDT AVAX-USDT-SWAP ETH-USDT ETH-USDT-SWAP LINK-USDT LINK-USDT-SWAP SOL-USDT SOL-USDT-SWAP DOGE-USDT DOGE-USDT-SWAP BNB-USDT BNB-USDT-SWAP" \
WORKERS=4 \
DL_CONCURRENCY=2 \
DL_RETRIES=5 \
RAW_ROOT="$HOME/data/okx/raw" \
PARQUET_ROOT="$HOME/data/okx/parquet" \
LOG_DIR="$HOME/data/okx/logs" \
RAW_MAX_GB=70 \
RAW_CHECK_INTERVAL_SECS=5 \
RETRY_DELAY_SECS=60 \
"$ROOT_DIR/scripts/vps_run_workload.sh"
