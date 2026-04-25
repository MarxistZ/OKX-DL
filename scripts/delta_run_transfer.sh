#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATA_ROOT="${DATA_ROOT:-$HOME/data/okx}"
PARQUET_ROOT="${PARQUET_ROOT:-$DATA_ROOT/parquet}"
TRANSFER_ROOT="${TRANSFER_ROOT:-$DATA_ROOT/transfer}"
LOG_DIR="${LOG_DIR:-$DATA_ROOT/logs}"
LOG_FILE="$LOG_DIR/delta-transfer-$(date -u +%Y%m%dT%H%M%SZ).log"

mkdir -p "$TRANSFER_ROOT" "$LOG_DIR"

BUILD="${BUILD:-1}" \
JOBS="${JOBS:-2}" \
SKIP_EXISTING="${SKIP_EXISTING:-1}" \
VERIFY="${VERIFY:-0}" \
ZSTD_LEVEL="${ZSTD_LEVEL:-19}" \
SCALE="${SCALE:-1000000}" \
"$ROOT_DIR/scripts/delta_batch_transfer.sh" encode \
  "$PARQUET_ROOT" \
  "$TRANSFER_ROOT" \
  >"$LOG_FILE" 2>&1
