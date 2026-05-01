#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${OKX_DAILY_ENV_FILE:-$HOME/.config/okx-lob-daily.env}"

if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
fi

export BUILD="${BUILD:-0}"
export LOOKBACK_DAYS="${LOOKBACK_DAYS:-1}"
export WORKERS="${WORKERS:-1}"
export DL_CONCURRENCY="${DL_CONCURRENCY:-1}"
export TRANSFER_JOBS="${TRANSFER_JOBS:-1}"
export RCLONE_TRANSFERS="${RCLONE_TRANSFERS:-1}"
export RCLONE_CHECKERS="${RCLONE_CHECKERS:-2}"
export ZSTD_LEVEL="${ZSTD_LEVEL:-3}"
export RAW_MAX_GB="${RAW_MAX_GB:-8}"
export RAW_CHECK_INTERVAL_SECS="${RAW_CHECK_INTERVAL_SECS:-10}"
export RETRY_DELAY_SECS="${RETRY_DELAY_SECS:-60}"
export CLEAN_PARQUET_AFTER_UPLOAD="${CLEAN_PARQUET_AFTER_UPLOAD:-1}"

cd "$ROOT_DIR"

if command -v ionice >/dev/null 2>&1; then
  exec ionice -c3 nice -n 19 "$ROOT_DIR/scripts/lob_run_daily.sh"
fi

exec nice -n 19 "$ROOT_DIR/scripts/lob_run_daily.sh"
