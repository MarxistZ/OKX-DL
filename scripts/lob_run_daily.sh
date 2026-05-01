#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="${BIN:-$ROOT_DIR/target/release/okx-lob}"
BUILD="${BUILD:-0}"

RUN_DATE="${RUN_DATE:-$(date -u +%F)}"
LOOKBACK_DAYS="${LOOKBACK_DAYS:-3}"
SYMBOLS="${SYMBOLS:-BTC-USDT-SWAP ETH-USDT-SWAP}"
WORKERS="${WORKERS:-4}"
DL_CONCURRENCY="${DL_CONCURRENCY:-2}"
DL_RETRIES="${DL_RETRIES:-5}"
RAW_MAX_GB="${RAW_MAX_GB:-70}"
RAW_CHECK_INTERVAL_SECS="${RAW_CHECK_INTERVAL_SECS:-5}"
RETRY_DELAY_SECS="${RETRY_DELAY_SECS:-60}"

DATA_ROOT="${DATA_ROOT:-$HOME/data/okx}"
RAW_ROOT="${RAW_ROOT:-$DATA_ROOT/raw}"
PARQUET_ROOT="${PARQUET_ROOT:-$DATA_ROOT/parquet}"
TRANSFER_ROOT="${TRANSFER_ROOT:-$DATA_ROOT/transfer}"
TARGET_DIR="${TARGET_DIR:-$DATA_ROOT/targets}"
LOG_DIR="${LOG_DIR:-$DATA_ROOT/logs}"
GDRIVE_PARQUET_DEST="${GDRIVE_PARQUET_DEST:-gdrive:okx/parquet}"
GDRIVE_TRANSFER_DEST="${GDRIVE_TRANSFER_DEST:-gdrive:okx/transfer}"
GDRIVE_LOG_DEST="${GDRIVE_LOG_DEST:-gdrive:okx/logs}"
CLEAN_PARQUET_AFTER_UPLOAD="${CLEAN_PARQUET_AFTER_UPLOAD:-0}"

RCLONE_TRANSFERS="${RCLONE_TRANSFERS:-8}"
RCLONE_CHECKERS="${RCLONE_CHECKERS:-16}"
RCLONE_DRIVE_CHUNK_SIZE="${RCLONE_DRIVE_CHUNK_SIZE:-128M}"
TRANSFER_JOBS="${TRANSFER_JOBS:-4}"
TRANSFER_VERIFY="${TRANSFER_VERIFY:-0}"
ZSTD_LEVEL="${ZSTD_LEVEL:-19}"
SCALE="${SCALE:-1000000}"

if [[ "$LOOKBACK_DAYS" -lt 1 ]]; then
  echo "LOOKBACK_DAYS must be >= 1" >&2
  exit 64
fi

if [[ "$BUILD" == "1" ]]; then
  cargo build --release --manifest-path "$ROOT_DIR/Cargo.toml"
fi

if [[ ! -x "$BIN" ]]; then
  echo "missing executable binary: $BIN" >&2
  exit 66
fi

if ! command -v rclone >/dev/null 2>&1; then
  echo "missing rclone executable" >&2
  exit 69
fi

mkdir -p "$RAW_ROOT" "$PARQUET_ROOT" "$TRANSFER_ROOT" "$TARGET_DIR" "$LOG_DIR"

stamp="$(date -u +%Y%m%dT%H%M%SZ)"
target_file="$TARGET_DIR/lob-daily-$stamp.targets"
log_file="$LOG_DIR/lob-daily-$stamp.log"
summary_file="$LOG_DIR/lob-daily-$stamp.summary.csv"

: > "$target_file"
read -r -a symbol_values <<<"$SYMBOLS"
for offset in $(seq 1 "$LOOKBACK_DAYS"); do
  trade_date="$(date -u -d "$RUN_DATE -${offset} day" +%F)"
  for symbol in "${symbol_values[@]}"; do
    echo "$symbol:$trade_date" >> "$target_file"
  done
done

cmd=(
  "$BIN"
  --target-file "$target_file"
  --summary-csv "$summary_file"
  --workers "$WORKERS"
  --dl-concurrency "$DL_CONCURRENCY"
  --dl-retries "$DL_RETRIES"
  --raw-root "$RAW_ROOT"
  --parquet-root "$PARQUET_ROOT"
  --raw-max-gb "$RAW_MAX_GB"
  --raw-check-interval-secs "$RAW_CHECK_INTERVAL_SECS"
  --retry-delay-secs "$RETRY_DELAY_SECS"
)

{
  echo "target_file: $target_file"
  echo "summary_file: $summary_file"
  echo "parquet_root: $PARQUET_ROOT"
  echo "transfer_root: $TRANSFER_ROOT"
  echo "gdrive_parquet_dest: $GDRIVE_PARQUET_DEST"
  echo "gdrive_transfer_dest: $GDRIVE_TRANSFER_DEST"
  echo "gdrive_log_dest: $GDRIVE_LOG_DEST"
} | tee "$log_file"

set +e
"${cmd[@]}" 2>&1 | tee -a "$log_file"
status=${PIPESTATUS[0]}
set -e
if [[ "$status" -ne 0 ]]; then
  exit "$status"
fi

rclone copy "$PARQUET_ROOT" "$GDRIVE_PARQUET_DEST" \
  --progress \
  --transfers "$RCLONE_TRANSFERS" \
  --checkers "$RCLONE_CHECKERS" \
  --fast-list \
  --drive-chunk-size "$RCLONE_DRIVE_CHUNK_SIZE" \
  --retries 10 \
  --low-level-retries 20 \
  --stats 30s \
  2>&1 | tee -a "$log_file"

BUILD=0 \
JOBS="$TRANSFER_JOBS" \
SKIP_EXISTING=1 \
VERIFY="$TRANSFER_VERIFY" \
ZSTD_LEVEL="$ZSTD_LEVEL" \
SCALE="$SCALE" \
"$ROOT_DIR/scripts/delta_batch_transfer.sh" encode \
  "$PARQUET_ROOT" \
  "$TRANSFER_ROOT" \
  2>&1 | tee -a "$log_file"

rclone copy "$TRANSFER_ROOT" "$GDRIVE_TRANSFER_DEST" \
  --progress \
  --transfers "$RCLONE_TRANSFERS" \
  --checkers "$RCLONE_CHECKERS" \
  --fast-list \
  --drive-chunk-size "$RCLONE_DRIVE_CHUNK_SIZE" \
  --retries 10 \
  --low-level-retries 20 \
  --stats 30s \
  2>&1 | tee -a "$log_file"

log_upload_status=0
if [[ -n "$GDRIVE_LOG_DEST" ]]; then
  set +e
  rclone copy "$LOG_DIR" "$GDRIVE_LOG_DEST" \
    --progress \
    --transfers 1 \
    --checkers 2 \
    --retries 5 \
    --low-level-retries 10 \
    --stats 30s \
    2>&1 | tee -a "$log_file"
  log_upload_status=${PIPESTATUS[0]}
  set -e
  if [[ "$log_upload_status" -ne 0 ]]; then
    echo "log upload failed with status $log_upload_status; continuing cleanup" | tee -a "$log_file"
  fi
fi

find "$RAW_ROOT" "$PARQUET_ROOT" -type f -name '*.tmp' -delete
find "$RAW_ROOT" -type d -empty -delete

if [[ "$CLEAN_PARQUET_AFTER_UPLOAD" == "1" ]]; then
  while IFS=: read -r symbol trade_date; do
    [[ -z "$symbol" || "$symbol" == \#* ]] && continue
    rm -f "$PARQUET_ROOT/$symbol/$trade_date.parquet"
    rm -f "$TRANSFER_ROOT/$symbol/$trade_date.okxd.zst"
  done < "$target_file"
  find "$PARQUET_ROOT" -type d -empty -delete
  find "$TRANSFER_ROOT" -type d -empty -delete
fi

exit "$log_upload_status"
