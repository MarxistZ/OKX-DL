#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -ne 0 ]]; then
  echo "lob_run_hc_volume.sh does not accept arguments" >&2
  exit 64
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="$ROOT_DIR/target/release/okx-lob"
VOLUME="/mnt/HC_Volume_105514197"
DATA_ROOT="$HOME/data/okx"
PARQUET_TARGET="$VOLUME/okx/parquet"
PARQUET_LINK="$DATA_ROOT/parquet"

if [[ ! -d "$VOLUME" ]]; then
  echo "missing volume: $VOLUME" >&2
  exit 65
fi

mkdir -p "$DATA_ROOT" "$DATA_ROOT/raw" "$DATA_ROOT/logs" "$PARQUET_TARGET"
ln -s "$PARQUET_TARGET" "$PARQUET_LINK"

cargo build --release --manifest-path "$ROOT_DIR/Cargo.toml"

echo "parquet: $PARQUET_LINK -> $PARQUET_TARGET"
echo "raw: $DATA_ROOT/raw"
echo "params: workers=10 dl_concurrency=4 raw_max_gb=320"

exec "$BIN" \
  --symbol \
  BTC-USDT BTC-USDT-SWAP \
  XRP-USDT XRP-USDT-SWAP \
  AVAX-USDT AVAX-USDT-SWAP \
  ETH-USDT ETH-USDT-SWAP \
  LINK-USDT LINK-USDT-SWAP \
  SOL-USDT SOL-USDT-SWAP \
  DOGE-USDT DOGE-USDT-SWAP \
  BNB-USDT BNB-USDT-SWAP \
  --start 2025-04-01 \
  --end 2026-04-01 \
  --workers 10 \
  --dl-concurrency 4 \
  --dl-retries 5 \
  --raw-root "$DATA_ROOT/raw" \
  --parquet-root "$PARQUET_LINK" \
  --raw-max-gb 320 \
  --raw-check-interval-secs 5 \
  --retry-delay-secs 60
