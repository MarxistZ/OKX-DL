#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<'USAGE'
Usage:
  scripts/delta_batch_transfer.sh encode <input_parquet_dir> <output_delta_dir>
  scripts/delta_batch_transfer.sh decode <input_delta_dir> <output_parquet_dir>

Environment:
  BIN=target/release/okx-delta     okx-delta binary path
  BUILD=0                          set to 1 to run cargo build --release first
  JOBS=4                           parallel file workers
  ZSTD_LEVEL=19                    encode zstd level
  SCALE=1000000                    encode price scale
  SKIP_EXISTING=1                  skip outputs that already exist
  VERIFY=0                         verify each encoded file against source parquet
  DRY_RUN=0                        print planned commands without executing
USAGE
}

if [[ "$#" -ne 3 ]]; then
  usage
  exit 64
fi

MODE="$1"
INPUT_DIR="$2"
OUTPUT_DIR="$3"

if [[ "$MODE" != "encode" && "$MODE" != "decode" ]]; then
  usage
  exit 64
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="${BIN:-$ROOT_DIR/target/release/okx-delta}"
BUILD="${BUILD:-0}"
JOBS="${JOBS:-4}"
ZSTD_LEVEL="${ZSTD_LEVEL:-19}"
SCALE="${SCALE:-1000000}"
SKIP_EXISTING="${SKIP_EXISTING:-1}"
VERIFY="${VERIFY:-0}"
DRY_RUN="${DRY_RUN:-0}"

if [[ "$BUILD" == "1" ]]; then
  cargo build --release --manifest-path "$ROOT_DIR/Cargo.toml"
fi

if [[ ! -x "$BIN" ]]; then
  echo "missing executable binary: $BIN" >&2
  exit 66
fi

if [[ ! -d "$INPUT_DIR" ]]; then
  echo "missing input directory: $INPUT_DIR" >&2
  exit 66
fi

mkdir -p "$OUTPUT_DIR"

export BIN MODE INPUT_DIR OUTPUT_DIR ZSTD_LEVEL SCALE SKIP_EXISTING VERIFY DRY_RUN

process_one() {
  local input="$1"
  local rel output

  rel="${input#"$INPUT_DIR"/}"
  case "$MODE" in
    encode)
      output="$OUTPUT_DIR/${rel%.parquet}.okxd.zst"
      ;;
    decode)
      output="$OUTPUT_DIR/${rel%.okxd.zst}.parquet"
      ;;
  esac

  if [[ "$SKIP_EXISTING" == "1" && -s "$output" ]]; then
    echo "skip existing: $output"
    return 0
  fi

  mkdir -p "$(dirname "$output")"

  if [[ "$DRY_RUN" == "1" ]]; then
    if [[ "$MODE" == "encode" ]]; then
      printf '%q encode %q -o %q --zstd-level %q --scale %q\n' \
        "$BIN" "$input" "$output" "$ZSTD_LEVEL" "$SCALE"
      if [[ "$VERIFY" == "1" ]]; then
        printf '%q verify %q %q\n' "$BIN" "$input" "$output"
      fi
    else
      printf '%q decode %q -o %q\n' "$BIN" "$input" "$output"
    fi
    return 0
  fi

  if [[ "$MODE" == "encode" ]]; then
    "$BIN" encode "$input" -o "$output" --zstd-level "$ZSTD_LEVEL" --scale "$SCALE"
    if [[ "$VERIFY" == "1" ]]; then
      "$BIN" verify "$input" "$output"
    fi
  else
    "$BIN" decode "$input" -o "$output"
  fi
}

export -f process_one

case "$MODE" in
  encode)
    find "$INPUT_DIR" -type f -name '*.parquet' -print0 |
      xargs -0 -n 1 -P "$JOBS" bash -c 'process_one "$0"'
    ;;
  decode)
    find "$INPUT_DIR" -type f -name '*.okxd.zst' -print0 |
      xargs -0 -n 1 -P "$JOBS" bash -c 'process_one "$0"'
    ;;
esac
