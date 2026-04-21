#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="${BIN:-$ROOT_DIR/target/release/okx-lob}"
LOG_DIR="${LOG_DIR:-$ROOT_DIR/logs}"
MODE="${MODE:-all}"
START="${START:-}"
END="${END:-}"

die() {
  echo "error: $*" >&2
  exit 1
}

[[ -n "$START" ]] || die "START and END are required, for example START=2024-07-01 END=2024-07-02"
[[ -n "$END" ]] || die "START and END are required, for example START=2024-07-01 END=2024-07-02"

case "$MODE" in
  all|download|process) ;;
  *)
    die "MODE must be one of: all, download, process"
    ;;
esac

[[ -x "$BIN" ]] || die "binary not found or not executable: $BIN; run cargo build --release first"

mkdir -p "$LOG_DIR"

timestamp="$(date +%Y%m%d_%H%M%S)"
log_file="$LOG_DIR/run_${timestamp}_${MODE}.log"

cmd=("$BIN")

case "$MODE" in
  download)
    cmd+=("--download-only")
    ;;
  process)
    cmd+=("--process-only")
    ;;
esac

if [[ -n "${SYMBOL:-}" ]]; then
  cmd+=("--symbol" "$SYMBOL")
fi

cmd+=("--start" "$START" "--end" "$END")

if [[ -n "${WORKERS:-}" ]]; then
  cmd+=("--workers" "$WORKERS")
fi

if [[ -n "${DL_CONCURRENCY:-}" ]]; then
  cmd+=("--dl-concurrency" "$DL_CONCURRENCY")
fi

if [[ -n "${DL_RETRIES:-}" ]]; then
  cmd+=("--dl-retries" "$DL_RETRIES")
fi

if [[ -n "${RAW_RETENTION:-}" ]]; then
  cmd+=("--raw-retention" "$RAW_RETENTION")
fi

echo "log file: $log_file"
printf 'command:'
for arg in "${cmd[@]}"; do
  printf ' %q' "$arg"
done
printf '\n'

set +e
"${cmd[@]}" 2>&1 | tee "$log_file"
status=${PIPESTATUS[0]}
set -e

exit "$status"
