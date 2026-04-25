#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SMOKE_SCRIPT="$ROOT_DIR/scripts/vps_smoke_test.sh"
WORKLOAD_SCRIPT="$ROOT_DIR/scripts/vps_run_workload.sh"
OLD_SCRIPT="$ROOT_DIR/scripts/run_okx_release.sh"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

assert_eq() {
  local actual="$1"
  local expected="$2"
  local message="$3"
  if [[ "$actual" != "$expected" ]]; then
    fail "$message: expected [$expected], got [$actual]"
  fi
}

make_stub() {
  local stub_path="$1"
  local exit_code="${2:-0}"
  cat >"$stub_path" <<EOF
#!/usr/bin/env bash
printf '%s\n' "\$@" > "\$ARGS_FILE"
exit $exit_code
EOF
  chmod +x "$stub_path"
}

[[ ! -e "$OLD_SCRIPT" ]] || fail "old runner should be removed: $OLD_SCRIPT"
[[ -x "$SMOKE_SCRIPT" ]] || fail "missing executable script: $SMOKE_SCRIPT"
[[ -x "$WORKLOAD_SCRIPT" ]] || fail "missing executable script: $WORKLOAD_SCRIPT"

test_smoke_script_uses_safe_defaults() {
  local stub_path args_file log_dir
  stub_path="$TMP_DIR/stub-okx-lob"
  args_file="$TMP_DIR/smoke-args.txt"
  log_dir="$TMP_DIR/logs-smoke"
  make_stub "$stub_path" 0

  BIN="$stub_path" \
  BUILD=0 \
  ARGS_FILE="$args_file" \
  LOG_DIR="$log_dir" \
  bash "$SMOKE_SCRIPT" >/dev/null

  assert_eq "$(cat "$args_file")" "--symbol
BTC-USDT-SWAP
--start
2024-07-01
--end
2024-07-01
--workers
1
--dl-concurrency
1
--dl-retries
1
--raw-root
$ROOT_DIR/data/raw
--parquet-root
$ROOT_DIR/data/parquet
--raw-max-gb
20
--raw-check-interval-secs
5
--retry-delay-secs
30" "unexpected smoke CLI arguments"

  compgen -G "$log_dir/smoke-*.log" >/dev/null || fail "smoke script did not create a log"
}

test_workload_requires_dates() {
  local stub_path args_file status
  stub_path="$TMP_DIR/stub-okx-lob"
  args_file="$TMP_DIR/workload-missing-dates-args.txt"
  make_stub "$stub_path" 0

  set +e
  BIN="$stub_path" BUILD=0 ARGS_FILE="$args_file" bash "$WORKLOAD_SCRIPT" >/dev/null 2>&1
  status=$?
  set -e

  assert_eq "$status" "64" "workload script should reject missing START/END"
}

test_workload_script_uses_explicit_environment() {
  local stub_path args_file log_dir
  stub_path="$TMP_DIR/stub-okx-lob"
  args_file="$TMP_DIR/workload-args.txt"
  log_dir="$TMP_DIR/logs-workload"
  make_stub "$stub_path" 0

  BIN="$stub_path" \
  BUILD=0 \
  ARGS_FILE="$args_file" \
  LOG_DIR="$log_dir" \
  START=2024-07-01 \
  END=2024-07-02 \
  SYMBOLS="BTC-USDT-SWAP ETH-USDT-SWAP" \
  WORKERS=4 \
  DL_CONCURRENCY=2 \
  DL_RETRIES=5 \
  RAW_ROOT=/data/okx/raw \
  PARQUET_ROOT=/data/okx/parquet \
  RAW_MAX_GB=70 \
  RAW_CHECK_INTERVAL_SECS=5 \
  RETRY_DELAY_SECS=60 \
  bash "$WORKLOAD_SCRIPT" >/dev/null

  assert_eq "$(cat "$args_file")" "--symbol
BTC-USDT-SWAP
ETH-USDT-SWAP
--start
2024-07-01
--end
2024-07-02
--workers
4
--dl-concurrency
2
--dl-retries
5
--raw-root
/data/okx/raw
--parquet-root
/data/okx/parquet
--raw-max-gb
70
--raw-check-interval-secs
5
--retry-delay-secs
60" "unexpected workload CLI arguments"

  compgen -G "$log_dir/workload-*.log" >/dev/null || fail "workload script did not create a log"
}

test_workload_preserves_binary_exit_code() {
  local stub_path args_file status
  stub_path="$TMP_DIR/stub-exit-7"
  args_file="$TMP_DIR/workload-exit-args.txt"
  make_stub "$stub_path" 7

  set +e
  BIN="$stub_path" \
  BUILD=0 \
  ARGS_FILE="$args_file" \
  START=2024-07-01 \
  END=2024-07-01 \
  bash "$WORKLOAD_SCRIPT" >/dev/null 2>&1
  status=$?
  set -e

  assert_eq "$status" "7" "workload script should preserve wrapped binary exit code"
}

test_smoke_script_uses_safe_defaults
test_workload_requires_dates
test_workload_script_uses_explicit_environment
test_workload_preserves_binary_exit_code

echo "ok"
