#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCRIPT_PATH="$ROOT_DIR/scripts/run_okx_release.sh"
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

assert_contains() {
  local haystack="$1"
  local needle="$2"
  local message="$3"
  if [[ "$haystack" != *"$needle"* ]]; then
    fail "$message: missing [$needle] in [$haystack]"
  fi
}

[[ -x "$SCRIPT_PATH" ]] || fail "missing executable script: $SCRIPT_PATH"

test_requires_start_and_end() {
  local output status

  set +e
  output="$(env -i PATH="$PATH" HOME="${HOME:-/tmp}" bash "$SCRIPT_PATH" 2>&1)"
  status=$?
  set -e

  [[ $status -ne 0 ]] || fail "expected missing START/END to fail"
  assert_contains "$output" "START" "missing date error should mention START"
  assert_contains "$output" "END" "missing date error should mention END"
}

test_wires_env_flags_and_creates_log() {
  local stub_path args_file before_count after_count
  stub_path="$TMP_DIR/stub-okx-lob"
  args_file="$TMP_DIR/args.txt"

  cat >"$stub_path" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$@" > "$ARGS_FILE"
exit 0
EOF
  chmod +x "$stub_path"
  mkdir -p "$TMP_DIR/logs"

  before_count="$(find "$TMP_DIR/logs" -maxdepth 1 -type f -name 'run_*_process.log' 2>/dev/null | wc -l | tr -d ' ')"

  START=2024-07-01 \
  END=2024-07-02 \
  MODE=process \
  SYMBOL=BTC-USDT \
  WORKERS=2 \
  DL_CONCURRENCY=3 \
  DL_RETRIES=4 \
  BIN="$stub_path" \
  LOG_DIR="$TMP_DIR/logs" \
  ARGS_FILE="$args_file" \
  bash "$SCRIPT_PATH" >/dev/null

  after_count="$(find "$TMP_DIR/logs" -maxdepth 1 -type f -name 'run_*_process.log' 2>/dev/null | wc -l | tr -d ' ')"

  assert_eq "$after_count" "$((before_count + 1))" "expected one new process log file"
  assert_eq "$(cat "$args_file")" $'--process-only\n--symbol\nBTC-USDT\n--start\n2024-07-01\n--end\n2024-07-02\n--workers\n2\n--dl-concurrency\n3\n--dl-retries\n4' "unexpected CLI argument wiring"
}

test_preserves_binary_exit_code() {
  local stub_path status
  stub_path="$TMP_DIR/stub-exit-7"

  cat >"$stub_path" <<'EOF'
#!/usr/bin/env bash
exit 7
EOF
  chmod +x "$stub_path"

  set +e
  START=2024-07-01 END=2024-07-02 BIN="$stub_path" LOG_DIR="$TMP_DIR/logs" bash "$SCRIPT_PATH" >/dev/null 2>&1
  status=$?
  set -e

  assert_eq "$status" "7" "runner should preserve wrapped binary exit code"
}

test_requires_start_and_end
test_wires_env_flags_and_creates_log
test_preserves_binary_exit_code

echo "ok"
