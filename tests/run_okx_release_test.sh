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

[[ -x "$SCRIPT_PATH" ]] || fail "missing executable script: $SCRIPT_PATH"

test_runs_fixed_command_and_creates_log() {
  local stub_path args_file
  stub_path="$TMP_DIR/stub-okx-lob"
  args_file="$TMP_DIR/args.txt"

  cat >"$stub_path" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$@" > "$ARGS_FILE"
exit 0
EOF
  chmod +x "$stub_path"

  BIN="$stub_path" \
  ARGS_FILE="$args_file" \
  bash "$SCRIPT_PATH" >/dev/null

  assert_eq "$(cat "$args_file")" $'--symbol\nBTC-USDT\nETH-USDT\nBTC-USDT-SWAP\nETH-USDT-SWAP\n--start\n2024-07-01\n--end\n2024-07-02\n--workers\n4\n--dl-concurrency\n2\n--dl-retries\n5\n--raw-root\n/home/ray/okx-lob/data/raw\n--parquet-root\n/home/ray/okx-lob/data/parquet\n--raw-max-gb\n70\n--raw-check-interval-secs\n5\n--retry-delay-secs\n60' "unexpected fixed CLI arguments"
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
  BIN="$stub_path" bash "$SCRIPT_PATH" >/dev/null 2>&1
  status=$?
  set -e

  assert_eq "$status" "7" "runner should preserve wrapped binary exit code"
}

test_runs_fixed_command_and_creates_log
test_preserves_binary_exit_code

echo "ok"
