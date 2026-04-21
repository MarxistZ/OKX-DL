# OKX Release Runner Script Design

**Goal:** Add a VPS-friendly shell script that runs the compiled `okx-lob` binary in the foreground with explicit date bounds, optional runtime knobs, and persistent logs.

## Scope

- Add one script at `scripts/run_okx_release.sh`
- Require explicit `START` and `END`
- Support `MODE=all|download|process`
- Support optional `SYMBOL`, `WORKERS`, `DL_CONCURRENCY`, `DL_RETRIES`, `RUST_LOG`
- Write logs under `logs/` while still streaming output to the terminal

## Interface

The script is driven by environment variables:

- Required: `START`, `END`
- Optional: `MODE`, `SYMBOL`, `WORKERS`, `DL_CONCURRENCY`, `DL_RETRIES`, `RUST_LOG`
- Testing override: `BIN` to point at an alternate executable instead of `target/release/okx-lob`

## Behavior

- Exit non-zero if `START` or `END` is missing
- Exit non-zero if `MODE` is not one of `all`, `download`, `process`
- Exit non-zero if the target binary does not exist or is not executable
- Create `logs/` automatically
- Build the CLI arguments from the selected environment variables
- Run in the foreground and `tee` stdout/stderr to a timestamped log file
- Preserve the wrapped program's exit code

## Non-Goals

- No background execution
- No cron/systemd unit generation
- No log rotation or lock files
