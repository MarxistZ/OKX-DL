# OKX Release Runner Script Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a shell wrapper that runs the compiled `okx-lob` binary with explicit date bounds and tee'd logs.

**Architecture:** Keep the runtime logic in one Bash script under `scripts/`. Validate required environment variables before execution, translate supported env vars into CLI flags, and preserve the binary exit code through `tee`. Use a tiny shell test script to cover validation and argument wiring.

**Tech Stack:** Bash, POSIX shell utilities, existing Rust release binary

---

## File Map

- Create: `scripts/run_okx_release.sh`
- Create: `tests/run_okx_release_test.sh`
- Create: `docs/superpowers/specs/2026-04-21-run-okx-release-script-design.md`
- Create: `docs/superpowers/plans/2026-04-21-run-okx-release-script.md`

### Task 1: Add Failing Shell Coverage

**Files:**
- Create: `tests/run_okx_release_test.sh`

- [ ] **Step 1: Write the failing shell test**

Create a shell test that:

- fails if `scripts/run_okx_release.sh` does not reject missing `START`/`END`
- verifies argument wiring by pointing `BIN` at a temporary stub executable

- [ ] **Step 2: Run the test to confirm it fails**

Run: `bash tests/run_okx_release_test.sh`
Expected: FAIL because `scripts/run_okx_release.sh` does not exist yet

### Task 2: Implement The Runner Script

**Files:**
- Create: `scripts/run_okx_release.sh`
- Modify: `tests/run_okx_release_test.sh`

- [ ] **Step 1: Implement minimal script behavior**

The script should:

- default `BIN` to `target/release/okx-lob`
- require `START` and `END`
- validate `MODE`
- create `logs/`
- translate env vars into CLI arguments
- run the binary in the foreground with `tee`
- preserve the wrapped exit code

- [ ] **Step 2: Run the shell test again**

Run: `bash tests/run_okx_release_test.sh`
Expected: PASS

- [ ] **Step 3: Run a syntax check**

Run: `bash -n scripts/run_okx_release.sh`
Expected: PASS

### Task 3: Minimal Runtime Verification

**Files:**
- Verify: `scripts/run_okx_release.sh`

- [ ] **Step 1: Run the script against the real release binary**

Run: `START=2024-07-01 END=2024-07-02 MODE=process SYMBOL=BTC-USDT WORKERS=2 scripts/run_okx_release.sh`
Expected: command exits successfully and writes a log file under `logs/`

- [ ] **Step 2: Commit**

```bash
git add scripts/run_okx_release.sh tests/run_okx_release_test.sh docs/superpowers/specs/2026-04-21-run-okx-release-script-design.md docs/superpowers/plans/2026-04-21-run-okx-release-script.md
git commit -m "feat: add release runner script"
```
