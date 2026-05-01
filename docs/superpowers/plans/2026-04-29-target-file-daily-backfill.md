# Target File Daily Backfill Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `inst:tradedate` target-file補录 plus a simple daily runner that generates targets, runs the pipeline, uploads to Google Drive, and cleans temporary files.

**Architecture:** Keep the current range mode intact. Add a task-list entry point in `pipeline`, parse target files in the CLI layer, and export a summary CSV from existing day-level ledger state. Use a shell script for scheduling integration and rclone upload.

**Tech Stack:** Rust, clap, chrono, existing ledger JSON files, Bash, rclone.

---

## File Structure

- Modify `src/bin/okx-lob.rs`: CLI arguments, target-file parsing, mode selection, summary CSV option, unit tests.
- Modify `src/pipeline.rs`: add `run_tasks` wrapper and `write_summary_csv` helper.
- Modify `README.md`: document target-file and daily runner usage.
- Create `scripts/lob_run_daily.sh`: generate target file, run binary, upload, clean.

## Tasks

### Task 1: Target File Parsing

**Files:**
- Modify: `src/bin/okx-lob.rs`

- [ ] **Step 1: Write failing parser tests**

Add tests for valid lines, comments/blanks, invalid separator, invalid date, and missing instrument.

- [ ] **Step 2: Run targeted test**

Run: `cargo test --bin okx-lob target_file`

Expected: fail because parser functions do not exist.

- [ ] **Step 3: Implement parser**

Add `parse_target_line` and `load_target_file` returning `Vec<pipeline::Task>`.

- [ ] **Step 4: Re-run targeted test**

Run: `cargo test --bin okx-lob target_file`

Expected: parser tests pass.

### Task 2: CLI Mode Selection

**Files:**
- Modify: `src/bin/okx-lob.rs`
- Modify: `src/pipeline.rs`

- [ ] **Step 1: Write failing CLI and task-entry tests**

Cover `--target-file` without `--symbol`, range mode still requiring `--symbol`, and direct task mode preserving exact listed tasks.

- [ ] **Step 2: Run targeted tests**

Run: `cargo test --bin okx-lob target_file -- --nocapture` and `cargo test pipeline::tests::run_tasks`

Expected: fail until mode selection and task entry point exist.

- [ ] **Step 3: Implement mode selection**

Make `--symbol` required unless `--target-file` is present. In `main`, load exact tasks when target file is present, otherwise build the current range mode.

- [ ] **Step 4: Re-run targeted tests**

Run the same targeted tests and confirm they pass.

### Task 3: Summary CSV

**Files:**
- Modify: `src/pipeline.rs`
- Modify: `src/bin/okx-lob.rs`

- [ ] **Step 1: Write failing summary test**

Create ledger states for two tasks and assert CSV rows contain `inst,tradedate,status,rows,error`.

- [ ] **Step 2: Run targeted test**

Run: `cargo test pipeline::tests::write_summary_csv`

Expected: fail because summary writer does not exist.

- [ ] **Step 3: Implement summary writer**

Read `load_day` for each task and write CSV with simple field escaping.

- [ ] **Step 4: Wire CLI option**

Add `--summary-csv <path>` and call summary writer after pipeline run, before failing on failed task count.

### Task 4: Daily Runner

**Files:**
- Create: `scripts/lob_run_daily.sh`
- Modify: `README.md`

- [ ] **Step 1: Write script**

Generate `inst:date` target files from `SYMBOLS`, `LOOKBACK_DAYS`, and UTC yesterday. Run the binary with `--target-file` and `--summary-csv`. Upload with `rclone copy` only after the binary exits successfully.

- [ ] **Step 2: Validate shell syntax**

Run: `bash -n scripts/lob_run_daily.sh scripts/*.sh`

Expected: all shell scripts parse.

- [ ] **Step 3: Document usage**

Add README examples for manual target file and cron usage.

### Task 5: Full Verification

**Files:**
- All changed files

- [ ] **Step 1: Run Rust tests**

Run: `cargo test`

Expected: all tests pass.

- [ ] **Step 2: Run shell syntax check**

Run: `bash -n scripts/*.sh`

Expected: all scripts parse.

- [ ] **Step 3: Review diff**

Run: `git diff --stat` and `git diff --check`

Expected: no whitespace errors; diff is limited to target-file補录, summary CSV, daily runner, and docs.
