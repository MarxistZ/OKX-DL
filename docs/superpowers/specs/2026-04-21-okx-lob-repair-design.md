# OKX LOB Repair Design

Date: 2026-04-21
Project: `okx-lob`
Status: Approved design, pending implementation plan

## 1. Goal

Repair the current Rust project so it can reliably run on a VPS to:

- download OKX daily orderbook archives
- convert each daily archive into parquet
- tolerate partial failures without silently producing corrupted downstream data
- preserve processing efficiency without relying on cross-day LOB state

This design explicitly removes the current cross-day checkpoint and resume chain from the processing model. Even though OKX daily files are assumed to begin with a full `snapshot`, the system must not depend on any prior day state when converting a day.

## 2. Scope

This design covers:

- standardizing the crate layout under `src/`
- initializing the repository as git and creating a clean baseline
- replacing cross-day processing semantics with day-isolated processing
- redefining ledger state around `(symbol, date)` task status
- making failures visible in program exit status
- improving runtime stability with bounded download concurrency and date-level processing concurrency
- adding minimum tests and a fixed verification command set

This design does not cover:

- CI platform integration
- database-backed state storage
- major crate split or multi-crate refactor
- parquet chunked writing as a required first-pass optimization

## 3. Success Criteria

The repair is complete when all of the following are true:

- the project builds with the standard Rust crate layout
- the binary can download daily raw files on a VPS
- the binary can process each day independently into parquet
- no processing path depends on prior-day LOB state
- `404` days are handled as expected missing data rather than failures
- real download or processing failures are collected and reported, and the process exits non-zero
- already successful days can be skipped safely on rerun
- minimum regression tests and verification commands pass

The chosen failure semantics are:

- `404` continues and does not make the program fail
- real failures continue collecting across other tasks
- the program exits non-zero if any real download or processing failure occurred

## 4. Current Problems

The current codebase has several structural and semantic defects:

- `Cargo.toml` points to `src/main.rs`, but source files are still at repo root
- the downloader currently creates all async tasks up front, which does not scale well for long date ranges
- processing keeps one `Lob` instance across dates, creating hidden coupling between days
- checkpoints currently serve as a cross-day state chain, which is explicitly undesired
- missing raw files or failed days do not cause a clear final failure status
- successful later days can be produced even after earlier failures, under logic that previously assumed cross-day continuity
- the code has no meaningful tests for download/process state transitions or daily processing assumptions

## 5. Architecture Overview

The repaired system will behave as a task-oriented pipeline:

1. enumerate `(symbol, date)` tasks
2. download raw daily files with bounded concurrency
3. process only successful raw daily files, one day at a time
4. write parquet atomically and validate it
5. record per-day status in ledger
6. print a summary and exit with the correct status code

The core architectural change is this:

- old model: cross-day stateful processing pipeline
- new model: independent day-level processing tasks with explicit status tracking

## 6. Repository Layout

The repository will be normalized to:

```text
okx-lob/
  .git/
  Cargo.toml
  Cargo.lock
  src/
    main.rs
    downloader.rs
    processor.rs
    lob.rs
    ledger.rs
  docs/
    superpowers/
      specs/
        2026-04-21-okx-lob-repair-design.md
```

The design document may be committed before implementation begins. The first implementation baseline after layout repair should contain only repository normalization changes, so later implementation diffs remain easy to review.

## 7. Module Responsibilities

### `main.rs`

`main.rs` will own:

- CLI parsing
- directory initialization
- task list construction
- download and processing stage orchestration
- aggregation of final results
- program exit code selection

`main.rs` will not own:

- HTTP download mechanics
- per-day LOB reconstruction details
- ledger persistence internals

### `downloader.rs`

`downloader.rs` will own:

- downloading raw archives for `(symbol, date)`
- bounded async concurrency
- retry behavior
- atomic file writes
- structured per-task result reporting

It will return structured results such as:

- `Success`
- `NotAvailable`
- `Failed { reason }`
- `Skipped`

### `processor.rs`

`processor.rs` will own:

- selecting processable day tasks
- per-day conversion orchestration
- parquet write and validation flow
- structured per-task processing result reporting

It will not own any cross-day resume chain.

Each day will be processed independently:

- create a fresh `Lob`
- parse the daily archive
- require a valid first `snapshot` before sampling
- write parquet
- validate parquet
- update ledger

### `lob.rs`

`lob.rs` will own:

- single-day LOB reconstruction logic
- applying snapshot and update messages
- fixed-interval sampling logic

It will not encode cross-day checkpoint semantics.

### `ledger.rs`

`ledger.rs` will own:

- per-day task state persistence
- skip eligibility checks
- failure reason persistence
- rerun-safe status tracking

It will no longer serve as a vehicle for cross-day state recovery.

## 8. State Model

Ledger state will become day-task oriented rather than chain oriented.

Recommended shape:

```rust
DayState {
  download: DownloadState,
  process: ProcessState,
  download_attempts: u32,
  process_attempts: u32,
  rows: Option<usize>,
  raw_present: bool,
  parquet_present: bool,
  last_error: Option<String>,
  updated_at: String,
}
```

Enum semantics:

- `DownloadState::Pending`
- `DownloadState::Success`
- `DownloadState::NotAvailable`
- `DownloadState::Failed`

- `ProcessState::Pending`
- `ProcessState::Success`
- `ProcessState::Failed`

Rules:

- `NotAvailable` means an explicit `404`
- `Failed` means a real operational failure
- `ProcessState::Success` is set only after parquet write succeeds and validation passes
- `last_error` stores the most recent meaningful failure reason
- raw and parquet presence flags reflect actual filesystem outcomes, not assumptions

## 9. Download Stage Design

The downloader will continue using async I/O, but with bounded task execution.

Design requirements:

- generate `(symbol, date)` work items
- do not create an unbounded number of active tasks or progress bars
- execute at most `dl_concurrency` concurrent downloads
- keep retry and backoff behavior
- write to a temp file and atomically rename on success
- detect and record `404` separately from real failures

Behavior:

- if raw file already exists and ledger says download succeeded, mark as `Skipped`
- if response is `404`, mark `NotAvailable`
- if network or file-write fails after retries, mark `Failed`
- continue other tasks even when some fail

## 10. Processing Stage Design

Processing will be date-isolated.

For each `(symbol, date)` eligible for processing:

1. create a fresh `Lob`
2. open the daily tar.gz
3. stream lines from the contained file
4. require the first valid book initialization to come from a `snapshot`
5. apply subsequent `update` events for that day only
6. sample snapshots at the configured interval
7. write parquet atomically
8. validate the parquet row count and structural sanity
9. update ledger
10. optionally delete raw only after process success

Eligibility rules:

- process only if `download == Success`
- skip if `process == Success` and output parquet is present and valid
- never process `NotAvailable` days
- if raw is missing while the ledger is not successful, record a processing failure

This design removes:

- `find_resume_point()`
- cross-day `Lob` reuse
- checkpoint-based recovery chain

Any checkpoint concept retained after implementation must be strictly local and optional. It must not influence correctness across dates.

## 11. Failure and Exit Semantics

The chosen runtime behavior is:

- `404` is normal absence of data and does not fail the program
- real download failures are collected
- real processing failures are collected
- the program continues other tasks after failures
- final exit code is non-zero if any real failure occurred

The final summary must include, at minimum:

- symbol
- date
- stage
- reason

This allows safe VPS batch runs where some days fail, without falsely reporting success.

## 12. Efficiency Strategy

Efficiency will come from safe parallelism and rerun skipping rather than cross-day state reuse.

### Download efficiency

- bounded concurrency instead of spawning all work at once
- lower memory pressure on long date ranges
- controlled network and file descriptor usage

### Processing efficiency

- date-level parallelism instead of symbol-level sequential dependency
- independent tasks distribute better across CPU cores
- stream parsing avoids whole-file JSON materialization
- skip already successful days on rerun

### Deferred optimization

If real samples show memory pressure during single-day parquet generation, batch writing with multiple `RecordBatch` instances may be added later. It is not a first-pass requirement.

## 13. Raw File Deletion Policy

Raw deletion will be local to a single day:

- raw may be deleted only after parquet write and validation succeed for that same day
- raw deletion must not be tied to prior or later dates
- the default may remain delete-on-success
- future CLI extension may add `--keep-raw`, but that is not required for this repair

## 14. Implementation Phases

### Phase 1: Repository normalization

- commit the approved design document before implementation work starts
- initialize git
- create `src/`
- move Rust source files into `src/`
- create `docs/superpowers/specs/`
- repair build layout until `cargo check` works
- create a clean implementation baseline commit after layout repair

### Phase 2: Remove cross-day processing chain

- remove cross-day resume logic
- rewrite processing control flow around independent day tasks
- stop using checkpoints for correctness

### Phase 3: Unify result and failure handling

- define structured download results
- define structured processing results
- collect failures centrally
- enforce final non-zero exit on real failures

### Phase 4: Runtime stability and performance

- bound download concurrency
- enable date-level processing concurrency
- preserve skip behavior for already successful days

### Phase 5: Minimum tests and verification

- add `lob` tests for snapshot and update semantics
- add processor tests for daily initialization assumptions
- add ledger tests for state persistence
- standardize the verification command set

## 15. Verification Requirements

At minimum, the repaired project must pass:

```bash
cargo fmt --check
cargo check
cargo test
```

Additional manual validation should include:

- a small date range with known available files
- at least one expected `404` date
- rerun behavior on already successful dates
- a simulated failed day to confirm final non-zero exit status

## 16. Risks and Mitigations

Risk: removing cross-day logic may uncover hidden assumptions in processing code.

Mitigation:

- isolate processing around explicit day tasks
- add tests for first-snapshot behavior

Risk: date-level task parallelism may increase output file contention or progress output noise.

Mitigation:

- keep output writes atomic
- centralize summary reporting
- keep concurrency bounded

Risk: ledger migration may leave stale legacy fields behind.

Mitigation:

- define a clear transitional read/write strategy during implementation
- prefer additive migration or clean rewrite for existing day-state files

## 17. Final Design Decision

The selected design is a controlled structural refactor:

- normalize the crate
- initialize git and create a baseline
- replace cross-day LOB processing with isolated day processing
- redesign ledger state around explicit day-task outcomes
- preserve efficiency through bounded concurrency and parallel day processing
- add the minimum tests needed to keep the repair stable

This keeps the scope focused on making the pipeline correct, rerunnable, and VPS-friendly without expanding into unrelated architecture work.
