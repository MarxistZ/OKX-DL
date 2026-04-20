# OKX LOB Repair Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Normalize the crate, remove cross-day LOB coupling, and make VPS downloads plus day-level parquet conversion rerunnable with correct failure semantics.

**Architecture:** Keep the current module layout, but change the runtime model from a cross-day symbol pipeline into independent `(symbol, date)` tasks. Downloads use bounded async concurrency, processing uses date-level parallelism, and ledger state is persisted per day so reruns and concurrent writes stay safe.

**Tech Stack:** Rust 2021, Tokio, Rayon, Reqwest, Serde, Chrono, Arrow, Parquet, Clap, Tracing

---

## File Map

- Modify: `.gitignore`
- Move: `main.rs` -> `src/main.rs`
- Move: `downloader.rs` -> `src/downloader.rs`
- Move: `processor.rs` -> `src/processor.rs`
- Move: `lob.rs` -> `src/lob.rs`
- Move: `ledger.rs` -> `src/ledger.rs`
- Modify: `src/main.rs`
  - remove checkpoint paths and checkpoint directory initialization
  - switch ledger path helper to `data/ledger/<symbol>/<date>.json`
  - orchestrate day-task downloads and processing
  - aggregate failures and return non-zero on real failures
- Modify: `src/ledger.rs`
  - replace legacy booleans with explicit `DownloadState` and `ProcessState`
  - store one JSON file per `(symbol, date)`
  - provide compatibility upgrade for legacy per-symbol JSON data
- Modify: `src/lob.rs`
  - reject invalid price strings instead of treating them as `0`
  - keep single-day snapshot and update behavior testable
- Modify: `src/processor.rs`
  - add day-isolated JSON line parser
  - add `ProcessTask`, `ProcessResult`, and day-level skip logic
  - remove cross-day resume and checkpoint correctness dependency
- Modify: `src/downloader.rs`
  - add `DownloadTask`, `DownloadResult`, and bounded concurrency execution
  - update per-day ledger state after each result
- Delete usage from code: `data/checkpoints`, `checkpoint_dir()`, `checkpoint_path()`, `find_resume_point()`

Current baseline: git repo already exists and the approved spec is committed at `d9858f2`. Start implementation from that commit rather than re-initializing git.

## Task 1: Normalize The Crate Layout

**Files:**
- Modify: `.gitignore`
- Create: `src/`
- Move: `main.rs` -> `src/main.rs`
- Move: `downloader.rs` -> `src/downloader.rs`
- Move: `processor.rs` -> `src/processor.rs`
- Move: `lob.rs` -> `src/lob.rs`
- Move: `ledger.rs` -> `src/ledger.rs`
- Modify: `src/downloader.rs`

- [ ] **Step 1: Capture the current build failure before changing layout**

Run: `cargo check`
Expected: FAIL with `can't find bin okx-lob at path .../src/main.rs`

- [ ] **Step 2: Extend the repository ignore file so generated data and build output do not get committed**

Update `.gitignore` to contain:

```gitignore
.worktrees/
target/
data/
.codex
.DS_Store
```

- [ ] **Step 3: Move the Rust source files under `src/`**

Run:

```bash
mkdir -p src
mv main.rs src/main.rs
mv downloader.rs src/downloader.rs
mv processor.rs src/processor.rs
mv lob.rs src/lob.rs
mv ledger.rs src/ledger.rs
```

Expected: `Cargo.toml` now points at a real `src/main.rs`

- [ ] **Step 4: Fix the known compile error in the downloader retry message**

Replace the retry message in `src/downloader.rs` with:

```rust
if attempt > 0 {
    let wait = Duration::from_secs(2u64.pow(attempt - 1).min(30));
    pbar.set_message(format!("等待重试 {attempt}/{retries} ({wait:?})..."));
    sleep(wait).await;
}
```

- [ ] **Step 5: Run the build again to verify the crate layout is now valid**

Run: `cargo check`
Expected: PASS

- [ ] **Step 6: Commit the normalized layout baseline**

Run:

```bash
git add .gitignore src/main.rs src/downloader.rs src/processor.rs src/lob.rs src/ledger.rs
git commit -m "chore: normalize rust crate layout"
```

## Task 2: Refactor Ledger State To Per-Day Files

**Files:**
- Modify: `src/main.rs`
- Modify: `src/ledger.rs`
- Test: `src/ledger.rs`

- [ ] **Step 1: Write the failing ledger tests for new state enums and legacy upgrade**

Add these tests to `src/ledger.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    #[test]
    fn upgrade_legacy_flags_maps_to_new_states() {
        let raw = serde_json::json!({
            "downloaded": true,
            "validated": true,
            "rows": 42,
            "raw_deleted": true
        });

        let state = DayState::from_legacy_value(&raw);

        assert_eq!(state.download, DownloadState::Success);
        assert_eq!(state.process, ProcessState::Success);
        assert_eq!(state.rows, Some(42));
        assert!(state.raw_deleted);
    }

    #[test]
    fn day_ledger_path_is_scoped_by_symbol_and_date() {
        let d = NaiveDate::from_ymd_opt(2024, 1, 2).unwrap();
        let path = crate::ledger_path("BTC-USDT", d);
        assert_eq!(
            path,
            std::path::PathBuf::from("data/ledger/BTC-USDT/2024-01-02.json")
        );
    }
}
```

- [ ] **Step 2: Run the targeted ledger tests to confirm they fail**

Run: `cargo test ledger::tests -- --nocapture`
Expected: FAIL because `DownloadState`, `ProcessState`, `from_legacy_value`, and the new `ledger_path` signature do not exist yet

- [ ] **Step 3: Replace the legacy booleans with explicit download and process enums**

Add these types to `src/ledger.rs`:

```rust
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum DownloadState {
    #[default]
    Pending,
    Success,
    NotAvailable,
    Failed,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum ProcessState {
    #[default]
    Pending,
    Success,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DayState {
    #[serde(default)]
    pub download: DownloadState,
    #[serde(default)]
    pub process: ProcessState,
    #[serde(default)]
    pub download_attempts: u32,
    #[serde(default)]
    pub process_attempts: u32,
    #[serde(default)]
    pub rows: Option<usize>,
    #[serde(default)]
    pub raw_present: bool,
    #[serde(default)]
    pub parquet_present: bool,
    #[serde(default)]
    pub raw_deleted: bool,
    #[serde(default)]
    pub last_error: Option<String>,
    #[serde(default = "utc_now_rfc3339")]
    pub updated_at: String,
}

impl Default for DayState {
    fn default() -> Self {
        Self {
            download: DownloadState::Pending,
            process: ProcessState::Pending,
            download_attempts: 0,
            process_attempts: 0,
            rows: None,
            raw_present: false,
            parquet_present: false,
            raw_deleted: false,
            last_error: None,
            updated_at: utc_now_rfc3339(),
        }
    }
}
```

- [ ] **Step 4: Implement per-day load/save helpers and legacy upgrade**

Add these functions to `src/ledger.rs`, and change `src/main.rs` to expose the new helper path:

```rust
pub fn utc_now_rfc3339() -> String {
    chrono::Utc::now().to_rfc3339()
}

impl DayState {
    pub fn from_legacy_value(raw: &serde_json::Value) -> Self {
        let mut state = DayState::default();

        if raw.get("not_available").and_then(|v| v.as_bool()) == Some(true) {
            state.download = DownloadState::NotAvailable;
        } else if raw.get("downloaded").and_then(|v| v.as_bool()) == Some(true) {
            state.download = DownloadState::Success;
        }

        if raw.get("validated").and_then(|v| v.as_bool()) == Some(true)
            || raw.get("processed").and_then(|v| v.as_bool()) == Some(true)
        {
            state.process = ProcessState::Success;
        }

        state.rows = raw.get("rows").and_then(|v| v.as_u64()).map(|v| v as usize);
        state.raw_deleted = raw.get("raw_deleted").and_then(|v| v.as_bool()).unwrap_or(false);
        state
    }

    pub fn can_skip_download(&self, raw_exists: bool) -> bool {
        raw_exists && self.download == DownloadState::Success
    }

    pub fn can_skip_process(&self, parquet_exists: bool) -> bool {
        parquet_exists && self.process == ProcessState::Success
    }
}

pub fn load_day(symbol: &str, d: NaiveDate) -> DayState {
    let path = crate::ledger_path(symbol, d);
    if !path.exists() {
        let legacy_path = crate::ledger_dir().join(format!("{symbol}.json"));
        if legacy_path.exists() {
            let text = match std::fs::read_to_string(&legacy_path) {
                Ok(text) => text,
                Err(_) => return DayState::default(),
            };

            let raw = serde_json::from_str::<serde_json::Value>(&text).unwrap_or_default();
            let key = d.format("%Y-%m-%d").to_string();
            if let Some(day_raw) = raw.get(&key) {
                return DayState::from_legacy_value(day_raw);
            }
        }
        return DayState::default();
    }

    let text = match std::fs::read_to_string(&path) {
        Ok(text) => text,
        Err(_) => return DayState::default(),
    };

    if let Ok(state) = serde_json::from_str::<DayState>(&text) {
        return state;
    }

    let raw = serde_json::from_str::<serde_json::Value>(&text).unwrap_or_default();
    DayState::from_legacy_value(&raw)
}

pub fn save_day(symbol: &str, d: NaiveDate, state: &DayState) -> anyhow::Result<()> {
    let path = crate::ledger_path(symbol, d);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, serde_json::to_string_pretty(state)?)?;
    std::fs::rename(&tmp, &path)?;
    Ok(())
}
```

And change `src/main.rs` to:

```rust
pub fn ledger_path(symbol: &str, d: NaiveDate) -> PathBuf {
    ledger_dir()
        .join(symbol)
        .join(format!("{}.json", d.format("%Y-%m-%d")))
}
```

- [ ] **Step 5: Run the ledger tests again**

Run: `cargo test ledger::tests -- --nocapture`
Expected: PASS

- [ ] **Step 6: Commit the ledger refactor**

Run:

```bash
git add src/main.rs src/ledger.rs
git commit -m "refactor: store ledger state per day"
```

## Task 3: Harden LOB Price Parsing

**Files:**
- Modify: `src/lob.rs`
- Test: `src/lob.rs`

- [ ] **Step 1: Write failing LOB tests for invalid price handling and zero-size removal**

Add these tests to `src/lob.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot(bids: &[(&str, &str)], asks: &[(&str, &str)]) -> OkxRecord {
        OkxRecord {
            action: "snapshot".to_string(),
            ts: "1000".to_string(),
            bids: bids
                .iter()
                .map(|(px, sz)| vec![(*px).to_string(), (*sz).to_string()])
                .collect(),
            asks: asks
                .iter()
                .map(|(px, sz)| vec![(*px).to_string(), (*sz).to_string()])
                .collect(),
        }
    }

    fn update(bids: &[(&str, &str)], asks: &[(&str, &str)]) -> OkxRecord {
        OkxRecord {
            action: "update".to_string(),
            ts: "1100".to_string(),
            bids: bids
                .iter()
                .map(|(px, sz)| vec![(*px).to_string(), (*sz).to_string()])
                .collect(),
            asks: asks
                .iter()
                .map(|(px, sz)| vec![(*px).to_string(), (*sz).to_string()])
                .collect(),
        }
    }

    #[test]
    fn snapshot_ignores_invalid_price_levels() {
        let mut lob = Lob::new();
        lob.apply(&snapshot(&[("bad", "1"), ("100.5", "2")], &[("101.0", "3")]));

        let snap = lob.snapshot(1000);
        assert_eq!(snap.bid_px[0], 100.5);
        assert!(snap.bid_px[1].is_nan());
    }

    #[test]
    fn update_zero_quantity_removes_existing_level() {
        let mut lob = Lob::new();
        lob.apply(&snapshot(&[("100.5", "2")], &[("101.0", "3")]));
        lob.apply(&update(&[("100.5", "0")], &[]));

        let snap = lob.snapshot(1100);
        assert!(snap.bid_px[0].is_nan());
    }
}
```

- [ ] **Step 2: Run the targeted LOB tests to confirm current parsing is wrong**

Run: `cargo test lob::tests -- --nocapture`
Expected: FAIL because invalid prices currently get coerced to `0`

- [ ] **Step 3: Replace `encode()` with explicit validated parsing helpers**

Replace the parsing helpers in `src/lob.rs` with:

```rust
#[inline]
fn encode(s: &str) -> Option<i64> {
    let px = s.parse::<f64>().ok()?;
    if !px.is_finite() || px <= 0.0 {
        return None;
    }
    Some((px * SCALE).round() as i64)
}

#[inline]
fn parse_qty(s: &str) -> Option<f32> {
    let qty = s.parse::<f32>().ok()?;
    if !qty.is_finite() || qty < 0.0 {
        return None;
    }
    Some(qty)
}
```

And update the insert/remove branches to use the helpers:

```rust
if level.len() < 2 {
    continue;
}
let Some(px) = encode(&level[0]) else {
    continue;
};
let Some(q) = parse_qty(&level[1]) else {
    continue;
};
if q > 0.0 {
    self.bids.insert(-px, q);
}
```

Use the same pattern for asks and update handling, with `q == 0.0` removing an existing level.

- [ ] **Step 4: Run the LOB tests again**

Run: `cargo test lob::tests -- --nocapture`
Expected: PASS

- [ ] **Step 5: Commit the LOB hardening**

Run:

```bash
git add src/lob.rs
git commit -m "fix: ignore invalid lob price levels"
```

## Task 4: Extract A Day-Isolated JSON Parser

**Files:**
- Modify: `src/processor.rs`
- Test: `src/processor.rs`

- [ ] **Step 1: Write failing parser tests for snapshot-first daily files**

Add these tests to `src/processor.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn process_json_lines_requires_snapshot_before_update() {
        let input = Cursor::new(
            "{\"action\":\"update\",\"ts\":\"1000\",\"bids\":[[\"100\",\"1\"]],\"asks\":[]}\n"
        );

        let err = process_json_lines(input).unwrap_err();
        assert!(err.to_string().contains("snapshot"));
    }

    #[test]
    fn process_json_lines_parses_one_day_independently() {
        let input = Cursor::new(concat!(
            "{\"action\":\"snapshot\",\"ts\":\"1000\",\"bids\":[[\"100\",\"1\"]],\"asks\":[[\"101\",\"2\"]]}\n",
            "{\"action\":\"update\",\"ts\":\"1100\",\"bids\":[[\"100\",\"3\"]],\"asks\":[]}\n"
        ));

        let snaps = process_json_lines(input).unwrap();
        assert!(!snaps.is_empty());
        assert_eq!(snaps[0].bid_px[0], 100.0);
    }
}
```

- [ ] **Step 2: Run the processor parser tests to verify they fail**

Run: `cargo test processor::tests::process_json_lines -- --nocapture`
Expected: FAIL because `process_json_lines` does not exist

- [ ] **Step 3: Implement a pure parser helper that starts from a fresh `Lob` every time**

Add these functions to `src/processor.rs`:

```rust
fn process_json_lines<R: BufRead>(reader: R) -> Result<Vec<Snapshot>> {
    let mut lob = Lob::new();
    let mut snaps = Vec::with_capacity(900_000);
    let mut next_sample_ms: Option<i64> = None;
    let mut saw_snapshot = false;
    let mut bad_lines = 0usize;
    let mut total_lines = 0usize;

    for line in reader.lines() {
        let line = match line {
            Ok(line) if !line.is_empty() => line,
            _ => continue,
        };
        total_lines += 1;

        let record: OkxRecord = match serde_json::from_str(&line) {
            Ok(record) => record,
            Err(_) => {
                bad_lines += 1;
                continue;
            }
        };

        match record.action.as_str() {
            "snapshot" => {
                lob.apply(&record);
                saw_snapshot = lob.ready;
            }
            "update" if !saw_snapshot => anyhow::bail!("first valid record must be snapshot"),
            "update" => lob.apply(&record),
            _ => continue,
        }

        if !lob.ready {
            continue;
        }

        let ts = lob.ts_ms;
        let next = next_sample_ms.get_or_insert_with(|| (ts / SAMPLE_MS + 1) * SAMPLE_MS);
        while ts >= *next {
            snaps.push(lob.snapshot(*next));
            *next += SAMPLE_MS;
        }
    }

    if !saw_snapshot {
        anyhow::bail!("no snapshot found in daily file");
    }

    if bad_lines > 0 && total_lines > 0 {
        let pct = bad_lines as f64 / total_lines as f64 * 100.0;
        if pct > 1.0 {
            tracing::warn!("坏行 {bad_lines}/{total_lines} ({pct:.1}%)");
        }
    }

    Ok(snaps)
}

pub fn process_day_archive(raw: &Path) -> Result<Vec<Snapshot>> {
    let file = std::fs::File::open(raw)?;
    let gz = GzDecoder::new(file);
    let mut ar = tar::Archive::new(gz);

    let mut entries = ar.entries()?;
    let entry = entries
        .next()
        .ok_or_else(|| anyhow::anyhow!("tar 为空"))??;

    let reader = BufReader::with_capacity(4 * 1024 * 1024, entry);
    process_json_lines(reader)
}
```

- [ ] **Step 4: Run the parser tests again**

Run: `cargo test processor::tests::process_json_lines -- --nocapture`
Expected: PASS

- [ ] **Step 5: Commit the day-isolated parser extraction**

Run:

```bash
git add src/processor.rs
git commit -m "refactor: parse each day from a fresh lob"
```

## Task 5: Replace Cross-Day Processing With Day Tasks

**Files:**
- Modify: `src/processor.rs`
- Modify: `src/ledger.rs`
- Test: `src/processor.rs`

- [ ] **Step 1: Write failing task-selection tests for processing eligibility**

Add these tests to `src/processor.rs`:

```rust
#[cfg(test)]
mod task_tests {
    use super::*;
    use crate::ledger::{DayState, DownloadState, ProcessState};

    #[test]
    fn should_process_day_only_when_download_succeeded_and_output_missing() {
        let ready = DayState {
            download: DownloadState::Success,
            process: ProcessState::Pending,
            ..Default::default()
        };
        let done = DayState {
            download: DownloadState::Success,
            process: ProcessState::Success,
            ..Default::default()
        };
        let not_available = DayState {
            download: DownloadState::NotAvailable,
            ..Default::default()
        };

        assert!(should_process_day(&ready, true, false));
        assert!(!should_process_day(&done, true, true));
        assert!(!should_process_day(&not_available, false, false));
    }
}
```

- [ ] **Step 2: Run the new processor task test to confirm the helper does not exist yet**

Run: `cargo test processor::task_tests -- --nocapture`
Expected: FAIL because `should_process_day` is missing

- [ ] **Step 3: Add process task types and local day-processing logic**

Add these definitions to `src/processor.rs`:

```rust
#[derive(Debug, Clone)]
pub struct ProcessTask {
    pub symbol: String,
    pub date: NaiveDate,
}

#[derive(Debug, Clone)]
pub enum ProcessResult {
    Skipped,
    Success { rows: usize, raw_deleted: bool },
    Failed { reason: String },
}

pub fn should_process_day(state: &DayState, raw_exists: bool, parquet_exists: bool) -> bool {
    state.download == DownloadState::Success
        && raw_exists
        && !(state.process == ProcessState::Success && parquet_exists)
}

pub fn collect_process_tasks(symbols: &[String], start: NaiveDate, end: NaiveDate) -> Vec<ProcessTask> {
    let mut tasks = Vec::new();

    for symbol in symbols {
        for d in date_range(start, end) {
            let state = crate::ledger::load_day(symbol, d);
            let raw = raw_path(symbol, d);
            let out = parquet_path(symbol, d);
            if should_process_day(&state, raw.exists(), out.exists()) {
                tasks.push(ProcessTask {
                    symbol: symbol.clone(),
                    date: d,
                });
            }
        }
    }

    tasks
}
```

- [ ] **Step 4: Replace the old `process_symbol()` loop with a single-day worker**

Replace the cross-day correctness path in `src/processor.rs` with a day worker:

```rust
pub fn process_day_task(task: &ProcessTask) -> ProcessResult {
    let raw = raw_path(&task.symbol, task.date);
    let out = parquet_path(&task.symbol, task.date);
    let mut state = crate::ledger::load_day(&task.symbol, task.date);

    if state.download == DownloadState::NotAvailable {
        return ProcessResult::Skipped;
    }

    if state.can_skip_process(out.exists()) {
        return ProcessResult::Skipped;
    }

    if !raw.exists() {
        state.process = ProcessState::Failed;
        state.process_attempts += 1;
        state.last_error = Some("raw file missing".to_string());
        state.updated_at = crate::ledger::utc_now_rfc3339();
        let _ = crate::ledger::save_day(&task.symbol, task.date, &state);
        return ProcessResult::Failed {
            reason: "raw file missing".to_string(),
        };
    }

    let snaps = match process_day_archive(&raw) {
        Ok(snaps) if !snaps.is_empty() => snaps,
        Ok(_) => {
            state.process = ProcessState::Failed;
            state.process_attempts += 1;
            state.last_error = Some("no snapshots produced".to_string());
            state.updated_at = crate::ledger::utc_now_rfc3339();
            let _ = crate::ledger::save_day(&task.symbol, task.date, &state);
            return ProcessResult::Failed {
                reason: "no snapshots produced".to_string(),
            };
        }
        Err(err) => {
            state.process = ProcessState::Failed;
            state.process_attempts += 1;
            state.last_error = Some(err.to_string());
            state.updated_at = crate::ledger::utc_now_rfc3339();
            let _ = crate::ledger::save_day(&task.symbol, task.date, &state);
            return ProcessResult::Failed {
                reason: err.to_string(),
            };
        }
    };

    if let Err(err) = write_parquet(&out, &snaps, &make_schema()).and_then(|_| validate_parquet(&out, snaps.len())) {
        state.process = ProcessState::Failed;
        state.process_attempts += 1;
        state.last_error = Some(err.to_string());
        state.updated_at = crate::ledger::utc_now_rfc3339();
        let _ = std::fs::remove_file(&out);
        let _ = crate::ledger::save_day(&task.symbol, task.date, &state);
        return ProcessResult::Failed {
            reason: err.to_string(),
        };
    }

    state.process = ProcessState::Success;
    state.process_attempts += 1;
    state.rows = Some(snaps.len());
    state.parquet_present = true;
    state.last_error = None;
    state.updated_at = crate::ledger::utc_now_rfc3339();

    let raw_deleted = std::fs::remove_file(&raw).is_ok();
    state.raw_present = !raw_deleted;
    state.raw_deleted = raw_deleted;
    let _ = crate::ledger::save_day(&task.symbol, task.date, &state);

    ProcessResult::Success {
        rows: snaps.len(),
        raw_deleted,
    }
}
```

Delete these code paths from `src/processor.rs` once the replacement compiles:

```rust
find_resume_point(...)
process_symbol(...)
checkpoint_path(...)
lob.save_checkpoint(...)
Lob::load_checkpoint(...)
```

- [ ] **Step 5: Run the processor task tests again**

Run: `cargo test processor::task_tests -- --nocapture`
Expected: PASS

- [ ] **Step 6: Commit the day-task processor refactor**

Run:

```bash
git add src/processor.rs src/ledger.rs
git commit -m "refactor: process parquet outputs one day at a time"
```

## Task 6: Rewrite The Downloader Around Bounded Concurrency

**Files:**
- Modify: `src/downloader.rs`
- Modify: `src/ledger.rs`
- Test: `src/downloader.rs`

- [ ] **Step 1: Write failing downloader tests for skip and failure classification**

Add these tests to `src/downloader.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::ledger::{DayState, DownloadState};

    #[test]
    fn should_skip_download_only_for_existing_successful_raw() {
        let state = DayState {
            download: DownloadState::Success,
            ..Default::default()
        };

        assert!(should_skip_download(&state, true));
        assert!(!should_skip_download(&state, false));
    }

    #[test]
    fn failed_downloads_are_real_failures_but_404_is_not() {
        assert!(DownloadResult::Failed {
            reason: "network".to_string()
        }.is_real_failure());
        assert!(!DownloadResult::NotAvailable.is_real_failure());
    }
}
```

- [ ] **Step 2: Run the downloader tests to verify they fail**

Run: `cargo test downloader::tests -- --nocapture`
Expected: FAIL because the new helper methods and enum shape do not exist

- [ ] **Step 3: Add task and result types with bounded concurrency execution**

Add these definitions to `src/downloader.rs`:

```rust
#[derive(Debug, Clone)]
pub struct DownloadTask {
    pub symbol: String,
    pub date: NaiveDate,
}

#[derive(Debug, Clone)]
pub enum DownloadResult {
    Skipped,
    Success,
    NotAvailable,
    Failed { reason: String },
}

impl DownloadResult {
    pub fn is_real_failure(&self) -> bool {
        matches!(self, Self::Failed { .. })
    }
}

fn should_skip_download(state: &DayState, raw_exists: bool) -> bool {
    state.can_skip_download(raw_exists)
}
```

And rewrite `download_all()` to use bounded task execution:

```rust
pub async fn download_all(
    symbols: &[String],
    start: NaiveDate,
    end: NaiveDate,
    mp: &MultiProgress,
    concurrency: usize,
    retries: u32,
) -> Result<Vec<(DownloadTask, DownloadResult)>> {
    let client = Arc::new(
        Client::builder()
            .user_agent("Mozilla/5.0")
            .timeout(Duration::from_secs(120))
            .build()?,
    );

    let tasks: Vec<DownloadTask> = symbols
        .iter()
        .flat_map(|symbol| {
            date_range(start, end).into_iter().map(move |date| DownloadTask {
                symbol: symbol.clone(),
                date,
            })
        })
        .collect();

    let total_bar = mp.add(ProgressBar::new(tasks.len() as u64));

    let results = futures::stream::iter(tasks.into_iter().map(|task| {
        let client = Arc::clone(&client);
        let mp = mp.clone();
        let total_bar = total_bar.clone();
        async move {
            let raw = raw_path(&task.symbol, task.date);
            let mut state = crate::ledger::load_day(&task.symbol, task.date);
            if should_skip_download(&state, raw.exists()) {
                total_bar.inc(1);
                return (task, DownloadResult::Skipped);
            }

            let result = download_one(
                client,
                task.symbol.clone(),
                task.date,
                raw,
                mp,
                total_bar,
                retries,
            ).await;

            (task, result)
        }
    }))
    .buffer_unordered(concurrency.max(1))
    .collect::<Vec<_>>()
    .await;

    Ok(results)
}
```

- [ ] **Step 4: Update ledger writes inside the download worker**

Change the end of the download worker in `src/downloader.rs` to update per-day ledger state:

```rust
match &result {
    DownloadResult::Success => {
        state.download = DownloadState::Success;
        state.download_attempts += 1;
        state.raw_present = true;
        state.last_error = None;
    }
    DownloadResult::NotAvailable => {
        state.download = DownloadState::NotAvailable;
        state.download_attempts += 1;
        state.raw_present = false;
        state.last_error = None;
    }
    DownloadResult::Failed { reason } => {
        state.download = DownloadState::Failed;
        state.download_attempts += 1;
        state.raw_present = false;
        state.last_error = Some(reason.clone());
    }
    DownloadResult::Skipped => {}
}
state.updated_at = crate::ledger::utc_now_rfc3339();
let _ = crate::ledger::save_day(&task.symbol, task.date, &state);
```

- [ ] **Step 5: Run the downloader tests again**

Run: `cargo test downloader::tests -- --nocapture`
Expected: PASS

- [ ] **Step 6: Commit the bounded downloader refactor**

Run:

```bash
git add src/downloader.rs src/ledger.rs
git commit -m "refactor: bound downloader concurrency"
```

## Task 7: Rebuild Main Around Structured Results And Exit Semantics

**Files:**
- Modify: `src/main.rs`
- Modify: `src/downloader.rs`
- Modify: `src/processor.rs`
- Test: `src/main.rs`

- [ ] **Step 1: Write failing summary tests for final exit semantics**

Add these tests to `src/main.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    #[test]
    fn summary_ignores_not_available_days() {
        let summary = RunSummary {
            failures: vec![],
            not_available: vec![FailureRecord {
                symbol: "BTC-USDT".to_string(),
                date: NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
                stage: "download",
                reason: "404".to_string(),
            }],
        };

        assert!(!summary.has_real_failures());
    }

    #[test]
    fn summary_reports_real_failures() {
        let summary = RunSummary {
            failures: vec![FailureRecord {
                symbol: "BTC-USDT".to_string(),
                date: NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(),
                stage: "process",
                reason: "raw file missing".to_string(),
            }],
            not_available: vec![],
        };

        assert!(summary.has_real_failures());
    }
}
```

- [ ] **Step 2: Run the summary tests to confirm the new types do not exist yet**

Run: `cargo test summary_ -- --nocapture`
Expected: FAIL because `RunSummary` and `FailureRecord` are missing

- [ ] **Step 3: Add summary types and use them to decide the final return value**

Add these types to `src/main.rs`:

```rust
#[derive(Debug, Clone)]
pub struct FailureRecord {
    pub symbol: String,
    pub date: NaiveDate,
    pub stage: &'static str,
    pub reason: String,
}

#[derive(Debug, Default)]
pub struct RunSummary {
    pub failures: Vec<FailureRecord>,
    pub not_available: Vec<FailureRecord>,
}

impl RunSummary {
    pub fn has_real_failures(&self) -> bool {
        !self.failures.is_empty()
    }

    pub fn print(&self) {
        for item in &self.not_available {
            tracing::info!("404 {} {} {}", item.symbol, item.date, item.reason);
        }
        for item in &self.failures {
            tracing::error!("失败 {} {} {} {}", item.stage, item.symbol, item.date, item.reason);
        }
    }
}
```

- [ ] **Step 4: Rewrite `main()` to orchestrate download and process tasks, then fail only on real failures**

Replace the stage orchestration in `src/main.rs` with this shape:

```rust
fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::from_default_env()
                .add_directive("okx_lob=info".parse().unwrap()),
        )
        .with_target(false)
        .without_time()
        .init();

    let cli = Cli::parse();
    let start = NaiveDate::parse_from_str(&cli.start, "%Y-%m-%d")?;
    let end = match &cli.end {
        Some(s) => NaiveDate::parse_from_str(s, "%Y-%m-%d")?,
        None => chrono::Local::now().date_naive(),
    };
    let symbols = match &cli.symbol {
        Some(s) => vec![s.clone()],
        None => all_symbols(),
    };
    let workers = cli.workers.unwrap_or_else(|| {
        thread::available_parallelism().map(|n| n.get()).unwrap_or(4)
    });

    for sym in &symbols {
        std::fs::create_dir_all(raw_dir().join(sym))?;
        std::fs::create_dir_all(parquet_dir().join(sym))?;
        std::fs::create_dir_all(ledger_dir().join(sym))?;
    }

    let mut summary = RunSummary::default();

    if !cli.process_only {
        let mp = MultiProgress::new();
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(cli.dl_concurrency.max(2))
            .enable_all()
            .build()?;

        let download_results = rt.block_on(downloader::download_all(
            &symbols,
            start,
            end,
            &mp,
            cli.dl_concurrency,
            cli.dl_retries,
        ))?;

        for (task, result) in download_results {
            match result {
                downloader::DownloadResult::NotAvailable => summary.not_available.push(FailureRecord {
                    symbol: task.symbol,
                    date: task.date,
                    stage: "download",
                    reason: "404".to_string(),
                }),
                downloader::DownloadResult::Failed { reason } => summary.failures.push(FailureRecord {
                    symbol: task.symbol,
                    date: task.date,
                    stage: "download",
                    reason,
                }),
                _ => {}
            }
        }
    }

    if !cli.download_only {
        rayon::ThreadPoolBuilder::new()
            .num_threads(workers)
            .build_global()
            .ok();

        let process_tasks = processor::collect_process_tasks(&symbols, start, end);
        let process_results: Vec<_> = process_tasks
            .par_iter()
            .map(processor::process_day_task)
            .collect();

        for (task, result) in process_tasks.into_iter().zip(process_results.into_iter()) {
            if let processor::ProcessResult::Failed { reason } = result {
                summary.failures.push(FailureRecord {
                    symbol: task.symbol,
                    date: task.date,
                    stage: "process",
                    reason,
                });
            }
        }
    }

    summary.print();
    if summary.has_real_failures() {
        anyhow::bail!("run completed with failures");
    }

    Ok(())
}
```

- [ ] **Step 5: Remove the checkpoint helpers from `src/main.rs`**

Delete these path helpers from `src/main.rs`:

```rust
pub fn checkpoint_dir() -> PathBuf { "data/checkpoints".into() }
pub fn checkpoint_path(symbol: &str, d: NaiveDate) -> PathBuf {
    checkpoint_dir().join(symbol).join(format!("{}.json", d.format("%Y-%m-%d")))
}
```

And keep only:

```rust
pub fn raw_dir() -> PathBuf { "data/raw".into() }
pub fn parquet_dir() -> PathBuf { "data/parquet".into() }
pub fn ledger_dir() -> PathBuf { "data/ledger".into() }
```

- [ ] **Step 6: Run the main summary tests again**

Run: `cargo test summary_ -- --nocapture`
Expected: PASS

- [ ] **Step 7: Commit the orchestration rewrite**

Run:

```bash
git add src/main.rs src/downloader.rs src/processor.rs
git commit -m "refactor: return nonzero on real task failures"
```

## Task 8: Final Verification And VPS Smoke Commands

**Files:**
- Modify: `src/main.rs`
- Modify: `src/downloader.rs`
- Modify: `src/processor.rs`
- Modify: `src/lob.rs`
- Modify: `src/ledger.rs`

- [ ] **Step 1: Format the codebase**

Run: `cargo fmt`
Expected: source files rewritten in standard Rust format

- [ ] **Step 2: Run the required verification commands**

Run:

```bash
cargo fmt --check
cargo check
cargo test
```

Expected: all three commands PASS

- [ ] **Step 3: Run a download-only smoke test on a tiny range**

Run:

```bash
cargo run -- --download-only --symbol BTC-USDT --start 2024-01-01 --end 2024-01-02 --dl-concurrency 2
```

Expected:

- one or more raw files created under `data/raw/BTC-USDT/`
- any `404` reported as informational only
- command exits `0` unless there is a real download failure

- [ ] **Step 4: Run a process-only smoke test on the same tiny range**

Run:

```bash
cargo run -- --process-only --symbol BTC-USDT --start 2024-01-01 --end 2024-01-02 --workers 2
```

Expected:

- parquet files created under `data/parquet/BTC-USDT/`
- ledger files created under `data/ledger/BTC-USDT/`
- raw file deleted only for dates whose parquet output validated successfully

- [ ] **Step 5: Re-run the same process command to verify skip behavior**

Run:

```bash
cargo run -- --process-only --symbol BTC-USDT --start 2024-01-01 --end 2024-01-02 --workers 2
```

Expected:

- already successful days are skipped
- command does not rebuild existing validated parquet files

- [ ] **Step 6: Commit the repaired pipeline**

Run:

```bash
git add src/main.rs src/downloader.rs src/processor.rs src/lob.rs src/ledger.rs .gitignore
git commit -m "fix: repair okx lob pipeline"
```
