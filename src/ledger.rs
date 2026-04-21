/// Transitional ledger layer:
/// - keeps legacy per-symbol JSON support for current downloader/processor code
/// - adds day-scoped state and load/save helpers for later tasks
use crate::{ledger_dir, ledger_path};
use chrono::NaiveDate;
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

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

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    #[default]
    Pending,
    Failed,
    Success,
    NotAvailable,
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
    pub task_status: TaskStatus,
    #[serde(default)]
    pub raw_deleted: bool,
    #[serde(default)]
    pub last_error: Option<String>,
    #[serde(default = "utc_now_rfc3339")]
    pub updated_at: String,
    #[serde(default)]
    pub downloaded: bool,
    #[serde(default)]
    pub not_available: bool,
    #[serde(default)]
    pub processed: bool,
    #[serde(default)]
    pub validated: bool,
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
            task_status: TaskStatus::Pending,
            raw_deleted: false,
            last_error: None,
            updated_at: utc_now_rfc3339(),
            downloaded: false,
            not_available: false,
            processed: false,
            validated: false,
        }
    }
}

impl DayState {
    pub fn from_legacy_value(raw: &serde_json::Value) -> Self {
        let mut state = DayState::default();

        state.downloaded = raw
            .get("downloaded")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        state.not_available = raw
            .get("not_available")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        state.download_attempts = raw
            .get("download_attempts")
            .and_then(|v| v.as_u64())
            .map(|v| v as u32)
            .unwrap_or(0);
        state.processed = raw
            .get("processed")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        state.validated = raw
            .get("validated")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        state.rows = raw.get("rows").and_then(|v| v.as_u64()).map(|v| v as usize);
        state.raw_deleted = raw
            .get("raw_deleted")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        state.raw_present = state.downloaded && !state.raw_deleted;
        state.parquet_present = state.processed || state.validated;

        state.download = if state.not_available {
            DownloadState::NotAvailable
        } else if state.downloaded {
            DownloadState::Success
        } else if state.download_attempts > 0 {
            DownloadState::Failed
        } else {
            DownloadState::Pending
        };

        state.process = if state.validated || state.processed {
            ProcessState::Success
        } else {
            ProcessState::Pending
        };

        state
    }

    #[cfg(test)]
    pub fn can_skip_download(&self, raw_exists: bool) -> bool {
        raw_exists && self.download == DownloadState::Success
    }

    #[cfg(test)]
    pub fn can_skip_process(&self, parquet_exists: bool) -> bool {
        parquet_exists && self.process == ProcessState::Success
    }

    pub(crate) fn normalize_derived_flags(&mut self) {
        self.downloaded = self.download == DownloadState::Success;
        self.not_available = self.download == DownloadState::NotAvailable;
        self.processed = self.process == ProcessState::Success;
        self.validated = self.process == ProcessState::Success;
        self.raw_present = self.downloaded && !self.raw_deleted;
        self.parquet_present = self.processed;
        self.task_status = if self.download == DownloadState::NotAvailable {
            TaskStatus::NotAvailable
        } else if self.process == ProcessState::Success {
            TaskStatus::Success
        } else if self.download == DownloadState::Failed || self.process == ProcessState::Failed {
            TaskStatus::Failed
        } else {
            TaskStatus::Pending
        };
    }

    pub(crate) fn touch_updated_at(&mut self) {
        self.updated_at = utc_now_rfc3339();
    }
}

fn legacy_ledger_path(symbol: &str) -> PathBuf {
    ledger_dir().join(format!("{symbol}.json"))
}

fn day_lock_path(symbol: &str, d: NaiveDate) -> PathBuf {
    ledger_dir()
        .join(symbol)
        .join(format!("{}.lock", d.format("%Y-%m-%d")))
}

pub fn utc_now_rfc3339() -> String {
    chrono::Utc::now().to_rfc3339()
}

fn read_day_state(path: &Path, symbol: &str, d: NaiveDate) -> DayState {
    if !path.exists() {
        let legacy_path = legacy_ledger_path(symbol);
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

    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(_) => return DayState::default(),
    };

    if let Ok(mut state) = serde_json::from_str::<DayState>(&text) {
        state.normalize_derived_flags();
        return state;
    }

    let raw = serde_json::from_str::<serde_json::Value>(&text).unwrap_or_default();
    DayState::from_legacy_value(&raw)
}

fn lock_day(symbol: &str, d: NaiveDate) -> anyhow::Result<std::fs::File> {
    let path = day_lock_path(symbol, d);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let file = std::fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(path)?;
    file.lock_exclusive()?;
    Ok(file)
}

pub fn load_day(symbol: &str, d: NaiveDate) -> DayState {
    let path = ledger_path(symbol, d);
    read_day_state(&path, symbol, d)
}

pub fn save_day(symbol: &str, d: NaiveDate, state: &DayState) -> anyhow::Result<()> {
    let path = ledger_path(symbol, d);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, serde_json::to_string_pretty(state)?)?;
    std::fs::rename(&tmp, &path)?;
    Ok(())
}

pub fn mutate_day<F, T>(symbol: &str, d: NaiveDate, mut f: F) -> anyhow::Result<T>
where
    F: FnMut(&mut DayState) -> anyhow::Result<T>,
{
    let _lock = lock_day(symbol, d)?;
    let path = ledger_path(symbol, d);
    let mut state = read_day_state(&path, symbol, d);
    state.normalize_derived_flags();
    let result = f(&mut state)?;
    state.normalize_derived_flags();
    state.touch_updated_at();
    save_day(symbol, d, &state)?;
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;
    use std::sync::{Arc, Barrier};
    use std::thread;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    fn unique_symbol(prefix: &str) -> String {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        format!("{prefix}-{nanos}")
    }

    fn cleanup_symbol(symbol: &str) {
        let _ = std::fs::remove_dir_all(crate::ledger_dir().join(symbol));
    }

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

    #[test]
    fn load_day_preserves_updated_at_for_current_json() {
        let symbol = unique_symbol("preserve-updated-at");
        let d = NaiveDate::from_ymd_opt(2024, 1, 2).unwrap();
        let updated_at = "2024-01-02T03:04:05Z".to_string();

        let state = DayState {
            updated_at: updated_at.clone(),
            ..Default::default()
        };
        save_day(&symbol, d, &state).unwrap();

        let loaded = load_day(&symbol, d);

        assert_eq!(loaded.updated_at, updated_at);
        cleanup_symbol(&symbol);
    }

    #[test]
    fn normalize_derived_fields_does_not_touch_timestamp() {
        let updated_at = "2024-01-02T03:04:05Z".to_string();
        let mut state = DayState {
            download: DownloadState::Success,
            process: ProcessState::Success,
            raw_deleted: true,
            updated_at: updated_at.clone(),
            ..Default::default()
        };

        state.normalize_derived_flags();

        assert_eq!(state.updated_at, updated_at);
        assert!(state.downloaded);
        assert!(state.processed);
        assert!(!state.raw_present);
        assert!(state.parquet_present);
    }

    #[test]
    fn mutate_day_updates_timestamp_on_write() {
        let symbol = unique_symbol("mutate-updated-at");
        let d = NaiveDate::from_ymd_opt(2024, 1, 3).unwrap();
        let old_timestamp = "2024-01-01T00:00:00Z".to_string();

        let state = DayState {
            updated_at: old_timestamp.clone(),
            ..Default::default()
        };
        save_day(&symbol, d, &state).unwrap();

        mutate_day(&symbol, d, |state| {
            state.download_attempts += 1;
            Ok(())
        })
        .unwrap();

        let loaded = load_day(&symbol, d);
        assert_eq!(loaded.download_attempts, 1);
        assert_ne!(loaded.updated_at, old_timestamp);
        cleanup_symbol(&symbol);
    }

    #[test]
    fn mutate_day_serializes_concurrent_updates() {
        let symbol = unique_symbol("mutate-concurrent");
        let d = NaiveDate::from_ymd_opt(2024, 1, 4).unwrap();
        let barrier = Arc::new(Barrier::new(2));
        let mut handles: Vec<thread::JoinHandle<anyhow::Result<()>>> = Vec::new();

        for _ in 0..2 {
            let symbol = symbol.clone();
            let barrier = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                barrier.wait();
                mutate_day(&symbol, d, |state| {
                    let attempts = state.download_attempts;
                    thread::sleep(Duration::from_millis(25));
                    state.download_attempts = attempts + 1;
                    Ok(())
                })
            }));
        }

        for handle in handles {
            handle.join().unwrap().unwrap();
        }

        let loaded = load_day(&symbol, d);
        assert_eq!(loaded.download_attempts, 2);
        cleanup_symbol(&symbol);
    }
}
