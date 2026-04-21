/// Transitional ledger layer:
/// - keeps legacy per-symbol JSON support for current downloader/processor code
/// - adds day-scoped state and load/save helpers for later tasks
use crate::{ledger_dir, ledger_path};
use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

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

        state.downloaded = raw.get("downloaded").and_then(|v| v.as_bool()).unwrap_or(false);
        state.not_available = raw.get("not_available").and_then(|v| v.as_bool()).unwrap_or(false);
        state.download_attempts = raw
            .get("download_attempts")
            .and_then(|v| v.as_u64())
            .map(|v| v as u32)
            .unwrap_or(0);
        state.processed = raw.get("processed").and_then(|v| v.as_bool()).unwrap_or(false);
        state.validated = raw.get("validated").and_then(|v| v.as_bool()).unwrap_or(false);
        state.rows = raw.get("rows").and_then(|v| v.as_u64()).map(|v| v as usize);
        state.raw_deleted = raw.get("raw_deleted").and_then(|v| v.as_bool()).unwrap_or(false);
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

    pub fn can_skip_download(&self, raw_exists: bool) -> bool {
        raw_exists && self.download == DownloadState::Success
    }

    pub fn can_skip_process(&self, parquet_exists: bool) -> bool {
        parquet_exists && self.process == ProcessState::Success
    }

    pub(crate) fn sync_legacy_flags(&mut self) {
        self.downloaded = self.download == DownloadState::Success;
        self.not_available = self.download == DownloadState::NotAvailable;
        self.processed = self.process == ProcessState::Success;
        self.validated = self.process == ProcessState::Success;
        self.raw_present = self.downloaded && !self.raw_deleted;
        self.parquet_present = self.processed;
        self.updated_at = utc_now_rfc3339();
    }
}

pub struct Ledger {
    symbol: String,
    map: HashMap<String, DayState>,
    dirty: bool,
}

fn legacy_ledger_path(symbol: &str) -> PathBuf {
    ledger_dir().join(format!("{symbol}.json"))
}

pub fn utc_now_rfc3339() -> String {
    chrono::Utc::now().to_rfc3339()
}

pub fn load_day(symbol: &str, d: NaiveDate) -> DayState {
    let path = ledger_path(symbol, d);
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

    let text = match std::fs::read_to_string(&path) {
        Ok(text) => text,
        Err(_) => return DayState::default(),
    };

    if let Ok(mut state) = serde_json::from_str::<DayState>(&text) {
        state.sync_legacy_flags();
        return state;
    }

    let raw = serde_json::from_str::<serde_json::Value>(&text).unwrap_or_default();
    DayState::from_legacy_value(&raw)
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

impl Ledger {
    pub fn load(symbol: &str) -> Self {
        let path = legacy_ledger_path(symbol);
        let map = if path.exists() {
            std::fs::read_to_string(&path)
                .ok()
                .and_then(|s| serde_json::from_str(&s).ok())
                .unwrap_or_default()
        } else {
            HashMap::new()
        };
        Self { symbol: symbol.to_string(), map, dirty: false }
    }

    fn key(d: NaiveDate) -> String {
        d.format("%Y-%m-%d").to_string()
    }

    pub fn get(&self, d: NaiveDate) -> DayState {
        self.map.get(&Self::key(d)).cloned().unwrap_or_default()
    }

    fn get_mut(&mut self, d: NaiveDate) -> &mut DayState {
        self.dirty = true;
        self.map.entry(Self::key(d)).or_default()
    }

    pub fn mark_downloaded(&mut self, d: NaiveDate) {
        let s = self.get_mut(d);
        s.download = DownloadState::Success;
        s.download_attempts += 1;
        s.last_error = None;
        s.sync_legacy_flags();
    }

    pub fn mark_not_available(&mut self, d: NaiveDate) {
        let s = self.get_mut(d);
        s.download = DownloadState::NotAvailable;
        s.download_attempts += 1;
        s.last_error = None;
        s.sync_legacy_flags();
    }

    pub fn inc_attempt(&mut self, d: NaiveDate) {
        let s = self.get_mut(d);
        s.download_attempts += 1;
        s.download = DownloadState::Failed;
        s.sync_legacy_flags();
    }

    pub fn mark_validated(&mut self, d: NaiveDate, rows: usize) {
        let s = self.get_mut(d);
        s.process = ProcessState::Success;
        s.rows = Some(rows);
        s.parquet_present = true;
        s.last_error = None;
        s.sync_legacy_flags();
    }

    pub fn mark_raw_deleted(&mut self, d: NaiveDate) {
        let s = self.get_mut(d);
        s.raw_deleted = true;
        s.sync_legacy_flags();
    }

    pub fn save(&mut self) {
        if !self.dirty {
            return;
        }
        let path = legacy_ledger_path(&self.symbol);
        let tmp = path.with_extension("tmp");
        if let Ok(json) = serde_json::to_string_pretty(&self.map) {
            if std::fs::write(&tmp, json).is_ok() {
                let _ = std::fs::rename(&tmp, &path);
                self.dirty = false;
            }
        }
    }
}

impl Drop for Ledger {
    fn drop(&mut self) {
        self.save();
    }
}

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
