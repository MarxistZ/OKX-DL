/// 每个币种一个 ledger JSON 文件，记录每天的处理状态。
/// 任何阶段中断后均可从正确状态恢复。
use crate::ledger_path;
use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DayState {
    /// 文件已下载到本地
    pub downloaded: bool,
    /// OKX 返回 404，该日期无数据
    pub not_available: bool,
    /// 下载尝试次数
    pub download_attempts: u32,
    /// LOB 重建 + 采样完成，parquet 已写入
    pub processed: bool,
    /// parquet 行数验证通过
    pub validated: bool,
    /// 验证通过的行数
    pub rows: usize,
    /// raw tar.gz 已删除
    pub raw_deleted: bool,
}

pub struct Ledger {
    symbol: String,
    map: HashMap<String, DayState>,
    dirty: bool,
}

impl Ledger {
    pub fn load(symbol: &str) -> Self {
        let path = ledger_path(symbol);
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
        s.downloaded = true;
        s.download_attempts += 1;
    }

    pub fn mark_not_available(&mut self, d: NaiveDate) {
        let s = self.get_mut(d);
        s.not_available   = true;
        s.download_attempts += 1;
    }

    pub fn inc_attempt(&mut self, d: NaiveDate) {
        self.get_mut(d).download_attempts += 1;
    }

    pub fn mark_validated(&mut self, d: NaiveDate, rows: usize) {
        let s = self.get_mut(d);
        s.processed  = true;
        s.validated  = true;
        s.rows       = rows;
    }

    pub fn mark_raw_deleted(&mut self, d: NaiveDate) {
        self.get_mut(d).raw_deleted = true;
    }

    /// 保存到磁盘（原子写入）
    pub fn save(&mut self) {
        if !self.dirty { return; }
        let path = ledger_path(&self.symbol);
        let tmp  = path.with_extension("tmp");
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
