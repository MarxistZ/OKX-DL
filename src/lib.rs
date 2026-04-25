pub mod delta;
mod downloader;
mod ledger;
mod lob;
pub mod pipeline;
mod processor;

use chrono::NaiveDate;
use std::path::PathBuf;

pub const DEPTH: usize = 20;
pub const SAMPLE_MS: i64 = 100;
pub const BASE_URL: &str = "https://static.okx.com/cdn/okx/match/orderbook/L2/400lv/daily";

pub fn raw_dir() -> PathBuf {
    "data/raw".into()
}

pub fn parquet_dir() -> PathBuf {
    "data/parquet".into()
}

pub fn ledger_dir() -> PathBuf {
    "data/ledger".into()
}

pub fn raw_path(symbol: &str, d: NaiveDate) -> PathBuf {
    raw_dir()
        .join(symbol)
        .join(format!("{}.tar.gz", d.format("%Y-%m-%d")))
}

pub fn parquet_path(symbol: &str, d: NaiveDate) -> PathBuf {
    parquet_dir()
        .join(symbol)
        .join(format!("{}.parquet", d.format("%Y-%m-%d")))
}

pub fn ledger_path(symbol: &str, d: NaiveDate) -> PathBuf {
    ledger_dir()
        .join(symbol)
        .join(format!("{}.json", d.format("%Y-%m-%d")))
}

pub fn file_url(symbol: &str, d: NaiveDate) -> String {
    format!(
        "{BASE_URL}/{compact}/{symbol}-L2orderbook-400lv-{dash}.tar.gz",
        compact = d.format("%Y%m%d"),
        dash = d.format("%Y-%m-%d"),
    )
}

pub fn date_range(start: NaiveDate, end: NaiveDate) -> Vec<NaiveDate> {
    let mut v = Vec::new();
    let mut cur = start;
    while cur <= end {
        v.push(cur);
        cur = cur.succ_opt().unwrap();
    }
    v
}
