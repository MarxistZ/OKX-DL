mod downloader;
mod ledger;
mod lob;
mod pipeline;
mod processor;

use anyhow::Result;
use chrono::NaiveDate;
use clap::Parser;
use std::{path::PathBuf, thread, time::Duration};
use tracing_subscriber::EnvFilter;

// ── 全局常量 ─────────────────────────────────────────────────────────────────

pub const DEPTH: usize = 20;
pub const SAMPLE_MS: i64 = 100;
pub const BASE_URL: &str = "https://static.okx.com/cdn/okx/match/orderbook/L2/400lv/daily";

pub const SPOTS: &[&str] = &[
    "BTC-USDT",
    "ETH-USDT",
    "SOL-USDT",
    "BNB-USDT",
    "XRP-USDT",
    "DOGE-USDT",
    "LINK-USDT",
    "AVAX-USDT",
];

pub fn all_symbols() -> Vec<String> {
    let mut v: Vec<String> = SPOTS.iter().map(|s| s.to_string()).collect();
    v.extend(SPOTS.iter().map(|s| format!("{s}-SWAP")));
    v
}

// ── 路径工具 ─────────────────────────────────────────────────────────────────

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

fn validate_date_bounds(start: NaiveDate, end: NaiveDate) -> Result<()> {
    if end < start {
        anyhow::bail!("invalid date range: --end {end} is before --start {start}");
    }
    Ok(())
}

// ── CLI ──────────────────────────────────────────────────────────────────────

#[derive(Parser)]
#[command(name = "okx-lob", about = "OKX L2 Orderbook 历史数据流水线（鲁棒版）")]
struct Cli {
    /// 单个币种，如 BTC-USDT
    #[arg(long)]
    symbol: Option<String>,

    /// 起始日期 YYYY-MM-DD
    #[arg(long, default_value = "2024-01-01")]
    start: String,

    /// 结束日期 YYYY-MM-DD（默认今天）
    #[arg(long)]
    end: Option<String>,

    /// 处理并行线程数（默认 = CPU 核心数）
    #[arg(long)]
    workers: Option<usize>,

    /// 下载并发数
    #[arg(long, default_value = "4")]
    dl_concurrency: usize,

    /// 下载失败最大重试次数
    #[arg(long, default_value = "5")]
    dl_retries: u32,

    /// 原始 tar.gz 根目录
    #[arg(long, default_value = "data/raw")]
    raw_root: PathBuf,

    /// parquet 输出根目录
    #[arg(long, default_value = "data/parquet")]
    parquet_root: PathBuf,

    /// raw 磁盘占用上限（GB）
    #[arg(long, default_value = "70")]
    raw_max_gb: u64,

    /// raw 磁盘水位检查间隔（秒）
    #[arg(long, default_value = "5")]
    raw_check_interval_secs: u64,

    /// 任务失败重试间隔（秒）
    #[arg(long, default_value = "60")]
    retry_delay_secs: u64,
}

// ── 入口 ─────────────────────────────────────────────────────────────────────

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::from_default_env().add_directive("okx_lob=info".parse().unwrap()),
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
    validate_date_bounds(start, end)?;
    let symbols: Vec<String> = match &cli.symbol {
        Some(s) => vec![s.clone()],
        None => all_symbols(),
    };
    let workers = cli.workers.unwrap_or_else(|| {
        thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
    });

    tracing::info!(
        "币种: {} 个  日期: {} → {}  处理线程: {}",
        symbols.len(),
        start,
        end,
        workers
    );

    let config = pipeline::PipelineConfig {
        paths: pipeline::Paths::new(cli.raw_root, cli.parquet_root),
        dl_concurrency: cli.dl_concurrency.max(1),
        process_workers: workers.max(1),
        process_queue_capacity: workers.max(1) * 2,
        raw_max_bytes: cli.raw_max_gb * 1024 * 1024 * 1024,
        raw_check_interval: Duration::from_secs(cli.raw_check_interval_secs.max(1)),
        retry_delay: Duration::from_secs(cli.retry_delay_secs),
        dl_retries: cli.dl_retries,
    };

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(cli.dl_concurrency.max(2))
        .enable_all()
        .build()?;

    let report = rt.block_on(pipeline::run(&symbols, start, end, config))?;
    tracing::info!(
        "完成：success {}  not_available {}",
        report.success_count,
        report.not_available_count
    );

    tracing::info!("=== 完成 ===");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    #[test]
    fn validate_date_bounds_rejects_end_before_start() {
        let start = NaiveDate::from_ymd_opt(2025, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();

        let err = validate_date_bounds(start, end).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("2025-01-01"));
        assert!(msg.contains("2024-01-01"));
    }

    #[test]
    fn validate_date_bounds_accepts_equal_dates() {
        let day = NaiveDate::from_ymd_opt(2025, 1, 1).unwrap();
        validate_date_bounds(day, day).unwrap();
    }
}
