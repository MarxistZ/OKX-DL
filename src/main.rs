mod downloader;
mod ledger;
mod lob;
mod processor;

use anyhow::Result;
use chrono::NaiveDate;
use clap::Parser;
use indicatif::MultiProgress;
use rayon::prelude::*;
use std::{path::PathBuf, thread};
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

fn build_process_pool(workers: usize) -> Result<rayon::ThreadPool> {
    rayon::ThreadPoolBuilder::new()
        .num_threads(workers.max(1))
        .build()
        .map_err(Into::into)
}

// ── CLI ──────────────────────────────────────────────────────────────────────

#[derive(Parser)]
#[command(name = "okx-lob", about = "OKX L2 Orderbook 历史数据流水线（鲁棒版）")]
struct Cli {
    /// 只下载，不处理
    #[arg(long)]
    download_only: bool,

    /// 只处理，不下载
    #[arg(long)]
    process_only: bool,

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
}

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
            tracing::error!(
                "失败 {} {} {} {}",
                item.stage,
                item.symbol,
                item.date,
                item.reason
            );
        }
    }
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

    for sym in &symbols {
        std::fs::create_dir_all(raw_dir().join(sym))?;
        std::fs::create_dir_all(parquet_dir().join(sym))?;
        std::fs::create_dir_all(ledger_dir().join(sym))?;
    }

    let mut summary = RunSummary::default();

    if !cli.process_only {
        tracing::info!("=== 下载阶段 ===");
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
            if matches!(result, downloader::DownloadResult::NotAvailable) {
                summary.not_available.push(FailureRecord {
                    symbol: task.symbol,
                    date: task.date,
                    stage: "download",
                    reason: "404".to_string(),
                });
            } else if result.is_real_failure() {
                let downloader::DownloadResult::Failed { reason } = result else {
                    unreachable!("real failures are always DownloadResult::Failed")
                };
                summary.failures.push(FailureRecord {
                    symbol: task.symbol,
                    date: task.date,
                    stage: "download",
                    reason,
                });
            }
        }
    }

    if !cli.download_only {
        tracing::info!("=== 处理阶段 ===");
        let process_tasks = processor::collect_process_tasks(&symbols, start, end);
        let pool = build_process_pool(workers)?;
        let process_results: Vec<_> =
            pool.install(|| process_tasks.par_iter().map(processor::process_day_task).collect());

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

    tracing::info!("=== 完成 ===");
    Ok(())
}

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

    #[test]
    fn build_process_pool_can_be_called_twice() {
        let first = build_process_pool(1).unwrap();
        let second = build_process_pool(1).unwrap();

        first.install(|| assert_eq!(rayon::current_num_threads(), 1));
        second.install(|| assert_eq!(rayon::current_num_threads(), 1));
    }
}
