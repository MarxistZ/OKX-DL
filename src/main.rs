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
pub const BASE_URL: &str =
    "https://static.okx.com/cdn/okx/match/orderbook/L2/400lv/daily";

pub const SPOTS: &[&str] = &[
    "BTC-USDT", "ETH-USDT", "SOL-USDT", "BNB-USDT",
    "XRP-USDT", "DOGE-USDT", "LINK-USDT", "AVAX-USDT",
];

pub fn all_symbols() -> Vec<String> {
    let mut v: Vec<String> = SPOTS.iter().map(|s| s.to_string()).collect();
    v.extend(SPOTS.iter().map(|s| format!("{s}-SWAP")));
    v
}

// ── 路径工具 ─────────────────────────────────────────────────────────────────

pub fn raw_dir()        -> PathBuf { "data/raw".into() }
pub fn parquet_dir()    -> PathBuf { "data/parquet".into() }
pub fn checkpoint_dir() -> PathBuf { "data/checkpoints".into() }
pub fn ledger_dir()     -> PathBuf { "data/ledger".into() }

pub fn raw_path(symbol: &str, d: NaiveDate) -> PathBuf {
    raw_dir().join(symbol).join(format!("{}.tar.gz", d.format("%Y-%m-%d")))
}
pub fn parquet_path(symbol: &str, d: NaiveDate) -> PathBuf {
    parquet_dir().join(symbol).join(format!("{}.parquet", d.format("%Y-%m-%d")))
}
pub fn checkpoint_path(symbol: &str, d: NaiveDate) -> PathBuf {
    checkpoint_dir().join(symbol).join(format!("{}.json", d.format("%Y-%m-%d")))
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
        dash    = d.format("%Y-%m-%d"),
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

// ── 入口 ─────────────────────────────────────────────────────────────────────

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
        None    => chrono::Local::now().date_naive(),
    };
    let symbols: Vec<String> = match &cli.symbol {
        Some(s) => vec![s.clone()],
        None    => all_symbols(),
    };
    let workers = cli.workers.unwrap_or_else(|| {
        thread::available_parallelism().map(|n| n.get()).unwrap_or(4)
    });

    tracing::info!("币种: {} 个  日期: {} → {}  处理线程: {}", symbols.len(), start, end, workers);

    // 初始化目录
    for sym in &symbols {
        std::fs::create_dir_all(raw_dir().join(sym))?;
        std::fs::create_dir_all(parquet_dir().join(sym))?;
        std::fs::create_dir_all(checkpoint_dir().join(sym))?;
    }
    std::fs::create_dir_all(ledger_dir())?;

    // ── 下载阶段 ────────────────────────────────────────────────────────────
    if !cli.process_only {
        tracing::info!("=== 下载阶段 ===");
        let mp = MultiProgress::new();
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(cli.dl_concurrency.max(2))
            .enable_all()
            .build()?;
        rt.block_on(downloader::download_all(
            &symbols, start, end, &mp,
            cli.dl_concurrency, cli.dl_retries,
        ))?;
    }

    // ── 处理阶段 ────────────────────────────────────────────────────────────
    if !cli.download_only {
        tracing::info!("=== 处理阶段 ===");
        rayon::ThreadPoolBuilder::new()
            .num_threads(workers)
            .build_global()
            .ok();
        let mp = MultiProgress::new();
        symbols.par_iter().for_each(|sym| {
            processor::process_symbol(sym, start, end, &mp);
        });
    }

    tracing::info!("=== 完成 ===");
    Ok(())
}
