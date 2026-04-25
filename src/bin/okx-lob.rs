use anyhow::Result;
use chrono::{Days, NaiveDate};
use clap::Parser;
use okx_lob::pipeline;
use std::{path::PathBuf, thread, time::Duration};
use tracing_subscriber::EnvFilter;

fn validate_date_bounds(start: NaiveDate, end: NaiveDate) -> Result<()> {
    if end < start {
        anyhow::bail!("invalid date range: --end {end} is before --start {start}");
    }
    Ok(())
}

fn default_terminal_not_available_cutoff() -> NaiveDate {
    chrono::Local::now()
        .date_naive()
        .checked_sub_days(Days::new(2))
        .unwrap_or_else(|| chrono::Local::now().date_naive())
}

fn ensure_no_failed_tasks(report: &pipeline::PipelineReport) -> Result<()> {
    if report.failed_count > 0 {
        anyhow::bail!("{} tasks failed", report.failed_count);
    }
    Ok(())
}

// ── CLI ──────────────────────────────────────────────────────────────────────

#[derive(Parser)]
#[command(name = "okx-lob", about = "OKX L2 Orderbook 历史数据流水线（鲁棒版）")]
struct Cli {
    /// 币种列表，如 --symbol BTC-USDT ETH-USDT BTC-USDT-SWAP
    #[arg(long = "symbol", num_args = 1.., required = true)]
    symbols: Vec<String>,

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
    let symbols = cli.symbols.clone();
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
        max_retries: 3,
        terminal_not_available_cutoff: default_terminal_not_available_cutoff(),
    };

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(cli.dl_concurrency.max(2))
        .enable_all()
        .build()?;

    let report = rt.block_on(pipeline::run(&symbols, start, end, config))?;
    tracing::info!(
        "完成：success {}  not_available {}  failed {}",
        report.success_count,
        report.not_available_count,
        report.failed_count
    );

    ensure_no_failed_tasks(&report)?;
    tracing::info!("=== 完成 ===");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;
    use clap::Parser;

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
    fn cli_accepts_multiple_symbols_when_symbol_is_last() {
        let cli = Cli::try_parse_from([
            "okx-lob",
            "--start",
            "2024-07-01",
            "--end",
            "2024-07-02",
            "--symbol",
            "BTC-USDT",
            "ETH-USDT",
            "BTC-USDT-SWAP",
            "ETH-USDT-SWAP",
        ])
        .unwrap();

        assert_eq!(
            cli.symbols,
            vec![
                "BTC-USDT".to_string(),
                "ETH-USDT".to_string(),
                "BTC-USDT-SWAP".to_string(),
                "ETH-USDT-SWAP".to_string(),
            ]
        );
    }

    #[test]
    fn cli_requires_symbol_argument() {
        let err = Cli::try_parse_from(["okx-lob", "--start", "2024-07-01"])
            .err()
            .unwrap();
        let msg = err.to_string();

        assert!(msg.contains("--symbol"));
    }

    #[test]
    fn final_report_with_failed_tasks_is_an_error() {
        let report = pipeline::PipelineReport {
            success_count: 1,
            not_available_count: 2,
            failed_count: 1,
        };

        let err = ensure_no_failed_tasks(&report).unwrap_err();

        assert!(err.to_string().contains("1 tasks failed"));
    }

    #[test]
    fn final_report_with_only_not_available_tasks_is_successful() {
        let report = pipeline::PipelineReport {
            success_count: 1,
            not_available_count: 2,
            failed_count: 0,
        };

        ensure_no_failed_tasks(&report).unwrap();
    }
}
