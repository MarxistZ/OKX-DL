use crate::downloader::{self, DownloadStageResult};
use crate::ledger::{load_day, mutate_day, DownloadState, ProcessState, TaskStatus};
use crate::processor::{self, ProcessStageResult};
use crate::{date_range, ledger_dir};
use anyhow::{Context, Result};
use chrono::NaiveDate;
use futures::future::BoxFuture;
use indicatif::MultiProgress;
use reqwest::Client;
use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task::JoinSet;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Task {
    pub symbol: String,
    pub date: NaiveDate,
}

impl Task {
    pub fn new(symbol: String, date: NaiveDate) -> Self {
        Self { symbol, date }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Paths {
    pub raw_root: PathBuf,
    pub parquet_root: PathBuf,
}

impl Default for Paths {
    fn default() -> Self {
        Self::new("data/raw", "data/parquet")
    }
}

impl Paths {
    pub fn new(raw_root: impl Into<PathBuf>, parquet_root: impl Into<PathBuf>) -> Self {
        Self {
            raw_root: raw_root.into(),
            parquet_root: parquet_root.into(),
        }
    }

    pub fn raw_dir(&self, symbol: &str) -> PathBuf {
        self.raw_root.join(symbol)
    }

    pub fn parquet_dir(&self, symbol: &str) -> PathBuf {
        self.parquet_root.join(symbol)
    }

    pub fn raw_path(&self, symbol: &str, d: NaiveDate) -> PathBuf {
        self.raw_dir(symbol)
            .join(format!("{}.tar.gz", d.format("%Y-%m-%d")))
    }

    pub fn parquet_path(&self, symbol: &str, d: NaiveDate) -> PathBuf {
        self.parquet_dir(symbol)
            .join(format!("{}.parquet", d.format("%Y-%m-%d")))
    }
}

#[derive(Debug, Clone)]
pub struct PipelineConfig {
    pub paths: Paths,
    pub dl_concurrency: usize,
    pub process_workers: usize,
    pub process_queue_capacity: usize,
    pub raw_max_bytes: u64,
    pub raw_check_interval: Duration,
    pub retry_delay: Duration,
    pub dl_retries: u32,
    pub max_retries: u32,
    pub terminal_not_available_cutoff: NaiveDate,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct PipelineReport {
    pub success_count: usize,
    pub not_available_count: usize,
    pub failed_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartupPlan {
    pub download_queue: VecDeque<Task>,
    pub process_queue: VecDeque<Task>,
    pub remaining: usize,
    pub success_count: usize,
    pub not_available_count: usize,
}

#[derive(Debug, Clone)]
struct ScheduledTask {
    ready_at: Instant,
    task: Task,
}

impl ScheduledTask {
    fn new(task: Task, delay: Duration) -> Self {
        Self {
            ready_at: Instant::now() + delay,
            task,
        }
    }
}

pub type DownloadFn =
    Arc<dyn Fn(Task) -> BoxFuture<'static, DownloadStageResult> + Send + Sync>;
pub type ProcessFn = Arc<dyn Fn(Task) -> ProcessStageResult + Send + Sync>;

pub fn prepare_startup(tasks: &[Task], paths: &Paths) -> Result<StartupPlan> {
    let mut startup = StartupPlan {
        download_queue: VecDeque::new(),
        process_queue: VecDeque::new(),
        remaining: 0,
        success_count: 0,
        not_available_count: 0,
    };

    for task in tasks {
        let state = load_day(&task.symbol, task.date);
        let raw = paths.raw_path(&task.symbol, task.date);
        let parquet = paths.parquet_path(&task.symbol, task.date);

        if state.task_status == TaskStatus::Success && parquet.exists() {
            let _ = remove_if_exists(&raw);
            startup.success_count += 1;
            continue;
        }

        if state.task_status == TaskStatus::NotAvailable {
            let _ = remove_if_exists(&raw);
            startup.not_available_count += 1;
            continue;
        }

        startup.remaining += 1;

        if raw.exists() && !parquet.exists() {
            startup.process_queue.push_back(task.clone());
        } else {
            startup.download_queue.push_back(task.clone());
        }
    }

    Ok(startup)
}

pub fn cleanup_temporary_files(paths: &Paths) -> Result<()> {
    cleanup_tmp_under(&paths.raw_root)?;
    cleanup_tmp_under(&paths.parquet_root)?;
    Ok(())
}

pub async fn run(
    symbols: &[String],
    start: NaiveDate,
    end: NaiveDate,
    config: PipelineConfig,
) -> Result<PipelineReport> {
    let tasks: Vec<Task> = symbols
        .iter()
        .flat_map(|symbol| {
            date_range(start, end)
                .into_iter()
                .map(move |date| Task::new(symbol.clone(), date))
        })
        .collect();

    for symbol in symbols {
        std::fs::create_dir_all(config.paths.raw_dir(symbol))?;
        std::fs::create_dir_all(config.paths.parquet_dir(symbol))?;
        std::fs::create_dir_all(ledger_dir().join(symbol))?;
    }

    let client = Arc::new(
        Client::builder()
            .user_agent("Mozilla/5.0")
            .timeout(Duration::from_secs(120))
            .build()?,
    );
    let mp = Arc::new(MultiProgress::new());

    let download_paths = config.paths.clone();
    let download_mp = Arc::clone(&mp);
    let download_client = Arc::clone(&client);
    let retries = config.dl_retries;
    let download: DownloadFn = Arc::new(move |task: Task| {
        let client = Arc::clone(&download_client);
        let paths = download_paths.clone();
        let mp = Arc::clone(&download_mp);
        Box::pin(async move { downloader::download_stage(client, &task, &paths, mp.as_ref(), retries).await })
    });

    let process_paths = config.paths.clone();
    let process: ProcessFn =
        Arc::new(move |task: Task| processor::process_stage(&task, &process_paths));

    run_with_stages(tasks, &config, download, process).await
}

pub async fn run_with_stages(
    tasks: Vec<Task>,
    config: &PipelineConfig,
    download: DownloadFn,
    process: ProcessFn,
) -> Result<PipelineReport> {
    cleanup_temporary_files(&config.paths)?;

    let mut startup = prepare_startup(&tasks, &config.paths)?;
    let mut report = PipelineReport {
        success_count: startup.success_count,
        not_available_count: startup.not_available_count,
        failed_count: 0,
    };
    let mut delayed = Vec::<ScheduledTask>::new();
    let mut retry_counts = HashMap::<Task, u32>::new();
    let mut downloads = JoinSet::new();
    let mut processes = JoinSet::new();

    while startup.remaining > 0 || !downloads.is_empty() || !processes.is_empty() {
        promote_ready_tasks(&mut delayed, &mut startup.download_queue);

        while processes.len() < config.process_workers.max(1) {
            let Some(task) = startup.process_queue.pop_front() else {
                break;
            };
            let process = Arc::clone(&process);
            processes.spawn_blocking(move || {
                let result = process(task.clone());
                (task, result)
            });
        }

        while downloads.len() < config.dl_concurrency.max(1)
            && startup.process_queue.len() < config.process_queue_capacity.max(1)
        {
            let Some(task) = startup.download_queue.pop_front() else {
                break;
            };

            if raw_usage_bytes(&config.paths.raw_root)? >= config.raw_max_bytes {
                startup.download_queue.push_front(task);
                break;
            }

            let download = Arc::clone(&download);
            downloads.spawn(async move {
                let result = download(task.clone()).await;
                (task, result)
            });
        }

        if downloads.is_empty() && processes.is_empty() {
            tokio::time::sleep(next_idle_delay(&delayed, config.raw_check_interval)).await;
            continue;
        }

        tokio::select! {
            Some(joined) = processes.join_next(), if !processes.is_empty() => {
                let (task, result) = joined.context("process worker panicked")?;
                match result {
                    ProcessStageResult::Success { rows } => {
                        cleanup_raw_artifacts(&config.paths, &task)?;
                        persist_process_success(&task, rows)?;
                        startup.remaining = startup.remaining.saturating_sub(1);
                        report.success_count += 1;
                    }
                    ProcessStageResult::Failed { reason } => {
                        cleanup_parquet_artifacts(&config.paths, &task)?;
                        cleanup_raw_artifacts(&config.paths, &task)?;
                        persist_process_failure(&task, &reason)?;
                        schedule_retry_or_fail(
                            &mut delayed,
                            &mut retry_counts,
                            &mut startup,
                            &mut report,
                            task,
                            config,
                            &reason,
                            "任务重试耗尽",
                        );
                    }
                }
            }
            Some(joined) = downloads.join_next(), if !downloads.is_empty() => {
                let (task, result) = joined.context("download worker panicked")?;
                match result {
                    DownloadStageResult::Success => {
                        persist_download_success(&task)?;
                        startup.process_queue.push_back(task);
                    }
                    DownloadStageResult::NotAvailable => {
                        cleanup_raw_artifacts(&config.paths, &task)?;
                        if task.date <= config.terminal_not_available_cutoff {
                            persist_not_available(&task)?;
                            startup.remaining = startup.remaining.saturating_sub(1);
                            report.not_available_count += 1;
                        } else {
                            let reason = format!(
                                "not available yet: {} is newer than terminal cutoff {}",
                                task.date, config.terminal_not_available_cutoff
                            );
                            persist_download_failure(&task, &reason)?;
                            schedule_retry_or_fail(
                                &mut delayed,
                                &mut retry_counts,
                                &mut startup,
                                &mut report,
                                task,
                                config,
                                &reason,
                                "下载重试耗尽",
                            );
                        }
                    }
                    DownloadStageResult::Failed { reason } => {
                        cleanup_raw_artifacts(&config.paths, &task)?;
                        persist_download_failure(&task, &reason)?;
                        schedule_retry_or_fail(
                            &mut delayed,
                            &mut retry_counts,
                            &mut startup,
                            &mut report,
                            task,
                            config,
                            &reason,
                            "下载重试耗尽",
                        );
                    }
                }
            }
        }
    }

    Ok(report)
}

fn schedule_retry_or_fail(
    delayed: &mut Vec<ScheduledTask>,
    retry_counts: &mut HashMap<Task, u32>,
    startup: &mut StartupPlan,
    report: &mut PipelineReport,
    task: Task,
    config: &PipelineConfig,
    reason: &str,
    exhausted_label: &str,
) {
    let retries = retry_counts.entry(task.clone()).or_insert(0);
    if *retries < config.max_retries {
        *retries += 1;
        delayed.push(ScheduledTask::new(task, config.retry_delay));
        return;
    }

    startup.remaining = startup.remaining.saturating_sub(1);
    report.failed_count += 1;
    tracing::error!(
        "{exhausted_label} {} {}: {reason}",
        task.symbol,
        task.date
    );
}

fn promote_ready_tasks(delayed: &mut Vec<ScheduledTask>, queue: &mut VecDeque<Task>) {
    let now = Instant::now();
    let mut idx = 0;
    while idx < delayed.len() {
        if delayed[idx].ready_at <= now {
            let item = delayed.remove(idx);
            queue.push_back(item.task);
        } else {
            idx += 1;
        }
    }
}

fn next_idle_delay(delayed: &[ScheduledTask], fallback: Duration) -> Duration {
    let now = Instant::now();
    delayed
        .iter()
        .map(|item| item.ready_at.saturating_duration_since(now))
        .min()
        .unwrap_or(fallback)
}

fn persist_download_success(task: &Task) -> Result<()> {
    mutate_day(&task.symbol, task.date, |state| {
        state.download = DownloadState::Success;
        state.download_attempts += 1;
        state.raw_deleted = false;
        state.last_error = None;
        state.rows = None;
        Ok(())
    })?;
    Ok(())
}

fn persist_download_failure(task: &Task, reason: &str) -> Result<()> {
    mutate_day(&task.symbol, task.date, |state| {
        state.download = DownloadState::Failed;
        state.download_attempts += 1;
        state.raw_deleted = true;
        state.last_error = Some(reason.to_string());
        state.rows = None;
        Ok(())
    })?;
    Ok(())
}

fn persist_not_available(task: &Task) -> Result<()> {
    mutate_day(&task.symbol, task.date, |state| {
        state.download = DownloadState::NotAvailable;
        state.download_attempts += 1;
        state.raw_deleted = true;
        state.last_error = None;
        state.rows = None;
        Ok(())
    })?;
    Ok(())
}

fn persist_process_success(task: &Task, rows: usize) -> Result<()> {
    mutate_day(&task.symbol, task.date, |state| {
        state.process = ProcessState::Success;
        state.process_attempts += 1;
        state.rows = Some(rows);
        state.raw_deleted = true;
        state.last_error = None;
        Ok(())
    })?;
    Ok(())
}

fn persist_process_failure(task: &Task, reason: &str) -> Result<()> {
    mutate_day(&task.symbol, task.date, |state| {
        state.process = ProcessState::Failed;
        state.process_attempts += 1;
        state.raw_deleted = true;
        state.last_error = Some(reason.to_string());
        state.rows = None;
        Ok(())
    })?;
    Ok(())
}

fn cleanup_raw_artifacts(paths: &Paths, task: &Task) -> Result<()> {
    remove_if_exists(&paths.raw_path(&task.symbol, task.date))?;
    remove_if_exists(&paths.raw_path(&task.symbol, task.date).with_extension("tmp"))?;
    Ok(())
}

fn cleanup_parquet_artifacts(paths: &Paths, task: &Task) -> Result<()> {
    remove_if_exists(&paths.parquet_path(&task.symbol, task.date))?;
    remove_if_exists(&paths.parquet_path(&task.symbol, task.date).with_extension("tmp"))?;
    Ok(())
}

fn cleanup_tmp_under(root: &Path) -> Result<()> {
    if !root.exists() {
        return Ok(());
    }

    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        if entry.file_type()?.is_dir() {
            cleanup_tmp_under(&path)?;
            continue;
        }
        if path.extension().and_then(|v| v.to_str()) == Some("tmp") {
            remove_if_exists(&path)?;
        }
    }
    Ok(())
}

fn raw_usage_bytes(root: &Path) -> Result<u64> {
    if !root.exists() {
        return Ok(0);
    }

    let mut total = 0u64;
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        if entry.file_type()?.is_dir() {
            total += raw_usage_bytes(&path)?;
            continue;
        }
        if is_raw_usage_file(&path) {
            total += entry.metadata()?.len();
        }
    }
    Ok(total)
}

fn is_raw_usage_file(path: &Path) -> bool {
    path.extension().and_then(|v| v.to_str()) == Some("tmp")
        || path
            .file_name()
            .and_then(|v| v.to_str())
            .map(|name| name.ends_with(".tar.gz"))
            .unwrap_or(false)
}

fn remove_if_exists(path: &Path) -> Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ledger::{load_day, mutate_day, DownloadState, ProcessState, TaskStatus};
    use chrono::NaiveDate;
    use futures::future::BoxFuture;
    use std::collections::VecDeque;
    use std::path::{Path, PathBuf};
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    fn unique_path(prefix: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("okx-lob-{prefix}-{nanos}"))
    }

    fn unique_symbol(prefix: &str) -> String {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        format!("{prefix}-{nanos}")
    }

    fn touch(path: &Path, body: &[u8]) {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(path, body).unwrap();
    }

    fn test_config(raw_root: PathBuf, parquet_root: PathBuf) -> PipelineConfig {
        PipelineConfig {
            paths: Paths::new(raw_root, parquet_root),
            dl_concurrency: 1,
            process_workers: 1,
            process_queue_capacity: 2,
            raw_max_bytes: 1024 * 1024,
            raw_check_interval: Duration::from_millis(1),
            retry_delay: Duration::ZERO,
            dl_retries: 0,
            max_retries: 3,
            terminal_not_available_cutoff: NaiveDate::from_ymd_opt(2024, 7, 1).unwrap(),
        }
    }

    fn cleanup_symbol(symbol: &str, paths: &Paths) {
        let _ = std::fs::remove_dir_all(paths.raw_dir(symbol));
        let _ = std::fs::remove_dir_all(paths.parquet_dir(symbol));
        let _ = std::fs::remove_dir_all(crate::ledger_dir().join(symbol));
    }

    #[test]
    fn prepare_startup_skips_terminal_days_and_routes_existing_raw_to_processing() {
        let raw_root = unique_path("startup-raw");
        let parquet_root = unique_path("startup-parquet");
        let paths = Paths::new(raw_root.clone(), parquet_root.clone());

        let symbol = unique_symbol("startup");
        let success_day = NaiveDate::from_ymd_opt(2024, 7, 1).unwrap();
        let not_available_day = NaiveDate::from_ymd_opt(2024, 7, 2).unwrap();
        let process_day = NaiveDate::from_ymd_opt(2024, 7, 3).unwrap();
        let download_day = NaiveDate::from_ymd_opt(2024, 7, 4).unwrap();

        touch(&paths.parquet_path(&symbol, success_day), b"parquet");
        mutate_day(&symbol, success_day, |state| {
            state.download = DownloadState::Success;
            state.process = ProcessState::Success;
            state.task_status = TaskStatus::Success;
            Ok(())
        })
        .unwrap();

        mutate_day(&symbol, not_available_day, |state| {
            state.download = DownloadState::NotAvailable;
            state.task_status = TaskStatus::NotAvailable;
            Ok(())
        })
        .unwrap();

        touch(&paths.raw_path(&symbol, process_day), b"raw");
        mutate_day(&symbol, process_day, |state| {
            state.download = DownloadState::Success;
            Ok(())
        })
        .unwrap();

        let tasks = vec![
            Task::new(symbol.clone(), success_day),
            Task::new(symbol.clone(), not_available_day),
            Task::new(symbol.clone(), process_day),
            Task::new(symbol.clone(), download_day),
        ];

        let startup = prepare_startup(&tasks, &paths).unwrap();

        assert_eq!(startup.remaining, 2);
        assert_eq!(startup.success_count, 1);
        assert_eq!(startup.not_available_count, 1);
        assert_eq!(
            startup.process_queue,
            VecDeque::from(vec![Task::new(symbol.clone(), process_day)])
        );
        assert_eq!(
            startup.download_queue,
            VecDeque::from(vec![Task::new(symbol.clone(), download_day)])
        );

        cleanup_symbol(&symbol, &paths);
        let _ = std::fs::remove_dir_all(raw_root);
        let _ = std::fs::remove_dir_all(parquet_root);
    }

    #[test]
    fn cleanup_temporary_files_removes_only_tmp_files() {
        let raw_root = unique_path("cleanup-raw");
        let parquet_root = unique_path("cleanup-parquet");
        let paths = Paths::new(raw_root.clone(), parquet_root.clone());
        let symbol = unique_symbol("cleanup");
        let day = NaiveDate::from_ymd_opt(2024, 7, 1).unwrap();

        let raw = paths.raw_path(&symbol, day);
        let raw_tmp = raw.with_extension("tmp");
        let parquet = paths.parquet_path(&symbol, day);
        let parquet_tmp = parquet.with_extension("tmp");

        touch(&raw, b"raw");
        touch(&raw_tmp, b"tmp");
        touch(&parquet, b"parquet");
        touch(&parquet_tmp, b"tmp");

        cleanup_temporary_files(&paths).unwrap();

        assert!(raw.exists());
        assert!(parquet.exists());
        assert!(!raw_tmp.exists());
        assert!(!parquet_tmp.exists());

        cleanup_symbol(&symbol, &paths);
        let _ = std::fs::remove_dir_all(raw_root);
        let _ = std::fs::remove_dir_all(parquet_root);
    }

    #[tokio::test]
    async fn pipeline_retries_failed_process_by_redownloading() {
        let raw_root = unique_path("retry-raw");
        let parquet_root = unique_path("retry-parquet");
        let config = test_config(raw_root.clone(), parquet_root.clone());
        let task = Task::new(unique_symbol("retry"), NaiveDate::from_ymd_opt(2024, 7, 1).unwrap());

        let download_attempts = Arc::new(AtomicUsize::new(0));
        let process_attempts = Arc::new(AtomicUsize::new(0));

        let download_paths = config.paths.clone();
        let download_counter = Arc::clone(&download_attempts);
        let download: DownloadFn = Arc::new(move |task: Task| -> BoxFuture<'static, DownloadStageResult> {
            let paths = download_paths.clone();
            let counter = Arc::clone(&download_counter);
            Box::pin(async move {
                counter.fetch_add(1, Ordering::SeqCst);
                touch(&paths.raw_path(&task.symbol, task.date), b"raw");
                DownloadStageResult::Success
            })
        });

        let process_paths = config.paths.clone();
        let process_counter = Arc::clone(&process_attempts);
        let process: ProcessFn = Arc::new(move |task: Task| {
            let current = process_counter.fetch_add(1, Ordering::SeqCst);
            if current == 0 {
                return ProcessStageResult::Failed {
                    reason: "first pass failed".to_string(),
                };
            }

            touch(&process_paths.parquet_path(&task.symbol, task.date), b"parquet");
            ProcessStageResult::Success { rows: 1 }
        });

        let report = run_with_stages(vec![task.clone()], &config, download, process)
            .await
            .unwrap();

        let state = load_day(&task.symbol, task.date);
        assert_eq!(report.success_count, 1);
        assert_eq!(report.not_available_count, 0);
        assert_eq!(download_attempts.load(Ordering::SeqCst), 2);
        assert_eq!(process_attempts.load(Ordering::SeqCst), 2);
        assert_eq!(state.task_status, TaskStatus::Success);
        assert_eq!(state.download_attempts, 2);
        assert_eq!(state.process_attempts, 2);
        assert!(config.paths.parquet_path(&task.symbol, task.date).exists());
        assert!(!config.paths.raw_path(&task.symbol, task.date).exists());

        cleanup_symbol(&task.symbol, &config.paths);
        let _ = std::fs::remove_dir_all(raw_root);
        let _ = std::fs::remove_dir_all(parquet_root);
    }

    #[tokio::test]
    async fn pipeline_marks_404_as_terminal_without_retrying() {
        let raw_root = unique_path("404-raw");
        let parquet_root = unique_path("404-parquet");
        let config = test_config(raw_root.clone(), parquet_root.clone());
        let task = Task::new(unique_symbol("404"), NaiveDate::from_ymd_opt(2024, 7, 1).unwrap());

        let download_attempts = Arc::new(AtomicUsize::new(0));
        let process_attempts = Arc::new(AtomicUsize::new(0));

        let download_counter = Arc::clone(&download_attempts);
        let download: DownloadFn = Arc::new(move |_task: Task| -> BoxFuture<'static, DownloadStageResult> {
            let counter = Arc::clone(&download_counter);
            Box::pin(async move {
                counter.fetch_add(1, Ordering::SeqCst);
                DownloadStageResult::NotAvailable
            })
        });

        let process_counter = Arc::clone(&process_attempts);
        let process: ProcessFn = Arc::new(move |_task: Task| {
            process_counter.fetch_add(1, Ordering::SeqCst);
            ProcessStageResult::Failed {
                reason: "process should not run".to_string(),
            }
        });

        let report = run_with_stages(vec![task.clone()], &config, download, process)
            .await
            .unwrap();

        let state = load_day(&task.symbol, task.date);
        assert_eq!(report.success_count, 0);
        assert_eq!(report.not_available_count, 1);
        assert_eq!(download_attempts.load(Ordering::SeqCst), 1);
        assert_eq!(process_attempts.load(Ordering::SeqCst), 0);
        assert_eq!(state.task_status, TaskStatus::NotAvailable);
        assert_eq!(state.download, DownloadState::NotAvailable);

        cleanup_symbol(&task.symbol, &config.paths);
        let _ = std::fs::remove_dir_all(raw_root);
        let _ = std::fs::remove_dir_all(parquet_root);
    }

    #[tokio::test]
    async fn pipeline_retries_recent_404_instead_of_marking_terminal() {
        let raw_root = unique_path("recent-404-raw");
        let parquet_root = unique_path("recent-404-parquet");
        let mut config = test_config(raw_root.clone(), parquet_root.clone());
        config.max_retries = 1;
        config.terminal_not_available_cutoff = NaiveDate::from_ymd_opt(2024, 6, 30).unwrap();
        let task = Task::new(
            unique_symbol("recent-404"),
            NaiveDate::from_ymd_opt(2024, 7, 1).unwrap(),
        );

        let download_attempts = Arc::new(AtomicUsize::new(0));
        let process_attempts = Arc::new(AtomicUsize::new(0));

        let download_counter = Arc::clone(&download_attempts);
        let download: DownloadFn = Arc::new(move |_task: Task| -> BoxFuture<'static, DownloadStageResult> {
            let counter = Arc::clone(&download_counter);
            Box::pin(async move {
                counter.fetch_add(1, Ordering::SeqCst);
                DownloadStageResult::NotAvailable
            })
        });

        let process_counter = Arc::clone(&process_attempts);
        let process: ProcessFn = Arc::new(move |_task: Task| {
            process_counter.fetch_add(1, Ordering::SeqCst);
            ProcessStageResult::Failed {
                reason: "process should not run".to_string(),
            }
        });

        let report = run_with_stages(vec![task.clone()], &config, download, process)
            .await
            .unwrap();

        let state = load_day(&task.symbol, task.date);
        assert_eq!(report.success_count, 0);
        assert_eq!(report.not_available_count, 0);
        assert_eq!(report.failed_count, 1);
        assert_eq!(download_attempts.load(Ordering::SeqCst), 2);
        assert_eq!(process_attempts.load(Ordering::SeqCst), 0);
        assert_eq!(state.task_status, TaskStatus::Failed);
        assert_eq!(state.download, DownloadState::Failed);
        assert!(state
            .last_error
            .as_deref()
            .unwrap_or_default()
            .contains("not available yet"));

        cleanup_symbol(&task.symbol, &config.paths);
        let _ = std::fs::remove_dir_all(raw_root);
        let _ = std::fs::remove_dir_all(parquet_root);
    }
}
