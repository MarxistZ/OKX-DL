use crate::lob::{Lob, OkxRecord, Snapshot};
use crate::pipeline::{Paths, Task};
use crate::{DEPTH, SAMPLE_MS};
use anyhow::Result;
use arrow::array::{ArrayRef, Float32Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use flate2::read::GzDecoder;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[cfg(test)]
use crate::ledger::{load_day, mutate_day, DayState, DownloadState, ProcessState};
#[cfg(test)]
use crate::{date_range, parquet_path, raw_path};
#[cfg(test)]
use chrono::NaiveDate;
#[cfg(test)]
use clap::ValueEnum;

// ── 单日 LOB 重建 ────────────────────────────────────────────────────────────

struct DaySampler {
    lob: Lob,
    next_sample_ms: Option<i64>,
    saw_snapshot: bool,
    bad_lines: usize,
    total_lines: usize,
}

impl DaySampler {
    fn new() -> Self {
        Self {
            lob: Lob::new(),
            next_sample_ms: None,
            saw_snapshot: false,
            bad_lines: 0,
            total_lines: 0,
        }
    }

    fn feed_reader<R, F>(&mut self, reader: R, on_snapshot: &mut F) -> Result<()>
    where
        R: BufRead,
        F: FnMut(Snapshot) -> Result<()>,
    {
        for line in reader.lines() {
            let line = match line {
                Ok(line) if !line.is_empty() => line,
                _ => continue,
            };
            self.total_lines += 1;

            let record: OkxRecord = match serde_json::from_str(&line) {
                Ok(record) => record,
                Err(_) => {
                    self.bad_lines += 1;
                    continue;
                }
            };

            match record.action.as_str() {
                "snapshot" => {
                    self.lob.apply(&record)?;
                    self.saw_snapshot = self.lob.ready;
                }
                "update" if !self.saw_snapshot => {
                    anyhow::bail!("first valid record must be snapshot")
                }
                "update" => self.lob.apply(&record)?,
                _ => continue,
            }

            if !self.lob.ready {
                continue;
            }

            let ts = self.lob.ts_ms;
            let next = self
                .next_sample_ms
                .get_or_insert_with(|| (ts / SAMPLE_MS + 1) * SAMPLE_MS);
            while ts >= *next {
                on_snapshot(self.lob.snapshot(*next))?;
                *next += SAMPLE_MS;
            }
        }

        Ok(())
    }

    fn finish(&self) -> Result<()> {
        if !self.saw_snapshot {
            anyhow::bail!("no snapshot found in daily file");
        }

        if self.bad_lines > 0 && self.total_lines > 0 {
            let pct = self.bad_lines as f64 / self.total_lines as f64 * 100.0;
            if pct > 1.0 {
                tracing::warn!("坏行 {}/{} ({pct:.1}%)", self.bad_lines, self.total_lines);
            }
        }

        Ok(())
    }
}

fn process_archive_entries<F>(raw: &Path, mut on_snapshot: F) -> Result<()>
where
    F: FnMut(Snapshot) -> Result<()>,
{
    let file = std::fs::File::open(raw)?;
    let gz = GzDecoder::new(file);
    let mut ar = tar::Archive::new(gz);
    let mut sampler = DaySampler::new();

    for entry in ar.entries()? {
        let entry = entry?;
        if !entry.header().entry_type().is_file() {
            continue;
        }
        if entry.header().size()? == 0 {
            continue;
        }

        let reader = BufReader::with_capacity(4 * 1024 * 1024, entry);
        sampler.feed_reader(reader, &mut on_snapshot)?;
    }

    sampler.finish()
}

#[cfg(test)]
fn process_json_lines<R: BufRead>(reader: R) -> Result<Vec<Snapshot>> {
    let mut sampler = DaySampler::new();
    let mut snaps = Vec::new();
    sampler.feed_reader(reader, &mut |snapshot| {
        snaps.push(snapshot);
        Ok(())
    })?;
    sampler.finish()?;
    Ok(snaps)
}

#[cfg(test)]
pub fn process_day_archive(raw: &Path) -> Result<Vec<Snapshot>> {
    let mut snaps = Vec::new();
    process_archive_entries(raw, |snapshot| {
        snaps.push(snapshot);
        Ok(())
    })?;
    Ok(snaps)
}

// ── Parquet 写入 + 验证 ───────────────────────────────────────────────────────

const SNAPSHOT_BATCH_SIZE: usize = 10_000;

fn make_schema() -> Arc<Schema> {
    let mut fields = vec![Field::new("timestamp_ms", DataType::Int64, false)];
    for i in 0..DEPTH {
        fields.push(Field::new(format!("bid_px_{i}"), DataType::Float32, true));
        fields.push(Field::new(format!("bid_sz_{i}"), DataType::Float32, true));
    }
    for i in 0..DEPTH {
        fields.push(Field::new(format!("ask_px_{i}"), DataType::Float32, true));
        fields.push(Field::new(format!("ask_sz_{i}"), DataType::Float32, true));
    }
    Arc::new(Schema::new(fields))
}

fn build_record_batch(snaps: &[Snapshot], schema: &Arc<Schema>) -> Result<RecordBatch> {
    let ts: Int64Array = snaps.iter().map(|s| s.ts_ms).collect();
    let mut arrays: Vec<ArrayRef> = vec![Arc::new(ts)];

    for i in 0..DEPTH {
        let px: Float32Array = snaps
            .iter()
            .map(|s| {
                let v = s.bid_px[i];
                if v.is_nan() {
                    None
                } else {
                    Some(v)
                }
            })
            .collect();
        let sz: Float32Array = snaps
            .iter()
            .map(|s| {
                let v = s.bid_sz[i];
                if v.is_nan() {
                    None
                } else {
                    Some(v)
                }
            })
            .collect();
        arrays.push(Arc::new(px));
        arrays.push(Arc::new(sz));
    }
    for i in 0..DEPTH {
        let px: Float32Array = snaps
            .iter()
            .map(|s| {
                let v = s.ask_px[i];
                if v.is_nan() {
                    None
                } else {
                    Some(v)
                }
            })
            .collect();
        let sz: Float32Array = snaps
            .iter()
            .map(|s| {
                let v = s.ask_sz[i];
                if v.is_nan() {
                    None
                } else {
                    Some(v)
                }
            })
            .collect();
        arrays.push(Arc::new(px));
        arrays.push(Arc::new(sz));
    }

    RecordBatch::try_new(schema.clone(), arrays).map_err(Into::into)
}

struct SnapshotBatchWriter {
    path: PathBuf,
    tmp: PathBuf,
    writer: Option<ArrowWriter<std::fs::File>>,
    schema: Arc<Schema>,
    pending: Vec<Snapshot>,
    batch_size: usize,
    rows_written: usize,
}

impl SnapshotBatchWriter {
    fn new(path: &Path, schema: &Arc<Schema>, batch_size: usize) -> Result<Self> {
        let tmp = path.with_extension("tmp");
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let file = std::fs::File::create(&tmp)?;
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;

        Ok(Self {
            path: path.to_path_buf(),
            tmp,
            writer: Some(writer),
            schema: schema.clone(),
            pending: Vec::with_capacity(batch_size.max(1)),
            batch_size: batch_size.max(1),
            rows_written: 0,
        })
    }

    fn push(&mut self, snapshot: Snapshot) -> Result<()> {
        self.pending.push(snapshot);
        if self.pending.len() >= self.batch_size {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }

        let batch = build_record_batch(&self.pending, &self.schema)?;
        self.writer
            .as_mut()
            .expect("writer should exist before finish")
            .write(&batch)?;
        self.rows_written += self.pending.len();
        self.pending.clear();
        Ok(())
    }

    fn finish(&mut self) -> Result<usize> {
        self.flush()?;
        self.writer
            .take()
            .expect("writer should exist before finish")
            .close()?;
        std::fs::rename(&self.tmp, &self.path)?;
        Ok(self.rows_written)
    }

    fn cleanup(&self) {
        let _ = std::fs::remove_file(&self.tmp);
    }
}

fn process_archive_to_parquet_with_batch_size(
    raw: &Path,
    out: &Path,
    schema: &Arc<Schema>,
    batch_size: usize,
) -> Result<usize> {
    let mut writer = SnapshotBatchWriter::new(out, schema, batch_size)?;
    let result = process_archive_entries(raw, |snapshot| writer.push(snapshot)).and_then(|_| {
        let rows = writer.finish()?;
        if rows == 0 {
            anyhow::bail!("no snapshots produced");
        }
        Ok(rows)
    });

    if result.is_err() {
        writer.cleanup();
    }

    result
}

fn validate_parquet(path: &Path, expected_rows: usize) -> Result<()> {
    let file = std::fs::File::open(path)?;
    let reader = SerializedFileReader::new(file)?;
    let actual = reader.metadata().file_metadata().num_rows() as usize;

    if actual == 0 {
        anyhow::bail!("parquet 行数为 0");
    }
    // 容忍 ±5%（day boundary 等边界情况）
    let lo = (expected_rows as f64 * 0.95) as usize;
    let hi = (expected_rows as f64 * 1.05) as usize;
    if actual < lo || actual > hi {
        anyhow::bail!("行数异常：期望 ~{expected_rows}，实际 {actual}");
    }
    Ok(())
}

#[cfg(test)]
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ProcessTask {
    pub symbol: String,
    pub date: NaiveDate,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
#[allow(dead_code)]
pub enum RawRetention {
    Keep,
    Delete,
}

#[cfg(test)]
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum ProcessResult {
    Skipped,
    Success,
    Failed { reason: String },
}

#[derive(Debug, Clone)]
pub enum ProcessStageResult {
    Success { rows: usize },
    Failed { reason: String },
}

#[cfg(test)]
#[allow(dead_code)]
pub fn should_process_day(state: &DayState, raw_exists: bool, parquet_exists: bool) -> bool {
    state.download == DownloadState::Success
        && raw_exists
        && !(state.process == ProcessState::Success && parquet_exists)
}

#[cfg(test)]
#[allow(dead_code)]
fn persist_process_failure(task: &ProcessTask, reason: &str) {
    let persisted = mutate_day(&task.symbol, task.date, |state| {
        state.process = ProcessState::Failed;
        state.process_attempts += 1;
        state.last_error = Some(reason.to_string());
        Ok(())
    });

    if let Err(err) = persisted {
        tracing::warn!(
            "写入处理失败 ledger 失败 {} {}: {err}",
            task.symbol,
            task.date
        );
    }
}

#[cfg(test)]
#[allow(dead_code)]
fn persist_process_success(task: &ProcessTask, rows: usize, raw_deleted: bool) {
    let persisted = mutate_day(&task.symbol, task.date, |state| {
        state.process = ProcessState::Success;
        state.process_attempts += 1;
        state.rows = Some(rows);
        state.raw_deleted = raw_deleted;
        state.last_error = None;
        Ok(())
    });

    if let Err(err) = persisted {
        tracing::warn!(
            "写入处理成功 ledger 失败 {} {}: {err}",
            task.symbol,
            task.date
        );
    }
}

#[cfg(test)]
#[allow(dead_code)]
pub fn collect_process_tasks(
    symbols: &[String],
    start: NaiveDate,
    end: NaiveDate,
) -> Vec<ProcessTask> {
    let mut tasks = Vec::new();

    for symbol in symbols {
        for d in date_range(start, end) {
            let state = load_day(symbol, d);
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

#[cfg(test)]
#[allow(dead_code)]
pub fn process_day_task(task: &ProcessTask, raw_retention: RawRetention) -> ProcessResult {
    let raw = raw_path(&task.symbol, task.date);
    let out = parquet_path(&task.symbol, task.date);
    let schema = make_schema();
    let state = load_day(&task.symbol, task.date);

    if state.download == DownloadState::NotAvailable {
        return ProcessResult::Skipped;
    }

    if state.can_skip_process(out.exists()) {
        return ProcessResult::Skipped;
    }

    if !raw.exists() {
        persist_process_failure(task, "raw file missing");
        return ProcessResult::Failed {
            reason: "raw file missing".to_string(),
        };
    }

    let rows = match process_archive_to_parquet_with_batch_size(
        &raw,
        &out,
        &schema,
        SNAPSHOT_BATCH_SIZE,
    ) {
        Ok(rows) => rows,
        Err(err) => {
            let reason = err.to_string();
            persist_process_failure(task, &reason);
            return ProcessResult::Failed {
                reason,
            };
        }
    };

    if let Err(err) = validate_parquet(&out, rows) {
        let reason = err.to_string();
        let _ = std::fs::remove_file(&out);
        persist_process_failure(task, &reason);
        return ProcessResult::Failed {
            reason,
        };
    }

    let raw_deleted = match raw_retention {
        RawRetention::Keep => false,
        RawRetention::Delete => std::fs::remove_file(&raw).is_ok(),
    };
    persist_process_success(task, rows, raw_deleted);

    ProcessResult::Success
}

pub fn process_stage(task: &Task, paths: &Paths) -> ProcessStageResult {
    let raw = paths.raw_path(&task.symbol, task.date);
    let out = paths.parquet_path(&task.symbol, task.date);
    let schema = make_schema();

    if !raw.exists() {
        return ProcessStageResult::Failed {
            reason: "raw file missing".to_string(),
        };
    }

    let rows = match process_archive_to_parquet_with_batch_size(&raw, &out, &schema, SNAPSHOT_BATCH_SIZE)
    {
        Ok(rows) => rows,
        Err(err) => {
            return ProcessStageResult::Failed {
                reason: err.to_string(),
            }
        }
    };

    if let Err(err) = validate_parquet(&out, rows) {
        let _ = std::fs::remove_file(&out);
        return ProcessStageResult::Failed {
            reason: err.to_string(),
        };
    }

    ProcessStageResult::Success { rows }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use std::io::Cursor;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tar::Builder;

    fn unique_symbol(prefix: &str) -> String {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        format!("{prefix}-{nanos}")
    }

    fn write_archive(raw: &Path, entries: &[(&str, &str)]) {
        if let Some(parent) = raw.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }

        let file = std::fs::File::create(raw).unwrap();
        let encoder = GzEncoder::new(file, Compression::default());
        let mut tar = Builder::new(encoder);

        for (name, body) in entries {
            let mut header = tar::Header::new_gnu();
            header.set_size(body.len() as u64);
            header.set_mode(0o644);
            header.set_cksum();
            tar.append_data(&mut header, *name, body.as_bytes()).unwrap();
        }

        tar.finish().unwrap();
    }

    fn cleanup_symbol(symbol: &str) {
        let _ = std::fs::remove_dir_all(crate::raw_dir().join(symbol));
        let _ = std::fs::remove_dir_all(crate::parquet_dir().join(symbol));
        let _ = std::fs::remove_dir_all(crate::ledger_dir().join(symbol));
    }

    #[test]
    fn process_json_lines_requires_snapshot_before_update() {
        let input = Cursor::new(
            "{\"action\":\"update\",\"ts\":\"1000\",\"bids\":[[\"100\",\"1\"]],\"asks\":[]}\n",
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

    #[test]
    fn process_day_archive_reads_all_regular_entries() {
        let symbol = unique_symbol("multi-entry");
        let d = NaiveDate::from_ymd_opt(2024, 1, 5).unwrap();
        let raw = crate::raw_path(&symbol, d);

        write_archive(
            &raw,
            &[
                (
                    "part-1.json",
                    "{\"action\":\"snapshot\",\"ts\":\"1000\",\"bids\":[[\"100\",\"1\"]],\"asks\":[[\"101\",\"2\"]]}\n",
                ),
                (
                    "part-2.json",
                    "{\"action\":\"update\",\"ts\":\"1100\",\"bids\":[[\"100\",\"3\"]],\"asks\":[]}\n",
                ),
            ],
        );

        let snaps = process_day_archive(&raw).unwrap();

        assert_eq!(snaps.len(), 1);
        assert_eq!(snaps[0].bid_sz[0], 3.0);
        cleanup_symbol(&symbol);
    }

    #[test]
    fn process_day_archive_writes_expected_rows_with_small_batch_size() {
        let symbol = unique_symbol("chunked-parquet");
        let d = NaiveDate::from_ymd_opt(2024, 1, 8).unwrap();
        let raw = crate::raw_path(&symbol, d);
        let out = crate::parquet_path(&symbol, d);
        let schema = make_schema();

        write_archive(
            &raw,
            &[(
                "day.json",
                concat!(
                    "{\"action\":\"snapshot\",\"ts\":\"1000\",\"bids\":[[\"100\",\"1\"]],\"asks\":[[\"101\",\"2\"]]}\n",
                    "{\"action\":\"update\",\"ts\":\"1100\",\"bids\":[[\"100\",\"2\"]],\"asks\":[]}\n",
                    "{\"action\":\"update\",\"ts\":\"1200\",\"bids\":[[\"100\",\"3\"]],\"asks\":[]}\n"
                ),
            )],
        );

        let rows = process_archive_to_parquet_with_batch_size(&raw, &out, &schema, 1).unwrap();

        assert_eq!(rows, 2);
        validate_parquet(&out, rows).unwrap();
        cleanup_symbol(&symbol);
    }

    #[test]
    fn processing_fails_on_timestamp_regression() {
        let symbol = unique_symbol("timestamp-regression");
        let d = NaiveDate::from_ymd_opt(2024, 1, 9).unwrap();
        let raw = crate::raw_path(&symbol, d);
        let out = crate::parquet_path(&symbol, d);
        let schema = make_schema();

        write_archive(
            &raw,
            &[(
                "day.json",
                concat!(
                    "{\"action\":\"snapshot\",\"ts\":\"1000\",\"bids\":[[\"100\",\"1\"]],\"asks\":[[\"101\",\"2\"]]}\n",
                    "{\"action\":\"update\",\"ts\":\"1100\",\"bids\":[[\"100\",\"2\"]],\"asks\":[]}\n",
                    "{\"action\":\"update\",\"ts\":\"1099\",\"bids\":[[\"100\",\"3\"]],\"asks\":[]}\n"
                ),
            )],
        );

        let err = process_archive_to_parquet_with_batch_size(&raw, &out, &schema, 2).unwrap_err();

        assert!(err.to_string().contains("1099"));
        cleanup_symbol(&symbol);
    }
}

#[cfg(test)]
mod task_tests {
    use super::*;
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use crate::ledger::{load_day, save_day, DayState, DownloadState, ProcessState};
    use tar::Builder;

    fn write_archive(raw: &Path, entries: &[(&str, &str)]) {
        if let Some(parent) = raw.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }

        let file = std::fs::File::create(raw).unwrap();
        let encoder = GzEncoder::new(file, Compression::default());
        let mut tar = Builder::new(encoder);

        for (name, body) in entries {
            let mut header = tar::Header::new_gnu();
            header.set_size(body.len() as u64);
            header.set_mode(0o644);
            header.set_cksum();
            tar.append_data(&mut header, *name, body.as_bytes()).unwrap();
        }

        tar.finish().unwrap();
    }

    fn cleanup_symbol(symbol: &str) {
        let _ = std::fs::remove_dir_all(crate::raw_dir().join(symbol));
        let _ = std::fs::remove_dir_all(crate::parquet_dir().join(symbol));
        let _ = std::fs::remove_dir_all(crate::ledger_dir().join(symbol));
    }

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

    #[test]
    fn process_day_task_keeps_raw_when_retention_is_keep() {
        let symbol = format!("keep-{}", chrono::Utc::now().timestamp_nanos_opt().unwrap());
        let d = NaiveDate::from_ymd_opt(2024, 1, 6).unwrap();
        let raw = crate::raw_path(&symbol, d);
        let task = ProcessTask {
            symbol: symbol.clone(),
            date: d,
        };

        write_archive(
            &raw,
            &[(
                "day.json",
                concat!(
                    "{\"action\":\"snapshot\",\"ts\":\"1000\",\"bids\":[[\"100\",\"1\"]],\"asks\":[[\"101\",\"2\"]]}\n",
                    "{\"action\":\"update\",\"ts\":\"1100\",\"bids\":[[\"100\",\"3\"]],\"asks\":[]}\n"
                ),
            )],
        );
        save_day(
            &symbol,
            d,
            &DayState {
                download: DownloadState::Success,
                ..Default::default()
            },
        )
        .unwrap();

        let result = process_day_task(&task, RawRetention::Keep);

        assert!(matches!(result, ProcessResult::Success));
        assert!(raw.exists());
        let state = load_day(&symbol, d);
        assert!(!state.raw_deleted);
        assert!(state.raw_present);
        cleanup_symbol(&symbol);
    }

    #[test]
    fn process_day_task_deletes_raw_when_retention_is_delete() {
        let symbol = format!("delete-{}", chrono::Utc::now().timestamp_nanos_opt().unwrap());
        let d = NaiveDate::from_ymd_opt(2024, 1, 7).unwrap();
        let raw = crate::raw_path(&symbol, d);
        let task = ProcessTask {
            symbol: symbol.clone(),
            date: d,
        };

        write_archive(
            &raw,
            &[(
                "day.json",
                concat!(
                    "{\"action\":\"snapshot\",\"ts\":\"1000\",\"bids\":[[\"100\",\"1\"]],\"asks\":[[\"101\",\"2\"]]}\n",
                    "{\"action\":\"update\",\"ts\":\"1100\",\"bids\":[[\"100\",\"3\"]],\"asks\":[]}\n"
                ),
            )],
        );
        save_day(
            &symbol,
            d,
            &DayState {
                download: DownloadState::Success,
                ..Default::default()
            },
        )
        .unwrap();

        let result = process_day_task(&task, RawRetention::Delete);

        assert!(matches!(result, ProcessResult::Success));
        assert!(!raw.exists());
        let state = load_day(&symbol, d);
        assert!(state.raw_deleted);
        assert!(!state.raw_present);
        cleanup_symbol(&symbol);
    }
}
