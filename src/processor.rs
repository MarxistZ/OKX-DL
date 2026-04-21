use crate::ledger::{load_day, mutate_day, DayState, DownloadState, ProcessState};
use crate::lob::{Lob, OkxRecord, Snapshot};
use crate::{date_range, parquet_path, raw_path, DEPTH, SAMPLE_MS};
use anyhow::Result;
use arrow::array::{ArrayRef, Float32Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::NaiveDate;
use flate2::read::GzDecoder;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::sync::Arc;

// ── 单日 LOB 重建 ────────────────────────────────────────────────────────────

fn process_json_lines<R: BufRead>(reader: R) -> Result<Vec<Snapshot>> {
    let mut lob = Lob::new();
    let mut snaps = Vec::with_capacity(900_000);
    let mut next_sample_ms: Option<i64> = None;
    let mut saw_snapshot = false;
    let mut bad_lines = 0usize;
    let mut total_lines = 0usize;

    for line in reader.lines() {
        let line = match line {
            Ok(line) if !line.is_empty() => line,
            _ => continue,
        };
        total_lines += 1;

        let record: OkxRecord = match serde_json::from_str(&line) {
            Ok(record) => record,
            Err(_) => {
                bad_lines += 1;
                continue;
            }
        };

        match record.action.as_str() {
            "snapshot" => {
                lob.apply(&record);
                saw_snapshot = lob.ready;
            }
            "update" if !saw_snapshot => anyhow::bail!("first valid record must be snapshot"),
            "update" => lob.apply(&record),
            _ => continue,
        }

        if !lob.ready {
            continue;
        }

        let ts = lob.ts_ms;
        let next = next_sample_ms.get_or_insert_with(|| (ts / SAMPLE_MS + 1) * SAMPLE_MS);
        while ts >= *next {
            snaps.push(lob.snapshot(*next));
            *next += SAMPLE_MS;
        }
    }

    if !saw_snapshot {
        anyhow::bail!("no snapshot found in daily file");
    }

    if bad_lines > 0 && total_lines > 0 {
        let pct = bad_lines as f64 / total_lines as f64 * 100.0;
        if pct > 1.0 {
            tracing::warn!("坏行 {bad_lines}/{total_lines} ({pct:.1}%)");
        }
    }

    Ok(snaps)
}

pub fn process_day_archive(raw: &Path) -> Result<Vec<Snapshot>> {
    let file = std::fs::File::open(raw)?;
    let gz = GzDecoder::new(file);
    let mut ar = tar::Archive::new(gz);

    let mut entries = ar.entries()?;
    let entry = entries
        .next()
        .ok_or_else(|| anyhow::anyhow!("tar 为空"))??;

    let reader = BufReader::with_capacity(4 * 1024 * 1024, entry);
    process_json_lines(reader)
}

// ── Parquet 写入 + 验证 ───────────────────────────────────────────────────────

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

fn write_parquet(path: &Path, snaps: &[Snapshot], schema: &Arc<Schema>) -> Result<()> {
    let tmp = path.with_extension("tmp");

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

    let batch = RecordBatch::try_new(schema.clone(), arrays)?;
    let file = std::fs::File::create(&tmp)?;
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;
    writer.write(&batch)?;
    writer.close()?;

    // 原子 rename
    std::fs::rename(&tmp, path)?;
    Ok(())
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

#[derive(Debug, Clone)]
pub struct ProcessTask {
    pub symbol: String,
    pub date: NaiveDate,
}

#[derive(Debug, Clone)]
pub enum ProcessResult {
    Skipped,
    Success,
    Failed { reason: String },
}

pub fn should_process_day(state: &DayState, raw_exists: bool, parquet_exists: bool) -> bool {
    state.download == DownloadState::Success
        && raw_exists
        && !(state.process == ProcessState::Success && parquet_exists)
}

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

pub fn process_day_task(task: &ProcessTask) -> ProcessResult {
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

    let snaps = match process_day_archive(&raw) {
        Ok(snaps) if !snaps.is_empty() => snaps,
        Ok(_) => {
            persist_process_failure(task, "no snapshots produced");
            return ProcessResult::Failed {
                reason: "no snapshots produced".to_string(),
            };
        }
        Err(err) => {
            let reason = err.to_string();
            persist_process_failure(task, &reason);
            return ProcessResult::Failed {
                reason,
            };
        }
    };

    if let Err(err) =
        write_parquet(&out, &snaps, &schema).and_then(|_| validate_parquet(&out, snaps.len()))
    {
        let reason = err.to_string();
        let _ = std::fs::remove_file(&out);
        persist_process_failure(task, &reason);
        return ProcessResult::Failed {
            reason,
        };
    }

    let raw_deleted = std::fs::remove_file(&raw).is_ok();
    persist_process_success(task, snaps.len(), raw_deleted);

    ProcessResult::Success
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

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
}

#[cfg(test)]
mod task_tests {
    use super::*;
    use crate::ledger::{DayState, DownloadState, ProcessState};

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
}
