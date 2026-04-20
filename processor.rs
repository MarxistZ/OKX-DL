use crate::lob::{Lob, OkxRecord, Snapshot};
use crate::ledger::Ledger;
use crate::{checkpoint_path, date_range, parquet_path, raw_path, DEPTH, SAMPLE_MS};
use anyhow::Result;
use arrow::array::{ArrayRef, Float32Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::NaiveDate;
use flate2::read::GzDecoder;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::sync::Arc;

// ── 单日 LOB 重建 ────────────────────────────────────────────────────────────

/// 从 raw tar.gz 重建 LOB，采样每 100ms 快照。
/// lob 状态跨调用保持（跨日连续）。
/// 返回采样结果（可能为空），或解析错误。
pub fn process_day_inner(
    raw: &Path,
    lob: &mut Lob,
) -> Result<Vec<Snapshot>> {
    let file  = std::fs::File::open(raw)?;
    let gz    = GzDecoder::new(file);
    let mut ar = tar::Archive::new(gz);

    let mut entries = ar.entries()?;
    let entry = entries
        .next()
        .ok_or_else(|| anyhow::anyhow!("tar 为空"))??;

    let reader = BufReader::with_capacity(4 * 1024 * 1024, entry);

    let mut snaps: Vec<Snapshot> = Vec::with_capacity(900_000);
    let mut next_sample_ms: Option<i64> = None;
    let mut bad_lines = 0usize;
    let mut total_lines = 0usize;

    for line in reader.lines() {
        let line = match line {
            Ok(l) if !l.is_empty() => l,
            _ => continue,
        };
        total_lines += 1;

        let record: OkxRecord = match serde_json::from_str(&line) {
            Ok(r)  => r,
            Err(_) => { bad_lines += 1; continue; }
        };

        lob.apply(&record);
        if !lob.ready { continue; }

        let ts  = lob.ts_ms;
        let nxt = next_sample_ms.get_or_insert_with(|| (ts / SAMPLE_MS + 1) * SAMPLE_MS);

        while ts >= *nxt {
            snaps.push(lob.snapshot(*nxt));
            *nxt += SAMPLE_MS;
        }
    }

    if bad_lines > 0 {
        let pct = bad_lines as f64 / total_lines as f64 * 100.0;
        if pct > 1.0 {
            tracing::warn!("  坏行 {bad_lines}/{total_lines} ({pct:.1}%)");
        }
    }

    Ok(snaps)
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
        let px: Float32Array = snaps.iter().map(|s| {
            let v = s.bid_px[i];
            if v.is_nan() { None } else { Some(v) }
        }).collect();
        let sz: Float32Array = snaps.iter().map(|s| {
            let v = s.bid_sz[i];
            if v.is_nan() { None } else { Some(v) }
        }).collect();
        arrays.push(Arc::new(px));
        arrays.push(Arc::new(sz));
    }
    for i in 0..DEPTH {
        let px: Float32Array = snaps.iter().map(|s| {
            let v = s.ask_px[i];
            if v.is_nan() { None } else { Some(v) }
        }).collect();
        let sz: Float32Array = snaps.iter().map(|s| {
            let v = s.ask_sz[i];
            if v.is_nan() { None } else { Some(v) }
        }).collect();
        arrays.push(Arc::new(px));
        arrays.push(Arc::new(sz));
    }

    let batch  = RecordBatch::try_new(schema.clone(), arrays)?;
    let file   = std::fs::File::create(&tmp)?;
    let props  = WriterProperties::builder()
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
    let file   = std::fs::File::open(path)?;
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

// ── 恢复断点 ──────────────────────────────────────────────────────────────────

/// 找到最近可用的 checkpoint，返回 (LOB状态, 应从哪个日期索引开始处理)
fn find_resume_point(
    symbol: &str,
    dates:  &[NaiveDate],
    ledger: &Ledger,
) -> (Lob, usize) {
    // 找到第一个需要处理的天（未 validated 且非 404）
    let first_todo = dates
        .iter()
        .position(|&d| {
            let s = ledger.get(d);
            !s.validated && !s.not_available
        })
        .unwrap_or(dates.len());

    if first_todo == 0 {
        return (Lob::new(), 0);
    }

    // 从 first_todo-1 向前找最近的 checkpoint
    for i in (0..first_todo).rev() {
        let ckpt = checkpoint_path(symbol, dates[i]);
        if ckpt.exists() {
            match Lob::load_checkpoint(&ckpt) {
                Ok(lob) => {
                    tracing::info!(
                        "{symbol}: 从 checkpoint {} 恢复，跳过前 {} 天",
                        dates[i], i + 1
                    );
                    return (lob, first_todo);
                }
                Err(e) => {
                    tracing::warn!("{symbol}: checkpoint {} 损坏: {e}", dates[i]);
                }
            }
        }
    }

    // 没有可用 checkpoint：从头 replay（会经过所有已 validated 的天更新 LOB）
    tracing::info!("{symbol}: 无可用 checkpoint，从第 0 天开始 replay");
    (Lob::new(), 0)
}

// ── 单币种全量处理 ────────────────────────────────────────────────────────────

pub fn process_symbol(symbol: &str, start: NaiveDate, end: NaiveDate, mp: &MultiProgress) {
    let dates   = date_range(start, end);
    let schema  = make_schema();
    let mut ledger = Ledger::load(symbol);

    let (mut lob, start_idx) = find_resume_point(symbol, &dates, &ledger);

    let pbar = mp.add(ProgressBar::new(dates.len() as u64));
    pbar.set_style(
        ProgressStyle::default_bar()
            .template("  {msg:25} [{bar:30}] {pos:>4}/{len}天  ETA {eta}")
            .unwrap()
            .progress_chars("=>-"),
    );
    pbar.set_message(symbol.to_string());
    pbar.inc(start_idx as u64);  // 跳过已恢复的天

    let mut new_rows = 0usize;

    for &d in &dates[start_idx..] {
        pbar.set_message(format!("{symbol} {d}"));
        let state   = ledger.get(d);
        let raw     = raw_path(symbol, d);
        let out     = parquet_path(symbol, d);
        let ckpt    = checkpoint_path(symbol, d);

        // ── Case 1: 已验证 ────────────────────────────────────────────────
        if state.validated {
            // 优先加载 checkpoint（快），否则 replay raw（慢但正确）
            if ckpt.exists() {
                match Lob::load_checkpoint(&ckpt) {
                    Ok(l) => { lob = l; }
                    Err(e) => {
                        pbar.println(format!("  ! {symbol} {d}: checkpoint 损坏 ({e})，replay raw"));
                        if raw.exists() {
                            if let Err(e) = process_day_inner(&raw, &mut lob) {
                                pbar.println(format!("  ! {symbol} {d}: replay 失败: {e}"));
                            }
                        } else {
                            pbar.println(format!("  ! {symbol} {d}: 无 checkpoint 无 raw，LOB 可能有误差"));
                        }
                    }
                }
            } else if raw.exists() {
                // checkpoint 缺失但 raw 存在，replay
                if let Err(e) = process_day_inner(&raw, &mut lob) {
                    pbar.println(format!("  ! {symbol} {d}: replay 失败: {e}"));
                }
                // 顺便补存 checkpoint
                let _ = lob.save_checkpoint(&ckpt);
            } else {
                pbar.println(format!("  ~ {symbol} {d}: 已验证，无 checkpoint/raw，LOB 跳过"));
            }
            pbar.inc(1);
            continue;
        }

        // ── Case 2: 404，该天无数据 ───────────────────────────────────────
        if state.not_available {
            // LOB 状态不变，保存 checkpoint 以维持链条
            if !ckpt.exists() {
                let _ = lob.save_checkpoint(&ckpt);
            }
            pbar.inc(1);
            continue;
        }

        // ── Case 3: raw 不存在（未下载） ─────────────────────────────────
        if !raw.exists() {
            // 保存 checkpoint（LOB 不变）以维持链条
            if !ckpt.exists() {
                let _ = lob.save_checkpoint(&ckpt);
            }
            pbar.inc(1);
            continue;
        }

        // ── Case 4: 正常处理 ──────────────────────────────────────────────
        match process_day_inner(&raw, &mut lob) {
            Err(e) => {
                pbar.println(format!("  ✗ {symbol} {d}: 解析失败: {e}"));
                // 保存 checkpoint（LOB 可能已部分更新，尽力而为）
                let _ = lob.save_checkpoint(&ckpt);
            }
            Ok(snaps) if snaps.is_empty() => {
                pbar.println(format!("  ! {symbol} {d}: 0 行快照（可能无有效数据）"));
                let _ = lob.save_checkpoint(&ckpt);
            }
            Ok(snaps) => {
                // 写 parquet（如果已存在且已验证则跳过）
                let write_result = if out.exists() && state.processed {
                    Ok(())
                } else {
                    write_parquet(&out, &snaps, &schema)
                };

                match write_result {
                    Err(e) => {
                        pbar.println(format!("  ✗ {symbol} {d}: 写 parquet 失败: {e}"));
                        let _ = lob.save_checkpoint(&ckpt);
                    }
                    Ok(()) => {
                        // 验证
                        match validate_parquet(&out, snaps.len()) {
                            Err(e) => {
                                pbar.println(format!("  ✗ {symbol} {d}: 验证失败: {e}"));
                                let _ = std::fs::remove_file(&out);  // 删坏文件
                                let _ = lob.save_checkpoint(&ckpt);
                            }
                            Ok(()) => {
                                ledger.mark_validated(d, snaps.len());
                                new_rows += snaps.len();

                                // 保存 checkpoint
                                if let Err(e) = lob.save_checkpoint(&ckpt) {
                                    pbar.println(format!("  ! {symbol} {d}: checkpoint 保存失败: {e}"));
                                }

                                // 删 raw（验证 + checkpoint 都成功后才删）
                                if ckpt.exists() {
                                    if let Err(e) = std::fs::remove_file(&raw) {
                                        pbar.println(format!("  ! {symbol} {d}: 删 raw 失败: {e}"));
                                    } else {
                                        ledger.mark_raw_deleted(d);
                                    }
                                }

                                let mb = out.metadata()
                                    .map(|m| m.len() as f64 / 1e6)
                                    .unwrap_or(0.0);
                                pbar.println(format!(
                                    "  ✓ {symbol} {d}  {:>8} 行  {mb:.1} MB",
                                    snaps.len()
                                ));
                            }
                        }
                    }
                }
            }
        }

        ledger.save();
        pbar.inc(1);
    }

    pbar.finish_with_message(format!("{symbol} 完成，新增 {new_rows} 行"));
}
