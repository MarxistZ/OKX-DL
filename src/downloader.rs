use crate::{date_range, file_url, raw_path};
use crate::ledger::Ledger;
use anyhow::Result;
use bytes::Bytes;
use chrono::NaiveDate;
use futures::StreamExt;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use reqwest::Client;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::sync::Semaphore;
use tokio::time::sleep;

// ── 下载单文件（含重试 + 指数退避） ──────────────────────────────────────────

/// 返回 Ok(true)=成功，Ok(false)=404，Err=不可恢复错误
async fn download_file(
    client:   &Client,
    url:      &str,
    out:      &std::path::Path,
    pbar:     &ProgressBar,
    retries:  u32,
) -> Result<bool> {
    let tmp = out.with_extension("tmp");

    for attempt in 0..=retries {
        if attempt > 0 {
            let wait = Duration::from_secs(2u64.pow(attempt - 1).min(30));
            pbar.set_message(format!("等待重试 {attempt}/{retries} ({wait:?})..."));
            sleep(wait).await;
        }

        let resp = match client.get(url).send().await {
            Ok(r)  => r,
            Err(e) => {
                tracing::warn!("下载失败 (attempt {attempt}): {e}");
                continue;
            }
        };

        if resp.status() == 404 {
            return Ok(false);
        }
        if !resp.status().is_success() {
            tracing::warn!("HTTP {} (attempt {attempt})", resp.status());
            continue;
        }

        let total = resp.content_length();
        if let Some(len) = total { pbar.set_length(len); }

        // 流式写入 .tmp
        let mut file   = tokio::fs::File::create(&tmp).await?;
        let mut stream = resp.bytes_stream();
        let mut written = 0u64;
        let mut ok = true;

        while let Some(chunk_result) = stream.next().await {
            match chunk_result {
                Ok(chunk) => {
                    let chunk: Bytes = chunk;
                    if let Err(e) = file.write_all(&chunk).await {
                        tracing::warn!("写入错误 (attempt {attempt}): {e}");
                        ok = false;
                        break;
                    }
                    written += chunk.len() as u64;
                    pbar.inc(chunk.len() as u64);
                }
                Err(e) => {
                    tracing::warn!("流中断 (attempt {attempt}): {e}");
                    ok = false;
                    break;
                }
            }
        }

        if !ok {
            let _ = tokio::fs::remove_file(&tmp).await;
            continue;
        }

        file.flush().await?;
        drop(file);

        // 校验：文件大小不能为 0，且如果有 Content-Length 要匹配
        if written == 0 {
            let _ = tokio::fs::remove_file(&tmp).await;
            tracing::warn!("空文件 (attempt {attempt})");
            continue;
        }
        if let Some(expected) = total {
            if written != expected {
                let _ = tokio::fs::remove_file(&tmp).await;
                tracing::warn!("大小不匹配：期望 {expected}，实际 {written} (attempt {attempt})");
                continue;
            }
        }

        // 原子 rename
        tokio::fs::rename(&tmp, out).await?;
        return Ok(true);
    }

    // 所有重试耗尽
    let _ = tokio::fs::remove_file(&tmp).await;
    Err(anyhow::anyhow!("下载失败，已重试 {retries} 次"))
}

// ── 下载单个 (symbol, date) 任务 ─────────────────────────────────────────────

async fn download_one(
    client:  Arc<Client>,
    symbol:  String,
    d:       NaiveDate,
    sem:     Arc<Semaphore>,
    total:   ProgressBar,
    mp:      MultiProgress,
    retries: u32,
) -> (String, NaiveDate, DownloadResult) {
    let out = raw_path(&symbol, d);

    // 已存在直接跳过
    if out.exists() {
        total.inc(1);
        return (symbol, d, DownloadResult::AlreadyExists);
    }

    if let Some(parent) = out.parent() {
        let _ = tokio::fs::create_dir_all(parent).await;
    }

    let url   = file_url(&symbol, d);
    let label = format!("{symbol} {d}");

    let pbar = mp.add(ProgressBar::new(0));
    pbar.set_style(
        ProgressStyle::default_bar()
            .template("  {msg:25} [{bar:25}] {bytes:>9}/{total_bytes:>9} {bytes_per_sec:>10}")
            .unwrap()
            .progress_chars("=>-"),
    );
    pbar.set_message(label.clone());

    let _permit = sem.acquire().await.unwrap();

    let result = match download_file(&client, &url, &out, &pbar, retries).await {
        Ok(true) => {
            let mb = out.metadata().map(|m| m.len() as f64 / 1e6).unwrap_or(0.0);
            total.println(format!("  ✓ {label}  {mb:.1} MB"));
            DownloadResult::Success
        }
        Ok(false) => {
            total.println(format!("  - {label}  (404)"));
            DownloadResult::NotAvailable
        }
        Err(e) => {
            total.println(format!("  ✗ {label}  {e}"));
            DownloadResult::Failed(e.to_string())
        }
    };

    pbar.finish_and_clear();
    total.inc(1);
    (symbol, d, result)
}

pub enum DownloadResult {
    AlreadyExists,
    Success,
    NotAvailable,
    Failed(String),
}

// ── 批量下载入口 ──────────────────────────────────────────────────────────────

pub async fn download_all(
    symbols:      &[String],
    start:        NaiveDate,
    end:          NaiveDate,
    mp:           &MultiProgress,
    concurrency:  usize,
    retries:      u32,
) -> Result<()> {
    let pairs: Vec<(String, NaiveDate)> = symbols
        .iter()
        .flat_map(|s| date_range(start, end).into_iter().map(|d| (s.clone(), d)))
        .collect();

    let client = Arc::new(
        Client::builder()
            .user_agent("Mozilla/5.0")
            .timeout(Duration::from_secs(120))
            .build()?,
    );
    let sem = Arc::new(Semaphore::new(concurrency));

    let total_bar = mp.add(ProgressBar::new(pairs.len() as u64));
    total_bar.set_style(
        ProgressStyle::default_bar()
            .template("  总进度 [{bar:40}] {pos:>5}/{len} 文件  已用 {elapsed}  ETA {eta}")
            .unwrap()
            .progress_chars("=>-"),
    );

    let mut handles = Vec::with_capacity(pairs.len());
    for (symbol, d) in pairs {
        let h = tokio::spawn(download_one(
            Arc::clone(&client),
            symbol,
            d,
            Arc::clone(&sem),
            total_bar.clone(),
            mp.clone(),
            retries,
        ));
        handles.push(h);
    }

    let mut ok = 0usize;
    let mut not_avail = 0usize;
    let mut failed = 0usize;
    let mut skipped = 0usize;

    for h in handles {
        if let Ok((symbol, d, result)) = h.await {
            // 更新 ledger（在同步上下文中需要用 spawn_blocking，
            // 但 ledger 很轻量，直接在 async 中操作也可接受）
            match result {
                DownloadResult::Success => {
                    ok += 1;
                    let mut ledger = Ledger::load(&symbol);
                    ledger.mark_downloaded(d);
                    ledger.save();
                }
                DownloadResult::NotAvailable => {
                    not_avail += 1;
                    let mut ledger = Ledger::load(&symbol);
                    ledger.mark_not_available(d);
                    ledger.save();
                }
                DownloadResult::Failed(_) => {
                    failed += 1;
                    let mut ledger = Ledger::load(&symbol);
                    ledger.inc_attempt(d);
                    ledger.save();
                }
                DownloadResult::AlreadyExists => {
                    skipped += 1;
                }
            }
        }
    }

    total_bar.finish_with_message("下载完成");
    tracing::info!(
        "下载结果：成功 {ok}，不存在 {not_avail}，跳过 {skipped}，失败 {failed}"
    );
    Ok(())
}
