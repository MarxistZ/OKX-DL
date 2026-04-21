use crate::pipeline::{Paths, Task};
use crate::file_url;
use anyhow::Result;
use bytes::Bytes;
use flate2::read::GzDecoder;
use futures::StreamExt;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use reqwest::Client;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::time::sleep;


// ── 下载单文件（含重试 + 指数退避） ──────────────────────────────────────────

fn validate_archive_file(path: &Path) -> Result<()> {
    let file = std::fs::File::open(path)?;
    if file.metadata()?.len() == 0 {
        anyhow::bail!("empty archive");
    }

    let mut gz = GzDecoder::new(file);
    let mut saw_entry = false;

    {
        let mut archive = tar::Archive::new(&mut gz);
        for entry in archive.entries()? {
            let mut entry = entry?;
            saw_entry = true;
            std::io::copy(&mut entry, &mut std::io::sink())?;
        }
    }

    if !saw_entry {
        anyhow::bail!("archive contains no entries");
    }

    std::io::copy(&mut gz, &mut std::io::sink())?;

    Ok(())
}

/// 返回 Ok(true)=成功，Ok(false)=404，Err=不可恢复错误
async fn download_file(
    client: &Client,
    url: &str,
    out: &std::path::Path,
    pbar: &ProgressBar,
    retries: u32,
) -> Result<bool> {
    let tmp = out.with_extension("tmp");

    for attempt in 0..=retries {
        if attempt > 0 {
            let wait = Duration::from_secs(2u64.pow(attempt - 1).min(30));
            pbar.set_message(format!("等待重试 {attempt}/{retries} ({wait:?})..."));
            sleep(wait).await;
        }

        let resp = match client.get(url).send().await {
            Ok(r) => r,
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
        if let Some(len) = total {
            pbar.set_length(len);
        }

        // 流式写入 .tmp
        let mut file = tokio::fs::File::create(&tmp).await?;
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

        let validate_path = tmp.clone();
        match tokio::task::spawn_blocking(move || validate_archive_file(&validate_path)).await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                let _ = tokio::fs::remove_file(&tmp).await;
                tracing::warn!("归档校验失败 (attempt {attempt}): {err}");
                continue;
            }
            Err(err) => {
                let _ = tokio::fs::remove_file(&tmp).await;
                tracing::warn!("归档校验任务失败 (attempt {attempt}): {err}");
                continue;
            }
        }

        // 原子 rename
        tokio::fs::rename(&tmp, out).await?;
        return Ok(true);
    }

    // 所有重试耗尽
    let _ = tokio::fs::remove_file(&tmp).await;
    Err(anyhow::anyhow!("下载失败，已重试 {retries} 次（共 {} 次尝试）", retries + 1))
}

#[derive(Debug, Clone)]
pub enum DownloadStageResult {
    Success,
    NotAvailable,
    Failed { reason: String },
}

pub async fn download_stage(
    client: Arc<Client>,
    task: &Task,
    paths: &Paths,
    mp: &MultiProgress,
    retries: u32,
) -> DownloadStageResult {
    let out = paths.raw_path(&task.symbol, task.date);

    if let Some(parent) = out.parent() {
        let _ = tokio::fs::create_dir_all(parent).await;
    }

    let url = file_url(&task.symbol, task.date);
    let label = format!("{} {}", task.symbol, task.date);
    let pbar = mp.add(ProgressBar::new(0));
    pbar.set_style(
        ProgressStyle::default_bar()
            .template("  {msg:25} [{bar:25}] {bytes:>9}/{total_bytes:>9} {bytes_per_sec:>10}")
            .unwrap()
            .progress_chars("=>-"),
    );
    pbar.set_message(label);

    let result = match download_file(&client, &url, &out, &pbar, retries).await {
        Ok(true) => DownloadStageResult::Success,
        Ok(false) => DownloadStageResult::NotAvailable,
        Err(err) => DownloadStageResult::Failed {
            reason: err.to_string(),
        },
    };

    pbar.finish_and_clear();
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use std::io::Write;
    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};
    use tar::Builder;

    fn unique_path(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("okx-lob-{name}-{nanos}.tar.gz"))
    }

    fn write_valid_archive(path: &Path) {
        let file = std::fs::File::create(path).unwrap();
        let encoder = GzEncoder::new(file, Compression::default());
        let mut tar = Builder::new(encoder);
        let body = b"{\"action\":\"snapshot\",\"ts\":\"1000\",\"bids\":[[\"100\",\"1\"]],\"asks\":[[\"101\",\"2\"]]}\n";
        let mut header = tar::Header::new_gnu();
        header.set_size(body.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        tar.append_data(&mut header, "day.json", &body[..]).unwrap();
        tar.finish().unwrap();
    }

    #[test]
    fn download_validation_rejects_empty_file() {
        let path = unique_path("empty");
        std::fs::write(&path, []).unwrap();

        let err = validate_archive_file(&path).unwrap_err();

        assert!(err.to_string().contains("empty"));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn download_validation_rejects_truncated_gzip() {
        let path = unique_path("truncated");
        write_valid_archive(&path);
        let mut bytes = std::fs::read(&path).unwrap();
        bytes.truncate(bytes.len().saturating_sub(8));
        std::fs::write(&path, bytes).unwrap();

        assert!(validate_archive_file(&path).is_err());
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn download_validation_rejects_invalid_tar_payload() {
        let path = unique_path("invalid-tar");
        let file = std::fs::File::create(&path).unwrap();
        let mut encoder = GzEncoder::new(file, Compression::default());
        encoder.write_all(b"not a tar archive").unwrap();
        encoder.finish().unwrap();

        assert!(validate_archive_file(&path).is_err());
        let _ = std::fs::remove_file(path);
    }
}
