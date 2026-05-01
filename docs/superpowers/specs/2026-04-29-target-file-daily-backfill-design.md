# Target File Daily Backfill Design

## Goal

Support a minimal operational loop for OKX LOB補录: one target per line as `inst:tradedate`, run only those exact targets, upload completed output to Google Drive, and clean temporary local artifacts after upload.

## Target Interface

The补录 file is plain text. Each non-empty, non-comment line has this format:

```text
BTC-USDT-SWAP:2024-07-01
ETH-USDT-SWAP:2024-07-03
```

Whitespace around the instrument and date is ignored. Dates must be `YYYY-MM-DD`. Invalid lines fail fast before any download starts.

`okx-lob` gains `--target-file <path>`. In this mode the program runs exactly the listed `(inst, tradedate)` tasks. The existing `--symbol ... --start ... --end ...` range mode remains unchanged.

## Daily Script

Add a daily shell runner that:

1. Generates a target file for a configured symbol list and recent completed dates.
2. Runs `okx-lob --target-file`.
3. Writes a run log and summary CSV.
4. Uploads parquet output with `rclone copy`.
5. Cleans temporary raw files, with optional parquet cleanup after upload.

The script is intended to be called from cron or systemd timer. It does not manage scheduling itself.

## Logs And Summary

The normal run log remains a timestamped text file. A summary CSV is generated from the day-level ledger for the exact target list:

```csv
inst,tradedate,status,rows,error
BTC-USDT-SWAP,2024-07-01,success,123456,
ETH-USDT-SWAP,2024-07-03,not_available,,
```

This keeps monitoring at day granularity without introducing a database or external monitoring service.

## Error Handling

Target parsing errors stop the command before starting the pipeline. Pipeline failures keep the existing semantics: failed tasks are retried, and final failed task count makes the command exit non-zero. The daily script only uploads after a successful pipeline exit.
