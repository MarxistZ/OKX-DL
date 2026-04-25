#!/usr/bin/env bash
set -euo pipefail

while true; do
  rclone copy /mnt/HC_Volume_105514197/okx/parquet gdrive:okx/parquet --progress --transfers 4 --checkers 8
  sleep 600
done
