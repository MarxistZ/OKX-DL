#!/usr/bin/env bash
set -euo pipefail

while true; do
  rclone copy "$HOME/data/okx/transfer" gdrive:okx/transfer \
    --progress \
    --transfers 8 \
    --checkers 16 \
    --fast-list \
    --drive-chunk-size 128M \
    --retries 10 \
    --low-level-retries 20 \
    --stats 30s

  sleep 60
done
