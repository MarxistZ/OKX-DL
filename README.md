# OKX L2 Orderbook Downloader

这是一个 OKX 历史 L2 orderbook 数据流水线，用于下载 OKX 公共归档文件，重建 20 档盘口，并输出 Parquet。

## 环境要求

- Linux VPS
- Rust stable toolchain
- 足够磁盘空间。原始压缩包会临时落盘，处理成功后删除；Parquet 会长期保留。
- VPS 可以访问 `https://static.okx.com`

安装 Rust：

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source "$HOME/.cargo/env"
```

## 构建

在项目根目录执行：

```bash
cargo build --release
```

生成的二进制在：

```bash
target/release/okx-lob
target/release/okx-delta
```

## VPS 烟雾测试

先跑一小段数据，确认 VPS 网络、编译、目录权限和处理链路正常：

```bash
scripts/lob_smoke.sh
```

默认会：

- 自动 `cargo build --release`
- 下载并处理 `BTC-USDT-SWAP`
- 日期为 `2024-07-01`
- 使用低并发参数
- 写日志到 `logs/pipeline-smoke-*.log`

## 正式运行

正式任务必须显式指定 `START` 和 `END`：

```bash
START=2024-07-01 END=2024-07-31 scripts/lob_run_range.sh
```

指定多个币种：

```bash
START=2024-07-01 \
END=2024-07-31 \
SYMBOLS="BTC-USDT-SWAP ETH-USDT-SWAP" \
scripts/lob_run_range.sh
```

常用参数：

```bash
START=2024-07-01 \
END=2024-07-31 \
SYMBOLS="BTC-USDT-SWAP ETH-USDT-SWAP" \
WORKERS=4 \
DL_CONCURRENCY=2 \
DL_RETRIES=5 \
RAW_MAX_GB=70 \
scripts/lob_run_range.sh
```

如需让脚本先构建再运行：

```bash
BUILD=1 START=2024-07-01 END=2024-07-31 scripts/lob_run_range.sh
```

## 输出目录

默认目录：

- 原始下载文件：`data/raw/<symbol>/<date>.tar.gz`
- Parquet 输出：`data/parquet/<symbol>/<date>.parquet`
- 任务状态账本：`data/ledger/<symbol>/<date>.json`
- 运行日志：`logs/*.log`

处理成功后，原始 `tar.gz` 会被删除，只保留 Parquet 和账本。

可自定义数据目录：

```bash
START=2024-07-01 \
END=2024-07-31 \
RAW_ROOT=/data/okx/raw \
PARQUET_ROOT=/data/okx/parquet \
LOG_DIR=/data/okx/logs \
scripts/lob_run_range.sh
```

注意：账本当前固定写入项目目录下的 `data/ledger`，请从项目根目录运行脚本。

## 失败语义

- 下载或处理失败超过重试次数后，程序返回非零退出码。
- 旧日期返回 404 会记为 `not_available`，不算失败。
- 最近日期返回 404 会按失败重试处理，避免 OKX 文件尚未发布时被永久跳过。
- JSON 坏行比例超过阈值时，该日处理失败。

## 直接运行二进制

也可以绕过脚本直接运行：

```bash
target/release/okx-lob \
  --symbol BTC-USDT-SWAP ETH-USDT-SWAP \
  --start 2024-07-01 \
  --end 2024-07-31 \
  --workers 4 \
  --dl-concurrency 2 \
  --dl-retries 5 \
  --raw-max-gb 70
```

## Delta 传输压缩

最终落盘格式仍是 Parquet。传输时可以把 Parquet 批量编码成 `.okxd.zst`，下载到本地后再批量解回 Parquet：

```bash
scripts/delta_batch_transfer.sh encode data/parquet data/transfer
scripts/delta_batch_transfer.sh decode data/transfer data/parquet-restored
```

常用参数：

```bash
BUILD=1 JOBS=8 VERIFY=1 ZSTD_LEVEL=19 \
scripts/delta_batch_transfer.sh encode data/parquet data/transfer
```

## 测试

```bash
cargo test
bash -n scripts/*.sh
```
