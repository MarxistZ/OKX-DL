#!/usr/bin/env python3
"""
Evaluate temporary delta/int transport encodings for OKX L2 snapshot Parquet.

This script does not replace the on-disk Parquet format. It creates temporary
binary blobs, compresses them with the system zstd command, and reports ratios.

Dependencies:
  python3 -m pip install pyarrow numpy
  zstd must be available on PATH.
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import tempfile
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq


DEPTH = 20
DEFAULT_SCALE = 1_000_000


def sizeof(path: Path) -> int:
    return path.stat().st_size


def mib(n: int) -> float:
    return n / 1024 / 1024


def run_zstd(src: Path, dst: Path, level: int) -> None:
    subprocess.run(
        ["zstd", f"-{level}", "-T0", "-f", str(src), "-o", str(dst)],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def append_json_header(fh, header: dict) -> None:
    raw = json.dumps(header, separators=(",", ":")).encode("utf-8")
    fh.write(len(raw).to_bytes(4, "little"))
    fh.write(raw)


def read_json_header(fh) -> dict:
    raw_len = int.from_bytes(fh.read(4), "little")
    if raw_len <= 0:
        raise ValueError(f"invalid header length: {raw_len}")
    return json.loads(fh.read(raw_len).decode("utf-8"))


def read_exact_array(fh, dtype: str, count: int, label: str) -> np.ndarray:
    dtype_obj = np.dtype(dtype)
    raw = fh.read(dtype_obj.itemsize * count)
    expected = dtype_obj.itemsize * count
    if len(raw) != expected:
        raise ValueError(f"{label} expected {expected} bytes, got {len(raw)}")
    return np.frombuffer(raw, dtype=dtype_obj)


def table_column_f32(table, name: str) -> np.ndarray:
    arr = table[name].combine_chunks()
    out = arr.to_numpy(zero_copy_only=False).astype(np.float32, copy=False)
    return out


def table_column_i64(table, name: str) -> np.ndarray:
    arr = table[name].combine_chunks()
    return arr.to_numpy(zero_copy_only=False).astype(np.int64, copy=False)


def prices_to_i64(table, cols: list[str], scale: int) -> list[np.ndarray]:
    out = []
    for col in cols:
        values = table_column_f32(table, col)
        encoded = np.rint(values.astype(np.float64) * scale).astype(np.int64)
        out.append(encoded)
    return out


def write_i32_checked(fh, values: np.ndarray, label: str) -> None:
    if values.size == 0:
        return
    lo = int(np.nanmin(values))
    hi = int(np.nanmax(values))
    if lo < np.iinfo(np.int32).min or hi > np.iinfo(np.int32).max:
        raise ValueError(f"{label} does not fit i32: min={lo} max={hi}")
    fh.write(values.astype("<i4", copy=False).tobytes(order="C"))


def write_timestamp_deltas(fh, ts: np.ndarray) -> dict:
    first = int(ts[0])
    deltas = np.diff(ts).astype(np.int64, copy=False)
    fh.write(np.array([first], dtype="<i8").tobytes())
    write_i32_checked(fh, deltas, "timestamp deltas")
    unique = np.unique(deltas[: min(len(deltas), 100_000)])
    return {
        "timestamp_first": first,
        "timestamp_delta_sample_unique": unique.astype(int).tolist()[:16],
    }


def read_delta_i64_series(fh, rows: int, label: str) -> np.ndarray:
    first = read_exact_array(fh, "<i8", 1, f"{label} first")[0].astype(np.int64)
    if rows == 1:
        return np.array([first], dtype=np.int64)
    deltas = read_exact_array(fh, "<i4", rows - 1, f"{label} deltas").astype(np.int64)
    out = np.empty(rows, dtype=np.int64)
    out[0] = first
    out[1:] = first + np.cumsum(deltas, dtype=np.int64)
    return out


def write_sizes_raw_f32(fh, table, size_cols: list[str]) -> None:
    for col in size_cols:
        values = table_column_f32(table, col)
        fh.write(values.astype("<f4", copy=False).tobytes(order="C"))


def write_column_time_delta_blob(
    path: Path,
    table,
    ts_col: str,
    price_cols: list[str],
    size_cols: list[str],
    scale: int,
) -> None:
    ts = table_column_i64(table, ts_col)
    encoded_prices = prices_to_i64(table, price_cols, scale)
    header = {
        "format": "column_time_delta_v1",
        "rows": int(table.num_rows),
        "scale": scale,
        "timestamp": ts_col,
        "price_cols": price_cols,
        "size_cols": size_cols,
        "sizes": "raw_f32",
    }

    with path.open("wb") as fh:
        append_json_header(fh, header)
        write_timestamp_deltas(fh, ts)
        for col, values in zip(price_cols, encoded_prices):
            fh.write(np.array([int(values[0])], dtype="<i8").tobytes())
            deltas = np.diff(values).astype(np.int64, copy=False)
            write_i32_checked(fh, deltas, f"{col} price deltas")
        write_sizes_raw_f32(fh, table, size_cols)


def write_base_offset_blob(
    path: Path,
    table,
    ts_col: str,
    bid_px_cols: list[str],
    ask_px_cols: list[str],
    size_cols: list[str],
    scale: int,
) -> None:
    ts = table_column_i64(table, ts_col)
    bid = prices_to_i64(table, bid_px_cols, scale)
    ask = prices_to_i64(table, ask_px_cols, scale)
    bid_matrix = np.column_stack(bid)
    ask_matrix = np.column_stack(ask)

    bid_base = bid_matrix[:, 0]
    ask_base = ask_matrix[:, 0]
    bid_offsets = bid_base[:, None] - bid_matrix
    ask_offsets = ask_matrix - ask_base[:, None]

    header = {
        "format": "base_offset_v1",
        "rows": int(table.num_rows),
        "depth": DEPTH,
        "scale": scale,
        "timestamp": ts_col,
        "bid_px_cols": bid_px_cols,
        "ask_px_cols": ask_px_cols,
        "size_cols": size_cols,
        "sizes": "raw_f32",
        "bid_offsets": "bid_px_0_minus_bid_px_i_i32",
        "ask_offsets": "ask_px_i_minus_ask_px_0_i32",
    }

    with path.open("wb") as fh:
        append_json_header(fh, header)
        write_timestamp_deltas(fh, ts)

        fh.write(np.array([int(bid_base[0])], dtype="<i8").tobytes())
        write_i32_checked(fh, np.diff(bid_base).astype(np.int64, copy=False), "bid base deltas")
        write_i32_checked(fh, bid_offsets.reshape(-1), "bid offsets")

        fh.write(np.array([int(ask_base[0])], dtype="<i8").tobytes())
        write_i32_checked(fh, np.diff(ask_base).astype(np.int64, copy=False), "ask base deltas")
        write_i32_checked(fh, ask_offsets.reshape(-1), "ask offsets")

        write_sizes_raw_f32(fh, table, size_cols)


def decode_base_offset_blob(path: Path) -> pa.Table:
    with path.open("rb") as fh:
        header = read_json_header(fh)
        if header.get("format") != "base_offset_v1":
            raise ValueError(f"unsupported blob format: {header.get('format')}")

        rows = int(header["rows"])
        depth = int(header["depth"])
        scale = float(header["scale"])
        ts_col = header["timestamp"]
        bid_px_cols = header["bid_px_cols"]
        ask_px_cols = header["ask_px_cols"]
        size_cols = header["size_cols"]

        ts = read_delta_i64_series(fh, rows, "timestamp")

        bid_base = read_delta_i64_series(fh, rows, "bid base")
        bid_offsets = read_exact_array(fh, "<i4", rows * depth, "bid offsets").reshape(rows, depth)
        bid_prices = (bid_base[:, None] - bid_offsets.astype(np.int64)).astype(np.float64) / scale

        ask_base = read_delta_i64_series(fh, rows, "ask base")
        ask_offsets = read_exact_array(fh, "<i4", rows * depth, "ask offsets").reshape(rows, depth)
        ask_prices = (ask_base[:, None] + ask_offsets.astype(np.int64)).astype(np.float64) / scale

        sizes = {}
        for col in size_cols:
            sizes[col] = read_exact_array(fh, "<f4", rows, col).astype(np.float32, copy=False)

        trailing = fh.read(1)
        if trailing:
            raise ValueError("blob has trailing unread bytes")

    arrays = [pa.array(ts, type=pa.int64())]
    names = [ts_col]
    for i, col in enumerate(bid_px_cols):
        arrays.append(pa.array(bid_prices[:, i].astype(np.float32), type=pa.float32()))
        arrays.append(pa.array(sizes[f"bid_sz_{i}"], type=pa.float32()))
        names.extend([col, f"bid_sz_{i}"])
    for i, col in enumerate(ask_px_cols):
        arrays.append(pa.array(ask_prices[:, i].astype(np.float32), type=pa.float32()))
        arrays.append(pa.array(sizes[f"ask_sz_{i}"], type=pa.float32()))
        names.extend([col, f"ask_sz_{i}"])
    return pa.Table.from_arrays(arrays, names=names)


def write_restored_parquet(blob: Path, out: Path) -> None:
    table = decode_base_offset_blob(blob)
    out.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, out, compression="snappy")


def compare_parquet_with_polars(left: Path, right: Path) -> dict:
    try:
        import polars as pl
    except ModuleNotFoundError as exc:
        raise SystemExit(
            "polars is required for distance calculation. "
            "Install it in the active environment, for example: "
            "conda run -n mineru python -m pip install polars"
        ) from exc

    left_df = pl.read_parquet(left)
    right_df = pl.read_parquet(right)
    if left_df.shape != right_df.shape:
        raise ValueError(f"shape mismatch: {left_df.shape} vs {right_df.shape}")
    if left_df.columns != right_df.columns:
        raise ValueError("column order/name mismatch")

    numeric_types = {
        pl.Int8,
        pl.Int16,
        pl.Int32,
        pl.Int64,
        pl.UInt8,
        pl.UInt16,
        pl.UInt32,
        pl.UInt64,
        pl.Float32,
        pl.Float64,
    }
    cols = [
        col
        for col, dtype in zip(left_df.columns, left_df.dtypes)
        if dtype in numeric_types
    ]
    if not cols:
        raise ValueError("no numeric columns to compare")

    per_col = []
    total_sq = 0.0
    total_abs = 0.0
    total_cells = 0
    nonzero_cells = 0
    max_abs_all = 0.0

    for col in cols:
        diff = (left_df[col] - right_df[col]).abs()
        sq = diff.cast(pl.Float64).pow(2)
        max_abs = float(diff.max())
        mean_abs = float(diff.mean())
        rmse = float(sq.mean() ** 0.5)
        nonzero = int((diff != 0).sum())
        cells = diff.len()
        per_col.append(
            {
                "column": col,
                "max_abs": max_abs,
                "mean_abs": mean_abs,
                "rmse": rmse,
                "nonzero": nonzero,
            }
        )
        total_sq += float(sq.sum())
        total_abs += float(diff.cast(pl.Float64).sum())
        total_cells += cells
        nonzero_cells += nonzero
        max_abs_all = max(max_abs_all, max_abs)

    worst = sorted(per_col, key=lambda item: item["max_abs"], reverse=True)[:10]
    return {
        "rows": left_df.height,
        "columns": len(cols),
        "cells": total_cells,
        "nonzero_cells": nonzero_cells,
        "max_abs": max_abs_all,
        "mean_abs": total_abs / total_cells,
        "rmse": (total_sq / total_cells) ** 0.5,
        "worst_columns": worst,
    }


def print_distance_report(report: dict) -> None:
    print("\nRoundtrip distance report")
    print("-" * 78)
    print(f"rows: {report['rows']:,}")
    print(f"numeric columns: {report['columns']}")
    print(f"numeric cells: {report['cells']:,}")
    print(f"nonzero cells: {report['nonzero_cells']:,}")
    print(f"max_abs: {report['max_abs']:.12g}")
    print(f"mean_abs: {report['mean_abs']:.12g}")
    print(f"rmse: {report['rmse']:.12g}")
    print("\nWorst columns by max_abs")
    print(f"{'column':20} {'max_abs':>14} {'mean_abs':>14} {'rmse':>14} {'nonzero':>12}")
    for row in report["worst_columns"]:
        print(
            f"{row['column']:20} "
            f"{row['max_abs']:14.12g} "
            f"{row['mean_abs']:14.12g} "
            f"{row['rmse']:14.12g} "
            f"{row['nonzero']:12d}"
        )


def report_row(label: str, size: int, original: int) -> dict:
    return {
        "name": label,
        "bytes": size,
        "mib": mib(size),
        "ratio": size / original,
        "saved_pct": (1 - size / original) * 100,
    }


def print_report(rows: list[dict]) -> None:
    print("\nCompression report")
    print("-" * 78)
    print(f"{'case':34} {'MiB':>10} {'ratio':>10} {'saved':>10}")
    print("-" * 78)
    for row in rows:
        print(
            f"{row['name']:34} "
            f"{row['mib']:10.2f} "
            f"{row['ratio'] * 100:9.2f}% "
            f"{row['saved_pct']:9.2f}%"
        )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("input", nargs="?", default="data/parquet/2026-03-05.parquet")
    parser.add_argument("--scale", type=int, default=DEFAULT_SCALE)
    parser.add_argument("--zstd-level", type=int, default=19)
    parser.add_argument("--keep", action="store_true", help="keep temporary blobs")
    parser.add_argument("--roundtrip", action="store_true", help="decode base-offset blob back to parquet and compare")
    parser.add_argument(
        "--restored-output",
        default="data/delta_eval/restored-2026-03-05.parquet",
        help="where to write the restored parquet when --roundtrip is enabled",
    )
    args = parser.parse_args()

    input_path = Path(args.input)
    if not input_path.exists():
        raise SystemExit(f"input not found: {input_path}")
    if shutil.which("zstd") is None:
        raise SystemExit("zstd command not found on PATH")

    original_size = sizeof(input_path)
    table = pq.read_table(input_path)
    names = table.schema.names
    ts_col = "timestamp_ms"
    bid_px_cols = [f"bid_px_{i}" for i in range(DEPTH)]
    ask_px_cols = [f"ask_px_{i}" for i in range(DEPTH)]
    bid_sz_cols = [f"bid_sz_{i}" for i in range(DEPTH)]
    ask_sz_cols = [f"ask_sz_{i}" for i in range(DEPTH)]
    required = [ts_col] + bid_px_cols + ask_px_cols + bid_sz_cols + ask_sz_cols
    missing = [col for col in required if col not in names]
    if missing:
        raise SystemExit(f"missing required columns: {missing}")

    tmp_ctx = tempfile.TemporaryDirectory(prefix="okx-delta-eval-")
    tmp_dir = Path(tmp_ctx.name)
    try:
        rows = [report_row("original parquet", original_size, original_size)]

        zstd_parquet = tmp_dir / f"{input_path.name}.zst"
        run_zstd(input_path, zstd_parquet, args.zstd_level)
        rows.append(report_row(f"parquet + zstd -{args.zstd_level}", sizeof(zstd_parquet), original_size))

        price_cols = bid_px_cols + ask_px_cols
        size_cols = bid_sz_cols + ask_sz_cols

        col_delta = tmp_dir / "column_time_delta.bin"
        col_delta_zst = tmp_dir / "column_time_delta.bin.zst"
        write_column_time_delta_blob(col_delta, table, ts_col, price_cols, size_cols, args.scale)
        rows.append(report_row("column delta blob raw", sizeof(col_delta), original_size))
        run_zstd(col_delta, col_delta_zst, args.zstd_level)
        rows.append(report_row(f"column delta blob zstd -{args.zstd_level}", sizeof(col_delta_zst), original_size))

        base_offset = tmp_dir / "base_offset.bin"
        base_offset_zst = tmp_dir / "base_offset.bin.zst"
        write_base_offset_blob(base_offset, table, ts_col, bid_px_cols, ask_px_cols, size_cols, args.scale)
        rows.append(report_row("base offset blob raw", sizeof(base_offset), original_size))
        run_zstd(base_offset, base_offset_zst, args.zstd_level)
        rows.append(report_row(f"base offset blob zstd -{args.zstd_level}", sizeof(base_offset_zst), original_size))

        restored = Path(args.restored_output)
        if args.roundtrip:
            write_restored_parquet(base_offset, restored)

        print(f"input: {input_path}")
        print(f"rows: {table.num_rows:,}")
        print(f"columns: {table.num_columns}")
        print_report(rows)
        if args.roundtrip:
            print(f"\nrestored parquet: {restored}")
            print_distance_report(compare_parquet_with_polars(input_path, restored))
        if args.keep:
            kept = Path.cwd() / "data" / "delta_eval"
            kept.mkdir(parents=True, exist_ok=True)
            for item in tmp_dir.iterdir():
                shutil.copy2(item, kept / item.name)
            print(f"\nkept temporary files under: {kept}")
    finally:
        if not args.keep:
            tmp_ctx.cleanup()


if __name__ == "__main__":
    main()
