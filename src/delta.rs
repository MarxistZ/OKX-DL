use anyhow::{Context, Result};
use arrow::array::{Array, ArrayRef, Float32Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{Cursor, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

const MAGIC: &[u8; 8] = b"OKXD\0\0\0\x01";
const FORMAT: &str = "base_offset_v1";
const DEPTH: usize = 20;
const F32_COLS: usize = DEPTH * 4;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InspectReport {
    pub rows: usize,
    pub depth: usize,
    pub scale: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Header {
    format: String,
    rows: usize,
    depth: usize,
    scale: i64,
    timestamp: String,
    bid_px_cols: Vec<String>,
    ask_px_cols: Vec<String>,
    size_cols: Vec<String>,
    sizes: String,
    null_bitmap: String,
}

struct OrderbookData {
    timestamps: Vec<i64>,
    bid_px: Vec<Vec<Option<f32>>>,
    ask_px: Vec<Vec<Option<f32>>>,
    bid_sz: Vec<Vec<Option<f32>>>,
    ask_sz: Vec<Vec<Option<f32>>>,
}

pub fn encode_file(
    input: &Path,
    output: &Path,
    scale: i64,
    zstd_level: i32,
) -> Result<InspectReport> {
    if scale <= 0 {
        anyhow::bail!("scale must be positive, got {scale}");
    }
    if !(1..=22).contains(&zstd_level) {
        anyhow::bail!("zstd level must be in 1..=22, got {zstd_level}");
    }

    let data = read_orderbook_parquet(input)?;
    if data.timestamps.is_empty() {
        anyhow::bail!("input parquet has no rows");
    }

    let header = Header {
        format: FORMAT.to_string(),
        rows: data.timestamps.len(),
        depth: DEPTH,
        scale,
        timestamp: "timestamp_ms".to_string(),
        bid_px_cols: (0..DEPTH).map(|i| format!("bid_px_{i}")).collect(),
        ask_px_cols: (0..DEPTH).map(|i| format!("ask_px_{i}")).collect(),
        size_cols: (0..DEPTH)
            .map(|i| format!("bid_sz_{i}"))
            .chain((0..DEPTH).map(|i| format!("ask_sz_{i}")))
            .collect(),
        sizes: "raw_f32".to_string(),
        null_bitmap: "one_bitmap_per_f32_column".to_string(),
    };
    let mut raw = Vec::with_capacity(estimate_raw_size(header.rows));
    write_blob(&mut raw, &header, &data)?;

    write_zstd_atomic(output, &raw, zstd_level)?;
    Ok(report_from_header(&header))
}

pub fn decode_file(input: &Path, output: &Path) -> Result<InspectReport> {
    let raw = read_zstd_file(input)?;
    let (header, data) = read_blob(&raw)?;
    write_orderbook_parquet(output, &header, &data)?;
    Ok(report_from_header(&header))
}

pub fn inspect_file(input: &Path) -> Result<InspectReport> {
    let raw = read_zstd_file(input)?;
    let (header, _) = read_header(&raw)?;
    validate_header(&header)?;
    Ok(report_from_header(&header))
}

pub fn verify_file(parquet: &Path, encoded: &Path) -> Result<InspectReport> {
    let original = read_orderbook_parquet(parquet)?;
    let raw = read_zstd_file(encoded)?;
    let (header, decoded) = read_blob(&raw)?;
    ensure_equal_data(&original, &decoded)?;
    Ok(report_from_header(&header))
}

fn report_from_header(header: &Header) -> InspectReport {
    InspectReport {
        rows: header.rows,
        depth: header.depth,
        scale: header.scale,
    }
}

fn estimate_raw_size(rows: usize) -> usize {
    4096 + rows * (8 + DEPTH * 2 * 4 + DEPTH * 2 * 4 + F32_COLS)
}

fn read_orderbook_parquet(path: &Path) -> Result<OrderbookData> {
    let file = File::open(path).with_context(|| format!("open parquet {}", path.display()))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .with_context(|| format!("read parquet metadata {}", path.display()))?;
    validate_schema(builder.schema().as_ref())?;

    let reader = builder
        .with_batch_size(65_536)
        .build()
        .with_context(|| format!("open parquet record reader {}", path.display()))?;
    let mut data = OrderbookData {
        timestamps: Vec::new(),
        bid_px: (0..DEPTH).map(|_| Vec::new()).collect(),
        ask_px: (0..DEPTH).map(|_| Vec::new()).collect(),
        bid_sz: (0..DEPTH).map(|_| Vec::new()).collect(),
        ask_sz: (0..DEPTH).map(|_| Vec::new()).collect(),
    };

    for batch in reader {
        let batch = batch.with_context(|| format!("read parquet batch {}", path.display()))?;
        append_batch(&mut data, &batch)?;
    }
    Ok(data)
}

fn validate_schema(schema: &Schema) -> Result<()> {
    validate_field(schema, "timestamp_ms", &DataType::Int64)?;
    for i in 0..DEPTH {
        validate_field(schema, &format!("bid_px_{i}"), &DataType::Float32)?;
        validate_field(schema, &format!("bid_sz_{i}"), &DataType::Float32)?;
    }
    for i in 0..DEPTH {
        validate_field(schema, &format!("ask_px_{i}"), &DataType::Float32)?;
        validate_field(schema, &format!("ask_sz_{i}"), &DataType::Float32)?;
    }
    Ok(())
}

fn validate_field(schema: &Schema, name: &str, expected: &DataType) -> Result<()> {
    let field = schema
        .field_with_name(name)
        .with_context(|| format!("missing required column {name}"))?;
    if field.data_type() != expected {
        anyhow::bail!(
            "column {name} has type {:?}, expected {:?}",
            field.data_type(),
            expected
        );
    }
    Ok(())
}

fn append_batch(data: &mut OrderbookData, batch: &RecordBatch) -> Result<()> {
    let ts = batch
        .column_by_name("timestamp_ms")
        .context("missing required column timestamp_ms")?
        .as_any()
        .downcast_ref::<Int64Array>()
        .context("timestamp_ms is not Int64")?;
    for row in 0..batch.num_rows() {
        if ts.is_null(row) {
            anyhow::bail!(
                "timestamp_ms contains null at row {}",
                data.timestamps.len() + row
            );
        }
        data.timestamps.push(ts.value(row));
    }

    for i in 0..DEPTH {
        append_f32_column(batch, &format!("bid_px_{i}"), &mut data.bid_px[i])?;
        append_f32_column(batch, &format!("bid_sz_{i}"), &mut data.bid_sz[i])?;
        append_f32_column(batch, &format!("ask_px_{i}"), &mut data.ask_px[i])?;
        append_f32_column(batch, &format!("ask_sz_{i}"), &mut data.ask_sz[i])?;
    }
    Ok(())
}

fn append_f32_column(batch: &RecordBatch, name: &str, out: &mut Vec<Option<f32>>) -> Result<()> {
    let arr = batch
        .column_by_name(name)
        .with_context(|| format!("missing required column {name}"))?
        .as_any()
        .downcast_ref::<Float32Array>()
        .with_context(|| format!("{name} is not Float32"))?;
    for row in 0..arr.len() {
        out.push(if arr.is_null(row) {
            None
        } else {
            Some(arr.value(row))
        });
    }
    Ok(())
}

fn write_blob(out: &mut Vec<u8>, header: &Header, data: &OrderbookData) -> Result<()> {
    validate_data_shape(data, header.rows)?;
    out.extend_from_slice(MAGIC);
    let header_bytes = serde_json::to_vec(header)?;
    let header_len: u32 = header_bytes.len().try_into().context("header too large")?;
    out.extend_from_slice(&header_len.to_le_bytes());
    out.extend_from_slice(&header_bytes);

    for col in f32_columns(data) {
        write_bitmap(out, col, header.rows);
    }

    write_delta_i64(out, &data.timestamps, "timestamp_ms")?;
    write_price_side(out, &data.bid_px, header.scale, PriceSide::Bid)?;
    write_price_side(out, &data.ask_px, header.scale, PriceSide::Ask)?;
    for col in data.bid_sz.iter().chain(data.ask_sz.iter()) {
        write_f32_values(out, col);
    }
    Ok(())
}

fn validate_data_shape(data: &OrderbookData, rows: usize) -> Result<()> {
    for (label, cols) in [
        ("bid_px", &data.bid_px),
        ("ask_px", &data.ask_px),
        ("bid_sz", &data.bid_sz),
        ("ask_sz", &data.ask_sz),
    ] {
        if cols.len() != DEPTH {
            anyhow::bail!("{label} depth is {}, expected {DEPTH}", cols.len());
        }
        for (idx, col) in cols.iter().enumerate() {
            if col.len() != rows {
                anyhow::bail!("{label}_{idx} rows is {}, expected {rows}", col.len());
            }
        }
    }
    Ok(())
}

fn f32_columns(data: &OrderbookData) -> Vec<&Vec<Option<f32>>> {
    let mut cols = Vec::with_capacity(F32_COLS);
    for i in 0..DEPTH {
        cols.push(&data.bid_px[i]);
        cols.push(&data.bid_sz[i]);
    }
    for i in 0..DEPTH {
        cols.push(&data.ask_px[i]);
        cols.push(&data.ask_sz[i]);
    }
    cols
}

fn write_bitmap(out: &mut Vec<u8>, values: &[Option<f32>], rows: usize) {
    let bytes = bitmap_len(rows);
    let start = out.len();
    out.resize(start + bytes, 0);
    for (idx, value) in values.iter().enumerate() {
        if value.is_some() {
            out[start + idx / 8] |= 1 << (idx % 8);
        }
    }
}

fn bitmap_len(rows: usize) -> usize {
    (rows + 7) / 8
}

fn write_delta_i64(out: &mut Vec<u8>, values: &[i64], label: &str) -> Result<()> {
    if values.is_empty() {
        anyhow::bail!("{label} has no values");
    }
    out.extend_from_slice(&values[0].to_le_bytes());
    for pair in values.windows(2) {
        let delta = pair[1]
            .checked_sub(pair[0])
            .with_context(|| format!("{label} delta overflow"))?;
        let delta_i32 = i32::try_from(delta)
            .with_context(|| format!("{label} delta does not fit i32: {delta}"))?;
        out.extend_from_slice(&delta_i32.to_le_bytes());
    }
    Ok(())
}

enum PriceSide {
    Bid,
    Ask,
}

fn write_price_side(
    out: &mut Vec<u8>,
    cols: &[Vec<Option<f32>>],
    scale: i64,
    side: PriceSide,
) -> Result<()> {
    let rows = cols[0].len();
    let mut base = Vec::with_capacity(rows);
    for row in 0..rows {
        base.push(scale_price(
            cols[0][row].with_context(|| format!("top-of-book price is null at row {row}"))?,
            scale,
            "top-of-book price",
        )?);
    }
    write_delta_i64(out, &base, "top-of-book price")?;

    for row in 0..rows {
        for (depth, col) in cols.iter().enumerate() {
            let offset = match col[row] {
                Some(value) => {
                    let px = scale_price(value, scale, "price")?;
                    match side {
                        PriceSide::Bid => {
                            base[row].checked_sub(px).context("bid offset overflow")?
                        }
                        PriceSide::Ask => {
                            px.checked_sub(base[row]).context("ask offset overflow")?
                        }
                    }
                }
                None => 0,
            };
            let offset_i32 = i32::try_from(offset).with_context(|| {
                format!("price offset at row {row} depth {depth} does not fit i32: {offset}")
            })?;
            out.extend_from_slice(&offset_i32.to_le_bytes());
        }
    }
    Ok(())
}

fn scale_price(value: f32, scale: i64, label: &str) -> Result<i64> {
    if !value.is_finite() {
        anyhow::bail!("{label} is not finite: {value}");
    }
    let scaled = (value as f64 * scale as f64).round();
    if scaled < i64::MIN as f64 || scaled > i64::MAX as f64 {
        anyhow::bail!("{label} scaled value overflows i64: {value}");
    }
    let encoded = scaled as i64;
    let restored = (encoded as f64 / scale as f64) as f32;
    if restored.to_bits() != value.to_bits() {
        anyhow::bail!(
            "{label} cannot roundtrip exactly with scale {scale}: original={value} restored={restored}"
        );
    }
    Ok(encoded)
}

fn write_f32_values(out: &mut Vec<u8>, values: &[Option<f32>]) {
    for value in values {
        out.extend_from_slice(&value.unwrap_or(0.0).to_le_bytes());
    }
}

fn write_zstd_atomic(output: &Path, raw: &[u8], zstd_level: i32) -> Result<()> {
    if let Some(parent) = output.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create output dir {}", parent.display()))?;
    }
    let tmp = tmp_path(output);
    let result = (|| -> Result<()> {
        let file = File::create(&tmp).with_context(|| format!("create temp {}", tmp.display()))?;
        let mut encoder =
            zstd::stream::write::Encoder::new(file, zstd_level).context("create zstd encoder")?;
        encoder.write_all(raw).context("write zstd stream")?;
        encoder.finish().context("finish zstd stream")?;
        std::fs::rename(&tmp, output)
            .with_context(|| format!("rename {} to {}", tmp.display(), output.display()))?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
    result
}

fn tmp_path(path: &Path) -> PathBuf {
    let mut tmp = path.as_os_str().to_os_string();
    tmp.push(".tmp");
    PathBuf::from(tmp)
}

fn read_zstd_file(path: &Path) -> Result<Vec<u8>> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    zstd::stream::decode_all(file).with_context(|| format!("decode zstd {}", path.display()))
}

fn read_blob(raw: &[u8]) -> Result<(Header, OrderbookData)> {
    let (header, mut cur) = read_header(raw)?;
    validate_header(&header)?;
    let rows = header.rows;
    let bitmaps = read_bitmaps(&mut cur, rows)?;
    let timestamps = read_delta_i64(&mut cur, rows, "timestamp_ms")?;
    let bid_px = read_price_side(&mut cur, rows, header.scale, PriceSide::Bid, &bitmaps, 0)?;
    let ask_px = read_price_side(
        &mut cur,
        rows,
        header.scale,
        PriceSide::Ask,
        &bitmaps,
        DEPTH * 2,
    )?;
    let mut bid_sz = Vec::with_capacity(DEPTH);
    let mut ask_sz = Vec::with_capacity(DEPTH);
    for i in 0..DEPTH {
        bid_sz.push(read_f32_column(&mut cur, rows, &bitmaps[i * 2 + 1])?);
    }
    for i in 0..DEPTH {
        ask_sz.push(read_f32_column(
            &mut cur,
            rows,
            &bitmaps[DEPTH * 2 + i * 2 + 1],
        )?);
    }
    if cur.position() != raw.len() as u64 {
        anyhow::bail!("delta blob has trailing unread bytes");
    }
    Ok((
        header,
        OrderbookData {
            timestamps,
            bid_px,
            ask_px,
            bid_sz,
            ask_sz,
        },
    ))
}

fn read_header(raw: &[u8]) -> Result<(Header, Cursor<&[u8]>)> {
    let mut cur = Cursor::new(raw);
    let mut magic = [0_u8; 8];
    cur.read_exact(&mut magic).context("read delta magic")?;
    if &magic != MAGIC {
        anyhow::bail!("invalid delta magic");
    }
    let header_len = read_u32(&mut cur)? as usize;
    if header_len == 0 || header_len > 64 * 1024 {
        anyhow::bail!("invalid delta header length: {header_len}");
    }
    let mut header_bytes = vec![0_u8; header_len];
    cur.read_exact(&mut header_bytes)
        .context("read delta header")?;
    let header: Header = serde_json::from_slice(&header_bytes).context("parse delta header")?;
    Ok((header, cur))
}

fn validate_header(header: &Header) -> Result<()> {
    if header.format != FORMAT {
        anyhow::bail!("unsupported delta format: {}", header.format);
    }
    if header.depth != DEPTH {
        anyhow::bail!("unsupported depth: {}", header.depth);
    }
    if header.rows == 0 {
        anyhow::bail!("delta file has no rows");
    }
    if header.scale <= 0 {
        anyhow::bail!("invalid scale: {}", header.scale);
    }
    Ok(())
}

fn read_bitmaps(cur: &mut Cursor<&[u8]>, rows: usize) -> Result<Vec<Vec<bool>>> {
    let mut bitmaps = Vec::with_capacity(F32_COLS);
    let bytes = bitmap_len(rows);
    for _ in 0..F32_COLS {
        let mut raw = vec![0_u8; bytes];
        cur.read_exact(&mut raw).context("read null bitmap")?;
        let mut bitmap = Vec::with_capacity(rows);
        for idx in 0..rows {
            bitmap.push(raw[idx / 8] & (1 << (idx % 8)) != 0);
        }
        bitmaps.push(bitmap);
    }
    Ok(bitmaps)
}

fn read_delta_i64(cur: &mut Cursor<&[u8]>, rows: usize, label: &str) -> Result<Vec<i64>> {
    let first = read_i64(cur).with_context(|| format!("read {label} first value"))?;
    let mut out = Vec::with_capacity(rows);
    out.push(first);
    for _ in 1..rows {
        let delta = read_i32(cur).with_context(|| format!("read {label} delta"))? as i64;
        let next = out[out.len() - 1]
            .checked_add(delta)
            .with_context(|| format!("{label} cumulative delta overflow"))?;
        out.push(next);
    }
    Ok(out)
}

fn read_price_side(
    cur: &mut Cursor<&[u8]>,
    rows: usize,
    scale: i64,
    side: PriceSide,
    bitmaps: &[Vec<bool>],
    bitmap_offset: usize,
) -> Result<Vec<Vec<Option<f32>>>> {
    let base = read_delta_i64(cur, rows, "top-of-book price")?;
    let mut cols: Vec<Vec<Option<f32>>> = (0..DEPTH).map(|_| Vec::with_capacity(rows)).collect();
    for (row, base_value) in base.iter().enumerate() {
        for depth in 0..DEPTH {
            let offset = read_i32(cur)? as i64;
            let encoded = match side {
                PriceSide::Bid => base_value
                    .checked_sub(offset)
                    .context("bid price reconstruction overflow")?,
                PriceSide::Ask => base_value
                    .checked_add(offset)
                    .context("ask price reconstruction overflow")?,
            };
            let value = (encoded as f64 / scale as f64) as f32;
            let bitmap_idx = bitmap_offset + depth * 2;
            cols[depth].push(if bitmaps[bitmap_idx][row] {
                Some(value)
            } else {
                None
            });
        }
    }
    Ok(cols)
}

fn read_f32_column(
    cur: &mut Cursor<&[u8]>,
    rows: usize,
    bitmap: &[bool],
) -> Result<Vec<Option<f32>>> {
    let mut out = Vec::with_capacity(rows);
    for valid in bitmap.iter().take(rows) {
        let value = read_f32(cur)?;
        out.push(if *valid { Some(value) } else { None });
    }
    Ok(out)
}

fn read_u32(cur: &mut Cursor<&[u8]>) -> Result<u32> {
    let mut raw = [0_u8; 4];
    cur.read_exact(&mut raw).context("read u32")?;
    Ok(u32::from_le_bytes(raw))
}

fn read_i64(cur: &mut Cursor<&[u8]>) -> Result<i64> {
    let mut raw = [0_u8; 8];
    cur.read_exact(&mut raw).context("read i64")?;
    Ok(i64::from_le_bytes(raw))
}

fn read_i32(cur: &mut Cursor<&[u8]>) -> Result<i32> {
    let mut raw = [0_u8; 4];
    cur.read_exact(&mut raw).context("read i32")?;
    Ok(i32::from_le_bytes(raw))
}

fn read_f32(cur: &mut Cursor<&[u8]>) -> Result<f32> {
    let mut raw = [0_u8; 4];
    cur.read_exact(&mut raw).context("read f32")?;
    Ok(f32::from_le_bytes(raw))
}

fn write_orderbook_parquet(output: &Path, header: &Header, data: &OrderbookData) -> Result<()> {
    let schema = orderbook_schema();
    let mut arrays: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(data.timestamps.clone()))];
    for i in 0..DEPTH {
        arrays.push(Arc::new(Float32Array::from(data.bid_px[i].clone())));
        arrays.push(Arc::new(Float32Array::from(data.bid_sz[i].clone())));
    }
    for i in 0..DEPTH {
        arrays.push(Arc::new(Float32Array::from(data.ask_px[i].clone())));
        arrays.push(Arc::new(Float32Array::from(data.ask_sz[i].clone())));
    }
    let batch =
        RecordBatch::try_new(schema.clone(), arrays).context("build decoded record batch")?;
    if batch.num_rows() != header.rows {
        anyhow::bail!(
            "decoded row count mismatch: {} vs {}",
            batch.num_rows(),
            header.rows
        );
    }

    if let Some(parent) = output.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create output dir {}", parent.display()))?;
    }
    let tmp = tmp_path(output);
    let result = (|| -> Result<()> {
        let file = File::create(&tmp).with_context(|| format!("create temp {}", tmp.display()))?;
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let mut writer =
            ArrowWriter::try_new(file, schema, Some(props)).context("create parquet writer")?;
        writer
            .write(&batch)
            .context("write decoded parquet batch")?;
        writer.close().context("close decoded parquet writer")?;
        std::fs::rename(&tmp, output)
            .with_context(|| format!("rename {} to {}", tmp.display(), output.display()))?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
    result
}

fn orderbook_schema() -> Arc<Schema> {
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

fn ensure_equal_data(left: &OrderbookData, right: &OrderbookData) -> Result<()> {
    if left.timestamps != right.timestamps {
        anyhow::bail!("timestamp values differ");
    }
    ensure_equal_side("bid_px", &left.bid_px, &right.bid_px)?;
    ensure_equal_side("ask_px", &left.ask_px, &right.ask_px)?;
    ensure_equal_side("bid_sz", &left.bid_sz, &right.bid_sz)?;
    ensure_equal_side("ask_sz", &left.ask_sz, &right.ask_sz)?;
    Ok(())
}

fn ensure_equal_side(
    label: &str,
    left: &[Vec<Option<f32>>],
    right: &[Vec<Option<f32>>],
) -> Result<()> {
    for (depth, (left_col, right_col)) in left.iter().zip(right.iter()).enumerate() {
        if left_col.len() != right_col.len() {
            anyhow::bail!("{label}_{depth} row count differs");
        }
        for (row, (left_value, right_value)) in left_col.iter().zip(right_col.iter()).enumerate() {
            if option_f32_bits(*left_value) != option_f32_bits(*right_value) {
                anyhow::bail!(
                    "{label}_{depth} differs at row {row}: {left_value:?} vs {right_value:?}"
                );
            }
        }
    }
    Ok(())
}

fn option_f32_bits(value: Option<f32>) -> Option<u32> {
    value.map(f32::to_bits)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Float32Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::arrow::ArrowWriter;
    use parquet::basic::Compression;
    use parquet::file::properties::WriterProperties;
    use std::fs::File;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_dir() -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("okx-delta-test-{nanos}"));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn test_schema() -> Arc<Schema> {
        let mut fields = vec![Field::new("timestamp_ms", DataType::Int64, false)];
        for i in 0..20 {
            fields.push(Field::new(format!("bid_px_{i}"), DataType::Float32, true));
            fields.push(Field::new(format!("bid_sz_{i}"), DataType::Float32, true));
        }
        for i in 0..20 {
            fields.push(Field::new(format!("ask_px_{i}"), DataType::Float32, true));
            fields.push(Field::new(format!("ask_sz_{i}"), DataType::Float32, true));
        }
        Arc::new(Schema::new(fields))
    }

    fn sample_batch() -> RecordBatch {
        let rows = 4;
        let schema = test_schema();
        let mut arrays: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(vec![
            1_700_000_000_000,
            1_700_000_000_100,
            1_700_000_000_200,
            1_700_000_000_300,
        ]))];

        for i in 0..20 {
            let px = Float32Array::from(
                (0..rows)
                    .map(|row| 100.0 - i as f32 * 0.1 + row as f32 * 0.01)
                    .collect::<Vec<_>>(),
            );
            let sz = Float32Array::from(
                (0..rows)
                    .map(|row| 1.0 + i as f32 * 0.01 + row as f32 * 0.001)
                    .collect::<Vec<_>>(),
            );
            arrays.push(Arc::new(px));
            arrays.push(Arc::new(sz));
        }

        for i in 0..20 {
            let px = Float32Array::from(
                (0..rows)
                    .map(|row| 100.5 + i as f32 * 0.1 + row as f32 * 0.01)
                    .collect::<Vec<_>>(),
            );
            let sz = Float32Array::from(
                (0..rows)
                    .map(|row| 2.0 + i as f32 * 0.01 + row as f32 * 0.001)
                    .collect::<Vec<_>>(),
            );
            arrays.push(Arc::new(px));
            arrays.push(Arc::new(sz));
        }

        RecordBatch::try_new(schema, arrays).unwrap()
    }

    fn write_parquet(path: &Path, batch: &RecordBatch) {
        let file = File::create(path).unwrap();
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();
        writer.write(batch).unwrap();
        writer.close().unwrap();
    }

    fn read_one_batch(path: &Path) -> RecordBatch {
        let file = File::open(path).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let mut reader = builder.with_batch_size(16).build().unwrap();
        reader.next().unwrap().unwrap()
    }

    #[test]
    fn base_offset_roundtrip_restores_parquet_cells_exactly() {
        let dir = unique_dir();
        let input = dir.join("input.parquet");
        let encoded = dir.join("input.okxd.zst");
        let restored = dir.join("restored.parquet");
        let original = sample_batch();
        write_parquet(&input, &original);

        let encode_report = encode_file(&input, &encoded, 1_000_000, 3).unwrap();
        let decode_report = decode_file(&encoded, &restored).unwrap();
        let restored_batch = read_one_batch(&restored);

        assert_eq!(encode_report.rows, 4);
        assert_eq!(encode_report.depth, 20);
        assert_eq!(decode_report, encode_report);
        assert_eq!(format!("{original:?}"), format!("{restored_batch:?}"));
    }

    #[test]
    fn inspect_reads_header_without_decoding_full_parquet() {
        let dir = unique_dir();
        let input = dir.join("input.parquet");
        let encoded = dir.join("input.okxd.zst");
        write_parquet(&input, &sample_batch());

        encode_file(&input, &encoded, 1_000_000, 3).unwrap();
        let report = inspect_file(&encoded).unwrap();

        assert_eq!(
            report,
            InspectReport {
                rows: 4,
                depth: 20,
                scale: 1_000_000,
            }
        );
    }

    #[test]
    fn encode_rejects_missing_orderbook_columns() {
        let dir = unique_dir();
        let input = dir.join("bad.parquet");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "timestamp_ms",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![1_700_000_000_000]))],
        )
        .unwrap();
        write_parquet(&input, &batch);

        let err = encode_file(&input, &dir.join("bad.okxd.zst"), 1_000_000, 3).unwrap_err();

        assert!(err.to_string().contains("missing required column"));
    }

    #[test]
    fn decode_rejects_corrupt_magic() {
        let dir = unique_dir();
        let corrupt = dir.join("corrupt.okxd.zst");
        std::fs::write(&corrupt, b"not a zstd encoded delta file").unwrap();

        let err = decode_file(&corrupt, &dir.join("out.parquet")).unwrap_err();

        assert!(
            err.to_string().contains("zstd")
                || err.to_string().contains("magic")
                || err.to_string().contains("decode")
        );
    }
}
