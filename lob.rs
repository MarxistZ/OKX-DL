use crate::DEPTH;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::Path;

// ── 价格编码 ─────────────────────────────────────────────────────────────────
// 价格 × 1_000_000 取整为 i64，避免浮点排序不确定性。
// 精度 1e-6 USDT，对所有主流合约足够。

const SCALE: f64 = 1_000_000.0;

#[inline]
fn encode(s: &str) -> i64 {
    (s.parse::<f64>().unwrap_or(0.0) * SCALE).round() as i64
}
#[inline]
pub fn decode(n: i64) -> f32 {
    (n as f64 / SCALE) as f32
}

// ── OKX 原始 JSON 结构 ────────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct OkxRecord {
    pub action: String,
    pub ts: String,
    #[serde(default)]
    pub bids: Vec<Vec<String>>,
    #[serde(default)]
    pub asks: Vec<Vec<String>>,
}

// ── Snapshot ──────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct Snapshot {
    pub ts_ms:  i64,
    pub bid_px: [f32; DEPTH],
    pub bid_sz: [f32; DEPTH],
    pub ask_px: [f32; DEPTH],
    pub ask_sz: [f32; DEPTH],
}

impl Snapshot {
    pub fn new(ts_ms: i64) -> Self {
        Self {
            ts_ms,
            bid_px: [f32::NAN; DEPTH],
            bid_sz: [f32::NAN; DEPTH],
            ask_px: [f32::NAN; DEPTH],
            ask_sz: [f32::NAN; DEPTH],
        }
    }
}

// ── LOB ──────────────────────────────────────────────────────────────────────

/// bids: key = -encoded_price（BTreeMap 升序 ⟹ 第一个是最高 bid）
/// asks: key = +encoded_price（BTreeMap 升序 ⟹ 第一个是最低 ask）
pub struct Lob {
    pub bids:  BTreeMap<i64, f32>,
    pub asks:  BTreeMap<i64, f32>,
    pub ts_ms: i64,
    pub ready: bool,
}

impl Lob {
    pub fn new() -> Self {
        Self {
            bids:  BTreeMap::new(),
            asks:  BTreeMap::new(),
            ts_ms: 0,
            ready: false,
        }
    }

    pub fn apply(&mut self, record: &OkxRecord) {
        let ts = record.ts.parse::<i64>().unwrap_or(self.ts_ms);
        // 容错：拒绝时间戳倒退超过 1 分钟的 update（可能是乱序数据）
        if ts < self.ts_ms - 60_000 && self.ready {
            return;
        }
        self.ts_ms = ts;

        match record.action.as_str() {
            "snapshot" => {
                self.bids.clear();
                self.asks.clear();
                for level in &record.bids {
                    if level.len() < 2 { continue; }
                    let q = level[1].parse::<f32>().unwrap_or(0.0);
                    if q > 0.0 { self.bids.insert(-encode(&level[0]), q); }
                }
                for level in &record.asks {
                    if level.len() < 2 { continue; }
                    let q = level[1].parse::<f32>().unwrap_or(0.0);
                    if q > 0.0 { self.asks.insert(encode(&level[0]), q); }
                }
                self.ready = true;
            }
            "update" => {
                for level in &record.bids {
                    if level.len() < 2 { continue; }
                    let key = -encode(&level[0]);
                    let q   = level[1].parse::<f32>().unwrap_or(0.0);
                    if q == 0.0 { self.bids.remove(&key); }
                    else        { self.bids.insert(key, q); }
                }
                for level in &record.asks {
                    if level.len() < 2 { continue; }
                    let key = encode(&level[0]);
                    let q   = level[1].parse::<f32>().unwrap_or(0.0);
                    if q == 0.0 { self.asks.remove(&key); }
                    else        { self.asks.insert(key, q); }
                }
            }
            _ => {}
        }
    }

    /// top-DEPTH 快照，O(DEPTH)
    pub fn snapshot(&self, ts_ms: i64) -> Snapshot {
        let mut s = Snapshot::new(ts_ms);
        for (i, (k, &q)) in self.bids.iter().take(DEPTH).enumerate() {
            s.bid_px[i] = decode(-k);
            s.bid_sz[i] = q;
        }
        for (i, (k, &q)) in self.asks.iter().take(DEPTH).enumerate() {
            s.ask_px[i] = decode(*k);
            s.ask_sz[i] = q;
        }
        s
    }

    // ── Checkpoint ───────────────────────────────────────────────────────────

    pub fn save_checkpoint(&self, path: &Path) -> Result<()> {
        let ckpt = LobCheckpoint {
            bids:  self.bids.iter().map(|(&k, &v)| (k, v)).collect(),
            asks:  self.asks.iter().map(|(&k, &v)| (k, v)).collect(),
            ts_ms: self.ts_ms,
        };
        let tmp = path.with_extension("tmp");
        std::fs::write(&tmp, serde_json::to_string(&ckpt)?)?;
        std::fs::rename(&tmp, path)?;  // 原子写入
        Ok(())
    }

    pub fn load_checkpoint(path: &Path) -> Result<Self> {
        let data = std::fs::read_to_string(path)?;
        let ckpt: LobCheckpoint = serde_json::from_str(&data)?;
        let mut lob = Lob::new();
        for (k, v) in ckpt.bids { lob.bids.insert(k, v); }
        for (k, v) in ckpt.asks { lob.asks.insert(k, v); }
        lob.ts_ms = ckpt.ts_ms;
        lob.ready = !lob.bids.is_empty() || !lob.asks.is_empty();
        Ok(lob)
    }
}

#[derive(Serialize, Deserialize)]
struct LobCheckpoint {
    bids:  Vec<(i64, f32)>,
    asks:  Vec<(i64, f32)>,
    ts_ms: i64,
}
