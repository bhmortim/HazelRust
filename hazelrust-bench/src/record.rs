//! Output record schema (one JSON per cell × client × fork × trial) and
//! HdrHistogram serialization. Matches the Java harness's output schema exactly.

use base64::Engine;
use hdrhistogram::serialization::{Serializer, V2DeflateSerializer};
use hdrhistogram::Histogram;
use serde::Serialize;

#[derive(Serialize)]
pub struct LatencyStats {
    /// number of recorded samples
    pub count: u64,
    /// all values in MICROSECONDS
    pub min: f64,
    pub p25: f64,
    pub p50: f64,
    pub p75: f64,
    pub p90: f64,
    pub p95: f64,
    pub p99: f64,
    pub p999: f64,
    pub p9999: f64,
    pub max: f64,
    pub mean: f64,
    pub stddev: f64,
    /// samples recorded at or above p99.9 (tail-validity check)
    pub samples_above_p999: u64,
    /// HdrHistogram V2+deflate, base64 — raw values are NANOSECONDS
    pub hgrm_b64: String,
}

const NS_PER_US: f64 = 1000.0;

pub fn latency_stats(hist: &Histogram<u64>) -> LatencyStats {
    let mut buf = Vec::new();
    let mut ser = V2DeflateSerializer::new();
    ser.serialize(hist, &mut buf).expect("hgrm serialize");
    let hgrm_b64 = base64::engine::general_purpose::STANDARD.encode(&buf);

    let v999 = hist.value_at_quantile(0.999);
    let samples_above_p999 = hist.count_between(v999, hist.max());

    LatencyStats {
        count: hist.len(),
        min: hist.min() as f64 / NS_PER_US,
        p25: hist.value_at_quantile(0.25) as f64 / NS_PER_US,
        p50: hist.value_at_quantile(0.50) as f64 / NS_PER_US,
        p75: hist.value_at_quantile(0.75) as f64 / NS_PER_US,
        p90: hist.value_at_quantile(0.90) as f64 / NS_PER_US,
        p95: hist.value_at_quantile(0.95) as f64 / NS_PER_US,
        p99: hist.value_at_quantile(0.99) as f64 / NS_PER_US,
        p999: hist.value_at_quantile(0.999) as f64 / NS_PER_US,
        p9999: hist.value_at_quantile(0.9999) as f64 / NS_PER_US,
        max: hist.max() as f64 / NS_PER_US,
        mean: hist.mean() / NS_PER_US,
        stddev: hist.stdev() / NS_PER_US,
        samples_above_p999,
        hgrm_b64,
    }
}

#[derive(Serialize, Default, Clone)]
pub struct Errors {
    pub total: u64,
    pub timeouts: u64,
    pub server: u64,
    pub connection: u64,
    pub other: u64,
}

impl Errors {
    pub fn bump(&mut self, code: u8) {
        self.total += 1;
        match code {
            1 => self.timeouts += 1,
            2 => self.connection += 1,
            3 => self.server += 1,
            _ => self.other += 1,
        }
    }
}

impl std::ops::AddAssign for Errors {
    fn add_assign(&mut self, o: Self) {
        self.total += o.total;
        self.timeouts += o.timeouts;
        self.server += o.server;
        self.connection += o.connection;
        self.other += o.other;
    }
}

/// Map a client error to an error class code: 1=timeout 2=connection 3=server 0=other.
pub fn classify_code(e: &anyhow::Error) -> u8 {
    let s = e.to_string().to_lowercase();
    if s.contains("timeout") || s.contains("timed out") {
        1
    } else if s.contains("connection") || s.contains("connect") || s.contains("closed") || s.contains("disconnect") {
        2
    } else if s.contains("exception") || s.contains("server") || s.contains("hazelcast") {
        3
    } else {
        0
    }
}

#[derive(Serialize)]
pub struct Record {
    pub client: String,
    pub version: String,
    pub commit: String,
    pub cell_id: String,
    pub suite: String,
    pub structure: String,
    pub op: String,
    pub variant: String,
    pub key_type: String,
    pub value_kind: String,
    pub value_size: u32,
    pub working_set: u64,
    pub concurrency: u32,
    pub load_model: String,
    pub target_rate: Option<f64>,
    pub distribution: String,
    pub batch_size: Option<u32>,
    pub fork: u32,
    pub trial: u32,
    pub ops: u64,
    pub duration_s: f64,
    pub throughput: f64,
    /// whether the open-loop generator kept up with the target rate
    pub saturated: bool,
    pub errors: Errors,
    pub latency: LatencyStats,
    /// per-1-second throughput series (stability)
    pub throughput_series: Vec<f64>,
    pub mem: Mem,
    pub provenance: serde_json::Value,
    pub ts_unix_ms: u128,
}

#[derive(Serialize, Default)]
pub struct Mem {
    pub rss_peak_kb: u64,
}
