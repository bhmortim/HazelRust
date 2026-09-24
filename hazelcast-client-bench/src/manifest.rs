//! Manifest schema — mirror of the JSON produced by `bench/gen_manifest.py`.
//! The Java harness deserializes the identical schema. Keep the two in sync.

use serde::Deserialize;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Deserialize)]
pub struct Manifest {
    pub meta: Meta,
    pub cells: Vec<Cell>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Meta {
    pub schema_version: u32,
    pub tier: String,
    pub seed: u64,
    #[serde(default)]
    pub zipf_theta: f64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Cell {
    pub id: String,
    pub suite: String,
    pub structure: String,
    pub op: String,
    #[serde(default)]
    pub variant: String,
    pub key_type: String,
    pub value_kind: String,
    pub value_size: u32,
    pub working_set: u64,
    pub concurrency: u32,
    pub load_model: String,
    #[serde(default)]
    pub rate_group: Option<String>,
    #[serde(default)]
    pub rate_frac: Option<f64>,
    /// Absolute target rate in ops/sec for open-loop cells. Null in the manifest;
    /// resolved by the orchestrator from the measured closed-loop max and passed
    /// to the harness via --target-rate (which overrides this).
    #[serde(default)]
    pub target_rate: Option<f64>,
    #[serde(default)]
    pub batch_size: Option<u32>,
    pub distribution: String,
    #[serde(default = "default_theta")]
    pub zipf_theta: f64,
    pub warmup_s: u64,
    pub measure_s: u64,
    pub forks: u32,
    pub trials: u32,
    #[serde(default)]
    pub min_ops: u64,
    /// Suite J mixed-workload op probabilities (e.g. {"get":0.5,"put":0.5}).
    #[serde(default)]
    pub mix: Option<BTreeMap<String, f64>>,
}

fn default_theta() -> f64 {
    0.99
}

impl Manifest {
    pub fn find(&self, cell_id: &str) -> Option<&Cell> {
        self.cells.iter().find(|c| c.id == cell_id)
    }
}
