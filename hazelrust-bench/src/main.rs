//! `hazelrust-bench` — end-to-end benchmark harness for the HazelRust client.
//! Consumes the shared manifest (`bench/manifest.*.json`), runs ONE cell (one
//! fork, one trial) against the live cluster, and emits a schema-matched JSON
//! result record + an embedded HdrHistogram. The Java harness mirrors this.

mod data;
mod manifest;
mod ops;
mod record;
mod runner;

use anyhow::{Context, Result};
use clap::Parser;
use hazelcast_client::{ClientConfig, HazelcastClient};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use manifest::Manifest;
use record::{latency_stats, Mem, Record};

#[derive(Parser, Debug)]
#[command(about = "HazelRust end-to-end benchmark harness")]
struct Args {
    #[arg(long)]
    manifest: String,
    #[arg(long)]
    cell: String,
    #[arg(long, default_value_t = 0)]
    fork: u32,
    #[arg(long, default_value_t = 0)]
    trial: u32,
    #[arg(long, default_value = ".")]
    out: String,
    #[arg(long, default_value = "127.0.0.1:5701")]
    cluster: String,
    #[arg(long, default_value = "dev")]
    cluster_name: String,
    /// Override the manifest seed.
    #[arg(long)]
    seed: Option<u64>,
    /// Open-loop target rate (ops/sec); overrides the cell's target_rate.
    #[arg(long)]
    target_rate: Option<f64>,
    /// Override warmup window (seconds).
    #[arg(long)]
    warmup_s: Option<u64>,
    /// Override measure window (seconds).
    #[arg(long)]
    measure_s: Option<u64>,
    #[arg(long, default_value = "unknown")]
    commit: String,
    /// Just (re)seed the shared dataset for the cell, then exit.
    #[arg(long, default_value_t = false)]
    seed_only: bool,
    /// Also print the record JSON to stdout.
    #[arg(long, default_value_t = false)]
    stdout: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let raw = std::fs::read_to_string(&args.manifest)
        .with_context(|| format!("reading manifest {}", args.manifest))?;
    let m: Manifest = serde_json::from_str(&raw).context("parsing manifest")?;
    let cell = m
        .find(&args.cell)
        .cloned()
        .with_context(|| format!("cell {} not found in manifest", args.cell))?;
    let seed = args.seed.unwrap_or(m.meta.seed);

    let addrs: Vec<SocketAddr> = args
        .cluster
        .split(',')
        .map(|s| s.trim().parse::<SocketAddr>())
        .collect::<std::result::Result<_, _>>()
        .context("parsing --cluster addresses")?;

    let mut builder = ClientConfig::builder().cluster_name(&args.cluster_name);
    for a in &addrs {
        builder = builder.add_address(*a);
    }
    // smart routing ON (default), statistics OFF (default), near-cache OFF
    // (not configured) — the fairness contract (§1.4).
    builder = builder.connection_timeout(Duration::from_secs(10));
    // Enable backup-ack-to-client by DEFAULT so the benchmark matches the Java
    // client's default (ON) for a fair comparison. The HazelRust library default
    // is OFF (RPO-0 safe); HZ_NO_BACKUP_ACK=1 disables it here for the A/B.
    let backup_ack = std::env::var("HZ_NO_BACKUP_ACK").as_deref() != Ok("1");
    builder = builder.network(move |n| n.backup_ack_to_client_enabled(backup_ack));
    let config: ClientConfig = builder.build().context("building client config")?;

    let client = Arc::new(
        HazelcastClient::new(config)
            .await
            .context("connecting to cluster")?,
    );

    // Seed the shared dataset (idempotent).
    ops::ensure_seeded(&client, &cell, seed)
        .await
        .context("seeding dataset")?;

    if args.seed_only {
        println!("seeded cell {} (structure={})", cell.id, cell.structure);
        client.shutdown().await.ok();
        return Ok(());
    }

    let warmup = Duration::from_secs(args.warmup_s.unwrap_or(cell.warmup_s));
    let measure = Duration::from_secs(args.measure_s.unwrap_or(cell.measure_s));

    let result = if cell.load_model == "open" {
        let rate = args
            .target_rate
            .or(cell.target_rate)
            .context("open-loop cell requires --target-rate (or cell.target_rate)")?;
        runner::run_open(&client, &cell, seed, warmup, measure, rate).await?
    } else {
        runner::run_closed(&client, &cell, seed, warmup, measure).await?
    };

    let throughput = if result.measure_secs > 0.0 {
        result.ops as f64 / result.measure_secs
    } else {
        0.0
    };

    let rss_peak_kb = read_vmhwm_kb();
    let ts_unix_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();

    let provenance = serde_json::json!({
        "client": "rust",
        "harness_version": env!("CARGO_PKG_VERSION"),
        "commit": args.commit,
        "rustc": option_env!("RUSTC_VERSION").unwrap_or("unknown"),
        "cargo_profile": if cfg!(debug_assertions) { "debug" } else { "release" },
        "cluster_name": args.cluster_name,
        "addresses": addrs.iter().map(|a| a.to_string()).collect::<Vec<_>>(),
        "smart_routing": true,
        "statistics": false,
        "near_cache": false,
        "pid": std::process::id(),
        "manifest_tier": m.meta.tier,
        "manifest_schema": m.meta.schema_version,
    });

    let rec = Record {
        client: "rust".into(),
        version: env!("CARGO_PKG_VERSION").into(),
        commit: args.commit.clone(),
        cell_id: cell.id.clone(),
        suite: cell.suite.clone(),
        structure: cell.structure.clone(),
        op: cell.op.clone(),
        variant: cell.variant.clone(),
        key_type: cell.key_type.clone(),
        value_kind: cell.value_kind.clone(),
        value_size: cell.value_size,
        working_set: cell.working_set,
        concurrency: cell.concurrency,
        load_model: cell.load_model.clone(),
        target_rate: args.target_rate.or(cell.target_rate),
        distribution: cell.distribution.clone(),
        batch_size: cell.batch_size,
        fork: args.fork,
        trial: args.trial,
        ops: result.ops,
        duration_s: result.measure_secs,
        throughput,
        saturated: result.saturated,
        errors: result.errors,
        latency: latency_stats(&result.hist),
        throughput_series: result.series,
        mem: Mem { rss_peak_kb },
        provenance,
        ts_unix_ms,
    };

    let fname = format!(
        "{}/{}.rust.f{}.t{}.json",
        args.out.trim_end_matches('/'),
        cell.id,
        args.fork,
        args.trial
    );
    let json = serde_json::to_string(&rec)?;
    std::fs::write(&fname, &json).with_context(|| format!("writing {fname}"))?;
    if args.stdout {
        println!("{json}");
    }
    eprintln!(
        "[rust] {} f{} t{}: ops={} thr={:.0}/s p50={:.1}us p99={:.1}us p99.9={:.1}us errors={} saturated={}",
        cell.id, args.fork, args.trial, rec.ops, rec.throughput,
        rec.latency.p50, rec.latency.p99, rec.latency.p999, rec.errors.total, rec.saturated
    );

    client.shutdown().await.ok();
    Ok(())
}

/// Peak resident set size (VmHWM) in kB from /proc/self/status (Linux).
fn read_vmhwm_kb() -> u64 {
    if let Ok(s) = std::fs::read_to_string("/proc/self/status") {
        for line in s.lines() {
            if let Some(rest) = line.strip_prefix("VmHWM:") {
                let n: u64 = rest
                    .trim()
                    .split_whitespace()
                    .next()
                    .and_then(|x| x.parse().ok())
                    .unwrap_or(0);
                return n;
            }
        }
    }
    0
}
