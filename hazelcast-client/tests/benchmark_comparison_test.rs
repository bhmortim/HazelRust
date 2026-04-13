//! HazelRust vs Java Benchmark Suite — Locked 43-scenario matrix.
//!
//! Both Rust and Java clients run the EXACT SAME scenarios so every
//! data point is directly comparable. No orphan scenarios.
//!
//! Run: cargo test --release --test benchmark_comparison_test -- --nocapture --test-threads=1
//! Env: HZ_ADDRESS=10.0.1.11:5701 HZ_CLUSTER=patina-test

mod common;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use hazelcast_client::{ClientConfigBuilder, HazelcastClient};
use crate::common::unique_name;

// ---------------------------------------------------------------------------
// Result + helpers
// ---------------------------------------------------------------------------

#[derive(serde::Serialize, Clone, Debug)]
struct BenchmarkResult {
    client: String,
    data_structure: String,
    operation: String,
    scale: usize,
    concurrency: usize,
    value_size_bytes: usize,
    near_cache: bool,
    ops_per_sec: f64,
    p50_ms: f64,
    p99_ms: f64,
    errors: usize,
    total_ops: usize,
    wall_time_ms: f64,
}

fn compute_result(
    ds: &str, op: &str, scale: usize, conc: usize, val: usize,
    lats: &[f64], errors: usize, wall_ms: f64,
) -> BenchmarkResult {
    let mut sorted = lats.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = sorted.len() as f64;
    let total_ms: f64 = sorted.iter().sum();
    let ops_sec = if total_ms > 0.0 { n / (total_ms / 1000.0) } else { 0.0 };
    let p50 = if !sorted.is_empty() { sorted[sorted.len() / 2] } else { 0.0 };
    let p99 = if !sorted.is_empty() { sorted[(n * 0.99) as usize % sorted.len()] } else { 0.0 };
    BenchmarkResult {
        client: "rust".into(), data_structure: ds.into(), operation: op.into(),
        scale, concurrency: conc, value_size_bytes: val, near_cache: false,
        ops_per_sec: ops_sec, p50_ms: p50, p99_ms: p99, errors,
        total_ops: lats.len(), wall_time_ms: wall_ms,
    }
}

fn val(size: usize) -> String { "x".repeat(size) }

// Measure N single-threaded operations, return (latencies_ms, errors)
async fn measure<F, Fut>(n: usize, mut f: F) -> (Vec<f64>, usize)
where
    F: FnMut(usize) -> Fut,
    Fut: std::future::Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>>,
{
    let mut lats = Vec::with_capacity(n);
    let mut errs = 0usize;
    for i in 0..n {
        let s = Instant::now();
        match f(i).await {
            Ok(_) => lats.push(s.elapsed().as_micros() as f64 / 1000.0),
            Err(_) => errs += 1,
        }
    }
    (lats, errs)
}

// ---------------------------------------------------------------------------
// Individual benchmark functions
// ---------------------------------------------------------------------------

async fn bench_map_op(client: &HazelcastClient, op: &str, scale: usize, conc: usize, vsize: usize) -> BenchmarkResult {
    let name = unique_name(&format!("bm-{op}"));
    let map = client.get_map::<String, String>(&name);
    let v = val(vsize);

    // Seed data for read/mutate operations
    let needs_seed = matches!(op, "get" | "remove" | "replace" | "contains_key" | "delete"
        | "set_ttl" | "lock_unlock" | "size" | "keys" | "entries" | "get_all(10)" | "get_all(100)");
    if needs_seed {
        for i in 0..scale { let _ = map.put(format!("k{i}"), v.clone()).await; }
    }

    // Warmup (20 iterations)
    for i in 0..20 {
        let k = format!("w{i}");
        match op {
            "put" => { let _ = map.put(k, v.clone()).await; },
            "get" => { let _ = map.get(&format!("k{}", i % scale.max(1))).await; },
            _ => { let _ = map.put(k, v.clone()).await; },
        }
    }
    let _ = map.remove(&"w0".to_string()).await; // cleanup warmup

    let wall_start = Instant::now();

    let (lats, errs) = if conc <= 1 {
        match op {
            "put" => measure(scale, |i| { let m = &map; let vv = v.clone(); Box::pin(async move { m.put(format!("k{i}"), vv).await.map(|_|()).map_err(|e| Box::new(e) as _) }) }).await,
            "get" => measure(scale, |i| { let m = &map; let s = scale; Box::pin(async move { m.get(&format!("k{}", i % s)).await.map(|_|()).map_err(|e| Box::new(e) as _) }) }).await,
            "remove" => measure(scale, |i| { let m = &map; Box::pin(async move { m.remove(&format!("k{i}")).await.map(|_|()).map_err(|e| Box::new(e) as _) }) }).await,
            "replace" => measure(scale, |i| { let m = &map; let vv = v.clone(); Box::pin(async move { m.replace(&format!("k{}", i % scale), vv).await.map(|_|()).map_err(|e| Box::new(e) as _) }) }).await,
            "contains_key" => measure(scale, |i| { let m = &map; Box::pin(async move { m.contains_key(&format!("k{i}")).await.map(|_|()).map_err(|e| Box::new(e) as _) }) }).await,
            "delete" => measure(scale, |i| { let m = &map; Box::pin(async move { m.delete(&format!("k{i}")).await.map(|_|()).map_err(|e| Box::new(e) as _) }) }).await,
            "set_ttl" => measure(scale, |i| { let m = &map; Box::pin(async move { m.set_ttl(&format!("k{i}"), Duration::from_secs(300)).await.map(|_|()).map_err(|e| Box::new(e) as _) }) }).await,
            "lock_unlock" => measure(scale, |i| { let m = &map; let s = scale; Box::pin(async move {
                let k = format!("k{}", i % s);
                m.lock(&k).await.map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
                m.unlock(&k).await.map_err(|e| Box::new(e) as _)
            }) }).await,
            "size" => { let mut lats = Vec::new(); let mut errs = 0;
                for _ in 0..scale { let s = Instant::now(); match map.size().await { Ok(_) => lats.push(s.elapsed().as_micros() as f64 / 1000.0), Err(_) => errs += 1 } }
                (lats, errs) },
            "keys" => { let iters = 10.max(1000 / scale.max(1)); let mut lats = Vec::new(); let mut errs = 0;
                for _ in 0..iters { let s = Instant::now(); match map.keys().await { Ok(_) => lats.push(s.elapsed().as_micros() as f64 / 1000.0), Err(_) => errs += 1 } }
                (lats, errs) },
            "entries" => { let iters = 10.max(1000 / scale.max(1)); let mut lats = Vec::new(); let mut errs = 0;
                for _ in 0..iters { let s = Instant::now(); match map.entries().await { Ok(_) => lats.push(s.elapsed().as_micros() as f64 / 1000.0), Err(_) => errs += 1 } }
                (lats, errs) },
            "put_all(10)" => { let nb = scale / 10; let mut lats = Vec::new(); let mut errs = 0;
                for b in 0..nb { let batch: HashMap<String,String> = (0..10).map(|i| (format!("k{}", b*10+i), val(vsize))).collect();
                    let s = Instant::now(); match map.put_all(batch).await { Ok(_) => lats.push(s.elapsed().as_micros() as f64 / 1000.0), Err(_) => errs += 1 } }
                (lats, errs) },
            "put_all(100)" => { let nb = scale / 100; let mut lats = Vec::new(); let mut errs = 0;
                for b in 0..nb { let batch: HashMap<String,String> = (0..100).map(|i| (format!("k{}", b*100+i), val(vsize))).collect();
                    let s = Instant::now(); match map.put_all(batch).await { Ok(_) => lats.push(s.elapsed().as_micros() as f64 / 1000.0), Err(_) => errs += 1 } }
                (lats, errs) },
            "get_all(10)" => { let nb = scale / 10; let mut lats = Vec::new(); let mut errs = 0;
                for b in 0..nb { let keys: Vec<String> = (0..10).map(|i| format!("k{}", b*10+i)).collect();
                    let s = Instant::now(); match map.get_all(&keys).await { Ok(_) => lats.push(s.elapsed().as_micros() as f64 / 1000.0), Err(_) => errs += 1 } }
                (lats, errs) },
            "get_all(100)" => { let nb = scale / 100; let mut lats = Vec::new(); let mut errs = 0;
                for b in 0..nb { let keys: Vec<String> = (0..100).map(|i| format!("k{}", b*100+i)).collect();
                    let s = Instant::now(); match map.get_all(&keys).await { Ok(_) => lats.push(s.elapsed().as_micros() as f64 / 1000.0), Err(_) => errs += 1 } }
                (lats, errs) },
            "mixed" => { let mut lats = Vec::new(); let mut errs = 0;
                for i in 0..scale { let pct = i % 10; let k = format!("k{}", i % scale);
                    let s = Instant::now();
                    let ok = if pct < 7 { map.get(&k).await.is_ok() } else if pct < 9 { map.put(k, val(64)).await.is_ok() } else { map.delete(&k).await.is_ok() };
                    if ok { lats.push(s.elapsed().as_micros() as f64 / 1000.0); } else { errs += 1; } }
                (lats, errs) },
            _ => (vec![], 0),
        }
    } else {
        // Concurrent: split work across N tokio tasks
        let map = Arc::new(map);
        let per = scale / conc;
        let mut handles = Vec::new();
        for t in 0..conc {
            let m = Arc::clone(&map);
            let vv = v.clone();
            let o = op.to_string();
            let off = t * per;
            let sc = scale;
            handles.push(tokio::spawn(async move {
                let mut lats = Vec::with_capacity(per);
                let mut errs = 0;
                for i in 0..per {
                    let idx = off + i;
                    let k = format!("k{}", idx % sc);
                    let s = Instant::now();
                    let ok = match o.as_str() {
                        "put" => m.put(k, vv.clone()).await.is_ok(),
                        "get" => m.get(&k).await.is_ok(),
                        "mixed" => { let pct = idx % 10; if pct < 7 { m.get(&k).await.is_ok() } else if pct < 9 { m.put(k, val(64)).await.is_ok() } else { m.delete(&k).await.is_ok() } },
                        _ => false,
                    };
                    if ok { lats.push(s.elapsed().as_micros() as f64 / 1000.0); } else { errs += 1; }
                }
                (lats, errs)
            }));
        }
        let mut all_lats = Vec::new();
        let mut all_errs = 0;
        for h in handles { let (l, e) = h.await.unwrap(); all_lats.extend(l); all_errs += e; }
        (all_lats, all_errs)
    };

    let wall_ms = wall_start.elapsed().as_micros() as f64 / 1000.0;
    let cleanup = client.get_map::<String, String>(&name);
    let _ = cleanup.clear().await;
    compute_result("IMap", op, scale, conc, vsize, &lats, errs, wall_ms)
}

async fn bench_queue_op(client: &HazelcastClient, op: &str, scale: usize) -> BenchmarkResult {
    let name = unique_name(&format!("bm-q{op}"));
    let queue = client.get_queue::<String>(&name);

    if op == "poll" || op == "peek" {
        for i in 0..scale { let _ = queue.offer(format!("v{i}")).await; }
    }

    let wall_start = Instant::now();
    let mut lats = Vec::with_capacity(scale);
    let mut errs = 0;
    for i in 0..scale {
        let s = Instant::now();
        let ok = match op {
            "offer" => queue.offer(format!("v{i}")).await.is_ok(),
            "poll" => queue.poll().await.is_ok(),
            "peek" => queue.peek().await.is_ok(),
            _ => false,
        };
        if ok { lats.push(s.elapsed().as_micros() as f64 / 1000.0); } else { errs += 1; }
    }
    let wall_ms = wall_start.elapsed().as_micros() as f64 / 1000.0;
    // drain
    while queue.poll().await.ok().flatten().is_some() {}
    compute_result("IQueue", op, scale, 1, 64, &lats, errs, wall_ms)
}

async fn bench_set_op(client: &HazelcastClient, op: &str, scale: usize) -> BenchmarkResult {
    let name = unique_name(&format!("bm-s{op}"));
    let set = client.get_set::<String>(&name);

    if op == "contains" || op == "remove" {
        for i in 0..scale { let _ = set.add(format!("v{i}")).await; }
    }

    let wall_start = Instant::now();
    let mut lats = Vec::with_capacity(scale);
    let mut errs = 0;
    for i in 0..scale {
        let s = Instant::now();
        let ok = match op {
            "add" => set.add(format!("v{i}")).await.is_ok(),
            "contains" => set.contains(&format!("v{i}")).await.is_ok(),
            "remove" => set.remove(&format!("v{i}")).await.is_ok(),
            _ => false,
        };
        if ok { lats.push(s.elapsed().as_micros() as f64 / 1000.0); } else { errs += 1; }
    }
    let wall_ms = wall_start.elapsed().as_micros() as f64 / 1000.0;
    let _ = set.clear().await;
    compute_result("ISet", op, scale, 1, 64, &lats, errs, wall_ms)
}

async fn bench_list_op(client: &HazelcastClient, op: &str, scale: usize) -> BenchmarkResult {
    let name = unique_name(&format!("bm-l{op}"));
    let list = client.get_list::<String>(&name);

    if op == "get" {
        for i in 0..scale { let _ = list.add(format!("v{i}")).await; }
    }

    let wall_start = Instant::now();
    let mut lats = Vec::with_capacity(scale);
    let mut errs = 0;
    for i in 0..scale {
        let s = Instant::now();
        let ok = match op {
            "add" => list.add(format!("v{i}")).await.is_ok(),
            "get" => list.get(i).await.is_ok(),
            _ => false,
        };
        if ok { lats.push(s.elapsed().as_micros() as f64 / 1000.0); } else { errs += 1; }
    }
    let wall_ms = wall_start.elapsed().as_micros() as f64 / 1000.0;
    let _ = list.clear().await;
    compute_result("IList", op, scale, 1, 64, &lats, errs, wall_ms)
}

// ---------------------------------------------------------------------------
// Main — locked 43-scenario matrix
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn run_full_benchmark_suite() {
    let cluster_addr = std::env::var("HZ_ADDRESS").unwrap_or_else(|_| "127.0.0.1:5701".to_string());
    let cluster_name = std::env::var("HZ_CLUSTER").unwrap_or_else(|_| "dev".to_string());
    let addr: std::net::SocketAddr = cluster_addr.parse().expect("invalid HZ_ADDRESS");
    if std::net::TcpStream::connect_timeout(&addr, Duration::from_secs(5)).is_err() {
        eprintln!("Skipping: cluster not available at {}", cluster_addr);
        return;
    }
    println!("Connecting to '{}' at {}...", cluster_name, cluster_addr);

    let config = ClientConfigBuilder::new()
        .cluster_name(&cluster_name)
        .add_address(addr)
        .build().expect("config");
    let client = HazelcastClient::new(config).await.expect("connect");
    println!("Connected. Running 43-scenario benchmark suite.\n");

    let mut results: Vec<BenchmarkResult> = Vec::new();

    // Helper to run and record
    macro_rules! run {
        ($r:expr) => { let r = $r; println!("  {}/{} s={} c={} v={}B: {:.0} ops/s p50={:.2}ms p99={:.2}ms err={}",
            r.data_structure, r.operation, r.scale, r.concurrency, r.value_size_bytes,
            r.ops_per_sec, r.p50_ms, r.p99_ms, r.errors); results.push(r); };
    }

    // ===================================================================
    // SECTION 1: IMap core operations (scale=1000, val=64, conc=1) — 15
    // ===================================================================
    println!("=== IMap Core Operations (1K entries, single-threaded) ===");
    for op in &["put","get","remove","replace","contains_key","delete","set_ttl",
                "lock_unlock","size","keys","entries",
                "put_all(10)","put_all(100)","get_all(10)","get_all(100)"] {
        run!(bench_map_op(&client, op, 1000, 1, 64).await);
    }

    // ===================================================================
    // SECTION 2: IMap scaling (conc=1, val=64) — 4
    // ===================================================================
    println!("\n=== IMap Scaling ===");
    for &scale in &[100, 10_000] {
        run!(bench_map_op(&client, "put", scale, 1, 64).await);
        run!(bench_map_op(&client, "get", scale, 1, 64).await);
    }

    // ===================================================================
    // SECTION 3: IMap concurrency (scale=1000, val=64) — 6
    // ===================================================================
    println!("\n=== IMap Concurrency Scaling ===");
    for &conc in &[4, 8, 16] {
        run!(bench_map_op(&client, "put", 1000, conc, 64).await);
        run!(bench_map_op(&client, "get", 1000, conc, 64).await);
    }

    // ===================================================================
    // SECTION 4: IMap value sizes (scale=1000, conc=1) — 4
    // ===================================================================
    println!("\n=== IMap Value Size Impact ===");
    for &vs in &[1024, 10240] {
        run!(bench_map_op(&client, "put", 1000, 1, vs).await);
        run!(bench_map_op(&client, "get", 1000, 1, vs).await);
    }

    // ===================================================================
    // SECTION 5: IMap mixed workload (scale=1000, val=64) — 4
    // ===================================================================
    println!("\n=== IMap Mixed Workload (70/20/10) ===");
    for &conc in &[1, 4, 8, 16] {
        run!(bench_map_op(&client, "mixed", 1000, conc, 64).await);
    }

    // ===================================================================
    // SECTION 6: Other data structures (scale=1000, val=64, conc=1) — 8
    // ===================================================================
    println!("\n=== IQueue ===");
    for op in &["offer","poll","peek"] {
        run!(bench_queue_op(&client, op, 1000).await);
    }

    println!("\n=== ISet ===");
    for op in &["add","contains","remove"] {
        run!(bench_set_op(&client, op, 1000).await);
    }

    println!("\n=== IList ===");
    for op in &["add","get"] {
        run!(bench_list_op(&client, op, 1000).await);
    }

    // ===================================================================
    // Summary + JSON output
    // ===================================================================
    println!("\n=== Summary ===");
    println!("Total scenarios: {}", results.len());
    println!("Total errors: {}", results.iter().map(|r| r.errors).sum::<usize>());

    let json = serde_json::to_string_pretty(&results).expect("json");
    let out = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..").join("benchmarks").join("results").join("rust_results.json");
    if let Some(p) = out.parent() { std::fs::create_dir_all(p).ok(); }
    std::fs::write(&out, &json).expect("write json");
    println!("Results: {}", out.display());
}
