//! Comprehensive performance benchmarks for HazelRust Hazelcast client.
//!
//! Run with: cargo test --test comprehensive_perf_test -- --nocapture --test-threads=1
//!
//! Requires a running Hazelcast cluster at 127.0.0.1:5701.
//! Start one with: docker run -d --name hazelcast -p 5701:5701 hazelcast/hazelcast:5.3
//!
//! Each benchmark runs 1000 iterations, measures per-op latency, and prints:
//!   {name}: {ops_sec} ops/s  p50={p50}ms  p99={p99}ms  errors={err_count}

mod common;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use hazelcast_client::{
    ClientConfigBuilder, HazelcastClient, NearCacheConfig,
};

use crate::common::{unique_name, wait_for_cluster_ready, skip_if_no_cluster};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const N: usize = 1000;

/// Collects latencies and prints a single summary line.
fn report(name: &str, latencies: &[f64], errors: usize) {
    if latencies.is_empty() {
        println!("{name}: NO DATA  errors={errors}");
        return;
    }
    let mut sorted = latencies.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let total_ms: f64 = sorted.iter().sum();
    let ops = sorted.len() as f64;
    let ops_sec = ops / (total_ms / 1000.0);
    let p50 = sorted[sorted.len() / 2];
    let p99 = sorted[((sorted.len() as f64) * 0.99) as usize];
    println!("{name}: {ops_sec:.0} ops/s  p50={p50:.2}ms  p99={p99:.2}ms  errors={errors}");
}

/// Generate a value string of exact byte length using repeated 'x'.
fn val(size: usize) -> String {
    "x".repeat(size)
}

// =========================================================================
// IMap benchmarks (20)
// =========================================================================

#[tokio::test]
async fn bench_map_put_64b() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let map_name = unique_name("bench-put64");
    let map = client.get_map::<String, String>(&map_name);
    let value = val(64);

    let mut latencies = Vec::with_capacity(N);
    let mut errors = 0usize;

    for i in 0..N {
        let start = Instant::now();
        match map.put(format!("k{i}"), value.clone()).await {
            Ok(_) => latencies.push(start.elapsed().as_micros() as f64 / 1000.0),
            Err(_) => errors += 1,
        }
    }

    report("bench_map_put_64b", &latencies, errors);
    let _ = map.clear().await;
}

#[tokio::test]
async fn bench_map_put_1kb() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let map_name = unique_name("bench-put1k");
    let map = client.get_map::<String, String>(&map_name);
    let value = val(1024);

    let mut latencies = Vec::with_capacity(N);
    let mut errors = 0usize;

    for i in 0..N {
        let start = Instant::now();
        match map.put(format!("k{i}"), value.clone()).await {
            Ok(_) => latencies.push(start.elapsed().as_micros() as f64 / 1000.0),
            Err(_) => errors += 1,
        }
    }

    report("bench_map_put_1kb", &latencies, errors);
    let _ = map.clear().await;
}

#[tokio::test]
async fn bench_map_put_10kb() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let map_name = unique_name("bench-put10k");
    let map = client.get_map::<String, String>(&map_name);
    let value = val(10240);

    let mut latencies = Vec::with_capacity(N);
    let mut errors = 0usize;

    for i in 0..N {
        let start = Instant::now();
        match map.put(format!("k{i}"), value.clone()).await {
            Ok(_) => latencies.push(start.elapsed().as_micros() as f64 / 1000.0),
            Err(_) => errors += 1,
        }
    }

    report("bench_map_put_10kb", &latencies, errors);
    let _ = map.clear().await;
}

#[tokio::test]
async fn bench_map_get() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let map_name = unique_name("bench-get");
    let map = client.get_map::<String, String>(&map_name);
    let value = val(64);

    // Setup: populate keys
    for i in 0..N {
        map.put(format!("k{i}"), value.clone()).await.unwrap();
    }

    let mut latencies = Vec::with_capacity(N);
    let mut errors = 0usize;

    for i in 0..N {
        let key = format!("k{i}");
        let start = Instant::now();
        match map.get(&key).await {
            Ok(_) => latencies.push(start.elapsed().as_micros() as f64 / 1000.0),
            Err(_) => errors += 1,
        }
    }

    report("bench_map_get", &latencies, errors);
    let _ = map.clear().await;
}

#[tokio::test]
async fn bench_map_get_missing() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let map_name = unique_name("bench-getmiss");
    let map = client.get_map::<String, String>(&map_name);

    let mut latencies = Vec::with_capacity(N);
    let mut errors = 0usize;

    for i in 0..N {
        let key = format!("missing-{i}");
        let start = Instant::now();
        match map.get(&key).await {
            Ok(_) => latencies.push(start.elapsed().as_micros() as f64 / 1000.0),
            Err(_) => errors += 1,
        }
    }

    report("bench_map_get_missing", &latencies, errors);
    let _ = map.clear().await;
}

#[tokio::test]
async fn bench_map_remove() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let map_name = unique_name("bench-remove");
    let map = client.get_map::<String, String>(&map_name);
    let value = val(64);

    // Setup: populate keys
    for i in 0..N {
        map.put(format!("k{i}"), value.clone()).await.unwrap();
    }

    let mut latencies = Vec::with_capacity(N);
    let mut errors = 0usize;

    for i in 0..N {
        let key = format!("k{i}");
        let start = Instant::now();
        match map.remove(&key).await {
            Ok(_) => latencies.push(start.elapsed().as_micros() as f64 / 1000.0),
            Err(_) => errors += 1,
        }
    }

    report("bench_map_remove", &latencies, errors);
    let _ = map.clear().await;
}

#[tokio::test]
async fn bench_map_contains_key() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let map_name = unique_name("bench-contains");
    let map = client.get_map::<String, String>(&map_name);
    let value = val(64);

    // Setup: populate half the keys
    for i in 0..N / 2 {
        map.put(format!("k{i}"), value.clone()).await.unwrap();
    }

    let mut latencies = Vec::with_capacity(N);
    let mut errors = 0usize;

    for i in 0..N {
        let key = format!("k{i}");
        let start = Instant::now();
        match map.contains_key(&key).await {
            Ok(_) => latencies.push(start.elapsed().as_micros() as f64 / 1000.0),
            Err(_) => errors += 1,
        }
    }

    report("bench_map_contains_key", &latencies, errors);
    let _ = map.clear().await;
}

#[tokio::test]
async fn bench_map_size() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let map_name = unique_name("bench-size");
    let map = client.get_map::<String, String>(&map_name);

    // Setup: populate some keys
    for i in 0..100 {
        map.put(format!("k{i}"), val(64)).await.unwrap();
    }

    let mut latencies = Vec::with_capacity(N);
    let mut errors = 0usize;

    for _ in 0..N {
        let start = Instant::now();
        match map.size().await {
            Ok(_) => latencies.push(start.elapsed().as_micros() as f64 / 1000.0),
            Err(_) => errors += 1,
        }
    }

    report("bench_map_size", &latencies, errors);
    let _ = map.clear().await;
}

#[tokio::test]
async fn bench_map_compute() {
    // Atomic read-modify-write via get + replace.
    // Simulates an increment-counter pattern.
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let map_name = unique_name("bench-compute");
    let map = client.get_map::<String, String>(&map_name);

    // Seed a counter key
    map.put("counter".to_string(), "0".to_string()).await.unwrap();

    let mut latencies = Vec::with_capacity(N);
    let mut errors = 0usize;

    for _ in 0..N {
        let start = Instant::now();
        let current = map.get(&"counter".to_string()).await;
        let ok = match current {
            Ok(v) => {
                let cur: i64 = v.unwrap_or_else(|| "0".to_string()).parse().unwrap_or(0);
                map.put("counter".to_string(), (cur + 1).to_string()).await.is_ok()
            }
            Err(_) => false,
        };
        if ok {
            latencies.push(start.elapsed().as_micros() as f64 / 1000.0);
        } else {
            errors += 1;
        }
    }

    report("bench_map_compute", &latencies, errors);
    let _ = map.clear().await;
}

#[tokio::test]
async fn bench_map_set_ttl() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let map_name = unique_name("bench-setttl");
    let map = client.get_map::<String, String>(&map_name);

    // Setup: populate keys
    for i in 0..N {
        map.put(format!("k{i}"), val(64)).await.unwrap();
    }

    let mut latencies = Vec::with_capacity(N);
    let mut errors = 0usize;

    for i in 0..N {
        let key = format!("k{i}");
        let start = Instant::now();
        match map.set_ttl(&key, Duration::from_secs(300)).await {
            Ok(_) => latencies.push(start.elapsed().as_micros() as f64 / 1000.0),
            Err(_) => errors += 1,
        }
    }

    report("bench_map_set_ttl", &latencies, errors);
    let _ = map.clear().await;
}

#[tokio::test]
async fn bench_map_put_all_10() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let map_name = unique_name("bench-putall10");
    let map = client.get_map::<String, String>(&map_name);
    let value = val(64);

    let mut latencies = Vec::with_capacity(N);
    let mut errors = 0usize;

    for batch in 0..N {
        let mut entries = HashMap::new();
        for j in 0..10 {
            entries.insert(format!("k{}-{j}", batch), value.clone());
        }
        let start = Instant::now();
        match map.put_all(entries).await {
            Ok(_) => latencies.push(start.elapsed().as_micros() as f64 / 1000.0),
            Err(_) => errors += 1,
        }
    }

    report("bench_map_put_all_10", &latencies, errors);
    let _ = map.clear().await;
}

#[tokio::test]
async fn bench_map_put_all_100() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let map_name = unique_name("bench-putall100");
    let map = client.get_map::<String, String>(&map_name);
    let value = val(64);

    // Use fewer iterations because each op is bigger (100 entries)
    let iters = 100;
    let mut latencies = Vec::with_capacity(iters);
    let mut errors = 0usize;

    for batch in 0..iters {
        let mut entries = HashMap::new();
        for j in 0..100 {
            entries.insert(format!("k{batch}-{j}"), value.clone());
        }
        let start = Instant::now();
        match map.put_all(entries).await {
            Ok(_) => latencies.push(start.elapsed().as_micros() as f64 / 1000.0),
            Err(_) => errors += 1,
        }
    }

    report("bench_map_put_all_100", &latencies, errors);
    let _ = map.clear().await;
}

#[tokio::test]
async fn bench_map_get_all_10() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let map_name = unique_name("bench-getall10");
    let map = client.get_map::<String, String>(&map_name);

    // Setup: populate 100 keys
    for i in 0..100 {
        map.put(format!("k{i}"), val(64)).await.unwrap();
    }

    let mut latencies = Vec::with_capacity(N);
    let mut errors = 0usize;

    for batch in 0..N {
        let keys: Vec<String> = (0..10).map(|j| format!("k{}", (batch * 10 + j) % 100)).collect();
        let start = Instant::now();
        match map.get_all(&keys).await {
            Ok(_) => latencies.push(start.elapsed().as_micros() as f64 / 1000.0),
            Err(_) => errors += 1,
        }
    }

    report("bench_map_get_all_10", &latencies, errors);
    let _ = map.clear().await;
}

#[tokio::test]
async fn bench_map_get_all_100() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let map_name = unique_name("bench-getall100");
    let map = client.get_map::<String, String>(&map_name);

    // Setup: populate 1000 keys
    for i in 0..1000 {
        map.put(format!("k{i}"), val(64)).await.unwrap();
    }

    let iters = 100;
    let mut latencies = Vec::with_capacity(iters);
    let mut errors = 0usize;

    for batch in 0..iters {
        let keys: Vec<String> = (0..100).map(|j| format!("k{}", (batch * 100 + j) % 1000)).collect();
        let start = Instant::now();
        match map.get_all(&keys).await {
            Ok(_) => latencies.push(start.elapsed().as_micros() as f64 / 1000.0),
            Err(_) => errors += 1,
        }
    }

    report("bench_map_get_all_100", &latencies, errors);
    let _ = map.clear().await;
}

#[tokio::test]
async fn bench_map_key_set_100() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let map_name = unique_name("bench-keyset100");
    let map = client.get_map::<String, String>(&map_name);

    // Setup: populate 100 keys
    for i in 0..100 {
        map.put(format!("k{i}"), val(64)).await.unwrap();
    }

    let iters = 100;
    let mut latencies = Vec::with_capacity(iters);
    let mut errors = 0usize;

    for _ in 0..iters {
        let start = Instant::now();
        match map.key_set().await {
            Ok(iter) => {
                match iter.collect().await {
                    Ok(keys) => {
                        let _ = keys.len();
                        latencies.push(start.elapsed().as_micros() as f64 / 1000.0);
                    }
                    Err(_) => errors += 1,
                }
            }
            Err(_) => errors += 1,
        }
    }

    report("bench_map_key_set_100", &latencies, errors);
    let _ = map.clear().await;
}

#[tokio::test]
async fn bench_map_key_set_1000() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let map_name = unique_name("bench-keyset1k");
    let map = client.get_map::<String, String>(&map_name);

    // Setup: populate 1000 keys
    for i in 0..1000 {
        map.put(format!("k{i}"), val(64)).await.unwrap();
    }

    let iters = 50;
    let mut latencies = Vec::with_capacity(iters);
    let mut errors = 0usize;

    for _ in 0..iters {
        let start = Instant::now();
        match map.key_set().await {
            Ok(iter) => {
                match iter.collect().await {
                    Ok(keys) => {
                        let _ = keys.len();
                        latencies.push(start.elapsed().as_micros() as f64 / 1000.0);
                    }
                    Err(_) => errors += 1,
                }
            }
            Err(_) => errors += 1,
        }
    }

    report("bench_map_key_set_1000", &latencies, errors);
    let _ = map.clear().await;
}

#[tokio::test]
async fn bench_map_entry_set_100() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let map_name = unique_name("bench-entryset100");
    let map = client.get_map::<String, String>(&map_name);

    // Setup: populate 100 entries
    for i in 0..100 {
        map.put(format!("k{i}"), val(64)).await.unwrap();
    }

    let iters = 100;
    let mut latencies = Vec::with_capacity(iters);
    let mut errors = 0usize;

    for _ in 0..iters {
        let start = Instant::now();
        match map.entry_set().await {
            Ok(iter) => {
                match iter.collect_entries().await {
                    Ok(entries) => {
                        let _ = entries.len();
                        latencies.push(start.elapsed().as_micros() as f64 / 1000.0);
                    }
                    Err(_) => errors += 1,
                }
            }
            Err(_) => errors += 1,
        }
    }

    report("bench_map_entry_set_100", &latencies, errors);
    let _ = map.clear().await;
}

#[tokio::test]
async fn bench_map_lock_unlock() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let map_name = unique_name("bench-lock");
    let map = client.get_map::<String, String>(&map_name);

    // Setup: populate a key to lock
    map.put("lockkey".to_string(), val(64)).await.unwrap();

    let mut latencies = Vec::with_capacity(N);
    let mut errors = 0usize;

    for _ in 0..N {
        let key = "lockkey".to_string();
        let start = Instant::now();
        let lock_ok = map.lock(&key).await.is_ok();
        let unlock_ok = if lock_ok {
            map.unlock(&key).await.is_ok()
        } else {
            false
        };
        if lock_ok && unlock_ok {
            latencies.push(start.elapsed().as_micros() as f64 / 1000.0);
        } else {
            errors += 1;
        }
    }

    report("bench_map_lock_unlock", &latencies, errors);
    let _ = map.clear().await;
}

#[tokio::test]
async fn bench_map_delete() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let map_name = unique_name("bench-delete");
    let map = client.get_map::<String, String>(&map_name);

    // Setup: populate keys
    for i in 0..N {
        map.put(format!("k{i}"), val(64)).await.unwrap();
    }

    let mut latencies = Vec::with_capacity(N);
    let mut errors = 0usize;

    for i in 0..N {
        let key = format!("k{i}");
        let start = Instant::now();
        match map.delete(&key).await {
            Ok(_) => latencies.push(start.elapsed().as_micros() as f64 / 1000.0),
            Err(_) => errors += 1,
        }
    }

    report("bench_map_delete", &latencies, errors);
    let _ = map.clear().await;
}

#[tokio::test]
async fn bench_map_replace() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let map_name = unique_name("bench-replace");
    let map = client.get_map::<String, String>(&map_name);
    let value = val(64);

    // Setup: populate keys
    for i in 0..N {
        map.put(format!("k{i}"), value.clone()).await.unwrap();
    }

    let replacement = val(64);
    let mut latencies = Vec::with_capacity(N);
    let mut errors = 0usize;

    for i in 0..N {
        let key = format!("k{i}");
        let start = Instant::now();
        match map.replace(&key, replacement.clone()).await {
            Ok(_) => latencies.push(start.elapsed().as_micros() as f64 / 1000.0),
            Err(_) => errors += 1,
        }
    }

    report("bench_map_replace", &latencies, errors);
    let _ = map.clear().await;
}

// =========================================================================
// ISet benchmarks (4)
// NOTE: ISet may have protocol-level quirks; these establish baselines.
// =========================================================================

#[tokio::test]
async fn bench_set_add() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let set_name = unique_name("bench-setadd");
    let set = client.get_set::<String>(&set_name);

    let mut latencies = Vec::with_capacity(N);
    let mut errors = 0usize;

    for i in 0..N {
        let start = Instant::now();
        match set.add(format!("item{i}")).await {
            Ok(_) => latencies.push(start.elapsed().as_micros() as f64 / 1000.0),
            Err(_) => errors += 1,
        }
    }

    report("bench_set_add", &latencies, errors);
    let _ = set.clear().await;
}

#[tokio::test]
async fn bench_set_contains() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let set_name = unique_name("bench-setcontains");
    let set = client.get_set::<String>(&set_name);

    // Setup: populate set
    for i in 0..N {
        set.add(format!("item{i}")).await.unwrap();
    }

    let mut latencies = Vec::with_capacity(N);
    let mut errors = 0usize;

    for i in 0..N {
        let item = format!("item{i}");
        let start = Instant::now();
        match set.contains(&item).await {
            Ok(_) => latencies.push(start.elapsed().as_micros() as f64 / 1000.0),
            Err(_) => errors += 1,
        }
    }

    report("bench_set_contains", &latencies, errors);
    let _ = set.clear().await;
}

#[tokio::test]
async fn bench_set_size() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let set_name = unique_name("bench-setsize");
    let set = client.get_set::<String>(&set_name);

    // Setup: populate set
    for i in 0..100 {
        set.add(format!("item{i}")).await.unwrap();
    }

    let mut latencies = Vec::with_capacity(N);
    let mut errors = 0usize;

    for _ in 0..N {
        let start = Instant::now();
        match set.size().await {
            Ok(_) => latencies.push(start.elapsed().as_micros() as f64 / 1000.0),
            Err(_) => errors += 1,
        }
    }

    report("bench_set_size", &latencies, errors);
    let _ = set.clear().await;
}

#[tokio::test]
async fn bench_set_get_all() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let set_name = unique_name("bench-setgetall");
    let set = client.get_set::<String>(&set_name);

    // Setup: populate 100 items
    for i in 0..100 {
        set.add(format!("item{i}")).await.unwrap();
    }

    let iters = 100;
    let mut latencies = Vec::with_capacity(iters);
    let mut errors = 0usize;

    for _ in 0..iters {
        let start = Instant::now();
        match set.get_all().await {
            Ok(items) => {
                let _ = items.len();
                latencies.push(start.elapsed().as_micros() as f64 / 1000.0);
            }
            Err(_) => errors += 1,
        }
    }

    report("bench_set_get_all", &latencies, errors);
    let _ = set.clear().await;
}

// =========================================================================
// IList benchmarks (4)
// NOTE: IList may have protocol-level quirks; these establish baselines.
// =========================================================================

#[tokio::test]
async fn bench_list_add() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let list_name = unique_name("bench-listadd");
    let list = client.get_list::<String>(&list_name);

    let mut latencies = Vec::with_capacity(N);
    let mut errors = 0usize;

    for i in 0..N {
        let start = Instant::now();
        match list.add(format!("item{i}")).await {
            Ok(_) => latencies.push(start.elapsed().as_micros() as f64 / 1000.0),
            Err(_) => errors += 1,
        }
    }

    report("bench_list_add", &latencies, errors);
    let _ = list.clear().await;
}

#[tokio::test]
async fn bench_list_get() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let list_name = unique_name("bench-listget");
    let list = client.get_list::<String>(&list_name);

    // Setup: populate list
    for i in 0..N {
        list.add(format!("item{i}")).await.unwrap();
    }

    let mut latencies = Vec::with_capacity(N);
    let mut errors = 0usize;

    for i in 0..N {
        let start = Instant::now();
        match list.get(i).await {
            Ok(_) => latencies.push(start.elapsed().as_micros() as f64 / 1000.0),
            Err(_) => errors += 1,
        }
    }

    report("bench_list_get", &latencies, errors);
    let _ = list.clear().await;
}

#[tokio::test]
async fn bench_list_remove_at() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let list_name = unique_name("bench-listremove");
    let list = client.get_list::<String>(&list_name);

    // Setup: populate list
    for i in 0..N {
        list.add(format!("item{i}")).await.unwrap();
    }

    let mut latencies = Vec::with_capacity(N);
    let mut errors = 0usize;

    // Always remove from index 0 (front) to keep it simple as list shrinks
    for _ in 0..N {
        let start = Instant::now();
        match list.remove_at(0).await {
            Ok(_) => latencies.push(start.elapsed().as_micros() as f64 / 1000.0),
            Err(_) => errors += 1,
        }
    }

    report("bench_list_remove_at", &latencies, errors);
    let _ = list.clear().await;
}

#[tokio::test]
async fn bench_list_size() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let list_name = unique_name("bench-listsize");
    let list = client.get_list::<String>(&list_name);

    // Setup: populate list
    for i in 0..100 {
        list.add(format!("item{i}")).await.unwrap();
    }

    let mut latencies = Vec::with_capacity(N);
    let mut errors = 0usize;

    for _ in 0..N {
        let start = Instant::now();
        match list.size().await {
            Ok(_) => latencies.push(start.elapsed().as_micros() as f64 / 1000.0),
            Err(_) => errors += 1,
        }
    }

    report("bench_list_size", &latencies, errors);
    let _ = list.clear().await;
}

// =========================================================================
// IQueue benchmarks (4)
// NOTE: IQueue may have protocol-level quirks; these establish baselines.
// =========================================================================

#[tokio::test]
async fn bench_queue_offer() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let queue_name = unique_name("bench-qoffer");
    let queue = client.get_queue::<String>(&queue_name);

    let mut latencies = Vec::with_capacity(N);
    let mut errors = 0usize;

    for i in 0..N {
        let start = Instant::now();
        match queue.offer(format!("item{i}")).await {
            Ok(_) => latencies.push(start.elapsed().as_micros() as f64 / 1000.0),
            Err(_) => errors += 1,
        }
    }

    report("bench_queue_offer", &latencies, errors);
    // Drain the queue
    while queue.poll().await.ok().flatten().is_some() {}
}

#[tokio::test]
async fn bench_queue_poll() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let queue_name = unique_name("bench-qpoll");
    let queue = client.get_queue::<String>(&queue_name);

    // Setup: populate queue
    for i in 0..N {
        queue.offer(format!("item{i}")).await.unwrap();
    }

    let mut latencies = Vec::with_capacity(N);
    let mut errors = 0usize;

    for _ in 0..N {
        let start = Instant::now();
        match queue.poll().await {
            Ok(_) => latencies.push(start.elapsed().as_micros() as f64 / 1000.0),
            Err(_) => errors += 1,
        }
    }

    report("bench_queue_poll", &latencies, errors);
}

#[tokio::test]
async fn bench_queue_peek() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let queue_name = unique_name("bench-qpeek");
    let queue = client.get_queue::<String>(&queue_name);

    // Setup: populate one item to peek at
    queue.offer("peek-item".to_string()).await.unwrap();

    let mut latencies = Vec::with_capacity(N);
    let mut errors = 0usize;

    for _ in 0..N {
        let start = Instant::now();
        match queue.peek().await {
            Ok(_) => latencies.push(start.elapsed().as_micros() as f64 / 1000.0),
            Err(_) => errors += 1,
        }
    }

    report("bench_queue_peek", &latencies, errors);
    while queue.poll().await.ok().flatten().is_some() {}
}

#[tokio::test]
async fn bench_queue_size() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let queue_name = unique_name("bench-qsize");
    let queue = client.get_queue::<String>(&queue_name);

    // Setup: populate queue
    for i in 0..100 {
        queue.offer(format!("item{i}")).await.unwrap();
    }

    let mut latencies = Vec::with_capacity(N);
    let mut errors = 0usize;

    for _ in 0..N {
        let start = Instant::now();
        match queue.size().await {
            Ok(_) => latencies.push(start.elapsed().as_micros() as f64 / 1000.0),
            Err(_) => errors += 1,
        }
    }

    report("bench_queue_size", &latencies, errors);
    while queue.poll().await.ok().flatten().is_some() {}
}

// =========================================================================
// AtomicLong benchmarks (3)
// =========================================================================

#[tokio::test]
async fn bench_atomic_increment() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let counter_name = unique_name("bench-atominc");
    let counter = client.get_atomic_long(&counter_name);
    counter.set(0).await.unwrap();

    let mut latencies = Vec::with_capacity(N);
    let mut errors = 0usize;

    for _ in 0..N {
        let start = Instant::now();
        match counter.increment_and_get().await {
            Ok(_) => latencies.push(start.elapsed().as_micros() as f64 / 1000.0),
            Err(_) => errors += 1,
        }
    }

    report("bench_atomic_increment", &latencies, errors);
}

#[tokio::test]
async fn bench_atomic_compare_and_set() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let counter_name = unique_name("bench-atomcas");
    let counter = client.get_atomic_long(&counter_name);
    counter.set(0).await.unwrap();

    let mut latencies = Vec::with_capacity(N);
    let mut errors = 0usize;

    for i in 0..N {
        let expected = i as i64;
        let start = Instant::now();
        match counter.compare_and_set(expected, expected + 1).await {
            Ok(_) => latencies.push(start.elapsed().as_micros() as f64 / 1000.0),
            Err(_) => errors += 1,
        }
    }

    report("bench_atomic_compare_and_set", &latencies, errors);
}

#[tokio::test]
async fn bench_atomic_get() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let counter_name = unique_name("bench-atomget");
    let counter = client.get_atomic_long(&counter_name);
    counter.set(42).await.unwrap();

    let mut latencies = Vec::with_capacity(N);
    let mut errors = 0usize;

    for _ in 0..N {
        let start = Instant::now();
        match counter.get().await {
            Ok(_) => latencies.push(start.elapsed().as_micros() as f64 / 1000.0),
            Err(_) => errors += 1,
        }
    }

    report("bench_atomic_get", &latencies, errors);
}

// =========================================================================
// Cross-cutting benchmarks (5)
// =========================================================================

#[tokio::test]
async fn bench_near_cache_hits() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let map_name = unique_name("bench-nc-hits");

    let near_cache_config = NearCacheConfig::builder(&map_name)
        .max_size(1000)
        .time_to_live(Duration::from_secs(300))
        .build()
        .unwrap();

    let config = ClientConfigBuilder::new()
        .cluster_name("dev")
        .add_address(common::DEFAULT_CLUSTER_ADDRESS.parse().unwrap())
        .add_near_cache_config(near_cache_config)
        .build()
        .unwrap();

    let client = HazelcastClient::new(config).await.expect("failed to connect");
    let map = client.get_map::<String, String>(&map_name);

    // Setup: put 100 keys and do a first get to populate near cache
    for i in 0..100 {
        map.put(format!("k{i}"), val(64)).await.unwrap();
    }
    // Prime the near cache with initial gets
    for i in 0..100 {
        let _ = map.get(&format!("k{i}")).await;
    }

    // Now benchmark: all gets should hit the near cache
    let mut latencies = Vec::with_capacity(N);
    let mut errors = 0usize;

    for i in 0..N {
        let key = format!("k{}", i % 100);
        let start = Instant::now();
        match map.get(&key).await {
            Ok(_) => latencies.push(start.elapsed().as_micros() as f64 / 1000.0),
            Err(_) => errors += 1,
        }
    }

    let stats = map.near_cache_stats();
    let hits = stats.as_ref().map(|s| s.hits()).unwrap_or(0);
    let misses = stats.as_ref().map(|s| s.misses()).unwrap_or(0);

    report("bench_near_cache_hits", &latencies, errors);
    println!("  near cache: hits={hits}  misses={misses}");
    let _ = map.clear().await;
}

#[tokio::test]
async fn bench_concurrent_4_tasks() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = Arc::new(
        HazelcastClient::new(common::default_config())
            .await.expect("failed to connect")
    );
    let map_name = unique_name("bench-concurrent4");

    let total_ops = N; // 1000 total: 250 per task
    let ops_per_task = total_ops / 4;
    let value = val(64);

    let overall_start = Instant::now();
    let mut handles = Vec::new();

    for task_id in 0..4u32 {
        let client_clone = Arc::clone(&client);
        let map_name_clone = map_name.clone();
        let value_clone = value.clone();

        let handle = tokio::spawn(async move {
            let map = client_clone.get_map::<String, String>(&map_name_clone);
            let mut task_latencies = Vec::with_capacity(ops_per_task);
            let mut task_errors = 0usize;

            for i in 0..ops_per_task {
                let key = format!("t{task_id}-k{i}");
                // PUT
                let start = Instant::now();
                match map.put(key.clone(), value_clone.clone()).await {
                    Ok(_) => task_latencies.push(start.elapsed().as_micros() as f64 / 1000.0),
                    Err(_) => task_errors += 1,
                }
                // GET
                let start = Instant::now();
                match map.get(&key).await {
                    Ok(_) => task_latencies.push(start.elapsed().as_micros() as f64 / 1000.0),
                    Err(_) => task_errors += 1,
                }
            }
            (task_latencies, task_errors)
        });

        handles.push(handle);
    }

    let mut all_latencies = Vec::new();
    let mut total_errors = 0usize;

    for handle in handles {
        let (lats, errs) = handle.await.unwrap();
        all_latencies.extend(lats);
        total_errors += errs;
    }

    let wall_time = overall_start.elapsed();
    let total_ops_done = all_latencies.len();
    let wall_ops_sec = total_ops_done as f64 / wall_time.as_secs_f64();

    report("bench_concurrent_4_tasks", &all_latencies, total_errors);
    println!("  wall-clock throughput: {wall_ops_sec:.0} ops/s  ({total_ops_done} ops in {:.2}s)",
             wall_time.as_secs_f64());

    let map = client.get_map::<String, String>(&map_name);
    let _ = map.clear().await;
}

#[tokio::test]
async fn bench_mixed_workload_70_20_10() {
    // 70% get, 20% put, 10% delete -- 1000 ops total
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let map_name = unique_name("bench-mixed");
    let map = client.get_map::<String, String>(&map_name);
    let value = val(64);

    // Setup: seed 500 keys so gets/deletes have data
    for i in 0..500 {
        map.put(format!("k{i}"), value.clone()).await.unwrap();
    }

    let mut latencies = Vec::with_capacity(N);
    let mut errors = 0usize;
    let mut put_count = 0usize;
    let mut next_key = 500usize;

    for i in 0..N {
        let pct = i % 10;
        if pct < 7 {
            // GET (70%)
            let key = format!("k{}", i % 500);
            let start = Instant::now();
            match map.get(&key).await {
                Ok(_) => latencies.push(start.elapsed().as_micros() as f64 / 1000.0),
                Err(_) => errors += 1,
            }
        } else if pct < 9 {
            // PUT (20%)
            let key = format!("k{next_key}");
            next_key += 1;
            put_count += 1;
            let start = Instant::now();
            match map.put(key, value.clone()).await {
                Ok(_) => latencies.push(start.elapsed().as_micros() as f64 / 1000.0),
                Err(_) => errors += 1,
            }
        } else {
            // DELETE (10%)
            let key = format!("k{}", i % 500);
            let start = Instant::now();
            match map.delete(&key).await {
                Ok(_) => latencies.push(start.elapsed().as_micros() as f64 / 1000.0),
                Err(_) => errors += 1,
            }
        }
    }

    report("bench_mixed_workload_70_20_10", &latencies, errors);
    println!("  mix: gets=700 puts={put_count} deletes={}", N - 700 - put_count);
    let _ = map.clear().await;
}

#[tokio::test]
async fn bench_large_key_set_iteration() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let map_name = unique_name("bench-largeks");
    let map = client.get_map::<String, String>(&map_name);

    // Setup: populate 5000 keys
    let value = val(64);
    for i in 0..5000 {
        map.put(format!("k{i}"), value.clone()).await.unwrap();
    }

    let iters = 20;
    let mut latencies = Vec::with_capacity(iters);
    let mut errors = 0usize;

    for _ in 0..iters {
        let start = Instant::now();
        match map.key_set().await {
            Ok(iter) => {
                match iter.collect().await {
                    Ok(keys) => {
                        assert!(keys.len() >= 4000, "Expected ~5000 keys, got {}", keys.len());
                        latencies.push(start.elapsed().as_micros() as f64 / 1000.0);
                    }
                    Err(_) => errors += 1,
                }
            }
            Err(_) => errors += 1,
        }
    }

    report("bench_large_key_set_iteration", &latencies, errors);
    let _ = map.clear().await;
}

#[tokio::test]
async fn bench_pipeline_style_batch() {
    // Fire 100 puts concurrently via join_all, then collect all.
    // Simulates pipeline-style batching.
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = Arc::new(
        HazelcastClient::new(common::default_config())
            .await.expect("failed to connect")
    );
    let map_name = unique_name("bench-pipeline");
    let value = val(64);

    let iters = 100; // 100 batches of 100 ops = 10,000 ops total
    let batch_size = 100;
    let mut latencies = Vec::with_capacity(iters);
    let mut errors = 0usize;

    for batch in 0..iters {
        let map = client.get_map::<String, String>(&map_name);
        let start = Instant::now();

        let futures: Vec<_> = (0..batch_size)
            .map(|j| {
                let map = map.clone();
                let val = value.clone();
                let key = format!("b{batch}-k{j}");
                async move { map.put(key, val).await }
            })
            .collect();

        let results = futures::future::join_all(futures).await;
        let batch_errors = results.iter().filter(|r| r.is_err()).count();

        if batch_errors == 0 {
            latencies.push(start.elapsed().as_micros() as f64 / 1000.0);
        } else {
            errors += batch_errors;
        }
    }

    report("bench_pipeline_style_batch", &latencies, errors);
    println!("  (each measurement = {batch_size} concurrent puts)");
    let map = client.get_map::<String, String>(&map_name);
    let _ = map.clear().await;
}
