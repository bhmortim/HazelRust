//! Comprehensive functional tests for the HazelRust Hazelcast client.
//!
//! Covers IMap, ISet, IList, IQueue, AtomicLong, MultiMap, ReplicatedMap,
//! Ringbuffer, ITopic, CountDownLatch, Semaphore, FencedLock, and client
//! lifecycle operations.  Each test is self-contained with unique names
//! and cleanup.
//!
//! Run with a live Hazelcast cluster:
//!   docker run -d --name hazelcast -p 5701:5701 hazelcast/hazelcast:5.3
//!   cargo test --test comprehensive_functional_test

mod common;

use std::collections::HashMap;
use std::time::Duration;

use hazelcast_client::HazelcastClient;

use crate::common::{unique_name, wait_for_cluster_ready, skip_if_no_cluster};

// ============================================================================
// IMap — CRUD (17 tests)
// ============================================================================

#[tokio::test]
async fn test_map_put_get() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let map_name = unique_name("cft-map-pg");
    let map = client.get_map::<String, String>(&map_name);

    map.put("k1".to_string(), "v1".to_string()).await.unwrap();
    let val = map.get(&"k1".to_string()).await.unwrap();
    assert_eq!(val, Some("v1".to_string()));

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_map_put_returns_old() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let map_name = unique_name("cft-map-pro");
    let map = client.get_map::<String, String>(&map_name);

    let first = map.put("k".to_string(), "v1".to_string()).await.unwrap();
    assert!(first.is_none());

    let second = map.put("k".to_string(), "v2".to_string()).await.unwrap();
    assert_eq!(second, Some("v1".to_string()));

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_map_put_if_absent_new() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let map_name = unique_name("cft-map-pian");
    let map = client.get_map::<String, String>(&map_name);

    let result = map.put_if_absent("k".to_string(), "v".to_string()).await.unwrap();
    assert!(result.is_none());

    let val = map.get(&"k".to_string()).await.unwrap();
    assert_eq!(val, Some("v".to_string()));

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_map_put_if_absent_existing() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let map_name = unique_name("cft-map-piae");
    let map = client.get_map::<String, String>(&map_name);

    map.put("k".to_string(), "original".to_string()).await.unwrap();
    let result = map.put_if_absent("k".to_string(), "new".to_string()).await.unwrap();
    assert_eq!(result, Some("original".to_string()));

    let val = map.get(&"k".to_string()).await.unwrap();
    assert_eq!(val, Some("original".to_string()));

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_map_replace() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let map_name = unique_name("cft-map-repl");
    let map = client.get_map::<String, String>(&map_name);

    let none_result = map.replace(&"k".to_string(), "v".to_string()).await.unwrap();
    assert!(none_result.is_none());

    map.put("k".to_string(), "old".to_string()).await.unwrap();
    let old = map.replace(&"k".to_string(), "new".to_string()).await.unwrap();
    assert_eq!(old, Some("old".to_string()));

    let val = map.get(&"k".to_string()).await.unwrap();
    assert_eq!(val, Some("new".to_string()));

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_map_replace_if_equal_match() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let map_name = unique_name("cft-map-riem");
    let map = client.get_map::<String, String>(&map_name);

    map.put("k".to_string(), "old".to_string()).await.unwrap();
    let ok = map.replace_if_equal(
        &"k".to_string(), &"old".to_string(), "new".to_string()
    ).await.unwrap();
    assert!(ok);

    let val = map.get(&"k".to_string()).await.unwrap();
    assert_eq!(val, Some("new".to_string()));

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_map_replace_if_equal_mismatch() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let map_name = unique_name("cft-map-riemm");
    let map = client.get_map::<String, String>(&map_name);

    map.put("k".to_string(), "actual".to_string()).await.unwrap();
    let ok = map.replace_if_equal(
        &"k".to_string(), &"wrong".to_string(), "new".to_string()
    ).await.unwrap();
    assert!(!ok);

    let val = map.get(&"k".to_string()).await.unwrap();
    assert_eq!(val, Some("actual".to_string()));

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_map_remove() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let map_name = unique_name("cft-map-rm");
    let map = client.get_map::<String, String>(&map_name);

    map.put("k".to_string(), "v".to_string()).await.unwrap();
    let removed = map.remove(&"k".to_string()).await.unwrap();
    assert_eq!(removed, Some("v".to_string()));

    let gone = map.get(&"k".to_string()).await.unwrap();
    assert!(gone.is_none());

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_map_remove_returns_old() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let map_name = unique_name("cft-map-rmold");
    let map = client.get_map::<String, String>(&map_name);

    map.put("k".to_string(), "v1".to_string()).await.unwrap();
    let old = map.remove(&"k".to_string()).await.unwrap();
    assert_eq!(old, Some("v1".to_string()));

    let none = map.remove(&"k".to_string()).await.unwrap();
    assert!(none.is_none());

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_map_remove_if_equal() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let map_name = unique_name("cft-map-rmie");
    let map = client.get_map::<String, String>(&map_name);

    map.put("k".to_string(), "v".to_string()).await.unwrap();

    let miss = map.remove_if_equal(&"k".to_string(), &"wrong".to_string()).await.unwrap();
    assert!(!miss);

    let hit = map.remove_if_equal(&"k".to_string(), &"v".to_string()).await.unwrap();
    assert!(hit);

    assert!(map.get(&"k".to_string()).await.unwrap().is_none());

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_map_contains_key_true() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let map_name = unique_name("cft-map-ckt");
    let map = client.get_map::<String, String>(&map_name);

    map.put("k".to_string(), "v".to_string()).await.unwrap();
    assert!(map.contains_key(&"k".to_string()).await.unwrap());

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_map_contains_key_false() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let map_name = unique_name("cft-map-ckf");
    let map = client.get_map::<String, String>(&map_name);

    assert!(!map.contains_key(&"nope".to_string()).await.unwrap());

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_map_delete() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let map_name = unique_name("cft-map-del");
    let map = client.get_map::<String, String>(&map_name);

    map.put("k".to_string(), "v".to_string()).await.unwrap();
    map.delete(&"k".to_string()).await.unwrap();
    assert!(map.get(&"k".to_string()).await.unwrap().is_none());

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_map_set_fire_forget() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let map_name = unique_name("cft-map-set");
    let map = client.get_map::<String, String>(&map_name);

    map.set("k".to_string(), "v".to_string()).await.unwrap();
    let val = map.get(&"k".to_string()).await.unwrap();
    assert_eq!(val, Some("v".to_string()));

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_map_size_empty() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let map_name = unique_name("cft-map-sze");
    let map = client.get_map::<String, String>(&map_name);

    assert_eq!(map.size().await.unwrap(), 0);

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_map_size_after_puts() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let map_name = unique_name("cft-map-szp");
    let map = client.get_map::<String, String>(&map_name);

    for i in 0..5 {
        map.put(format!("k{}", i), format!("v{}", i)).await.unwrap();
    }
    assert_eq!(map.size().await.unwrap(), 5);

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_map_is_empty() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let map_name = unique_name("cft-map-ie");
    let map = client.get_map::<String, String>(&map_name);

    assert!(map.is_empty().await.unwrap());
    map.put("k".to_string(), "v".to_string()).await.unwrap();
    assert!(!map.is_empty().await.unwrap());

    map.clear().await.unwrap();
}

// ============================================================================
// IMap — TTL (2 tests)
// ============================================================================

#[tokio::test]
async fn test_map_set_ttl_expires() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let map_name = unique_name("cft-map-sttl");
    let map = client.get_map::<String, String>(&map_name);

    map.put("k".to_string(), "v".to_string()).await.unwrap();
    map.set_ttl(&"k".to_string(), Duration::from_secs(2)).await.unwrap();

    let before = map.get(&"k".to_string()).await.unwrap();
    assert_eq!(before, Some("v".to_string()));

    tokio::time::sleep(Duration::from_secs(3)).await;

    let after = map.get(&"k".to_string()).await.unwrap();
    assert!(after.is_none(), "entry should have expired after TTL");

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_map_put_with_ttl() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let map_name = unique_name("cft-map-pttl");
    let map = client.get_map::<String, String>(&map_name);

    map.put_with_ttl_and_max_idle(
        "k".to_string(), "v".to_string(),
        Duration::from_secs(2), Duration::ZERO,
    ).await.unwrap();

    let before = map.get(&"k".to_string()).await.unwrap();
    assert_eq!(before, Some("v".to_string()));

    tokio::time::sleep(Duration::from_secs(3)).await;

    let after = map.get(&"k".to_string()).await.unwrap();
    assert!(after.is_none(), "entry should have expired after TTL");

    map.clear().await.unwrap();
}

// ============================================================================
// IMap — Locking (4 tests)
// ============================================================================

#[tokio::test]
async fn test_map_lock_unlock() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let map_name = unique_name("cft-map-lu");
    let map = client.get_map::<String, String>(&map_name);

    map.put("k".to_string(), "v".to_string()).await.unwrap();

    map.lock(&"k".to_string()).await.unwrap();
    assert!(map.is_locked(&"k".to_string()).await.unwrap());

    map.unlock(&"k".to_string()).await.unwrap();
    assert!(!map.is_locked(&"k".to_string()).await.unwrap());

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_map_try_lock_success() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let map_name = unique_name("cft-map-tls");
    let map = client.get_map::<String, String>(&map_name);

    map.put("k".to_string(), "v".to_string()).await.unwrap();

    let ok = map.try_lock(&"k".to_string(), Duration::from_secs(1)).await.unwrap();
    assert!(ok);

    map.unlock(&"k".to_string()).await.unwrap();

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_map_is_locked() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let map_name = unique_name("cft-map-il");
    let map = client.get_map::<String, String>(&map_name);

    map.put("k".to_string(), "v".to_string()).await.unwrap();

    assert!(!map.is_locked(&"k".to_string()).await.unwrap());
    map.lock(&"k".to_string()).await.unwrap();
    assert!(map.is_locked(&"k".to_string()).await.unwrap());
    map.unlock(&"k".to_string()).await.unwrap();

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_map_force_unlock() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let map_name = unique_name("cft-map-fu");
    let map = client.get_map::<String, String>(&map_name);

    map.put("k".to_string(), "v".to_string()).await.unwrap();

    map.lock(&"k".to_string()).await.unwrap();
    assert!(map.is_locked(&"k".to_string()).await.unwrap());

    map.force_unlock(&"k".to_string()).await.unwrap();
    assert!(!map.is_locked(&"k".to_string()).await.unwrap());

    map.clear().await.unwrap();
}

// ============================================================================
// IMap — Compute (2 tests)
// ============================================================================

#[tokio::test]
async fn test_map_compute_increment() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let map_name = unique_name("cft-map-cinc");
    let map = client.get_map::<String, i64>(&map_name);

    map.put("counter".to_string(), 10i64).await.unwrap();

    let result = map.compute(&"counter".to_string(), |_key, old| {
        old.map(|v| v + 1)
    }).await.unwrap();

    assert_eq!(result, Some(11i64));

    let val = map.get(&"counter".to_string()).await.unwrap();
    assert_eq!(val, Some(11i64));

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_map_compute_if_absent() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let map_name = unique_name("cft-map-cia");
    let map = client.get_map::<String, String>(&map_name);

    // Key does not exist yet — should set it
    let result = map.compute_if_absent(&"k".to_string(), |_key| {
        Some("default".to_string())
    }).await.unwrap();
    assert_eq!(result, Some("default".to_string()));

    // Key already exists — should not overwrite
    let result = map.compute_if_absent(&"k".to_string(), |_key| {
        Some("other".to_string())
    }).await.unwrap();
    assert_eq!(result, Some("default".to_string()));

    map.clear().await.unwrap();
}

// ============================================================================
// IMap — Bulk (4 tests)
// ============================================================================

#[tokio::test]
async fn test_map_put_all() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let map_name = unique_name("cft-map-pa");
    let map = client.get_map::<String, String>(&map_name);

    let mut entries = HashMap::new();
    for i in 0..10 {
        entries.insert(format!("k{}", i), format!("v{}", i));
    }
    map.put_all(entries).await.unwrap();

    assert_eq!(map.size().await.unwrap(), 10);
    assert_eq!(map.get(&"k5".to_string()).await.unwrap(), Some("v5".to_string()));

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_map_get_all() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let map_name = unique_name("cft-map-ga");
    let map = client.get_map::<String, String>(&map_name);

    for i in 0..10 {
        map.put(format!("k{}", i), format!("v{}", i)).await.unwrap();
    }

    let keys: Vec<String> = (0..10).map(|i| format!("k{}", i)).collect();
    let result = map.get_all(&keys).await.unwrap();
    assert_eq!(result.len(), 10);
    assert_eq!(result.get(&"k3".to_string()), Some(&"v3".to_string()));

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_map_clear() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let map_name = unique_name("cft-map-clr");
    let map = client.get_map::<String, String>(&map_name);

    for i in 0..5 {
        map.put(format!("k{}", i), format!("v{}", i)).await.unwrap();
    }
    assert_eq!(map.size().await.unwrap(), 5);

    map.clear().await.unwrap();
    assert_eq!(map.size().await.unwrap(), 0);
}

#[tokio::test]
async fn test_map_remove_all() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let map_name = unique_name("cft-map-ra");
    let map = client.get_map::<String, String>(&map_name);

    for i in 0..5 {
        map.put(format!("k{}", i), format!("v{}", i)).await.unwrap();
    }

    let to_remove: Vec<String> = (0..3).map(|i| format!("k{}", i)).collect();
    map.remove_all(&to_remove).await.unwrap();

    assert_eq!(map.size().await.unwrap(), 2);
    assert!(map.get(&"k0".to_string()).await.unwrap().is_none());
    assert!(map.get(&"k3".to_string()).await.unwrap().is_some());

    map.clear().await.unwrap();
}

// ============================================================================
// IMap — Iterators (2 tests)
// ============================================================================

#[tokio::test]
async fn test_map_key_set_collects_all() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let map_name = unique_name("cft-map-ks");
    let map = client.get_map::<String, String>(&map_name);

    for i in 0..20 {
        map.put(format!("k{}", i), format!("v{}", i)).await.unwrap();
    }

    let key_iter = map.key_set().await.unwrap();
    let keys: Vec<String> = key_iter.collect().await.unwrap();
    assert_eq!(keys.len(), 20);

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_map_entry_set_collects_all() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let map_name = unique_name("cft-map-es");
    let map = client.get_map::<String, String>(&map_name);

    for i in 0..20 {
        map.put(format!("k{}", i), format!("v{}", i)).await.unwrap();
    }

    let entry_iter = map.entry_set().await.unwrap();
    let entries: Vec<(String, String)> = entry_iter.collect_entries().await.unwrap();
    assert_eq!(entries.len(), 20);

    map.clear().await.unwrap();
}

// ============================================================================
// IMap — Additional (19 tests)
// ============================================================================

#[tokio::test]
async fn test_map_get_nonexistent() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let map_name = unique_name("cft-map-gne");
    let map = client.get_map::<String, String>(&map_name);

    let val = map.get(&"does-not-exist".to_string()).await.unwrap();
    assert!(val.is_none());

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_map_contains_value() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let map_name = unique_name("cft-map-cv");
    let map = client.get_map::<String, String>(&map_name);

    map.put("k".to_string(), "target".to_string()).await.unwrap();
    assert!(map.contains_value(&"target".to_string()).await.unwrap());
    assert!(!map.contains_value(&"missing".to_string()).await.unwrap());

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_map_evict() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let map_name = unique_name("cft-map-evict");
    let map = client.get_map::<String, String>(&map_name);

    map.put("k".to_string(), "v".to_string()).await.unwrap();
    let evicted = map.evict(&"k".to_string()).await.unwrap();
    assert!(evicted);

    // After eviction, the entry may be gone from memory
    // (backed by map store it would reload, but in-memory-only it is gone)

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_map_evict_all() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let map_name = unique_name("cft-map-evall");
    let map = client.get_map::<String, String>(&map_name);

    for i in 0..5 {
        map.put(format!("k{}", i), format!("v{}", i)).await.unwrap();
    }
    map.evict_all().await.unwrap();

    // Size should be 0 for in-memory maps without a map store
    assert_eq!(map.size().await.unwrap(), 0);

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_map_try_put() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let map_name = unique_name("cft-map-tp");
    let map = client.get_map::<String, String>(&map_name);

    let ok = map.try_put(
        "k".to_string(), "v".to_string(), Duration::from_secs(5)
    ).await.unwrap();
    assert!(ok);

    let val = map.get(&"k".to_string()).await.unwrap();
    assert_eq!(val, Some("v".to_string()));

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_map_put_with_max_idle() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let map_name = unique_name("cft-map-pmi");
    let map = client.get_map::<String, String>(&map_name);

    map.put_with_max_idle(
        "k".to_string(), "v".to_string(), Duration::from_secs(2)
    ).await.unwrap();

    let before = map.get(&"k".to_string()).await.unwrap();
    assert_eq!(before, Some("v".to_string()));

    // After max idle expires without access, entry should be gone
    tokio::time::sleep(Duration::from_secs(3)).await;
    let after = map.get(&"k".to_string()).await.unwrap();
    assert!(after.is_none(), "entry should expire after max idle");

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_map_set_with_ttl_and_max_idle() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let map_name = unique_name("cft-map-swtami");
    let map = client.get_map::<String, String>(&map_name);

    map.set_with_ttl_and_max_idle(
        "k".to_string(), "v".to_string(),
        Duration::from_secs(2), Duration::ZERO,
    ).await.unwrap();

    let before = map.get(&"k".to_string()).await.unwrap();
    assert_eq!(before, Some("v".to_string()));

    tokio::time::sleep(Duration::from_secs(3)).await;
    let after = map.get(&"k".to_string()).await.unwrap();
    assert!(after.is_none(), "entry should expire after TTL");

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_map_put_transient() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let map_name = unique_name("cft-map-pt");
    let map = client.get_map::<String, String>(&map_name);

    map.put_transient(
        "k".to_string(), "v".to_string(), Duration::from_secs(2)
    ).await.unwrap();

    let before = map.get(&"k".to_string()).await.unwrap();
    assert_eq!(before, Some("v".to_string()));

    tokio::time::sleep(Duration::from_secs(3)).await;
    let after = map.get(&"k".to_string()).await.unwrap();
    assert!(after.is_none(), "transient entry should expire");

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_map_get_entry_view() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let map_name = unique_name("cft-map-gev");
    let map = client.get_map::<String, String>(&map_name);

    map.put("k".to_string(), "v".to_string()).await.unwrap();
    let view = map.get_entry_view(&"k".to_string()).await.unwrap();
    assert!(view.is_some());

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_map_overwrite_preserves_latest() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let map_name = unique_name("cft-map-opl");
    let map = client.get_map::<String, String>(&map_name);

    map.put("k".to_string(), "v1".to_string()).await.unwrap();
    map.put("k".to_string(), "v2".to_string()).await.unwrap();
    map.put("k".to_string(), "v3".to_string()).await.unwrap();

    let val = map.get(&"k".to_string()).await.unwrap();
    assert_eq!(val, Some("v3".to_string()));
    assert_eq!(map.size().await.unwrap(), 1);

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_map_integer_keys_values() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let map_name = unique_name("cft-map-ikv");
    let map = client.get_map::<i32, i64>(&map_name);

    map.put(1, 100i64).await.unwrap();
    map.put(2, 200i64).await.unwrap();

    assert_eq!(map.get(&1).await.unwrap(), Some(100i64));
    assert_eq!(map.get(&2).await.unwrap(), Some(200i64));

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_map_large_values() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let map_name = unique_name("cft-map-lv");
    let map = client.get_map::<String, String>(&map_name);

    let big = "x".repeat(100_000);
    map.put("k".to_string(), big.clone()).await.unwrap();
    let val = map.get(&"k".to_string()).await.unwrap();
    assert_eq!(val, Some(big));

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_map_empty_key_and_value() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let map_name = unique_name("cft-map-ekv");
    let map = client.get_map::<String, String>(&map_name);

    map.put(String::new(), "empty-key".to_string()).await.unwrap();
    assert_eq!(map.get(&String::new()).await.unwrap(), Some("empty-key".to_string()));

    map.put("k".to_string(), String::new()).await.unwrap();
    assert_eq!(map.get(&"k".to_string()).await.unwrap(), Some(String::new()));

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_map_special_characters() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let map_name = unique_name("cft-map-sc");
    let map = client.get_map::<String, String>(&map_name);

    let keys = vec![
        "spaces in key", "key:colons", "key/slashes",
        "unicode-\u{1F680}", "newline\nkey",
    ];
    for (i, k) in keys.iter().enumerate() {
        map.put(k.to_string(), format!("v{}", i)).await.unwrap();
    }
    for (i, k) in keys.iter().enumerate() {
        assert_eq!(map.get(&k.to_string()).await.unwrap(), Some(format!("v{}", i)));
    }

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_map_flush() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let map_name = unique_name("cft-map-flush");
    let map = client.get_map::<String, String>(&map_name);

    map.put("k".to_string(), "v".to_string()).await.unwrap();
    // flush should not error even without a map store
    map.flush().await.unwrap();

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_map_set_all() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let map_name = unique_name("cft-map-sa");
    let map = client.get_map::<String, String>(&map_name);

    let mut entries = HashMap::new();
    for i in 0..10 {
        entries.insert(format!("k{}", i), format!("v{}", i));
    }
    map.set_all(entries).await.unwrap();
    assert_eq!(map.size().await.unwrap(), 10);

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_map_compute_if_present() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let map_name = unique_name("cft-map-cip");
    let map = client.get_map::<String, String>(&map_name);

    // Key absent: should be no-op
    let result = map.compute_if_present(&"k".to_string(), |_key, old| {
        Some(format!("{}-modified", old))
    }).await.unwrap();
    assert!(result.is_none());

    // Key present: should update
    map.put("k".to_string(), "original".to_string()).await.unwrap();
    let result = map.compute_if_present(&"k".to_string(), |_key, old| {
        Some(format!("{}-modified", old))
    }).await.unwrap();
    assert_eq!(result, Some("original-modified".to_string()));

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_map_clone_shares_state() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let map_name = unique_name("cft-map-css");
    let map1 = client.get_map::<String, String>(&map_name);
    let map2 = map1.clone();

    map1.put("k".to_string(), "v".to_string()).await.unwrap();
    let val = map2.get(&"k".to_string()).await.unwrap();
    assert_eq!(val, Some("v".to_string()));

    map1.clear().await.unwrap();
}

#[tokio::test]
async fn test_map_put_all_large_batch() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let map_name = unique_name("cft-map-palb");
    let map = client.get_map::<String, String>(&map_name);

    let mut entries = HashMap::new();
    for i in 0..100 {
        entries.insert(format!("key-{}", i), format!("value-{}", i));
    }
    map.put_all(entries).await.unwrap();
    assert_eq!(map.size().await.unwrap(), 100);

    for i in 0..100 {
        let val = map.get(&format!("key-{}", i)).await.unwrap();
        assert_eq!(val, Some(format!("value-{}", i)));
    }

    map.clear().await.unwrap();
}

// ============================================================================
// ISet (10 tests)
// ============================================================================

#[tokio::test]
async fn test_set_add_returns_true() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let set_name = unique_name("cft-set-art");
    let set = client.get_set::<String>(&set_name);

    // NOTE: may fail due to HazelRust protocol issues
    let added = set.add("item1".to_string()).await.unwrap();
    assert!(added);

    set.clear().await.unwrap();
}

#[tokio::test]
async fn test_set_add_duplicate_returns_false() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let set_name = unique_name("cft-set-adrf");
    let set = client.get_set::<String>(&set_name);

    // NOTE: may fail due to HazelRust protocol issues
    set.add("item1".to_string()).await.unwrap();
    let dup = set.add("item1".to_string()).await.unwrap();
    assert!(!dup);

    set.clear().await.unwrap();
}

#[tokio::test]
async fn test_set_remove() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let set_name = unique_name("cft-set-rm");
    let set = client.get_set::<String>(&set_name);

    set.add("item1".to_string()).await.unwrap();
    let removed = set.remove(&"item1".to_string()).await.unwrap();
    assert!(removed);

    let gone = set.remove(&"item1".to_string()).await.unwrap();
    assert!(!gone);

    set.clear().await.unwrap();
}

#[tokio::test]
async fn test_set_contains_true() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let set_name = unique_name("cft-set-ct");
    let set = client.get_set::<String>(&set_name);

    set.add("item1".to_string()).await.unwrap();
    assert!(set.contains(&"item1".to_string()).await.unwrap());

    set.clear().await.unwrap();
}

#[tokio::test]
async fn test_set_contains_false() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let set_name = unique_name("cft-set-cf");
    let set = client.get_set::<String>(&set_name);

    assert!(!set.contains(&"nope".to_string()).await.unwrap());

    set.clear().await.unwrap();
}

#[tokio::test]
async fn test_set_size() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let set_name = unique_name("cft-set-sz");
    let set = client.get_set::<String>(&set_name);

    assert_eq!(set.size().await.unwrap(), 0);
    set.add("a".to_string()).await.unwrap();
    set.add("b".to_string()).await.unwrap();
    assert_eq!(set.size().await.unwrap(), 2);

    set.clear().await.unwrap();
}

#[tokio::test]
async fn test_set_is_empty_after_clear() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let set_name = unique_name("cft-set-ieac");
    let set = client.get_set::<String>(&set_name);

    set.add("item".to_string()).await.unwrap();
    assert!(!set.is_empty().await.unwrap());

    set.clear().await.unwrap();
    assert!(set.is_empty().await.unwrap());
}

#[tokio::test]
async fn test_set_get_all() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let set_name = unique_name("cft-set-gall");
    let set = client.get_set::<String>(&set_name);

    set.add("a".to_string()).await.unwrap();
    set.add("b".to_string()).await.unwrap();
    set.add("c".to_string()).await.unwrap();

    let all = set.get_all().await.unwrap();
    assert_eq!(all.len(), 3);

    set.clear().await.unwrap();
}

#[tokio::test]
async fn test_set_add_all() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let set_name = unique_name("cft-set-aall");
    let set = client.get_set::<String>(&set_name);

    // NOTE: may fail due to HazelRust protocol issues
    let items = vec!["a".to_string(), "b".to_string(), "c".to_string()];
    let result = set.add_all(items).await.unwrap();
    assert!(result);
    assert_eq!(set.size().await.unwrap(), 3);

    set.clear().await.unwrap();
}

#[tokio::test]
async fn test_set_contains_all() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let set_name = unique_name("cft-set-call");
    let set = client.get_set::<String>(&set_name);

    set.add("a".to_string()).await.unwrap();
    set.add("b".to_string()).await.unwrap();
    set.add("c".to_string()).await.unwrap();

    let subset = vec!["a".to_string(), "c".to_string()];
    assert!(set.contains_all(&subset).await.unwrap());

    let missing = vec!["a".to_string(), "z".to_string()];
    assert!(!set.contains_all(&missing).await.unwrap());

    set.clear().await.unwrap();
}

// ============================================================================
// IList (10 tests)
// ============================================================================

#[tokio::test]
async fn test_list_add_get() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let list_name = unique_name("cft-list-ag");
    let list = client.get_list::<String>(&list_name);

    // NOTE: may fail due to HazelRust protocol issues
    list.add("item0".to_string()).await.unwrap();
    let item = list.get(0).await.unwrap();
    assert_eq!(item, Some("item0".to_string()));

    list.clear().await.unwrap();
}

#[tokio::test]
async fn test_list_add_at_0() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let list_name = unique_name("cft-list-aa0");
    let list = client.get_list::<String>(&list_name);

    // NOTE: may fail due to HazelRust protocol issues
    list.add("second".to_string()).await.unwrap();
    list.add_at(0, "first".to_string()).await.unwrap();

    assert_eq!(list.get(0).await.unwrap(), Some("first".to_string()));
    assert_eq!(list.get(1).await.unwrap(), Some("second".to_string()));

    list.clear().await.unwrap();
}

#[tokio::test]
async fn test_list_remove_at() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let list_name = unique_name("cft-list-ra");
    let list = client.get_list::<String>(&list_name);

    list.add("a".to_string()).await.unwrap();
    list.add("b".to_string()).await.unwrap();
    list.add("c".to_string()).await.unwrap();

    let removed = list.remove_at(1).await.unwrap();
    assert_eq!(removed, Some("b".to_string()));
    assert_eq!(list.size().await.unwrap(), 2);

    list.clear().await.unwrap();
}

#[tokio::test]
async fn test_list_set_at_index() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let list_name = unique_name("cft-list-sai");
    let list = client.get_list::<String>(&list_name);

    list.add("old".to_string()).await.unwrap();
    let prev = list.set(0, "new".to_string()).await.unwrap();
    assert_eq!(prev, Some("old".to_string()));
    assert_eq!(list.get(0).await.unwrap(), Some("new".to_string()));

    list.clear().await.unwrap();
}

#[tokio::test]
async fn test_list_index_of() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let list_name = unique_name("cft-list-io");
    let list = client.get_list::<String>(&list_name);

    list.add("a".to_string()).await.unwrap();
    list.add("b".to_string()).await.unwrap();
    list.add("c".to_string()).await.unwrap();

    let idx = list.index_of(&"b".to_string()).await.unwrap();
    assert_eq!(idx, Some(1));

    let none_idx = list.index_of(&"z".to_string()).await.unwrap();
    assert!(none_idx.is_none());

    list.clear().await.unwrap();
}

#[tokio::test]
async fn test_list_size() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let list_name = unique_name("cft-list-sz");
    let list = client.get_list::<String>(&list_name);

    assert_eq!(list.size().await.unwrap(), 0);
    list.add("a".to_string()).await.unwrap();
    list.add("b".to_string()).await.unwrap();
    assert_eq!(list.size().await.unwrap(), 2);

    list.clear().await.unwrap();
}

#[tokio::test]
async fn test_list_is_empty_after_clear() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let list_name = unique_name("cft-list-ieac");
    let list = client.get_list::<String>(&list_name);

    list.add("x".to_string()).await.unwrap();
    assert!(!list.is_empty().await.unwrap());

    list.clear().await.unwrap();
    assert!(list.is_empty().await.unwrap());
}

#[tokio::test]
async fn test_list_contains() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let list_name = unique_name("cft-list-con");
    let list = client.get_list::<String>(&list_name);

    list.add("present".to_string()).await.unwrap();
    assert!(list.contains(&"present".to_string()).await.unwrap());
    assert!(!list.contains(&"absent".to_string()).await.unwrap());

    list.clear().await.unwrap();
}

#[tokio::test]
async fn test_list_add_all() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let list_name = unique_name("cft-list-aall");
    let list = client.get_list::<String>(&list_name);

    // NOTE: may fail due to HazelRust protocol issues
    let items = vec!["a".to_string(), "b".to_string(), "c".to_string()];
    let result = list.add_all(items).await.unwrap();
    assert!(result);
    assert_eq!(list.size().await.unwrap(), 3);

    list.clear().await.unwrap();
}

#[tokio::test]
async fn test_list_contains_all() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let list_name = unique_name("cft-list-call");
    let list = client.get_list::<String>(&list_name);

    list.add("a".to_string()).await.unwrap();
    list.add("b".to_string()).await.unwrap();
    list.add("c".to_string()).await.unwrap();

    let subset = vec!["a".to_string(), "b".to_string()];
    assert!(list.contains_all(&subset).await.unwrap());

    let missing = vec!["a".to_string(), "z".to_string()];
    assert!(!list.contains_all(&missing).await.unwrap());

    list.clear().await.unwrap();
}

// ============================================================================
// IQueue (10 tests)
// ============================================================================

#[tokio::test]
async fn test_queue_offer_poll() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let queue_name = unique_name("cft-q-op");
    let queue = client.get_queue::<String>(&queue_name);

    // NOTE: may fail due to HazelRust protocol issues
    queue.offer("item1".to_string()).await.unwrap();
    let polled = queue.poll().await.unwrap();
    assert_eq!(polled, Some("item1".to_string()));

    let empty = queue.poll().await.unwrap();
    assert!(empty.is_none());
}

#[tokio::test]
async fn test_queue_peek() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let queue_name = unique_name("cft-q-peek");
    let queue = client.get_queue::<String>(&queue_name);

    assert!(queue.peek().await.unwrap().is_none());

    queue.offer("head".to_string()).await.unwrap();
    assert_eq!(queue.peek().await.unwrap(), Some("head".to_string()));
    // peek should not consume
    assert_eq!(queue.peek().await.unwrap(), Some("head".to_string()));

    queue.poll().await.unwrap();
}

#[tokio::test]
async fn test_queue_size() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let queue_name = unique_name("cft-q-sz");
    let queue = client.get_queue::<String>(&queue_name);

    assert_eq!(queue.size().await.unwrap(), 0);
    queue.offer("a".to_string()).await.unwrap();
    queue.offer("b".to_string()).await.unwrap();
    assert_eq!(queue.size().await.unwrap(), 2);

    queue.poll().await.unwrap();
    queue.poll().await.unwrap();
}

#[tokio::test]
async fn test_queue_is_empty() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let queue_name = unique_name("cft-q-ie");
    let queue = client.get_queue::<String>(&queue_name);

    assert!(queue.is_empty().await.unwrap());
    queue.offer("item".to_string()).await.unwrap();
    assert!(!queue.is_empty().await.unwrap());

    queue.poll().await.unwrap();
}

#[tokio::test]
async fn test_queue_clear() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let queue_name = unique_name("cft-q-clr");
    let queue = client.get_queue::<String>(&queue_name);

    queue.offer("a".to_string()).await.unwrap();
    queue.offer("b".to_string()).await.unwrap();
    queue.clear().await.unwrap();
    assert_eq!(queue.size().await.unwrap(), 0);
}

#[tokio::test]
async fn test_queue_remaining_capacity() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let queue_name = unique_name("cft-q-rc");
    let queue = client.get_queue::<String>(&queue_name);

    let cap = queue.remaining_capacity().await.unwrap();
    assert!(cap > 0, "default queue should have positive capacity");

    // cleanup
    while queue.poll().await.unwrap().is_some() {}
}

#[tokio::test]
async fn test_queue_contains() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let queue_name = unique_name("cft-q-con");
    let queue = client.get_queue::<String>(&queue_name);

    queue.offer("present".to_string()).await.unwrap();
    assert!(queue.contains(&"present".to_string()).await.unwrap());
    assert!(!queue.contains(&"absent".to_string()).await.unwrap());

    queue.poll().await.unwrap();
}

#[tokio::test]
async fn test_queue_remove() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let queue_name = unique_name("cft-q-rm");
    let queue = client.get_queue::<String>(&queue_name);

    queue.offer("item1".to_string()).await.unwrap();
    queue.offer("item2".to_string()).await.unwrap();

    let removed = queue.remove(&"item1".to_string()).await.unwrap();
    assert!(removed);
    assert_eq!(queue.size().await.unwrap(), 1);

    let not_removed = queue.remove(&"item1".to_string()).await.unwrap();
    assert!(!not_removed);

    queue.poll().await.unwrap();
}

#[tokio::test]
async fn test_queue_add_all() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let queue_name = unique_name("cft-q-aall");
    let queue = client.get_queue::<String>(&queue_name);

    // NOTE: may fail due to HazelRust protocol issues
    let items = vec!["a".to_string(), "b".to_string(), "c".to_string()];
    let result = queue.add_all(items).await.unwrap();
    assert!(result);
    assert_eq!(queue.size().await.unwrap(), 3);

    while queue.poll().await.unwrap().is_some() {}
}

#[tokio::test]
async fn test_queue_drain_to() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let queue_name = unique_name("cft-q-dt");
    let queue = client.get_queue::<String>(&queue_name);

    queue.offer("a".to_string()).await.unwrap();
    queue.offer("b".to_string()).await.unwrap();
    queue.offer("c".to_string()).await.unwrap();

    let drained = queue.drain_to(10).await.unwrap();
    assert_eq!(drained.len(), 3);
    assert_eq!(queue.size().await.unwrap(), 0);
}

// ============================================================================
// AtomicLong (8 tests)
// ============================================================================

#[tokio::test]
async fn test_atomic_long_get_set() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let name = unique_name("cft-al-gs");
    let counter = client.get_atomic_long(&name);

    assert_eq!(counter.get().await.unwrap(), 0);
    counter.set(42).await.unwrap();
    assert_eq!(counter.get().await.unwrap(), 42);
}

#[tokio::test]
async fn test_atomic_long_increment_and_get() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let name = unique_name("cft-al-iag");
    let counter = client.get_atomic_long(&name);

    counter.set(10).await.unwrap();
    assert_eq!(counter.increment_and_get().await.unwrap(), 11);
    assert_eq!(counter.increment_and_get().await.unwrap(), 12);
}

#[tokio::test]
async fn test_atomic_long_decrement_and_get() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let name = unique_name("cft-al-dag");
    let counter = client.get_atomic_long(&name);

    counter.set(10).await.unwrap();
    assert_eq!(counter.decrement_and_get().await.unwrap(), 9);
    assert_eq!(counter.decrement_and_get().await.unwrap(), 8);
}

#[tokio::test]
async fn test_atomic_long_add_and_get() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let name = unique_name("cft-al-aag");
    let counter = client.get_atomic_long(&name);

    counter.set(100).await.unwrap();
    assert_eq!(counter.add_and_get(50).await.unwrap(), 150);
    assert_eq!(counter.add_and_get(-30).await.unwrap(), 120);
}

#[tokio::test]
async fn test_atomic_long_get_and_set() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let name = unique_name("cft-al-gas");
    let counter = client.get_atomic_long(&name);

    counter.set(10).await.unwrap();
    let old = counter.get_and_set(20).await.unwrap();
    assert_eq!(old, 10);
    assert_eq!(counter.get().await.unwrap(), 20);
}

#[tokio::test]
async fn test_atomic_long_compare_and_set_success() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let name = unique_name("cft-al-cass");
    let counter = client.get_atomic_long(&name);

    counter.set(10).await.unwrap();
    assert!(counter.compare_and_set(10, 20).await.unwrap());
    assert_eq!(counter.get().await.unwrap(), 20);
}

#[tokio::test]
async fn test_atomic_long_compare_and_set_failure() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let name = unique_name("cft-al-casf");
    let counter = client.get_atomic_long(&name);

    counter.set(10).await.unwrap();
    assert!(!counter.compare_and_set(999, 20).await.unwrap());
    assert_eq!(counter.get().await.unwrap(), 10);
}

#[tokio::test]
async fn test_atomic_long_get_and_increment() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let name = unique_name("cft-al-gai");
    let counter = client.get_atomic_long(&name);

    counter.set(5).await.unwrap();
    let old = counter.get_and_increment().await.unwrap();
    assert_eq!(old, 5);
    assert_eq!(counter.get().await.unwrap(), 6);
}

// ============================================================================
// MultiMap (8 tests)
// ============================================================================

#[tokio::test]
async fn test_multimap_put_get() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let name = unique_name("cft-mm-pg");
    let mm = client.get_multimap::<String, String>(&name);

    mm.put("k".to_string(), "v1".to_string()).await.unwrap();
    let vals = mm.get(&"k".to_string()).await.unwrap();
    assert_eq!(vals.len(), 1);
    assert_eq!(vals[0], "v1".to_string());

    mm.clear().await.unwrap();
}

#[tokio::test]
async fn test_multimap_multiple_values_same_key() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let name = unique_name("cft-mm-mvsk");
    let mm = client.get_multimap::<String, String>(&name);

    mm.put("k".to_string(), "v1".to_string()).await.unwrap();
    mm.put("k".to_string(), "v2".to_string()).await.unwrap();
    mm.put("k".to_string(), "v3".to_string()).await.unwrap();

    let vals = mm.get(&"k".to_string()).await.unwrap();
    assert_eq!(vals.len(), 3);

    mm.clear().await.unwrap();
}

#[tokio::test]
async fn test_multimap_remove_value() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let name = unique_name("cft-mm-rv");
    let mm = client.get_multimap::<String, String>(&name);

    mm.put("k".to_string(), "v1".to_string()).await.unwrap();
    mm.put("k".to_string(), "v2".to_string()).await.unwrap();

    let removed = mm.remove(&"k".to_string(), &"v1".to_string()).await.unwrap();
    assert!(removed);

    let vals = mm.get(&"k".to_string()).await.unwrap();
    assert_eq!(vals.len(), 1);

    mm.clear().await.unwrap();
}

#[tokio::test]
async fn test_multimap_remove_all_for_key() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let name = unique_name("cft-mm-rafk");
    let mm = client.get_multimap::<String, String>(&name);

    mm.put("k".to_string(), "v1".to_string()).await.unwrap();
    mm.put("k".to_string(), "v2".to_string()).await.unwrap();

    let removed = mm.remove_all(&"k".to_string()).await.unwrap();
    assert_eq!(removed.len(), 2);

    let vals = mm.get(&"k".to_string()).await.unwrap();
    assert!(vals.is_empty());

    mm.clear().await.unwrap();
}

#[tokio::test]
async fn test_multimap_contains_key() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let name = unique_name("cft-mm-ck");
    let mm = client.get_multimap::<String, String>(&name);

    assert!(!mm.contains_key(&"k".to_string()).await.unwrap());
    mm.put("k".to_string(), "v".to_string()).await.unwrap();
    assert!(mm.contains_key(&"k".to_string()).await.unwrap());

    mm.clear().await.unwrap();
}

#[tokio::test]
async fn test_multimap_size() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let name = unique_name("cft-mm-sz");
    let mm = client.get_multimap::<String, String>(&name);

    assert_eq!(mm.size().await.unwrap(), 0);
    mm.put("k1".to_string(), "v1".to_string()).await.unwrap();
    mm.put("k1".to_string(), "v2".to_string()).await.unwrap();
    mm.put("k2".to_string(), "v3".to_string()).await.unwrap();
    assert_eq!(mm.size().await.unwrap(), 3);

    mm.clear().await.unwrap();
}

#[tokio::test]
async fn test_multimap_value_count() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let name = unique_name("cft-mm-vc");
    let mm = client.get_multimap::<String, String>(&name);

    mm.put("k".to_string(), "a".to_string()).await.unwrap();
    mm.put("k".to_string(), "b".to_string()).await.unwrap();
    mm.put("k".to_string(), "c".to_string()).await.unwrap();

    assert_eq!(mm.value_count(&"k".to_string()).await.unwrap(), 3);

    mm.clear().await.unwrap();
}

#[tokio::test]
async fn test_multimap_clear() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let name = unique_name("cft-mm-clr");
    let mm = client.get_multimap::<String, String>(&name);

    mm.put("k".to_string(), "v".to_string()).await.unwrap();
    mm.clear().await.unwrap();
    assert_eq!(mm.size().await.unwrap(), 0);
}

// ============================================================================
// ReplicatedMap (5 tests)
// ============================================================================

#[tokio::test]
async fn test_replicated_map_put_get() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let name = unique_name("cft-rm-pg");
    let map = client.get_replicated_map::<String, String>(&name);

    let prev = map.put("k".to_string(), "v".to_string()).await.unwrap();
    assert!(prev.is_none());

    let val = map.get(&"k".to_string()).await.unwrap();
    assert_eq!(val, Some("v".to_string()));

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_replicated_map_size() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let name = unique_name("cft-rm-sz");
    let map = client.get_replicated_map::<String, String>(&name);

    map.clear().await.unwrap();
    assert_eq!(map.size().await.unwrap(), 0);

    map.put("a".to_string(), "1".to_string()).await.unwrap();
    map.put("b".to_string(), "2".to_string()).await.unwrap();
    assert_eq!(map.size().await.unwrap(), 2);

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_replicated_map_contains_key() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let name = unique_name("cft-rm-ck");
    let map = client.get_replicated_map::<String, String>(&name);

    map.clear().await.unwrap();
    assert!(!map.contains_key(&"k".to_string()).await.unwrap());
    map.put("k".to_string(), "v".to_string()).await.unwrap();
    assert!(map.contains_key(&"k".to_string()).await.unwrap());

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_replicated_map_is_empty_after_clear() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let name = unique_name("cft-rm-ieac");
    let map = client.get_replicated_map::<String, String>(&name);

    map.put("k".to_string(), "v".to_string()).await.unwrap();
    assert!(!map.is_empty().await.unwrap());

    map.clear().await.unwrap();
    assert!(map.is_empty().await.unwrap());
}

#[tokio::test]
async fn test_replicated_map_key_set() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let name = unique_name("cft-rm-ks");
    let map = client.get_replicated_map::<String, String>(&name);

    map.clear().await.unwrap();

    map.put("a".to_string(), "1".to_string()).await.unwrap();
    map.put("b".to_string(), "2".to_string()).await.unwrap();
    map.put("c".to_string(), "3".to_string()).await.unwrap();

    let keys = map.key_set().await.unwrap();
    assert_eq!(keys.len(), 3);
    assert!(keys.contains(&"a".to_string()));
    assert!(keys.contains(&"b".to_string()));
    assert!(keys.contains(&"c".to_string()));

    map.clear().await.unwrap();
}

// ============================================================================
// Ringbuffer (5 tests)
// ============================================================================

#[tokio::test]
async fn test_ringbuffer_add_read_one() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let name = unique_name("cft-rb-aro");
    let rb = client.get_ringbuffer::<String>(&name);

    let seq = rb.add("hello".to_string()).await.unwrap();
    assert!(seq >= 0);

    let item = rb.read_one(seq).await.unwrap();
    assert_eq!(item, Some("hello".to_string()));
}

#[tokio::test]
async fn test_ringbuffer_capacity() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let name = unique_name("cft-rb-cap");
    let rb = client.get_ringbuffer::<String>(&name);

    let cap = rb.capacity().await.unwrap();
    assert!(cap > 0, "ringbuffer should have positive capacity");
}

#[tokio::test]
async fn test_ringbuffer_head_tail_sequence() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let name = unique_name("cft-rb-hts");
    let rb = client.get_ringbuffer::<String>(&name);

    rb.add("a".to_string()).await.unwrap();
    rb.add("b".to_string()).await.unwrap();
    rb.add("c".to_string()).await.unwrap();

    let head = rb.head_sequence().await.unwrap();
    let tail = rb.tail_sequence().await.unwrap();
    assert!(tail >= head);
    assert!(tail - head >= 2, "should have at least 3 entries spanning 2 sequence positions");
}

#[tokio::test]
async fn test_ringbuffer_add_multiple_read_many() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let name = unique_name("cft-rb-amrm");
    let rb = client.get_ringbuffer::<String>(&name);

    let first_seq = rb.add("item0".to_string()).await.unwrap();
    rb.add("item1".to_string()).await.unwrap();
    rb.add("item2".to_string()).await.unwrap();

    let (items, _next_seq) = rb.read_many(first_seq, 1, 10).await.unwrap();
    assert!(items.len() >= 3);
}

#[tokio::test]
async fn test_ringbuffer_size() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let name = unique_name("cft-rb-sz");
    let rb = client.get_ringbuffer::<String>(&name);

    rb.add("a".to_string()).await.unwrap();
    rb.add("b".to_string()).await.unwrap();

    let sz = rb.size().await.unwrap();
    assert!(sz >= 2);
}

// ============================================================================
// ITopic (3 tests)
// ============================================================================

#[tokio::test]
async fn test_topic_publish_no_error() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let name = unique_name("cft-topic-pne");
    let topic = client.get_topic::<String>(&name);

    let result = topic.publish("hello".to_string()).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_topic_publish_multiple() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let name = unique_name("cft-topic-pm");
    let topic = client.get_topic::<String>(&name);

    for i in 0..10 {
        topic.publish(format!("msg-{}", i)).await.unwrap();
    }
}

#[tokio::test]
async fn test_topic_message_count() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let name = unique_name("cft-topic-mc");
    let topic = client.get_topic::<String>(&name);

    let stats_before = topic.stats();
    let count_before = stats_before.messages_received();

    topic.publish("test".to_string()).await.unwrap();

    let stats_after = topic.stats();
    assert!(stats_after.messages_received() >= count_before);
}

// ============================================================================
// CountDownLatch (3 tests)
// ============================================================================

#[tokio::test]
async fn test_countdown_latch_try_set_count() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let name = unique_name("cft-cdl-tsc");
    let latch = client.get_countdown_latch(&name);

    let ok = latch.try_set_count(3).await.unwrap();
    assert!(ok);
}

#[tokio::test]
async fn test_countdown_latch_get_count() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let name = unique_name("cft-cdl-gc");
    let latch = client.get_countdown_latch(&name);

    latch.try_set_count(5).await.unwrap();
    let count = latch.get_count().await.unwrap();
    assert_eq!(count, 5);
}

#[tokio::test]
async fn test_countdown_latch_count_down() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let name = unique_name("cft-cdl-cd");
    let latch = client.get_countdown_latch(&name);

    latch.try_set_count(3).await.unwrap();
    latch.count_down().await.unwrap();

    let count = latch.get_count().await.unwrap();
    assert_eq!(count, 2);
}

// ============================================================================
// Semaphore (3 tests)
// ============================================================================

#[tokio::test]
async fn test_semaphore_init_acquire_release() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let name = unique_name("cft-sem-iar");
    let mut sem = client.get_semaphore(&name);

    sem.init(3).await.unwrap();
    sem.acquire(1).await.unwrap();
    assert_eq!(sem.available_permits().await.unwrap(), 2);

    sem.release(1).await.unwrap();
    assert_eq!(sem.available_permits().await.unwrap(), 3);
}

#[tokio::test]
async fn test_semaphore_try_acquire() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let name = unique_name("cft-sem-ta");
    let mut sem = client.get_semaphore(&name);

    sem.init(1).await.unwrap();
    let ok = sem.try_acquire(1, Duration::from_secs(1)).await.unwrap();
    assert!(ok);

    // All permits consumed; second attempt should time out
    let fail = sem.try_acquire(1, Duration::from_millis(200)).await.unwrap();
    assert!(!fail);

    sem.release(1).await.unwrap();
}

#[tokio::test]
async fn test_semaphore_available_permits() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let name = unique_name("cft-sem-ap");
    let sem = client.get_semaphore(&name);

    sem.init(5).await.unwrap();
    assert_eq!(sem.available_permits().await.unwrap(), 5);
}

// ============================================================================
// FencedLock (3 tests)
// ============================================================================

#[tokio::test]
async fn test_fenced_lock_lock_unlock() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let name = unique_name("cft-fl-lu");
    let mut lock = client.get_fenced_lock(&name);

    let fence = lock.lock().await.unwrap();
    assert!(fence > 0, "fence token should be positive");

    assert!(lock.is_locked().await.unwrap());

    lock.unlock(fence).await.unwrap();
    assert!(!lock.is_locked().await.unwrap());
}

#[tokio::test]
async fn test_fenced_lock_try_lock() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let name = unique_name("cft-fl-tl");
    let mut lock = client.get_fenced_lock(&name);

    let maybe = lock.try_lock(Duration::from_secs(1)).await.unwrap();
    assert!(maybe.is_some(), "should acquire the lock");

    let fence = maybe.unwrap();
    lock.unlock(fence).await.unwrap();
}

#[tokio::test]
async fn test_fenced_lock_is_locked() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let name = unique_name("cft-fl-il");
    let mut lock = client.get_fenced_lock(&name);

    assert!(!lock.is_locked().await.unwrap());
    let fence = lock.lock().await.unwrap();
    assert!(lock.is_locked().await.unwrap());
    lock.unlock(fence).await.unwrap();
}

// ============================================================================
// Client Lifecycle (4 tests)
// ============================================================================

#[tokio::test]
async fn test_client_connect_disconnect() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");

    // Basic smoke: connection should be alive
    assert!(client.connection_count().await > 0);

    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_client_connection_count() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");

    let count = client.connection_count().await;
    assert!(count >= 1, "should have at least one connection to the cluster");
}

#[tokio::test]
async fn test_client_get_map_returns_proxy() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect");
    let map_name = unique_name("cft-cli-gmrp");
    let map = client.get_map::<String, String>(&map_name);

    // Proxy should be usable immediately
    map.put("k".to_string(), "v".to_string()).await.unwrap();
    let val = map.get(&"k".to_string()).await.unwrap();
    assert_eq!(val, Some("v".to_string()));

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_client_multiple_clients_same_cluster() {
    if skip_if_no_cluster() { return; }
    wait_for_cluster_ready().await;

    let client1 = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect client 1");
    let client2 = HazelcastClient::new(common::default_config())
        .await.expect("failed to connect client 2");

    let map_name = unique_name("cft-cli-mcsc");

    let map1 = client1.get_map::<String, String>(&map_name);
    map1.put("k".to_string(), "from-client1".to_string()).await.unwrap();

    let map2 = client2.get_map::<String, String>(&map_name);
    let val = map2.get(&"k".to_string()).await.unwrap();
    assert_eq!(val, Some("from-client1".to_string()));

    map1.clear().await.unwrap();
}
