//! Integration tests for near cache functionality.

mod common;

use std::sync::Arc;
use std::time::Duration;

use hazelcast_client::{ClientConfigBuilder, EvictionPolicy, HazelcastClient, NearCacheConfig};

use crate::common::{skip_if_no_cluster, unique_name, wait_for_cluster_ready};

#[tokio::test]
async fn test_near_cache_get_populates_cache() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let map_name = unique_name("test-nc-populate");

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

    let client = HazelcastClient::new(config)
        .await
        .expect("failed to connect");
    let map = client.get_map::<String, String>(&map_name);

    assert!(map.has_near_cache());

    map.put("key1".to_string(), "value1".to_string())
        .await
        .unwrap();

    let _ = map.get(&"key1".to_string()).await.unwrap();

    let stats = map.near_cache_stats().unwrap();
    assert_eq!(stats.misses(), 1);

    let _ = map.get(&"key1".to_string()).await.unwrap();

    let stats = map.near_cache_stats().unwrap();
    assert_eq!(stats.hits(), 1);
    assert_eq!(stats.misses(), 1);

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_near_cache_put_invalidates() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let map_name = unique_name("test-nc-invalidate");

    let near_cache_config = NearCacheConfig::builder(&map_name)
        .max_size(1000)
        .build()
        .unwrap();

    let config = ClientConfigBuilder::new()
        .cluster_name("dev")
        .add_address(common::DEFAULT_CLUSTER_ADDRESS.parse().unwrap())
        .add_near_cache_config(near_cache_config)
        .build()
        .unwrap();

    let client = HazelcastClient::new(config)
        .await
        .expect("failed to connect");
    let map = client.get_map::<String, String>(&map_name);

    map.put("key1".to_string(), "value1".to_string())
        .await
        .unwrap();
    let _ = map.get(&"key1".to_string()).await.unwrap();

    map.put("key1".to_string(), "value2".to_string())
        .await
        .unwrap();

    let value = map.get(&"key1".to_string()).await.unwrap();
    assert_eq!(value, Some("value2".to_string()));

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_near_cache_remove_invalidates() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let map_name = unique_name("test-nc-remove");

    let near_cache_config = NearCacheConfig::builder(&map_name)
        .max_size(1000)
        .build()
        .unwrap();

    let config = ClientConfigBuilder::new()
        .cluster_name("dev")
        .add_address(common::DEFAULT_CLUSTER_ADDRESS.parse().unwrap())
        .add_near_cache_config(near_cache_config)
        .build()
        .unwrap();

    let client = HazelcastClient::new(config)
        .await
        .expect("failed to connect");
    let map = client.get_map::<String, String>(&map_name);

    map.put("key1".to_string(), "value1".to_string())
        .await
        .unwrap();
    let _ = map.get(&"key1".to_string()).await.unwrap();

    map.remove(&"key1".to_string()).await.unwrap();

    let value = map.get(&"key1".to_string()).await.unwrap();
    assert!(value.is_none());

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_near_cache_clear_invalidates_all() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let map_name = unique_name("test-nc-clear");

    let near_cache_config = NearCacheConfig::builder(&map_name)
        .max_size(1000)
        .build()
        .unwrap();

    let config = ClientConfigBuilder::new()
        .cluster_name("dev")
        .add_address(common::DEFAULT_CLUSTER_ADDRESS.parse().unwrap())
        .add_near_cache_config(near_cache_config)
        .build()
        .unwrap();

    let client = HazelcastClient::new(config)
        .await
        .expect("failed to connect");
    let map = client.get_map::<String, String>(&map_name);

    for i in 0..10 {
        map.put(format!("key{}", i), format!("value{}", i))
            .await
            .unwrap();
    }

    for i in 0..10 {
        let _ = map.get(&format!("key{}", i)).await.unwrap();
    }

    map.clear().await.unwrap();

    for i in 0..10 {
        let value = map.get(&format!("key{}", i)).await.unwrap();
        assert!(value.is_none());
    }
}

#[tokio::test]
async fn test_near_cache_manual_invalidation() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let map_name = unique_name("test-nc-manual");

    let near_cache_config = NearCacheConfig::builder(&map_name)
        .max_size(1000)
        .build()
        .unwrap();

    let config = ClientConfigBuilder::new()
        .cluster_name("dev")
        .add_address(common::DEFAULT_CLUSTER_ADDRESS.parse().unwrap())
        .add_near_cache_config(near_cache_config)
        .build()
        .unwrap();

    let client = HazelcastClient::new(config)
        .await
        .expect("failed to connect");
    let map = client.get_map::<String, String>(&map_name);

    map.put("key1".to_string(), "value1".to_string())
        .await
        .unwrap();
    let _ = map.get(&"key1".to_string()).await.unwrap();

    map.invalidate_near_cache_entry(&"key1".to_string())
        .unwrap();

    let _ = map.get(&"key1".to_string()).await.unwrap();

    let stats = map.near_cache_stats().unwrap();
    assert_eq!(stats.misses(), 2);

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_near_cache_clear_local_only() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let map_name = unique_name("test-nc-clear-local");

    let near_cache_config = NearCacheConfig::builder(&map_name)
        .max_size(1000)
        .build()
        .unwrap();

    let config = ClientConfigBuilder::new()
        .cluster_name("dev")
        .add_address(common::DEFAULT_CLUSTER_ADDRESS.parse().unwrap())
        .add_near_cache_config(near_cache_config)
        .build()
        .unwrap();

    let client = HazelcastClient::new(config)
        .await
        .expect("failed to connect");
    let map = client.get_map::<String, String>(&map_name);

    map.put("key1".to_string(), "value1".to_string())
        .await
        .unwrap();
    let _ = map.get(&"key1".to_string()).await.unwrap();

    map.clear_near_cache();

    let value = map.get(&"key1".to_string()).await.unwrap();
    assert_eq!(value, Some("value1".to_string()));

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_near_cache_stats() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let map_name = unique_name("test-nc-stats");

    let near_cache_config = NearCacheConfig::builder(&map_name)
        .max_size(1000)
        .build()
        .unwrap();

    let config = ClientConfigBuilder::new()
        .cluster_name("dev")
        .add_address(common::DEFAULT_CLUSTER_ADDRESS.parse().unwrap())
        .add_near_cache_config(near_cache_config)
        .build()
        .unwrap();

    let client = HazelcastClient::new(config)
        .await
        .expect("failed to connect");
    let map = client.get_map::<String, String>(&map_name);

    let initial_stats = map.near_cache_stats().unwrap();
    assert_eq!(initial_stats.hits(), 0);
    assert_eq!(initial_stats.misses(), 0);

    map.put("key1".to_string(), "value1".to_string())
        .await
        .unwrap();

    let _ = map.get(&"key1".to_string()).await.unwrap();
    let stats = map.near_cache_stats().unwrap();
    assert_eq!(stats.misses(), 1);

    let _ = map.get(&"key1".to_string()).await.unwrap();
    let _ = map.get(&"key1".to_string()).await.unwrap();
    let stats = map.near_cache_stats().unwrap();
    assert_eq!(stats.hits(), 2);

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_near_cache_with_ttl() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let map_name = unique_name("test-nc-ttl");

    let near_cache_config = NearCacheConfig::builder(&map_name)
        .max_size(1000)
        .time_to_live(Duration::from_secs(1))
        .build()
        .unwrap();

    let config = ClientConfigBuilder::new()
        .cluster_name("dev")
        .add_address(common::DEFAULT_CLUSTER_ADDRESS.parse().unwrap())
        .add_near_cache_config(near_cache_config)
        .build()
        .unwrap();

    let client = HazelcastClient::new(config)
        .await
        .expect("failed to connect");
    let map = client.get_map::<String, String>(&map_name);

    map.put("key1".to_string(), "value1".to_string())
        .await
        .unwrap();
    let _ = map.get(&"key1".to_string()).await.unwrap();
    let _ = map.get(&"key1".to_string()).await.unwrap();

    let stats = map.near_cache_stats().unwrap();
    assert_eq!(stats.hits(), 1);

    tokio::time::sleep(Duration::from_secs(2)).await;

    let _ = map.get(&"key1".to_string()).await.unwrap();

    let stats = map.near_cache_stats().unwrap();
    assert!(stats.misses() >= 2);

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_near_cache_eviction_lru() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let map_name = unique_name("test-nc-lru");

    let near_cache_config = NearCacheConfig::builder(&map_name)
        .max_size(10)
        .eviction_policy(EvictionPolicy::Lru)
        .build()
        .unwrap();

    let config = ClientConfigBuilder::new()
        .cluster_name("dev")
        .add_address(common::DEFAULT_CLUSTER_ADDRESS.parse().unwrap())
        .add_near_cache_config(near_cache_config)
        .build()
        .unwrap();

    let client = HazelcastClient::new(config)
        .await
        .expect("failed to connect");
    let map = client.get_map::<String, String>(&map_name);

    for i in 0..20 {
        map.put(format!("key{}", i), format!("value{}", i))
            .await
            .unwrap();
        let _ = map.get(&format!("key{}", i)).await.unwrap();
    }

    let stats = map.near_cache_stats().unwrap();
    assert!(stats.evictions() > 0);

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_near_cache_eviction_lfu() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let map_name = unique_name("test-nc-lfu");

    let near_cache_config = NearCacheConfig::builder(&map_name)
        .max_size(10)
        .eviction_policy(EvictionPolicy::Lfu)
        .build()
        .unwrap();

    let config = ClientConfigBuilder::new()
        .cluster_name("dev")
        .add_address(common::DEFAULT_CLUSTER_ADDRESS.parse().unwrap())
        .add_near_cache_config(near_cache_config)
        .build()
        .unwrap();

    let client = HazelcastClient::new(config)
        .await
        .expect("failed to connect");
    let map = client.get_map::<String, String>(&map_name);

    for i in 0..5 {
        map.put(format!("key{}", i), format!("value{}", i))
            .await
            .unwrap();
        let _ = map.get(&format!("key{}", i)).await.unwrap();
    }

    for _ in 0..10 {
        let _ = map.get(&"key0".to_string()).await.unwrap();
    }

    for i in 5..15 {
        map.put(format!("key{}", i), format!("value{}", i))
            .await
            .unwrap();
        let _ = map.get(&format!("key{}", i)).await.unwrap();
    }

    let _ = map.get(&"key0".to_string()).await.unwrap();
    let stats = map.near_cache_stats().unwrap();
    assert!(stats.hits() >= 10);

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_near_cache_wildcard_config() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let map_name = unique_name("users-cache");

    let near_cache_config = NearCacheConfig::builder("users-*")
        .max_size(1000)
        .build()
        .unwrap();

    let config = ClientConfigBuilder::new()
        .cluster_name("dev")
        .add_address(common::DEFAULT_CLUSTER_ADDRESS.parse().unwrap())
        .add_near_cache_config(near_cache_config)
        .build()
        .unwrap();

    let client = HazelcastClient::new(config)
        .await
        .expect("failed to connect");
    let map = client.get_map::<String, String>(&map_name);

    assert!(map.has_near_cache());

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_near_cache_clone_shares_cache() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let map_name = unique_name("test-nc-clone");

    let near_cache_config = NearCacheConfig::builder(&map_name)
        .max_size(1000)
        .build()
        .unwrap();

    let config = ClientConfigBuilder::new()
        .cluster_name("dev")
        .add_address(common::DEFAULT_CLUSTER_ADDRESS.parse().unwrap())
        .add_near_cache_config(near_cache_config)
        .build()
        .unwrap();

    let client = HazelcastClient::new(config)
        .await
        .expect("failed to connect");
    let map1 = client.get_map::<String, String>(&map_name);
    let map2 = map1.clone();

    map1.put("key1".to_string(), "value1".to_string())
        .await
        .unwrap();
    let _ = map1.get(&"key1".to_string()).await.unwrap();

    let _ = map2.get(&"key1".to_string()).await.unwrap();

    let stats1 = map1.near_cache_stats().unwrap();
    let stats2 = map2.near_cache_stats().unwrap();

    assert_eq!(stats1.hits(), stats2.hits());

    map1.clear().await.unwrap();
}

#[tokio::test]
async fn test_near_cache_concurrent_access() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let map_name = unique_name("test-nc-concurrent");

    let near_cache_config = NearCacheConfig::builder(&map_name)
        .max_size(1000)
        .build()
        .unwrap();

    let config = ClientConfigBuilder::new()
        .cluster_name("dev")
        .add_address(common::DEFAULT_CLUSTER_ADDRESS.parse().unwrap())
        .add_near_cache_config(near_cache_config)
        .build()
        .unwrap();

    let client = Arc::new(
        HazelcastClient::new(config)
            .await
            .expect("failed to connect"),
    );

    let map = client.get_map::<String, String>(&map_name);
    for i in 0..10 {
        map.put(format!("key{}", i), format!("value{}", i))
            .await
            .unwrap();
    }

    let mut handles = Vec::new();

    for _ in 0..5 {
        let client_clone = Arc::clone(&client);
        let map_name_clone = map_name.clone();

        let handle = tokio::spawn(async move {
            let map = client_clone.get_map::<String, String>(&map_name_clone);
            for _ in 0..100 {
                for i in 0..10 {
                    let _ = map.get(&format!("key{}", i)).await;
                }
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }

    let stats = map.near_cache_stats().unwrap();
    assert!(stats.hits() > 0);

    map.clear().await.unwrap();
}

/// A write on one client must invalidate the near cache of every *other*
/// client, not just its own.
///
/// This is the scenario that made the eviction-key mismatch matter in
/// practice. `IMap::get` populates the near cache under `serialize_value(key)`
/// — full `Data` form, `[partition_hash i32][type_id i32][payload]` — while the
/// invalidation listener used to rebuild the key with a bare
/// `ObjectDataOutput`, omitting the 8-byte header. The eviction addressed a key
/// that was never stored, so it removed nothing, silently.
///
/// A single client never sees it: `put`, `set` and `remove` invalidate through
/// `serialize_value`, so a client's own writes always evicted correctly. It
/// takes two clients for the bug to appear, which is why the unit test on key
/// derivation is not enough on its own.
///
/// Marked `#[ignore]` like the other cluster-backed tests; run with
/// `cargo test --test near_cache_integration_test -- --ignored`.
#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_near_cache_invalidated_by_another_clients_write() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let map_name = unique_name("test-nc-cross-client");
    let key = "order:42".to_string();

    // Two independent clients, each with its own near cache on the same map.
    let mut clients = Vec::new();
    for _ in 0..2 {
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
        clients.push(
            HazelcastClient::new(config)
                .await
                .expect("failed to connect"),
        );
    }
    let reader = clients[0].get_map::<String, String>(&map_name);
    let writer = clients[1].get_map::<String, String>(&map_name);
    reader.start_near_cache_invalidation().await.unwrap();
    writer.start_near_cache_invalidation().await.unwrap();

    writer
        .put(key.clone(), "tier=silver".to_string())
        .await
        .unwrap();

    // First read populates the reader's near cache; the second must be served
    // from it. Asserting the hit matters: without it, a near cache that never
    // engaged would make the rest of this test pass for the wrong reason.
    assert_eq!(
        reader.get(&key).await.unwrap(),
        Some("tier=silver".to_string())
    );
    assert_eq!(
        reader.get(&key).await.unwrap(),
        Some("tier=silver".to_string())
    );
    let stats = reader.near_cache_stats().unwrap();
    assert_eq!(
        stats.hits(),
        1,
        "second read should come from the near cache"
    );

    // The write the reader must not miss.
    writer
        .put(key.clone(), "tier=GOLD".to_string())
        .await
        .unwrap();

    // Invalidation is a server push and normally lands within a millisecond;
    // poll rather than sleep a fixed amount, so a slow cluster is not a flake
    // and a broken one is not hidden by a generous sleep. Unfixed, this never
    // converges and the assertion below reports the stale value.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let mut observed = None;
    while std::time::Instant::now() < deadline {
        observed = reader.get(&key).await.unwrap();
        if observed.as_deref() == Some("tier=GOLD") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    assert_eq!(
        observed,
        Some("tier=GOLD".to_string()),
        "the reader's near cache was never invalidated by the writer's update, \
         so it is still serving a stale value; the cluster holds {:?}",
        writer.get(&key).await.unwrap()
    );

    reader.clear().await.unwrap();
    for client in clients {
        client.shutdown().await.expect("shutdown failed");
    }
}
