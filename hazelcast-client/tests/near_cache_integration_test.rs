//! Integration tests for near cache functionality.

mod common;

use std::sync::Arc;
use std::time::Duration;

use hazelcast_client::{
    ClientConfigBuilder, HazelcastClient, NearCacheConfig, EvictionPolicy, InMemoryFormat,
};

use crate::common::{unique_name, wait_for_cluster_ready, skip_if_no_cluster};

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
        .address(common::DEFAULT_CLUSTER_ADDRESS)
        .add_near_cache_config(near_cache_config)
        .build()
        .unwrap();

    let client = HazelcastClient::new(config).await.expect("failed to connect");
    let map = client.get_map::<String, String>(&map_name).await.unwrap();

    assert!(map.has_near_cache());

    map.put("key1".to_string(), "value1".to_string()).await.unwrap();

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
        .address(common::DEFAULT_CLUSTER_ADDRESS)
        .add_near_cache_config(near_cache_config)
        .build()
        .unwrap();

    let client = HazelcastClient::new(config).await.expect("failed to connect");
    let map = client.get_map::<String, String>(&map_name).await.unwrap();

    map.put("key1".to_string(), "value1".to_string()).await.unwrap();
    let _ = map.get(&"key1".to_string()).await.unwrap();

    map.put("key1".to_string(), "value2".to_string()).await.unwrap();

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
        .address(common::DEFAULT_CLUSTER_ADDRESS)
        .add_near_cache_config(near_cache_config)
        .build()
        .unwrap();

    let client = HazelcastClient::new(config).await.expect("failed to connect");
    let map = client.get_map::<String, String>(&map_name).await.unwrap();

    map.put("key1".to_string(), "value1".to_string()).await.unwrap();
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
        .address(common::DEFAULT_CLUSTER_ADDRESS)
        .add_near_cache_config(near_cache_config)
        .build()
        .unwrap();

    let client = HazelcastClient::new(config).await.expect("failed to connect");
    let map = client.get_map::<String, String>(&map_name).await.unwrap();

    for i in 0..10 {
        map.put(format!("key{}", i), format!("value{}", i)).await.unwrap();
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
        .address(common::DEFAULT_CLUSTER_ADDRESS)
        .add_near_cache_config(near_cache_config)
        .build()
        .unwrap();

    let client = HazelcastClient::new(config).await.expect("failed to connect");
    let map = client.get_map::<String, String>(&map_name).await.unwrap();

    map.put("key1".to_string(), "value1".to_string()).await.unwrap();
    let _ = map.get(&"key1".to_string()).await.unwrap();

    map.invalidate_near_cache_entry(&"key1".to_string()).unwrap();

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
        .address(common::DEFAULT_CLUSTER_ADDRESS)
        .add_near_cache_config(near_cache_config)
        .build()
        .unwrap();

    let client = HazelcastClient::new(config).await.expect("failed to connect");
    let map = client.get_map::<String, String>(&map_name).await.unwrap();

    map.put("key1".to_string(), "value1".to_string()).await.unwrap();
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
        .address(common::DEFAULT_CLUSTER_ADDRESS)
        .add_near_cache_config(near_cache_config)
        .build()
        .unwrap();

    let client = HazelcastClient::new(config).await.expect("failed to connect");
    let map = client.get_map::<String, String>(&map_name).await.unwrap();

    let initial_stats = map.near_cache_stats().unwrap();
    assert_eq!(initial_stats.hits(), 0);
    assert_eq!(initial_stats.misses(), 0);

    map.put("key1".to_string(), "value1".to_string()).await.unwrap();
    
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
        .address(common::DEFAULT_CLUSTER_ADDRESS)
        .add_near_cache_config(near_cache_config)
        .build()
        .unwrap();

    let client = HazelcastClient::new(config).await.expect("failed to connect");
    let map = client.get_map::<String, String>(&map_name).await.unwrap();

    map.put("key1".to_string(), "value1".to_string()).await.unwrap();
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
        .address(common::DEFAULT_CLUSTER_ADDRESS)
        .add_near_cache_config(near_cache_config)
        .build()
        .unwrap();

    let client = HazelcastClient::new(config).await.expect("failed to connect");
    let map = client.get_map::<String, String>(&map_name).await.unwrap();

    for i in 0..20 {
        map.put(format!("key{}", i), format!("value{}", i)).await.unwrap();
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
        .address(common::DEFAULT_CLUSTER_ADDRESS)
        .add_near_cache_config(near_cache_config)
        .build()
        .unwrap();

    let client = HazelcastClient::new(config).await.expect("failed to connect");
    let map = client.get_map::<String, String>(&map_name).await.unwrap();

    for i in 0..5 {
        map.put(format!("key{}", i), format!("value{}", i)).await.unwrap();
        let _ = map.get(&format!("key{}", i)).await.unwrap();
    }

    for _ in 0..10 {
        let _ = map.get(&"key0".to_string()).await.unwrap();
    }

    for i in 5..15 {
        map.put(format!("key{}", i), format!("value{}", i)).await.unwrap();
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
        .address(common::DEFAULT_CLUSTER_ADDRESS)
        .add_near_cache_config(near_cache_config)
        .build()
        .unwrap();

    let client = HazelcastClient::new(config).await.expect("failed to connect");
    let map = client.get_map::<String, String>(&map_name).await.unwrap();

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
        .address(common::DEFAULT_CLUSTER_ADDRESS)
        .add_near_cache_config(near_cache_config)
        .build()
        .unwrap();

    let client = HazelcastClient::new(config).await.expect("failed to connect");
    let map1 = client.get_map::<String, String>(&map_name).await.unwrap();
    let map2 = map1.clone();

    map1.put("key1".to_string(), "value1".to_string()).await.unwrap();
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
        .address(common::DEFAULT_CLUSTER_ADDRESS)
        .add_near_cache_config(near_cache_config)
        .build()
        .unwrap();

    let client = Arc::new(HazelcastClient::new(config).await.expect("failed to connect"));
    
    let map = client.get_map::<String, String>(&map_name).await.unwrap();
    for i in 0..10 {
        map.put(format!("key{}", i), format!("value{}", i)).await.unwrap();
    }

    let mut handles = Vec::new();

    for _ in 0..5 {
        let client_clone = Arc::clone(&client);
        let map_name_clone = map_name.clone();
        
        let handle = tokio::spawn(async move {
            let map = client_clone.get_map::<String, String>(&map_name_clone).await.unwrap();
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
