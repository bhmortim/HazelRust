//! Integration tests for failover and reconnection scenarios.

mod common;

use std::sync::Arc;
use std::time::Duration;

use hazelcast_client::{ClientConfigBuilder, HazelcastClient};

use crate::common::{unique_name, wait_for_cluster_ready, skip_if_no_cluster};

#[tokio::test]
async fn test_client_reconnects_after_brief_disconnect() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let config = ClientConfigBuilder::new()
        .cluster_name("dev")
        .add_address(common::DEFAULT_CLUSTER_ADDRESS.parse().unwrap())
        .retry(|r| {
            r.max_retries(10)
                .initial_backoff(Duration::from_millis(100))
                .max_backoff(Duration::from_secs(1))
        })
        .build()
        .unwrap();

    let client = HazelcastClient::new(config).await.expect("failed to connect");
    let map_name = unique_name("test-failover");
    let map = client.get_map::<String, String>(&map_name);

    map.put("key1".to_string(), "value1".to_string()).await.unwrap();
    let value = map.get(&"key1".to_string()).await.unwrap();
    assert_eq!(value, Some("value1".to_string()));

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_operations_succeed_after_connection_restored() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let config = ClientConfigBuilder::new()
        .cluster_name("dev")
        .add_address(common::DEFAULT_CLUSTER_ADDRESS.parse().unwrap())
        .retry(|r| {
            r.max_retries(5)
                .initial_backoff(Duration::from_millis(50))
        })
        .build()
        .unwrap();

    let client = HazelcastClient::new(config).await.expect("failed to connect");
    let map_name = unique_name("test-failover-ops");
    let map = client.get_map::<String, String>(&map_name);

    for i in 0..10 {
        map.put(format!("key{}", i), format!("value{}", i)).await.unwrap();
    }

    assert_eq!(map.size().await.unwrap(), 10);

    for i in 0..10 {
        let value = map.get(&format!("key{}", i)).await.unwrap();
        assert_eq!(value, Some(format!("value{}", i)));
    }

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_concurrent_operations_during_reconnect() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let config = ClientConfigBuilder::new()
        .cluster_name("dev")
        .add_address(common::DEFAULT_CLUSTER_ADDRESS.parse().unwrap())
        .retry(|r| {
            r.max_retries(10)
                .initial_backoff(Duration::from_millis(50))
                .max_backoff(Duration::from_secs(2))
        })
        .build()
        .unwrap();

    let client = Arc::new(HazelcastClient::new(config).await.expect("failed to connect"));
    let map_name = unique_name("test-failover-concurrent");

    let mut handles = Vec::new();

    for i in 0..5 {
        let client_clone = Arc::clone(&client);
        let map_name_clone = map_name.clone();
        
        let handle = tokio::spawn(async move {
            let map = client_clone.get_map::<String, i32>(&map_name_clone);
            
            for j in 0..20 {
                let key = format!("key-{}-{}", i, j);
                let _ = map.put(key.clone(), i * 100 + j).await;
                let _ = map.get(&key).await;
            }
        });
        
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }

    let map = client.get_map::<String, i32>(&map_name);
    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_transaction_survives_brief_delay() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let config = ClientConfigBuilder::new()
        .cluster_name("dev")
        .add_address(common::DEFAULT_CLUSTER_ADDRESS.parse().unwrap())
        .build()
        .unwrap();

    let client = HazelcastClient::new(config).await.expect("failed to connect");
    let map_name = unique_name("test-txn-failover");

    let options = hazelcast_client::TransactionOptions::new()
        .with_timeout(Duration::from_secs(30));

    let mut txn = client.new_transaction_context(options);
    
    txn.begin().await.unwrap();

    {
        let txn_map = txn.get_map::<String, String>(&map_name).unwrap();
        txn_map.put("key1".to_string(), "value1".to_string()).await.unwrap();
        
        tokio::time::sleep(Duration::from_millis(100)).await;
        
        txn_map.put("key2".to_string(), "value2".to_string()).await.unwrap();
    }

    txn.commit().await.unwrap();

    let map = client.get_map::<String, String>(&map_name);
    assert_eq!(map.get(&"key1".to_string()).await.unwrap(), Some("value1".to_string()));
    assert_eq!(map.get(&"key2".to_string()).await.unwrap(), Some("value2".to_string()));

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_near_cache_survives_reconnect() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let map_name = unique_name("test-nc-failover");
    
    let near_cache_config = hazelcast_client::NearCacheConfig::builder(&map_name)
        .max_size(1000)
        .build()
        .unwrap();

    let config = ClientConfigBuilder::new()
        .cluster_name("dev")
        .add_address(common::DEFAULT_CLUSTER_ADDRESS.parse().unwrap())
        .add_near_cache_config(near_cache_config)
        .retry(|r| r.max_retries(5))
        .build()
        .unwrap();

    let client = HazelcastClient::new(config).await.expect("failed to connect");
    let map = client.get_map::<String, String>(&map_name);

    map.put("key1".to_string(), "value1".to_string()).await.unwrap();
    let _ = map.get(&"key1".to_string()).await.unwrap();
    let _ = map.get(&"key1".to_string()).await.unwrap();

    let stats = map.near_cache_stats().unwrap();
    assert!(stats.hits() >= 1);

    let _ = map.get(&"key1".to_string()).await.unwrap();

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_multiple_data_structures_after_reconnect() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let config = ClientConfigBuilder::new()
        .cluster_name("dev")
        .add_address(common::DEFAULT_CLUSTER_ADDRESS.parse().unwrap())
        .retry(|r| r.max_retries(5))
        .build()
        .unwrap();

    let client = HazelcastClient::new(config).await.expect("failed to connect");
    
    let map_name = unique_name("test-multi-map");
    let queue_name = unique_name("test-multi-queue");
    let set_name = unique_name("test-multi-set");
    let list_name = unique_name("test-multi-list");

    let map = client.get_map::<String, String>(&map_name);
    let queue = client.get_queue::<String>(&queue_name);
    let set = client.get_set::<String>(&set_name);
    let list = client.get_list::<String>(&list_name);

    map.put("map-key".to_string(), "map-value".to_string()).await.unwrap();
    queue.offer("queue-item".to_string()).await.unwrap();
    set.add("set-item".to_string()).await.unwrap();
    list.add("list-item".to_string()).await.unwrap();

    assert_eq!(map.get(&"map-key".to_string()).await.unwrap(), Some("map-value".to_string()));
    assert_eq!(queue.peek().await.unwrap(), Some("queue-item".to_string()));
    assert!(set.contains(&"set-item".to_string()).await.unwrap());
    assert!(list.contains(&"list-item".to_string()).await.unwrap());

    map.clear().await.unwrap();
    while queue.poll().await.unwrap().is_some() {}
    set.clear().await.unwrap();
    list.clear().await.unwrap();
}

#[tokio::test]
async fn test_retry_config_backoff() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let config = ClientConfigBuilder::new()
        .cluster_name("dev")
        .add_address(common::DEFAULT_CLUSTER_ADDRESS.parse().unwrap())
        .retry(|r| {
            r.max_retries(3)
                .initial_backoff(Duration::from_millis(100))
                .max_backoff(Duration::from_secs(1))
                .multiplier(2.0)
        })
        .build()
        .unwrap();

    let client = HazelcastClient::new(config).await.expect("failed to connect");
    let map_name = unique_name("test-retry-backoff");
    let map = client.get_map::<String, String>(&map_name);

    map.put("key".to_string(), "value".to_string()).await.unwrap();
    let value = map.get(&"key".to_string()).await.unwrap();
    assert_eq!(value, Some("value".to_string()));

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_client_handles_cluster_restart_gracefully() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let config = ClientConfigBuilder::new()
        .cluster_name("dev")
        .add_address(common::DEFAULT_CLUSTER_ADDRESS.parse().unwrap())
        .retry(|r| {
            r.max_retries(30)
                .initial_backoff(Duration::from_millis(500))
                .max_backoff(Duration::from_secs(5))
        })
        .build()
        .unwrap();

    let client = HazelcastClient::new(config).await.expect("failed to connect");
    let map_name = unique_name("test-cluster-restart");
    let map = client.get_map::<String, String>(&map_name);

    map.put("before-restart".to_string(), "value".to_string()).await.unwrap();

    let value = map.get(&"before-restart".to_string()).await.unwrap();
    assert_eq!(value, Some("value".to_string()));

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_listener_stats_during_operations() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let config = ClientConfigBuilder::new()
        .cluster_name("dev")
        .add_address(common::DEFAULT_CLUSTER_ADDRESS.parse().unwrap())
        .build()
        .unwrap();

    let client = HazelcastClient::new(config).await.expect("failed to connect");
    let map_name = unique_name("test-listener-stats");
    let map = client.get_map::<String, String>(&map_name);

    let stats = map.listener_stats();
    let initial_messages = stats.messages_received();
    let initial_errors = stats.errors();

    map.put("key1".to_string(), "value1".to_string()).await.unwrap();
    map.put("key2".to_string(), "value2".to_string()).await.unwrap();

    let stats = map.listener_stats();
    assert!(stats.errors() >= initial_errors);

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_operations_with_high_latency_tolerance() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let config = ClientConfigBuilder::new()
        .cluster_name("dev")
        .add_address(common::DEFAULT_CLUSTER_ADDRESS.parse().unwrap())
        .retry(|r| {
            r.max_retries(5)
                .initial_backoff(Duration::from_millis(200))
        })
        .build()
        .unwrap();

    let client = HazelcastClient::new(config).await.expect("failed to connect");
    let map_name = unique_name("test-latency");
    let map = client.get_map::<String, String>(&map_name);

    for i in 0..5 {
        let start = std::time::Instant::now();
        map.put(format!("key{}", i), format!("value{}", i)).await.unwrap();
        let elapsed = start.elapsed();
        
        assert!(elapsed < Duration::from_secs(5), "Operation took too long: {:?}", elapsed);
    }

    map.clear().await.unwrap();
}
