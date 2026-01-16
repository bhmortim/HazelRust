//! Main integration test entry point.
//!
//! This module re-exports all integration test modules and provides
//! shared setup/teardown functionality.
//!
//! # Running Integration Tests
//!
//! Integration tests require a running Hazelcast cluster. Start one using Docker:
//!
//! ```bash
//! docker run -d --name hazelcast -p 5701:5701 hazelcast/hazelcast:5.3
//! ```
//!
//! Then run the tests:
//!
//! ```bash
//! cargo test --test integration_test
//! ```
//!
//! Or run all integration tests:
//!
//! ```bash
//! cargo test --test '*_integration_test'
//! ```

mod common;

use crate::common::{wait_for_cluster_ready, skip_if_no_cluster, unique_name};

#[tokio::test]
async fn test_cluster_connectivity() {
    if skip_if_no_cluster() {
        eprintln!("Skipping integration tests: no Hazelcast cluster available");
        return;
    }
    wait_for_cluster_ready().await;
    
    let client = hazelcast_client::HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect to cluster");
    
    let map_name = unique_name("connectivity-test");
    let map = client.get_map::<String, String>(&map_name).await.unwrap();
    
    map.put("test-key".to_string(), "test-value".to_string()).await.unwrap();
    let value = map.get(&"test-key".to_string()).await.unwrap();
    assert_eq!(value, Some("test-value".to_string()));
    
    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_multiple_clients() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;
    
    let client1 = hazelcast_client::HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect client1");
    
    let client2 = hazelcast_client::HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect client2");
    
    let map_name = unique_name("multi-client-test");
    
    let map1 = client1.get_map::<String, String>(&map_name).await.unwrap();
    let map2 = client2.get_map::<String, String>(&map_name).await.unwrap();
    
    map1.put("key1".to_string(), "from-client1".to_string()).await.unwrap();
    
    let value = map2.get(&"key1".to_string()).await.unwrap();
    assert_eq!(value, Some("from-client1".to_string()));
    
    map2.put("key2".to_string(), "from-client2".to_string()).await.unwrap();
    
    let value = map1.get(&"key2".to_string()).await.unwrap();
    assert_eq!(value, Some("from-client2".to_string()));
    
    map1.clear().await.unwrap();
}

#[tokio::test]
async fn test_data_structure_isolation() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;
    
    let client = hazelcast_client::HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    
    let map1_name = unique_name("isolation-map1");
    let map2_name = unique_name("isolation-map2");
    
    let map1 = client.get_map::<String, String>(&map1_name).await.unwrap();
    let map2 = client.get_map::<String, String>(&map2_name).await.unwrap();
    
    map1.put("shared-key".to_string(), "map1-value".to_string()).await.unwrap();
    map2.put("shared-key".to_string(), "map2-value".to_string()).await.unwrap();
    
    assert_eq!(
        map1.get(&"shared-key".to_string()).await.unwrap(),
        Some("map1-value".to_string())
    );
    assert_eq!(
        map2.get(&"shared-key".to_string()).await.unwrap(),
        Some("map2-value".to_string())
    );
    
    map1.clear().await.unwrap();
    map2.clear().await.unwrap();
}

#[tokio::test]
async fn test_cross_data_structure_operations() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;
    
    let client = hazelcast_client::HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    
    let map_name = unique_name("cross-ds-map");
    let queue_name = unique_name("cross-ds-queue");
    let set_name = unique_name("cross-ds-set");
    let list_name = unique_name("cross-ds-list");
    
    let map = client.get_map::<String, i32>(&map_name).await.unwrap();
    let queue = client.get_queue::<String>(&queue_name).await.unwrap();
    let set = client.get_set::<String>(&set_name).await.unwrap();
    let list = client.get_list::<String>(&list_name).await.unwrap();
    
    map.put("counter".to_string(), 0).await.unwrap();
    
    for i in 0..5 {
        let item = format!("item-{}", i);
        queue.offer(item.clone()).await.unwrap();
        set.add(item.clone()).await.unwrap();
        list.add(item.clone()).await.unwrap();
        
        let counter = map.get(&"counter".to_string()).await.unwrap().unwrap();
        map.put("counter".to_string(), counter + 1).await.unwrap();
    }
    
    assert_eq!(map.get(&"counter".to_string()).await.unwrap(), Some(5));
    assert_eq!(queue.size().await.unwrap(), 5);
    assert_eq!(set.size().await.unwrap(), 5);
    assert_eq!(list.size().await.unwrap(), 5);
    
    map.clear().await.unwrap();
    while queue.poll().await.unwrap().is_some() {}
    set.clear().await.unwrap();
    list.clear().await.unwrap();
}

#[tokio::test]
async fn test_serialization_types() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;
    
    let client = hazelcast_client::HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    
    let i32_map_name = unique_name("ser-i32");
    let i32_map = client.get_map::<String, i32>(&i32_map_name).await.unwrap();
    i32_map.put("key".to_string(), i32::MAX).await.unwrap();
    assert_eq!(i32_map.get(&"key".to_string()).await.unwrap(), Some(i32::MAX));
    i32_map.clear().await.unwrap();
    
    let i64_map_name = unique_name("ser-i64");
    let i64_map = client.get_map::<String, i64>(&i64_map_name).await.unwrap();
    i64_map.put("key".to_string(), i64::MAX).await.unwrap();
    assert_eq!(i64_map.get(&"key".to_string()).await.unwrap(), Some(i64::MAX));
    i64_map.clear().await.unwrap();
    
    let f32_map_name = unique_name("ser-f32");
    let f32_map = client.get_map::<String, f32>(&f32_map_name).await.unwrap();
    f32_map.put("key".to_string(), std::f32::consts::PI).await.unwrap();
    let value = f32_map.get(&"key".to_string()).await.unwrap().unwrap();
    assert!((value - std::f32::consts::PI).abs() < f32::EPSILON);
    f32_map.clear().await.unwrap();
    
    let f64_map_name = unique_name("ser-f64");
    let f64_map = client.get_map::<String, f64>(&f64_map_name).await.unwrap();
    f64_map.put("key".to_string(), std::f64::consts::E).await.unwrap();
    let value = f64_map.get(&"key".to_string()).await.unwrap().unwrap();
    assert!((value - std::f64::consts::E).abs() < f64::EPSILON);
    f64_map.clear().await.unwrap();
    
    let bool_map_name = unique_name("ser-bool");
    let bool_map = client.get_map::<String, bool>(&bool_map_name).await.unwrap();
    bool_map.put("true".to_string(), true).await.unwrap();
    bool_map.put("false".to_string(), false).await.unwrap();
    assert_eq!(bool_map.get(&"true".to_string()).await.unwrap(), Some(true));
    assert_eq!(bool_map.get(&"false".to_string()).await.unwrap(), Some(false));
    bool_map.clear().await.unwrap();
}

#[tokio::test]
async fn test_empty_operations() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;
    
    let client = hazelcast_client::HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    
    let map_name = unique_name("empty-map");
    let map = client.get_map::<String, String>(&map_name).await.unwrap();
    
    assert_eq!(map.size().await.unwrap(), 0);
    assert_eq!(map.get(&"nonexistent".to_string()).await.unwrap(), None);
    assert_eq!(map.remove(&"nonexistent".to_string()).await.unwrap(), None);
    assert!(!map.contains_key(&"nonexistent".to_string()).await.unwrap());
    
    let queue_name = unique_name("empty-queue");
    let queue = client.get_queue::<String>(&queue_name).await.unwrap();
    
    assert_eq!(queue.size().await.unwrap(), 0);
    assert!(queue.is_empty().await.unwrap());
    assert_eq!(queue.poll().await.unwrap(), None);
    assert_eq!(queue.peek().await.unwrap(), None);
    
    let set_name = unique_name("empty-set");
    let set = client.get_set::<String>(&set_name).await.unwrap();
    
    assert_eq!(set.size().await.unwrap(), 0);
    assert!(set.is_empty().await.unwrap());
    assert!(!set.contains(&"nonexistent".to_string()).await.unwrap());
    
    let list_name = unique_name("empty-list");
    let list = client.get_list::<String>(&list_name).await.unwrap();
    
    assert_eq!(list.size().await.unwrap(), 0);
    assert!(list.is_empty().await.unwrap());
    assert!(!list.contains(&"nonexistent".to_string()).await.unwrap());
}
