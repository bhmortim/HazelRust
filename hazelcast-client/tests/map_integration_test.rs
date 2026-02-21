//! Integration tests for IMap distributed map.

mod common;

use std::sync::Arc;
use std::time::Duration;

use hazelcast_client::HazelcastClient;
use hazelcast_client::proxy::{IndexConfig, IndexType};
use hazelcast_client::listener::EntryListenerConfig;

use crate::common::{unique_name, wait_for_cluster_ready, skip_if_no_cluster};

#[tokio::test]
async fn test_map_put_and_get() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let map_name = unique_name("test-map");
    let map = client.get_map::<String, String>(&map_name);

    let old = map.put("key1".to_string(), "value1".to_string()).await.unwrap();
    assert!(old.is_none());

    let value = map.get(&"key1".to_string()).await.unwrap();
    assert_eq!(value, Some("value1".to_string()));

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_map_put_returns_old_value() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let map_name = unique_name("test-map");
    let map = client.get_map::<String, String>(&map_name);

    map.put("key1".to_string(), "value1".to_string()).await.unwrap();
    let old = map.put("key1".to_string(), "value2".to_string()).await.unwrap();
    
    assert_eq!(old, Some("value1".to_string()));

    let current = map.get(&"key1".to_string()).await.unwrap();
    assert_eq!(current, Some("value2".to_string()));

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_map_remove() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let map_name = unique_name("test-map");
    let map = client.get_map::<String, String>(&map_name);

    map.put("key1".to_string(), "value1".to_string()).await.unwrap();
    
    let removed = map.remove(&"key1".to_string()).await.unwrap();
    assert_eq!(removed, Some("value1".to_string()));

    let value = map.get(&"key1".to_string()).await.unwrap();
    assert!(value.is_none());

    let removed_again = map.remove(&"key1".to_string()).await.unwrap();
    assert!(removed_again.is_none());

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_map_contains_key() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let map_name = unique_name("test-map");
    let map = client.get_map::<String, String>(&map_name);

    assert!(!map.contains_key(&"key1".to_string()).await.unwrap());

    map.put("key1".to_string(), "value1".to_string()).await.unwrap();
    assert!(map.contains_key(&"key1".to_string()).await.unwrap());

    map.remove(&"key1".to_string()).await.unwrap();
    assert!(!map.contains_key(&"key1".to_string()).await.unwrap());

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_map_size() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let map_name = unique_name("test-map");
    let map = client.get_map::<String, String>(&map_name);

    assert_eq!(map.size().await.unwrap(), 0);

    map.put("key1".to_string(), "value1".to_string()).await.unwrap();
    assert_eq!(map.size().await.unwrap(), 1);

    map.put("key2".to_string(), "value2".to_string()).await.unwrap();
    assert_eq!(map.size().await.unwrap(), 2);

    map.remove(&"key1".to_string()).await.unwrap();
    assert_eq!(map.size().await.unwrap(), 1);

    map.clear().await.unwrap();
    assert_eq!(map.size().await.unwrap(), 0);
}

#[tokio::test]
async fn test_map_clear() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let map_name = unique_name("test-map");
    let map = client.get_map::<String, String>(&map_name);

    for i in 0..10 {
        map.put(format!("key{}", i), format!("value{}", i)).await.unwrap();
    }
    assert_eq!(map.size().await.unwrap(), 10);

    map.clear().await.unwrap();
    assert_eq!(map.size().await.unwrap(), 0);
}

#[tokio::test]
async fn test_map_with_integer_keys() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let map_name = unique_name("test-map-int");
    let map = client.get_map::<i32, String>(&map_name);

    map.put(42, "answer".to_string()).await.unwrap();
    
    let value = map.get(&42).await.unwrap();
    assert_eq!(value, Some("answer".to_string()));

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_map_with_integer_values() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let map_name = unique_name("test-map-int-val");
    let map = client.get_map::<String, i64>(&map_name);

    map.put("counter".to_string(), 100).await.unwrap();
    
    let value = map.get(&"counter".to_string()).await.unwrap();
    assert_eq!(value, Some(100i64));

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_map_bulk_operations() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let map_name = unique_name("test-map-bulk");
    let map = client.get_map::<String, i32>(&map_name);

    for i in 0..100 {
        map.put(format!("key-{}", i), i).await.unwrap();
    }

    assert_eq!(map.size().await.unwrap(), 100);

    for i in 0..100 {
        let value = map.get(&format!("key-{}", i)).await.unwrap();
        assert_eq!(value, Some(i));
    }

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_map_concurrent_operations() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = Arc::new(
        HazelcastClient::new(common::default_config())
            .await
            .expect("failed to connect")
    );
    let map_name = unique_name("test-map-concurrent");

    let mut handles = Vec::new();

    for i in 0..10 {
        let client_clone = Arc::clone(&client);
        let map_name_clone = map_name.clone();
        
        let handle = tokio::spawn(async move {
            let map = client_clone
                .get_map::<String, i32>(&map_name_clone);
            
            for j in 0..10 {
                let key = format!("key-{}-{}", i, j);
                map.put(key, i * 10 + j).await.unwrap();
            }
        });
        
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }

    let map = client.get_map::<String, i32>(&map_name);
    assert_eq!(map.size().await.unwrap(), 100);

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_map_entry_listener_added_event() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let map_name = unique_name("test-map-listener");
    let map = client.get_map::<String, String>(&map_name);

    let events = Arc::new(std::sync::Mutex::new(Vec::new()));
    let events_clone = Arc::clone(&events);

    let config = EntryListenerConfig::new()
        .include_value(true)
        .on_added();

    let registration = map
        .add_entry_listener(config, move |event| {
            events_clone.lock().unwrap().push(event);
        })
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    map.put("key1".to_string(), "value1".to_string()).await.unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;

    map.remove_entry_listener(&registration).await.unwrap();
    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_map_add_index() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let map_name = unique_name("test-map-index");
    let map = client.get_map::<String, String>(&map_name);

    let sorted_index = IndexConfig::builder()
        .name("age-idx")
        .index_type(IndexType::Sorted)
        .add_attribute("age")
        .build();

    let result = map.add_index(sorted_index).await;
    assert!(result.is_ok());

    let hash_index = IndexConfig::builder()
        .index_type(IndexType::Hash)
        .add_attribute("email")
        .build();

    let result = map.add_index(hash_index).await;
    assert!(result.is_ok());

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_map_special_characters_in_keys() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let map_name = unique_name("test-map-special");
    let map = client.get_map::<String, String>(&map_name);

    let special_keys = vec![
        "key with spaces",
        "key-with-dashes",
        "key_with_underscores",
        "key.with.dots",
        "key/with/slashes",
        "key:with:colons",
        "key@with@at",
        "key#with#hash",
        "日本語キー",
        "🚀emoji🎉key",
    ];

    for (i, key) in special_keys.iter().enumerate() {
        map.put(key.to_string(), format!("value-{}", i)).await.unwrap();
    }

    for (i, key) in special_keys.iter().enumerate() {
        let value = map.get(&key.to_string()).await.unwrap();
        assert_eq!(value, Some(format!("value-{}", i)));
    }

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_map_large_values() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let map_name = unique_name("test-map-large");
    let map = client.get_map::<String, String>(&map_name);

    let large_value = "x".repeat(100_000);
    map.put("large-key".to_string(), large_value.clone()).await.unwrap();

    let retrieved = map.get(&"large-key".to_string()).await.unwrap();
    assert_eq!(retrieved, Some(large_value));

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_map_empty_string_key_and_value() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let map_name = unique_name("test-map-empty");
    let map = client.get_map::<String, String>(&map_name);

    map.put(String::new(), "empty-key-value".to_string()).await.unwrap();
    let value = map.get(&String::new()).await.unwrap();
    assert_eq!(value, Some("empty-key-value".to_string()));

    map.put("non-empty-key".to_string(), String::new()).await.unwrap();
    let value = map.get(&"non-empty-key".to_string()).await.unwrap();
    assert_eq!(value, Some(String::new()));

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_map_clone_shares_state() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let map_name = unique_name("test-map-clone");
    let map1 = client.get_map::<String, String>(&map_name);
    let map2 = map1.clone();

    map1.put("key1".to_string(), "value1".to_string()).await.unwrap();

    let value = map2.get(&"key1".to_string()).await.unwrap();
    assert_eq!(value, Some("value1".to_string()));

    map2.put("key2".to_string(), "value2".to_string()).await.unwrap();

    let value = map1.get(&"key2".to_string()).await.unwrap();
    assert_eq!(value, Some("value2".to_string()));

    map1.clear().await.unwrap();
}
