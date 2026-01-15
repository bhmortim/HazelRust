//! Integration tests for HazelcastClient and IMap.
//!
//! These tests require a running Hazelcast instance.
//!
//! # Docker Setup
//!
//! Start a Hazelcast container:
//! ```bash
//! docker run -d --name hazelcast -p 5701:5701 hazelcast/hazelcast:5.3
//! ```
//!
//! Run the tests:
//! ```bash
//! cargo test --package hazelcast-client --test integration_test -- --ignored
//! ```

use std::time::Duration;

use hazelcast_client::{ClientConfig, ClientConfigBuilder, HazelcastClient};

async fn create_test_client() -> HazelcastClient {
    let config = ClientConfigBuilder::new()
        .cluster_name("dev")
        .add_address("127.0.0.1:5701".parse().unwrap())
        .connection_timeout(Duration::from_secs(10))
        .build()
        .unwrap();

    HazelcastClient::new(config)
        .await
        .expect("Failed to connect to Hazelcast. Is the server running?")
}

#[tokio::test]
#[ignore = "requires running Hazelcast instance"]
async fn test_client_connect() {
    let client = create_test_client().await;

    assert_eq!(client.cluster_name(), "dev");
    assert!(client.connection_count().await > 0);

    client.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast instance"]
async fn test_map_put_and_get() {
    let client = create_test_client().await;
    let map = client.get_map::<String, String>("test-map-put-get");

    map.clear().await.unwrap();

    let old_value = map.put("key1".to_string(), "value1".to_string()).await.unwrap();
    assert!(old_value.is_none());

    let value = map.get(&"key1".to_string()).await.unwrap();
    assert_eq!(value, Some("value1".to_string()));

    client.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast instance"]
async fn test_map_put_returns_old_value() {
    let client = create_test_client().await;
    let map = client.get_map::<String, String>("test-map-old-value");

    map.clear().await.unwrap();

    map.put("key1".to_string(), "value1".to_string()).await.unwrap();
    let old_value = map.put("key1".to_string(), "value2".to_string()).await.unwrap();

    assert_eq!(old_value, Some("value1".to_string()));

    let current = map.get(&"key1".to_string()).await.unwrap();
    assert_eq!(current, Some("value2".to_string()));

    client.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast instance"]
async fn test_map_get_nonexistent_key() {
    let client = create_test_client().await;
    let map = client.get_map::<String, String>("test-map-nonexistent");

    map.clear().await.unwrap();

    let value = map.get(&"nonexistent-key".to_string()).await.unwrap();
    assert!(value.is_none());

    client.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast instance"]
async fn test_map_remove() {
    let client = create_test_client().await;
    let map = client.get_map::<String, String>("test-map-remove");

    map.clear().await.unwrap();

    map.put("key1".to_string(), "value1".to_string()).await.unwrap();

    let removed = map.remove(&"key1".to_string()).await.unwrap();
    assert_eq!(removed, Some("value1".to_string()));

    let value = map.get(&"key1".to_string()).await.unwrap();
    assert!(value.is_none());

    client.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast instance"]
async fn test_map_remove_nonexistent() {
    let client = create_test_client().await;
    let map = client.get_map::<String, String>("test-map-remove-nonexistent");

    map.clear().await.unwrap();

    let removed = map.remove(&"nonexistent".to_string()).await.unwrap();
    assert!(removed.is_none());

    client.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast instance"]
async fn test_map_contains_key() {
    let client = create_test_client().await;
    let map = client.get_map::<String, String>("test-map-contains");

    map.clear().await.unwrap();

    assert!(!map.contains_key(&"key1".to_string()).await.unwrap());

    map.put("key1".to_string(), "value1".to_string()).await.unwrap();

    assert!(map.contains_key(&"key1".to_string()).await.unwrap());

    client.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast instance"]
async fn test_map_size() {
    let client = create_test_client().await;
    let map = client.get_map::<String, String>("test-map-size");

    map.clear().await.unwrap();

    assert_eq!(map.size().await.unwrap(), 0);

    map.put("key1".to_string(), "value1".to_string()).await.unwrap();
    assert_eq!(map.size().await.unwrap(), 1);

    map.put("key2".to_string(), "value2".to_string()).await.unwrap();
    assert_eq!(map.size().await.unwrap(), 2);

    map.remove(&"key1".to_string()).await.unwrap();
    assert_eq!(map.size().await.unwrap(), 1);

    client.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast instance"]
async fn test_map_clear() {
    let client = create_test_client().await;
    let map = client.get_map::<String, String>("test-map-clear");

    map.put("key1".to_string(), "value1".to_string()).await.unwrap();
    map.put("key2".to_string(), "value2".to_string()).await.unwrap();
    map.put("key3".to_string(), "value3".to_string()).await.unwrap();

    assert!(map.size().await.unwrap() >= 3);

    map.clear().await.unwrap();

    assert_eq!(map.size().await.unwrap(), 0);

    client.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast instance"]
async fn test_map_with_integer_keys() {
    let client = create_test_client().await;
    let map = client.get_map::<i32, String>("test-map-int-keys");

    map.clear().await.unwrap();

    map.put(42, "forty-two".to_string()).await.unwrap();
    map.put(-1, "negative-one".to_string()).await.unwrap();

    assert_eq!(map.get(&42).await.unwrap(), Some("forty-two".to_string()));
    assert_eq!(map.get(&-1).await.unwrap(), Some("negative-one".to_string()));

    client.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast instance"]
async fn test_map_with_integer_values() {
    let client = create_test_client().await;
    let map = client.get_map::<String, i64>("test-map-int-values");

    map.clear().await.unwrap();

    map.put("count".to_string(), 12345678901234i64).await.unwrap();

    let value = map.get(&"count".to_string()).await.unwrap();
    assert_eq!(value, Some(12345678901234i64));

    client.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast instance"]
async fn test_multiple_maps() {
    let client = create_test_client().await;

    let map1 = client.get_map::<String, String>("multi-map-1");
    let map2 = client.get_map::<String, String>("multi-map-2");

    map1.clear().await.unwrap();
    map2.clear().await.unwrap();

    map1.put("key".to_string(), "value-from-map1".to_string()).await.unwrap();
    map2.put("key".to_string(), "value-from-map2".to_string()).await.unwrap();

    assert_eq!(
        map1.get(&"key".to_string()).await.unwrap(),
        Some("value-from-map1".to_string())
    );
    assert_eq!(
        map2.get(&"key".to_string()).await.unwrap(),
        Some("value-from-map2".to_string())
    );

    client.shutdown().await.unwrap();
}
