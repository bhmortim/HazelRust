//! Integration tests for the distributed ReplicatedMap.

use hazelcast_client::{ClientConfig, HazelcastClient};

async fn create_client() -> HazelcastClient {
    let config = ClientConfig::builder()
        .cluster_name("dev")
        .build()
        .expect("Failed to build config");

    HazelcastClient::new(config)
        .await
        .expect("Failed to connect to cluster")
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_replicated_map_put_and_get() {
    let client = create_client().await;
    let map = client.get_replicated_map::<String, String>("test-replicated-map-put-get");

    map.clear().await.expect("Failed to clear map");

    let prev = map
        .put("key1".to_string(), "value1".to_string())
        .await
        .expect("Failed to put");
    assert!(prev.is_none());

    let value = map.get(&"key1".to_string()).await.expect("Failed to get");
    assert_eq!(value, Some("value1".to_string()));

    let prev = map
        .put("key1".to_string(), "value2".to_string())
        .await
        .expect("Failed to put");
    assert_eq!(prev, Some("value1".to_string()));

    let value = map.get(&"key1".to_string()).await.expect("Failed to get");
    assert_eq!(value, Some("value2".to_string()));

    client.shutdown().await.expect("Failed to shutdown");
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_replicated_map_remove() {
    let client = create_client().await;
    let map = client.get_replicated_map::<String, i32>("test-replicated-map-remove");

    map.clear().await.expect("Failed to clear map");

    map.put("key1".to_string(), 100)
        .await
        .expect("Failed to put");

    let removed = map
        .remove(&"key1".to_string())
        .await
        .expect("Failed to remove");
    assert_eq!(removed, Some(100));

    let removed = map
        .remove(&"key1".to_string())
        .await
        .expect("Failed to remove");
    assert!(removed.is_none());

    client.shutdown().await.expect("Failed to shutdown");
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_replicated_map_contains_key() {
    let client = create_client().await;
    let map = client.get_replicated_map::<String, String>("test-replicated-map-contains-key");

    map.clear().await.expect("Failed to clear map");

    let contains = map
        .contains_key(&"key1".to_string())
        .await
        .expect("Failed to check contains_key");
    assert!(!contains);

    map.put("key1".to_string(), "value1".to_string())
        .await
        .expect("Failed to put");

    let contains = map
        .contains_key(&"key1".to_string())
        .await
        .expect("Failed to check contains_key");
    assert!(contains);

    client.shutdown().await.expect("Failed to shutdown");
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_replicated_map_contains_value() {
    let client = create_client().await;
    let map = client.get_replicated_map::<String, String>("test-replicated-map-contains-value");

    map.clear().await.expect("Failed to clear map");

    let contains = map
        .contains_value(&"value1".to_string())
        .await
        .expect("Failed to check contains_value");
    assert!(!contains);

    map.put("key1".to_string(), "value1".to_string())
        .await
        .expect("Failed to put");

    let contains = map
        .contains_value(&"value1".to_string())
        .await
        .expect("Failed to check contains_value");
    assert!(contains);

    client.shutdown().await.expect("Failed to shutdown");
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_replicated_map_size_and_is_empty() {
    let client = create_client().await;
    let map = client.get_replicated_map::<String, i32>("test-replicated-map-size");

    map.clear().await.expect("Failed to clear map");

    let is_empty = map.is_empty().await.expect("Failed to check is_empty");
    assert!(is_empty);

    let size = map.size().await.expect("Failed to get size");
    assert_eq!(size, 0);

    map.put("key1".to_string(), 1).await.expect("Failed to put");
    map.put("key2".to_string(), 2).await.expect("Failed to put");
    map.put("key3".to_string(), 3).await.expect("Failed to put");

    let is_empty = map.is_empty().await.expect("Failed to check is_empty");
    assert!(!is_empty);

    let size = map.size().await.expect("Failed to get size");
    assert_eq!(size, 3);

    client.shutdown().await.expect("Failed to shutdown");
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_replicated_map_clear() {
    let client = create_client().await;
    let map = client.get_replicated_map::<String, String>("test-replicated-map-clear");

    map.put("key1".to_string(), "value1".to_string())
        .await
        .expect("Failed to put");
    map.put("key2".to_string(), "value2".to_string())
        .await
        .expect("Failed to put");

    let size = map.size().await.expect("Failed to get size");
    assert!(size >= 2);

    map.clear().await.expect("Failed to clear map");

    let size = map.size().await.expect("Failed to get size");
    assert_eq!(size, 0);

    client.shutdown().await.expect("Failed to shutdown");
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_replicated_map_key_set() {
    let client = create_client().await;
    let map = client.get_replicated_map::<String, i32>("test-replicated-map-key-set");

    map.clear().await.expect("Failed to clear map");

    map.put("alpha".to_string(), 1).await.expect("Failed to put");
    map.put("beta".to_string(), 2).await.expect("Failed to put");
    map.put("gamma".to_string(), 3).await.expect("Failed to put");

    let keys = map.key_set().await.expect("Failed to get key_set");
    assert_eq!(keys.len(), 3);
    assert!(keys.contains(&"alpha".to_string()));
    assert!(keys.contains(&"beta".to_string()));
    assert!(keys.contains(&"gamma".to_string()));

    client.shutdown().await.expect("Failed to shutdown");
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_replicated_map_values() {
    let client = create_client().await;
    let map = client.get_replicated_map::<String, i32>("test-replicated-map-values");

    map.clear().await.expect("Failed to clear map");

    map.put("a".to_string(), 10).await.expect("Failed to put");
    map.put("b".to_string(), 20).await.expect("Failed to put");
    map.put("c".to_string(), 30).await.expect("Failed to put");

    let values = map.values().await.expect("Failed to get values");
    assert_eq!(values.len(), 3);
    assert!(values.contains(&10));
    assert!(values.contains(&20));
    assert!(values.contains(&30));

    client.shutdown().await.expect("Failed to shutdown");
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_replicated_map_entry_set() {
    let client = create_client().await;
    let map = client.get_replicated_map::<String, i32>("test-replicated-map-entry-set");

    map.clear().await.expect("Failed to clear map");

    map.put("x".to_string(), 100).await.expect("Failed to put");
    map.put("y".to_string(), 200).await.expect("Failed to put");

    let entries = map.entry_set().await.expect("Failed to get entry_set");
    assert_eq!(entries.len(), 2);

    let entries_map: std::collections::HashMap<_, _> = entries.into_iter().collect();
    assert_eq!(entries_map.get(&"x".to_string()), Some(&100));
    assert_eq!(entries_map.get(&"y".to_string()), Some(&200));

    client.shutdown().await.expect("Failed to shutdown");
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_replicated_map_get_nonexistent() {
    let client = create_client().await;
    let map = client.get_replicated_map::<String, String>("test-replicated-map-nonexistent");

    map.clear().await.expect("Failed to clear map");

    let value = map
        .get(&"nonexistent-key".to_string())
        .await
        .expect("Failed to get");
    assert!(value.is_none());

    client.shutdown().await.expect("Failed to shutdown");
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_replicated_map_with_numeric_keys() {
    let client = create_client().await;
    let map = client.get_replicated_map::<i64, String>("test-replicated-map-numeric");

    map.clear().await.expect("Failed to clear map");

    map.put(42, "forty-two".to_string())
        .await
        .expect("Failed to put");
    map.put(100, "hundred".to_string())
        .await
        .expect("Failed to put");

    let value = map.get(&42).await.expect("Failed to get");
    assert_eq!(value, Some("forty-two".to_string()));

    let contains = map.contains_key(&100).await.expect("Failed to check");
    assert!(contains);

    client.shutdown().await.expect("Failed to shutdown");
}
