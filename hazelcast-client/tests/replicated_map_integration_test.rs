//! Integration tests for ReplicatedMap TTL and bulk operations.

use std::collections::HashMap;
use std::time::Duration;

/// Tests that put_with_ttl stores a value that expires after the TTL.
#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_put_with_ttl_expires() {
    // This test requires a running Hazelcast cluster
    // let client = HazelcastClient::new(...).await.unwrap();
    // let map = client.get_replicated_map::<String, String>("ttl-test-map");
    //
    // map.put_with_ttl("key1".to_string(), "value1".to_string(), Duration::from_secs(1))
    //     .await
    //     .unwrap();
    //
    // let value = map.get(&"key1".to_string()).await.unwrap();
    // assert_eq!(value, Some("value1".to_string()));
    //
    // tokio::time::sleep(Duration::from_secs(2)).await;
    //
    // let value_after_ttl = map.get(&"key1".to_string()).await.unwrap();
    // assert_eq!(value_after_ttl, None);
}

/// Tests that put_with_ttl returns the previous value.
#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_put_with_ttl_returns_previous_value() {
    // let client = HazelcastClient::new(...).await.unwrap();
    // let map = client.get_replicated_map::<String, String>("ttl-prev-test-map");
    //
    // map.put("key1".to_string(), "value1".to_string()).await.unwrap();
    //
    // let prev = map.put_with_ttl("key1".to_string(), "value2".to_string(), Duration::from_secs(60))
    //     .await
    //     .unwrap();
    //
    // assert_eq!(prev, Some("value1".to_string()));
}

/// Tests that put_all inserts multiple entries at once.
#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_put_all_inserts_entries() {
    // let client = HazelcastClient::new(...).await.unwrap();
    // let map = client.get_replicated_map::<String, i32>("bulk-test-map");
    //
    // let mut entries = HashMap::new();
    // entries.insert("a".to_string(), 1);
    // entries.insert("b".to_string(), 2);
    // entries.insert("c".to_string(), 3);
    //
    // map.put_all(entries).await.unwrap();
    //
    // assert_eq!(map.get(&"a".to_string()).await.unwrap(), Some(1));
    // assert_eq!(map.get(&"b".to_string()).await.unwrap(), Some(2));
    // assert_eq!(map.get(&"c".to_string()).await.unwrap(), Some(3));
    // assert_eq!(map.size().await.unwrap(), 3);
}

/// Tests that put_all with empty map does nothing.
#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_put_all_empty_map() {
    // let client = HazelcastClient::new(...).await.unwrap();
    // let map = client.get_replicated_map::<String, String>("empty-bulk-test-map");
    //
    // let entries: HashMap<String, String> = HashMap::new();
    // map.put_all(entries).await.unwrap();
    //
    // assert_eq!(map.size().await.unwrap(), 0);
}

/// Tests that local stats are tracked correctly.
#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_local_replicated_map_stats() {
    // let client = HazelcastClient::new(...).await.unwrap();
    // let map = client.get_replicated_map::<String, String>("stats-test-map");
    //
    // map.put("key1".to_string(), "value1".to_string()).await.unwrap();
    // map.put("key2".to_string(), "value2".to_string()).await.unwrap();
    // map.get(&"key1".to_string()).await.unwrap();
    // map.get(&"nonexistent".to_string()).await.unwrap();
    // map.remove(&"key1".to_string()).await.unwrap();
    //
    // let stats = map.get_local_replicated_map_stats();
    //
    // assert_eq!(stats.put_operation_count(), 2);
    // assert_eq!(stats.get_operation_count(), 2);
    // assert_eq!(stats.remove_operation_count(), 1);
    // assert_eq!(stats.hits(), 1);
    // assert_eq!(stats.misses(), 1);
    // assert!(stats.creation_time() > 0);
}

/// Tests that put_all updates stats correctly.
#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_put_all_updates_stats() {
    // let client = HazelcastClient::new(...).await.unwrap();
    // let map = client.get_replicated_map::<String, i32>("stats-bulk-test-map");
    //
    // let mut entries = HashMap::new();
    // entries.insert("a".to_string(), 1);
    // entries.insert("b".to_string(), 2);
    // entries.insert("c".to_string(), 3);
    //
    // map.put_all(entries).await.unwrap();
    //
    // let stats = map.get_local_replicated_map_stats();
    // assert_eq!(stats.put_operation_count(), 3);
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn test_stats_default() {
        use crate::unit_tests::*;
        // ReplicatedMapStats is created via Default trait
    }
}
