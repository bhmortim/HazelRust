//! Integration tests validating IMap method parity with the Java Hazelcast driver.
//!
//! These tests cover:
//! - Basic CRUD operations with TTL and max-idle
//! - Bulk operations and predicates
//! - Async entry processors
//! - Local statistics tracking

use std::collections::HashMap;
use std::time::Duration;

use hazelcast_client::config::ClientConfig;
use hazelcast_client::proxy::IMap;
use hazelcast_client::query::Predicate;
use hazelcast_client::HazelcastClient;

/// Test helper to create a client connected to a local cluster.
async fn create_test_client() -> HazelcastClient {
    let mut config = ClientConfig::default();
    config.network.addresses.push("127.0.0.1:5701".to_string());
    HazelcastClient::new(config).await.expect("Failed to create client")
}

/// Test helper to get a uniquely named map for test isolation.
async fn get_test_map(client: &HazelcastClient, test_name: &str) -> IMap<String, String> {
    client
        .get_map(&format!("java_parity_test_{}", test_name))
        .await
        .expect("Failed to get map")
}

// ============================================================================
// Basic CRUD Operations
// ============================================================================

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_put_and_get() {
    let client = create_test_client().await;
    let map = get_test_map(&client, "put_and_get").await;

    let key = "test_key".to_string();
    let value = "test_value".to_string();

    let previous = map.put(key.clone(), value.clone()).await.unwrap();
    assert!(previous.is_none(), "Expected no previous value for new key");

    let retrieved = map.get(&key).await.unwrap();
    assert_eq!(retrieved, Some(value));

    map.destroy().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_put_returns_previous_value() {
    let client = create_test_client().await;
    let map = get_test_map(&client, "put_returns_previous").await;

    let key = "key".to_string();
    let value1 = "value1".to_string();
    let value2 = "value2".to_string();

    map.put(key.clone(), value1.clone()).await.unwrap();
    let previous = map.put(key.clone(), value2.clone()).await.unwrap();

    assert_eq!(previous, Some(value1));

    map.destroy().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_put_if_absent() {
    let client = create_test_client().await;
    let map = get_test_map(&client, "put_if_absent").await;

    let key = "key".to_string();
    let value1 = "value1".to_string();
    let value2 = "value2".to_string();

    let result1 = map.put_if_absent(key.clone(), value1.clone()).await.unwrap();
    assert!(result1.is_none(), "First put_if_absent should return None");

    let result2 = map.put_if_absent(key.clone(), value2.clone()).await.unwrap();
    assert_eq!(result2, Some(value1.clone()), "Second put_if_absent should return existing value");

    let retrieved = map.get(&key).await.unwrap();
    assert_eq!(retrieved, Some(value1), "Value should not have changed");

    map.destroy().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_remove() {
    let client = create_test_client().await;
    let map = get_test_map(&client, "remove").await;

    let key = "key".to_string();
    let value = "value".to_string();

    map.put(key.clone(), value.clone()).await.unwrap();

    let removed = map.remove(&key).await.unwrap();
    assert_eq!(removed, Some(value));

    let after_remove = map.get(&key).await.unwrap();
    assert!(after_remove.is_none());

    map.destroy().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_remove_if_value_matches() {
    let client = create_test_client().await;
    let map = get_test_map(&client, "remove_if_value_matches").await;

    let key = "key".to_string();
    let value = "value".to_string();
    let wrong_value = "wrong".to_string();

    map.put(key.clone(), value.clone()).await.unwrap();

    let not_removed = map.remove_if_equals(&key, &wrong_value).await.unwrap();
    assert!(!not_removed, "Should not remove when value doesn't match");

    let removed = map.remove_if_equals(&key, &value).await.unwrap();
    assert!(removed, "Should remove when value matches");

    map.destroy().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_contains_key_and_value() {
    let client = create_test_client().await;
    let map = get_test_map(&client, "contains").await;

    let key = "key".to_string();
    let value = "value".to_string();

    assert!(!map.contains_key(&key).await.unwrap());
    assert!(!map.contains_value(&value).await.unwrap());

    map.put(key.clone(), value.clone()).await.unwrap();

    assert!(map.contains_key(&key).await.unwrap());
    assert!(map.contains_value(&value).await.unwrap());
    assert!(!map.contains_key(&"nonexistent".to_string()).await.unwrap());
    assert!(!map.contains_value(&"nonexistent".to_string()).await.unwrap());

    map.destroy().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_replace() {
    let client = create_test_client().await;
    let map = get_test_map(&client, "replace").await;

    let key = "key".to_string();
    let value1 = "value1".to_string();
    let value2 = "value2".to_string();

    let no_replace = map.replace(&key, value2.clone()).await.unwrap();
    assert!(no_replace.is_none(), "Replace on missing key should return None");

    map.put(key.clone(), value1.clone()).await.unwrap();

    let replaced = map.replace(&key, value2.clone()).await.unwrap();
    assert_eq!(replaced, Some(value1));

    let current = map.get(&key).await.unwrap();
    assert_eq!(current, Some(value2));

    map.destroy().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_replace_if_equals() {
    let client = create_test_client().await;
    let map = get_test_map(&client, "replace_if_equals").await;

    let key = "key".to_string();
    let value1 = "value1".to_string();
    let value2 = "value2".to_string();
    let wrong = "wrong".to_string();

    map.put(key.clone(), value1.clone()).await.unwrap();

    let not_replaced = map.replace_if_equals(&key, &wrong, value2.clone()).await.unwrap();
    assert!(!not_replaced);

    let replaced = map.replace_if_equals(&key, &value1, value2.clone()).await.unwrap();
    assert!(replaced);

    assert_eq!(map.get(&key).await.unwrap(), Some(value2));

    map.destroy().await.unwrap();
}

// ============================================================================
// TTL and Max-Idle Operations
// ============================================================================

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_put_with_ttl() {
    let client = create_test_client().await;
    let map = get_test_map(&client, "put_with_ttl").await;

    let key = "ttl_key".to_string();
    let value = "ttl_value".to_string();
    let ttl = Duration::from_secs(2);

    map.put_with_ttl(key.clone(), value.clone(), ttl).await.unwrap();

    let retrieved = map.get(&key).await.unwrap();
    assert_eq!(retrieved, Some(value.clone()));

    tokio::time::sleep(Duration::from_secs(3)).await;

    let expired = map.get(&key).await.unwrap();
    assert!(expired.is_none(), "Entry should have expired after TTL");

    map.destroy().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_put_with_ttl_and_max_idle() {
    let client = create_test_client().await;
    let map = get_test_map(&client, "put_ttl_max_idle").await;

    let key = "key".to_string();
    let value = "value".to_string();
    let ttl = Duration::from_secs(10);
    let max_idle = Duration::from_secs(2);

    map.put_with_ttl_and_max_idle(key.clone(), value.clone(), ttl, max_idle)
        .await
        .unwrap();

    let retrieved = map.get(&key).await.unwrap();
    assert_eq!(retrieved, Some(value.clone()));

    tokio::time::sleep(Duration::from_secs(3)).await;

    let expired = map.get(&key).await.unwrap();
    assert!(expired.is_none(), "Entry should have expired due to max-idle");

    map.destroy().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_set_with_ttl() {
    let client = create_test_client().await;
    let map = get_test_map(&client, "set_with_ttl").await;

    let key = "key".to_string();
    let value = "value".to_string();
    let ttl = Duration::from_secs(2);

    map.set_with_ttl(key.clone(), value.clone(), ttl).await.unwrap();

    assert_eq!(map.get(&key).await.unwrap(), Some(value));

    tokio::time::sleep(Duration::from_secs(3)).await;

    assert!(map.get(&key).await.unwrap().is_none());

    map.destroy().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_put_transient() {
    let client = create_test_client().await;
    let map = get_test_map(&client, "put_transient").await;

    let key = "transient_key".to_string();
    let value = "transient_value".to_string();
    let ttl = Duration::from_secs(2);

    map.put_transient(key.clone(), value.clone(), ttl).await.unwrap();

    assert_eq!(map.get(&key).await.unwrap(), Some(value));

    tokio::time::sleep(Duration::from_secs(3)).await;

    assert!(map.get(&key).await.unwrap().is_none());

    map.destroy().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_set_ttl_on_existing_entry() {
    let client = create_test_client().await;
    let map = get_test_map(&client, "set_ttl_existing").await;

    let key = "key".to_string();
    let value = "value".to_string();

    map.put(key.clone(), value.clone()).await.unwrap();

    let updated = map.set_ttl(&key, Duration::from_secs(2)).await.unwrap();
    assert!(updated, "set_ttl should return true for existing entry");

    tokio::time::sleep(Duration::from_secs(3)).await;

    assert!(map.get(&key).await.unwrap().is_none());

    map.destroy().await.unwrap();
}

// ============================================================================
// Bulk Operations
// ============================================================================

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_put_all() {
    let client = create_test_client().await;
    let map = get_test_map(&client, "put_all").await;

    let mut entries = HashMap::new();
    entries.insert("key1".to_string(), "value1".to_string());
    entries.insert("key2".to_string(), "value2".to_string());
    entries.insert("key3".to_string(), "value3".to_string());

    map.put_all(entries.clone()).await.unwrap();

    assert_eq!(map.size().await.unwrap(), 3);

    for (key, value) in entries {
        assert_eq!(map.get(&key).await.unwrap(), Some(value));
    }

    map.destroy().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_get_all() {
    let client = create_test_client().await;
    let map = get_test_map(&client, "get_all").await;

    map.put("key1".to_string(), "value1".to_string()).await.unwrap();
    map.put("key2".to_string(), "value2".to_string()).await.unwrap();
    map.put("key3".to_string(), "value3".to_string()).await.unwrap();

    let keys = vec!["key1".to_string(), "key2".to_string(), "key4".to_string()];
    let results = map.get_all(keys).await.unwrap();

    assert_eq!(results.len(), 2);
    assert_eq!(results.get("key1"), Some(&"value1".to_string()));
    assert_eq!(results.get("key2"), Some(&"value2".to_string()));
    assert!(results.get("key4").is_none());

    map.destroy().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_clear() {
    let client = create_test_client().await;
    let map = get_test_map(&client, "clear").await;

    map.put("key1".to_string(), "value1".to_string()).await.unwrap();
    map.put("key2".to_string(), "value2".to_string()).await.unwrap();

    assert_eq!(map.size().await.unwrap(), 2);

    map.clear().await.unwrap();

    assert_eq!(map.size().await.unwrap(), 0);
    assert!(map.is_empty().await.unwrap());

    map.destroy().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_key_set() {
    let client = create_test_client().await;
    let map = get_test_map(&client, "key_set").await;

    map.put("a".to_string(), "1".to_string()).await.unwrap();
    map.put("b".to_string(), "2".to_string()).await.unwrap();
    map.put("c".to_string(), "3".to_string()).await.unwrap();

    let keys = map.key_set().await.unwrap();

    assert_eq!(keys.len(), 3);
    assert!(keys.contains(&"a".to_string()));
    assert!(keys.contains(&"b".to_string()));
    assert!(keys.contains(&"c".to_string()));

    map.destroy().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_values() {
    let client = create_test_client().await;
    let map = get_test_map(&client, "values").await;

    map.put("a".to_string(), "1".to_string()).await.unwrap();
    map.put("b".to_string(), "2".to_string()).await.unwrap();
    map.put("c".to_string(), "3".to_string()).await.unwrap();

    let values = map.values().await.unwrap();

    assert_eq!(values.len(), 3);
    assert!(values.contains(&"1".to_string()));
    assert!(values.contains(&"2".to_string()));
    assert!(values.contains(&"3".to_string()));

    map.destroy().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_entry_set() {
    let client = create_test_client().await;
    let map = get_test_map(&client, "entry_set").await;

    map.put("a".to_string(), "1".to_string()).await.unwrap();
    map.put("b".to_string(), "2".to_string()).await.unwrap();

    let entries = map.entry_set().await.unwrap();

    assert_eq!(entries.len(), 2);

    let as_map: HashMap<String, String> = entries.into_iter().collect();
    assert_eq!(as_map.get("a"), Some(&"1".to_string()));
    assert_eq!(as_map.get("b"), Some(&"2".to_string()));

    map.destroy().await.unwrap();
}

// ============================================================================
// Predicate Operations
// ============================================================================

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_key_set_with_predicate() {
    let client = create_test_client().await;
    let map = get_test_map(&client, "key_set_predicate").await;

    map.put("user_1".to_string(), "active".to_string()).await.unwrap();
    map.put("user_2".to_string(), "inactive".to_string()).await.unwrap();
    map.put("user_3".to_string(), "active".to_string()).await.unwrap();

    let predicate = Predicate::equal("this", "active");
    let keys = map.key_set_with_predicate(predicate).await.unwrap();

    assert_eq!(keys.len(), 2);
    assert!(keys.contains(&"user_1".to_string()));
    assert!(keys.contains(&"user_3".to_string()));

    map.destroy().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_values_with_predicate() {
    let client = create_test_client().await;
    let map = get_test_map(&client, "values_predicate").await;

    map.put("a".to_string(), "10".to_string()).await.unwrap();
    map.put("b".to_string(), "20".to_string()).await.unwrap();
    map.put("c".to_string(), "30".to_string()).await.unwrap();

    let predicate = Predicate::sql("this > 15");
    let values = map.values_with_predicate(predicate).await.unwrap();

    assert_eq!(values.len(), 2);
    assert!(values.contains(&"20".to_string()));
    assert!(values.contains(&"30".to_string()));

    map.destroy().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_entry_set_with_predicate() {
    let client = create_test_client().await;
    let map = get_test_map(&client, "entry_set_predicate").await;

    map.put("item_a".to_string(), "100".to_string()).await.unwrap();
    map.put("item_b".to_string(), "200".to_string()).await.unwrap();
    map.put("item_c".to_string(), "50".to_string()).await.unwrap();

    let predicate = Predicate::sql("this >= 100");
    let entries = map.entry_set_with_predicate(predicate).await.unwrap();

    assert_eq!(entries.len(), 2);

    let as_map: HashMap<String, String> = entries.into_iter().collect();
    assert!(as_map.contains_key("item_a"));
    assert!(as_map.contains_key("item_b"));

    map.destroy().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_remove_all_with_predicate() {
    let client = create_test_client().await;
    let map = get_test_map(&client, "remove_all_predicate").await;

    map.put("keep_1".to_string(), "important".to_string()).await.unwrap();
    map.put("delete_1".to_string(), "temporary".to_string()).await.unwrap();
    map.put("keep_2".to_string(), "important".to_string()).await.unwrap();
    map.put("delete_2".to_string(), "temporary".to_string()).await.unwrap();

    assert_eq!(map.size().await.unwrap(), 4);

    let predicate = Predicate::equal("this", "temporary");
    map.remove_all(predicate).await.unwrap();

    assert_eq!(map.size().await.unwrap(), 2);
    assert!(map.contains_key(&"keep_1".to_string()).await.unwrap());
    assert!(map.contains_key(&"keep_2".to_string()).await.unwrap());
    assert!(!map.contains_key(&"delete_1".to_string()).await.unwrap());
    assert!(!map.contains_key(&"delete_2".to_string()).await.unwrap());

    map.destroy().await.unwrap();
}

// ============================================================================
// Entry Processor Operations
// ============================================================================

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_execute_on_key() {
    let client = create_test_client().await;
    let map = get_test_map(&client, "execute_on_key").await;

    map.put("counter".to_string(), "0".to_string()).await.unwrap();

    let processor = IncrementProcessor::new(5);
    let result: Option<String> = map.execute_on_key("counter".to_string(), processor).await.unwrap();

    assert_eq!(result, Some("5".to_string()));
    assert_eq!(map.get(&"counter".to_string()).await.unwrap(), Some("5".to_string()));

    map.destroy().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_execute_on_keys() {
    let client = create_test_client().await;
    let map = get_test_map(&client, "execute_on_keys").await;

    map.put("a".to_string(), "1".to_string()).await.unwrap();
    map.put("b".to_string(), "2".to_string()).await.unwrap();
    map.put("c".to_string(), "3".to_string()).await.unwrap();

    let keys = vec!["a".to_string(), "b".to_string()];
    let processor = IncrementProcessor::new(10);
    let results: HashMap<String, String> = map.execute_on_keys(keys, processor).await.unwrap();

    assert_eq!(results.len(), 2);
    assert_eq!(results.get("a"), Some(&"11".to_string()));
    assert_eq!(results.get("b"), Some(&"12".to_string()));

    map.destroy().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_execute_on_entries_with_predicate() {
    let client = create_test_client().await;
    let map = get_test_map(&client, "execute_on_entries_predicate").await;

    map.put("user_1".to_string(), "50".to_string()).await.unwrap();
    map.put("user_2".to_string(), "150".to_string()).await.unwrap();
    map.put("user_3".to_string(), "200".to_string()).await.unwrap();

    let predicate = Predicate::sql("this > 100");
    let processor = IncrementProcessor::new(10);
    let results: HashMap<String, String> = map
        .execute_on_entries_with_predicate(processor, predicate)
        .await
        .unwrap();

    assert_eq!(results.len(), 2);
    assert_eq!(results.get("user_2"), Some(&"160".to_string()));
    assert_eq!(results.get("user_3"), Some(&"210".to_string()));

    map.destroy().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_submit_to_key_async() {
    let client = create_test_client().await;
    let map = get_test_map(&client, "submit_to_key").await;

    map.put("async_key".to_string(), "100".to_string()).await.unwrap();

    let processor = IncrementProcessor::new(50);
    let future = map.submit_to_key("async_key".to_string(), processor);

    let result: Option<String> = future.await.unwrap();
    assert_eq!(result, Some("150".to_string()));

    map.destroy().await.unwrap();
}

// ============================================================================
// Local Statistics
// ============================================================================

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_local_map_stats_basic() {
    let client = create_test_client().await;
    let map = get_test_map(&client, "local_stats_basic").await;

    map.put("key1".to_string(), "value1".to_string()).await.unwrap();
    map.put("key2".to_string(), "value2".to_string()).await.unwrap();
    map.get(&"key1".to_string()).await.unwrap();
    map.get(&"key1".to_string()).await.unwrap();
    map.get(&"nonexistent".to_string()).await.unwrap();

    let stats = map.local_map_stats();

    assert!(stats.put_count() >= 2);
    assert!(stats.get_count() >= 3);
    assert!(stats.hit_count() >= 2);
    assert!(stats.miss_count() >= 1);

    map.destroy().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_local_map_stats_timing() {
    let client = create_test_client().await;
    let map = get_test_map(&client, "local_stats_timing").await;

    for i in 0..10 {
        map.put(format!("key_{}", i), format!("value_{}", i)).await.unwrap();
    }

    for i in 0..10 {
        map.get(&format!("key_{}", i)).await.unwrap();
    }

    let stats = map.local_map_stats();

    assert!(stats.total_put_latency() > Duration::ZERO);
    assert!(stats.total_get_latency() > Duration::ZERO);
    assert!(stats.max_put_latency() > Duration::ZERO);
    assert!(stats.max_get_latency() > Duration::ZERO);

    map.destroy().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_local_map_stats_remove_operations() {
    let client = create_test_client().await;
    let map = get_test_map(&client, "local_stats_remove").await;

    map.put("key1".to_string(), "value1".to_string()).await.unwrap();
    map.put("key2".to_string(), "value2".to_string()).await.unwrap();

    map.remove(&"key1".to_string()).await.unwrap();
    map.remove(&"nonexistent".to_string()).await.unwrap();

    let stats = map.local_map_stats();

    assert!(stats.remove_count() >= 1);

    map.destroy().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_local_map_stats_near_cache() {
    let mut config = ClientConfig::default();
    config.network.addresses.push("127.0.0.1:5701".to_string());
    config.near_cache.enabled = true;
    config.near_cache.max_size = 1000;

    let client = HazelcastClient::new(config).await.expect("Failed to create client");
    let map: IMap<String, String> = client
        .get_map("java_parity_test_near_cache_stats")
        .await
        .expect("Failed to get map");

    map.put("nc_key".to_string(), "nc_value".to_string()).await.unwrap();

    map.get(&"nc_key".to_string()).await.unwrap();
    map.get(&"nc_key".to_string()).await.unwrap();
    map.get(&"nc_key".to_string()).await.unwrap();

    let stats = map.local_map_stats();
    let nc_stats = stats.near_cache_stats();

    assert!(nc_stats.is_some());
    if let Some(nc) = nc_stats {
        assert!(nc.hits() >= 2);
        assert!(nc.owned_entry_count() >= 1);
    }

    map.destroy().await.unwrap();
}

// ============================================================================
// Locking Operations
// ============================================================================

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_lock_and_unlock() {
    let client = create_test_client().await;
    let map = get_test_map(&client, "lock_unlock").await;

    let key = "locked_key".to_string();
    map.put(key.clone(), "initial".to_string()).await.unwrap();

    map.lock(&key).await.unwrap();
    assert!(map.is_locked(&key).await.unwrap());

    map.put(key.clone(), "modified".to_string()).await.unwrap();

    map.unlock(&key).await.unwrap();
    assert!(!map.is_locked(&key).await.unwrap());

    assert_eq!(map.get(&key).await.unwrap(), Some("modified".to_string()));

    map.destroy().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_try_lock_with_timeout() {
    let client = create_test_client().await;
    let map = get_test_map(&client, "try_lock_timeout").await;

    let key = "contended_key".to_string();
    map.put(key.clone(), "value".to_string()).await.unwrap();

    map.lock(&key).await.unwrap();

    let acquired = map.try_lock(&key, Duration::from_millis(100)).await.unwrap();
    assert!(!acquired, "Should not acquire already-locked key");

    map.unlock(&key).await.unwrap();

    let acquired_after = map.try_lock(&key, Duration::from_millis(100)).await.unwrap();
    assert!(acquired_after, "Should acquire after unlock");

    map.unlock(&key).await.unwrap();
    map.destroy().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_force_unlock() {
    let client = create_test_client().await;
    let map = get_test_map(&client, "force_unlock").await;

    let key = "force_key".to_string();
    map.put(key.clone(), "value".to_string()).await.unwrap();

    map.lock(&key).await.unwrap();
    assert!(map.is_locked(&key).await.unwrap());

    map.force_unlock(&key).await.unwrap();
    assert!(!map.is_locked(&key).await.unwrap());

    map.destroy().await.unwrap();
}

// ============================================================================
// Async Operations
// ============================================================================

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_get_async() {
    let client = create_test_client().await;
    let map = get_test_map(&client, "get_async").await;

    map.put("async_get_key".to_string(), "async_value".to_string())
        .await
        .unwrap();

    let future = map.get_async("async_get_key".to_string());
    let result = future.await.unwrap();

    assert_eq!(result, Some("async_value".to_string()));

    map.destroy().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_put_async() {
    let client = create_test_client().await;
    let map = get_test_map(&client, "put_async").await;

    let future = map.put_async("async_put_key".to_string(), "async_put_value".to_string());
    let previous = future.await.unwrap();

    assert!(previous.is_none());
    assert_eq!(
        map.get(&"async_put_key".to_string()).await.unwrap(),
        Some("async_put_value".to_string())
    );

    map.destroy().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_remove_async() {
    let client = create_test_client().await;
    let map = get_test_map(&client, "remove_async").await;

    map.put("async_remove_key".to_string(), "to_be_removed".to_string())
        .await
        .unwrap();

    let future = map.remove_async("async_remove_key".to_string());
    let removed = future.await.unwrap();

    assert_eq!(removed, Some("to_be_removed".to_string()));
    assert!(map.get(&"async_remove_key".to_string()).await.unwrap().is_none());

    map.destroy().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_set_async() {
    let client = create_test_client().await;
    let map = get_test_map(&client, "set_async").await;

    let future = map.set_async("set_async_key".to_string(), "set_value".to_string());
    future.await.unwrap();

    assert_eq!(
        map.get(&"set_async_key".to_string()).await.unwrap(),
        Some("set_value".to_string())
    );

    map.destroy().await.unwrap();
}

// ============================================================================
// Eviction and Loading
// ============================================================================

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_evict() {
    let client = create_test_client().await;
    let map = get_test_map(&client, "evict").await;

    map.put("evict_key".to_string(), "evict_value".to_string())
        .await
        .unwrap();

    let evicted = map.evict(&"evict_key".to_string()).await.unwrap();
    assert!(evicted);

    map.destroy().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_evict_all() {
    let client = create_test_client().await;
    let map = get_test_map(&client, "evict_all").await;

    for i in 0..5 {
        map.put(format!("key_{}", i), format!("value_{}", i))
            .await
            .unwrap();
    }

    map.evict_all().await.unwrap();

    map.destroy().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_load_all() {
    let client = create_test_client().await;
    let map = get_test_map(&client, "load_all").await;

    map.load_all(true).await.unwrap();

    map.destroy().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_load_all_keys() {
    let client = create_test_client().await;
    let map = get_test_map(&client, "load_all_keys").await;

    let keys = vec!["load_1".to_string(), "load_2".to_string()];
    map.load_all_keys(keys, true).await.unwrap();

    map.destroy().await.unwrap();
}

// ============================================================================
// Aggregate Operations
// ============================================================================

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_aggregate() {
    let client = create_test_client().await;
    let map: IMap<String, i64> = client
        .get_map("java_parity_test_aggregate")
        .await
        .expect("Failed to get map");

    map.put("a".to_string(), 10).await.unwrap();
    map.put("b".to_string(), 20).await.unwrap();
    map.put("c".to_string(), 30).await.unwrap();

    let sum: i64 = map.aggregate(Aggregators::long_sum("this")).await.unwrap();
    assert_eq!(sum, 60);

    let avg: f64 = map.aggregate(Aggregators::long_avg("this")).await.unwrap();
    assert!((avg - 20.0).abs() < 0.001);

    let count: i64 = map.aggregate(Aggregators::count("this")).await.unwrap();
    assert_eq!(count, 3);

    map.destroy().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_aggregate_with_predicate() {
    let client = create_test_client().await;
    let map: IMap<String, i64> = client
        .get_map("java_parity_test_aggregate_pred")
        .await
        .expect("Failed to get map");

    map.put("a".to_string(), 10).await.unwrap();
    map.put("b".to_string(), 20).await.unwrap();
    map.put("c".to_string(), 30).await.unwrap();
    map.put("d".to_string(), 40).await.unwrap();

    let predicate = Predicate::sql("this > 15");
    let sum: i64 = map
        .aggregate_with_predicate(Aggregators::long_sum("this"), predicate)
        .await
        .unwrap();

    assert_eq!(sum, 90);

    map.destroy().await.unwrap();
}

// ============================================================================
// Projection Operations
// ============================================================================

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_project() {
    let client = create_test_client().await;
    let map = get_test_map(&client, "project").await;

    map.put("user_1".to_string(), r#"{"name":"Alice","age":30}"#.to_string())
        .await
        .unwrap();
    map.put("user_2".to_string(), r#"{"name":"Bob","age":25}"#.to_string())
        .await
        .unwrap();

    let names: Vec<String> = map
        .project(Projections::single_attribute("name"))
        .await
        .unwrap();

    assert_eq!(names.len(), 2);
    assert!(names.contains(&"Alice".to_string()));
    assert!(names.contains(&"Bob".to_string()));

    map.destroy().await.unwrap();
}

// ============================================================================
// Entry View Operations
// ============================================================================

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_get_entry_view() {
    let client = create_test_client().await;
    let map = get_test_map(&client, "entry_view").await;

    let key = "view_key".to_string();
    let value = "view_value".to_string();

    map.put(key.clone(), value.clone()).await.unwrap();
    map.get(&key).await.unwrap();
    map.get(&key).await.unwrap();

    let view = map.get_entry_view(&key).await.unwrap();

    assert!(view.is_some());
    let entry_view = view.unwrap();

    assert_eq!(entry_view.key(), &key);
    assert_eq!(entry_view.value(), &value);
    assert!(entry_view.hits() >= 2);
    assert!(entry_view.creation_time() > 0);
    assert!(entry_view.last_access_time() > 0);

    map.destroy().await.unwrap();
}

// ============================================================================
// Helper Types for Entry Processors
// ============================================================================

use hazelcast_client::proxy::EntryProcessor;

struct IncrementProcessor {
    increment: i64,
}

impl IncrementProcessor {
    fn new(increment: i64) -> Self {
        Self { increment }
    }
}

impl EntryProcessor for IncrementProcessor {
    fn class_id() -> i32 {
        1
    }

    fn factory_id() -> i32 {
        1
    }
}

struct Aggregators;

impl Aggregators {
    fn long_sum(_attribute: &str) -> LongSumAggregator {
        LongSumAggregator
    }

    fn long_avg(_attribute: &str) -> LongAvgAggregator {
        LongAvgAggregator
    }

    fn count(_attribute: &str) -> CountAggregator {
        CountAggregator
    }
}

struct LongSumAggregator;
struct LongAvgAggregator;
struct CountAggregator;

struct Projections;

impl Projections {
    fn single_attribute(_attribute: &str) -> SingleAttributeProjection {
        SingleAttributeProjection
    }
}

struct SingleAttributeProjection;
