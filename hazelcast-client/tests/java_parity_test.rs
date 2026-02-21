//! Integration tests validating IMap method parity with the Java Hazelcast driver.
//!
//! These tests cover:
//! - Basic CRUD operations with TTL and max-idle
//! - Bulk operations and predicates
//! - Async entry processors
//! - Local statistics tracking

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

use hazelcast_client::config::ClientConfig;
use hazelcast_client::proxy::IMap;
use hazelcast_client::query::{Aggregators, Predicates, Projections};
use hazelcast_client::HazelcastClient;

/// Test helper to create a client connected to a local cluster.
async fn create_test_client() -> HazelcastClient {
    let config = ClientConfig::builder()
        .cluster_name("dev")
        .add_address("127.0.0.1:5701".parse::<SocketAddr>().unwrap())
        .build()
        .expect("Failed to build config");
    HazelcastClient::new(config).await.expect("Failed to create client")
}

/// Test helper to get a uniquely named map for test isolation.
fn get_test_map(client: &HazelcastClient, test_name: &str) -> IMap<String, String> {
    client.get_map(&format!("java_parity_test_{}", test_name))
}

// ============================================================================
// Basic CRUD Operations
// ============================================================================

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_put_and_get() {
    let client = create_test_client().await;
    let map = get_test_map(&client, "put_and_get");

    let key = "test_key".to_string();
    let value = "test_value".to_string();

    let previous = map.put(key.clone(), value.clone()).await.unwrap();
    assert!(previous.is_none(), "Expected no previous value for new key");

    let retrieved = map.get(&key).await.unwrap();
    assert_eq!(retrieved, Some(value));

    map.clear().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_put_returns_previous_value() {
    let client = create_test_client().await;
    let map = get_test_map(&client, "put_returns_previous");

    let key = "key".to_string();
    let value1 = "value1".to_string();
    let value2 = "value2".to_string();

    map.put(key.clone(), value1.clone()).await.unwrap();
    let previous = map.put(key.clone(), value2.clone()).await.unwrap();

    assert_eq!(previous, Some(value1));

    map.clear().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_put_if_absent() {
    let client = create_test_client().await;
    let map = get_test_map(&client, "put_if_absent");

    let key = "key".to_string();
    let value1 = "value1".to_string();
    let value2 = "value2".to_string();

    let result1 = map.put_if_absent(key.clone(), value1.clone()).await.unwrap();
    assert!(result1.is_none(), "First put_if_absent should return None");

    let result2 = map.put_if_absent(key.clone(), value2.clone()).await.unwrap();
    assert_eq!(result2, Some(value1.clone()), "Second put_if_absent should return existing value");

    let retrieved = map.get(&key).await.unwrap();
    assert_eq!(retrieved, Some(value1), "Value should not have changed");

    map.clear().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_remove() {
    let client = create_test_client().await;
    let map = get_test_map(&client, "remove");

    let key = "key".to_string();
    let value = "value".to_string();

    map.put(key.clone(), value.clone()).await.unwrap();

    let removed = map.remove(&key).await.unwrap();
    assert_eq!(removed, Some(value));

    let after_remove = map.get(&key).await.unwrap();
    assert!(after_remove.is_none());

    map.clear().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_remove_if_value_matches() {
    let client = create_test_client().await;
    let map = get_test_map(&client, "remove_if_value_matches");

    let key = "key".to_string();
    let value = "value".to_string();
    let wrong_value = "wrong".to_string();

    map.put(key.clone(), value.clone()).await.unwrap();

    let not_removed = map.remove_if_equal(&key, &wrong_value).await.unwrap();
    assert!(!not_removed, "Should not remove when value doesn't match");

    let removed = map.remove_if_equal(&key, &value).await.unwrap();
    assert!(removed, "Should remove when value matches");

    map.clear().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_contains_key_and_value() {
    let client = create_test_client().await;
    let map = get_test_map(&client, "contains");

    let key = "key".to_string();
    let value = "value".to_string();

    assert!(!map.contains_key(&key).await.unwrap());

    map.put(key.clone(), value.clone()).await.unwrap();

    assert!(map.contains_key(&key).await.unwrap());
    assert!(!map.contains_key(&"nonexistent".to_string()).await.unwrap());

    map.clear().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_replace() {
    let client = create_test_client().await;
    let map = get_test_map(&client, "replace");

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

    map.clear().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_replace_if_equal() {
    let client = create_test_client().await;
    let map = get_test_map(&client, "replace_if_equal");

    let key = "key".to_string();
    let value1 = "value1".to_string();
    let value2 = "value2".to_string();
    let wrong = "wrong".to_string();

    map.put(key.clone(), value1.clone()).await.unwrap();

    let not_replaced = map.replace_if_equal(&key, &wrong, value2.clone()).await.unwrap();
    assert!(!not_replaced);

    let replaced = map.replace_if_equal(&key, &value1, value2.clone()).await.unwrap();
    assert!(replaced);

    assert_eq!(map.get(&key).await.unwrap(), Some(value2));

    map.clear().await.unwrap();
}

// ============================================================================
// TTL and Max-Idle Operations
// ============================================================================

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_put_with_ttl() {
    let client = create_test_client().await;
    let map = get_test_map(&client, "put_with_ttl");

    let key = "ttl_key".to_string();
    let value = "ttl_value".to_string();
    let ttl = Duration::from_secs(2);

    map.put_with_ttl_and_max_idle(key.clone(), value.clone(), ttl, Duration::from_secs(0)).await.unwrap();

    let retrieved = map.get(&key).await.unwrap();
    assert_eq!(retrieved, Some(value.clone()));

    tokio::time::sleep(Duration::from_secs(3)).await;

    let expired = map.get(&key).await.unwrap();
    assert!(expired.is_none(), "Entry should have expired after TTL");

    map.clear().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_put_with_ttl_and_max_idle() {
    let client = create_test_client().await;
    let map = get_test_map(&client, "put_ttl_max_idle");

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

    map.clear().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_set_with_ttl() {
    let client = create_test_client().await;
    let map = get_test_map(&client, "set_with_ttl");

    let key = "key".to_string();
    let value = "value".to_string();
    let ttl = Duration::from_secs(2);

    map.set_with_ttl_and_max_idle(key.clone(), value.clone(), ttl, Duration::from_secs(0)).await.unwrap();

    assert_eq!(map.get(&key).await.unwrap(), Some(value));

    tokio::time::sleep(Duration::from_secs(3)).await;

    assert!(map.get(&key).await.unwrap().is_none());

    map.clear().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_put_transient() {
    let client = create_test_client().await;
    let map = get_test_map(&client, "put_transient");

    let key = "transient_key".to_string();
    let value = "transient_value".to_string();
    let ttl = Duration::from_secs(2);

    map.put_transient(key.clone(), value.clone(), ttl).await.unwrap();

    assert_eq!(map.get(&key).await.unwrap(), Some(value));

    tokio::time::sleep(Duration::from_secs(3)).await;

    assert!(map.get(&key).await.unwrap().is_none());

    map.clear().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_set_ttl_on_existing_entry() {
    let client = create_test_client().await;
    let map = get_test_map(&client, "set_ttl_existing");

    let key = "key".to_string();
    let value = "value".to_string();

    map.put(key.clone(), value.clone()).await.unwrap();

    let updated = map.set_ttl(&key, Duration::from_secs(2)).await.unwrap();
    assert!(updated, "set_ttl should return true for existing entry");

    tokio::time::sleep(Duration::from_secs(3)).await;

    assert!(map.get(&key).await.unwrap().is_none());

    map.clear().await.unwrap();
}

// ============================================================================
// Bulk Operations
// ============================================================================

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_put_all() {
    let client = create_test_client().await;
    let map = get_test_map(&client, "put_all");

    let mut entries = HashMap::new();
    entries.insert("key1".to_string(), "value1".to_string());
    entries.insert("key2".to_string(), "value2".to_string());
    entries.insert("key3".to_string(), "value3".to_string());

    map.put_all(entries.clone()).await.unwrap();

    assert_eq!(map.size().await.unwrap(), 3);

    for (key, value) in entries {
        assert_eq!(map.get(&key).await.unwrap(), Some(value));
    }

    map.clear().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_get_all() {
    let client = create_test_client().await;
    let map = get_test_map(&client, "get_all");

    map.put("key1".to_string(), "value1".to_string()).await.unwrap();
    map.put("key2".to_string(), "value2".to_string()).await.unwrap();
    map.put("key3".to_string(), "value3".to_string()).await.unwrap();

    let keys = vec!["key1".to_string(), "key2".to_string(), "key4".to_string()];
    let results = map.get_all(&keys).await.unwrap();

    assert_eq!(results.len(), 2);
    assert_eq!(results.get("key1"), Some(&"value1".to_string()));
    assert_eq!(results.get("key2"), Some(&"value2".to_string()));
    assert!(results.get("key4").is_none());

    map.clear().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_clear() {
    let client = create_test_client().await;
    let map = get_test_map(&client, "clear");

    map.put("key1".to_string(), "value1".to_string()).await.unwrap();
    map.put("key2".to_string(), "value2".to_string()).await.unwrap();

    assert_eq!(map.size().await.unwrap(), 2);

    map.clear().await.unwrap();

    assert_eq!(map.size().await.unwrap(), 0);
    assert_eq!(map.size().await.unwrap(), 0);

    map.clear().await.unwrap();
}

// ============================================================================
// Predicate Operations
// ============================================================================

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_key_set_with_predicate() {
    let client = create_test_client().await;
    let map = get_test_map(&client, "key_set_predicate");

    map.put("user_1".to_string(), "active".to_string()).await.unwrap();
    map.put("user_2".to_string(), "inactive".to_string()).await.unwrap();
    map.put("user_3".to_string(), "active".to_string()).await.unwrap();

    let predicate = Predicates::equal("this", &"active".to_string()).unwrap();
    let keys = map.keys_with_predicate(&predicate).await.unwrap();

    assert_eq!(keys.len(), 2);
    assert!(keys.contains(&"user_1".to_string()));
    assert!(keys.contains(&"user_3".to_string()));

    map.clear().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_values_with_predicate() {
    let client = create_test_client().await;
    let map = get_test_map(&client, "values_predicate");

    map.put("a".to_string(), "10".to_string()).await.unwrap();
    map.put("b".to_string(), "20".to_string()).await.unwrap();
    map.put("c".to_string(), "30".to_string()).await.unwrap();

    let predicate = Predicates::sql("this > 15");
    let values = map.values_with_predicate(&predicate).await.unwrap();

    assert_eq!(values.len(), 2);
    assert!(values.contains(&"20".to_string()));
    assert!(values.contains(&"30".to_string()));

    map.clear().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_entry_set_with_predicate() {
    let client = create_test_client().await;
    let map = get_test_map(&client, "entry_set_predicate");

    map.put("item_a".to_string(), "100".to_string()).await.unwrap();
    map.put("item_b".to_string(), "200".to_string()).await.unwrap();
    map.put("item_c".to_string(), "50".to_string()).await.unwrap();

    let predicate = Predicates::sql("this >= 100");
    let entries = map.entries_with_predicate(&predicate).await.unwrap();

    assert_eq!(entries.len(), 2);

    let as_map: HashMap<String, String> = entries.into_iter().collect();
    assert!(as_map.contains_key("item_a"));
    assert!(as_map.contains_key("item_b"));

    map.clear().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_remove_all_with_predicate() {
    let client = create_test_client().await;
    let map = get_test_map(&client, "remove_all_predicate");

    map.put("keep_1".to_string(), "important".to_string()).await.unwrap();
    map.put("delete_1".to_string(), "temporary".to_string()).await.unwrap();
    map.put("keep_2".to_string(), "important".to_string()).await.unwrap();
    map.put("delete_2".to_string(), "temporary".to_string()).await.unwrap();

    assert_eq!(map.size().await.unwrap(), 4);

    let predicate = Predicates::equal("this", &"temporary".to_string()).unwrap();
    map.remove_all_with_predicate(&predicate).await.unwrap();

    assert_eq!(map.size().await.unwrap(), 2);
    assert!(map.contains_key(&"keep_1".to_string()).await.unwrap());
    assert!(map.contains_key(&"keep_2".to_string()).await.unwrap());
    assert!(!map.contains_key(&"delete_1".to_string()).await.unwrap());
    assert!(!map.contains_key(&"delete_2".to_string()).await.unwrap());

    map.clear().await.unwrap();
}

// ============================================================================
// Entry Processor Operations
// ============================================================================

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_execute_on_key() {
    let client = create_test_client().await;
    let map = get_test_map(&client, "execute_on_key");

    map.put("counter".to_string(), "0".to_string()).await.unwrap();

    let processor = IncrementProcessor::new(5);
    let result: Option<String> = map.execute_on_key(&"counter".to_string(), &processor).await.unwrap();

    assert_eq!(result, Some("5".to_string()));
    assert_eq!(map.get(&"counter".to_string()).await.unwrap(), Some("5".to_string()));

    map.clear().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_execute_on_keys() {
    let client = create_test_client().await;
    let map = get_test_map(&client, "execute_on_keys");

    map.put("a".to_string(), "1".to_string()).await.unwrap();
    map.put("b".to_string(), "2".to_string()).await.unwrap();
    map.put("c".to_string(), "3".to_string()).await.unwrap();

    let keys = vec!["a".to_string(), "b".to_string()];
    let processor = IncrementProcessor::new(10);
    let results = map.execute_on_keys(&keys, &processor).await.unwrap();

    assert_eq!(results.len(), 2);
    assert_eq!(results.get(&"a".to_string()), Some(&"11".to_string()));
    assert_eq!(results.get(&"b".to_string()), Some(&"12".to_string()));

    map.clear().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_execute_on_entries_with_predicate() {
    let client = create_test_client().await;
    let map = get_test_map(&client, "execute_on_entries_predicate");

    map.put("user_1".to_string(), "50".to_string()).await.unwrap();
    map.put("user_2".to_string(), "150".to_string()).await.unwrap();
    map.put("user_3".to_string(), "200".to_string()).await.unwrap();

    let predicate = Predicates::sql("this > 100");
    let processor = IncrementProcessor::new(10);
    let results = map
        .execute_on_entries_with_predicate(&processor, &predicate)
        .await
        .unwrap();

    assert_eq!(results.len(), 2);
    assert_eq!(results.get(&"user_2".to_string()), Some(&"160".to_string()));
    assert_eq!(results.get(&"user_3".to_string()), Some(&"210".to_string()));

    map.clear().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_submit_to_key_async() {
    let client = create_test_client().await;
    let map = get_test_map(&client, "submit_to_key");

    map.put("async_key".to_string(), "100".to_string()).await.unwrap();

    let processor = IncrementProcessor::new(50);
    let future = map.submit_to_key("async_key".to_string(), processor);

    let result: Option<String> = future.await.unwrap().unwrap();
    assert_eq!(result, Some("150".to_string()));

    map.clear().await.unwrap();
}

// ============================================================================
// Locking Operations
// ============================================================================

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_lock_and_unlock() {
    let client = create_test_client().await;
    let map = get_test_map(&client, "lock_unlock");

    let key = "locked_key".to_string();
    map.put(key.clone(), "initial".to_string()).await.unwrap();

    map.lock(&key).await.unwrap();
    assert!(map.is_locked(&key).await.unwrap());

    map.put(key.clone(), "modified".to_string()).await.unwrap();

    map.unlock(&key).await.unwrap();
    assert!(!map.is_locked(&key).await.unwrap());

    assert_eq!(map.get(&key).await.unwrap(), Some("modified".to_string()));

    map.clear().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_try_lock_with_timeout() {
    let client = create_test_client().await;
    let map = get_test_map(&client, "try_lock_timeout");

    let key = "contended_key".to_string();
    map.put(key.clone(), "value".to_string()).await.unwrap();

    map.lock(&key).await.unwrap();

    let acquired = map.try_lock(&key, Duration::from_millis(100)).await.unwrap();
    assert!(!acquired, "Should not acquire already-locked key");

    map.unlock(&key).await.unwrap();

    let acquired_after = map.try_lock(&key, Duration::from_millis(100)).await.unwrap();
    assert!(acquired_after, "Should acquire after unlock");

    map.unlock(&key).await.unwrap();
    map.clear().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_force_unlock() {
    let client = create_test_client().await;
    let map = get_test_map(&client, "force_unlock");

    let key = "force_key".to_string();
    map.put(key.clone(), "value".to_string()).await.unwrap();

    map.lock(&key).await.unwrap();
    assert!(map.is_locked(&key).await.unwrap());

    map.force_unlock(&key).await.unwrap();
    assert!(!map.is_locked(&key).await.unwrap());

    map.clear().await.unwrap();
}

// ============================================================================
// Async Operations
// ============================================================================

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_get_async() {
    let client = create_test_client().await;
    let map = get_test_map(&client, "get_async");

    map.put("async_get_key".to_string(), "async_value".to_string())
        .await
        .unwrap();

    let future = map.get_async("async_get_key".to_string());
    let result = future.await.unwrap().unwrap();

    assert_eq!(result, Some("async_value".to_string()));

    map.clear().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_put_async() {
    let client = create_test_client().await;
    let map = get_test_map(&client, "put_async");

    let future = map.put_async("async_put_key".to_string(), "async_put_value".to_string());
    let previous = future.await.unwrap().unwrap();

    assert!(previous.is_none());
    assert_eq!(
        map.get(&"async_put_key".to_string()).await.unwrap(),
        Some("async_put_value".to_string())
    );

    map.clear().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_remove_async() {
    let client = create_test_client().await;
    let map = get_test_map(&client, "remove_async");

    map.put("async_remove_key".to_string(), "to_be_removed".to_string())
        .await
        .unwrap();

    let future = map.remove_async("async_remove_key".to_string());
    let removed = future.await.unwrap().unwrap();

    assert_eq!(removed, Some("to_be_removed".to_string()));
    assert!(map.get(&"async_remove_key".to_string()).await.unwrap().is_none());

    map.clear().await.unwrap();
}

// ============================================================================
// Eviction and Loading
// ============================================================================

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_evict() {
    let client = create_test_client().await;
    let map = get_test_map(&client, "evict");

    map.put("evict_key".to_string(), "evict_value".to_string())
        .await
        .unwrap();

    let evicted = map.evict(&"evict_key".to_string()).await.unwrap();
    assert!(evicted);

    map.clear().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_evict_all() {
    let client = create_test_client().await;
    let map = get_test_map(&client, "evict_all");

    for i in 0..5 {
        map.put(format!("key_{}", i), format!("value_{}", i))
            .await
            .unwrap();
    }

    map.evict_all().await.unwrap();

    map.clear().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_load_all() {
    let client = create_test_client().await;
    let map = get_test_map(&client, "load_all");

    map.load_all_keys(true).await.unwrap();

    map.clear().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_load_all_keys() {
    let client = create_test_client().await;
    let map = get_test_map(&client, "load_all_keys");

    let keys = vec!["load_1".to_string(), "load_2".to_string()];
    map.load_all(&keys, true).await.unwrap();

    map.clear().await.unwrap();
}

// ============================================================================
// Aggregate Operations
// ============================================================================

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_aggregate() {
    let client = create_test_client().await;
    let map: IMap<String, i64> = client.get_map("java_parity_test_aggregate");

    map.put("a".to_string(), 10).await.unwrap();
    map.put("b".to_string(), 20).await.unwrap();
    map.put("c".to_string(), 30).await.unwrap();

    let sum: i64 = map.aggregate(&hazelcast_client::query::Aggregators::long_sum("this")).await.unwrap();
    assert_eq!(sum, 60);

    let avg: f64 = map.aggregate(&hazelcast_client::query::Aggregators::long_avg("this")).await.unwrap();
    assert!((avg - 20.0).abs() < 0.001);

    let count: i64 = map.aggregate(&hazelcast_client::query::Aggregators::count()).await.unwrap();
    assert_eq!(count, 3);

    map.clear().await.unwrap();
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_aggregate_with_predicate() {
    let client = create_test_client().await;
    let map: IMap<String, i64> = client.get_map("java_parity_test_aggregate_pred");

    map.put("a".to_string(), 10).await.unwrap();
    map.put("b".to_string(), 20).await.unwrap();
    map.put("c".to_string(), 30).await.unwrap();
    map.put("d".to_string(), 40).await.unwrap();

    let predicate = Predicates::sql("this > 15");
    let sum: i64 = map
        .aggregate_with_predicate(&hazelcast_client::query::Aggregators::long_sum("this"), &predicate)
        .await
        .unwrap();

    assert_eq!(sum, 90);

    map.clear().await.unwrap();
}

// ============================================================================
// Projection Operations
// ============================================================================

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_project() {
    let client = create_test_client().await;
    let map = get_test_map(&client, "project");

    map.put("user_1".to_string(), r#"{"name":"Alice","age":30}"#.to_string())
        .await
        .unwrap();
    map.put("user_2".to_string(), r#"{"name":"Bob","age":25}"#.to_string())
        .await
        .unwrap();

    let projection = Projections::single::<String>("name");
    let names: Vec<String> = map
        .project(&projection)
        .await
        .unwrap();

    assert_eq!(names.len(), 2);
    assert!(names.contains(&"Alice".to_string()));
    assert!(names.contains(&"Bob".to_string()));

    map.clear().await.unwrap();
}

// ============================================================================
// Entry View Operations
// ============================================================================

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_get_entry_view() {
    let client = create_test_client().await;
    let map = get_test_map(&client, "entry_view");

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

    map.clear().await.unwrap();
}

// ============================================================================
// Helper Types for Entry Processors
// ============================================================================

use hazelcast_client::proxy::EntryProcessor;
use hazelcast_core::serialization::{DataInput, DataOutput, Deserializable, ObjectDataOutput, Serializable};

struct IncrementProcessor {
    increment: i64,
}

impl IncrementProcessor {
    fn new(increment: i64) -> Self {
        Self { increment }
    }
}

impl EntryProcessor for IncrementProcessor {
    type Output = String;
}

impl Serializable for IncrementProcessor {
    fn serialize<W: DataOutput>(&self, output: &mut W) -> hazelcast_core::Result<()> {
        output.write_long(self.increment)?;
        Ok(())
    }
}

