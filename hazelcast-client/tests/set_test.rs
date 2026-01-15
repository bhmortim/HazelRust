//! Integration tests for ISet distributed set.
//!
//! These tests require a running Hazelcast cluster.
//! Start one with: docker run -p 5701:5701 hazelcast/hazelcast:5.3

use std::time::Duration;

use hazelcast_client::{ClientConfigBuilder, HazelcastClient};

async fn create_test_client() -> HazelcastClient {
    let config = ClientConfigBuilder::new()
        .cluster_name("dev")
        .add_address("127.0.0.1:5701".parse().unwrap())
        .connection_timeout(Duration::from_secs(5))
        .build()
        .unwrap();

    HazelcastClient::new(config).await.unwrap()
}

#[tokio::test]
#[ignore] // Requires running Hazelcast cluster
async fn test_set_add_and_contains() {
    let client = create_test_client().await;
    let set = client.get_set::<String>("test-set-add-contains");

    // Clear any existing data
    set.clear().await.unwrap();

    // Add should return true for new element
    let added = set.add("item1".to_string()).await.unwrap();
    assert!(added, "add should return true for new element");

    // Add same element should return false
    let added_again = set.add("item1".to_string()).await.unwrap();
    assert!(!added_again, "add should return false for existing element");

    // Contains should return true
    let contains = set.contains(&"item1".to_string()).await.unwrap();
    assert!(contains, "set should contain the added item");

    // Contains should return false for non-existent item
    let contains_missing = set.contains(&"item2".to_string()).await.unwrap();
    assert!(!contains_missing, "set should not contain non-added item");

    set.clear().await.unwrap();
    client.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore] // Requires running Hazelcast cluster
async fn test_set_remove() {
    let client = create_test_client().await;
    let set = client.get_set::<String>("test-set-remove");

    set.clear().await.unwrap();

    // Add an item
    set.add("to-remove".to_string()).await.unwrap();

    // Remove should return true for existing element
    let removed = set.remove(&"to-remove".to_string()).await.unwrap();
    assert!(removed, "remove should return true for existing element");

    // Remove again should return false
    let removed_again = set.remove(&"to-remove".to_string()).await.unwrap();
    assert!(!removed_again, "remove should return false for non-existing element");

    // Contains should return false after removal
    let contains = set.contains(&"to-remove".to_string()).await.unwrap();
    assert!(!contains, "set should not contain removed item");

    client.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore] // Requires running Hazelcast cluster
async fn test_set_size_and_is_empty() {
    let client = create_test_client().await;
    let set = client.get_set::<String>("test-set-size");

    set.clear().await.unwrap();

    // Empty set
    assert!(set.is_empty().await.unwrap(), "set should be empty initially");
    assert_eq!(set.size().await.unwrap(), 0, "size should be 0");

    // Add items
    set.add("one".to_string()).await.unwrap();
    assert!(!set.is_empty().await.unwrap(), "set should not be empty");
    assert_eq!(set.size().await.unwrap(), 1, "size should be 1");

    set.add("two".to_string()).await.unwrap();
    assert_eq!(set.size().await.unwrap(), 2, "size should be 2");

    // Adding duplicate shouldn't increase size
    set.add("one".to_string()).await.unwrap();
    assert_eq!(set.size().await.unwrap(), 2, "size should still be 2 after duplicate add");

    set.clear().await.unwrap();
    client.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore] // Requires running Hazelcast cluster
async fn test_set_clear() {
    let client = create_test_client().await;
    let set = client.get_set::<String>("test-set-clear");

    set.clear().await.unwrap();

    // Add multiple items
    set.add("a".to_string()).await.unwrap();
    set.add("b".to_string()).await.unwrap();
    set.add("c".to_string()).await.unwrap();

    assert_eq!(set.size().await.unwrap(), 3);

    // Clear the set
    set.clear().await.unwrap();

    assert!(set.is_empty().await.unwrap(), "set should be empty after clear");
    assert_eq!(set.size().await.unwrap(), 0, "size should be 0 after clear");

    client.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore] // Requires running Hazelcast cluster
async fn test_set_with_integers() {
    let client = create_test_client().await;
    let set = client.get_set::<i32>("test-set-integers");

    set.clear().await.unwrap();

    set.add(42).await.unwrap();
    set.add(100).await.unwrap();
    set.add(-1).await.unwrap();

    assert!(set.contains(&42).await.unwrap());
    assert!(set.contains(&100).await.unwrap());
    assert!(set.contains(&-1).await.unwrap());
    assert!(!set.contains(&999).await.unwrap());

    assert_eq!(set.size().await.unwrap(), 3);

    set.clear().await.unwrap();
    client.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore] // Requires running Hazelcast cluster
async fn test_set_clone() {
    let client = create_test_client().await;
    let set1 = client.get_set::<String>("test-set-clone");

    set1.clear().await.unwrap();

    // Clone the set proxy
    let set2 = set1.clone();

    // Operations through clone should affect the same set
    set1.add("from-set1".to_string()).await.unwrap();

    assert!(set2.contains(&"from-set1".to_string()).await.unwrap());

    set2.add("from-set2".to_string()).await.unwrap();

    assert!(set1.contains(&"from-set2".to_string()).await.unwrap());

    set1.clear().await.unwrap();
    client.shutdown().await.unwrap();
}

#[test]
fn test_iset_is_send_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<hazelcast_client::ISet<String>>();
    assert_send_sync::<hazelcast_client::ISet<i32>>();
}
