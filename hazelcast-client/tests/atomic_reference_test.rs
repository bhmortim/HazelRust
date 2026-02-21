//! Integration tests for AtomicReference operations.
//!
//! These tests require a running Hazelcast cluster with CP subsystem enabled.
//! Run with: `cargo test --test atomic_reference_test -- --ignored`

use std::net::SocketAddr;

use hazelcast_client::{ClientConfig, HazelcastClient};
use hazelcast_client::proxy::AtomicReference;

async fn create_test_client() -> HazelcastClient {
    let config = ClientConfig::builder()
        .cluster_name("dev")
        .add_address("127.0.0.1:5701".parse::<SocketAddr>().unwrap())
        .build()
        .expect("Failed to build config");

    HazelcastClient::new(config)
        .await
        .expect("Failed to connect to Hazelcast")
}

#[test]
fn test_atomic_reference_is_send_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<AtomicReference<String>>();
    assert_send_sync::<AtomicReference<i64>>();
    assert_send_sync::<AtomicReference<Vec<u8>>>();
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster with CP subsystem"]
async fn test_atomic_reference_creation() {
    let client = create_test_client().await;
    let reference = client.get_atomic_reference::<String>("test-ref");

    assert_eq!(reference.name(), "test-ref");

    client.shutdown().await.expect("shutdown failed");
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster with CP subsystem"]
async fn test_atomic_reference_clone() {
    let client = create_test_client().await;
    let reference = client.get_atomic_reference::<String>("ref1");
    let cloned = reference.clone();

    assert_eq!(reference.name(), cloned.name());

    client.shutdown().await.expect("shutdown failed");
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster with CP subsystem"]
async fn test_atomic_reference_get() {
    let client = create_test_client().await;
    let reference = client.get_atomic_reference::<String>("test-get");

    let result = reference.get().await;
    println!("Get result: {:?}", result);

    client.shutdown().await.expect("shutdown failed");
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster with CP subsystem"]
async fn test_atomic_reference_set() {
    let client = create_test_client().await;
    let reference = client.get_atomic_reference::<String>("test-set");

    let result = reference.set(Some("value".to_string())).await;
    println!("Set result: {:?}", result);

    client.shutdown().await.expect("shutdown failed");
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster with CP subsystem"]
async fn test_atomic_reference_get_and_set() {
    let client = create_test_client().await;
    let reference = client.get_atomic_reference::<String>("test-get-and-set");

    // Set initial value
    reference.set(Some("initial".to_string())).await.expect("set should work");

    // Get and set
    let old = reference.get_and_set(Some("updated".to_string())).await;
    println!("Old value: {:?}", old);

    let current = reference.get().await;
    println!("Current value: {:?}", current);

    client.shutdown().await.expect("shutdown failed");
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster with CP subsystem"]
async fn test_atomic_reference_compare_and_set() {
    let client = create_test_client().await;
    let reference = client.get_atomic_reference::<String>("test-cas");

    // Set initial value
    reference.set(Some("original".to_string())).await.expect("set should work");

    // Compare and set with correct expected value
    let expected = "original".to_string();
    let result = reference.compare_and_set(
        Some(&expected),
        Some("updated".to_string()),
    ).await;
    println!("CAS result (should succeed): {:?}", result);

    // Compare and set with wrong expected value
    let wrong = "wrong".to_string();
    let result = reference.compare_and_set(
        Some(&wrong),
        Some("should-not-set".to_string()),
    ).await;
    println!("CAS result (should fail): {:?}", result);

    client.shutdown().await.expect("shutdown failed");
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster with CP subsystem"]
async fn test_atomic_reference_contains() {
    let client = create_test_client().await;
    let reference = client.get_atomic_reference::<String>("test-contains");

    reference.set(Some("hello".to_string())).await.expect("set should work");

    let result = reference.contains(&"hello".to_string()).await;
    println!("Contains 'hello': {:?}", result);

    let result = reference.contains(&"world".to_string()).await;
    println!("Contains 'world': {:?}", result);

    client.shutdown().await.expect("shutdown failed");
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster with CP subsystem"]
async fn test_atomic_reference_is_null() {
    let client = create_test_client().await;
    let reference = client.get_atomic_reference::<String>("test-is-null");

    let result = reference.is_null().await;
    println!("Is null (initial): {:?}", result);

    reference.set(Some("value".to_string())).await.expect("set should work");
    let result = reference.is_null().await;
    println!("Is null (after set): {:?}", result);

    client.shutdown().await.expect("shutdown failed");
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster with CP subsystem"]
async fn test_atomic_reference_clear() {
    let client = create_test_client().await;
    let reference = client.get_atomic_reference::<String>("test-clear");

    reference.set(Some("value".to_string())).await.expect("set should work");

    let result = reference.clear().await;
    println!("Clear result: {:?}", result);

    let is_null = reference.is_null().await;
    println!("Is null after clear: {:?}", is_null);

    client.shutdown().await.expect("shutdown failed");
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster with CP subsystem"]
async fn test_atomic_reference_with_different_types() {
    let client = create_test_client().await;

    let _string_ref = client.get_atomic_reference::<String>("str-ref");
    let _i64_ref = client.get_atomic_reference::<i64>("i64-ref");
    let _vec_ref = client.get_atomic_reference::<Vec<u8>>("vec-ref");

    client.shutdown().await.expect("shutdown failed");
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster with CP subsystem"]
async fn test_atomic_reference_name_preserved_on_clone() {
    let client = create_test_client().await;
    let original = client.get_atomic_reference::<i32>("counter");
    let cloned = original.clone();

    assert_eq!(original.name(), "counter");
    assert_eq!(cloned.name(), "counter");

    client.shutdown().await.expect("shutdown failed");
}
