//! Integration tests for AtomicReference operations.
//!
//! These tests require a running Hazelcast cluster with the CP subsystem enabled
//! (an Enterprise feature in Hazelcast 5.7+). Run with:
//! `cargo test --test atomic_reference_test -- --ignored`
//!
//! Unlike smoke tests, these assert the *returned values* of every operation, so a
//! regression that silently no-ops (e.g. dropping the Raft groupId — issue #12) fails
//! the suite instead of passing quietly.

use std::net::SocketAddr;

use hazelcast_client::proxy::AtomicReference;
use hazelcast_client::{ClientConfig, HazelcastClient};

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

/// Unique per-process object name so repeated runs against a persistent CP cluster
/// don't collide on leftover state.
fn ref_name(base: &str) -> String {
    format!("{}-{}", base, std::process::id())
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
async fn test_atomic_reference_name_preserved_on_clone() {
    let client = create_test_client().await;
    let reference = client.get_atomic_reference::<String>("ref1");
    let cloned = reference.clone();
    assert_eq!(reference.name(), cloned.name());
    client.shutdown().await.expect("shutdown failed");
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster with CP subsystem"]
async fn test_atomic_reference_set_and_get() {
    let client = create_test_client().await;
    let reference = client.get_atomic_reference::<String>(&ref_name("test-set-get"));

    reference
        .set(Some("hello".to_string()))
        .await
        .expect("set should succeed");

    let value = reference.get().await.expect("get should succeed");
    assert_eq!(value, Some("hello".to_string()));

    client.shutdown().await.expect("shutdown failed");
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster with CP subsystem"]
async fn test_atomic_reference_set_null_then_get() {
    let client = create_test_client().await;
    let reference = client.get_atomic_reference::<String>(&ref_name("test-set-null"));

    reference
        .set(Some("not-null".to_string()))
        .await
        .expect("set should succeed");
    assert_eq!(
        reference.get().await.expect("get should succeed"),
        Some("not-null".to_string())
    );

    reference.set(None).await.expect("set(None) should succeed");
    assert_eq!(reference.get().await.expect("get should succeed"), None);

    client.shutdown().await.expect("shutdown failed");
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster with CP subsystem"]
async fn test_atomic_reference_get_initial_is_null() {
    let client = create_test_client().await;
    let reference = client.get_atomic_reference::<String>(&ref_name("test-initial-null"));

    assert_eq!(reference.get().await.expect("get should succeed"), None);
    assert!(reference.is_null().await.expect("is_null should succeed"));

    client.shutdown().await.expect("shutdown failed");
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster with CP subsystem"]
async fn test_atomic_reference_get_and_set() {
    let client = create_test_client().await;
    let reference = client.get_atomic_reference::<String>(&ref_name("test-get-and-set"));

    reference
        .set(Some("initial".to_string()))
        .await
        .expect("set should succeed");

    let old = reference
        .get_and_set(Some("updated".to_string()))
        .await
        .expect("get_and_set should succeed");
    assert_eq!(old, Some("initial".to_string()));

    let current = reference.get().await.expect("get should succeed");
    assert_eq!(current, Some("updated".to_string()));

    client.shutdown().await.expect("shutdown failed");
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster with CP subsystem"]
async fn test_atomic_reference_compare_and_set() {
    let client = create_test_client().await;
    let reference = client.get_atomic_reference::<String>(&ref_name("test-cas"));

    reference
        .set(Some("original".to_string()))
        .await
        .expect("set should succeed");

    let expected = "original".to_string();
    let ok = reference
        .compare_and_set(Some(&expected), Some("updated".to_string()))
        .await
        .expect("compare_and_set should succeed");
    assert!(ok, "CAS with correct expected value must succeed");
    assert_eq!(
        reference.get().await.expect("get should succeed"),
        Some("updated".to_string())
    );

    let wrong = "wrong".to_string();
    let ok = reference
        .compare_and_set(Some(&wrong), Some("should-not-set".to_string()))
        .await
        .expect("compare_and_set should succeed");
    assert!(!ok, "CAS with wrong expected value must fail");
    assert_eq!(
        reference.get().await.expect("get should succeed"),
        Some("updated".to_string())
    );

    client.shutdown().await.expect("shutdown failed");
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster with CP subsystem"]
async fn test_atomic_reference_compare_and_set_from_null() {
    let client = create_test_client().await;
    let reference = client.get_atomic_reference::<String>(&ref_name("test-cas-null"));

    reference.set(None).await.expect("set(None) should succeed");

    let ok = reference
        .compare_and_set(None, Some("first".to_string()))
        .await
        .expect("compare_and_set should succeed");
    assert!(ok, "CAS from null must succeed");
    assert_eq!(
        reference.get().await.expect("get should succeed"),
        Some("first".to_string())
    );

    client.shutdown().await.expect("shutdown failed");
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster with CP subsystem"]
async fn test_atomic_reference_contains() {
    let client = create_test_client().await;
    let reference = client.get_atomic_reference::<String>(&ref_name("test-contains"));

    reference
        .set(Some("present".to_string()))
        .await
        .expect("set should succeed");

    assert!(reference
        .contains(&"present".to_string())
        .await
        .expect("contains should succeed"));
    assert!(!reference
        .contains(&"absent".to_string())
        .await
        .expect("contains should succeed"));

    client.shutdown().await.expect("shutdown failed");
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster with CP subsystem"]
async fn test_atomic_reference_is_null() {
    let client = create_test_client().await;
    let reference = client.get_atomic_reference::<String>(&ref_name("test-is-null"));

    reference.set(None).await.expect("set(None) should succeed");
    assert!(reference.is_null().await.expect("is_null should succeed"));

    reference
        .set(Some("value".to_string()))
        .await
        .expect("set should succeed");
    assert!(!reference.is_null().await.expect("is_null should succeed"));

    client.shutdown().await.expect("shutdown failed");
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster with CP subsystem"]
async fn test_atomic_reference_clear() {
    let client = create_test_client().await;
    let reference = client.get_atomic_reference::<String>(&ref_name("test-clear"));

    reference
        .set(Some("value".to_string()))
        .await
        .expect("set should succeed");
    assert!(!reference.is_null().await.expect("is_null should succeed"));

    reference.clear().await.expect("clear should succeed");
    assert_eq!(reference.get().await.expect("get should succeed"), None);
    assert!(reference.is_null().await.expect("is_null should succeed"));

    client.shutdown().await.expect("shutdown failed");
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster with CP subsystem"]
async fn test_atomic_reference_clone_shares_state() {
    let client = create_test_client().await;
    let reference = client.get_atomic_reference::<String>(&ref_name("test-clone-shared"));
    let cloned = reference.clone();

    reference
        .set(Some("via-original".to_string()))
        .await
        .expect("set should succeed");

    assert_eq!(
        cloned.get().await.expect("get should succeed"),
        Some("via-original".to_string())
    );

    client.shutdown().await.expect("shutdown failed");
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster with CP subsystem"]
async fn test_atomic_reference_with_different_types() {
    let client = create_test_client().await;

    let int_ref = client.get_atomic_reference::<i64>(&ref_name("test-int"));
    int_ref.set(Some(42)).await.expect("set i64 should succeed");
    assert_eq!(int_ref.get().await.expect("get should succeed"), Some(42));

    let old = int_ref
        .get_and_set(Some(-7))
        .await
        .expect("get_and_set i64 should succeed");
    assert_eq!(old, Some(42));
    assert_eq!(int_ref.get().await.expect("get should succeed"), Some(-7));

    client.shutdown().await.expect("shutdown failed");
}
