//! Integration tests for AtomicLong proxy.
//!
//! These tests require a running Hazelcast cluster with CP subsystem enabled.
//! Run with: `cargo test --test atomic_long_test -- --ignored`
//!
//! Start Hazelcast with CP subsystem:
//! ```
//! docker run -d --name hazelcast-cp -p 5701:5701 \
//!   -e HZ_CPSUBSYSTEM_CPSESSIONTTLSECONDS=300 \
//!   -e HZ_CPSUBSYSTEM_CPMEMBERCOUNT=1 \
//!   hazelcast/hazelcast:5.3
//! ```

use hazelcast_client::{ClientConfig, HazelcastClient};
use std::net::SocketAddr;

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

#[tokio::test]
#[ignore]
async fn test_atomic_long_get_initial_value() {
    let client = create_test_client().await;
    let counter = client.get_atomic_long("test-counter-initial");

    let value = counter.get().await.expect("Failed to get value");
    assert_eq!(value, 0);

    client.shutdown().await.expect("Failed to shutdown");
}

#[tokio::test]
#[ignore]
async fn test_atomic_long_set_and_get() {
    let client = create_test_client().await;
    let counter = client.get_atomic_long("test-counter-set-get");

    counter.set(42).await.expect("Failed to set value");
    let value = counter.get().await.expect("Failed to get value");
    assert_eq!(value, 42);

    counter.set(-100).await.expect("Failed to set negative value");
    let value = counter.get().await.expect("Failed to get value");
    assert_eq!(value, -100);

    client.shutdown().await.expect("Failed to shutdown");
}

#[tokio::test]
#[ignore]
async fn test_atomic_long_get_and_set() {
    let client = create_test_client().await;
    let counter = client.get_atomic_long("test-counter-get-and-set");

    counter.set(10).await.expect("Failed to set initial value");

    let old_value = counter.get_and_set(20).await.expect("Failed to get_and_set");
    assert_eq!(old_value, 10);

    let new_value = counter.get().await.expect("Failed to get value");
    assert_eq!(new_value, 20);

    client.shutdown().await.expect("Failed to shutdown");
}

#[tokio::test]
#[ignore]
async fn test_atomic_long_compare_and_set() {
    let client = create_test_client().await;
    let counter = client.get_atomic_long("test-counter-cas");

    counter.set(100).await.expect("Failed to set initial value");

    let success = counter.compare_and_set(100, 200).await.expect("Failed to CAS");
    assert!(success);
    assert_eq!(counter.get().await.expect("Failed to get"), 200);

    let success = counter.compare_and_set(100, 300).await.expect("Failed to CAS");
    assert!(!success);
    assert_eq!(counter.get().await.expect("Failed to get"), 200);

    client.shutdown().await.expect("Failed to shutdown");
}

#[tokio::test]
#[ignore]
async fn test_atomic_long_increment_and_get() {
    let client = create_test_client().await;
    let counter = client.get_atomic_long("test-counter-inc-get");

    counter.set(0).await.expect("Failed to set initial value");

    let value = counter.increment_and_get().await.expect("Failed to increment");
    assert_eq!(value, 1);

    let value = counter.increment_and_get().await.expect("Failed to increment");
    assert_eq!(value, 2);

    client.shutdown().await.expect("Failed to shutdown");
}

#[tokio::test]
#[ignore]
async fn test_atomic_long_decrement_and_get() {
    let client = create_test_client().await;
    let counter = client.get_atomic_long("test-counter-dec-get");

    counter.set(10).await.expect("Failed to set initial value");

    let value = counter.decrement_and_get().await.expect("Failed to decrement");
    assert_eq!(value, 9);

    let value = counter.decrement_and_get().await.expect("Failed to decrement");
    assert_eq!(value, 8);

    client.shutdown().await.expect("Failed to shutdown");
}

#[tokio::test]
#[ignore]
async fn test_atomic_long_get_and_increment() {
    let client = create_test_client().await;
    let counter = client.get_atomic_long("test-counter-get-inc");

    counter.set(5).await.expect("Failed to set initial value");

    let old_value = counter.get_and_increment().await.expect("Failed to get_and_increment");
    assert_eq!(old_value, 5);

    let new_value = counter.get().await.expect("Failed to get");
    assert_eq!(new_value, 6);

    client.shutdown().await.expect("Failed to shutdown");
}

#[tokio::test]
#[ignore]
async fn test_atomic_long_get_and_decrement() {
    let client = create_test_client().await;
    let counter = client.get_atomic_long("test-counter-get-dec");

    counter.set(5).await.expect("Failed to set initial value");

    let old_value = counter.get_and_decrement().await.expect("Failed to get_and_decrement");
    assert_eq!(old_value, 5);

    let new_value = counter.get().await.expect("Failed to get");
    assert_eq!(new_value, 4);

    client.shutdown().await.expect("Failed to shutdown");
}

#[tokio::test]
#[ignore]
async fn test_atomic_long_add_and_get() {
    let client = create_test_client().await;
    let counter = client.get_atomic_long("test-counter-add-get");

    counter.set(10).await.expect("Failed to set initial value");

    let value = counter.add_and_get(5).await.expect("Failed to add_and_get");
    assert_eq!(value, 15);

    let value = counter.add_and_get(-20).await.expect("Failed to add_and_get negative");
    assert_eq!(value, -5);

    client.shutdown().await.expect("Failed to shutdown");
}

#[tokio::test]
#[ignore]
async fn test_atomic_long_get_and_add() {
    let client = create_test_client().await;
    let counter = client.get_atomic_long("test-counter-get-add");

    counter.set(10).await.expect("Failed to set initial value");

    let old_value = counter.get_and_add(5).await.expect("Failed to get_and_add");
    assert_eq!(old_value, 10);

    let new_value = counter.get().await.expect("Failed to get");
    assert_eq!(new_value, 15);

    client.shutdown().await.expect("Failed to shutdown");
}

#[tokio::test]
#[ignore]
async fn test_atomic_long_clone() {
    let client = create_test_client().await;
    let counter1 = client.get_atomic_long("test-counter-clone");
    let counter2 = counter1.clone();

    counter1.set(100).await.expect("Failed to set via counter1");
    let value = counter2.get().await.expect("Failed to get via counter2");
    assert_eq!(value, 100);

    counter2.increment_and_get().await.expect("Failed to increment via counter2");
    let value = counter1.get().await.expect("Failed to get via counter1");
    assert_eq!(value, 101);

    client.shutdown().await.expect("Failed to shutdown");
}

#[test]
fn test_atomic_long_is_send_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<hazelcast_client::AtomicLong>();
}
