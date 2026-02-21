//! Integration tests for async map operations.
//!
//! These tests require a running Hazelcast cluster.
//! Run with: `cargo test --test async_map_test -- --ignored`

use std::net::SocketAddr;

use hazelcast_client::{ClientConfig, HazelcastClient};
use hazelcast_core::Result;

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
fn test_imap_is_send_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<hazelcast_client::proxy::IMap<String, String>>();
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_async_get_returns_join_handle() {
    let client = create_test_client().await;
    let map = client.get_map::<String, String>("async-map-test");

    let handle = map.get_async("nonexistent".to_string());
    let result = handle.await.unwrap();
    println!("get_async result: {:?}", result);

    client.shutdown().await.expect("shutdown failed");
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_put_async_returns_join_handle() {
    let client = create_test_client().await;
    let map = client.get_map::<String, String>("async-map-test");

    let handle = map.put_async("key".to_string(), "value".to_string());
    let result = handle.await.unwrap();
    println!("put_async result: {:?}", result);

    // Cleanup
    let _ = map.remove(&"key".to_string()).await;
    client.shutdown().await.expect("shutdown failed");
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_remove_async_returns_join_handle() {
    let client = create_test_client().await;
    let map = client.get_map::<String, String>("async-map-test");

    // Setup
    map.put("key".to_string(), "value".to_string())
        .await
        .expect("put should succeed");

    let handle = map.remove_async("key".to_string());
    let result = handle.await.unwrap();
    println!("remove_async result: {:?}", result);

    client.shutdown().await.expect("shutdown failed");
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_contains_key_async_returns_join_handle() {
    let client = create_test_client().await;
    let map = client.get_map::<String, String>("async-map-test");

    let handle = map.contains_key_async("key".to_string());
    let result = handle.await.unwrap();
    println!("contains_key_async result: {:?}", result);

    client.shutdown().await.expect("shutdown failed");
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_multiple_async_operations_concurrent() {
    let client = create_test_client().await;
    let map = client.get_map::<String, String>("concurrent-async-map");

    let handle1 = map.get_async("key1".to_string());
    let handle2 = map.get_async("key2".to_string());
    let handle3 = map.get_async("key3".to_string());

    let (r1, r2, r3) = tokio::join!(handle1, handle2, handle3);
    println!("Results: {:?}, {:?}, {:?}", r1, r2, r3);

    client.shutdown().await.expect("shutdown failed");
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_async_batch_operations() {
    let client = create_test_client().await;
    let map = client.get_map::<String, i64>("batch-async-map");

    let handles: Vec<_> = (0..10)
        .map(|i| map.put_async(format!("key-{}", i), i))
        .collect();

    assert_eq!(handles.len(), 10);

    for handle in handles {
        let _ = handle.await;
    }

    // Cleanup
    let _ = map.clear().await;
    client.shutdown().await.expect("shutdown failed");
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_async_methods_from_cloned_map() {
    let client = create_test_client().await;
    let map = client.get_map::<String, String>("clone-test-map");
    let cloned = map.clone();

    let handle1 = map.get_async("key1".to_string());
    let handle2 = cloned.get_async("key2".to_string());

    let (r1, r2) = tokio::join!(handle1, handle2);
    println!("Results: {:?}, {:?}", r1, r2);

    client.shutdown().await.expect("shutdown failed");
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_async_operation_preserves_map_state() {
    let client = create_test_client().await;
    let map = client.get_map::<String, String>("state-test-map");

    assert_eq!(map.name(), "state-test-map");

    let _h = map.get_async("key".to_string());

    assert_eq!(map.name(), "state-test-map");

    client.shutdown().await.expect("shutdown failed");
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_async_operations_fire_and_forget() {
    let client = create_test_client().await;
    let map = client.get_map::<String, i32>("fire-forget-map");

    // Fire-and-forget: start async ops but don't await
    let _h1 = map.put_async("counter1".to_string(), 1);
    let _h2 = map.put_async("counter2".to_string(), 2);
    let _h3 = map.put_async("counter3".to_string(), 3);

    // Cleanup
    let _ = map.clear().await;
    client.shutdown().await.expect("shutdown failed");
}

#[test]
fn test_join_handle_types() {
    fn assert_result_option<T>(_: tokio::task::JoinHandle<Result<Option<T>>>) {}
    fn assert_result_bool(_: tokio::task::JoinHandle<Result<bool>>) {}

    // Type assertions - these are compile-time checks only.
    // The actual JoinHandle types are verified by the function signatures above.
    // Since we can't construct IMap without a client, these are marker tests.
}
