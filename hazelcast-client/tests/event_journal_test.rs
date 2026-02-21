//! Integration tests for Event Journal functionality.
//!
//! Note: EventJournalConfig, EventJournalEventType, EventJournalMapEvent, and
//! EventJournalStream types are internal to the proxy module and not re-exported.
//! These tests verify the public API surface that is accessible to users.
//!
//! Run with: `cargo test --test event_journal_test -- --ignored`
//! Requires a Hazelcast cluster running at 127.0.0.1:5701

use std::net::SocketAddr;

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

#[test]
fn test_imap_is_send_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<hazelcast_client::proxy::IMap<String, String>>();
    assert_send_sync::<hazelcast_client::proxy::IMap<i64, Vec<u8>>>();
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_map_operations_for_journal() {
    let client = create_test_client().await;
    let map_name = format!("journal-test-{}", std::process::id());
    let map = client.get_map::<String, i32>(&map_name);

    // Add entries that would generate journal events
    map.put("key1".to_string(), 10).await.expect("put should succeed");
    map.put("key2".to_string(), 20).await.expect("put should succeed");

    // Update generates Updated event in journal
    map.put("key1".to_string(), 15).await.expect("update should succeed");

    // Remove generates Removed event in journal
    map.remove(&"key2".to_string()).await.expect("remove should succeed");

    // Verify map state
    let val = map.get(&"key1".to_string()).await.expect("get should work");
    assert_eq!(val, Some(15));

    let val = map.get(&"key2".to_string()).await.expect("get should work");
    assert_eq!(val, None);

    // Cleanup
    let _ = map.clear().await;
    client.shutdown().await.expect("shutdown failed");
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_map_eviction_events() {
    let client = create_test_client().await;
    let map_name = format!("eviction-test-{}", std::process::id());
    let map = client.get_map::<String, String>(&map_name);

    // Put entries
    for i in 0..10 {
        map.put(format!("key-{}", i), format!("value-{}", i))
            .await
            .expect("put should succeed");
    }

    // Evict a key
    let _ = map.evict(&"key-0".to_string()).await;

    // Clear generates clear event
    let _ = map.clear().await;

    client.shutdown().await.expect("shutdown failed");
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_map_clone_shares_event_journal() {
    let client = create_test_client().await;
    let map_name = format!("journal-clone-{}", std::process::id());
    let map1 = client.get_map::<String, i32>(&map_name);
    let map2 = map1.clone();

    // Operations through either reference affect same map
    map1.put("key-a".to_string(), 1).await.expect("put should work");
    map2.put("key-b".to_string(), 2).await.expect("put should work");

    let val_a = map2.get(&"key-a".to_string()).await.expect("get should work");
    assert_eq!(val_a, Some(1));

    let val_b = map1.get(&"key-b".to_string()).await.expect("get should work");
    assert_eq!(val_b, Some(2));

    // Cleanup
    let _ = map1.clear().await;
    client.shutdown().await.expect("shutdown failed");
}
