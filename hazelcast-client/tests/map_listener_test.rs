//! Integration tests for IMap entry listeners.
//!
//! These tests require a running Hazelcast cluster. Run with:
//! ```sh
//! docker run -d --name hazelcast -p 5701:5701 hazelcast/hazelcast:5.3
//! cargo test --test map_listener_test -- --ignored
//! ```

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use hazelcast_client::listener::{EntryEventType, EntryListenerConfig};
use hazelcast_client::{ClientConfigBuilder, HazelcastClient, IMap};

async fn create_test_client() -> HazelcastClient {
    let config = ClientConfigBuilder::new()
        .cluster_name("dev")
        .add_address("127.0.0.1:5701".parse().unwrap())
        .build()
        .expect("Failed to build config");

    HazelcastClient::new(config)
        .await
        .expect("Failed to connect to Hazelcast")
}

fn unique_map_name() -> String {
    format!("test-map-listener-{}", uuid::Uuid::new_v4())
}

#[tokio::test]
#[ignore]
async fn test_entry_listener_receives_put_event() {
    let client = create_test_client().await;
    let map: IMap<String, String> = client.get_map(&unique_map_name());

    let event_count = Arc::new(AtomicU32::new(0));
    let event_count_clone = Arc::clone(&event_count);

    let config = EntryListenerConfig::new().include_value(true).on_added();

    let registration = map
        .add_entry_listener(config, move |event| {
            assert_eq!(event.event_type, EntryEventType::Added);
            event_count_clone.fetch_add(1, Ordering::SeqCst);
        })
        .await
        .expect("Failed to add listener");

    assert!(registration.is_active());

    tokio::time::sleep(Duration::from_millis(500)).await;

    map.put("key1".to_string(), "value1".to_string())
        .await
        .expect("Failed to put");

    tokio::time::sleep(Duration::from_secs(2)).await;

    map.remove_entry_listener(&registration)
        .await
        .expect("Failed to remove listener");

    assert!(!registration.is_active());

    client.shutdown().await;
}

#[tokio::test]
#[ignore]
async fn test_entry_listener_receives_remove_event() {
    let client = create_test_client().await;
    let map: IMap<String, String> = client.get_map(&unique_map_name());

    let removed_count = Arc::new(AtomicU32::new(0));
    let removed_count_clone = Arc::clone(&removed_count);

    let config = EntryListenerConfig::new().include_value(true).on_removed();

    let registration = map
        .add_entry_listener(config, move |event| {
            if event.event_type == EntryEventType::Removed {
                removed_count_clone.fetch_add(1, Ordering::SeqCst);
            }
        })
        .await
        .expect("Failed to add listener");

    map.put("key1".to_string(), "value1".to_string())
        .await
        .expect("Failed to put");

    tokio::time::sleep(Duration::from_millis(500)).await;

    map.remove(&"key1".to_string())
        .await
        .expect("Failed to remove");

    tokio::time::sleep(Duration::from_secs(2)).await;

    map.remove_entry_listener(&registration)
        .await
        .expect("Failed to remove listener");

    client.shutdown().await;
}

#[tokio::test]
#[ignore]
async fn test_entry_listener_receives_update_event() {
    let client = create_test_client().await;
    let map: IMap<String, String> = client.get_map(&unique_map_name());

    let updated_count = Arc::new(AtomicU32::new(0));
    let updated_count_clone = Arc::clone(&updated_count);

    let config = EntryListenerConfig::new().include_value(true).on_updated();

    let registration = map
        .add_entry_listener(config, move |event| {
            if event.event_type == EntryEventType::Updated {
                updated_count_clone.fetch_add(1, Ordering::SeqCst);
            }
        })
        .await
        .expect("Failed to add listener");

    map.put("key1".to_string(), "value1".to_string())
        .await
        .expect("Failed to put");

    tokio::time::sleep(Duration::from_millis(500)).await;

    map.put("key1".to_string(), "value2".to_string())
        .await
        .expect("Failed to update");

    tokio::time::sleep(Duration::from_secs(2)).await;

    map.remove_entry_listener(&registration)
        .await
        .expect("Failed to remove listener");

    client.shutdown().await;
}

#[tokio::test]
#[ignore]
async fn test_entry_listener_all_events() {
    let client = create_test_client().await;
    let map: IMap<String, String> = client.get_map(&unique_map_name());

    let total_events = Arc::new(AtomicU32::new(0));
    let total_events_clone = Arc::clone(&total_events);

    let config = EntryListenerConfig::all();

    let registration = map
        .add_entry_listener(config, move |_event| {
            total_events_clone.fetch_add(1, Ordering::SeqCst);
        })
        .await
        .expect("Failed to add listener");

    tokio::time::sleep(Duration::from_millis(500)).await;

    map.put("key1".to_string(), "value1".to_string())
        .await
        .expect("Failed to put");

    map.put("key1".to_string(), "value2".to_string())
        .await
        .expect("Failed to update");

    map.remove(&"key1".to_string())
        .await
        .expect("Failed to remove");

    tokio::time::sleep(Duration::from_secs(2)).await;

    map.remove_entry_listener(&registration)
        .await
        .expect("Failed to remove listener");

    assert!(!registration.is_active());

    client.shutdown().await;
}

#[tokio::test]
#[ignore]
async fn test_multiple_entry_listeners() {
    let client = create_test_client().await;
    let map: IMap<String, String> = client.get_map(&unique_map_name());

    let listener1_count = Arc::new(AtomicU32::new(0));
    let listener1_count_clone = Arc::clone(&listener1_count);

    let listener2_count = Arc::new(AtomicU32::new(0));
    let listener2_count_clone = Arc::clone(&listener2_count);

    let config1 = EntryListenerConfig::new().on_added();
    let config2 = EntryListenerConfig::new().on_added().on_removed();

    let reg1 = map
        .add_entry_listener(config1, move |_| {
            listener1_count_clone.fetch_add(1, Ordering::SeqCst);
        })
        .await
        .expect("Failed to add listener 1");

    let reg2 = map
        .add_entry_listener(config2, move |_| {
            listener2_count_clone.fetch_add(1, Ordering::SeqCst);
        })
        .await
        .expect("Failed to add listener 2");

    tokio::time::sleep(Duration::from_millis(500)).await;

    map.put("key1".to_string(), "value1".to_string())
        .await
        .expect("Failed to put");

    tokio::time::sleep(Duration::from_secs(2)).await;

    map.remove_entry_listener(&reg1)
        .await
        .expect("Failed to remove listener 1");
    map.remove_entry_listener(&reg2)
        .await
        .expect("Failed to remove listener 2");

    client.shutdown().await;
}

#[tokio::test]
#[ignore]
async fn test_listener_stats() {
    let client = create_test_client().await;
    let map: IMap<String, String> = client.get_map(&unique_map_name());

    let config = EntryListenerConfig::all();

    let registration = map
        .add_entry_listener(config, |_event| {})
        .await
        .expect("Failed to add listener");

    assert_eq!(map.listener_stats().messages_received(), 0);
    assert_eq!(map.listener_stats().errors(), 0);

    tokio::time::sleep(Duration::from_millis(500)).await;

    map.put("key1".to_string(), "value1".to_string())
        .await
        .expect("Failed to put");

    tokio::time::sleep(Duration::from_secs(2)).await;

    map.remove_entry_listener(&registration)
        .await
        .expect("Failed to remove listener");

    client.shutdown().await;
}

#[test]
fn test_entry_listener_config_builder() {
    let config = EntryListenerConfig::new()
        .include_value(true)
        .on_added()
        .on_updated()
        .on_removed();

    assert!(config.include_value);
    assert!(config.on_added);
    assert!(config.on_updated);
    assert!(config.on_removed);
    assert!(!config.on_evicted);
    assert!(!config.on_expired);

    assert_eq!(config.event_flags(), 7);
}

#[test]
fn test_entry_listener_config_accepts() {
    let config = EntryListenerConfig::new().on_added().on_removed();

    assert!(config.accepts(EntryEventType::Added));
    assert!(!config.accepts(EntryEventType::Updated));
    assert!(config.accepts(EntryEventType::Removed));
    assert!(!config.accepts(EntryEventType::Evicted));
    assert!(!config.accepts(EntryEventType::Expired));
}
