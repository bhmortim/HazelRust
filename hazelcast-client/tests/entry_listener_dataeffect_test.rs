//! Live data-effect test for the IMap entry-listener decode fix (cbdc).
//!
//! `decode_entry_event` was broadly mis-framed against the EE 5.7
//! `MapAddEntryListenerCodec` EVENT_ENTRY layout (wrong eventType offset, a
//! 16-byte instead of 17-byte uuid, the wrong var-frame indices for key/value,
//! and — worst — it read each `Data` payload from offset 0, consuming the 8-byte
//! header as payload, while `.ok()` silently dropped value decode failures). The
//! existing `map_listener_test.rs` listener tests never assert the callback
//! fired, so they passed vacuously even though no event was ever delivered.
//!
//! This asserts the **data effect**: the listener must deliver the correct key
//! and value for ADD and UPDATE events. Before the fix the callback never fires
//! (decode error) or delivers a garbled key — the recv() times out and this
//! FAILS; after the fix it delivers the right key/value and PASSES.

mod common;

use std::time::Duration;

use hazelcast_client::listener::{EntryEvent, EntryEventType, EntryListenerConfig};
use hazelcast_client::HazelcastClient;

#[tokio::test]
#[ignore = "requires a live Hazelcast cluster (CLUSTER_ADDRESS=127.0.0.1:5701)"]
async fn test_entry_listener_delivers_correct_key_and_value() {
    // Connect to all members so the entry-listener registration and the puts
    // share established connections (a single-seed client warms non-seed
    // connections in the background, which can momentarily race event delivery).
    let client = HazelcastClient::new(common::multi_member_config())
        .await
        .expect("connect to dev cluster");

    let map = client.get_map::<String, String>(&common::unique_name("entry_listener_de"));

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<EntryEvent<String, String>>();
    let config = EntryListenerConfig::new()
        .include_value(true)
        .on_added()
        .on_updated();
    let _reg = map
        .add_entry_listener(config, move |ev| {
            let _ = tx.send(ev);
        })
        .await
        .expect("register entry listener");

    // Give the server-side registration a moment to take effect.
    tokio::time::sleep(Duration::from_millis(400)).await;

    // ADD — the event must carry the exact key + new value (a money-path read of
    // a mutation: a garbled key or vanished value is catastrophic).
    map.put("acct-42".to_string(), "balance-100".to_string())
        .await
        .expect("put");
    let ev = tokio::time::timeout(Duration::from_secs(8), rx.recv())
        .await
        .expect("timed out waiting for ADDED event (listener never delivered it)")
        .expect("event channel closed");
    assert_eq!(
        ev.event_type,
        EntryEventType::Added,
        "expected an ADDED event"
    );
    assert_eq!(ev.key, "acct-42", "listener must deliver the correct key");
    assert_eq!(
        ev.new_value.as_deref(),
        Some("balance-100"),
        "listener must deliver the correct new value for ADD"
    );

    // A second, distinct ADD — proves multiple events are delivered and decoded
    // (not just the first), with the correct key/value each time.
    map.put("acct-99".to_string(), "balance-777".to_string())
        .await
        .expect("put 2");
    let ev2 = loop {
        let e = tokio::time::timeout(Duration::from_secs(8), rx.recv())
            .await
            .expect("timed out waiting for second ADDED event")
            .expect("event channel closed");
        if e.key == "acct-99" {
            break e;
        }
    };
    assert_eq!(ev2.event_type, EntryEventType::Added);
    assert_eq!(
        ev2.new_value.as_deref(),
        Some("balance-777"),
        "second ADD event must carry the correct value"
    );

    // UPDATE of an existing key — must carry both the new and the previous value.
    map.put("acct-42".to_string(), "balance-250".to_string())
        .await
        .expect("put update");
    let ev3 = loop {
        let e = tokio::time::timeout(Duration::from_secs(8), rx.recv())
            .await
            .expect("timed out waiting for UPDATED event")
            .expect("event channel closed");
        if e.event_type == EntryEventType::Updated {
            break e;
        }
    };
    assert_eq!(ev3.key, "acct-42");
    assert_eq!(
        ev3.new_value.as_deref(),
        Some("balance-250"),
        "update event must carry the new value"
    );
    assert_eq!(
        ev3.old_value.as_deref(),
        Some("balance-100"),
        "update event must carry the previous (old) value"
    );

    map.clear().await.ok();
    client.shutdown().await.ok();
}
