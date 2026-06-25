//! Live data-effect test for the event-journal read fix (cbdc).
//!
//! Requires an event-journal-enabled map ("ej-*" wildcard in the dev cluster
//! config). Puts entries, reads the per-partition journal, and asserts the event
//! key/value. Run with HZ_DEBUG_EVENT_JOURNAL=1 to dump the raw response.

mod common;

use std::time::Duration;

use futures::StreamExt;
use hazelcast_client::{EventJournalConfig, EventJournalEventType, HazelcastClient};

#[tokio::test]
#[ignore = "requires the dev cluster with an event-journal-enabled map (ej-*)"]
async fn test_event_journal_read_data_effect() {
    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("connect to dev cluster");

    let map = client.get_map::<String, String>(&common::unique_name("ej-journal"));
    map.put("acct-1".to_string(), "bal-100".to_string())
        .await
        .expect("put");
    map.put("acct-1".to_string(), "bal-200".to_string())
        .await
        .expect("put update");

    // The routing fix: PartitionService::get_partition(key) must point at the
    // SAME partition the cluster placed the entry on (and thus whose event journal
    // holds the events). Before the fix this returned 123 while the entry lived on
    // 198, so reading "the key's partition journal" silently missed every event.
    let expected = client
        .partition_service()
        .get_partition(&"acct-1".to_string())
        .await;
    eprintln!("[ej-test] get_partition(acct-1) = {}", expected.id());

    let config = EventJournalConfig::new()
        .start_sequence(0)
        .min_size(1)
        .max_size(100);
    let mut stream = map
        .read_from_event_journal(expected.id(), config)
        .await
        .expect("read_from_event_journal request");

    // min_size=1 blocks server-side until >=1 event is available. A timeout here
    // means get_partition pointed at a partition with no events (routing bug).
    let ev = tokio::time::timeout(Duration::from_secs(8), stream.next())
        .await
        .expect("timed out: get_partition(acct-1) points at a journal with no events")
        .expect("event-journal stream ended unexpectedly")
        .expect("event-journal event error");

    eprintln!(
        "[ej-test] EVENT key={:?} type={:?} old={:?} new={:?}",
        ev.key(),
        ev.event_type(),
        ev.old_value(),
        ev.new_value()
    );
    assert_eq!(
        ev.event_type(),
        EventJournalEventType::Added,
        "first journal event for a fresh key should be ADDED"
    );
    assert_eq!(
        ev.key(),
        "acct-1",
        "event-journal event key must decode to the put key"
    );

    client.shutdown().await.ok();
}
