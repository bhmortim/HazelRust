//! Live data-effect regression for the cbdc "silent-element-drop" class.
//!
//! Several decoders iterated a List<Data>/EntryList and used the pattern
//! `if let Ok(x) = deserialize(..) { push(x) }` (or a match arm that only logged),
//! so a single element that failed to deserialize was *silently dropped* from the
//! returned collection. For a ledger, a vanished entry (wrong values()/entry_set()
//! sum, a lost key) is catastrophic and undetectable. The fix propagates the decode
//! error with `?`.
//!
//! These tests prove the data effect live: a value that cannot be deserialized as
//! the proxy's value type must make the bulk read return `Err`, not silently return
//! a short collection. We create the poison by writing an `i64` value under a map
//! the reader views as `<String, String>`: reading those 8 bytes as a Hazelcast
//! String yields an absurd length prefix and fails to deserialize.

mod common;

use hazelcast_client::HazelcastClient;

use crate::common::{default_config, unique_name};

async fn seed_with_poison(client: &HazelcastClient, name: &str) {
    let smap = client.get_map::<String, String>(name);
    for i in 0..5 {
        smap.put(format!("k{i}"), format!("v{i}")).await.unwrap();
    }
    // Inject an entry whose value will not deserialize as a String.
    let imap = client.get_map::<String, i64>(name);
    imap.put("poison".to_string(), i64::MAX).await.unwrap();
}

#[tokio::test]
#[ignore = "requires live Hazelcast cluster (CLUSTER_ADDRESS=127.0.0.1:5701)"]
async fn test_entries_propagates_undeserializable_element() {
    let client = HazelcastClient::new(default_config()).await.unwrap();
    let name = unique_name("poison-entries");
    seed_with_poison(&client, &name).await;

    // entries() -> decode_entries_response. Must surface the decode failure rather
    // than silently dropping the poison entry (returning 5 of 6).
    let result = client.get_map::<String, String>(&name).entries().await;
    assert!(
        result.is_err(),
        "entries() must propagate an undeserializable value, not silently drop it; got Ok(len={:?})",
        result.as_ref().map(|v| v.len())
    );

    client.get_map::<String, String>(&name).clear().await.ok();
    client.shutdown().await.ok();
}

#[tokio::test]
#[ignore = "requires live Hazelcast cluster (CLUSTER_ADDRESS=127.0.0.1:5701)"]
async fn test_entry_set_iterator_propagates_undeserializable_element() {
    let client = HazelcastClient::new(default_config()).await.unwrap();
    let name = unique_name("poison-entryset");
    seed_with_poison(&client, &name).await;

    // entry_set() -> DistributedIterator -> decode_entries (paged). Iterating must
    // surface the decode failure rather than silently skipping the poison entry.
    let mut it = client
        .get_map::<String, String>(&name)
        .entry_set()
        .await
        .unwrap();
    let mut hit_err = false;
    loop {
        match it.next_entry().await {
            Ok(Some(_)) => continue,
            Ok(None) => break,
            Err(_) => {
                hit_err = true;
                break;
            }
        }
    }
    assert!(
        hit_err,
        "entry_set() iterator must propagate an undeserializable element, not silently skip it"
    );

    client.get_map::<String, String>(&name).clear().await.ok();
    client.shutdown().await.ok();
}
