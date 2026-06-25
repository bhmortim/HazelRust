//! Cross-client value-fidelity check for the collection structures (cbdc).
//!
//! A stock Java client (XColl.java on the instance) writes a known String value
//! into each structure; the Rust client must read the SAME value back. A
//! wrong/garbled value would mean a Data-value framing bug (like the one found
//! and fixed in AtomicReference). Assertions are skipped when the Java harness
//! wasn't run (a clean cluster restart clears AP state), so this is safe in the
//! suite; run `XColl write` on the instance first for the full check.

mod common;

use hazelcast_client::HazelcastClient;

#[tokio::test]
#[ignore = "requires the dev cluster + Java-written collections (XColl.java)"]
async fn test_collections_read_java_written_values() {
    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("connect to dev cluster");

    // IMap (control — known cross-client-correct via java_parity).
    let imap = client.get_map::<String, String>("xc_map");
    if let Some(v) = imap.get(&"k".to_string()).await.expect("imap get") {
        assert_eq!(v, "imap-java", "IMap cross-client value corrupted");
    } else {
        eprintln!("NOTE: xc_map empty — run `XColl write` on the instance for the full check");
        client.shutdown().await.ok();
        return;
    }

    // ReplicatedMap.
    let rmap = client.get_replicated_map::<String, String>("xc_rmap");
    if let Some(v) = rmap.get(&"k".to_string()).await.expect("rmap get") {
        assert_eq!(v, "rmap-java", "ReplicatedMap cross-client value corrupted");
    }

    // IList.
    let list = client.get_list::<String>("xc_list");
    assert!(
        list.contains(&"list-java".to_string())
            .await
            .expect("list contains"),
        "IList cross-client: 'list-java' not found (value corrupted?)"
    );

    // ISet.
    let set = client.get_set::<String>("xc_set");
    assert!(
        set.contains(&"set-java".to_string())
            .await
            .expect("set contains"),
        "ISet cross-client: 'set-java' not found (value corrupted?)"
    );

    // MultiMap.
    let mmap = client.get_multimap::<String, String>("xc_mmap");
    let mvals = mmap.get(&"k".to_string()).await.expect("mmap get");
    assert!(
        mvals.iter().any(|v| v == "mmap-java"),
        "MultiMap cross-client: got {mvals:?} (value corrupted?)"
    );

    // IQueue.
    let queue = client.get_queue::<String>("xc_queue");
    let qvals = queue.to_vec().await.expect("queue to_vec");
    assert!(
        qvals.iter().any(|v| v == "queue-java"),
        "IQueue cross-client: got {qvals:?} (value corrupted?)"
    );

    client.shutdown().await.ok();
}
