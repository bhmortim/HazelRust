//! Distributed-correctness: IMap optimistic concurrency single-winner (cbdc).
//!
//! The AP-map equivalent of the CP no-double-spend guarantee. A single key's
//! partition owner serializes operations, so among many concurrent attempts:
//!   - replace_if_equal(key, "0", id): EXACTLY ONE must win.
//!   - put_if_absent(key, id) on an absent key: EXACTLY ONE wins (returns None).
//! These are the optimistic-concurrency primitives a CBDC ledger uses on IMap.

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use hazelcast_client::HazelcastClient;

const CONTENDERS: usize = 20;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the dev cluster"]
async fn test_imap_concurrent_cas_exactly_one_winner() {
    let client = Arc::new(
        HazelcastClient::new(common::default_config())
            .await
            .expect("connect"),
    );

    let map_name = common::unique_name("imap-cas");
    let key = "acct".to_string();
    client
        .get_map::<String, String>(&map_name)
        .put(key.clone(), "0".to_string())
        .await
        .expect("seed");

    let winners = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();
    for id in 1..=CONTENDERS {
        let c = Arc::clone(&client);
        let n = map_name.clone();
        let k = key.clone();
        let w = Arc::clone(&winners);
        handles.push(tokio::spawn(async move {
            let map = c.get_map::<String, String>(&n);
            if map
                .replace_if_equal(&k, &"0".to_string(), id.to_string())
                .await
                .expect("replace_if_equal")
            {
                w.fetch_add(1, Ordering::SeqCst);
            }
        }));
    }
    for h in handles {
        h.await.expect("join");
    }

    assert_eq!(
        winners.load(Ordering::SeqCst),
        1,
        "exactly one concurrent IMap CAS(0 -> id) must win"
    );
    client.shutdown().await.ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the dev cluster"]
async fn test_imap_concurrent_put_if_absent_exactly_one_winner() {
    let client = Arc::new(
        HazelcastClient::new(common::default_config())
            .await
            .expect("connect"),
    );

    let map_name = common::unique_name("imap-pia");
    let key = "acct".to_string(); // absent initially

    let winners = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();
    for id in 1..=CONTENDERS {
        let c = Arc::clone(&client);
        let n = map_name.clone();
        let k = key.clone();
        let w = Arc::clone(&winners);
        handles.push(tokio::spawn(async move {
            let map = c.get_map::<String, String>(&n);
            // The winner created the entry -> put_if_absent returns None; losers
            // see the existing value (Some).
            if map
                .put_if_absent(k, id.to_string())
                .await
                .expect("put_if_absent")
                .is_none()
            {
                w.fetch_add(1, Ordering::SeqCst);
            }
        }));
    }
    for h in handles {
        h.await.expect("join");
    }

    assert_eq!(
        winners.load(Ordering::SeqCst),
        1,
        "exactly one concurrent put_if_absent must win (return None)"
    );
    client.shutdown().await.ok();
}
