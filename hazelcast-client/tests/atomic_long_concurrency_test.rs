//! Distributed-correctness: concurrent atomic increments, no lost updates (cbdc).
//!
//! A CBDC must apply concurrent balance updates atomically — N tasks each doing M
//! atomic increments on one counter must yield EXACTLY N*M (no lost updates, no
//! response cross-wiring under concurrency). This shares one client across many
//! tokio tasks hammering a single CP AtomicLong and asserts the exact total — also
//! exercising the invocation layer's correlation-id demultiplexing under load.

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use hazelcast_client::HazelcastClient;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the dev cluster CP subsystem"]
async fn test_atomic_long_concurrent_increments_no_lost_updates() {
    let client = Arc::new(
        HazelcastClient::new(common::default_config())
            .await
            .expect("connect"),
    );

    let name = common::unique_name("xc_along_concurrent");
    client.get_atomic_long(&name).set(0).await.expect("set 0");

    const TASKS: i64 = 10;
    const PER_TASK: i64 = 100;

    let mut handles = Vec::new();
    for _ in 0..TASKS {
        let c = Arc::clone(&client);
        let n = name.clone();
        handles.push(tokio::spawn(async move {
            let along = c.get_atomic_long(&n);
            for _ in 0..PER_TASK {
                along
                    .increment_and_get()
                    .await
                    .expect("concurrent increment");
            }
        }));
    }
    for h in handles {
        h.await.expect("join task");
    }

    let total = client.get_atomic_long(&name).get().await.expect("final get");
    assert_eq!(
        total,
        TASKS * PER_TASK,
        "concurrent atomic increments must not lose updates (expected {} got {total})",
        TASKS * PER_TASK
    );
    client.shutdown().await.ok();
}

/// Linearizable CAS: among N tasks all attempting compare_and_set(0 -> their id),
/// EXACTLY ONE must win. This is the no-double-spend / single-writer guarantee a
/// CBDC optimistic balance update relies on.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the dev cluster CP subsystem"]
async fn test_atomic_long_concurrent_cas_exactly_one_winner() {
    let client = Arc::new(
        HazelcastClient::new(common::default_config())
            .await
            .expect("connect"),
    );

    let name = common::unique_name("xc_along_cas");
    client.get_atomic_long(&name).set(0).await.expect("set 0");

    const CONTENDERS: i64 = 20;
    let winners = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();
    for id in 1..=CONTENDERS {
        let c = Arc::clone(&client);
        let n = name.clone();
        let w = Arc::clone(&winners);
        handles.push(tokio::spawn(async move {
            let along = c.get_atomic_long(&n);
            if along.compare_and_set(0, id).await.expect("cas") {
                w.fetch_add(1, Ordering::SeqCst);
            }
        }));
    }
    for h in handles {
        h.await.expect("join task");
    }

    assert_eq!(
        winners.load(Ordering::SeqCst),
        1,
        "exactly one concurrent CAS(0 -> id) must win (linearizable single-writer)"
    );
    let final_v = client.get_atomic_long(&name).get().await.expect("final get");
    assert!(
        (1..=CONTENDERS).contains(&final_v),
        "final value must be the winning contender's id, got {final_v}"
    );
    client.shutdown().await.ok();
}
