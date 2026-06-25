//! Distributed-correctness: AtomicLong exact count across a member loss (cbdc).
//!
//! A CBDC balance counter must not lose or double-apply increments when a cluster
//! member dies. This increments a CP AtomicLong 50 times, idles for a window
//! during which external orchestration kills a member (docker stop hz2), then
//! increments 50 more and asserts the total is EXACTLY 100.
//!
//! Why this is deterministic: the kill happens while the client is idle (no
//! in-flight op), so a post-kill increment fails at connect/route time (the op did
//! NOT reach the server) — making a test-level retry exactly-once-safe. The client
//! does not auto-retry non-idempotent CP ops, so it never blindly double-applies.

mod common;

use std::time::Duration;

use hazelcast_client::HazelcastClient;

#[tokio::test]
#[ignore = "requires the 3-member dev cluster + external member kill during the idle window"]
async fn test_atomic_long_exact_count_under_member_failover() {
    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("connect");

    let along = client.get_atomic_long("xc_along_failover");
    along.set(0).await.expect("set 0");

    for _ in 0..50 {
        along
            .increment_and_get()
            .await
            .expect("first-half increment");
    }
    eprintln!(
        "FAILOVER_FIRST_50_DONE={}",
        along.get().await.expect("mid get")
    );

    // Idle window: external orchestration kills a member here (no in-flight op).
    tokio::time::sleep(Duration::from_secs(15)).await;

    // Resume. On a post-kill failure (connect/route error → op did not apply), a
    // test-level retry is exactly-once-safe; bound total recovery time.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(45);
    for i in 0..50 {
        loop {
            match along.increment_and_get().await {
                Ok(_) => break,
                Err(e) => {
                    assert!(
                        tokio::time::Instant::now() < deadline,
                        "increment {i} never recovered after the member loss: {e}"
                    );
                    eprintln!("failover retry on inc {i}: {e}");
                    tokio::time::sleep(Duration::from_millis(300)).await;
                }
            }
        }
    }

    let total = along.get().await.expect("final get");
    eprintln!("FAILOVER_TOTAL={total}");
    assert_eq!(
        total, 100,
        "AtomicLong must be EXACT (no lost or double-applied increments) across a member loss"
    );
    client.shutdown().await.ok();
}
