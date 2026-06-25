//! Cross-client AtomicLong INCREMENT/CAS fidelity (cbdc).
//!
//! Pass 8 verified AtomicLong *read* (Rust read a Java-written value). This
//! verifies the actual money operation: an atomic balance increment + CAS. Rust
//! runs set -> add_and_get -> compare_and_set against a CP AtomicLong; the Java
//! reader (XAlong) asserts the resulting value, proving the delta/CAS request
//! framing is correct and the CP write is visible cross-client (refuting the
//! audit's "AtomicLong delta sent as a separate frame -> applies 0" claim).

mod common;

use hazelcast_client::HazelcastClient;

#[tokio::test]
#[ignore = "requires the dev cluster CP subsystem; XAlong (java) asserts the result"]
async fn test_atomic_long_money_ops_cross_client() {
    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("connect");

    let along = client.get_atomic_long("xc_along_money");

    along.set(7000).await.expect("set");
    // The core money op: atomic increment returning the new balance.
    let after_add = along.add_and_get(370).await.expect("add_and_get");
    assert_eq!(
        after_add, 7370,
        "add_and_get must apply the delta and return 7370 (delta framing)"
    );
    // Atomic conditional update (optimistic balance update).
    let cas = along
        .compare_and_set(7370, 8000)
        .await
        .expect("compare_and_set");
    assert!(cas, "compare_and_set(7370 -> 8000) must succeed");
    assert_eq!(along.get().await.expect("get"), 8000, "value after CAS must be 8000");

    eprintln!("RUST_ALONG_8000");
    client.shutdown().await.ok();
}
