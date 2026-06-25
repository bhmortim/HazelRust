//! Cross-client CPMap (linearizable map) write/CAS fidelity (cbdc).
//!
//! Pass 8 verified CPMap *read* (Rust read a Java-written value). A CBDC ledger
//! using the strongly-consistent map relies on its put/compareAndSet. This runs
//! the linearizable money ops from Rust (set -> put-returns-old -> CAS) on an i64
//! balance, then the Java reader (XCpMoney) asserts the result is a Long(1500) —
//! proving CPMap request framing + value type_id are correct and the linearizable
//! write is visible cross-client.

mod common;

use hazelcast_client::HazelcastClient;

#[tokio::test]
#[ignore = "requires the dev cluster CP subsystem; XCpMoney (java) asserts the result"]
async fn test_cpmap_money_ops_cross_client() {
    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("connect");

    let cpmap = client.get_cp_map::<String, i64>("xcpmap_money");

    cpmap.set("acct-1".to_string(), 1000).await.expect("set");
    let old = cpmap.put("acct-1".to_string(), 1200).await.expect("put");
    assert_eq!(old, Some(1000), "put must return the previous balance");
    let cas = cpmap
        .compare_and_set(&"acct-1".to_string(), &1200, 1500)
        .await
        .expect("compare_and_set");
    assert!(cas, "linearizable CAS(1200 -> 1500) must succeed");
    assert_eq!(
        cpmap.get(&"acct-1".to_string()).await.expect("get"),
        Some(1500),
        "balance after CAS must be 1500"
    );

    eprintln!("RUST_CPMAP_1500");
    client.shutdown().await.ok();
}
