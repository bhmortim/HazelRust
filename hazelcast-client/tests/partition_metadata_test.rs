//! Regression test for cbdc lead #1 — the client must parse `partition_count`
//! from the ClientAuthentication response (fixed-block offset 32), not silently
//! leave it at 0.
//!
//! Before the fix `connect_to` read the count at offset 30 (treating the member
//! UUID as 16 bytes and omitting its 1-byte not-null flag), producing a garbage
//! value that the range check rejected — so `partition_count` stayed 0 and every
//! partition-aware caller fell back to the hard-coded 271 default (and
//! `get_partition` collapsed every key onto partition 0). Verified live by a
//! hexdump of a real EE 5.7 auth response: partitionCount = 0x0000010F = 271 at
//! offset 32. Production never populates the partition table (no partition-table
//! fetch/listener exists), so the auth parse is the *only* source of
//! `partition_count`; this test reads 0 if the offset regresses.

mod common;

use hazelcast_client::HazelcastClient;

#[tokio::test]
#[ignore = "requires a live Hazelcast cluster (CLUSTER_ADDRESS=127.0.0.1:5701)"]
async fn test_partition_count_parsed_from_auth_response() {
    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("connect to dev cluster");

    let count = client.partition_service().get_partition_count();

    assert_eq!(
        count, 271,
        "partition_count must be parsed from the auth response (offset 32); got \
         {count} (0 means the parse was rejected — the offset regressed)"
    );

    client.shutdown().await.ok();
}
