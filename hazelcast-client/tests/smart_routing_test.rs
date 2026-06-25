//! Live verification for the smart-routing fix (cbdc): the client must register
//! the `ClientAddClusterViewListener` and decode the streamed members-view /
//! partitions-view events to populate the member map + partition table in
//! production.
//!
//! Before the fix, `update_partition_table`/`set_partition_owner` and
//! `set_initial_members` had only `#[cfg(test)]` callers, so production left both
//! maps empty and every partition op fell back to `addresses[0]` (smart routing a
//! no-op, `PartitionService::get_partition_owner` always `None`). This test fails
//! (every partition owner `None`, distinct-owner count 0) without the listener.
//!
//! Run against the 3-member dev cluster:
//!   CLUSTER_ADDRESS=127.0.0.1:5701 cargo nextest run -p hazelcast-client \
//!     --run-ignored all -E 'test(test_partition_table_populated_from_cluster_view)'

mod common;

use std::collections::HashSet;
use std::time::Duration;

use hazelcast_client::HazelcastClient;

#[tokio::test]
#[ignore = "requires the live 3-member Hazelcast dev cluster (CLUSTER_ADDRESS=127.0.0.1:5701)"]
async fn test_partition_table_populated_from_cluster_view() {
    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("connect to dev cluster");

    let ps = client.partition_service();

    // partition_count comes from the auth response and is available immediately.
    assert_eq!(
        ps.get_partition_count(),
        271,
        "partition_count must be 271 (parsed from auth response)"
    );

    // The members-view / partitions-view events arrive asynchronously after the
    // listener registers; poll until every partition has a known owner (or time out).
    let mut partitions = ps.get_partitions().await;
    let mut owned = 0usize;
    for _ in 0..50 {
        owned = partitions
            .iter()
            .filter(|p| p.owner_uuid().is_some())
            .count();
        if owned == 271 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        partitions = ps.get_partitions().await;
    }

    assert_eq!(partitions.len(), 271, "expected 271 partitions");
    assert_eq!(
        owned, 271,
        "every partition must have a known owner after the cluster-view events \
         (got {owned}/271 — 0 means the listener never populated the table)"
    );

    // A 3-member CP-enabled dev cluster distributes the 271 partitions across all
    // three members; the distinct owner-uuid count proves we decoded real member
    // uuids (not a single bogus owner).
    let owners: HashSet<_> = partitions.iter().filter_map(|p| p.owner_uuid()).collect();
    assert_eq!(
        owners.len(),
        3,
        "271 partitions must be owned across 3 distinct members; got {} distinct owners",
        owners.len()
    );

    // Each owner uuid must resolve to a Member with an address (members-view
    // populated and the partition→address cache can route).
    for p in partitions.iter().take(8) {
        let owner = ps.get_partition_owner(p.id()).await;
        let owner =
            owner.unwrap_or_else(|| panic!("partition {} owner must resolve to a Member", p.id()));
        let addr = owner.address();
        assert!(
            addr.port() >= 5701 && addr.port() <= 5703,
            "owner address {addr} must be one of the dev members (5701-5703)"
        );
    }

    // Smart routing must not break data correctness: a put/get round-trip that is
    // now routed to the partition owner still returns the value.
    let map = client.get_map::<String, String>("smart_routing_round_trip");
    map.put("k-route".to_string(), "v-route".to_string())
        .await
        .expect("put");
    let got = map.get(&"k-route".to_string()).await.expect("get");
    assert_eq!(
        got.as_deref(),
        Some("v-route"),
        "routed put/get must round-trip"
    );
    map.remove(&"k-route".to_string()).await.ok();

    client.shutdown().await.ok();
}

/// Proves partition-aware routing engages: a burst of puts across many keys must
/// spread across all three members (not collapse onto `addresses[0]` = 5701).
/// Run under `HZ_DEBUG_ROUTING=1` to see each routing decision, and under
/// `tcpdump` to confirm MAP_PUT packets reach all three member ports.
#[tokio::test]
#[ignore = "requires the live 3-member dev cluster (members 5701/5702/5703)"]
async fn test_routing_spreads_across_partition_owners() {
    // Connect to all three members (a production smart client is configured with
    // every member, or discovers them) so partition ops can route directly to
    // their owners rather than warming up the non-seed connections in the
    // background.
    let client = HazelcastClient::new(common::multi_member_config())
        .await
        .expect("connect to dev cluster");

    let ps = client.partition_service();
    for _ in 0..50 {
        let parts = ps.get_partitions().await;
        if parts.iter().filter(|p| p.owner_uuid().is_some()).count() == 271 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let map = client.get_map::<String, String>("routing_spread");
    let mut owner_ports: std::collections::HashMap<u16, usize> = std::collections::HashMap::new();
    for i in 0..400 {
        let k = format!("spread-{i}");
        let part = ps.get_partition(&k).await;
        if let Some(owner) = ps.get_partition_owner(part.id()).await {
            *owner_ports.entry(owner.address().port()).or_default() += 1;
        }
        map.put(k.clone(), "v".to_string()).await.expect("put");
        let got = map.get(&k).await.expect("get");
        assert_eq!(
            got.as_deref(),
            Some("v"),
            "routed put/get must round-trip for {k}"
        );
    }
    eprintln!("ROUTING_SPREAD owner_port_distribution={owner_ports:?}");

    assert!(
        owner_ports.len() >= 3,
        "keys must route across all 3 members; got distribution {owner_ports:?} \
         (a single entry means routing collapsed onto addresses[0])"
    );
    assert!(
        owner_ports.keys().any(|&p| p != 5701),
        "some keys must be owned by (and routed to) a non-5701 member; got {owner_ports:?}"
    );

    // clean up
    for i in 0..400 {
        map.remove(&format!("spread-{i}")).await.ok();
    }
    client.shutdown().await.ok();
}
