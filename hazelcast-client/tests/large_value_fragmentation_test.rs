//! Verify-first probe for client message fragmentation (cbdc).
//!
//! Hazelcast fragments large ClientMessages into multiple fragments (each with a
//! fragmentation frame) that the client must reassemble by fragmentation id. If
//! reassembly is missing, a large value round-trip corrupts or fails. This puts
//! values of increasing size through IMap and asserts an exact round-trip.

mod common;

use hazelcast_client::HazelcastClient;

async fn roundtrip_of_size(client: &HazelcastClient, n: usize) {
    let map = client.get_map::<String, String>(&common::unique_name("frag"));
    // A non-trivial, position-dependent payload so any mis-framing corrupts it.
    let value: String = (0..n).map(|i| (b'A' + (i % 26) as u8) as char).collect();
    let key = format!("big-{n}");
    map.put(key.clone(), value.clone())
        .await
        .unwrap_or_else(|e| panic!("put of {n}-byte value failed: {e}"));
    let got = map
        .get(&key)
        .await
        .unwrap_or_else(|e| panic!("get of {n}-byte value failed: {e}"))
        .unwrap_or_else(|| panic!("get of {n}-byte value returned None"));
    assert_eq!(
        got.len(),
        value.len(),
        "{n}-byte value: length mismatch on round-trip"
    );
    assert_eq!(
        got, value,
        "{n}-byte value: content corrupted on round-trip"
    );
}

#[tokio::test]
#[ignore = "requires the dev cluster"]
async fn test_large_value_roundtrip_fragmentation() {
    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("connect");

    // Span below and above typical fragmentation thresholds (tens of KB to several MB).
    for n in [16 * 1024, 256 * 1024, 1024 * 1024, 4 * 1024 * 1024] {
        roundtrip_of_size(&client, n).await;
        eprintln!("[frag] {n}-byte value round-trip OK");
    }

    client.shutdown().await.ok();
}
