/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
