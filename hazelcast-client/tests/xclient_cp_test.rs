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

//! Cross-client value-fidelity check for CP structures (cbdc).
//!
//! CPMap (the strongly-consistent map a CBDC would use for ledger
//! metadata/config) stores `Data` values like AtomicReference did, so it is a
//! prime candidate for the same cross-client Data-header bug — pass 2 only
//! verified a same-client round-trip. A stock Java client (XCp.java) writes a
//! CPMap value + an AtomicLong; the Rust client must read the same back.
//! Tolerant of a clean cluster restart (CP state cleared).

mod common;

use hazelcast_client::HazelcastClient;

#[tokio::test]
#[ignore = "requires the dev cluster + Java-written CP values (XCp.java)"]
async fn test_cp_read_java_written_values() {
    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("connect to dev cluster");

    // CPMap (linearizable map; stores Data values).
    let cpmap = client.get_cp_map::<String, String>("xcpmap");
    match cpmap.get(&"k".to_string()).await.expect("cpmap get") {
        Some(v) => assert_eq!(v, "cpmap-java", "CPMap cross-client value corrupted"),
        None => {
            eprintln!("NOTE: xcpmap empty — run `XCp` on the instance for the full check");
            client.shutdown().await.ok();
            return;
        }
    }

    // AtomicLong (raw i64, no Data header — sanity).
    let along = client.get_atomic_long("xclong");
    let n = along.get().await.expect("atomiclong get");
    assert_eq!(n, 4242, "AtomicLong cross-client value corrupted");

    client.shutdown().await.ok();
}
