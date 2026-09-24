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

//! Live data-effect test for the paging-predicate fix (cbdc).
//!
//! Paging was fully broken: the request sent the predicate as a single `Data`
//! blob, but 5.x expects a `PagingPredicateHolder` structured codec (the server
//! threw NullPointerException, "initialFrame is null"). With the holder encoding
//! the query now works, which also exercises the (previously dead) page-value
//! decode skip-8 fix.

mod common;

use hazelcast_client::query::PagingPredicate;
use hazelcast_client::HazelcastClient;

#[tokio::test]
#[ignore = "requires a live Hazelcast cluster (CLUSTER_ADDRESS=127.0.0.1:5701)"]
async fn test_paging_predicate_values_decode_correctly() {
    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("connect to dev cluster");

    let map = client.get_map::<String, String>(&common::unique_name("paging_de"));
    for i in 0..15 {
        map.put(format!("k{i:02}"), format!("val-{i:02}"))
            .await
            .expect("put");
    }

    let mut paging: PagingPredicate<String, String> = PagingPredicate::new(10);
    let page0 = map
        .values_with_paging_predicate(&mut paging)
        .await
        .expect("first page (paging request must be a valid PagingPredicateHolder)");

    assert_eq!(
        page0.data.len(),
        10,
        "first page must contain page_size (10) values, got {}",
        page0.data.len()
    );
    for v in &page0.data {
        assert!(
            v.starts_with("val-"),
            "paged value must decode to the real stored string, got {v:?}"
        );
    }
    let distinct: std::collections::HashSet<_> = page0.data.iter().collect();
    assert_eq!(distinct.len(), 10, "paged values must be distinct");

    // keys_with_paging_predicate (List<Data> of keys).
    let mut paging_k: PagingPredicate<String, String> = PagingPredicate::new(10);
    let keys0 = map
        .keys_with_paging_predicate(&mut paging_k)
        .await
        .expect("keys page");
    assert_eq!(keys0.data.len(), 10, "keys page must contain 10 keys");
    for k in &keys0.data {
        assert!(k.starts_with('k'), "paged key must decode correctly: {k:?}");
    }

    // entries_with_paging_predicate (EntryList<Data,Data>).
    let mut paging_e: PagingPredicate<String, String> = PagingPredicate::new(10);
    let entries0 = map
        .entries_with_paging_predicate(&mut paging_e)
        .await
        .expect("entries page");
    assert_eq!(
        entries0.data.len(),
        10,
        "entries page must contain 10 entries"
    );
    for (k, v) in &entries0.data {
        assert!(
            k.starts_with('k') && v.starts_with("val-"),
            "paged entry must decode correctly: {k:?}={v:?}"
        );
    }

    map.clear().await.ok();
    client.shutdown().await.ok();
}
