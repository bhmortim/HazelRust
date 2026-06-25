//! Live data-effect test for the ringbuffer `read_many` ReadResultSet reframe (cbdc).
//!
//! The old `read_many` read every trailing frame and relied on `decode_value_at`
//! FAILING on the `itemSeqs` long[] frame to skip it. For a fixed-width element
//! type (e.g. `i64`) that decode does NOT fail, so the sequence array was injected
//! as a bogus extra item — a silent corruption. The existing String test only
//! asserts `len() >= 3`, so it never caught this. This asserts the EXACT count and
//! values for `i64` items.

mod common;

use hazelcast_client::HazelcastClient;

#[tokio::test]
#[ignore = "requires a live Hazelcast cluster (CLUSTER_ADDRESS=127.0.0.1:5701)"]
async fn test_ringbuffer_read_many_numeric_exact_count() {
    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("connect to dev cluster");

    let rb = client.get_ringbuffer::<i64>(&common::unique_name("rb_numeric"));
    let start = rb.tail_sequence().await.expect("tail") + 1;

    for i in 0..5i64 {
        rb.add(1000 + i).await.expect("add");
    }

    let (items, _next) = rb.read_many(start, 0, 100).await.expect("read_many");

    assert_eq!(
        items.len(),
        5,
        "read_many must return exactly the 5 added items — not the trailing \
         itemSeqs long[] decoded as a 6th i64. got {items:?}"
    );
    for (idx, v) in items.iter().enumerate() {
        assert_eq!(*v, 1000 + idx as i64, "item {idx} value mismatch");
    }

    client.shutdown().await.ok();
}
