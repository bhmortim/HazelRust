//! Cross-client NUMERIC/binary value-fidelity check (cbdc).
//!
//! Pass 8's cross-client sweep only covered String values; the CBDC money path
//! uses i64 balances (and decimals/bytes). This verifies the Data type_id + byte
//! order are wire-compatible with a stock Java client BOTH directions:
//!   - Java writes typed Long/Integer/Double/byte[] -> Rust reads them back.
//!   - Rust writes typed i64/i32/f64/Vec<u8> -> the Java reader (XNumRead) asserts
//!     each `instanceof Long/Integer/Double/byte[]` (proving Rust emits the correct
//!     type_id, e.g. -8 LONG, not -11 STRING) and the exact value.
//! Run order on the instance: XNum (java write) -> these tests -> XNumRead (java
//! verify Rust's writes). Each per-type map is separate (the typed Rust API binds
//! one value type per map).

mod common;

use hazelcast_client::HazelcastClient;

const LONG_V: i64 = 9_223_372_036_854_775_123; // near i64::MAX — a large balance
const INT_V: i32 = 1_234_567_890;
const DOUBLE_V: f64 = 3.141_592_653_589_793;
fn bytes_v() -> Vec<u8> {
    vec![1u8, 2, 3, 254, 255]
}

#[tokio::test]
#[ignore = "requires the dev cluster + Java-written numerics (XNum)"]
async fn test_read_java_numerics() {
    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("connect");

    let ml = client.get_map::<String, i64>("xc_num_java_long");
    match ml.get(&"v".to_string()).await.expect("get long") {
        Some(v) => assert_eq!(v, LONG_V, "i64 cross-client value corrupted"),
        None => {
            eprintln!("NOTE: xc_num_java_long empty — run `XNum` on the instance first");
            client.shutdown().await.ok();
            return;
        }
    }
    let mi = client.get_map::<String, i32>("xc_num_java_int");
    assert_eq!(
        mi.get(&"v".to_string()).await.expect("get int"),
        Some(INT_V),
        "i32 cross-client value corrupted"
    );
    let md = client.get_map::<String, f64>("xc_num_java_double");
    assert_eq!(
        md.get(&"v".to_string()).await.expect("get double"),
        Some(DOUBLE_V),
        "f64 cross-client value corrupted"
    );
    let mb = client.get_map::<String, Vec<u8>>("xc_num_java_bytes");
    assert_eq!(
        mb.get(&"v".to_string()).await.expect("get bytes"),
        Some(bytes_v()),
        "byte[] cross-client value corrupted"
    );
    client.shutdown().await.ok();
}

#[tokio::test]
#[ignore = "requires the dev cluster; XNumRead (java) asserts the written values"]
async fn test_write_numerics_for_java() {
    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("connect");

    client
        .get_map::<String, i64>("xc_num_rust_long")
        .put("v".to_string(), LONG_V)
        .await
        .expect("put long");
    client
        .get_map::<String, i32>("xc_num_rust_int")
        .put("v".to_string(), INT_V)
        .await
        .expect("put int");
    client
        .get_map::<String, f64>("xc_num_rust_double")
        .put("v".to_string(), DOUBLE_V)
        .await
        .expect("put double");
    client
        .get_map::<String, Vec<u8>>("xc_num_rust_bytes")
        .put("v".to_string(), bytes_v())
        .await
        .expect("put bytes");

    eprintln!("RUST_WROTE_NUM");
    client.shutdown().await.ok();
}
