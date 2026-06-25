//! Cross-client Data-header verification (cbdc).
//!
//! ICache and AtomicReference `serialize_value` write a bare payload with **no**
//! 8-byte `[partition_hash][type_id]` Data header, and decode from offset 0 —
//! self-consistent for a Rust↔Rust round-trip but not a valid server-side
//! `Data`, so a value written by a Java client cannot be read by the Rust client
//! (and vice versa). This proves it: a Java client (XClient.java on the
//! instance) writes a CP AtomicReference value, and the Rust client must read
//! the same string. Before the fix the Rust decode reads the header bytes as the
//! string payload → wrong/garbled/Err; after the fix (skip 8) it reads the real
//! value.
//!
//! Setup (on the instance): `java ... XClient write xref hello-from-java`.

mod common;

use hazelcast_client::HazelcastClient;

#[tokio::test]
#[ignore = "requires the dev cluster + a Java-written AtomicReference 'xref' (see XClient.java)"]
async fn test_atomicref_reads_java_written_value() {
    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("connect to dev cluster");

    let reference = client.get_atomic_reference::<String>("xref");
    let v = reference.get().await.expect("get AtomicReference 'xref'");

    assert_eq!(
        v.as_deref(),
        Some("hello-from-java"),
        "Rust must read the exact value a Java client wrote (cross-client Data \
         framing); a wrong/garbled value means the 8-byte Data header was not \
         skipped. got {v:?}"
    );

    // Encode direction: write a value Rust-side so a Java client can read it back
    // (verified by `XClient read rref` on the instance). Also a Rust round-trip.
    let rust_ref = client.get_atomic_reference::<String>("rref");
    rust_ref
        .set(Some("hello-from-rust".to_string()))
        .await
        .expect("set rref");
    let back = rust_ref.get().await.expect("get rref");
    assert_eq!(
        back.as_deref(),
        Some("hello-from-rust"),
        "Rust AtomicReference round-trip must hold after the header fix"
    );

    // NB: ICache shares the identical `serialize_value` header fix, but its
    // round-trip cannot be exercised here because ICache `put` has a *separate*
    // request-framing bug (the server throws ArrayIndexOutOfBoundsException
    // decoding the put request) — tracked open. The header fix is correct by
    // parity with AtomicReference (proven cross-client above) and IMap.

    client.shutdown().await.ok();
}
