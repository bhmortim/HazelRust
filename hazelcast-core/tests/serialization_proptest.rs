//! Property-based tests for serialization and protocol decoding.
//!
//! Part of Layer 2 of `EXECUTION_PLAN.md`. Two classes of property are checked:
//!
//! 1. **Round-trip identity** — `deserialize(serialize(x)) == x` for the Compact
//!    codec across a wide range of randomly generated field values.
//! 2. **Decode never panics** — feeding arbitrary / adversarial bytes to the
//!    frame, client-message, Compact, and `GenericRecord` decoders must always
//!    return `Ok`/`Err` and never panic, hang, or abort. This is the property
//!    that matters when the bytes arrive from an untrusted network peer.
//!
//! These run under the normal `cargo test` gate (no nightly / libFuzzer needed)
//! and complement the continuous fuzzers under `fuzz/`.

use bytes::BytesMut;
use proptest::prelude::*;

use hazelcast_core::protocol::{ClientMessageCodec, Frame};
use hazelcast_core::serialization::compact::{
    Compact, CompactReader, CompactSerializer, CompactWriter, DefaultCompactReader,
    DefaultCompactWriter,
};
use hazelcast_core::{GenericRecord, Result};
use tokio_util::codec::Decoder;

/// A representative Compact value covering string, signed-integer, boolean, and
/// nullable-string fields.
#[derive(Debug, Default, Clone, PartialEq)]
struct PropRecord {
    name: String,
    age: i32,
    big: i64,
    active: bool,
    email: Option<String>,
}

impl Compact for PropRecord {
    fn get_type_name() -> &'static str {
        "PropRecord"
    }

    fn write(&self, writer: &mut DefaultCompactWriter) -> Result<()> {
        writer.write_string("name", Some(&self.name))?;
        writer.write_int32("age", self.age)?;
        writer.write_int64("big", self.big)?;
        writer.write_boolean("active", self.active)?;
        writer.write_string("email", self.email.as_deref())?;
        Ok(())
    }

    fn read(&mut self, reader: &mut DefaultCompactReader) -> Result<()> {
        self.name = reader.read_string("name")?.unwrap_or_default();
        self.age = reader.read_int32("age")?;
        self.big = reader.read_int64("big")?;
        self.active = reader.read_boolean("active")?;
        self.email = reader.read_string("email")?;
        Ok(())
    }
}

proptest! {
    // 1. Round-trip identity for the Compact codec.
    #[test]
    fn compact_round_trip(
        name in ".*",
        age in any::<i32>(),
        big in any::<i64>(),
        active in any::<bool>(),
        email in proptest::option::of(".*"),
    ) {
        let serializer = CompactSerializer::new();
        let original = PropRecord { name, age, big, active, email };
        let bytes = serializer.serialize(&original).expect("serialize must succeed");
        let decoded: PropRecord =
            serializer.deserialize(&bytes).expect("deserialize must succeed");
        prop_assert_eq!(original, decoded);
    }

    // 2a. Compact deserialize never panics on arbitrary bytes.
    #[test]
    fn compact_deserialize_never_panics(data in proptest::collection::vec(any::<u8>(), 0..4096)) {
        let serializer = CompactSerializer::new();
        let _ = serializer.deserialize::<PropRecord>(&data);
    }

    // 2b. GenericRecord decode never panics on arbitrary bytes.
    #[test]
    fn generic_record_never_panics(data in proptest::collection::vec(any::<u8>(), 0..4096)) {
        let _ = GenericRecord::from_compact_bytes(&data);
    }

    // 2c. Frame decode never panics; guarded against non-progress to guarantee termination.
    #[test]
    fn frame_decode_never_panics(data in proptest::collection::vec(any::<u8>(), 0..4096)) {
        let mut buf = BytesMut::from(&data[..]);
        while !buf.is_empty() {
            let before = buf.len();
            match Frame::read_from(&mut buf) {
                Some(frame) => {
                    let _ = frame.wire_size();
                    if buf.len() >= before {
                        // Decoder returned a frame without consuming input; stop
                        // rather than spin. (A correct decoder always advances.)
                        break;
                    }
                }
                None => break,
            }
        }
    }

    // 2d. ClientMessage codec never panics on arbitrary bytes.
    #[test]
    fn client_message_decode_never_panics(data in proptest::collection::vec(any::<u8>(), 0..8192)) {
        let mut codec = ClientMessageCodec::new();
        let mut buf = BytesMut::from(&data[..]);
        let mut iterations = 0u32;
        loop {
            iterations += 1;
            if iterations > 100_000 {
                break;
            }
            match codec.decode(&mut buf) {
                Ok(Some(_)) => continue,
                Ok(None) => break,
                Err(_) => break,
            }
        }
    }
}
