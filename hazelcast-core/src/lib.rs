//! Core types and wire protocol for the Hazelcast Rust client.
//!
//! This crate implements the low-level building blocks that
//! [`hazelcast-client`](https://crates.io/crates/hazelcast-client) depends on.
//! Most users should depend on `hazelcast-client` directly — this crate is useful
//! if you need to work with the wire protocol, serialization formats, or error
//! types without pulling in the full client.
//!
//! # Modules
//!
//! | Module | Purpose |
//! |--------|---------|
//! | [`protocol`] | Hazelcast Open Binary Protocol — [`Frame`], [`ClientMessage`], and codec |
//! | [`serialization`] | Portable, Compact, and serde-based serialization |
//! | [`error`] | [`HazelcastError`], [`ServerErrorCode`], and the crate-wide [`Result`] alias |
//!
//! # Wire Protocol
//!
//! Hazelcast uses a frame-based binary protocol. Each [`ClientMessage`] is a
//! sequence of [`Frame`]s, where each frame carries a length, flags, and a byte
//! payload. This crate provides:
//!
//! - **[`Frame`]** — a single protocol frame with read/write helpers for
//!   primitive types (`i32`, `i64`, `bool`).
//! - **[`ClientMessage`]** — an ordered collection of frames representing a
//!   request or response, with correlation ID and message type.
//! - **[`ClientMessageCodec`]** — a Tokio codec for framing TCP streams.
//!
//! # Serialization
//!
//! Three serialization strategies are supported:
//!
//! 1. **Portable** — schema-based serialization compatible with Java/Python/.NET
//!    clients. Implement the [`Portable`] trait and register a [`PortableFactory`].
//! 2. **Compact** — the newer, schema-less serialization format. Implement the
//!    [`Compact`] trait and provide a [`CompactSerializer`].
//! 3. **serde** (feature `serde`) — use `#[derive(Serialize, Deserialize)]` on
//!    your types. Enable with `hazelcast-core = { features = ["serde"] }`.
//!
//! Low-level byte I/O is exposed through [`ObjectDataInput`] / [`ObjectDataOutput`]
//! and the [`Serializable`] / [`Deserializable`] traits.
//!
//! # Error Handling
//!
//! All fallible operations return [`Result<T>`](Result), which is an alias for
//! `std::result::Result<T, HazelcastError>`. Server-side exceptions are mapped to
//! [`ServerErrorCode`] variants that mirror the Java Hazelcast exception hierarchy.

#![warn(missing_docs)]

pub mod error;
pub mod partition_aware;
pub mod protocol;
pub mod serialization;

pub use error::{HazelcastError, Result, ServerErrorCode};
pub use partition_aware::PartitionAware;
pub use protocol::{compute_partition_hash, ClientMessage, ClientMessageCodec, Frame};
pub use serialization::{
    ClassDefinition, Compact, CompactReader, CompactSerializer, CompactWriter, CustomSerializer,
    DataInput, DataOutput, DefaultCompactReader, DefaultCompactWriter, DefaultPortableReader,
    DefaultPortableWriter, Deserializable, FieldDefinition, FieldDescriptor, FieldKind, FieldType,
    GenericRecord, GenericRecordBuilder, ObjectDataInput, ObjectDataOutput, Portable,
    PortableFactory, PortableReader, PortableSerializer, PortableWriter, Schema, Serializable,
    SerializationConfig, COMPACT_TYPE_ID, PORTABLE_TYPE_ID,
};

#[cfg(feature = "serde")]
pub use serialization::Serde;
