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
//!    your types. Enable with `hazelcast-client-core = { features = ["serde"] }`.
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
// See note in hazelcast-client/src/lib.rs: complex generic signatures in the
// serialization/protocol layer; aliasing tracked separately.
#![allow(clippy::type_complexity)]
#![allow(clippy::multiple_bound_locations)]
// Test fixtures use literals like 3.14 / 2.718 purely as sample float values, not as
// PI/E. `approx_constant` is deny-by-default; allow it in test builds only so the
// clippy --all-targets gate is green without masking the lint in production code.
#![cfg_attr(test, allow(clippy::approx_constant))]

pub mod error;
pub mod partition_aware;
pub mod protocol;
pub mod serialization;

pub use error::{ErrorCategory, HazelcastError, Result, ServerErrorCode};
pub use partition_aware::PartitionAware;
pub use protocol::{
    compute_partition_hash, partition_id_for_hash, partition_id_for_key_data, ClientMessage,
    ClientMessageCodec, Frame,
};
pub use serialization::{
    ClassDefinition, Compact, CompactReader, CompactSerializer, CompactWriter, CustomSerializer,
    DataInput, DataOutput, DefaultCompactReader, DefaultCompactWriter, DefaultPortableReader,
    DefaultPortableWriter, Deserializable, FieldDefinition, FieldDescriptor, FieldKind, FieldType,
    GenericRecord, GenericRecordBuilder, GlobalSerializer, ObjectDataInput, ObjectDataOutput,
    Portable, PortableFactory, PortableReader, PortableSerializer, PortableWriter, Schema,
    Serializable, SerializationConfig, COMPACT_TYPE_ID, PORTABLE_TYPE_ID,
};

#[cfg(feature = "serde")]
pub use serialization::Serde;

#[cfg(feature = "derive")]
pub use hazelcast_client_derive::{
    HazelcastCompact, HazelcastPortable, IdentifiedDataSerializable,
};
