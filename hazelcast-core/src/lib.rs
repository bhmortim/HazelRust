//! Core types and protocols for Hazelcast.

#![warn(missing_docs)]

pub mod error;
pub mod serialization;

pub use error::{HazelcastError, Result};
pub use serialization::{
    DataInput, DataOutput, Deserializable, ObjectDataInput, ObjectDataOutput, Serializable,
};
