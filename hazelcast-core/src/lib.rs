//! Core types and protocols for Hazelcast.

#![warn(missing_docs)]

pub mod error;
pub mod protocol;
pub mod serialization;

pub use error::{HazelcastError, Result};
pub use protocol::{compute_partition_hash, ClientMessage, ClientMessageCodec, Frame};
pub use serialization::{
    ClassDefinition, DataInput, DataOutput, DefaultPortableReader, DefaultPortableWriter,
    Deserializable, FieldDefinition, FieldType, ObjectDataInput, ObjectDataOutput, Portable,
    PortableFactory, PortableReader, PortableSerializer, PortableWriter, Serializable,
    PORTABLE_TYPE_ID,
};

#[cfg(feature = "serde")]
pub use serialization::Serde;
