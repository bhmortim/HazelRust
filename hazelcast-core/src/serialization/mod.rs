//! Serialization framework for Hazelcast's binary format.

mod data_input;
mod data_output;
mod identified;
mod json;
pub mod compact;
pub mod portable;
mod traits;

#[cfg(feature = "serde")]
mod serde;

pub use compact::{
    Compact, CompactReader, CompactSerializer, CompactWriter, DefaultCompactReader,
    DefaultCompactWriter, DefaultFieldValue, FieldDescriptor, FieldKind, Schema,
    SchemaEvolutionResult, SchemaEvolutionValidator, SchemaRegistry, COMPACT_TYPE_ID,
};
pub use data_input::{DataInput, ObjectDataInput};
pub use data_output::{DataOutput, ObjectDataOutput};
pub use identified::{
    DataSerializableFactory, FactoryRegistry, IdentifiedDataSerializable,
    IDENTIFIED_DATA_SERIALIZABLE_TYPE_ID,
};
pub use portable::{
    ClassDefinition, DefaultPortableReader, DefaultPortableWriter, FieldDefinition, FieldType,
    Portable, PortableFactory, PortableReader, PortableSerializer, PortableWriter,
    PORTABLE_TYPE_ID,
};
pub use json::{HazelcastJsonValue, JSON_TYPE_ID};
pub use traits::{Deserializable, Serializable};

#[cfg(feature = "serde")]
pub use self::serde::Serde;
