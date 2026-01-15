//! Serialization framework for Hazelcast's binary format.

mod data_input;
mod data_output;
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
pub use portable::{
    ClassDefinition, DefaultPortableReader, DefaultPortableWriter, FieldDefinition, FieldType,
    Portable, PortableFactory, PortableReader, PortableSerializer, PortableWriter,
    PORTABLE_TYPE_ID,
};
pub use traits::{Deserializable, Serializable};

#[cfg(feature = "serde")]
pub use self::serde::Serde;
