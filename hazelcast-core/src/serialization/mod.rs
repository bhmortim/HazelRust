//! Serialization framework for Hazelcast's binary format.

mod data_input;
mod data_output;
pub mod portable;
mod traits;

#[cfg(feature = "serde")]
mod serde;

pub use data_input::{DataInput, ObjectDataInput};
pub use data_output::{DataOutput, ObjectDataOutput};
pub use portable::{
    ClassDefinition, FieldDefinition, FieldType, Portable, PortableFactory, PortableReader,
    PortableWriter,
};
pub use traits::{Deserializable, Serializable};

#[cfg(feature = "serde")]
pub use self::serde::Serde;
