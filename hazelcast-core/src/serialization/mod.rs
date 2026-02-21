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
    rabin_fingerprint_64, Compact, CompactReader, CompactSerializer, CompactWriter,
    DefaultCompactReader, DefaultCompactWriter, DefaultFieldValue, FieldDescriptor, FieldKind,
    GenericRecord, GenericRecordBuilder, Schema, SchemaEvolutionResult, SchemaEvolutionValidator,
    SchemaRegistry, COMPACT_TYPE_ID,
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
pub use traits::{Deserializable, PartitionAware, Serializable};

#[cfg(feature = "serde")]
pub use self::serde::Serde;

/// A global serializer used as the last-resort fallback for types that don't
/// match any other serialization strategy.
///
/// Register a global serializer via [`SerializationConfig`] to handle custom
/// types that aren't covered by Compact, Portable, IdentifiedDataSerializable,
/// or serde.
pub trait GlobalSerializer: Send + Sync {
    /// Returns the type ID for the global serializer.
    fn type_id(&self) -> i32;

    /// Serializes a value to the output stream.
    fn serialize(
        &self,
        output: &mut ObjectDataOutput,
        value: &dyn std::any::Any,
    ) -> crate::Result<()>;

    /// Deserializes a value from the input stream.
    fn deserialize(
        &self,
        input: &mut ObjectDataInput,
    ) -> crate::Result<Box<dyn std::any::Any + Send>>;
}

/// Configuration for the serialization subsystem.
#[derive(Default)]
pub struct SerializationConfig {
    /// The global serializer used as a fallback for unrecognized types.
    pub global_serializer: Option<std::sync::Arc<dyn GlobalSerializer>>,
    /// The portable serialization version (default: 0).
    pub portable_version: i32,
    /// Whether to check class definition errors (default: true).
    pub check_class_def_errors: bool,
}

impl SerializationConfig {
    /// Creates a new default serialization config.
    pub fn new() -> Self {
        Self {
            global_serializer: None,
            portable_version: 0,
            check_class_def_errors: true,
        }
    }

    /// Sets the global serializer fallback.
    pub fn with_global_serializer(
        mut self,
        serializer: std::sync::Arc<dyn GlobalSerializer>,
    ) -> Self {
        self.global_serializer = Some(serializer);
        self
    }

    /// Sets the portable serialization version.
    pub fn with_portable_version(mut self, version: i32) -> Self {
        self.portable_version = version;
        self
    }
}

impl std::fmt::Debug for SerializationConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SerializationConfig")
            .field("has_global_serializer", &self.global_serializer.is_some())
            .field("portable_version", &self.portable_version)
            .field("check_class_def_errors", &self.check_class_def_errors)
            .finish()
    }
}
