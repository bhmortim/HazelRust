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

use std::collections::HashMap;
use std::sync::Arc;

use crate::Result;

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

/// Trait for custom serializers that can serialize/deserialize arbitrary types.
///
/// Custom serializers are identified by a unique type ID and provide raw byte-level
/// serialization. They are used when the built-in serialization formats (Portable,
/// Compact, serde) are not suitable.
pub trait CustomSerializer: Send + Sync {
    /// Returns the unique type ID for this serializer.
    ///
    /// Type IDs must be positive integers and must not conflict with built-in
    /// Hazelcast type IDs (which are negative).
    fn type_id(&self) -> i32;

    /// Serializes the given raw bytes to the output.
    fn write(&self, output: &mut ObjectDataOutput, data: &[u8]) -> Result<()>;

    /// Deserializes raw bytes from the input.
    fn read(&self, input: &mut ObjectDataInput) -> Result<Vec<u8>>;
}

/// Configuration for serialization behavior.
///
/// Allows registering custom serializers, a global fallback serializer,
/// and configuring Portable serialization version.
pub struct SerializationConfig {
    custom_serializers: HashMap<i32, Arc<dyn CustomSerializer>>,
    global_serializer: Option<Arc<dyn CustomSerializer>>,
    portable_version: i32,
    /// Whether to check class definition errors (default: true).
    pub check_class_def_errors: bool,
}

impl Default for SerializationConfig {
    fn default() -> Self {
        Self {
            custom_serializers: HashMap::new(),
            global_serializer: None,
            portable_version: 0,
            check_class_def_errors: true,
        }
    }
}

impl std::fmt::Debug for SerializationConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SerializationConfig")
            .field("custom_serializer_count", &self.custom_serializers.len())
            .field("has_global_serializer", &self.global_serializer.is_some())
            .field("portable_version", &self.portable_version)
            .field("check_class_def_errors", &self.check_class_def_errors)
            .finish()
    }
}

impl Clone for SerializationConfig {
    fn clone(&self) -> Self {
        Self {
            custom_serializers: self.custom_serializers.clone(),
            global_serializer: self.global_serializer.clone(),
            portable_version: self.portable_version,
            check_class_def_errors: self.check_class_def_errors,
        }
    }
}

impl SerializationConfig {
    /// Creates a new serialization config with defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a custom serializer for a specific type ID.
    ///
    /// The serializer's type ID must be a positive integer.
    pub fn add_custom_serializer(mut self, serializer: impl CustomSerializer + 'static) -> Self {
        let type_id = serializer.type_id();
        self.custom_serializers.insert(type_id, Arc::new(serializer));
        self
    }

    /// Sets the global fallback serializer.
    ///
    /// The global serializer is used when no specific serializer is found for a type.
    pub fn global_serializer(mut self, serializer: impl CustomSerializer + 'static) -> Self {
        self.global_serializer = Some(Arc::new(serializer));
        self
    }

    /// Sets the Portable serialization version.
    pub fn portable_version(mut self, version: i32) -> Self {
        self.portable_version = version;
        self
    }

    /// Sets whether to check class definition errors.
    pub fn with_check_class_def_errors(mut self, check: bool) -> Self {
        self.check_class_def_errors = check;
        self
    }

    /// Returns the custom serializer for the given type ID, if registered.
    pub fn find_custom_serializer(&self, type_id: i32) -> Option<&Arc<dyn CustomSerializer>> {
        self.custom_serializers.get(&type_id)
    }

    /// Returns the global fallback serializer, if set.
    pub fn get_global_serializer(&self) -> Option<&Arc<dyn CustomSerializer>> {
        self.global_serializer.as_ref()
    }

    /// Returns the configured Portable serialization version.
    pub fn portable_version_value(&self) -> i32 {
        self.portable_version
    }

    /// Returns the number of registered custom serializers.
    pub fn custom_serializer_count(&self) -> usize {
        self.custom_serializers.len()
    }
}
