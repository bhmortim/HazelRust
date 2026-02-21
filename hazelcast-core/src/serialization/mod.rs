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
pub use traits::{Deserializable, Serializable};

#[cfg(feature = "serde")]
pub use self::serde::Serde;

use std::collections::HashMap;
use std::sync::Arc;

use crate::Result;

/// Trait for custom serializers that can serialize/deserialize arbitrary types.
///
/// Custom serializers are identified by a unique type ID and provide raw byte-level
/// serialization. They are used when the built-in serialization formats (Portable,
/// Compact, serde) are not suitable.
///
/// # Example
///
/// ```ignore
/// use hazelcast_core::serialization::{CustomSerializer, ObjectDataInput, ObjectDataOutput};
///
/// struct MySerializer;
///
/// impl CustomSerializer for MySerializer {
///     fn type_id(&self) -> i32 { 1000 }
///
///     fn write(&self, output: &mut ObjectDataOutput, data: &[u8]) -> hazelcast_core::Result<()> {
///         output.write_bytes(data);
///         Ok(())
///     }
///
///     fn read(&self, input: &mut ObjectDataInput) -> hazelcast_core::Result<Vec<u8>> {
///         input.read_remaining_bytes()
///     }
/// }
/// ```
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
///
/// # Example
///
/// ```ignore
/// use hazelcast_core::serialization::SerializationConfig;
///
/// let config = SerializationConfig::new()
///     .portable_version(2)
///     .global_serializer(MyGlobalSerializer);
/// ```
pub struct SerializationConfig {
    custom_serializers: HashMap<i32, Arc<dyn CustomSerializer>>,
    global_serializer: Option<Arc<dyn CustomSerializer>>,
    portable_version: i32,
}

impl Default for SerializationConfig {
    fn default() -> Self {
        Self {
            custom_serializers: HashMap::new(),
            global_serializer: None,
            portable_version: 0,
        }
    }
}

impl std::fmt::Debug for SerializationConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SerializationConfig")
            .field("custom_serializer_count", &self.custom_serializers.len())
            .field("has_global_serializer", &self.global_serializer.is_some())
            .field("portable_version", &self.portable_version)
            .finish()
    }
}

impl Clone for SerializationConfig {
    fn clone(&self) -> Self {
        Self {
            custom_serializers: self.custom_serializers.clone(),
            global_serializer: self.global_serializer.clone(),
            portable_version: self.portable_version,
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
