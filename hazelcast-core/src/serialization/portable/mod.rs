//! Portable serialization framework for cross-language compatibility.

mod reader_writer;
mod serializer;

use crate::{HazelcastError, Result};
use std::collections::HashMap;

pub use reader_writer::{DefaultPortableReader, DefaultPortableWriter};
pub use serializer::{PortableSerializer, PORTABLE_TYPE_ID};

/// Supported field types in Portable serialization.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i32)]
pub enum FieldType {
    /// Signed 8-bit integer.
    Byte = 1,
    /// Boolean value.
    Bool = 2,
    /// 16-bit Unicode character.
    Char = 3,
    /// Signed 16-bit integer.
    Short = 4,
    /// Signed 32-bit integer.
    Int = 5,
    /// Signed 64-bit integer.
    Long = 6,
    /// 32-bit floating point.
    Float = 7,
    /// 64-bit floating point.
    Double = 8,
    /// UTF-8 string.
    Utf8 = 9,
    /// Nested Portable object.
    Portable = 10,
    /// Array of bytes.
    ByteArray = 11,
    /// Array of booleans.
    BoolArray = 12,
    /// Array of chars.
    CharArray = 13,
    /// Array of shorts.
    ShortArray = 14,
    /// Array of ints.
    IntArray = 15,
    /// Array of longs.
    LongArray = 16,
    /// Array of floats.
    FloatArray = 17,
    /// Array of doubles.
    DoubleArray = 18,
    /// Array of strings.
    Utf8Array = 19,
    /// Array of Portable objects.
    PortableArray = 20,
}

impl FieldType {
    /// Creates a FieldType from its wire representation.
    pub fn from_id(id: i32) -> Result<Self> {
        match id {
            1 => Ok(Self::Byte),
            2 => Ok(Self::Bool),
            3 => Ok(Self::Char),
            4 => Ok(Self::Short),
            5 => Ok(Self::Int),
            6 => Ok(Self::Long),
            7 => Ok(Self::Float),
            8 => Ok(Self::Double),
            9 => Ok(Self::Utf8),
            10 => Ok(Self::Portable),
            11 => Ok(Self::ByteArray),
            12 => Ok(Self::BoolArray),
            13 => Ok(Self::CharArray),
            14 => Ok(Self::ShortArray),
            15 => Ok(Self::IntArray),
            16 => Ok(Self::LongArray),
            17 => Ok(Self::FloatArray),
            18 => Ok(Self::DoubleArray),
            19 => Ok(Self::Utf8Array),
            20 => Ok(Self::PortableArray),
            _ => Err(HazelcastError::Serialization(format!(
                "Unknown field type id: {}",
                id
            ))),
        }
    }

    /// Returns the wire representation of this field type.
    pub fn id(&self) -> i32 {
        *self as i32
    }

    /// Returns true if this is an array type.
    pub fn is_array(&self) -> bool {
        matches!(
            self,
            Self::ByteArray
                | Self::BoolArray
                | Self::CharArray
                | Self::ShortArray
                | Self::IntArray
                | Self::LongArray
                | Self::FloatArray
                | Self::DoubleArray
                | Self::Utf8Array
                | Self::PortableArray
        )
    }
}

/// Definition of a single field within a Portable class.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldDefinition {
    name: String,
    field_type: FieldType,
    index: i32,
    factory_id: i32,
    class_id: i32,
    version: i32,
}

impl FieldDefinition {
    /// Creates a new field definition for a primitive or string field.
    pub fn new(name: impl Into<String>, field_type: FieldType, index: i32) -> Self {
        Self {
            name: name.into(),
            field_type,
            index,
            factory_id: 0,
            class_id: 0,
            version: 0,
        }
    }

    /// Creates a new field definition for a nested Portable field.
    pub fn new_portable(
        name: impl Into<String>,
        index: i32,
        factory_id: i32,
        class_id: i32,
        version: i32,
    ) -> Self {
        Self {
            name: name.into(),
            field_type: FieldType::Portable,
            index,
            factory_id,
            class_id,
            version,
        }
    }

    /// Creates a new field definition for a Portable array field.
    pub fn new_portable_array(
        name: impl Into<String>,
        index: i32,
        factory_id: i32,
        class_id: i32,
        version: i32,
    ) -> Self {
        Self {
            name: name.into(),
            field_type: FieldType::PortableArray,
            index,
            factory_id,
            class_id,
            version,
        }
    }

    /// Returns the field name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the field type.
    pub fn field_type(&self) -> FieldType {
        self.field_type
    }

    /// Returns the field index within the class.
    pub fn index(&self) -> i32 {
        self.index
    }

    /// Returns the factory ID for nested Portable fields.
    pub fn factory_id(&self) -> i32 {
        self.factory_id
    }

    /// Returns the class ID for nested Portable fields.
    pub fn class_id(&self) -> i32 {
        self.class_id
    }

    /// Returns the version for nested Portable fields.
    pub fn version(&self) -> i32 {
        self.version
    }
}

/// Definition of a Portable class schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClassDefinition {
    factory_id: i32,
    class_id: i32,
    version: i32,
    fields: Vec<FieldDefinition>,
    field_indices: HashMap<String, usize>,
}

impl ClassDefinition {
    /// Creates a new class definition.
    pub fn new(factory_id: i32, class_id: i32, version: i32) -> Self {
        Self {
            factory_id,
            class_id,
            version,
            fields: Vec::new(),
            field_indices: HashMap::new(),
        }
    }

    /// Creates a new class definition with the given fields.
    pub fn with_fields(
        factory_id: i32,
        class_id: i32,
        version: i32,
        fields: Vec<FieldDefinition>,
    ) -> Self {
        let field_indices = fields
            .iter()
            .enumerate()
            .map(|(i, f)| (f.name.clone(), i))
            .collect();
        Self {
            factory_id,
            class_id,
            version,
            fields,
            field_indices,
        }
    }

    /// Returns the factory ID.
    pub fn factory_id(&self) -> i32 {
        self.factory_id
    }

    /// Returns the class ID.
    pub fn class_id(&self) -> i32 {
        self.class_id
    }

    /// Returns the schema version.
    pub fn version(&self) -> i32 {
        self.version
    }

    /// Returns the number of fields.
    pub fn field_count(&self) -> usize {
        self.fields.len()
    }

    /// Returns all field definitions.
    pub fn fields(&self) -> &[FieldDefinition] {
        &self.fields
    }

    /// Looks up a field by name.
    pub fn field(&self, name: &str) -> Option<&FieldDefinition> {
        self.field_indices.get(name).map(|&i| &self.fields[i])
    }

    /// Returns true if a field with the given name exists.
    pub fn has_field(&self, name: &str) -> bool {
        self.field_indices.contains_key(name)
    }

    /// Adds a field to this class definition.
    pub fn add_field(&mut self, field: FieldDefinition) {
        self.field_indices.insert(field.name.clone(), self.fields.len());
        self.fields.push(field);
    }
}

/// Factory for creating Portable instances.
pub trait PortableFactory: Send + Sync {
    /// Returns the factory ID this factory handles.
    fn factory_id(&self) -> i32;

    /// Creates a new instance for the given class ID.
    fn create(&self, class_id: i32) -> Option<Box<dyn Portable>>;
}

/// Trait for reading Portable fields during deserialization.
pub trait PortableReader {
    /// Returns the schema version being read.
    fn version(&self) -> i32;

    /// Returns true if a field with the given name exists.
    fn has_field(&self, name: &str) -> bool;

    /// Reads a byte field.
    fn read_byte(&mut self, name: &str) -> Result<i8>;

    /// Reads a boolean field.
    fn read_bool(&mut self, name: &str) -> Result<bool>;

    /// Reads a char field.
    fn read_char(&mut self, name: &str) -> Result<char>;

    /// Reads a short field.
    fn read_short(&mut self, name: &str) -> Result<i16>;

    /// Reads an int field.
    fn read_int(&mut self, name: &str) -> Result<i32>;

    /// Reads a long field.
    fn read_long(&mut self, name: &str) -> Result<i64>;

    /// Reads a float field.
    fn read_float(&mut self, name: &str) -> Result<f32>;

    /// Reads a double field.
    fn read_double(&mut self, name: &str) -> Result<f64>;

    /// Reads a string field.
    fn read_string(&mut self, name: &str) -> Result<Option<String>>;

    /// Reads a nested Portable field.
    fn read_portable<P: Portable>(&mut self, name: &str) -> Result<Option<P>>;

    /// Reads a byte array field.
    fn read_byte_array(&mut self, name: &str) -> Result<Option<Vec<i8>>>;

    /// Reads a boolean array field.
    fn read_bool_array(&mut self, name: &str) -> Result<Option<Vec<bool>>>;

    /// Reads a char array field.
    fn read_char_array(&mut self, name: &str) -> Result<Option<Vec<char>>>;

    /// Reads a short array field.
    fn read_short_array(&mut self, name: &str) -> Result<Option<Vec<i16>>>;

    /// Reads an int array field.
    fn read_int_array(&mut self, name: &str) -> Result<Option<Vec<i32>>>;

    /// Reads a long array field.
    fn read_long_array(&mut self, name: &str) -> Result<Option<Vec<i64>>>;

    /// Reads a float array field.
    fn read_float_array(&mut self, name: &str) -> Result<Option<Vec<f32>>>;

    /// Reads a double array field.
    fn read_double_array(&mut self, name: &str) -> Result<Option<Vec<f64>>>;

    /// Reads a string array field.
    fn read_string_array(&mut self, name: &str) -> Result<Option<Vec<String>>>;

    /// Reads a Portable array field.
    fn read_portable_array<P: Portable>(&mut self, name: &str) -> Result<Option<Vec<P>>>;
}

/// Trait for writing Portable fields during serialization.
pub trait PortableWriter {
    /// Writes a byte field.
    fn write_byte(&mut self, name: &str, value: i8) -> Result<()>;

    /// Writes a boolean field.
    fn write_bool(&mut self, name: &str, value: bool) -> Result<()>;

    /// Writes a char field.
    fn write_char(&mut self, name: &str, value: char) -> Result<()>;

    /// Writes a short field.
    fn write_short(&mut self, name: &str, value: i16) -> Result<()>;

    /// Writes an int field.
    fn write_int(&mut self, name: &str, value: i32) -> Result<()>;

    /// Writes a long field.
    fn write_long(&mut self, name: &str, value: i64) -> Result<()>;

    /// Writes a float field.
    fn write_float(&mut self, name: &str, value: f32) -> Result<()>;

    /// Writes a double field.
    fn write_double(&mut self, name: &str, value: f64) -> Result<()>;

    /// Writes a string field.
    fn write_string(&mut self, name: &str, value: Option<&str>) -> Result<()>;

    /// Writes a nested Portable field.
    fn write_portable<P: Portable>(&mut self, name: &str, value: Option<&P>) -> Result<()>;

    /// Writes a byte array field.
    fn write_byte_array(&mut self, name: &str, value: Option<&[i8]>) -> Result<()>;

    /// Writes a boolean array field.
    fn write_bool_array(&mut self, name: &str, value: Option<&[bool]>) -> Result<()>;

    /// Writes a char array field.
    fn write_char_array(&mut self, name: &str, value: Option<&[char]>) -> Result<()>;

    /// Writes a short array field.
    fn write_short_array(&mut self, name: &str, value: Option<&[i16]>) -> Result<()>;

    /// Writes an int array field.
    fn write_int_array(&mut self, name: &str, value: Option<&[i32]>) -> Result<()>;

    /// Writes a long array field.
    fn write_long_array(&mut self, name: &str, value: Option<&[i64]>) -> Result<()>;

    /// Writes a float array field.
    fn write_float_array(&mut self, name: &str, value: Option<&[f32]>) -> Result<()>;

    /// Writes a double array field.
    fn write_double_array(&mut self, name: &str, value: Option<&[f64]>) -> Result<()>;

    /// Writes a string array field.
    fn write_string_array(&mut self, name: &str, value: Option<&[String]>) -> Result<()>;

    /// Writes a Portable array field.
    fn write_portable_array<P: Portable>(&mut self, name: &str, value: Option<&[P]>) -> Result<()>;
}

/// Trait for types that can be serialized using Portable serialization.
pub trait Portable: Send + Sync {
    /// Returns the factory ID for this type.
    fn factory_id(&self) -> i32;

    /// Returns the class ID for this type.
    fn class_id(&self) -> i32;

    /// Writes this object's fields to the given writer.
    fn write_portable(&self, writer: &mut dyn PortableWriter) -> Result<()>;

    /// Reads this object's fields from the given reader.
    fn read_portable(&mut self, reader: &mut dyn PortableReader) -> Result<()>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn field_type_round_trip() {
        for id in 1..=20 {
            let ft = FieldType::from_id(id).unwrap();
            assert_eq!(ft.id(), id);
        }
    }

    #[test]
    fn field_type_invalid_id() {
        assert!(FieldType::from_id(0).is_err());
        assert!(FieldType::from_id(21).is_err());
        assert!(FieldType::from_id(-1).is_err());
    }

    #[test]
    fn field_type_is_array() {
        assert!(!FieldType::Byte.is_array());
        assert!(!FieldType::Int.is_array());
        assert!(!FieldType::Portable.is_array());
        assert!(FieldType::ByteArray.is_array());
        assert!(FieldType::IntArray.is_array());
        assert!(FieldType::PortableArray.is_array());
    }

    #[test]
    fn field_definition_primitive() {
        let field = FieldDefinition::new("age", FieldType::Int, 0);
        assert_eq!(field.name(), "age");
        assert_eq!(field.field_type(), FieldType::Int);
        assert_eq!(field.index(), 0);
        assert_eq!(field.factory_id(), 0);
        assert_eq!(field.class_id(), 0);
    }

    #[test]
    fn field_definition_portable() {
        let field = FieldDefinition::new_portable("address", 1, 100, 200, 1);
        assert_eq!(field.name(), "address");
        assert_eq!(field.field_type(), FieldType::Portable);
        assert_eq!(field.index(), 1);
        assert_eq!(field.factory_id(), 100);
        assert_eq!(field.class_id(), 200);
        assert_eq!(field.version(), 1);
    }

    #[test]
    fn field_definition_portable_array() {
        let field = FieldDefinition::new_portable_array("items", 2, 100, 300, 2);
        assert_eq!(field.field_type(), FieldType::PortableArray);
        assert_eq!(field.factory_id(), 100);
        assert_eq!(field.class_id(), 300);
    }

    #[test]
    fn class_definition_new() {
        let def = ClassDefinition::new(1, 2, 3);
        assert_eq!(def.factory_id(), 1);
        assert_eq!(def.class_id(), 2);
        assert_eq!(def.version(), 3);
        assert_eq!(def.field_count(), 0);
    }

    #[test]
    fn class_definition_with_fields() {
        let fields = vec![
            FieldDefinition::new("name", FieldType::Utf8, 0),
            FieldDefinition::new("age", FieldType::Int, 1),
        ];
        let def = ClassDefinition::with_fields(1, 2, 1, fields);

        assert_eq!(def.field_count(), 2);
        assert!(def.has_field("name"));
        assert!(def.has_field("age"));
        assert!(!def.has_field("unknown"));

        let name_field = def.field("name").unwrap();
        assert_eq!(name_field.field_type(), FieldType::Utf8);
        assert_eq!(name_field.index(), 0);

        let age_field = def.field("age").unwrap();
        assert_eq!(age_field.field_type(), FieldType::Int);
        assert_eq!(age_field.index(), 1);
    }

    #[test]
    fn class_definition_add_field() {
        let mut def = ClassDefinition::new(1, 1, 1);
        def.add_field(FieldDefinition::new("id", FieldType::Long, 0));
        def.add_field(FieldDefinition::new("active", FieldType::Bool, 1));

        assert_eq!(def.field_count(), 2);
        assert!(def.has_field("id"));
        assert!(def.has_field("active"));

        let fields = def.fields();
        assert_eq!(fields[0].name(), "id");
        assert_eq!(fields[1].name(), "active");
    }
}
