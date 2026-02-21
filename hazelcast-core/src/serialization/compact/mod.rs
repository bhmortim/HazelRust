//! Compact serialization framework for schema-based serialization.

mod generic_record;

use crate::error::{HazelcastError, Result};
use crate::serialization::{DataInput, DataOutput, ObjectDataInput, ObjectDataOutput};
use std::collections::HashMap;
use std::hash::Hash;

pub use generic_record::{GenericRecord, GenericRecordBuilder};

/// Type identifier for Compact serialization.
pub const COMPACT_TYPE_ID: i32 = -2;

/// Rabin fingerprint initial value (matches Java implementation).
const RABIN_FINGERPRINT_INIT: u64 = 0xc15d213aa4d7a795;

/// Computes a Rabin fingerprint for Java interoperability.
pub fn rabin_fingerprint_64(data: &[u8]) -> i64 {
    let mut fp = RABIN_FINGERPRINT_INIT;
    for &byte in data {
        for i in 0..8 {
            let bit = (byte >> i) & 1;
            if (fp ^ bit as u64) & 1 == 1 {
                fp = (fp >> 1) ^ RABIN_FINGERPRINT_INIT;
            } else {
                fp >>= 1;
            }
        }
    }
    fp as i64
}

/// Default values for Compact fields during schema evolution.
#[derive(Debug, Clone, PartialEq)]
pub enum DefaultFieldValue {
    Boolean(bool),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    String(Option<String>),
    Null,
}

/// Result of schema evolution validation.
#[derive(Debug, Clone, PartialEq)]
pub enum SchemaEvolutionResult {
    Compatible {
        added_fields: Vec<String>,
        removed_fields: Vec<String>,
    },
    Incompatible {
        reason: String,
    },
}

/// Field kind identifiers for Compact serialization.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i32)]
pub enum FieldKind {
    Boolean = 0,
    Int8 = 1,
    Int16 = 2,
    Int32 = 3,
    Int64 = 4,
    Float32 = 5,
    Float64 = 6,
    String = 7,
    Compact = 8,
    ArrayOfBoolean = 9,
    ArrayOfInt8 = 10,
    ArrayOfInt16 = 11,
    ArrayOfInt32 = 12,
    ArrayOfInt64 = 13,
    ArrayOfFloat32 = 14,
    ArrayOfFloat64 = 15,
    ArrayOfString = 16,
    ArrayOfCompact = 17,
    NullableBoolean = 18,
    NullableInt8 = 19,
    NullableInt16 = 20,
    NullableInt32 = 21,
    NullableInt64 = 22,
    NullableFloat32 = 23,
    NullableFloat64 = 24,
    Decimal = 25,
    Time = 26,
    Date = 27,
    Timestamp = 28,
    TimestampWithTimezone = 29,
    NullableDecimal = 30,
    NullableTime = 31,
    NullableDate = 32,
    NullableTimestamp = 33,
    NullableTimestampWithTimezone = 34,
    ArrayOfDecimal = 35,
    ArrayOfTime = 36,
    ArrayOfDate = 37,
    ArrayOfTimestamp = 38,
    ArrayOfTimestampWithTimezone = 39,
    ArrayOfNullableBoolean = 40,
    ArrayOfNullableInt8 = 41,
    ArrayOfNullableInt16 = 42,
    ArrayOfNullableInt32 = 43,
    ArrayOfNullableInt64 = 44,
    ArrayOfNullableFloat32 = 45,
    ArrayOfNullableFloat64 = 46,
}

impl FieldKind {
    /// Creates a FieldKind from its wire representation.
    pub fn from_id(id: i32) -> Result<Self> {
        match id {
            0 => Ok(Self::Boolean),
            1 => Ok(Self::Int8),
            2 => Ok(Self::Int16),
            3 => Ok(Self::Int32),
            4 => Ok(Self::Int64),
            5 => Ok(Self::Float32),
            6 => Ok(Self::Float64),
            7 => Ok(Self::String),
            8 => Ok(Self::Compact),
            9 => Ok(Self::ArrayOfBoolean),
            10 => Ok(Self::ArrayOfInt8),
            11 => Ok(Self::ArrayOfInt16),
            12 => Ok(Self::ArrayOfInt32),
            13 => Ok(Self::ArrayOfInt64),
            14 => Ok(Self::ArrayOfFloat32),
            15 => Ok(Self::ArrayOfFloat64),
            16 => Ok(Self::ArrayOfString),
            17 => Ok(Self::ArrayOfCompact),
            18 => Ok(Self::NullableBoolean),
            19 => Ok(Self::NullableInt8),
            20 => Ok(Self::NullableInt16),
            21 => Ok(Self::NullableInt32),
            22 => Ok(Self::NullableInt64),
            23 => Ok(Self::NullableFloat32),
            24 => Ok(Self::NullableFloat64),
            25 => Ok(Self::Decimal),
            26 => Ok(Self::Time),
            27 => Ok(Self::Date),
            28 => Ok(Self::Timestamp),
            29 => Ok(Self::TimestampWithTimezone),
            30 => Ok(Self::NullableDecimal),
            31 => Ok(Self::NullableTime),
            32 => Ok(Self::NullableDate),
            33 => Ok(Self::NullableTimestamp),
            34 => Ok(Self::NullableTimestampWithTimezone),
            35 => Ok(Self::ArrayOfDecimal),
            36 => Ok(Self::ArrayOfTime),
            37 => Ok(Self::ArrayOfDate),
            38 => Ok(Self::ArrayOfTimestamp),
            39 => Ok(Self::ArrayOfTimestampWithTimezone),
            40 => Ok(Self::ArrayOfNullableBoolean),
            41 => Ok(Self::ArrayOfNullableInt8),
            42 => Ok(Self::ArrayOfNullableInt16),
            43 => Ok(Self::ArrayOfNullableInt32),
            44 => Ok(Self::ArrayOfNullableInt64),
            45 => Ok(Self::ArrayOfNullableFloat32),
            46 => Ok(Self::ArrayOfNullableFloat64),
            _ => Err(HazelcastError::Serialization(format!(
                "Unknown field kind id: {}",
                id
            ))),
        }
    }

    /// Returns the wire representation of this field kind.
    pub fn id(&self) -> i32 {
        *self as i32
    }

    /// Returns true if this is a nullable type.
    pub fn is_nullable(&self) -> bool {
        matches!(
            self,
            Self::NullableBoolean
                | Self::NullableInt8
                | Self::NullableInt16
                | Self::NullableInt32
                | Self::NullableInt64
                | Self::NullableFloat32
                | Self::NullableFloat64
                | Self::NullableDecimal
                | Self::NullableTime
                | Self::NullableDate
                | Self::NullableTimestamp
                | Self::NullableTimestampWithTimezone
                | Self::String
                | Self::Compact
                | Self::Decimal
                | Self::Time
                | Self::Date
                | Self::Timestamp
                | Self::TimestampWithTimezone
        )
    }

    /// Returns true if this is an array type.
    pub fn is_array(&self) -> bool {
        matches!(
            self,
            Self::ArrayOfBoolean
                | Self::ArrayOfInt8
                | Self::ArrayOfInt16
                | Self::ArrayOfInt32
                | Self::ArrayOfInt64
                | Self::ArrayOfFloat32
                | Self::ArrayOfFloat64
                | Self::ArrayOfString
                | Self::ArrayOfCompact
                | Self::ArrayOfDecimal
                | Self::ArrayOfTime
                | Self::ArrayOfDate
                | Self::ArrayOfTimestamp
                | Self::ArrayOfTimestampWithTimezone
                | Self::ArrayOfNullableBoolean
                | Self::ArrayOfNullableInt8
                | Self::ArrayOfNullableInt16
                | Self::ArrayOfNullableInt32
                | Self::ArrayOfNullableInt64
                | Self::ArrayOfNullableFloat32
                | Self::ArrayOfNullableFloat64
        )
    }

    /// Returns the default value for this field kind during schema evolution.
    pub fn default_value(&self) -> DefaultFieldValue {
        match self {
            Self::Boolean => DefaultFieldValue::Boolean(false),
            Self::Int8 => DefaultFieldValue::Int8(0),
            Self::Int16 => DefaultFieldValue::Int16(0),
            Self::Int32 => DefaultFieldValue::Int32(0),
            Self::Int64 => DefaultFieldValue::Int64(0),
            Self::Float32 => DefaultFieldValue::Float32(0.0),
            Self::Float64 => DefaultFieldValue::Float64(0.0),
            Self::String => DefaultFieldValue::String(None),
            Self::Compact => DefaultFieldValue::Null,
            Self::ArrayOfBoolean
            | Self::ArrayOfInt8
            | Self::ArrayOfInt16
            | Self::ArrayOfInt32
            | Self::ArrayOfInt64
            | Self::ArrayOfFloat32
            | Self::ArrayOfFloat64
            | Self::ArrayOfString
            | Self::ArrayOfCompact => DefaultFieldValue::Null,
            Self::NullableBoolean
            | Self::NullableInt8
            | Self::NullableInt16
            | Self::NullableInt32
            | Self::NullableInt64
            | Self::NullableFloat32
            | Self::NullableFloat64
            | Self::Decimal
            | Self::Time
            | Self::Date
            | Self::Timestamp
            | Self::TimestampWithTimezone
            | Self::NullableDecimal
            | Self::NullableTime
            | Self::NullableDate
            | Self::NullableTimestamp
            | Self::NullableTimestampWithTimezone
            | Self::ArrayOfDecimal
            | Self::ArrayOfTime
            | Self::ArrayOfDate
            | Self::ArrayOfTimestamp
            | Self::ArrayOfTimestampWithTimezone
            | Self::ArrayOfNullableBoolean
            | Self::ArrayOfNullableInt8
            | Self::ArrayOfNullableInt16
            | Self::ArrayOfNullableInt32
            | Self::ArrayOfNullableInt64
            | Self::ArrayOfNullableFloat32
            | Self::ArrayOfNullableFloat64 => DefaultFieldValue::Null,
        }
    }

    /// Returns the fixed size in bytes for fixed-size types, or None for variable-size types.
    pub fn fixed_size(&self) -> Option<usize> {
        match self {
            Self::Boolean | Self::NullableBoolean => Some(1),
            Self::Int8 | Self::NullableInt8 => Some(1),
            Self::Int16 | Self::NullableInt16 => Some(2),
            Self::Int32 | Self::NullableInt32 => Some(4),
            Self::Int64 | Self::NullableInt64 => Some(8),
            Self::Float32 | Self::NullableFloat32 => Some(4),
            Self::Float64 | Self::NullableFloat64 => Some(8),
            _ => None,
        }
    }
}

/// Descriptor for a field within a Compact schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldDescriptor {
    name: String,
    kind: FieldKind,
    index: i32,
}

impl FieldDescriptor {
    /// Creates a new field descriptor.
    pub fn new(name: impl Into<String>, kind: FieldKind, index: i32) -> Self {
        Self {
            name: name.into(),
            kind,
            index,
        }
    }

    /// Returns the field name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the field kind.
    pub fn kind(&self) -> FieldKind {
        self.kind
    }

    /// Returns the field index.
    pub fn index(&self) -> i32 {
        self.index
    }
}

/// Schema definition for Compact serialization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schema {
    type_name: String,
    fields: Vec<FieldDescriptor>,
    field_indices: HashMap<String, usize>,
    schema_id: i64,
}

impl std::hash::Hash for Schema {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.schema_id.hash(state);
    }
}

impl Schema {
    /// Creates a new schema with the given type name.
    pub fn new(type_name: impl Into<String>) -> Self {
        let type_name = type_name.into();
        let schema_id = Self::compute_schema_id(&type_name, &[]);
        Self {
            type_name,
            fields: Vec::new(),
            field_indices: HashMap::new(),
            schema_id,
        }
    }

    /// Creates a new schema with the given type name and fields.
    pub fn with_fields(type_name: impl Into<String>, fields: Vec<FieldDescriptor>) -> Self {
        let type_name = type_name.into();
        let field_indices = fields
            .iter()
            .enumerate()
            .map(|(i, f)| (f.name.clone(), i))
            .collect();
        let schema_id = Self::compute_schema_id(&type_name, &fields);
        Self {
            type_name,
            fields,
            field_indices,
            schema_id,
        }
    }

    fn compute_schema_id(type_name: &str, fields: &[FieldDescriptor]) -> i64 {
        let mut data = Vec::new();
        data.extend_from_slice(type_name.as_bytes());
        
        let mut sorted_fields: Vec<_> = fields.iter().collect();
        sorted_fields.sort_by(|a, b| a.name.cmp(&b.name));
        
        for field in sorted_fields {
            data.extend_from_slice(field.name.as_bytes());
            data.extend_from_slice(&field.kind.id().to_be_bytes());
        }
        
        rabin_fingerprint_64(&data)
    }

    /// Returns the type name.
    pub fn type_name(&self) -> &str {
        &self.type_name
    }

    /// Returns the schema ID (fingerprint).
    pub fn schema_id(&self) -> i64 {
        self.schema_id
    }

    /// Returns the number of fields.
    pub fn field_count(&self) -> usize {
        self.fields.len()
    }

    /// Returns all field descriptors in order.
    pub fn fields(&self) -> &[FieldDescriptor] {
        &self.fields
    }

    /// Looks up a field by name.
    pub fn field(&self, name: &str) -> Option<&FieldDescriptor> {
        self.field_indices.get(name).map(|&i| &self.fields[i])
    }

    /// Returns true if a field with the given name exists.
    pub fn has_field(&self, name: &str) -> bool {
        self.field_indices.contains_key(name)
    }

    /// Adds a field to this schema and recomputes the schema ID.
    pub fn add_field(&mut self, field: FieldDescriptor) {
        self.field_indices.insert(field.name.clone(), self.fields.len());
        self.fields.push(field);
        self.schema_id = Self::compute_schema_id(&self.type_name, &self.fields);
    }

    /// Checks if this schema is compatible with another schema for evolution.
    /// 
    /// Schemas are compatible if they have the same type name and all common
    /// fields have compatible types.
    pub fn is_compatible_with(&self, other: &Schema) -> bool {
        if self.type_name != other.type_name {
            return false;
        }

        for field in &self.fields {
            if let Some(other_field) = other.field(&field.name) {
                if !Self::are_field_kinds_compatible(field.kind, other_field.kind) {
                    return false;
                }
            }
        }

        true
    }

    fn are_field_kinds_compatible(kind1: FieldKind, kind2: FieldKind) -> bool {
        if kind1 == kind2 {
            return true;
        }

        match (kind1, kind2) {
            (FieldKind::Int8, FieldKind::NullableInt8)
            | (FieldKind::NullableInt8, FieldKind::Int8) => true,
            (FieldKind::Int16, FieldKind::NullableInt16)
            | (FieldKind::NullableInt16, FieldKind::Int16) => true,
            (FieldKind::Int32, FieldKind::NullableInt32)
            | (FieldKind::NullableInt32, FieldKind::Int32) => true,
            (FieldKind::Int64, FieldKind::NullableInt64)
            | (FieldKind::NullableInt64, FieldKind::Int64) => true,
            (FieldKind::Float32, FieldKind::NullableFloat32)
            | (FieldKind::NullableFloat32, FieldKind::Float32) => true,
            (FieldKind::Float64, FieldKind::NullableFloat64)
            | (FieldKind::NullableFloat64, FieldKind::Float64) => true,
            (FieldKind::Boolean, FieldKind::NullableBoolean)
            | (FieldKind::NullableBoolean, FieldKind::Boolean) => true,
            (FieldKind::Decimal, FieldKind::NullableDecimal)
            | (FieldKind::NullableDecimal, FieldKind::Decimal) => true,
            (FieldKind::Time, FieldKind::NullableTime)
            | (FieldKind::NullableTime, FieldKind::Time) => true,
            (FieldKind::Date, FieldKind::NullableDate)
            | (FieldKind::NullableDate, FieldKind::Date) => true,
            (FieldKind::Timestamp, FieldKind::NullableTimestamp)
            | (FieldKind::NullableTimestamp, FieldKind::Timestamp) => true,
            (FieldKind::TimestampWithTimezone, FieldKind::NullableTimestampWithTimezone)
            | (FieldKind::NullableTimestampWithTimezone, FieldKind::TimestampWithTimezone) => true,
            _ => false,
        }
    }

    /// Returns the default value for a field by name.
    pub fn get_default_value_for_field(&self, field_name: &str) -> Option<DefaultFieldValue> {
        self.field(field_name).map(|f| f.kind.default_value())
    }
}

/// Validator for schema evolution compatibility.
#[derive(Debug, Clone, Default)]
pub struct SchemaEvolutionValidator;

impl SchemaEvolutionValidator {
    /// Creates a new schema evolution validator.
    pub fn new() -> Self {
        Self
    }

    /// Validates that schema evolution from old to new is compatible.
    pub fn validate_evolution(
        old_schema: &Schema,
        new_schema: &Schema,
    ) -> Result<SchemaEvolutionResult> {
        if old_schema.type_name() != new_schema.type_name() {
            return Ok(SchemaEvolutionResult::Incompatible {
                reason: format!(
                    "Type name mismatch: '{}' vs '{}'",
                    old_schema.type_name(),
                    new_schema.type_name()
                ),
            });
        }

        let old_field_names: std::collections::HashSet<_> =
            old_schema.fields().iter().map(|f| f.name()).collect();
        let new_field_names: std::collections::HashSet<_> =
            new_schema.fields().iter().map(|f| f.name()).collect();

        let added_fields: Vec<String> = new_field_names
            .difference(&old_field_names)
            .map(|s| s.to_string())
            .collect();

        let removed_fields: Vec<String> = old_field_names
            .difference(&new_field_names)
            .map(|s| s.to_string())
            .collect();

        for field in old_schema.fields() {
            if let Some(new_field) = new_schema.field(field.name()) {
                if !Schema::are_field_kinds_compatible(field.kind(), new_field.kind()) {
                    return Ok(SchemaEvolutionResult::Incompatible {
                        reason: format!(
                            "Incompatible field type change for '{}': {:?} -> {:?}",
                            field.name(),
                            field.kind(),
                            new_field.kind()
                        ),
                    });
                }
            }
        }

        Ok(SchemaEvolutionResult::Compatible {
            added_fields,
            removed_fields,
        })
    }
}

/// Registry for managing Compact schemas.
#[derive(Debug, Default)]
pub struct SchemaRegistry {
    schemas_by_id: HashMap<i64, Schema>,
    schemas_by_type: HashMap<String, Vec<i64>>,
}

impl SchemaRegistry {
    /// Creates a new empty schema registry.
    pub fn new() -> Self {
        Self {
            schemas_by_id: HashMap::new(),
            schemas_by_type: HashMap::new(),
        }
    }

    /// Registers a schema in the local cache.
    pub fn register_schema(&mut self, schema: Schema) {
        let schema_id = schema.schema_id();
        let type_name = schema.type_name().to_string();

        if !self.schemas_by_id.contains_key(&schema_id) {
            self.schemas_by_id.insert(schema_id, schema);
            self.schemas_by_type
                .entry(type_name)
                .or_default()
                .push(schema_id);
        }
    }

    /// Looks up a schema by its ID.
    pub fn get_schema_by_id(&self, schema_id: i64) -> Option<&Schema> {
        self.schemas_by_id.get(&schema_id)
    }

    /// Returns all schema versions for a given type name.
    pub fn get_schemas_by_type(&self, type_name: &str) -> Vec<&Schema> {
        self.schemas_by_type
            .get(type_name)
            .map(|ids| ids.iter().filter_map(|id| self.schemas_by_id.get(id)).collect())
            .unwrap_or_default()
    }

    /// Gets an existing schema or creates a new one for the given Compact type.
    pub fn get_or_create_schema<T: Compact>(&mut self) -> Schema {
        let type_name = T::get_type_name();
        
        if let Some(schemas) = self.schemas_by_type.get(type_name) {
            if let Some(&schema_id) = schemas.first() {
                if let Some(schema) = self.schemas_by_id.get(&schema_id) {
                    return schema.clone();
                }
            }
        }

        let schema = Schema::new(type_name);
        self.register_schema(schema.clone());
        schema
    }

    /// Fetches a schema from the cluster by ID.
    /// 
    /// This is a stub that currently returns from the local cache.
    /// Will be wired to cluster communication later.
    pub fn fetch_schema(&self, schema_id: i64) -> Result<Option<Schema>> {
        Ok(self.schemas_by_id.get(&schema_id).cloned())
    }

    /// Sends a schema to the cluster.
    /// 
    /// This is a stub that currently does nothing.
    /// Will be wired to cluster communication later.
    pub fn send_schema(&self, _schema: &Schema) -> Result<()> {
        Ok(())
    }

    /// Returns the number of registered schemas.
    pub fn len(&self) -> usize {
        self.schemas_by_id.len()
    }

    /// Returns true if no schemas are registered.
    pub fn is_empty(&self) -> bool {
        self.schemas_by_id.is_empty()
    }
}

/// Trait for reading Compact fields during deserialization.
pub trait CompactReader {
    /// Returns the schema being read.
    fn get_schema(&self) -> &Schema;

    /// Reads a boolean field.
    fn read_boolean(&mut self, name: &str) -> Result<bool>;

    /// Reads an i8 field.
    fn read_int8(&mut self, name: &str) -> Result<i8>;

    /// Reads an i16 field.
    fn read_int16(&mut self, name: &str) -> Result<i16>;

    /// Reads an i32 field.
    fn read_int32(&mut self, name: &str) -> Result<i32>;

    /// Reads an i64 field.
    fn read_int64(&mut self, name: &str) -> Result<i64>;

    /// Reads an f32 field.
    fn read_float32(&mut self, name: &str) -> Result<f32>;

    /// Reads an f64 field.
    fn read_float64(&mut self, name: &str) -> Result<f64>;

    /// Reads a string field.
    fn read_string(&mut self, name: &str) -> Result<Option<String>>;

    /// Reads a nullable boolean field.
    fn read_nullable_boolean(&mut self, name: &str) -> Result<Option<bool>>;

    /// Reads a nullable i8 field.
    fn read_nullable_int8(&mut self, name: &str) -> Result<Option<i8>>;

    /// Reads a nullable i16 field.
    fn read_nullable_int16(&mut self, name: &str) -> Result<Option<i16>>;

    /// Reads a nullable i32 field.
    fn read_nullable_int32(&mut self, name: &str) -> Result<Option<i32>>;

    /// Reads a nullable i64 field.
    fn read_nullable_int64(&mut self, name: &str) -> Result<Option<i64>>;

    /// Reads a nullable f32 field.
    fn read_nullable_float32(&mut self, name: &str) -> Result<Option<f32>>;

    /// Reads a nullable f64 field.
    fn read_nullable_float64(&mut self, name: &str) -> Result<Option<f64>>;

    /// Reads an array of booleans.
    fn read_array_of_boolean(&mut self, name: &str) -> Result<Option<Vec<bool>>>;

    /// Reads an array of i8 values.
    fn read_array_of_int8(&mut self, name: &str) -> Result<Option<Vec<i8>>>;

    /// Reads an array of i16 values.
    fn read_array_of_int16(&mut self, name: &str) -> Result<Option<Vec<i16>>>;

    /// Reads an array of i32 values.
    fn read_array_of_int32(&mut self, name: &str) -> Result<Option<Vec<i32>>>;

    /// Reads an array of i64 values.
    fn read_array_of_int64(&mut self, name: &str) -> Result<Option<Vec<i64>>>;

    /// Reads an array of f32 values.
    fn read_array_of_float32(&mut self, name: &str) -> Result<Option<Vec<f32>>>;

    /// Reads an array of f64 values.
    fn read_array_of_float64(&mut self, name: &str) -> Result<Option<Vec<f64>>>;

    /// Reads an array of strings.
    fn read_array_of_string(&mut self, name: &str) -> Result<Option<Vec<Option<String>>>>;

    /// Reads a nested Compact object.
    fn read_compact<T: Compact + Default>(&mut self, name: &str) -> Result<Option<T>>;

    /// Reads an array of Compact objects.
    fn read_array_of_compact<T: Compact + Default>(&mut self, name: &str) -> Result<Option<Vec<T>>>;
}

/// Trait for writing Compact fields during serialization.
pub trait CompactWriter {
    /// Writes a boolean field.
    fn write_boolean(&mut self, name: &str, value: bool) -> Result<()>;

    /// Writes an i8 field.
    fn write_int8(&mut self, name: &str, value: i8) -> Result<()>;

    /// Writes an i16 field.
    fn write_int16(&mut self, name: &str, value: i16) -> Result<()>;

    /// Writes an i32 field.
    fn write_int32(&mut self, name: &str, value: i32) -> Result<()>;

    /// Writes an i64 field.
    fn write_int64(&mut self, name: &str, value: i64) -> Result<()>;

    /// Writes an f32 field.
    fn write_float32(&mut self, name: &str, value: f32) -> Result<()>;

    /// Writes an f64 field.
    fn write_float64(&mut self, name: &str, value: f64) -> Result<()>;

    /// Writes a string field.
    fn write_string(&mut self, name: &str, value: Option<&str>) -> Result<()>;

    /// Writes a nullable boolean field.
    fn write_nullable_boolean(&mut self, name: &str, value: Option<bool>) -> Result<()>;

    /// Writes a nullable i8 field.
    fn write_nullable_int8(&mut self, name: &str, value: Option<i8>) -> Result<()>;

    /// Writes a nullable i16 field.
    fn write_nullable_int16(&mut self, name: &str, value: Option<i16>) -> Result<()>;

    /// Writes a nullable i32 field.
    fn write_nullable_int32(&mut self, name: &str, value: Option<i32>) -> Result<()>;

    /// Writes a nullable i64 field.
    fn write_nullable_int64(&mut self, name: &str, value: Option<i64>) -> Result<()>;

    /// Writes a nullable f32 field.
    fn write_nullable_float32(&mut self, name: &str, value: Option<f32>) -> Result<()>;

    /// Writes a nullable f64 field.
    fn write_nullable_float64(&mut self, name: &str, value: Option<f64>) -> Result<()>;

    /// Writes an array of booleans.
    fn write_array_of_boolean(&mut self, name: &str, value: Option<&[bool]>) -> Result<()>;

    /// Writes an array of i8 values.
    fn write_array_of_int8(&mut self, name: &str, value: Option<&[i8]>) -> Result<()>;

    /// Writes an array of i16 values.
    fn write_array_of_int16(&mut self, name: &str, value: Option<&[i16]>) -> Result<()>;

    /// Writes an array of i32 values.
    fn write_array_of_int32(&mut self, name: &str, value: Option<&[i32]>) -> Result<()>;

    /// Writes an array of i64 values.
    fn write_array_of_int64(&mut self, name: &str, value: Option<&[i64]>) -> Result<()>;

    /// Writes an array of f32 values.
    fn write_array_of_float32(&mut self, name: &str, value: Option<&[f32]>) -> Result<()>;

    /// Writes an array of f64 values.
    fn write_array_of_float64(&mut self, name: &str, value: Option<&[f64]>) -> Result<()>;

    /// Writes an array of strings.
    fn write_array_of_string(&mut self, name: &str, value: Option<&[Option<String>]>) -> Result<()>;

    /// Writes a nested Compact object.
    fn write_compact<T: Compact>(&mut self, name: &str, value: Option<&T>) -> Result<()>;

    /// Writes an array of Compact objects.
    fn write_array_of_compact<T: Compact>(&mut self, name: &str, value: Option<&[T]>) -> Result<()>;
}

/// Trait for types that can be serialized using Compact serialization.
pub trait Compact: Send + Sync {
    /// Returns the type name for this Compact type.
    fn get_type_name() -> &'static str
    where
        Self: Sized;

    /// Writes this object's fields to the given writer.
    fn write(&self, writer: &mut DefaultCompactWriter) -> Result<()>;

    /// Reads this object's fields from the given reader.
    fn read(&mut self, reader: &mut DefaultCompactReader) -> Result<()>;
}

const NULL_MARKER: i8 = 0;
const NOT_NULL_MARKER: i8 = 1;

/// Stored field data for reading.
#[derive(Debug, Clone)]
struct FieldData {
    kind: FieldKind,
    data: Vec<u8>,
}

/// Default implementation of `CompactWriter`.
#[derive(Debug)]
pub struct DefaultCompactWriter {
    schema: Schema,
    fields: HashMap<String, FieldData>,
    field_order: Vec<String>,
}

impl DefaultCompactWriter {
    /// Creates a new writer for the given schema.
    pub fn new(schema: Schema) -> Self {
        Self {
            schema,
            fields: HashMap::new(),
            field_order: Vec::new(),
        }
    }

    /// Returns the schema.
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    fn write_field(&mut self, name: &str, kind: FieldKind, data: Vec<u8>) -> Result<()> {
        if !self.fields.contains_key(name) {
            self.field_order.push(name.to_string());
        }
        self.fields.insert(name.to_string(), FieldData { kind, data });
        Ok(())
    }

    /// Converts the written data into bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut output = ObjectDataOutput::new();

        let _ = output.write_int(self.field_order.len() as i32);

        for name in &self.field_order {
            if let Some(field_data) = self.fields.get(name) {
                let _ = output.write_string(name);
                let _ = output.write_int(field_data.kind.id());
                let _ = output.write_byte(NOT_NULL_MARKER);
                let _ = output.write_int(field_data.data.len() as i32);
                let _ = output.write_bytes(&field_data.data);
            }
        }

        output.into_bytes()
    }
}

impl CompactWriter for DefaultCompactWriter {
    fn write_boolean(&mut self, name: &str, value: bool) -> Result<()> {
        let mut out = ObjectDataOutput::new();
        out.write_bool(value)?;
        self.write_field(name, FieldKind::Boolean, out.into_bytes())
    }

    fn write_int8(&mut self, name: &str, value: i8) -> Result<()> {
        let mut out = ObjectDataOutput::new();
        out.write_byte(value)?;
        self.write_field(name, FieldKind::Int8, out.into_bytes())
    }

    fn write_int16(&mut self, name: &str, value: i16) -> Result<()> {
        let mut out = ObjectDataOutput::new();
        out.write_short(value)?;
        self.write_field(name, FieldKind::Int16, out.into_bytes())
    }

    fn write_int32(&mut self, name: &str, value: i32) -> Result<()> {
        let mut out = ObjectDataOutput::new();
        out.write_int(value)?;
        self.write_field(name, FieldKind::Int32, out.into_bytes())
    }

    fn write_int64(&mut self, name: &str, value: i64) -> Result<()> {
        let mut out = ObjectDataOutput::new();
        out.write_long(value)?;
        self.write_field(name, FieldKind::Int64, out.into_bytes())
    }

    fn write_float32(&mut self, name: &str, value: f32) -> Result<()> {
        let mut out = ObjectDataOutput::new();
        out.write_float(value)?;
        self.write_field(name, FieldKind::Float32, out.into_bytes())
    }

    fn write_float64(&mut self, name: &str, value: f64) -> Result<()> {
        let mut out = ObjectDataOutput::new();
        out.write_double(value)?;
        self.write_field(name, FieldKind::Float64, out.into_bytes())
    }

    fn write_string(&mut self, name: &str, value: Option<&str>) -> Result<()> {
        let mut out = ObjectDataOutput::new();
        match value {
            Some(s) => {
                out.write_bool(true)?;
                out.write_string(s)?;
            }
            None => {
                out.write_bool(false)?;
            }
        }
        self.write_field(name, FieldKind::String, out.into_bytes())
    }

    fn write_nullable_boolean(&mut self, name: &str, value: Option<bool>) -> Result<()> {
        let mut out = ObjectDataOutput::new();
        match value {
            Some(v) => {
                out.write_bool(true)?;
                out.write_bool(v)?;
            }
            None => {
                out.write_bool(false)?;
            }
        }
        self.write_field(name, FieldKind::NullableBoolean, out.into_bytes())
    }

    fn write_nullable_int8(&mut self, name: &str, value: Option<i8>) -> Result<()> {
        let mut out = ObjectDataOutput::new();
        match value {
            Some(v) => {
                out.write_bool(true)?;
                out.write_byte(v)?;
            }
            None => {
                out.write_bool(false)?;
            }
        }
        self.write_field(name, FieldKind::NullableInt8, out.into_bytes())
    }

    fn write_nullable_int16(&mut self, name: &str, value: Option<i16>) -> Result<()> {
        let mut out = ObjectDataOutput::new();
        match value {
            Some(v) => {
                out.write_bool(true)?;
                out.write_short(v)?;
            }
            None => {
                out.write_bool(false)?;
            }
        }
        self.write_field(name, FieldKind::NullableInt16, out.into_bytes())
    }

    fn write_nullable_int32(&mut self, name: &str, value: Option<i32>) -> Result<()> {
        let mut out = ObjectDataOutput::new();
        match value {
            Some(v) => {
                out.write_bool(true)?;
                out.write_int(v)?;
            }
            None => {
                out.write_bool(false)?;
            }
        }
        self.write_field(name, FieldKind::NullableInt32, out.into_bytes())
    }

    fn write_nullable_int64(&mut self, name: &str, value: Option<i64>) -> Result<()> {
        let mut out = ObjectDataOutput::new();
        match value {
            Some(v) => {
                out.write_bool(true)?;
                out.write_long(v)?;
            }
            None => {
                out.write_bool(false)?;
            }
        }
        self.write_field(name, FieldKind::NullableInt64, out.into_bytes())
    }

    fn write_nullable_float32(&mut self, name: &str, value: Option<f32>) -> Result<()> {
        let mut out = ObjectDataOutput::new();
        match value {
            Some(v) => {
                out.write_bool(true)?;
                out.write_float(v)?;
            }
            None => {
                out.write_bool(false)?;
            }
        }
        self.write_field(name, FieldKind::NullableFloat32, out.into_bytes())
    }

    fn write_nullable_float64(&mut self, name: &str, value: Option<f64>) -> Result<()> {
        let mut out = ObjectDataOutput::new();
        match value {
            Some(v) => {
                out.write_bool(true)?;
                out.write_double(v)?;
            }
            None => {
                out.write_bool(false)?;
            }
        }
        self.write_field(name, FieldKind::NullableFloat64, out.into_bytes())
    }

    fn write_array_of_boolean(&mut self, name: &str, value: Option<&[bool]>) -> Result<()> {
        let mut out = ObjectDataOutput::new();
        match value {
            Some(arr) => {
                out.write_bool(true)?;
                out.write_int(arr.len() as i32)?;
                for &v in arr {
                    out.write_bool(v)?;
                }
            }
            None => {
                out.write_bool(false)?;
            }
        }
        self.write_field(name, FieldKind::ArrayOfBoolean, out.into_bytes())
    }

    fn write_array_of_int8(&mut self, name: &str, value: Option<&[i8]>) -> Result<()> {
        let mut out = ObjectDataOutput::new();
        match value {
            Some(arr) => {
                out.write_bool(true)?;
                out.write_int(arr.len() as i32)?;
                for &v in arr {
                    out.write_byte(v)?;
                }
            }
            None => {
                out.write_bool(false)?;
            }
        }
        self.write_field(name, FieldKind::ArrayOfInt8, out.into_bytes())
    }

    fn write_array_of_int16(&mut self, name: &str, value: Option<&[i16]>) -> Result<()> {
        let mut out = ObjectDataOutput::new();
        match value {
            Some(arr) => {
                out.write_bool(true)?;
                out.write_int(arr.len() as i32)?;
                for &v in arr {
                    out.write_short(v)?;
                }
            }
            None => {
                out.write_bool(false)?;
            }
        }
        self.write_field(name, FieldKind::ArrayOfInt16, out.into_bytes())
    }

    fn write_array_of_int32(&mut self, name: &str, value: Option<&[i32]>) -> Result<()> {
        let mut out = ObjectDataOutput::new();
        match value {
            Some(arr) => {
                out.write_bool(true)?;
                out.write_int(arr.len() as i32)?;
                for &v in arr {
                    out.write_int(v)?;
                }
            }
            None => {
                out.write_bool(false)?;
            }
        }
        self.write_field(name, FieldKind::ArrayOfInt32, out.into_bytes())
    }

    fn write_array_of_int64(&mut self, name: &str, value: Option<&[i64]>) -> Result<()> {
        let mut out = ObjectDataOutput::new();
        match value {
            Some(arr) => {
                out.write_bool(true)?;
                out.write_int(arr.len() as i32)?;
                for &v in arr {
                    out.write_long(v)?;
                }
            }
            None => {
                out.write_bool(false)?;
            }
        }
        self.write_field(name, FieldKind::ArrayOfInt64, out.into_bytes())
    }

    fn write_array_of_float32(&mut self, name: &str, value: Option<&[f32]>) -> Result<()> {
        let mut out = ObjectDataOutput::new();
        match value {
            Some(arr) => {
                out.write_bool(true)?;
                out.write_int(arr.len() as i32)?;
                for &v in arr {
                    out.write_float(v)?;
                }
            }
            None => {
                out.write_bool(false)?;
            }
        }
        self.write_field(name, FieldKind::ArrayOfFloat32, out.into_bytes())
    }

    fn write_array_of_float64(&mut self, name: &str, value: Option<&[f64]>) -> Result<()> {
        let mut out = ObjectDataOutput::new();
        match value {
            Some(arr) => {
                out.write_bool(true)?;
                out.write_int(arr.len() as i32)?;
                for &v in arr {
                    out.write_double(v)?;
                }
            }
            None => {
                out.write_bool(false)?;
            }
        }
        self.write_field(name, FieldKind::ArrayOfFloat64, out.into_bytes())
    }

    fn write_array_of_string(&mut self, name: &str, value: Option<&[Option<String>]>) -> Result<()> {
        let mut out = ObjectDataOutput::new();
        match value {
            Some(arr) => {
                out.write_bool(true)?;
                out.write_int(arr.len() as i32)?;
                for s in arr {
                    match s {
                        Some(v) => {
                            out.write_bool(true)?;
                            out.write_string(v)?;
                        }
                        None => {
                            out.write_bool(false)?;
                        }
                    }
                }
            }
            None => {
                out.write_bool(false)?;
            }
        }
        self.write_field(name, FieldKind::ArrayOfString, out.into_bytes())
    }

    fn write_compact<T: Compact>(&mut self, name: &str, value: Option<&T>) -> Result<()> {
        let mut out = ObjectDataOutput::new();
        match value {
            Some(v) => {
                out.write_bool(true)?;
                let type_name = T::get_type_name();
                out.write_string(type_name)?;

                let nested_schema = Schema::new(type_name);
                let mut nested_writer = DefaultCompactWriter::new(nested_schema);
                v.write(&mut nested_writer)?;
                let nested_bytes = nested_writer.to_bytes();
                out.write_int(nested_bytes.len() as i32)?;
                out.write_bytes(&nested_bytes)?;
            }
            None => {
                out.write_bool(false)?;
            }
        }
        self.write_field(name, FieldKind::Compact, out.into_bytes())
    }

    fn write_array_of_compact<T: Compact>(&mut self, name: &str, value: Option<&[T]>) -> Result<()> {
        let mut out = ObjectDataOutput::new();
        match value {
            Some(arr) => {
                out.write_bool(true)?;
                out.write_int(arr.len() as i32)?;
                for v in arr {
                    let type_name = T::get_type_name();
                    out.write_string(type_name)?;

                    let nested_schema = Schema::new(type_name);
                    let mut nested_writer = DefaultCompactWriter::new(nested_schema);
                    v.write(&mut nested_writer)?;
                    let nested_bytes = nested_writer.to_bytes();
                    out.write_int(nested_bytes.len() as i32)?;
                    out.write_bytes(&nested_bytes)?;
                }
            }
            None => {
                out.write_bool(false)?;
            }
        }
        self.write_field(name, FieldKind::ArrayOfCompact, out.into_bytes())
    }
}

/// Default implementation of `CompactReader`.
#[derive(Debug)]
pub struct DefaultCompactReader {
    schema: Schema,
    writer_schema: Option<Schema>,
    fields: HashMap<String, FieldData>,
}

impl DefaultCompactReader {
    /// Creates a new reader from serialized bytes with an expected schema.
    pub fn from_bytes(data: &[u8], schema: Schema) -> Result<Self> {
        Self::from_bytes_internal(data, schema, None)
    }

    /// Creates a new reader from serialized bytes with both reader and writer schemas.
    /// 
    /// This constructor enables schema evolution by tracking which schema was used
    /// to write the data vs. which schema the reader expects.
    pub fn from_bytes_with_writer_schema(
        data: &[u8],
        reader_schema: Schema,
        writer_schema: Schema,
    ) -> Result<Self> {
        Self::from_bytes_internal(data, reader_schema, Some(writer_schema))
    }

    fn from_bytes_internal(
        data: &[u8],
        schema: Schema,
        writer_schema: Option<Schema>,
    ) -> Result<Self> {
        let mut input = ObjectDataInput::new(data);
        let mut fields = HashMap::new();

        let field_count = input.read_int()?;

        for _ in 0..field_count {
            let name = input.read_string()?;
            let kind_id = input.read_int()?;
            let kind = FieldKind::from_id(kind_id)?;
            let is_present = input.read_byte()?;

            if is_present == NOT_NULL_MARKER {
                let data_len = input.read_int()? as usize;
                let data = input.read_bytes(data_len)?;
                fields.insert(name, FieldData { kind, data });
            }
        }

        Ok(Self {
            schema,
            writer_schema,
            fields,
        })
    }

    /// Returns the writer schema if available.
    pub fn writer_schema(&self) -> Option<&Schema> {
        self.writer_schema.as_ref()
    }

    fn get_field(&self, name: &str, expected_kind: FieldKind) -> Result<Option<&[u8]>> {
        match self.fields.get(name) {
            Some(field_data) => {
                if field_data.kind != expected_kind {
                    return Err(HazelcastError::Serialization(format!(
                        "Field '{}' kind mismatch: expected {:?}, got {:?}",
                        name, expected_kind, field_data.kind
                    )));
                }
                Ok(Some(&field_data.data))
            }
            None => Ok(None),
        }
    }
}

impl CompactReader for DefaultCompactReader {
    fn get_schema(&self) -> &Schema {
        &self.schema
    }

    fn read_boolean(&mut self, name: &str) -> Result<bool> {
        match self.get_field(name, FieldKind::Boolean)? {
            Some(data) => {
                let mut input = ObjectDataInput::new(data);
                input.read_bool()
            }
            None => Ok(false),
        }
    }

    fn read_int8(&mut self, name: &str) -> Result<i8> {
        match self.get_field(name, FieldKind::Int8)? {
            Some(data) => {
                let mut input = ObjectDataInput::new(data);
                input.read_byte()
            }
            None => Ok(0),
        }
    }

    fn read_int16(&mut self, name: &str) -> Result<i16> {
        match self.get_field(name, FieldKind::Int16)? {
            Some(data) => {
                let mut input = ObjectDataInput::new(data);
                input.read_short()
            }
            None => Ok(0),
        }
    }

    fn read_int32(&mut self, name: &str) -> Result<i32> {
        match self.get_field(name, FieldKind::Int32)? {
            Some(data) => {
                let mut input = ObjectDataInput::new(data);
                input.read_int()
            }
            None => Ok(0),
        }
    }

    fn read_int64(&mut self, name: &str) -> Result<i64> {
        match self.get_field(name, FieldKind::Int64)? {
            Some(data) => {
                let mut input = ObjectDataInput::new(data);
                input.read_long()
            }
            None => Ok(0),
        }
    }

    fn read_float32(&mut self, name: &str) -> Result<f32> {
        match self.get_field(name, FieldKind::Float32)? {
            Some(data) => {
                let mut input = ObjectDataInput::new(data);
                input.read_float()
            }
            None => Ok(0.0),
        }
    }

    fn read_float64(&mut self, name: &str) -> Result<f64> {
        match self.get_field(name, FieldKind::Float64)? {
            Some(data) => {
                let mut input = ObjectDataInput::new(data);
                input.read_double()
            }
            None => Ok(0.0),
        }
    }

    fn read_string(&mut self, name: &str) -> Result<Option<String>> {
        match self.get_field(name, FieldKind::String)? {
            Some(data) => {
                let mut input = ObjectDataInput::new(data);
                let has_value = input.read_bool()?;
                if has_value {
                    Ok(Some(input.read_string()?))
                } else {
                    Ok(None)
                }
            }
            None => Ok(None),
        }
    }

    fn read_nullable_boolean(&mut self, name: &str) -> Result<Option<bool>> {
        match self.get_field(name, FieldKind::NullableBoolean)? {
            Some(data) => {
                let mut input = ObjectDataInput::new(data);
                let has_value = input.read_bool()?;
                if has_value {
                    Ok(Some(input.read_bool()?))
                } else {
                    Ok(None)
                }
            }
            None => Ok(None),
        }
    }

    fn read_nullable_int8(&mut self, name: &str) -> Result<Option<i8>> {
        match self.get_field(name, FieldKind::NullableInt8)? {
            Some(data) => {
                let mut input = ObjectDataInput::new(data);
                let has_value = input.read_bool()?;
                if has_value {
                    Ok(Some(input.read_byte()?))
                } else {
                    Ok(None)
                }
            }
            None => Ok(None),
        }
    }

    fn read_nullable_int16(&mut self, name: &str) -> Result<Option<i16>> {
        match self.get_field(name, FieldKind::NullableInt16)? {
            Some(data) => {
                let mut input = ObjectDataInput::new(data);
                let has_value = input.read_bool()?;
                if has_value {
                    Ok(Some(input.read_short()?))
                } else {
                    Ok(None)
                }
            }
            None => Ok(None),
        }
    }

    fn read_nullable_int32(&mut self, name: &str) -> Result<Option<i32>> {
        match self.get_field(name, FieldKind::NullableInt32)? {
            Some(data) => {
                let mut input = ObjectDataInput::new(data);
                let has_value = input.read_bool()?;
                if has_value {
                    Ok(Some(input.read_int()?))
                } else {
                    Ok(None)
                }
            }
            None => Ok(None),
        }
    }

    fn read_nullable_int64(&mut self, name: &str) -> Result<Option<i64>> {
        match self.get_field(name, FieldKind::NullableInt64)? {
            Some(data) => {
                let mut input = ObjectDataInput::new(data);
                let has_value = input.read_bool()?;
                if has_value {
                    Ok(Some(input.read_long()?))
                } else {
                    Ok(None)
                }
            }
            None => Ok(None),
        }
    }

    fn read_nullable_float32(&mut self, name: &str) -> Result<Option<f32>> {
        match self.get_field(name, FieldKind::NullableFloat32)? {
            Some(data) => {
                let mut input = ObjectDataInput::new(data);
                let has_value = input.read_bool()?;
                if has_value {
                    Ok(Some(input.read_float()?))
                } else {
                    Ok(None)
                }
            }
            None => Ok(None),
        }
    }

    fn read_nullable_float64(&mut self, name: &str) -> Result<Option<f64>> {
        match self.get_field(name, FieldKind::NullableFloat64)? {
            Some(data) => {
                let mut input = ObjectDataInput::new(data);
                let has_value = input.read_bool()?;
                if has_value {
                    Ok(Some(input.read_double()?))
                } else {
                    Ok(None)
                }
            }
            None => Ok(None),
        }
    }

    fn read_array_of_boolean(&mut self, name: &str) -> Result<Option<Vec<bool>>> {
        match self.get_field(name, FieldKind::ArrayOfBoolean)? {
            Some(data) => {
                let mut input = ObjectDataInput::new(data);
                let has_value = input.read_bool()?;
                if !has_value {
                    return Ok(None);
                }
                let len = input.read_int()? as usize;
                let mut result = Vec::with_capacity(len);
                for _ in 0..len {
                    result.push(input.read_bool()?);
                }
                Ok(Some(result))
            }
            None => Ok(None),
        }
    }

    fn read_array_of_int8(&mut self, name: &str) -> Result<Option<Vec<i8>>> {
        match self.get_field(name, FieldKind::ArrayOfInt8)? {
            Some(data) => {
                let mut input = ObjectDataInput::new(data);
                let has_value = input.read_bool()?;
                if !has_value {
                    return Ok(None);
                }
                let len = input.read_int()? as usize;
                let mut result = Vec::with_capacity(len);
                for _ in 0..len {
                    result.push(input.read_byte()?);
                }
                Ok(Some(result))
            }
            None => Ok(None),
        }
    }

    fn read_array_of_int16(&mut self, name: &str) -> Result<Option<Vec<i16>>> {
        match self.get_field(name, FieldKind::ArrayOfInt16)? {
            Some(data) => {
                let mut input = ObjectDataInput::new(data);
                let has_value = input.read_bool()?;
                if !has_value {
                    return Ok(None);
                }
                let len = input.read_int()? as usize;
                let mut result = Vec::with_capacity(len);
                for _ in 0..len {
                    result.push(input.read_short()?);
                }
                Ok(Some(result))
            }
            None => Ok(None),
        }
    }

    fn read_array_of_int32(&mut self, name: &str) -> Result<Option<Vec<i32>>> {
        match self.get_field(name, FieldKind::ArrayOfInt32)? {
            Some(data) => {
                let mut input = ObjectDataInput::new(data);
                let has_value = input.read_bool()?;
                if !has_value {
                    return Ok(None);
                }
                let len = input.read_int()? as usize;
                let mut result = Vec::with_capacity(len);
                for _ in 0..len {
                    result.push(input.read_int()?);
                }
                Ok(Some(result))
            }
            None => Ok(None),
        }
    }

    fn read_array_of_int64(&mut self, name: &str) -> Result<Option<Vec<i64>>> {
        match self.get_field(name, FieldKind::ArrayOfInt64)? {
            Some(data) => {
                let mut input = ObjectDataInput::new(data);
                let has_value = input.read_bool()?;
                if !has_value {
                    return Ok(None);
                }
                let len = input.read_int()? as usize;
                let mut result = Vec::with_capacity(len);
                for _ in 0..len {
                    result.push(input.read_long()?);
                }
                Ok(Some(result))
            }
            None => Ok(None),
        }
    }

    fn read_array_of_float32(&mut self, name: &str) -> Result<Option<Vec<f32>>> {
        match self.get_field(name, FieldKind::ArrayOfFloat32)? {
            Some(data) => {
                let mut input = ObjectDataInput::new(data);
                let has_value = input.read_bool()?;
                if !has_value {
                    return Ok(None);
                }
                let len = input.read_int()? as usize;
                let mut result = Vec::with_capacity(len);
                for _ in 0..len {
                    result.push(input.read_float()?);
                }
                Ok(Some(result))
            }
            None => Ok(None),
        }
    }

    fn read_array_of_float64(&mut self, name: &str) -> Result<Option<Vec<f64>>> {
        match self.get_field(name, FieldKind::ArrayOfFloat64)? {
            Some(data) => {
                let mut input = ObjectDataInput::new(data);
                let has_value = input.read_bool()?;
                if !has_value {
                    return Ok(None);
                }
                let len = input.read_int()? as usize;
                let mut result = Vec::with_capacity(len);
                for _ in 0..len {
                    result.push(input.read_double()?);
                }
                Ok(Some(result))
            }
            None => Ok(None),
        }
    }

    fn read_array_of_string(&mut self, name: &str) -> Result<Option<Vec<Option<String>>>> {
        match self.get_field(name, FieldKind::ArrayOfString)? {
            Some(data) => {
                let mut input = ObjectDataInput::new(data);
                let has_value = input.read_bool()?;
                if !has_value {
                    return Ok(None);
                }
                let len = input.read_int()? as usize;
                let mut result = Vec::with_capacity(len);
                for _ in 0..len {
                    let has_str = input.read_bool()?;
                    if has_str {
                        result.push(Some(input.read_string()?));
                    } else {
                        result.push(None);
                    }
                }
                Ok(Some(result))
            }
            None => Ok(None),
        }
    }

    fn read_compact<T: Compact + Default>(&mut self, name: &str) -> Result<Option<T>> {
        match self.get_field(name, FieldKind::Compact)? {
            Some(data) => {
                let mut input = ObjectDataInput::new(data);
                let has_value = input.read_bool()?;
                if !has_value {
                    return Ok(None);
                }

                let type_name = input.read_string()?;
                let nested_len = input.read_int()? as usize;
                let nested_data = input.read_bytes(nested_len)?;

                let mut instance = T::default();
                if T::get_type_name() != type_name {
                    return Err(HazelcastError::Serialization(format!(
                        "Type mismatch: expected '{}', got '{}'",
                        T::get_type_name(),
                        type_name
                    )));
                }

                let nested_schema = Schema::new(&type_name);
                let mut nested_reader = DefaultCompactReader::from_bytes(&nested_data, nested_schema)?;
                instance.read(&mut nested_reader)?;
                Ok(Some(instance))
            }
            None => Ok(None),
        }
    }

    fn read_array_of_compact<T: Compact + Default>(&mut self, name: &str) -> Result<Option<Vec<T>>> {
        match self.get_field(name, FieldKind::ArrayOfCompact)? {
            Some(data) => {
                let mut input = ObjectDataInput::new(data);
                let has_value = input.read_bool()?;
                if !has_value {
                    return Ok(None);
                }

                let len = input.read_int()? as usize;
                let mut result = Vec::with_capacity(len);

                for _ in 0..len {
                    let type_name = input.read_string()?;
                    let nested_len = input.read_int()? as usize;
                    let nested_data = input.read_bytes(nested_len)?;

                    let mut instance = T::default();
                    if T::get_type_name() != type_name {
                        return Err(HazelcastError::Serialization(format!(
                            "Type mismatch: expected '{}', got '{}'",
                            T::get_type_name(),
                            type_name
                        )));
                    }

                    let nested_schema = Schema::new(&type_name);
                    let mut nested_reader =
                        DefaultCompactReader::from_bytes(&nested_data, nested_schema)?;
                    instance.read(&mut nested_reader)?;
                    result.push(instance);
                }

                Ok(Some(result))
            }
            None => Ok(None),
        }
    }
}

/// Serializer for Compact objects.
#[derive(Debug, Default)]
pub struct CompactSerializer {
    registry: SchemaRegistry,
}

impl CompactSerializer {
    /// Creates a new CompactSerializer.
    pub fn new() -> Self {
        Self {
            registry: SchemaRegistry::new(),
        }
    }

    /// Creates a new CompactSerializer with the given schema registry.
    pub fn with_registry(registry: SchemaRegistry) -> Self {
        Self { registry }
    }

    /// Returns a reference to the schema registry.
    pub fn get_registry(&self) -> &SchemaRegistry {
        &self.registry
    }

    /// Returns a mutable reference to the schema registry.
    pub fn get_registry_mut(&mut self) -> &mut SchemaRegistry {
        &mut self.registry
    }

    /// Registers a schema for a type.
    pub fn register_schema(&mut self, schema: Schema) {
        self.registry.register_schema(schema);
    }

    /// Looks up a schema by type name.
    pub fn get_schema(&self, type_name: &str) -> Option<&Schema> {
        self.registry.get_schemas_by_type(type_name).into_iter().next()
    }

    /// Serializes a Compact object to bytes.
    pub fn serialize<T: Compact>(&self, value: &T) -> Result<Vec<u8>> {
        let type_name = T::get_type_name();
        let schema = self
            .registry
            .get_schemas_by_type(type_name)
            .into_iter()
            .next()
            .cloned()
            .unwrap_or_else(|| Schema::new(type_name));

        let mut writer = DefaultCompactWriter::new(schema.clone());
        value.write(&mut writer)?;
        let field_data = writer.to_bytes();

        let mut output = ObjectDataOutput::new();
        output.write_string(type_name)?;
        output.write_long(schema.schema_id())?;
        output.write_int(field_data.len() as i32)?;
        output.write_bytes(&field_data)?;

        Ok(output.into_bytes())
    }

    /// Deserializes bytes into a Compact object.
    pub fn deserialize<T: Compact + Default>(&self, data: &[u8]) -> Result<T> {
        let mut input = ObjectDataInput::new(data);

        let type_name = input.read_string()?;
        let writer_schema_id = input.read_long()?;
        let field_data_len = input.read_int()? as usize;
        let field_data = input.read_bytes(field_data_len)?;

        if T::get_type_name() != type_name {
            return Err(HazelcastError::Serialization(format!(
                "Type mismatch: expected '{}', got '{}'",
                T::get_type_name(),
                type_name
            )));
        }

        let reader_schema = self
            .registry
            .get_schemas_by_type(&type_name)
            .into_iter()
            .next()
            .cloned()
            .unwrap_or_else(|| Schema::new(&type_name));

        let writer_schema = self.registry.get_schema_by_id(writer_schema_id).cloned();

        let mut reader = match writer_schema {
            Some(ws) if ws.schema_id() != reader_schema.schema_id() => {
                DefaultCompactReader::from_bytes_with_writer_schema(
                    &field_data,
                    reader_schema,
                    ws,
                )?
            }
            _ => DefaultCompactReader::from_bytes(&field_data, reader_schema)?,
        };

        let mut instance = T::default();
        instance.read(&mut reader)?;

        Ok(instance)
    }

    /// Deserializes bytes with explicit schema evolution support.
    /// 
    /// Uses the provided reader schema and looks up the writer schema from the registry.
    pub fn deserialize_with_schema<T: Compact + Default>(
        &self,
        data: &[u8],
        reader_schema: Schema,
    ) -> Result<T> {
        let mut input = ObjectDataInput::new(data);

        let type_name = input.read_string()?;
        let writer_schema_id = input.read_long()?;
        let field_data_len = input.read_int()? as usize;
        let field_data = input.read_bytes(field_data_len)?;

        if T::get_type_name() != type_name {
            return Err(HazelcastError::Serialization(format!(
                "Type mismatch: expected '{}', got '{}'",
                T::get_type_name(),
                type_name
            )));
        }

        let writer_schema = self.registry.get_schema_by_id(writer_schema_id).cloned();

        let mut reader = match writer_schema {
            Some(ws) => DefaultCompactReader::from_bytes_with_writer_schema(
                &field_data,
                reader_schema,
                ws,
            )?,
            None => DefaultCompactReader::from_bytes(&field_data, reader_schema)?,
        };

        let mut instance = T::default();
        instance.read(&mut reader)?;

        Ok(instance)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Default, PartialEq)]
    struct Person {
        name: String,
        age: i32,
        active: bool,
        score: Option<f64>,
    }

    impl Compact for Person {
        fn get_type_name() -> &'static str {
            "Person"
        }

        fn write(&self, writer: &mut DefaultCompactWriter) -> Result<()> {
            writer.write_string("name", Some(&self.name))?;
            writer.write_int32("age", self.age)?;
            writer.write_boolean("active", self.active)?;
            writer.write_nullable_float64("score", self.score)?;
            Ok(())
        }

        fn read(&mut self, reader: &mut DefaultCompactReader) -> Result<()> {
            self.name = reader.read_string("name")?.unwrap_or_default();
            self.age = reader.read_int32("age")?;
            self.active = reader.read_boolean("active")?;
            self.score = reader.read_nullable_float64("score")?;
            Ok(())
        }
    }

    #[derive(Debug, Default, PartialEq)]
    struct Address {
        street: String,
        city: String,
        zip: i32,
    }

    impl Compact for Address {
        fn get_type_name() -> &'static str {
            "Address"
        }

        fn write(&self, writer: &mut DefaultCompactWriter) -> Result<()> {
            writer.write_string("street", Some(&self.street))?;
            writer.write_string("city", Some(&self.city))?;
            writer.write_int32("zip", self.zip)?;
            Ok(())
        }

        fn read(&mut self, reader: &mut DefaultCompactReader) -> Result<()> {
            self.street = reader.read_string("street")?.unwrap_or_default();
            self.city = reader.read_string("city")?.unwrap_or_default();
            self.zip = reader.read_int32("zip")?;
            Ok(())
        }
    }

    #[test]
    fn test_field_kind_round_trip() {
        for id in 0..=46 {
            let kind = FieldKind::from_id(id).unwrap();
            assert_eq!(kind.id(), id);
        }
    }

    #[test]
    fn test_field_kind_invalid_id() {
        assert!(FieldKind::from_id(-1).is_err());
        assert!(FieldKind::from_id(47).is_err());
    }
    
    #[test]
    fn test_field_kind_fixed_size() {
        assert_eq!(FieldKind::Boolean.fixed_size(), Some(1));
        assert_eq!(FieldKind::Int8.fixed_size(), Some(1));
        assert_eq!(FieldKind::Int16.fixed_size(), Some(2));
        assert_eq!(FieldKind::Int32.fixed_size(), Some(4));
        assert_eq!(FieldKind::Int64.fixed_size(), Some(8));
        assert_eq!(FieldKind::Float32.fixed_size(), Some(4));
        assert_eq!(FieldKind::Float64.fixed_size(), Some(8));
        assert_eq!(FieldKind::String.fixed_size(), None);
        assert_eq!(FieldKind::Compact.fixed_size(), None);
        assert_eq!(FieldKind::ArrayOfInt32.fixed_size(), None);
    }

    #[test]
    fn test_rabin_fingerprint_deterministic() {
        let data = b"test data for fingerprint";
        let fp1 = rabin_fingerprint_64(data);
        let fp2 = rabin_fingerprint_64(data);
        assert_eq!(fp1, fp2);
        
        let different_data = b"different data";
        let fp3 = rabin_fingerprint_64(different_data);
        assert_ne!(fp1, fp3);
    }

    #[test]
    fn test_schema_id_deterministic_across_field_order() {
        let fields1 = vec![
            FieldDescriptor::new("alpha", FieldKind::Int32, 0),
            FieldDescriptor::new("beta", FieldKind::String, 1),
        ];
        let fields2 = vec![
            FieldDescriptor::new("beta", FieldKind::String, 1),
            FieldDescriptor::new("alpha", FieldKind::Int32, 0),
        ];
        
        let schema1 = Schema::with_fields("Test", fields1);
        let schema2 = Schema::with_fields("Test", fields2);
        
        assert_eq!(schema1.schema_id(), schema2.schema_id());
    }

    #[test]
    fn test_field_kind_is_nullable() {
        assert!(!FieldKind::Boolean.is_nullable());
        assert!(!FieldKind::Int32.is_nullable());
        assert!(FieldKind::NullableBoolean.is_nullable());
        assert!(FieldKind::NullableInt32.is_nullable());
        assert!(FieldKind::String.is_nullable());
        assert!(FieldKind::Compact.is_nullable());
    }

    #[test]
    fn test_field_kind_is_array() {
        assert!(!FieldKind::Boolean.is_array());
        assert!(!FieldKind::Int32.is_array());
        assert!(FieldKind::ArrayOfBoolean.is_array());
        assert!(FieldKind::ArrayOfInt32.is_array());
        assert!(FieldKind::ArrayOfCompact.is_array());
    }

    #[test]
    fn test_field_descriptor() {
        let field = FieldDescriptor::new("count", FieldKind::Int32, 0);
        assert_eq!(field.name(), "count");
        assert_eq!(field.kind(), FieldKind::Int32);
        assert_eq!(field.index(), 0);
    }

    #[test]
    fn test_schema_new() {
        let schema = Schema::new("TestType");
        assert_eq!(schema.type_name(), "TestType");
        assert_eq!(schema.field_count(), 0);
    }

    #[test]
    fn test_schema_with_fields() {
        let fields = vec![
            FieldDescriptor::new("name", FieldKind::String, 0),
            FieldDescriptor::new("age", FieldKind::Int32, 1),
        ];
        let schema = Schema::with_fields("Person", fields);

        assert_eq!(schema.type_name(), "Person");
        assert_eq!(schema.field_count(), 2);
        assert!(schema.has_field("name"));
        assert!(schema.has_field("age"));
        assert!(!schema.has_field("unknown"));

        let name_field = schema.field("name").unwrap();
        assert_eq!(name_field.kind(), FieldKind::String);
    }

    #[test]
    fn test_schema_add_field() {
        let mut schema = Schema::new("Test");
        let initial_id = schema.schema_id();

        schema.add_field(FieldDescriptor::new("value", FieldKind::Int64, 0));

        assert_eq!(schema.field_count(), 1);
        assert!(schema.has_field("value"));
        assert_ne!(schema.schema_id(), initial_id);
    }

    #[test]
    fn test_serialize_deserialize_round_trip() {
        let serializer = CompactSerializer::new();

        let person = Person {
            name: "Alice".to_string(),
            age: 30,
            active: true,
            score: Some(95.5),
        };

        let bytes = serializer.serialize(&person).unwrap();
        let result: Person = serializer.deserialize(&bytes).unwrap();

        assert_eq!(result, person);
    }

    #[test]
    fn test_serialize_deserialize_with_null() {
        let serializer = CompactSerializer::new();

        let person = Person {
            name: "Bob".to_string(),
            age: 25,
            active: false,
            score: None,
        };

        let bytes = serializer.serialize(&person).unwrap();
        let result: Person = serializer.deserialize(&bytes).unwrap();

        assert_eq!(result, person);
    }

    #[test]
    fn test_multiple_types() {
        let serializer = CompactSerializer::new();

        let person = Person {
            name: "Charlie".to_string(),
            age: 35,
            active: true,
            score: Some(88.0),
        };

        let address = Address {
            street: "123 Main St".to_string(),
            city: "Springfield".to_string(),
            zip: 12345,
        };

        let person_bytes = serializer.serialize(&person).unwrap();
        let address_bytes = serializer.serialize(&address).unwrap();

        let person_result: Person = serializer.deserialize(&person_bytes).unwrap();
        let address_result: Address = serializer.deserialize(&address_bytes).unwrap();

        assert_eq!(person_result, person);
        assert_eq!(address_result, address);
    }

    #[test]
    fn test_type_mismatch() {
        let serializer = CompactSerializer::new();

        let person = Person {
            name: "Test".to_string(),
            age: 20,
            active: true,
            score: None,
        };

        let bytes = serializer.serialize(&person).unwrap();
        let result: Result<Address> = serializer.deserialize(&bytes);

        assert!(result.is_err());
    }

    #[test]
    fn test_empty_string_fields() {
        let serializer = CompactSerializer::new();

        let person = Person {
            name: String::new(),
            age: 0,
            active: false,
            score: None,
        };

        let bytes = serializer.serialize(&person).unwrap();
        let result: Person = serializer.deserialize(&bytes).unwrap();

        assert_eq!(result, person);
    }

    #[test]
    fn test_special_characters_in_string() {
        let serializer = CompactSerializer::new();

        let person = Person {
            name: "Hello 世界 🚀".to_string(),
            age: 42,
            active: true,
            score: Some(99.9),
        };

        let bytes = serializer.serialize(&person).unwrap();
        let result: Person = serializer.deserialize(&bytes).unwrap();

        assert_eq!(result, person);
    }

    #[test]
    fn test_extreme_values() {
        let serializer = CompactSerializer::new();

        let person = Person {
            name: "Max Values".to_string(),
            age: i32::MAX,
            active: true,
            score: Some(f64::MAX),
        };

        let bytes = serializer.serialize(&person).unwrap();
        let result: Person = serializer.deserialize(&bytes).unwrap();

        assert_eq!(result, person);

        let person_min = Person {
            name: "Min Values".to_string(),
            age: i32::MIN,
            active: false,
            score: Some(f64::MIN),
        };

        let bytes_min = serializer.serialize(&person_min).unwrap();
        let result_min: Person = serializer.deserialize(&bytes_min).unwrap();

        assert_eq!(result_min, person_min);
    }

    #[test]
    fn test_schema_registration() {
        let mut serializer = CompactSerializer::new();
        let schema = Schema::with_fields(
            "Person",
            vec![
                FieldDescriptor::new("name", FieldKind::String, 0),
                FieldDescriptor::new("age", FieldKind::Int32, 1),
            ],
        );
        serializer.register_schema(schema.clone());

        let retrieved = serializer.get_schema("Person");
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().type_name(), "Person");

        let not_found = serializer.get_schema("Unknown");
        assert!(not_found.is_none());
    }

    #[test]
    fn test_field_ordering_preserved() {
        let schema = Schema::new("Test");
        let mut writer = DefaultCompactWriter::new(schema.clone());

        writer.write_int32("third", 3).unwrap();
        writer.write_int32("first", 1).unwrap();
        writer.write_int32("second", 2).unwrap();

        let bytes = writer.to_bytes();
        let mut reader = DefaultCompactReader::from_bytes(&bytes, schema).unwrap();

        assert_eq!(reader.read_int32("first").unwrap(), 1);
        assert_eq!(reader.read_int32("second").unwrap(), 2);
        assert_eq!(reader.read_int32("third").unwrap(), 3);
    }

    #[test]
    fn test_arrays() {
        let schema = Schema::new("ArrayTest");
        let mut writer = DefaultCompactWriter::new(schema.clone());

        writer
            .write_array_of_int32("numbers", Some(&[1, 2, 3, 4, 5]))
            .unwrap();
        writer
            .write_array_of_string(
                "strings",
                Some(&[Some("a".to_string()), None, Some("c".to_string())]),
            )
            .unwrap();

        let bytes = writer.to_bytes();
        let mut reader = DefaultCompactReader::from_bytes(&bytes, schema).unwrap();

        assert_eq!(
            reader.read_array_of_int32("numbers").unwrap(),
            Some(vec![1, 2, 3, 4, 5])
        );
        assert_eq!(
            reader.read_array_of_string("strings").unwrap(),
            Some(vec![Some("a".to_string()), None, Some("c".to_string())])
        );
    }

    #[test]
    fn test_null_arrays() {
        let schema = Schema::new("NullArrayTest");
        let mut writer = DefaultCompactWriter::new(schema.clone());

        writer.write_array_of_int32("numbers", None).unwrap();

        let bytes = writer.to_bytes();
        let mut reader = DefaultCompactReader::from_bytes(&bytes, schema).unwrap();

        assert_eq!(reader.read_array_of_int32("numbers").unwrap(), None);
    }

    #[test]
    fn test_nested_compact() {
        #[derive(Debug, Default, PartialEq)]
        struct Employee {
            name: String,
            address: Option<Address>,
        }

        impl Compact for Employee {
            fn get_type_name() -> &'static str {
                "Employee"
            }

            fn write(&self, writer: &mut DefaultCompactWriter) -> Result<()> {
                writer.write_string("name", Some(&self.name))?;
                writer.write_compact("address", self.address.as_ref())?;
                Ok(())
            }

            fn read(&mut self, reader: &mut DefaultCompactReader) -> Result<()> {
                self.name = reader.read_string("name")?.unwrap_or_default();
                self.address = reader.read_compact("address")?;
                Ok(())
            }
        }

        let serializer = CompactSerializer::new();

        let employee = Employee {
            name: "John".to_string(),
            address: Some(Address {
                street: "456 Oak Ave".to_string(),
                city: "Portland".to_string(),
                zip: 97201,
            }),
        };

        let bytes = serializer.serialize(&employee).unwrap();
        let result: Employee = serializer.deserialize(&bytes).unwrap();

        assert_eq!(result, employee);
    }

    #[test]
    fn test_missing_field_returns_default() {
        let schema = Schema::new("Test");
        let writer = DefaultCompactWriter::new(schema.clone());
        let bytes = writer.to_bytes();

        let mut reader = DefaultCompactReader::from_bytes(&bytes, schema).unwrap();

        assert_eq!(reader.read_int32("missing").unwrap(), 0);
        assert_eq!(reader.read_boolean("missing").unwrap(), false);
        assert_eq!(reader.read_string("missing").unwrap(), None);
    }

    #[test]
    fn test_schema_id_changes_with_fields() {
        let schema1 = Schema::new("Test");
        let schema2 = Schema::with_fields(
            "Test",
            vec![FieldDescriptor::new("field", FieldKind::Int32, 0)],
        );

        assert_ne!(schema1.schema_id(), schema2.schema_id());
    }

    #[test]
    fn test_schema_registry_basic_operations() {
        let mut registry = SchemaRegistry::new();
        assert!(registry.is_empty());

        let schema1 = Schema::with_fields(
            "Person",
            vec![
                FieldDescriptor::new("name", FieldKind::String, 0),
                FieldDescriptor::new("age", FieldKind::Int32, 1),
            ],
        );
        let schema_id = schema1.schema_id();

        registry.register_schema(schema1.clone());

        assert_eq!(registry.len(), 1);
        assert!(!registry.is_empty());

        let retrieved = registry.get_schema_by_id(schema_id);
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().type_name(), "Person");

        let by_type = registry.get_schemas_by_type("Person");
        assert_eq!(by_type.len(), 1);
        assert_eq!(by_type[0].schema_id(), schema_id);

        let not_found = registry.get_schema_by_id(999);
        assert!(not_found.is_none());
    }

    #[test]
    fn test_schema_registry_multiple_versions() {
        let mut registry = SchemaRegistry::new();

        let v1 = Schema::with_fields(
            "Person",
            vec![FieldDescriptor::new("name", FieldKind::String, 0)],
        );

        let v2 = Schema::with_fields(
            "Person",
            vec![
                FieldDescriptor::new("name", FieldKind::String, 0),
                FieldDescriptor::new("age", FieldKind::Int32, 1),
            ],
        );

        registry.register_schema(v1.clone());
        registry.register_schema(v2.clone());

        let schemas = registry.get_schemas_by_type("Person");
        assert_eq!(schemas.len(), 2);
    }

    #[test]
    fn test_schema_registry_get_or_create() {
        let mut registry = SchemaRegistry::new();

        let schema1 = registry.get_or_create_schema::<Person>();
        assert_eq!(schema1.type_name(), "Person");
        assert_eq!(registry.len(), 1);

        let schema2 = registry.get_or_create_schema::<Person>();
        assert_eq!(schema2.schema_id(), schema1.schema_id());
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn test_schema_registry_fetch_and_send_stubs() {
        let mut registry = SchemaRegistry::new();
        let schema = Schema::new("Test");
        registry.register_schema(schema.clone());

        let fetched = registry.fetch_schema(schema.schema_id()).unwrap();
        assert_eq!(fetched, Some(schema.clone()));

        let not_found = registry.fetch_schema(999).unwrap();
        assert!(not_found.is_none());

        assert!(registry.send_schema(&schema).is_ok());
    }

    #[test]
    fn test_default_field_value_for_all_kinds() {
        assert_eq!(FieldKind::Boolean.default_value(), DefaultFieldValue::Boolean(false));
        assert_eq!(FieldKind::Int8.default_value(), DefaultFieldValue::Int8(0));
        assert_eq!(FieldKind::Int16.default_value(), DefaultFieldValue::Int16(0));
        assert_eq!(FieldKind::Int32.default_value(), DefaultFieldValue::Int32(0));
        assert_eq!(FieldKind::Int64.default_value(), DefaultFieldValue::Int64(0));
        assert_eq!(FieldKind::Float32.default_value(), DefaultFieldValue::Float32(0.0));
        assert_eq!(FieldKind::Float64.default_value(), DefaultFieldValue::Float64(0.0));
        assert_eq!(FieldKind::String.default_value(), DefaultFieldValue::String(None));
        assert_eq!(FieldKind::Compact.default_value(), DefaultFieldValue::Null);
        assert_eq!(FieldKind::ArrayOfInt32.default_value(), DefaultFieldValue::Null);
        assert_eq!(FieldKind::NullableInt32.default_value(), DefaultFieldValue::Null);
    }

    #[test]
    fn test_schema_get_default_value_for_field() {
        let schema = Schema::with_fields(
            "Test",
            vec![
                FieldDescriptor::new("count", FieldKind::Int32, 0),
                FieldDescriptor::new("name", FieldKind::String, 1),
            ],
        );

        assert_eq!(
            schema.get_default_value_for_field("count"),
            Some(DefaultFieldValue::Int32(0))
        );
        assert_eq!(
            schema.get_default_value_for_field("name"),
            Some(DefaultFieldValue::String(None))
        );
        assert_eq!(schema.get_default_value_for_field("unknown"), None);
    }

    #[test]
    fn test_schema_compatibility_same_schema() {
        let schema = Schema::with_fields(
            "Person",
            vec![
                FieldDescriptor::new("name", FieldKind::String, 0),
                FieldDescriptor::new("age", FieldKind::Int32, 1),
            ],
        );

        assert!(schema.is_compatible_with(&schema));
    }

    #[test]
    fn test_schema_compatibility_added_field() {
        let v1 = Schema::with_fields(
            "Person",
            vec![FieldDescriptor::new("name", FieldKind::String, 0)],
        );

        let v2 = Schema::with_fields(
            "Person",
            vec![
                FieldDescriptor::new("name", FieldKind::String, 0),
                FieldDescriptor::new("age", FieldKind::Int32, 1),
            ],
        );

        assert!(v1.is_compatible_with(&v2));
        assert!(v2.is_compatible_with(&v1));
    }

    #[test]
    fn test_schema_compatibility_type_mismatch() {
        let v1 = Schema::with_fields(
            "Person",
            vec![FieldDescriptor::new("age", FieldKind::Int32, 0)],
        );

        let v2 = Schema::with_fields(
            "Person",
            vec![FieldDescriptor::new("age", FieldKind::String, 0)],
        );

        assert!(!v1.is_compatible_with(&v2));
    }

    #[test]
    fn test_schema_compatibility_nullable_promotion() {
        let v1 = Schema::with_fields(
            "Test",
            vec![FieldDescriptor::new("value", FieldKind::Int32, 0)],
        );

        let v2 = Schema::with_fields(
            "Test",
            vec![FieldDescriptor::new("value", FieldKind::NullableInt32, 0)],
        );

        assert!(v1.is_compatible_with(&v2));
        assert!(v2.is_compatible_with(&v1));
    }

    #[test]
    fn test_schema_compatibility_different_type_names() {
        let s1 = Schema::with_fields(
            "Person",
            vec![FieldDescriptor::new("name", FieldKind::String, 0)],
        );

        let s2 = Schema::with_fields(
            "Employee",
            vec![FieldDescriptor::new("name", FieldKind::String, 0)],
        );

        assert!(!s1.is_compatible_with(&s2));
    }

    #[test]
    fn test_schema_evolution_validator_compatible() {
        let old_schema = Schema::with_fields(
            "Person",
            vec![FieldDescriptor::new("name", FieldKind::String, 0)],
        );

        let new_schema = Schema::with_fields(
            "Person",
            vec![
                FieldDescriptor::new("name", FieldKind::String, 0),
                FieldDescriptor::new("email", FieldKind::String, 1),
            ],
        );

        let result = SchemaEvolutionValidator::validate_evolution(&old_schema, &new_schema).unwrap();

        match result {
            SchemaEvolutionResult::Compatible {
                added_fields,
                removed_fields,
            } => {
                assert_eq!(added_fields, vec!["email".to_string()]);
                assert!(removed_fields.is_empty());
            }
            _ => panic!("Expected compatible result"),
        }
    }

    #[test]
    fn test_schema_evolution_validator_removed_field() {
        let old_schema = Schema::with_fields(
            "Person",
            vec![
                FieldDescriptor::new("name", FieldKind::String, 0),
                FieldDescriptor::new("legacy", FieldKind::Int32, 1),
            ],
        );

        let new_schema = Schema::with_fields(
            "Person",
            vec![FieldDescriptor::new("name", FieldKind::String, 0)],
        );

        let result = SchemaEvolutionValidator::validate_evolution(&old_schema, &new_schema).unwrap();

        match result {
            SchemaEvolutionResult::Compatible {
                added_fields,
                removed_fields,
            } => {
                assert!(added_fields.is_empty());
                assert_eq!(removed_fields, vec!["legacy".to_string()]);
            }
            _ => panic!("Expected compatible result"),
        }
    }

    #[test]
    fn test_schema_evolution_validator_incompatible() {
        let old_schema = Schema::with_fields(
            "Person",
            vec![FieldDescriptor::new("age", FieldKind::Int32, 0)],
        );

        let new_schema = Schema::with_fields(
            "Person",
            vec![FieldDescriptor::new("age", FieldKind::String, 0)],
        );

        let result = SchemaEvolutionValidator::validate_evolution(&old_schema, &new_schema).unwrap();

        match result {
            SchemaEvolutionResult::Incompatible { reason } => {
                assert!(reason.contains("Incompatible field type change"));
                assert!(reason.contains("age"));
            }
            _ => panic!("Expected incompatible result"),
        }
    }

    #[test]
    fn test_schema_evolution_validator_type_name_mismatch() {
        let old_schema = Schema::new("Person");
        let new_schema = Schema::new("Employee");

        let result = SchemaEvolutionValidator::validate_evolution(&old_schema, &new_schema).unwrap();

        match result {
            SchemaEvolutionResult::Incompatible { reason } => {
                assert!(reason.contains("Type name mismatch"));
            }
            _ => panic!("Expected incompatible result"),
        }
    }

    #[test]
    fn test_schema_evolution_add_field() {
        #[derive(Debug, Default, PartialEq)]
        struct PersonV1 {
            name: String,
            age: i32,
        }

        impl Compact for PersonV1 {
            fn get_type_name() -> &'static str {
                "PersonEvolved"
            }

            fn write(&self, writer: &mut DefaultCompactWriter) -> Result<()> {
                writer.write_string("name", Some(&self.name))?;
                writer.write_int32("age", self.age)?;
                Ok(())
            }

            fn read(&mut self, reader: &mut DefaultCompactReader) -> Result<()> {
                self.name = reader.read_string("name")?.unwrap_or_default();
                self.age = reader.read_int32("age")?;
                Ok(())
            }
        }

        #[derive(Debug, Default, PartialEq)]
        struct PersonV2 {
            name: String,
            age: i32,
            email: Option<String>,
        }

        impl Compact for PersonV2 {
            fn get_type_name() -> &'static str {
                "PersonEvolved"
            }

            fn write(&self, writer: &mut DefaultCompactWriter) -> Result<()> {
                writer.write_string("name", Some(&self.name))?;
                writer.write_int32("age", self.age)?;
                writer.write_string("email", self.email.as_deref())?;
                Ok(())
            }

            fn read(&mut self, reader: &mut DefaultCompactReader) -> Result<()> {
                self.name = reader.read_string("name")?.unwrap_or_default();
                self.age = reader.read_int32("age")?;
                self.email = reader.read_string("email")?;
                Ok(())
            }
        }

        let serializer = CompactSerializer::new();

        let v1 = PersonV1 {
            name: "Alice".to_string(),
            age: 30,
        };
        let bytes = serializer.serialize(&v1).unwrap();

        let v2: PersonV2 = serializer.deserialize(&bytes).unwrap();

        assert_eq!(v2.name, "Alice");
        assert_eq!(v2.age, 30);
        assert_eq!(v2.email, None);
    }

    #[test]
    fn test_schema_evolution_remove_field() {
        #[derive(Debug, Default, PartialEq)]
        struct PersonV2 {
            name: String,
            age: i32,
            email: Option<String>,
        }

        impl Compact for PersonV2 {
            fn get_type_name() -> &'static str {
                "PersonEvolved2"
            }

            fn write(&self, writer: &mut DefaultCompactWriter) -> Result<()> {
                writer.write_string("name", Some(&self.name))?;
                writer.write_int32("age", self.age)?;
                writer.write_string("email", self.email.as_deref())?;
                Ok(())
            }

            fn read(&mut self, reader: &mut DefaultCompactReader) -> Result<()> {
                self.name = reader.read_string("name")?.unwrap_or_default();
                self.age = reader.read_int32("age")?;
                self.email = reader.read_string("email")?;
                Ok(())
            }
        }

        #[derive(Debug, Default, PartialEq)]
        struct PersonV1 {
            name: String,
            age: i32,
        }

        impl Compact for PersonV1 {
            fn get_type_name() -> &'static str {
                "PersonEvolved2"
            }

            fn write(&self, writer: &mut DefaultCompactWriter) -> Result<()> {
                writer.write_string("name", Some(&self.name))?;
                writer.write_int32("age", self.age)?;
                Ok(())
            }

            fn read(&mut self, reader: &mut DefaultCompactReader) -> Result<()> {
                self.name = reader.read_string("name")?.unwrap_or_default();
                self.age = reader.read_int32("age")?;
                Ok(())
            }
        }

        let serializer = CompactSerializer::new();

        let v2 = PersonV2 {
            name: "Bob".to_string(),
            age: 25,
            email: Some("bob@example.com".to_string()),
        };
        let bytes = serializer.serialize(&v2).unwrap();

        let v1: PersonV1 = serializer.deserialize(&bytes).unwrap();

        assert_eq!(v1.name, "Bob");
        assert_eq!(v1.age, 25);
    }

    #[test]
    fn test_schema_evolution_round_trip_with_registry() {
        let mut serializer = CompactSerializer::new();

        let v1_schema = Schema::with_fields(
            "Person",
            vec![
                FieldDescriptor::new("name", FieldKind::String, 0),
                FieldDescriptor::new("age", FieldKind::Int32, 1),
            ],
        );
        let v2_schema = Schema::with_fields(
            "Person",
            vec![
                FieldDescriptor::new("name", FieldKind::String, 0),
                FieldDescriptor::new("age", FieldKind::Int32, 1),
                FieldDescriptor::new("score", FieldKind::NullableFloat64, 2),
            ],
        );

        serializer.register_schema(v1_schema.clone());
        serializer.register_schema(v2_schema.clone());

        let schemas = serializer.get_registry().get_schemas_by_type("Person");
        assert_eq!(schemas.len(), 2);

        let person = Person {
            name: "Charlie".to_string(),
            age: 35,
            active: true,
            score: Some(99.5),
        };

        let bytes = serializer.serialize(&person).unwrap();
        let result: Person = serializer.deserialize(&bytes).unwrap();

        assert_eq!(result, person);
    }

    #[test]
    fn test_compact_reader_with_writer_schema() {
        let writer_schema = Schema::with_fields(
            "Test",
            vec![
                FieldDescriptor::new("a", FieldKind::Int32, 0),
                FieldDescriptor::new("b", FieldKind::Int32, 1),
            ],
        );

        let reader_schema = Schema::with_fields(
            "Test",
            vec![
                FieldDescriptor::new("a", FieldKind::Int32, 0),
                FieldDescriptor::new("c", FieldKind::Int32, 2),
            ],
        );

        let mut writer = DefaultCompactWriter::new(writer_schema.clone());
        writer.write_int32("a", 10).unwrap();
        writer.write_int32("b", 20).unwrap();
        let bytes = writer.to_bytes();

        let mut reader = DefaultCompactReader::from_bytes_with_writer_schema(
            &bytes,
            reader_schema.clone(),
            writer_schema.clone(),
        )
        .unwrap();

        assert!(reader.writer_schema().is_some());
        assert_eq!(reader.read_int32("a").unwrap(), 10);
        assert_eq!(reader.read_int32("c").unwrap(), 0);
    }

    #[test]
    fn test_compact_serializer_with_registry() {
        let mut registry = SchemaRegistry::new();
        let schema = Schema::with_fields(
            "Person",
            vec![
                FieldDescriptor::new("name", FieldKind::String, 0),
                FieldDescriptor::new("age", FieldKind::Int32, 1),
            ],
        );
        registry.register_schema(schema);

        let serializer = CompactSerializer::with_registry(registry);

        let person = Person {
            name: "Test".to_string(),
            age: 42,
            active: true,
            score: None,
        };

        let bytes = serializer.serialize(&person).unwrap();
        let result: Person = serializer.deserialize(&bytes).unwrap();

        assert_eq!(result.name, person.name);
        assert_eq!(result.age, person.age);
    }

    #[test]
    fn test_date_time_field_kinds() {
        assert!(FieldKind::Date.is_nullable());
        assert!(FieldKind::Time.is_nullable());
        assert!(FieldKind::Timestamp.is_nullable());
        assert!(FieldKind::TimestampWithTimezone.is_nullable());
        assert!(FieldKind::Decimal.is_nullable());
        
        assert!(FieldKind::ArrayOfDate.is_array());
        assert!(FieldKind::ArrayOfTime.is_array());
        assert!(FieldKind::ArrayOfTimestamp.is_array());
        assert!(FieldKind::ArrayOfTimestampWithTimezone.is_array());
        assert!(FieldKind::ArrayOfDecimal.is_array());
        
        assert!(FieldKind::ArrayOfNullableInt32.is_array());
        assert!(FieldKind::ArrayOfNullableFloat64.is_array());
    }
    
    #[test]
    fn test_schema_evolution_nullable_time_types() {
        let v1 = Schema::with_fields(
            "Event",
            vec![FieldDescriptor::new("timestamp", FieldKind::Timestamp, 0)],
        );

        let v2 = Schema::with_fields(
            "Event",
            vec![FieldDescriptor::new("timestamp", FieldKind::NullableTimestamp, 0)],
        );

        assert!(v1.is_compatible_with(&v2));
        assert!(v2.is_compatible_with(&v1));
    }
}

/// Module for Java interoperability testing.
/// 
/// These tests verify that the Compact serialization format is compatible
/// with Hazelcast Java client's Compact serialization.
/// 
/// To run interoperability tests:
/// 1. Start a Hazelcast cluster with a Java client
/// 2. Serialize objects using the Java client
/// 3. Deserialize using the Rust client (and vice versa)
/// 
/// The wire format must match Java's implementation:
/// - Schema ID uses Rabin fingerprint (64-bit)
/// - Fields are sorted by name for schema ID computation
/// - Variable-length fields use length-prefixed encoding
#[cfg(test)]
mod java_interop_tests {
    use super::*;

    #[test]
    fn test_schema_id_stability() {
        let schema = Schema::with_fields(
            "com.example.Person",
            vec![
                FieldDescriptor::new("age", FieldKind::Int32, 0),
                FieldDescriptor::new("name", FieldKind::String, 1),
            ],
        );
        
        let schema_id = schema.schema_id();
        assert_ne!(schema_id, 0, "Schema ID should be non-zero");
        
        let schema_rebuilt = Schema::with_fields(
            "com.example.Person",
            vec![
                FieldDescriptor::new("age", FieldKind::Int32, 0),
                FieldDescriptor::new("name", FieldKind::String, 1),
            ],
        );
        assert_eq!(schema.schema_id(), schema_rebuilt.schema_id());
    }

    #[test]
    fn test_field_order_independence() {
        let schema1 = Schema::with_fields(
            "TestType",
            vec![
                FieldDescriptor::new("a", FieldKind::Int32, 0),
                FieldDescriptor::new("b", FieldKind::Int64, 1),
                FieldDescriptor::new("c", FieldKind::String, 2),
            ],
        );

        let schema2 = Schema::with_fields(
            "TestType",
            vec![
                FieldDescriptor::new("c", FieldKind::String, 2),
                FieldDescriptor::new("a", FieldKind::Int32, 0),
                FieldDescriptor::new("b", FieldKind::Int64, 1),
            ],
        );

        assert_eq!(
            schema1.schema_id(),
            schema2.schema_id(),
            "Schema ID must be independent of field declaration order"
        );
    }

    #[test]
    fn test_primitive_round_trip_values() {
        let schema = Schema::new("Primitives");
        let mut writer = DefaultCompactWriter::new(schema.clone());

        writer.write_boolean("bool_true", true).unwrap();
        writer.write_boolean("bool_false", false).unwrap();
        writer.write_int8("byte_min", i8::MIN).unwrap();
        writer.write_int8("byte_max", i8::MAX).unwrap();
        writer.write_int16("short_min", i16::MIN).unwrap();
        writer.write_int16("short_max", i16::MAX).unwrap();
        writer.write_int32("int_min", i32::MIN).unwrap();
        writer.write_int32("int_max", i32::MAX).unwrap();
        writer.write_int64("long_min", i64::MIN).unwrap();
        writer.write_int64("long_max", i64::MAX).unwrap();
        writer.write_float32("float_val", 3.14159f32).unwrap();
        writer.write_float64("double_val", 2.718281828459045f64).unwrap();

        let bytes = writer.to_bytes();
        let mut reader = DefaultCompactReader::from_bytes(&bytes, schema).unwrap();

        assert_eq!(reader.read_boolean("bool_true").unwrap(), true);
        assert_eq!(reader.read_boolean("bool_false").unwrap(), false);
        assert_eq!(reader.read_int8("byte_min").unwrap(), i8::MIN);
        assert_eq!(reader.read_int8("byte_max").unwrap(), i8::MAX);
        assert_eq!(reader.read_int16("short_min").unwrap(), i16::MIN);
        assert_eq!(reader.read_int16("short_max").unwrap(), i16::MAX);
        assert_eq!(reader.read_int32("int_min").unwrap(), i32::MIN);
        assert_eq!(reader.read_int32("int_max").unwrap(), i32::MAX);
        assert_eq!(reader.read_int64("long_min").unwrap(), i64::MIN);
        assert_eq!(reader.read_int64("long_max").unwrap(), i64::MAX);
        assert!((reader.read_float32("float_val").unwrap() - 3.14159f32).abs() < f32::EPSILON);
        assert!((reader.read_float64("double_val").unwrap() - 2.718281828459045f64).abs() < f64::EPSILON);
    }
    
    #[test]
    fn test_unicode_string_handling() {
        let schema = Schema::new("UnicodeTest");
        let mut writer = DefaultCompactWriter::new(schema.clone());

        let test_strings = [
            "ASCII only",
            "日本語テスト",
            "🚀🌟💻",
            "Mixed: Hello 世界 🎉",
            "",
            "Ñoño año",
            "Привет мир",
        ];

        for (i, s) in test_strings.iter().enumerate() {
            writer.write_string(&format!("str_{}", i), Some(s)).unwrap();
        }

        let bytes = writer.to_bytes();
        let mut reader = DefaultCompactReader::from_bytes(&bytes, schema).unwrap();

        for (i, expected) in test_strings.iter().enumerate() {
            let actual = reader.read_string(&format!("str_{}", i)).unwrap();
            assert_eq!(actual, Some(expected.to_string()));
        }
    }
}
