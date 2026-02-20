//! GenericRecord for schema-less Compact serialization access.

use crate::error::{HazelcastError, Result};
use crate::serialization::{DataInput, DataOutput, ObjectDataInput, ObjectDataOutput};
use std::collections::HashMap;

use super::FieldKind;

const NOT_NULL_MARKER: i8 = 1;

#[derive(Debug, Clone)]
struct FieldData {
    kind: FieldKind,
    data: Vec<u8>,
}

/// A schema-less record for reading Compact-serialized data dynamically.
///
/// `GenericRecord` allows reading fields from Compact-serialized data without
/// knowing the schema at compile time. This is useful for:
/// - Generic data processing pipelines
/// - Data inspection and debugging
/// - Handling data with evolving schemas
#[derive(Debug, Clone)]
pub struct GenericRecord {
    type_name: String,
    schema_id: i64,
    fields: HashMap<String, FieldData>,
}

impl GenericRecord {
    /// Creates a GenericRecord from Compact-serialized bytes.
    ///
    /// The bytes should be in the format produced by `CompactSerializer::serialize()`.
    pub fn from_compact_bytes(data: &[u8]) -> Result<Self> {
        let mut input = ObjectDataInput::new(data);

        let type_name = input.read_string()?;
        let schema_id = input.read_long()?;
        let field_data_len = input.read_int()?;
        if field_data_len < 0 {
            return Err(HazelcastError::Serialization(format!(
                "invalid field data length: {}",
                field_data_len
            )));
        }
        let field_data = input.read_bytes(field_data_len as usize)?;

        Self::from_field_data(&type_name, schema_id, &field_data)
    }

    /// Creates a GenericRecord from raw field data bytes.
    ///
    /// This is the format produced by `DefaultCompactWriter::to_bytes()`.
    pub fn from_field_data(type_name: &str, schema_id: i64, data: &[u8]) -> Result<Self> {
        let mut input = ObjectDataInput::new(data);
        let mut fields = HashMap::new();

        let field_count = input.read_int()?;
        if field_count < 0 {
            return Err(HazelcastError::Serialization(format!(
                "invalid field count: {}",
                field_count
            )));
        }

        for _ in 0..field_count {
            let name = input.read_string()?;
            let kind_id = input.read_int()?;
            let kind = FieldKind::from_id(kind_id)?;
            let is_present = input.read_byte()?;

            if is_present == NOT_NULL_MARKER {
                let data_len = input.read_int()?;
                if data_len < 0 {
                    return Err(HazelcastError::Serialization(format!(
                        "invalid data length for field '{}': {}",
                        name, data_len
                    )));
                }
                let field_bytes = input.read_bytes(data_len as usize)?;
                fields.insert(name, FieldData { kind, data: field_bytes });
            }
        }

        Ok(Self {
            type_name: type_name.to_string(),
            schema_id,
            fields,
        })
    }

    /// Returns the type name of this record.
    pub fn type_name(&self) -> &str {
        &self.type_name
    }

    /// Returns the schema ID of this record.
    pub fn schema_id(&self) -> i64 {
        self.schema_id
    }

    /// Returns true if this record has a field with the given name.
    pub fn has_field(&self, name: &str) -> bool {
        self.fields.contains_key(name)
    }

    /// Returns the field kind for the given field name, if it exists.
    pub fn get_field_kind(&self, name: &str) -> Option<FieldKind> {
        self.fields.get(name).map(|f| f.kind)
    }

    /// Returns an iterator over all field names.
    pub fn field_names(&self) -> impl Iterator<Item = &str> {
        self.fields.keys().map(|s| s.as_str())
    }

    /// Returns the number of fields in this record.
    pub fn field_count(&self) -> usize {
        self.fields.len()
    }

    fn get_field_data(&self, name: &str, expected_kind: FieldKind) -> Result<Option<&[u8]>> {
        match self.fields.get(name) {
            Some(field) => {
                if field.kind != expected_kind {
                    return Err(HazelcastError::Serialization(format!(
                        "field '{}' kind mismatch: expected {:?}, got {:?}",
                        name, expected_kind, field.kind
                    )));
                }
                Ok(Some(&field.data))
            }
            None => Ok(None),
        }
    }

    /// Gets a boolean field value.
    pub fn get_boolean(&self, name: &str) -> Result<bool> {
        match self.get_field_data(name, FieldKind::Boolean)? {
            Some(data) => {
                let mut input = ObjectDataInput::new(data);
                input.read_bool()
            }
            None => Ok(false),
        }
    }

    /// Gets an i8 field value.
    pub fn get_int8(&self, name: &str) -> Result<i8> {
        match self.get_field_data(name, FieldKind::Int8)? {
            Some(data) => {
                let mut input = ObjectDataInput::new(data);
                input.read_byte()
            }
            None => Ok(0),
        }
    }

    /// Gets an i16 field value.
    pub fn get_int16(&self, name: &str) -> Result<i16> {
        match self.get_field_data(name, FieldKind::Int16)? {
            Some(data) => {
                let mut input = ObjectDataInput::new(data);
                input.read_short()
            }
            None => Ok(0),
        }
    }

    /// Gets an i32 field value.
    pub fn get_int32(&self, name: &str) -> Result<i32> {
        match self.get_field_data(name, FieldKind::Int32)? {
            Some(data) => {
                let mut input = ObjectDataInput::new(data);
                input.read_int()
            }
            None => Ok(0),
        }
    }

    /// Gets an i64 field value.
    pub fn get_int64(&self, name: &str) -> Result<i64> {
        match self.get_field_data(name, FieldKind::Int64)? {
            Some(data) => {
                let mut input = ObjectDataInput::new(data);
                input.read_long()
            }
            None => Ok(0),
        }
    }

    /// Gets an f32 field value.
    pub fn get_float32(&self, name: &str) -> Result<f32> {
        match self.get_field_data(name, FieldKind::Float32)? {
            Some(data) => {
                let mut input = ObjectDataInput::new(data);
                input.read_float()
            }
            None => Ok(0.0),
        }
    }

    /// Gets an f64 field value.
    pub fn get_float64(&self, name: &str) -> Result<f64> {
        match self.get_field_data(name, FieldKind::Float64)? {
            Some(data) => {
                let mut input = ObjectDataInput::new(data);
                input.read_double()
            }
            None => Ok(0.0),
        }
    }

    /// Gets a string field value.
    pub fn get_string(&self, name: &str) -> Result<Option<String>> {
        match self.get_field_data(name, FieldKind::String)? {
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

    /// Gets a nullable boolean field value.
    pub fn get_nullable_boolean(&self, name: &str) -> Result<Option<bool>> {
        match self.get_field_data(name, FieldKind::NullableBoolean)? {
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

    /// Gets a nullable i8 field value.
    pub fn get_nullable_int8(&self, name: &str) -> Result<Option<i8>> {
        match self.get_field_data(name, FieldKind::NullableInt8)? {
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

    /// Gets a nullable i16 field value.
    pub fn get_nullable_int16(&self, name: &str) -> Result<Option<i16>> {
        match self.get_field_data(name, FieldKind::NullableInt16)? {
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

    /// Gets a nullable i32 field value.
    pub fn get_nullable_int32(&self, name: &str) -> Result<Option<i32>> {
        match self.get_field_data(name, FieldKind::NullableInt32)? {
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

    /// Gets a nullable i64 field value.
    pub fn get_nullable_int64(&self, name: &str) -> Result<Option<i64>> {
        match self.get_field_data(name, FieldKind::NullableInt64)? {
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

    /// Gets a nullable f32 field value.
    pub fn get_nullable_float32(&self, name: &str) -> Result<Option<f32>> {
        match self.get_field_data(name, FieldKind::NullableFloat32)? {
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

    /// Gets a nullable f64 field value.
    pub fn get_nullable_float64(&self, name: &str) -> Result<Option<f64>> {
        match self.get_field_data(name, FieldKind::NullableFloat64)? {
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

    /// Gets an array of boolean field values.
    pub fn get_array_of_boolean(&self, name: &str) -> Result<Option<Vec<bool>>> {
        self.read_array(name, FieldKind::ArrayOfBoolean, |input| input.read_bool())
    }

    /// Gets an array of i8 field values.
    pub fn get_array_of_int8(&self, name: &str) -> Result<Option<Vec<i8>>> {
        self.read_array(name, FieldKind::ArrayOfInt8, |input| input.read_byte())
    }

    /// Gets an array of i16 field values.
    pub fn get_array_of_int16(&self, name: &str) -> Result<Option<Vec<i16>>> {
        self.read_array(name, FieldKind::ArrayOfInt16, |input| input.read_short())
    }

    /// Gets an array of i32 field values.
    pub fn get_array_of_int32(&self, name: &str) -> Result<Option<Vec<i32>>> {
        self.read_array(name, FieldKind::ArrayOfInt32, |input| input.read_int())
    }

    /// Gets an array of i64 field values.
    pub fn get_array_of_int64(&self, name: &str) -> Result<Option<Vec<i64>>> {
        self.read_array(name, FieldKind::ArrayOfInt64, |input| input.read_long())
    }

    /// Gets an array of f32 field values.
    pub fn get_array_of_float32(&self, name: &str) -> Result<Option<Vec<f32>>> {
        self.read_array(name, FieldKind::ArrayOfFloat32, |input| input.read_float())
    }

    /// Gets an array of f64 field values.
    pub fn get_array_of_float64(&self, name: &str) -> Result<Option<Vec<f64>>> {
        self.read_array(name, FieldKind::ArrayOfFloat64, |input| input.read_double())
    }

    /// Gets an array of string field values.
    pub fn get_array_of_string(&self, name: &str) -> Result<Option<Vec<Option<String>>>> {
        match self.get_field_data(name, FieldKind::ArrayOfString)? {
            Some(data) => {
                let mut input = ObjectDataInput::new(data);
                let has_value = input.read_bool()?;
                if !has_value {
                    return Ok(None);
                }
                let len = input.read_int()?;
                if len < 0 {
                    return Err(HazelcastError::Serialization(format!(
                        "invalid array length: {}",
                        len
                    )));
                }
                let mut result = Vec::with_capacity(len as usize);
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

    /// Gets a nested GenericRecord field value.
    pub fn get_generic_record(&self, name: &str) -> Result<Option<GenericRecord>> {
        match self.get_field_data(name, FieldKind::Compact)? {
            Some(data) => {
                let mut input = ObjectDataInput::new(data);
                let has_value = input.read_bool()?;
                if !has_value {
                    return Ok(None);
                }

                let type_name = input.read_string()?;
                let nested_len = input.read_int()?;
                if nested_len < 0 {
                    return Err(HazelcastError::Serialization(format!(
                        "invalid nested data length: {}",
                        nested_len
                    )));
                }
                let nested_data = input.read_bytes(nested_len as usize)?;

                Ok(Some(GenericRecord::from_field_data(&type_name, 0, &nested_data)?))
            }
            None => Ok(None),
        }
    }

    /// Gets an array of nested GenericRecord field values.
    pub fn get_array_of_generic_record(&self, name: &str) -> Result<Option<Vec<GenericRecord>>> {
        match self.get_field_data(name, FieldKind::ArrayOfCompact)? {
            Some(data) => {
                let mut input = ObjectDataInput::new(data);
                let has_value = input.read_bool()?;
                if !has_value {
                    return Ok(None);
                }

                let len = input.read_int()?;
                if len < 0 {
                    return Err(HazelcastError::Serialization(format!(
                        "invalid array length: {}",
                        len
                    )));
                }
                let mut result = Vec::with_capacity(len as usize);

                for _ in 0..len {
                    let type_name = input.read_string()?;
                    let nested_len = input.read_int()?;
                    if nested_len < 0 {
                        return Err(HazelcastError::Serialization(format!(
                            "invalid nested data length: {}",
                            nested_len
                        )));
                    }
                    let nested_data = input.read_bytes(nested_len as usize)?;
                    result.push(GenericRecord::from_field_data(&type_name, 0, &nested_data)?);
                }

                Ok(Some(result))
            }
            None => Ok(None),
        }
    }

    fn read_array<T, F>(&self, name: &str, kind: FieldKind, read_element: F) -> Result<Option<Vec<T>>>
    where
        F: Fn(&mut ObjectDataInput) -> Result<T>,
    {
        match self.get_field_data(name, kind)? {
            Some(data) => {
                let mut input = ObjectDataInput::new(data);
                let has_value = input.read_bool()?;
                if !has_value {
                    return Ok(None);
                }
                let len = input.read_int()?;
                if len < 0 {
                    return Err(HazelcastError::Serialization(format!(
                        "invalid array length: {}",
                        len
                    )));
                }
                let mut result = Vec::with_capacity(len as usize);
                for _ in 0..len {
                    result.push(read_element(&mut input)?);
                }
                Ok(Some(result))
            }
            None => Ok(None),
        }
    }
}

/// Builder for creating `GenericRecord` instances programmatically.
#[derive(Debug)]
pub struct GenericRecordBuilder {
    type_name: String,
    fields: HashMap<String, FieldData>,
    field_order: Vec<String>,
}

impl GenericRecordBuilder {
    /// Creates a new builder for the given type name.
    pub fn new(type_name: impl Into<String>) -> Self {
        Self {
            type_name: type_name.into(),
            fields: HashMap::new(),
            field_order: Vec::new(),
        }
    }

    fn set_field(&mut self, name: &str, kind: FieldKind, data: Vec<u8>) {
        if !self.fields.contains_key(name) {
            self.field_order.push(name.to_string());
        }
        self.fields.insert(name.to_string(), FieldData { kind, data });
    }

    /// Sets a boolean field.
    pub fn set_boolean(mut self, name: &str, value: bool) -> Self {
        let mut out = ObjectDataOutput::new();
        let _ = out.write_bool(value);
        self.set_field(name, FieldKind::Boolean, out.into_bytes());
        self
    }

    /// Sets an i8 field.
    pub fn set_int8(mut self, name: &str, value: i8) -> Self {
        let mut out = ObjectDataOutput::new();
        let _ = out.write_byte(value);
        self.set_field(name, FieldKind::Int8, out.into_bytes());
        self
    }

    /// Sets an i16 field.
    pub fn set_int16(mut self, name: &str, value: i16) -> Self {
        let mut out = ObjectDataOutput::new();
        let _ = out.write_short(value);
        self.set_field(name, FieldKind::Int16, out.into_bytes());
        self
    }

    /// Sets an i32 field.
    pub fn set_int32(mut self, name: &str, value: i32) -> Self {
        let mut out = ObjectDataOutput::new();
        let _ = out.write_int(value);
        self.set_field(name, FieldKind::Int32, out.into_bytes());
        self
    }

    /// Sets an i64 field.
    pub fn set_int64(mut self, name: &str, value: i64) -> Self {
        let mut out = ObjectDataOutput::new();
        let _ = out.write_long(value);
        self.set_field(name, FieldKind::Int64, out.into_bytes());
        self
    }

    /// Sets an f32 field.
    pub fn set_float32(mut self, name: &str, value: f32) -> Self {
        let mut out = ObjectDataOutput::new();
        let _ = out.write_float(value);
        self.set_field(name, FieldKind::Float32, out.into_bytes());
        self
    }

    /// Sets an f64 field.
    pub fn set_float64(mut self, name: &str, value: f64) -> Self {
        let mut out = ObjectDataOutput::new();
        let _ = out.write_double(value);
        self.set_field(name, FieldKind::Float64, out.into_bytes());
        self
    }

    /// Sets a string field.
    pub fn set_string(mut self, name: &str, value: Option<&str>) -> Self {
        let mut out = ObjectDataOutput::new();
        match value {
            Some(s) => {
                let _ = out.write_bool(true);
                let _ = out.write_string(s);
            }
            None => {
                let _ = out.write_bool(false);
            }
        }
        self.set_field(name, FieldKind::String, out.into_bytes());
        self
    }

    /// Sets an array of i32 field.
    pub fn set_array_of_int32(mut self, name: &str, value: Option<&[i32]>) -> Self {
        let mut out = ObjectDataOutput::new();
        match value {
            Some(arr) => {
                let _ = out.write_bool(true);
                let _ = out.write_int(arr.len() as i32);
                for &v in arr {
                    let _ = out.write_int(v);
                }
            }
            None => {
                let _ = out.write_bool(false);
            }
        }
        self.set_field(name, FieldKind::ArrayOfInt32, out.into_bytes());
        self
    }

    /// Sets an array of string field.
    pub fn set_array_of_string(mut self, name: &str, value: Option<&[Option<String>]>) -> Self {
        let mut out = ObjectDataOutput::new();
        match value {
            Some(arr) => {
                let _ = out.write_bool(true);
                let _ = out.write_int(arr.len() as i32);
                for s in arr {
                    match s {
                        Some(v) => {
                            let _ = out.write_bool(true);
                            let _ = out.write_string(v);
                        }
                        None => {
                            let _ = out.write_bool(false);
                        }
                    }
                }
            }
            None => {
                let _ = out.write_bool(false);
            }
        }
        self.set_field(name, FieldKind::ArrayOfString, out.into_bytes());
        self
    }

    /// Builds the GenericRecord.
    pub fn build(self) -> GenericRecord {
        GenericRecord {
            type_name: self.type_name,
            schema_id: 0,
            fields: self.fields,
        }
    }

    /// Serializes the record to Compact format bytes.
    pub fn to_compact_bytes(&self) -> Vec<u8> {
        let field_data = self.field_data_bytes();

        let mut output = ObjectDataOutput::new();
        let _ = output.write_string(&self.type_name);
        let _ = output.write_long(0);
        let _ = output.write_int(field_data.len() as i32);
        let _ = output.write_bytes(&field_data);

        output.into_bytes()
    }

    fn field_data_bytes(&self) -> Vec<u8> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serialization::compact::{Compact, CompactReader, CompactSerializer, CompactWriter, Schema};

    #[test]
    fn test_generic_record_from_compact_bytes() {
        #[derive(Debug, Default)]
        struct Person {
            name: String,
            age: i32,
        }

        impl Compact for Person {
            fn get_type_name() -> &'static str {
                "Person"
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
        let person = Person {
            name: "Alice".to_string(),
            age: 30,
        };

        let bytes = serializer.serialize(&person).unwrap();
        let record = GenericRecord::from_compact_bytes(&bytes).unwrap();

        assert_eq!(record.type_name(), "Person");
        assert_eq!(record.get_string("name").unwrap(), Some("Alice".to_string()));
        assert_eq!(record.get_int32("age").unwrap(), 30);
    }

    #[test]
    fn test_generic_record_field_inspection() {
        let record = GenericRecordBuilder::new("Test")
            .set_int32("count", 42)
            .set_string("label", Some("hello"))
            .set_boolean("active", true)
            .build();

        assert!(record.has_field("count"));
        assert!(record.has_field("label"));
        assert!(record.has_field("active"));
        assert!(!record.has_field("missing"));

        assert_eq!(record.get_field_kind("count"), Some(FieldKind::Int32));
        assert_eq!(record.get_field_kind("label"), Some(FieldKind::String));
        assert_eq!(record.get_field_kind("missing"), None);

        assert_eq!(record.field_count(), 3);
    }

    #[test]
    fn test_generic_record_primitives() {
        let record = GenericRecordBuilder::new("Primitives")
            .set_boolean("bool", true)
            .set_int8("i8", -42)
            .set_int16("i16", 1000)
            .set_int32("i32", 100_000)
            .set_int64("i64", 10_000_000_000)
            .set_float32("f32", 3.14)
            .set_float64("f64", 2.718281828)
            .build();

        assert_eq!(record.get_boolean("bool").unwrap(), true);
        assert_eq!(record.get_int8("i8").unwrap(), -42);
        assert_eq!(record.get_int16("i16").unwrap(), 1000);
        assert_eq!(record.get_int32("i32").unwrap(), 100_000);
        assert_eq!(record.get_int64("i64").unwrap(), 10_000_000_000);
        assert!((record.get_float32("f32").unwrap() - 3.14).abs() < 0.001);
        assert!((record.get_float64("f64").unwrap() - 2.718281828).abs() < 0.0001);
    }

    #[test]
    fn test_generic_record_missing_field_defaults() {
        let record = GenericRecordBuilder::new("Empty").build();

        assert_eq!(record.get_boolean("missing").unwrap(), false);
        assert_eq!(record.get_int32("missing").unwrap(), 0);
        assert_eq!(record.get_string("missing").unwrap(), None);
    }

    #[test]
    fn test_generic_record_field_kind_mismatch() {
        let record = GenericRecordBuilder::new("Test")
            .set_int32("value", 42)
            .build();

        let result = record.get_string("value");
        assert!(result.is_err());
    }

    #[test]
    fn test_generic_record_array_of_int32() {
        let record = GenericRecordBuilder::new("Test")
            .set_array_of_int32("numbers", Some(&[1, 2, 3, 4, 5]))
            .build();

        assert_eq!(
            record.get_array_of_int32("numbers").unwrap(),
            Some(vec![1, 2, 3, 4, 5])
        );
    }

    #[test]
    fn test_generic_record_null_array() {
        let record = GenericRecordBuilder::new("Test")
            .set_array_of_int32("numbers", None)
            .build();

        assert_eq!(record.get_array_of_int32("numbers").unwrap(), None);
    }

    #[test]
    fn test_generic_record_array_of_string() {
        let arr = vec![Some("a".to_string()), None, Some("c".to_string())];
        let record = GenericRecordBuilder::new("Test")
            .set_array_of_string("strings", Some(&arr))
            .build();

        assert_eq!(
            record.get_array_of_string("strings").unwrap(),
            Some(vec![Some("a".to_string()), None, Some("c".to_string())])
        );
    }

    #[test]
    fn test_generic_record_builder_round_trip() {
        let bytes = GenericRecordBuilder::new("Person")
            .set_string("name", Some("Bob"))
            .set_int32("age", 25)
            .to_compact_bytes();

        let record = GenericRecord::from_compact_bytes(&bytes).unwrap();

        assert_eq!(record.type_name(), "Person");
        assert_eq!(record.get_string("name").unwrap(), Some("Bob".to_string()));
        assert_eq!(record.get_int32("age").unwrap(), 25);
    }

    #[test]
    fn test_generic_record_empty_bytes_error() {
        let result = GenericRecord::from_compact_bytes(&[]);
        assert!(result.is_err());
    }

    #[test]
    fn test_generic_record_truncated_bytes_error() {
        let result = GenericRecord::from_compact_bytes(&[0, 0, 0, 4, b't', b'e']);
        assert!(result.is_err());
    }

    #[test]
    fn test_generic_record_negative_field_count_error() {
        let mut out = ObjectDataOutput::new();
        let _ = out.write_string("Test");
        let _ = out.write_long(0);
        let _ = out.write_int(4);
        let _ = out.write_int(-1);

        let result = GenericRecord::from_compact_bytes(out.as_bytes());
        assert!(result.is_err());
    }

    #[test]
    fn test_generic_record_invalid_field_kind_error() {
        let mut field_data = ObjectDataOutput::new();
        let _ = field_data.write_int(1);
        let _ = field_data.write_string("field");
        let _ = field_data.write_int(999);
        let _ = field_data.write_byte(1);
        let _ = field_data.write_int(0);
        let field_bytes = field_data.into_bytes();

        let mut out = ObjectDataOutput::new();
        let _ = out.write_string("Test");
        let _ = out.write_long(0);
        let _ = out.write_int(field_bytes.len() as i32);
        let _ = out.write_bytes(&field_bytes);

        let result = GenericRecord::from_compact_bytes(out.as_bytes());
        assert!(result.is_err());
    }

    #[test]
    fn test_generic_record_negative_data_length_error() {
        let mut field_data = ObjectDataOutput::new();
        let _ = field_data.write_int(1);
        let _ = field_data.write_string("field");
        let _ = field_data.write_int(3);
        let _ = field_data.write_byte(1);
        let _ = field_data.write_int(-5);
        let field_bytes = field_data.into_bytes();

        let mut out = ObjectDataOutput::new();
        let _ = out.write_string("Test");
        let _ = out.write_long(0);
        let _ = out.write_int(field_bytes.len() as i32);
        let _ = out.write_bytes(&field_bytes);

        let result = GenericRecord::from_compact_bytes(out.as_bytes());
        assert!(result.is_err());
    }

    #[test]
    fn test_generic_record_field_names_iterator() {
        let record = GenericRecordBuilder::new("Test")
            .set_int32("a", 1)
            .set_int32("b", 2)
            .set_int32("c", 3)
            .build();

        let names: Vec<&str> = record.field_names().collect();
        assert_eq!(names.len(), 3);
        assert!(names.contains(&"a"));
        assert!(names.contains(&"b"));
        assert!(names.contains(&"c"));
    }
}
