//! Default implementations of PortableReader and PortableWriter.

use super::{ClassDefinition, FieldType, Portable, PortableFactory, PortableReader, PortableWriter};
use crate::error::{HazelcastError, Result};
use crate::serialization::{DataInput, DataOutput, ObjectDataInput, ObjectDataOutput};
use std::collections::HashMap;
use std::sync::Arc;

const NULL_MARKER: i8 = 0;
const NOT_NULL_MARKER: i8 = 1;

/// Default implementation of `PortableWriter`.
#[derive(Debug)]
pub struct DefaultPortableWriter {
    class_def: ClassDefinition,
    fields: HashMap<String, Vec<u8>>,
}

impl DefaultPortableWriter {
    /// Creates a new writer for the given class definition.
    pub fn new(class_def: ClassDefinition) -> Self {
        Self {
            class_def,
            fields: HashMap::new(),
        }
    }

    /// Returns the class definition.
    pub fn class_definition(&self) -> &ClassDefinition {
        &self.class_def
    }

    fn write_field(&mut self, name: &str, expected_type: FieldType, data: Vec<u8>) -> Result<()> {
        let field = self.class_def.field(name).ok_or_else(|| {
            HazelcastError::Serialization(format!("Unknown field: {}", name))
        })?;

        if field.field_type() != expected_type {
            return Err(HazelcastError::Serialization(format!(
                "Field '{}' type mismatch: expected {:?}, got {:?}",
                name,
                field.field_type(),
                expected_type
            )));
        }

        self.fields.insert(name.to_string(), data);
        Ok(())
    }

    /// Converts the written data into bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut output = ObjectDataOutput::new();

        let _ = output.write_int(self.class_def.field_count() as i32);

        for field_def in self.class_def.fields() {
            let name = field_def.name();
            let _ = output.write_string(name);
            let _ = output.write_int(field_def.field_type().id());

            if let Some(data) = self.fields.get(name) {
                let _ = output.write_byte(NOT_NULL_MARKER);
                let _ = output.write_int(data.len() as i32);
                let _ = output.write_bytes(data);
            } else {
                let _ = output.write_byte(NULL_MARKER);
            }
        }

        output.into_bytes()
    }
}

impl PortableWriter for DefaultPortableWriter {
    fn write_byte(&mut self, name: &str, value: i8) -> Result<()> {
        let mut out = ObjectDataOutput::new();
        out.write_byte(value)?;
        self.write_field(name, FieldType::Byte, out.into_bytes())
    }

    fn write_bool(&mut self, name: &str, value: bool) -> Result<()> {
        let mut out = ObjectDataOutput::new();
        out.write_bool(value)?;
        self.write_field(name, FieldType::Bool, out.into_bytes())
    }

    fn write_char(&mut self, name: &str, value: char) -> Result<()> {
        let mut out = ObjectDataOutput::new();
        out.write_short(value as i16)?;
        self.write_field(name, FieldType::Char, out.into_bytes())
    }

    fn write_short(&mut self, name: &str, value: i16) -> Result<()> {
        let mut out = ObjectDataOutput::new();
        out.write_short(value)?;
        self.write_field(name, FieldType::Short, out.into_bytes())
    }

    fn write_int(&mut self, name: &str, value: i32) -> Result<()> {
        let mut out = ObjectDataOutput::new();
        out.write_int(value)?;
        self.write_field(name, FieldType::Int, out.into_bytes())
    }

    fn write_long(&mut self, name: &str, value: i64) -> Result<()> {
        let mut out = ObjectDataOutput::new();
        out.write_long(value)?;
        self.write_field(name, FieldType::Long, out.into_bytes())
    }

    fn write_float(&mut self, name: &str, value: f32) -> Result<()> {
        let mut out = ObjectDataOutput::new();
        out.write_float(value)?;
        self.write_field(name, FieldType::Float, out.into_bytes())
    }

    fn write_double(&mut self, name: &str, value: f64) -> Result<()> {
        let mut out = ObjectDataOutput::new();
        out.write_double(value)?;
        self.write_field(name, FieldType::Double, out.into_bytes())
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
        self.write_field(name, FieldType::Utf8, out.into_bytes())
    }

    fn write_portable<P: Portable>(&mut self, name: &str, value: Option<&P>) -> Result<()> {
        let mut out = ObjectDataOutput::new();
        match value {
            Some(p) => {
                out.write_bool(true)?;
                out.write_int(p.factory_id())?;
                out.write_int(p.class_id())?;

                let mut nested_writer =
                    DefaultPortableWriter::new(ClassDefinition::new(p.factory_id(), p.class_id(), 0));
                p.write_portable(&mut nested_writer)?;
                let nested_bytes = nested_writer.to_bytes();
                out.write_int(nested_bytes.len() as i32)?;
                out.write_bytes(&nested_bytes)?;
            }
            None => {
                out.write_bool(false)?;
            }
        }
        self.write_field(name, FieldType::Portable, out.into_bytes())
    }

    fn write_byte_array(&mut self, name: &str, value: Option<&[i8]>) -> Result<()> {
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
        self.write_field(name, FieldType::ByteArray, out.into_bytes())
    }

    fn write_bool_array(&mut self, name: &str, value: Option<&[bool]>) -> Result<()> {
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
        self.write_field(name, FieldType::BoolArray, out.into_bytes())
    }

    fn write_char_array(&mut self, name: &str, value: Option<&[char]>) -> Result<()> {
        let mut out = ObjectDataOutput::new();
        match value {
            Some(arr) => {
                out.write_bool(true)?;
                out.write_int(arr.len() as i32)?;
                for &v in arr {
                    out.write_short(v as i16)?;
                }
            }
            None => {
                out.write_bool(false)?;
            }
        }
        self.write_field(name, FieldType::CharArray, out.into_bytes())
    }

    fn write_short_array(&mut self, name: &str, value: Option<&[i16]>) -> Result<()> {
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
        self.write_field(name, FieldType::ShortArray, out.into_bytes())
    }

    fn write_int_array(&mut self, name: &str, value: Option<&[i32]>) -> Result<()> {
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
        self.write_field(name, FieldType::IntArray, out.into_bytes())
    }

    fn write_long_array(&mut self, name: &str, value: Option<&[i64]>) -> Result<()> {
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
        self.write_field(name, FieldType::LongArray, out.into_bytes())
    }

    fn write_float_array(&mut self, name: &str, value: Option<&[f32]>) -> Result<()> {
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
        self.write_field(name, FieldType::FloatArray, out.into_bytes())
    }

    fn write_double_array(&mut self, name: &str, value: Option<&[f64]>) -> Result<()> {
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
        self.write_field(name, FieldType::DoubleArray, out.into_bytes())
    }

    fn write_string_array(&mut self, name: &str, value: Option<&[String]>) -> Result<()> {
        let mut out = ObjectDataOutput::new();
        match value {
            Some(arr) => {
                out.write_bool(true)?;
                out.write_int(arr.len() as i32)?;
                for s in arr {
                    out.write_string(s)?;
                }
            }
            None => {
                out.write_bool(false)?;
            }
        }
        self.write_field(name, FieldType::Utf8Array, out.into_bytes())
    }

    fn write_portable_array<P: Portable>(&mut self, name: &str, value: Option<&[P]>) -> Result<()> {
        let mut out = ObjectDataOutput::new();
        match value {
            Some(arr) => {
                out.write_bool(true)?;
                out.write_int(arr.len() as i32)?;
                for p in arr {
                    out.write_int(p.factory_id())?;
                    out.write_int(p.class_id())?;

                    let mut nested_writer = DefaultPortableWriter::new(ClassDefinition::new(
                        p.factory_id(),
                        p.class_id(),
                        0,
                    ));
                    p.write_portable(&mut nested_writer)?;
                    let nested_bytes = nested_writer.to_bytes();
                    out.write_int(nested_bytes.len() as i32)?;
                    out.write_bytes(&nested_bytes)?;
                }
            }
            None => {
                out.write_bool(false)?;
            }
        }
        self.write_field(name, FieldType::PortableArray, out.into_bytes())
    }
}

/// Stored field data for reading.
#[derive(Debug, Clone)]
struct FieldData {
    field_type: FieldType,
    data: Vec<u8>,
}

/// Default implementation of `PortableReader`.
#[derive(Debug)]
pub struct DefaultPortableReader {
    version: i32,
    fields: HashMap<String, FieldData>,
    factories: HashMap<i32, Arc<dyn PortableFactory>>,
}

impl DefaultPortableReader {
    /// Creates a new reader from serialized bytes.
    pub fn from_bytes(
        data: &[u8],
        version: i32,
        factories: HashMap<i32, Arc<dyn PortableFactory>>,
    ) -> Result<Self> {
        let mut input = ObjectDataInput::new(data);
        let mut fields = HashMap::new();

        let field_count = input.read_int()?;

        for _ in 0..field_count {
            let name = input.read_string()?;
            let field_type_id = input.read_int()?;
            let field_type = FieldType::from_id(field_type_id)?;
            let is_null = input.read_byte()?;

            if is_null == NOT_NULL_MARKER {
                let data_len = input.read_int()? as usize;
                let data = input.read_bytes(data_len)?;
                fields.insert(name, FieldData { field_type, data });
            }
        }

        Ok(Self {
            version,
            fields,
            factories,
        })
    }

    fn get_field(&self, name: &str, expected_type: FieldType) -> Result<Option<&[u8]>> {
        match self.fields.get(name) {
            Some(field_data) => {
                if field_data.field_type != expected_type {
                    return Err(HazelcastError::Serialization(format!(
                        "Field '{}' type mismatch: expected {:?}, got {:?}",
                        name, expected_type, field_data.field_type
                    )));
                }
                Ok(Some(&field_data.data))
            }
            None => Ok(None),
        }
    }

    fn read_nested_portable<P: Portable + Default>(
        &self,
        data: &[u8],
    ) -> Result<P> {
        let mut input = ObjectDataInput::new(data);
        let has_value = input.read_bool()?;
        if !has_value {
            return Err(HazelcastError::Serialization(
                "Expected nested portable but found null".to_string(),
            ));
        }

        let factory_id = input.read_int()?;
        let class_id = input.read_int()?;
        let nested_len = input.read_int()? as usize;
        let nested_data = input.read_bytes(nested_len)?;

        let nested_reader =
            DefaultPortableReader::from_bytes(&nested_data, 0, self.factories.clone())?;

        let mut instance = P::default();
        if instance.factory_id() != factory_id || instance.class_id() != class_id {
            return Err(HazelcastError::Serialization(format!(
                "Type mismatch: expected factory_id={}, class_id={}, got factory_id={}, class_id={}",
                instance.factory_id(),
                instance.class_id(),
                factory_id,
                class_id
            )));
        }

        instance.read_portable(&mut { nested_reader })?;
        Ok(instance)
    }
}

impl PortableReader for DefaultPortableReader {
    fn version(&self) -> i32 {
        self.version
    }

    fn has_field(&self, name: &str) -> bool {
        self.fields.contains_key(name)
    }

    fn read_byte(&mut self, name: &str) -> Result<i8> {
        match self.get_field(name, FieldType::Byte)? {
            Some(data) => {
                let mut input = ObjectDataInput::new(data);
                input.read_byte()
            }
            None => Ok(0),
        }
    }

    fn read_bool(&mut self, name: &str) -> Result<bool> {
        match self.get_field(name, FieldType::Bool)? {
            Some(data) => {
                let mut input = ObjectDataInput::new(data);
                input.read_bool()
            }
            None => Ok(false),
        }
    }

    fn read_char(&mut self, name: &str) -> Result<char> {
        match self.get_field(name, FieldType::Char)? {
            Some(data) => {
                let mut input = ObjectDataInput::new(data);
                let code = input.read_short()? as u32;
                char::from_u32(code).ok_or_else(|| {
                    HazelcastError::Serialization(format!("Invalid char code: {}", code))
                })
            }
            None => Ok('\0'),
        }
    }

    fn read_short(&mut self, name: &str) -> Result<i16> {
        match self.get_field(name, FieldType::Short)? {
            Some(data) => {
                let mut input = ObjectDataInput::new(data);
                input.read_short()
            }
            None => Ok(0),
        }
    }

    fn read_int(&mut self, name: &str) -> Result<i32> {
        match self.get_field(name, FieldType::Int)? {
            Some(data) => {
                let mut input = ObjectDataInput::new(data);
                input.read_int()
            }
            None => Ok(0),
        }
    }

    fn read_long(&mut self, name: &str) -> Result<i64> {
        match self.get_field(name, FieldType::Long)? {
            Some(data) => {
                let mut input = ObjectDataInput::new(data);
                input.read_long()
            }
            None => Ok(0),
        }
    }

    fn read_float(&mut self, name: &str) -> Result<f32> {
        match self.get_field(name, FieldType::Float)? {
            Some(data) => {
                let mut input = ObjectDataInput::new(data);
                input.read_float()
            }
            None => Ok(0.0),
        }
    }

    fn read_double(&mut self, name: &str) -> Result<f64> {
        match self.get_field(name, FieldType::Double)? {
            Some(data) => {
                let mut input = ObjectDataInput::new(data);
                input.read_double()
            }
            None => Ok(0.0),
        }
    }

    fn read_string(&mut self, name: &str) -> Result<Option<String>> {
        match self.get_field(name, FieldType::Utf8)? {
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

    fn read_portable<P: Portable>(&mut self, name: &str) -> Result<Option<P>> {
        match self.get_field(name, FieldType::Portable)? {
            Some(data) => {
                let mut input = ObjectDataInput::new(data);
                let has_value = input.read_bool()?;
                if !has_value {
                    return Ok(None);
                }

                let factory_id = input.read_int()?;
                let class_id = input.read_int()?;
                let nested_len = input.read_int()? as usize;
                let nested_data = input.read_bytes(nested_len)?;

                let factory = self.factories.get(&factory_id).ok_or_else(|| {
                    HazelcastError::Serialization(format!(
                        "No factory registered for factory_id={}",
                        factory_id
                    ))
                })?;

                let mut instance = factory.create(class_id).ok_or_else(|| {
                    HazelcastError::Serialization(format!(
                        "Factory {} cannot create class_id={}",
                        factory_id, class_id
                    ))
                })?;

                let mut nested_reader =
                    DefaultPortableReader::from_bytes(&nested_data, 0, self.factories.clone())?;
                instance.read_portable(&mut nested_reader)?;

                let instance_any = instance as Box<dyn std::any::Any>;
                match instance_any.downcast::<P>() {
                    Ok(typed) => Ok(Some(*typed)),
                    Err(_) => Err(HazelcastError::Serialization(
                        "Type mismatch when reading portable".to_string(),
                    )),
                }
            }
            None => Ok(None),
        }
    }

    fn read_byte_array(&mut self, name: &str) -> Result<Option<Vec<i8>>> {
        match self.get_field(name, FieldType::ByteArray)? {
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

    fn read_bool_array(&mut self, name: &str) -> Result<Option<Vec<bool>>> {
        match self.get_field(name, FieldType::BoolArray)? {
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

    fn read_char_array(&mut self, name: &str) -> Result<Option<Vec<char>>> {
        match self.get_field(name, FieldType::CharArray)? {
            Some(data) => {
                let mut input = ObjectDataInput::new(data);
                let has_value = input.read_bool()?;
                if !has_value {
                    return Ok(None);
                }
                let len = input.read_int()? as usize;
                let mut result = Vec::with_capacity(len);
                for _ in 0..len {
                    let code = input.read_short()? as u32;
                    let c = char::from_u32(code).ok_or_else(|| {
                        HazelcastError::Serialization(format!("Invalid char code: {}", code))
                    })?;
                    result.push(c);
                }
                Ok(Some(result))
            }
            None => Ok(None),
        }
    }

    fn read_short_array(&mut self, name: &str) -> Result<Option<Vec<i16>>> {
        match self.get_field(name, FieldType::ShortArray)? {
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

    fn read_int_array(&mut self, name: &str) -> Result<Option<Vec<i32>>> {
        match self.get_field(name, FieldType::IntArray)? {
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

    fn read_long_array(&mut self, name: &str) -> Result<Option<Vec<i64>>> {
        match self.get_field(name, FieldType::LongArray)? {
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

    fn read_float_array(&mut self, name: &str) -> Result<Option<Vec<f32>>> {
        match self.get_field(name, FieldType::FloatArray)? {
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

    fn read_double_array(&mut self, name: &str) -> Result<Option<Vec<f64>>> {
        match self.get_field(name, FieldType::DoubleArray)? {
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

    fn read_string_array(&mut self, name: &str) -> Result<Option<Vec<String>>> {
        match self.get_field(name, FieldType::Utf8Array)? {
            Some(data) => {
                let mut input = ObjectDataInput::new(data);
                let has_value = input.read_bool()?;
                if !has_value {
                    return Ok(None);
                }
                let len = input.read_int()? as usize;
                let mut result = Vec::with_capacity(len);
                for _ in 0..len {
                    result.push(input.read_string()?);
                }
                Ok(Some(result))
            }
            None => Ok(None),
        }
    }

    fn read_portable_array<P: Portable>(&mut self, name: &str) -> Result<Option<Vec<P>>> {
        match self.get_field(name, FieldType::PortableArray)? {
            Some(data) => {
                let mut input = ObjectDataInput::new(data);
                let has_value = input.read_bool()?;
                if !has_value {
                    return Ok(None);
                }

                let len = input.read_int()? as usize;
                let mut result = Vec::with_capacity(len);

                for _ in 0..len {
                    let factory_id = input.read_int()?;
                    let class_id = input.read_int()?;
                    let nested_len = input.read_int()? as usize;
                    let nested_data = input.read_bytes(nested_len)?;

                    let factory = self.factories.get(&factory_id).ok_or_else(|| {
                        HazelcastError::Serialization(format!(
                            "No factory registered for factory_id={}",
                            factory_id
                        ))
                    })?;

                    let mut instance = factory.create(class_id).ok_or_else(|| {
                        HazelcastError::Serialization(format!(
                            "Factory {} cannot create class_id={}",
                            factory_id, class_id
                        ))
                    })?;

                    let mut nested_reader = DefaultPortableReader::from_bytes(
                        &nested_data,
                        0,
                        self.factories.clone(),
                    )?;
                    instance.read_portable(&mut nested_reader)?;

                    let instance_any = instance as Box<dyn std::any::Any>;
                    match instance_any.downcast::<P>() {
                        Ok(typed) => result.push(*typed),
                        Err(_) => {
                            return Err(HazelcastError::Serialization(
                                "Type mismatch when reading portable array element".to_string(),
                            ))
                        }
                    }
                }

                Ok(Some(result))
            }
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_class_def() -> ClassDefinition {
        ClassDefinition::with_fields(
            1,
            1,
            1,
            vec![
                FieldDefinition::new("byte_field", FieldType::Byte, 0),
                FieldDefinition::new("bool_field", FieldType::Bool, 1),
                FieldDefinition::new("char_field", FieldType::Char, 2),
                FieldDefinition::new("short_field", FieldType::Short, 3),
                FieldDefinition::new("int_field", FieldType::Int, 4),
                FieldDefinition::new("long_field", FieldType::Long, 5),
                FieldDefinition::new("float_field", FieldType::Float, 6),
                FieldDefinition::new("double_field", FieldType::Double, 7),
                FieldDefinition::new("string_field", FieldType::Utf8, 8),
            ],
        )
    }

    #[test]
    fn test_write_and_read_primitives() {
        let class_def = create_test_class_def();
        let mut writer = DefaultPortableWriter::new(class_def.clone());

        writer.write_byte("byte_field", 42).unwrap();
        writer.write_bool("bool_field", true).unwrap();
        writer.write_char("char_field", 'A').unwrap();
        writer.write_short("short_field", 1234).unwrap();
        writer.write_int("int_field", 123456).unwrap();
        writer.write_long("long_field", 123456789012).unwrap();
        writer.write_float("float_field", 3.14).unwrap();
        writer.write_double("double_field", 2.71828).unwrap();
        writer
            .write_string("string_field", Some("Hello, World!"))
            .unwrap();

        let bytes = writer.to_bytes();
        let mut reader =
            DefaultPortableReader::from_bytes(&bytes, 1, HashMap::new()).unwrap();

        assert_eq!(reader.read_byte("byte_field").unwrap(), 42);
        assert!(reader.read_bool("bool_field").unwrap());
        assert_eq!(reader.read_char("char_field").unwrap(), 'A');
        assert_eq!(reader.read_short("short_field").unwrap(), 1234);
        assert_eq!(reader.read_int("int_field").unwrap(), 123456);
        assert_eq!(reader.read_long("long_field").unwrap(), 123456789012);
        assert!((reader.read_float("float_field").unwrap() - 3.14).abs() < 0.001);
        assert!((reader.read_double("double_field").unwrap() - 2.71828).abs() < 0.00001);
        assert_eq!(
            reader.read_string("string_field").unwrap(),
            Some("Hello, World!".to_string())
        );
    }

    #[test]
    fn test_null_string() {
        let class_def =
            ClassDefinition::with_fields(1, 1, 1, vec![FieldDefinition::new("s", FieldType::Utf8, 0)]);

        let mut writer = DefaultPortableWriter::new(class_def);
        writer.write_string("s", None).unwrap();

        let bytes = writer.to_bytes();
        let mut reader =
            DefaultPortableReader::from_bytes(&bytes, 1, HashMap::new()).unwrap();

        assert_eq!(reader.read_string("s").unwrap(), None);
    }

    #[test]
    fn test_arrays() {
        let class_def = ClassDefinition::with_fields(
            1,
            1,
            1,
            vec![
                FieldDefinition::new("int_array", FieldType::IntArray, 0),
                FieldDefinition::new("string_array", FieldType::Utf8Array, 1),
            ],
        );

        let mut writer = DefaultPortableWriter::new(class_def);
        writer
            .write_int_array("int_array", Some(&[1, 2, 3, 4, 5]))
            .unwrap();
        writer
            .write_string_array(
                "string_array",
                Some(&["a".to_string(), "b".to_string(), "c".to_string()]),
            )
            .unwrap();

        let bytes = writer.to_bytes();
        let mut reader =
            DefaultPortableReader::from_bytes(&bytes, 1, HashMap::new()).unwrap();

        assert_eq!(
            reader.read_int_array("int_array").unwrap(),
            Some(vec![1, 2, 3, 4, 5])
        );
        assert_eq!(
            reader.read_string_array("string_array").unwrap(),
            Some(vec!["a".to_string(), "b".to_string(), "c".to_string()])
        );
    }

    #[test]
    fn test_null_arrays() {
        let class_def = ClassDefinition::with_fields(
            1,
            1,
            1,
            vec![FieldDefinition::new("int_array", FieldType::IntArray, 0)],
        );

        let mut writer = DefaultPortableWriter::new(class_def);
        writer.write_int_array("int_array", None).unwrap();

        let bytes = writer.to_bytes();
        let mut reader =
            DefaultPortableReader::from_bytes(&bytes, 1, HashMap::new()).unwrap();

        assert_eq!(reader.read_int_array("int_array").unwrap(), None);
    }

    #[test]
    fn test_field_type_mismatch() {
        let class_def =
            ClassDefinition::with_fields(1, 1, 1, vec![FieldDefinition::new("f", FieldType::Int, 0)]);

        let mut writer = DefaultPortableWriter::new(class_def);
        let result = writer.write_string("f", Some("test"));
        assert!(result.is_err());
    }

    #[test]
    fn test_unknown_field() {
        let class_def = ClassDefinition::new(1, 1, 1);
        let mut writer = DefaultPortableWriter::new(class_def);
        let result = writer.write_int("unknown", 42);
        assert!(result.is_err());
    }

    #[test]
    fn test_has_field() {
        let class_def =
            ClassDefinition::with_fields(1, 1, 1, vec![FieldDefinition::new("f", FieldType::Int, 0)]);

        let mut writer = DefaultPortableWriter::new(class_def);
        writer.write_int("f", 42).unwrap();

        let bytes = writer.to_bytes();
        let reader = DefaultPortableReader::from_bytes(&bytes, 1, HashMap::new()).unwrap();

        assert!(reader.has_field("f"));
        assert!(!reader.has_field("unknown"));
    }

    #[test]
    fn test_version() {
        let class_def = ClassDefinition::new(1, 1, 5);
        let writer = DefaultPortableWriter::new(class_def);
        let bytes = writer.to_bytes();

        let reader = DefaultPortableReader::from_bytes(&bytes, 5, HashMap::new()).unwrap();
        assert_eq!(reader.version(), 5);
    }

    #[test]
    fn test_missing_field_returns_default() {
        let class_def =
            ClassDefinition::with_fields(1, 1, 1, vec![FieldDefinition::new("f", FieldType::Int, 0)]);

        let writer = DefaultPortableWriter::new(class_def);
        let bytes = writer.to_bytes();

        let mut reader =
            DefaultPortableReader::from_bytes(&bytes, 1, HashMap::new()).unwrap();

        assert_eq!(reader.read_int("f").unwrap(), 0);
    }
}
