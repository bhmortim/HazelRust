//! Data output traits and implementations for Hazelcast serialization.

use crate::error::Result;
use bytes::{BufMut, BytesMut};

/// Trait for writing primitive values in Hazelcast's binary format.
///
/// All multi-byte values are written in big-endian byte order.
pub trait DataOutput {
    /// Writes a single byte (i8).
    fn write_byte(&mut self, v: i8) -> Result<()>;

    /// Writes a boolean as a single byte (0 for false, 1 for true).
    fn write_bool(&mut self, v: bool) -> Result<()>;

    /// Writes a 16-bit signed integer in big-endian order.
    fn write_short(&mut self, v: i16) -> Result<()>;

    /// Writes a 32-bit signed integer in big-endian order.
    fn write_int(&mut self, v: i32) -> Result<()>;

    /// Writes a 64-bit signed integer in big-endian order.
    fn write_long(&mut self, v: i64) -> Result<()>;

    /// Writes a 32-bit floating point in big-endian order.
    fn write_float(&mut self, v: f32) -> Result<()>;

    /// Writes a 64-bit floating point in big-endian order.
    fn write_double(&mut self, v: f64) -> Result<()>;

    /// Writes raw bytes without length prefix.
    fn write_bytes(&mut self, v: &[u8]) -> Result<()>;

    /// Writes a string with its length prefix.
    fn write_string(&mut self, v: &str) -> Result<()>;
}

/// A buffer-based implementation of `DataOutput`.
#[derive(Debug)]
pub struct ObjectDataOutput {
    buffer: BytesMut,
}

impl ObjectDataOutput {
    /// Creates a new `ObjectDataOutput` with default capacity.
    pub fn new() -> Self {
        Self {
            buffer: BytesMut::with_capacity(256),
        }
    }

    /// Creates a new `ObjectDataOutput` with the specified capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            buffer: BytesMut::with_capacity(capacity),
        }
    }

    /// Returns the written bytes as a slice.
    pub fn as_bytes(&self) -> &[u8] {
        &self.buffer
    }

    /// Consumes the output and returns the written bytes.
    pub fn into_bytes(self) -> Vec<u8> {
        self.buffer.to_vec()
    }

    /// Returns the number of bytes written.
    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    /// Returns true if no bytes have been written.
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    /// Clears the buffer, removing all written data.
    pub fn clear(&mut self) {
        self.buffer.clear();
    }
}

impl Default for ObjectDataOutput {
    fn default() -> Self {
        Self::new()
    }
}

impl DataOutput for ObjectDataOutput {
    fn write_byte(&mut self, v: i8) -> Result<()> {
        self.buffer.put_i8(v);
        Ok(())
    }

    fn write_bool(&mut self, v: bool) -> Result<()> {
        self.buffer.put_u8(if v { 1 } else { 0 });
        Ok(())
    }

    fn write_short(&mut self, v: i16) -> Result<()> {
        self.buffer.put_i16(v);
        Ok(())
    }

    fn write_int(&mut self, v: i32) -> Result<()> {
        self.buffer.put_i32(v);
        Ok(())
    }

    fn write_long(&mut self, v: i64) -> Result<()> {
        self.buffer.put_i64(v);
        Ok(())
    }

    fn write_float(&mut self, v: f32) -> Result<()> {
        self.buffer.put_f32(v);
        Ok(())
    }

    fn write_double(&mut self, v: f64) -> Result<()> {
        self.buffer.put_f64(v);
        Ok(())
    }

    fn write_bytes(&mut self, v: &[u8]) -> Result<()> {
        self.buffer.put_slice(v);
        Ok(())
    }

    fn write_string(&mut self, v: &str) -> Result<()> {
        let bytes = v.as_bytes();
        self.write_int(bytes.len() as i32)?;
        self.write_bytes(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_output_is_empty() {
        let output = ObjectDataOutput::new();
        assert!(output.is_empty());
        assert_eq!(output.len(), 0);
    }

    #[test]
    fn test_with_capacity() {
        let output = ObjectDataOutput::with_capacity(1024);
        assert!(output.is_empty());
    }

    #[test]
    fn test_write_byte() {
        let mut output = ObjectDataOutput::new();
        output.write_byte(42).unwrap();
        assert_eq!(output.as_bytes(), &[42u8]);
    }

    #[test]
    fn test_write_byte_negative() {
        let mut output = ObjectDataOutput::new();
        output.write_byte(-1).unwrap();
        assert_eq!(output.as_bytes(), &[0xFF]);
    }

    #[test]
    fn test_write_bool_true() {
        let mut output = ObjectDataOutput::new();
        output.write_bool(true).unwrap();
        assert_eq!(output.as_bytes(), &[1]);
    }

    #[test]
    fn test_write_bool_false() {
        let mut output = ObjectDataOutput::new();
        output.write_bool(false).unwrap();
        assert_eq!(output.as_bytes(), &[0]);
    }

    #[test]
    fn test_write_short_big_endian() {
        let mut output = ObjectDataOutput::new();
        output.write_short(0x0102).unwrap();
        assert_eq!(output.as_bytes(), &[0x01, 0x02]);
    }

    #[test]
    fn test_write_int_big_endian() {
        let mut output = ObjectDataOutput::new();
        output.write_int(0x01020304).unwrap();
        assert_eq!(output.as_bytes(), &[0x01, 0x02, 0x03, 0x04]);
    }

    #[test]
    fn test_write_long_big_endian() {
        let mut output = ObjectDataOutput::new();
        output.write_long(0x0102030405060708).unwrap();
        assert_eq!(
            output.as_bytes(),
            &[0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]
        );
    }

    #[test]
    fn test_write_float() {
        let mut output = ObjectDataOutput::new();
        output.write_float(1.0).unwrap();
        assert_eq!(output.len(), 4);
    }

    #[test]
    fn test_write_double() {
        let mut output = ObjectDataOutput::new();
        output.write_double(1.0).unwrap();
        assert_eq!(output.len(), 8);
    }

    #[test]
    fn test_write_bytes() {
        let mut output = ObjectDataOutput::new();
        output.write_bytes(&[1, 2, 3, 4, 5]).unwrap();
        assert_eq!(output.as_bytes(), &[1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_write_string() {
        let mut output = ObjectDataOutput::new();
        output.write_string("test").unwrap();
        assert_eq!(output.as_bytes(), &[0, 0, 0, 4, b't', b'e', b's', b't']);
    }

    #[test]
    fn test_write_empty_string() {
        let mut output = ObjectDataOutput::new();
        output.write_string("").unwrap();
        assert_eq!(output.as_bytes(), &[0, 0, 0, 0]);
    }

    #[test]
    fn test_into_bytes() {
        let mut output = ObjectDataOutput::new();
        output.write_int(42).unwrap();
        let bytes = output.into_bytes();
        assert_eq!(bytes, vec![0, 0, 0, 42]);
    }

    #[test]
    fn test_clear() {
        let mut output = ObjectDataOutput::new();
        output.write_int(42).unwrap();
        output.clear();
        assert!(output.is_empty());
    }

    #[test]
    fn test_default() {
        let output = ObjectDataOutput::default();
        assert!(output.is_empty());
    }
}
