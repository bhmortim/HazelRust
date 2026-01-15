//! Data input traits and implementations for Hazelcast serialization.

use crate::error::{HazelcastError, Result};
use bytes::Buf;
use std::io::Cursor;

/// Trait for reading primitive values from Hazelcast's binary format.
///
/// All multi-byte values are read in big-endian byte order.
pub trait DataInput {
    /// Reads a single byte (i8).
    fn read_byte(&mut self) -> Result<i8>;

    /// Reads a boolean from a single byte.
    fn read_bool(&mut self) -> Result<bool>;

    /// Reads a 16-bit signed integer in big-endian order.
    fn read_short(&mut self) -> Result<i16>;

    /// Reads a 32-bit signed integer in big-endian order.
    fn read_int(&mut self) -> Result<i32>;

    /// Reads a 64-bit signed integer in big-endian order.
    fn read_long(&mut self) -> Result<i64>;

    /// Reads a 32-bit floating point in big-endian order.
    fn read_float(&mut self) -> Result<f32>;

    /// Reads a 64-bit floating point in big-endian order.
    fn read_double(&mut self) -> Result<f64>;

    /// Reads the specified number of raw bytes.
    fn read_bytes(&mut self, len: usize) -> Result<Vec<u8>>;

    /// Reads a length-prefixed string.
    fn read_string(&mut self) -> Result<String>;
}

/// A buffer-based implementation of `DataInput`.
#[derive(Debug)]
pub struct ObjectDataInput<'a> {
    cursor: Cursor<&'a [u8]>,
}

impl<'a> ObjectDataInput<'a> {
    /// Creates a new `ObjectDataInput` from the given byte slice.
    pub fn new(data: &'a [u8]) -> Self {
        Self {
            cursor: Cursor::new(data),
        }
    }

    /// Returns the number of bytes remaining to be read.
    pub fn remaining(&self) -> usize {
        self.cursor.remaining()
    }

    /// Returns the current position in the buffer.
    pub fn position(&self) -> u64 {
        self.cursor.position()
    }

    fn ensure_remaining(&self, n: usize) -> Result<()> {
        if self.cursor.remaining() < n {
            Err(HazelcastError::Serialization(format!(
                "insufficient data: need {} bytes, have {}",
                n,
                self.cursor.remaining()
            )))
        } else {
            Ok(())
        }
    }
}

impl DataInput for ObjectDataInput<'_> {
    fn read_byte(&mut self) -> Result<i8> {
        self.ensure_remaining(1)?;
        Ok(self.cursor.get_i8())
    }

    fn read_bool(&mut self) -> Result<bool> {
        self.ensure_remaining(1)?;
        Ok(self.cursor.get_u8() != 0)
    }

    fn read_short(&mut self) -> Result<i16> {
        self.ensure_remaining(2)?;
        Ok(self.cursor.get_i16())
    }

    fn read_int(&mut self) -> Result<i32> {
        self.ensure_remaining(4)?;
        Ok(self.cursor.get_i32())
    }

    fn read_long(&mut self) -> Result<i64> {
        self.ensure_remaining(8)?;
        Ok(self.cursor.get_i64())
    }

    fn read_float(&mut self) -> Result<f32> {
        self.ensure_remaining(4)?;
        Ok(self.cursor.get_f32())
    }

    fn read_double(&mut self) -> Result<f64> {
        self.ensure_remaining(8)?;
        Ok(self.cursor.get_f64())
    }

    fn read_bytes(&mut self, len: usize) -> Result<Vec<u8>> {
        self.ensure_remaining(len)?;
        let mut buf = vec![0u8; len];
        self.cursor.copy_to_slice(&mut buf);
        Ok(buf)
    }

    fn read_string(&mut self) -> Result<String> {
        let len = self.read_int()?;
        if len < 0 {
            return Err(HazelcastError::Serialization(format!(
                "invalid string length: {}",
                len
            )));
        }
        let bytes = self.read_bytes(len as usize)?;
        String::from_utf8(bytes)
            .map_err(|e| HazelcastError::Serialization(format!("invalid UTF-8 string: {}", e)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_input() {
        let data = [1, 2, 3, 4];
        let input = ObjectDataInput::new(&data);
        assert_eq!(input.remaining(), 4);
        assert_eq!(input.position(), 0);
    }

    #[test]
    fn test_read_byte() {
        let data = [42u8];
        let mut input = ObjectDataInput::new(&data);
        assert_eq!(input.read_byte().unwrap(), 42);
    }

    #[test]
    fn test_read_byte_negative() {
        let data = [0xFFu8];
        let mut input = ObjectDataInput::new(&data);
        assert_eq!(input.read_byte().unwrap(), -1);
    }

    #[test]
    fn test_read_bool_true() {
        let data = [1u8];
        let mut input = ObjectDataInput::new(&data);
        assert!(input.read_bool().unwrap());
    }

    #[test]
    fn test_read_bool_false() {
        let data = [0u8];
        let mut input = ObjectDataInput::new(&data);
        assert!(!input.read_bool().unwrap());
    }

    #[test]
    fn test_read_bool_nonzero_is_true() {
        let data = [42u8];
        let mut input = ObjectDataInput::new(&data);
        assert!(input.read_bool().unwrap());
    }

    #[test]
    fn test_read_short_big_endian() {
        let data = [0x01, 0x02];
        let mut input = ObjectDataInput::new(&data);
        assert_eq!(input.read_short().unwrap(), 0x0102);
    }

    #[test]
    fn test_read_int_big_endian() {
        let data = [0x01, 0x02, 0x03, 0x04];
        let mut input = ObjectDataInput::new(&data);
        assert_eq!(input.read_int().unwrap(), 0x01020304);
    }

    #[test]
    fn test_read_long_big_endian() {
        let data = [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08];
        let mut input = ObjectDataInput::new(&data);
        assert_eq!(input.read_long().unwrap(), 0x0102030405060708);
    }

    #[test]
    fn test_read_float() {
        let data = [0x3F, 0x80, 0x00, 0x00];
        let mut input = ObjectDataInput::new(&data);
        assert_eq!(input.read_float().unwrap(), 1.0f32);
    }

    #[test]
    fn test_read_double() {
        let data = [0x3F, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        let mut input = ObjectDataInput::new(&data);
        assert_eq!(input.read_double().unwrap(), 1.0f64);
    }

    #[test]
    fn test_read_bytes() {
        let data = [1, 2, 3, 4, 5];
        let mut input = ObjectDataInput::new(&data);
        assert_eq!(input.read_bytes(3).unwrap(), vec![1, 2, 3]);
        assert_eq!(input.remaining(), 2);
    }

    #[test]
    fn test_read_string() {
        let data = [0, 0, 0, 4, b't', b'e', b's', b't'];
        let mut input = ObjectDataInput::new(&data);
        assert_eq!(input.read_string().unwrap(), "test");
    }

    #[test]
    fn test_read_empty_string() {
        let data = [0, 0, 0, 0];
        let mut input = ObjectDataInput::new(&data);
        assert_eq!(input.read_string().unwrap(), "");
    }

    #[test]
    fn test_insufficient_data_byte() {
        let data: [u8; 0] = [];
        let mut input = ObjectDataInput::new(&data);
        assert!(input.read_byte().is_err());
    }

    #[test]
    fn test_insufficient_data_short() {
        let data = [0x01];
        let mut input = ObjectDataInput::new(&data);
        assert!(input.read_short().is_err());
    }

    #[test]
    fn test_insufficient_data_int() {
        let data = [0x01, 0x02, 0x03];
        let mut input = ObjectDataInput::new(&data);
        assert!(input.read_int().is_err());
    }

    #[test]
    fn test_insufficient_data_long() {
        let data = [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07];
        let mut input = ObjectDataInput::new(&data);
        assert!(input.read_long().is_err());
    }

    #[test]
    fn test_insufficient_data_bytes() {
        let data = [1, 2, 3];
        let mut input = ObjectDataInput::new(&data);
        assert!(input.read_bytes(5).is_err());
    }

    #[test]
    fn test_invalid_utf8_string() {
        let data = [0, 0, 0, 2, 0xFF, 0xFE];
        let mut input = ObjectDataInput::new(&data);
        assert!(input.read_string().is_err());
    }

    #[test]
    fn test_negative_string_length() {
        let data = [0xFF, 0xFF, 0xFF, 0xFF];
        let mut input = ObjectDataInput::new(&data);
        assert!(input.read_string().is_err());
    }

    #[test]
    fn test_position_advances() {
        let data = [0, 0, 0, 42, 1, 2, 3, 4];
        let mut input = ObjectDataInput::new(&data);
        assert_eq!(input.position(), 0);
        input.read_int().unwrap();
        assert_eq!(input.position(), 4);
        input.read_int().unwrap();
        assert_eq!(input.position(), 8);
    }
}
