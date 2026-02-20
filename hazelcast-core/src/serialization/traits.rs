//! Serialization traits and implementations for primitive types.

use super::{DataInput, DataOutput};
use crate::error::Result;

/// Trait for types that can be serialized to Hazelcast's binary format.
pub trait Serializable {
    /// Serializes this value to the given output.
    fn serialize<W: DataOutput>(&self, output: &mut W) -> Result<()>;

    /// Convenience method: serializes this value to a byte vector.
    fn to_bytes(&self) -> Result<Vec<u8>>
    where
        Self: Sized,
    {
        let mut output = super::ObjectDataOutput::new();
        self.serialize(&mut output)?;
        Ok(output.into_bytes())
    }
}

/// Trait for types that can be deserialized from Hazelcast's binary format.
pub trait Deserializable: Sized {
    /// Deserializes a value from the given input.
    fn deserialize<R: DataInput>(input: &mut R) -> Result<Self>;

    /// Convenience method: deserializes a value from a byte slice.
    fn from_bytes(data: &[u8]) -> Result<Self> {
        let mut input = super::ObjectDataInput::new(data);
        Self::deserialize(&mut input)
    }
}

impl Serializable for i8 {
    fn serialize<W: DataOutput>(&self, output: &mut W) -> Result<()> {
        output.write_byte(*self)
    }
}

impl Deserializable for i8 {
    fn deserialize<R: DataInput>(input: &mut R) -> Result<Self> {
        input.read_byte()
    }
}

impl Serializable for i16 {
    fn serialize<W: DataOutput>(&self, output: &mut W) -> Result<()> {
        output.write_short(*self)
    }
}

impl Deserializable for i16 {
    fn deserialize<R: DataInput>(input: &mut R) -> Result<Self> {
        input.read_short()
    }
}

impl Serializable for i32 {
    fn serialize<W: DataOutput>(&self, output: &mut W) -> Result<()> {
        output.write_int(*self)
    }
}

impl Deserializable for i32 {
    fn deserialize<R: DataInput>(input: &mut R) -> Result<Self> {
        input.read_int()
    }
}

impl Serializable for i64 {
    fn serialize<W: DataOutput>(&self, output: &mut W) -> Result<()> {
        output.write_long(*self)
    }
}

impl Deserializable for i64 {
    fn deserialize<R: DataInput>(input: &mut R) -> Result<Self> {
        input.read_long()
    }
}

impl Serializable for f32 {
    fn serialize<W: DataOutput>(&self, output: &mut W) -> Result<()> {
        output.write_float(*self)
    }
}

impl Deserializable for f32 {
    fn deserialize<R: DataInput>(input: &mut R) -> Result<Self> {
        input.read_float()
    }
}

impl Serializable for f64 {
    fn serialize<W: DataOutput>(&self, output: &mut W) -> Result<()> {
        output.write_double(*self)
    }
}

impl Deserializable for f64 {
    fn deserialize<R: DataInput>(input: &mut R) -> Result<Self> {
        input.read_double()
    }
}

impl Serializable for bool {
    fn serialize<W: DataOutput>(&self, output: &mut W) -> Result<()> {
        output.write_bool(*self)
    }
}

impl Deserializable for bool {
    fn deserialize<R: DataInput>(input: &mut R) -> Result<Self> {
        input.read_bool()
    }
}

impl Serializable for String {
    fn serialize<W: DataOutput>(&self, output: &mut W) -> Result<()> {
        output.write_string(self)
    }
}

impl Deserializable for String {
    fn deserialize<R: DataInput>(input: &mut R) -> Result<Self> {
        input.read_string()
    }
}

impl Serializable for str {
    fn serialize<W: DataOutput>(&self, output: &mut W) -> Result<()> {
        output.write_string(self)
    }
}

impl Serializable for Vec<u8> {
    fn serialize<W: DataOutput>(&self, output: &mut W) -> Result<()> {
        output.write_int(self.len() as i32)?;
        output.write_bytes(self)
    }
}

impl Deserializable for Vec<u8> {
    fn deserialize<R: DataInput>(input: &mut R) -> Result<Self> {
        let len = input.read_int()? as usize;
        input.read_bytes(len)
    }
}

impl Serializable for [u8] {
    fn serialize<W: DataOutput>(&self, output: &mut W) -> Result<()> {
        output.write_int(self.len() as i32)?;
        output.write_bytes(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serialization::{ObjectDataInput, ObjectDataOutput};

    fn round_trip<T: Serializable + Deserializable + PartialEq + std::fmt::Debug>(value: T) {
        let mut output = ObjectDataOutput::new();
        value.serialize(&mut output).unwrap();
        let bytes = output.as_bytes();
        let mut input = ObjectDataInput::new(bytes);
        let result = T::deserialize(&mut input).unwrap();
        assert_eq!(value, result);
    }

    #[test]
    fn test_i8_round_trip() {
        round_trip(0i8);
        round_trip(127i8);
        round_trip(-128i8);
        round_trip(42i8);
        round_trip(-42i8);
    }

    #[test]
    fn test_i16_round_trip() {
        round_trip(0i16);
        round_trip(i16::MAX);
        round_trip(i16::MIN);
        round_trip(12345i16);
        round_trip(-12345i16);
    }

    #[test]
    fn test_i32_round_trip() {
        round_trip(0i32);
        round_trip(i32::MAX);
        round_trip(i32::MIN);
        round_trip(123456789i32);
        round_trip(-123456789i32);
    }

    #[test]
    fn test_i64_round_trip() {
        round_trip(0i64);
        round_trip(i64::MAX);
        round_trip(i64::MIN);
        round_trip(1234567890123456789i64);
        round_trip(-1234567890123456789i64);
    }

    #[test]
    fn test_f32_round_trip() {
        round_trip(0.0f32);
        round_trip(1.0f32);
        round_trip(-1.0f32);
        round_trip(3.14159f32);
        round_trip(-273.15f32);
        round_trip(f32::MAX);
        round_trip(f32::MIN);
        round_trip(f32::MIN_POSITIVE);
    }

    #[test]
    fn test_f64_round_trip() {
        round_trip(0.0f64);
        round_trip(1.0f64);
        round_trip(-1.0f64);
        round_trip(std::f64::consts::PI);
        round_trip(std::f64::consts::E);
        round_trip(-273.15f64);
        round_trip(f64::MAX);
        round_trip(f64::MIN);
        round_trip(f64::MIN_POSITIVE);
    }

    #[test]
    fn test_bool_round_trip() {
        round_trip(true);
        round_trip(false);
    }

    #[test]
    fn test_string_round_trip() {
        round_trip(String::new());
        round_trip(String::from("Hello, World!"));
        round_trip(String::from("こんにちは"));
        round_trip(String::from("🚀🎉✨"));
        round_trip(String::from("Hello\nWorld\t!"));
        round_trip("a".repeat(10000));
    }

    #[test]
    fn test_vec_u8_round_trip() {
        round_trip(Vec::<u8>::new());
        round_trip(vec![1u8, 2, 3, 4, 5]);
        round_trip(vec![0u8; 100]);
        round_trip((0u8..=255).collect::<Vec<_>>());
    }

    #[test]
    fn test_str_serializable() {
        let mut output = ObjectDataOutput::new();
        "test".serialize(&mut output).unwrap();
        let bytes = output.as_bytes();
        let mut input = ObjectDataInput::new(bytes);
        let result = String::deserialize(&mut input).unwrap();
        assert_eq!(result, "test");
    }

    #[test]
    fn test_slice_u8_serializable() {
        let data: &[u8] = &[1, 2, 3, 4, 5];
        let mut output = ObjectDataOutput::new();
        data.serialize(&mut output).unwrap();
        let bytes = output.as_bytes();
        let mut input = ObjectDataInput::new(bytes);
        let result = Vec::<u8>::deserialize(&mut input).unwrap();
        assert_eq!(result, data);
    }

    #[test]
    fn test_big_endian_i16() {
        let mut output = ObjectDataOutput::new();
        0x0102i16.serialize(&mut output).unwrap();
        assert_eq!(output.as_bytes(), &[0x01, 0x02]);
    }

    #[test]
    fn test_big_endian_i32() {
        let mut output = ObjectDataOutput::new();
        0x01020304i32.serialize(&mut output).unwrap();
        assert_eq!(output.as_bytes(), &[0x01, 0x02, 0x03, 0x04]);
    }

    #[test]
    fn test_big_endian_i64() {
        let mut output = ObjectDataOutput::new();
        0x0102030405060708i64.serialize(&mut output).unwrap();
        assert_eq!(
            output.as_bytes(),
            &[0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]
        );
    }

    #[test]
    fn test_multiple_values_sequential() {
        let mut output = ObjectDataOutput::new();
        42i32.serialize(&mut output).unwrap();
        true.serialize(&mut output).unwrap();
        "test".serialize(&mut output).unwrap();
        3.14f64.serialize(&mut output).unwrap();
        vec![1u8, 2, 3].serialize(&mut output).unwrap();

        let bytes = output.as_bytes();
        let mut input = ObjectDataInput::new(bytes);

        assert_eq!(i32::deserialize(&mut input).unwrap(), 42);
        assert!(bool::deserialize(&mut input).unwrap());
        assert_eq!(String::deserialize(&mut input).unwrap(), "test");
        assert!((f64::deserialize(&mut input).unwrap() - 3.14).abs() < f64::EPSILON);
        assert_eq!(Vec::<u8>::deserialize(&mut input).unwrap(), vec![1, 2, 3]);
        assert_eq!(input.remaining(), 0);
    }

    #[test]
    fn test_insufficient_data_error() {
        let bytes = [0x01, 0x02];
        let mut input = ObjectDataInput::new(&bytes);
        let result = i32::deserialize(&mut input);
        assert!(result.is_err());
    }

    #[test]
    fn test_float_special_values() {
        let mut output = ObjectDataOutput::new();
        f32::INFINITY.serialize(&mut output).unwrap();
        f32::NEG_INFINITY.serialize(&mut output).unwrap();

        let bytes = output.as_bytes();
        let mut input = ObjectDataInput::new(bytes);

        assert_eq!(f32::deserialize(&mut input).unwrap(), f32::INFINITY);
        assert_eq!(f32::deserialize(&mut input).unwrap(), f32::NEG_INFINITY);
    }

    #[test]
    fn test_double_special_values() {
        let mut output = ObjectDataOutput::new();
        f64::INFINITY.serialize(&mut output).unwrap();
        f64::NEG_INFINITY.serialize(&mut output).unwrap();

        let bytes = output.as_bytes();
        let mut input = ObjectDataInput::new(bytes);

        assert_eq!(f64::deserialize(&mut input).unwrap(), f64::INFINITY);
        assert_eq!(f64::deserialize(&mut input).unwrap(), f64::NEG_INFINITY);
    }

    #[test]
    fn test_nan_round_trip() {
        let mut output = ObjectDataOutput::new();
        f64::NAN.serialize(&mut output).unwrap();

        let bytes = output.as_bytes();
        let mut input = ObjectDataInput::new(bytes);

        let result = f64::deserialize(&mut input).unwrap();
        assert!(result.is_nan());
    }
}
