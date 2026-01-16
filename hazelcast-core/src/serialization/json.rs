//! JSON serialization support for Hazelcast.

use crate::error::Result;

use super::{DataInput, DataOutput, Deserializable, Serializable};

/// Type ID for JSON serialization in Hazelcast.
pub const JSON_TYPE_ID: i32 = -130;

/// A wrapper type for storing JSON documents in Hazelcast.
///
/// This type allows you to store and retrieve JSON documents without
/// requiring Rust struct definitions. The JSON is stored as a string
/// and can be queried using Hazelcast's JSON query capabilities.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct HazelcastJsonValue {
    json: String,
}

impl HazelcastJsonValue {
    /// Creates a new `HazelcastJsonValue` from a JSON string.
    ///
    /// Note: This does not validate that the string is valid JSON.
    pub fn from_string(json: impl Into<String>) -> Self {
        Self { json: json.into() }
    }

    /// Returns the JSON string as a reference.
    pub fn as_str(&self) -> &str {
        &self.json
    }

    /// Consumes the wrapper and returns the inner JSON string.
    pub fn into_string(self) -> String {
        self.json
    }
}

#[cfg(feature = "json")]
impl HazelcastJsonValue {
    /// Creates a new `HazelcastJsonValue` from a serde_json `Value`.
    pub fn from_value(value: &serde_json::Value) -> Result<Self> {
        let json = serde_json::to_string(value)
            .map_err(|e| crate::error::HazelcastError::Serialization(e.to_string()))?;
        Ok(Self { json })
    }

    /// Parses the JSON string into a serde_json `Value`.
    pub fn to_value(&self) -> Result<serde_json::Value> {
        serde_json::from_str(&self.json)
            .map_err(|e| crate::error::HazelcastError::Serialization(e.to_string()))
    }
}

impl std::fmt::Display for HazelcastJsonValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.json)
    }
}

impl Serializable for HazelcastJsonValue {
    fn serialize<W: DataOutput>(&self, output: &mut W) -> Result<()> {
        output.write_string(&self.json)
    }
}

impl Deserializable for HazelcastJsonValue {
    fn deserialize<R: DataInput>(input: &mut R) -> Result<Self> {
        let json = input.read_string()?;
        Ok(Self { json })
    }
}

impl From<String> for HazelcastJsonValue {
    fn from(json: String) -> Self {
        Self::from_string(json)
    }
}

impl From<&str> for HazelcastJsonValue {
    fn from(json: &str) -> Self {
        Self::from_string(json)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serialization::{ObjectDataInput, ObjectDataOutput};

    #[test]
    fn test_from_string() {
        let json = HazelcastJsonValue::from_string(r#"{"key": "value"}"#);
        assert_eq!(json.as_str(), r#"{"key": "value"}"#);
    }

    #[test]
    fn test_from_string_owned() {
        let json = HazelcastJsonValue::from_string(String::from(r#"{"key": "value"}"#));
        assert_eq!(json.as_str(), r#"{"key": "value"}"#);
    }

    #[test]
    fn test_into_string() {
        let json = HazelcastJsonValue::from_string(r#"{"key": "value"}"#);
        let s = json.into_string();
        assert_eq!(s, r#"{"key": "value"}"#);
    }

    #[test]
    fn test_display() {
        let json = HazelcastJsonValue::from_string(r#"{"key": "value"}"#);
        assert_eq!(format!("{}", json), r#"{"key": "value"}"#);
    }

    #[test]
    fn test_to_string_via_display() {
        let json = HazelcastJsonValue::from_string(r#"{"key": "value"}"#);
        assert_eq!(json.to_string(), r#"{"key": "value"}"#);
    }

    #[test]
    fn test_from_str_trait() {
        let json: HazelcastJsonValue = r#"{"key": "value"}"#.into();
        assert_eq!(json.as_str(), r#"{"key": "value"}"#);
    }

    #[test]
    fn test_from_string_trait() {
        let json: HazelcastJsonValue = String::from(r#"{"key": "value"}"#).into();
        assert_eq!(json.as_str(), r#"{"key": "value"}"#);
    }

    #[test]
    fn test_round_trip() {
        let original = HazelcastJsonValue::from_string(r#"{"name":"John","age":30}"#);

        let mut output = ObjectDataOutput::new();
        original.serialize(&mut output).unwrap();

        let bytes = output.as_bytes();
        let mut input = ObjectDataInput::new(bytes);
        let deserialized = HazelcastJsonValue::deserialize(&mut input).unwrap();

        assert_eq!(original, deserialized);
    }

    #[test]
    fn test_empty_json() {
        let json = HazelcastJsonValue::from_string("{}");

        let mut output = ObjectDataOutput::new();
        json.serialize(&mut output).unwrap();

        let bytes = output.as_bytes();
        let mut input = ObjectDataInput::new(bytes);
        let deserialized = HazelcastJsonValue::deserialize(&mut input).unwrap();

        assert_eq!(json, deserialized);
    }

    #[test]
    fn test_equality() {
        let json1 = HazelcastJsonValue::from_string(r#"{"key": "value"}"#);
        let json2 = HazelcastJsonValue::from_string(r#"{"key": "value"}"#);
        let json3 = HazelcastJsonValue::from_string(r#"{"key": "other"}"#);

        assert_eq!(json1, json2);
        assert_ne!(json1, json3);
    }

    #[test]
    fn test_clone() {
        let json1 = HazelcastJsonValue::from_string(r#"{"key": "value"}"#);
        let json2 = json1.clone();
        assert_eq!(json1, json2);
    }

    #[cfg(feature = "json")]
    #[test]
    fn test_from_value() {
        let value = serde_json::json!({"key": "value"});
        let json = HazelcastJsonValue::from_value(&value).unwrap();
        assert_eq!(json.as_str(), r#"{"key":"value"}"#);
    }

    #[cfg(feature = "json")]
    #[test]
    fn test_to_value() {
        let json = HazelcastJsonValue::from_string(r#"{"key":"value"}"#);
        let value = json.to_value().unwrap();
        assert_eq!(value, serde_json::json!({"key": "value"}));
    }

    #[cfg(feature = "json")]
    #[test]
    fn test_to_value_invalid_json() {
        let json = HazelcastJsonValue::from_string("not valid json");
        assert!(json.to_value().is_err());
    }
}
