//! Serde integration for user-type serialization.
//!
//! This module provides a wrapper type [`Serde<T>`] that enables any type implementing
//! `serde::Serialize` and `serde::DeserializeOwned` to be used with Hazelcast's
//! serialization framework.
//!
//! # Example
//!
//! ```rust,ignore
//! use serde::{Serialize, Deserialize};
//! use hazelcast_core::serialization::{Serde, Serializable, Deserializable};
//!
//! #[derive(Serialize, Deserialize, Debug, PartialEq)]
//! struct User {
//!     name: String,
//!     age: u32,
//! }
//!
//! let user = User { name: "Alice".to_string(), age: 30 };
//! let wrapped = Serde::new(user);
//! // Now `wrapped` can be used with IMap, IQueue, etc.
//! ```
//!
//! # Usage with Distributed Data Structures
//!
//! The `Serde<T>` wrapper integrates seamlessly with Hazelcast's distributed data structures:
//!
//! ```rust,ignore
//! use serde::{Serialize, Deserialize};
//! use hazelcast_core::Serde;
//!
//! #[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
//! struct Product {
//!     id: u64,
//!     name: String,
//!     price: f64,
//! }
//!
//! // With IMap
//! let map = client.get_map::<String, Serde<Product>>("products");
//! map.put("item1".to_string(), Serde::new(Product {
//!     id: 1,
//!     name: "Widget".to_string(),
//!     price: 9.99,
//! })).await?;
//!
//! // Retrieve and unwrap
//! if let Some(wrapped) = map.get(&"item1".to_string()).await? {
//!     let product: Product = wrapped.into_inner();
//!     println!("Got product: {:?}", product);
//! }
//! ```

use std::ops::{Deref, DerefMut};

use serde::{de::DeserializeOwned, Serialize};

use super::{DataInput, DataOutput};
use crate::error::{HazelcastError, Result};
use crate::serialization::{Deserializable, Serializable};

/// A wrapper type that enables Serde-compatible types to be serialized
/// using Hazelcast's binary format.
///
/// This wrapper uses [bincode](https://docs.rs/bincode) for efficient binary
/// serialization, prefixed with a length field for framing.
///
/// # Type Parameters
///
/// - `T`: The inner type to wrap. Must implement `serde::Serialize` for
///   serialization and `serde::DeserializeOwned` for deserialization.
///
/// # Wire Format
///
/// The serialized format consists of:
/// 1. A 4-byte big-endian length prefix (i32)
/// 2. The bincode-serialized payload
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct Serde<T>(T);

impl<T> Serde<T> {
    /// Creates a new `Serde` wrapper around the given value.
    pub fn new(value: T) -> Self {
        Serde(value)
    }

    /// Consumes the wrapper and returns the inner value.
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> From<T> for Serde<T> {
    fn from(value: T) -> Self {
        Serde::new(value)
    }
}

impl<T> Deref for Serde<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> DerefMut for Serde<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T: Serialize> Serializable for Serde<T> {
    fn serialize<W: DataOutput>(&self, output: &mut W) -> Result<()> {
        let bytes = bincode::serialize(&self.0).map_err(|e| {
            HazelcastError::Serialization(format!("bincode serialize failed: {}", e))
        })?;
        output.write_int(bytes.len() as i32)?;
        output.write_bytes(&bytes)
    }
}

impl<T: DeserializeOwned> Deserializable for Serde<T> {
    fn deserialize<R: DataInput>(input: &mut R) -> Result<Self> {
        let len = input.read_int()? as usize;
        let bytes = input.read_bytes(len)?;
        let value = bincode::deserialize(&bytes).map_err(|e| {
            HazelcastError::Serialization(format!("bincode deserialize failed: {}", e))
        })?;
        Ok(Serde(value))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use serde::{Deserialize, Serialize};

    use super::*;
    use crate::serialization::{ObjectDataInput, ObjectDataOutput};

    fn round_trip<T>(value: T) -> T
    where
        T: Serialize + DeserializeOwned + Clone,
    {
        let wrapped = Serde::new(value);
        let mut output = ObjectDataOutput::new();
        wrapped.serialize(&mut output).unwrap();
        let bytes = output.as_bytes();
        let mut input = ObjectDataInput::new(bytes);
        Serde::<T>::deserialize(&mut input).unwrap().into_inner()
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct SimpleUser {
        name: String,
        age: u32,
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Address {
        street: String,
        city: String,
        zip: String,
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct UserWithAddress {
        name: String,
        email: String,
        address: Address,
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Container<T> {
        items: Vec<T>,
        metadata: HashMap<String, String>,
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct OptionalFields {
        required: String,
        optional_string: Option<String>,
        optional_number: Option<i64>,
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    enum Status {
        Active,
        Inactive,
        Pending { reason: String },
        Error(String),
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct WithEnum {
        id: u64,
        status: Status,
    }

    #[test]
    fn test_simple_struct_round_trip() {
        let user = SimpleUser {
            name: "Alice".to_string(),
            age: 30,
        };
        let result = round_trip(user.clone());
        assert_eq!(result, user);
    }

    #[test]
    fn test_nested_struct_round_trip() {
        let user = UserWithAddress {
            name: "Bob".to_string(),
            email: "bob@example.com".to_string(),
            address: Address {
                street: "123 Main St".to_string(),
                city: "Springfield".to_string(),
                zip: "12345".to_string(),
            },
        };
        let result = round_trip(user.clone());
        assert_eq!(result, user);
    }

    #[test]
    fn test_vec_round_trip() {
        let items = vec![
            SimpleUser {
                name: "Alice".to_string(),
                age: 30,
            },
            SimpleUser {
                name: "Bob".to_string(),
                age: 25,
            },
            SimpleUser {
                name: "Charlie".to_string(),
                age: 35,
            },
        ];
        let result = round_trip(items.clone());
        assert_eq!(result, items);
    }

    #[test]
    fn test_hashmap_round_trip() {
        let mut map = HashMap::new();
        map.insert("key1".to_string(), 100i64);
        map.insert("key2".to_string(), 200i64);
        map.insert("key3".to_string(), 300i64);
        let result = round_trip(map.clone());
        assert_eq!(result, map);
    }

    #[test]
    fn test_container_with_collections() {
        let mut metadata = HashMap::new();
        metadata.insert("version".to_string(), "1.0".to_string());
        metadata.insert("author".to_string(), "test".to_string());

        let container = Container {
            items: vec!["item1".to_string(), "item2".to_string()],
            metadata,
        };
        let result = round_trip(container.clone());
        assert_eq!(result, container);
    }

    #[test]
    fn test_option_some_round_trip() {
        let fields = OptionalFields {
            required: "required_value".to_string(),
            optional_string: Some("optional_value".to_string()),
            optional_number: Some(42),
        };
        let result = round_trip(fields.clone());
        assert_eq!(result, fields);
    }

    #[test]
    fn test_option_none_round_trip() {
        let fields = OptionalFields {
            required: "required_value".to_string(),
            optional_string: None,
            optional_number: None,
        };
        let result = round_trip(fields.clone());
        assert_eq!(result, fields);
    }

    #[test]
    fn test_enum_unit_variant() {
        let data = WithEnum {
            id: 1,
            status: Status::Active,
        };
        let result = round_trip(data.clone());
        assert_eq!(result, data);
    }

    #[test]
    fn test_enum_tuple_variant() {
        let data = WithEnum {
            id: 2,
            status: Status::Error("something went wrong".to_string()),
        };
        let result = round_trip(data.clone());
        assert_eq!(result, data);
    }

    #[test]
    fn test_enum_struct_variant() {
        let data = WithEnum {
            id: 3,
            status: Status::Pending {
                reason: "awaiting approval".to_string(),
            },
        };
        let result = round_trip(data.clone());
        assert_eq!(result, data);
    }

    #[test]
    fn test_serde_wrapper_deref() {
        let user = SimpleUser {
            name: "Alice".to_string(),
            age: 30,
        };
        let wrapped = Serde::new(user);
        assert_eq!(wrapped.name, "Alice");
        assert_eq!(wrapped.age, 30);
    }

    #[test]
    fn test_serde_wrapper_deref_mut() {
        let user = SimpleUser {
            name: "Alice".to_string(),
            age: 30,
        };
        let mut wrapped = Serde::new(user);
        wrapped.age = 31;
        assert_eq!(wrapped.age, 31);
    }

    #[test]
    fn test_serde_wrapper_from() {
        let user = SimpleUser {
            name: "Alice".to_string(),
            age: 30,
        };
        let wrapped: Serde<SimpleUser> = user.clone().into();
        assert_eq!(*wrapped, user);
    }

    #[test]
    fn test_serde_wrapper_into_inner() {
        let user = SimpleUser {
            name: "Alice".to_string(),
            age: 30,
        };
        let wrapped = Serde::new(user.clone());
        let unwrapped = wrapped.into_inner();
        assert_eq!(unwrapped, user);
    }

    #[test]
    fn test_empty_vec_round_trip() {
        let items: Vec<SimpleUser> = vec![];
        let result = round_trip(items.clone());
        assert_eq!(result, items);
    }

    #[test]
    fn test_empty_string_round_trip() {
        let user = SimpleUser {
            name: String::new(),
            age: 0,
        };
        let result = round_trip(user.clone());
        assert_eq!(result, user);
    }

    #[test]
    fn test_unicode_strings() {
        let user = SimpleUser {
            name: "日本語テスト 🎉".to_string(),
            age: 25,
        };
        let result = round_trip(user.clone());
        assert_eq!(result, user);
    }

    #[test]
    fn test_large_data() {
        let items: Vec<SimpleUser> = (0..1000)
            .map(|i| SimpleUser {
                name: format!("User{}", i),
                age: i as u32,
            })
            .collect();
        let result = round_trip(items.clone());
        assert_eq!(result, items);
    }

    #[test]
    fn test_deserialize_invalid_data() {
        let invalid_bytes = [0x00, 0x00, 0x00, 0x05, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF];
        let mut input = ObjectDataInput::new(&invalid_bytes);
        let result = Serde::<SimpleUser>::deserialize(&mut input);
        assert!(result.is_err());
    }

    #[test]
    fn test_deserialize_truncated_data() {
        let user = SimpleUser {
            name: "Alice".to_string(),
            age: 30,
        };
        let wrapped = Serde::new(user);
        let mut output = ObjectDataOutput::new();
        wrapped.serialize(&mut output).unwrap();
        let bytes = output.as_bytes();

        let truncated = &bytes[..bytes.len() - 5];
        let mut input = ObjectDataInput::new(truncated);
        let result = Serde::<SimpleUser>::deserialize(&mut input);
        assert!(result.is_err());
    }

    #[test]
    fn test_multiple_values_sequential() {
        let user1 = SimpleUser {
            name: "Alice".to_string(),
            age: 30,
        };
        let user2 = SimpleUser {
            name: "Bob".to_string(),
            age: 25,
        };

        let mut output = ObjectDataOutput::new();
        Serde::new(user1.clone()).serialize(&mut output).unwrap();
        Serde::new(user2.clone()).serialize(&mut output).unwrap();

        let bytes = output.as_bytes();
        let mut input = ObjectDataInput::new(bytes);

        let result1 = Serde::<SimpleUser>::deserialize(&mut input)
            .unwrap()
            .into_inner();
        let result2 = Serde::<SimpleUser>::deserialize(&mut input)
            .unwrap()
            .into_inner();

        assert_eq!(result1, user1);
        assert_eq!(result2, user2);
        assert_eq!(input.remaining(), 0);
    }

    #[test]
    fn test_primitive_types_via_serde() {
        assert_eq!(round_trip(42i32), 42i32);
        assert_eq!(round_trip(3.14f64), 3.14f64);
        assert_eq!(round_trip(true), true);
        assert_eq!(round_trip("hello".to_string()), "hello");
    }

    #[test]
    fn test_tuple_round_trip() {
        let tuple = ("hello".to_string(), 42i32, true);
        let result = round_trip(tuple.clone());
        assert_eq!(result, tuple);
    }
}
