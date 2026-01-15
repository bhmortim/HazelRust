//! Distributed multi-value map proxy implementation.

use std::marker::PhantomData;
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::BytesMut;
use hazelcast_core::protocol::constants::*;
use hazelcast_core::protocol::Frame;
use hazelcast_core::serialization::{ObjectDataInput, ObjectDataOutput};
use hazelcast_core::{
    compute_partition_hash, ClientMessage, Deserializable, HazelcastError, Result, Serializable,
};

use crate::connection::ConnectionManager;

/// A distributed multi-value map proxy for storing multiple values per key.
///
/// `MultiMap` provides async operations for managing key-value pairs where
/// a single key can be associated with multiple values.
#[derive(Debug)]
pub struct MultiMap<K, V> {
    name: String,
    connection_manager: Arc<ConnectionManager>,
    _phantom: PhantomData<fn() -> (K, V)>,
}

impl<K, V> MultiMap<K, V> {
    /// Creates a new multi-map proxy.
    pub(crate) fn new(name: String, connection_manager: Arc<ConnectionManager>) -> Self {
        Self {
            name,
            connection_manager,
            _phantom: PhantomData,
        }
    }

    /// Returns the name of this multi-map.
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl<K, V> MultiMap<K, V>
where
    K: Serializable + Deserializable + Send + Sync,
    V: Serializable + Deserializable + Send + Sync,
{
    /// Stores a key-value pair in the multi-map.
    ///
    /// Returns `true` if the pair was added, `false` if it already existed.
    pub async fn put(&self, key: K, value: V) -> Result<bool> {
        let key_data = Self::serialize_value(&key)?;
        let value_data = Self::serialize_value(&value)?;
        let partition_id = compute_partition_hash(&key_data);

        let mut message = ClientMessage::create_for_encode(MULTI_MAP_PUT, partition_id);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&key_data));
        message.add_frame(Self::data_frame(&value_data));

        let response = self.invoke(message).await?;
        Self::decode_bool_response(&response)
    }

    /// Retrieves all values associated with the given key.
    ///
    /// Returns an empty vector if the key does not exist.
    pub async fn get(&self, key: &K) -> Result<Vec<V>> {
        let key_data = Self::serialize_value(key)?;
        let partition_id = compute_partition_hash(&key_data);

        let mut message = ClientMessage::create_for_encode(MULTI_MAP_GET, partition_id);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&key_data));

        let response = self.invoke(message).await?;
        Self::decode_collection_response(&response)
    }

    /// Removes a specific key-value pair from the multi-map.
    ///
    /// Returns `true` if the pair was removed, `false` if it did not exist.
    pub async fn remove(&self, key: &K, value: &V) -> Result<bool> {
        let key_data = Self::serialize_value(key)?;
        let value_data = Self::serialize_value(value)?;
        let partition_id = compute_partition_hash(&key_data);

        let mut message = ClientMessage::create_for_encode(MULTI_MAP_REMOVE_ENTRY, partition_id);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&key_data));
        message.add_frame(Self::data_frame(&value_data));

        let response = self.invoke(message).await?;
        Self::decode_bool_response(&response)
    }

    /// Removes all values associated with the given key.
    ///
    /// Returns all removed values.
    pub async fn remove_all(&self, key: &K) -> Result<Vec<V>> {
        let key_data = Self::serialize_value(key)?;
        let partition_id = compute_partition_hash(&key_data);

        let mut message = ClientMessage::create_for_encode(MULTI_MAP_REMOVE, partition_id);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&key_data));

        let response = self.invoke(message).await?;
        Self::decode_collection_response(&response)
    }

    /// Returns `true` if this multi-map contains the specified key.
    pub async fn contains_key(&self, key: &K) -> Result<bool> {
        let key_data = Self::serialize_value(key)?;
        let partition_id = compute_partition_hash(&key_data);

        let mut message = ClientMessage::create_for_encode(MULTI_MAP_CONTAINS_KEY, partition_id);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&key_data));

        let response = self.invoke(message).await?;
        Self::decode_bool_response(&response)
    }

    /// Returns `true` if this multi-map contains the specified value.
    pub async fn contains_value(&self, value: &V) -> Result<bool> {
        let value_data = Self::serialize_value(value)?;

        let mut message = ClientMessage::create_for_encode_any_partition(MULTI_MAP_CONTAINS_VALUE);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&value_data));

        let response = self.invoke(message).await?;
        Self::decode_bool_response(&response)
    }

    /// Returns `true` if this multi-map contains the specified key-value pair.
    pub async fn contains_entry(&self, key: &K, value: &V) -> Result<bool> {
        let key_data = Self::serialize_value(key)?;
        let value_data = Self::serialize_value(value)?;
        let partition_id = compute_partition_hash(&key_data);

        let mut message = ClientMessage::create_for_encode(MULTI_MAP_CONTAINS_ENTRY, partition_id);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&key_data));
        message.add_frame(Self::data_frame(&value_data));

        let response = self.invoke(message).await?;
        Self::decode_bool_response(&response)
    }

    /// Returns the total number of key-value pairs in this multi-map.
    pub async fn size(&self) -> Result<usize> {
        let mut message = ClientMessage::create_for_encode_any_partition(MULTI_MAP_SIZE);
        message.add_frame(Self::string_frame(&self.name));

        let response = self.invoke(message).await?;
        Self::decode_int_response(&response).map(|v| v as usize)
    }

    /// Returns the number of values associated with the given key.
    pub async fn value_count(&self, key: &K) -> Result<usize> {
        let key_data = Self::serialize_value(key)?;
        let partition_id = compute_partition_hash(&key_data);

        let mut message = ClientMessage::create_for_encode(MULTI_MAP_VALUE_COUNT, partition_id);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&key_data));

        let response = self.invoke(message).await?;
        Self::decode_int_response(&response).map(|v| v as usize)
    }

    /// Removes all entries from this multi-map.
    pub async fn clear(&self) -> Result<()> {
        let mut message = ClientMessage::create_for_encode_any_partition(MULTI_MAP_CLEAR);
        message.add_frame(Self::string_frame(&self.name));

        self.invoke(message).await?;
        Ok(())
    }

    fn serialize_value<T: Serializable>(value: &T) -> Result<Vec<u8>> {
        let mut output = ObjectDataOutput::new();
        value.serialize(&mut output)?;
        Ok(output.into_bytes())
    }

    fn string_frame(s: &str) -> Frame {
        Frame::with_content(BytesMut::from(s.as_bytes()))
    }

    fn data_frame(data: &[u8]) -> Frame {
        Frame::with_content(BytesMut::from(data))
    }

    async fn invoke(&self, message: ClientMessage) -> Result<ClientMessage> {
        let address = self.get_connection_address().await?;

        self.connection_manager.send_to(address, message).await?;
        self.connection_manager
            .receive_from(address)
            .await?
            .ok_or_else(|| HazelcastError::Connection("connection closed unexpectedly".to_string()))
    }

    async fn get_connection_address(&self) -> Result<SocketAddr> {
        let addresses = self.connection_manager.connected_addresses().await;
        addresses.into_iter().next().ok_or_else(|| {
            HazelcastError::Connection("no connections available".to_string())
        })
    }

    fn decode_bool_response(response: &ClientMessage) -> Result<bool> {
        let frames = response.frames();
        if frames.is_empty() {
            return Err(HazelcastError::Serialization(
                "empty response".to_string(),
            ));
        }

        let initial_frame = &frames[0];
        if initial_frame.content.len() > RESPONSE_HEADER_SIZE {
            Ok(initial_frame.content[RESPONSE_HEADER_SIZE] != 0)
        } else {
            Ok(false)
        }
    }

    fn decode_int_response(response: &ClientMessage) -> Result<i32> {
        let frames = response.frames();
        if frames.is_empty() {
            return Err(HazelcastError::Serialization(
                "empty response".to_string(),
            ));
        }

        let initial_frame = &frames[0];
        if initial_frame.content.len() >= RESPONSE_HEADER_SIZE + 4 {
            let offset = RESPONSE_HEADER_SIZE;
            Ok(i32::from_le_bytes([
                initial_frame.content[offset],
                initial_frame.content[offset + 1],
                initial_frame.content[offset + 2],
                initial_frame.content[offset + 3],
            ]))
        } else {
            Ok(0)
        }
    }

    fn decode_collection_response<T: Deserializable>(response: &ClientMessage) -> Result<Vec<T>> {
        let frames = response.frames();
        let mut result = Vec::new();

        for frame in frames.iter().skip(1) {
            if frame.flags & IS_NULL_FLAG != 0 {
                continue;
            }

            if frame.content.is_empty() {
                continue;
            }

            let mut input = ObjectDataInput::new(&frame.content);
            if let Ok(value) = T::deserialize(&mut input) {
                result.push(value);
            }
        }

        Ok(result)
    }
}

impl<K, V> Clone for MultiMap<K, V> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            connection_manager: Arc::clone(&self.connection_manager),
            _phantom: PhantomData,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_multimap_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<MultiMap<String, String>>();
    }

    #[test]
    fn test_multimap_clone() {
        fn assert_clone<T: Clone>() {}
        assert_clone::<MultiMap<String, String>>();
    }

    #[test]
    fn test_string_frame() {
        let frame = MultiMap::<String, String>::string_frame("test-multimap");
        assert_eq!(&frame.content[..], b"test-multimap");
    }

    #[test]
    fn test_data_frame() {
        let data = vec![1, 2, 3, 4];
        let frame = MultiMap::<String, String>::data_frame(&data);
        assert_eq!(&frame.content[..], &data[..]);
    }

    #[test]
    fn test_serialize_string() {
        let data = MultiMap::<String, String>::serialize_value(&"hello".to_string()).unwrap();
        assert!(!data.is_empty());
    }
}
