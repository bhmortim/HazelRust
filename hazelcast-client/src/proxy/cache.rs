//! JCache API proxy implementation.

use std::collections::HashMap;
use std::hash::Hash;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::BytesMut;
use hazelcast_core::protocol::constants::{
    CACHE_CLEAR, CACHE_CONTAINS_KEY, CACHE_GET, CACHE_GET_ALL, CACHE_GET_AND_PUT,
    CACHE_GET_AND_REMOVE, CACHE_GET_AND_REPLACE, CACHE_PUT, CACHE_PUT_ALL, CACHE_PUT_IF_ABSENT,
    CACHE_REMOVE, CACHE_REMOVE_ALL, CACHE_REPLACE, CACHE_REPLACE_IF_SAME, END_FLAG, IS_NULL_FLAG,
    PARTITION_ID_ANY, RESPONSE_HEADER_SIZE,
};
use hazelcast_core::protocol::Frame;
use hazelcast_core::serialization::{ObjectDataInput, ObjectDataOutput};
use hazelcast_core::{
    compute_partition_hash, ClientMessage, Deserializable, HazelcastError, Result, Serializable,
};

use crate::connection::ConnectionManager;

/// A distributed cache proxy implementing a subset of the JCache (JSR-107) API.
///
/// `ICache` provides standard caching operations with strong consistency guarantees.
/// Unlike `IMap`, caches are designed specifically for caching use cases and may have
/// different default behaviors for expiration, eviction, and persistence.
///
/// # Example
///
/// ```ignore
/// let cache = client.get_cache::<String, User>("user-cache");
///
/// // Basic operations
/// cache.put("user:1".to_string(), user).await?;
/// let user = cache.get(&"user:1".to_string()).await?;
///
/// // Atomic operations
/// let old = cache.get_and_put("user:1".to_string(), new_user).await?;
/// let removed = cache.get_and_remove(&"user:1".to_string()).await?;
///
/// // Conditional operations
/// cache.put_if_absent("user:2".to_string(), user2).await?;
/// cache.replace_if_equals(&"user:1".to_string(), &old_user, new_user).await?;
/// ```
#[derive(Debug)]
pub struct ICache<K, V> {
    name: String,
    connection_manager: Arc<ConnectionManager>,
    _phantom: PhantomData<fn() -> (K, V)>,
}

impl<K, V> ICache<K, V> {
    /// Creates a new cache proxy.
    pub(crate) fn new(name: String, connection_manager: Arc<ConnectionManager>) -> Self {
        Self {
            name,
            connection_manager,
            _phantom: PhantomData,
        }
    }

    /// Returns the name of this cache.
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl<K, V> ICache<K, V>
where
    K: Serializable + Deserializable + Send + Sync,
    V: Serializable + Deserializable + Send + Sync,
{
    /// Retrieves the value associated with the given key.
    ///
    /// Returns `None` if the key does not exist in the cache.
    pub async fn get(&self, key: &K) -> Result<Option<V>> {
        let key_data = Self::serialize_value(key)?;
        let partition_id = compute_partition_hash(&key_data);

        let mut message = ClientMessage::create_for_encode(CACHE_GET, partition_id);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&key_data));

        let response = self.invoke(message).await?;
        Self::decode_nullable_response(&response)
    }

    /// Retrieves all values for the given keys.
    ///
    /// Returns a map containing only the keys that exist in the cache.
    pub async fn get_all(&self, keys: &[K]) -> Result<HashMap<K, V>>
    where
        K: Clone + Eq + Hash,
    {
        if keys.is_empty() {
            return Ok(HashMap::new());
        }

        let mut message = ClientMessage::create_for_encode(CACHE_GET_ALL, PARTITION_ID_ANY);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::int_frame(keys.len() as i32));

        for key in keys {
            let key_data = Self::serialize_value(key)?;
            message.add_frame(Self::data_frame(&key_data));
        }

        let response = self.invoke(message).await?;
        Self::decode_entries_response(&response)
    }

    /// Associates the specified value with the specified key.
    ///
    /// If the cache previously contained a mapping for the key, the old value
    /// is replaced by the specified value.
    pub async fn put(&self, key: K, value: V) -> Result<()> {
        let key_data = Self::serialize_value(&key)?;
        let value_data = Self::serialize_value(&value)?;
        let partition_id = compute_partition_hash(&key_data);

        let mut message = ClientMessage::create_for_encode(CACHE_PUT, partition_id);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&key_data));
        message.add_frame(Self::data_frame(&value_data));

        self.invoke(message).await?;
        Ok(())
    }

    /// Puts all entries from the given map into this cache.
    ///
    /// This is more efficient than calling `put` for each entry individually.
    pub async fn put_all(&self, entries: HashMap<K, V>) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        let mut message = ClientMessage::create_for_encode(CACHE_PUT_ALL, PARTITION_ID_ANY);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::int_frame(entries.len() as i32));

        for (key, value) in entries {
            let key_data = Self::serialize_value(&key)?;
            let value_data = Self::serialize_value(&value)?;
            message.add_frame(Self::data_frame(&key_data));
            message.add_frame(Self::data_frame(&value_data));
        }

        self.invoke(message).await?;
        Ok(())
    }

    /// Associates the specified value with the specified key only if the key
    /// is not already associated with a value.
    ///
    /// Returns `true` if the value was inserted (key was absent),
    /// `false` if the key was already present.
    pub async fn put_if_absent(&self, key: K, value: V) -> Result<bool> {
        let key_data = Self::serialize_value(&key)?;
        let value_data = Self::serialize_value(&value)?;
        let partition_id = compute_partition_hash(&key_data);

        let mut message = ClientMessage::create_for_encode(CACHE_PUT_IF_ABSENT, partition_id);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&key_data));
        message.add_frame(Self::data_frame(&value_data));

        let response = self.invoke(message).await?;
        Self::decode_bool_response(&response)
    }

    /// Removes the mapping for a key from this cache if it is present.
    ///
    /// Returns `true` if the cache contained a mapping for the key,
    /// `false` otherwise.
    pub async fn remove(&self, key: &K) -> Result<bool> {
        let key_data = Self::serialize_value(key)?;
        let partition_id = compute_partition_hash(&key_data);

        let mut message = ClientMessage::create_for_encode(CACHE_REMOVE, partition_id);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&key_data));

        let response = self.invoke(message).await?;
        Self::decode_bool_response(&response)
    }

    /// Removes all entries from this cache.
    ///
    /// This clears all mappings from the cache.
    pub async fn remove_all(&self) -> Result<()> {
        let mut message = ClientMessage::create_for_encode(CACHE_REMOVE_ALL, PARTITION_ID_ANY);
        message.add_frame(Self::string_frame(&self.name));

        self.invoke(message).await?;
        Ok(())
    }

    /// Replaces the entry for the specified key only if it is currently mapped to some value.
    ///
    /// Returns `true` if the value was replaced, `false` if the key was not found.
    pub async fn replace(&self, key: K, value: V) -> Result<bool> {
        let key_data = Self::serialize_value(&key)?;
        let value_data = Self::serialize_value(&value)?;
        let partition_id = compute_partition_hash(&key_data);

        let mut message = ClientMessage::create_for_encode(CACHE_REPLACE, partition_id);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&key_data));
        message.add_frame(Self::data_frame(&value_data));

        let response = self.invoke(message).await?;
        Self::decode_bool_response(&response)
    }

    /// Replaces the entry for the specified key only if currently mapped to the specified value.
    ///
    /// This is an atomic compare-and-swap operation.
    ///
    /// Returns `true` if the value was replaced, `false` otherwise.
    pub async fn replace_if_equals(&self, key: &K, old_value: &V, new_value: V) -> Result<bool> {
        let key_data = Self::serialize_value(key)?;
        let old_value_data = Self::serialize_value(old_value)?;
        let new_value_data = Self::serialize_value(&new_value)?;
        let partition_id = compute_partition_hash(&key_data);

        let mut message = ClientMessage::create_for_encode(CACHE_REPLACE_IF_SAME, partition_id);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&key_data));
        message.add_frame(Self::data_frame(&old_value_data));
        message.add_frame(Self::data_frame(&new_value_data));

        let response = self.invoke(message).await?;
        Self::decode_bool_response(&response)
    }

    /// Returns `true` if this cache contains a mapping for the specified key.
    pub async fn contains_key(&self, key: &K) -> Result<bool> {
        let key_data = Self::serialize_value(key)?;
        let partition_id = compute_partition_hash(&key_data);

        let mut message = ClientMessage::create_for_encode(CACHE_CONTAINS_KEY, partition_id);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&key_data));

        let response = self.invoke(message).await?;
        Self::decode_bool_response(&response)
    }

    /// Clears all entries from this cache.
    ///
    /// This is equivalent to `remove_all()` but may have different semantics
    /// regarding cache listeners and statistics.
    pub async fn clear(&self) -> Result<()> {
        let mut message = ClientMessage::create_for_encode(CACHE_CLEAR, PARTITION_ID_ANY);
        message.add_frame(Self::string_frame(&self.name));

        self.invoke(message).await?;
        Ok(())
    }

    /// Associates the specified value with the specified key, returning the
    /// previously associated value if any.
    ///
    /// Returns `None` if there was no previous mapping for the key.
    pub async fn get_and_put(&self, key: K, value: V) -> Result<Option<V>> {
        let key_data = Self::serialize_value(&key)?;
        let value_data = Self::serialize_value(&value)?;
        let partition_id = compute_partition_hash(&key_data);

        let mut message = ClientMessage::create_for_encode(CACHE_GET_AND_PUT, partition_id);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&key_data));
        message.add_frame(Self::data_frame(&value_data));

        let response = self.invoke(message).await?;
        Self::decode_nullable_response(&response)
    }

    /// Removes the mapping for the specified key, returning the previously
    /// associated value if any.
    ///
    /// Returns `None` if there was no mapping for the key.
    pub async fn get_and_remove(&self, key: &K) -> Result<Option<V>> {
        let key_data = Self::serialize_value(key)?;
        let partition_id = compute_partition_hash(&key_data);

        let mut message = ClientMessage::create_for_encode(CACHE_GET_AND_REMOVE, partition_id);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&key_data));

        let response = self.invoke(message).await?;
        Self::decode_nullable_response(&response)
    }

    /// Replaces the value for the specified key, returning the previously
    /// associated value if any.
    ///
    /// Returns `None` if there was no mapping for the key (and no replacement occurred).
    pub async fn get_and_replace(&self, key: K, value: V) -> Result<Option<V>> {
        let key_data = Self::serialize_value(&key)?;
        let value_data = Self::serialize_value(&value)?;
        let partition_id = compute_partition_hash(&key_data);

        let mut message = ClientMessage::create_for_encode(CACHE_GET_AND_REPLACE, partition_id);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&key_data));
        message.add_frame(Self::data_frame(&value_data));

        let response = self.invoke(message).await?;
        Self::decode_nullable_response(&response)
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

    fn int_frame(value: i32) -> Frame {
        let mut buf = BytesMut::with_capacity(4);
        buf.extend_from_slice(&value.to_le_bytes());
        Frame::with_content(buf)
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

    fn decode_nullable_response<T: Deserializable>(response: &ClientMessage) -> Result<Option<T>> {
        let frames = response.frames();
        if frames.len() < 2 {
            return Ok(None);
        }

        let data_frame = &frames[1];

        if data_frame.flags & IS_NULL_FLAG != 0 {
            return Ok(None);
        }

        if data_frame.content.is_empty() {
            return Ok(None);
        }

        let mut input = ObjectDataInput::new(&data_frame.content);
        T::deserialize(&mut input).map(Some)
    }

    fn decode_bool_response(response: &ClientMessage) -> Result<bool> {
        let frames = response.frames();
        if frames.is_empty() {
            return Err(HazelcastError::Serialization("empty response".to_string()));
        }

        let initial_frame = &frames[0];
        if initial_frame.content.len() > RESPONSE_HEADER_SIZE {
            Ok(initial_frame.content[RESPONSE_HEADER_SIZE] != 0)
        } else {
            Ok(false)
        }
    }

    fn decode_entries_response(response: &ClientMessage) -> Result<HashMap<K, V>>
    where
        K: Eq + Hash,
    {
        let frames = response.frames();
        let mut entries = HashMap::new();

        let data_frames: Vec<_> = frames
            .iter()
            .skip(1)
            .filter(|f| f.flags & IS_NULL_FLAG == 0 && !f.content.is_empty())
            .collect();

        let mut i = 0;
        while i + 1 < data_frames.len() {
            let key_frame = data_frames[i];
            let value_frame = data_frames[i + 1];

            if key_frame.flags & END_FLAG != 0 && key_frame.content.is_empty() {
                break;
            }

            let mut key_input = ObjectDataInput::new(&key_frame.content);
            let mut value_input = ObjectDataInput::new(&value_frame.content);

            if let (Ok(key), Ok(value)) = (K::deserialize(&mut key_input), V::deserialize(&mut value_input)) {
                entries.insert(key, value);
            }

            i += 2;
        }

        Ok(entries)
    }
}

impl<K, V> Clone for ICache<K, V> {
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
    fn test_icache_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<ICache<String, String>>();
    }

    #[test]
    fn test_icache_clone() {
        fn assert_clone<T: Clone>() {}
        assert_clone::<ICache<String, String>>();
    }

    #[test]
    fn test_string_frame() {
        let frame = ICache::<String, String>::string_frame("test-cache");
        assert_eq!(&frame.content[..], b"test-cache");
    }

    #[test]
    fn test_int_frame() {
        let frame = ICache::<String, String>::int_frame(42);
        assert_eq!(frame.content.len(), 4);
        assert_eq!(
            i32::from_le_bytes(frame.content[..4].try_into().unwrap()),
            42
        );
    }

    #[test]
    fn test_serialize_string() {
        let data = ICache::<String, String>::serialize_value(&"hello".to_string()).unwrap();
        assert!(!data.is_empty());
    }

    #[test]
    fn test_decode_empty_entries_response() {
        let message = ClientMessage::create_for_encode(0, -1);
        let entries: HashMap<String, String> =
            ICache::<String, String>::decode_entries_response(&message).unwrap();
        assert!(entries.is_empty());
    }
}
