//! Distributed multi-value map proxy implementation.

use std::collections::HashSet;
use std::hash::Hash;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::BytesMut;
use hazelcast_core::protocol::constants::*;
use hazelcast_core::protocol::Frame;
use hazelcast_core::serialization::{ObjectDataInput, ObjectDataOutput};
use hazelcast_core::{
    compute_partition_hash, ClientMessage, Deserializable, HazelcastError, Result, Serializable,
};

use crate::connection::ConnectionManager;

static MULTIMAP_INVOCATION_COUNTER: AtomicU64 = AtomicU64::new(1);

/// A distributed multi-value map proxy for storing multiple values per key.
///
/// `MultiMap` provides async operations for managing key-value pairs where
/// a single key can be associated with multiple values.
#[derive(Debug)]
pub struct MultiMap<K, V> {
    name: String,
    connection_manager: Arc<ConnectionManager>,
    thread_id: i64,
    _phantom: PhantomData<fn() -> (K, V)>,
}

impl<K, V> MultiMap<K, V> {
    /// Creates a new multi-map proxy.
    pub(crate) fn new(name: String, connection_manager: Arc<ConnectionManager>) -> Self {
        Self {
            name,
            connection_manager,
            thread_id: std::process::id() as i64,
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

    /// Returns a set view of the keys contained in this multi-map.
    pub async fn key_set(&self) -> Result<HashSet<K>>
    where
        K: Eq + Hash,
    {
        let mut message = ClientMessage::create_for_encode_any_partition(MULTI_MAP_KEY_SET);
        message.add_frame(Self::string_frame(&self.name));

        let response = self.invoke(message).await?;
        let keys: Vec<K> = Self::decode_collection_response(&response)?;
        Ok(keys.into_iter().collect())
    }

    /// Returns a collection view of the values contained in this multi-map.
    pub async fn values(&self) -> Result<Vec<V>> {
        let mut message = ClientMessage::create_for_encode_any_partition(MULTI_MAP_VALUES);
        message.add_frame(Self::string_frame(&self.name));

        let response = self.invoke(message).await?;
        Self::decode_collection_response(&response)
    }

    /// Returns a collection view of all key-value pairs contained in this multi-map.
    pub async fn entry_set(&self) -> Result<Vec<(K, V)>> {
        let mut message = ClientMessage::create_for_encode_any_partition(MULTI_MAP_ENTRY_SET);
        message.add_frame(Self::string_frame(&self.name));

        let response = self.invoke(message).await?;
        Self::decode_entry_set_response(&response)
    }

    /// Acquires the lock for the specified key.
    ///
    /// If the lock is not available, the current task waits until the lock
    /// has been acquired.
    ///
    /// Locks are re-entrant: the same thread can acquire the lock multiple
    /// times. Each `lock` call must be balanced by a corresponding `unlock`.
    pub async fn lock(&self, key: &K) -> Result<()> {
        let key_data = Self::serialize_value(key)?;
        let partition_id = compute_partition_hash(&key_data);

        let mut message = ClientMessage::create_for_encode(MULTI_MAP_LOCK, partition_id);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&key_data));
        message.add_frame(Self::long_frame(self.thread_id));
        message.add_frame(Self::long_frame(-1)); // TTL: indefinite
        message.add_frame(Self::invocation_uid_frame());

        self.invoke(message).await?;
        Ok(())
    }

    /// Tries to acquire the lock for the specified key within the given timeout.
    ///
    /// Returns immediately if the lock is available. Otherwise waits up to
    /// the specified timeout for the lock to become available.
    ///
    /// Returns `true` if the lock was acquired, `false` if the timeout expired.
    pub async fn try_lock(&self, key: &K, timeout: Duration) -> Result<bool> {
        let key_data = Self::serialize_value(key)?;
        let partition_id = compute_partition_hash(&key_data);
        let timeout_ms = timeout.as_millis() as i64;

        let mut message = ClientMessage::create_for_encode(MULTI_MAP_TRY_LOCK, partition_id);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&key_data));
        message.add_frame(Self::long_frame(self.thread_id));
        message.add_frame(Self::long_frame(-1)); // TTL: indefinite once acquired
        message.add_frame(Self::long_frame(timeout_ms));
        message.add_frame(Self::invocation_uid_frame());

        let response = self.invoke(message).await?;
        Self::decode_bool_response(&response)
    }

    /// Releases the lock for the specified key.
    ///
    /// The lock must have been acquired by this thread. If the lock was
    /// acquired multiple times (re-entrant), it must be released the same
    /// number of times before other threads can acquire it.
    pub async fn unlock(&self, key: &K) -> Result<()> {
        let key_data = Self::serialize_value(key)?;
        let partition_id = compute_partition_hash(&key_data);

        let mut message = ClientMessage::create_for_encode(MULTI_MAP_UNLOCK, partition_id);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&key_data));
        message.add_frame(Self::long_frame(self.thread_id));
        message.add_frame(Self::invocation_uid_frame());

        self.invoke(message).await?;
        Ok(())
    }

    /// Returns whether the specified key is locked.
    ///
    /// Returns `true` if the key is locked by any thread, `false` otherwise.
    pub async fn is_locked(&self, key: &K) -> Result<bool> {
        let key_data = Self::serialize_value(key)?;
        let partition_id = compute_partition_hash(&key_data);

        let mut message = ClientMessage::create_for_encode(MULTI_MAP_IS_LOCKED, partition_id);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&key_data));

        let response = self.invoke(message).await?;
        Self::decode_bool_response(&response)
    }

    /// Forcefully unlocks the specified key, regardless of the lock owner.
    ///
    /// This method should be used with caution. It can break the lock
    /// semantics and lead to data inconsistency if used incorrectly.
    pub async fn force_unlock(&self, key: &K) -> Result<()> {
        let key_data = Self::serialize_value(key)?;
        let partition_id = compute_partition_hash(&key_data);

        let mut message = ClientMessage::create_for_encode(MULTI_MAP_FORCE_UNLOCK, partition_id);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&key_data));
        message.add_frame(Self::invocation_uid_frame());

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

    fn long_frame(value: i64) -> Frame {
        let mut buf = BytesMut::with_capacity(8);
        buf.extend_from_slice(&value.to_le_bytes());
        Frame::with_content(buf)
    }

    fn invocation_uid_frame() -> Frame {
        let counter = MULTIMAP_INVOCATION_COUNTER.fetch_add(1, Ordering::Relaxed);
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);

        let mut buf = BytesMut::with_capacity(16);
        buf.extend_from_slice(&timestamp.to_le_bytes());
        buf.extend_from_slice(&counter.to_le_bytes());
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

    fn decode_entry_set_response(response: &ClientMessage) -> Result<Vec<(K, V)>> {
        let frames = response.frames();
        let mut entries = Vec::new();

        let data_frames: Vec<_> = frames
            .iter()
            .skip(1)
            .filter(|f| f.flags & IS_NULL_FLAG == 0 && !f.content.is_empty())
            .collect();

        let mut i = 0;
        while i + 1 < data_frames.len() {
            let key_frame = data_frames[i];
            let value_frame = data_frames[i + 1];

            let mut key_input = ObjectDataInput::new(&key_frame.content);
            let mut value_input = ObjectDataInput::new(&value_frame.content);

            if let (Ok(key), Ok(value)) = (K::deserialize(&mut key_input), V::deserialize(&mut value_input)) {
                entries.push((key, value));
            }

            i += 2;
        }

        Ok(entries)
    }
}

impl<K, V> Clone for MultiMap<K, V> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            connection_manager: Arc::clone(&self.connection_manager),
            thread_id: self.thread_id,
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

    #[test]
    fn test_long_frame() {
        let frame = MultiMap::<String, String>::long_frame(-1);
        assert_eq!(frame.content.len(), 8);
        assert_eq!(
            i64::from_le_bytes(frame.content[..8].try_into().unwrap()),
            -1
        );
    }

    #[test]
    fn test_invocation_uid_frame() {
        let frame = MultiMap::<String, String>::invocation_uid_frame();
        assert_eq!(frame.content.len(), 16);
    }

    #[test]
    fn test_multimap_thread_id_preserved_on_clone() {
        use crate::connection::ConnectionManager;
        use std::net::SocketAddr;

        let addr: SocketAddr = "127.0.0.1:5701".parse().unwrap();
        let cm = Arc::new(ConnectionManager::new(vec![addr]));
        let map1: MultiMap<String, String> = MultiMap::new("test".to_string(), cm);
        let map2 = map1.clone();

        assert_eq!(map1.thread_id, map2.thread_id);
    }
}
