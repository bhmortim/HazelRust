//! Distributed multi-value map proxy implementation.

use std::collections::HashSet;
use std::hash::Hash;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::BytesMut;
use tokio::spawn;
use uuid::Uuid;
use hazelcast_core::protocol::constants::*;
use hazelcast_core::protocol::Frame;
use hazelcast_core::serialization::{ObjectDataInput, ObjectDataOutput};
use hazelcast_core::{
    compute_partition_hash, ClientMessage, Deserializable, HazelcastError, Result, Serializable,
};

use crate::connection::ConnectionManager;
use crate::listener::{
    dispatch_entry_event, BoxedEntryListener, EntryEvent, EntryEventType, EntryListener,
    EntryListenerConfig, ListenerId, ListenerRegistration, ListenerStats,
};

static MULTIMAP_INVOCATION_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Local statistics for MultiMap operations.
#[derive(Debug)]
pub struct LocalMultiMapStats {
    creation_time: i64,
    put_count: AtomicI64,
    get_count: AtomicI64,
    remove_count: AtomicI64,
    hit_count: AtomicI64,
    miss_count: AtomicI64,
    last_access_time: AtomicI64,
    last_update_time: AtomicI64,
}

impl LocalMultiMapStats {
    /// Creates a new LocalMultiMapStats with the current time as creation time.
    pub fn new() -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        Self {
            creation_time: now,
            put_count: AtomicI64::new(0),
            get_count: AtomicI64::new(0),
            remove_count: AtomicI64::new(0),
            hit_count: AtomicI64::new(0),
            miss_count: AtomicI64::new(0),
            last_access_time: AtomicI64::new(0),
            last_update_time: AtomicI64::new(0),
        }
    }

    /// Returns the creation time of this stats instance.
    pub fn creation_time(&self) -> i64 {
        self.creation_time
    }

    /// Returns the number of put operations.
    pub fn put_count(&self) -> i64 {
        self.put_count.load(Ordering::Relaxed)
    }

    /// Returns the number of get operations.
    pub fn get_count(&self) -> i64 {
        self.get_count.load(Ordering::Relaxed)
    }

    /// Returns the number of remove operations.
    pub fn remove_count(&self) -> i64 {
        self.remove_count.load(Ordering::Relaxed)
    }

    /// Returns the number of cache hits.
    pub fn hit_count(&self) -> i64 {
        self.hit_count.load(Ordering::Relaxed)
    }

    /// Returns the number of cache misses.
    pub fn miss_count(&self) -> i64 {
        self.miss_count.load(Ordering::Relaxed)
    }

    /// Returns the last access time in milliseconds since epoch.
    pub fn last_access_time(&self) -> i64 {
        self.last_access_time.load(Ordering::Relaxed)
    }

    /// Returns the last update time in milliseconds since epoch.
    pub fn last_update_time(&self) -> i64 {
        self.last_update_time.load(Ordering::Relaxed)
    }

    fn record_put(&self) {
        self.put_count.fetch_add(1, Ordering::Relaxed);
        self.update_last_update_time();
    }

    fn record_get(&self, hit: bool) {
        self.get_count.fetch_add(1, Ordering::Relaxed);
        if hit {
            self.hit_count.fetch_add(1, Ordering::Relaxed);
        } else {
            self.miss_count.fetch_add(1, Ordering::Relaxed);
        }
        self.update_last_access_time();
    }

    fn record_remove(&self) {
        self.remove_count.fetch_add(1, Ordering::Relaxed);
        self.update_last_update_time();
    }

    fn update_last_access_time(&self) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        self.last_access_time.store(now, Ordering::Relaxed);
    }

    fn update_last_update_time(&self) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        self.last_update_time.store(now, Ordering::Relaxed);
    }
}

impl Default for LocalMultiMapStats {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for LocalMultiMapStats {
    fn clone(&self) -> Self {
        Self {
            creation_time: self.creation_time,
            put_count: AtomicI64::new(self.put_count.load(Ordering::Relaxed)),
            get_count: AtomicI64::new(self.get_count.load(Ordering::Relaxed)),
            remove_count: AtomicI64::new(self.remove_count.load(Ordering::Relaxed)),
            hit_count: AtomicI64::new(self.hit_count.load(Ordering::Relaxed)),
            miss_count: AtomicI64::new(self.miss_count.load(Ordering::Relaxed)),
            last_access_time: AtomicI64::new(self.last_access_time.load(Ordering::Relaxed)),
            last_update_time: AtomicI64::new(self.last_update_time.load(Ordering::Relaxed)),
        }
    }
}

/// A distributed multi-value map proxy for storing multiple values per key.
///
/// `MultiMap` provides async operations for managing key-value pairs where
/// a single key can be associated with multiple values.
#[derive(Debug)]
pub struct MultiMap<K, V> {
    name: String,
    connection_manager: Arc<ConnectionManager>,
    listener_stats: Arc<ListenerStats>,
    local_stats: Arc<LocalMultiMapStats>,
    thread_id: i64,
    _phantom: PhantomData<fn() -> (K, V)>,
}

impl<K, V> MultiMap<K, V> {
    /// Creates a new multi-map proxy.
    pub(crate) fn new(name: String, connection_manager: Arc<ConnectionManager>) -> Self {
        Self {
            name,
            connection_manager,
            listener_stats: Arc::new(ListenerStats::new()),
            local_stats: Arc::new(LocalMultiMapStats::new()),
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
        let result = Self::decode_bool_response(&response)?;
        self.local_stats.record_put();
        Ok(result)
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
        let result = Self::decode_collection_response(&response)?;
        self.local_stats.record_get(!result.is_empty());
        Ok(result)
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
        let result = Self::decode_bool_response(&response)?;
        if result {
            self.local_stats.record_remove();
        }
        Ok(result)
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
        let result = Self::decode_collection_response(&response)?;
        if !result.is_empty() {
            self.local_stats.record_remove();
        }
        Ok(result)
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

    /// Tries to acquire the lock for the specified key with a lease time.
    ///
    /// Returns immediately if the lock is available. Otherwise waits up to
    /// the specified timeout for the lock to become available.
    ///
    /// If the lock is acquired, it will be automatically released after the
    /// lease time expires, even if `unlock` is not called.
    ///
    /// Returns `true` if the lock was acquired, `false` if the timeout expired.
    pub async fn try_lock_with_lease(
        &self,
        key: &K,
        timeout: Duration,
        lease_time: Duration,
    ) -> Result<bool> {
        let key_data = Self::serialize_value(key)?;
        let partition_id = compute_partition_hash(&key_data);
        let timeout_ms = timeout.as_millis() as i64;
        let lease_ms = lease_time.as_millis() as i64;

        let mut message = ClientMessage::create_for_encode(MULTI_MAP_TRY_LOCK, partition_id);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&key_data));
        message.add_frame(Self::long_frame(self.thread_id));
        message.add_frame(Self::long_frame(lease_ms));
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

    fn bool_frame(value: bool) -> Frame {
        let mut buf = BytesMut::with_capacity(1);
        buf.extend_from_slice(&[if value { 1 } else { 0 }]);
        Frame::with_content(buf)
    }

    fn int_frame(value: i32) -> Frame {
        let mut buf = BytesMut::with_capacity(4);
        buf.extend_from_slice(&value.to_le_bytes());
        Frame::with_content(buf)
    }

    fn uuid_frame(uuid: Uuid) -> Frame {
        let mut buf = BytesMut::with_capacity(16);
        buf.extend_from_slice(uuid.as_bytes());
        Frame::with_content(buf)
    }

    fn decode_uuid_response(response: &ClientMessage) -> Result<Uuid> {
        let frames = response.frames();
        if frames.is_empty() {
            return Err(HazelcastError::Serialization("empty response".to_string()));
        }

        let initial_frame = &frames[0];
        if initial_frame.content.len() >= RESPONSE_HEADER_SIZE + 16 {
            let offset = RESPONSE_HEADER_SIZE;
            let uuid_bytes: [u8; 16] = initial_frame.content[offset..offset + 16]
                .try_into()
                .map_err(|_| HazelcastError::Serialization("invalid UUID bytes".to_string()))?;
            Ok(Uuid::from_bytes(uuid_bytes))
        } else {
            Ok(Uuid::new_v4())
        }
    }

    fn is_entry_event(message: &ClientMessage, map_name: &str) -> bool {
        let frames = message.frames();
        if frames.is_empty() {
            return false;
        }

        let initial_frame = &frames[0];
        if initial_frame.flags & IS_EVENT_FLAG == 0 {
            return false;
        }

        if frames.len() > 1 {
            let name_frame = &frames[1];
            if let Ok(name) = std::str::from_utf8(&name_frame.content) {
                return name == map_name;
            }
        }

        true
    }

    fn decode_entry_event(message: &ClientMessage, include_value: bool) -> Result<EntryEvent<K, V>> {
        let frames = message.frames();
        if frames.len() < 3 {
            return Err(HazelcastError::Serialization(
                "insufficient frames for entry event".to_string(),
            ));
        }

        let initial_frame = &frames[0];
        let mut offset = RESPONSE_HEADER_SIZE;

        let event_type_value = if initial_frame.content.len() >= offset + 4 {
            i32::from_le_bytes([
                initial_frame.content[offset],
                initial_frame.content[offset + 1],
                initial_frame.content[offset + 2],
                initial_frame.content[offset + 3],
            ])
        } else {
            1
        };
        offset += 4;

        let event_type =
            EntryEventType::from_value(event_type_value).unwrap_or(EntryEventType::Added);

        let member_uuid = if initial_frame.content.len() >= offset + 16 {
            let uuid_bytes: [u8; 16] = initial_frame.content[offset..offset + 16]
                .try_into()
                .unwrap_or([0u8; 16]);
            Uuid::from_bytes(uuid_bytes)
        } else {
            Uuid::nil()
        };
        offset += 16;

        let timestamp = if initial_frame.content.len() >= offset + 8 {
            i64::from_le_bytes([
                initial_frame.content[offset],
                initial_frame.content[offset + 1],
                initial_frame.content[offset + 2],
                initial_frame.content[offset + 3],
                initial_frame.content[offset + 4],
                initial_frame.content[offset + 5],
                initial_frame.content[offset + 6],
                initial_frame.content[offset + 7],
            ])
        } else {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0)
        };

        let key_frame = &frames[2];
        let key = if !key_frame.content.is_empty() && key_frame.flags & IS_NULL_FLAG == 0 {
            let mut input = ObjectDataInput::new(&key_frame.content);
            K::deserialize(&mut input)?
        } else {
            return Err(HazelcastError::Serialization(
                "missing key in entry event".to_string(),
            ));
        };

        let (old_value, new_value) = if include_value && frames.len() >= 5 {
            let old_value_frame = &frames[3];
            let old_value = if !old_value_frame.content.is_empty()
                && old_value_frame.flags & IS_NULL_FLAG == 0
            {
                let mut input = ObjectDataInput::new(&old_value_frame.content);
                V::deserialize(&mut input).ok()
            } else {
                None
            };

            let new_value_frame = &frames[4];
            let new_value = if !new_value_frame.content.is_empty()
                && new_value_frame.flags & IS_NULL_FLAG == 0
            {
                let mut input = ObjectDataInput::new(&new_value_frame.content);
                V::deserialize(&mut input).ok()
            } else {
                None
            };

            (old_value, new_value)
        } else {
            (None, None)
        };

        Ok(EntryEvent::new(
            key,
            old_value,
            new_value,
            event_type,
            member_uuid,
            timestamp,
        ))
    }

    /// Adds an entry listener to this multi-map.
    ///
    /// The handler will be called for each entry event that matches the listener configuration.
    /// Returns a registration that can be used to remove the listener.
    pub async fn add_entry_listener<F>(
        &self,
        config: EntryListenerConfig,
        handler: F,
    ) -> Result<ListenerRegistration>
    where
        F: Fn(EntryEvent<K, V>) + Send + Sync + 'static,
        K: 'static,
        V: 'static,
    {
        let mut message =
            ClientMessage::create_for_encode(MULTI_MAP_ADD_ENTRY_LISTENER, PARTITION_ID_ANY);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::bool_frame(config.include_value));
        message.add_frame(Self::int_frame(config.event_flags()));
        message.add_frame(Self::bool_frame(false));

        let response = self.invoke(message).await?;
        let listener_uuid = Self::decode_uuid_response(&response)?;

        let registration = ListenerRegistration::new(ListenerId::from_uuid(listener_uuid));
        let active_flag = registration.active_flag();
        let shutdown_rx = registration.shutdown_receiver();

        let connection_manager = Arc::clone(&self.connection_manager);
        let listener_stats = Arc::clone(&self.listener_stats);
        let handler = Arc::new(handler);
        let map_name = self.name.clone();
        let include_value = config.include_value;

        spawn(async move {
            let mut shutdown_rx = match shutdown_rx {
                Some(rx) => rx,
                None => return,
            };

            loop {
                if !active_flag.load(Ordering::Acquire) {
                    break;
                }

                tokio::select! {
                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() {
                            break;
                        }
                    }
                    _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {
                        let addresses = connection_manager.connected_addresses().await;
                        for address in addresses {
                            if !active_flag.load(Ordering::Acquire) {
                                break;
                            }

                            match connection_manager.receive_from(address).await {
                                Ok(Some(msg)) => {
                                    if Self::is_entry_event(&msg, &map_name) {
                                        listener_stats.record_message();
                                        if let Ok(event) =
                                            Self::decode_entry_event(&msg, include_value)
                                        {
                                            handler(event);
                                        } else {
                                            listener_stats.record_error();
                                        }
                                    }
                                }
                                Ok(None) => {}
                                Err(_) => {
                                    listener_stats.record_error();
                                }
                            }
                        }
                    }
                }
            }
        });

        Ok(registration)
    }

    /// Adds an entry listener using the [`EntryListener`] trait.
    ///
    /// Events are dispatched to the appropriate trait method based on event type.
    pub async fn add_entry_listener_obj(
        &self,
        config: EntryListenerConfig,
        listener: BoxedEntryListener<K, V>,
    ) -> Result<ListenerRegistration>
    where
        K: 'static,
        V: 'static,
    {
        let mut message =
            ClientMessage::create_for_encode(MULTI_MAP_ADD_ENTRY_LISTENER, PARTITION_ID_ANY);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::bool_frame(config.include_value));
        message.add_frame(Self::int_frame(config.event_flags()));
        message.add_frame(Self::bool_frame(false));

        let response = self.invoke(message).await?;
        let listener_uuid = Self::decode_uuid_response(&response)?;

        let registration = ListenerRegistration::new(ListenerId::from_uuid(listener_uuid));
        let active_flag = registration.active_flag();
        let shutdown_rx = registration.shutdown_receiver();

        let connection_manager = Arc::clone(&self.connection_manager);
        let listener_stats = Arc::clone(&self.listener_stats);
        let map_name = self.name.clone();
        let include_value = config.include_value;

        spawn(async move {
            let mut shutdown_rx = match shutdown_rx {
                Some(rx) => rx,
                None => return,
            };

            loop {
                if !active_flag.load(Ordering::Acquire) {
                    break;
                }

                tokio::select! {
                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() {
                            break;
                        }
                    }
                    _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {
                        let addresses = connection_manager.connected_addresses().await;
                        for address in addresses {
                            if !active_flag.load(Ordering::Acquire) {
                                break;
                            }

                            match connection_manager.receive_from(address).await {
                                Ok(Some(msg)) => {
                                    if Self::is_entry_event(&msg, &map_name) {
                                        listener_stats.record_message();
                                        match Self::decode_entry_event(&msg, include_value) {
                                            Ok(event) => {
                                                dispatch_entry_event(listener.as_ref(), event);
                                            }
                                            Err(_) => {
                                                listener_stats.record_error();
                                            }
                                        }
                                    }
                                }
                                Ok(None) => {}
                                Err(_) => {
                                    listener_stats.record_error();
                                }
                            }
                        }
                    }
                }
            }
        });

        Ok(registration)
    }

    /// Adds an entry listener with a predicate filter.
    ///
    /// Only entries matching the predicate will trigger events.
    pub async fn add_entry_listener_with_predicate<F>(
        &self,
        config: EntryListenerConfig,
        predicate_data: &[u8],
        handler: F,
    ) -> Result<ListenerRegistration>
    where
        F: Fn(EntryEvent<K, V>) + Send + Sync + 'static,
        K: 'static,
        V: 'static,
    {
        let mut message =
            ClientMessage::create_for_encode(MULTI_MAP_ADD_ENTRY_LISTENER_WITH_PREDICATE, PARTITION_ID_ANY);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::bool_frame(config.include_value));
        message.add_frame(Self::int_frame(config.event_flags()));
        message.add_frame(Self::bool_frame(false));
        message.add_frame(Self::data_frame(predicate_data));

        let response = self.invoke(message).await?;
        let listener_uuid = Self::decode_uuid_response(&response)?;

        let registration = ListenerRegistration::new(ListenerId::from_uuid(listener_uuid));
        let active_flag = registration.active_flag();
        let shutdown_rx = registration.shutdown_receiver();

        let connection_manager = Arc::clone(&self.connection_manager);
        let listener_stats = Arc::clone(&self.listener_stats);
        let handler = Arc::new(handler);
        let map_name = self.name.clone();
        let include_value = config.include_value;

        spawn(async move {
            let mut shutdown_rx = match shutdown_rx {
                Some(rx) => rx,
                None => return,
            };

            loop {
                if !active_flag.load(Ordering::Acquire) {
                    break;
                }

                tokio::select! {
                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() {
                            break;
                        }
                    }
                    _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {
                        let addresses = connection_manager.connected_addresses().await;
                        for address in addresses {
                            if !active_flag.load(Ordering::Acquire) {
                                break;
                            }

                            match connection_manager.receive_from(address).await {
                                Ok(Some(msg)) => {
                                    if Self::is_entry_event(&msg, &map_name) {
                                        listener_stats.record_message();
                                        if let Ok(event) =
                                            Self::decode_entry_event(&msg, include_value)
                                        {
                                            handler(event);
                                        } else {
                                            listener_stats.record_error();
                                        }
                                    }
                                }
                                Ok(None) => {}
                                Err(_) => {
                                    listener_stats.record_error();
                                }
                            }
                        }
                    }
                }
            }
        });

        Ok(registration)
    }

    /// Adds a local entry listener to this multi-map.
    ///
    /// The handler will be called only for entry events that occur on the local member.
    /// This is more efficient than a regular entry listener when you only need local events.
    /// Returns a registration that can be used to remove the listener.
    pub async fn add_local_entry_listener<F>(
        &self,
        config: EntryListenerConfig,
        handler: F,
    ) -> Result<ListenerRegistration>
    where
        F: Fn(EntryEvent<K, V>) + Send + Sync + 'static,
        K: 'static,
        V: 'static,
    {
        let mut message =
            ClientMessage::create_for_encode(MULTI_MAP_ADD_ENTRY_LISTENER, PARTITION_ID_ANY);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::bool_frame(config.include_value));
        message.add_frame(Self::int_frame(config.event_flags()));
        message.add_frame(Self::bool_frame(true));

        let response = self.invoke(message).await?;
        let listener_uuid = Self::decode_uuid_response(&response)?;

        let registration = ListenerRegistration::new(ListenerId::from_uuid(listener_uuid));
        let active_flag = registration.active_flag();
        let shutdown_rx = registration.shutdown_receiver();

        let connection_manager = Arc::clone(&self.connection_manager);
        let listener_stats = Arc::clone(&self.listener_stats);
        let handler = Arc::new(handler);
        let map_name = self.name.clone();
        let include_value = config.include_value;

        spawn(async move {
            let mut shutdown_rx = match shutdown_rx {
                Some(rx) => rx,
                None => return,
            };

            loop {
                if !active_flag.load(Ordering::Acquire) {
                    break;
                }

                tokio::select! {
                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() {
                            break;
                        }
                    }
                    _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {
                        let addresses = connection_manager.connected_addresses().await;
                        for address in addresses {
                            if !active_flag.load(Ordering::Acquire) {
                                break;
                            }

                            match connection_manager.receive_from(address).await {
                                Ok(Some(msg)) => {
                                    if Self::is_entry_event(&msg, &map_name) {
                                        listener_stats.record_message();
                                        if let Ok(event) =
                                            Self::decode_entry_event(&msg, include_value)
                                        {
                                            handler(event);
                                        } else {
                                            listener_stats.record_error();
                                        }
                                    }
                                }
                                Ok(None) => {}
                                Err(_) => {
                                    listener_stats.record_error();
                                }
                            }
                        }
                    }
                }
            }
        });

        Ok(registration)
    }

    /// Adds a local entry listener using the [`EntryListener`] trait.
    ///
    /// Events are dispatched to the appropriate trait method based on event type.
    /// Only events from the local member will be delivered.
    pub async fn add_local_entry_listener_obj(
        &self,
        config: EntryListenerConfig,
        listener: BoxedEntryListener<K, V>,
    ) -> Result<ListenerRegistration>
    where
        K: 'static,
        V: 'static,
    {
        let mut message =
            ClientMessage::create_for_encode(MULTI_MAP_ADD_ENTRY_LISTENER, PARTITION_ID_ANY);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::bool_frame(config.include_value));
        message.add_frame(Self::int_frame(config.event_flags()));
        message.add_frame(Self::bool_frame(true));

        let response = self.invoke(message).await?;
        let listener_uuid = Self::decode_uuid_response(&response)?;

        let registration = ListenerRegistration::new(ListenerId::from_uuid(listener_uuid));
        let active_flag = registration.active_flag();
        let shutdown_rx = registration.shutdown_receiver();

        let connection_manager = Arc::clone(&self.connection_manager);
        let listener_stats = Arc::clone(&self.listener_stats);
        let map_name = self.name.clone();
        let include_value = config.include_value;

        spawn(async move {
            let mut shutdown_rx = match shutdown_rx {
                Some(rx) => rx,
                None => return,
            };

            loop {
                if !active_flag.load(Ordering::Acquire) {
                    break;
                }

                tokio::select! {
                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() {
                            break;
                        }
                    }
                    _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {
                        let addresses = connection_manager.connected_addresses().await;
                        for address in addresses {
                            if !active_flag.load(Ordering::Acquire) {
                                break;
                            }

                            match connection_manager.receive_from(address).await {
                                Ok(Some(msg)) => {
                                    if Self::is_entry_event(&msg, &map_name) {
                                        listener_stats.record_message();
                                        match Self::decode_entry_event(&msg, include_value) {
                                            Ok(event) => {
                                                dispatch_entry_event(listener.as_ref(), event);
                                            }
                                            Err(_) => {
                                                listener_stats.record_error();
                                            }
                                        }
                                    }
                                }
                                Ok(None) => {}
                                Err(_) => {
                                    listener_stats.record_error();
                                }
                            }
                        }
                    }
                }
            }
        });

        Ok(registration)
    }

    /// Removes an entry listener from this multi-map.
    ///
    /// Returns `true` if the listener was successfully removed.
    pub async fn remove_entry_listener(&self, registration: &ListenerRegistration) -> Result<bool> {
        registration.deactivate();

        let mut message =
            ClientMessage::create_for_encode(MULTI_MAP_REMOVE_ENTRY_LISTENER, PARTITION_ID_ANY);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::uuid_frame(registration.id().as_uuid()));

        let response = self.invoke(message).await?;
        Self::decode_bool_response(&response)
    }

    /// Returns the listener statistics for this multi-map.
    pub fn listener_stats(&self) -> &ListenerStats {
        &self.listener_stats
    }

    /// Returns a snapshot of the local statistics for this multi-map.
    ///
    /// The returned stats reflect operation counts and timing information
    /// for operations performed through this client instance.
    pub fn get_local_multi_map_stats(&self) -> LocalMultiMapStats {
        (*self.local_stats).clone()
    }
}

impl<K, V> Clone for MultiMap<K, V> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            connection_manager: Arc::clone(&self.connection_manager),
            listener_stats: Arc::clone(&self.listener_stats),
            local_stats: Arc::clone(&self.local_stats),
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

    #[test]
    fn test_local_multi_map_stats_default() {
        let stats = LocalMultiMapStats::default();
        assert_eq!(stats.put_count(), 0);
        assert_eq!(stats.get_count(), 0);
        assert_eq!(stats.remove_count(), 0);
        assert_eq!(stats.hit_count(), 0);
        assert_eq!(stats.miss_count(), 0);
        assert!(stats.creation_time() > 0);
    }

    #[test]
    fn test_local_multi_map_stats_record_put() {
        let stats = LocalMultiMapStats::new();
        stats.record_put();
        stats.record_put();
        assert_eq!(stats.put_count(), 2);
        assert!(stats.last_update_time() > 0);
    }

    #[test]
    fn test_local_multi_map_stats_record_get_hit() {
        let stats = LocalMultiMapStats::new();
        stats.record_get(true);
        assert_eq!(stats.get_count(), 1);
        assert_eq!(stats.hit_count(), 1);
        assert_eq!(stats.miss_count(), 0);
        assert!(stats.last_access_time() > 0);
    }

    #[test]
    fn test_local_multi_map_stats_record_get_miss() {
        let stats = LocalMultiMapStats::new();
        stats.record_get(false);
        assert_eq!(stats.get_count(), 1);
        assert_eq!(stats.hit_count(), 0);
        assert_eq!(stats.miss_count(), 1);
    }

    #[test]
    fn test_local_multi_map_stats_record_remove() {
        let stats = LocalMultiMapStats::new();
        stats.record_remove();
        assert_eq!(stats.remove_count(), 1);
        assert!(stats.last_update_time() > 0);
    }

    #[test]
    fn test_local_multi_map_stats_clone() {
        let stats = LocalMultiMapStats::new();
        stats.record_put();
        stats.record_get(true);
        stats.record_remove();

        let cloned = stats.clone();
        assert_eq!(cloned.put_count(), 1);
        assert_eq!(cloned.get_count(), 1);
        assert_eq!(cloned.remove_count(), 1);
        assert_eq!(cloned.hit_count(), 1);
        assert_eq!(cloned.creation_time(), stats.creation_time());
    }
}
