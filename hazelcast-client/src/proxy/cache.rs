//! JCache API proxy implementation.

use std::collections::HashMap;
use std::hash::Hash;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::BytesMut;
use futures::Stream;
use tokio::spawn;
use tokio::sync::mpsc;
use hazelcast_core::protocol::constants::{
    CACHE_CLEAR, CACHE_CONTAINS_KEY, CACHE_EVENT_JOURNAL_READ, CACHE_EVENT_JOURNAL_SUBSCRIBE,
    CACHE_GET, CACHE_GET_ALL, CACHE_GET_AND_PUT, CACHE_GET_AND_REMOVE, CACHE_GET_AND_REPLACE,
    CACHE_PUT, CACHE_PUT_ALL, CACHE_PUT_IF_ABSENT, CACHE_REMOVE, CACHE_REMOVE_ALL, CACHE_REPLACE,
    CACHE_REPLACE_IF_SAME, END_FLAG, IS_NULL_FLAG, PARTITION_ID_ANY, RESPONSE_HEADER_SIZE,
};
use hazelcast_core::protocol::Frame;
use hazelcast_core::serialization::{ObjectDataInput, ObjectDataOutput};
use hazelcast_core::{
    compute_partition_hash, ClientMessage, Deserializable, HazelcastError, Result, Serializable,
};

use crate::connection::ConnectionManager;

/// Event types for Event Journal cache events.
///
/// These represent the different kinds of mutations that can occur on cache entries
/// and are recorded in the Event Journal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i32)]
pub enum EventJournalCacheEventType {
    /// Entry was created in the cache.
    Created = 1,
    /// Entry was updated in the cache.
    Updated = 2,
    /// Entry was removed from the cache.
    Removed = 3,
    /// Entry expired in the cache.
    Expired = 4,
}

impl EventJournalCacheEventType {
    /// Creates an event type from its integer value.
    ///
    /// Returns `None` if the value doesn't correspond to a valid event type.
    pub fn from_value(value: i32) -> Option<Self> {
        match value {
            1 => Some(Self::Created),
            2 => Some(Self::Updated),
            3 => Some(Self::Removed),
            4 => Some(Self::Expired),
            _ => None,
        }
    }

    /// Returns the integer value of this event type.
    pub fn value(self) -> i32 {
        self as i32
    }
}

/// An event from the Event Journal for a cache entry.
///
/// Event Journal events represent mutations that have occurred on cache entries.
/// Each event has a sequence number that can be used to track position in the journal.
#[derive(Debug, Clone)]
pub struct EventJournalCacheEvent<K, V> {
    /// The type of this event.
    pub event_type: EventJournalCacheEventType,
    /// The key of the entry.
    pub key: K,
    /// The old value (before the change), if available.
    pub old_value: Option<V>,
    /// The new value (after the change), if available.
    pub new_value: Option<V>,
    /// The sequence number of this event in the journal.
    pub sequence: i64,
}

impl<K, V> EventJournalCacheEvent<K, V> {
    /// Returns the event type.
    pub fn event_type(&self) -> EventJournalCacheEventType {
        self.event_type
    }

    /// Returns a reference to the key.
    pub fn key(&self) -> &K {
        &self.key
    }

    /// Returns a reference to the old value, if present.
    pub fn old_value(&self) -> Option<&V> {
        self.old_value.as_ref()
    }

    /// Returns a reference to the new value, if present.
    pub fn new_value(&self) -> Option<&V> {
        self.new_value.as_ref()
    }

    /// Returns the sequence number of this event.
    pub fn sequence(&self) -> i64 {
        self.sequence
    }
}

/// Configuration for reading from a cache Event Journal.
#[derive(Debug, Clone)]
pub struct CacheEventJournalConfig {
    /// The starting sequence number to read from.
    pub start_sequence: i64,
    /// The minimum number of events to read in each batch.
    pub min_size: i32,
    /// The maximum number of events to read in each batch.
    pub max_size: i32,
}

impl Default for CacheEventJournalConfig {
    fn default() -> Self {
        Self {
            start_sequence: -1,
            min_size: 1,
            max_size: 100,
        }
    }
}

impl CacheEventJournalConfig {
    /// Creates a new configuration with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the starting sequence number.
    ///
    /// Use `-1` to start from the oldest available sequence.
    pub fn start_sequence(mut self, sequence: i64) -> Self {
        self.start_sequence = sequence;
        self
    }

    /// Sets the minimum batch size.
    pub fn min_size(mut self, size: i32) -> Self {
        self.min_size = size;
        self
    }

    /// Sets the maximum batch size.
    pub fn max_size(mut self, size: i32) -> Self {
        self.max_size = size;
        self
    }
}

/// An async stream of cache Event Journal events.
///
/// This stream yields events from the Event Journal as they are read from the cluster.
pub struct CacheEventJournalStream<K, V> {
    receiver: mpsc::Receiver<Result<EventJournalCacheEvent<K, V>>>,
}

impl<K, V> Stream for CacheEventJournalStream<K, V> {
    type Item = Result<EventJournalCacheEvent<K, V>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.receiver).poll_recv(cx)
    }
}

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

    /// Reads events from the Event Journal for this cache.
    ///
    /// The Event Journal is a ring buffer that stores a history of mutations on cache entries.
    /// This method returns an async stream that yields events as they are read from the journal.
    ///
    /// # Arguments
    ///
    /// * `partition_id` - The partition to read events from
    /// * `config` - Configuration for reading from the journal
    ///
    /// # Returns
    ///
    /// An async stream of `EventJournalCacheEvent<K, V>` representing mutations.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use futures::StreamExt;
    ///
    /// let config = CacheEventJournalConfig::new()
    ///     .start_sequence(-1)
    ///     .max_size(100);
    ///
    /// let mut stream = cache.read_from_event_journal(0, config).await?;
    ///
    /// while let Some(result) = stream.next().await {
    ///     match result {
    ///         Ok(event) => {
    ///             println!("Event: {:?}, Key: {:?}, Seq: {}",
    ///                 event.event_type(), event.key(), event.sequence());
    ///         }
    ///         Err(e) => eprintln!("Error: {}", e),
    ///     }
    /// }
    /// ```
    pub async fn read_from_event_journal(
        &self,
        partition_id: i32,
        config: CacheEventJournalConfig,
    ) -> Result<CacheEventJournalStream<K, V>>
    where
        K: 'static,
        V: 'static,
    {
        let (oldest_sequence, _newest_sequence) = self.subscribe_to_event_journal(partition_id).await?;

        let start_sequence = if config.start_sequence < 0 {
            oldest_sequence
        } else {
            config.start_sequence.max(oldest_sequence)
        };

        let (tx, rx) = mpsc::channel(config.max_size as usize);

        let connection_manager = Arc::clone(&self.connection_manager);
        let cache_name = self.name.clone();
        let min_size = config.min_size;
        let max_size = config.max_size;

        spawn(async move {
            let mut current_sequence = start_sequence;

            loop {
                match Self::read_journal_batch(
                    &connection_manager,
                    &cache_name,
                    partition_id,
                    current_sequence,
                    min_size,
                    max_size,
                )
                .await
                {
                    Ok(events) => {
                        if events.is_empty() {
                            tokio::time::sleep(Duration::from_millis(100)).await;
                            continue;
                        }

                        for event in events {
                            current_sequence = event.sequence + 1;
                            if tx.send(Ok(event)).await.is_err() {
                                return;
                            }
                        }
                    }
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                        return;
                    }
                }
            }
        });

        Ok(CacheEventJournalStream { receiver: rx })
    }

    async fn subscribe_to_event_journal(&self, partition_id: i32) -> Result<(i64, i64)> {
        let mut message =
            ClientMessage::create_for_encode(CACHE_EVENT_JOURNAL_SUBSCRIBE, partition_id);
        message.add_frame(Self::string_frame(&self.name));

        let response = self.invoke(message).await?;
        Self::decode_event_journal_subscribe_response(&response)
    }

    async fn read_journal_batch(
        connection_manager: &ConnectionManager,
        cache_name: &str,
        partition_id: i32,
        start_sequence: i64,
        min_size: i32,
        max_size: i32,
    ) -> Result<Vec<EventJournalCacheEvent<K, V>>> {
        let mut message = ClientMessage::create_for_encode(CACHE_EVENT_JOURNAL_READ, partition_id);
        message.add_frame(Self::string_frame(cache_name));
        message.add_frame(Self::long_frame(start_sequence));
        message.add_frame(Self::int_frame(min_size));
        message.add_frame(Self::int_frame(max_size));

        let addresses = connection_manager.connected_addresses().await;
        let address = addresses.into_iter().next().ok_or_else(|| {
            HazelcastError::Connection("no connections available".to_string())
        })?;

        connection_manager.send_to(address, message).await?;
        let response = connection_manager
            .receive_from(address)
            .await?
            .ok_or_else(|| {
                HazelcastError::Connection("connection closed unexpectedly".to_string())
            })?;

        Self::decode_event_journal_read_response(&response)
    }

    fn decode_event_journal_subscribe_response(response: &ClientMessage) -> Result<(i64, i64)> {
        let frames = response.frames();
        if frames.is_empty() {
            return Err(HazelcastError::Serialization(
                "empty event journal subscribe response".to_string(),
            ));
        }

        let initial_frame = &frames[0];
        if initial_frame.content.len() < RESPONSE_HEADER_SIZE + 16 {
            return Err(HazelcastError::Serialization(
                "invalid event journal subscribe response".to_string(),
            ));
        }

        let mut offset = RESPONSE_HEADER_SIZE;
        let oldest_sequence = i64::from_le_bytes(
            initial_frame.content[offset..offset + 8]
                .try_into()
                .map_err(|_| {
                    HazelcastError::Serialization("invalid oldest sequence".to_string())
                })?,
        );
        offset += 8;

        let newest_sequence = i64::from_le_bytes(
            initial_frame.content[offset..offset + 8]
                .try_into()
                .map_err(|_| {
                    HazelcastError::Serialization("invalid newest sequence".to_string())
                })?,
        );

        Ok((oldest_sequence, newest_sequence))
    }

    fn decode_event_journal_read_response(
        response: &ClientMessage,
    ) -> Result<Vec<EventJournalCacheEvent<K, V>>> {
        let frames = response.frames();
        if frames.is_empty() {
            return Ok(Vec::new());
        }

        let initial_frame = &frames[0];
        let mut offset = RESPONSE_HEADER_SIZE;

        let count = if initial_frame.content.len() >= offset + 4 {
            i32::from_le_bytes([
                initial_frame.content[offset],
                initial_frame.content[offset + 1],
                initial_frame.content[offset + 2],
                initial_frame.content[offset + 3],
            ])
        } else {
            return Ok(Vec::new());
        };
        offset += 4;

        let _next_sequence = if initial_frame.content.len() >= offset + 8 {
            i64::from_le_bytes(
                initial_frame.content[offset..offset + 8]
                    .try_into()
                    .unwrap_or([0u8; 8]),
            )
        } else {
            0
        };

        let mut events = Vec::with_capacity(count as usize);
        let mut frame_idx = 1;

        for _ in 0..count {
            if frame_idx + 4 >= frames.len() {
                break;
            }

            let event_type_frame = &frames[frame_idx];
            let event_type_value = if event_type_frame.content.len() >= 4 {
                i32::from_le_bytes([
                    event_type_frame.content[0],
                    event_type_frame.content[1],
                    event_type_frame.content[2],
                    event_type_frame.content[3],
                ])
            } else {
                1
            };
            frame_idx += 1;

            let event_type =
                EventJournalCacheEventType::from_value(event_type_value).unwrap_or(EventJournalCacheEventType::Created);

            let sequence_frame = &frames[frame_idx];
            let sequence = if sequence_frame.content.len() >= 8 {
                i64::from_le_bytes(
                    sequence_frame.content[..8]
                        .try_into()
                        .unwrap_or([0u8; 8]),
                )
            } else {
                0
            };
            frame_idx += 1;

            let key_frame = &frames[frame_idx];
            let key = if !key_frame.content.is_empty() && key_frame.flags & IS_NULL_FLAG == 0 {
                let mut input = ObjectDataInput::new(&key_frame.content);
                K::deserialize(&mut input)?
            } else {
                frame_idx += 3;
                continue;
            };
            frame_idx += 1;

            let old_value_frame = &frames[frame_idx];
            let old_value = if !old_value_frame.content.is_empty()
                && old_value_frame.flags & IS_NULL_FLAG == 0
            {
                let mut input = ObjectDataInput::new(&old_value_frame.content);
                V::deserialize(&mut input).ok()
            } else {
                None
            };
            frame_idx += 1;

            let new_value_frame = &frames[frame_idx];
            let new_value = if !new_value_frame.content.is_empty()
                && new_value_frame.flags & IS_NULL_FLAG == 0
            {
                let mut input = ObjectDataInput::new(&new_value_frame.content);
                V::deserialize(&mut input).ok()
            } else {
                None
            };
            frame_idx += 1;

            events.push(EventJournalCacheEvent {
                event_type,
                key,
                old_value,
                new_value,
                sequence,
            });
        }

        Ok(events)
    }

    fn long_frame(value: i64) -> Frame {
        let mut buf = BytesMut::with_capacity(8);
        buf.extend_from_slice(&value.to_le_bytes());
        Frame::with_content(buf)
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

    #[test]
    fn test_event_journal_cache_event_type_from_value() {
        assert_eq!(
            EventJournalCacheEventType::from_value(1),
            Some(EventJournalCacheEventType::Created)
        );
        assert_eq!(
            EventJournalCacheEventType::from_value(2),
            Some(EventJournalCacheEventType::Updated)
        );
        assert_eq!(
            EventJournalCacheEventType::from_value(3),
            Some(EventJournalCacheEventType::Removed)
        );
        assert_eq!(
            EventJournalCacheEventType::from_value(4),
            Some(EventJournalCacheEventType::Expired)
        );
        assert_eq!(EventJournalCacheEventType::from_value(0), None);
        assert_eq!(EventJournalCacheEventType::from_value(5), None);
    }

    #[test]
    fn test_event_journal_cache_event_type_value() {
        assert_eq!(EventJournalCacheEventType::Created.value(), 1);
        assert_eq!(EventJournalCacheEventType::Updated.value(), 2);
        assert_eq!(EventJournalCacheEventType::Removed.value(), 3);
        assert_eq!(EventJournalCacheEventType::Expired.value(), 4);
    }

    #[test]
    fn test_event_journal_cache_event_accessors() {
        let event: EventJournalCacheEvent<String, i32> = EventJournalCacheEvent {
            event_type: EventJournalCacheEventType::Updated,
            key: "test-key".to_string(),
            old_value: Some(10),
            new_value: Some(20),
            sequence: 42,
        };

        assert_eq!(event.event_type(), EventJournalCacheEventType::Updated);
        assert_eq!(event.key(), "test-key");
        assert_eq!(event.old_value(), Some(&10));
        assert_eq!(event.new_value(), Some(&20));
        assert_eq!(event.sequence(), 42);
    }

    #[test]
    fn test_event_journal_cache_event_none_values() {
        let event: EventJournalCacheEvent<String, i32> = EventJournalCacheEvent {
            event_type: EventJournalCacheEventType::Created,
            key: "key".to_string(),
            old_value: None,
            new_value: Some(100),
            sequence: 1,
        };

        assert_eq!(event.old_value(), None);
        assert_eq!(event.new_value(), Some(&100));
    }

    #[test]
    fn test_cache_event_journal_config_default() {
        let config = CacheEventJournalConfig::default();
        assert_eq!(config.start_sequence, -1);
        assert_eq!(config.min_size, 1);
        assert_eq!(config.max_size, 100);
    }

    #[test]
    fn test_cache_event_journal_config_builder() {
        let config = CacheEventJournalConfig::new()
            .start_sequence(100)
            .min_size(10)
            .max_size(500);

        assert_eq!(config.start_sequence, 100);
        assert_eq!(config.min_size, 10);
        assert_eq!(config.max_size, 500);
    }

    #[test]
    fn test_event_journal_cache_event_type_equality() {
        assert_eq!(EventJournalCacheEventType::Created, EventJournalCacheEventType::Created);
        assert_ne!(EventJournalCacheEventType::Created, EventJournalCacheEventType::Removed);
    }

    #[test]
    fn test_event_journal_cache_event_type_copy() {
        let t1 = EventJournalCacheEventType::Updated;
        let t2 = t1;
        assert_eq!(t1, t2);
    }

    #[test]
    fn test_event_journal_cache_event_clone() {
        let event: EventJournalCacheEvent<String, i32> = EventJournalCacheEvent {
            event_type: EventJournalCacheEventType::Expired,
            key: "key".to_string(),
            old_value: Some(5),
            new_value: None,
            sequence: 99,
        };

        let cloned = event.clone();
        assert_eq!(cloned.event_type, event.event_type);
        assert_eq!(cloned.key, event.key);
        assert_eq!(cloned.old_value, event.old_value);
        assert_eq!(cloned.new_value, event.new_value);
        assert_eq!(cloned.sequence, event.sequence);
    }

    #[test]
    fn test_event_journal_cache_event_debug() {
        let event: EventJournalCacheEvent<String, i32> = EventJournalCacheEvent {
            event_type: EventJournalCacheEventType::Removed,
            key: "k".to_string(),
            old_value: Some(1),
            new_value: None,
            sequence: 0,
        };

        let debug_str = format!("{:?}", event);
        assert!(debug_str.contains("EventJournalCacheEvent"));
        assert!(debug_str.contains("Removed"));
    }

    #[test]
    fn test_cache_event_journal_stream_is_send() {
        fn assert_send<T: Send>() {}
        assert_send::<CacheEventJournalStream<String, String>>();
    }
}
