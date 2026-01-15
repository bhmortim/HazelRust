//! Distributed map proxy implementation.

use std::collections::HashMap;
use std::hash::Hash;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::BytesMut;
use tokio::spawn;
use uuid::Uuid;
use hazelcast_core::protocol::constants::{
    IS_EVENT_FLAG, IS_NULL_FLAG, END_FLAG, MAP_ADD_ENTRY_LISTENER, MAP_ADD_INDEX, MAP_AGGREGATE,
    MAP_AGGREGATE_WITH_PREDICATE, MAP_CLEAR, MAP_CONTAINS_KEY, MAP_ENTRIES_WITH_PREDICATE,
    MAP_EXECUTE_ON_ALL_KEYS, MAP_EXECUTE_ON_KEY, MAP_EXECUTE_ON_KEYS, MAP_GET,
    MAP_KEYS_WITH_PREDICATE, MAP_PROJECT, MAP_PROJECT_WITH_PREDICATE, MAP_PUT, MAP_REMOVE,
    MAP_REMOVE_ENTRY_LISTENER, MAP_SIZE, MAP_VALUES_WITH_PREDICATE, PARTITION_ID_ANY,
    RESPONSE_HEADER_SIZE,
};
use hazelcast_core::protocol::Frame;
use hazelcast_core::serialization::{ObjectDataInput, ObjectDataOutput};
use hazelcast_core::{
    compute_partition_hash, ClientMessage, Deserializable, HazelcastError, Result, Serializable,
};

use crate::cache::{NearCache, NearCacheConfig, NearCacheStats};

/// The type of index to create on a map attribute.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IndexType {
    /// A sorted (ordered) index for range queries.
    ///
    /// Supports equality, comparison, and range predicates efficiently.
    /// Uses a B-tree structure internally.
    Sorted = 0,

    /// A hash index for equality queries.
    ///
    /// Optimized for exact-match lookups. Does not support range queries.
    Hash = 1,
}

impl IndexType {
    fn as_i32(self) -> i32 {
        self as i32
    }
}

/// Configuration for a map index.
///
/// Use [`IndexConfig::builder`] to create a new configuration with the builder pattern.
///
/// # Example
///
/// ```ignore
/// let config = IndexConfig::builder()
///     .name("age-index")
///     .index_type(IndexType::Sorted)
///     .add_attribute("age")
///     .build();
/// ```
#[derive(Debug, Clone)]
pub struct IndexConfig {
    name: Option<String>,
    index_type: IndexType,
    attributes: Vec<String>,
}

impl IndexConfig {
    /// Creates a new builder for `IndexConfig`.
    pub fn builder() -> IndexConfigBuilder {
        IndexConfigBuilder::new()
    }

    /// Returns the name of this index, if set.
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    /// Returns the type of this index.
    pub fn index_type(&self) -> IndexType {
        self.index_type
    }

    /// Returns the attributes this index covers.
    pub fn attributes(&self) -> &[String] {
        &self.attributes
    }
}

/// Builder for creating [`IndexConfig`] instances.
#[derive(Debug, Clone)]
pub struct IndexConfigBuilder {
    name: Option<String>,
    index_type: IndexType,
    attributes: Vec<String>,
}

impl IndexConfigBuilder {
    /// Creates a new builder with default values.
    fn new() -> Self {
        Self {
            name: None,
            index_type: IndexType::Sorted,
            attributes: Vec::new(),
        }
    }

    /// Sets the name of the index.
    ///
    /// Index names must be unique within a map. If not set, Hazelcast
    /// will generate a name based on the indexed attributes.
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Sets the type of the index.
    ///
    /// Defaults to [`IndexType::Sorted`] if not specified.
    pub fn index_type(mut self, index_type: IndexType) -> Self {
        self.index_type = index_type;
        self
    }

    /// Adds an attribute to be indexed.
    ///
    /// At least one attribute must be added before building.
    /// For composite indexes, add multiple attributes in order.
    pub fn add_attribute(mut self, attribute: impl Into<String>) -> Self {
        self.attributes.push(attribute.into());
        self
    }

    /// Adds multiple attributes to be indexed.
    pub fn add_attributes<I, S>(mut self, attributes: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.attributes.extend(attributes.into_iter().map(Into::into));
        self
    }

    /// Builds the [`IndexConfig`].
    ///
    /// # Panics
    ///
    /// Panics if no attributes have been added.
    pub fn build(self) -> IndexConfig {
        assert!(
            !self.attributes.is_empty(),
            "IndexConfig requires at least one attribute"
        );

        IndexConfig {
            name: self.name,
            index_type: self.index_type,
            attributes: self.attributes,
        }
    }

    /// Builds the [`IndexConfig`], returning an error if invalid.
    ///
    /// Returns an error if no attributes have been added.
    pub fn try_build(self) -> Result<IndexConfig> {
        if self.attributes.is_empty() {
            return Err(HazelcastError::Configuration(
                "IndexConfig requires at least one attribute".to_string(),
            ));
        }

        Ok(IndexConfig {
            name: self.name,
            index_type: self.index_type,
            attributes: self.attributes,
        })
    }
}

impl Default for IndexConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}
use crate::connection::ConnectionManager;
use crate::listener::{
    EntryEvent, EntryEventType, EntryListenerConfig, ListenerId, ListenerRegistration,
    ListenerStats,
};
use crate::proxy::entry_processor::{EntryProcessor, EntryProcessorResult};
use crate::query::{Aggregator, Predicate, Projection};

/// A distributed map proxy for performing key-value operations on a Hazelcast cluster.
///
/// `IMap` provides async CRUD operations with automatic serialization and partition routing.
/// Optionally supports client-side near-caching for improved read performance.
#[derive(Debug)]
pub struct IMap<K, V> {
    name: String,
    connection_manager: Arc<ConnectionManager>,
    listener_stats: Arc<ListenerStats>,
    near_cache: Option<Arc<Mutex<NearCache<Vec<u8>, Vec<u8>>>>>,
    _phantom: PhantomData<fn() -> (K, V)>,
}

impl<K, V> IMap<K, V> {
    /// Creates a new map proxy.
    pub(crate) fn new(name: String, connection_manager: Arc<ConnectionManager>) -> Self {
        Self {
            name,
            connection_manager,
            listener_stats: Arc::new(ListenerStats::new()),
            near_cache: None,
            _phantom: PhantomData,
        }
    }

    /// Creates a new map proxy with near-cache enabled.
    pub(crate) fn new_with_near_cache(
        name: String,
        connection_manager: Arc<ConnectionManager>,
        config: NearCacheConfig,
    ) -> Self {
        Self {
            name,
            connection_manager,
            listener_stats: Arc::new(ListenerStats::new()),
            near_cache: Some(Arc::new(Mutex::new(NearCache::new(config)))),
            _phantom: PhantomData,
        }
    }

    /// Returns the name of this map.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns `true` if this map has near-cache enabled.
    pub fn has_near_cache(&self) -> bool {
        self.near_cache.is_some()
    }
}

impl<K, V> IMap<K, V>
where
    K: Serializable + Deserializable + Send + Sync,
    V: Serializable + Deserializable + Send + Sync,
{
    /// Retrieves the value associated with the given key.
    ///
    /// If near-cache is enabled, checks the local cache first. On a cache miss,
    /// fetches from the cluster and populates the near-cache.
    ///
    /// Returns `None` if the key does not exist in the map.
    pub async fn get(&self, key: &K) -> Result<Option<V>> {
        let key_data = Self::serialize_value(key)?;

        // Check near-cache first
        if let Some(ref cache) = self.near_cache {
            let mut cache_guard = cache.lock().unwrap();
            if let Some(value_data) = cache_guard.get(&key_data) {
                let mut input = ObjectDataInput::new(&value_data);
                return V::deserialize(&mut input).map(Some);
            }
        }

        let partition_id = compute_partition_hash(&key_data);

        let mut message = ClientMessage::create_for_encode(MAP_GET, partition_id);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&key_data));

        let response = self.invoke(message).await?;
        let result: Option<V> = Self::decode_nullable_response(&response)?;

        // Populate near-cache on successful remote fetch
        if let Some(ref value) = result {
            if let Some(ref cache) = self.near_cache {
                let value_data = Self::serialize_value(value)?;
                let mut cache_guard = cache.lock().unwrap();
                cache_guard.put(key_data, value_data);
            }
        }

        Ok(result)
    }

    /// Associates the specified value with the specified key.
    ///
    /// If near-cache is enabled, invalidates the local cache entry before
    /// sending the update to the cluster.
    ///
    /// Returns the previous value associated with the key, or `None` if there was no mapping.
    pub async fn put(&self, key: K, value: V) -> Result<Option<V>> {
        let key_data = Self::serialize_value(&key)?;
        let value_data = Self::serialize_value(&value)?;

        // Invalidate near-cache before remote operation
        if let Some(ref cache) = self.near_cache {
            let mut cache_guard = cache.lock().unwrap();
            cache_guard.invalidate(&key_data);
        }

        let partition_id = compute_partition_hash(&key_data);

        let mut message = ClientMessage::create_for_encode(MAP_PUT, partition_id);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&key_data));
        message.add_frame(Self::data_frame(&value_data));
        message.add_frame(Self::long_frame(-1)); // TTL: no expiry
        message.add_frame(Self::long_frame(-1)); // Max idle: no expiry

        let response = self.invoke(message).await?;
        Self::decode_nullable_response(&response)
    }

    /// Removes the mapping for a key from this map if it is present.
    ///
    /// If near-cache is enabled, invalidates the local cache entry before
    /// sending the remove to the cluster.
    ///
    /// Returns the previous value associated with the key, or `None` if there was no mapping.
    pub async fn remove(&self, key: &K) -> Result<Option<V>> {
        let key_data = Self::serialize_value(key)?;

        // Invalidate near-cache before remote operation
        if let Some(ref cache) = self.near_cache {
            let mut cache_guard = cache.lock().unwrap();
            cache_guard.invalidate(&key_data);
        }

        let partition_id = compute_partition_hash(&key_data);

        let mut message = ClientMessage::create_for_encode(MAP_REMOVE, partition_id);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&key_data));

        let response = self.invoke(message).await?;
        Self::decode_nullable_response(&response)
    }

    /// Returns `true` if this map contains a mapping for the specified key.
    pub async fn contains_key(&self, key: &K) -> Result<bool> {
        let key_data = Self::serialize_value(key)?;
        let partition_id = compute_partition_hash(&key_data);

        let mut message = ClientMessage::create_for_encode(MAP_CONTAINS_KEY, partition_id);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&key_data));

        let response = self.invoke(message).await?;
        Self::decode_bool_response(&response)
    }

    /// Returns the number of key-value mappings in this map.
    pub async fn size(&self) -> Result<usize> {
        let message = ClientMessage::create_for_encode(MAP_SIZE, PARTITION_ID_ANY);
        let mut msg = message;
        msg.add_frame(Self::string_frame(&self.name));

        let response = self.invoke(msg).await?;
        Self::decode_int_response(&response).map(|v| v as usize)
    }

    /// Removes all entries from this map.
    ///
    /// If near-cache is enabled, clears the local cache as well.
    pub async fn clear(&self) -> Result<()> {
        // Clear near-cache before remote operation
        if let Some(ref cache) = self.near_cache {
            let mut cache_guard = cache.lock().unwrap();
            cache_guard.clear();
        }

        let message = ClientMessage::create_for_encode(MAP_CLEAR, PARTITION_ID_ANY);
        let mut msg = message;
        msg.add_frame(Self::string_frame(&self.name));

        self.invoke(msg).await?;
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

    async fn invoke(&self, mut message: ClientMessage) -> Result<ClientMessage> {
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

    /// Adds an entry listener to this map.
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
            ClientMessage::create_for_encode(MAP_ADD_ENTRY_LISTENER, PARTITION_ID_ANY);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::bool_frame(config.include_value));
        message.add_frame(Self::int_frame(config.event_flags()));
        message.add_frame(Self::bool_frame(false)); // local only = false

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

    /// Removes an entry listener from this map.
    ///
    /// Returns `true` if the listener was successfully removed, `false` if it was not found.
    pub async fn remove_entry_listener(&self, registration: &ListenerRegistration) -> Result<bool> {
        registration.deactivate();

        let mut message =
            ClientMessage::create_for_encode(MAP_REMOVE_ENTRY_LISTENER, PARTITION_ID_ANY);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::uuid_frame(registration.id().as_uuid()));

        let response = self.invoke(message).await?;
        Self::decode_bool_response(&response)
    }

    /// Returns the listener statistics for this map.
    pub fn listener_stats(&self) -> &ListenerStats {
        &self.listener_stats
    }

    /// Returns the near-cache statistics, if near-cache is enabled.
    ///
    /// Returns `None` if this map does not have near-cache configured.
    pub fn near_cache_stats(&self) -> Option<NearCacheStats> {
        self.near_cache
            .as_ref()
            .map(|cache| cache.lock().unwrap().stats())
    }

    /// Invalidates a specific entry in the near-cache.
    ///
    /// This is useful for manual cache management or when receiving
    /// invalidation events from the cluster.
    pub fn invalidate_near_cache_entry(&self, key: &K) -> Result<()> {
        if let Some(ref cache) = self.near_cache {
            let key_data = Self::serialize_value(key)?;
            let mut cache_guard = cache.lock().unwrap();
            cache_guard.invalidate(&key_data);
        }
        Ok(())
    }

    /// Clears all entries from the near-cache without affecting the remote map.
    pub fn clear_near_cache(&self) {
        if let Some(ref cache) = self.near_cache {
            let mut cache_guard = cache.lock().unwrap();
            cache_guard.clear();
        }
    }

    /// Returns all values matching the given predicate.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use hazelcast_client::query::Predicates;
    ///
    /// let predicate = Predicates::greater_than("age", &18i32)?;
    /// let values = map.values_with_predicate(&predicate).await?;
    /// ```
    pub async fn values_with_predicate(&self, predicate: &dyn Predicate) -> Result<Vec<V>> {
        let predicate_data = predicate.to_predicate_data()?;

        let mut message =
            ClientMessage::create_for_encode(MAP_VALUES_WITH_PREDICATE, PARTITION_ID_ANY);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&predicate_data));

        let response = self.invoke(message).await?;
        Self::decode_values_response(&response)
    }

    /// Returns all keys matching the given predicate.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use hazelcast_client::query::Predicates;
    ///
    /// let predicate = Predicates::like("name", "John%");
    /// let keys = map.keys_with_predicate(&predicate).await?;
    /// ```
    pub async fn keys_with_predicate(&self, predicate: &dyn Predicate) -> Result<Vec<K>> {
        let predicate_data = predicate.to_predicate_data()?;

        let mut message =
            ClientMessage::create_for_encode(MAP_KEYS_WITH_PREDICATE, PARTITION_ID_ANY);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&predicate_data));

        let response = self.invoke(message).await?;
        Self::decode_keys_response(&response)
    }

    /// Returns all entries matching the given predicate.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use hazelcast_client::query::Predicates;
    ///
    /// let predicate = Predicates::between("score", &0i32, &100i32)?;
    /// let entries = map.entries_with_predicate(&predicate).await?;
    /// for (key, value) in entries {
    ///     println!("{}: {:?}", key, value);
    /// }
    /// ```
    pub async fn entries_with_predicate(&self, predicate: &dyn Predicate) -> Result<Vec<(K, V)>> {
        let predicate_data = predicate.to_predicate_data()?;

        let mut message =
            ClientMessage::create_for_encode(MAP_ENTRIES_WITH_PREDICATE, PARTITION_ID_ANY);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&predicate_data));

        let response = self.invoke(message).await?;
        Self::decode_entries_response(&response)
    }

    fn decode_values_response<T: Deserializable>(response: &ClientMessage) -> Result<Vec<T>> {
        let frames = response.frames();
        let mut values = Vec::new();

        // Skip initial frame, iterate over data frames
        for frame in frames.iter().skip(1) {
            if frame.flags & IS_NULL_FLAG != 0 {
                continue;
            }
            if frame.flags & END_FLAG != 0 && frame.content.is_empty() {
                break;
            }
            if frame.content.is_empty() {
                continue;
            }

            let mut input = ObjectDataInput::new(&frame.content);
            if let Ok(value) = T::deserialize(&mut input) {
                values.push(value);
            }
        }

        Ok(values)
    }

    fn decode_keys_response<T: Deserializable>(response: &ClientMessage) -> Result<Vec<T>> {
        Self::decode_values_response(response)
    }

    fn decode_entries_response(response: &ClientMessage) -> Result<Vec<(K, V)>> {
        let frames = response.frames();
        let mut entries = Vec::new();

        // Skip initial frame, pairs of frames are key/value
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
                entries.push((key, value));
            }

            i += 2;
        }

        Ok(entries)
    }

    /// Executes an entry processor on the entry with the given key.
    ///
    /// The processor is serialized and sent to the cluster member owning the key,
    /// where it executes atomically on the entry. This avoids transferring the
    /// entry data to the client for modification.
    ///
    /// # Type Parameters
    ///
    /// - `E`: The entry processor type implementing [`EntryProcessor`]
    ///
    /// # Returns
    ///
    /// The result of executing the processor on the entry, or `None` if the key
    /// does not exist and the processor returns no result.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let result = map.execute_on_key(&"user:123".to_string(), &increment_age_processor).await?;
    /// println!("New age: {:?}", result);
    /// ```
    pub async fn execute_on_key<E>(&self, key: &K, processor: &E) -> Result<Option<E::Output>>
    where
        E: EntryProcessor,
    {
        let key_data = Self::serialize_value(key)?;
        let processor_data = Self::serialize_value(processor)?;
        let partition_id = compute_partition_hash(&key_data);

        let mut message = ClientMessage::create_for_encode(MAP_EXECUTE_ON_KEY, partition_id);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&processor_data));
        message.add_frame(Self::data_frame(&key_data));

        let response = self.invoke(message).await?;
        Self::decode_nullable_response::<E::Output>(&response)
    }

    /// Executes an entry processor on entries with the given keys.
    ///
    /// The processor is sent to each cluster member owning one or more of the keys.
    /// Each member executes the processor on its local entries atomically.
    ///
    /// # Type Parameters
    ///
    /// - `E`: The entry processor type implementing [`EntryProcessor`]
    ///
    /// # Returns
    ///
    /// A map of keys to their processing results. Keys that don't exist in the map
    /// may be omitted from the result.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let keys = vec!["user:1".to_string(), "user:2".to_string(), "user:3".to_string()];
    /// let results = map.execute_on_keys(&keys, &increment_processor).await?;
    /// for (key, result) in results {
    ///     println!("{}: {:?}", key, result);
    /// }
    /// ```
    pub async fn execute_on_keys<E>(
        &self,
        keys: &[K],
        processor: &E,
    ) -> Result<EntryProcessorResult<K, E::Output>>
    where
        E: EntryProcessor,
        K: Eq + Hash + Clone,
    {
        if keys.is_empty() {
            return Ok(EntryProcessorResult::default());
        }

        let processor_data = Self::serialize_value(processor)?;

        let mut message = ClientMessage::create_for_encode(MAP_EXECUTE_ON_KEYS, PARTITION_ID_ANY);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&processor_data));

        // Encode keys count
        message.add_frame(Self::int_frame(keys.len() as i32));

        // Encode each key
        for key in keys {
            let key_data = Self::serialize_value(key)?;
            message.add_frame(Self::data_frame(&key_data));
        }

        let response = self.invoke(message).await?;
        self.decode_entry_processor_results::<E::Output>(&response)
    }

    /// Executes an entry processor on all entries in this map.
    ///
    /// The processor is sent to all cluster members, where it executes on each
    /// entry atomically. This is useful for bulk updates or aggregations.
    ///
    /// # Type Parameters
    ///
    /// - `E`: The entry processor type implementing [`EntryProcessor`]
    ///
    /// # Returns
    ///
    /// A map of all keys to their processing results.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let results = map.execute_on_entries(&reset_counter_processor).await?;
    /// println!("Processed {} entries", results.len());
    /// ```
    pub async fn execute_on_entries<E>(
        &self,
        processor: &E,
    ) -> Result<EntryProcessorResult<K, E::Output>>
    where
        E: EntryProcessor,
        K: Eq + Hash + Clone,
    {
        let processor_data = Self::serialize_value(processor)?;

        let mut message =
            ClientMessage::create_for_encode(MAP_EXECUTE_ON_ALL_KEYS, PARTITION_ID_ANY);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&processor_data));

        let response = self.invoke(message).await?;
        self.decode_entry_processor_results::<E::Output>(&response)
    }

    /// Adds an index to this map with the given configuration.
    ///
    /// Indexes improve query performance for predicates that filter on the
    /// indexed attributes. Creating an index on a populated map will trigger
    /// a full scan to build the index.
    ///
    /// # Index Types
    ///
    /// - [`IndexType::Sorted`]: Best for range queries (`<`, `>`, `BETWEEN`).
    ///   Also supports equality checks.
    /// - [`IndexType::Hash`]: Best for equality queries (`=`). Does not support
    ///   range queries but has faster lookups for exact matches.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create a sorted index on the "age" attribute
    /// let config = IndexConfig::builder()
    ///     .name("age-idx")
    ///     .index_type(IndexType::Sorted)
    ///     .add_attribute("age")
    ///     .build();
    ///
    /// map.add_index(config).await?;
    ///
    /// // Create a hash index for exact-match lookups on "email"
    /// let email_index = IndexConfig::builder()
    ///     .index_type(IndexType::Hash)
    ///     .add_attribute("email")
    ///     .build();
    ///
    /// map.add_index(email_index).await?;
    ///
    /// // Create a composite index on multiple attributes
    /// let composite = IndexConfig::builder()
    ///     .index_type(IndexType::Sorted)
    ///     .add_attributes(["lastName", "firstName"])
    ///     .build();
    ///
    /// map.add_index(composite).await?;
    /// ```
    pub async fn add_index(&self, config: IndexConfig) -> Result<()> {
        let mut message = ClientMessage::create_for_encode(MAP_ADD_INDEX, PARTITION_ID_ANY);
        message.add_frame(Self::string_frame(&self.name));

        // Encode index config as nested structure
        // Index type
        message.add_frame(Self::int_frame(config.index_type.as_i32()));

        // Index name (nullable)
        if let Some(ref name) = config.name {
            message.add_frame(Self::string_frame(name));
        } else {
            message.add_frame(Frame::null());
        }

        // Attributes count
        message.add_frame(Self::int_frame(config.attributes.len() as i32));

        // Encode each attribute
        for attr in &config.attributes {
            message.add_frame(Self::string_frame(attr));
        }

        self.invoke(message).await?;
        Ok(())
    }

    /// Aggregates map entries using the given aggregator.
    ///
    /// The aggregation is performed on the cluster and only the final result
    /// is returned to the client.
    ///
    /// # Type Parameters
    ///
    /// - `A`: The aggregator type implementing [`Aggregator`]
    ///
    /// # Returns
    ///
    /// The aggregated result computed over all map entries.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use hazelcast_client::query::Aggregators;
    ///
    /// // Count all entries
    /// let count: i64 = map.aggregate(&Aggregators::count()).await?;
    ///
    /// // Sum all salaries
    /// let total: i64 = map.aggregate(&Aggregators::long_sum("salary")).await?;
    /// ```
    pub async fn aggregate<A>(&self, aggregator: &A) -> Result<A::Output>
    where
        A: Aggregator,
        A::Output: Deserializable,
    {
        let aggregator_data = aggregator.to_aggregator_data()?;

        let mut message = ClientMessage::create_for_encode(MAP_AGGREGATE, PARTITION_ID_ANY);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&aggregator_data));

        let response = self.invoke(message).await?;
        Self::decode_aggregate_response::<A::Output>(&response)
    }

    /// Aggregates map entries matching a predicate using the given aggregator.
    ///
    /// The predicate filters which entries are included in the aggregation.
    /// Both the predicate evaluation and aggregation are performed on the cluster.
    ///
    /// # Type Parameters
    ///
    /// - `A`: The aggregator type implementing [`Aggregator`]
    ///
    /// # Returns
    ///
    /// The aggregated result computed over matching map entries.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use hazelcast_client::query::{Aggregators, Predicates};
    ///
    /// // Count active users
    /// let predicate = Predicates::equal("status", &"active".to_string())?;
    /// let count: i64 = map.aggregate_with_predicate(&Aggregators::count(), &predicate).await?;
    ///
    /// // Average salary for employees over 30
    /// let age_predicate = Predicates::greater_than("age", &30i32)?;
    /// let avg: f64 = map.aggregate_with_predicate(&Aggregators::double_avg("salary"), &age_predicate).await?;
    /// ```
    pub async fn aggregate_with_predicate<A>(
        &self,
        aggregator: &A,
        predicate: &dyn Predicate,
    ) -> Result<A::Output>
    where
        A: Aggregator,
        A::Output: Deserializable,
    {
        let aggregator_data = aggregator.to_aggregator_data()?;
        let predicate_data = predicate.to_predicate_data()?;

        let mut message =
            ClientMessage::create_for_encode(MAP_AGGREGATE_WITH_PREDICATE, PARTITION_ID_ANY);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&aggregator_data));
        message.add_frame(Self::data_frame(&predicate_data));

        let response = self.invoke(message).await?;
        Self::decode_aggregate_response::<A::Output>(&response)
    }

    /// Projects specific attributes from all map entries.
    ///
    /// The projection is performed on the cluster and only the projected
    /// attributes are returned to the client, reducing data transfer.
    ///
    /// # Type Parameters
    ///
    /// - `P`: The projection type implementing [`Projection`]
    ///
    /// # Returns
    ///
    /// A vector of projected values for all map entries.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use hazelcast_client::query::Projections;
    ///
    /// // Project a single attribute from all entries
    /// let names: Vec<String> = map.project(&Projections::single("name")).await?;
    ///
    /// // Project multiple attributes
    /// let projection = Projections::multi::<String>(["firstName", "lastName"]);
    /// let results: Vec<Vec<Option<String>>> = map.project(&projection).await?;
    /// ```
    pub async fn project<P>(&self, projection: &P) -> Result<Vec<P::Output>>
    where
        P: Projection,
        P::Output: Deserializable,
    {
        let projection_data = projection.to_projection_data()?;

        let mut message = ClientMessage::create_for_encode(MAP_PROJECT, PARTITION_ID_ANY);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&projection_data));

        let response = self.invoke(message).await?;
        Self::decode_projection_response::<P::Output>(&response)
    }

    /// Projects specific attributes from map entries matching a predicate.
    ///
    /// The predicate filters which entries are included in the projection.
    /// Both the predicate evaluation and projection are performed on the cluster.
    ///
    /// # Type Parameters
    ///
    /// - `P`: The projection type implementing [`Projection`]
    ///
    /// # Returns
    ///
    /// A vector of projected values for matching map entries.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use hazelcast_client::query::{Projections, Predicates};
    ///
    /// // Project names of active users
    /// let predicate = Predicates::equal("status", &"active".to_string())?;
    /// let names: Vec<String> = map
    ///     .project_with_predicate(&Projections::single("name"), &predicate)
    ///     .await?;
    ///
    /// // Project contact info for users over 18
    /// let age_predicate = Predicates::greater_than("age", &18i32)?;
    /// let projection = Projections::multi::<String>(["email", "phone"]);
    /// let contacts: Vec<Vec<Option<String>>> = map
    ///     .project_with_predicate(&projection, &age_predicate)
    ///     .await?;
    /// ```
    pub async fn project_with_predicate<P>(
        &self,
        projection: &P,
        predicate: &dyn Predicate,
    ) -> Result<Vec<P::Output>>
    where
        P: Projection,
        P::Output: Deserializable,
    {
        let projection_data = projection.to_projection_data()?;
        let predicate_data = predicate.to_predicate_data()?;

        let mut message =
            ClientMessage::create_for_encode(MAP_PROJECT_WITH_PREDICATE, PARTITION_ID_ANY);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&projection_data));
        message.add_frame(Self::data_frame(&predicate_data));

        let response = self.invoke(message).await?;
        Self::decode_projection_response::<P::Output>(&response)
    }

    fn decode_projection_response<T: Deserializable>(response: &ClientMessage) -> Result<Vec<T>> {
        let frames = response.frames();
        let mut results = Vec::new();

        for frame in frames.iter().skip(1) {
            if frame.flags & IS_NULL_FLAG != 0 {
                continue;
            }
            if frame.flags & END_FLAG != 0 && frame.content.is_empty() {
                break;
            }
            if frame.content.is_empty() {
                continue;
            }

            let mut input = ObjectDataInput::new(&frame.content);
            if let Ok(value) = T::deserialize(&mut input) {
                results.push(value);
            }
        }

        Ok(results)
    }

    fn decode_aggregate_response<T: Deserializable>(response: &ClientMessage) -> Result<T> {
        let frames = response.frames();
        if frames.len() < 2 {
            return Err(HazelcastError::Serialization(
                "empty aggregate response".to_string(),
            ));
        }

        let data_frame = &frames[1];
        if data_frame.content.is_empty() {
            return Err(HazelcastError::Serialization(
                "empty aggregate result".to_string(),
            ));
        }

        let mut input = ObjectDataInput::new(&data_frame.content);
        T::deserialize(&mut input)
    }

    fn decode_entry_processor_results<R>(
        &self,
        response: &ClientMessage,
    ) -> Result<EntryProcessorResult<K, R>>
    where
        R: Deserializable,
        K: Eq + Hash + Clone,
    {
        let frames = response.frames();
        let mut results = HashMap::new();

        // Skip initial frame, pairs of frames are key/result
        let data_frames: Vec<_> = frames
            .iter()
            .skip(1)
            .filter(|f| !f.content.is_empty())
            .collect();

        let mut i = 0;
        while i + 1 < data_frames.len() {
            let key_frame = data_frames[i];
            let result_frame = data_frames[i + 1];

            if key_frame.flags & END_FLAG != 0 && key_frame.content.is_empty() {
                break;
            }

            if key_frame.flags & IS_NULL_FLAG != 0 {
                i += 2;
                continue;
            }

            let mut key_input = ObjectDataInput::new(&key_frame.content);
            if let Ok(key) = K::deserialize(&mut key_input) {
                if result_frame.flags & IS_NULL_FLAG == 0 && !result_frame.content.is_empty() {
                    let mut result_input = ObjectDataInput::new(&result_frame.content);
                    if let Ok(result) = R::deserialize(&mut result_input) {
                        results.insert(key, result);
                    }
                }
            }

            i += 2;
        }

        Ok(EntryProcessorResult::new(results))
    }
}

impl<K, V> Clone for IMap<K, V> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            connection_manager: Arc::clone(&self.connection_manager),
            listener_stats: Arc::clone(&self.listener_stats),
            near_cache: self.near_cache.as_ref().map(Arc::clone),
            _phantom: PhantomData,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::EvictionPolicy;
    use std::time::Duration;

    #[test]
    fn test_imap_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<IMap<String, String>>();
    }

    #[test]
    fn test_imap_has_near_cache() {
        use crate::connection::ConnectionManager;
        use std::net::SocketAddr;

        let addr: SocketAddr = "127.0.0.1:5701".parse().unwrap();
        let cm = ConnectionManager::new(vec![addr]);

        let map_without: IMap<String, String> = IMap::new("test".to_string(), Arc::new(cm.clone()));
        assert!(!map_without.has_near_cache());
        assert!(map_without.near_cache_stats().is_none());

        let config = NearCacheConfig::builder("test").build().unwrap();
        let map_with: IMap<String, String> =
            IMap::new_with_near_cache("test".to_string(), Arc::new(cm), config);
        assert!(map_with.has_near_cache());
        assert!(map_with.near_cache_stats().is_some());
    }

    #[test]
    fn test_near_cache_stats_initial() {
        use crate::connection::ConnectionManager;
        use std::net::SocketAddr;

        let addr: SocketAddr = "127.0.0.1:5701".parse().unwrap();
        let cm = Arc::new(ConnectionManager::new(vec![addr]));
        let config = NearCacheConfig::builder("test").build().unwrap();
        let map: IMap<String, String> =
            IMap::new_with_near_cache("test".to_string(), cm, config);

        let stats = map.near_cache_stats().unwrap();
        assert_eq!(stats.hits(), 0);
        assert_eq!(stats.misses(), 0);
        assert_eq!(stats.evictions(), 0);
        assert_eq!(stats.expirations(), 0);
    }

    #[test]
    fn test_near_cache_clone_shares_cache() {
        use crate::connection::ConnectionManager;
        use std::net::SocketAddr;

        let addr: SocketAddr = "127.0.0.1:5701".parse().unwrap();
        let cm = Arc::new(ConnectionManager::new(vec![addr]));
        let config = NearCacheConfig::builder("test").build().unwrap();
        let map1: IMap<String, String> =
            IMap::new_with_near_cache("test".to_string(), cm, config);
        let map2 = map1.clone();

        // Both should share the same near-cache
        assert!(map1.has_near_cache());
        assert!(map2.has_near_cache());

        // Clearing one should affect the other (shared Arc)
        map1.clear_near_cache();
        let stats1 = map1.near_cache_stats().unwrap();
        let stats2 = map2.near_cache_stats().unwrap();
        assert_eq!(stats1.hits(), stats2.hits());
    }

    #[test]
    fn test_invalidate_near_cache_entry() {
        use crate::connection::ConnectionManager;
        use std::net::SocketAddr;

        let addr: SocketAddr = "127.0.0.1:5701".parse().unwrap();
        let cm = Arc::new(ConnectionManager::new(vec![addr]));
        let config = NearCacheConfig::builder("test").build().unwrap();
        let map: IMap<String, String> =
            IMap::new_with_near_cache("test".to_string(), cm, config);

        // Should not error even if key doesn't exist
        assert!(map.invalidate_near_cache_entry(&"key1".to_string()).is_ok());
    }

    #[test]
    fn test_clear_near_cache() {
        use crate::connection::ConnectionManager;
        use std::net::SocketAddr;

        let addr: SocketAddr = "127.0.0.1:5701".parse().unwrap();
        let cm = Arc::new(ConnectionManager::new(vec![addr]));
        let config = NearCacheConfig::builder("test").build().unwrap();
        let map: IMap<String, String> =
            IMap::new_with_near_cache("test".to_string(), cm, config);

        // Should not panic
        map.clear_near_cache();
    }

    #[test]
    fn test_near_cache_config_builder_integration() {
        let config = NearCacheConfig::builder("user-*")
            .max_size(1000)
            .time_to_live(Duration::from_secs(60))
            .eviction_policy(EvictionPolicy::Lfu)
            .build()
            .unwrap();

        assert_eq!(config.name(), "user-*");
        assert_eq!(config.max_size(), 1000);
        assert!(config.matches("user-map"));
        assert!(!config.matches("other-map"));
    }

    #[test]
    fn test_imap_clone() {
        fn assert_clone<T: Clone>() {}
        assert_clone::<IMap<String, String>>();
    }

    #[test]
    fn test_string_frame() {
        let frame = IMap::<String, String>::string_frame("test-map");
        assert_eq!(&frame.content[..], b"test-map");
    }

    #[test]
    fn test_long_frame() {
        let frame = IMap::<String, String>::long_frame(-1);
        assert_eq!(frame.content.len(), 8);
        assert_eq!(
            i64::from_le_bytes(frame.content[..8].try_into().unwrap()),
            -1
        );
    }

    #[test]
    fn test_serialize_string() {
        let data = IMap::<String, String>::serialize_value(&"hello".to_string()).unwrap();
        assert!(!data.is_empty());
    }

    #[test]
    fn test_bool_frame() {
        let frame_true = IMap::<String, String>::bool_frame(true);
        assert_eq!(frame_true.content[0], 1);

        let frame_false = IMap::<String, String>::bool_frame(false);
        assert_eq!(frame_false.content[0], 0);
    }

    #[test]
    fn test_int_frame() {
        let frame = IMap::<String, String>::int_frame(42);
        assert_eq!(frame.content.len(), 4);
        assert_eq!(
            i32::from_le_bytes(frame.content[..4].try_into().unwrap()),
            42
        );
    }

    #[test]
    fn test_uuid_frame() {
        let uuid = Uuid::new_v4();
        let frame = IMap::<String, String>::uuid_frame(uuid);
        assert_eq!(frame.content.len(), 16);
        assert_eq!(
            Uuid::from_bytes(frame.content[..16].try_into().unwrap()),
            uuid
        );
    }

    #[test]
    fn test_decode_empty_values_response() {
        let mut message = ClientMessage::create_for_encode(0, -1);
        let values: Vec<String> = IMap::<String, String>::decode_values_response(&message).unwrap();
        assert!(values.is_empty());
    }

    #[test]
    fn test_decode_empty_entries_response() {
        let message = ClientMessage::create_for_encode(0, -1);
        let entries: Vec<(String, String)> =
            IMap::<String, String>::decode_entries_response(&message).unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn test_execute_on_key_serializes_processor() {
        use crate::proxy::EntryProcessor;

        struct TestProcessor {
            increment: i32,
        }

        impl EntryProcessor for TestProcessor {
            type Output = i32;
        }

        impl Serializable for TestProcessor {
            fn serialize(&self, output: &mut ObjectDataOutput) -> hazelcast_core::Result<()> {
                output.write_i32(self.increment)?;
                Ok(())
            }
        }

        let processor = TestProcessor { increment: 5 };
        let mut output = ObjectDataOutput::new();
        processor.serialize(&mut output).unwrap();
        let bytes = output.into_bytes();
        assert!(!bytes.is_empty());
        assert_eq!(bytes.len(), 4);
    }

    #[test]
    fn test_entry_processor_result_integration() {
        use crate::proxy::EntryProcessorResult;

        let mut map = std::collections::HashMap::new();
        map.insert("key1".to_string(), 100i64);
        map.insert("key2".to_string(), 200i64);

        let result: EntryProcessorResult<String, i64> = EntryProcessorResult::new(map);
        assert_eq!(result.len(), 2);
        assert_eq!(result.get(&"key1".to_string()), Some(&100i64));
        assert_eq!(result.get(&"key2".to_string()), Some(&200i64));
    }

    #[test]
    fn test_index_type_values() {
        assert_eq!(IndexType::Sorted.as_i32(), 0);
        assert_eq!(IndexType::Hash.as_i32(), 1);
    }

    #[test]
    fn test_index_config_builder() {
        let config = IndexConfig::builder()
            .name("test-index")
            .index_type(IndexType::Hash)
            .add_attribute("field1")
            .build();

        assert_eq!(config.name(), Some("test-index"));
        assert_eq!(config.index_type(), IndexType::Hash);
        assert_eq!(config.attributes(), &["field1".to_string()]);
    }

    #[test]
    fn test_index_config_builder_multiple_attributes() {
        let config = IndexConfig::builder()
            .index_type(IndexType::Sorted)
            .add_attribute("lastName")
            .add_attribute("firstName")
            .build();

        assert_eq!(config.name(), None);
        assert_eq!(config.index_type(), IndexType::Sorted);
        assert_eq!(
            config.attributes(),
            &["lastName".to_string(), "firstName".to_string()]
        );
    }

    #[test]
    fn test_index_config_builder_add_attributes() {
        let config = IndexConfig::builder()
            .add_attributes(["a", "b", "c"])
            .build();

        assert_eq!(config.attributes().len(), 3);
        assert_eq!(
            config.attributes(),
            &["a".to_string(), "b".to_string(), "c".to_string()]
        );
    }

    #[test]
    fn test_index_config_builder_default_type() {
        let config = IndexConfig::builder().add_attribute("field").build();

        assert_eq!(config.index_type(), IndexType::Sorted);
    }

    #[test]
    #[should_panic(expected = "requires at least one attribute")]
    fn test_index_config_builder_no_attributes_panics() {
        IndexConfig::builder().build();
    }

    #[test]
    fn test_index_config_try_build_no_attributes() {
        let result = IndexConfig::builder().try_build();
        assert!(result.is_err());
    }

    #[test]
    fn test_index_config_try_build_success() {
        let result = IndexConfig::builder()
            .add_attribute("field")
            .try_build();
        assert!(result.is_ok());
    }

    #[test]
    fn test_index_config_clone() {
        let config = IndexConfig::builder()
            .name("idx")
            .add_attribute("field")
            .build();

        let cloned = config.clone();
        assert_eq!(config.name(), cloned.name());
        assert_eq!(config.index_type(), cloned.index_type());
        assert_eq!(config.attributes(), cloned.attributes());
    }

    #[test]
    fn test_index_type_equality() {
        assert_eq!(IndexType::Sorted, IndexType::Sorted);
        assert_eq!(IndexType::Hash, IndexType::Hash);
        assert_ne!(IndexType::Sorted, IndexType::Hash);
    }

    #[test]
    fn test_index_type_copy() {
        let t1 = IndexType::Hash;
        let t2 = t1;
        assert_eq!(t1, t2);
    }

    #[test]
    fn test_index_config_builder_chaining() {
        let config = IndexConfig::builder()
            .name("my-index")
            .index_type(IndexType::Hash)
            .add_attribute("attr1")
            .add_attributes(vec!["attr2", "attr3"])
            .build();

        assert_eq!(config.name(), Some("my-index"));
        assert_eq!(config.attributes().len(), 3);
    }

    #[test]
    fn test_aggregator_serialization() {
        use crate::query::Aggregators;

        let count = Aggregators::count();
        let data = count.to_aggregator_data().unwrap();
        assert!(!data.is_empty());

        let sum = Aggregators::long_sum("value");
        let data = sum.to_aggregator_data().unwrap();
        assert!(!data.is_empty());
    }

    #[test]
    fn test_aggregator_with_predicate_types() {
        use crate::query::{Aggregators, Predicates};

        let _count = Aggregators::count();
        let _sum = Aggregators::integer_sum("value");
        let _avg = Aggregators::double_avg("price");
        let _min: crate::query::MinAggregator<i64> = Aggregators::min("score");
        let _max: crate::query::MaxAggregator<i64> = Aggregators::max("score");

        let _pred = Predicates::greater_than("age", &18i32).unwrap();
    }

    #[test]
    fn test_entry_processor_with_complex_output() {
        use crate::proxy::EntryProcessor;

        #[derive(Debug, PartialEq)]
        struct ProcessorResult {
            old_value: i32,
            new_value: i32,
        }

        impl Deserializable for ProcessorResult {
            fn deserialize(input: &mut ObjectDataInput) -> hazelcast_core::Result<Self> {
                Ok(Self {
                    old_value: input.read_i32()?,
                    new_value: input.read_i32()?,
                })
            }
        }

        struct UpdateProcessor {
            delta: i32,
        }

        impl EntryProcessor for UpdateProcessor {
            type Output = ProcessorResult;
        }

        impl Serializable for UpdateProcessor {
            fn serialize(&self, output: &mut ObjectDataOutput) -> hazelcast_core::Result<()> {
                output.write_i32(self.delta)?;
                Ok(())
            }
        }

        let processor = UpdateProcessor { delta: 10 };
        let mut output = ObjectDataOutput::new();
        processor.serialize(&mut output).unwrap();
        assert_eq!(output.into_bytes().len(), 4);
    }
}
