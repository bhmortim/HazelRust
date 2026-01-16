//! Distributed ReplicatedMap implementation.

use std::collections::HashMap;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::BytesMut;
use tokio::spawn;
use uuid::Uuid;
use hazelcast_core::protocol::constants::{
    IS_EVENT_FLAG, IS_NULL_FLAG, PARTITION_ID_ANY, REPLICATED_MAP_ADD_ENTRY_LISTENER,
    REPLICATED_MAP_ADD_ENTRY_LISTENER_WITH_PREDICATE, REPLICATED_MAP_CLEAR,
    REPLICATED_MAP_CONTAINS_KEY, REPLICATED_MAP_CONTAINS_VALUE, REPLICATED_MAP_ENTRY_SET,
    REPLICATED_MAP_GET, REPLICATED_MAP_IS_EMPTY, REPLICATED_MAP_KEY_SET, REPLICATED_MAP_PUT,
    REPLICATED_MAP_PUT_ALL, REPLICATED_MAP_PUT_WITH_TTL, REPLICATED_MAP_REMOVE,
    REPLICATED_MAP_REMOVE_ENTRY_LISTENER, REPLICATED_MAP_SIZE, REPLICATED_MAP_VALUES,
    RESPONSE_HEADER_SIZE,
};
use hazelcast_core::protocol::Frame;
use hazelcast_core::serialization::{ObjectDataInput, ObjectDataOutput};
use hazelcast_core::{ClientMessage, Deserializable, HazelcastError, Result, Serializable};

use crate::connection::ConnectionManager;
use crate::listener::{
    dispatch_entry_event, BoxedEntryListener, EntryEvent, EntryEventType, EntryListener,
    EntryListenerConfig, ListenerId, ListenerRegistration, ListenerStats,
};

static REPLICATED_MAP_INVOCATION_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Statistics for a replicated map instance.
#[derive(Debug, Clone)]
pub struct ReplicatedMapStats {
    put_operation_count: Arc<AtomicI64>,
    get_operation_count: Arc<AtomicI64>,
    remove_operation_count: Arc<AtomicI64>,
    hits: Arc<AtomicI64>,
    misses: Arc<AtomicI64>,
    creation_time: i64,
}

impl ReplicatedMapStats {
    fn new() -> Self {
        Self {
            put_operation_count: Arc::new(AtomicI64::new(0)),
            get_operation_count: Arc::new(AtomicI64::new(0)),
            remove_operation_count: Arc::new(AtomicI64::new(0)),
            hits: Arc::new(AtomicI64::new(0)),
            misses: Arc::new(AtomicI64::new(0)),
            creation_time: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0),
        }
    }

    /// Returns the number of put operations.
    pub fn put_operation_count(&self) -> i64 {
        self.put_operation_count.load(Ordering::Relaxed)
    }

    /// Returns the number of get operations.
    pub fn get_operation_count(&self) -> i64 {
        self.get_operation_count.load(Ordering::Relaxed)
    }

    /// Returns the number of remove operations.
    pub fn remove_operation_count(&self) -> i64 {
        self.remove_operation_count.load(Ordering::Relaxed)
    }

    /// Returns the number of cache hits.
    pub fn hits(&self) -> i64 {
        self.hits.load(Ordering::Relaxed)
    }

    /// Returns the number of cache misses.
    pub fn misses(&self) -> i64 {
        self.misses.load(Ordering::Relaxed)
    }

    /// Returns the creation time in milliseconds since epoch.
    pub fn creation_time(&self) -> i64 {
        self.creation_time
    }

    fn record_put(&self) {
        self.put_operation_count.fetch_add(1, Ordering::Relaxed);
    }

    fn record_get(&self, hit: bool) {
        self.get_operation_count.fetch_add(1, Ordering::Relaxed);
        if hit {
            self.hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn record_remove(&self) {
        self.remove_operation_count.fetch_add(1, Ordering::Relaxed);
    }
}

impl Default for ReplicatedMapStats {
    fn default() -> Self {
        Self::new()
    }
}

/// A distributed, eventually-consistent map with full replication to all members.
///
/// Unlike `IMap`, data is replicated to all cluster members, providing
/// faster reads at the cost of higher memory usage and eventual consistency.
///
/// # Type Parameters
///
/// - `K`: The key type, must implement `Serializable`, `Deserializable`, `Send`, and `Sync`
/// - `V`: The value type, must implement `Serializable`, `Deserializable`, `Send`, and `Sync`
///
/// # Example
///
/// ```ignore
/// let replicated_map = client.get_replicated_map::<String, String>("my-replicated-map");
/// replicated_map.put("key".to_string(), "value".to_string()).await?;
/// let value = replicated_map.get(&"key".to_string()).await?;
/// ```
#[derive(Debug)]
pub struct ReplicatedMap<K, V> {
    name: String,
    connection_manager: Arc<ConnectionManager>,
    listener_stats: Arc<ListenerStats>,
    stats: Arc<ReplicatedMapStats>,
    _marker: PhantomData<(K, V)>,
}

impl<K, V> ReplicatedMap<K, V>
where
    K: Serializable + Deserializable + Send + Sync,
    V: Serializable + Deserializable + Send + Sync,
{
    /// Creates a new ReplicatedMap proxy.
    pub(crate) fn new(name: String, connection_manager: Arc<ConnectionManager>) -> Self {
        Self {
            name,
            connection_manager,
            listener_stats: Arc::new(ListenerStats::new()),
            stats: Arc::new(ReplicatedMapStats::new()),
            _marker: PhantomData,
        }
    }

    /// Returns the name of this replicated map.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Associates the specified value with the specified key in this map.
    ///
    /// Returns the previous value associated with the key, or `None` if there was no mapping.
    pub async fn put(&self, key: K, value: V) -> Result<Option<V>> {
        self.stats.record_put();
        let key_data = Self::serialize_value(&key)?;
        let value_data = Self::serialize_value(&value)?;

        let mut message = ClientMessage::create_for_encode(REPLICATED_MAP_PUT, PARTITION_ID_ANY);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&key_data));
        message.add_frame(Self::data_frame(&value_data));
        message.add_frame(Self::long_frame(-1)); // TTL: no expiry

        let response = self.invoke_on_random(message).await?;
        Self::decode_nullable_response(&response)
    }

    /// Associates the specified value with the specified key in this map with a TTL.
    ///
    /// The entry will automatically expire after the specified duration.
    /// Returns the previous value associated with the key, or `None` if there was no mapping.
    pub async fn put_with_ttl(&self, key: K, value: V, ttl: Duration) -> Result<Option<V>> {
        self.stats.record_put();
        let key_data = Self::serialize_value(&key)?;
        let value_data = Self::serialize_value(&value)?;
        let ttl_millis = ttl.as_millis() as i64;

        let mut message = ClientMessage::create_for_encode(REPLICATED_MAP_PUT_WITH_TTL, PARTITION_ID_ANY);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&key_data));
        message.add_frame(Self::data_frame(&value_data));
        message.add_frame(Self::long_frame(ttl_millis));

        let response = self.invoke_on_random(message).await?;
        Self::decode_nullable_response(&response)
    }

    /// Copies all of the mappings from the specified map to this map.
    ///
    /// The effect of this call is equivalent to calling `put(k, v)` on this map
    /// for each mapping from key `k` to value `v` in the specified map.
    pub async fn put_all(&self, entries: HashMap<K, V>) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        let mut serialized_entries = Vec::with_capacity(entries.len());
        for (key, value) in entries {
            self.stats.record_put();
            let key_data = Self::serialize_value(&key)?;
            let value_data = Self::serialize_value(&value)?;
            serialized_entries.push((key_data, value_data));
        }

        let mut message = ClientMessage::create_for_encode(REPLICATED_MAP_PUT_ALL, PARTITION_ID_ANY);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::int_frame(serialized_entries.len() as i32));

        for (key_data, value_data) in serialized_entries {
            message.add_frame(Self::data_frame(&key_data));
            message.add_frame(Self::data_frame(&value_data));
        }

        self.invoke_on_random(message).await?;
        Ok(())
    }

    /// Returns the value associated with the specified key, or `None` if no mapping exists.
    pub async fn get(&self, key: &K) -> Result<Option<V>> {
        let key_data = Self::serialize_value(key)?;

        let mut message = ClientMessage::create_for_encode(REPLICATED_MAP_GET, PARTITION_ID_ANY);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&key_data));

        let response = self.invoke_on_random(message).await?;
        let result: Option<V> = Self::decode_nullable_response(&response)?;
        self.stats.record_get(result.is_some());
        Ok(result)
    }

    /// Removes the mapping for a key from this map if present.
    ///
    /// Returns the previous value associated with the key, or `None` if there was no mapping.
    pub async fn remove(&self, key: &K) -> Result<Option<V>> {
        self.stats.record_remove();
        let key_data = Self::serialize_value(key)?;

        let mut message = ClientMessage::create_for_encode(REPLICATED_MAP_REMOVE, PARTITION_ID_ANY);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&key_data));

        let response = self.invoke_on_random(message).await?;
        Self::decode_nullable_response(&response)
    }

    /// Returns `true` if this map contains a mapping for the specified key.
    pub async fn contains_key(&self, key: &K) -> Result<bool> {
        let key_data = Self::serialize_value(key)?;

        let mut message = ClientMessage::create_for_encode(REPLICATED_MAP_CONTAINS_KEY, PARTITION_ID_ANY);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&key_data));

        let response = self.invoke_on_random(message).await?;
        Self::decode_bool_response(&response)
    }

    /// Returns `true` if this map contains one or more mappings to the specified value.
    pub async fn contains_value(&self, value: &V) -> Result<bool> {
        let value_data = Self::serialize_value(value)?;

        let mut message = ClientMessage::create_for_encode(REPLICATED_MAP_CONTAINS_VALUE, PARTITION_ID_ANY);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&value_data));

        let response = self.invoke_on_random(message).await?;
        Self::decode_bool_response(&response)
    }

    /// Returns the number of key-value mappings in this map.
    pub async fn size(&self) -> Result<i32> {
        let mut message = ClientMessage::create_for_encode(REPLICATED_MAP_SIZE, PARTITION_ID_ANY);
        message.add_frame(Self::string_frame(&self.name));

        let response = self.invoke_on_random(message).await?;
        Self::decode_int_response(&response)
    }

    /// Returns `true` if this map contains no key-value mappings.
    pub async fn is_empty(&self) -> Result<bool> {
        let mut message = ClientMessage::create_for_encode(REPLICATED_MAP_IS_EMPTY, PARTITION_ID_ANY);
        message.add_frame(Self::string_frame(&self.name));

        let response = self.invoke_on_random(message).await?;
        Self::decode_bool_response(&response)
    }

    /// Removes all key-value mappings from this map.
    pub async fn clear(&self) -> Result<()> {
        let mut message = ClientMessage::create_for_encode(REPLICATED_MAP_CLEAR, PARTITION_ID_ANY);
        message.add_frame(Self::string_frame(&self.name));

        self.invoke_on_random(message).await?;
        Ok(())
    }

    /// Returns a collection view of the keys contained in this map.
    pub async fn key_set(&self) -> Result<Vec<K>> {
        let mut message = ClientMessage::create_for_encode(REPLICATED_MAP_KEY_SET, PARTITION_ID_ANY);
        message.add_frame(Self::string_frame(&self.name));

        let response = self.invoke_on_random(message).await?;
        Self::decode_collection_response(&response)
    }

    /// Returns a collection view of the values contained in this map.
    pub async fn values(&self) -> Result<Vec<V>> {
        let mut message = ClientMessage::create_for_encode(REPLICATED_MAP_VALUES, PARTITION_ID_ANY);
        message.add_frame(Self::string_frame(&self.name));

        let response = self.invoke_on_random(message).await?;
        Self::decode_collection_response(&response)
    }

    /// Returns a collection view of the mappings contained in this map.
    pub async fn entry_set(&self) -> Result<Vec<(K, V)>> {
        let mut message = ClientMessage::create_for_encode(REPLICATED_MAP_ENTRY_SET, PARTITION_ID_ANY);
        message.add_frame(Self::string_frame(&self.name));

        let response = self.invoke_on_random(message).await?;
        Self::decode_entries_response(&response)
    }

    /// Adds an entry listener to this replicated map.
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
            ClientMessage::create_for_encode(REPLICATED_MAP_ADD_ENTRY_LISTENER, PARTITION_ID_ANY);
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
            ClientMessage::create_for_encode(REPLICATED_MAP_ADD_ENTRY_LISTENER, PARTITION_ID_ANY);
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
        let mut message = ClientMessage::create_for_encode(
            REPLICATED_MAP_ADD_ENTRY_LISTENER_WITH_PREDICATE,
            PARTITION_ID_ANY,
        );
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

    /// Removes an entry listener from this replicated map.
    ///
    /// Returns `true` if the listener was successfully removed.
    pub async fn remove_entry_listener(&self, registration: &ListenerRegistration) -> Result<bool> {
        registration.deactivate();

        let mut message =
            ClientMessage::create_for_encode(REPLICATED_MAP_REMOVE_ENTRY_LISTENER, PARTITION_ID_ANY);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::uuid_frame(registration.id().as_uuid()));

        let response = self.invoke(message).await?;
        Self::decode_bool_response(&response)
    }

    /// Returns the listener statistics for this replicated map.
    pub fn listener_stats(&self) -> &ListenerStats {
        &self.listener_stats
    }

    /// Returns the local statistics for this replicated map.
    ///
    /// These statistics are tracked locally and reflect operations performed
    /// through this client instance only.
    pub fn get_local_replicated_map_stats(&self) -> ReplicatedMapStats {
        (*self.stats).clone()
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

    async fn invoke_on_random(&self, message: ClientMessage) -> Result<ClientMessage> {
        self.connection_manager.invoke_on_random(message).await
    }

    fn long_frame(value: i64) -> Frame {
        let mut buf = BytesMut::with_capacity(8);
        buf.extend_from_slice(&value.to_le_bytes());
        Frame::with_content(buf)
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

    fn decode_int_response(response: &ClientMessage) -> Result<i32> {
        let frames = response.frames();
        if frames.is_empty() {
            return Err(HazelcastError::Serialization("empty response".to_string()));
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

    fn decode_entries_response(response: &ClientMessage) -> Result<Vec<(K, V)>> {
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
}

impl<K, V> Clone for ReplicatedMap<K, V> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            connection_manager: Arc::clone(&self.connection_manager),
            listener_stats: Arc::clone(&self.listener_stats),
            stats: Arc::clone(&self.stats),
            _marker: PhantomData,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_replicated_map_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<ReplicatedMap<String, String>>();
    }

    #[test]
    fn test_replicated_map_with_various_types() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<ReplicatedMap<i32, i64>>();
        assert_send_sync::<ReplicatedMap<String, Vec<u8>>>();
        assert_send_sync::<ReplicatedMap<u64, bool>>();
    }

    #[test]
    fn test_replicated_map_clone() {
        fn assert_clone<T: Clone>() {}
        assert_clone::<ReplicatedMap<String, String>>();
    }
}
