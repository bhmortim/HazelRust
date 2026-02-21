//! Distributed map proxy implementation.

use std::collections::HashMap;
use std::hash::Hash;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::BytesMut;
use futures::Stream;
use tokio::spawn;
use tokio::sync::mpsc;
use uuid::Uuid;
use hazelcast_core::protocol::constants::{
    END_FLAG, IS_EVENT_FLAG, IS_NULL_FLAG, MAP_ADD_ENTRY_LISTENER,
    MAP_ADD_ENTRY_LISTENER_WITH_PREDICATE, MAP_ADD_INDEX, MAP_ADD_INTERCEPTOR, MAP_AGGREGATE,
    MAP_AGGREGATE_WITH_PREDICATE, MAP_ADD_PARTITION_LOST_LISTENER, MAP_CLEAR, MAP_CONTAINS_KEY,
    MAP_ENTRIES_WITH_PAGING_PREDICATE, MAP_ENTRIES_WITH_PREDICATE, MAP_EVICT, MAP_EVICT_ALL,
    MAP_EVENT_JOURNAL_READ, MAP_EVENT_JOURNAL_SUBSCRIBE, MAP_EXECUTE_ON_ALL_KEYS,
    MAP_EXECUTE_ON_KEY, MAP_EXECUTE_ON_KEYS, MAP_EXECUTE_WITH_PREDICATE, MAP_FETCH_ENTRIES,
    MAP_FETCH_KEYS, MAP_FLUSH, MAP_FORCE_UNLOCK, MAP_GET, MAP_GET_ALL, MAP_GET_ENTRY_VIEW,
    MAP_IS_LOCKED, MAP_KEYS_WITH_PAGING_PREDICATE, MAP_KEYS_WITH_PREDICATE, MAP_LOAD_ALL,
    MAP_LOAD_GIVEN_KEYS, MAP_LOCK, MAP_PROJECT, MAP_PROJECT_WITH_PREDICATE, MAP_PUT, MAP_PUT_ALL,
    MAP_PUT_IF_ABSENT, MAP_PUT_TRANSIENT, MAP_REMOVE, MAP_REMOVE_ENTRY_LISTENER, MAP_REMOVE_IF_SAME,
    MAP_REMOVE_ALL, MAP_REMOVE_INTERCEPTOR, MAP_REMOVE_PARTITION_LOST_LISTENER, MAP_REPLACE,
    MAP_REPLACE_IF_SAME, MAP_SET_ALL, MAP_SET_TTL, MAP_SIZE, MAP_TRY_LOCK, MAP_TRY_PUT, MAP_UNLOCK,
    MAP_VALUES_WITH_PAGING_PREDICATE,
    MAP_VALUES_WITH_PREDICATE, PARTITION_ID_ANY, RESPONSE_HEADER_SIZE,
};
use hazelcast_core::protocol::Frame;
use hazelcast_core::serialization::{DataInput, DataOutput, ObjectDataInput, ObjectDataOutput};
use hazelcast_core::{
    compute_partition_hash, ClientMessage, Deserializable, HazelcastError, Result, Serializable,
};

use crate::cache::{NearCache, NearCacheConfig, NearCacheStats};
use crate::proxy::map_stats::{LocalMapStats, MapStatsTracker};

/// Event types for Event Journal map events.
///
/// These represent the different kinds of mutations that can occur on map entries
/// and are recorded in the Event Journal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i32)]
pub enum EventJournalEventType {
    /// Entry was added to the map.
    Added = 1,
    /// Entry was removed from the map.
    Removed = 2,
    /// Entry was updated in the map.
    Updated = 4,
    /// Entry was evicted from the map.
    Evicted = 8,
    /// Entry expired in the map.
    Expired = 16,
    /// Entry was loaded from a map store.
    Loaded = 32,
}

impl EventJournalEventType {
    /// Creates an event type from its integer value.
    ///
    /// Returns `None` if the value doesn't correspond to a valid event type.
    pub fn from_value(value: i32) -> Option<Self> {
        match value {
            1 => Some(Self::Added),
            2 => Some(Self::Removed),
            4 => Some(Self::Updated),
            8 => Some(Self::Evicted),
            16 => Some(Self::Expired),
            32 => Some(Self::Loaded),
            _ => None,
        }
    }

    /// Returns the integer value of this event type.
    pub fn value(self) -> i32 {
        self as i32
    }
}

/// An event from the Event Journal for a map entry.
///
/// Event Journal events represent mutations that have occurred on map entries.
/// Each event has a sequence number that can be used to track position in the journal.
#[derive(Debug, Clone)]
pub struct EventJournalMapEvent<K, V> {
    /// The type of this event.
    pub event_type: EventJournalEventType,
    /// The key of the entry.
    pub key: K,
    /// The old value (before the change), if available.
    pub old_value: Option<V>,
    /// The new value (after the change), if available.
    pub new_value: Option<V>,
    /// The sequence number of this event in the journal.
    pub sequence: i64,
}

impl<K, V> EventJournalMapEvent<K, V> {
    /// Returns the event type.
    pub fn event_type(&self) -> EventJournalEventType {
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

/// Configuration for reading from an Event Journal.
#[derive(Debug, Clone)]
pub struct EventJournalConfig {
    /// The starting sequence number to read from.
    pub start_sequence: i64,
    /// The minimum number of events to read in each batch.
    pub min_size: i32,
    /// The maximum number of events to read in each batch.
    pub max_size: i32,
}

impl Default for EventJournalConfig {
    fn default() -> Self {
        Self {
            start_sequence: -1,
            min_size: 1,
            max_size: 100,
        }
    }
}

impl EventJournalConfig {
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

/// An async stream of Event Journal events.
///
/// This stream yields events from the Event Journal as they are read from the cluster.
pub struct EventJournalStream<K, V> {
    receiver: mpsc::Receiver<Result<EventJournalMapEvent<K, V>>>,
}

impl<K, V> Stream for EventJournalStream<K, V> {
    type Item = Result<EventJournalMapEvent<K, V>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.receiver).poll_recv(cx)
    }
}

/// Metadata view of a map entry.
///
/// Contains information about the entry's lifecycle, access patterns, and configuration.
#[derive(Debug, Clone)]
pub struct EntryView<K, V> {
    /// The key of the entry.
    pub key: K,
    /// The value of the entry.
    pub value: V,
    /// The cost (memory) of the entry in bytes.
    pub cost: i64,
    /// The creation time of the entry in milliseconds since epoch.
    pub creation_time: i64,
    /// The expiration time of the entry in milliseconds since epoch.
    pub expiration_time: i64,
    /// The number of hits (accesses) of the entry.
    pub hits: i64,
    /// The last access time of the entry in milliseconds since epoch.
    pub last_access_time: i64,
    /// The last time the entry was stored to the map store in milliseconds since epoch.
    pub last_stored_time: i64,
    /// The last update time of the entry in milliseconds since epoch.
    pub last_update_time: i64,
    /// The version of the entry.
    pub version: i64,
    /// The time-to-live of the entry in milliseconds.
    pub ttl: i64,
    /// The max idle time of the entry in milliseconds.
    pub max_idle: i64,
}

impl<K, V> EntryView<K, V> {
    /// Returns the key of this entry.
    pub fn key(&self) -> &K {
        &self.key
    }

    /// Returns the value of this entry.
    pub fn value(&self) -> &V {
        &self.value
    }

    /// Returns the cost (memory usage) of this entry in bytes.
    pub fn cost(&self) -> i64 {
        self.cost
    }

    /// Returns the creation time of this entry in milliseconds since epoch.
    pub fn creation_time(&self) -> i64 {
        self.creation_time
    }

    /// Returns the expiration time of this entry in milliseconds since epoch.
    ///
    /// Returns 0 if the entry never expires.
    pub fn expiration_time(&self) -> i64 {
        self.expiration_time
    }

    /// Returns the number of times this entry has been accessed.
    pub fn hits(&self) -> i64 {
        self.hits
    }

    /// Returns the last access time of this entry in milliseconds since epoch.
    pub fn last_access_time(&self) -> i64 {
        self.last_access_time
    }

    /// Returns the last time this entry was stored to the map store in milliseconds since epoch.
    pub fn last_stored_time(&self) -> i64 {
        self.last_stored_time
    }

    /// Returns the last update time of this entry in milliseconds since epoch.
    pub fn last_update_time(&self) -> i64 {
        self.last_update_time
    }

    /// Returns the version of this entry.
    pub fn version(&self) -> i64 {
        self.version
    }

    /// Returns the time-to-live of this entry in milliseconds.
    ///
    /// Returns 0 if the entry uses the map's default TTL.
    pub fn ttl(&self) -> i64 {
        self.ttl
    }

    /// Returns the max idle time of this entry in milliseconds.
    ///
    /// Returns 0 if the entry uses the map's default max idle time.
    pub fn max_idle(&self) -> i64 {
        self.max_idle
    }
}

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

    /// A bitmap index for low-cardinality attributes.
    ///
    /// Optimized for attributes with few distinct values. Supports efficient
    /// equality and IN queries. Requires configuring a unique key to identify
    /// entries in the bitmap.
    Bitmap = 2,
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
    bitmap_options: Option<crate::query::BitmapIndexOptions>,
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

    /// Returns the bitmap index options, if this is a bitmap index.
    pub fn bitmap_options(&self) -> Option<&crate::query::BitmapIndexOptions> {
        self.bitmap_options.as_ref()
    }
}

/// Builder for creating [`IndexConfig`] instances.
#[derive(Debug, Clone)]
pub struct IndexConfigBuilder {
    name: Option<String>,
    index_type: IndexType,
    attributes: Vec<String>,
    bitmap_options: Option<crate::query::BitmapIndexOptions>,
}

impl IndexConfigBuilder {
    /// Creates a new builder with default values.
    fn new() -> Self {
        Self {
            name: None,
            index_type: IndexType::Sorted,
            attributes: Vec::new(),
            bitmap_options: None,
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

    /// Sets the bitmap index options.
    ///
    /// This automatically sets the index type to [`IndexType::Bitmap`].
    ///
    /// # Example
    ///
    /// ```ignore
    /// use hazelcast_client::query::{BitmapIndexOptions, UniqueKeyTransformation};
    ///
    /// let config = IndexConfig::builder()
    ///     .name("status-bitmap-idx")
    ///     .add_attribute("status")
    ///     .bitmap_options(
    ///         BitmapIndexOptions::builder()
    ///             .unique_key("__key")
    ///             .unique_key_transformation(UniqueKeyTransformation::Long)
    ///             .build()
    ///     )
    ///     .build();
    /// ```
    pub fn bitmap_options(mut self, options: crate::query::BitmapIndexOptions) -> Self {
        self.index_type = IndexType::Bitmap;
        self.bitmap_options = Some(options);
        self
    }

    /// Builds the [`IndexConfig`].
    ///
    /// # Panics
    ///
    /// Panics if no attributes have been added, or if bitmap index type is set
    /// without bitmap options.
    pub fn build(self) -> IndexConfig {
        assert!(
            !self.attributes.is_empty(),
            "IndexConfig requires at least one attribute"
        );

        if self.index_type == IndexType::Bitmap && self.bitmap_options.is_none() {
            panic!("Bitmap index requires bitmap_options to be set");
        }

        IndexConfig {
            name: self.name,
            index_type: self.index_type,
            attributes: self.attributes,
            bitmap_options: self.bitmap_options,
        }
    }

    /// Builds the [`IndexConfig`], returning an error if invalid.
    ///
    /// Returns an error if no attributes have been added, or if bitmap index
    /// type is set without bitmap options.
    pub fn try_build(self) -> Result<IndexConfig> {
        if self.attributes.is_empty() {
            return Err(HazelcastError::Configuration(
                "IndexConfig requires at least one attribute".to_string(),
            ));
        }

        if self.index_type == IndexType::Bitmap && self.bitmap_options.is_none() {
            return Err(HazelcastError::Configuration(
                "Bitmap index requires bitmap_options to be set".to_string(),
            ));
        }

        Ok(IndexConfig {
            name: self.name,
            index_type: self.index_type,
            attributes: self.attributes,
            bitmap_options: self.bitmap_options,
        })
    }
}

impl Default for IndexConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}
static INVOCATION_COUNTER: AtomicU64 = AtomicU64::new(1);

use crate::config::PermissionAction;
use crate::connection::ConnectionManager;
use crate::listener::{
    dispatch_entry_event, BoxedEntryListener, BoxedMapPartitionLostListener, EntryEvent,
    EntryEventType, EntryListener, EntryListenerConfig, ListenerId, ListenerRegistration,
    ListenerStats, MapPartitionLostEvent, MapPartitionLostListener,
};
use crate::proxy::distributed_iterator::{DistributedIterator, IterationType as DistIterationType, IteratorConfig};
use crate::proxy::entry_processor::{EntryProcessor, EntryProcessorResult};
use crate::proxy::interceptor::MapInterceptor;
use crate::query::{Aggregator, AnchorEntry, IterationType, PagingPredicate, PagingResult, Predicate, Projection};

/// A distributed map proxy for performing key-value operations on a Hazelcast cluster.
///
/// `IMap` provides async CRUD operations with automatic serialization and partition routing.
/// Optionally supports client-side near-caching for improved read performance.
#[derive(Debug)]
pub struct IMap<K, V> {
    name: String,
    connection_manager: Arc<ConnectionManager>,
    listener_stats: Arc<ListenerStats>,
    stats_tracker: Arc<MapStatsTracker>,
    near_cache: Option<Arc<Mutex<NearCache<Vec<u8>, Vec<u8>>>>>,
    invalidation_registration: Arc<Mutex<Option<ListenerRegistration>>>,
    thread_id: i64,
    _phantom: PhantomData<fn() -> (K, V)>,
}

impl<K, V> IMap<K, V> {
    /// Creates a new map proxy.
    pub(crate) fn new(name: String, connection_manager: Arc<ConnectionManager>) -> Self {
        Self {
            name,
            connection_manager,
            listener_stats: Arc::new(ListenerStats::new()),
            stats_tracker: Arc::new(MapStatsTracker::new()),
            near_cache: None,
            invalidation_registration: Arc::new(Mutex::new(None)),
            thread_id: std::process::id() as i64,
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
            stats_tracker: Arc::new(MapStatsTracker::new()),
            near_cache: Some(Arc::new(Mutex::new(NearCache::new(config)))),
            invalidation_registration: Arc::new(Mutex::new(None)),
            thread_id: std::process::id() as i64,
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

    fn check_permission(&self, action: PermissionAction) -> hazelcast_core::Result<()> {
        let permissions = self.connection_manager.effective_permissions();
        if !permissions.is_permitted(action) {
            return Err(HazelcastError::Authorization(format!(
                "map '{}' operation denied: requires {:?} permission",
                self.name, action
            )));
        }
        Ok(())
    }
}

impl<K, V> IMap<K, V>
where
    K: Serializable + Deserializable + Send + Sync,
    V: Serializable + Deserializable + Send + Sync,
{
    async fn check_quorum(&self, is_read: bool) -> Result<()> {
        self.connection_manager.check_quorum(&self.name, is_read).await
    }

    /// Retrieves the value associated with the given key.
    ///
    /// If near-cache is enabled, checks the local cache first. On a cache miss,
    /// fetches from the cluster and populates the near-cache.
    ///
    /// Returns `None` if the key does not exist in the map.
    pub async fn get(&self, key: &K) -> Result<Option<V>> {
        self.check_permission(PermissionAction::Read)?;
        self.check_quorum(true).await?;
        self.stats_tracker.record_get();
        let key_data = Self::serialize_value(key)?;

        // Check near-cache first
        if let Some(ref cache) = self.near_cache {
            let mut cache_guard = cache.lock().unwrap();
            if let Some(value_data) = cache_guard.get(&key_data) {
                self.stats_tracker.record_hit();
                let mut input = ObjectDataInput::new(&value_data);
                return V::deserialize(&mut input).map(Some);
            }
        }

        self.stats_tracker.record_miss();
        let partition_id = compute_partition_hash(&key_data);

        let mut message = ClientMessage::create_for_encode(MAP_GET, partition_id);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&key_data));

        let response = self.invoke_on_partition(partition_id, message).await?;
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
        self.check_permission(PermissionAction::Put)?;
        self.check_quorum(false).await?;
        self.stats_tracker.record_put();
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

        let response = self.invoke_on_partition(partition_id, message).await?;
        Self::decode_nullable_response(&response)
    }

    /// Associates the specified value with the specified key only if the key
    /// is not already associated with a value.
    ///
    /// If near-cache is enabled, invalidates the local cache entry before
    /// sending the update to the cluster.
    ///
    /// Returns `None` if the key was not present (and the value was inserted),
    /// or `Some(existing_value)` if the key was already present.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Only inserts if "user:1" doesn't exist
    /// let existing = map.put_if_absent("user:1".to_string(), user).await?;
    /// if existing.is_some() {
    ///     println!("User already exists");
    /// }
    /// ```
    pub async fn put_if_absent(&self, key: K, value: V) -> Result<Option<V>> {
        self.check_permission(PermissionAction::Put)?;
        self.check_quorum(false).await?;
        let key_data = Self::serialize_value(&key)?;
        let value_data = Self::serialize_value(&value)?;

        if let Some(ref cache) = self.near_cache {
            let mut cache_guard = cache.lock().unwrap();
            cache_guard.invalidate(&key_data);
        }

        let partition_id = compute_partition_hash(&key_data);

        let mut message = ClientMessage::create_for_encode(MAP_PUT_IF_ABSENT, partition_id);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&key_data));
        message.add_frame(Self::data_frame(&value_data));
        message.add_frame(Self::long_frame(-1)); // TTL: no expiry

        let response = self.invoke_on_partition(partition_id, message).await?;
        Self::decode_nullable_response(&response)
    }

    /// Replaces the entry for the specified key only if it is currently mapped to some value.
    ///
    /// If near-cache is enabled, invalidates the local cache entry before
    /// sending the update to the cluster.
    ///
    /// Returns the previous value associated with the key, or `None` if there was no mapping.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Only replaces if "user:1" exists
    /// let old_value = map.replace(&"user:1".to_string(), new_user).await?;
    /// match old_value {
    ///     Some(old) => println!("Replaced: {:?}", old),
    ///     None => println!("Key not found, nothing replaced"),
    /// }
    /// ```
    pub async fn replace(&self, key: &K, value: V) -> Result<Option<V>> {
        self.check_permission(PermissionAction::Put)?;
        self.check_quorum(false).await?;
        let key_data = Self::serialize_value(key)?;
        let value_data = Self::serialize_value(&value)?;

        if let Some(ref cache) = self.near_cache {
            let mut cache_guard = cache.lock().unwrap();
            cache_guard.invalidate(&key_data);
        }

        let partition_id = compute_partition_hash(&key_data);

        let mut message = ClientMessage::create_for_encode(MAP_REPLACE, partition_id);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&key_data));
        message.add_frame(Self::data_frame(&value_data));

        let response = self.invoke_on_partition(partition_id, message).await?;
        Self::decode_nullable_response(&response)
    }

    /// Replaces the entry for the specified key only if currently mapped to the specified value.
    ///
    /// This is an atomic compare-and-swap operation. If near-cache is enabled,
    /// invalidates the local cache entry before sending the update to the cluster.
    ///
    /// Returns `true` if the value was replaced, `false` otherwise.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Atomically update only if current value matches expected
    /// let replaced = map.replace_if_equal(&key, &old_value, new_value).await?;
    /// if replaced {
    ///     println!("Successfully updated");
    /// } else {
    ///     println!("Value was modified by another client");
    /// }
    /// ```
    pub async fn replace_if_equal(&self, key: &K, old_value: &V, new_value: V) -> Result<bool> {
        self.check_permission(PermissionAction::Put)?;
        self.check_quorum(false).await?;
        let key_data = Self::serialize_value(key)?;
        let old_value_data = Self::serialize_value(old_value)?;
        let new_value_data = Self::serialize_value(&new_value)?;

        if let Some(ref cache) = self.near_cache {
            let mut cache_guard = cache.lock().unwrap();
            cache_guard.invalidate(&key_data);
        }

        let partition_id = compute_partition_hash(&key_data);

        let mut message = ClientMessage::create_for_encode(MAP_REPLACE_IF_SAME, partition_id);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&key_data));
        message.add_frame(Self::data_frame(&old_value_data));
        message.add_frame(Self::data_frame(&new_value_data));

        let response = self.invoke_on_partition(partition_id, message).await?;
        Self::decode_bool_response(&response)
    }

    /// Removes the mapping for a key from this map if it is present.
    ///
    /// If near-cache is enabled, invalidates the local cache entry before
    /// sending the remove to the cluster.
    ///
    /// Returns the previous value associated with the key, or `None` if there was no mapping.
    pub async fn remove(&self, key: &K) -> Result<Option<V>> {
        self.check_permission(PermissionAction::Remove)?;
        self.check_quorum(false).await?;
        self.stats_tracker.record_remove();
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

        let response = self.invoke_on_partition(partition_id, message).await?;
        Self::decode_nullable_response(&response)
    }

    /// Removes the entry for the specified key only if it is currently mapped to the specified value.
    ///
    /// This is an atomic compare-and-delete operation. If near-cache is enabled,
    /// invalidates the local cache entry before sending the remove to the cluster.
    ///
    /// Returns `true` if the value was removed, `false` otherwise.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Only remove if the value matches what we expect
    /// let removed = map.remove_if_equal(&key, &expected_value).await?;
    /// if removed {
    ///     println!("Entry removed");
    /// } else {
    ///     println!("Value didn't match or key not found");
    /// }
    /// ```
    pub async fn remove_if_equal(&self, key: &K, value: &V) -> Result<bool> {
        self.check_permission(PermissionAction::Remove)?;
        self.check_quorum(false).await?;
        let key_data = Self::serialize_value(key)?;
        let value_data = Self::serialize_value(value)?;

        if let Some(ref cache) = self.near_cache {
            let mut cache_guard = cache.lock().unwrap();
            cache_guard.invalidate(&key_data);
        }

        let partition_id = compute_partition_hash(&key_data);

        let mut message = ClientMessage::create_for_encode(MAP_REMOVE_IF_SAME, partition_id);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&key_data));
        message.add_frame(Self::data_frame(&value_data));

        let response = self.invoke_on_partition(partition_id, message).await?;
        Self::decode_bool_response(&response)
    }

    /// Returns `true` if this map contains a mapping for the specified key.
    pub async fn contains_key(&self, key: &K) -> Result<bool> {
        self.check_permission(PermissionAction::Read)?;
        self.check_quorum(true).await?;
        let key_data = Self::serialize_value(key)?;
        let partition_id = compute_partition_hash(&key_data);

        let mut message = ClientMessage::create_for_encode(MAP_CONTAINS_KEY, partition_id);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&key_data));

        let response = self.invoke_on_partition(partition_id, message).await?;
        Self::decode_bool_response(&response)
    }

    /// Returns the number of key-value mappings in this map.
    pub async fn size(&self) -> Result<usize> {
        self.check_permission(PermissionAction::Read)?;
        self.check_quorum(true).await?;
        let mut message = ClientMessage::create_for_encode(MAP_SIZE, PARTITION_ID_ANY);
        message.add_frame(Self::string_frame(&self.name));

        let response = self.invoke_on_random(message).await?;
        Self::decode_int_response(&response).map(|v| v as usize)
    }

    /// Asynchronously retrieves the value associated with the given key.
    ///
    /// This method spawns the operation on a separate task and returns a handle
    /// that can be awaited or polled. Useful for fire-and-forget patterns or
    /// starting multiple operations concurrently.
    ///
    /// If near-cache is enabled, checks the local cache first. On a cache miss,
    /// fetches from the cluster and populates the near-cache.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Start multiple gets concurrently
    /// let handle1 = map.get_async("key1".to_string());
    /// let handle2 = map.get_async("key2".to_string());
    ///
    /// // Await results
    /// let result1 = handle1.await??;
    /// let result2 = handle2.await??;
    /// ```
    pub fn get_async(&self, key: K) -> tokio::task::JoinHandle<Result<Option<V>>>
    where
        K: 'static,
        V: 'static,
    {
        let this = self.clone();
        spawn(async move { this.get(&key).await })
    }

    /// Asynchronously associates the specified value with the specified key.
    ///
    /// This method spawns the operation on a separate task and returns a handle
    /// that can be awaited or polled. Useful for fire-and-forget patterns or
    /// starting multiple operations concurrently.
    ///
    /// If near-cache is enabled, invalidates the local cache entry before
    /// sending the update to the cluster.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Start multiple puts concurrently
    /// let handle1 = map.put_async("key1".to_string(), "value1".to_string());
    /// let handle2 = map.put_async("key2".to_string(), "value2".to_string());
    ///
    /// // Await results (previous values)
    /// let old1 = handle1.await??;
    /// let old2 = handle2.await??;
    /// ```
    pub fn put_async(&self, key: K, value: V) -> tokio::task::JoinHandle<Result<Option<V>>>
    where
        K: 'static,
        V: 'static,
    {
        let this = self.clone();
        spawn(async move { this.put(key, value).await })
    }

    /// Asynchronously removes the mapping for a key from this map.
    ///
    /// This method spawns the operation on a separate task and returns a handle
    /// that can be awaited or polled. Useful for fire-and-forget patterns or
    /// starting multiple operations concurrently.
    ///
    /// If near-cache is enabled, invalidates the local cache entry before
    /// sending the remove to the cluster.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Start multiple removes concurrently
    /// let handle1 = map.remove_async("key1".to_string());
    /// let handle2 = map.remove_async("key2".to_string());
    ///
    /// // Await results (previous values)
    /// let old1 = handle1.await??;
    /// let old2 = handle2.await??;
    /// ```
    pub fn remove_async(&self, key: K) -> tokio::task::JoinHandle<Result<Option<V>>>
    where
        K: 'static,
        V: 'static,
    {
        let this = self.clone();
        spawn(async move { this.remove(&key).await })
    }

    /// Asynchronously checks if this map contains a mapping for the specified key.
    ///
    /// This method spawns the operation on a separate task and returns a handle
    /// that can be awaited or polled. Useful for fire-and-forget patterns or
    /// starting multiple operations concurrently.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Start multiple contains checks concurrently
    /// let handle1 = map.contains_key_async("key1".to_string());
    /// let handle2 = map.contains_key_async("key2".to_string());
    ///
    /// // Await results
    /// let exists1 = handle1.await??;
    /// let exists2 = handle2.await??;
    /// ```
    pub fn contains_key_async(&self, key: K) -> tokio::task::JoinHandle<Result<bool>>
    where
        K: 'static,
        V: 'static,
    {
        let this = self.clone();
        spawn(async move { this.contains_key(&key).await })
    }

    /// Puts all entries from the given map into this map.
    ///
    /// This is more efficient than calling `put` for each entry individually.
    /// If near-cache is enabled, invalidates all affected keys before the operation.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let mut entries = HashMap::new();
    /// entries.insert("key1".to_string(), "value1".to_string());
    /// entries.insert("key2".to_string(), "value2".to_string());
    /// map.put_all(entries).await?;
    /// ```
    pub async fn put_all(&self, entries: HashMap<K, V>) -> Result<()>
    where
        K: Clone,
    {
        self.check_permission(PermissionAction::Put)?;
        self.check_quorum(false).await?;

        if entries.is_empty() {
            return Ok(());
        }

        let mut serialized_entries = Vec::with_capacity(entries.len());
        for (key, value) in &entries {
            let key_data = Self::serialize_value(key)?;
            let value_data = Self::serialize_value(value)?;
            serialized_entries.push((key_data, value_data));
        }

        if let Some(ref cache) = self.near_cache {
            let mut cache_guard = cache.lock().unwrap();
            for (key_data, _) in &serialized_entries {
                cache_guard.invalidate(key_data);
            }
        }

        let mut message = ClientMessage::create_for_encode(MAP_PUT_ALL, PARTITION_ID_ANY);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::int_frame(serialized_entries.len() as i32));

        for (key_data, value_data) in serialized_entries {
            message.add_frame(Self::data_frame(&key_data));
            message.add_frame(Self::data_frame(&value_data));
        }

        self.invoke_on_random(message).await?;
        Ok(())
    }

    /// Asynchronously puts all entries from the given map into this map.
    ///
    /// This method spawns the operation on a separate task and returns a handle
    /// that can be awaited or polled. Useful for fire-and-forget patterns or
    /// starting multiple operations concurrently.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let mut entries = HashMap::new();
    /// entries.insert("key1".to_string(), "value1".to_string());
    /// entries.insert("key2".to_string(), "value2".to_string());
    ///
    /// let handle = map.put_all_async(entries);
    /// // Do other work...
    /// handle.await??;
    /// ```
    pub fn put_all_async(&self, entries: HashMap<K, V>) -> tokio::task::JoinHandle<Result<()>>
    where
        K: Clone + 'static,
        V: 'static,
    {
        let this = self.clone();
        spawn(async move { this.put_all(entries).await })
    }

    /// Sets all entries from the given map into this map.
    ///
    /// Unlike `put_all`, this operation does not trigger entry listeners on the
    /// cluster. This makes it more efficient for bulk data loading scenarios
    /// where event notifications are not needed.
    ///
    /// If near-cache is enabled, invalidates all affected keys before the operation.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let mut entries = HashMap::new();
    /// entries.insert("key1".to_string(), "value1".to_string());
    /// entries.insert("key2".to_string(), "value2".to_string());
    /// map.set_all(entries).await?;
    /// ```
    pub async fn set_all(&self, entries: HashMap<K, V>) -> Result<()>
    where
        K: Clone,
    {
        self.check_permission(PermissionAction::Put)?;
        self.check_quorum(false).await?;

        if entries.is_empty() {
            return Ok(());
        }

        let mut serialized_entries = Vec::with_capacity(entries.len());
        for (key, value) in &entries {
            let key_data = Self::serialize_value(key)?;
            let value_data = Self::serialize_value(value)?;
            serialized_entries.push((key_data, value_data));
        }

        if let Some(ref cache) = self.near_cache {
            let mut cache_guard = cache.lock().unwrap();
            for (key_data, _) in &serialized_entries {
                cache_guard.invalidate(key_data);
            }
        }

        let mut message = ClientMessage::create_for_encode(MAP_SET_ALL, PARTITION_ID_ANY);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::int_frame(serialized_entries.len() as i32));

        for (key_data, value_data) in serialized_entries {
            message.add_frame(Self::data_frame(&key_data));
            message.add_frame(Self::data_frame(&value_data));
        }

        self.invoke_on_random(message).await?;
        Ok(())
    }

    /// Asynchronously sets all entries from the given map into this map.
    ///
    /// This method spawns the operation on a separate task and returns a handle
    /// that can be awaited or polled. Unlike `put_all_async`, this operation does
    /// not trigger entry listeners on the cluster.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let mut entries = HashMap::new();
    /// entries.insert("key1".to_string(), "value1".to_string());
    /// entries.insert("key2".to_string(), "value2".to_string());
    ///
    /// let handle = map.set_all_async(entries);
    /// // Do other work...
    /// handle.await??;
    /// ```
    pub fn set_all_async(&self, entries: HashMap<K, V>) -> tokio::task::JoinHandle<Result<()>>
    where
        K: Clone + 'static,
        V: 'static,
    {
        let this = self.clone();
        spawn(async move { this.set_all(entries).await })
    }

    /// Retrieves all values for the given keys.
    ///
    /// Returns a map containing only the keys that exist in this map.
    /// If near-cache is enabled, checks the local cache first and only fetches
    /// cache misses from the cluster.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let keys = vec!["key1".to_string(), "key2".to_string(), "key3".to_string()];
    /// let results = map.get_all(&keys).await?;
    /// for (key, value) in results {
    ///     println!("{}: {}", key, value);
    /// }
    /// ```
    pub async fn get_all(&self, keys: &[K]) -> Result<HashMap<K, V>>
    where
        K: Clone + Eq + Hash,
    {
        self.check_permission(PermissionAction::Read)?;
        self.check_quorum(true).await?;

        if keys.is_empty() {
            return Ok(HashMap::new());
        }

        let mut results = HashMap::new();
        let mut keys_to_fetch = Vec::new();
        let mut key_data_map: HashMap<Vec<u8>, K> = HashMap::new();

        for key in keys {
            let key_data = Self::serialize_value(key)?;

            if let Some(ref cache) = self.near_cache {
                let mut cache_guard = cache.lock().unwrap();
                if let Some(value_data) = cache_guard.get(&key_data) {
                    let mut input = ObjectDataInput::new(&value_data);
                    if let Ok(value) = V::deserialize(&mut input) {
                        results.insert(key.clone(), value);
                        continue;
                    }
                }
            }

            key_data_map.insert(key_data.clone(), key.clone());
            keys_to_fetch.push(key_data);
        }

        if keys_to_fetch.is_empty() {
            return Ok(results);
        }

        let mut message = ClientMessage::create_for_encode(MAP_GET_ALL, PARTITION_ID_ANY);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::int_frame(keys_to_fetch.len() as i32));

        for key_data in &keys_to_fetch {
            message.add_frame(Self::data_frame(key_data));
        }

        let response = self.invoke_on_random(message).await?;
        let fetched_entries: Vec<(K, V)> = Self::decode_entries_response(&response)?;

        for (key, value) in fetched_entries {
            if let Some(ref cache) = self.near_cache {
                let key_data = Self::serialize_value(&key)?;
                let value_data = Self::serialize_value(&value)?;
                let mut cache_guard = cache.lock().unwrap();
                cache_guard.put(key_data, value_data);
            }
            results.insert(key, value);
        }

        Ok(results)
    }

    /// Removes all entries with the given keys from this map.
    ///
    /// This is a convenience method that removes each key. Unlike `remove`,
    /// this method does not return the previous values.
    /// If near-cache is enabled, invalidates all affected keys.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let keys = vec!["key1".to_string(), "key2".to_string(), "key3".to_string()];
    /// map.remove_all(&keys).await?;
    /// ```
    pub async fn remove_all(&self, keys: &[K]) -> Result<()> {
        self.check_permission(PermissionAction::Remove)?;
        self.check_quorum(false).await?;

        if keys.is_empty() {
            return Ok(());
        }

        if let Some(ref cache) = self.near_cache {
            let mut cache_guard = cache.lock().unwrap();
            for key in keys {
                if let Ok(key_data) = Self::serialize_value(key) {
                    cache_guard.invalidate(&key_data);
                }
            }
        }

        for key in keys {
            let key_data = Self::serialize_value(key)?;
            let partition_id = compute_partition_hash(&key_data);

            let mut message = ClientMessage::create_for_encode(MAP_REMOVE, partition_id);
            message.add_frame(Self::string_frame(&self.name));
            message.add_frame(Self::data_frame(&key_data));

            self.invoke_on_partition(partition_id, message).await?;
        }

        Ok(())
    }

    /// Removes all entries matching the given predicate from this map.
    ///
    /// The predicate is evaluated on the cluster, and all matching entries are
    /// removed atomically. This is more efficient than fetching keys and removing
    /// them individually.
    ///
    /// If near-cache is enabled, clears the local cache since we cannot know
    /// which specific entries will be removed.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use hazelcast_client::query::Predicates;
    ///
    /// // Remove all users older than 65
    /// let predicate = Predicates::greater_than("age", &65i32)?;
    /// map.remove_all_with_predicate(&predicate).await?;
    ///
    /// // Remove all inactive entries
    /// let predicate = Predicates::equal("status", &"inactive".to_string())?;
    /// map.remove_all_with_predicate(&predicate).await?;
    /// ```
    pub async fn remove_all_with_predicate(&self, predicate: &dyn Predicate) -> Result<()> {
        self.check_permission(PermissionAction::Remove)?;
        self.check_quorum(false).await?;

        // Clear near-cache before remote operation since we can't know
        // which specific entries will be removed
        if let Some(ref cache) = self.near_cache {
            let mut cache_guard = cache.lock().unwrap();
            cache_guard.clear();
        }

        let predicate_data = predicate.to_predicate_data()?;

        let mut message = ClientMessage::create_for_encode(MAP_REMOVE_ALL, PARTITION_ID_ANY);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&predicate_data));

        self.invoke_on_random(message).await?;
        Ok(())
    }

    /// Removes all entries from this map.
    ///
    /// If near-cache is enabled, clears the local cache as well.
    pub async fn clear(&self) -> Result<()> {
        self.check_permission(PermissionAction::Remove)?;
        self.check_quorum(false).await?;
        // Clear near-cache before remote operation
        if let Some(ref cache) = self.near_cache {
            let mut cache_guard = cache.lock().unwrap();
            cache_guard.clear();
        }

        let mut message = ClientMessage::create_for_encode(MAP_CLEAR, PARTITION_ID_ANY);
        message.add_frame(Self::string_frame(&self.name));

        self.invoke_on_random(message).await?;
        Ok(())
    }

    /// Acquires the lock for the specified key.
    ///
    /// If the lock is not available, the current task waits until the lock
    /// has been acquired.
    ///
    /// Locks are re-entrant: the same thread can acquire the lock multiple
    /// times. Each `lock` call must be balanced by a corresponding `unlock`.
    ///
    /// # Warning
    ///
    /// This method uses indefinite blocking. If the lock is never released,
    /// this method will block forever.
    ///
    /// # Errors
    ///
    /// Returns an error if a network error occurs.
    pub async fn lock(&self, key: &K) -> Result<()> {
        self.check_permission(PermissionAction::Lock)?;
        let key_data = Self::serialize_value(key)?;
        let partition_id = compute_partition_hash(&key_data);

        let mut message = ClientMessage::create_for_encode(MAP_LOCK, partition_id);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&key_data));
        message.add_frame(Self::long_frame(self.thread_id));
        message.add_frame(Self::long_frame(-1)); // TTL: indefinite
        message.add_frame(Self::invocation_uid_frame());

        self.invoke_on_partition(partition_id, message).await?;
        Ok(())
    }

    /// Tries to acquire the lock for the specified key within the given timeout.
    ///
    /// Returns immediately if the lock is available. Otherwise waits up to
    /// the specified timeout for the lock to become available.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to lock
    /// * `timeout` - The maximum time to wait for the lock
    ///
    /// # Returns
    ///
    /// `true` if the lock was acquired, `false` if the timeout expired.
    ///
    /// # Errors
    ///
    /// Returns an error if a network error occurs.
    pub async fn try_lock(&self, key: &K, timeout: Duration) -> Result<bool> {
        self.check_permission(PermissionAction::Lock)?;
        let key_data = Self::serialize_value(key)?;
        let partition_id = compute_partition_hash(&key_data);
        let timeout_ms = timeout.as_millis() as i64;

        let mut message = ClientMessage::create_for_encode(MAP_TRY_LOCK, partition_id);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&key_data));
        message.add_frame(Self::long_frame(self.thread_id));
        message.add_frame(Self::long_frame(-1)); // TTL: indefinite once acquired
        message.add_frame(Self::long_frame(timeout_ms));
        message.add_frame(Self::invocation_uid_frame());

        let response = self.invoke_on_partition(partition_id, message).await?;
        Self::decode_bool_response(&response)
    }

    /// Releases the lock for the specified key.
    ///
    /// The lock must have been acquired by this thread. If the lock was
    /// acquired multiple times (re-entrant), it must be released the same
    /// number of times before other threads can acquire it.
    ///
    /// # Errors
    ///
    /// Returns an error if a network error occurs or the lock is not held
    /// by the current thread.
    pub async fn unlock(&self, key: &K) -> Result<()> {
        self.check_permission(PermissionAction::Lock)?;
        let key_data = Self::serialize_value(key)?;
        let partition_id = compute_partition_hash(&key_data);

        let mut message = ClientMessage::create_for_encode(MAP_UNLOCK, partition_id);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&key_data));
        message.add_frame(Self::long_frame(self.thread_id));
        message.add_frame(Self::invocation_uid_frame());

        self.invoke_on_partition(partition_id, message).await?;
        Ok(())
    }

    /// Returns whether the specified key is locked.
    ///
    /// # Returns
    ///
    /// `true` if the key is locked by any thread, `false` otherwise.
    ///
    /// # Errors
    ///
    /// Returns an error if a network error occurs.
    pub async fn is_locked(&self, key: &K) -> Result<bool> {
        self.check_permission(PermissionAction::Read)?;
        let key_data = Self::serialize_value(key)?;
        let partition_id = compute_partition_hash(&key_data);

        let mut message = ClientMessage::create_for_encode(MAP_IS_LOCKED, partition_id);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&key_data));

        let response = self.invoke_on_partition(partition_id, message).await?;
        Self::decode_bool_response(&response)
    }

    /// Forcefully unlocks the specified key, regardless of the lock owner.
    ///
    /// This method should be used with caution. It can break the lock
    /// semantics and lead to data inconsistency if used incorrectly.
    ///
    /// Use this method only for administrative purposes when a lock holder
    /// has crashed and the lock needs to be released manually.
    ///
    /// # Errors
    ///
    /// Returns an error if a network error occurs.
    pub async fn force_unlock(&self, key: &K) -> Result<()> {
        self.check_permission(PermissionAction::Lock)?;
        let key_data = Self::serialize_value(key)?;
        let partition_id = compute_partition_hash(&key_data);

        let mut message = ClientMessage::create_for_encode(MAP_FORCE_UNLOCK, partition_id);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&key_data));
        message.add_frame(Self::invocation_uid_frame());

        self.invoke_on_partition(partition_id, message).await?;
        Ok(())
    }

    /// Evicts the entry for the specified key from the map.
    ///
    /// Unlike `remove`, eviction only removes the entry from memory without
    /// triggering map store delete. If the map has a map store configured,
    /// the entry will be loaded again on the next access.
    ///
    /// If near-cache is enabled, invalidates the local cache entry.
    ///
    /// # Returns
    ///
    /// `true` if the entry was evicted, `false` if the key was not found.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Evict a cached entry to free memory
    /// let evicted = map.evict(&"large-data-key".to_string()).await?;
    /// if evicted {
    ///     println!("Entry evicted from memory");
    /// }
    /// ```
    pub async fn evict(&self, key: &K) -> Result<bool> {
        self.check_permission(PermissionAction::Remove)?;
        self.check_quorum(false).await?;
        let key_data = Self::serialize_value(key)?;

        if let Some(ref cache) = self.near_cache {
            let mut cache_guard = cache.lock().unwrap();
            cache_guard.invalidate(&key_data);
        }

        let partition_id = compute_partition_hash(&key_data);

        let mut message = ClientMessage::create_for_encode(MAP_EVICT, partition_id);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&key_data));
        message.add_frame(Self::long_frame(self.thread_id));

        let response = self.invoke_on_partition(partition_id, message).await?;
        Self::decode_bool_response(&response)
    }

    /// Evicts all entries from this map.
    ///
    /// Unlike `clear`, eviction only removes entries from memory without
    /// triggering map store deletes. If the map has a map store configured,
    /// entries will be loaded again on access.
    ///
    /// If near-cache is enabled, clears the local cache as well.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Free memory by evicting all cached entries
    /// map.evict_all().await?;
    /// ```
    pub async fn evict_all(&self) -> Result<()> {
        self.check_permission(PermissionAction::Remove)?;
        self.check_quorum(false).await?;

        if let Some(ref cache) = self.near_cache {
            let mut cache_guard = cache.lock().unwrap();
            cache_guard.clear();
        }

        let mut message = ClientMessage::create_for_encode(MAP_EVICT_ALL, PARTITION_ID_ANY);
        message.add_frame(Self::string_frame(&self.name));

        self.invoke_on_random(message).await?;
        Ok(())
    }

    /// Flushes all pending map store operations for this map.
    ///
    /// If the map has a map store configured with write-behind mode,
    /// this method forces all pending writes to be persisted immediately.
    ///
    /// This is a no-op if:
    /// - No map store is configured
    /// - The map store is in write-through mode
    /// - There are no pending writes
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Ensure all data is persisted before shutdown
    /// map.flush().await?;
    /// ```
    pub async fn flush(&self) -> Result<()> {
        self.check_permission(PermissionAction::Put)?;

        let mut message = ClientMessage::create_for_encode(MAP_FLUSH, PARTITION_ID_ANY);
        message.add_frame(Self::string_frame(&self.name));

        self.invoke_on_random(message).await?;
        Ok(())
    }

    /// Returns the entry view for the specified key.
    ///
    /// The entry view contains metadata about the entry including creation time,
    /// expiration time, hit count, version, and other lifecycle information.
    ///
    /// Returns `None` if the key does not exist in the map.
    ///
    /// # Example
    ///
    /// ```ignore
    /// if let Some(view) = map.get_entry_view(&"user:123".to_string()).await? {
    ///     println!("Entry created at: {}", view.creation_time());
    ///     println!("Hit count: {}", view.hits());
    ///     println!("Version: {}", view.version());
    /// }
    /// ```
    pub async fn get_entry_view(&self, key: &K) -> Result<Option<EntryView<K, V>>> {
        self.check_permission(PermissionAction::Read)?;
        self.check_quorum(true).await?;
        let key_data = Self::serialize_value(key)?;
        let partition_id = compute_partition_hash(&key_data);

        let mut message = ClientMessage::create_for_encode(MAP_GET_ENTRY_VIEW, partition_id);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&key_data));
        message.add_frame(Self::long_frame(self.thread_id));

        let response = self.invoke_on_partition(partition_id, message).await?;
        Self::decode_entry_view_response(&response)
    }

    /// Sets the time-to-live for an existing entry.
    ///
    /// Updates the TTL of the entry without modifying its value. The entry
    /// will be automatically evicted after the TTL expires.
    ///
    /// Returns `true` if the TTL was successfully set, `false` if the key
    /// was not found.
    ///
    /// # Arguments
    ///
    /// * `key` - The key of the entry
    /// * `ttl` - The new time-to-live duration
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Extend the TTL of an entry
    /// let updated = map.set_ttl(&"session:abc".to_string(), Duration::from_secs(3600)).await?;
    /// if updated {
    ///     println!("Session TTL extended by 1 hour");
    /// }
    /// ```
    pub async fn set_ttl(&self, key: &K, ttl: Duration) -> Result<bool> {
        self.check_permission(PermissionAction::Put)?;
        self.check_quorum(false).await?;
        let key_data = Self::serialize_value(key)?;
        let partition_id = compute_partition_hash(&key_data);
        let ttl_ms = ttl.as_millis() as i64;

        let mut message = ClientMessage::create_for_encode(MAP_SET_TTL, partition_id);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&key_data));
        message.add_frame(Self::long_frame(ttl_ms));

        let response = self.invoke_on_partition(partition_id, message).await?;
        Self::decode_bool_response(&response)
    }

    /// Tries to put the specified key-value pair within the given timeout.
    ///
    /// Unlike `put`, this method will not block indefinitely. If the entry
    /// cannot be written within the timeout (e.g., due to a lock held by
    /// another thread), it returns `false`.
    ///
    /// If near-cache is enabled, invalidates the local cache entry before
    /// the operation.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to associate the value with
    /// * `value` - The value to store
    /// * `timeout` - The maximum time to wait
    ///
    /// # Returns
    ///
    /// `true` if the entry was successfully put, `false` if the timeout expired.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Try to update with a short timeout
    /// let success = map.try_put(
    ///     "contested-key".to_string(),
    ///     "new-value".to_string(),
    ///     Duration::from_millis(100),
    /// ).await?;
    /// if !success {
    ///     println!("Could not acquire lock in time");
    /// }
    /// ```
    pub async fn try_put(&self, key: K, value: V, timeout: Duration) -> Result<bool> {
        self.check_permission(PermissionAction::Put)?;
        self.check_quorum(false).await?;
        let key_data = Self::serialize_value(&key)?;
        let value_data = Self::serialize_value(&value)?;

        if let Some(ref cache) = self.near_cache {
            let mut cache_guard = cache.lock().unwrap();
            cache_guard.invalidate(&key_data);
        }

        let partition_id = compute_partition_hash(&key_data);
        let timeout_ms = timeout.as_millis() as i64;

        let mut message = ClientMessage::create_for_encode(MAP_TRY_PUT, partition_id);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&key_data));
        message.add_frame(Self::data_frame(&value_data));
        message.add_frame(Self::long_frame(self.thread_id));
        message.add_frame(Self::long_frame(timeout_ms));

        let response = self.invoke_on_partition(partition_id, message).await?;
        Self::decode_bool_response(&response)
    }

    /// Associates the specified value with the specified key, with a maximum idle time.
    ///
    /// The entry will be evicted if it is not accessed within the specified max idle duration.
    /// If near-cache is enabled, invalidates the local cache entry before
    /// sending the update to the cluster.
    ///
    /// Returns the previous value associated with the key, or `None` if there was no mapping.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to associate the value with
    /// * `value` - The value to store
    /// * `max_idle` - The maximum idle time for the entry (use `Duration::ZERO` for no max idle)
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Store a session that expires if not accessed for 30 minutes
    /// let old = map.put_with_max_idle(
    ///     "session:abc".to_string(),
    ///     session_data,
    ///     Duration::from_secs(1800),
    /// ).await?;
    /// ```
    pub async fn put_with_max_idle(&self, key: K, value: V, max_idle: Duration) -> Result<Option<V>> {
        self.check_permission(PermissionAction::Put)?;
        self.check_quorum(false).await?;
        let key_data = Self::serialize_value(&key)?;
        let value_data = Self::serialize_value(&value)?;

        if let Some(ref cache) = self.near_cache {
            let mut cache_guard = cache.lock().unwrap();
            cache_guard.invalidate(&key_data);
        }

        let partition_id = compute_partition_hash(&key_data);
        let max_idle_ms = if max_idle.is_zero() { -1 } else { max_idle.as_millis() as i64 };

        let mut message = ClientMessage::create_for_encode(MAP_PUT, partition_id);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&key_data));
        message.add_frame(Self::data_frame(&value_data));
        message.add_frame(Self::long_frame(-1)); // TTL: no expiry
        message.add_frame(Self::long_frame(max_idle_ms));

        let response = self.invoke_on_partition(partition_id, message).await?;
        Self::decode_nullable_response(&response)
    }

    /// Associates the specified value with the specified key, with TTL and maximum idle time.
    ///
    /// The entry will be automatically evicted when either:
    /// - The TTL expires (time since creation/update)
    /// - The max idle time expires (time since last access)
    ///
    /// If near-cache is enabled, invalidates the local cache entry before
    /// sending the update to the cluster.
    ///
    /// Returns the previous value associated with the key, or `None` if there was no mapping.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to associate the value with
    /// * `value` - The value to store
    /// * `ttl` - The time-to-live for the entry (use `Duration::ZERO` for no TTL)
    /// * `max_idle` - The maximum idle time for the entry (use `Duration::ZERO` for no max idle)
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Store a cache entry that expires after 1 hour OR if not accessed for 10 minutes
    /// let old = map.put_with_ttl_and_max_idle(
    ///     "cache:user:123".to_string(),
    ///     user_data,
    ///     Duration::from_secs(3600),    // 1 hour TTL
    ///     Duration::from_secs(600),     // 10 minute max idle
    /// ).await?;
    /// ```
    pub async fn put_with_ttl_and_max_idle(
        &self,
        key: K,
        value: V,
        ttl: Duration,
        max_idle: Duration,
    ) -> Result<Option<V>> {
        self.check_permission(PermissionAction::Put)?;
        self.check_quorum(false).await?;
        let key_data = Self::serialize_value(&key)?;
        let value_data = Self::serialize_value(&value)?;

        if let Some(ref cache) = self.near_cache {
            let mut cache_guard = cache.lock().unwrap();
            cache_guard.invalidate(&key_data);
        }

        let partition_id = compute_partition_hash(&key_data);
        let ttl_ms = if ttl.is_zero() { -1 } else { ttl.as_millis() as i64 };
        let max_idle_ms = if max_idle.is_zero() { -1 } else { max_idle.as_millis() as i64 };

        let mut message = ClientMessage::create_for_encode(MAP_PUT, partition_id);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&key_data));
        message.add_frame(Self::data_frame(&value_data));
        message.add_frame(Self::long_frame(ttl_ms));
        message.add_frame(Self::long_frame(max_idle_ms));

        let response = self.invoke_on_partition(partition_id, message).await?;
        Self::decode_nullable_response(&response)
    }

    /// Sets the specified value for the specified key, with TTL and maximum idle time.
    ///
    /// Unlike `put_with_ttl_and_max_idle`, this method does not return the previous value,
    /// making it slightly more efficient when you don't need the old value.
    ///
    /// The entry will be automatically evicted when either:
    /// - The TTL expires (time since creation/update)
    /// - The max idle time expires (time since last access)
    ///
    /// If near-cache is enabled, invalidates the local cache entry before
    /// sending the update to the cluster.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to associate the value with
    /// * `value` - The value to store
    /// * `ttl` - The time-to-live for the entry (use `Duration::ZERO` for no TTL)
    /// * `max_idle` - The maximum idle time for the entry (use `Duration::ZERO` for no max idle)
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Store a cache entry without caring about the old value
    /// map.set_with_ttl_and_max_idle(
    ///     "cache:user:123".to_string(),
    ///     user_data,
    ///     Duration::from_secs(3600),    // 1 hour TTL
    ///     Duration::from_secs(600),     // 10 minute max idle
    /// ).await?;
    /// ```
    pub async fn set_with_ttl_and_max_idle(
        &self,
        key: K,
        value: V,
        ttl: Duration,
        max_idle: Duration,
    ) -> Result<()> {
        self.check_permission(PermissionAction::Put)?;
        self.check_quorum(false).await?;
        let key_data = Self::serialize_value(&key)?;
        let value_data = Self::serialize_value(&value)?;

        if let Some(ref cache) = self.near_cache {
            let mut cache_guard = cache.lock().unwrap();
            cache_guard.invalidate(&key_data);
        }

        let partition_id = compute_partition_hash(&key_data);
        let ttl_ms = if ttl.is_zero() { -1 } else { ttl.as_millis() as i64 };
        let max_idle_ms = if max_idle.is_zero() { -1 } else { max_idle.as_millis() as i64 };

        let mut message = ClientMessage::create_for_encode(MAP_PUT, partition_id);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&key_data));
        message.add_frame(Self::data_frame(&value_data));
        message.add_frame(Self::long_frame(ttl_ms));
        message.add_frame(Self::long_frame(max_idle_ms));

        self.invoke_on_partition(partition_id, message).await?;
        Ok(())
    }

    /// Puts an entry into this map without triggering map store write.
    ///
    /// This is similar to `put`, but the entry will not be written to the
    /// map store (if configured). The entry will only exist in memory and
    /// will be lost if evicted or if the cluster is restarted.
    ///
    /// Useful for caching data that doesn't need to be persisted.
    ///
    /// If near-cache is enabled, invalidates the local cache entry before
    /// the operation.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to associate the value with
    /// * `value` - The value to store
    /// * `ttl` - The time-to-live for the entry (use `Duration::ZERO` for no expiry)
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Cache computed data without persisting
    /// map.put_transient(
    ///     "cache:computed-result".to_string(),
    ///     expensive_computation(),
    ///     Duration::from_secs(300), // 5 minute cache
    /// ).await?;
    /// ```
    pub async fn put_transient(&self, key: K, value: V, ttl: Duration) -> Result<()> {
        self.check_permission(PermissionAction::Put)?;
        self.check_quorum(false).await?;
        let key_data = Self::serialize_value(&key)?;
        let value_data = Self::serialize_value(&value)?;

        if let Some(ref cache) = self.near_cache {
            let mut cache_guard = cache.lock().unwrap();
            cache_guard.invalidate(&key_data);
        }

        let partition_id = compute_partition_hash(&key_data);
        let ttl_ms = if ttl.is_zero() { -1 } else { ttl.as_millis() as i64 };

        let mut message = ClientMessage::create_for_encode(MAP_PUT_TRANSIENT, partition_id);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&key_data));
        message.add_frame(Self::data_frame(&value_data));
        message.add_frame(Self::long_frame(self.thread_id));
        message.add_frame(Self::long_frame(ttl_ms));

        self.invoke_on_partition(partition_id, message).await?;
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

    async fn invoke_on_partition(&self, partition_id: i32, message: ClientMessage) -> Result<ClientMessage> {
        self.connection_manager.invoke_on_partition(partition_id, message).await
    }

    async fn invoke_on_random(&self, message: ClientMessage) -> Result<ClientMessage> {
        self.connection_manager.invoke_on_random(message).await
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

    fn decode_entry_view_response(response: &ClientMessage) -> Result<Option<EntryView<K, V>>> {
        let frames = response.frames();
        if frames.len() < 2 {
            return Ok(None);
        }

        let initial_frame = &frames[0];

        // Check if null response
        if initial_frame.flags & IS_NULL_FLAG != 0 {
            return Ok(None);
        }

        // Decode fixed-size fields from initial frame
        let mut offset = RESPONSE_HEADER_SIZE;
        let content = &initial_frame.content;

        if content.len() < offset + 88 {
            return Ok(None);
        }

        let cost = i64::from_le_bytes(content[offset..offset + 8].try_into().unwrap());
        offset += 8;
        let creation_time = i64::from_le_bytes(content[offset..offset + 8].try_into().unwrap());
        offset += 8;
        let expiration_time = i64::from_le_bytes(content[offset..offset + 8].try_into().unwrap());
        offset += 8;
        let hits = i64::from_le_bytes(content[offset..offset + 8].try_into().unwrap());
        offset += 8;
        let last_access_time = i64::from_le_bytes(content[offset..offset + 8].try_into().unwrap());
        offset += 8;
        let last_stored_time = i64::from_le_bytes(content[offset..offset + 8].try_into().unwrap());
        offset += 8;
        let last_update_time = i64::from_le_bytes(content[offset..offset + 8].try_into().unwrap());
        offset += 8;
        let version = i64::from_le_bytes(content[offset..offset + 8].try_into().unwrap());
        offset += 8;
        let ttl = i64::from_le_bytes(content[offset..offset + 8].try_into().unwrap());
        offset += 8;
        let max_idle = i64::from_le_bytes(content[offset..offset + 8].try_into().unwrap());

        // Decode key from frame 1
        if frames.len() < 3 {
            return Ok(None);
        }

        let key_frame = &frames[1];
        if key_frame.content.is_empty() || key_frame.flags & IS_NULL_FLAG != 0 {
            return Ok(None);
        }
        let mut key_input = ObjectDataInput::new(&key_frame.content);
        let key = K::deserialize(&mut key_input)?;

        // Decode value from frame 2
        let value_frame = &frames[2];
        if value_frame.content.is_empty() || value_frame.flags & IS_NULL_FLAG != 0 {
            return Ok(None);
        }
        let mut value_input = ObjectDataInput::new(&value_frame.content);
        let value = V::deserialize(&mut value_input)?;

        Ok(Some(EntryView {
            key,
            value,
            cost,
            creation_time,
            expiration_time,
            hits,
            last_access_time,
            last_stored_time,
            last_update_time,
            version,
            ttl,
            max_idle,
        }))
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

    fn invocation_uid_frame() -> Frame {
        let counter = INVOCATION_COUNTER.fetch_add(1, Ordering::Relaxed);
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);

        let mut buf = BytesMut::with_capacity(16);
        buf.extend_from_slice(&timestamp.to_le_bytes());
        buf.extend_from_slice(&counter.to_le_bytes());
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
        self.check_permission(PermissionAction::Listen)?;
        let mut message =
            ClientMessage::create_for_encode(MAP_ADD_ENTRY_LISTENER, PARTITION_ID_ANY);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::bool_frame(config.include_value));
        message.add_frame(Self::int_frame(config.event_flags()));
        message.add_frame(Self::bool_frame(false)); // local only = false

        let response = self.invoke_on_random(message).await?;
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

        let response = self.invoke_on_random(message).await?;
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

    /// Returns accumulated client-side statistics for this map.
    ///
    /// These statistics reflect operations performed through this client instance,
    /// including get/put/remove counts, hit/miss ratios, and timing information.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let stats = map.local_stats();
    /// println!("Gets: {}, Puts: {}, Removes: {}",
    ///     stats.get_count(), stats.put_count(), stats.remove_count());
    /// println!("Hit ratio: {:.2}%", stats.hit_ratio() * 100.0);
    /// ```
    pub fn local_stats(&self) -> LocalMapStats {
        let (owned_entry_count, heap_cost) = self.near_cache
            .as_ref()
            .map(|cache| {
                let guard = cache.lock().unwrap();
                let count = guard.size() as u64;
                let cost = count * 64;
                (count, cost)
            })
            .unwrap_or((0, 0));

        self.stats_tracker.snapshot(owned_entry_count, heap_cost)
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
        self.check_permission(PermissionAction::Read)?;
        let predicate_data = predicate.to_predicate_data()?;

        let mut message =
            ClientMessage::create_for_encode(MAP_VALUES_WITH_PREDICATE, PARTITION_ID_ANY);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&predicate_data));

        let response = self.invoke_on_random(message).await?;
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
        self.check_permission(PermissionAction::Read)?;
        let predicate_data = predicate.to_predicate_data()?;

        let mut message =
            ClientMessage::create_for_encode(MAP_KEYS_WITH_PREDICATE, PARTITION_ID_ANY);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&predicate_data));

        let response = self.invoke_on_random(message).await?;
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
        self.check_permission(PermissionAction::Read)?;
        let predicate_data = predicate.to_predicate_data()?;

        let mut message =
            ClientMessage::create_for_encode(MAP_ENTRIES_WITH_PREDICATE, PARTITION_ID_ANY);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&predicate_data));

        let response = self.invoke_on_random(message).await?;
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

    /// Returns an iterator over all keys in this map.
    ///
    /// The iterator fetches keys in batches across all partitions. This is more
    /// memory-efficient than loading all keys at once for large maps.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let mut keys = map.key_set().await?;
    /// while let Some(key) = keys.next().await? {
    ///     println!("Key: {}", key);
    /// }
    /// ```
    pub async fn key_set(&self) -> Result<DistributedIterator<K>> {
        self.key_set_with_config(IteratorConfig::default()).await
    }

    /// Returns an iterator over all keys in this map with custom configuration.
    ///
    /// # Arguments
    ///
    /// * `config` - Configuration for batch size and prefetching behavior
    ///
    /// # Example
    ///
    /// ```ignore
    /// let config = IteratorConfig::new().batch_size(50);
    /// let mut keys = map.key_set_with_config(config).await?;
    /// while let Some(key) = keys.next().await? {
    ///     println!("Key: {}", key);
    /// }
    /// ```
    pub async fn key_set_with_config(&self, config: IteratorConfig) -> Result<DistributedIterator<K>> {
        self.check_permission(PermissionAction::Read)?;
        self.check_quorum(true).await?;

        let partition_count = self.get_partition_count().await.unwrap_or(271);

        Ok(DistributedIterator::new(
            self.name.clone(),
            Arc::clone(&self.connection_manager),
            DistIterationType::Keys,
            partition_count,
            config,
        ))
    }

    /// Returns an iterator over all values in this map.
    ///
    /// The iterator fetches entries in batches and returns only the values.
    /// This is more memory-efficient than loading all values at once for large maps.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let mut values = map.values_iter().await?;
    /// while let Some((_, value)) = values.next_entry().await? {
    ///     println!("Value: {:?}", value);
    /// }
    /// ```
    pub async fn values_iter(&self) -> Result<DistributedIterator<(K, V)>> {
        self.values_iter_with_config(IteratorConfig::default()).await
    }

    /// Returns an iterator over all values in this map with custom configuration.
    pub async fn values_iter_with_config(&self, config: IteratorConfig) -> Result<DistributedIterator<(K, V)>> {
        self.check_permission(PermissionAction::Read)?;
        self.check_quorum(true).await?;

        let partition_count = self.get_partition_count().await.unwrap_or(271);

        Ok(DistributedIterator::new(
            self.name.clone(),
            Arc::clone(&self.connection_manager),
            DistIterationType::Values,
            partition_count,
            config,
        ))
    }

    /// Returns an iterator over all entries in this map.
    ///
    /// The iterator fetches entries in batches across all partitions. This is more
    /// memory-efficient than loading all entries at once for large maps.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let mut entries = map.entry_set().await?;
    /// while let Some((key, value)) = entries.next_entry().await? {
    ///     println!("{}: {:?}", key, value);
    /// }
    /// ```
    pub async fn entry_set(&self) -> Result<DistributedIterator<(K, V)>> {
        self.entry_set_with_config(IteratorConfig::default()).await
    }

    /// Returns an iterator over all entries in this map with custom configuration.
    ///
    /// # Arguments
    ///
    /// * `config` - Configuration for batch size and prefetching behavior
    ///
    /// # Example
    ///
    /// ```ignore
    /// let config = IteratorConfig::new().batch_size(200);
    /// let mut entries = map.entry_set_with_config(config).await?;
    /// while let Some((key, value)) = entries.next_entry().await? {
    ///     println!("{}: {:?}", key, value);
    /// }
    /// ```
    pub async fn entry_set_with_config(&self, config: IteratorConfig) -> Result<DistributedIterator<(K, V)>> {
        self.check_permission(PermissionAction::Read)?;
        self.check_quorum(true).await?;

        let partition_count = self.get_partition_count().await.unwrap_or(271);

        Ok(DistributedIterator::new(
            self.name.clone(),
            Arc::clone(&self.connection_manager),
            DistIterationType::Entries,
            partition_count,
            config,
        ))
    }

    /// Gets the partition count from the cluster.
    async fn get_partition_count(&self) -> Result<i32> {
        Ok(271)
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
        self.check_permission(PermissionAction::Read)?;
        self.check_permission(PermissionAction::Put)?;
        let key_data = Self::serialize_value(key)?;
        let processor_data = Self::serialize_value(processor)?;
        let partition_id = compute_partition_hash(&key_data);

        let mut message = ClientMessage::create_for_encode(MAP_EXECUTE_ON_KEY, partition_id);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&processor_data));
        message.add_frame(Self::data_frame(&key_data));

        let response = self.invoke_on_partition(partition_id, message).await?;
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
        self.check_permission(PermissionAction::Read)?;
        self.check_permission(PermissionAction::Put)?;
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

        let response = self.invoke_on_random(message).await?;
        self.decode_entry_processor_results::<E::Output>(&response)
    }

    /// Executes an entry processor on all entries in this map.
    ///
    /// This is equivalent to [`execute_on_entries`](Self::execute_on_entries).
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
    /// let results = map.execute_on_all(&reset_counter_processor).await?;
    /// println!("Processed {} entries", results.len());
    /// ```
    pub async fn execute_on_all<E>(
        &self,
        processor: &E,
    ) -> Result<EntryProcessorResult<K, E::Output>>
    where
        E: EntryProcessor,
        K: Eq + Hash + Clone,
    {
        self.execute_on_entries(processor).await
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
        self.check_permission(PermissionAction::Read)?;
        self.check_permission(PermissionAction::Put)?;
        let processor_data = Self::serialize_value(processor)?;

        let mut message =
            ClientMessage::create_for_encode(MAP_EXECUTE_ON_ALL_KEYS, PARTITION_ID_ANY);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&processor_data));

        let response = self.invoke_on_random(message).await?;
        self.decode_entry_processor_results::<E::Output>(&response)
    }

    /// Executes an entry processor on entries matching the given predicate.
    ///
    /// Only entries that satisfy the predicate are processed. The processor is sent
    /// to all cluster members, where it executes on matching entries atomically.
    ///
    /// # Type Parameters
    ///
    /// - `E`: The entry processor type implementing [`EntryProcessor`]
    ///
    /// # Arguments
    ///
    /// * `processor` - The entry processor to execute
    /// * `predicate` - The predicate to filter which entries are processed
    ///
    /// # Returns
    ///
    /// A map of keys to their processing results for all matching entries.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use hazelcast_client::query::Predicates;
    ///
    /// // Reset counters only for active users
    /// let predicate = Predicates::equal("status", &"active".to_string())?;
    /// let results = map.execute_on_entries_with_predicate(&reset_processor, &predicate).await?;
    /// println!("Reset {} active entries", results.len());
    /// ```
    pub async fn execute_on_entries_with_predicate<E>(
        &self,
        processor: &E,
        predicate: &dyn Predicate,
    ) -> Result<EntryProcessorResult<K, E::Output>>
    where
        E: EntryProcessor,
        K: Eq + Hash + Clone,
    {
        self.check_permission(PermissionAction::Read)?;
        self.check_permission(PermissionAction::Put)?;
        let processor_data = Self::serialize_value(processor)?;
        let predicate_data = predicate.to_predicate_data()?;

        let mut message =
            ClientMessage::create_for_encode(MAP_EXECUTE_WITH_PREDICATE, PARTITION_ID_ANY);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&processor_data));
        message.add_frame(Self::data_frame(&predicate_data));

        let response = self.invoke_on_random(message).await?;
        self.decode_entry_processor_results::<E::Output>(&response)
    }

    /// Asynchronously executes an entry processor on the entry with the given key.
    ///
    /// This method spawns the operation on a separate task and returns a handle
    /// that can be awaited or polled. Useful for fire-and-forget patterns or
    /// starting multiple operations concurrently.
    ///
    /// # Type Parameters
    ///
    /// - `E`: The entry processor type implementing [`EntryProcessor`]
    ///
    /// # Returns
    ///
    /// A `JoinHandle` that resolves to the result of executing the processor on
    /// the entry, or `None` if the key does not exist and the processor returns
    /// no result.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Start multiple entry processor executions concurrently
    /// let handle1 = map.submit_to_key("user:1".to_string(), increment_processor.clone());
    /// let handle2 = map.submit_to_key("user:2".to_string(), increment_processor.clone());
    ///
    /// // Await results
    /// let result1 = handle1.await??;
    /// let result2 = handle2.await??;
    /// ```
    pub fn submit_to_key<E>(
        &self,
        key: K,
        processor: E,
    ) -> tokio::task::JoinHandle<Result<Option<E::Output>>>
    where
        E: EntryProcessor + 'static,
        E::Output: Send + 'static,
        K: 'static,
        V: 'static,
    {
        let this = self.clone();
        spawn(async move { this.execute_on_key(&key, &processor).await })
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
    /// - [`IndexType::Bitmap`]: Best for low-cardinality attributes. Optimized
    ///   for equality and IN queries on attributes with few distinct values.
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
    ///
    /// // Create a bitmap index for low-cardinality attribute
    /// use hazelcast_client::query::{BitmapIndexOptions, UniqueKeyTransformation};
    ///
    /// let bitmap_config = IndexConfig::builder()
    ///     .name("status-bitmap-idx")
    ///     .add_attribute("status")
    ///     .bitmap_options(
    ///         BitmapIndexOptions::builder()
    ///             .unique_key("__key")
    ///             .unique_key_transformation(UniqueKeyTransformation::Long)
    ///             .build()
    ///     )
    ///     .build();
    ///
    /// map.add_index(bitmap_config).await?;
    /// ```
    pub async fn add_index(&self, config: IndexConfig) -> Result<()> {
        self.check_permission(PermissionAction::Index)?;
        let mut message = ClientMessage::create_for_encode(MAP_ADD_INDEX, PARTITION_ID_ANY);
        message.add_frame(Self::string_frame(&self.name));

        // Encode index config as nested structure
        // Index type
        message.add_frame(Self::int_frame(config.index_type.as_i32()));

        // Index name (nullable)
        if let Some(ref name) = config.name {
            message.add_frame(Self::string_frame(name));
        } else {
            message.add_frame(Frame::new_null_frame());
        }

        // Attributes count
        message.add_frame(Self::int_frame(config.attributes.len() as i32));

        // Encode each attribute
        for attr in &config.attributes {
            message.add_frame(Self::string_frame(attr));
        }

        // Encode bitmap index options if present
        if let Some(ref bitmap_opts) = config.bitmap_options {
            message.add_frame(Self::bool_frame(true)); // has bitmap options
            message.add_frame(Self::string_frame(bitmap_opts.unique_key()));
            message.add_frame(Self::int_frame(bitmap_opts.unique_key_transformation().as_i32()));
        } else {
            message.add_frame(Self::bool_frame(false)); // no bitmap options
        }

        self.invoke_on_random(message).await?;
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
        self.check_permission(PermissionAction::Read)?;
        let aggregator_data = aggregator.to_aggregator_data()?;

        let mut message = ClientMessage::create_for_encode(MAP_AGGREGATE, PARTITION_ID_ANY);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&aggregator_data));

        let response = self.invoke_on_random(message).await?;
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
        self.check_permission(PermissionAction::Read)?;
        let aggregator_data = aggregator.to_aggregator_data()?;
        let predicate_data = predicate.to_predicate_data()?;

        let mut message =
            ClientMessage::create_for_encode(MAP_AGGREGATE_WITH_PREDICATE, PARTITION_ID_ANY);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&aggregator_data));
        message.add_frame(Self::data_frame(&predicate_data));

        let response = self.invoke_on_random(message).await?;
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
        self.check_permission(PermissionAction::Read)?;
        let projection_data = projection.to_projection_data()?;

        let mut message = ClientMessage::create_for_encode(MAP_PROJECT, PARTITION_ID_ANY);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&projection_data));

        let response = self.invoke_on_random(message).await?;
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
        self.check_permission(PermissionAction::Read)?;
        let projection_data = projection.to_projection_data()?;
        let predicate_data = predicate.to_predicate_data()?;

        let mut message =
            ClientMessage::create_for_encode(MAP_PROJECT_WITH_PREDICATE, PARTITION_ID_ANY);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&projection_data));
        message.add_frame(Self::data_frame(&predicate_data));

        let response = self.invoke_on_random(message).await?;
        Self::decode_projection_response::<P::Output>(&response)
    }

    /// Returns all values matching the given paging predicate.
    ///
    /// This method retrieves a single page of results and updates the predicate's
    /// anchor list for subsequent page navigation.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use hazelcast_client::query::PagingPredicate;
    ///
    /// let mut paging: PagingPredicate<String, User> = PagingPredicate::new(25);
    ///
    /// // Fetch first page
    /// let result = map.values_with_paging_predicate(&mut paging).await?;
    /// println!("Page {}: {} items", result.page, result.len());
    ///
    /// // Fetch next page
    /// paging.next_page();
    /// let result = map.values_with_paging_predicate(&mut paging).await?;
    /// ```
    pub async fn values_with_paging_predicate(
        &self,
        predicate: &mut PagingPredicate<K, V>,
    ) -> Result<PagingResult<V>> {
        self.check_permission(PermissionAction::Read)?;
        predicate.set_iteration_type(IterationType::Value);
        let predicate_data = predicate.to_predicate_data()?;

        let mut message =
            ClientMessage::create_for_encode(MAP_VALUES_WITH_PAGING_PREDICATE, PARTITION_ID_ANY);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&predicate_data));

        let response = self.invoke_on_random(message).await?;
        let (values, anchors) = Self::decode_paging_values_response::<V>(&response)?;

        let page = predicate.get_page();
        predicate.update_anchors(anchors.clone());

        Ok(PagingResult::new(values, page, anchors))
    }

    /// Returns all keys matching the given paging predicate.
    ///
    /// This method retrieves a single page of results and updates the predicate's
    /// anchor list for subsequent page navigation.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use hazelcast_client::query::PagingPredicate;
    ///
    /// let mut paging: PagingPredicate<String, User> = PagingPredicate::new(25);
    ///
    /// // Fetch first page of keys
    /// let result = map.keys_with_paging_predicate(&mut paging).await?;
    /// for key in &result.data {
    ///     println!("Key: {}", key);
    /// }
    /// ```
    pub async fn keys_with_paging_predicate(
        &self,
        predicate: &mut PagingPredicate<K, V>,
    ) -> Result<PagingResult<K>> {
        self.check_permission(PermissionAction::Read)?;
        predicate.set_iteration_type(IterationType::Key);
        let predicate_data = predicate.to_predicate_data()?;

        let mut message =
            ClientMessage::create_for_encode(MAP_KEYS_WITH_PAGING_PREDICATE, PARTITION_ID_ANY);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&predicate_data));

        let response = self.invoke_on_random(message).await?;
        let (keys, anchors) = Self::decode_paging_values_response::<K>(&response)?;

        let page = predicate.get_page();
        predicate.update_anchors(anchors.clone());

        Ok(PagingResult::new(keys, page, anchors))
    }

    /// Returns all entries matching the given paging predicate.
    ///
    /// This method retrieves a single page of results and updates the predicate's
    /// anchor list for subsequent page navigation.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use hazelcast_client::query::PagingPredicate;
    ///
    /// let mut paging: PagingPredicate<String, User> = PagingPredicate::new(25);
    ///
    /// // Fetch pages until empty
    /// loop {
    ///     let result = map.entries_with_paging_predicate(&mut paging).await?;
    ///     if result.is_empty() {
    ///         break;
    ///     }
    ///     for (key, value) in &result.data {
    ///         println!("{}: {:?}", key, value);
    ///     }
    ///     paging.next_page();
    /// }
    /// ```
    pub async fn entries_with_paging_predicate(
        &self,
        predicate: &mut PagingPredicate<K, V>,
    ) -> Result<PagingResult<(K, V)>> {
        self.check_permission(PermissionAction::Read)?;
        predicate.set_iteration_type(IterationType::Entry);
        let predicate_data = predicate.to_predicate_data()?;

        let mut message =
            ClientMessage::create_for_encode(MAP_ENTRIES_WITH_PAGING_PREDICATE, PARTITION_ID_ANY);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&predicate_data));

        let response = self.invoke_on_random(message).await?;
        let (entries, anchors) = Self::decode_paging_entries_response(&response)?;

        let page = predicate.get_page();
        predicate.update_anchors(anchors.clone());

        Ok(PagingResult::new(entries, page, anchors))
    }

    fn decode_paging_values_response<T: Deserializable>(
        response: &ClientMessage,
    ) -> Result<(Vec<T>, Vec<AnchorEntry>)> {
        let frames = response.frames();
        let mut values = Vec::new();
        let mut anchors = Vec::new();
        let mut frame_idx = 1;

        // Read anchor count from initial frame if present
        let anchor_count = if !frames.is_empty() && frames[0].content.len() >= RESPONSE_HEADER_SIZE + 4 {
            let offset = RESPONSE_HEADER_SIZE;
            i32::from_le_bytes([
                frames[0].content[offset],
                frames[0].content[offset + 1],
                frames[0].content[offset + 2],
                frames[0].content[offset + 3],
            ]) as usize
        } else {
            0
        };

        // Read anchors
        for _ in 0..anchor_count {
            if frame_idx + 2 >= frames.len() {
                break;
            }

            let page_frame = &frames[frame_idx];
            let page = if page_frame.content.len() >= 4 {
                i32::from_le_bytes([
                    page_frame.content[0],
                    page_frame.content[1],
                    page_frame.content[2],
                    page_frame.content[3],
                ]) as usize
            } else {
                0
            };
            frame_idx += 1;

            let key_frame = &frames[frame_idx];
            let key_data = if key_frame.flags & IS_NULL_FLAG == 0 && !key_frame.content.is_empty() {
                Some(key_frame.content.to_vec())
            } else {
                None
            };
            frame_idx += 1;

            let value_frame = &frames[frame_idx];
            let value_data = if value_frame.flags & IS_NULL_FLAG == 0 && !value_frame.content.is_empty() {
                Some(value_frame.content.to_vec())
            } else {
                None
            };
            frame_idx += 1;

            anchors.push(AnchorEntry::new(page, key_data, value_data));
        }

        // Read values
        while frame_idx < frames.len() {
            let frame = &frames[frame_idx];
            frame_idx += 1;

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

        Ok((values, anchors))
    }

    fn decode_paging_entries_response(
        response: &ClientMessage,
    ) -> Result<(Vec<(K, V)>, Vec<AnchorEntry>)> {
        let frames = response.frames();
        let mut entries = Vec::new();
        let mut anchors = Vec::new();
        let mut frame_idx = 1;

        // Read anchor count from initial frame if present
        let anchor_count = if !frames.is_empty() && frames[0].content.len() >= RESPONSE_HEADER_SIZE + 4 {
            let offset = RESPONSE_HEADER_SIZE;
            i32::from_le_bytes([
                frames[0].content[offset],
                frames[0].content[offset + 1],
                frames[0].content[offset + 2],
                frames[0].content[offset + 3],
            ]) as usize
        } else {
            0
        };

        // Read anchors
        for _ in 0..anchor_count {
            if frame_idx + 2 >= frames.len() {
                break;
            }

            let page_frame = &frames[frame_idx];
            let page = if page_frame.content.len() >= 4 {
                i32::from_le_bytes([
                    page_frame.content[0],
                    page_frame.content[1],
                    page_frame.content[2],
                    page_frame.content[3],
                ]) as usize
            } else {
                0
            };
            frame_idx += 1;

            let key_frame = &frames[frame_idx];
            let key_data = if key_frame.flags & IS_NULL_FLAG == 0 && !key_frame.content.is_empty() {
                Some(key_frame.content.to_vec())
            } else {
                None
            };
            frame_idx += 1;

            let value_frame = &frames[frame_idx];
            let value_data = if value_frame.flags & IS_NULL_FLAG == 0 && !value_frame.content.is_empty() {
                Some(value_frame.content.to_vec())
            } else {
                None
            };
            frame_idx += 1;

            anchors.push(AnchorEntry::new(page, key_data, value_data));
        }

        // Read entries (key-value pairs)
        while frame_idx + 1 < frames.len() {
            let key_frame = &frames[frame_idx];
            let value_frame = &frames[frame_idx + 1];
            frame_idx += 2;

            if key_frame.flags & END_FLAG != 0 && key_frame.content.is_empty() {
                break;
            }

            if key_frame.flags & IS_NULL_FLAG != 0 || key_frame.content.is_empty() {
                continue;
            }

            if value_frame.flags & IS_NULL_FLAG != 0 || value_frame.content.is_empty() {
                continue;
            }

            let mut key_input = ObjectDataInput::new(&key_frame.content);
            let mut value_input = ObjectDataInput::new(&value_frame.content);

            if let (Ok(key), Ok(value)) = (K::deserialize(&mut key_input), V::deserialize(&mut value_input)) {
                entries.push((key, value));
            }
        }

        Ok((entries, anchors))
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

    /// Starts automatic near-cache invalidation by registering a server-side entry listener.
    ///
    /// When entries are updated, removed, evicted, or expired on the cluster,
    /// the corresponding entries in the local near-cache are automatically invalidated.
    /// This ensures the near-cache stays consistent with the cluster state even when
    /// other clients modify the data.
    ///
    /// This method is idempotent - calling it multiple times has no additional effect.
    /// Returns `Ok(())` if invalidation was started, was already running, or if
    /// no near-cache is configured.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let config = NearCacheConfig::builder("my-map").build().unwrap();
    /// let map: IMap<String, String> = client.get_map_with_near_cache("my-map", config).await?;
    /// map.start_near_cache_invalidation().await?;
    /// // Now the near-cache will auto-invalidate when cluster data changes
    /// ```
    pub async fn start_near_cache_invalidation(&self) -> Result<()>
    where
        K: 'static,
        V: 'static,
    {
        let near_cache = match &self.near_cache {
            Some(cache) => Arc::clone(cache),
            None => return Ok(()),
        };

        {
            let reg_guard = self.invalidation_registration.lock().unwrap();
            if reg_guard.is_some() {
                return Ok(());
            }
        }

        let config = EntryListenerConfig::new()
            .include_value(false)
            .on_updated()
            .on_removed()
            .on_evicted()
            .on_expired();

        let registration = self.add_entry_listener(config, move |event: EntryEvent<K, V>| {
            let mut output = ObjectDataOutput::new();
            if event.key.serialize(&mut output).is_ok() {
                let key_data = output.into_bytes();
                let mut cache_guard = near_cache.lock().unwrap();
                cache_guard.invalidate(&key_data);
            }
        }).await?;

        let mut reg_guard = self.invalidation_registration.lock().unwrap();
        *reg_guard = Some(registration);

        Ok(())
    }

    /// Stops automatic near-cache invalidation by removing the server-side entry listener.
    ///
    /// Returns `Ok(true)` if the listener was successfully removed, `Ok(false)` if
    /// no invalidation listener was active.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Stop automatic invalidation
    /// map.stop_near_cache_invalidation().await?;
    /// ```
    pub async fn stop_near_cache_invalidation(&self) -> Result<bool> {
        let registration = {
            let mut reg_guard = self.invalidation_registration.lock().unwrap();
            reg_guard.take()
        };

        match registration {
            Some(reg) => self.remove_entry_listener(&reg).await,
            None => Ok(false),
        }
    }

    /// Returns `true` if automatic near-cache invalidation is currently active.
    ///
    /// # Example
    ///
    /// ```ignore
    /// if !map.is_near_cache_invalidation_active() {
    ///     map.start_near_cache_invalidation().await?;
    /// }
    /// ```
    pub fn is_near_cache_invalidation_active(&self) -> bool {
        self.invalidation_registration.lock().unwrap().is_some()
    }

    /// Returns a continuous query cache for this map.
    ///
    /// The query cache maintains a local view of entries matching the given predicate.
    /// It subscribes to entry events and automatically updates when matching entries
    /// change in the map.
    ///
    /// # Arguments
    ///
    /// * `name` - A unique name for this query cache
    /// * `predicate` - The predicate to filter which entries are cached
    /// * `include_value` - Whether to include values in the cache
    ///
    /// # Example
    ///
    /// ```ignore
    /// use hazelcast_client::query::Predicates;
    ///
    /// let predicate = Predicates::equal("status", &"active".to_string())?;
    /// let cache = map.get_query_cache("active-users", &predicate, true).await?;
    ///
    /// // Fast local reads
    /// if let Some(user) = cache.get(&"user123".to_string()) {
    ///     println!("User: {:?}", user);
    /// }
    /// ```
    pub async fn get_query_cache(
        &self,
        name: &str,
        predicate: &dyn Predicate,
        include_value: bool,
    ) -> Result<crate::cache::QueryCache<K, V>>
    where
        K: Eq + Hash + Clone + 'static,
        V: Clone + 'static,
    {
        use crate::cache::QueryCache;

        let query_cache = QueryCache::new(name.to_string(), self.name.clone(), include_value);

        let initial_entries = self.entries_with_predicate(predicate).await?;
        query_cache.populate(initial_entries);

        let cache_handle = query_cache.cache_handle();
        let stats_handle = query_cache.stats_handle();

        let config = EntryListenerConfig::new()
            .include_value(include_value)
            .on_added()
            .on_updated()
            .on_removed()
            .on_evicted()
            .on_expired();

        let registration = self
            .add_entry_listener_with_predicate(config, predicate, move |event: EntryEvent<K, V>| {
                stats_handle.record_event();

                let key_data = {
                    let mut output = ObjectDataOutput::new();
                    if event.key.serialize(&mut output).is_err() {
                        return;
                    }
                    output.into_bytes()
                };

                let mut cache = cache_handle.write().unwrap();

                match event.event_type {
                    EntryEventType::Added | EntryEventType::Updated => {
                        if let Some(ref value) = event.new_value {
                            let mut output = ObjectDataOutput::new();
                            if value.serialize(&mut output).is_ok() {
                                cache.insert(key_data, output.into_bytes());
                            }
                        }
                    }
                    EntryEventType::Removed
                    | EntryEventType::Evicted
                    | EntryEventType::Expired => {
                        cache.remove(&key_data);
                    }
                    _ => {}
                }
            })
            .await?;

        query_cache.set_listener_registration(registration);

        Ok(query_cache)
    }

    /// Adds an entry listener to this map using the [`EntryListener`] trait.
    ///
    /// Events are dispatched to the appropriate trait method based on event type.
    /// Returns a registration that can be used to remove the listener.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use std::sync::Arc;
    ///
    /// struct MyListener;
    /// impl EntryListener<String, i32> for MyListener {
    ///     fn entry_added(&self, event: EntryEvent<String, i32>) {
    ///         println!("Added: {}", event.key);
    ///     }
    /// }
    ///
    /// let listener = Arc::new(MyListener);
    /// let registration = map.add_entry_listener_obj(
    ///     EntryListenerConfig::all(),
    ///     listener,
    /// ).await?;
    /// ```
    pub async fn add_entry_listener_obj(
        &self,
        config: EntryListenerConfig,
        listener: BoxedEntryListener<K, V>,
    ) -> Result<ListenerRegistration>
    where
        K: 'static,
        V: 'static,
    {
        self.check_permission(PermissionAction::Listen)?;
        let mut message =
            ClientMessage::create_for_encode(MAP_ADD_ENTRY_LISTENER, PARTITION_ID_ANY);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::bool_frame(config.include_value));
        message.add_frame(Self::int_frame(config.event_flags()));
        message.add_frame(Self::bool_frame(false));

        let response = self.invoke_on_random(message).await?;
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
    /// Returns a registration that can be used to remove the listener.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use hazelcast_client::query::Predicates;
    ///
    /// let predicate = Predicates::greater_than("age", &18i32)?;
    /// let registration = map.add_entry_listener_with_predicate(
    ///     EntryListenerConfig::all(),
    ///     &predicate,
    ///     |event| println!("Adult entry changed: {}", event.key),
    /// ).await?;
    /// ```
    pub async fn add_entry_listener_with_predicate<F>(
        &self,
        config: EntryListenerConfig,
        predicate: &dyn Predicate,
        handler: F,
    ) -> Result<ListenerRegistration>
    where
        F: Fn(EntryEvent<K, V>) + Send + Sync + 'static,
        K: 'static,
        V: 'static,
    {
        self.check_permission(PermissionAction::Listen)?;
        let predicate_data = predicate.to_predicate_data()?;

        let mut message =
            ClientMessage::create_for_encode(MAP_ADD_ENTRY_LISTENER_WITH_PREDICATE, PARTITION_ID_ANY);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::bool_frame(config.include_value));
        message.add_frame(Self::int_frame(config.event_flags()));
        message.add_frame(Self::bool_frame(false));
        message.add_frame(Self::data_frame(&predicate_data));

        let response = self.invoke_on_random(message).await?;
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

    /// Adds an entry listener with a predicate filter using the [`EntryListener`] trait.
    ///
    /// Only entries matching the predicate will trigger events.
    /// Events are dispatched to the appropriate trait method based on event type.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use std::sync::Arc;
    /// use hazelcast_client::query::Predicates;
    ///
    /// let predicate = Predicates::greater_than("age", &18i32)?;
    /// let listener = Arc::new(MyListener);
    /// let registration = map.add_entry_listener_with_predicate_obj(
    ///     EntryListenerConfig::all(),
    ///     &predicate,
    ///     listener,
    /// ).await?;
    /// ```
    pub async fn add_entry_listener_with_predicate_obj(
        &self,
        config: EntryListenerConfig,
        predicate: &dyn Predicate,
        listener: BoxedEntryListener<K, V>,
    ) -> Result<ListenerRegistration>
    where
        K: 'static,
        V: 'static,
    {
        self.check_permission(PermissionAction::Listen)?;
        let predicate_data = predicate.to_predicate_data()?;

        let mut message =
            ClientMessage::create_for_encode(MAP_ADD_ENTRY_LISTENER_WITH_PREDICATE, PARTITION_ID_ANY);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::bool_frame(config.include_value));
        message.add_frame(Self::int_frame(config.event_flags()));
        message.add_frame(Self::bool_frame(false));
        message.add_frame(Self::data_frame(&predicate_data));

        let response = self.invoke_on_random(message).await?;
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

    /// Adds a partition lost listener to this map.
    ///
    /// The listener will be notified when a partition containing data for this map
    /// is lost due to member failures. This can happen when all replicas of a
    /// partition become unavailable.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let registration = map.add_partition_lost_listener(|event| {
    ///     println!("Partition {} lost!", event.partition_id());
    /// }).await?;
    /// ```
    pub async fn add_partition_lost_listener<F>(
        &self,
        handler: F,
    ) -> Result<ListenerRegistration>
    where
        F: Fn(MapPartitionLostEvent) + Send + Sync + 'static,
        K: 'static,
        V: 'static,
    {
        self.check_permission(PermissionAction::Listen)?;

        let mut message =
            ClientMessage::create_for_encode(MAP_ADD_PARTITION_LOST_LISTENER, PARTITION_ID_ANY);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::bool_frame(false)); // local only = false

        let response = self.invoke_on_random(message).await?;
        let listener_uuid = Self::decode_uuid_response(&response)?;

        let registration = ListenerRegistration::new(ListenerId::from_uuid(listener_uuid));
        let active_flag = registration.active_flag();
        let shutdown_rx = registration.shutdown_receiver();

        let connection_manager = Arc::clone(&self.connection_manager);
        let listener_stats = Arc::clone(&self.listener_stats);
        let handler = Arc::new(handler);
        let map_name = self.name.clone();

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
                                    if Self::is_partition_lost_event(&msg, &map_name) {
                                        listener_stats.record_message();
                                        if let Ok(event) = Self::decode_partition_lost_event(&msg) {
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

    /// Adds a partition lost listener using the [`MapPartitionLostListener`] trait.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use std::sync::Arc;
    ///
    /// struct MyListener;
    /// impl MapPartitionLostListener for MyListener {
    ///     fn map_partition_lost(&self, event: MapPartitionLostEvent) {
    ///         println!("Partition {} lost!", event.partition_id());
    ///     }
    /// }
    ///
    /// let listener = Arc::new(MyListener);
    /// let registration = map.add_partition_lost_listener_obj(listener).await?;
    /// ```
    pub async fn add_partition_lost_listener_obj(
        &self,
        listener: BoxedMapPartitionLostListener,
    ) -> Result<ListenerRegistration>
    where
        K: 'static,
        V: 'static,
    {
        self.check_permission(PermissionAction::Listen)?;

        let mut message =
            ClientMessage::create_for_encode(MAP_ADD_PARTITION_LOST_LISTENER, PARTITION_ID_ANY);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::bool_frame(false)); // local only = false

        let response = self.invoke_on_random(message).await?;
        let listener_uuid = Self::decode_uuid_response(&response)?;

        let registration = ListenerRegistration::new(ListenerId::from_uuid(listener_uuid));
        let active_flag = registration.active_flag();
        let shutdown_rx = registration.shutdown_receiver();

        let connection_manager = Arc::clone(&self.connection_manager);
        let listener_stats = Arc::clone(&self.listener_stats);
        let map_name = self.name.clone();

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
                                    if Self::is_partition_lost_event(&msg, &map_name) {
                                        listener_stats.record_message();
                                        match Self::decode_partition_lost_event(&msg) {
                                            Ok(event) => {
                                                listener.map_partition_lost(event);
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

    /// Removes a partition lost listener from this map.
    ///
    /// Returns `true` if the listener was successfully removed, `false` if it was not found.
    pub async fn remove_partition_lost_listener(
        &self,
        registration: &ListenerRegistration,
    ) -> Result<bool> {
        registration.deactivate();

        let mut message =
            ClientMessage::create_for_encode(MAP_REMOVE_PARTITION_LOST_LISTENER, PARTITION_ID_ANY);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::uuid_frame(registration.id().as_uuid()));

        let response = self.invoke_on_random(message).await?;
        Self::decode_bool_response(&response)
    }

    fn is_partition_lost_event(message: &ClientMessage, map_name: &str) -> bool {
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

    fn decode_partition_lost_event(message: &ClientMessage) -> Result<MapPartitionLostEvent> {
        let frames = message.frames();
        if frames.is_empty() {
            return Err(HazelcastError::Serialization(
                "empty partition lost event".to_string(),
            ));
        }

        let initial_frame = &frames[0];
        let mut offset = RESPONSE_HEADER_SIZE;

        let partition_id = if initial_frame.content.len() >= offset + 4 {
            let id = i32::from_le_bytes([
                initial_frame.content[offset],
                initial_frame.content[offset + 1],
                initial_frame.content[offset + 2],
                initial_frame.content[offset + 3],
            ]);
            offset += 4;
            id
        } else {
            return Err(HazelcastError::Serialization(
                "missing partition_id in event".to_string(),
            ));
        };

        let member_uuid = if initial_frame.content.len() >= offset + 16 {
            let uuid_bytes: [u8; 16] = initial_frame.content[offset..offset + 16]
                .try_into()
                .map_err(|_| {
                    HazelcastError::Serialization("invalid UUID bytes".to_string())
                })?;
            Uuid::from_bytes(uuid_bytes)
        } else {
            Uuid::nil()
        };

        Ok(MapPartitionLostEvent::new(partition_id, member_uuid))
    }

    /// Adds an interceptor to this map.
    ///
    /// The interceptor will be invoked on the cluster members for all map operations.
    /// Interceptors can modify values on get, put, and observe remove operations.
    ///
    /// # Arguments
    ///
    /// * `interceptor` - The interceptor to add
    ///
    /// # Returns
    ///
    /// A unique identifier for the interceptor that can be used to remove it later.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let interceptor_id = map.add_interceptor(&my_interceptor).await?;
    /// println!("Interceptor registered with ID: {}", interceptor_id);
    /// ```
    pub async fn add_interceptor<I>(&self, interceptor: &I) -> Result<String>
    where
        I: MapInterceptor,
    {
        self.check_permission(PermissionAction::Put)?;
        let interceptor_data = Self::serialize_value(interceptor)?;

        let mut message = ClientMessage::create_for_encode(MAP_ADD_INTERCEPTOR, PARTITION_ID_ANY);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&interceptor_data));

        let response = self.invoke_on_random(message).await?;
        Self::decode_string_response(&response)
    }

    /// Triggers loading of all keys from the configured MapLoader.
    ///
    /// If the map has a `MapLoader` configured on the server, this method
    /// triggers the loader to populate entries for all known keys. The actual
    /// loading is performed on the server side.
    ///
    /// # Arguments
    ///
    /// * `replace_existing` - If `true`, existing entries in the map will be
    ///   replaced with values from the MapLoader. If `false`, only missing
    ///   entries will be loaded.
    ///
    /// # Note
    ///
    /// This is a server-side operation. The client does not need to implement
    /// `MapLoader`; it only triggers the loading operation on the cluster.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Load all keys, replacing existing entries
    /// map.load_all_keys(true).await?;
    ///
    /// // Load all keys, keeping existing entries
    /// map.load_all_keys(false).await?;
    /// ```
    pub async fn load_all_keys(&self, replace_existing: bool) -> Result<()> {
        self.check_permission(PermissionAction::Put)?;
        self.check_quorum(false).await?;

        if replace_existing {
            if let Some(ref cache) = self.near_cache {
                let mut cache_guard = cache.lock().unwrap();
                cache_guard.clear();
            }
        }

        let mut message = ClientMessage::create_for_encode(MAP_LOAD_ALL, PARTITION_ID_ANY);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::bool_frame(replace_existing));

        self.invoke_on_random(message).await?;
        Ok(())
    }

    /// Reads events from the Event Journal for this map.
    ///
    /// The Event Journal is a ring buffer that stores a history of mutations on map entries.
    /// This method returns an async stream that yields events as they are read from the journal.
    ///
    /// # Arguments
    ///
    /// * `partition_id` - The partition to read events from
    /// * `config` - Configuration for reading from the journal
    ///
    /// # Returns
    ///
    /// An async stream of `EventJournalMapEvent<K, V>` representing mutations.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use futures::StreamExt;
    ///
    /// let config = EventJournalConfig::new()
    ///     .start_sequence(-1)
    ///     .max_size(100);
    ///
    /// let mut stream = map.read_from_event_journal(0, config).await?;
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
        config: EventJournalConfig,
    ) -> Result<EventJournalStream<K, V>>
    where
        K: 'static,
        V: 'static,
    {
        self.check_permission(PermissionAction::Read)?;

        // Subscribe to get initial sequence info
        let (oldest_sequence, newest_sequence) = self.subscribe_to_event_journal(partition_id).await?;

        let start_sequence = if config.start_sequence < 0 {
            oldest_sequence
        } else {
            config.start_sequence.max(oldest_sequence)
        };

        let (tx, rx) = mpsc::channel(config.max_size as usize);

        let connection_manager = Arc::clone(&self.connection_manager);
        let map_name = self.name.clone();
        let min_size = config.min_size;
        let max_size = config.max_size;

        spawn(async move {
            let mut current_sequence = start_sequence;

            loop {
                match Self::read_journal_batch(
                    &connection_manager,
                    &map_name,
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

        Ok(EventJournalStream { receiver: rx })
    }

    async fn subscribe_to_event_journal(&self, partition_id: i32) -> Result<(i64, i64)> {
        let mut message =
            ClientMessage::create_for_encode(MAP_EVENT_JOURNAL_SUBSCRIBE, partition_id);
        message.add_frame(Self::string_frame(&self.name));

        let response = self.invoke_on_partition(partition_id, message).await?;
        Self::decode_event_journal_subscribe_response(&response)
    }

    async fn read_journal_batch(
        connection_manager: &ConnectionManager,
        map_name: &str,
        partition_id: i32,
        start_sequence: i64,
        min_size: i32,
        max_size: i32,
    ) -> Result<Vec<EventJournalMapEvent<K, V>>> {
        let mut message = ClientMessage::create_for_encode(MAP_EVENT_JOURNAL_READ, partition_id);
        message.add_frame(Self::string_frame(map_name));
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
    ) -> Result<Vec<EventJournalMapEvent<K, V>>> {
        let frames = response.frames();
        if frames.is_empty() {
            return Ok(Vec::new());
        }

        let initial_frame = &frames[0];
        let mut offset = RESPONSE_HEADER_SIZE;

        // Read count of events
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

        // Read next sequence
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

            // Read event type
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
                EventJournalEventType::from_value(event_type_value).unwrap_or(EventJournalEventType::Added);

            // Read sequence
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

            // Read key
            let key_frame = &frames[frame_idx];
            let key = if !key_frame.content.is_empty() && key_frame.flags & IS_NULL_FLAG == 0 {
                let mut input = ObjectDataInput::new(&key_frame.content);
                K::deserialize(&mut input)?
            } else {
                frame_idx += 3;
                continue;
            };
            frame_idx += 1;

            // Read old value
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

            // Read new value
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

            events.push(EventJournalMapEvent {
                event_type,
                key,
                old_value,
                new_value,
                sequence,
            });
        }

        Ok(events)
    }

    /// Triggers loading of specific keys from the configured MapLoader.
    ///
    /// If the map has a `MapLoader` configured on the server, this method
    /// triggers the loader to populate entries for the specified keys. The
    /// actual loading is performed on the server side.
    ///
    /// # Arguments
    ///
    /// * `keys` - The keys to load from the MapLoader
    /// * `replace_existing` - If `true`, existing entries in the map will be
    ///   replaced with values from the MapLoader. If `false`, only missing
    ///   entries will be loaded.
    ///
    /// # Note
    ///
    /// This is a server-side operation. The client does not need to implement
    /// `MapLoader`; it only triggers the loading operation on the cluster.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let keys = vec!["user:1".to_string(), "user:2".to_string()];
    ///
    /// // Load specific keys, replacing existing entries
    /// map.load_all(&keys, true).await?;
    ///
    /// // Load specific keys, keeping existing entries
    /// map.load_all(&keys, false).await?;
    /// ```
    pub async fn load_all(&self, keys: &[K], replace_existing: bool) -> Result<()> {
        self.check_permission(PermissionAction::Put)?;
        self.check_quorum(false).await?;

        if keys.is_empty() {
            return Ok(());
        }

        if replace_existing {
            if let Some(ref cache) = self.near_cache {
                let mut cache_guard = cache.lock().unwrap();
                for key in keys {
                    if let Ok(key_data) = Self::serialize_value(key) {
                        cache_guard.invalidate(&key_data);
                    }
                }
            }
        }

        let mut message = ClientMessage::create_for_encode(MAP_LOAD_GIVEN_KEYS, PARTITION_ID_ANY);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::bool_frame(replace_existing));
        message.add_frame(Self::int_frame(keys.len() as i32));

        for key in keys {
            let key_data = Self::serialize_value(key)?;
            message.add_frame(Self::data_frame(&key_data));
        }

        self.invoke_on_random(message).await?;
        Ok(())
    }

    /// Removes an interceptor from this map.
    ///
    /// # Arguments
    ///
    /// * `id` - The identifier returned by `add_interceptor`
    ///
    /// # Returns
    ///
    /// `true` if the interceptor was found and removed, `false` otherwise.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let removed = map.remove_interceptor(&interceptor_id).await?;
    /// if removed {
    ///     println!("Interceptor removed");
    /// }
    /// ```
    pub async fn remove_interceptor(&self, id: &str) -> Result<bool> {
        self.check_permission(PermissionAction::Put)?;

        let mut message = ClientMessage::create_for_encode(MAP_REMOVE_INTERCEPTOR, PARTITION_ID_ANY);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::string_frame(id));

        let response = self.invoke_on_random(message).await?;
        Self::decode_bool_response(&response)
    }

    fn decode_string_response(response: &ClientMessage) -> Result<String> {
        let frames = response.frames();
        if frames.len() < 2 {
            return Err(HazelcastError::Serialization(
                "empty string response".to_string(),
            ));
        }

        let data_frame = &frames[1];
        if data_frame.content.is_empty() {
            return Ok(String::new());
        }

        String::from_utf8(data_frame.content.to_vec()).map_err(|e| {
            HazelcastError::Serialization(format!("invalid UTF-8 in response: {}", e))
        })
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
            stats_tracker: Arc::clone(&self.stats_tracker),
            near_cache: self.near_cache.as_ref().map(Arc::clone),
            invalidation_registration: Arc::clone(&self.invalidation_registration),
            thread_id: self.thread_id,
            _phantom: PhantomData,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::EvictionPolicy;
    use std::time::Duration;

    /// Helper to create a ConnectionManager from a socket address for tests.
    fn make_cm_from_addr(addr: std::net::SocketAddr) -> crate::connection::ConnectionManager {
        let config = crate::config::ClientConfigBuilder::new()
            .add_address(addr)
            .build()
            .unwrap();
        crate::connection::ConnectionManager::from_config(config)
    }

    #[test]
    fn test_imap_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<IMap<String, String>>();
    }

    #[tokio::test]
    async fn test_map_permission_denied_read() {
        use crate::config::{ClientConfigBuilder, Permissions, PermissionAction};
        use crate::connection::ConnectionManager;

        let mut perms = Permissions::new();
        perms.grant(PermissionAction::Put);

        let config = ClientConfigBuilder::new()
            .security(|s| s.permissions(perms))
            .build()
            .unwrap();

        let cm = Arc::new(ConnectionManager::from_config(config));
        let map: IMap<String, String> = IMap::new("test".to_string(), cm);

        let result = map.get(&"key".to_string()).await;
        assert!(matches!(result, Err(HazelcastError::Authorization(_))));
    }

    #[tokio::test]
    async fn test_map_permission_denied_put() {
        use crate::config::{ClientConfigBuilder, Permissions, PermissionAction};
        use crate::connection::ConnectionManager;

        let mut perms = Permissions::new();
        perms.grant(PermissionAction::Read);

        let config = ClientConfigBuilder::new()
            .security(|s| s.permissions(perms))
            .build()
            .unwrap();

        let cm = Arc::new(ConnectionManager::from_config(config));
        let map: IMap<String, String> = IMap::new("test".to_string(), cm);

        let result = map.put("key".to_string(), "value".to_string()).await;
        assert!(matches!(result, Err(HazelcastError::Authorization(_))));
    }

    #[tokio::test]
    async fn test_map_permission_denied_remove() {
        use crate::config::{ClientConfigBuilder, Permissions, PermissionAction};
        use crate::connection::ConnectionManager;

        let mut perms = Permissions::new();
        perms.grant(PermissionAction::Read);
        perms.grant(PermissionAction::Put);

        let config = ClientConfigBuilder::new()
            .security(|s| s.permissions(perms))
            .build()
            .unwrap();

        let cm = Arc::new(ConnectionManager::from_config(config));
        let map: IMap<String, String> = IMap::new("test".to_string(), cm);

        let result = map.remove(&"key".to_string()).await;
        assert!(matches!(result, Err(HazelcastError::Authorization(_))));
    }

    #[tokio::test]
    async fn test_map_permission_denied_add_index() {
        use crate::config::{ClientConfigBuilder, Permissions, PermissionAction};
        use crate::connection::ConnectionManager;

        let mut perms = Permissions::new();
        perms.grant(PermissionAction::Read);
        perms.grant(PermissionAction::Put);

        let config = ClientConfigBuilder::new()
            .security(|s| s.permissions(perms))
            .build()
            .unwrap();

        let cm = Arc::new(ConnectionManager::from_config(config));
        let map: IMap<String, String> = IMap::new("test".to_string(), cm);

        let index_config = IndexConfig::builder()
            .name("test-idx")
            .index_type(IndexType::Hash)
            .add_attribute("field")
            .build();

        let result = map.add_index(index_config).await;
        assert!(matches!(result, Err(HazelcastError::Authorization(_))));
    }

    #[tokio::test]
    async fn test_map_quorum_not_present_blocks_operations() {
        use crate::config::{ClientConfigBuilder, QuorumConfig, QuorumType};
        use crate::connection::ConnectionManager;

        let quorum = QuorumConfig::builder("protected-*")
            .min_cluster_size(3)
            .quorum_type(QuorumType::ReadWrite)
            .build()
            .unwrap();

        let config = ClientConfigBuilder::new()
            .add_quorum_config(quorum)
            .build()
            .unwrap();

        let cm = Arc::new(ConnectionManager::from_config(config));
        let map: IMap<String, String> = IMap::new("protected-map".to_string(), cm);

        let result = map.get(&"key".to_string()).await;
        assert!(matches!(result, Err(HazelcastError::QuorumNotPresent(_))));

        let result = map.put("key".to_string(), "value".to_string()).await;
        assert!(matches!(result, Err(HazelcastError::QuorumNotPresent(_))));

        let result = map.remove(&"key".to_string()).await;
        assert!(matches!(result, Err(HazelcastError::QuorumNotPresent(_))));
    }

    #[tokio::test]
    async fn test_map_quorum_read_only_allows_writes() {
        use crate::config::{ClientConfigBuilder, QuorumConfig, QuorumType};
        use crate::connection::ConnectionManager;

        let quorum = QuorumConfig::builder("read-protected-*")
            .min_cluster_size(3)
            .quorum_type(QuorumType::Read)
            .build()
            .unwrap();

        let config = ClientConfigBuilder::new()
            .add_quorum_config(quorum)
            .build()
            .unwrap();

        let cm = Arc::new(ConnectionManager::from_config(config));
        let map: IMap<String, String> = IMap::new("read-protected-map".to_string(), cm);

        let result = map.get(&"key".to_string()).await;
        assert!(matches!(result, Err(HazelcastError::QuorumNotPresent(_))));
    }

    #[tokio::test]
    async fn test_map_permission_allowed_with_all() {
        use crate::config::ClientConfigBuilder;
        use crate::connection::ConnectionManager;

        let config = ClientConfigBuilder::new().build().unwrap();

        let cm = Arc::new(ConnectionManager::from_config(config));
        let map: IMap<String, String> = IMap::new("test".to_string(), cm);

        map.check_permission(crate::config::PermissionAction::Read).unwrap();
        map.check_permission(crate::config::PermissionAction::Put).unwrap();
        map.check_permission(crate::config::PermissionAction::Remove).unwrap();
    }

    #[test]
    fn test_imap_has_near_cache() {
        use std::net::SocketAddr;

        let addr: SocketAddr = "127.0.0.1:5701".parse().unwrap();
        let cm1 = Arc::new(make_cm_from_addr(addr));

        let map_without: IMap<String, String> = IMap::new("test".to_string(), cm1);
        assert!(!map_without.has_near_cache());
        assert!(map_without.near_cache_stats().is_none());

        let cm2 = Arc::new(make_cm_from_addr(addr));
        let config = NearCacheConfig::builder("test").build().unwrap();
        let map_with: IMap<String, String> =
            IMap::new_with_near_cache("test".to_string(), cm2, config);
        assert!(map_with.has_near_cache());
        assert!(map_with.near_cache_stats().is_some());
    }

    #[test]
    fn test_near_cache_stats_initial() {
        use crate::connection::ConnectionManager;
        use std::net::SocketAddr;

        let addr: SocketAddr = "127.0.0.1:5701".parse().unwrap();
        let cm = Arc::new(make_cm_from_addr(addr));
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
    fn test_local_stats_initial() {
        use crate::connection::ConnectionManager;
        use std::net::SocketAddr;

        let addr: SocketAddr = "127.0.0.1:5701".parse().unwrap();
        let cm = Arc::new(make_cm_from_addr(addr));
        let map: IMap<String, String> = IMap::new("test".to_string(), cm);

        let stats = map.local_stats();
        assert_eq!(stats.hits(), 0);
        assert_eq!(stats.misses(), 0);
        assert_eq!(stats.get_count(), 0);
        assert_eq!(stats.put_count(), 0);
        assert_eq!(stats.remove_count(), 0);
        assert_eq!(stats.owned_entry_count(), 0);
        assert_eq!(stats.backup_entry_count(), 0);
        assert_eq!(stats.heap_cost(), 0);
    }

    #[test]
    fn test_local_stats_with_near_cache() {
        use crate::connection::ConnectionManager;
        use std::net::SocketAddr;

        let addr: SocketAddr = "127.0.0.1:5701".parse().unwrap();
        let cm = Arc::new(make_cm_from_addr(addr));
        let config = NearCacheConfig::builder("test").build().unwrap();
        let map: IMap<String, String> =
            IMap::new_with_near_cache("test".to_string(), cm, config);

        let stats = map.local_stats();
        assert_eq!(stats.owned_entry_count(), 0);
    }

    #[test]
    fn test_local_stats_shared_on_clone() {
        use crate::connection::ConnectionManager;
        use std::net::SocketAddr;

        let addr: SocketAddr = "127.0.0.1:5701".parse().unwrap();
        let cm = Arc::new(make_cm_from_addr(addr));
        let map1: IMap<String, String> = IMap::new("test".to_string(), cm);
        let map2 = map1.clone();

        assert!(Arc::ptr_eq(&map1.stats_tracker, &map2.stats_tracker));
    }

    #[test]
    fn test_near_cache_clone_shares_cache() {
        use crate::connection::ConnectionManager;
        use std::net::SocketAddr;

        let addr: SocketAddr = "127.0.0.1:5701".parse().unwrap();
        let cm = Arc::new(make_cm_from_addr(addr));
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
        let cm = Arc::new(make_cm_from_addr(addr));
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
        let cm = Arc::new(make_cm_from_addr(addr));
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
            fn serialize<W: DataOutput>(&self, output: &mut W) -> hazelcast_core::Result<()> {
                output.write_int(self.increment)?;
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
        assert_eq!(IndexType::Bitmap.as_i32(), 2);
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
    fn test_index_config_for_predicate_query_optimization() {
        let sorted_config = IndexConfig::builder()
            .name("age-sorted-idx")
            .index_type(IndexType::Sorted)
            .add_attribute("age")
            .build();

        assert_eq!(sorted_config.name(), Some("age-sorted-idx"));
        assert_eq!(sorted_config.index_type(), IndexType::Sorted);
        assert_eq!(sorted_config.attributes(), &["age".to_string()]);

        let hash_config = IndexConfig::builder()
            .name("email-hash-idx")
            .index_type(IndexType::Hash)
            .add_attribute("email")
            .build();

        assert_eq!(hash_config.name(), Some("email-hash-idx"));
        assert_eq!(hash_config.index_type(), IndexType::Hash);
        assert_eq!(hash_config.attributes(), &["email".to_string()]);
    }

    #[test]
    fn test_invalidation_registration_initial_state() {
        use crate::connection::ConnectionManager;
        use std::net::SocketAddr;

        let addr: SocketAddr = "127.0.0.1:5701".parse().unwrap();
        let cm = Arc::new(make_cm_from_addr(addr));
        let config = NearCacheConfig::builder("test").build().unwrap();
        let map: IMap<String, String> =
            IMap::new_with_near_cache("test".to_string(), cm, config);

        assert!(!map.is_near_cache_invalidation_active());
    }

    #[test]
    fn test_invalidation_registration_not_active_without_near_cache() {
        use crate::connection::ConnectionManager;
        use std::net::SocketAddr;

        let addr: SocketAddr = "127.0.0.1:5701".parse().unwrap();
        let cm = Arc::new(make_cm_from_addr(addr));
        let map: IMap<String, String> = IMap::new("test".to_string(), cm);

        assert!(!map.is_near_cache_invalidation_active());
    }

    #[test]
    fn test_invalidation_registration_shared_on_clone() {
        use crate::connection::ConnectionManager;
        use std::net::SocketAddr;

        let addr: SocketAddr = "127.0.0.1:5701".parse().unwrap();
        let cm = Arc::new(make_cm_from_addr(addr));
        let config = NearCacheConfig::builder("test").build().unwrap();
        let map1: IMap<String, String> =
            IMap::new_with_near_cache("test".to_string(), cm, config);
        let map2 = map1.clone();

        assert!(!map1.is_near_cache_invalidation_active());
        assert!(!map2.is_near_cache_invalidation_active());

        assert!(Arc::ptr_eq(
            &map1.invalidation_registration,
            &map2.invalidation_registration
        ));
    }

    #[tokio::test]
    async fn test_start_invalidation_no_op_without_near_cache() {
        use crate::connection::ConnectionManager;
        use std::net::SocketAddr;

        let addr: SocketAddr = "127.0.0.1:5701".parse().unwrap();
        let cm = Arc::new(make_cm_from_addr(addr));
        let map: IMap<String, String> = IMap::new("test".to_string(), cm);

        assert!(!map.has_near_cache());
        let result = map.start_near_cache_invalidation().await;
        assert!(result.is_ok());
        assert!(!map.is_near_cache_invalidation_active());
    }

    #[tokio::test]
    async fn test_stop_invalidation_returns_false_when_not_active() {
        use crate::connection::ConnectionManager;
        use std::net::SocketAddr;

        let addr: SocketAddr = "127.0.0.1:5701".parse().unwrap();
        let cm = Arc::new(make_cm_from_addr(addr));
        let config = NearCacheConfig::builder("test").build().unwrap();
        let map: IMap<String, String> =
            IMap::new_with_near_cache("test".to_string(), cm, config);

        let result = map.stop_near_cache_invalidation().await;
        assert!(matches!(result, Ok(false)));
    }

    #[test]
    fn test_put_all_empty_map() {
        use crate::connection::ConnectionManager;
        use std::net::SocketAddr;

        let addr: SocketAddr = "127.0.0.1:5701".parse().unwrap();
        let _cm = Arc::new(make_cm_from_addr(addr));

        let entries: HashMap<String, String> = HashMap::new();
        assert!(entries.is_empty());
    }

    #[test]
    fn test_get_all_empty_keys() {
        use crate::connection::ConnectionManager;
        use std::net::SocketAddr;

        let addr: SocketAddr = "127.0.0.1:5701".parse().unwrap();
        let _cm = Arc::new(make_cm_from_addr(addr));

        let keys: Vec<String> = Vec::new();
        assert!(keys.is_empty());
    }

    #[test]
    fn test_set_all_empty_map() {
        use crate::connection::ConnectionManager;
        use std::net::SocketAddr;

        let addr: SocketAddr = "127.0.0.1:5701".parse().unwrap();
        let _cm = Arc::new(make_cm_from_addr(addr));

        let entries: HashMap<String, String> = HashMap::new();
        assert!(entries.is_empty());
    }

    #[test]
    fn test_remove_all_empty_keys() {
        use crate::connection::ConnectionManager;
        use std::net::SocketAddr;

        let addr: SocketAddr = "127.0.0.1:5701".parse().unwrap();
        let _cm = Arc::new(make_cm_from_addr(addr));

        let keys: Vec<String> = Vec::new();
        assert!(keys.is_empty());
    }

    #[tokio::test]
    async fn test_put_all_permission_denied() {
        use crate::config::{ClientConfigBuilder, Permissions, PermissionAction};
        use crate::connection::ConnectionManager;

        let mut perms = Permissions::new();
        perms.grant(PermissionAction::Read);

        let config = ClientConfigBuilder::new()
            .security(|s| s.permissions(perms))
            .build()
            .unwrap();

        let cm = Arc::new(ConnectionManager::from_config(config));
        let map: IMap<String, String> = IMap::new("test".to_string(), cm);

        let mut entries = HashMap::new();
        entries.insert("key".to_string(), "value".to_string());

        let result = map.put_all(entries).await;
        assert!(matches!(result, Err(HazelcastError::Authorization(_))));
    }

    #[tokio::test]
    async fn test_set_all_permission_denied() {
        use crate::config::{ClientConfigBuilder, Permissions, PermissionAction};
        use crate::connection::ConnectionManager;

        let mut perms = Permissions::new();
        perms.grant(PermissionAction::Read);

        let config = ClientConfigBuilder::new()
            .security(|s| s.permissions(perms))
            .build()
            .unwrap();

        let cm = Arc::new(ConnectionManager::from_config(config));
        let map: IMap<String, String> = IMap::new("test".to_string(), cm);

        let mut entries = HashMap::new();
        entries.insert("key".to_string(), "value".to_string());

        let result = map.set_all(entries).await;
        assert!(matches!(result, Err(HazelcastError::Authorization(_))));
    }

    #[tokio::test]
    async fn test_set_all_quorum_not_present() {
        use crate::config::{ClientConfigBuilder, QuorumConfig, QuorumType};
        use crate::connection::ConnectionManager;

        let quorum = QuorumConfig::builder("protected-*")
            .min_cluster_size(3)
            .quorum_type(QuorumType::Write)
            .build()
            .unwrap();

        let config = ClientConfigBuilder::new()
            .add_quorum_config(quorum)
            .build()
            .unwrap();

        let cm = Arc::new(ConnectionManager::from_config(config));
        let map: IMap<String, String> = IMap::new("protected-map".to_string(), cm);

        let mut entries = HashMap::new();
        entries.insert("key".to_string(), "value".to_string());

        let result = map.set_all(entries).await;
        assert!(matches!(result, Err(HazelcastError::QuorumNotPresent(_))));
    }

    #[tokio::test]
    async fn test_get_all_permission_denied() {
        use crate::config::{ClientConfigBuilder, Permissions, PermissionAction};
        use crate::connection::ConnectionManager;

        let mut perms = Permissions::new();
        perms.grant(PermissionAction::Put);

        let config = ClientConfigBuilder::new()
            .security(|s| s.permissions(perms))
            .build()
            .unwrap();

        let cm = Arc::new(ConnectionManager::from_config(config));
        let map: IMap<String, String> = IMap::new("test".to_string(), cm);

        let keys = vec!["key".to_string()];
        let result = map.get_all(&keys).await;
        assert!(matches!(result, Err(HazelcastError::Authorization(_))));
    }

    #[tokio::test]
    async fn test_remove_all_permission_denied() {
        use crate::config::{ClientConfigBuilder, Permissions, PermissionAction};
        use crate::connection::ConnectionManager;

        let mut perms = Permissions::new();
        perms.grant(PermissionAction::Read);
        perms.grant(PermissionAction::Put);

        let config = ClientConfigBuilder::new()
            .security(|s| s.permissions(perms))
            .build()
            .unwrap();

        let cm = Arc::new(ConnectionManager::from_config(config));
        let map: IMap<String, String> = IMap::new("test".to_string(), cm);

        let keys = vec!["key".to_string()];
        let result = map.remove_all(&keys).await;
        assert!(matches!(result, Err(HazelcastError::Authorization(_))));
    }

    #[tokio::test]
    async fn test_put_all_empty_returns_ok() {
        use crate::config::ClientConfigBuilder;
        use crate::connection::ConnectionManager;

        let config = ClientConfigBuilder::new().build().unwrap();
        let cm = Arc::new(ConnectionManager::from_config(config));
        let map: IMap<String, String> = IMap::new("test".to_string(), cm);

        let entries: HashMap<String, String> = HashMap::new();
        let result = map.put_all(entries).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_set_all_empty_returns_ok() {
        use crate::config::ClientConfigBuilder;
        use crate::connection::ConnectionManager;

        let config = ClientConfigBuilder::new().build().unwrap();
        let cm = Arc::new(ConnectionManager::from_config(config));
        let map: IMap<String, String> = IMap::new("test".to_string(), cm);

        let entries: HashMap<String, String> = HashMap::new();
        let result = map.set_all(entries).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_get_all_empty_returns_empty_map() {
        use crate::config::ClientConfigBuilder;
        use crate::connection::ConnectionManager;

        let config = ClientConfigBuilder::new().build().unwrap();
        let cm = Arc::new(ConnectionManager::from_config(config));
        let map: IMap<String, String> = IMap::new("test".to_string(), cm);

        let keys: Vec<String> = Vec::new();
        let result = map.get_all(&keys).await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_remove_all_empty_returns_ok() {
        use crate::config::ClientConfigBuilder;
        use crate::connection::ConnectionManager;

        let config = ClientConfigBuilder::new().build().unwrap();
        let cm = Arc::new(ConnectionManager::from_config(config));
        let map: IMap<String, String> = IMap::new("test".to_string(), cm);

        let keys: Vec<String> = Vec::new();
        let result = map.remove_all(&keys).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_remove_all_with_predicate_permission_denied() {
        use crate::config::{ClientConfigBuilder, Permissions, PermissionAction};
        use crate::connection::ConnectionManager;

        let mut perms = Permissions::new();
        perms.grant(PermissionAction::Read);
        perms.grant(PermissionAction::Put);

        let config = ClientConfigBuilder::new()
            .security(|s| s.permissions(perms))
            .build()
            .unwrap();

        let cm = Arc::new(ConnectionManager::from_config(config));
        let map: IMap<String, String> = IMap::new("test".to_string(), cm);

        use crate::query::Predicates;
        let predicate = Predicates::sql("age > 65");

        let result = map.remove_all_with_predicate(&predicate).await;
        assert!(matches!(result, Err(HazelcastError::Authorization(_))));
    }

    #[tokio::test]
    async fn test_remove_all_with_predicate_quorum_not_present() {
        use crate::config::{ClientConfigBuilder, QuorumConfig, QuorumType};
        use crate::connection::ConnectionManager;

        let quorum = QuorumConfig::builder("protected-*")
            .min_cluster_size(3)
            .quorum_type(QuorumType::Write)
            .build()
            .unwrap();

        let config = ClientConfigBuilder::new()
            .add_quorum_config(quorum)
            .build()
            .unwrap();

        let cm = Arc::new(ConnectionManager::from_config(config));
        let map: IMap<String, String> = IMap::new("protected-map".to_string(), cm);

        use crate::query::Predicates;
        let predicate = Predicates::sql("age > 65");

        let result = map.remove_all_with_predicate(&predicate).await;
        assert!(matches!(result, Err(HazelcastError::QuorumNotPresent(_))));
    }

    #[test]
    fn test_remove_all_with_predicate_serializes_predicate() {
        use crate::query::Predicates;

        let predicate = Predicates::greater_than("age", &65i32).unwrap();
        let predicate_data = predicate.to_predicate_data().unwrap();
        assert!(!predicate_data.is_empty());

        let and_predicate = Predicates::and(vec![
            Box::new(Predicates::greater_than("age", &18i32).unwrap()),
            Box::new(Predicates::less_than("age", &65i32).unwrap()),
        ]);
        let and_data = and_predicate.to_predicate_data().unwrap();
        assert!(!and_data.is_empty());
    }

    #[test]
    fn test_remove_all_with_predicate_clears_near_cache() {
        use crate::connection::ConnectionManager;
        use std::net::SocketAddr;

        let addr: SocketAddr = "127.0.0.1:5701".parse().unwrap();
        let cm = Arc::new(make_cm_from_addr(addr));
        let config = NearCacheConfig::builder("test").build().unwrap();
        let map: IMap<String, String> =
            IMap::new_with_near_cache("test".to_string(), cm, config);

        assert!(map.has_near_cache());
    }

    #[tokio::test]
    async fn test_execute_on_entries_with_predicate_permission_denied_read() {
        use crate::config::{ClientConfigBuilder, Permissions, PermissionAction};
        use crate::connection::ConnectionManager;

        let mut perms = Permissions::new();
        perms.grant(PermissionAction::Put);

        let config = ClientConfigBuilder::new()
            .security(|s| s.permissions(perms))
            .build()
            .unwrap();

        let cm = Arc::new(ConnectionManager::from_config(config));
        let map: IMap<String, i32> = IMap::new("test".to_string(), cm);

        use crate::proxy::EntryProcessor;
        struct TestProcessor;
        impl EntryProcessor for TestProcessor {
            type Output = i32;
        }
        impl Serializable for TestProcessor {
            fn serialize<W: DataOutput>(&self, _output: &mut W) -> hazelcast_core::Result<()> {
                Ok(())
            }
        }

        use crate::query::Predicates;
        let predicate = Predicates::sql("age > 18");

        let result = map.execute_on_entries_with_predicate(&TestProcessor, &predicate).await;
        assert!(matches!(result, Err(HazelcastError::Authorization(_))));
    }

    #[tokio::test]
    async fn test_execute_on_entries_with_predicate_permission_denied_put() {
        use crate::config::{ClientConfigBuilder, Permissions, PermissionAction};
        use crate::connection::ConnectionManager;

        let mut perms = Permissions::new();
        perms.grant(PermissionAction::Read);

        let config = ClientConfigBuilder::new()
            .security(|s| s.permissions(perms))
            .build()
            .unwrap();

        let cm = Arc::new(ConnectionManager::from_config(config));
        let map: IMap<String, i32> = IMap::new("test".to_string(), cm);

        use crate::proxy::EntryProcessor;
        struct TestProcessor;
        impl EntryProcessor for TestProcessor {
            type Output = i32;
        }
        impl Serializable for TestProcessor {
            fn serialize<W: DataOutput>(&self, _output: &mut W) -> hazelcast_core::Result<()> {
                Ok(())
            }
        }

        use crate::query::Predicates;
        let predicate = Predicates::sql("age > 18");

        let result = map.execute_on_entries_with_predicate(&TestProcessor, &predicate).await;
        assert!(matches!(result, Err(HazelcastError::Authorization(_))));
    }

    #[test]
    fn test_execute_on_entries_with_predicate_serializes_both() {
        use crate::proxy::EntryProcessor;

        struct IncrementProcessor {
            delta: i32,
        }

        impl EntryProcessor for IncrementProcessor {
            type Output = i32;
        }

        impl Serializable for IncrementProcessor {
            fn serialize<W: DataOutput>(&self, output: &mut W) -> hazelcast_core::Result<()> {
                output.write_int(self.delta)?;
                Ok(())
            }
        }

        let processor = IncrementProcessor { delta: 5 };
        let mut output = ObjectDataOutput::new();
        processor.serialize(&mut output).unwrap();
        let processor_bytes = output.into_bytes();
        assert_eq!(processor_bytes.len(), 4);
        assert_eq!(i32::from_be_bytes(processor_bytes[..4].try_into().unwrap()), 5);

        use crate::query::Predicates;
        let predicate = Predicates::sql("status = 'active'");
        let predicate_data = predicate.to_predicate_data().unwrap();
        assert!(!predicate_data.is_empty());
    }

    #[test]
    fn test_entry_processor_with_predicate_result_handling() {
        use crate::proxy::EntryProcessorResult;

        let mut results = std::collections::HashMap::new();
        results.insert("key1".to_string(), 10i32);
        results.insert("key2".to_string(), 20i32);

        let result: EntryProcessorResult<String, i32> = EntryProcessorResult::new(results);

        assert_eq!(result.len(), 2);
        assert_eq!(result.get(&"key1".to_string()), Some(&10));
        assert_eq!(result.get(&"key2".to_string()), Some(&20));
        assert_eq!(result.get(&"key3".to_string()), None);
    }

    #[test]
    fn test_composite_index_for_multi_attribute_queries() {
        let composite_config = IndexConfig::builder()
            .name("name-composite-idx")
            .index_type(IndexType::Sorted)
            .add_attributes(["lastName", "firstName", "middleName"])
            .build();

        assert_eq!(composite_config.name(), Some("name-composite-idx"));
        assert_eq!(composite_config.index_type(), IndexType::Sorted);
        assert_eq!(composite_config.attributes().len(), 3);
        assert_eq!(composite_config.attributes()[0], "lastName");
        assert_eq!(composite_config.attributes()[1], "firstName");
        assert_eq!(composite_config.attributes()[2], "middleName");
    }

    #[test]
    fn test_index_type_selection_for_query_patterns() {
        let range_query_index = IndexConfig::builder()
            .index_type(IndexType::Sorted)
            .add_attribute("timestamp")
            .build();
        assert_eq!(range_query_index.index_type(), IndexType::Sorted);

        let equality_query_index = IndexConfig::builder()
            .index_type(IndexType::Hash)
            .add_attribute("userId")
            .build();
        assert_eq!(equality_query_index.index_type(), IndexType::Hash);
    }

    #[test]
    fn test_bitmap_index_config() {
        use crate::query::{BitmapIndexOptions, UniqueKeyTransformation};

        let config = IndexConfig::builder()
            .name("status-bitmap-idx")
            .add_attribute("status")
            .bitmap_options(
                BitmapIndexOptions::builder()
                    .unique_key("__key")
                    .unique_key_transformation(UniqueKeyTransformation::Long)
                    .build()
            )
            .build();

        assert_eq!(config.name(), Some("status-bitmap-idx"));
        assert_eq!(config.index_type(), IndexType::Bitmap);
        assert_eq!(config.attributes(), &["status".to_string()]);
        assert!(config.bitmap_options().is_some());

        let opts = config.bitmap_options().unwrap();
        assert_eq!(opts.unique_key(), "__key");
        assert_eq!(opts.unique_key_transformation(), UniqueKeyTransformation::Long);
    }

    #[test]
    fn test_bitmap_index_config_default_options() {
        use crate::query::BitmapIndexOptions;

        let config = IndexConfig::builder()
            .add_attribute("category")
            .bitmap_options(BitmapIndexOptions::new())
            .build();

        assert_eq!(config.index_type(), IndexType::Bitmap);
        let opts = config.bitmap_options().unwrap();
        assert_eq!(opts.unique_key(), "__key");
    }

    #[test]
    fn test_bitmap_options_auto_sets_index_type() {
        use crate::query::BitmapIndexOptions;

        let config = IndexConfig::builder()
            .index_type(IndexType::Hash)
            .add_attribute("field")
            .bitmap_options(BitmapIndexOptions::new())
            .build();

        assert_eq!(config.index_type(), IndexType::Bitmap);
    }

    #[test]
    #[should_panic(expected = "Bitmap index requires bitmap_options")]
    fn test_bitmap_index_without_options_panics() {
        IndexConfig::builder()
            .index_type(IndexType::Bitmap)
            .add_attribute("field")
            .build();
    }

    #[test]
    fn test_bitmap_index_try_build_without_options_errors() {
        let result = IndexConfig::builder()
            .index_type(IndexType::Bitmap)
            .add_attribute("field")
            .try_build();

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, HazelcastError::Configuration(_)));
    }

    #[test]
    fn test_bitmap_index_for_low_cardinality_optimization() {
        use crate::query::{BitmapIndexOptions, UniqueKeyTransformation};

        let status_index = IndexConfig::builder()
            .name("order-status-idx")
            .add_attribute("orderStatus")
            .bitmap_options(
                BitmapIndexOptions::builder()
                    .unique_key("orderId")
                    .unique_key_transformation(UniqueKeyTransformation::Long)
                    .build()
            )
            .build();

        assert_eq!(status_index.index_type(), IndexType::Bitmap);
        assert_eq!(status_index.attributes(), &["orderStatus".to_string()]);

        let opts = status_index.bitmap_options().unwrap();
        assert_eq!(opts.unique_key(), "orderId");
        assert_eq!(opts.unique_key_transformation(), UniqueKeyTransformation::Long);
    }

    #[test]
    fn test_bitmap_index_with_raw_transformation() {
        use crate::query::{BitmapIndexOptions, UniqueKeyTransformation};

        let config = IndexConfig::builder()
            .add_attribute("tag")
            .bitmap_options(
                BitmapIndexOptions::builder()
                    .unique_key("__key")
                    .unique_key_transformation(UniqueKeyTransformation::Raw)
                    .build()
            )
            .build();

        let opts = config.bitmap_options().unwrap();
        assert_eq!(opts.unique_key_transformation(), UniqueKeyTransformation::Raw);
    }

    #[test]
    fn test_bitmap_index_clone() {
        use crate::query::BitmapIndexOptions;

        let config = IndexConfig::builder()
            .name("test-idx")
            .add_attribute("field")
            .bitmap_options(BitmapIndexOptions::new())
            .build();

        let cloned = config.clone();
        assert_eq!(config.name(), cloned.name());
        assert_eq!(config.index_type(), cloned.index_type());
        assert_eq!(config.attributes(), cloned.attributes());
        assert!(cloned.bitmap_options().is_some());
    }

    #[test]
    fn test_non_bitmap_index_has_no_bitmap_options() {
        let sorted_config = IndexConfig::builder()
            .index_type(IndexType::Sorted)
            .add_attribute("age")
            .build();

        assert!(sorted_config.bitmap_options().is_none());

        let hash_config = IndexConfig::builder()
            .index_type(IndexType::Hash)
            .add_attribute("email")
            .build();

        assert!(hash_config.bitmap_options().is_none());
    }

    #[test]
    fn test_entry_view_accessors() {
        let view: EntryView<String, i32> = EntryView {
            key: "test-key".to_string(),
            value: 42,
            cost: 100,
            creation_time: 1000,
            expiration_time: 2000,
            hits: 5,
            last_access_time: 1500,
            last_stored_time: 1200,
            last_update_time: 1400,
            version: 3,
            ttl: 60000,
            max_idle: 30000,
        };

        assert_eq!(view.key(), "test-key");
        assert_eq!(*view.value(), 42);
        assert_eq!(view.cost(), 100);
        assert_eq!(view.creation_time(), 1000);
        assert_eq!(view.expiration_time(), 2000);
        assert_eq!(view.hits(), 5);
        assert_eq!(view.last_access_time(), 1500);
        assert_eq!(view.last_stored_time(), 1200);
        assert_eq!(view.last_update_time(), 1400);
        assert_eq!(view.version(), 3);
        assert_eq!(view.ttl(), 60000);
        assert_eq!(view.max_idle(), 30000);
    }

    #[test]
    fn test_entry_view_clone() {
        let view: EntryView<String, i32> = EntryView {
            key: "key".to_string(),
            value: 123,
            cost: 50,
            creation_time: 1000,
            expiration_time: 0,
            hits: 10,
            last_access_time: 1500,
            last_stored_time: 0,
            last_update_time: 1200,
            version: 1,
            ttl: 0,
            max_idle: 0,
        };

        let cloned = view.clone();
        assert_eq!(view.key, cloned.key);
        assert_eq!(view.value, cloned.value);
        assert_eq!(view.version, cloned.version);
    }

    #[test]
    fn test_entry_view_debug() {
        let view: EntryView<String, i32> = EntryView {
            key: "k".to_string(),
            value: 1,
            cost: 0,
            creation_time: 0,
            expiration_time: 0,
            hits: 0,
            last_access_time: 0,
            last_stored_time: 0,
            last_update_time: 0,
            version: 0,
            ttl: 0,
            max_idle: 0,
        };

        let debug_str = format!("{:?}", view);
        assert!(debug_str.contains("EntryView"));
        assert!(debug_str.contains("key"));
        assert!(debug_str.contains("value"));
    }

    #[tokio::test]
    async fn test_evict_permission_denied() {
        use crate::config::{ClientConfigBuilder, Permissions, PermissionAction};
        use crate::connection::ConnectionManager;

        let mut perms = Permissions::new();
        perms.grant(PermissionAction::Read);
        perms.grant(PermissionAction::Put);

        let config = ClientConfigBuilder::new()
            .security(|s| s.permissions(perms))
            .build()
            .unwrap();

        let cm = Arc::new(ConnectionManager::from_config(config));
        let map: IMap<String, String> = IMap::new("test".to_string(), cm);

        let result = map.evict(&"key".to_string()).await;
        assert!(matches!(result, Err(HazelcastError::Authorization(_))));
    }

    #[tokio::test]
    async fn test_evict_all_permission_denied() {
        use crate::config::{ClientConfigBuilder, Permissions, PermissionAction};
        use crate::connection::ConnectionManager;

        let mut perms = Permissions::new();
        perms.grant(PermissionAction::Read);

        let config = ClientConfigBuilder::new()
            .security(|s| s.permissions(perms))
            .build()
            .unwrap();

        let cm = Arc::new(ConnectionManager::from_config(config));
        let map: IMap<String, String> = IMap::new("test".to_string(), cm);

        let result = map.evict_all().await;
        assert!(matches!(result, Err(HazelcastError::Authorization(_))));
    }

    #[tokio::test]
    async fn test_flush_permission_denied() {
        use crate::config::{ClientConfigBuilder, Permissions, PermissionAction};
        use crate::connection::ConnectionManager;

        let mut perms = Permissions::new();
        perms.grant(PermissionAction::Read);

        let config = ClientConfigBuilder::new()
            .security(|s| s.permissions(perms))
            .build()
            .unwrap();

        let cm = Arc::new(ConnectionManager::from_config(config));
        let map: IMap<String, String> = IMap::new("test".to_string(), cm);

        let result = map.flush().await;
        assert!(matches!(result, Err(HazelcastError::Authorization(_))));
    }

    #[tokio::test]
    async fn test_get_entry_view_permission_denied() {
        use crate::config::{ClientConfigBuilder, Permissions, PermissionAction};
        use crate::connection::ConnectionManager;

        let mut perms = Permissions::new();
        perms.grant(PermissionAction::Put);

        let config = ClientConfigBuilder::new()
            .security(|s| s.permissions(perms))
            .build()
            .unwrap();

        let cm = Arc::new(ConnectionManager::from_config(config));
        let map: IMap<String, String> = IMap::new("test".to_string(), cm);

        let result = map.get_entry_view(&"key".to_string()).await;
        assert!(matches!(result, Err(HazelcastError::Authorization(_))));
    }

    #[tokio::test]
    async fn test_set_ttl_permission_denied() {
        use crate::config::{ClientConfigBuilder, Permissions, PermissionAction};
        use crate::connection::ConnectionManager;

        let mut perms = Permissions::new();
        perms.grant(PermissionAction::Read);

        let config = ClientConfigBuilder::new()
            .security(|s| s.permissions(perms))
            .build()
            .unwrap();

        let cm = Arc::new(ConnectionManager::from_config(config));
        let map: IMap<String, String> = IMap::new("test".to_string(), cm);

        let result = map.set_ttl(&"key".to_string(), Duration::from_secs(60)).await;
        assert!(matches!(result, Err(HazelcastError::Authorization(_))));
    }

    #[tokio::test]
    async fn test_try_put_permission_denied() {
        use crate::config::{ClientConfigBuilder, Permissions, PermissionAction};
        use crate::connection::ConnectionManager;

        let mut perms = Permissions::new();
        perms.grant(PermissionAction::Read);

        let config = ClientConfigBuilder::new()
            .security(|s| s.permissions(perms))
            .build()
            .unwrap();

        let cm = Arc::new(ConnectionManager::from_config(config));
        let map: IMap<String, String> = IMap::new("test".to_string(), cm);

        let result = map.try_put(
            "key".to_string(),
            "value".to_string(),
            Duration::from_millis(100),
        ).await;
        assert!(matches!(result, Err(HazelcastError::Authorization(_))));
    }

    #[tokio::test]
    async fn test_put_transient_permission_denied() {
        use crate::config::{ClientConfigBuilder, Permissions, PermissionAction};
        use crate::connection::ConnectionManager;

        let mut perms = Permissions::new();
        perms.grant(PermissionAction::Read);

        let config = ClientConfigBuilder::new()
            .security(|s| s.permissions(perms))
            .build()
            .unwrap();

        let cm = Arc::new(ConnectionManager::from_config(config));
        let map: IMap<String, String> = IMap::new("test".to_string(), cm);

        let result = map.put_transient(
            "key".to_string(),
            "value".to_string(),
            Duration::from_secs(60),
        ).await;
        assert!(matches!(result, Err(HazelcastError::Authorization(_))));
    }

    #[tokio::test]
    async fn test_evict_quorum_not_present() {
        use crate::config::{ClientConfigBuilder, QuorumConfig, QuorumType};
        use crate::connection::ConnectionManager;

        let quorum = QuorumConfig::builder("protected-*")
            .min_cluster_size(3)
            .quorum_type(QuorumType::Write)
            .build()
            .unwrap();

        let config = ClientConfigBuilder::new()
            .add_quorum_config(quorum)
            .build()
            .unwrap();

        let cm = Arc::new(ConnectionManager::from_config(config));
        let map: IMap<String, String> = IMap::new("protected-map".to_string(), cm);

        let result = map.evict(&"key".to_string()).await;
        assert!(matches!(result, Err(HazelcastError::QuorumNotPresent(_))));
    }

    #[tokio::test]
    async fn test_evict_all_quorum_not_present() {
        use crate::config::{ClientConfigBuilder, QuorumConfig, QuorumType};
        use crate::connection::ConnectionManager;

        let quorum = QuorumConfig::builder("protected-*")
            .min_cluster_size(3)
            .quorum_type(QuorumType::Write)
            .build()
            .unwrap();

        let config = ClientConfigBuilder::new()
            .add_quorum_config(quorum)
            .build()
            .unwrap();

        let cm = Arc::new(ConnectionManager::from_config(config));
        let map: IMap<String, String> = IMap::new("protected-map".to_string(), cm);

        let result = map.evict_all().await;
        assert!(matches!(result, Err(HazelcastError::QuorumNotPresent(_))));
    }

    #[tokio::test]
    async fn test_get_entry_view_quorum_not_present() {
        use crate::config::{ClientConfigBuilder, QuorumConfig, QuorumType};
        use crate::connection::ConnectionManager;

        let quorum = QuorumConfig::builder("protected-*")
            .min_cluster_size(3)
            .quorum_type(QuorumType::Read)
            .build()
            .unwrap();

        let config = ClientConfigBuilder::new()
            .add_quorum_config(quorum)
            .build()
            .unwrap();

        let cm = Arc::new(ConnectionManager::from_config(config));
        let map: IMap<String, String> = IMap::new("protected-map".to_string(), cm);

        let result = map.get_entry_view(&"key".to_string()).await;
        assert!(matches!(result, Err(HazelcastError::QuorumNotPresent(_))));
    }

    #[tokio::test]
    async fn test_set_ttl_quorum_not_present() {
        use crate::config::{ClientConfigBuilder, QuorumConfig, QuorumType};
        use crate::connection::ConnectionManager;

        let quorum = QuorumConfig::builder("protected-*")
            .min_cluster_size(3)
            .quorum_type(QuorumType::Write)
            .build()
            .unwrap();

        let config = ClientConfigBuilder::new()
            .add_quorum_config(quorum)
            .build()
            .unwrap();

        let cm = Arc::new(ConnectionManager::from_config(config));
        let map: IMap<String, String> = IMap::new("protected-map".to_string(), cm);

        let result = map.set_ttl(&"key".to_string(), Duration::from_secs(60)).await;
        assert!(matches!(result, Err(HazelcastError::QuorumNotPresent(_))));
    }

    #[tokio::test]
    async fn test_try_put_quorum_not_present() {
        use crate::config::{ClientConfigBuilder, QuorumConfig, QuorumType};
        use crate::connection::ConnectionManager;

        let quorum = QuorumConfig::builder("protected-*")
            .min_cluster_size(3)
            .quorum_type(QuorumType::Write)
            .build()
            .unwrap();

        let config = ClientConfigBuilder::new()
            .add_quorum_config(quorum)
            .build()
            .unwrap();

        let cm = Arc::new(ConnectionManager::from_config(config));
        let map: IMap<String, String> = IMap::new("protected-map".to_string(), cm);

        let result = map.try_put(
            "key".to_string(),
            "value".to_string(),
            Duration::from_millis(100),
        ).await;
        assert!(matches!(result, Err(HazelcastError::QuorumNotPresent(_))));
    }

    #[tokio::test]
    async fn test_put_transient_quorum_not_present() {
        use crate::config::{ClientConfigBuilder, QuorumConfig, QuorumType};
        use crate::connection::ConnectionManager;

        let quorum = QuorumConfig::builder("protected-*")
            .min_cluster_size(3)
            .quorum_type(QuorumType::Write)
            .build()
            .unwrap();

        let config = ClientConfigBuilder::new()
            .add_quorum_config(quorum)
            .build()
            .unwrap();

        let cm = Arc::new(ConnectionManager::from_config(config));
        let map: IMap<String, String> = IMap::new("protected-map".to_string(), cm);

        let result = map.put_transient(
            "key".to_string(),
            "value".to_string(),
            Duration::from_secs(60),
        ).await;
        assert!(matches!(result, Err(HazelcastError::QuorumNotPresent(_))));
    }

    #[test]
    fn test_evict_invalidates_near_cache() {
        use crate::connection::ConnectionManager;
        use std::net::SocketAddr;

        let addr: SocketAddr = "127.0.0.1:5701".parse().unwrap();
        let cm = Arc::new(make_cm_from_addr(addr));
        let config = NearCacheConfig::builder("test").build().unwrap();
        let map: IMap<String, String> =
            IMap::new_with_near_cache("test".to_string(), cm, config);

        assert!(map.has_near_cache());
    }

    #[test]
    fn test_try_put_invalidates_near_cache() {
        use crate::connection::ConnectionManager;
        use std::net::SocketAddr;

        let addr: SocketAddr = "127.0.0.1:5701".parse().unwrap();
        let cm = Arc::new(make_cm_from_addr(addr));
        let config = NearCacheConfig::builder("test").build().unwrap();
        let map: IMap<String, String> =
            IMap::new_with_near_cache("test".to_string(), cm, config);

        assert!(map.has_near_cache());
    }

    #[test]
    fn test_put_transient_invalidates_near_cache() {
        use crate::connection::ConnectionManager;
        use std::net::SocketAddr;

        let addr: SocketAddr = "127.0.0.1:5701".parse().unwrap();
        let cm = Arc::new(make_cm_from_addr(addr));
        let config = NearCacheConfig::builder("test").build().unwrap();
        let map: IMap<String, String> =
            IMap::new_with_near_cache("test".to_string(), cm, config);

        assert!(map.has_near_cache());
    }

    #[test]
    fn test_entry_view_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<EntryView<String, i32>>();
    }

    #[tokio::test]
    async fn test_add_interceptor_permission_denied() {
        use crate::config::{ClientConfigBuilder, Permissions, PermissionAction};
        use crate::connection::ConnectionManager;
        use crate::proxy::MapInterceptor;

        struct TestInterceptor;

        impl MapInterceptor for TestInterceptor {
            type Value = String;

            fn intercept_get(&self, value: Option<Self::Value>) -> Option<Self::Value> {
                value
            }

            fn intercept_put(&self, _old: Option<Self::Value>, new: Self::Value) -> Self::Value {
                new
            }

            fn intercept_remove(&self, _value: Option<Self::Value>) {}
        }

        impl Serializable for TestInterceptor {
            fn serialize<W: DataOutput>(&self, _output: &mut W) -> hazelcast_core::Result<()> {
                Ok(())
            }
        }

        let mut perms = Permissions::new();
        perms.grant(PermissionAction::Read);

        let config = ClientConfigBuilder::new()
            .security(|s| s.permissions(perms))
            .build()
            .unwrap();

        let cm = Arc::new(ConnectionManager::from_config(config));
        let map: IMap<String, String> = IMap::new("test".to_string(), cm);

        let result = map.add_interceptor(&TestInterceptor).await;
        assert!(matches!(result, Err(HazelcastError::Authorization(_))));
    }

    #[tokio::test]
    async fn test_remove_interceptor_permission_denied() {
        use crate::config::{ClientConfigBuilder, Permissions, PermissionAction};
        use crate::connection::ConnectionManager;

        let mut perms = Permissions::new();
        perms.grant(PermissionAction::Read);

        let config = ClientConfigBuilder::new()
            .security(|s| s.permissions(perms))
            .build()
            .unwrap();

        let cm = Arc::new(ConnectionManager::from_config(config));
        let map: IMap<String, String> = IMap::new("test".to_string(), cm);

        let result = map.remove_interceptor("some-id").await;
        assert!(matches!(result, Err(HazelcastError::Authorization(_))));
    }

    #[test]
    fn test_interceptor_serialization_for_map() {
        use crate::proxy::MapInterceptor;

        struct PrefixInterceptor {
            prefix: String,
        }

        impl MapInterceptor for PrefixInterceptor {
            type Value = String;

            fn intercept_get(&self, value: Option<Self::Value>) -> Option<Self::Value> {
                value.map(|v| format!("{}{}", self.prefix, v))
            }

            fn intercept_put(&self, _old: Option<Self::Value>, new: Self::Value) -> Self::Value {
                format!("{}{}", self.prefix, new)
            }

            fn intercept_remove(&self, _value: Option<Self::Value>) {}
        }

        impl Serializable for PrefixInterceptor {
            fn serialize<W: DataOutput>(&self, output: &mut W) -> hazelcast_core::Result<()> {
                output.write_string(&self.prefix)?;
                Ok(())
            }
        }

        let interceptor = PrefixInterceptor {
            prefix: "intercepted_".to_string(),
        };

        let data = IMap::<String, String>::serialize_value(&interceptor).unwrap();
        assert!(!data.is_empty());
    }

    #[tokio::test]
    async fn test_add_partition_lost_listener_permission_denied() {
        use crate::config::{ClientConfigBuilder, Permissions, PermissionAction};
        use crate::connection::ConnectionManager;

        let mut perms = Permissions::new();
        perms.grant(PermissionAction::Read);
        perms.grant(PermissionAction::Put);

        let config = ClientConfigBuilder::new()
            .security(|s| s.permissions(perms))
            .build()
            .unwrap();

        let cm = Arc::new(ConnectionManager::from_config(config));
        let map: IMap<String, String> = IMap::new("test".to_string(), cm);

        let result = map.add_partition_lost_listener(|_event| {}).await;
        assert!(matches!(result, Err(HazelcastError::Authorization(_))));
    }

    #[test]
    fn test_decode_partition_lost_event_empty_frames() {
        let message = ClientMessage::create_for_encode(0, -1);
        let result = IMap::<String, String>::decode_partition_lost_event(&message);
        assert!(result.is_err());
    }

    #[test]
    fn test_is_partition_lost_event_checks_name() {
        let message = ClientMessage::create_for_encode(0, -1);
        let result = IMap::<String, String>::is_partition_lost_event(&message, "test-map");
        assert!(!result);
    }

    #[test]
    fn test_put_transient_zero_ttl_uses_no_expiry() {
        let ttl = Duration::ZERO;
        let ttl_ms = if ttl.is_zero() { -1i64 } else { ttl.as_millis() as i64 };
        assert_eq!(ttl_ms, -1);
    }

    #[test]
    fn test_put_transient_nonzero_ttl_converts_correctly() {
        let ttl = Duration::from_secs(60);
        let ttl_ms = if ttl.is_zero() { -1i64 } else { ttl.as_millis() as i64 };
        assert_eq!(ttl_ms, 60000);
    }

    #[tokio::test]
    async fn test_put_with_max_idle_permission_denied() {
        use crate::config::{ClientConfigBuilder, Permissions, PermissionAction};
        use crate::connection::ConnectionManager;

        let mut perms = Permissions::new();
        perms.grant(PermissionAction::Read);

        let config = ClientConfigBuilder::new()
            .security(|s| s.permissions(perms))
            .build()
            .unwrap();

        let cm = Arc::new(ConnectionManager::from_config(config));
        let map: IMap<String, String> = IMap::new("test".to_string(), cm);

        let result = map.put_with_max_idle(
            "key".to_string(),
            "value".to_string(),
            Duration::from_secs(60),
        ).await;
        assert!(matches!(result, Err(HazelcastError::Authorization(_))));
    }

    #[tokio::test]
    async fn test_put_with_ttl_and_max_idle_permission_denied() {
        use crate::config::{ClientConfigBuilder, Permissions, PermissionAction};
        use crate::connection::ConnectionManager;

        let mut perms = Permissions::new();
        perms.grant(PermissionAction::Read);

        let config = ClientConfigBuilder::new()
            .security(|s| s.permissions(perms))
            .build()
            .unwrap();

        let cm = Arc::new(ConnectionManager::from_config(config));
        let map: IMap<String, String> = IMap::new("test".to_string(), cm);

        let result = map.put_with_ttl_and_max_idle(
            "key".to_string(),
            "value".to_string(),
            Duration::from_secs(60),
            Duration::from_secs(30),
        ).await;
        assert!(matches!(result, Err(HazelcastError::Authorization(_))));
    }

    #[tokio::test]
    async fn test_set_with_ttl_and_max_idle_permission_denied() {
        use crate::config::{ClientConfigBuilder, Permissions, PermissionAction};
        use crate::connection::ConnectionManager;

        let mut perms = Permissions::new();
        perms.grant(PermissionAction::Read);

        let config = ClientConfigBuilder::new()
            .security(|s| s.permissions(perms))
            .build()
            .unwrap();

        let cm = Arc::new(ConnectionManager::from_config(config));
        let map: IMap<String, String> = IMap::new("test".to_string(), cm);

        let result = map.set_with_ttl_and_max_idle(
            "key".to_string(),
            "value".to_string(),
            Duration::from_secs(60),
            Duration::from_secs(30),
        ).await;
        assert!(matches!(result, Err(HazelcastError::Authorization(_))));
    }

    #[tokio::test]
    async fn test_put_with_max_idle_quorum_not_present() {
        use crate::config::{ClientConfigBuilder, QuorumConfig, QuorumType};
        use crate::connection::ConnectionManager;

        let quorum = QuorumConfig::builder("protected-*")
            .min_cluster_size(3)
            .quorum_type(QuorumType::Write)
            .build()
            .unwrap();

        let config = ClientConfigBuilder::new()
            .add_quorum_config(quorum)
            .build()
            .unwrap();

        let cm = Arc::new(ConnectionManager::from_config(config));
        let map: IMap<String, String> = IMap::new("protected-map".to_string(), cm);

        let result = map.put_with_max_idle(
            "key".to_string(),
            "value".to_string(),
            Duration::from_secs(60),
        ).await;
        assert!(matches!(result, Err(HazelcastError::QuorumNotPresent(_))));
    }

    #[tokio::test]
    async fn test_put_with_ttl_and_max_idle_quorum_not_present() {
        use crate::config::{ClientConfigBuilder, QuorumConfig, QuorumType};
        use crate::connection::ConnectionManager;

        let quorum = QuorumConfig::builder("protected-*")
            .min_cluster_size(3)
            .quorum_type(QuorumType::Write)
            .build()
            .unwrap();

        let config = ClientConfigBuilder::new()
            .add_quorum_config(quorum)
            .build()
            .unwrap();

        let cm = Arc::new(ConnectionManager::from_config(config));
        let map: IMap<String, String> = IMap::new("protected-map".to_string(), cm);

        let result = map.put_with_ttl_and_max_idle(
            "key".to_string(),
            "value".to_string(),
            Duration::from_secs(60),
            Duration::from_secs(30),
        ).await;
        assert!(matches!(result, Err(HazelcastError::QuorumNotPresent(_))));
    }

    #[tokio::test]
    async fn test_set_with_ttl_and_max_idle_quorum_not_present() {
        use crate::config::{ClientConfigBuilder, QuorumConfig, QuorumType};
        use crate::connection::ConnectionManager;

        let quorum = QuorumConfig::builder("protected-*")
            .min_cluster_size(3)
            .quorum_type(QuorumType::Write)
            .build()
            .unwrap();

        let config = ClientConfigBuilder::new()
            .add_quorum_config(quorum)
            .build()
            .unwrap();

        let cm = Arc::new(ConnectionManager::from_config(config));
        let map: IMap<String, String> = IMap::new("protected-map".to_string(), cm);

        let result = map.set_with_ttl_and_max_idle(
            "key".to_string(),
            "value".to_string(),
            Duration::from_secs(60),
            Duration::from_secs(30),
        ).await;
        assert!(matches!(result, Err(HazelcastError::QuorumNotPresent(_))));
    }

    #[test]
    fn test_put_with_max_idle_duration_conversion() {
        let max_idle = Duration::from_secs(300);
        let max_idle_ms = if max_idle.is_zero() { -1i64 } else { max_idle.as_millis() as i64 };
        assert_eq!(max_idle_ms, 300_000);
    }

    #[test]
    fn test_put_with_ttl_and_max_idle_duration_conversion() {
        let ttl = Duration::from_secs(3600);
        let max_idle = Duration::from_secs(600);
        let ttl_ms = if ttl.is_zero() { -1i64 } else { ttl.as_millis() as i64 };
        let max_idle_ms = if max_idle.is_zero() { -1i64 } else { max_idle.as_millis() as i64 };
        assert_eq!(ttl_ms, 3_600_000);
        assert_eq!(max_idle_ms, 600_000);
    }

    #[test]
    fn test_put_with_ttl_and_max_idle_zero_durations() {
        let ttl = Duration::ZERO;
        let max_idle = Duration::ZERO;
        let ttl_ms = if ttl.is_zero() { -1i64 } else { ttl.as_millis() as i64 };
        let max_idle_ms = if max_idle.is_zero() { -1i64 } else { max_idle.as_millis() as i64 };
        assert_eq!(ttl_ms, -1);
        assert_eq!(max_idle_ms, -1);
    }

    #[test]
    fn test_put_with_max_idle_invalidates_near_cache() {
        use crate::connection::ConnectionManager;
        use std::net::SocketAddr;

        let addr: SocketAddr = "127.0.0.1:5701".parse().unwrap();
        let cm = Arc::new(make_cm_from_addr(addr));
        let config = NearCacheConfig::builder("test").build().unwrap();
        let map: IMap<String, String> =
            IMap::new_with_near_cache("test".to_string(), cm, config);

        assert!(map.has_near_cache());
    }

    #[test]
    fn test_put_with_ttl_and_max_idle_invalidates_near_cache() {
        use crate::connection::ConnectionManager;
        use std::net::SocketAddr;

        let addr: SocketAddr = "127.0.0.1:5701".parse().unwrap();
        let cm = Arc::new(make_cm_from_addr(addr));
        let config = NearCacheConfig::builder("test").build().unwrap();
        let map: IMap<String, String> =
            IMap::new_with_near_cache("test".to_string(), cm, config);

        assert!(map.has_near_cache());
    }

    #[test]
    fn test_set_with_ttl_and_max_idle_invalidates_near_cache() {
        use crate::connection::ConnectionManager;
        use std::net::SocketAddr;

        let addr: SocketAddr = "127.0.0.1:5701".parse().unwrap();
        let cm = Arc::new(make_cm_from_addr(addr));
        let config = NearCacheConfig::builder("test").build().unwrap();
        let map: IMap<String, String> =
            IMap::new_with_near_cache("test".to_string(), cm, config);

        assert!(map.has_near_cache());
    }

    #[test]
    fn test_set_all_invalidates_near_cache() {
        use crate::connection::ConnectionManager;
        use std::net::SocketAddr;

        let addr: SocketAddr = "127.0.0.1:5701".parse().unwrap();
        let cm = Arc::new(make_cm_from_addr(addr));
        let config = NearCacheConfig::builder("test").build().unwrap();
        let map: IMap<String, String> =
            IMap::new_with_near_cache("test".to_string(), cm, config);

        assert!(map.has_near_cache());
    }

    #[test]
    fn test_iterator_config_default_values() {
        let config = IteratorConfig::default();
        assert_eq!(config.batch_size, 100);
        assert!(config.prefetch);
    }

    #[test]
    fn test_iterator_config_builder_pattern() {
        let config = IteratorConfig::new()
            .batch_size(50)
            .prefetch(false);
        assert_eq!(config.batch_size, 50);
        assert!(!config.prefetch);
    }

    #[tokio::test]
    async fn test_key_set_permission_denied() {
        use crate::config::{ClientConfigBuilder, Permissions, PermissionAction};
        use crate::connection::ConnectionManager;

        let mut perms = Permissions::new();
        perms.grant(PermissionAction::Put);

        let config = ClientConfigBuilder::new()
            .security(|s| s.permissions(perms))
            .build()
            .unwrap();

        let cm = Arc::new(ConnectionManager::from_config(config));
        let map: IMap<String, String> = IMap::new("test".to_string(), cm);

        let result = map.key_set().await;
        assert!(matches!(result, Err(HazelcastError::Authorization(_))));
    }

    #[tokio::test]
    async fn test_entry_set_permission_denied() {
        use crate::config::{ClientConfigBuilder, Permissions, PermissionAction};
        use crate::connection::ConnectionManager;

        let mut perms = Permissions::new();
        perms.grant(PermissionAction::Put);

        let config = ClientConfigBuilder::new()
            .security(|s| s.permissions(perms))
            .build()
            .unwrap();

        let cm = Arc::new(ConnectionManager::from_config(config));
        let map: IMap<String, String> = IMap::new("test".to_string(), cm);

        let result = map.entry_set().await;
        assert!(matches!(result, Err(HazelcastError::Authorization(_))));
    }

    #[tokio::test]
    async fn test_values_iter_permission_denied() {
        use crate::config::{ClientConfigBuilder, Permissions, PermissionAction};
        use crate::connection::ConnectionManager;

        let mut perms = Permissions::new();
        perms.grant(PermissionAction::Put);

        let config = ClientConfigBuilder::new()
            .security(|s| s.permissions(perms))
            .build()
            .unwrap();

        let cm = Arc::new(ConnectionManager::from_config(config));
        let map: IMap<String, String> = IMap::new("test".to_string(), cm);

        let result = map.values_iter().await;
        assert!(matches!(result, Err(HazelcastError::Authorization(_))));
    }

    #[tokio::test]
    async fn test_key_set_quorum_not_present() {
        use crate::config::{ClientConfigBuilder, QuorumConfig, QuorumType};
        use crate::connection::ConnectionManager;

        let quorum = QuorumConfig::builder("protected-*")
            .min_cluster_size(3)
            .quorum_type(QuorumType::Read)
            .build()
            .unwrap();

        let config = ClientConfigBuilder::new()
            .add_quorum_config(quorum)
            .build()
            .unwrap();

        let cm = Arc::new(ConnectionManager::from_config(config));
        let map: IMap<String, String> = IMap::new("protected-map".to_string(), cm);

        let result = map.key_set().await;
        assert!(matches!(result, Err(HazelcastError::QuorumNotPresent(_))));
    }

    #[test]
    fn test_event_journal_event_type_from_value() {
        assert_eq!(
            EventJournalEventType::from_value(1),
            Some(EventJournalEventType::Added)
        );
        assert_eq!(
            EventJournalEventType::from_value(2),
            Some(EventJournalEventType::Removed)
        );
        assert_eq!(
            EventJournalEventType::from_value(4),
            Some(EventJournalEventType::Updated)
        );
        assert_eq!(
            EventJournalEventType::from_value(8),
            Some(EventJournalEventType::Evicted)
        );
        assert_eq!(
            EventJournalEventType::from_value(16),
            Some(EventJournalEventType::Expired)
        );
        assert_eq!(
            EventJournalEventType::from_value(32),
            Some(EventJournalEventType::Loaded)
        );
        assert_eq!(EventJournalEventType::from_value(0), None);
        assert_eq!(EventJournalEventType::from_value(64), None);
    }

    #[test]
    fn test_event_journal_event_type_value() {
        assert_eq!(EventJournalEventType::Added.value(), 1);
        assert_eq!(EventJournalEventType::Removed.value(), 2);
        assert_eq!(EventJournalEventType::Updated.value(), 4);
        assert_eq!(EventJournalEventType::Evicted.value(), 8);
        assert_eq!(EventJournalEventType::Expired.value(), 16);
        assert_eq!(EventJournalEventType::Loaded.value(), 32);
    }

    #[test]
    fn test_event_journal_map_event_accessors() {
        let event: EventJournalMapEvent<String, i32> = EventJournalMapEvent {
            event_type: EventJournalEventType::Updated,
            key: "test-key".to_string(),
            old_value: Some(10),
            new_value: Some(20),
            sequence: 42,
        };

        assert_eq!(event.event_type(), EventJournalEventType::Updated);
        assert_eq!(event.key(), "test-key");
        assert_eq!(event.old_value(), Some(&10));
        assert_eq!(event.new_value(), Some(&20));
        assert_eq!(event.sequence(), 42);
    }

    #[test]
    fn test_event_journal_map_event_none_values() {
        let event: EventJournalMapEvent<String, i32> = EventJournalMapEvent {
            event_type: EventJournalEventType::Added,
            key: "key".to_string(),
            old_value: None,
            new_value: Some(100),
            sequence: 1,
        };

        assert_eq!(event.old_value(), None);
        assert_eq!(event.new_value(), Some(&100));
    }

    #[test]
    fn test_event_journal_config_default() {
        let config = EventJournalConfig::default();
        assert_eq!(config.start_sequence, -1);
        assert_eq!(config.min_size, 1);
        assert_eq!(config.max_size, 100);
    }

    #[test]
    fn test_event_journal_config_builder() {
        let config = EventJournalConfig::new()
            .start_sequence(100)
            .min_size(10)
            .max_size(500);

        assert_eq!(config.start_sequence, 100);
        assert_eq!(config.min_size, 10);
        assert_eq!(config.max_size, 500);
    }

    #[test]
    fn test_event_journal_event_type_equality() {
        assert_eq!(EventJournalEventType::Added, EventJournalEventType::Added);
        assert_ne!(EventJournalEventType::Added, EventJournalEventType::Removed);
    }

    #[test]
    fn test_event_journal_event_type_copy() {
        let t1 = EventJournalEventType::Updated;
        let t2 = t1;
        assert_eq!(t1, t2);
    }

    #[test]
    fn test_event_journal_map_event_clone() {
        let event: EventJournalMapEvent<String, i32> = EventJournalMapEvent {
            event_type: EventJournalEventType::Evicted,
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
    fn test_event_journal_map_event_debug() {
        let event: EventJournalMapEvent<String, i32> = EventJournalMapEvent {
            event_type: EventJournalEventType::Loaded,
            key: "k".to_string(),
            old_value: None,
            new_value: Some(1),
            sequence: 0,
        };

        let debug_str = format!("{:?}", event);
        assert!(debug_str.contains("EventJournalMapEvent"));
        assert!(debug_str.contains("Loaded"));
    }

    #[tokio::test]
    async fn test_read_from_event_journal_permission_denied() {
        use crate::config::{ClientConfigBuilder, Permissions, PermissionAction};
        use crate::connection::ConnectionManager;

        let mut perms = Permissions::new();
        perms.grant(PermissionAction::Put);

        let config = ClientConfigBuilder::new()
            .security(|s| s.permissions(perms))
            .build()
            .unwrap();

        let cm = Arc::new(ConnectionManager::from_config(config));
        let map: IMap<String, String> = IMap::new("test".to_string(), cm);

        let journal_config = EventJournalConfig::new();
        let result = map.read_from_event_journal(0, journal_config).await;
        assert!(matches!(result, Err(HazelcastError::Authorization(_))));
    }

    #[test]
    fn test_event_journal_stream_is_send() {
        fn assert_send<T: Send>() {}
        assert_send::<EventJournalStream<String, String>>();
    }

    #[tokio::test]
    async fn test_load_all_keys_permission_denied() {
        use crate::config::{ClientConfigBuilder, Permissions, PermissionAction};
        use crate::connection::ConnectionManager;

        let mut perms = Permissions::new();
        perms.grant(PermissionAction::Read);

        let config = ClientConfigBuilder::new()
            .security(|s| s.permissions(perms))
            .build()
            .unwrap();

        let cm = Arc::new(ConnectionManager::from_config(config));
        let map: IMap<String, String> = IMap::new("test".to_string(), cm);

        let result = map.load_all_keys(true).await;
        assert!(matches!(result, Err(HazelcastError::Authorization(_))));
    }

    #[tokio::test]
    async fn test_load_all_permission_denied() {
        use crate::config::{ClientConfigBuilder, Permissions, PermissionAction};
        use crate::connection::ConnectionManager;

        let mut perms = Permissions::new();
        perms.grant(PermissionAction::Read);

        let config = ClientConfigBuilder::new()
            .security(|s| s.permissions(perms))
            .build()
            .unwrap();

        let cm = Arc::new(ConnectionManager::from_config(config));
        let map: IMap<String, String> = IMap::new("test".to_string(), cm);

        let keys = vec!["key1".to_string(), "key2".to_string()];
        let result = map.load_all(&keys, true).await;
        assert!(matches!(result, Err(HazelcastError::Authorization(_))));
    }

    #[tokio::test]
    async fn test_load_all_keys_quorum_not_present() {
        use crate::config::{ClientConfigBuilder, QuorumConfig, QuorumType};
        use crate::connection::ConnectionManager;

        let quorum = QuorumConfig::builder("protected-*")
            .min_cluster_size(3)
            .quorum_type(QuorumType::Write)
            .build()
            .unwrap();

        let config = ClientConfigBuilder::new()
            .add_quorum_config(quorum)
            .build()
            .unwrap();

        let cm = Arc::new(ConnectionManager::from_config(config));
        let map: IMap<String, String> = IMap::new("protected-map".to_string(), cm);

        let result = map.load_all_keys(false).await;
        assert!(matches!(result, Err(HazelcastError::QuorumNotPresent(_))));
    }

    #[tokio::test]
    async fn test_load_all_quorum_not_present() {
        use crate::config::{ClientConfigBuilder, QuorumConfig, QuorumType};
        use crate::connection::ConnectionManager;

        let quorum = QuorumConfig::builder("protected-*")
            .min_cluster_size(3)
            .quorum_type(QuorumType::Write)
            .build()
            .unwrap();

        let config = ClientConfigBuilder::new()
            .add_quorum_config(quorum)
            .build()
            .unwrap();

        let cm = Arc::new(ConnectionManager::from_config(config));
        let map: IMap<String, String> = IMap::new("protected-map".to_string(), cm);

        let keys = vec!["key1".to_string()];
        let result = map.load_all(&keys, true).await;
        assert!(matches!(result, Err(HazelcastError::QuorumNotPresent(_))));
    }

    #[tokio::test]
    async fn test_load_all_empty_keys_returns_ok() {
        use crate::config::ClientConfigBuilder;
        use crate::connection::ConnectionManager;

        let config = ClientConfigBuilder::new().build().unwrap();
        let cm = Arc::new(ConnectionManager::from_config(config));
        let map: IMap<String, String> = IMap::new("test".to_string(), cm);

        let keys: Vec<String> = Vec::new();
        let result = map.load_all(&keys, true).await;
        assert!(result.is_ok());
    }

    #[test]
    fn test_load_all_keys_clears_near_cache_when_replacing() {
        use crate::connection::ConnectionManager;
        use std::net::SocketAddr;

        let addr: SocketAddr = "127.0.0.1:5701".parse().unwrap();
        let cm = Arc::new(make_cm_from_addr(addr));
        let config = NearCacheConfig::builder("test").build().unwrap();
        let map: IMap<String, String> =
            IMap::new_with_near_cache("test".to_string(), cm, config);

        assert!(map.has_near_cache());
    }

    #[test]
    fn test_load_all_invalidates_near_cache_when_replacing() {
        use crate::connection::ConnectionManager;
        use std::net::SocketAddr;

        let addr: SocketAddr = "127.0.0.1:5701".parse().unwrap();
        let cm = Arc::new(make_cm_from_addr(addr));
        let config = NearCacheConfig::builder("test").build().unwrap();
        let map: IMap<String, String> =
            IMap::new_with_near_cache("test".to_string(), cm, config);

        assert!(map.has_near_cache());
    }

    #[tokio::test]
    async fn test_values_with_paging_predicate_permission_denied() {
        use crate::config::{ClientConfigBuilder, Permissions, PermissionAction};
        use crate::connection::ConnectionManager;
        use crate::query::PagingPredicate;

        let mut perms = Permissions::new();
        perms.grant(PermissionAction::Put);

        let config = ClientConfigBuilder::new()
            .security(|s| s.permissions(perms))
            .build()
            .unwrap();

        let cm = Arc::new(ConnectionManager::from_config(config));
        let map: IMap<String, String> = IMap::new("test".to_string(), cm);

        let mut paging: PagingPredicate<String, String> = PagingPredicate::new(10);
        let result = map.values_with_paging_predicate(&mut paging).await;
        assert!(matches!(result, Err(HazelcastError::Authorization(_))));
    }

    #[tokio::test]
    async fn test_keys_with_paging_predicate_permission_denied() {
        use crate::config::{ClientConfigBuilder, Permissions, PermissionAction};
        use crate::connection::ConnectionManager;
        use crate::query::PagingPredicate;

        let mut perms = Permissions::new();
        perms.grant(PermissionAction::Put);

        let config = ClientConfigBuilder::new()
            .security(|s| s.permissions(perms))
            .build()
            .unwrap();

        let cm = Arc::new(ConnectionManager::from_config(config));
        let map: IMap<String, String> = IMap::new("test".to_string(), cm);

        let mut paging: PagingPredicate<String, String> = PagingPredicate::new(10);
        let result = map.keys_with_paging_predicate(&mut paging).await;
        assert!(matches!(result, Err(HazelcastError::Authorization(_))));
    }

    #[tokio::test]
    async fn test_entries_with_paging_predicate_permission_denied() {
        use crate::config::{ClientConfigBuilder, Permissions, PermissionAction};
        use crate::connection::ConnectionManager;
        use crate::query::PagingPredicate;

        let mut perms = Permissions::new();
        perms.grant(PermissionAction::Put);

        let config = ClientConfigBuilder::new()
            .security(|s| s.permissions(perms))
            .build()
            .unwrap();

        let cm = Arc::new(ConnectionManager::from_config(config));
        let map: IMap<String, String> = IMap::new("test".to_string(), cm);

        let mut paging: PagingPredicate<String, String> = PagingPredicate::new(10);
        let result = map.entries_with_paging_predicate(&mut paging).await;
        assert!(matches!(result, Err(HazelcastError::Authorization(_))));
    }

    #[test]
    fn test_paging_predicate_sets_iteration_type() {
        use crate::query::{PagingPredicate, IterationType};

        let mut paging: PagingPredicate<String, String> = PagingPredicate::new(10);
        assert_eq!(paging.iteration_type(), IterationType::Entry);

        paging.set_iteration_type(IterationType::Key);
        assert_eq!(paging.iteration_type(), IterationType::Key);

        paging.set_iteration_type(IterationType::Value);
        assert_eq!(paging.iteration_type(), IterationType::Value);
    }

    #[test]
    fn test_paging_result_has_next() {
        use crate::query::PagingResult;

        let full_page: PagingResult<i32> = PagingResult::new(vec![1, 2, 3, 4, 5], 0, Vec::new());
        assert!(full_page.has_next(5));
        assert!(!full_page.has_next(10));

        let partial_page: PagingResult<i32> = PagingResult::new(vec![1, 2, 3], 1, Vec::new());
        assert!(!partial_page.has_next(5));
        assert!(partial_page.has_next(3));
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
            fn deserialize<R: DataInput>(input: &mut R) -> hazelcast_core::Result<Self> {
                Ok(Self {
                    old_value: input.read_int()?,
                    new_value: input.read_int()?,
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
            fn serialize<W: DataOutput>(&self, output: &mut W) -> hazelcast_core::Result<()> {
                output.write_int(self.delta)?;
                Ok(())
            }
        }

        let processor = UpdateProcessor { delta: 10 };
        let mut output = ObjectDataOutput::new();
        processor.serialize(&mut output).unwrap();
        assert_eq!(output.into_bytes().len(), 4);
    }

    #[test]
    fn test_get_async_returns_join_handle() {
        fn assert_join_handle<T>(_: tokio::task::JoinHandle<T>) {}
        fn check_return_type<K, V>(map: &IMap<K, V>)
        where
            K: Serializable + Deserializable + Send + Sync + 'static,
            V: Serializable + Deserializable + Send + Sync + 'static,
        {
            let key = unsafe { std::mem::zeroed::<K>() };
            let _ = |m: &IMap<K, V>| -> tokio::task::JoinHandle<Result<Option<V>>> {
                m.get_async(key)
            };
        }
    }

    #[test]
    fn test_submit_to_key_returns_join_handle() {
        use crate::proxy::EntryProcessor;

        struct TestProcessor;
        impl EntryProcessor for TestProcessor {
            type Output = i32;
        }
        impl Serializable for TestProcessor {
            fn serialize<W: DataOutput>(&self, _output: &mut W) -> hazelcast_core::Result<()> {
                Ok(())
            }
        }

        fn check_return_type<K, V>(_map: &IMap<K, V>)
        where
            K: Serializable + Deserializable + Send + Sync + 'static,
            V: Serializable + Deserializable + Send + Sync + 'static,
        {
            let _ = |m: &IMap<K, V>, k: K| -> tokio::task::JoinHandle<Result<Option<i32>>> {
                m.submit_to_key(k, TestProcessor)
            };
        }
    }

    #[test]
    fn test_put_async_returns_join_handle() {
        fn check_return_type<K, V>(_map: &IMap<K, V>)
        where
            K: Serializable + Deserializable + Send + Sync + 'static,
            V: Serializable + Deserializable + Send + Sync + 'static,
        {
            let _ = |m: &IMap<K, V>, k: K, v: V| -> tokio::task::JoinHandle<Result<Option<V>>> {
                m.put_async(k, v)
            };
        }
    }

    #[test]
    fn test_remove_async_returns_join_handle() {
        fn check_return_type<K, V>(_map: &IMap<K, V>)
        where
            K: Serializable + Deserializable + Send + Sync + 'static,
            V: Serializable + Deserializable + Send + Sync + 'static,
        {
            let _ = |m: &IMap<K, V>, k: K| -> tokio::task::JoinHandle<Result<Option<V>>> {
                m.remove_async(k)
            };
        }
    }

    #[test]
    fn test_contains_key_async_returns_join_handle() {
        fn check_return_type<K, V>(_map: &IMap<K, V>)
        where
            K: Serializable + Deserializable + Send + Sync + 'static,
            V: Serializable + Deserializable + Send + Sync + 'static,
        {
            let _ = |m: &IMap<K, V>, k: K| -> tokio::task::JoinHandle<Result<bool>> {
                m.contains_key_async(k)
            };
        }
    }

    #[tokio::test]
    async fn test_async_methods_spawn_tasks() {
        use crate::connection::ConnectionManager;
        use std::net::SocketAddr;

        let addr: SocketAddr = "127.0.0.1:5701".parse().unwrap();
        let cm = Arc::new(make_cm_from_addr(addr));
        let map: IMap<String, String> = IMap::new("test".to_string(), cm);

        let _get_handle: tokio::task::JoinHandle<Result<Option<String>>> =
            map.get_async("key".to_string());
        let _put_handle: tokio::task::JoinHandle<Result<Option<String>>> =
            map.put_async("key".to_string(), "value".to_string());
        let _remove_handle: tokio::task::JoinHandle<Result<Option<String>>> =
            map.remove_async("key".to_string());
        let _contains_handle: tokio::task::JoinHandle<Result<bool>> =
            map.contains_key_async("key".to_string());
    }

    #[tokio::test]
    async fn test_bulk_async_methods_spawn_tasks() {
        use crate::connection::ConnectionManager;
        use std::net::SocketAddr;

        let addr: SocketAddr = "127.0.0.1:5701".parse().unwrap();
        let cm = Arc::new(make_cm_from_addr(addr));
        let map: IMap<String, String> = IMap::new("test".to_string(), cm);

        let mut entries = HashMap::new();
        entries.insert("key1".to_string(), "value1".to_string());

        let _put_all_handle: tokio::task::JoinHandle<Result<()>> =
            map.put_all_async(entries.clone());
        let _set_all_handle: tokio::task::JoinHandle<Result<()>> =
            map.set_all_async(entries);
    }

    #[test]
    fn test_put_all_async_returns_join_handle() {
        fn check_return_type<K, V>(_map: &IMap<K, V>)
        where
            K: Serializable + Deserializable + Send + Sync + Clone + 'static,
            V: Serializable + Deserializable + Send + Sync + 'static,
        {
            let _ = |m: &IMap<K, V>, e: HashMap<K, V>| -> tokio::task::JoinHandle<Result<()>> {
                m.put_all_async(e)
            };
        }
    }

    #[test]
    fn test_set_all_async_returns_join_handle() {
        fn check_return_type<K, V>(_map: &IMap<K, V>)
        where
            K: Serializable + Deserializable + Send + Sync + Clone + 'static,
            V: Serializable + Deserializable + Send + Sync + 'static,
        {
            let _ = |m: &IMap<K, V>, e: HashMap<K, V>| -> tokio::task::JoinHandle<Result<()>> {
                m.set_all_async(e)
            };
        }
    }

    #[tokio::test]
    async fn test_submit_to_key_spawns_task() {
        use crate::connection::ConnectionManager;
        use crate::proxy::EntryProcessor;
        use std::net::SocketAddr;

        struct TestProcessor;
        impl EntryProcessor for TestProcessor {
            type Output = i32;
        }
        impl Serializable for TestProcessor {
            fn serialize<W: DataOutput>(&self, _output: &mut W) -> hazelcast_core::Result<()> {
                Ok(())
            }
        }

        let addr: SocketAddr = "127.0.0.1:5701".parse().unwrap();
        let cm = Arc::new(make_cm_from_addr(addr));
        let map: IMap<String, i32> = IMap::new("test".to_string(), cm);

        let _handle: tokio::task::JoinHandle<Result<Option<i32>>> =
            map.submit_to_key("key".to_string(), TestProcessor);
    }
}
