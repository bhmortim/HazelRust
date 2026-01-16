//! Continuous Query Cache implementation for client-side caching with predicate filtering.

use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use hazelcast_core::serialization::{ObjectDataInput, ObjectDataOutput};
use hazelcast_core::{Deserializable, Result, Serializable};

use crate::listener::{EntryEvent, EntryEventType, ListenerRegistration};

/// Statistics for a QueryCache instance.
#[derive(Debug, Clone, Default)]
pub struct QueryCacheStats {
    hits: u64,
    misses: u64,
    event_count: u64,
}

impl QueryCacheStats {
    /// Creates new empty statistics.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the number of cache hits.
    pub fn hits(&self) -> u64 {
        self.hits
    }

    /// Returns the number of cache misses.
    pub fn misses(&self) -> u64 {
        self.misses
    }

    /// Returns the total number of events processed.
    pub fn event_count(&self) -> u64 {
        self.event_count
    }

    /// Returns the hit ratio (0.0 to 1.0).
    pub fn hit_ratio(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64
        }
    }
}

/// Internal statistics tracker with atomic counters.
#[derive(Debug)]
pub(crate) struct StatsTracker {
    hits: AtomicU64,
    misses: AtomicU64,
    event_count: AtomicU64,
}

impl StatsTracker {
    pub(crate) fn new() -> Self {
        Self {
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            event_count: AtomicU64::new(0),
        }
    }

    pub(crate) fn record_hit(&self) {
        self.hits.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_miss(&self) {
        self.misses.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_event(&self) {
        self.event_count.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn snapshot(&self) -> QueryCacheStats {
        QueryCacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            event_count: self.event_count.load(Ordering::Relaxed),
        }
    }
}

impl Default for StatsTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// Configuration for a QueryCache.
#[derive(Debug, Clone)]
pub struct QueryCacheConfig {
    include_value: bool,
    populate: bool,
    coalesce: bool,
    delay_seconds: u32,
    batch_size: u32,
    in_memory_format: super::InMemoryFormat,
}

impl Default for QueryCacheConfig {
    fn default() -> Self {
        Self {
            include_value: true,
            populate: true,
            coalesce: false,
            delay_seconds: 0,
            batch_size: 1000,
            in_memory_format: super::InMemoryFormat::Binary,
        }
    }
}

impl QueryCacheConfig {
    /// Creates a new configuration with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a builder for QueryCacheConfig.
    pub fn builder() -> QueryCacheConfigBuilder {
        QueryCacheConfigBuilder::default()
    }

    /// Returns whether values are included in the cache.
    pub fn include_value(&self) -> bool {
        self.include_value
    }

    /// Returns whether to populate on creation.
    pub fn populate(&self) -> bool {
        self.populate
    }

    /// Returns whether event coalescing is enabled.
    pub fn coalesce(&self) -> bool {
        self.coalesce
    }

    /// Returns the coalescing delay in seconds.
    pub fn delay_seconds(&self) -> u32 {
        self.delay_seconds
    }

    /// Returns the batch size for initial population.
    pub fn batch_size(&self) -> u32 {
        self.batch_size
    }

    /// Returns the in-memory format.
    pub fn in_memory_format(&self) -> super::InMemoryFormat {
        self.in_memory_format
    }
}

/// Builder for QueryCacheConfig.
#[derive(Debug, Clone, Default)]
pub struct QueryCacheConfigBuilder {
    include_value: Option<bool>,
    populate: Option<bool>,
    coalesce: Option<bool>,
    delay_seconds: Option<u32>,
    batch_size: Option<u32>,
    in_memory_format: Option<super::InMemoryFormat>,
}

impl QueryCacheConfigBuilder {
    /// Sets whether to include values in the cache.
    pub fn include_value(mut self, include: bool) -> Self {
        self.include_value = Some(include);
        self
    }

    /// Sets whether to populate the cache on creation.
    pub fn populate(mut self, populate: bool) -> Self {
        self.populate = Some(populate);
        self
    }

    /// Sets whether to coalesce events.
    pub fn coalesce(mut self, coalesce: bool) -> Self {
        self.coalesce = Some(coalesce);
        self
    }

    /// Sets the coalescing delay in seconds.
    pub fn delay_seconds(mut self, delay: u32) -> Self {
        self.delay_seconds = Some(delay);
        self
    }

    /// Sets the batch size for initial population.
    pub fn batch_size(mut self, size: u32) -> Self {
        self.batch_size = Some(size);
        self
    }

    /// Sets the in-memory format.
    pub fn in_memory_format(mut self, format: super::InMemoryFormat) -> Self {
        self.in_memory_format = Some(format);
        self
    }

    /// Builds the configuration.
    pub fn build(self) -> QueryCacheConfig {
        QueryCacheConfig {
            include_value: self.include_value.unwrap_or(true),
            populate: self.populate.unwrap_or(true),
            coalesce: self.coalesce.unwrap_or(false),
            delay_seconds: self.delay_seconds.unwrap_or(0),
            batch_size: self.batch_size.unwrap_or(1000),
            in_memory_format: self.in_memory_format.unwrap_or_default(),
        }
    }
}

/// A continuous query cache that maintains a local view of map entries matching a predicate.
///
/// The QueryCache subscribes to entry events from the underlying map and automatically
/// updates its local cache when entries are added, updated, or removed.
///
/// # Example
///
/// ```ignore
/// use hazelcast_client::query::Predicates;
///
/// // Get a query cache for active users
/// let predicate = Predicates::equal("status", &"active".to_string())?;
/// let cache = map.get_query_cache("active-users", &predicate, true).await?;
///
/// // Fast local access
/// let user = cache.get(&user_id);
/// let all_keys = cache.key_set();
/// ```
#[derive(Debug)]
pub struct QueryCache<K, V> {
    name: String,
    map_name: String,
    cache: Arc<RwLock<HashMap<Vec<u8>, Vec<u8>>>>,
    include_value: bool,
    stats: Arc<StatsTracker>,
    listener_registration: Arc<Mutex<Option<ListenerRegistration>>>,
    destroyed: AtomicBool,
    _phantom: PhantomData<fn() -> (K, V)>,
}

impl<K, V> QueryCache<K, V>
where
    K: Serializable + Deserializable + Eq + Hash + Clone + Send + Sync,
    V: Serializable + Deserializable + Clone + Send + Sync,
{
    /// Creates a new QueryCache (internal constructor).
    pub(crate) fn new(name: String, map_name: String, include_value: bool) -> Self {
        Self {
            name,
            map_name,
            cache: Arc::new(RwLock::new(HashMap::new())),
            include_value,
            stats: Arc::new(StatsTracker::new()),
            listener_registration: Arc::new(Mutex::new(None)),
            destroyed: AtomicBool::new(false),
            _phantom: PhantomData,
        }
    }

    /// Returns the name of this query cache.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the name of the underlying map.
    pub fn map_name(&self) -> &str {
        &self.map_name
    }

    /// Returns the value associated with the given key, if present.
    ///
    /// This is a local operation and does not access the cluster.
    pub fn get(&self, key: &K) -> Option<V> {
        if self.destroyed.load(Ordering::Acquire) {
            return None;
        }

        let key_data = Self::serialize_value(key).ok()?;
        let cache = self.cache.read().unwrap();

        if let Some(value_data) = cache.get(&key_data) {
            self.stats.record_hit();
            let mut input = ObjectDataInput::new(value_data);
            V::deserialize(&mut input).ok()
        } else {
            self.stats.record_miss();
            None
        }
    }

    /// Returns `true` if the cache contains an entry for the given key.
    ///
    /// This is a local operation and does not access the cluster.
    pub fn contains_key(&self, key: &K) -> bool {
        if self.destroyed.load(Ordering::Acquire) {
            return false;
        }

        if let Ok(key_data) = Self::serialize_value(key) {
            let cache = self.cache.read().unwrap();
            cache.contains_key(&key_data)
        } else {
            false
        }
    }

    /// Returns `true` if the cache contains the given value.
    ///
    /// This is a local operation that scans all cached values.
    pub fn contains_value(&self, value: &V) -> bool {
        if self.destroyed.load(Ordering::Acquire) {
            return false;
        }

        let value_data = match Self::serialize_value(value) {
            Ok(data) => data,
            Err(_) => return false,
        };

        let cache = self.cache.read().unwrap();
        cache.values().any(|v| *v == value_data)
    }

    /// Returns the number of entries in the cache.
    ///
    /// This is a local operation and does not access the cluster.
    pub fn size(&self) -> usize {
        if self.destroyed.load(Ordering::Acquire) {
            return 0;
        }

        let cache = self.cache.read().unwrap();
        cache.len()
    }

    /// Returns `true` if the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.size() == 0
    }

    /// Returns all entries for the given keys.
    ///
    /// This is a local operation and does not access the cluster.
    pub fn get_all(&self, keys: &[K]) -> HashMap<K, V> {
        if self.destroyed.load(Ordering::Acquire) {
            return HashMap::new();
        }

        let cache = self.cache.read().unwrap();
        let mut result = HashMap::new();

        for key in keys {
            if let Ok(key_data) = Self::serialize_value(key) {
                if let Some(value_data) = cache.get(&key_data) {
                    self.stats.record_hit();
                    let mut input = ObjectDataInput::new(value_data);
                    if let Ok(value) = V::deserialize(&mut input) {
                        result.insert(key.clone(), value);
                    }
                } else {
                    self.stats.record_miss();
                }
            }
        }

        result
    }

    /// Returns all values in the cache.
    ///
    /// This is a local operation and does not access the cluster.
    pub fn values(&self) -> Vec<V> {
        if self.destroyed.load(Ordering::Acquire) {
            return Vec::new();
        }

        let cache = self.cache.read().unwrap();
        cache
            .values()
            .filter_map(|value_data| {
                let mut input = ObjectDataInput::new(value_data);
                V::deserialize(&mut input).ok()
            })
            .collect()
    }

    /// Returns all keys in the cache.
    ///
    /// This is a local operation and does not access the cluster.
    pub fn key_set(&self) -> HashSet<K> {
        if self.destroyed.load(Ordering::Acquire) {
            return HashSet::new();
        }

        let cache = self.cache.read().unwrap();
        cache
            .keys()
            .filter_map(|key_data| {
                let mut input = ObjectDataInput::new(key_data);
                K::deserialize(&mut input).ok()
            })
            .collect()
    }

    /// Returns all entries in the cache as key-value pairs.
    ///
    /// This is a local operation and does not access the cluster.
    pub fn entry_set(&self) -> Vec<(K, V)> {
        if self.destroyed.load(Ordering::Acquire) {
            return Vec::new();
        }

        let cache = self.cache.read().unwrap();
        cache
            .iter()
            .filter_map(|(key_data, value_data)| {
                let mut key_input = ObjectDataInput::new(key_data);
                let mut value_input = ObjectDataInput::new(value_data);
                match (
                    K::deserialize(&mut key_input),
                    V::deserialize(&mut value_input),
                ) {
                    (Ok(key), Ok(value)) => Some((key, value)),
                    _ => None,
                }
            })
            .collect()
    }

    /// Returns the cache statistics.
    pub fn stats(&self) -> QueryCacheStats {
        self.stats.snapshot()
    }

    /// Returns `true` if this cache has been destroyed.
    pub fn is_destroyed(&self) -> bool {
        self.destroyed.load(Ordering::Acquire)
    }

    /// Returns `true` if values are included in this cache.
    pub fn include_value(&self) -> bool {
        self.include_value
    }

    /// Clears all entries from the local cache.
    ///
    /// This only affects the local cache and does not modify the underlying map.
    pub fn clear(&self) {
        if !self.destroyed.load(Ordering::Acquire) {
            let mut cache = self.cache.write().unwrap();
            cache.clear();
        }
    }

    /// Destroys this query cache and releases resources.
    ///
    /// After calling this method, the cache will no longer receive updates
    /// and all methods will return empty/default values.
    pub fn destroy(&self) {
        self.destroyed.store(true, Ordering::Release);
        let mut cache = self.cache.write().unwrap();
        cache.clear();

        let mut reg = self.listener_registration.lock().unwrap();
        if let Some(registration) = reg.take() {
            registration.deactivate();
        }
    }

    /// Handles an entry event by updating the local cache.
    pub(crate) fn handle_event(&self, event: &EntryEvent<K, V>) {
        if self.destroyed.load(Ordering::Acquire) {
            return;
        }

        self.stats.record_event();

        let key_data = match Self::serialize_value(&event.key) {
            Ok(data) => data,
            Err(_) => return,
        };

        let mut cache = self.cache.write().unwrap();

        match event.event_type {
            EntryEventType::Added | EntryEventType::Updated => {
                if let Some(ref value) = event.new_value {
                    if let Ok(value_data) = Self::serialize_value(value) {
                        cache.insert(key_data, value_data);
                    }
                }
            }
            EntryEventType::Removed | EntryEventType::Evicted | EntryEventType::Expired => {
                cache.remove(&key_data);
            }
            _ => {}
        }
    }

    /// Populates the cache with initial entries.
    pub(crate) fn populate(&self, entries: Vec<(K, V)>) {
        if self.destroyed.load(Ordering::Acquire) {
            return;
        }

        let mut cache = self.cache.write().unwrap();
        for (key, value) in entries {
            if let (Ok(key_data), Ok(value_data)) =
                (Self::serialize_value(&key), Self::serialize_value(&value))
            {
                cache.insert(key_data, value_data);
            }
        }
    }

    /// Sets the listener registration for this cache.
    pub(crate) fn set_listener_registration(&self, registration: ListenerRegistration) {
        let mut reg = self.listener_registration.lock().unwrap();
        *reg = Some(registration);
    }

    /// Returns a clone of the internal cache Arc for event handling.
    pub(crate) fn cache_handle(&self) -> Arc<RwLock<HashMap<Vec<u8>, Vec<u8>>>> {
        Arc::clone(&self.cache)
    }

    /// Returns a clone of the stats tracker for event handling.
    pub(crate) fn stats_handle(&self) -> Arc<StatsTracker> {
        Arc::clone(&self.stats)
    }

    fn serialize_value<T: Serializable>(value: &T) -> Result<Vec<u8>> {
        let mut output = ObjectDataOutput::new();
        value.serialize(&mut output)?;
        Ok(output.into_bytes())
    }
}

impl<K, V> Clone for QueryCache<K, V> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            map_name: self.map_name.clone(),
            cache: Arc::clone(&self.cache),
            include_value: self.include_value,
            stats: Arc::clone(&self.stats),
            listener_registration: Arc::clone(&self.listener_registration),
            destroyed: AtomicBool::new(self.destroyed.load(Ordering::Acquire)),
            _phantom: PhantomData,
        }
    }
}

unsafe impl<K: Send, V: Send> Send for QueryCache<K, V> {}
unsafe impl<K: Sync, V: Sync> Sync for QueryCache<K, V> {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_cache_stats_default() {
        let stats = QueryCacheStats::new();
        assert_eq!(stats.hits(), 0);
        assert_eq!(stats.misses(), 0);
        assert_eq!(stats.event_count(), 0);
        assert_eq!(stats.hit_ratio(), 0.0);
    }

    #[test]
    fn test_query_cache_stats_hit_ratio() {
        let stats = QueryCacheStats {
            hits: 80,
            misses: 20,
            event_count: 100,
        };
        assert_eq!(stats.hit_ratio(), 0.8);
    }

    #[test]
    fn test_query_cache_config_defaults() {
        let config = QueryCacheConfig::new();
        assert!(config.include_value());
        assert!(config.populate());
        assert!(!config.coalesce());
        assert_eq!(config.delay_seconds(), 0);
        assert_eq!(config.batch_size(), 1000);
    }

    #[test]
    fn test_query_cache_config_builder() {
        let config = QueryCacheConfig::builder()
            .include_value(false)
            .populate(false)
            .coalesce(true)
            .delay_seconds(5)
            .batch_size(500)
            .build();

        assert!(!config.include_value());
        assert!(!config.populate());
        assert!(config.coalesce());
        assert_eq!(config.delay_seconds(), 5);
        assert_eq!(config.batch_size(), 500);
    }

    #[test]
    fn test_query_cache_basic_operations() {
        let cache: QueryCache<String, String> =
            QueryCache::new("test-cache".to_string(), "test-map".to_string(), true);

        assert_eq!(cache.name(), "test-cache");
        assert_eq!(cache.map_name(), "test-map");
        assert!(cache.include_value());
        assert!(!cache.is_destroyed());
        assert!(cache.is_empty());
        assert_eq!(cache.size(), 0);
    }

    #[test]
    fn test_query_cache_populate() {
        let cache: QueryCache<String, i32> =
            QueryCache::new("test-cache".to_string(), "test-map".to_string(), true);

        let entries = vec![
            ("key1".to_string(), 100),
            ("key2".to_string(), 200),
            ("key3".to_string(), 300),
        ];

        cache.populate(entries);

        assert_eq!(cache.size(), 3);
        assert!(!cache.is_empty());
        assert!(cache.contains_key(&"key1".to_string()));
        assert!(cache.contains_key(&"key2".to_string()));
        assert!(cache.contains_key(&"key3".to_string()));
        assert!(!cache.contains_key(&"key4".to_string()));
    }

    #[test]
    fn test_query_cache_get() {
        let cache: QueryCache<String, i32> =
            QueryCache::new("test-cache".to_string(), "test-map".to_string(), true);

        cache.populate(vec![("key1".to_string(), 42)]);

        assert_eq!(cache.get(&"key1".to_string()), Some(42));
        assert_eq!(cache.get(&"key2".to_string()), None);

        let stats = cache.stats();
        assert_eq!(stats.hits(), 1);
        assert_eq!(stats.misses(), 1);
    }

    #[test]
    fn test_query_cache_get_all() {
        let cache: QueryCache<String, i32> =
            QueryCache::new("test-cache".to_string(), "test-map".to_string(), true);

        cache.populate(vec![("key1".to_string(), 100), ("key2".to_string(), 200)]);

        let keys = vec![
            "key1".to_string(),
            "key2".to_string(),
            "key3".to_string(),
        ];
        let result = cache.get_all(&keys);

        assert_eq!(result.len(), 2);
        assert_eq!(result.get(&"key1".to_string()), Some(&100));
        assert_eq!(result.get(&"key2".to_string()), Some(&200));
    }

    #[test]
    fn test_query_cache_values() {
        let cache: QueryCache<String, i32> =
            QueryCache::new("test-cache".to_string(), "test-map".to_string(), true);

        cache.populate(vec![("key1".to_string(), 100), ("key2".to_string(), 200)]);

        let values = cache.values();
        assert_eq!(values.len(), 2);
        assert!(values.contains(&100));
        assert!(values.contains(&200));
    }

    #[test]
    fn test_query_cache_key_set() {
        let cache: QueryCache<String, i32> =
            QueryCache::new("test-cache".to_string(), "test-map".to_string(), true);

        cache.populate(vec![("key1".to_string(), 100), ("key2".to_string(), 200)]);

        let keys = cache.key_set();
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&"key1".to_string()));
        assert!(keys.contains(&"key2".to_string()));
    }

    #[test]
    fn test_query_cache_entry_set() {
        let cache: QueryCache<String, i32> =
            QueryCache::new("test-cache".to_string(), "test-map".to_string(), true);

        cache.populate(vec![("key1".to_string(), 100), ("key2".to_string(), 200)]);

        let entries = cache.entry_set();
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn test_query_cache_contains_value() {
        let cache: QueryCache<String, i32> =
            QueryCache::new("test-cache".to_string(), "test-map".to_string(), true);

        cache.populate(vec![("key1".to_string(), 42)]);

        assert!(cache.contains_value(&42));
        assert!(!cache.contains_value(&999));
    }

    #[test]
    fn test_query_cache_clear() {
        let cache: QueryCache<String, i32> =
            QueryCache::new("test-cache".to_string(), "test-map".to_string(), true);

        cache.populate(vec![("key1".to_string(), 100), ("key2".to_string(), 200)]);

        assert_eq!(cache.size(), 2);
        cache.clear();
        assert_eq!(cache.size(), 0);
        assert!(cache.is_empty());
    }

    #[test]
    fn test_query_cache_destroy() {
        let cache: QueryCache<String, i32> =
            QueryCache::new("test-cache".to_string(), "test-map".to_string(), true);

        cache.populate(vec![("key1".to_string(), 100)]);

        assert!(!cache.is_destroyed());
        cache.destroy();
        assert!(cache.is_destroyed());
        assert_eq!(cache.size(), 0);
        assert!(cache.get(&"key1".to_string()).is_none());
    }

    #[test]
    fn test_query_cache_clone() {
        let cache: QueryCache<String, i32> =
            QueryCache::new("test-cache".to_string(), "test-map".to_string(), true);

        cache.populate(vec![("key1".to_string(), 100)]);

        let cloned = cache.clone();
        assert_eq!(cloned.name(), cache.name());
        assert_eq!(cloned.get(&"key1".to_string()), Some(100));

        cache.clear();
        assert_eq!(cloned.size(), 0);
    }

    #[test]
    fn test_query_cache_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<QueryCache<String, String>>();
        assert_send_sync::<QueryCacheStats>();
        assert_send_sync::<QueryCacheConfig>();
    }

    #[test]
    fn test_stats_tracker_concurrent() {
        let tracker = Arc::new(StatsTracker::new());

        for _ in 0..100 {
            tracker.record_hit();
            tracker.record_miss();
            tracker.record_event();
        }

        let snapshot = tracker.snapshot();
        assert_eq!(snapshot.hits(), 100);
        assert_eq!(snapshot.misses(), 100);
        assert_eq!(snapshot.event_count(), 100);
    }
}
