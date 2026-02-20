//! Near-cache implementation for client-side caching.

use std::collections::HashMap;
use std::future::Future;
use std::hash::Hash;
use std::time::Instant;

use super::{EvictionPolicy, NearCacheConfig};

/// Statistics for near-cache operations.
#[derive(Debug, Clone, Default)]
pub struct NearCacheStats {
    hits: u64,
    misses: u64,
    evictions: u64,
    expirations: u64,
}

impl NearCacheStats {
    /// Returns the number of cache hits.
    pub fn hits(&self) -> u64 {
        self.hits
    }

    /// Returns the number of cache misses.
    pub fn misses(&self) -> u64 {
        self.misses
    }

    /// Returns the number of entries evicted due to capacity.
    pub fn evictions(&self) -> u64 {
        self.evictions
    }

    /// Returns the number of entries expired due to TTL or max-idle.
    pub fn expirations(&self) -> u64 {
        self.expirations
    }

    /// Returns the hit ratio (hits / total lookups).
    ///
    /// Returns `0.0` if no lookups have been performed.
    pub fn hit_ratio(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64
        }
    }
}

/// Statistics from a preload operation.
#[derive(Debug, Clone, Default)]
pub struct PreloadStats {
    entries_loaded: u64,
    entries_skipped: u64,
    batches_processed: u32,
}

impl PreloadStats {
    /// Returns the number of entries successfully loaded.
    pub fn entries_loaded(&self) -> u64 {
        self.entries_loaded
    }

    /// Returns the number of entries skipped (e.g., due to capacity).
    pub fn entries_skipped(&self) -> u64 {
        self.entries_skipped
    }

    /// Returns the number of batches processed.
    pub fn batches_processed(&self) -> u32 {
        self.batches_processed
    }
}

/// Internal cache entry with metadata for TTL and eviction tracking.
struct CacheEntry<V> {
    value: V,
    created_at: Instant,
    last_accessed: Instant,
    access_count: u64,
}

impl<V> CacheEntry<V> {
    fn new(value: V, now: Instant) -> Self {
        Self {
            value,
            created_at: now,
            last_accessed: now,
            access_count: 1,
        }
    }

    fn touch(&mut self, now: Instant) {
        self.last_accessed = now;
        self.access_count += 1;
    }
}

/// A client-side near-cache for reducing network round-trips.
///
/// The near-cache stores frequently accessed entries locally and provides
/// configurable TTL, max-idle expiration, and eviction policies.
pub struct NearCache<K, V> {
    config: NearCacheConfig,
    store: HashMap<K, CacheEntry<V>>,
    stats: NearCacheStats,
}

impl<K, V> std::fmt::Debug for NearCache<K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NearCache")
            .field("config", &self.config)
            .field("store_size", &self.store.len())
            .field("stats", &self.stats)
            .finish()
    }
}

impl<K, V> NearCache<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    /// Creates a new near-cache with the given configuration.
    pub fn new(config: NearCacheConfig) -> Self {
        Self {
            config,
            store: HashMap::new(),
            stats: NearCacheStats::default(),
        }
    }

    /// Returns a clone of the cached value if present and not expired.
    ///
    /// This method updates access statistics and touch time for LRU/LFU tracking.
    pub fn get(&mut self, key: &K) -> Option<V> {
        let now = Instant::now();

        // Check if entry exists
        if !self.store.contains_key(key) {
            self.stats.misses += 1;
            return None;
        }

        // Check expiration
        let is_expired = self
            .store
            .get(key)
            .map(|e| self.is_expired(e, now))
            .unwrap_or(false);

        if is_expired {
            self.store.remove(key);
            self.stats.expirations += 1;
            self.stats.misses += 1;
            return None;
        }

        // Touch and return cloned value
        self.stats.hits += 1;
        if let Some(entry) = self.store.get_mut(key) {
            entry.touch(now);
            return Some(entry.value.clone());
        }

        None
    }

    /// Inserts a value into the cache.
    ///
    /// If the cache is at capacity, an entry will be evicted according to
    /// the configured eviction policy. If the policy is `None`, the entry
    /// is not inserted when the cache is full.
    ///
    /// Returns `true` if the entry was inserted, `false` if rejected.
    pub fn put(&mut self, key: K, value: V) -> bool {
        let now = Instant::now();

        // Remove expired entries opportunistically
        self.remove_expired_entries(now);

        // Check if we need to evict (only for new keys)
        if !self.store.contains_key(&key) && self.store.len() >= self.config.max_size() as usize {
            if !self.evict_one() {
                return false;
            }
        }

        self.store.insert(key, CacheEntry::new(value, now));
        true
    }

    /// Removes an entry from the cache.
    pub fn invalidate(&mut self, key: &K) {
        self.store.remove(key);
    }

    /// Removes all entries from the cache.
    pub fn clear(&mut self) {
        self.store.clear();
    }

    /// Returns the number of entries in the cache.
    pub fn size(&self) -> usize {
        self.store.len()
    }

    /// Returns a copy of the current statistics.
    pub fn stats(&self) -> NearCacheStats {
        self.stats.clone()
    }

    /// Returns a reference to the cache configuration.
    pub fn config(&self) -> &NearCacheConfig {
        &self.config
    }

    /// Preloads entries for specific keys from an async data source.
    ///
    /// The `fetcher` function receives a batch of keys and should return
    /// the corresponding (key, value) pairs from the remote data source.
    /// Keys are processed in batches according to the configured batch size.
    ///
    /// # Arguments
    ///
    /// * `keys` - The keys to preload
    /// * `fetcher` - An async function that fetches values for a batch of keys
    ///
    /// # Returns
    ///
    /// Statistics about the preload operation.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let keys_to_preload = vec!["key1".to_string(), "key2".to_string()];
    /// let stats = cache.preload_keys(keys_to_preload, |batch_keys| async move {
    ///     // Fetch from cluster using getAll or similar
    ///     map.get_all(&batch_keys).await
    /// }).await?;
    /// ```
    pub async fn preload_keys<F, Fut, E>(
        &mut self,
        keys: Vec<K>,
        mut fetcher: F,
    ) -> Result<PreloadStats, E>
    where
        F: FnMut(Vec<K>) -> Fut,
        Fut: Future<Output = Result<Vec<(K, V)>, E>>,
    {
        let mut stats = PreloadStats::default();

        if keys.is_empty() {
            return Ok(stats);
        }

        let batch_size = self.config.preload_config().batch_size() as usize;

        for chunk in keys.chunks(batch_size) {
            let entries = fetcher(chunk.to_vec()).await?;
            stats.batches_processed += 1;

            for (key, value) in entries {
                if self.put(key, value) {
                    stats.entries_loaded += 1;
                } else {
                    stats.entries_skipped += 1;
                }
            }
        }

        Ok(stats)
    }

    /// Preloads all entries from an async data source.
    ///
    /// This is a convenience wrapper around [`preload`](Self::preload) that
    /// makes the intent clearer when preloading the entire dataset.
    ///
    /// The `fetcher` function is called repeatedly with the batch number (starting from 0)
    /// until it returns an empty vector, indicating no more data.
    ///
    /// # Arguments
    ///
    /// * `fetcher` - An async function that takes a batch number and returns entries
    ///
    /// # Returns
    ///
    /// Statistics about the preload operation.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let stats = cache.preload_all(|batch_num| async move {
    ///     map.get_entries_batch(batch_num).await
    /// }).await?;
    /// ```
    pub async fn preload_all<F, Fut, E>(&mut self, fetcher: F) -> Result<PreloadStats, E>
    where
        F: FnMut(u32) -> Fut,
        Fut: Future<Output = Result<Vec<(K, V)>, E>>,
    {
        self.preload(fetcher).await
    }

    /// Preloads entries into the cache from an async data source.
    ///
    /// The `fetcher` function is called repeatedly with the batch number (starting from 0)
    /// until it returns an empty vector, indicating no more data.
    ///
    /// # Arguments
    ///
    /// * `fetcher` - An async function that takes a batch number and returns a vector of (key, value) pairs.
    ///
    /// # Returns
    ///
    /// Statistics about the preload operation.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let stats = cache.preload(|batch_num| async move {
    ///     // Fetch batch from cluster
    ///     map.get_all_batch(batch_num, batch_size).await
    /// }).await;
    /// ```
    pub async fn preload<F, Fut, E>(&mut self, fetcher: F) -> Result<PreloadStats, E>
    where
        F: FnMut(u32) -> Fut,
        Fut: Future<Output = Result<Vec<(K, V)>, E>>,
    {
        let batch_size = self.config.preload_config().batch_size();
        self.preload_batched(fetcher, batch_size).await
    }

    /// Preloads entries with a custom batch size.
    ///
    /// Similar to `preload`, but allows overriding the configured batch size.
    pub async fn preload_batched<F, Fut, E>(
        &mut self,
        mut fetcher: F,
        _batch_size: u32,
    ) -> Result<PreloadStats, E>
    where
        F: FnMut(u32) -> Fut,
        Fut: Future<Output = Result<Vec<(K, V)>, E>>,
    {
        let mut stats = PreloadStats::default();
        let mut batch_num = 0u32;

        loop {
            let entries = fetcher(batch_num).await?;

            if entries.is_empty() {
                break;
            }

            stats.batches_processed += 1;

            for (key, value) in entries {
                if self.put(key, value) {
                    stats.entries_loaded += 1;
                } else {
                    stats.entries_skipped += 1;
                }
            }

            batch_num += 1;
        }

        Ok(stats)
    }

    /// Preloads entries from an iterator synchronously.
    ///
    /// This is useful when entries are already available in memory.
    ///
    /// # Returns
    ///
    /// Statistics about the preload operation.
    pub fn preload_entries(&mut self, entries: impl IntoIterator<Item = (K, V)>) -> PreloadStats {
        let mut stats = PreloadStats::default();
        stats.batches_processed = 1;

        for (key, value) in entries {
            if self.put(key, value) {
                stats.entries_loaded += 1;
            } else {
                stats.entries_skipped += 1;
            }
        }

        stats
    }

    /// Returns whether preloading is enabled for this cache.
    pub fn is_preload_enabled(&self) -> bool {
        self.config.preload_config().enabled()
    }

    fn is_expired(&self, entry: &CacheEntry<V>, now: Instant) -> bool {
        let ttl = self.config.time_to_live();
        let max_idle = self.config.max_idle();

        // Check TTL (zero = infinite)
        if !ttl.is_zero() && now.duration_since(entry.created_at) > ttl {
            return true;
        }

        // Check max-idle (zero = infinite)
        if !max_idle.is_zero() && now.duration_since(entry.last_accessed) > max_idle {
            return true;
        }

        false
    }

    fn remove_expired_entries(&mut self, now: Instant) {
        let expired_keys: Vec<K> = self
            .store
            .iter()
            .filter(|(_, entry)| self.is_expired(entry, now))
            .map(|(k, _)| k.clone())
            .collect();

        for key in expired_keys {
            self.store.remove(&key);
            self.stats.expirations += 1;
        }
    }

    fn evict_one(&mut self) -> bool {
        match self.config.eviction_policy() {
            EvictionPolicy::Lru => self.evict_lru(),
            EvictionPolicy::Lfu => self.evict_lfu(),
            EvictionPolicy::Random => self.evict_random(),
            EvictionPolicy::None => false,
        }
    }

    fn evict_lru(&mut self) -> bool {
        if self.store.is_empty() {
            return false;
        }

        let lru_key = self
            .store
            .iter()
            .min_by_key(|(_, entry)| entry.last_accessed)
            .map(|(k, _)| k.clone());

        if let Some(key) = lru_key {
            self.store.remove(&key);
            self.stats.evictions += 1;
            true
        } else {
            false
        }
    }

    fn evict_lfu(&mut self) -> bool {
        if self.store.is_empty() {
            return false;
        }

        let lfu_key = self
            .store
            .iter()
            .min_by_key(|(_, entry)| entry.access_count)
            .map(|(k, _)| k.clone());

        if let Some(key) = lfu_key {
            self.store.remove(&key);
            self.stats.evictions += 1;
            true
        } else {
            false
        }
    }

    fn evict_random(&mut self) -> bool {
        if self.store.is_empty() {
            return false;
        }

        // HashMap iteration order is arbitrary, making this effectively random
        let key = self.store.keys().next().cloned();

        if let Some(key) = key {
            self.store.remove(&key);
            self.stats.evictions += 1;
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::sleep;
    use std::time::Duration;

    fn make_config(name: &str) -> NearCacheConfig {
        NearCacheConfig::builder(name).build().unwrap()
    }

    #[test]
    fn test_put_and_get() {
        let config = make_config("test");
        let mut cache = NearCache::new(config);

        assert!(cache.put("key1".to_string(), "value1".to_string()));
        assert_eq!(cache.get(&"key1".to_string()), Some("value1".to_string()));
    }

    #[test]
    fn test_get_miss() {
        let config = make_config("test");
        let mut cache: NearCache<String, String> = NearCache::new(config);

        assert_eq!(cache.get(&"nonexistent".to_string()), None);
        assert_eq!(cache.stats().misses(), 1);
    }

    #[test]
    fn test_invalidate() {
        let config = make_config("test");
        let mut cache = NearCache::new(config);

        cache.put("key1".to_string(), "value1".to_string());
        cache.invalidate(&"key1".to_string());
        assert_eq!(cache.get(&"key1".to_string()), None);
    }

    #[test]
    fn test_clear() {
        let config = make_config("test");
        let mut cache = NearCache::new(config);

        cache.put("key1".to_string(), "value1".to_string());
        cache.put("key2".to_string(), "value2".to_string());
        cache.clear();
        assert_eq!(cache.size(), 0);
    }

    #[test]
    fn test_size() {
        let config = make_config("test");
        let mut cache = NearCache::new(config);

        assert_eq!(cache.size(), 0);
        cache.put("key1".to_string(), "value1".to_string());
        assert_eq!(cache.size(), 1);
        cache.put("key2".to_string(), "value2".to_string());
        assert_eq!(cache.size(), 2);
    }

    #[test]
    fn test_stats_hits_and_misses() {
        let config = make_config("test");
        let mut cache = NearCache::new(config);

        cache.put("key1".to_string(), "value1".to_string());

        cache.get(&"key1".to_string()); // hit
        cache.get(&"key1".to_string()); // hit
        cache.get(&"key2".to_string()); // miss

        let stats = cache.stats();
        assert_eq!(stats.hits(), 2);
        assert_eq!(stats.misses(), 1);
    }

    #[test]
    fn test_hit_ratio() {
        let mut stats = NearCacheStats::default();
        assert_eq!(stats.hit_ratio(), 0.0);

        stats.hits = 3;
        stats.misses = 1;
        assert!((stats.hit_ratio() - 0.75).abs() < f64::EPSILON);
    }

    #[test]
    fn test_eviction_lru() {
        let config = NearCacheConfig::builder("test")
            .max_size(2)
            .eviction_policy(EvictionPolicy::Lru)
            .build()
            .unwrap();

        let mut cache = NearCache::new(config);

        cache.put("key1".to_string(), "value1".to_string());
        sleep(Duration::from_millis(10));
        cache.put("key2".to_string(), "value2".to_string());

        // Access key1 to make it more recently used
        cache.get(&"key1".to_string());
        sleep(Duration::from_millis(10));

        // Adding key3 should evict key2 (least recently used)
        cache.put("key3".to_string(), "value3".to_string());

        assert!(cache.get(&"key1".to_string()).is_some());
        assert!(cache.get(&"key2".to_string()).is_none());
        assert!(cache.get(&"key3".to_string()).is_some());
        assert_eq!(cache.stats().evictions(), 1);
    }

    #[test]
    fn test_eviction_lfu() {
        let config = NearCacheConfig::builder("test")
            .max_size(2)
            .eviction_policy(EvictionPolicy::Lfu)
            .build()
            .unwrap();

        let mut cache = NearCache::new(config);

        cache.put("key1".to_string(), "value1".to_string());
        cache.put("key2".to_string(), "value2".to_string());

        // Access key1 multiple times to increase its frequency
        cache.get(&"key1".to_string());
        cache.get(&"key1".to_string());

        // Adding key3 should evict key2 (least frequently used)
        cache.put("key3".to_string(), "value3".to_string());

        assert!(cache.get(&"key1".to_string()).is_some());
        assert!(cache.get(&"key2".to_string()).is_none());
        assert!(cache.get(&"key3".to_string()).is_some());
        assert_eq!(cache.stats().evictions(), 1);
    }

    #[test]
    fn test_eviction_random() {
        let config = NearCacheConfig::builder("test")
            .max_size(2)
            .eviction_policy(EvictionPolicy::Random)
            .build()
            .unwrap();

        let mut cache = NearCache::new(config);

        cache.put("key1".to_string(), "value1".to_string());
        cache.put("key2".to_string(), "value2".to_string());
        cache.put("key3".to_string(), "value3".to_string());

        assert_eq!(cache.size(), 2);
        assert_eq!(cache.stats().evictions(), 1);
    }

    #[test]
    fn test_eviction_none_rejects_new_entries() {
        let config = NearCacheConfig::builder("test")
            .max_size(2)
            .eviction_policy(EvictionPolicy::None)
            .build()
            .unwrap();

        let mut cache = NearCache::new(config);

        assert!(cache.put("key1".to_string(), "value1".to_string()));
        assert!(cache.put("key2".to_string(), "value2".to_string()));
        assert!(!cache.put("key3".to_string(), "value3".to_string()));

        assert_eq!(cache.size(), 2);
        assert_eq!(cache.stats().evictions(), 0);
    }

    #[test]
    fn test_ttl_expiration() {
        let config = NearCacheConfig::builder("test")
            .time_to_live(Duration::from_millis(50))
            .build()
            .unwrap();

        let mut cache = NearCache::new(config);

        cache.put("key1".to_string(), "value1".to_string());
        assert!(cache.get(&"key1".to_string()).is_some());

        sleep(Duration::from_millis(60));

        assert!(cache.get(&"key1".to_string()).is_none());
        assert_eq!(cache.stats().expirations(), 1);
    }

    #[test]
    fn test_max_idle_expiration() {
        let config = NearCacheConfig::builder("test")
            .max_idle(Duration::from_millis(50))
            .build()
            .unwrap();

        let mut cache = NearCache::new(config);

        cache.put("key1".to_string(), "value1".to_string());

        // Keep accessing to prevent idle expiration
        sleep(Duration::from_millis(30));
        assert!(cache.get(&"key1".to_string()).is_some());
        sleep(Duration::from_millis(30));
        assert!(cache.get(&"key1".to_string()).is_some());

        // Now let it idle
        sleep(Duration::from_millis(60));
        assert!(cache.get(&"key1".to_string()).is_none());
        assert_eq!(cache.stats().expirations(), 1);
    }

    #[test]
    fn test_update_existing_key() {
        let config = make_config("test");
        let mut cache = NearCache::new(config);

        cache.put("key1".to_string(), "value1".to_string());
        cache.put("key1".to_string(), "value2".to_string());

        assert_eq!(cache.get(&"key1".to_string()), Some("value2".to_string()));
        assert_eq!(cache.size(), 1);
    }

    #[test]
    fn test_update_existing_key_at_capacity() {
        let config = NearCacheConfig::builder("test")
            .max_size(2)
            .eviction_policy(EvictionPolicy::None)
            .build()
            .unwrap();

        let mut cache = NearCache::new(config);

        cache.put("key1".to_string(), "value1".to_string());
        cache.put("key2".to_string(), "value2".to_string());

        // Update existing key should succeed even at capacity
        assert!(cache.put("key1".to_string(), "value1-updated".to_string()));
        assert_eq!(
            cache.get(&"key1".to_string()),
            Some("value1-updated".to_string())
        );
        assert_eq!(cache.size(), 2);
    }

    #[test]
    fn test_expired_entries_removed_on_put() {
        let config = NearCacheConfig::builder("test")
            .max_size(2)
            .time_to_live(Duration::from_millis(30))
            .eviction_policy(EvictionPolicy::None)
            .build()
            .unwrap();

        let mut cache = NearCache::new(config);

        cache.put("key1".to_string(), "value1".to_string());
        cache.put("key2".to_string(), "value2".to_string());

        // Wait for expiration
        sleep(Duration::from_millis(40));

        // New put should succeed because expired entries are cleaned up
        assert!(cache.put("key3".to_string(), "value3".to_string()));
        assert_eq!(cache.stats().expirations(), 2);
    }

    #[test]
    fn test_near_cache_is_send() {
        fn assert_send<T: Send>() {}
        assert_send::<NearCache<String, String>>();
    }

    #[test]
    fn test_stats_clone() {
        let mut stats = NearCacheStats::default();
        stats.hits = 10;
        stats.misses = 5;

        let cloned = stats.clone();
        assert_eq!(cloned.hits(), 10);
        assert_eq!(cloned.misses(), 5);
    }

    #[test]
    fn test_config_accessor() {
        let config = NearCacheConfig::builder("my-cache")
            .max_size(500)
            .build()
            .unwrap();

        let cache: NearCache<String, String> = NearCache::new(config);
        assert_eq!(cache.config().name(), "my-cache");
        assert_eq!(cache.config().max_size(), 500);
    }

    #[test]
    fn test_preload_entries() {
        let config = NearCacheConfig::builder("test")
            .preload_enabled(true)
            .build()
            .unwrap();
        let mut cache = NearCache::new(config);

        let entries = vec![
            ("key1".to_string(), "value1".to_string()),
            ("key2".to_string(), "value2".to_string()),
            ("key3".to_string(), "value3".to_string()),
        ];

        let stats = cache.preload_entries(entries);

        assert_eq!(stats.entries_loaded(), 3);
        assert_eq!(stats.entries_skipped(), 0);
        assert_eq!(stats.batches_processed(), 1);
        assert_eq!(cache.size(), 3);
        assert_eq!(cache.get(&"key1".to_string()), Some("value1".to_string()));
    }

    #[test]
    fn test_preload_entries_respects_capacity() {
        let config = NearCacheConfig::builder("test")
            .max_size(2)
            .eviction_policy(EvictionPolicy::None)
            .preload_enabled(true)
            .build()
            .unwrap();
        let mut cache = NearCache::new(config);

        let entries = vec![
            ("key1".to_string(), "value1".to_string()),
            ("key2".to_string(), "value2".to_string()),
            ("key3".to_string(), "value3".to_string()),
        ];

        let stats = cache.preload_entries(entries);

        assert_eq!(stats.entries_loaded(), 2);
        assert_eq!(stats.entries_skipped(), 1);
        assert_eq!(cache.size(), 2);
    }

    #[test]
    fn test_preload_entries_with_eviction() {
        let config = NearCacheConfig::builder("test")
            .max_size(2)
            .eviction_policy(EvictionPolicy::Lru)
            .preload_enabled(true)
            .build()
            .unwrap();
        let mut cache = NearCache::new(config);

        let entries = vec![
            ("key1".to_string(), "value1".to_string()),
            ("key2".to_string(), "value2".to_string()),
            ("key3".to_string(), "value3".to_string()),
        ];

        let stats = cache.preload_entries(entries);

        assert_eq!(stats.entries_loaded(), 3);
        assert_eq!(stats.entries_skipped(), 0);
        assert_eq!(cache.size(), 2);
    }

    #[tokio::test]
    async fn test_async_preload() {
        let config = NearCacheConfig::builder("test")
            .preload_enabled(true)
            .preload_batch_size(2)
            .build()
            .unwrap();
        let mut cache = NearCache::new(config);

        let batches: Vec<Vec<(String, String)>> = vec![
            vec![
                ("key1".to_string(), "value1".to_string()),
                ("key2".to_string(), "value2".to_string()),
            ],
            vec![("key3".to_string(), "value3".to_string())],
            vec![],
        ];

        let batches_clone = batches.clone();
        let mut batch_idx = 0usize;

        let result: Result<PreloadStats, String> = cache
            .preload(|_batch_num| {
                let batch = if batch_idx < batches_clone.len() {
                    batches_clone[batch_idx].clone()
                } else {
                    vec![]
                };
                batch_idx += 1;
                async move { Ok(batch) }
            })
            .await;

        let stats = result.unwrap();
        assert_eq!(stats.entries_loaded(), 3);
        assert_eq!(stats.batches_processed(), 2);
        assert_eq!(cache.size(), 3);
    }

    #[tokio::test]
    async fn test_async_preload_error_handling() {
        let config = NearCacheConfig::builder("test")
            .preload_enabled(true)
            .build()
            .unwrap();
        let mut cache: NearCache<String, String> = NearCache::new(config);

        let result: Result<PreloadStats, &str> = cache
            .preload(|batch_num| async move {
                if batch_num == 0 {
                    Ok(vec![("key1".to_string(), "value1".to_string())])
                } else {
                    Err("Simulated error")
                }
            })
            .await;

        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Simulated error");
        assert_eq!(cache.size(), 1);
    }

    #[test]
    fn test_is_preload_enabled() {
        let config = NearCacheConfig::builder("test")
            .preload_enabled(true)
            .build()
            .unwrap();
        let cache: NearCache<String, String> = NearCache::new(config);
        assert!(cache.is_preload_enabled());

        let config2 = NearCacheConfig::builder("test").build().unwrap();
        let cache2: NearCache<String, String> = NearCache::new(config2);
        assert!(!cache2.is_preload_enabled());
    }

    #[test]
    fn test_preload_stats_default() {
        let stats = PreloadStats::default();
        assert_eq!(stats.entries_loaded(), 0);
        assert_eq!(stats.entries_skipped(), 0);
        assert_eq!(stats.batches_processed(), 0);
    }

    #[test]
    fn test_preload_stats_clone() {
        let mut stats = PreloadStats::default();
        stats.entries_loaded = 10;
        stats.batches_processed = 2;

        let cloned = stats.clone();
        assert_eq!(cloned.entries_loaded(), 10);
        assert_eq!(cloned.batches_processed(), 2);
    }

    #[test]
    fn test_preload_empty_iterator() {
        let config = NearCacheConfig::builder("test")
            .preload_enabled(true)
            .build()
            .unwrap();
        let mut cache: NearCache<String, String> = NearCache::new(config);

        let stats = cache.preload_entries(std::iter::empty());

        assert_eq!(stats.entries_loaded(), 0);
        assert_eq!(stats.entries_skipped(), 0);
        assert_eq!(stats.batches_processed(), 1);
        assert_eq!(cache.size(), 0);
    }

    #[tokio::test]
    async fn test_preload_keys() {
        let config = NearCacheConfig::builder("test")
            .preload_enabled(true)
            .preload_batch_size(10)
            .build()
            .unwrap();
        let mut cache = NearCache::new(config);

        let keys = vec![
            "key1".to_string(),
            "key2".to_string(),
            "key3".to_string(),
        ];

        let result: Result<PreloadStats, String> = cache
            .preload_keys(keys, |batch_keys| async move {
                Ok(batch_keys
                    .into_iter()
                    .map(|k| {
                        let v = format!("value-{}", k);
                        (k, v)
                    })
                    .collect())
            })
            .await;

        let stats = result.unwrap();
        assert_eq!(stats.entries_loaded(), 3);
        assert_eq!(stats.entries_skipped(), 0);
        assert_eq!(stats.batches_processed(), 1);
        assert_eq!(cache.size(), 3);
        assert_eq!(
            cache.get(&"key1".to_string()),
            Some("value-key1".to_string())
        );
        assert_eq!(
            cache.get(&"key2".to_string()),
            Some("value-key2".to_string())
        );
        assert_eq!(
            cache.get(&"key3".to_string()),
            Some("value-key3".to_string())
        );
    }

    #[tokio::test]
    async fn test_preload_keys_respects_batch_size() {
        let config = NearCacheConfig::builder("test")
            .preload_enabled(true)
            .preload_batch_size(2)
            .build()
            .unwrap();
        let mut cache = NearCache::new(config);

        let keys = vec![
            "k1".to_string(),
            "k2".to_string(),
            "k3".to_string(),
            "k4".to_string(),
            "k5".to_string(),
        ];

        let batch_sizes = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let batch_sizes_clone = batch_sizes.clone();

        let result: Result<PreloadStats, String> = cache
            .preload_keys(keys, |batch_keys| {
                let batch_sizes = batch_sizes_clone.clone();
                async move {
                    batch_sizes.lock().unwrap().push(batch_keys.len());
                    Ok(batch_keys
                        .into_iter()
                        .map(|k| (k.clone(), k))
                        .collect())
                }
            })
            .await;

        let stats = result.unwrap();
        assert_eq!(stats.entries_loaded(), 5);
        assert_eq!(stats.batches_processed(), 3);

        let sizes = batch_sizes.lock().unwrap();
        assert_eq!(*sizes, vec![2, 2, 1]);
    }

    #[tokio::test]
    async fn test_preload_keys_empty() {
        let config = NearCacheConfig::builder("test")
            .preload_enabled(true)
            .build()
            .unwrap();
        let mut cache: NearCache<String, String> = NearCache::new(config);

        let result: Result<PreloadStats, String> = cache
            .preload_keys(vec![], |_batch_keys| async move {
                panic!("fetcher should not be called for empty keys")
            })
            .await;

        let stats = result.unwrap();
        assert_eq!(stats.entries_loaded(), 0);
        assert_eq!(stats.entries_skipped(), 0);
        assert_eq!(stats.batches_processed(), 0);
        assert_eq!(cache.size(), 0);
    }

    #[tokio::test]
    async fn test_preload_keys_respects_capacity() {
        let config = NearCacheConfig::builder("test")
            .max_size(2)
            .eviction_policy(super::EvictionPolicy::None)
            .preload_enabled(true)
            .preload_batch_size(10)
            .build()
            .unwrap();
        let mut cache = NearCache::new(config);

        let keys = vec![
            "k1".to_string(),
            "k2".to_string(),
            "k3".to_string(),
            "k4".to_string(),
        ];

        let result: Result<PreloadStats, String> = cache
            .preload_keys(keys, |batch_keys| async move {
                Ok(batch_keys
                    .into_iter()
                    .map(|k| (k.clone(), k))
                    .collect())
            })
            .await;

        let stats = result.unwrap();
        assert_eq!(stats.entries_loaded(), 2);
        assert_eq!(stats.entries_skipped(), 2);
        assert_eq!(cache.size(), 2);
    }

    #[tokio::test]
    async fn test_preload_keys_error_handling() {
        let config = NearCacheConfig::builder("test")
            .preload_enabled(true)
            .preload_batch_size(2)
            .build()
            .unwrap();
        let mut cache: NearCache<String, String> = NearCache::new(config);

        let keys = vec![
            "k1".to_string(),
            "k2".to_string(),
            "k3".to_string(),
        ];

        let call_count = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        let call_count_clone = call_count.clone();

        let result: Result<PreloadStats, &str> = cache
            .preload_keys(keys, |batch_keys| {
                let call_count = call_count_clone.clone();
                async move {
                    let count = call_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    if count == 0 {
                        Ok(batch_keys
                            .into_iter()
                            .map(|k| (k.clone(), k))
                            .collect())
                    } else {
                        Err("Simulated error on second batch")
                    }
                }
            })
            .await;

        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Simulated error on second batch");
        assert_eq!(cache.size(), 2);
    }

    #[tokio::test]
    async fn test_preload_all() {
        let config = NearCacheConfig::builder("test")
            .preload_enabled(true)
            .preload_batch_size(2)
            .build()
            .unwrap();
        let mut cache = NearCache::new(config);

        let batches: Vec<Vec<(String, String)>> = vec![
            vec![
                ("k1".to_string(), "v1".to_string()),
                ("k2".to_string(), "v2".to_string()),
            ],
            vec![("k3".to_string(), "v3".to_string())],
            vec![],
        ];

        let batch_idx = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let batches_clone = batches.clone();

        let result: Result<PreloadStats, String> = cache
            .preload_all(|_batch_num| {
                let batch_idx = batch_idx.clone();
                let batches = batches_clone.clone();
                async move {
                    let idx = batch_idx.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    Ok(batches.get(idx).cloned().unwrap_or_default())
                }
            })
            .await;

        let stats = result.unwrap();
        assert_eq!(stats.entries_loaded(), 3);
        assert_eq!(stats.batches_processed(), 2);
        assert_eq!(cache.size(), 3);
        assert_eq!(cache.get(&"k1".to_string()), Some("v1".to_string()));
        assert_eq!(cache.get(&"k2".to_string()), Some("v2".to_string()));
        assert_eq!(cache.get(&"k3".to_string()), Some("v3".to_string()));
    }

    #[tokio::test]
    async fn test_preload_keys_verifies_cache_populated() {
        let config = NearCacheConfig::builder("test")
            .preload_enabled(true)
            .build()
            .unwrap();
        let mut cache = NearCache::new(config);

        let keys: Vec<i32> = (1..=10).collect();

        let result: Result<PreloadStats, String> = cache
            .preload_keys(keys, |batch_keys| async move {
                Ok(batch_keys
                    .into_iter()
                    .map(|k| (k, k * 100))
                    .collect())
            })
            .await;

        result.unwrap();

        for i in 1..=10 {
            let cached_value = cache.get(&i);
            assert_eq!(cached_value, Some(i * 100), "Key {} should be cached", i);
        }

        let stats = cache.stats();
        assert_eq!(stats.hits(), 10);
        assert_eq!(stats.misses(), 0);
    }

    #[tokio::test]
    async fn test_preload_keys_partial_fetch() {
        let config = NearCacheConfig::builder("test")
            .preload_enabled(true)
            .preload_batch_size(10)
            .build()
            .unwrap();
        let mut cache = NearCache::new(config);

        let keys = vec![
            "exists1".to_string(),
            "missing".to_string(),
            "exists2".to_string(),
        ];

        let result: Result<PreloadStats, String> = cache
            .preload_keys(keys, |batch_keys| async move {
                Ok(batch_keys
                    .into_iter()
                    .filter(|k| k.starts_with("exists"))
                    .map(|k| {
                        let v = format!("value-{}", k);
                        (k, v)
                    })
                    .collect())
            })
            .await;

        let stats = result.unwrap();
        assert_eq!(stats.entries_loaded(), 2);
        assert_eq!(cache.size(), 2);

        assert!(cache.get(&"exists1".to_string()).is_some());
        assert!(cache.get(&"exists2".to_string()).is_some());
        assert!(cache.get(&"missing".to_string()).is_none());
    }
}
