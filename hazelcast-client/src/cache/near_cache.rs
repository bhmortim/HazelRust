//! Near-cache implementation for client-side caching.

use std::collections::HashMap;
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
}
