//! Near-cache configuration and implementation for client-side caching.

mod near_cache;
mod query_cache;

pub use near_cache::{NearCache, NearCacheStats, PreloadStats};
pub use query_cache::{QueryCache, QueryCacheConfig, QueryCacheConfigBuilder, QueryCacheStats};
pub(crate) use query_cache::StatsTracker;
// Note: EvictionPolicy, InMemoryFormat, NearCacheConfig, NearCacheConfigBuilder,
// PreloadConfig, PreloadConfigBuilder are defined below in this module and already public.

use std::time::Duration;

use crate::config::ConfigError;

/// Default time-to-live for near-cache entries (0 = infinite).
const DEFAULT_TTL: Duration = Duration::ZERO;
/// Default max idle time for near-cache entries (0 = infinite).
const DEFAULT_MAX_IDLE: Duration = Duration::ZERO;
/// Default maximum size of the near-cache.
const DEFAULT_MAX_SIZE: u32 = 10_000;

/// In-memory storage format for near-cache entries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum InMemoryFormat {
    /// Store entries in serialized binary form. Lower memory overhead, but
    /// requires deserialization on each access.
    #[default]
    Binary,
    /// Store entries as deserialized objects. Faster access, but higher
    /// memory usage and requires the value type to be `Clone`.
    Object,
    /// Store entries in the same format as the cluster member's native memory.
    ///
    /// This format avoids serialization/deserialization overhead by keeping
    /// entries in the internal representation used by the Hazelcast cluster.
    /// It is most useful when the near-cache is tightly integrated with the
    /// member's storage engine.
    Native,
}

/// Eviction policy for near-cache when max size is reached.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum EvictionPolicy {
    /// Least Recently Used - evicts entries that haven't been accessed recently.
    #[default]
    Lru,
    /// Least Frequently Used - evicts entries with the lowest access count.
    Lfu,
    /// Random - evicts entries randomly.
    Random,
    /// None - no eviction; new entries are rejected when cache is full.
    None,
}

/// Configuration for near-cache preloading.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreloadConfig {
    enabled: bool,
    batch_size: u32,
    store_initial_value: bool,
}

impl Default for PreloadConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            batch_size: 1000,
            store_initial_value: true,
        }
    }
}

impl PreloadConfig {
    /// Creates a new preload configuration builder.
    pub fn builder() -> PreloadConfigBuilder {
        PreloadConfigBuilder::default()
    }

    /// Returns whether preloading is enabled.
    pub fn enabled(&self) -> bool {
        self.enabled
    }

    /// Returns the batch size for preloading operations.
    pub fn batch_size(&self) -> u32 {
        self.batch_size
    }

    /// Returns whether to store the initial value during preload.
    pub fn store_initial_value(&self) -> bool {
        self.store_initial_value
    }
}

/// Builder for `PreloadConfig`.
#[derive(Debug, Clone, Default)]
pub struct PreloadConfigBuilder {
    enabled: Option<bool>,
    batch_size: Option<u32>,
    store_initial_value: Option<bool>,
}

impl PreloadConfigBuilder {
    /// Enables or disables preloading.
    pub fn enabled(mut self, enabled: bool) -> Self {
        self.enabled = Some(enabled);
        self
    }

    /// Sets the batch size for preloading.
    ///
    /// Default is 1000 entries per batch.
    pub fn batch_size(mut self, size: u32) -> Self {
        self.batch_size = Some(size);
        self
    }

    /// Sets whether to store the initial value during preload.
    ///
    /// When true, both key and value are stored. When false, only keys
    /// are tracked and values are fetched on first access.
    pub fn store_initial_value(mut self, store: bool) -> Self {
        self.store_initial_value = Some(store);
        self
    }

    /// Builds the preload configuration.
    pub fn build(self) -> PreloadConfig {
        PreloadConfig {
            enabled: self.enabled.unwrap_or(false),
            batch_size: self.batch_size.unwrap_or(1000),
            store_initial_value: self.store_initial_value.unwrap_or(true),
        }
    }
}

/// Configuration for a near-cache associated with a distributed map.
///
/// Near-caches provide local caching of frequently accessed entries to reduce
/// network round-trips and improve read performance.
#[derive(Debug, Clone)]
pub struct NearCacheConfig {
    name: String,
    in_memory_format: InMemoryFormat,
    time_to_live: Duration,
    max_idle: Duration,
    max_size: u32,
    eviction_policy: EvictionPolicy,
    invalidate_on_change: bool,
    serialize_keys: bool,
    preload_config: PreloadConfig,
    cache_local_entries: bool,
}

impl NearCacheConfig {
    /// Creates a new near-cache configuration builder.
    pub fn builder(name: impl Into<String>) -> NearCacheConfigBuilder {
        NearCacheConfigBuilder::new(name)
    }

    /// Returns the name pattern for this near-cache.
    ///
    /// The name can be an exact map name or a wildcard pattern (e.g., "user-*").
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the in-memory storage format.
    pub fn in_memory_format(&self) -> InMemoryFormat {
        self.in_memory_format
    }

    /// Returns the time-to-live duration for cached entries.
    ///
    /// A value of `Duration::ZERO` means entries never expire based on time.
    pub fn time_to_live(&self) -> Duration {
        self.time_to_live
    }

    /// Returns the maximum idle time for cached entries.
    ///
    /// A value of `Duration::ZERO` means entries never expire based on idle time.
    pub fn max_idle(&self) -> Duration {
        self.max_idle
    }

    /// Returns the maximum number of entries in the near-cache.
    pub fn max_size(&self) -> u32 {
        self.max_size
    }

    /// Returns the eviction policy used when the cache is full.
    pub fn eviction_policy(&self) -> EvictionPolicy {
        self.eviction_policy
    }

    /// Returns whether the near-cache is invalidated on remote changes.
    pub fn invalidate_on_change(&self) -> bool {
        self.invalidate_on_change
    }

    /// Returns whether keys should be stored in serialized form.
    pub fn serialize_keys(&self) -> bool {
        self.serialize_keys
    }

    /// Returns whether entries owned by the local member should be cached.
    ///
    /// When `true`, entries that are owned by the local member (in embedded mode)
    /// are also cached in the near-cache. This can improve read performance
    /// even for local entries by avoiding map-level locking.
    ///
    /// Default is `false`.
    pub fn cache_local_entries(&self) -> bool {
        self.cache_local_entries
    }

    /// Returns the preload configuration.
    pub fn preload_config(&self) -> &PreloadConfig {
        &self.preload_config
    }

    /// Checks if this configuration matches the given map name.
    ///
    /// Supports exact matches and simple wildcard patterns with `*` at the end.
    pub fn matches(&self, map_name: &str) -> bool {
        if self.name.ends_with('*') {
            let prefix = &self.name[..self.name.len() - 1];
            map_name.starts_with(prefix)
        } else {
            self.name == map_name
        }
    }
}

/// Builder for `NearCacheConfig`.
#[derive(Debug, Clone)]
pub struct NearCacheConfigBuilder {
    name: String,
    in_memory_format: Option<InMemoryFormat>,
    time_to_live: Option<Duration>,
    max_idle: Option<Duration>,
    max_size: Option<u32>,
    eviction_policy: Option<EvictionPolicy>,
    invalidate_on_change: Option<bool>,
    serialize_keys: Option<bool>,
    preload_config: Option<PreloadConfig>,
    cache_local_entries: Option<bool>,
}

impl NearCacheConfigBuilder {
    /// Creates a new near-cache configuration builder with the given name pattern.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            in_memory_format: None,
            time_to_live: None,
            max_idle: None,
            max_size: None,
            eviction_policy: None,
            invalidate_on_change: None,
            serialize_keys: None,
            preload_config: None,
            cache_local_entries: None,
        }
    }

    /// Sets the in-memory storage format.
    pub fn in_memory_format(mut self, format: InMemoryFormat) -> Self {
        self.in_memory_format = Some(format);
        self
    }

    /// Sets the time-to-live duration for cached entries.
    pub fn time_to_live(mut self, ttl: Duration) -> Self {
        self.time_to_live = Some(ttl);
        self
    }

    /// Sets the maximum idle time for cached entries.
    pub fn max_idle(mut self, max_idle: Duration) -> Self {
        self.max_idle = Some(max_idle);
        self
    }

    /// Sets the maximum number of entries in the near-cache.
    pub fn max_size(mut self, max_size: u32) -> Self {
        self.max_size = Some(max_size);
        self
    }

    /// Sets the eviction policy.
    pub fn eviction_policy(mut self, policy: EvictionPolicy) -> Self {
        self.eviction_policy = Some(policy);
        self
    }

    /// Sets whether the near-cache should be invalidated on remote changes.
    ///
    /// When enabled, the near-cache registers for cluster events and invalidates
    /// local entries when they are modified remotely.
    pub fn invalidate_on_change(mut self, invalidate: bool) -> Self {
        self.invalidate_on_change = Some(invalidate);
        self
    }

    /// Sets whether keys should be stored in serialized form.
    ///
    /// When enabled, keys are serialized before being used as cache keys,
    /// which can improve memory efficiency for complex key types.
    pub fn serialize_keys(mut self, serialize: bool) -> Self {
        self.serialize_keys = Some(serialize);
        self
    }

    /// Sets the preload configuration.
    pub fn preload_config(mut self, config: PreloadConfig) -> Self {
        self.preload_config = Some(config);
        self
    }

    /// Enables preloading with default settings.
    pub fn preload_enabled(mut self, enabled: bool) -> Self {
        let config = self.preload_config.take().unwrap_or_default();
        self.preload_config = Some(PreloadConfig {
            enabled,
            ..config
        });
        self
    }

    /// Sets the preload batch size.
    pub fn preload_batch_size(mut self, size: u32) -> Self {
        let config = self.preload_config.take().unwrap_or_default();
        self.preload_config = Some(PreloadConfig {
            batch_size: size,
            ..config
        });
        self
    }

    /// Sets whether entries owned by the local member should be cached.
    ///
    /// When `true`, entries that are owned by the local member (in embedded mode)
    /// are also cached in the near-cache. This can improve read performance
    /// even for local entries by avoiding map-level locking.
    ///
    /// Default is `false`.
    pub fn cache_local_entries(mut self, cache: bool) -> Self {
        self.cache_local_entries = Some(cache);
        self
    }

    /// Builds the near-cache configuration.
    ///
    /// # Errors
    ///
    /// Returns `ConfigError` if:
    /// - The name is empty
    /// - `max_size` is zero
    pub fn build(self) -> Result<NearCacheConfig, ConfigError> {
        if self.name.is_empty() {
            return Err(ConfigError::new("near-cache name must not be empty"));
        }

        let max_size = self.max_size.unwrap_or(DEFAULT_MAX_SIZE);
        if max_size == 0 {
            return Err(ConfigError::new("near-cache max_size must be greater than zero"));
        }

        Ok(NearCacheConfig {
            name: self.name,
            in_memory_format: self.in_memory_format.unwrap_or_default(),
            time_to_live: self.time_to_live.unwrap_or(DEFAULT_TTL),
            max_idle: self.max_idle.unwrap_or(DEFAULT_MAX_IDLE),
            max_size,
            eviction_policy: self.eviction_policy.unwrap_or_default(),
            invalidate_on_change: self.invalidate_on_change.unwrap_or(true),
            serialize_keys: self.serialize_keys.unwrap_or(true),
            preload_config: self.preload_config.unwrap_or_default(),
            cache_local_entries: self.cache_local_entries.unwrap_or(false),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_near_cache_config_defaults() {
        let config = NearCacheConfig::builder("test-map").build().unwrap();

        assert_eq!(config.name(), "test-map");
        assert_eq!(config.in_memory_format(), InMemoryFormat::Binary);
        assert_eq!(config.time_to_live(), Duration::ZERO);
        assert_eq!(config.max_idle(), Duration::ZERO);
        assert_eq!(config.max_size(), 10_000);
        assert_eq!(config.eviction_policy(), EvictionPolicy::Lru);
        assert!(config.invalidate_on_change());
        assert!(config.serialize_keys());
    }

    #[test]
    fn test_near_cache_config_custom_values() {
        let config = NearCacheConfig::builder("user-cache")
            .in_memory_format(InMemoryFormat::Object)
            .time_to_live(Duration::from_secs(300))
            .max_idle(Duration::from_secs(60))
            .max_size(5000)
            .eviction_policy(EvictionPolicy::Lfu)
            .invalidate_on_change(false)
            .serialize_keys(false)
            .build()
            .unwrap();

        assert_eq!(config.name(), "user-cache");
        assert_eq!(config.in_memory_format(), InMemoryFormat::Object);
        assert_eq!(config.time_to_live(), Duration::from_secs(300));
        assert_eq!(config.max_idle(), Duration::from_secs(60));
        assert_eq!(config.max_size(), 5000);
        assert_eq!(config.eviction_policy(), EvictionPolicy::Lfu);
        assert!(!config.invalidate_on_change());
        assert!(!config.serialize_keys());
    }

    #[test]
    fn test_near_cache_config_empty_name_fails() {
        let result = NearCacheConfig::builder("").build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("name must not be empty"));
    }

    #[test]
    fn test_near_cache_config_zero_max_size_fails() {
        let result = NearCacheConfig::builder("test").max_size(0).build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("max_size must be greater than zero"));
    }

    #[test]
    fn test_near_cache_config_exact_match() {
        let config = NearCacheConfig::builder("user-map").build().unwrap();

        assert!(config.matches("user-map"));
        assert!(!config.matches("user-map-backup"));
        assert!(!config.matches("other-map"));
    }

    #[test]
    fn test_near_cache_config_wildcard_match() {
        let config = NearCacheConfig::builder("user-*").build().unwrap();

        assert!(config.matches("user-"));
        assert!(config.matches("user-map"));
        assert!(config.matches("user-cache"));
        assert!(config.matches("user-data-store"));
        assert!(!config.matches("other-map"));
        assert!(!config.matches("users"));
    }

    #[test]
    fn test_in_memory_format_default() {
        assert_eq!(InMemoryFormat::default(), InMemoryFormat::Binary);
    }

    #[test]
    fn test_eviction_policy_default() {
        assert_eq!(EvictionPolicy::default(), EvictionPolicy::Lru);
    }

    #[test]
    fn test_eviction_policy_variants() {
        let policies = [
            EvictionPolicy::Lru,
            EvictionPolicy::Lfu,
            EvictionPolicy::Random,
            EvictionPolicy::None,
        ];

        for policy in policies {
            let config = NearCacheConfig::builder("test")
                .eviction_policy(policy)
                .build()
                .unwrap();
            assert_eq!(config.eviction_policy(), policy);
        }
    }

    #[test]
    fn test_in_memory_format_variants() {
        let formats = [InMemoryFormat::Binary, InMemoryFormat::Object];

        for format in formats {
            let config = NearCacheConfig::builder("test")
                .in_memory_format(format)
                .build()
                .unwrap();
            assert_eq!(config.in_memory_format(), format);
        }
    }

    #[test]
    fn test_near_cache_config_clone() {
        let config = NearCacheConfig::builder("test")
            .max_size(500)
            .eviction_policy(EvictionPolicy::Random)
            .build()
            .unwrap();

        let cloned = config.clone();
        assert_eq!(cloned.name(), config.name());
        assert_eq!(cloned.max_size(), config.max_size());
        assert_eq!(cloned.eviction_policy(), config.eviction_policy());
    }

    #[test]
    fn test_near_cache_config_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<NearCacheConfig>();
        assert_send_sync::<NearCacheConfigBuilder>();
        assert_send_sync::<InMemoryFormat>();
        assert_send_sync::<EvictionPolicy>();
    }

    #[test]
    fn test_builder_clone() {
        let builder = NearCacheConfigBuilder::new("test")
            .max_size(1000)
            .eviction_policy(EvictionPolicy::Lfu);

        let cloned = builder.clone();
        let config1 = builder.build().unwrap();
        let config2 = cloned.build().unwrap();

        assert_eq!(config1.max_size(), config2.max_size());
        assert_eq!(config1.eviction_policy(), config2.eviction_policy());
    }

    #[test]
    fn test_preload_config_defaults() {
        let config = PreloadConfig::default();
        assert!(!config.enabled());
        assert_eq!(config.batch_size(), 1000);
        assert!(config.store_initial_value());
    }

    #[test]
    fn test_preload_config_builder() {
        let config = PreloadConfig::builder()
            .enabled(true)
            .batch_size(500)
            .store_initial_value(false)
            .build();

        assert!(config.enabled());
        assert_eq!(config.batch_size(), 500);
        assert!(!config.store_initial_value());
    }

    #[test]
    fn test_near_cache_config_with_preload() {
        let preload = PreloadConfig::builder()
            .enabled(true)
            .batch_size(2000)
            .build();

        let config = NearCacheConfig::builder("test")
            .preload_config(preload)
            .build()
            .unwrap();

        assert!(config.preload_config().enabled());
        assert_eq!(config.preload_config().batch_size(), 2000);
    }

    #[test]
    fn test_near_cache_config_preload_shortcuts() {
        let config = NearCacheConfig::builder("test")
            .preload_enabled(true)
            .preload_batch_size(500)
            .build()
            .unwrap();

        assert!(config.preload_config().enabled());
        assert_eq!(config.preload_config().batch_size(), 500);
    }

    #[test]
    fn test_preload_config_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<PreloadConfig>();
        assert_send_sync::<PreloadConfigBuilder>();
    }
}
