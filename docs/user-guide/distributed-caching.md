# Distributed Caching with Near Cache

Near Cache is a local cache layer that stores frequently accessed data on the client side, dramatically reducing network round-trips and latency for read-heavy workloads.

## When to Use Near Cache

| Scenario | Near Cache Benefit |
|----------|-------------------|
| Read-heavy workloads | Eliminates network latency for cached entries |
| Frequently accessed data | Hot data served from local memory |
| Latency-sensitive applications | Sub-millisecond reads from local cache |
| Reference data | Rarely changing lookup tables |

## Basic Near Cache Configuration

```rust
use hazelcast_client::{ClientConfig, HazelcastClient};
use hazelcast_client::config::{NearCacheConfig, InMemoryFormat, EvictionPolicy};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let near_cache_config = NearCacheConfig::new("products")
        .in_memory_format(InMemoryFormat::Binary)
        .max_size(10_000)
        .eviction_policy(EvictionPolicy::Lru)
        .time_to_live_seconds(300)
        .max_idle_seconds(60);

    let config = ClientConfig::builder()
        .add_address("127.0.0.1:5701".parse()?)
        .add_near_cache_config(near_cache_config)
        .build()?;

    let client = HazelcastClient::new(config).await?;

    // The map automatically uses Near Cache
    let products = client.get_map::<String, String>("products");

    // First read - fetches from cluster
    let _ = products.get(&"SKU-001".to_string()).await?;

    // Subsequent reads - served from local Near Cache
    let _ = products.get(&"SKU-001".to_string()).await?;

    client.shutdown().await?;
    Ok(())
}
```

## Configuration Options

### In-Memory Format

| Format | Description | Use Case |
|--------|-------------|----------|
| `Binary` | Stores serialized bytes | Lower memory, faster replication |
| `Object` | Stores deserialized objects | Faster reads, higher memory |

```rust
use hazelcast_client::config::{NearCacheConfig, InMemoryFormat};

// Binary format - recommended for most cases
let binary_cache = NearCacheConfig::new("cache-a")
    .in_memory_format(InMemoryFormat::Binary);

// Object format - for extremely read-heavy scenarios
let object_cache = NearCacheConfig::new("cache-b")
    .in_memory_format(InMemoryFormat::Object);
```

### Eviction Policies

```rust
use hazelcast_client::config::{NearCacheConfig, EvictionPolicy};

// LRU - Least Recently Used (default)
let lru_cache = NearCacheConfig::new("lru-cache")
    .eviction_policy(EvictionPolicy::Lru)
    .max_size(5_000);

// LFU - Least Frequently Used
let lfu_cache = NearCacheConfig::new("lfu-cache")
    .eviction_policy(EvictionPolicy::Lfu)
    .max_size(5_000);

// Random eviction
let random_cache = NearCacheConfig::new("random-cache")
    .eviction_policy(EvictionPolicy::Random)
    .max_size(5_000);

// No eviction - use with caution!
let no_eviction = NearCacheConfig::new("bounded-cache")
    .eviction_policy(EvictionPolicy::None)
    .max_size(1_000);
```

### Expiration Settings

```rust
use hazelcast_client::config::NearCacheConfig;

let cache_config = NearCacheConfig::new("session-cache")
    // Entry expires 5 minutes after creation
    .time_to_live_seconds(300)
    // Entry expires if not accessed for 60 seconds
    .max_idle_seconds(60);
```

## Invalidation Strategies

Near Cache entries must be invalidated when the underlying data changes on the cluster.

### Automatic Invalidation (Recommended)

```rust
use hazelcast_client::config::NearCacheConfig;

let config = NearCacheConfig::new("realtime-cache")
    // Receive invalidation events from the cluster
    .invalidate_on_change(true);
```

### Manual Invalidation Control

For scenarios where you need explicit control:

```rust
use hazelcast_client::{ClientConfig, HazelcastClient};
use hazelcast_client::config::NearCacheConfig;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ClientConfig::builder()
        .add_address("127.0.0.1:5701".parse()?)
        .add_near_cache_config(
            NearCacheConfig::new("managed-cache")
                .invalidate_on_change(false)
        )
        .build()?;

    let client = HazelcastClient::new(config).await?;
    let map = client.get_map::<String, String>("managed-cache");

    // Manually clear the local Near Cache
    map.clear_near_cache();

    client.shutdown().await?;
    Ok(())
}
```

## Preloading Near Cache

For applications that benefit from a warm cache on startup:

```rust
use hazelcast_client::config::{NearCacheConfig, PreloadConfig};

let preload_config = PreloadConfig::new()
    .enabled(true)
    .store_initial_keys(true)
    .directory("/var/cache/hazelcast");

let near_cache = NearCacheConfig::new("preloaded-cache")
    .preload_config(preload_config)
    .max_size(50_000);
```

## Near Cache Statistics

Monitor Near Cache performance:

```rust
use hazelcast_client::{ClientConfig, HazelcastClient};
use hazelcast_client::config::NearCacheConfig;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ClientConfig::builder()
        .add_address("127.0.0.1:5701".parse()?)
        .add_near_cache_config(
            NearCacheConfig::new("monitored-cache")
                .max_size(10_000)
        )
        .build()?;

    let client = HazelcastClient::new(config).await?;
    let map = client.get_map::<String, String>("monitored-cache");

    // Perform some operations
    for i in 0..100 {
        let key = format!("key-{}", i);
        map.put(key.clone(), format!("value-{}", i)).await?;
        let _ = map.get(&key).await?;
        let _ = map.get(&key).await?; // Cache hit
    }

    // Retrieve statistics
    let stats = map.get_near_cache_stats()?;

    println!("Near Cache Statistics:");
    println!("  Hits: {}", stats.hits);
    println!("  Misses: {}", stats.misses);
    println!("  Hit ratio: {:.2}%", stats.hit_ratio() * 100.0);
    println!("  Owned entries: {}", stats.owned_entry_count);
    println!("  Memory cost: {} bytes", stats.owned_entry_memory_cost);

    client.shutdown().await?;
    Ok(())
}
```

## Pattern: Read-Through with Near Cache

Combine Near Cache with a loader pattern:

```rust
use hazelcast_client::{ClientConfig, HazelcastClient};
use hazelcast_client::config::NearCacheConfig;

async fn get_user_with_fallback(
    map: &hazelcast_client::map::IMap<String, String>,
    user_id: &str,
) -> Result<Option<String>, Box<dyn std::error::Error>> {
    // Try Near Cache first, then cluster
    if let Some(user) = map.get(&user_id.to_string()).await? {
        return Ok(Some(user));
    }

    // Load from external source (database, API, etc.)
    let user_data = load_from_database(user_id).await?;

    if let Some(ref data) = user_data {
        // Populate cache for future reads
        map.put(user_id.to_string(), data.clone()).await?;
    }

    Ok(user_data)
}

async fn load_from_database(user_id: &str) -> Result<Option<String>, Box<dyn std::error::Error>> {
    // Simulated database lookup
    Ok(Some(format!(r#"{{"id": "{}", "name": "User {}"}}"#, user_id, user_id)))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ClientConfig::builder()
        .add_address("127.0.0.1:5701".parse()?)
        .add_near_cache_config(
            NearCacheConfig::new("users")
                .max_size(10_000)
                .time_to_live_seconds(600)
        )
        .build()?;

    let client = HazelcastClient::new(config).await?;
    let users = client.get_map::<String, String>("users");

    // First call loads from DB and caches
    let user = get_user_with_fallback(&users, "user-123").await?;
    println!("User: {:?}", user);

    // Second call hits Near Cache
    let cached = get_user_with_fallback(&users, "user-123").await?;
    println!("Cached: {:?}", cached);

    client.shutdown().await?;
    Ok(())
}
```

## Best Practices

1. **Size appropriately**: Set `max_size` based on available client memory
2. **Use Binary format**: Unless you have extreme read-to-write ratios (>1000:1)
3. **Enable invalidation**: Always use `invalidate_on_change(true)` for consistency
4. **Monitor hit ratios**: Target >90% hit ratio for optimal benefit
5. **Consider TTL**: Set reasonable expiration to prevent stale data

## Next Steps

- [SQL Queries](sql-queries.md) - Query cached data with SQL
- [Cluster Failover](cluster-failover.md) - Ensure cache availability during failures
