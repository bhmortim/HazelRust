//! Example: Near Cache for client-side caching.
//!
//! Demonstrates configuring and using Near Cache for reduced read latency.
//! Near Cache keeps a local copy of frequently accessed entries so that
//! reads hit local memory instead of crossing the network.
//!
//! Run with: `cargo run --example near_cache`
//!
//! Requires a Hazelcast cluster running on localhost:5701.

use hazelcast_client::{ClientConfig, EvictionPolicy, HazelcastClient, InMemoryFormat, NearCacheConfig};
use std::time::{Duration, Instant};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Configure near cache for the "products" map
    let near_cache_config = NearCacheConfig::builder("products")
        .max_size(1000)
        .eviction_policy(EvictionPolicy::Lru)
        .time_to_live(Duration::from_secs(60))
        .in_memory_format(InMemoryFormat::Binary)
        .build()?;

    let config = ClientConfig::builder()
        .cluster_name("dev")
        .add_address("127.0.0.1:5701".parse()?)
        .add_near_cache_config(near_cache_config)
        .build()?;

    let client = HazelcastClient::new(config).await?;
    let map = client.get_map::<String, String>("products");

    // Populate the map
    println!("Populating map with sample data...");
    for i in 0..100 {
        map.put(format!("product:{}", i), format!("Product {} Description", i))
            .await?;
    }

    // Warm up the near cache
    println!("Warming up near cache...\n");
    for i in 0..100 {
        let _ = map.get(&format!("product:{}", i)).await?;
    }

    // Benchmark: Compare cache hits vs misses
    let iterations = 1000;

    // Cached reads (should be fast — hitting local near cache)
    println!("--- Cached Reads (Near Cache Hits) ---");
    let start = Instant::now();
    for _ in 0..iterations {
        for i in 0..10 {
            let _ = map.get(&format!("product:{}", i)).await?;
        }
    }
    let cached_duration = start.elapsed();
    println!(
        "  {} reads in {:?} ({:.2} ops/sec)",
        iterations * 10,
        cached_duration,
        (iterations * 10) as f64 / cached_duration.as_secs_f64()
    );

    // Show near cache statistics
    if let Some(stats) = map.get_near_cache_stats() {
        println!("\n--- Near Cache Statistics ---");
        println!("  Hits:        {}", stats.hits);
        println!("  Misses:      {}", stats.misses);
        println!("  Hit Ratio:   {:.1}%", stats.hit_ratio() * 100.0);
        println!("  Owned:       {} entries", stats.owned_entry_count);
        println!("  Memory:      {} bytes", stats.owned_entry_memory);
        println!("  Evictions:   {}", stats.evictions);
        println!("  Expirations: {}", stats.expirations);
    }

    // Demonstrate invalidation
    println!("\n--- Invalidation Demo ---");
    println!("Updating product:0 on server...");
    map.put("product:0".into(), "UPDATED Product 0".into())
        .await?;

    // With invalidate_on_change=true, the near cache entry is invalidated
    tokio::time::sleep(Duration::from_millis(100)).await;

    let value = map.get(&"product:0".into()).await?;
    println!("After invalidation, product:0 = {:?}", value);

    // Demonstrate TTL expiration
    println!("\n--- TTL Expiration Demo ---");
    println!("Near cache entries expire after 60 seconds TTL");

    // Eviction policy demonstration
    println!("\n--- Eviction Policy: LRU ---");
    println!("When max_size (1000) is reached, least recently used entries are evicted");

    // Clean up
    map.clear().await?;
    client.shutdown().await?;

    println!("\nDone!");
    Ok(())
}
