//! Basic usage example demonstrating IMap and IQueue operations.
//!
//! Run with: `cargo run --example basic_usage`
//!
//! Requires a Hazelcast cluster running on localhost:5701.

use std::time::Duration;

use hazelcast_client::{ClientConfigBuilder, HazelcastClient};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    println!("=== Hazelcast Client Basic Usage Example ===\n");

    let config = ClientConfigBuilder::new()
        .cluster_name("dev")
        .add_address("127.0.0.1:5701".parse()?)
        .connection_timeout(Duration::from_secs(10))
        .build()?;

    println!("Connecting to Hazelcast cluster...");
    let client = HazelcastClient::new(config).await?;
    println!(
        "Connected to cluster '{}' with {} connection(s)\n",
        client.cluster_name(),
        client.connection_count().await
    );

    // ========== IMap Operations ==========
    println!("--- IMap Operations ---\n");

    let map = client.get_map::<String, String>("example-map");

    println!("Putting entries into map...");
    map.put("key1".to_string(), "value1".to_string()).await?;
    map.put("key2".to_string(), "value2".to_string()).await?;
    map.put("key3".to_string(), "value3".to_string()).await?;

    println!("Map size: {}", map.size().await?);

    println!("\nRetrieving entries:");
    for key in ["key1", "key2", "key3", "nonexistent"] {
        let value = map.get(&key.to_string()).await?;
        match value {
            Some(v) => println!("  {} -> {}", key, v),
            None => println!("  {} -> (not found)", key),
        }
    }

    println!("\nChecking key existence:");
    println!(
        "  contains 'key1': {}",
        map.contains_key(&"key1".to_string()).await?
    );
    println!(
        "  contains 'missing': {}",
        map.contains_key(&"missing".to_string()).await?
    );

    println!("\nRemoving 'key2'...");
    let removed = map.remove(&"key2".to_string()).await?;
    println!("  Removed value: {:?}", removed);
    println!("  Map size after removal: {}", map.size().await?);

    println!("\nClearing map...");
    map.clear().await?;
    println!("  Map size after clear: {}", map.size().await?);

    // ========== IQueue Operations ==========
    println!("\n--- IQueue Operations ---\n");

    let queue = client.get_queue::<String>("example-queue");

    println!("Offering items to queue...");
    queue.offer("first".to_string()).await?;
    queue.offer("second".to_string()).await?;
    queue.offer("third".to_string()).await?;

    println!("Queue size: {}", queue.size().await?);
    println!("Queue is empty: {}", queue.is_empty().await?);

    println!("\nPeeking at head (doesn't remove):");
    let peeked = queue.peek().await?;
    println!("  Peeked: {:?}", peeked);
    println!("  Size after peek: {}", queue.size().await?);

    println!("\nPolling items (removes from queue):");
    while let Some(item) = queue.poll().await? {
        println!("  Polled: {}", item);
    }
    println!("  Queue is now empty: {}", queue.is_empty().await?);

    println!("\nTesting poll with timeout on empty queue...");
    let start = std::time::Instant::now();
    let result = queue.poll_timeout(Duration::from_secs(2)).await?;
    let elapsed = start.elapsed();
    println!(
        "  Result after {:?}: {:?}",
        elapsed, result
    );

    // ========== Cleanup ==========
    println!("\n--- Cleanup ---\n");
    println!("Shutting down client...");
    client.shutdown().await?;
    println!("Client shutdown complete.");

    println!("\n=== Example Complete ===");
    Ok(())
}
