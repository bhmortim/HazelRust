# Getting Started with Hazelcast Rust Client

This guide walks you through connecting to a Hazelcast cluster and performing basic distributed map operations.

## Prerequisites

- Rust 1.70 or later
- A running Hazelcast cluster (local or remote)

## Adding the Dependency

Add `hazelcast-client` to your `Cargo.toml`:

```toml
[dependencies]
hazelcast-client = "0.1"
tokio = { version = "1", features = ["full"] }
```

## Connecting to a Cluster

### Basic Connection

```rust
use hazelcast_client::{ClientConfig, HazelcastClient};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a client configuration
    let config = ClientConfig::new()
        .cluster_name("dev")
        .addresses(vec!["127.0.0.1:5701"]);

    // Connect to the cluster
    let client = HazelcastClient::new(config).await?;

    println!("Connected to Hazelcast cluster!");

    // Shutdown gracefully
    client.shutdown().await?;
    Ok(())
}
```

### Connection with Multiple Members

For production environments, specify multiple cluster members for redundancy:

```rust
use hazelcast_client::{ClientConfig, HazelcastClient};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ClientConfig::new()
        .cluster_name("production")
        .addresses(vec![
            "192.168.1.10:5701",
            "192.168.1.11:5701",
            "192.168.1.12:5701",
        ]);

    let client = HazelcastClient::new(config).await?;
    client.shutdown().await?;
    Ok(())
}
```

## Working with Distributed Maps

The `IMap` is Hazelcast's distributed implementation of a concurrent map.

### Basic CRUD Operations

```rust
use hazelcast_client::{ClientConfig, HazelcastClient};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ClientConfig::new()
        .cluster_name("dev")
        .addresses(vec!["127.0.0.1:5701"]);

    let client = HazelcastClient::new(config).await?;

    // Get a distributed map
    let map = client.get_map::<String, String>("my-distributed-map").await?;

    // Put a key-value pair
    map.put("key1".to_string(), "value1".to_string()).await?;

    // Get a value
    if let Some(value) = map.get(&"key1".to_string()).await? {
        println!("Retrieved: {}", value);
    }

    // Check if key exists
    let exists = map.contains_key(&"key1".to_string()).await?;
    println!("Key exists: {}", exists);

    // Remove a key
    let removed = map.remove(&"key1".to_string()).await?;
    println!("Removed value: {:?}", removed);

    // Get map size
    let size = map.size().await?;
    println!("Map size: {}", size);

    client.shutdown().await?;
    Ok(())
}
```

### Put with Time-To-Live (TTL)

Entries can automatically expire after a specified duration:

```rust
use hazelcast_client::{ClientConfig, HazelcastClient};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = HazelcastClient::new(
        ClientConfig::new().addresses(vec!["127.0.0.1:5701"])
    ).await?;

    let map = client.get_map::<String, i64>("session-cache").await?;

    // Entry expires after 30 minutes
    map.put_with_ttl(
        "session:abc123".to_string(),
        1234567890_i64,
        Duration::from_secs(30 * 60),
    ).await?;

    client.shutdown().await?;
    Ok(())
}
```

### Bulk Operations

```rust
use hazelcast_client::{ClientConfig, HazelcastClient};
use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = HazelcastClient::new(
        ClientConfig::new().addresses(vec!["127.0.0.1:5701"])
    ).await?;

    let map = client.get_map::<String, i32>("inventory").await?;

    // Put multiple entries at once
    let mut entries = HashMap::new();
    entries.insert("item-001".to_string(), 100);
    entries.insert("item-002".to_string(), 250);
    entries.insert("item-003".to_string(), 75);
    map.put_all(entries).await?;

    // Get multiple entries
    let keys = vec![
        "item-001".to_string(),
        "item-002".to_string(),
    ];
    let results = map.get_all(&keys).await?;

    for (key, value) in results {
        println!("{}: {}", key, value);
    }

    // Clear all entries
    map.clear().await?;

    client.shutdown().await?;
    Ok(())
}
```

### Atomic Operations

```rust
use hazelcast_client::{ClientConfig, HazelcastClient};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = HazelcastClient::new(
        ClientConfig::new().addresses(vec!["127.0.0.1:5701"])
    ).await?;

    let map = client.get_map::<String, i32>("counters").await?;

    // Put if absent - only inserts if key doesn't exist
    let previous = map.put_if_absent("counter".to_string(), 0).await?;
    println!("Previous value: {:?}", previous);

    // Replace - only updates if key exists
    let old_value = map.replace("counter".to_string(), 1).await?;
    println!("Old value: {:?}", old_value);

    // Compare and set - atomic conditional update
    let success = map.replace_if_same(
        "counter".to_string(),
        1,  // expected current value
        2,  // new value
    ).await?;
    println!("CAS succeeded: {}", success);

    // Remove if value matches
    let removed = map.remove_if_same("counter".to_string(), 2).await?;
    println!("Removed: {}", removed);

    client.shutdown().await?;
    Ok(())
}
```

## Entry Listeners

React to map changes in real-time:

```rust
use hazelcast_client::{ClientConfig, HazelcastClient};
use hazelcast_client::map::{EntryEvent, EntryListener};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = HazelcastClient::new(
        ClientConfig::new().addresses(vec!["127.0.0.1:5701"])
    ).await?;

    let map = client.get_map::<String, String>("events-demo").await?;

    // Register a listener for all entry events
    let listener = EntryListener::new()
        .on_added(|event: EntryEvent<String, String>| {
            println!("Entry added: {} = {}", event.key, event.value.unwrap());
        })
        .on_updated(|event: EntryEvent<String, String>| {
            println!(
                "Entry updated: {} changed from {:?} to {:?}",
                event.key, event.old_value, event.value
            );
        })
        .on_removed(|event: EntryEvent<String, String>| {
            println!("Entry removed: {}", event.key);
        })
        .include_value(true);

    let registration_id = map.add_entry_listener(listener).await?;
    println!("Listener registered: {}", registration_id);

    // Trigger some events
    map.put("greeting".to_string(), "Hello".to_string()).await?;
    map.put("greeting".to_string(), "Hello, World!".to_string()).await?;
    map.remove(&"greeting".to_string()).await?;

    // Allow time for events to process
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    // Remove the listener when done
    map.remove_entry_listener(&registration_id).await?;

    client.shutdown().await?;
    Ok(())
}
```

## Using Custom Types

Implement `Serialize` and `Deserialize` for your domain objects:

```rust
use hazelcast_client::{ClientConfig, HazelcastClient};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct User {
    id: u64,
    username: String,
    email: String,
    active: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = HazelcastClient::new(
        ClientConfig::new().addresses(vec!["127.0.0.1:5701"])
    ).await?;

    let users = client.get_map::<u64, User>("users").await?;

    let user = User {
        id: 1,
        username: "alice".to_string(),
        email: "alice@example.com".to_string(),
        active: true,
    };

    users.put(user.id, user.clone()).await?;

    if let Some(retrieved) = users.get(&1).await? {
        println!("Found user: {:?}", retrieved);
    }

    client.shutdown().await?;
    Ok(())
}
```

## Next Steps

- [Distributed Caching](distributed-caching.md) - Configure Near Cache for low-latency reads
- [SQL Queries](sql-queries.md) - Query your data with SQL
- [Transactions](transactions.md) - Execute atomic multi-map operations
- [Cluster Failover](cluster-failover.md) - Configure resilience and failover
