# hazelcast-client

Rust client library for [Hazelcast](https://hazelcast.com/) — the real-time data platform.

[![Crates.io](https://img.shields.io/crates/v/hazelcast-client.svg)](https://crates.io/crates/hazelcast-client)
[![Documentation](https://docs.rs/hazelcast-client/badge.svg)](https://docs.rs/hazelcast-client)
[![License](https://img.shields.io/crates/l/hazelcast-client.svg)](LICENSE)

## Quick Start

Add to your `Cargo.toml`:

```toml
[dependencies]
hazelcast-client = "0.1"
tokio = { version = "1", features = ["full"] }
```

Basic usage:

```rust
use hazelcast_client::{Client, ClientConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ClientConfig::new()
        .cluster_name("dev")
        .address("127.0.0.1:5701");
    
    let client = Client::new(config).await?;
    
    // Distributed map
    let map = client.get_map::<String, String>("my-map").await?;
    map.put("key".into(), "value".into()).await?;
    
    let value = map.get(&"key".into()).await?;
    println!("Value: {:?}", value);
    
    client.shutdown().await?;
    Ok(())
}
```

## Features

### Distributed Data Structures

| Structure | Description |
|-----------|-------------|
| `IMap<K, V>` | Distributed hash map with TTL, listeners, and near-cache support |
| `IQueue<E>` | Distributed FIFO queue with blocking operations |
| `ISet<E>` | Distributed set with unique elements |
| `IList<E>` | Distributed list preserving insertion order |
| `MultiMap<K, V>` | Map allowing multiple values per key |
| `ReplicatedMap<K, V>` | Eventually consistent replicated map |
| `Ringbuffer<E>` | Fixed-capacity circular buffer |

### Pub/Sub Messaging

| Structure | Description |
|-----------|-------------|
| `ITopic<E>` | Publish/subscribe messaging |
| `ReliableTopic<E>` | Reliable messaging backed by Ringbuffer |

### Distributed Primitives (CP Subsystem)

| Structure | Description |
|-----------|-------------|
| `AtomicLong` | Distributed atomic counter |
| `FencedLock` | Distributed lock with fencing tokens |
| `Semaphore` | Distributed counting semaphore |
| `CountDownLatch` | Distributed countdown synchronization |
| `PNCounter` | CRDT-based eventually consistent counter |

### ID Generation

| Structure | Description |
|-----------|-------------|
| `FlakeIdGenerator` | Cluster-wide unique ID generator |

### Query & Compute

| Feature | Description |
|---------|-------------|
| SQL Service | Execute SQL queries against maps |
| Predicate API | Filter map entries with type-safe predicates |
| ExecutorService | Submit tasks to cluster members |
| ScheduledExecutorService | Schedule delayed/periodic tasks |

### Transactions

ACID transactions spanning multiple data structures:

```rust
let ctx = client.new_transaction_context().await?;
ctx.begin().await?;

let map = ctx.get_map::<String, i64>("accounts").await?;
let queue = ctx.get_queue::<String>("transfers").await?;

map.put("alice".into(), 100).await?;
queue.offer("transfer-001".into()).await?;

ctx.commit().await?;
```

### Near Cache

Client-side caching for frequently accessed map entries:

```rust
use hazelcast_client::cache::{NearCacheConfig, EvictionPolicy, InMemoryFormat};

let near_cache = NearCacheConfig::new("my-map")
    .max_size(10_000)
    .eviction_policy(EvictionPolicy::Lru)
    .time_to_live(Duration::from_secs(300))
    .in_memory_format(InMemoryFormat::Binary);

let config = ClientConfig::new()
    .near_cache(near_cache);
```

## Configuration

### Network Settings

```rust
let config = ClientConfig::new()
    .cluster_name("production")
    .address("10.0.0.1:5701")
    .address("10.0.0.2:5701")
    .connection_timeout(Duration::from_secs(10))
    .heartbeat_interval(Duration::from_secs(5));
```

### Security (TLS)

Requires the `tls` feature:

```rust
let config = ClientConfig::new()
    .tls(TlsConfig::new()
        .ca_path("/path/to/ca.pem")
        .cert_path("/path/to/client.pem")
        .key_path("/path/to/client-key.pem"));
```

### Authentication

```rust
let config = ClientConfig::new()
    .security(SecurityConfig::new()
        .username("admin")
        .password("secret"));
```

## Optional Features

| Feature | Description | Dependencies |
|---------|-------------|--------------|
| `tls` | TLS/SSL encrypted connections | `rustls`, `tokio-rustls` |
| `metrics` | Prometheus metrics integration | `prometheus` |
| `aws` | AWS EC2 cluster discovery | `aws-sdk-ec2` |
| `kubernetes` | Kubernetes cluster discovery | `kube`, `k8s-openapi` |
| `cloud` | Hazelcast Cloud discovery | — |
| `websocket` | WebSocket transport | `tokio-tungstenite` |

Enable features in `Cargo.toml`:

```toml
[dependencies]
hazelcast-client = { version = "0.1", features = ["tls", "metrics"] }
```

### AWS Discovery

```rust
let config = ClientConfig::new()
    .aws(AwsConfig::new()
        .region("us-east-1")
        .tag_key("hazelcast-cluster")
        .tag_value("production"));
```

### Kubernetes Discovery

```rust
let config = ClientConfig::new()
    .kubernetes(KubernetesConfig::new()
        .namespace("default")
        .service_name("hazelcast"));
```

### Hazelcast Cloud

```rust
let config = ClientConfig::new()
    .cloud(CloudConfig::new()
        .cluster_name("my-cluster")
        .token("your-api-token"));
```

## Event Listeners

### Map Entry Listeners

```rust
let listener_id = map.add_entry_listener(
    |event| {
        println!("Entry added: {:?} -> {:?}", event.key, event.value);
    },
    true,  // include value
).await?;

// Later: remove listener
map.remove_entry_listener(listener_id).await?;
```

### Cluster Membership

```rust
client.add_membership_listener(|event| {
    match event {
        MemberEvent::Added(member) => println!("Member joined: {}", member.address),
        MemberEvent::Removed(member) => println!("Member left: {}", member.address),
    }
}).await?;
```

## Examples

See the [`examples/`](examples/) directory for complete programs:

| Example | Description |
|---------|-------------|
| [`basic_usage.rs`](examples/basic_usage.rs) | IMap and IQueue operations |
| [`map_listeners.rs`](examples/map_listeners.rs) | Entry listeners and events |
| [`sql_queries.rs`](examples/sql_queries.rs) | SQL queries against maps |
| [`transactions.rs`](examples/transactions.rs) | ACID transactions |
| [`near_cache.rs`](examples/near_cache.rs) | Client-side caching |
| [`cluster_discovery.rs`](examples/cluster_discovery.rs) | Cloud/K8s/AWS discovery |

Run an example:

```bash
cargo run --example basic_usage
cargo run --example map_listeners
cargo run --example sql_queries --features tls
```

## Running Benchmarks

The client includes a comprehensive benchmark suite using [Criterion.rs](https://github.com/bheisler/criterion.rs).

### Run All Benchmarks

```bash
cargo bench
```

### Run Specific Benchmark Group

```bash
# Map operation benchmarks
cargo bench --bench map_benchmarks

# Queue operation benchmarks
cargo bench --bench queue_benchmarks

# Serialization benchmarks
cargo bench --bench serialization_benchmarks
```

### Run Specific Benchmark Function

```bash
# Run only partition hash benchmarks
cargo bench --bench map_benchmarks -- partition_hash

# Run only string serialization benchmarks
cargo bench --bench serialization_benchmarks -- string_serialize
```

### Benchmark Output

Benchmark results are saved to `target/criterion/`. Open `target/criterion/report/index.html` in a browser to view detailed HTML reports with:

- Throughput measurements (ops/sec, bytes/sec)
- Latency percentiles (min, median, max)
- Historical comparison graphs
- Statistical analysis

### Benchmark Categories

| Benchmark | Description |
|-----------|-------------|
| `serialization_benchmarks` | Primitive types, strings, vectors encoding/decoding |
| `map_benchmarks` | IMap message creation, partition hashing, response decoding |
| `queue_benchmarks` | IQueue offer/poll/peek message encoding, response parsing |

### Tips

- Run benchmarks on a quiet system for consistent results
- Use `--save-baseline <name>` to save results for comparison
- Use `--baseline <name>` to compare against a saved baseline
- Add `-- --verbose` for detailed output during benchmark runs

```bash
# Save current performance as baseline
cargo bench --bench serialization_benchmarks -- --save-baseline main

# Compare against baseline after changes
cargo bench --bench serialization_benchmarks -- --baseline main
```

## API Documentation

Generate and view documentation locally:

```bash
cargo doc --open --features tls,metrics,aws,kubernetes,cloud,websocket
```

Or view online at [docs.rs/hazelcast-client](https://docs.rs/hazelcast-client).

## Compatibility

| Hazelcast Version | Client Version |
|-------------------|----------------|
| 5.x | 0.1.x |

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines on:

- Reporting issues
- Submitting pull requests
- Code style and testing requirements
- Development setup

## License

Licensed under the Apache License 2.0. See [LICENSE](LICENSE) for details.
