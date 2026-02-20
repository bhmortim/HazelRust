# hazelcast-client

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](../LICENSE)

<!-- Uncomment after publishing to crates.io:
[![Crates.io](https://img.shields.io/crates/v/hazelcast-client.svg)](https://crates.io/crates/hazelcast-client)
[![Documentation](https://docs.rs/hazelcast-client/badge.svg)](https://docs.rs/hazelcast-client)
-->

An async Rust client for [Hazelcast](https://hazelcast.com/) built on Tokio. Connect to a Hazelcast 5.x cluster and use distributed maps, queues, topics, locks, counters, SQL, transactions, and streaming pipelines from idiomatic Rust.

## Quick Start

```toml
[dependencies]
hazelcast-client = "0.1"
tokio = { version = "1", features = ["full"] }
```

```rust
use hazelcast_client::{HazelcastClient, ClientConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ClientConfig::builder()
        .cluster_name("dev")
        .build()?;
    let client = HazelcastClient::new(config).await?;

    let map = client.get_map::<String, String>("my-map");
    map.put("hello".into(), "world".into()).await?;
    assert_eq!(map.get(&"hello".into()).await?, Some("world".into()));

    client.shutdown().await?;
    Ok(())
}
```

## Data Structures

All structures are cluster-wide — every connected client (Rust, Java, Python, Go, .NET) sees the same data.

| Type | Description |
|------|-------------|
| `IMap<K, V>` | Distributed hash map with TTL, entry listeners, near-cache, SQL queries |
| `IQueue<T>` | Distributed blocking FIFO queue |
| `ISet<T>` | Distributed set with unique elements |
| `IList<T>` | Distributed ordered list |
| `MultiMap<K, V>` | Map supporting multiple values per key |
| `ReplicatedMap<K, V>` | Eventually consistent, fully replicated map |
| `Ringbuffer<T>` | Fixed-capacity circular buffer |
| `ITopic<T>` | Publish/subscribe messaging |
| `ReliableTopic<T>` | Reliable pub/sub with message replay |
| `AtomicLong` | Linearizable distributed counter (CP Subsystem) |
| `FencedLock` | Distributed lock with fencing tokens (CP Subsystem) |
| `Semaphore` | Distributed counting semaphore (CP Subsystem) |
| `CountDownLatch` | Distributed countdown latch (CP Subsystem) |
| `PNCounter` | CRDT conflict-free counter |
| `FlakeIdGenerator` | Cluster-wide unique ID generator |

## Configuration

```rust
use hazelcast_client::ClientConfig;
use std::time::Duration;

let config = ClientConfig::builder()
    .cluster_name("production")
    .add_address("10.0.0.1:5701".parse()?)
    .add_address("10.0.0.2:5701".parse()?)
    .connection_timeout(Duration::from_secs(10))
    .credentials("admin", "secret")
    .retry(|r| r
        .initial_backoff(Duration::from_millis(100))
        .max_backoff(Duration::from_secs(30))
        .multiplier(2.0)
        .max_retries(10))
    .build()?;
```

### TLS (requires `tls` feature)

```rust
let config = ClientConfig::builder()
    .network(|n| n
        .tls(|t| t
            .enabled(true)
            .ca_cert_path("/path/to/ca.pem")
            .client_auth("/path/to/cert.pem", "/path/to/key.pem")))
    .build()?;
```

### Near Cache

```rust
use hazelcast_client::{NearCacheConfig, EvictionPolicy, InMemoryFormat};

let near_cache = NearCacheConfig::builder("hot-data-*")
    .max_size(50_000)
    .eviction_policy(EvictionPolicy::Lru)
    .time_to_live(Duration::from_secs(60))
    .build()?;

let config = ClientConfig::builder()
    .add_near_cache_config(near_cache)
    .build()?;
```

## Transactions

ACID transactions spanning multiple data structures:

```rust
use hazelcast_client::{TransactionOptions, TransactionType};

let options = TransactionOptions::new()
    .with_timeout(Duration::from_secs(30))
    .with_type(TransactionType::TwoPhase);

let mut txn = client.new_transaction_context(options);
txn.begin().await?;

let accounts = txn.get_map::<String, i64>("accounts")?;
accounts.put("alice".into(), 950).await?;
accounts.put("bob".into(), 1050).await?;

txn.commit().await?;
```

XA distributed transactions are also supported via `XATransaction`.

## SQL

```rust
let sql = client.get_sql_service();
let result = sql.execute("SELECT * FROM cities WHERE population > ?", &[&1_000_000]).await?;
for row in result {
    println!("{}: {}", row.get::<String>("name")?, row.get::<i64>("population")?);
}
```

## Streaming (Jet)

```rust
let jet = client.get_jet_service();
let pipeline = Pipeline::builder()
    .read_from("events")
    .filter(|e| e.contains("ERROR"))
    .write_to("error-events")
    .build();
let job = jet.submit(pipeline, JobConfig::default()).await?;
```

## Optional Features

| Feature | Description |
|---------|-------------|
| `tls` | TLS/SSL encrypted connections |
| `metrics` | Prometheus metrics integration |
| `aws` | AWS EC2 cluster discovery |
| `azure` | Azure VM cluster discovery |
| `gcp` | GCP Compute Engine discovery |
| `kubernetes` | Kubernetes pod discovery |
| `cloud` | Hazelcast Cloud discovery |
| `kafka` | Kafka connectors for Jet |
| `websocket` | WebSocket transport |

```toml
hazelcast-client = { version = "0.1", features = ["tls", "metrics"] }
```

## Examples

See the [`examples/`](examples/) directory:

| Example | Description |
|---------|-------------|
| [`basic_usage.rs`](examples/basic_usage.rs) | IMap and IQueue operations |
| [`map_listeners.rs`](examples/map_listeners.rs) | Entry event listeners |
| [`sql_queries.rs`](examples/sql_queries.rs) | SQL queries against maps |
| [`transactions.rs`](examples/transactions.rs) | ACID transactions |
| [`near_cache.rs`](examples/near_cache.rs) | Client-side caching |
| [`cluster_discovery.rs`](examples/cluster_discovery.rs) | Cloud/K8s/AWS discovery |
| [`prometheus_metrics.rs`](examples/prometheus_metrics.rs) | Prometheus exporter |
| [`entry_processor_deployment.rs`](examples/entry_processor_deployment.rs) | Server-side entry processing |

```sh
docker run --rm -p 5701:5701 hazelcast/hazelcast:5.5
cargo run --example basic_usage
```

## Benchmarks

```sh
cargo bench                            # all benchmarks
cargo bench --bench map_benchmarks     # map operations only
cargo bench --bench serialization_benchmarks -- string_serialize  # specific function
```

Results are saved to `target/criterion/` with HTML reports.

## Compatibility

| Hazelcast Server | Client Version |
|------------------|----------------|
| 5.x | 0.1.x |

## License

Apache License 2.0 — see [LICENSE](../LICENSE).
