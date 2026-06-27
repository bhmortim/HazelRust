# hazelcast-client

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](../LICENSE)

<!-- Uncomment after publishing to crates.io:
[![Crates.io](https://img.shields.io/crates/v/hazelcast-client.svg)](https://crates.io/crates/hazelcast-client)
[![Documentation](https://docs.rs/hazelcast-client/badge.svg)](https://docs.rs/hazelcast-client)
-->

An asynchronous Rust client for [Hazelcast](https://hazelcast.com/) 5.x, built on
[Tokio](https://tokio.rs/). Connect to a Hazelcast cluster and use its distributed maps,
queues, topics, locks, counters, SQL service, transactions, and Jet jobs from idiomatic
`async` Rust.

> **Not an official Hazelcast client.** This is an independent, community-developed crate.
> It is not affiliated with or endorsed by Hazelcast, Inc. The vendor-supported clients are
> Java, Python, Node.js, Go, .NET, and C++. See the
> [workspace README](https://github.com/bhmortim/HazelRust) for a full feature overview and
> a maturity breakdown.

## Quick start

```toml
[dependencies]
hazelcast-client = "0.1"
tokio = { version = "1", features = ["full"] }
```

```rust
use hazelcast_client::{ClientConfig, HazelcastClient};

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

Proxy accessors (`get_map`, `get_queue`, …) are synchronous and cheap; the operations on
the returned proxy perform I/O and are `async`.

## Data structures

Every structure is cluster-wide: each connected client (Rust, Java, Python, Go, .NET) sees
the same data.

| Type | Accessor | Description |
|------|----------|-------------|
| `IMap<K, V>` | `get_map` | Distributed map with TTL, listeners, near cache, SQL/predicate queries |
| `IQueue<T>` | `get_queue` | Distributed blocking FIFO queue |
| `ISet<T>` | `get_set` | Distributed set of unique elements |
| `IList<T>` | `get_list` | Distributed ordered list |
| `MultiMap<K, V>` | `get_multimap` | Multiple values per key |
| `ReplicatedMap<K, V>` | `get_replicated_map` | Eventually consistent, fully replicated map |
| `Ringbuffer<T>` | `get_ringbuffer` | Fixed-capacity circular buffer |
| `ITopic<T>` | `get_topic` | Publish/subscribe messaging |
| `ReliableTopic<T>` | `get_reliable_topic` | Reliable pub/sub with replay |
| `ICache<K, V>` | `get_cache` | JCache (JSR-107) cache |
| `AtomicLong` | `get_atomic_long` | Linearizable counter (CP subsystem) |
| `FencedLock` | `get_fenced_lock` | Distributed lock with fencing tokens (CP subsystem) |
| `Semaphore` | `get_semaphore` | Distributed counting semaphore (CP subsystem) |
| `CountDownLatch` | `get_countdown_latch` | Distributed countdown latch (CP subsystem) |
| `CPMap<K, V>` | `get_cp_map` | Strongly consistent CP map |
| `PNCounter` | `get_pn_counter` | CRDT conflict-free counter |
| `FlakeIdGenerator` | `get_flake_id_generator` | Cluster-unique ID generator |

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

### TLS (requires the `tls` feature)

```rust
let config = ClientConfig::builder()
    .network(|n| n.tls(|t| t
        .enabled(true)
        .ca_cert_path("/path/to/ca.pem")
        .client_auth("/path/to/cert.pem", "/path/to/key.pem")
        .verify_hostname(true)))
    .build()?;
```

### Near cache

```rust
use hazelcast_client::{EvictionPolicy, InMemoryFormat, NearCacheConfig};
use std::time::Duration;

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

All-or-nothing operations spanning multiple data structures. The transactional accessors
return a `Result`, so unwrap them with `?`:

```rust
use hazelcast_client::{TransactionOptions, TransactionType};
use std::time::Duration;

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
use hazelcast_client::{SqlStatement, SqlValue};

let sql = client.sql();
let mut result = sql
    .execute(SqlStatement::new("SELECT name, population FROM cities WHERE population > ?")
        .add_parameter(SqlValue::Integer(1_000_000)))
    .await?;

while let Some(row) = result.next_row().await? {
    println!("{:?}", row.get_by_name("name"));
}
result.close().await?;
```

## Streaming (Jet)

Jet support is primarily a job-control API. Pipeline sources, sinks, and transforms
reference server-side constructs (maps, lists, Kafka topics, named processors) rather than
Rust closures, because processing runs on the JVM members.

```rust
use hazelcast_client::Pipeline;
use hazelcast_client::jet::{map_sink, map_source};

let jet = client.jet();
let pipeline = Pipeline::builder()
    .read_from(map_source("events"))
    .filter("error-only")          // processor name, not a Rust closure
    .write_to(map_sink("error-events"))
    .build();

let job = jet.submit_job(&pipeline, None).await?;
println!("status: {:?}", job.get_status().await?);
```

## Optional features

All features are off by default.

| Feature | Description |
|---------|-------------|
| `tls` | TLS/SSL connections |
| `metrics` | Prometheus exporter |
| `kafka` | Kafka sources/sinks for Jet (native `librdkafka`) |
| `aws` / `azure` / `gcp` / `kubernetes` / `cloud` / `eureka` | Cluster discovery providers |
| `kerberos` | Kerberos authentication |
| `config-file` | YAML/TOML configuration files |
| `websocket` | WebSocket transport |
| `tower` | `tower::Service` adapter |

```toml
hazelcast-client = { version = "0.1", features = ["tls", "metrics"] }
```

## Examples

See the [`examples/`](examples/) directory:

| Example | Description |
|---------|-------------|
| [`basic_usage.rs`](examples/basic_usage.rs) | `IMap` and `IQueue` operations |
| [`map_listeners.rs`](examples/map_listeners.rs) | Entry event listeners |
| [`sql_queries.rs`](examples/sql_queries.rs) | SQL queries against a map |
| [`transactions.rs`](examples/transactions.rs) | Transactions |
| [`near_cache.rs`](examples/near_cache.rs) | Client-side near caching |
| [`cluster_discovery.rs`](examples/cluster_discovery.rs) | Cloud / Kubernetes / AWS discovery |
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
```

Criterion writes HTML reports to `target/criterion/`.

## Compatibility

| Hazelcast server | Client version |
|------------------|----------------|
| 5.x | 0.1.x |

## License

Apache License 2.0 — see [LICENSE](../LICENSE).
