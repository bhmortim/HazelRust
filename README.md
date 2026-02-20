# Hazelcast Rust Client

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.70%2B-orange.svg)](https://www.rust-lang.org)

<!-- Uncomment after publishing to crates.io:
[![Crates.io](https://img.shields.io/crates/v/hazelcast-client.svg)](https://crates.io/crates/hazelcast-client)
[![Documentation](https://docs.rs/hazelcast-client/badge.svg)](https://docs.rs/hazelcast-client)
-->

The official Rust client for [Hazelcast](https://hazelcast.com/) — the real-time data platform for building low-latency applications at any scale.

**Hazelcast gives your Rust services a shared, distributed memory layer** that survives process restarts, scales horizontally across machines, and keeps data consistent with ACID transactions — all accessible through an idiomatic `async`/`await` API built on Tokio.

## When to Reach for Hazelcast

| Scenario | What Hazelcast Provides |
|----------|------------------------|
| **Session storage across service replicas** | A distributed `IMap` that every replica reads and writes — no sticky sessions, no external database round-trip |
| **Rate limiting / deduplication at the edge** | `AtomicLong` increments are linearizable across the cluster, and `IMap` entries expire with server-side TTL |
| **Real-time leaderboards and counters** | `PNCounter` gives you conflict-free counters that work even during network partitions |
| **Coordinating microservices** | `FencedLock`, `Semaphore`, and `CountDownLatch` in the CP Subsystem provide distributed coordination backed by Raft consensus |
| **Event-driven architectures** | `ITopic` and `ReliableTopic` fan out messages to every subscriber; `Ringbuffer` gives you ordered, replayable event storage |
| **Write-behind caching for databases** | Near Cache brings sub-millisecond reads to the client; MapStore/MapLoader integrate with your persistence layer on the server |
| **Streaming data pipelines** | Jet pipelines process unbounded streams with exactly-once guarantees, with built-in Kafka connectors |

## Project Structure

| Crate | Purpose |
|-------|---------|
| [`hazelcast-core`](hazelcast-core/) | Wire protocol, serialization (Portable, Compact, serde), error types |
| [`hazelcast-client`](hazelcast-client/) | Connection management, data structure proxies, transactions, SQL, Jet |

## Quick Start

### Prerequisites

Start a Hazelcast cluster (Docker is the fastest path):

```sh
docker run --rm -p 5701:5701 hazelcast/hazelcast:5.5
```

### Add the dependency

```toml
[dependencies]
hazelcast-client = "0.1"
tokio = { version = "1", features = ["full"] }
```

### Connect and use a distributed map

```rust
use hazelcast_client::{HazelcastClient, ClientConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to a local Hazelcast cluster named "dev"
    let config = ClientConfig::builder()
        .cluster_name("dev")
        .build()?;
    let client = HazelcastClient::new(config).await?;

    // Get a distributed map — this map is shared across every connected client
    let map = client.get_map::<String, String>("cities");
    map.put("NYC".into(), "New York City".into()).await?;
    map.put("LON".into(), "London".into()).await?;

    // Read it back (from any client in the cluster)
    let city = map.get(&"NYC".into()).await?;
    println!("NYC -> {:?}", city); // Some("New York City")

    println!("Map has {} entries", map.size().await?);

    client.shutdown().await?;
    Ok(())
}
```

## Feature Tour

### Distributed Data Structures

Every structure below is cluster-wide: multiple Rust processes (or Java, Python, Go, .NET clients) see the same data in real time.

| Structure | Description | Best For |
|-----------|-------------|----------|
| `IMap<K, V>` | Distributed hash map with TTL, near-cache, entry listeners, and SQL queryability | Caching, session stores, lookup tables |
| `IQueue<T>` | Blocking FIFO queue distributed across the cluster | Task queues, work distribution |
| `ISet<T>` | Distributed set with unique elements | Deduplication, membership tracking |
| `IList<T>` | Distributed list preserving insertion order | Ordered logs, audit trails |
| `MultiMap<K, V>` | Map allowing multiple values per key | Tagging, indexing |
| `ReplicatedMap<K, V>` | Eventually consistent, fully replicated map | Read-heavy reference data |
| `Ringbuffer<T>` | Fixed-capacity circular buffer | Event sourcing, recent-activity feeds |

### Pub/Sub Messaging

| Structure | Description |
|-----------|-------------|
| `ITopic<T>` | Fire-and-forget publish/subscribe |
| `ReliableTopic<T>` | Reliable pub/sub backed by Ringbuffer with message replay |

### CP Subsystem (Raft-Backed Primitives)

These primitives are **linearizable** — safe for leader election, locking, and coordination even during network partitions.

| Structure | Description |
|-----------|-------------|
| `AtomicLong` | Distributed atomic counter |
| `FencedLock` | Distributed lock with fencing tokens to prevent stale-lock hazards |
| `Semaphore` | Distributed counting semaphore |
| `CountDownLatch` | Distributed countdown synchronization |

### CRDT Counter

| Structure | Description |
|-----------|-------------|
| `PNCounter` | Conflict-free replicated counter — works during partitions without CP Subsystem |

### Query & Compute

| Feature | Description |
|---------|-------------|
| `SqlService` | Run SQL queries against maps (`SELECT * FROM cities WHERE population > 1000000`) |
| Predicate API | Type-safe server-side filtering (`Predicates::greater_than("age", 21)`) |
| `ExecutorService` | Submit Rust closures for execution on cluster members |

### Transactions

```rust
use hazelcast_client::{HazelcastClient, TransactionOptions, TransactionType};
use std::time::Duration;

let options = TransactionOptions::new()
    .with_timeout(Duration::from_secs(30))
    .with_type(TransactionType::TwoPhase);

let mut txn = client.new_transaction_context(options);
txn.begin().await?;

let accounts = txn.get_map::<String, i64>("accounts")?;
let transfers = txn.get_queue::<String>("pending-transfers")?;

accounts.put("alice".into(), 950).await?;
accounts.put("bob".into(), 1050).await?;
transfers.offer("alice->bob:50".into()).await?;

txn.commit().await?; // all-or-nothing across map + queue
```

XA transactions (`XATransaction`) are also available for two-phase commit with external resource managers.

### Streaming (Jet)

Build and submit streaming data pipelines that run continuously on the cluster:

```rust
let jet = client.get_jet_service();

let pipeline = Pipeline::builder()
    .read_from("events")
    .filter(|event| event.contains("ERROR"))
    .write_to("error-events")
    .build();

let job = jet.submit(pipeline, JobConfig::builder().name("error-filter").build()?).await?;
println!("Job status: {:?}", job.status().await?);
```

## Configuration

### Connecting to a Production Cluster

```rust
use hazelcast_client::ClientConfig;
use std::time::Duration;

let config = ClientConfig::builder()
    .cluster_name("production")
    .add_address("10.0.0.1:5701".parse()?)
    .add_address("10.0.0.2:5701".parse()?)
    .connection_timeout(Duration::from_secs(10))
    .credentials("app-user", "secret")
    .build()?;
```

### TLS Encryption

Requires the `tls` feature.

```rust
let config = ClientConfig::builder()
    .network(|n| n
        .tls(|t| t
            .enabled(true)
            .ca_cert_path("/etc/ssl/hazelcast/ca.pem")
            .client_auth("/etc/ssl/hazelcast/client.pem",
                         "/etc/ssl/hazelcast/client-key.pem")
            .verify_hostname(true)))
    .build()?;
```

### Near Cache (Client-Side Caching)

Near Cache keeps a local copy of frequently accessed entries. Reads hit local memory instead of crossing the network.

```rust
use hazelcast_client::{ClientConfig, NearCacheConfig, EvictionPolicy, InMemoryFormat};
use std::time::Duration;

let near_cache = NearCacheConfig::builder("user-*")
    .max_size(10_000)
    .eviction_policy(EvictionPolicy::Lru)
    .time_to_live(Duration::from_secs(300))
    .in_memory_format(InMemoryFormat::Binary)
    .build()?;

let config = ClientConfig::builder()
    .add_near_cache_config(near_cache)
    .build()?;
```

### Retry & Resilience

```rust
let config = ClientConfig::builder()
    .retry(|r| r
        .initial_backoff(Duration::from_millis(100))
        .max_backoff(Duration::from_secs(30))
        .multiplier(2.0)
        .max_retries(10))
    .build()?;
```

### Cloud & Orchestrator Discovery

Hazelcast can auto-discover cluster members in cloud environments. Enable the appropriate feature flag:

```toml
hazelcast-client = { version = "0.1", features = ["aws"] }
```

```rust
use hazelcast_client::ClientConfig;
use hazelcast_client::connection::AwsDiscoveryConfig;

let aws = AwsDiscoveryConfig::new("us-west-2")
    .with_tag("hazelcast:cluster", "production")
    .with_port(5701);

let config = ClientConfig::builder()
    .network(|n| n.aws_discovery(aws))
    .build()?;
```

Discovery is also available for **Kubernetes**, **Azure**, **GCP**, and **Hazelcast Cloud** — see the feature table below.

## Optional Features

| Feature | Description | Dependencies |
|---------|-------------|--------------|
| `tls` | TLS/SSL encrypted connections | `rustls`, `tokio-rustls` |
| `metrics` | Prometheus metrics integration | `prometheus` |
| `aws` | AWS EC2 cluster discovery | `aws-sdk-ec2` |
| `azure` | Azure VM cluster discovery | `azure_mgmt_compute` |
| `gcp` | GCP Compute Engine discovery | `google-cloud-compute` |
| `kubernetes` | Kubernetes pod discovery | `kube`, `k8s-openapi` |
| `cloud` | Hazelcast Cloud managed service discovery | — |
| `kafka` | Kafka connectors for Jet pipelines | `rdkafka` |
| `websocket` | WebSocket transport | `tokio-tungstenite` |

```toml
[dependencies]
hazelcast-client = { version = "0.1", features = ["tls", "metrics", "kubernetes"] }
```

## Examples

The [`hazelcast-client/examples/`](hazelcast-client/examples/) directory contains runnable programs covering common use cases:

| Example | What It Demonstrates |
|---------|---------------------|
| [`basic_usage.rs`](hazelcast-client/examples/basic_usage.rs) | IMap and IQueue CRUD operations |
| [`map_listeners.rs`](hazelcast-client/examples/map_listeners.rs) | Real-time entry event listeners |
| [`sql_queries.rs`](hazelcast-client/examples/sql_queries.rs) | SQL queries against distributed maps |
| [`transactions.rs`](hazelcast-client/examples/transactions.rs) | ACID transactions across data structures |
| [`near_cache.rs`](hazelcast-client/examples/near_cache.rs) | Client-side caching with Near Cache |
| [`cluster_discovery.rs`](hazelcast-client/examples/cluster_discovery.rs) | Cloud/Kubernetes/AWS discovery |
| [`prometheus_metrics.rs`](hazelcast-client/examples/prometheus_metrics.rs) | Prometheus metrics exporter |
| [`entry_processor_deployment.rs`](hazelcast-client/examples/entry_processor_deployment.rs) | Server-side entry processing |

```sh
# Start a local Hazelcast instance first
docker run --rm -p 5701:5701 hazelcast/hazelcast:5.5

# Run an example
cargo run --example basic_usage
cargo run --example transactions
cargo run --example sql_queries
```

## Building from Source

```sh
git clone https://github.com/bhmortim/HazelRust.git
cd HazelRust
cargo build
cargo test
```

Build with all optional features:

```sh
cargo build --all-features
```

Generate API docs locally:

```sh
cargo doc --open --all-features
```

## Compatibility

| Hazelcast Server | Client Version |
|------------------|----------------|
| 5.x | 0.1.x |

The client speaks the [Hazelcast Open Binary Protocol](https://github.com/hazelcast/hazelcast-client-protocol) and is compatible with any Hazelcast 5.x cluster, including Hazelcast Cloud managed clusters.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup, code style, and pull request guidelines. Please read our [Code of Conduct](CODE_OF_CONDUCT.md) before participating.

## License

Licensed under the [Apache License 2.0](LICENSE).
