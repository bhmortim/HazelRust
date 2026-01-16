# Hazelcast Rust Client

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

<!-- Uncomment after adding .github/workflows/ci.yml:
[![CI](https://github.com/hazelcast/hazelcast-rust-client/actions/workflows/ci.yml/badge.svg)](https://github.com/hazelcast/hazelcast-rust-client/actions)
-->

<!-- Uncomment these badges after publishing to crates.io:
[![Crates.io](https://img.shields.io/crates/v/hazelcast-client.svg)](https://crates.io/crates/hazelcast-client)
[![Documentation](https://docs.rs/hazelcast-client/badge.svg)](https://docs.rs/hazelcast-client)
[![Coverage](https://codecov.io/gh/hazelcast/hazelcast-rust-client/branch/main/graph/badge.svg)](https://codecov.io/gh/hazelcast/hazelcast-rust-client)
-->

A Rust client library for [Hazelcast](https://hazelcast.com/) — the real-time data platform for consistent, low-latency access to data at any scale.

## Project Structure

| Crate | Description |
|-------|-------------|
| `hazelcast-core` | Core types, protocols, and serialization |
| `hazelcast-client` | Client implementation and connection management |

## Quick Start

Add to your `Cargo.toml`:

```toml
[dependencies]
hazelcast-client = "0.1"
tokio = { version = "1", features = ["full"] }
```

Basic usage:

```rust
use hazelcast_client::{HazelcastClient, ClientConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ClientConfig::builder()
        .cluster_name("dev")
        .build()?;

    let client = HazelcastClient::new(config).await?;

    // Distributed map
    let map = client.get_map::<String, String>("my-map");
    map.put("key".to_string(), "value".to_string()).await?;

    let value = map.get(&"key".to_string()).await?;
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
| `IQueue<T>` | Distributed FIFO queue with blocking operations |
| `ISet<T>` | Distributed set with unique elements |
| `IList<T>` | Distributed list preserving insertion order |
| `MultiMap<K, V>` | Map allowing multiple values per key |
| `ReplicatedMap<K, V>` | Eventually consistent replicated map |
| `Ringbuffer<T>` | Fixed-capacity circular buffer |

### Pub/Sub Messaging

| Structure | Description |
|-----------|-------------|
| `ITopic<T>` | Publish/subscribe messaging |
| `ReliableTopic<T>` | Reliable messaging backed by Ringbuffer |

### Distributed Primitives (CP Subsystem)

| Structure | Description |
|-----------|-------------|
| `AtomicLong` | Distributed atomic counter |
| `FencedLock` | Distributed lock with fencing tokens |
| `Semaphore` | Distributed counting semaphore |
| `CountDownLatch` | Distributed countdown synchronization |

### CRDT Primitives

| Structure | Description |
|-----------|-------------|
| `PNCounter` | CRDT-based eventually consistent counter (does not require CP subsystem) |

### ID Generation

| Structure | Description |
|-----------|-------------|
| `FlakeIdGenerator` | Cluster-wide unique ID generator (Snowflake-like) |

### Query & Compute

| Feature | Description |
|---------|-------------|
| `SqlService` | Execute SQL queries against maps |
| Predicate API | Filter map entries with type-safe predicates |
| `ExecutorService` | Submit tasks to cluster members |

### Transactions

| Feature | Description |
|---------|-------------|
| `TransactionContext` | ACID transactions spanning multiple data structures |
| `XATransaction` | XA distributed transactions for two-phase commit |

### Streaming (Jet)

| Feature | Description |
|---------|-------------|
| `JetService` | Submit and manage streaming pipelines |
| `Pipeline` | Build streaming data pipelines |
| `Job` | Monitor and control running jobs |

## Configuration

### Basic Configuration

```rust
use hazelcast_client::ClientConfig;
use std::time::Duration;

let config = ClientConfig::builder()
    .cluster_name("production")
    .add_address("192.168.1.1:5701".parse()?)
    .add_address("192.168.1.2:5701".parse()?)
    .connection_timeout(Duration::from_secs(10))
    .build()?;
```

### Network Settings

```rust
use hazelcast_client::ClientConfig;
use std::time::Duration;

let config = ClientConfig::builder()
    .cluster_name("dev")
    .network(|n| n
        .add_address("10.0.0.1:5701".parse().unwrap())
        .connection_timeout(Duration::from_secs(15))
        .heartbeat_interval(Duration::from_secs(10)))
    .build()?;
```

### TLS/SSL Configuration

```rust
use hazelcast_client::ClientConfig;

let config = ClientConfig::builder()
    .network(|n| n
        .tls(|t| t
            .enabled(true)
            .ca_cert_path("/path/to/ca.pem")
            .client_auth("/path/to/cert.pem", "/path/to/key.pem")
            .verify_hostname(true)))
    .build()?;
```

### Authentication

```rust
use hazelcast_client::ClientConfig;

// Username/password authentication
let config = ClientConfig::builder()
    .credentials("admin", "secret")
    .build()?;

// Token-based authentication
let config = ClientConfig::builder()
    .security(|s| s.token("your-auth-token"))
    .build()?;
```

### Retry Configuration

```rust
use hazelcast_client::ClientConfig;
use std::time::Duration;

let config = ClientConfig::builder()
    .retry(|r| r
        .initial_backoff(Duration::from_millis(100))
        .max_backoff(Duration::from_secs(30))
        .multiplier(2.0)
        .max_retries(10))
    .build()?;
```

### Near Cache Configuration

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

### Split-Brain Protection (Quorum)

```rust
use hazelcast_client::{ClientConfig, QuorumConfig, QuorumType};

let quorum = QuorumConfig::builder("critical-*")
    .min_cluster_size(3)
    .quorum_type(QuorumType::ReadWrite)
    .build()?;

let config = ClientConfig::builder()
    .add_quorum_config(quorum)
    .build()?;
```

## Optional Features

| Feature | Description | Dependencies |
|---------|-------------|--------------|
| `tls` | TLS/SSL encrypted connections | `rustls`, `tokio-rustls` |
| `metrics` | Prometheus metrics integration | `prometheus` |
| `aws` | AWS EC2 cluster discovery | `aws-sdk-ec2` |
| `azure` | Azure VM cluster discovery | `azure_mgmt_compute` |
| `gcp` | GCP Compute Engine discovery | `google-cloud-compute` |
| `kubernetes` | Kubernetes pod discovery | `kube`, `k8s-openapi` |
| `cloud` | Hazelcast Cloud discovery | — |

Enable features in `Cargo.toml`:

```toml
[dependencies]
hazelcast-client = { version = "0.1", features = ["tls", "metrics"] }
```

## Cloud Discovery

### AWS Discovery

```rust
use hazelcast_client::ClientConfig;
use hazelcast_client::connection::AwsDiscoveryConfig;

let aws_config = AwsDiscoveryConfig::new("us-west-2")
    .with_tag("hazelcast:cluster", "production")
    .with_port(5701);

let config = ClientConfig::builder()
    .network(|n| n.aws_discovery(aws_config))
    .build()?;
```

### Kubernetes Discovery

```rust
use hazelcast_client::ClientConfig;
use hazelcast_client::connection::KubernetesDiscoveryConfig;

let k8s_config = KubernetesDiscoveryConfig::new()
    .namespace("hazelcast")
    .service_name("hz-service")
    .pod_label("app", "hazelcast")
    .port(5701);

let config = ClientConfig::builder()
    .network(|n| n.kubernetes_discovery(k8s_config))
    .build()?;
```

### Hazelcast Cloud

```rust
use hazelcast_client::ClientConfig;
use hazelcast_client::connection::CloudDiscoveryConfig;

let cloud_config = CloudDiscoveryConfig::new("your-discovery-token")
    .with_timeout(30);

let config = ClientConfig::builder()
    .cluster_name("my-cloud-cluster")
    .network(|n| n.cloud_discovery(cloud_config))
    .build()?;
```

## Transactions

```rust
use hazelcast_client::{HazelcastClient, TransactionOptions};

let client = HazelcastClient::new(config).await?;
let options = TransactionOptions::default();

let mut txn = client.new_transaction_context(options);
txn.begin().await?;

let map = txn.get_map::<String, i64>("accounts");
map.put("alice".to_string(), 100).await?;

txn.commit().await?;
```

## Cluster Events

```rust
use hazelcast_client::HazelcastClient;

let client = HazelcastClient::new(config).await?;

// Subscribe to membership events
let mut membership = client.subscribe_membership();
tokio::spawn(async move {
    while let Ok(event) = membership.recv().await {
        println!("Membership event: {:?}", event);
    }
});

// Subscribe to lifecycle events
let mut lifecycle = client.subscribe_lifecycle();
tokio::spawn(async move {
    while let Ok(event) = lifecycle.recv().await {
        println!("Lifecycle event: {:?}", event);
    }
});
```

## Building

```sh
cargo build
```

Build with all features:

```sh
cargo build --all-features
```

## Testing

```sh
cargo test
```

## Examples

See the [`hazelcast-client/examples/`](hazelcast-client/examples/) directory for complete programs.

Run an example:

```sh
cargo run --example basic_usage
```

## API Documentation

Generate and view documentation locally:

```sh
cargo doc --open --all-features
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

Please read our [Code of Conduct](CODE_OF_CONDUCT.md) before participating.

## License

Licensed under the Apache License 2.0. See [LICENSE](LICENSE) for details.
