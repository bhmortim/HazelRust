# Hazelcast Rust Client (HazelRust)

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.85%2B-orange.svg)](https://www.rust-lang.org)

<!-- Uncomment after publishing to crates.io:
[![Crates.io](https://img.shields.io/crates/v/hazelcast-client.svg)](https://crates.io/crates/hazelcast-client)
[![Documentation](https://docs.rs/hazelcast-client/badge.svg)](https://docs.rs/hazelcast-client)
-->

An asynchronous Rust client for [Hazelcast](https://hazelcast.com/) 5.x, built on
[Tokio](https://tokio.rs/). HazelRust connects to a Hazelcast cluster over the
[Hazelcast Open Binary Client Protocol](https://github.com/hazelcast/hazelcast-client-protocol)
and exposes the cluster's distributed data structures, CP (Raft) primitives, SQL service,
transactions, and serialization formats through an idiomatic `async`/`await` API.

> **Project status & affiliation.** HazelRust is an independent, community-developed
> client. It is **not** an official Hazelcast product and is **not** affiliated with or
> endorsed by Hazelcast, Inc. The vendor-supported clients are Java, Python, Node.js, Go,
> .NET, and C++. This crate is at version `0.1`, has not yet been published to crates.io,
> and targets Hazelcast server 5.x. See [Maturity & status](#maturity--status) for an
> honest breakdown of what is fully implemented versus partial.

## Contents

- [Overview](#overview)
- [Requirements](#requirements)
- [Quick start](#quick-start)
- [How it works](#how-it-works)
- [Distributed data structures](#distributed-data-structures)
- [CP subsystem](#cp-subsystem-raft-backed-primitives)
- [Querying: predicates, SQL, aggregations](#querying-predicates-sql-aggregations)
- [Transactions](#transactions)
- [Streaming (Jet)](#streaming-jet)
- [Serialization](#serialization)
- [Configuration](#configuration)
- [Cargo feature flags](#cargo-feature-flags)
- [Examples](#examples)
- [Building, testing, and docs](#building-testing-and-docs)
- [Compatibility](#compatibility)
- [Maturity & status](#maturity--status)
- [Performance](#performance)
- [Contributing](#contributing)
- [License](#license)

## Overview

Hazelcast is a distributed in-memory data store and compute platform. A Hazelcast cluster
partitions and replicates data across its members; clients connect to the cluster and read
and write that data over the network. HazelRust is the client half of that picture for
Rust services: it manages connections to cluster members, routes each operation to the
member that owns the relevant partition, serializes keys and values to the wire format the
cluster understands, and resolves the asynchronous responses back into Rust values.

The workspace is split into two published crates:

| Crate | Responsibility |
|-------|----------------|
| [`hazelcast-core`](hazelcast-core/) | Wire protocol (`ClientMessage`/`Frame` encoding), serialization (Compact, Portable, IdentifiedDataSerializable, JSON, serde), and shared error types. |
| [`hazelcast-client`](hazelcast-client/) | Connection and cluster management, the data-structure proxies, CP subsystem, SQL, transactions, Jet job control, near cache, and configuration. |

Two internal crates support development: `hazelcast-derive` (derive macros for the
serialization traits) and `hazelrust-bench` (the benchmarking harness).

## Requirements

- **Rust 1.85 or later** (the CI-verified minimum supported version; declared as
  `rust-version` in the workspace manifest).
- **A reachable Hazelcast 5.x cluster.** For local development, a single-node cluster in
  Docker is the fastest option:

  ```sh
  docker run --rm -p 5701:5701 hazelcast/hazelcast:5.5
  ```

## Quick start

Add the client and an async runtime to your `Cargo.toml`:

```toml
[dependencies]
hazelcast-client = "0.1"
tokio = { version = "1", features = ["full"] }
```

Connect to a cluster and use a distributed map. An `IMap` is shared cluster-wide: every
client connected to the same cluster — whether Rust, Java, Python, Go, or .NET — sees the
same entries.

```rust
use hazelcast_client::{ClientConfig, HazelcastClient};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to a local cluster named "dev".
    let config = ClientConfig::builder()
        .cluster_name("dev")
        .build()?;
    let client = HazelcastClient::new(config).await?;

    // `get_map` returns a lightweight proxy; no network call happens here.
    let map = client.get_map::<String, String>("cities");
    map.put("NYC".into(), "New York City".into()).await?;
    map.put("LON".into(), "London".into()).await?;

    // Reads are served by the partition owner (or the near cache, if configured).
    let city = map.get(&"NYC".into()).await?;
    println!("NYC -> {city:?}"); // Some("New York City")
    println!("map size: {}", map.size().await?);

    client.shutdown().await?;
    Ok(())
}
```

By default the client uses **smart routing**: it opens a connection to every reachable
member and sends each operation directly to the member that owns the target partition.
Proxy accessors such as `get_map` are synchronous and cheap; only the operations on the
proxy (`put`, `get`, …) perform I/O and are therefore `async`.

## How it works

- **Asynchronous, Tokio-based.** Every operation that touches the network returns a
  `Future`. Multiple in-flight operations on one connection are multiplexed by correlation
  ID, so concurrency does not require a connection per request.
- **Smart, partition-aware routing.** The client tracks the cluster's partition table and
  routes key-based operations to the owning member. A `unisocket` (single-connection) mode
  is available for environments where the cluster is reached through a single endpoint.
- **Pipelined invocations with a coalescing writer.** Each connection has a dedicated
  writer task. Frames queued while a write is in progress are gathered and flushed together
  using vectored I/O (`writev`), which improves throughput under concurrency without adding
  latency to isolated requests.
- **Resilience.** Configurable reconnection (`OFF`/`ON`/`ASYNC`), heartbeats, connection
  timeouts, and exponential backoff with jitter. Membership, lifecycle, migration, and
  partition-lost events can be subscribed to.

## Distributed data structures

Accessor methods on `HazelcastClient` return proxy handles; the operations on them are
`async`.

| Structure | Accessor | Description |
|-----------|----------|-------------|
| `IMap<K, V>` | `get_map` | Distributed hash map with TTL, entry listeners, near cache, entry processors, and SQL/predicate queryability. |
| `IQueue<T>` | `get_queue` | Distributed FIFO queue with blocking `take`/`put`, bulk drain, and capacity bounds. |
| `ISet<T>` | `get_set` | Distributed set of unique elements. |
| `IList<T>` | `get_list` | Distributed list preserving insertion order, with indexed access. |
| `MultiMap<K, V>` | `get_multimap` | Map allowing multiple values per key. |
| `ReplicatedMap<K, V>` | `get_replicated_map` | Eventually consistent map fully replicated to every member; optimized for read-heavy reference data. |
| `Ringbuffer<T>` | `get_ringbuffer` | Fixed-capacity circular buffer with sequence-based reads and overflow policies. |
| `ITopic<T>` | `get_topic` | Fire-and-forget publish/subscribe. |
| `ReliableTopic<T>` | `get_reliable_topic` | Pub/sub backed by a Ringbuffer, supporting message replay. |
| `ICache<K, V>` | `get_cache` | JCache (JSR-107) cache with entry processors and event-journal reads. |
| `PNCounter` | `get_pn_counter` | CRDT counter that remains available during network partitions. |
| `FlakeIdGenerator` | `get_flake_id_generator` | Cluster-unique, roughly time-ordered ID generator with client-side batching. |
| `CardinalityEstimator` | `get_cardinality_estimator` | HyperLogLog-based distinct-count estimator. |
| `VectorCollection<K, V>` | `get_vector_collection` | Vector store with nearest-neighbour search. |

```rust
let queue = client.get_queue::<String>("tasks");
queue.offer("job-1".into()).await?;
let next = queue.poll().await?; // Some("job-1")

let counter = client.get_pn_counter("page-views");
counter.add_and_get(1).await?;
```

## CP subsystem (Raft-backed primitives)

The CP subsystem provides **linearizable** primitives backed by the Raft consensus
algorithm on the server. They are appropriate for coordination tasks — leader election,
distributed locking, and bounded concurrency — that require strong consistency rather than
availability during partitions. The client manages CP **sessions** automatically: sessions
are created on first use, heartbeated by a background task, and closed on shutdown.

| Primitive | Accessor | Notes |
|-----------|----------|-------|
| `AtomicLong` | `get_atomic_long` | `get`, `set`, `get_and_set`, `compare_and_set`, `add_and_get`, `get_and_add`. |
| `AtomicReference` | `get_atomic_reference` | Core read/compare-and-set operations. (`alter`/`apply` — server-side function execution — are not exposed.) |
| `FencedLock` | `get_fenced_lock` | Distributed lock that returns a monotonic fencing token to guard against stale-lock hazards. |
| `Semaphore` | `get_semaphore` | Distributed counting semaphore. |
| `CountDownLatch` | `get_countdown_latch` | Distributed countdown synchronization. |
| `CPMap<K, V>` | `get_cp_map` | Strongly consistent key/value map in a CP group. |

```rust
let lock = client.get_fenced_lock("resource-lock");
let fence = lock.lock().await?;     // blocks until acquired; returns a fencing token
// ... critical section, guarded by `fence` ...
lock.unlock().await?;
```

## Querying: predicates, SQL, aggregations

`IMap` supports server-side filtering, aggregation, and projection so that only matching
data crosses the network.

**Type-safe predicates.** Constructors that compare against a value take it by reference
and return a `Result` (serialization can fail); combinators such as `Predicates::and` take
a `Vec<Box<dyn Predicate>>`.

```rust
use hazelcast_client::{Predicate, Predicates};

let pred = Predicates::and(vec![
    Box::new(Predicates::greater_than("age", &21i32)?) as Box<dyn Predicate>,
    Box::new(Predicates::like("city", "New%")),
]);
let adults = map.values_with_predicate(&pred).await?;
```

The predicate API covers equality, comparison, range (`between`), set membership (`is_in`),
`like`/`regex`, SQL-string predicates, and boolean composition, plus paging predicates,
aggregations (count/sum/avg/min/max/distinct), and single- or multi-attribute projections.

**SQL service:**

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

`SqlStatement` carries the query text, parameters (`SqlValue`), cursor buffer size,
timeout, and schema. Results are paged automatically via a server-side cursor.

## Transactions

Transactions span multiple data structures with all-or-nothing semantics. Note that the
transactional proxy accessors (`get_map`, `get_queue`, …) return a `Result` and must be
unwrapped with `?`.

```rust
use hazelcast_client::{TransactionOptions, TransactionType};
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

txn.commit().await?; // commit (or `txn.rollback().await?`) across map and queue together
```

Transactional variants exist for `IMap`, `MultiMap`, `IQueue`, `ISet`, and `IList`. For
two-phase commit with an external transaction manager, an `XAResource`/`XATransaction`
implementation is also provided.

## Streaming (Jet)

The Jet integration is primarily a **job-control** API: it submits pipelines to the
cluster's Jet engine and manages their lifecycle. Pipeline *construction* is intentionally
limited — sources, sinks, and transforms reference server-side constructs (maps, lists,
Kafka topics, and named processors), not arbitrary Rust closures, because the processing
runs on the JVM members rather than in the client.

```rust
use hazelcast_client::Pipeline;
use hazelcast_client::jet::{map_sink, map_source};

let jet = client.jet();

let pipeline = Pipeline::builder()
    .read_from(map_source("source-map"))
    .filter("error-only")     // references a processor by name; not a Rust closure
    .write_to(map_sink("sink-map"))
    .build();

let job = jet.submit_job(&pipeline, None).await?;
println!("status: {:?}", job.get_status().await?);
// job.cancel().await?, job.suspend().await?, job.resume().await?, job.restart().await?
```

Solidly supported: job submission, status, cancel/suspend/resume/restart, metrics,
snapshot export, and job listing. Sources/sinks cover IMap, IList, and (with the `kafka`
feature) Kafka. The set of available transforms is narrower than the Java client's.

## Serialization

Keys and values implement the `Serializable`/`Deserializable` traits. Several formats are
supported and interoperate with other Hazelcast clients:

- **Compact** — the schema-based default for Hazelcast 5.x, with a `SchemaRegistry` and a
  schema-agnostic `GenericRecord` API.
- **Portable** — versioned, field-addressable serialization with class definitions and
  factory registration.
- **IdentifiedDataSerializable** — factory-based binary serialization.
- **`HazelcastJsonValue`** — JSON values queryable with JSON predicates.
- **serde** — a `Serde<T>` wrapper bridges any `serde`-serializable type onto the wire.

Derive macros reduce boilerplate:

```rust
use hazelcast_client::core::CompactSerializable;

#[derive(CompactSerializable)]
struct Account {
    id: i64,
    balance: i64,
    owner: String,
}
```

`#[derive(PortableSerializable)]` and `#[derive(IdentifiedSerializable)]` are also
available.

## Configuration

`ClientConfig` is built with a fluent builder. Sub-builders (`network`, `retry`, `tls`,
near cache) are configured through closures.

**Connecting to a multi-node cluster with credentials:**

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

**TLS** (requires the `tls` feature):

```rust
let config = ClientConfig::builder()
    .network(|n| n.tls(|t| t
        .enabled(true)
        .ca_cert_path("/etc/ssl/hazelcast/ca.pem")
        .client_auth("/etc/ssl/hazelcast/client.pem", "/etc/ssl/hazelcast/client-key.pem")
        .verify_hostname(true)))
    .build()?;
```

**Near cache** — keeps a local copy of hot entries so reads are served from client memory.
It supports eviction (LRU/LFU/random), TTL, max-idle, and server-driven invalidation:

```rust
use hazelcast_client::{ClientConfig, EvictionPolicy, InMemoryFormat, NearCacheConfig};
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

**Retry / backoff:**

```rust
let config = ClientConfig::builder()
    .retry(|r| r
        .initial_backoff(Duration::from_millis(100))
        .max_backoff(Duration::from_secs(30))
        .multiplier(2.0)
        .max_retries(10))
    .build()?;
```

**Cluster discovery.** Static addresses work out of the box. Cloud and orchestrator
discovery is feature-gated — enable the relevant feature and configure it on the network
builder. Discovery providers are available for Hazelcast Cloud, AWS, Azure, GCP,
Kubernetes, Eureka, and multicast, plus environment auto-detection.

Configuration can also be loaded from YAML or TOML files via the `config-file` feature.

## Cargo feature flags

All features are **off by default**; the default build is portable and pulls no native
dependencies.

| Feature | Effect | Notable dependencies |
|---------|--------|----------------------|
| `tls` | TLS/SSL connections | `rustls`, `tokio-rustls` |
| `metrics` | Prometheus exporter | `prometheus` |
| `kafka` | Kafka sources/sinks for Jet | `rdkafka` (native librdkafka) |
| `aws` | AWS EC2 discovery | `aws-sdk-ec2` |
| `azure` | Azure VM discovery | `azure_mgmt_compute` |
| `gcp` | GCP Compute Engine discovery | `google-cloud-*` |
| `kubernetes` | Kubernetes discovery | `kube`, `k8s-openapi` |
| `cloud` | Hazelcast Cloud discovery | — |
| `eureka` | Eureka discovery | — |
| `kerberos` | Kerberos authentication | — |
| `config-file` | YAML/TOML configuration files | `serde_yaml`, `toml` |
| `websocket` | WebSocket transport | `tokio-tungstenite` |
| `tower` | `tower::Service` adapter | `tower` |

```toml
[dependencies]
hazelcast-client = { version = "0.1", features = ["tls", "metrics", "kubernetes"] }
```

> The `kafka` feature compiles a vendored `librdkafka` from source and needs a C toolchain,
> CMake, and the SASL/OpenSSL/zlib/curl development headers on the build host.

## Examples

Runnable programs live in [`hazelcast-client/examples/`](hazelcast-client/examples/):

| Example | Demonstrates |
|---------|--------------|
| [`basic_usage.rs`](hazelcast-client/examples/basic_usage.rs) | `IMap` and `IQueue` operations |
| [`map_listeners.rs`](hazelcast-client/examples/map_listeners.rs) | Real-time entry event listeners |
| [`sql_queries.rs`](hazelcast-client/examples/sql_queries.rs) | SQL queries against a map |
| [`transactions.rs`](hazelcast-client/examples/transactions.rs) | Transactions across structures |
| [`near_cache.rs`](hazelcast-client/examples/near_cache.rs) | Client-side near caching |
| [`cluster_discovery.rs`](hazelcast-client/examples/cluster_discovery.rs) | Cloud / Kubernetes / AWS discovery |
| [`prometheus_metrics.rs`](hazelcast-client/examples/prometheus_metrics.rs) | Prometheus metrics export |
| [`entry_processor_deployment.rs`](hazelcast-client/examples/entry_processor_deployment.rs) | Server-side entry processing |

```sh
# Start a local cluster first.
docker run --rm -p 5701:5701 hazelcast/hazelcast:5.5

cargo run --example basic_usage
cargo run --example transactions
```

## Building, testing, and docs

```sh
git clone https://github.com/bhmortim/HazelRust.git
cd HazelRust

cargo build                       # default features
cargo test                        # unit + non-ignored integration tests
cargo build --all-features        # requires the native deps noted above
cargo doc --open                  # browse the API documentation
```

Integration tests that require a live cluster are marked `#[ignore]` and are excluded from
the default `cargo test` run. Continuous integration enforces a clean build, the test
suite, `rustfmt`, and a `cargo-deny` supply-chain check on every push and pull request.

## Compatibility

| Hazelcast server | Client version |
|------------------|----------------|
| 5.x | 0.1.x |

The client speaks the Hazelcast Open Binary Client Protocol and is intended to work with
any Hazelcast 5.x cluster, including Hazelcast Cloud managed clusters.

## Maturity & status

This is an early-stage (`0.1`) client. The breadth of the protocol surface is wide, but the
maturity is uneven, and it should be evaluated against your own correctness and reliability
requirements before production use.

**Fully implemented** (send real protocol messages and are covered by integration tests):
the distributed data structures listed above, the CP primitives with automatic session
management, SQL, transactions (including XA), the serialization formats, the predicate /
aggregation / projection query system, near cache and query cache, cluster and lifecycle
event listeners, the discovery providers, TLS and authentication, and the metrics exporter.

**Partial or limited:**

- **`ITopic` / `ReliableTopic`** — registering a message listener works; *removing* a
  listener currently tears down the client-side subscription only, without a server-side
  deregistration message.
- **Jet pipelines** — job submission and lifecycle control are complete, but the
  source/sink/transform vocabulary for *building* pipelines is narrower than the Java
  client's.
- **`AtomicReference`** — `alter`/`apply` (server-side function execution) are not exposed.

For a feature-by-feature comparison against the Java client, see
[`docs/CLIENT_COMPLETENESS.md`](docs/CLIENT_COMPLETENESS.md).

## Performance

HazelRust is designed for low client-side overhead: a fixed, small set of connections, a
per-connection coalescing writer that batches queued frames into a single vectored write,
partition-aware routing, and an optional near cache for read-heavy workloads. A
reproducible head-to-head benchmark against the Hazelcast Java client — covering throughput
and p50–p999 latency across IMap, CP, and collection workloads, plus client CPU and memory
sampling — and its methodology are documented under [`docs/cbdc/`](docs/cbdc/). Treat those
numbers as specific to that rig and workload, not as universal guarantees.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup, coding conventions, and pull
request guidelines, and the [Code of Conduct](CODE_OF_CONDUCT.md) before participating.

## License

Licensed under the [Apache License 2.0](LICENSE).
