//! Async Rust client for [Hazelcast](https://hazelcast.com/) — the real-time data platform.
//!
//! This crate provides a full-featured, production-ready client that connects to a
//! Hazelcast 5.x cluster over the [Hazelcast Open Binary Protocol](https://github.com/hazelcast/hazelcast-client-protocol).
//! It is built on [Tokio](https://tokio.rs/) and exposes every operation as an `async fn`.
//!
//! # Quick Start
//!
//! ```rust,no_run
//! use hazelcast_client::{HazelcastClient, ClientConfig};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let config = ClientConfig::builder()
//!         .cluster_name("dev")
//!         .build()?;
//!     let client = HazelcastClient::new(config).await?;
//!
//!     // Distributed map — shared across every connected client
//!     let map = client.get_map::<String, String>("my-map");
//!     map.put("key".into(), "value".into()).await?;
//!     let value = map.get(&"key".into()).await?;
//!     println!("{:?}", value); // Some("value")
//!
//!     client.shutdown().await?;
//!     Ok(())
//! }
//! ```
//!
//! # Distributed Data Structures
//!
//! Every structure listed below lives on the Hazelcast cluster and is visible to all
//! connected clients (Rust, Java, Python, Go, .NET). Handles are obtained through
//! `HazelcastClient::get_*` methods.
//!
//! | Type | Obtain via | Description |
//! |------|-----------|-------------|
//! | [`IMap<K, V>`] | [`get_map`](HazelcastClient::get_map) | Distributed hash map with TTL, near-cache, listeners, and SQL queryability |
//! | [`IQueue<T>`] | [`get_queue`](HazelcastClient::get_queue) | Distributed blocking FIFO queue |
//! | [`ISet<T>`] | [`get_set`](HazelcastClient::get_set) | Distributed set with unique elements |
//! | [`IList<T>`] | [`get_list`](HazelcastClient::get_list) | Distributed ordered list |
//! | [`MultiMap<K, V>`] | [`get_multimap`](HazelcastClient::get_multimap) | Map supporting multiple values per key |
//! | [`ITopic<T>`] | [`get_topic`](HazelcastClient::get_topic) | Publish/subscribe messaging |
//!
//! ## CP Subsystem (Raft-Backed)
//!
//! These primitives are **linearizable** and safe for distributed coordination:
//!
//! - **`AtomicLong`** — distributed atomic counter
//! - **`FencedLock`** — distributed lock with fencing tokens
//! - **`Semaphore`** — distributed counting semaphore
//! - **`CountDownLatch`** — distributed countdown latch
//!
//! # Configuration
//!
//! Use [`ClientConfig::builder()`](ClientConfig::builder) (or equivalently
//! [`ClientConfigBuilder::new()`](ClientConfigBuilder::new)) to construct a config:
//!
//! ```rust,no_run
//! use hazelcast_client::ClientConfig;
//! use std::time::Duration;
//!
//! let config = ClientConfig::builder()
//!     .cluster_name("production")
//!     .add_address("10.0.0.1:5701".parse().unwrap())
//!     .connection_timeout(Duration::from_secs(10))
//!     .credentials("admin", "secret")
//!     .retry(|r| r
//!         .initial_backoff(Duration::from_millis(100))
//!         .max_backoff(Duration::from_secs(30))
//!         .multiplier(2.0)
//!         .max_retries(10))
//!     .build()
//!     .expect("invalid config");
//! ```
//!
//! ## TLS (requires `tls` feature)
//!
//! ```rust,ignore
//! let config = ClientConfig::builder()
//!     .network(|n| n
//!         .tls(|t| t
//!             .enabled(true)
//!             .ca_cert_path("/path/to/ca.pem")
//!             .client_auth("/path/to/cert.pem", "/path/to/key.pem")))
//!     .build()?;
//! ```
//!
//! # Transactions
//!
//! ACID transactions that span multiple data structures:
//!
//! ```rust,ignore
//! use hazelcast_client::{TransactionOptions, TransactionType};
//! use std::time::Duration;
//!
//! let options = TransactionOptions::new()
//!     .with_timeout(Duration::from_secs(30))
//!     .with_type(TransactionType::TwoPhase);
//!
//! let mut txn = client.new_transaction_context(options);
//! txn.begin().await?;
//!
//! let accounts = txn.get_map::<String, i64>("accounts")?;
//! accounts.put("alice".into(), 950).await?;
//! accounts.put("bob".into(), 1050).await?;
//!
//! txn.commit().await?;
//! ```
//!
//! # Feature Flags
//!
//! | Flag | Purpose |
//! |------|---------|
//! | `tls` | TLS/SSL connections via `rustls` |
//! | `metrics` | Prometheus metrics exporter |
//! | `aws` | AWS EC2 cluster discovery |
//! | `azure` | Azure VM cluster discovery |
//! | `gcp` | GCP Compute Engine discovery |
//! | `kubernetes` | Kubernetes pod discovery |
//! | `cloud` | Hazelcast Cloud discovery |
//! | `kafka` | Kafka connectors for Jet pipelines |
//! | `websocket` | WebSocket transport |

#![warn(missing_docs)]

pub mod cache;
pub mod cluster;
mod client;
pub mod config;
pub mod connection;
pub mod deployment;
pub mod diagnostics;
pub mod executor;
pub mod jet;
pub mod listener;
#[cfg(feature = "metrics")]
pub mod metrics;
pub mod proxy;
pub mod query;
pub mod security;
pub mod sql;
pub mod transaction;

pub use cache::{EvictionPolicy, InMemoryFormat, NearCacheConfig, NearCacheConfigBuilder, QueryCache, QueryCacheConfig, QueryCacheConfigBuilder, QueryCacheStats};
pub use client::HazelcastClient;
pub use executor::{
    Callable, CallableTask, ExecutionCallback, ExecutionTarget, ExecutorService,
    FnExecutionCallback, Runnable, RunnableTask,
};
pub use config::{
    ClientConfig, ClientConfigBuilder, ConfigError, DiagnosticsConfig, DiagnosticsConfigBuilder,
    NetworkConfig, NetworkConfigBuilder, PermissionAction, Permissions, QuorumConfig,
    QuorumConfigBuilder, QuorumFunction, QuorumType, RetryConfig, RetryConfigBuilder,
    SecurityConfig, SecurityConfigBuilder, WanReplicationConfig, WanReplicationConfigBuilder,
    WanReplicationRef, WanReplicationRefBuilder, WanTargetClusterConfig,
    WanTargetClusterConfigBuilder,
};
pub use diagnostics::{OperationTracker, SlowOperationDetector};
#[cfg(feature = "metrics")]
pub use metrics::{
    MetricsRecorderHandle, OperationLatencyGuard, PrometheusError, PrometheusExporter,
    PrometheusExporterBuilder,
};
pub use connection::{
    ClusterDiscovery, Connection, ConnectionEvent, ConnectionId, ConnectionManager,
    StaticAddressDiscovery,
};
pub use hazelcast_core as core;
pub use listener::{
    BoxedEntryListener, BoxedItemListener, EntryListener, FnEntryListener, FnEntryListenerBuilder,
    FnItemListener, ItemEvent, ItemEventType, ItemListener, ItemListenerConfig, LifecycleEvent,
    ListenerId, ListenerRegistration, ListenerStats,
};
pub use proxy::{AtomicReference, EntryView, IList, IMap, IQueue, ISet, ITopic, MultiMap, TopicMessage};
pub use query::{
    AndPredicate, BetweenPredicate, EqualPredicate, FalsePredicate, GreaterThanPredicate,
    InPredicate, LessThanPredicate, LikePredicate, MultiAttributeProjection, NotEqualPredicate,
    NotPredicate, OrPredicate, Predicate, Predicates, Projection, Projections, RegexPredicate,
    SingleAttributeProjection, SqlPredicate, TruePredicate,
};
pub use sql::{
    SqlColumnMetadata, SqlColumnType, SqlResult, SqlRow, SqlRowMetadata, SqlService, SqlStatement,
    SqlValue,
};
pub use transaction::{
    TransactionContext, TransactionOptions, TransactionState, TransactionType,
    TransactionalList, TransactionalMap, TransactionalQueue, TransactionalSet,
    XAResource, XATransaction, XaTransactionState, Xid,
    XA_TMNOFLAGS, XA_TMJOIN, XA_TMRESUME, XA_TMSUCCESS, XA_TMFAIL, XA_TMSUSPEND,
    XA_TMSTARTRSCAN, XA_TMENDRSCAN, XA_TMONEPHASE,
    XA_OK, XA_RDONLY, XA_HEURRB, XA_HEURCOM, XA_HEURHAZ, XA_HEURMIX, XA_RETRY,
    XA_RBBASE, XA_RBROLLBACK, XA_RBCOMMFAIL, XA_RBDEADLOCK, XA_RBINTEGRITY,
    XA_RBOTHER, XA_RBPROTO, XA_RBTIMEOUT, XA_RBTRANSIENT, XA_RBEND,
};
pub use jet::{JetService, Job, JobConfig, JobConfigBuilder, JobStatus, Pipeline, PipelineBuilder};
#[cfg(feature = "kafka")]
pub use jet::{
    kafka_sink, kafka_source, Acks, AutoOffsetReset, CompressionType, IsolationLevel, KafkaSink,
    KafkaSinkConfig, KafkaSinkConfigBuilder, KafkaSource, KafkaSourceConfig,
    KafkaSourceConfigBuilder,
};
pub use cluster::{
    BoxedMigrationListener, BoxedPartitionLostListener, ClientInfo, ClusterService,
    FnMigrationListener, FnPartitionLostListener, MigrationEvent, MigrationListener,
    MigrationState, Partition, PartitionLostEvent, PartitionLostListener, PartitionService,
};
pub use deployment::{
    ClassDefinition, ClassDefinitionBuilder, ClassProviderMode, ResourceEntry,
    UserCodeDeploymentConfig, UserCodeDeploymentConfigBuilder,
};
pub use security::{
    AuthError, AuthResponse, Authenticator, CredentialError, CredentialProvider, Credentials,
    CustomCredentials, DefaultAuthenticator, EnvironmentCredentialProvider, HostnameVerification,
    TlsConfig, TlsConfigBuilder, TlsConfigError, TlsProtocolVersion, cipher_suites,
};

#[cfg(feature = "aws")]
pub use security::AwsCredentialProvider;

#[cfg(feature = "azure")]
pub use security::AzureCredentialProvider;

#[cfg(feature = "gcp")]
pub use security::GcpCredentialProvider;

#[cfg(feature = "kubernetes")]
pub use security::KubernetesCredentialProvider;
