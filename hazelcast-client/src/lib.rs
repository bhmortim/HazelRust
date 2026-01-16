//! Hazelcast client implementation.

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
pub use proxy::{EntryView, IList, IMap, IQueue, ISet, ITopic, MultiMap, TopicMessage};
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
pub use cluster::{
    ClientInfo, ClusterService, MigrationEvent, MigrationListener, MigrationState, Partition,
    PartitionService,
};
pub use deployment::{
    ClassDefinition, ClassDefinitionBuilder, ClassProviderMode, ResourceEntry,
    UserCodeDeploymentConfig, UserCodeDeploymentConfigBuilder,
};
pub use security::{
    AuthError, AuthResponse, Authenticator, Credentials, CustomCredentials, DefaultAuthenticator,
};
