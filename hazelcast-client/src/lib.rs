//! Hazelcast client implementation.

#![warn(missing_docs)]

pub mod cache;
mod client;
pub mod config;
pub mod connection;
pub mod executor;
pub mod listener;
pub mod proxy;
pub mod query;
pub mod sql;
pub mod transaction;

pub use cache::{EvictionPolicy, InMemoryFormat, NearCacheConfig, NearCacheConfigBuilder};
pub use client::HazelcastClient;
pub use executor::{
    Callable, CallableTask, ExecutionCallback, ExecutionTarget, ExecutorService,
    FnExecutionCallback, Runnable, RunnableTask,
};
pub use config::{
    ClientConfig, ClientConfigBuilder, ConfigError, NetworkConfig, NetworkConfigBuilder,
    RetryConfig, RetryConfigBuilder, SecurityConfig, SecurityConfigBuilder,
};
pub use connection::{
    ClusterDiscovery, Connection, ConnectionEvent, ConnectionId, ConnectionManager,
    StaticAddressDiscovery,
};
pub use hazelcast_core as core;
pub use listener::{ListenerId, ListenerRegistration, ListenerStats};
pub use proxy::{IList, IMap, IQueue, ISet, ITopic, MultiMap, TopicMessage};
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
};
