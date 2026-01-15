//! Hazelcast client implementation.

#![warn(missing_docs)]

mod client;
pub mod config;
pub mod connection;
pub mod listener;
pub mod proxy;
pub mod sql;

pub use client::HazelcastClient;
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
pub use sql::{
    SqlColumnMetadata, SqlColumnType, SqlResult, SqlRow, SqlRowMetadata, SqlService, SqlStatement,
    SqlValue,
};
