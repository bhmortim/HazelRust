//! Connection management for Hazelcast client.

mod connection;
mod discovery;
mod manager;

pub use connection::{Connection, ConnectionId};
pub use discovery::{ClusterDiscovery, StaticAddressDiscovery};
pub use manager::{ConnectionEvent, ConnectionManager};
