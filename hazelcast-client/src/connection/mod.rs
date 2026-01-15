//! Connection management for Hazelcast client.

mod connection;
mod discovery;
mod manager;

#[cfg(feature = "aws")]
mod aws;

pub use connection::{Connection, ConnectionId};
pub use discovery::{ClusterDiscovery, StaticAddressDiscovery};
pub use manager::{ConnectionEvent, ConnectionManager};

#[cfg(feature = "aws")]
pub use aws::{AwsDiscovery, AwsDiscoveryConfig};
