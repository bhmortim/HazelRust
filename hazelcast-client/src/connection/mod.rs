//! Connection management for Hazelcast client.

mod connection;
mod discovery;
mod manager;

#[cfg(feature = "aws")]
mod aws;

#[cfg(feature = "kubernetes")]
mod kubernetes;

pub use connection::{Connection, ConnectionId};
pub use discovery::{ClusterDiscovery, StaticAddressDiscovery};
pub use manager::{ConnectionEvent, ConnectionManager};

#[cfg(feature = "aws")]
pub use aws::{AwsDiscovery, AwsDiscoveryConfig};

#[cfg(feature = "kubernetes")]
pub use kubernetes::{KubernetesDiscovery, KubernetesDiscoveryConfig};
