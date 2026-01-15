//! Connection management for Hazelcast client.

mod connection;
mod discovery;
mod manager;

#[cfg(feature = "aws")]
mod aws;

#[cfg(feature = "kubernetes")]
mod kubernetes;

#[cfg(feature = "cloud")]
mod cloud;

#[cfg(feature = "azure")]
mod azure;

#[cfg(feature = "gcp")]
mod gcp;

#[cfg(feature = "websocket")]
mod websocket;

pub use connection::{Connection, ConnectionId};
pub use discovery::{ClusterDiscovery, StaticAddressDiscovery};
pub use manager::{ConnectionEvent, ConnectionManager};

#[cfg(feature = "aws")]
pub use aws::{AwsDiscovery, AwsDiscoveryConfig};

#[cfg(feature = "kubernetes")]
pub use kubernetes::{KubernetesDiscovery, KubernetesDiscoveryConfig};

#[cfg(feature = "cloud")]
pub use cloud::{CloudDiscovery, CloudDiscoveryConfig};

#[cfg(feature = "azure")]
pub use azure::{AzureDiscovery, AzureDiscoveryConfig};

#[cfg(feature = "gcp")]
pub use gcp::{GcpDiscovery, GcpDiscoveryConfig};

#[cfg(feature = "websocket")]
pub use websocket::WebSocketConnection;
