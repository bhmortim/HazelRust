//! Connection management for Hazelcast client.

mod connection;
mod data_connection;
mod discovery;
mod interceptor;
mod load_balancer;
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

mod multicast;
mod auto_detect;

#[cfg(feature = "eureka")]
mod eureka;

#[cfg(feature = "websocket")]
mod websocket;

pub use connection::{Connection, ConnectionId};
pub use data_connection::{DataConnectionConfig, DataConnectionService};
pub use discovery::{ClusterDiscovery, StaticAddressDiscovery};
pub use load_balancer::{
    default_load_balancer, LoadBalancer, RandomLoadBalancer, RoundRobinLoadBalancer,
};
pub use interceptor::{SocketInterceptor, SocketOptions};
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

pub use multicast::{MulticastDiscovery, MulticastDiscoveryConfig};
pub use auto_detect::{AutoDetectionDiscovery, DetectedEnvironment};

#[cfg(feature = "eureka")]
pub use eureka::{EurekaDiscovery, EurekaDiscoveryConfig};

#[cfg(feature = "websocket")]
pub use websocket::WebSocketConnection;
