/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

mod auto_detect;
mod multicast;

#[cfg(feature = "eureka")]
mod eureka;

#[cfg(feature = "websocket")]
mod websocket;

pub use connection::{Connection, ConnectionId};
pub use data_connection::{DataConnectionConfig, DataConnectionService};
pub use discovery::{ClusterDiscovery, StaticAddressDiscovery};
pub use interceptor::{SocketInterceptor, SocketOptions};
pub use load_balancer::{
    default_load_balancer, LoadBalancer, RandomLoadBalancer, RoundRobinLoadBalancer,
};
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

pub use auto_detect::{AutoDetectionDiscovery, DetectedEnvironment};
pub use multicast::{MulticastDiscovery, MulticastDiscoveryConfig};

#[cfg(feature = "eureka")]
pub use eureka::{EurekaDiscovery, EurekaDiscoveryConfig};

#[cfg(feature = "websocket")]
pub use websocket::WebSocketConnection;
pub mod invocation;
