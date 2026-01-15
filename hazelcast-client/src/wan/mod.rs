//! WAN Replication support for cross-datacenter data synchronization.
//!
//! This module provides client-side WAN replication awareness, allowing
//! the client to forward events to target clusters in different data centers.
//!
//! # Example
//!
//! ```ignore
//! use hazelcast_client::wan::{WanPublisher, WanEvent, WanEventType};
//! use hazelcast_client::config::{WanReplicationConfigBuilder, WanTargetClusterConfigBuilder};
//!
//! // Configure target cluster
//! let target = WanTargetClusterConfigBuilder::new("dc-west")
//!     .add_endpoint("10.1.0.1:5701".parse().unwrap())
//!     .build()
//!     .unwrap();
//!
//! // Configure WAN replication scheme
//! let wan_config = WanReplicationConfigBuilder::new("geo-replication")
//!     .add_target_cluster(target)
//!     .build()
//!     .unwrap();
//!
//! // Create publisher and forward events
//! let publisher = WanPublisher::new(wan_config);
//! publisher.start().await?;
//!
//! let event = WanEvent::new(
//!     WanEventType::Put,
//!     "my-map",
//!     key_data,
//!     Some(value_data),
//! );
//! publisher.publish(event).await?;
//! ```

mod event;
mod publisher;

pub use event::{WanEvent, WanEventType};
pub use publisher::{WanPublisher, WanPublisherState, WanPublishError};
