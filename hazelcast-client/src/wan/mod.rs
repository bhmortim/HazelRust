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
