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

//! Cluster management services for Hazelcast client.

mod cluster_service;
pub(crate) mod cluster_view;
mod cp_management;
mod cp_session;
mod lifecycle_service;
mod partition_service;
mod split_brain;

pub use cluster_service::{ClientInfo, ClusterService, ClusterView};
pub use cp_management::{
    CPGroup, CPGroupId, CPGroupStatus, CPMember, CPSubsystemManagementService,
};
pub use cp_session::{
    CPSession, CPSessionEndpointType, CPSessionId, CPSessionManagementService, CPSessionManager,
    NO_SESSION_ID,
};
pub use lifecycle_service::{LifecycleListenerRegistration, LifecycleService};
pub use partition_service::{
    BoxedMigrationListener, BoxedPartitionLostListener, FnMigrationListener,
    FnPartitionLostListener, MigrationEvent, MigrationListener, MigrationState, Partition,
    PartitionLostEvent, PartitionLostListener, PartitionService,
};
pub use split_brain::SplitBrainProtectionService;
