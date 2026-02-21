//! Cluster management services for Hazelcast client.

mod cluster_service;
mod cp_management;
mod cp_session;
mod lifecycle_service;
mod partition_service;
mod split_brain;

pub use cluster_service::{ClientInfo, ClusterService, ClusterView};
pub use cp_management::{CPGroup, CPGroupId, CPGroupStatus, CPMember, CPSubsystemManagementService};
pub use cp_session::{CPSession, CPSessionEndpointType, CPSessionId, CPSessionManagementService};
pub use lifecycle_service::{LifecycleListenerRegistration, LifecycleService};
pub use partition_service::{
    BoxedMigrationListener, BoxedPartitionLostListener, FnMigrationListener,
    FnPartitionLostListener, MigrationEvent, MigrationListener, MigrationState, Partition,
    PartitionLostEvent, PartitionLostListener, PartitionService,
};
pub use split_brain::SplitBrainProtectionService;
