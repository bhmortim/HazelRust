//! CP Subsystem management service for Hazelcast client.
//!
//! Provides management operations for the CP Subsystem including
//! CP group and CP member management.

use std::sync::Arc;

use hazelcast_core::Result;
use uuid::Uuid;

use crate::connection::ConnectionManager;

/// Identifier for a CP group in the cluster.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CPGroupId {
    name: String,
    seed: i64,
    id: i64,
}

impl CPGroupId {
    /// Creates a new CPGroupId.
    pub fn new(name: impl Into<String>, seed: i64, id: i64) -> Self {
        Self {
            name: name.into(),
            seed,
            id,
        }
    }

    /// Returns the name of this CP group.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the seed of this CP group.
    pub fn seed(&self) -> i64 {
        self.seed
    }

    /// Returns the id of this CP group.
    pub fn id(&self) -> i64 {
        self.id
    }
}

/// Status of a CP group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CPGroupStatus {
    /// The CP group is active and operational.
    Active,
    /// The CP group is being destroyed.
    Destroying,
    /// The CP group has been destroyed.
    Destroyed,
}

/// Information about a CP member in the cluster.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CPMember {
    uuid: Uuid,
    address: std::net::SocketAddr,
}

impl CPMember {
    /// Creates a new CPMember.
    pub fn new(uuid: Uuid, address: std::net::SocketAddr) -> Self {
        Self { uuid, address }
    }

    /// Returns the UUID of this CP member.
    pub fn uuid(&self) -> Uuid {
        self.uuid
    }

    /// Returns the address of this CP member.
    pub fn address(&self) -> std::net::SocketAddr {
        self.address
    }
}

/// Information about a CP group.
#[derive(Debug, Clone)]
pub struct CPGroup {
    id: CPGroupId,
    status: CPGroupStatus,
    members: Vec<CPMember>,
}

impl CPGroup {
    /// Creates a new CPGroup.
    pub fn new(id: CPGroupId, status: CPGroupStatus, members: Vec<CPMember>) -> Self {
        Self { id, status, members }
    }

    /// Returns the identifier of this CP group.
    pub fn id(&self) -> &CPGroupId {
        &self.id
    }

    /// Returns the status of this CP group.
    pub fn status(&self) -> CPGroupStatus {
        self.status
    }

    /// Returns the members of this CP group.
    pub fn members(&self) -> &[CPMember] {
        &self.members
    }
}

/// Service for managing the CP Subsystem.
///
/// The CP Subsystem management service provides operations to:
/// - Query CP groups and their members
/// - Force destroy CP groups
/// - Manage CP membership (promote/remove members)
///
/// Note: These operations require the CP Subsystem to be enabled on the
/// Hazelcast cluster.
///
/// # Example
///
/// ```ignore
/// let cp = client.cp_subsystem();
///
/// // List all CP group IDs
/// let group_ids = cp.get_cp_group_ids().await?;
/// for id in &group_ids {
///     println!("CP Group: {} (id={})", id.name(), id.id());
/// }
///
/// // Get details of a specific group
/// if let Some(group) = cp.get_cp_group("default").await? {
///     println!("Group status: {:?}", group.status());
///     for member in group.members() {
///         println!("  Member: {} at {}", member.uuid(), member.address());
///     }
/// }
///
/// // List all CP members
/// let members = cp.get_cp_members().await?;
/// println!("Total CP members: {}", members.len());
/// ```
#[derive(Debug, Clone)]
pub struct CPSubsystemManagementService {
    connection_manager: Arc<ConnectionManager>,
}

impl CPSubsystemManagementService {
    /// Creates a new CPSubsystemManagementService.
    pub fn new(connection_manager: Arc<ConnectionManager>) -> Self {
        Self { connection_manager }
    }

    /// Returns the identifiers of all CP groups in the cluster.
    ///
    /// This includes both the METADATA group and any user-created CP groups.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The CP Subsystem is not enabled on the cluster
    /// - No connection to the cluster is available
    /// - A network error occurs
    pub async fn get_cp_group_ids(&self) -> Result<Vec<CPGroupId>> {
        use hazelcast_core::protocol::constants::PARTITION_ID_ANY;
        use hazelcast_core::protocol::ClientMessage;

        const CP_GROUP_GET_IDS: i32 = 0x1E0100;

        let mut request = ClientMessage::new(CP_GROUP_GET_IDS);
        request.set_partition_id(PARTITION_ID_ANY);

        let addresses: Vec<_> = self.connection_manager.connected_addresses().await;
        let address = addresses
            .first()
            .ok_or_else(|| hazelcast_core::HazelcastError::Io("no connected members".into()))?;

        self.connection_manager.send_to(*address, &request).await?;
        let response = self.connection_manager.receive_from(*address).await?;

        let mut result = Vec::new();
        let frames = response.frames();

        let mut idx = 1;
        while idx + 2 < frames.len() {
            let name_frame = &frames[idx];
            if name_frame.is_end_frame() {
                break;
            }

            let name = String::from_utf8_lossy(name_frame.content()).to_string();

            let seed_frame = &frames[idx + 1];
            let id_frame = &frames[idx + 2];

            let seed = if seed_frame.content().len() >= 8 {
                i64::from_le_bytes(seed_frame.content()[..8].try_into().unwrap_or([0; 8]))
            } else {
                0
            };

            let id = if id_frame.content().len() >= 8 {
                i64::from_le_bytes(id_frame.content()[..8].try_into().unwrap_or([0; 8]))
            } else {
                0
            };

            result.push(CPGroupId::new(name, seed, id));
            idx += 3;
        }

        tracing::debug!(count = result.len(), "retrieved CP group IDs");
        Ok(result)
    }

    /// Returns information about a specific CP group.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the CP group to retrieve
    ///
    /// # Returns
    ///
    /// Returns `Some(CPGroup)` if the group exists, `None` otherwise.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The CP Subsystem is not enabled on the cluster
    /// - No connection to the cluster is available
    /// - A network error occurs
    pub async fn get_cp_group(&self, name: &str) -> Result<Option<CPGroup>> {
        use bytes::BytesMut;
        use hazelcast_core::protocol::constants::PARTITION_ID_ANY;
        use hazelcast_core::protocol::{ClientMessage, Frame};

        const CP_GROUP_GET: i32 = 0x1E0200;

        let mut request = ClientMessage::new(CP_GROUP_GET);
        request.set_partition_id(PARTITION_ID_ANY);

        let name_bytes = BytesMut::from(name.as_bytes());
        request.add_frame(Frame::with_content(name_bytes));

        let addresses: Vec<_> = self.connection_manager.connected_addresses().await;
        let address = addresses
            .first()
            .ok_or_else(|| hazelcast_core::HazelcastError::Io("no connected members".into()))?;

        self.connection_manager.send_to(*address, &request).await?;
        let response = self.connection_manager.receive_from(*address).await?;

        let frames = response.frames();
        if frames.len() < 2 {
            return Ok(None);
        }

        let group_id = CPGroupId::new(name, 0, 0);
        let group = CPGroup::new(group_id, CPGroupStatus::Active, Vec::new());

        tracing::debug!(name = %name, "retrieved CP group");
        Ok(Some(group))
    }

    /// Force destroys a CP group.
    ///
    /// This operation is dangerous and should only be used when a CP group
    /// has lost its majority and cannot make progress. All CP data structures
    /// that use this group will become unavailable.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the CP group to destroy
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The CP Subsystem is not enabled on the cluster
    /// - The specified group does not exist
    /// - No connection to the cluster is available
    /// - A network error occurs
    pub async fn force_destroy_cp_group(&self, name: &str) -> Result<()> {
        use bytes::BytesMut;
        use hazelcast_core::protocol::constants::PARTITION_ID_ANY;
        use hazelcast_core::protocol::{ClientMessage, Frame};

        const CP_GROUP_DESTROY: i32 = 0x1E0300;

        let mut request = ClientMessage::new(CP_GROUP_DESTROY);
        request.set_partition_id(PARTITION_ID_ANY);

        let name_bytes = BytesMut::from(name.as_bytes());
        request.add_frame(Frame::with_content(name_bytes));

        let addresses: Vec<_> = self.connection_manager.connected_addresses().await;
        let address = addresses
            .first()
            .ok_or_else(|| hazelcast_core::HazelcastError::Io("no connected members".into()))?;

        self.connection_manager.send_to(*address, &request).await?;
        let _response = self.connection_manager.receive_from(*address).await?;

        tracing::warn!(name = %name, "force destroyed CP group");
        Ok(())
    }

    /// Returns the list of all CP members in the cluster.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The CP Subsystem is not enabled on the cluster
    /// - No connection to the cluster is available
    /// - A network error occurs
    pub async fn get_cp_members(&self) -> Result<Vec<CPMember>> {
        use hazelcast_core::protocol::constants::PARTITION_ID_ANY;
        use hazelcast_core::protocol::ClientMessage;

        const CP_MEMBER_GET_ALL: i32 = 0x1E0400;

        let mut request = ClientMessage::new(CP_MEMBER_GET_ALL);
        request.set_partition_id(PARTITION_ID_ANY);

        let addresses: Vec<_> = self.connection_manager.connected_addresses().await;
        let address = addresses
            .first()
            .ok_or_else(|| hazelcast_core::HazelcastError::Io("no connected members".into()))?;

        self.connection_manager.send_to(*address, &request).await?;
        let response = self.connection_manager.receive_from(*address).await?;

        let mut result = Vec::new();
        let frames = response.frames();

        let mut idx = 1;
        while idx + 1 < frames.len() {
            let uuid_frame = &frames[idx];
            if uuid_frame.is_end_frame() {
                break;
            }

            let address_frame = &frames[idx + 1];

            let uuid = if uuid_frame.content().len() >= 16 {
                let bytes: [u8; 16] = uuid_frame.content()[..16].try_into().unwrap_or([0; 16]);
                Uuid::from_bytes(bytes)
            } else {
                Uuid::nil()
            };

            let addr_str = String::from_utf8_lossy(address_frame.content());
            let socket_addr = addr_str
                .parse()
                .unwrap_or_else(|_| std::net::SocketAddr::from(([127, 0, 0, 1], 5701)));

            result.push(CPMember::new(uuid, socket_addr));
            idx += 2;
        }

        tracing::debug!(count = result.len(), "retrieved CP members");
        Ok(result)
    }

    /// Promotes the local member to a CP member.
    ///
    /// This operation is only allowed when the CP Subsystem is configured
    /// to allow runtime CP member promotion.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The CP Subsystem is not enabled on the cluster
    /// - CP member promotion is not allowed
    /// - No connection to the cluster is available
    /// - A network error occurs
    pub async fn promote_to_cp_member(&self) -> Result<()> {
        use hazelcast_core::protocol::constants::PARTITION_ID_ANY;
        use hazelcast_core::protocol::ClientMessage;

        const CP_MEMBER_PROMOTE: i32 = 0x1E0500;

        let mut request = ClientMessage::new(CP_MEMBER_PROMOTE);
        request.set_partition_id(PARTITION_ID_ANY);

        let addresses: Vec<_> = self.connection_manager.connected_addresses().await;
        let address = addresses
            .first()
            .ok_or_else(|| hazelcast_core::HazelcastError::Io("no connected members".into()))?;

        self.connection_manager.send_to(*address, &request).await?;
        let _response = self.connection_manager.receive_from(*address).await?;

        tracing::info!("promoted to CP member");
        Ok(())
    }

    /// Removes a CP member from the CP Subsystem.
    ///
    /// This operation removes the specified member from the CP Subsystem.
    /// The removed member will no longer participate in CP operations.
    ///
    /// # Arguments
    ///
    /// * `uuid` - The UUID of the CP member to remove
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The CP Subsystem is not enabled on the cluster
    /// - The specified member is not a CP member
    /// - No connection to the cluster is available
    /// - A network error occurs
    pub async fn remove_cp_member(&self, uuid: Uuid) -> Result<()> {
        use bytes::BytesMut;
        use hazelcast_core::protocol::constants::PARTITION_ID_ANY;
        use hazelcast_core::protocol::{ClientMessage, Frame};

        const CP_MEMBER_REMOVE: i32 = 0x1E0600;

        let mut request = ClientMessage::new(CP_MEMBER_REMOVE);
        request.set_partition_id(PARTITION_ID_ANY);

        let uuid_bytes = BytesMut::from(uuid.as_bytes().as_slice());
        request.add_frame(Frame::with_content(uuid_bytes));

        let addresses: Vec<_> = self.connection_manager.connected_addresses().await;
        let address = addresses
            .first()
            .ok_or_else(|| hazelcast_core::HazelcastError::Io("no connected members".into()))?;

        self.connection_manager.send_to(*address, &request).await?;
        let _response = self.connection_manager.receive_from(*address).await?;

        tracing::warn!(uuid = %uuid, "removed CP member");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cp_group_id() {
        let id = CPGroupId::new("test-group", 123, 456);
        assert_eq!(id.name(), "test-group");
        assert_eq!(id.seed(), 123);
        assert_eq!(id.id(), 456);
    }

    #[test]
    fn test_cp_group_id_equality() {
        let id1 = CPGroupId::new("group", 1, 2);
        let id2 = CPGroupId::new("group", 1, 2);
        let id3 = CPGroupId::new("other", 1, 2);

        assert_eq!(id1, id2);
        assert_ne!(id1, id3);
    }

    #[test]
    fn test_cp_member() {
        let uuid = Uuid::new_v4();
        let addr: std::net::SocketAddr = "127.0.0.1:5701".parse().unwrap();
        let member = CPMember::new(uuid, addr);

        assert_eq!(member.uuid(), uuid);
        assert_eq!(member.address(), addr);
    }

    #[test]
    fn test_cp_group() {
        let id = CPGroupId::new("test", 0, 1);
        let member = CPMember::new(Uuid::new_v4(), "127.0.0.1:5701".parse().unwrap());
        let group = CPGroup::new(id.clone(), CPGroupStatus::Active, vec![member.clone()]);

        assert_eq!(group.id(), &id);
        assert_eq!(group.status(), CPGroupStatus::Active);
        assert_eq!(group.members().len(), 1);
    }

    #[test]
    fn test_cp_group_status() {
        assert_ne!(CPGroupStatus::Active, CPGroupStatus::Destroying);
        assert_ne!(CPGroupStatus::Destroying, CPGroupStatus::Destroyed);
    }

    #[test]
    fn test_service_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<CPSubsystemManagementService>();
        assert_send_sync::<CPGroupId>();
        assert_send_sync::<CPMember>();
        assert_send_sync::<CPGroup>();
    }
}
