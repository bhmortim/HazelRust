//! CP Subsystem management service for Hazelcast client.
//!
//! Provides management operations for the CP Subsystem including
//! CP group and CP member management.

use std::sync::Arc;

use bytes::BytesMut;
use hazelcast_core::protocol::constants::{
    CP_SUBSYSTEM_FORCE_DESTROY_GROUP, CP_SUBSYSTEM_GET_CP_MEMBERS, CP_SUBSYSTEM_GET_GROUP,
    CP_SUBSYSTEM_GET_GROUP_IDS, CP_SUBSYSTEM_PROMOTE_TO_CP_MEMBER, CP_SUBSYSTEM_REMOVE_CP_MEMBER,
    PARTITION_ID_ANY,
};
use hazelcast_core::protocol::{ClientMessage, Frame};
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
        let mut request = ClientMessage::new_request(CP_SUBSYSTEM_GET_GROUP_IDS);
        request.set_partition_id(PARTITION_ID_ANY);

        let response = self.connection_manager.send(request).await?;

        let mut result = Vec::new();
        let frames = response.frames();

        if frames.len() < 2 {
            tracing::debug!(count = 0, "retrieved CP group IDs (empty response)");
            return Ok(result);
        }

        let mut idx = 1;
        while idx < frames.len() {
            let name_frame = &frames[idx];
            if name_frame.is_end_frame() || name_frame.content().is_empty() {
                break;
            }

            if idx + 2 >= frames.len() {
                break;
            }

            let name = String::from_utf8_lossy(name_frame.content()).to_string();

            let seed_frame = &frames[idx + 1];
            let id_frame = &frames[idx + 2];

            let seed = Self::parse_i64_from_frame(seed_frame);
            let id = Self::parse_i64_from_frame(id_frame);

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
        let mut request = ClientMessage::new_request(CP_SUBSYSTEM_GET_GROUP);
        request.set_partition_id(PARTITION_ID_ANY);

        let name_bytes = BytesMut::from(name.as_bytes());
        request.add_frame(Frame::with_content(name_bytes));

        let response = self.connection_manager.send(request).await?;

        let frames = response.frames();

        // Check for null/empty response indicating group doesn't exist
        if frames.len() < 2 {
            return Ok(None);
        }

        // Check if first data frame is a null frame
        if frames.get(1).map_or(true, |f| f.is_null_frame()) {
            return Ok(None);
        }

        // Parse group id components
        let mut idx = 1;

        // Parse group name
        let group_name = if idx < frames.len() && !frames[idx].is_end_frame() {
            let n = String::from_utf8_lossy(frames[idx].content()).to_string();
            idx += 1;
            n
        } else {
            name.to_string()
        };

        // Parse seed
        let seed = if idx < frames.len() && !frames[idx].is_end_frame() {
            let s = Self::parse_i64_from_frame(&frames[idx]);
            idx += 1;
            s
        } else {
            0
        };

        // Parse id
        let group_id_value = if idx < frames.len() && !frames[idx].is_end_frame() {
            let i = Self::parse_i64_from_frame(&frames[idx]);
            idx += 1;
            i
        } else {
            0
        };

        let group_id = CPGroupId::new(group_name, seed, group_id_value);

        // Parse status (integer: 0=Active, 1=Destroying, 2=Destroyed)
        let status = if idx < frames.len() && !frames[idx].is_end_frame() {
            let status_value = Self::parse_i32_from_frame(&frames[idx]);
            idx += 1;
            Self::status_from_int(status_value)
        } else {
            CPGroupStatus::Active
        };

        // Parse members list
        let mut members = Vec::new();
        while idx + 1 < frames.len() {
            let uuid_frame = &frames[idx];
            if uuid_frame.is_end_frame() {
                break;
            }

            let address_frame = &frames[idx + 1];
            if address_frame.is_end_frame() {
                break;
            }

            let uuid = Self::parse_uuid_from_frame(uuid_frame);
            let socket_addr = Self::parse_address_from_frame(address_frame);

            members.push(CPMember::new(uuid, socket_addr));
            idx += 2;
        }

        let member_count = members.len();
        let group = CPGroup::new(group_id, status, members);

        tracing::debug!(name = %name, status = ?status, members = member_count, "retrieved CP group");
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
        let mut request = ClientMessage::new_request(CP_SUBSYSTEM_FORCE_DESTROY_GROUP);
        request.set_partition_id(PARTITION_ID_ANY);

        let name_bytes = BytesMut::from(name.as_bytes());
        request.add_frame(Frame::with_content(name_bytes));

        let _response = self.connection_manager.send(request).await?;

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
        let mut request = ClientMessage::new_request(CP_SUBSYSTEM_GET_CP_MEMBERS);
        request.set_partition_id(PARTITION_ID_ANY);

        let response = self.connection_manager.send(request).await?;

        let mut result = Vec::new();
        let frames = response.frames();

        if frames.len() < 2 {
            tracing::debug!(count = 0, "retrieved CP members (empty response)");
            return Ok(result);
        }

        let mut idx = 1;
        while idx < frames.len() {
            let uuid_frame = &frames[idx];
            if uuid_frame.is_end_frame() || uuid_frame.content().is_empty() {
                break;
            }

            if idx + 1 >= frames.len() {
                break;
            }

            let address_frame = &frames[idx + 1];
            if address_frame.is_end_frame() {
                break;
            }

            let uuid = Self::parse_uuid_from_frame(uuid_frame);
            let socket_addr = Self::parse_address_from_frame(address_frame);

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
        let mut request = ClientMessage::new_request(CP_SUBSYSTEM_PROMOTE_TO_CP_MEMBER);
        request.set_partition_id(PARTITION_ID_ANY);

        let _response = self.connection_manager.send(request).await?;

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
        let mut request = ClientMessage::new_request(CP_SUBSYSTEM_REMOVE_CP_MEMBER);
        request.set_partition_id(PARTITION_ID_ANY);

        let uuid_bytes = BytesMut::from(uuid.as_bytes().as_slice());
        request.add_frame(Frame::with_content(uuid_bytes));

        let _response = self.connection_manager.send(request).await?;

        tracing::warn!(uuid = %uuid, "removed CP member");
        Ok(())
    }

    fn parse_i64_from_frame(frame: &Frame) -> i64 {
        if frame.content().len() >= 8 {
            i64::from_le_bytes(frame.content()[..8].try_into().unwrap_or([0; 8]))
        } else {
            0
        }
    }

    fn parse_i32_from_frame(frame: &Frame) -> i32 {
        if frame.content().len() >= 4 {
            i32::from_le_bytes(frame.content()[..4].try_into().unwrap_or([0; 4]))
        } else {
            0
        }
    }

    fn parse_uuid_from_frame(frame: &Frame) -> Uuid {
        if frame.content().len() >= 16 {
            let bytes: [u8; 16] = frame.content()[..16].try_into().unwrap_or([0; 16]);
            Uuid::from_bytes(bytes)
        } else {
            Uuid::nil()
        }
    }

    fn parse_address_from_frame(frame: &Frame) -> std::net::SocketAddr {
        let addr_str = String::from_utf8_lossy(frame.content());
        addr_str
            .parse()
            .unwrap_or_else(|_| std::net::SocketAddr::from(([127, 0, 0, 1], 5701)))
    }

    fn status_from_int(value: i32) -> CPGroupStatus {
        match value {
            1 => CPGroupStatus::Destroying,
            2 => CPGroupStatus::Destroyed,
            _ => CPGroupStatus::Active,
        }
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

    #[test]
    fn test_cp_group_status_values() {
        let active = CPGroupStatus::Active;
        let destroying = CPGroupStatus::Destroying;
        let destroyed = CPGroupStatus::Destroyed;

        assert_eq!(active, CPGroupStatus::Active);
        assert_eq!(destroying, CPGroupStatus::Destroying);
        assert_eq!(destroyed, CPGroupStatus::Destroyed);
    }

    #[test]
    fn test_cp_group_id_clone() {
        let id1 = CPGroupId::new("test", 100, 200);
        let id2 = id1.clone();

        assert_eq!(id1.name(), id2.name());
        assert_eq!(id1.seed(), id2.seed());
        assert_eq!(id1.id(), id2.id());
    }

    #[test]
    fn test_cp_group_id_hash() {
        use std::collections::HashSet;

        let id1 = CPGroupId::new("group1", 1, 1);
        let id2 = CPGroupId::new("group1", 1, 1);
        let id3 = CPGroupId::new("group2", 1, 1);

        let mut set = HashSet::new();
        set.insert(id1.clone());

        set.insert(id2);
        assert_eq!(set.len(), 1);

        set.insert(id3);
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn test_cp_member_equality() {
        let uuid = Uuid::new_v4();
        let addr: std::net::SocketAddr = "192.168.1.1:5701".parse().unwrap();

        let m1 = CPMember::new(uuid, addr);
        let m2 = CPMember::new(uuid, addr);
        let m3 = CPMember::new(Uuid::new_v4(), addr);

        assert_eq!(m1, m2);
        assert_ne!(m1, m3);
    }

    #[test]
    fn test_cp_group_with_multiple_members() {
        let id = CPGroupId::new("raft-group", 42, 1);
        let members = vec![
            CPMember::new(Uuid::new_v4(), "127.0.0.1:5701".parse().unwrap()),
            CPMember::new(Uuid::new_v4(), "127.0.0.1:5702".parse().unwrap()),
            CPMember::new(Uuid::new_v4(), "127.0.0.1:5703".parse().unwrap()),
        ];

        let group = CPGroup::new(id, CPGroupStatus::Active, members);

        assert_eq!(group.members().len(), 3);
        assert_eq!(group.status(), CPGroupStatus::Active);
        assert_eq!(group.id().name(), "raft-group");
    }

    #[test]
    fn test_cp_group_destroying_status() {
        let id = CPGroupId::new("dying-group", 1, 1);
        let group = CPGroup::new(id, CPGroupStatus::Destroying, Vec::new());

        assert_eq!(group.status(), CPGroupStatus::Destroying);
        assert!(group.members().is_empty());
    }

    #[test]
    fn test_cp_group_destroyed_status() {
        let id = CPGroupId::new("dead-group", 1, 1);
        let group = CPGroup::new(id, CPGroupStatus::Destroyed, Vec::new());

        assert_eq!(group.status(), CPGroupStatus::Destroyed);
    }

    #[test]
    fn test_cp_group_id_default_metadata_group() {
        let metadata_id = CPGroupId::new("METADATA", 0, 0);
        assert_eq!(metadata_id.name(), "METADATA");
    }

    #[test]
    fn test_cp_member_ipv6_address() {
        let uuid = Uuid::new_v4();
        let addr: std::net::SocketAddr = "[::1]:5701".parse().unwrap();
        let member = CPMember::new(uuid, addr);

        assert_eq!(member.address().to_string(), "[::1]:5701");
    }

    #[test]
    fn test_cp_group_id_debug_format() {
        let id = CPGroupId::new("test-group", 123, 456);
        let debug_str = format!("{:?}", id);

        assert!(debug_str.contains("test-group"));
        assert!(debug_str.contains("123"));
        assert!(debug_str.contains("456"));
    }

    #[test]
    fn test_cp_group_status_is_copy() {
        let status1 = CPGroupStatus::Active;
        let status2 = status1;
        assert_eq!(status1, status2);
    }
}
