//! Cluster service for querying and monitoring cluster membership.

use std::sync::Arc;
use std::time::Instant;

use tokio::sync::broadcast;
use uuid::Uuid;

use crate::connection::ConnectionManager;
use crate::listener::{Member, MemberEvent};

/// Information about the local Hazelcast client instance.
#[derive(Debug, Clone)]
pub struct ClientInfo {
    uuid: Uuid,
    name: String,
    labels: Vec<String>,
}

impl ClientInfo {
    /// Creates a new client info instance.
    pub fn new(uuid: Uuid, name: String, labels: Vec<String>) -> Self {
        Self { uuid, name, labels }
    }

    /// Returns the unique identifier of this client instance.
    pub fn uuid(&self) -> Uuid {
        self.uuid
    }

    /// Returns the name of this client instance.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the labels associated with this client.
    pub fn labels(&self) -> &[String] {
        &self.labels
    }
}

impl std::fmt::Display for ClientInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ClientInfo[uuid={}, name={}]", self.uuid, self.name)
    }
}

/// Service for querying and monitoring cluster membership.
///
/// The cluster service provides access to:
/// - Current cluster members and their attributes
/// - Local client information
/// - Cluster time (synchronized across the cluster)
/// - Membership event notifications
///
/// # Example
///
/// ```ignore
/// let cluster = client.cluster();
///
/// // Get all cluster members
/// let members = cluster.get_members().await;
/// for member in members {
///     println!("Member: {} at {}", member.uuid(), member.address());
/// }
///
/// // Get local client info
/// let client_info = cluster.get_local_client();
/// println!("Client UUID: {}", client_info.uuid());
///
/// // Subscribe to membership changes
/// let mut listener = cluster.add_membership_listener();
/// tokio::spawn(async move {
///     while let Ok(event) = listener.recv().await {
///         println!("Membership event: {}", event);
///     }
/// });
/// ```
#[derive(Debug, Clone)]
pub struct ClusterService {
    connection_manager: Arc<ConnectionManager>,
    client_uuid: Uuid,
    client_name: String,
    client_labels: Vec<String>,
    start_time: Instant,
}

impl ClusterService {
    /// Creates a new cluster service.
    pub(crate) fn new(
        connection_manager: Arc<ConnectionManager>,
        client_uuid: Uuid,
        client_name: String,
        client_labels: Vec<String>,
    ) -> Self {
        Self {
            connection_manager,
            client_uuid,
            client_name,
            client_labels,
            start_time: Instant::now(),
        }
    }

    /// Returns all members currently known to be in the cluster.
    ///
    /// The returned list includes both data members and lite members.
    /// The list is updated automatically as members join or leave.
    pub async fn get_members(&self) -> Vec<Member> {
        self.connection_manager.members().await
    }

    /// Returns the member with the specified UUID, if present in the cluster.
    ///
    /// Returns `None` if no member with the given UUID is currently known.
    pub async fn get_member(&self, uuid: &Uuid) -> Option<Member> {
        self.connection_manager.get_member(uuid).await
    }

    /// Returns information about the local client instance.
    ///
    /// This includes the client's UUID, name, and any configured labels.
    pub fn get_local_client(&self) -> ClientInfo {
        ClientInfo::new(
            self.client_uuid,
            self.client_name.clone(),
            self.client_labels.clone(),
        )
    }

    /// Returns the current cluster time.
    ///
    /// The cluster time is synchronized across all cluster members and clients,
    /// providing a consistent view of time for distributed operations.
    ///
    /// Note: In the current implementation, this returns the local system time.
    /// Full cluster time synchronization requires additional protocol support.
    pub fn get_cluster_time(&self) -> Instant {
        Instant::now()
    }

    /// Returns the time elapsed since this client connected to the cluster.
    pub fn uptime(&self) -> std::time::Duration {
        self.start_time.elapsed()
    }

    /// Subscribes to cluster membership events.
    ///
    /// Returns a broadcast receiver that emits `MemberEvent`s when members
    /// join or leave the cluster.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let mut listener = cluster.add_membership_listener();
    /// while let Ok(event) = listener.recv().await {
    ///     match event.event_type {
    ///         MemberEventType::Added => println!("Member joined: {}", event.member),
    ///         MemberEventType::Removed => println!("Member left: {}", event.member),
    ///     }
    /// }
    /// ```
    pub fn add_membership_listener(&self) -> broadcast::Receiver<MemberEvent> {
        self.connection_manager.subscribe_membership()
    }

    /// Returns the number of members currently in the cluster.
    pub async fn member_count(&self) -> usize {
        self.connection_manager.member_count().await
    }

    /// Returns whether this client is connected to the cluster.
    pub async fn is_connected(&self) -> bool {
        self.connection_manager.connection_count().await > 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_info_creation() {
        let uuid = Uuid::new_v4();
        let info = ClientInfo::new(uuid, "test-client".to_string(), vec!["label1".to_string()]);

        assert_eq!(info.uuid(), uuid);
        assert_eq!(info.name(), "test-client");
        assert_eq!(info.labels(), &["label1".to_string()]);
    }

    #[test]
    fn test_client_info_display() {
        let uuid = Uuid::new_v4();
        let info = ClientInfo::new(uuid, "my-client".to_string(), vec![]);

        let display = info.to_string();
        assert!(display.contains("ClientInfo["));
        assert!(display.contains(&uuid.to_string()));
        assert!(display.contains("my-client"));
    }

    #[test]
    fn test_client_info_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<ClientInfo>();
    }

    #[test]
    fn test_cluster_service_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<ClusterService>();
    }
}
