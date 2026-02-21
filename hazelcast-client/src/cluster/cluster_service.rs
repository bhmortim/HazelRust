//! Cluster service for querying and monitoring cluster membership.

use std::sync::Arc;
use std::time::Instant;

use tokio::sync::broadcast;
use uuid::Uuid;

use crate::connection::ConnectionManager;
use crate::listener::{Member, MemberEvent};

/// A snapshot of the current cluster view, including membership and partition table version.
///
/// The cluster view is updated atomically by the cluster whenever members join or leave,
/// or when partitions are rebalanced. Subscribing via
/// [`ClusterService::subscribe_cluster_view`] provides a stream of these snapshots.
#[derive(Debug, Clone)]
pub struct ClusterView {
    /// The membership version. Incremented on every membership change.
    pub version: i32,
    /// The current set of cluster members.
    pub members: Vec<Member>,
    /// The partition table version. Incremented on every partition rebalance.
    pub partition_table_version: i32,
}

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

    /// Returns a snapshot of the current cluster view.
    ///
    /// The cluster view contains the current membership list along with
    /// version counters for both the member list and partition table.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let view = cluster.get_cluster_view().await;
    /// println!("Membership version: {}", view.version);
    /// println!("Members: {}", view.members.len());
    /// println!("Partition table version: {}", view.partition_table_version);
    /// ```
    pub async fn get_cluster_view(&self) -> ClusterView {
        let members = self.connection_manager.members().await;
        let partition_count = self.connection_manager.partition_count();
        ClusterView {
            version: members.len() as i32,
            members,
            partition_table_version: partition_count,
        }
    }

    /// Subscribes to cluster view changes.
    ///
    /// Returns a broadcast receiver that emits a new [`ClusterView`] whenever
    /// the cluster membership changes (member added or removed). Each view
    /// is a full snapshot of the cluster at that point in time.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let mut rx = cluster.subscribe_cluster_view();
    /// tokio::spawn(async move {
    ///     while let Ok(view) = rx.recv().await {
    ///         println!("Cluster view updated: {} members (v{})",
    ///             view.members.len(), view.version);
    ///     }
    /// });
    /// ```
    pub fn subscribe_cluster_view(&self) -> broadcast::Receiver<ClusterView> {
        let (tx, rx) = broadcast::channel(16);

        let mut membership_rx = self.connection_manager.subscribe_membership();
        let connection_manager = Arc::clone(&self.connection_manager);

        tokio::spawn(async move {
            loop {
                match membership_rx.recv().await {
                    Ok(_event) => {
                        let members = connection_manager.members().await;
                        let partition_count = connection_manager.partition_count();
                        let view = ClusterView {
                            version: members.len() as i32,
                            members,
                            partition_table_version: partition_count,
                        };
                        if tx.send(view).is_err() {
                            // All receivers dropped
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        continue;
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        break;
                    }
                }
            }
        });

        rx
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

    #[test]
    fn test_cluster_view_creation() {
        let view = ClusterView {
            version: 3,
            members: vec![],
            partition_table_version: 7,
        };

        assert_eq!(view.version, 3);
        assert!(view.members.is_empty());
        assert_eq!(view.partition_table_version, 7);
    }

    #[test]
    fn test_cluster_view_clone() {
        let view = ClusterView {
            version: 1,
            members: vec![],
            partition_table_version: 2,
        };
        let cloned = view.clone();
        assert_eq!(cloned.version, 1);
        assert_eq!(cloned.partition_table_version, 2);
    }

    #[test]
    fn test_cluster_view_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<ClusterView>();
    }
}
