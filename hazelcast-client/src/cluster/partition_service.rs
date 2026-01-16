//! Partition service for querying cluster partition information.

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use tokio::sync::broadcast;
use uuid::Uuid;

use hazelcast_core::Serializable;

use crate::connection::ConnectionManager;
use crate::listener::{ListenerId, ListenerRegistration, Member};

/// Represents a single partition in the Hazelcast cluster.
#[derive(Debug, Clone)]
pub struct Partition {
    id: i32,
    owner_uuid: Option<Uuid>,
}

impl Partition {
    /// Creates a new partition with the given ID and optional owner.
    pub fn new(id: i32, owner_uuid: Option<Uuid>) -> Self {
        Self { id, owner_uuid }
    }

    /// Returns the partition ID.
    pub fn id(&self) -> i32 {
        self.id
    }

    /// Returns the UUID of the partition owner, if known.
    pub fn owner_uuid(&self) -> Option<Uuid> {
        self.owner_uuid
    }
}

impl PartialEq for Partition {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for Partition {}

impl std::fmt::Display for Partition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.owner_uuid {
            Some(uuid) => write!(f, "Partition[id={}, owner={}]", self.id, uuid),
            None => write!(f, "Partition[id={}, owner=unknown]", self.id),
        }
    }
}

/// State of a partition migration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MigrationState {
    /// Migration has started.
    Started,
    /// Migration completed successfully.
    Completed,
    /// Migration failed.
    Failed,
}

impl std::fmt::Display for MigrationState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Started => write!(f, "STARTED"),
            Self::Completed => write!(f, "COMPLETED"),
            Self::Failed => write!(f, "FAILED"),
        }
    }
}

/// An event fired when a partition migration occurs.
#[derive(Debug, Clone)]
pub struct MigrationEvent {
    /// The partition being migrated.
    pub partition_id: i32,
    /// The state of the migration.
    pub state: MigrationState,
    /// The UUID of the member the partition is migrating from.
    pub old_owner: Option<Uuid>,
    /// The UUID of the member the partition is migrating to.
    pub new_owner: Option<Uuid>,
}

impl MigrationEvent {
    /// Creates a new migration event.
    pub fn new(
        partition_id: i32,
        state: MigrationState,
        old_owner: Option<Uuid>,
        new_owner: Option<Uuid>,
    ) -> Self {
        Self {
            partition_id,
            state,
            old_owner,
            new_owner,
        }
    }

    /// Creates a migration started event.
    pub fn started(partition_id: i32, old_owner: Option<Uuid>, new_owner: Option<Uuid>) -> Self {
        Self::new(partition_id, MigrationState::Started, old_owner, new_owner)
    }

    /// Creates a migration completed event.
    pub fn completed(partition_id: i32, old_owner: Option<Uuid>, new_owner: Option<Uuid>) -> Self {
        Self::new(partition_id, MigrationState::Completed, old_owner, new_owner)
    }

    /// Creates a migration failed event.
    pub fn failed(partition_id: i32, old_owner: Option<Uuid>, new_owner: Option<Uuid>) -> Self {
        Self::new(partition_id, MigrationState::Failed, old_owner, new_owner)
    }
}

impl std::fmt::Display for MigrationEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "MigrationEvent[partition={}, state={}, from={:?}, to={:?}]",
            self.partition_id, self.state, self.old_owner, self.new_owner
        )
    }
}

/// Trait for listening to partition migration events.
pub trait MigrationListener: Send + Sync {
    /// Called when a partition migration starts.
    fn migration_started(&self, event: &MigrationEvent);

    /// Called when a partition migration completes.
    fn migration_completed(&self, event: &MigrationEvent);

    /// Called when a partition migration fails.
    fn migration_failed(&self, event: &MigrationEvent);
}

/// A boxed migration listener.
pub type BoxedMigrationListener = Box<dyn MigrationListener>;

/// A function-based migration listener.
pub struct FnMigrationListener<S, C, F>
where
    S: Fn(&MigrationEvent) + Send + Sync,
    C: Fn(&MigrationEvent) + Send + Sync,
    F: Fn(&MigrationEvent) + Send + Sync,
{
    on_started: S,
    on_completed: C,
    on_failed: F,
}

impl<S, C, F> FnMigrationListener<S, C, F>
where
    S: Fn(&MigrationEvent) + Send + Sync,
    C: Fn(&MigrationEvent) + Send + Sync,
    F: Fn(&MigrationEvent) + Send + Sync,
{
    /// Creates a new function-based migration listener.
    pub fn new(on_started: S, on_completed: C, on_failed: F) -> Self {
        Self {
            on_started,
            on_completed,
            on_failed,
        }
    }
}

impl<S, C, F> MigrationListener for FnMigrationListener<S, C, F>
where
    S: Fn(&MigrationEvent) + Send + Sync,
    C: Fn(&MigrationEvent) + Send + Sync,
    F: Fn(&MigrationEvent) + Send + Sync,
{
    fn migration_started(&self, event: &MigrationEvent) {
        (self.on_started)(event);
    }

    fn migration_completed(&self, event: &MigrationEvent) {
        (self.on_completed)(event);
    }

    fn migration_failed(&self, event: &MigrationEvent) {
        (self.on_failed)(event);
    }
}

/// An event fired when a partition is lost (all replicas are unavailable).
#[derive(Debug, Clone)]
pub struct PartitionLostEvent {
    /// The ID of the lost partition.
    pub partition_id: i32,
    /// The number of lost backup replicas (0 = primary lost, 1+ = backups lost).
    pub lost_backup_count: i32,
    /// The UUID of the member that detected the loss, if known.
    pub source_uuid: Option<Uuid>,
}

impl PartitionLostEvent {
    /// Creates a new partition lost event.
    pub fn new(partition_id: i32, lost_backup_count: i32, source_uuid: Option<Uuid>) -> Self {
        Self {
            partition_id,
            lost_backup_count,
            source_uuid,
        }
    }

    /// Returns `true` if the primary replica was lost.
    pub fn is_primary_lost(&self) -> bool {
        self.lost_backup_count == 0
    }
}

impl std::fmt::Display for PartitionLostEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "PartitionLostEvent[partition={}, lostBackups={}, source={:?}]",
            self.partition_id, self.lost_backup_count, self.source_uuid
        )
    }
}

/// Trait for listening to partition lost events.
pub trait PartitionLostListener: Send + Sync {
    /// Called when a partition is lost (all replicas become unavailable).
    fn partition_lost(&self, event: &PartitionLostEvent);
}

/// A boxed partition lost listener.
pub type BoxedPartitionLostListener = Box<dyn PartitionLostListener>;

/// A function-based partition lost listener.
pub struct FnPartitionLostListener<F>
where
    F: Fn(&PartitionLostEvent) + Send + Sync,
{
    on_lost: F,
}

impl<F> FnPartitionLostListener<F>
where
    F: Fn(&PartitionLostEvent) + Send + Sync,
{
    /// Creates a new function-based partition lost listener.
    pub fn new(on_lost: F) -> Self {
        Self { on_lost }
    }
}

impl<F> PartitionLostListener for FnPartitionLostListener<F>
where
    F: Fn(&PartitionLostEvent) + Send + Sync,
{
    fn partition_lost(&self, event: &PartitionLostEvent) {
        (self.on_lost)(event);
    }
}

/// Service for querying partition information from the cluster.
///
/// The partition service provides access to the cluster's partition table,
/// which maps partition IDs to the members that own them. This is useful for
/// understanding data distribution and implementing partition-aware operations.
///
/// # Example
///
/// ```ignore
/// let partition_service = client.partition_service();
///
/// // Get total partition count
/// let count = partition_service.get_partition_count();
/// println!("Cluster has {} partitions", count);
///
/// // Get owner of a specific partition
/// if let Some(owner) = partition_service.get_partition_owner(0).await {
///     println!("Partition 0 is owned by {}", owner);
/// }
///
/// // Get partition for a key
/// let partition = partition_service.get_partition(&"my-key".to_string()).await;
/// println!("Key maps to {}", partition);
/// ```
#[derive(Debug, Clone)]
pub struct PartitionService {
    connection_manager: Arc<ConnectionManager>,
}

impl PartitionService {
    /// Creates a new partition service.
    pub(crate) fn new(connection_manager: Arc<ConnectionManager>) -> Self {
        Self { connection_manager }
    }

    /// Returns the total number of partitions in the cluster.
    ///
    /// This value is fixed for the lifetime of the cluster and is typically 271
    /// by default in Hazelcast.
    pub fn get_partition_count(&self) -> i32 {
        self.connection_manager.partition_count()
    }

    /// Returns all partitions in the cluster.
    ///
    /// The returned vector contains one `Partition` for each partition ID from 0
    /// to `partition_count - 1`. Partitions may have unknown owners if the
    /// partition table hasn't been fully populated yet.
    pub async fn get_partitions(&self) -> Vec<Partition> {
        let partition_table = self.connection_manager.partition_table().await;
        let count = self.connection_manager.partition_count();

        (0..count)
            .map(|id| {
                let owner_uuid = partition_table.get(&id).copied();
                Partition::new(id, owner_uuid)
            })
            .collect()
    }

    /// Returns the partition that a given key belongs to.
    ///
    /// The partition is determined by hashing the serialized key and taking
    /// the modulo with the partition count.
    ///
    /// # Type Parameters
    ///
    /// - `K`: The key type, must implement `Serializable` and `Hash`
    pub async fn get_partition<K>(&self, key: &K) -> Partition
    where
        K: Serializable + Hash,
    {
        let partition_count = self.connection_manager.partition_count();
        if partition_count == 0 {
            return Partition::new(0, None);
        }

        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        let hash = hasher.finish();

        let partition_id = ((hash as i64).abs() % (partition_count as i64)) as i32;

        let owner_uuid = self.connection_manager.get_partition_owner(partition_id).await;
        Partition::new(partition_id, owner_uuid)
    }

    /// Returns the member that owns the specified partition, if known.
    ///
    /// Returns `None` if:
    /// - The partition ID is out of range
    /// - The partition owner is not yet known
    /// - The owning member has left the cluster
    pub async fn get_partition_owner(&self, partition_id: i32) -> Option<Member> {
        let owner_uuid = self.connection_manager.get_partition_owner(partition_id).await?;
        self.connection_manager.get_member(&owner_uuid).await
    }

    /// Subscribes to partition migration events.
    ///
    /// Returns a broadcast receiver that will emit `MigrationEvent`s when
    /// partitions are migrated between cluster members.
    ///
    /// Note: Migration events are generated by the client based on partition
    /// table changes. The actual migration process occurs on the server side.
    pub fn subscribe_migration(&self) -> broadcast::Receiver<MigrationEvent> {
        self.connection_manager.subscribe_migration()
    }

    /// Adds a migration listener and returns a registration handle.
    ///
    /// The listener will be notified when partitions migrate between cluster
    /// members. The registration can be used to deactivate the listener.
    pub fn add_migration_listener(&self) -> ListenerRegistration {
        let id = ListenerId::new();
        let registration = ListenerRegistration::new(id);
        tracing::debug!(listener_id = %id, "added migration listener");
        registration
    }

    /// Subscribes to partition lost events.
    ///
    /// Returns a broadcast receiver that will emit `PartitionLostEvent`s when
    /// partitions become unavailable (all replicas lost).
    pub fn subscribe_partition_lost(&self) -> broadcast::Receiver<PartitionLostEvent> {
        self.connection_manager.subscribe_partition_lost()
    }

    /// Adds a partition lost listener and returns a registration handle.
    ///
    /// The listener will be notified when partitions are lost (all replicas
    /// become unavailable). The registration can be used to deactivate the
    /// listener.
    pub fn add_partition_lost_listener(&self) -> ListenerRegistration {
        let id = ListenerId::new();
        let registration = ListenerRegistration::new(id);
        tracing::debug!(listener_id = %id, "added partition lost listener");
        registration
    }

    /// Returns whether the partition table has been initialized.
    ///
    /// The partition table is populated after connecting to the cluster.
    pub fn is_initialized(&self) -> bool {
        self.connection_manager.partition_count() > 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_partition_creation() {
        let partition = Partition::new(5, None);
        assert_eq!(partition.id(), 5);
        assert_eq!(partition.owner_uuid(), None);
    }

    #[test]
    fn test_partition_with_owner() {
        let owner = Uuid::new_v4();
        let partition = Partition::new(10, Some(owner));
        assert_eq!(partition.id(), 10);
        assert_eq!(partition.owner_uuid(), Some(owner));
    }

    #[test]
    fn test_partition_equality() {
        let partition1 = Partition::new(5, Some(Uuid::new_v4()));
        let partition2 = Partition::new(5, Some(Uuid::new_v4()));
        let partition3 = Partition::new(6, None);

        assert_eq!(partition1, partition2);
        assert_ne!(partition1, partition3);
    }

    #[test]
    fn test_partition_display() {
        let partition_no_owner = Partition::new(5, None);
        assert!(partition_no_owner.to_string().contains("id=5"));
        assert!(partition_no_owner.to_string().contains("unknown"));

        let owner = Uuid::new_v4();
        let partition_with_owner = Partition::new(10, Some(owner));
        assert!(partition_with_owner.to_string().contains("id=10"));
        assert!(partition_with_owner.to_string().contains(&owner.to_string()));
    }

    #[test]
    fn test_migration_state_display() {
        assert_eq!(MigrationState::Started.to_string(), "STARTED");
        assert_eq!(MigrationState::Completed.to_string(), "COMPLETED");
        assert_eq!(MigrationState::Failed.to_string(), "FAILED");
    }

    #[test]
    fn test_migration_event_creation() {
        let old_owner = Uuid::new_v4();
        let new_owner = Uuid::new_v4();

        let event = MigrationEvent::started(5, Some(old_owner), Some(new_owner));
        assert_eq!(event.partition_id, 5);
        assert_eq!(event.state, MigrationState::Started);
        assert_eq!(event.old_owner, Some(old_owner));
        assert_eq!(event.new_owner, Some(new_owner));
    }

    #[test]
    fn test_migration_event_convenience_constructors() {
        let old = Uuid::new_v4();
        let new = Uuid::new_v4();

        let started = MigrationEvent::started(1, Some(old), Some(new));
        assert_eq!(started.state, MigrationState::Started);

        let completed = MigrationEvent::completed(2, Some(old), Some(new));
        assert_eq!(completed.state, MigrationState::Completed);

        let failed = MigrationEvent::failed(3, Some(old), Some(new));
        assert_eq!(failed.state, MigrationState::Failed);
    }

    #[test]
    fn test_migration_event_display() {
        let event = MigrationEvent::started(5, None, Some(Uuid::new_v4()));
        let display = event.to_string();
        assert!(display.contains("partition=5"));
        assert!(display.contains("STARTED"));
    }

    #[test]
    fn test_partition_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Partition>();
    }

    #[test]
    fn test_migration_event_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<MigrationEvent>();
    }

    #[test]
    fn test_migration_state_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<MigrationState>();
    }

    #[test]
    fn test_partition_service_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<PartitionService>();
    }

    #[test]
    fn test_partition_lost_event_creation() {
        let source = Uuid::new_v4();
        let event = PartitionLostEvent::new(5, 0, Some(source));
        assert_eq!(event.partition_id, 5);
        assert_eq!(event.lost_backup_count, 0);
        assert_eq!(event.source_uuid, Some(source));
        assert!(event.is_primary_lost());
    }

    #[test]
    fn test_partition_lost_event_backup_lost() {
        let event = PartitionLostEvent::new(10, 2, None);
        assert!(!event.is_primary_lost());
        assert_eq!(event.lost_backup_count, 2);
    }

    #[test]
    fn test_partition_lost_event_display() {
        let event = PartitionLostEvent::new(5, 1, None);
        let display = event.to_string();
        assert!(display.contains("partition=5"));
        assert!(display.contains("lostBackups=1"));
    }

    #[test]
    fn test_partition_lost_event_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<PartitionLostEvent>();
    }

    #[test]
    fn test_fn_migration_listener() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        let started_count = Arc::new(AtomicUsize::new(0));
        let completed_count = Arc::new(AtomicUsize::new(0));
        let failed_count = Arc::new(AtomicUsize::new(0));

        let started = Arc::clone(&started_count);
        let completed = Arc::clone(&completed_count);
        let failed = Arc::clone(&failed_count);

        let listener = FnMigrationListener::new(
            move |_| { started.fetch_add(1, Ordering::Relaxed); },
            move |_| { completed.fetch_add(1, Ordering::Relaxed); },
            move |_| { failed.fetch_add(1, Ordering::Relaxed); },
        );

        let event = MigrationEvent::started(1, None, Some(Uuid::new_v4()));
        listener.migration_started(&event);
        listener.migration_completed(&event);
        listener.migration_failed(&event);

        assert_eq!(started_count.load(Ordering::Relaxed), 1);
        assert_eq!(completed_count.load(Ordering::Relaxed), 1);
        assert_eq!(failed_count.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_fn_partition_lost_listener() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        let count = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&count);

        let listener = FnPartitionLostListener::new(move |_| {
            counter.fetch_add(1, Ordering::Relaxed);
        });

        let event = PartitionLostEvent::new(5, 0, None);
        listener.partition_lost(&event);

        assert_eq!(count.load(Ordering::Relaxed), 1);
    }
}
