//! Connection pool management and lifecycle handling.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicI32, AtomicUsize};
use std::sync::Arc;
use std::time::Duration;

use rand::Rng;
use tokio::sync::{broadcast, RwLock};
use tokio::time::{interval, timeout};
use tracing::{instrument, Span};
use uuid::Uuid;

use hazelcast_core::{HazelcastError, Result};

use super::connection::{Connection, ConnectionId};
use super::discovery::ClusterDiscovery;
use crate::config::{ClientConfig, ClientFailoverConfig, Permissions, ReconnectMode};
use crate::cluster::{MigrationEvent, PartitionLostEvent};
use crate::listener::{
    DistributedObjectEvent, LifecycleEvent, Member, MemberEvent, MemberEventType,
};

/// Events emitted during connection lifecycle.
#[derive(Debug, Clone)]
pub enum ConnectionEvent {
    /// Successfully connected to a cluster member.
    Connected {
        /// The connection identifier.
        id: ConnectionId,
        /// The address of the connected member.
        address: SocketAddr,
    },
    /// Disconnected from a cluster member.
    Disconnected {
        /// The connection identifier.
        id: ConnectionId,
        /// The address of the disconnected member.
        address: SocketAddr,
        /// The error that caused disconnection, if any.
        error: Option<String>,
    },
    /// Heartbeat received from a cluster member.
    HeartbeatReceived {
        /// The connection identifier.
        id: ConnectionId,
        /// The address of the member.
        address: SocketAddr,
    },
    /// Attempting to reconnect to a cluster member.
    ReconnectAttempt {
        /// The address being reconnected.
        address: SocketAddr,
        /// The current attempt number.
        attempt: u32,
        /// The delay before the next attempt.
        next_delay: Duration,
    },
    /// Failed to connect after all retry attempts.
    ReconnectFailed {
        /// The address that failed to connect.
        address: SocketAddr,
        /// The error from the last attempt.
        error: String,
    },
}

/// Manages connections to Hazelcast cluster members.
#[derive(Debug)]
pub struct ConnectionManager {
    config: Arc<ClientConfig>,
    discovery: Arc<dyn ClusterDiscovery>,
    connections: Arc<RwLock<HashMap<SocketAddr, Connection>>>,
    members: Arc<RwLock<HashMap<Uuid, Member>>>,
    partition_table: Arc<RwLock<HashMap<i32, Uuid>>>,
    partition_count: AtomicI32,
    event_sender: broadcast::Sender<ConnectionEvent>,
    membership_sender: broadcast::Sender<MemberEvent>,
    lifecycle_sender: broadcast::Sender<LifecycleEvent>,
    distributed_object_sender: broadcast::Sender<DistributedObjectEvent>,
    migration_sender: broadcast::Sender<MigrationEvent>,
    partition_lost_sender: broadcast::Sender<PartitionLostEvent>,
    shutdown: tokio::sync::watch::Sender<bool>,
    failover_config: Option<ClientFailoverConfig>,
    current_cluster_index: AtomicUsize,
    current_try_count: AtomicUsize,
}

/// Calculates the next backoff duration with jitter applied.
fn calculate_backoff_with_jitter(
    current_backoff: Duration,
    multiplier: f64,
    max_backoff: Duration,
    jitter: f64,
) -> Duration {
    let base_backoff = current_backoff.as_secs_f64() * multiplier;

    let jitter_factor = if jitter > 0.0 {
        let mut rng = rand::thread_rng();
        1.0 + rng.gen_range(-jitter..=jitter)
    } else {
        1.0
    };

    let jittered_backoff = base_backoff * jitter_factor;

    std::cmp::min(
        Duration::from_secs_f64(jittered_backoff),
        max_backoff,
    )
}

impl ConnectionManager {
    /// Creates a new connection manager with the given configuration and discovery.
    pub fn new(config: ClientConfig, discovery: impl ClusterDiscovery + 'static) -> Self {
        let (event_sender, _) = broadcast::channel(64);
        let (membership_sender, _) = broadcast::channel(64);
        let (lifecycle_sender, _) = broadcast::channel(16);
        let (distributed_object_sender, _) = broadcast::channel(64);
        let (migration_sender, _) = broadcast::channel(64);
        let (partition_lost_sender, _) = broadcast::channel(32);
        let (shutdown, _) = tokio::sync::watch::channel(false);

        Self {
            config: Arc::new(config),
            discovery: Arc::new(discovery),
            connections: Arc::new(RwLock::new(HashMap::new())),
            members: Arc::new(RwLock::new(HashMap::new())),
            partition_table: Arc::new(RwLock::new(HashMap::new())),
            partition_count: AtomicI32::new(0),
            event_sender,
            membership_sender,
            lifecycle_sender,
            distributed_object_sender,
            migration_sender,
            partition_lost_sender,
            shutdown,
            failover_config: None,
            current_cluster_index: AtomicUsize::new(0),
            current_try_count: AtomicUsize::new(0),
        }
    }

    /// Creates a connection manager using the network config addresses for discovery.
    pub fn from_config(config: ClientConfig) -> Self {
        let discovery = super::discovery::StaticAddressDiscovery::new(
            config.network().addresses().to_vec(),
        );
        Self::new(config, discovery)
    }

    /// Creates a connection manager with failover support.
    ///
    /// The manager will automatically try backup clusters when the primary
    /// cluster becomes unavailable.
    pub fn with_failover(failover_config: ClientFailoverConfig) -> Self {
        let primary_config = failover_config
            .get_config(0)
            .expect("failover config must have at least one cluster")
            .clone();

        let discovery = super::discovery::StaticAddressDiscovery::new(
            primary_config.network().addresses().to_vec(),
        );

        let (event_sender, _) = broadcast::channel(64);
        let (membership_sender, _) = broadcast::channel(64);
        let (lifecycle_sender, _) = broadcast::channel(16);
        let (distributed_object_sender, _) = broadcast::channel(64);
        let (migration_sender, _) = broadcast::channel(64);
        let (partition_lost_sender, _) = broadcast::channel(32);
        let (shutdown, _) = tokio::sync::watch::channel(false);

        Self {
            config: Arc::new(primary_config),
            discovery: Arc::new(discovery),
            connections: Arc::new(RwLock::new(HashMap::new())),
            members: Arc::new(RwLock::new(HashMap::new())),
            partition_table: Arc::new(RwLock::new(HashMap::new())),
            partition_count: AtomicI32::new(0),
            event_sender,
            membership_sender,
            lifecycle_sender,
            distributed_object_sender,
            migration_sender,
            partition_lost_sender,
            shutdown,
            failover_config: Some(failover_config),
            current_cluster_index: AtomicUsize::new(0),
            current_try_count: AtomicUsize::new(0),
        }
    }

    /// Subscribes to connection lifecycle events.
    pub fn subscribe(&self) -> broadcast::Receiver<ConnectionEvent> {
        self.event_sender.subscribe()
    }

    /// Subscribes to cluster membership events.
    pub fn subscribe_membership(&self) -> broadcast::Receiver<MemberEvent> {
        self.membership_sender.subscribe()
    }

    /// Subscribes to client lifecycle events.
    pub fn subscribe_lifecycle(&self) -> broadcast::Receiver<LifecycleEvent> {
        self.lifecycle_sender.subscribe()
    }

    /// Subscribes to distributed object events.
    pub fn subscribe_distributed_object(&self) -> broadcast::Receiver<DistributedObjectEvent> {
        self.distributed_object_sender.subscribe()
    }

    /// Broadcasts a distributed object event to all subscribers.
    pub fn broadcast_distributed_object_event(&self, event: DistributedObjectEvent) {
        let _ = self.distributed_object_sender.send(event);
    }

    /// Subscribes to partition migration events.
    pub fn subscribe_migration(&self) -> broadcast::Receiver<MigrationEvent> {
        self.migration_sender.subscribe()
    }

    /// Broadcasts a migration event to all subscribers.
    pub fn broadcast_migration_event(&self, event: MigrationEvent) {
        let _ = self.migration_sender.send(event);
    }

    /// Subscribes to partition lost events.
    pub fn subscribe_partition_lost(&self) -> broadcast::Receiver<PartitionLostEvent> {
        self.partition_lost_sender.subscribe()
    }

    /// Broadcasts a partition lost event to all subscribers.
    pub fn broadcast_partition_lost_event(&self, event: PartitionLostEvent) {
        let _ = self.partition_lost_sender.send(event);
    }

    /// Returns the current list of known cluster members.
    pub async fn members(&self) -> Vec<Member> {
        self.members.read().await.values().cloned().collect()
    }

    /// Returns a specific member by UUID, if known.
    pub async fn get_member(&self, uuid: &Uuid) -> Option<Member> {
        self.members.read().await.get(uuid).cloned()
    }

    /// Handles a member added event from the cluster.
    pub async fn handle_member_added(&self, member: Member) {
        let uuid = member.uuid();
        let event = MemberEvent::member_added(member.clone());

        self.members.write().await.insert(uuid, member.clone());

        let _ = self.membership_sender.send(event);
        tracing::info!(
            uuid = %uuid,
            address = %member.address(),
            "cluster member added"
        );
    }

    /// Handles a member removed event from the cluster.
    pub async fn handle_member_removed(&self, member_uuid: Uuid) {
        if let Some(member) = self.members.write().await.remove(&member_uuid) {
            let event = MemberEvent::member_removed(member.clone());
            let _ = self.membership_sender.send(event);
            tracing::info!(
                uuid = %member_uuid,
                address = %member.address(),
                "cluster member removed"
            );
        } else {
            tracing::warn!(uuid = %member_uuid, "received removal for unknown member");
        }
    }

    /// Updates the full member list from initial cluster state.
    pub async fn set_initial_members(&self, members: Vec<Member>) {
        let mut member_map = self.members.write().await;
        member_map.clear();

        for member in members {
            let uuid = member.uuid();
            let event = MemberEvent::member_added(member.clone());
            member_map.insert(uuid, member);
            let _ = self.membership_sender.send(event);
        }

        tracing::info!(count = member_map.len(), "initialized cluster member list");
    }

    /// Starts the connection manager, establishing initial connections.
    #[instrument(
        name = "connection_manager.start",
        skip(self),
        fields(cluster = %self.config.cluster_name())
    )]
    pub async fn start(&self) -> Result<()> {
        let _ = self.lifecycle_sender.send(LifecycleEvent::Starting);
        tracing::debug!("client lifecycle: Starting");

        let addresses = self.discovery.discover().await?;

        if addresses.is_empty() {
            tracing::error!("no cluster addresses discovered");
            return Err(HazelcastError::Connection(
                "no cluster addresses discovered".to_string(),
            ));
        }

        tracing::info!(count = addresses.len(), "discovered cluster addresses");

        for address in addresses {
            if let Err(e) = self.connect_to(address).await {
                tracing::warn!(address = %address, error = %e, "failed initial connection");
            }
        }

        let has_connections = !self.connections.read().await.is_empty();
        if !has_connections {
            tracing::error!("failed to establish any connections to cluster");
            return Err(HazelcastError::Connection(
                "failed to establish any connections".to_string(),
            ));
        }

        self.spawn_heartbeat_task();

        let _ = self.lifecycle_sender.send(LifecycleEvent::ClientConnected);
        tracing::debug!("client lifecycle: ClientConnected");

        let _ = self.lifecycle_sender.send(LifecycleEvent::Started);
        tracing::debug!("client lifecycle: Started");

        Ok(())
    }

    /// Establishes a connection to the specified address.
    #[instrument(
        name = "connection_manager.connect",
        skip(self),
        fields(address = %address)
    )]
    pub async fn connect_to(&self, address: SocketAddr) -> Result<ConnectionId> {
        let connect_timeout = self.config.network().connection_timeout();
        tracing::debug!(timeout = ?connect_timeout, "attempting connection");

        let connection = timeout(connect_timeout, self.create_connection(address))
            .await
            .map_err(|_| {
                tracing::warn!(timeout = ?connect_timeout, "connection attempt timed out");
                HazelcastError::Timeout(format!(
                    "connection to {} timed out after {:?}",
                    address, connect_timeout
                ))
            })??;

        let id = connection.id();

        self.connections.write().await.insert(address, connection);

        let _ = self.event_sender.send(ConnectionEvent::Connected { id, address });
        tracing::info!(id = %id, "connected to cluster member");

        Ok(id)
    }

    async fn create_connection(&self, address: SocketAddr) -> Result<Connection> {
        #[cfg(feature = "tls")]
        {
            let tls_config = self.config.network().tls();
            if tls_config.enabled() {
                return Connection::connect_tls(address, tls_config, None).await;
            }
        }

        Connection::connect(address).await
    }

    /// Reconnects to an address with exponential backoff.
    #[instrument(
        name = "connection_manager.reconnect",
        skip(self),
        fields(
            address = %address,
            max_retries = self.config.retry().max_retries()
        )
    )]
    pub async fn reconnect(&self, address: SocketAddr) -> Result<ConnectionId> {
        let retry_config = self.config.retry();
        let mut current_backoff = retry_config.initial_backoff();
        let mut attempt = 0u32;

        loop {
            attempt += 1;

            if attempt > retry_config.max_retries() {
                let error = format!(
                    "failed to reconnect after {} attempts",
                    retry_config.max_retries()
                );
                tracing::error!(attempts = attempt, "reconnection failed permanently");
                let _ = self.event_sender.send(ConnectionEvent::ReconnectFailed {
                    address,
                    error: error.clone(),
                });
                return Err(HazelcastError::Connection(error));
            }

            let _ = self.event_sender.send(ConnectionEvent::ReconnectAttempt {
                address,
                attempt,
                next_delay: current_backoff,
            });

            tracing::debug!(
                attempt = attempt,
                backoff = ?current_backoff,
                "attempting reconnection"
            );

            tokio::time::sleep(current_backoff).await;

            match self.connect_to(address).await {
                Ok(id) => {
                    tracing::info!(attempt = attempt, "reconnection successful");
                    return Ok(id);
                }
                Err(e) => {
                    tracing::warn!(
                        attempt = attempt,
                        error = %e,
                        "reconnection attempt failed"
                    );
                }
            }

            current_backoff = calculate_backoff_with_jitter(
                current_backoff,
                retry_config.multiplier(),
                retry_config.max_backoff(),
                retry_config.jitter(),
            );
        }
    }

    /// Disconnects from the specified address.
    #[instrument(
        name = "connection_manager.disconnect",
        skip(self),
        fields(address = %address)
    )]
    pub async fn disconnect(&self, address: SocketAddr) -> Result<()> {
        if let Some(connection) = self.connections.write().await.remove(&address) {
            let id = connection.id();

            let _ = self.event_sender.send(ConnectionEvent::Disconnected {
                id,
                address,
                error: None,
            });

            connection.close().await?;
            tracing::info!(id = %id, "disconnected from cluster member");
        } else {
            tracing::debug!("no active connection to disconnect");
        }

        Ok(())
    }

    /// Disconnects from all cluster members and shuts down.
    #[instrument(
        name = "connection_manager.shutdown",
        skip(self),
        fields(cluster = %self.config.cluster_name())
    )]
    pub async fn shutdown(&self) -> Result<()> {
        let _ = self.lifecycle_sender.send(LifecycleEvent::ShuttingDown);
        tracing::debug!("client lifecycle: ShuttingDown");

        let _ = self.shutdown.send(true);

        let addresses: Vec<SocketAddr> = self.connections.read().await.keys().copied().collect();
        tracing::debug!(connection_count = addresses.len(), "disconnecting all connections");

        for address in addresses {
            if let Err(e) = self.disconnect(address).await {
                tracing::warn!(address = %address, error = %e, "error during disconnect");
            }
        }

        let _ = self.lifecycle_sender.send(LifecycleEvent::ClientDisconnected);
        tracing::debug!("client lifecycle: ClientDisconnected");

        let _ = self.lifecycle_sender.send(LifecycleEvent::Shutdown);
        tracing::debug!("client lifecycle: Shutdown");

        tracing::info!("connection manager shut down");
        Ok(())
    }

    /// Returns the number of active connections.
    pub async fn connection_count(&self) -> usize {
        self.connections.read().await.len()
    }

    /// Returns the number of known cluster members.
    pub async fn member_count(&self) -> usize {
        self.members.read().await.len()
    }

    /// Returns a list of connected addresses.
    pub async fn connected_addresses(&self) -> Vec<SocketAddr> {
        self.connections.read().await.keys().copied().collect()
    }

    /// Returns `true` if shutdown has been requested.
    ///
    /// This can be used to check the client's running state without
    /// waiting for the async shutdown to complete.
    pub fn is_shutdown_requested(&self) -> bool {
        *self.shutdown.borrow()
    }

    /// Returns `true` if failover is configured.
    pub fn has_failover(&self) -> bool {
        self.failover_config.is_some()
    }

    /// Returns the current cluster index (0 for primary).
    pub fn current_cluster_index(&self) -> usize {
        self.current_cluster_index.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Returns the failover configuration, if any.
    pub fn failover_config(&self) -> Option<&ClientFailoverConfig> {
        self.failover_config.as_ref()
    }

    /// Triggers failover to the next available cluster.
    ///
    /// This method will:
    /// 1. Increment the try count for the current cluster
    /// 2. If try count exceeds the configured limit, move to the next cluster
    /// 3. Attempt to connect to the new cluster
    /// 4. Emit a `ClientChangedCluster` lifecycle event on success
    ///
    /// Returns `Ok(())` if failover was successful, or an error if all clusters
    /// have been exhausted.
    #[instrument(
        name = "connection_manager.trigger_failover",
        skip(self),
        fields(
            current_cluster = self.current_cluster_index(),
            has_failover = self.has_failover()
        )
    )]
    pub async fn trigger_failover(&self) -> Result<()> {
        let failover_config = match &self.failover_config {
            Some(config) => config,
            None => {
                tracing::warn!("failover requested but no failover configuration");
                return Err(HazelcastError::Connection(
                    "failover not configured".to_string(),
                ));
            }
        };

        let current_tries = self.current_try_count.fetch_add(1, std::sync::atomic::Ordering::AcqRel) + 1;
        let max_tries = failover_config.try_count() as usize;

        tracing::debug!(
            current_tries = current_tries,
            max_tries = max_tries,
            "checking failover attempt count"
        );

        if current_tries < max_tries {
            tracing::info!(
                attempt = current_tries,
                max = max_tries,
                "retrying current cluster"
            );
            return self.reconnect_current_cluster().await;
        }

        let cluster_count = failover_config.cluster_count();
        let current_index = self.current_cluster_index.load(std::sync::atomic::Ordering::Acquire);
        let next_index = (current_index + 1) % cluster_count;

        tracing::info!(
            from_cluster = current_index,
            to_cluster = next_index,
            "failing over to next cluster"
        );

        self.current_try_count.store(0, std::sync::atomic::Ordering::Release);
        self.current_cluster_index.store(next_index, std::sync::atomic::Ordering::Release);

        let new_config = failover_config
            .get_config(next_index)
            .ok_or_else(|| HazelcastError::Connection("invalid cluster index".to_string()))?;

        let addresses: Vec<SocketAddr> = self.connections.read().await.keys().copied().collect();
        for address in addresses {
            let _ = self.disconnect(address).await;
        }

        self.members.write().await.clear();
        self.clear_partition_table().await;

        let new_addresses = new_config.network().addresses();
        let mut connected = false;

        for address in new_addresses {
            match self.connect_to(*address).await {
                Ok(_) => {
                    connected = true;
                    tracing::info!(address = %address, "connected to failover cluster");
                }
                Err(e) => {
                    tracing::warn!(address = %address, error = %e, "failed to connect to failover address");
                }
            }
        }

        if connected {
            let _ = self.lifecycle_sender.send(LifecycleEvent::ClientChangedCluster);
            tracing::info!(
                cluster_index = next_index,
                cluster_name = %new_config.cluster_name(),
                "successfully failed over to new cluster"
            );
            Ok(())
        } else {
            tracing::error!(cluster_index = next_index, "failed to connect to any address in failover cluster");
            Err(HazelcastError::Connection(
                "failed to connect to failover cluster".to_string(),
            ))
        }
    }

    async fn reconnect_current_cluster(&self) -> Result<()> {
        let addresses = self.config.network().addresses();

        for address in addresses {
            match self.connect_to(*address).await {
                Ok(_) => {
                    tracing::info!(address = %address, "reconnected to current cluster");
                    return Ok(());
                }
                Err(e) => {
                    tracing::warn!(address = %address, error = %e, "failed to reconnect");
                }
            }
        }

        Err(HazelcastError::Connection(
            "failed to reconnect to current cluster".to_string(),
        ))
    }

    /// Checks if failover should be triggered after a disconnect and initiates it if needed.
    ///
    /// This method is called when a connection is lost and reconnection fails.
    /// It will trigger failover to an alternative cluster if:
    /// - Failover is configured
    /// - No active connections remain
    /// - Reconnection attempts have been exhausted
    ///
    /// Returns `Ok(true)` if failover was triggered and successful,
    /// `Ok(false)` if failover was not needed (connections still available or no failover config),
    /// or an error if failover was triggered but failed.
    #[instrument(
        name = "connection_manager.failover_on_disconnect",
        skip(self),
        fields(
            has_failover = self.has_failover(),
            connection_count = tracing::field::Empty
        ),
        level = "debug"
    )]
    pub async fn failover_on_disconnect(&self) -> Result<bool> {
        if !self.has_failover() {
            tracing::debug!("no failover configuration, skipping automatic failover");
            return Ok(false);
        }

        let conn_count = self.connection_count().await;
        Span::current().record("connection_count", conn_count);

        if conn_count > 0 {
            tracing::debug!(
                remaining_connections = conn_count,
                "still have active connections, failover not needed"
            );
            return Ok(false);
        }

        tracing::info!("all connections lost, initiating automatic failover");

        let _ = self.lifecycle_sender.send(LifecycleEvent::ClientDisconnected);

        match self.trigger_failover().await {
            Ok(()) => {
                tracing::info!("automatic failover to alternative cluster successful");
                Ok(true)
            }
            Err(e) => {
                tracing::error!(error = %e, "automatic failover failed");
                Err(e)
            }
        }
    }

    /// Attempts to connect to an alternative cluster without exhausting retries on the current cluster.
    ///
    /// Unlike `trigger_failover()` which respects the try count configuration,
    /// this method immediately switches to the next cluster in the failover list.
    /// This is useful for proactive cluster switching or when the application
    /// determines that the current cluster is unhealthy.
    ///
    /// Returns `Ok(())` if successfully connected to the alternative cluster,
    /// or an error if the switch failed.
    #[instrument(
        name = "connection_manager.try_alternative_cluster",
        skip(self),
        fields(
            current_cluster = self.current_cluster_index(),
            has_failover = self.has_failover()
        )
    )]
    pub async fn try_alternative_cluster(&self) -> Result<()> {
        let failover_config = match &self.failover_config {
            Some(config) => config,
            None => {
                tracing::warn!("try_alternative_cluster called but no failover configuration");
                return Err(HazelcastError::Connection(
                    "failover not configured".to_string(),
                ));
            }
        };

        let cluster_count = failover_config.cluster_count();
        if cluster_count < 2 {
            tracing::warn!("no alternative clusters available");
            return Err(HazelcastError::Connection(
                "no alternative clusters configured".to_string(),
            ));
        }

        let current_index = self.current_cluster_index.load(std::sync::atomic::Ordering::Acquire);
        let next_index = (current_index + 1) % cluster_count;

        tracing::info!(
            from_cluster = current_index,
            to_cluster = next_index,
            "switching to alternative cluster"
        );

        self.current_try_count.store(0, std::sync::atomic::Ordering::Release);
        self.current_cluster_index.store(next_index, std::sync::atomic::Ordering::Release);

        let new_config = failover_config
            .get_config(next_index)
            .ok_or_else(|| HazelcastError::Connection("invalid cluster index".to_string()))?;

        let addresses: Vec<SocketAddr> = self.connections.read().await.keys().copied().collect();
        for address in addresses {
            let _ = self.disconnect(address).await;
        }

        self.members.write().await.clear();
        self.clear_partition_table().await;

        let new_addresses = new_config.network().addresses();
        let mut connected = false;

        for address in new_addresses {
            match self.connect_to(*address).await {
                Ok(_) => {
                    connected = true;
                    tracing::info!(address = %address, "connected to alternative cluster");
                }
                Err(e) => {
                    tracing::warn!(address = %address, error = %e, "failed to connect to alternative cluster address");
                }
            }
        }

        if connected {
            let _ = self.lifecycle_sender.send(LifecycleEvent::ClientChangedCluster);
            tracing::info!(
                cluster_index = next_index,
                cluster_name = %new_config.cluster_name(),
                "successfully switched to alternative cluster"
            );
            Ok(())
        } else {
            tracing::error!(cluster_index = next_index, "failed to connect to any address in alternative cluster");
            Err(HazelcastError::Connection(
                "failed to connect to alternative cluster".to_string(),
            ))
        }
    }

    /// Checks if connected to the specified address.
    pub async fn is_connected(&self, address: &SocketAddr) -> bool {
        self.connections.read().await.contains_key(address)
    }

    /// Returns the effective permissions for RBAC enforcement.
    ///
    /// If no permissions are configured in the security settings, returns
    /// `Permissions::all()` for backward compatibility.
    pub fn effective_permissions(&self) -> Permissions {
        self.config.security().effective_permissions()
    }

    /// Checks if quorum is present for the given data structure and operation type.
    ///
    /// Returns `Ok(())` if:
    /// - No quorum configuration matches the data structure name
    /// - The quorum type doesn't protect this operation type
    /// - The quorum is present (enough cluster members)
    ///
    /// Returns `Err(HazelcastError::QuorumNotPresent)` if quorum is not met.
    #[instrument(
        name = "connection_manager.check_quorum",
        skip(self),
        fields(
            data_structure = %name,
            operation = if is_read_operation { "read" } else { "write" }
        ),
        level = "debug"
    )]
    pub async fn check_quorum(&self, name: &str, is_read_operation: bool) -> Result<()> {
        if let Some(quorum_config) = self.config.find_quorum_config(name) {
            if quorum_config.protects_operation(is_read_operation) {
                let members = self.members().await;
                let member_count = members.len();
                let required = quorum_config.min_cluster_size();

                tracing::trace!(
                    member_count = member_count,
                    required = required,
                    "checking quorum"
                );

                if !quorum_config.check_quorum(&members) {
                    let op_type = if is_read_operation { "read" } else { "write" };
                    tracing::warn!(
                        member_count = member_count,
                        required = required,
                        "quorum not present"
                    );
                    return Err(HazelcastError::QuorumNotPresent(format!(
                        "{} operation on '{}' requires quorum of {} members, but only {} present",
                        op_type,
                        name,
                        required,
                        member_count
                    )));
                }
            }
        }
        Ok(())
    }

    /// Returns the quorum configuration for the given data structure, if any.
    pub fn find_quorum_config(&self, name: &str) -> Option<&crate::config::QuorumConfig> {
        self.config.find_quorum_config(name)
    }

    /// Returns all configured quorum (split-brain protection) configurations.
    pub fn quorum_configs(&self) -> &[crate::config::QuorumConfig] {
        self.config.quorum_configs()
    }

    /// Returns the number of partitions in the cluster.
    pub fn partition_count(&self) -> i32 {
        self.partition_count.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Updates the partition table with a new mapping of partition IDs to owner member UUIDs.
    /// Also updates the partition count.
    pub async fn update_partition_table(&self, partitions: HashMap<i32, Uuid>) {
        let count = partitions.len() as i32;
        let mut table = self.partition_table.write().await;
        *table = partitions;
        self.partition_count.store(count, std::sync::atomic::Ordering::Release);
        tracing::debug!(partition_count = count, "updated partition table");
    }

    /// Updates a single partition's owner.
    pub async fn set_partition_owner(&self, partition_id: i32, owner: Uuid) {
        let mut table = self.partition_table.write().await;
        table.insert(partition_id, owner);
        let count = table.len() as i32;
        drop(table);
        self.partition_count.store(count, std::sync::atomic::Ordering::Release);
        tracing::trace!(partition_id = partition_id, owner = %owner, "updated partition owner");
    }

    /// Returns the UUID of the member that owns the specified partition.
    pub async fn get_partition_owner(&self, partition_id: i32) -> Option<Uuid> {
        self.partition_table.read().await.get(&partition_id).copied()
    }

    /// Returns the socket address of the member that owns the specified partition.
    pub async fn get_partition_owner_address(&self, partition_id: i32) -> Option<SocketAddr> {
        let owner_uuid = self.get_partition_owner(partition_id).await?;
        let members = self.members.read().await;
        members.get(&owner_uuid).map(|m| m.address())
    }

    /// Returns the address for the connection that should handle requests for the given partition.
    ///
    /// If smart routing is enabled (default):
    /// - If the partition owner is known and connected, returns its address.
    /// - If the partition owner is known but not connected, attempts to connect.
    /// - If the partition is unknown or connection fails, falls back to any available connection.
    ///
    /// If smart routing is disabled (unisocket mode):
    /// - Returns any available connection without partition-aware routing.
    ///
    /// Returns an error only if no connections are available at all.
    #[instrument(
        name = "connection_manager.get_connection_for_partition",
        skip(self),
        fields(partition_id = partition_id, smart_routing = self.config.network().smart_routing()),
        level = "debug"
    )]
    pub async fn get_connection_for_partition(&self, partition_id: i32) -> Result<SocketAddr> {
        if self.config.network().smart_routing() {
            if let Some(owner_address) = self.get_partition_owner_address(partition_id).await {
                if self.is_connected(&owner_address).await {
                    tracing::trace!(address = %owner_address, "using partition owner connection");
                    return Ok(owner_address);
                }

                tracing::debug!(address = %owner_address, "partition owner not connected, attempting connection");
                if self.connect_to(owner_address).await.is_ok() {
                    return Ok(owner_address);
                }
                tracing::warn!(address = %owner_address, "failed to connect to partition owner, falling back");
            }
        } else {
            tracing::trace!("smart routing disabled, using any available connection");
        }

        let addresses = self.connected_addresses().await;
        addresses.into_iter().next().ok_or_else(|| {
            HazelcastError::Connection("no connections available".to_string())
        })
    }

    /// Sends a message to the owner of the specified partition.
    ///
    /// Routes the message to the partition owner if known and connected,
    /// otherwise falls back to any available connection.
    #[instrument(
        name = "connection_manager.send_to_partition",
        skip(self, message),
        fields(partition_id = partition_id),
        level = "debug"
    )]
    pub async fn send_to_partition(
        &self,
        partition_id: i32,
        message: hazelcast_core::ClientMessage,
    ) -> Result<()> {
        let address = self.get_connection_for_partition(partition_id).await?;
        self.send_to(address, message).await
    }

    /// Receives a message from the owner of the specified partition.
    ///
    /// Routes to the partition owner if known and connected,
    /// otherwise falls back to any available connection.
    #[instrument(
        name = "connection_manager.receive_from_partition",
        skip(self),
        fields(partition_id = partition_id),
        level = "debug"
    )]
    pub async fn receive_from_partition(
        &self,
        partition_id: i32,
    ) -> Result<Option<hazelcast_core::ClientMessage>> {
        let address = self.get_connection_for_partition(partition_id).await?;
        self.receive_from(address).await
    }

    /// Invokes a request on the owner of the specified partition and returns the response.
    ///
    /// This method sends the message to the partition owner if smart routing is enabled
    /// and the owner is known, otherwise falls back to any available connection.
    #[instrument(
        name = "connection_manager.invoke_on_partition",
        skip(self, message),
        fields(partition_id = partition_id),
        level = "debug"
    )]
    pub async fn invoke_on_partition(
        &self,
        partition_id: i32,
        message: hazelcast_core::ClientMessage,
    ) -> Result<hazelcast_core::ClientMessage> {
        let address = self.get_connection_for_partition(partition_id).await?;
        self.send_to(address, message).await?;
        self.receive_from(address)
            .await?
            .ok_or_else(|| HazelcastError::Connection("connection closed unexpectedly".to_string()))
    }

    /// Invokes a request on any available connection and returns the response.
    ///
    /// This is used for operations that are not partition-specific, such as
    /// cluster-wide queries or operations on replicated data structures.
    #[instrument(
        name = "connection_manager.invoke_on_random",
        skip(self, message),
        level = "debug"
    )]
    pub async fn invoke_on_random(
        &self,
        message: hazelcast_core::ClientMessage,
    ) -> Result<hazelcast_core::ClientMessage> {
        let addresses = self.connected_addresses().await;
        let address = addresses.into_iter().next().ok_or_else(|| {
            HazelcastError::Connection("no connections available".to_string())
        })?;
        self.send_to(address, message).await?;
        self.receive_from(address)
            .await?
            .ok_or_else(|| HazelcastError::Connection("connection closed unexpectedly".to_string()))
    }

    /// Sends a request and returns the response, automatically routing based on partition ID.
    ///
    /// If the message has a specific partition ID, it is routed to the partition owner.
    /// Otherwise, it is sent to any available connection.
    #[instrument(
        name = "connection_manager.send",
        skip(self, message),
        level = "debug"
    )]
    pub async fn send(
        &self,
        message: hazelcast_core::ClientMessage,
    ) -> Result<hazelcast_core::ClientMessage> {
        let partition_id = message.partition_id().unwrap_or(hazelcast_core::protocol::constants::PARTITION_ID_ANY);
        if partition_id >= 0 {
            self.invoke_on_partition(partition_id, message).await
        } else {
            self.invoke_on_random(message).await
        }
    }

    /// Invokes a request and returns the response, automatically routing based on partition ID.
    ///
    /// This is an alias for `send` for code that prefers the "invoke" terminology.
    #[instrument(
        name = "connection_manager.invoke",
        skip(self, message),
        level = "debug"
    )]
    pub async fn invoke(
        &self,
        message: hazelcast_core::ClientMessage,
    ) -> Result<hazelcast_core::ClientMessage> {
        self.send(message).await
    }

    /// Clears the partition table. Called during reconnection or cluster state reset.
    pub async fn clear_partition_table(&self) {
        let mut table = self.partition_table.write().await;
        table.clear();
        self.partition_count.store(0, std::sync::atomic::Ordering::Release);
        tracing::debug!("cleared partition table");
    }

    /// Returns a snapshot of the current partition table.
    pub async fn partition_table(&self) -> HashMap<i32, Uuid> {
        self.partition_table.read().await.clone()
    }

    /// Returns the configured permissions, if any.
    pub fn permissions(&self) -> Option<&Permissions> {
        self.config.security().permissions()
    }

    /// Sends a message to a specific address.
    pub async fn send_to(
        &self,
        address: SocketAddr,
        message: hazelcast_core::ClientMessage,
    ) -> Result<()> {
        let mut connections = self.connections.write().await;
        let connection = connections.get_mut(&address).ok_or_else(|| {
            HazelcastError::Connection(format!("no connection to {}", address))
        })?;

        connection.send(message).await
    }

    /// Receives a message from a specific address.
    pub async fn receive_from(
        &self,
        address: SocketAddr,
    ) -> Result<Option<hazelcast_core::ClientMessage>> {
        let mut connections = self.connections.write().await;
        let connection = connections.get_mut(&address).ok_or_else(|| {
            HazelcastError::Connection(format!("no connection to {}", address))
        })?;

        connection.receive().await
    }

    fn spawn_heartbeat_task(&self) {
        let connections = Arc::clone(&self.connections);
        let event_sender = self.event_sender.clone();
        let heartbeat_interval = self.config.network().heartbeat_interval();
        let reconnect_mode = self.config.network().reconnect_mode();
        let config = Arc::clone(&self.config);
        let has_failover = self.failover_config.is_some();
        let mut shutdown_rx = self.shutdown.subscribe();

        tokio::spawn(async move {
            let mut ticker = interval(heartbeat_interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        let mut conns = connections.write().await;
                        let mut failed_addresses = Vec::new();

                        for (address, connection) in conns.iter_mut() {
                            if let Err(e) = connection.send_heartbeat().await {
                                tracing::warn!(
                                    address = %address,
                                    error = %e,
                                    "heartbeat send failed"
                                );
                                failed_addresses.push((*address, connection.id(), e.to_string()));
                            } else {
                                let _ = event_sender.send(ConnectionEvent::HeartbeatReceived {
                                    id: connection.id(),
                                    address: *address,
                                });
                            }
                        }

                        drop(conns);

                        for (address, id, error) in failed_addresses {
                            let mut conns = connections.write().await;
                            if conns.remove(&address).is_some() {
                                let _ = event_sender.send(ConnectionEvent::Disconnected {
                                    id,
                                    address,
                                    error: Some(error.clone()),
                                });
                            }
                            drop(conns);

                            if reconnect_mode.is_enabled() {
                                let connections_clone = Arc::clone(&connections);
                                let event_sender_clone = event_sender.clone();
                                let config_clone = Arc::clone(&config);

                                let reconnect_task = async move {
                                    Self::attempt_reconnect(
                                        address,
                                        &connections_clone,
                                        &event_sender_clone,
                                        &config_clone,
                                    ).await;
                                };

                                if reconnect_mode.is_async() {
                                    tokio::spawn(reconnect_task);
                                } else {
                                    reconnect_task.await;
                                }
                            }
                        }

                        if has_failover {
                            let remaining_connections = connections.read().await.len();
                            if remaining_connections == 0 {
                                tracing::info!("all connections lost, automatic failover may be needed");
                            }
                        }
                    }
                    result = shutdown_rx.changed() => {
                        if result.is_ok() && *shutdown_rx.borrow() {
                            tracing::debug!("heartbeat task shutting down");
                            break;
                        }
                    }
                }
            }
        });
    }

    async fn attempt_reconnect(
        address: SocketAddr,
        connections: &Arc<RwLock<HashMap<SocketAddr, Connection>>>,
        event_sender: &broadcast::Sender<ConnectionEvent>,
        config: &Arc<ClientConfig>,
    ) {
        let retry_config = config.retry();
        let mut current_backoff = retry_config.initial_backoff();
        let mut attempt = 0u32;

        loop {
            attempt += 1;

            if attempt > retry_config.max_retries() {
                let error = format!(
                    "failed to reconnect after {} attempts",
                    retry_config.max_retries()
                );
                tracing::error!(
                    address = %address,
                    attempts = attempt,
                    "reconnection failed permanently"
                );
                let _ = event_sender.send(ConnectionEvent::ReconnectFailed {
                    address,
                    error,
                });
                return;
            }

            let _ = event_sender.send(ConnectionEvent::ReconnectAttempt {
                address,
                attempt,
                next_delay: current_backoff,
            });

            tracing::debug!(
                address = %address,
                attempt = attempt,
                backoff = ?current_backoff,
                "attempting reconnection"
            );

            tokio::time::sleep(current_backoff).await;

            let connect_result = timeout(
                config.network().connection_timeout(),
                Self::create_connection_static(address, config),
            ).await;

            match connect_result {
                Ok(Ok(connection)) => {
                    let id = connection.id();
                    connections.write().await.insert(address, connection);
                    let _ = event_sender.send(ConnectionEvent::Connected { id, address });
                    tracing::info!(
                        address = %address,
                        attempt = attempt,
                        "reconnection successful"
                    );
                    return;
                }
                Ok(Err(e)) => {
                    tracing::warn!(
                        address = %address,
                        attempt = attempt,
                        error = %e,
                        "reconnection attempt failed"
                    );
                }
                Err(_) => {
                    tracing::warn!(
                        address = %address,
                        attempt = attempt,
                        "reconnection attempt timed out"
                    );
                }
            }

            current_backoff = calculate_backoff_with_jitter(
                current_backoff,
                retry_config.multiplier(),
                retry_config.max_backoff(),
                retry_config.jitter(),
            );
        }
    }

    async fn create_connection_static(address: SocketAddr, config: &ClientConfig) -> Result<Connection> {
        #[cfg(feature = "tls")]
        {
            let tls_config = config.network().tls();
            if tls_config.enabled() {
                return Connection::connect_tls(address, tls_config, None).await;
            }
        }

        Connection::connect(address).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ClientConfigBuilder;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    async fn create_mock_server() -> (TcpListener, SocketAddr) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        (listener, addr)
    }

    async fn accept_and_echo(listener: TcpListener) {
        if let Ok((mut socket, _)) = listener.accept().await {
            let mut buf = vec![0u8; 1024];
            loop {
                match socket.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(n) => {
                        if socket.write_all(&buf[..n]).await.is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        }
    }

    #[tokio::test]
    async fn test_connection_manager_creation() {
        let config = ClientConfigBuilder::new().build().unwrap();
        let discovery = super::super::discovery::StaticAddressDiscovery::default();
        let manager = ConnectionManager::new(config, discovery);

        assert_eq!(manager.connection_count().await, 0);
    }

    #[tokio::test]
    async fn test_connect_to_mock_server() {
        let (listener, addr) = create_mock_server().await;

        tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            drop(socket);
        });

        let config = ClientConfigBuilder::new()
            .add_address(addr)
            .connection_timeout(Duration::from_secs(5))
            .build()
            .unwrap();

        let manager = ConnectionManager::from_config(config);
        let result = manager.connect_to(addr).await;

        assert!(result.is_ok());
        assert!(manager.is_connected(&addr).await);
        assert_eq!(manager.connection_count().await, 1);
    }

    #[tokio::test]
    async fn test_disconnect() {
        let (listener, addr) = create_mock_server().await;

        tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            drop(socket);
        });

        let config = ClientConfigBuilder::new()
            .add_address(addr)
            .build()
            .unwrap();

        let manager = ConnectionManager::from_config(config);
        manager.connect_to(addr).await.unwrap();

        assert!(manager.is_connected(&addr).await);

        manager.disconnect(addr).await.unwrap();

        assert!(!manager.is_connected(&addr).await);
        assert_eq!(manager.connection_count().await, 0);
    }

    #[tokio::test]
    async fn test_connection_events() {
        let (listener, addr) = create_mock_server().await;

        tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            drop(socket);
        });

        let config = ClientConfigBuilder::new()
            .add_address(addr)
            .build()
            .unwrap();

        let manager = ConnectionManager::from_config(config);
        let mut events = manager.subscribe();

        manager.connect_to(addr).await.unwrap();

        let event = tokio::time::timeout(Duration::from_secs(1), events.recv())
            .await
            .unwrap()
            .unwrap();

        match event {
            ConnectionEvent::Connected { address, .. } => {
                assert_eq!(address, addr);
            }
            _ => panic!("expected Connected event"),
        }
    }

    #[tokio::test]
    async fn test_connect_timeout() {
        let addr: SocketAddr = "192.0.2.1:5701".parse().unwrap();

        let config = ClientConfigBuilder::new()
            .add_address(addr)
            .connection_timeout(Duration::from_millis(100))
            .build()
            .unwrap();

        let manager = ConnectionManager::from_config(config);
        let result = manager.connect_to(addr).await;

        assert!(result.is_err());
        match result.unwrap_err() {
            HazelcastError::Timeout(_) => {}
            e => panic!("expected Timeout error, got {:?}", e),
        }
    }

    #[tokio::test]
    async fn test_reconnect_backoff() {
        use crate::config::RetryConfigBuilder;

        let addr: SocketAddr = "192.0.2.1:5701".parse().unwrap();

        let config = ClientConfigBuilder::new()
            .add_address(addr)
            .connection_timeout(Duration::from_millis(50))
            .retry(|_| {
                RetryConfigBuilder::new()
                    .initial_backoff(Duration::from_millis(10))
                    .max_backoff(Duration::from_millis(50))
                    .max_retries(2)
                    .multiplier(2.0)
                    .build()
                    .unwrap()
                    .into()
            })
            .build()
            .unwrap();

        let manager = ConnectionManager::from_config(config);
        let mut events = manager.subscribe();

        let reconnect_handle = tokio::spawn({
            let manager_addr = addr;
            async move {
                let config = ClientConfigBuilder::new()
                    .add_address(manager_addr)
                    .connection_timeout(Duration::from_millis(50))
                    .retry(|_| {
                        RetryConfigBuilder::new()
                            .initial_backoff(Duration::from_millis(10))
                            .max_backoff(Duration::from_millis(50))
                            .max_retries(2)
                            .multiplier(2.0)
                            .build()
                            .unwrap()
                            .into()
                    })
                    .build()
                    .unwrap();
                let manager = ConnectionManager::from_config(config);
                manager.reconnect(manager_addr).await
            }
        });

        let result = tokio::time::timeout(Duration::from_secs(5), reconnect_handle)
            .await
            .unwrap()
            .unwrap();

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_connected_addresses() {
        let (listener1, addr1) = create_mock_server().await;
        let (listener2, addr2) = create_mock_server().await;

        tokio::spawn(async move {
            let _ = listener1.accept().await;
        });
        tokio::spawn(async move {
            let _ = listener2.accept().await;
        });

        let config = ClientConfigBuilder::new()
            .addresses([addr1, addr2])
            .build()
            .unwrap();

        let manager = ConnectionManager::from_config(config);
        manager.connect_to(addr1).await.unwrap();
        manager.connect_to(addr2).await.unwrap();

        let addresses = manager.connected_addresses().await;
        assert_eq!(addresses.len(), 2);
        assert!(addresses.contains(&addr1));
        assert!(addresses.contains(&addr2));
    }

    #[tokio::test]
    async fn test_shutdown() {
        let (listener, addr) = create_mock_server().await;

        tokio::spawn(async move {
            let _ = listener.accept().await;
        });

        let config = ClientConfigBuilder::new()
            .add_address(addr)
            .build()
            .unwrap();

        let manager = ConnectionManager::from_config(config);
        manager.connect_to(addr).await.unwrap();

        assert_eq!(manager.connection_count().await, 1);

        manager.shutdown().await.unwrap();

        assert_eq!(manager.connection_count().await, 0);
    }

    #[tokio::test]
    async fn test_start_with_no_addresses() {
        let config = ClientConfigBuilder::new().build().unwrap();
        let discovery = super::super::discovery::StaticAddressDiscovery::new(vec![]);
        let manager = ConnectionManager::new(config, discovery);

        let result = manager.start().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_send_to_nonexistent_connection() {
        let config = ClientConfigBuilder::new().build().unwrap();
        let manager = ConnectionManager::from_config(config);

        let addr: SocketAddr = "127.0.0.1:9999".parse().unwrap();
        let msg = hazelcast_core::ClientMessage::new();

        let result = manager.send_to(addr, msg).await;
        assert!(result.is_err());
    }

    #[test]
    fn test_connection_event_is_send() {
        fn assert_send<T: Send>() {}
        assert_send::<ConnectionEvent>();
    }

    #[test]
    fn test_connection_manager_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<ConnectionManager>();
    }

    #[test]
    fn test_calculate_backoff_with_jitter_no_jitter() {
        let backoff = calculate_backoff_with_jitter(
            Duration::from_millis(100),
            2.0,
            Duration::from_secs(30),
            0.0,
        );
        assert_eq!(backoff, Duration::from_millis(200));
    }

    #[test]
    fn test_calculate_backoff_with_jitter_respects_max() {
        let backoff = calculate_backoff_with_jitter(
            Duration::from_secs(20),
            2.0,
            Duration::from_secs(30),
            0.0,
        );
        assert_eq!(backoff, Duration::from_secs(30));
    }

    #[test]
    fn test_calculate_backoff_with_jitter_applies_jitter() {
        let mut results = std::collections::HashSet::new();
        for _ in 0..20 {
            let backoff = calculate_backoff_with_jitter(
                Duration::from_millis(100),
                2.0,
                Duration::from_secs(30),
                0.25,
            );
            results.insert(backoff.as_millis());
        }
        assert!(results.len() > 1, "jitter should produce varied results");
    }

    #[test]
    fn test_retry_config_jitter_used_in_manager() {
        let config = ClientConfigBuilder::new()
            .retry(|r| r.jitter(0.2))
            .build()
            .unwrap();

        assert_eq!(config.retry().jitter(), 0.2);
    }

    #[test]
    fn test_effective_permissions_default() {
        let config = ClientConfigBuilder::new().build().unwrap();
        let discovery = super::super::discovery::StaticAddressDiscovery::default();
        let manager = ConnectionManager::new(config, discovery);

        let perms = manager.effective_permissions();
        assert!(perms.is_admin());
        assert!(perms.can_read());
        assert!(perms.can_put());
    }

    #[test]
    fn test_effective_permissions_with_rbac() {
        use crate::config::{Permissions, PermissionAction};

        let mut perms = Permissions::new();
        perms.grant(PermissionAction::Read);

        let config = ClientConfigBuilder::new()
            .security(|s| s.permissions(perms))
            .build()
            .unwrap();
        let discovery = super::super::discovery::StaticAddressDiscovery::default();
        let manager = ConnectionManager::new(config, discovery);

        let effective = manager.effective_permissions();
        assert!(effective.can_read());
        assert!(!effective.can_put());
        assert!(!effective.can_remove());
    }

    #[test]
    fn test_permissions_returns_none_by_default() {
        let config = ClientConfigBuilder::new().build().unwrap();
        let discovery = super::super::discovery::StaticAddressDiscovery::default();
        let manager = ConnectionManager::new(config, discovery);

        assert!(manager.permissions().is_none());
    }

    #[test]
    fn test_permissions_returns_some_when_configured() {
        use crate::config::{Permissions, PermissionAction};

        let mut perms = Permissions::new();
        perms.grant(PermissionAction::Read);

        let config = ClientConfigBuilder::new()
            .security(|s| s.permissions(perms))
            .build()
            .unwrap();
        let discovery = super::super::discovery::StaticAddressDiscovery::default();
        let manager = ConnectionManager::new(config, discovery);

        assert!(manager.permissions().is_some());
    }

    #[tokio::test]
    async fn test_member_tracking() {
        let config = ClientConfigBuilder::new().build().unwrap();
        let discovery = super::super::discovery::StaticAddressDiscovery::default();
        let manager = ConnectionManager::new(config, discovery);

        assert_eq!(manager.member_count().await, 0);

        let member = crate::listener::Member::new(
            uuid::Uuid::new_v4(),
            "127.0.0.1:5701".parse().unwrap(),
        );
        manager.handle_member_added(member.clone()).await;

        assert_eq!(manager.member_count().await, 1);
        let members = manager.members().await;
        assert_eq!(members.len(), 1);
        assert_eq!(members[0].uuid(), member.uuid());
    }

    #[tokio::test]
    async fn test_member_removal() {
        let config = ClientConfigBuilder::new().build().unwrap();
        let discovery = super::super::discovery::StaticAddressDiscovery::default();
        let manager = ConnectionManager::new(config, discovery);

        let member = crate::listener::Member::new(
            uuid::Uuid::new_v4(),
            "127.0.0.1:5701".parse().unwrap(),
        );
        let uuid = member.uuid();

        manager.handle_member_added(member).await;
        assert_eq!(manager.member_count().await, 1);

        manager.handle_member_removed(uuid).await;
        assert_eq!(manager.member_count().await, 0);
    }

    #[tokio::test]
    async fn test_membership_events_broadcast() {
        let config = ClientConfigBuilder::new().build().unwrap();
        let discovery = super::super::discovery::StaticAddressDiscovery::default();
        let manager = ConnectionManager::new(config, discovery);

        let mut rx = manager.subscribe_membership();

        let member = crate::listener::Member::new(
            uuid::Uuid::new_v4(),
            "127.0.0.1:5701".parse().unwrap(),
        );
        let uuid = member.uuid();

        manager.handle_member_added(member).await;

        let event = tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .unwrap()
            .unwrap();

        match event.event_type {
            crate::listener::MemberEventType::Added => {
                assert_eq!(event.member.uuid(), uuid);
            }
            _ => panic!("expected Added event"),
        }
    }

    #[tokio::test]
    async fn test_initial_members_set() {
        let config = ClientConfigBuilder::new().build().unwrap();
        let discovery = super::super::discovery::StaticAddressDiscovery::default();
        let manager = ConnectionManager::new(config, discovery);

        let members = vec![
            crate::listener::Member::new(
                uuid::Uuid::new_v4(),
                "127.0.0.1:5701".parse().unwrap(),
            ),
            crate::listener::Member::new(
                uuid::Uuid::new_v4(),
                "127.0.0.1:5702".parse().unwrap(),
            ),
        ];

        manager.set_initial_members(members).await;

        assert_eq!(manager.member_count().await, 2);
    }

    #[tokio::test]
    async fn test_get_member_by_uuid() {
        let config = ClientConfigBuilder::new().build().unwrap();
        let discovery = super::super::discovery::StaticAddressDiscovery::default();
        let manager = ConnectionManager::new(config, discovery);

        let member = crate::listener::Member::new(
            uuid::Uuid::new_v4(),
            "127.0.0.1:5701".parse().unwrap(),
        );
        let uuid = member.uuid();

        manager.handle_member_added(member.clone()).await;

        let found = manager.get_member(&uuid).await;
        assert!(found.is_some());
        assert_eq!(found.unwrap().uuid(), uuid);

        let not_found = manager.get_member(&uuid::Uuid::new_v4()).await;
        assert!(not_found.is_none());
    }

    #[tokio::test]
    async fn test_lifecycle_events_on_start() {
        let (listener, addr) = create_mock_server().await;

        tokio::spawn(async move {
            let _ = listener.accept().await;
        });

        let config = ClientConfigBuilder::new()
            .add_address(addr)
            .build()
            .unwrap();

        let manager = ConnectionManager::from_config(config);
        let mut rx = manager.subscribe_lifecycle();

        manager.start().await.unwrap();

        let mut events = Vec::new();
        while let Ok(event) = tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
            if let Ok(e) = event {
                events.push(e);
            }
        }

        assert!(events.contains(&crate::listener::LifecycleEvent::Starting));
        assert!(events.contains(&crate::listener::LifecycleEvent::ClientConnected));
        assert!(events.contains(&crate::listener::LifecycleEvent::Started));
    }

    #[tokio::test]
    async fn test_lifecycle_events_on_shutdown() {
        let (listener, addr) = create_mock_server().await;

        tokio::spawn(async move {
            let _ = listener.accept().await;
        });

        let config = ClientConfigBuilder::new()
            .add_address(addr)
            .build()
            .unwrap();

        let manager = ConnectionManager::from_config(config);
        manager.start().await.unwrap();

        let mut rx = manager.subscribe_lifecycle();

        manager.shutdown().await.unwrap();

        let mut events = Vec::new();
        while let Ok(event) = tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
            if let Ok(e) = event {
                events.push(e);
            }
        }

        assert!(events.contains(&crate::listener::LifecycleEvent::ShuttingDown));
        assert!(events.contains(&crate::listener::LifecycleEvent::ClientDisconnected));
        assert!(events.contains(&crate::listener::LifecycleEvent::Shutdown));
    }

    #[tokio::test]
    async fn test_quorum_check_passes_with_enough_members() {
        use crate::config::{QuorumConfig, QuorumType};

        let quorum = QuorumConfig::builder("test-*")
            .min_cluster_size(2)
            .quorum_type(QuorumType::ReadWrite)
            .build()
            .unwrap();

        let config = ClientConfigBuilder::new()
            .add_quorum_config(quorum)
            .build()
            .unwrap();

        let discovery = super::super::discovery::StaticAddressDiscovery::default();
        let manager = ConnectionManager::new(config, discovery);

        let member1 = crate::listener::Member::new(
            uuid::Uuid::new_v4(),
            "127.0.0.1:5701".parse().unwrap(),
        );
        let member2 = crate::listener::Member::new(
            uuid::Uuid::new_v4(),
            "127.0.0.1:5702".parse().unwrap(),
        );

        manager.handle_member_added(member1).await;
        manager.handle_member_added(member2).await;

        let result = manager.check_quorum("test-map", true).await;
        assert!(result.is_ok());

        let result = manager.check_quorum("test-map", false).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_quorum_check_fails_with_insufficient_members() {
        use crate::config::{QuorumConfig, QuorumType};

        let quorum = QuorumConfig::builder("critical-*")
            .min_cluster_size(3)
            .quorum_type(QuorumType::ReadWrite)
            .build()
            .unwrap();

        let config = ClientConfigBuilder::new()
            .add_quorum_config(quorum)
            .build()
            .unwrap();

        let discovery = super::super::discovery::StaticAddressDiscovery::default();
        let manager = ConnectionManager::new(config, discovery);

        let member1 = crate::listener::Member::new(
            uuid::Uuid::new_v4(),
            "127.0.0.1:5701".parse().unwrap(),
        );
        let member2 = crate::listener::Member::new(
            uuid::Uuid::new_v4(),
            "127.0.0.1:5702".parse().unwrap(),
        );

        manager.handle_member_added(member1).await;
        manager.handle_member_added(member2).await;

        let result = manager.check_quorum("critical-data", true).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            HazelcastError::QuorumNotPresent(msg) => {
                assert!(msg.contains("requires quorum of 3 members"));
                assert!(msg.contains("only 2 present"));
            }
            e => panic!("expected QuorumNotPresent error, got {:?}", e),
        }
    }

    #[tokio::test]
    async fn test_quorum_check_after_network_partition() {
        use crate::config::{QuorumConfig, QuorumType};

        let quorum = QuorumConfig::builder("important-*")
            .min_cluster_size(3)
            .quorum_type(QuorumType::ReadWrite)
            .build()
            .unwrap();

        let config = ClientConfigBuilder::new()
            .add_quorum_config(quorum)
            .build()
            .unwrap();

        let discovery = super::super::discovery::StaticAddressDiscovery::default();
        let manager = ConnectionManager::new(config, discovery);

        let member1 = crate::listener::Member::new(
            uuid::Uuid::new_v4(),
            "127.0.0.1:5701".parse().unwrap(),
        );
        let member2 = crate::listener::Member::new(
            uuid::Uuid::new_v4(),
            "127.0.0.1:5702".parse().unwrap(),
        );
        let member3 = crate::listener::Member::new(
            uuid::Uuid::new_v4(),
            "127.0.0.1:5703".parse().unwrap(),
        );

        manager.handle_member_added(member1.clone()).await;
        manager.handle_member_added(member2.clone()).await;
        manager.handle_member_added(member3.clone()).await;

        assert!(manager.check_quorum("important-map", false).await.is_ok());

        manager.handle_member_removed(member3.uuid()).await;

        let result = manager.check_quorum("important-map", false).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), HazelcastError::QuorumNotPresent(_)));

        let member4 = crate::listener::Member::new(
            uuid::Uuid::new_v4(),
            "127.0.0.1:5704".parse().unwrap(),
        );
        manager.handle_member_added(member4).await;

        assert!(manager.check_quorum("important-map", false).await.is_ok());
    }

    #[tokio::test]
    async fn test_quorum_read_only_allows_writes() {
        use crate::config::{QuorumConfig, QuorumType};

        let quorum = QuorumConfig::builder("read-protected-*")
            .min_cluster_size(3)
            .quorum_type(QuorumType::Read)
            .build()
            .unwrap();

        let config = ClientConfigBuilder::new()
            .add_quorum_config(quorum)
            .build()
            .unwrap();

        let discovery = super::super::discovery::StaticAddressDiscovery::default();
        let manager = ConnectionManager::new(config, discovery);

        let member = crate::listener::Member::new(
            uuid::Uuid::new_v4(),
            "127.0.0.1:5701".parse().unwrap(),
        );
        manager.handle_member_added(member).await;

        let read_result = manager.check_quorum("read-protected-map", true).await;
        assert!(read_result.is_err());

        let write_result = manager.check_quorum("read-protected-map", false).await;
        assert!(write_result.is_ok());
    }

    #[tokio::test]
    async fn test_quorum_write_only_allows_reads() {
        use crate::config::{QuorumConfig, QuorumType};

        let quorum = QuorumConfig::builder("write-protected-*")
            .min_cluster_size(3)
            .quorum_type(QuorumType::Write)
            .build()
            .unwrap();

        let config = ClientConfigBuilder::new()
            .add_quorum_config(quorum)
            .build()
            .unwrap();

        let discovery = super::super::discovery::StaticAddressDiscovery::default();
        let manager = ConnectionManager::new(config, discovery);

        let member = crate::listener::Member::new(
            uuid::Uuid::new_v4(),
            "127.0.0.1:5701".parse().unwrap(),
        );
        manager.handle_member_added(member).await;

        let read_result = manager.check_quorum("write-protected-map", true).await;
        assert!(read_result.is_ok());

        let write_result = manager.check_quorum("write-protected-map", false).await;
        assert!(write_result.is_err());
    }

    #[tokio::test]
    async fn test_quorum_no_config_always_passes() {
        let config = ClientConfigBuilder::new().build().unwrap();

        let discovery = super::super::discovery::StaticAddressDiscovery::default();
        let manager = ConnectionManager::new(config, discovery);

        assert!(manager.check_quorum("any-map", true).await.is_ok());
        assert!(manager.check_quorum("any-map", false).await.is_ok());
    }

    #[tokio::test]
    async fn test_quorum_pattern_matching() {
        use crate::config::{QuorumConfig, QuorumType};

        let quorum = QuorumConfig::builder("user-*")
            .min_cluster_size(2)
            .quorum_type(QuorumType::ReadWrite)
            .build()
            .unwrap();

        let config = ClientConfigBuilder::new()
            .add_quorum_config(quorum)
            .build()
            .unwrap();

        let discovery = super::super::discovery::StaticAddressDiscovery::default();
        let manager = ConnectionManager::new(config, discovery);

        assert!(manager.check_quorum("user-sessions", false).await.is_err());
        assert!(manager.check_quorum("user-data", false).await.is_err());

        assert!(manager.check_quorum("product-catalog", false).await.is_ok());
        assert!(manager.check_quorum("admin-users", false).await.is_ok());
    }

    #[tokio::test]
    async fn test_quorum_custom_function() {
        use crate::config::{QuorumConfig, QuorumFunction, QuorumType};

        struct OddMemberQuorum;
        impl QuorumFunction for OddMemberQuorum {
            fn is_present(&self, members: &[crate::listener::Member]) -> bool {
                members.len() % 2 == 1
            }
        }

        let quorum = QuorumConfig::builder("custom-*")
            .min_cluster_size(0)
            .quorum_type(QuorumType::ReadWrite)
            .quorum_function(std::sync::Arc::new(OddMemberQuorum))
            .build()
            .unwrap();

        let config = ClientConfigBuilder::new()
            .add_quorum_config(quorum)
            .build()
            .unwrap();

        let discovery = super::super::discovery::StaticAddressDiscovery::default();
        let manager = ConnectionManager::new(config, discovery);

        assert!(manager.check_quorum("custom-map", false).await.is_err());

        let member1 = crate::listener::Member::new(
            uuid::Uuid::new_v4(),
            "127.0.0.1:5701".parse().unwrap(),
        );
        manager.handle_member_added(member1.clone()).await;
        assert!(manager.check_quorum("custom-map", false).await.is_ok());

        let member2 = crate::listener::Member::new(
            uuid::Uuid::new_v4(),
            "127.0.0.1:5702".parse().unwrap(),
        );
        manager.handle_member_added(member2).await;
        assert!(manager.check_quorum("custom-map", false).await.is_err());

        let member3 = crate::listener::Member::new(
            uuid::Uuid::new_v4(),
            "127.0.0.1:5703".parse().unwrap(),
        );
        manager.handle_member_added(member3).await;
        assert!(manager.check_quorum("custom-map", false).await.is_ok());
    }

    #[tokio::test]
    async fn test_quorum_multiple_configs() {
        use crate::config::{QuorumConfig, QuorumType};

        let critical_quorum = QuorumConfig::builder("critical-*")
            .min_cluster_size(5)
            .quorum_type(QuorumType::ReadWrite)
            .build()
            .unwrap();

        let standard_quorum = QuorumConfig::builder("standard-*")
            .min_cluster_size(2)
            .quorum_type(QuorumType::Write)
            .build()
            .unwrap();

        let config = ClientConfigBuilder::new()
            .add_quorum_config(critical_quorum)
            .add_quorum_config(standard_quorum)
            .build()
            .unwrap();

        let discovery = super::super::discovery::StaticAddressDiscovery::default();
        let manager = ConnectionManager::new(config, discovery);

        for i in 0..3 {
            let member = crate::listener::Member::new(
                uuid::Uuid::new_v4(),
                format!("127.0.0.1:{}", 5701 + i).parse().unwrap(),
            );
            manager.handle_member_added(member).await;
        }

        assert!(manager.check_quorum("critical-data", false).await.is_err());
        assert!(manager.check_quorum("standard-data", false).await.is_ok());
        assert!(manager.check_quorum("standard-data", true).await.is_ok());
    }

    #[tokio::test]
    async fn test_lifecycle_event_order_on_start() {
        let (listener, addr) = create_mock_server().await;

        tokio::spawn(async move {
            let _ = listener.accept().await;
        });

        let config = ClientConfigBuilder::new()
            .add_address(addr)
            .build()
            .unwrap();

        let manager = ConnectionManager::from_config(config);
        let mut rx = manager.subscribe_lifecycle();

        manager.start().await.unwrap();

        let event1 = tokio::time::timeout(Duration::from_millis(100), rx.recv())
            .await
            .unwrap()
            .unwrap();
        let event2 = tokio::time::timeout(Duration::from_millis(100), rx.recv())
            .await
            .unwrap()
            .unwrap();
        let event3 = tokio::time::timeout(Duration::from_millis(100), rx.recv())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(event1, crate::listener::LifecycleEvent::Starting);
        assert_eq!(event2, crate::listener::LifecycleEvent::ClientConnected);
        assert_eq!(event3, crate::listener::LifecycleEvent::Started);
    }

    #[test]
    fn test_lifecycle_event_is_send() {
        fn assert_send<T: Send>() {}
        assert_send::<crate::listener::LifecycleEvent>();
    }

    #[test]
    fn test_find_quorum_config_returns_none_when_no_match() {
        let config = ClientConfigBuilder::new().build().unwrap();
        let discovery = super::super::discovery::StaticAddressDiscovery::default();
        let manager = ConnectionManager::new(config, discovery);

        assert!(manager.find_quorum_config("any-map").is_none());
    }

    #[test]
    fn test_find_quorum_config_returns_matching() {
        use crate::config::{QuorumConfig, QuorumType};

        let quorum = QuorumConfig::builder("test-*")
            .min_cluster_size(3)
            .quorum_type(QuorumType::ReadWrite)
            .build()
            .unwrap();

        let config = ClientConfigBuilder::new()
            .add_quorum_config(quorum)
            .build()
            .unwrap();

        let discovery = super::super::discovery::StaticAddressDiscovery::default();
        let manager = ConnectionManager::new(config, discovery);

        let found = manager.find_quorum_config("test-map");
        assert!(found.is_some());
        assert_eq!(found.unwrap().min_cluster_size(), 3);
    }

    #[tokio::test]
    async fn test_partition_table_operations() {
        let config = ClientConfigBuilder::new().build().unwrap();
        let discovery = super::super::discovery::StaticAddressDiscovery::default();
        let manager = ConnectionManager::new(config, discovery);

        assert_eq!(manager.partition_count(), 0);
        assert!(manager.get_partition_owner(0).await.is_none());

        let member1_uuid = uuid::Uuid::new_v4();
        let member2_uuid = uuid::Uuid::new_v4();

        let mut partitions = HashMap::new();
        partitions.insert(0, member1_uuid);
        partitions.insert(1, member2_uuid);
        partitions.insert(2, member1_uuid);

        manager.update_partition_table(partitions).await;

        assert_eq!(manager.partition_count(), 3);
        assert_eq!(manager.get_partition_owner(0).await, Some(member1_uuid));
        assert_eq!(manager.get_partition_owner(1).await, Some(member2_uuid));
        assert_eq!(manager.get_partition_owner(2).await, Some(member1_uuid));
        assert!(manager.get_partition_owner(99).await.is_none());
    }

    #[tokio::test]
    async fn test_set_partition_owner() {
        let config = ClientConfigBuilder::new().build().unwrap();
        let discovery = super::super::discovery::StaticAddressDiscovery::default();
        let manager = ConnectionManager::new(config, discovery);

        let member_uuid = uuid::Uuid::new_v4();

        manager.set_partition_owner(5, member_uuid).await;

        assert_eq!(manager.partition_count(), 1);
        assert_eq!(manager.get_partition_owner(5).await, Some(member_uuid));
    }

    #[tokio::test]
    async fn test_get_partition_owner_address() {
        let config = ClientConfigBuilder::new().build().unwrap();
        let discovery = super::super::discovery::StaticAddressDiscovery::default();
        let manager = ConnectionManager::new(config, discovery);

        let member_uuid = uuid::Uuid::new_v4();
        let member_addr: SocketAddr = "127.0.0.1:5701".parse().unwrap();
        let member = crate::listener::Member::new(member_uuid, member_addr);

        manager.handle_member_added(member).await;
        manager.set_partition_owner(0, member_uuid).await;

        let addr = manager.get_partition_owner_address(0).await;
        assert_eq!(addr, Some(member_addr));

        assert!(manager.get_partition_owner_address(99).await.is_none());
    }

    #[tokio::test]
    async fn test_get_connection_for_partition_fallback() {
        let (listener, addr) = create_mock_server().await;

        tokio::spawn(async move {
            let _ = listener.accept().await;
        });

        let config = ClientConfigBuilder::new()
            .add_address(addr)
            .build()
            .unwrap();

        let manager = ConnectionManager::from_config(config);
        manager.connect_to(addr).await.unwrap();

        let result = manager.get_connection_for_partition(0).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), addr);
    }

    #[tokio::test]
    async fn test_get_connection_for_partition_with_owner() {
        let (listener, addr) = create_mock_server().await;

        tokio::spawn(async move {
            let _ = listener.accept().await;
        });

        let config = ClientConfigBuilder::new()
            .add_address(addr)
            .build()
            .unwrap();

        let manager = ConnectionManager::from_config(config);

        let member_uuid = uuid::Uuid::new_v4();
        let member = crate::listener::Member::new(member_uuid, addr);

        manager.handle_member_added(member).await;
        manager.set_partition_owner(0, member_uuid).await;
        manager.connect_to(addr).await.unwrap();

        let result = manager.get_connection_for_partition(0).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), addr);
    }

    #[tokio::test]
    async fn test_get_connection_for_partition_no_connections() {
        let config = ClientConfigBuilder::new().build().unwrap();
        let discovery = super::super::discovery::StaticAddressDiscovery::default();
        let manager = ConnectionManager::new(config, discovery);

        let result = manager.get_connection_for_partition(0).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_get_connection_for_partition_smart_routing_disabled() {
        let (listener, addr) = create_mock_server().await;

        tokio::spawn(async move {
            let _ = listener.accept().await;
        });

        let config = ClientConfigBuilder::new()
            .add_address(addr)
            .network(|n| n.smart_routing(false))
            .build()
            .unwrap();

        let manager = ConnectionManager::from_config(config);

        let member_uuid = uuid::Uuid::new_v4();
        let other_addr: SocketAddr = "192.0.2.1:5701".parse().unwrap();
        let member = crate::listener::Member::new(member_uuid, other_addr);

        manager.handle_member_added(member).await;
        manager.set_partition_owner(0, member_uuid).await;
        manager.connect_to(addr).await.unwrap();

        let result = manager.get_connection_for_partition(0).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), addr);
    }

    #[tokio::test]
    async fn test_clear_partition_table() {
        let config = ClientConfigBuilder::new().build().unwrap();
        let discovery = super::super::discovery::StaticAddressDiscovery::default();
        let manager = ConnectionManager::new(config, discovery);

        let mut partitions = HashMap::new();
        partitions.insert(0, uuid::Uuid::new_v4());
        partitions.insert(1, uuid::Uuid::new_v4());

        manager.update_partition_table(partitions).await;
        assert_eq!(manager.partition_count(), 2);

        manager.clear_partition_table().await;
        assert_eq!(manager.partition_count(), 0);
        assert!(manager.get_partition_owner(0).await.is_none());
    }

    #[tokio::test]
    async fn test_partition_table_snapshot() {
        let config = ClientConfigBuilder::new().build().unwrap();
        let discovery = super::super::discovery::StaticAddressDiscovery::default();
        let manager = ConnectionManager::new(config, discovery);

        let uuid1 = uuid::Uuid::new_v4();
        let uuid2 = uuid::Uuid::new_v4();

        let mut partitions = HashMap::new();
        partitions.insert(0, uuid1);
        partitions.insert(1, uuid2);

        manager.update_partition_table(partitions.clone()).await;

        let snapshot = manager.partition_table().await;
        assert_eq!(snapshot, partitions);
    }

    #[tokio::test]
    async fn test_send_to_partition() {
        let (listener, addr) = create_mock_server().await;

        tokio::spawn(async move {
            accept_and_echo(listener).await;
        });

        let config = ClientConfigBuilder::new()
            .add_address(addr)
            .build()
            .unwrap();

        let manager = ConnectionManager::from_config(config);

        let member_uuid = uuid::Uuid::new_v4();
        let member = crate::listener::Member::new(member_uuid, addr);

        manager.handle_member_added(member).await;
        manager.set_partition_owner(0, member_uuid).await;
        manager.connect_to(addr).await.unwrap();

        let msg = hazelcast_core::ClientMessage::create_for_encode(0x000100, 0);
        let result = manager.send_to_partition(0, msg).await;
        assert!(result.is_ok(), "send_to_partition failed: {:?}", result.err());
    }

    #[tokio::test]
    async fn test_handle_member_removed_unknown_uuid() {
        let config = ClientConfigBuilder::new().build().unwrap();
        let discovery = super::super::discovery::StaticAddressDiscovery::default();
        let manager = ConnectionManager::new(config, discovery);

        let unknown_uuid = uuid::Uuid::new_v4();
        manager.handle_member_removed(unknown_uuid).await;

        assert_eq!(manager.member_count().await, 0);
    }

    #[tokio::test]
    async fn test_receive_from_nonexistent_connection() {
        let config = ClientConfigBuilder::new().build().unwrap();
        let manager = ConnectionManager::from_config(config);

        let addr: SocketAddr = "127.0.0.1:9999".parse().unwrap();
        let result = manager.receive_from(addr).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_disconnect_nonexistent_address() {
        let config = ClientConfigBuilder::new().build().unwrap();
        let manager = ConnectionManager::from_config(config);

        let addr: SocketAddr = "127.0.0.1:9999".parse().unwrap();
        let result = manager.disconnect(addr).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_set_initial_members_clears_existing() {
        let config = ClientConfigBuilder::new().build().unwrap();
        let discovery = super::super::discovery::StaticAddressDiscovery::default();
        let manager = ConnectionManager::new(config, discovery);

        let member1 = crate::listener::Member::new(
            uuid::Uuid::new_v4(),
            "127.0.0.1:5701".parse().unwrap(),
        );
        manager.handle_member_added(member1).await;
        assert_eq!(manager.member_count().await, 1);

        let new_members = vec![
            crate::listener::Member::new(
                uuid::Uuid::new_v4(),
                "127.0.0.1:5702".parse().unwrap(),
            ),
            crate::listener::Member::new(
                uuid::Uuid::new_v4(),
                "127.0.0.1:5703".parse().unwrap(),
            ),
        ];

        manager.set_initial_members(new_members).await;
        assert_eq!(manager.member_count().await, 2);
    }

    #[test]
    fn test_connection_event_variants() {
        let id = super::super::connection::ConnectionId::new();
        let addr: SocketAddr = "127.0.0.1:5701".parse().unwrap();

        let connected = ConnectionEvent::Connected { id, address: addr };
        let disconnected = ConnectionEvent::Disconnected {
            id,
            address: addr,
            error: Some("test error".to_string()),
        };
        let heartbeat = ConnectionEvent::HeartbeatReceived { id, address: addr };
        let reconnect_attempt = ConnectionEvent::ReconnectAttempt {
            address: addr,
            attempt: 1,
            next_delay: Duration::from_secs(1),
        };
        let reconnect_failed = ConnectionEvent::ReconnectFailed {
            address: addr,
            error: "failed".to_string(),
        };

        assert!(format!("{:?}", connected).contains("Connected"));
        assert!(format!("{:?}", disconnected).contains("Disconnected"));
        assert!(format!("{:?}", heartbeat).contains("HeartbeatReceived"));
        assert!(format!("{:?}", reconnect_attempt).contains("ReconnectAttempt"));
        assert!(format!("{:?}", reconnect_failed).contains("ReconnectFailed"));
    }

    #[test]
    fn test_connection_event_clone() {
        let id = super::super::connection::ConnectionId::new();
        let addr: SocketAddr = "127.0.0.1:5701".parse().unwrap();

        let event = ConnectionEvent::Connected { id, address: addr };
        let cloned = event.clone();

        match cloned {
            ConnectionEvent::Connected { address, .. } => {
                assert_eq!(address, addr);
            }
            _ => panic!("expected Connected event"),
        }
    }

    #[test]
    fn test_connection_manager_with_failover() {
        use crate::config::{ClientConfigBuilder, ClientFailoverConfig};

        let config1 = ClientConfigBuilder::new()
            .cluster_name("primary")
            .build()
            .unwrap();
        let config2 = ClientConfigBuilder::new()
            .cluster_name("backup")
            .build()
            .unwrap();

        let failover = ClientFailoverConfig::builder()
            .add_client_config(config1)
            .add_client_config(config2)
            .try_count(2)
            .build()
            .unwrap();

        let manager = ConnectionManager::with_failover(failover);

        assert!(manager.has_failover());
        assert_eq!(manager.current_cluster_index(), 0);
        assert!(manager.failover_config().is_some());
        assert_eq!(manager.failover_config().unwrap().cluster_count(), 2);
    }

    #[test]
    fn test_connection_manager_without_failover() {
        let config = ClientConfigBuilder::new().build().unwrap();
        let manager = ConnectionManager::from_config(config);

        assert!(!manager.has_failover());
        assert_eq!(manager.current_cluster_index(), 0);
        assert!(manager.failover_config().is_none());
    }

    #[tokio::test]
    async fn test_trigger_failover_without_config() {
        let config = ClientConfigBuilder::new().build().unwrap();
        let manager = ConnectionManager::from_config(config);

        let result = manager.trigger_failover().await;
        assert!(result.is_err());
        match result.unwrap_err() {
            HazelcastError::Connection(msg) => {
                assert!(msg.contains("failover not configured"));
            }
            e => panic!("expected Connection error, got {:?}", e),
        }
    }

    #[tokio::test]
    async fn test_failover_lifecycle_event() {
        use crate::config::{ClientConfigBuilder, ClientFailoverConfig};

        let (listener1, addr1) = create_mock_server().await;
        let (listener2, addr2) = create_mock_server().await;

        tokio::spawn(async move {
            let _ = listener1.accept().await;
        });
        tokio::spawn(async move {
            let _ = listener2.accept().await;
        });

        let config1 = ClientConfigBuilder::new()
            .cluster_name("primary")
            .add_address(addr1)
            .build()
            .unwrap();
        let config2 = ClientConfigBuilder::new()
            .cluster_name("backup")
            .add_address(addr2)
            .build()
            .unwrap();

        let failover = ClientFailoverConfig::builder()
            .add_client_config(config1)
            .add_client_config(config2)
            .try_count(1)
            .build()
            .unwrap();

        let manager = ConnectionManager::with_failover(failover);
        let mut lifecycle_rx = manager.subscribe_lifecycle();

        manager.connect_to(addr1).await.unwrap();

        let _ = manager.trigger_failover().await;

        assert_eq!(manager.current_cluster_index(), 1);

        let mut found_event = false;
        while let Ok(event) = tokio::time::timeout(Duration::from_millis(100), lifecycle_rx.recv()).await {
            if let Ok(LifecycleEvent::ClientChangedCluster) = event {
                found_event = true;
                break;
            }
        }
        assert!(found_event, "expected ClientChangedCluster lifecycle event");
    }

    #[tokio::test]
    async fn test_reconnect_mode_off_no_reconnection() {
        use crate::config::ReconnectMode;

        let config = ClientConfigBuilder::new()
            .network(|n| n.reconnect_mode(ReconnectMode::Off))
            .build()
            .unwrap();

        let manager = ConnectionManager::from_config(config);
        assert_eq!(manager.config.network().reconnect_mode(), ReconnectMode::Off);
    }

    #[tokio::test]
    async fn test_reconnect_mode_on_default() {
        use crate::config::ReconnectMode;

        let config = ClientConfigBuilder::new().build().unwrap();
        let manager = ConnectionManager::from_config(config);
        assert_eq!(manager.config.network().reconnect_mode(), ReconnectMode::On);
    }

    #[tokio::test]
    async fn test_reconnect_mode_async() {
        use crate::config::ReconnectMode;

        let config = ClientConfigBuilder::new()
            .network(|n| n.reconnect_mode(ReconnectMode::Async))
            .build()
            .unwrap();

        let manager = ConnectionManager::from_config(config);
        assert_eq!(manager.config.network().reconnect_mode(), ReconnectMode::Async);
    }

    #[tokio::test]
    async fn test_failover_cycles_through_clusters() {
        use crate::config::{ClientConfigBuilder, ClientFailoverConfig};

        let config1 = ClientConfigBuilder::new()
            .cluster_name("cluster1")
            .add_address("192.0.2.1:5701".parse().unwrap())
            .build()
            .unwrap();
        let config2 = ClientConfigBuilder::new()
            .cluster_name("cluster2")
            .add_address("192.0.2.2:5701".parse().unwrap())
            .build()
            .unwrap();
        let config3 = ClientConfigBuilder::new()
            .cluster_name("cluster3")
            .add_address("192.0.2.3:5701".parse().unwrap())
            .build()
            .unwrap();

        let failover = ClientFailoverConfig::builder()
            .add_client_config(config1)
            .add_client_config(config2)
            .add_client_config(config3)
            .try_count(1)
            .build()
            .unwrap();

        let manager = ConnectionManager::with_failover(failover);

        assert_eq!(manager.current_cluster_index(), 0);

        let _ = manager.trigger_failover().await;
        assert_eq!(manager.current_cluster_index(), 1);

        let _ = manager.trigger_failover().await;
        assert_eq!(manager.current_cluster_index(), 2);

        let _ = manager.trigger_failover().await;
        assert_eq!(manager.current_cluster_index(), 0);
    }

    #[tokio::test]
    async fn test_failover_on_disconnect_no_config() {
        let config = ClientConfigBuilder::new().build().unwrap();
        let manager = ConnectionManager::from_config(config);

        let result = manager.failover_on_disconnect().await;
        assert!(result.is_ok());
        assert!(!result.unwrap());
    }

    #[tokio::test]
    async fn test_failover_on_disconnect_with_active_connections() {
        use crate::config::{ClientConfigBuilder, ClientFailoverConfig};

        let (listener, addr) = create_mock_server().await;

        tokio::spawn(async move {
            let _ = listener.accept().await;
        });

        let config1 = ClientConfigBuilder::new()
            .cluster_name("primary")
            .add_address(addr)
            .build()
            .unwrap();
        let config2 = ClientConfigBuilder::new()
            .cluster_name("backup")
            .add_address("192.0.2.1:5701".parse().unwrap())
            .build()
            .unwrap();

        let failover = ClientFailoverConfig::builder()
            .add_client_config(config1)
            .add_client_config(config2)
            .build()
            .unwrap();

        let manager = ConnectionManager::with_failover(failover);
        manager.connect_to(addr).await.unwrap();

        let result = manager.failover_on_disconnect().await;
        assert!(result.is_ok());
        assert!(!result.unwrap());
    }

    #[tokio::test]
    async fn test_failover_on_disconnect_triggers_failover() {
        use crate::config::{ClientConfigBuilder, ClientFailoverConfig};

        let (listener, addr) = create_mock_server().await;

        tokio::spawn(async move {
            let _ = listener.accept().await;
        });

        let config1 = ClientConfigBuilder::new()
            .cluster_name("primary")
            .add_address("192.0.2.1:5701".parse().unwrap())
            .build()
            .unwrap();
        let config2 = ClientConfigBuilder::new()
            .cluster_name("backup")
            .add_address(addr)
            .build()
            .unwrap();

        let failover = ClientFailoverConfig::builder()
            .add_client_config(config1)
            .add_client_config(config2)
            .try_count(1)
            .build()
            .unwrap();

        let manager = ConnectionManager::with_failover(failover);

        let result = manager.failover_on_disconnect().await;

        assert!(result.is_ok() || result.is_err());
    }

    #[tokio::test]
    async fn test_try_alternative_cluster_no_config() {
        let config = ClientConfigBuilder::new().build().unwrap();
        let manager = ConnectionManager::from_config(config);

        let result = manager.try_alternative_cluster().await;
        assert!(result.is_err());
        match result.unwrap_err() {
            HazelcastError::Connection(msg) => {
                assert!(msg.contains("failover not configured"));
            }
            e => panic!("expected Connection error, got {:?}", e),
        }
    }

    #[tokio::test]
    async fn test_try_alternative_cluster_single_cluster() {
        use crate::config::{ClientConfigBuilder, ClientFailoverConfig};

        let config = ClientConfigBuilder::new()
            .cluster_name("only")
            .build()
            .unwrap();

        let failover = ClientFailoverConfig::builder()
            .add_client_config(config)
            .build()
            .unwrap();

        let manager = ConnectionManager::with_failover(failover);

        let result = manager.try_alternative_cluster().await;
        assert!(result.is_err());
        match result.unwrap_err() {
            HazelcastError::Connection(msg) => {
                assert!(msg.contains("no alternative clusters configured"));
            }
            e => panic!("expected Connection error, got {:?}", e),
        }
    }

    #[tokio::test]
    async fn test_try_alternative_cluster_success() {
        use crate::config::{ClientConfigBuilder, ClientFailoverConfig};

        let (listener1, addr1) = create_mock_server().await;
        let (listener2, addr2) = create_mock_server().await;

        tokio::spawn(async move {
            let _ = listener1.accept().await;
        });
        tokio::spawn(async move {
            let _ = listener2.accept().await;
        });

        let config1 = ClientConfigBuilder::new()
            .cluster_name("primary")
            .add_address(addr1)
            .build()
            .unwrap();
        let config2 = ClientConfigBuilder::new()
            .cluster_name("backup")
            .add_address(addr2)
            .build()
            .unwrap();

        let failover = ClientFailoverConfig::builder()
            .add_client_config(config1)
            .add_client_config(config2)
            .build()
            .unwrap();

        let manager = ConnectionManager::with_failover(failover);
        manager.connect_to(addr1).await.unwrap();

        assert_eq!(manager.current_cluster_index(), 0);

        let result = manager.try_alternative_cluster().await;
        assert!(result.is_ok());
        assert_eq!(manager.current_cluster_index(), 1);
    }

    #[tokio::test]
    async fn test_try_alternative_cluster_emits_lifecycle_event() {
        use crate::config::{ClientConfigBuilder, ClientFailoverConfig};

        let (listener1, addr1) = create_mock_server().await;
        let (listener2, addr2) = create_mock_server().await;

        tokio::spawn(async move {
            let _ = listener1.accept().await;
        });
        tokio::spawn(async move {
            let _ = listener2.accept().await;
        });

        let config1 = ClientConfigBuilder::new()
            .cluster_name("primary")
            .add_address(addr1)
            .build()
            .unwrap();
        let config2 = ClientConfigBuilder::new()
            .cluster_name("backup")
            .add_address(addr2)
            .build()
            .unwrap();

        let failover = ClientFailoverConfig::builder()
            .add_client_config(config1)
            .add_client_config(config2)
            .build()
            .unwrap();

        let manager = ConnectionManager::with_failover(failover);
        let mut lifecycle_rx = manager.subscribe_lifecycle();

        manager.connect_to(addr1).await.unwrap();
        manager.try_alternative_cluster().await.unwrap();

        let mut found_event = false;
        while let Ok(event) = tokio::time::timeout(Duration::from_millis(100), lifecycle_rx.recv()).await {
            if let Ok(LifecycleEvent::ClientChangedCluster) = event {
                found_event = true;
                break;
            }
        }
        assert!(found_event, "expected ClientChangedCluster lifecycle event");
    }

    #[tokio::test]
    async fn test_try_alternative_cluster_clears_state() {
        use crate::config::{ClientConfigBuilder, ClientFailoverConfig};

        let (listener1, addr1) = create_mock_server().await;
        let (listener2, addr2) = create_mock_server().await;

        tokio::spawn(async move {
            let _ = listener1.accept().await;
        });
        tokio::spawn(async move {
            let _ = listener2.accept().await;
        });

        let config1 = ClientConfigBuilder::new()
            .cluster_name("primary")
            .add_address(addr1)
            .build()
            .unwrap();
        let config2 = ClientConfigBuilder::new()
            .cluster_name("backup")
            .add_address(addr2)
            .build()
            .unwrap();

        let failover = ClientFailoverConfig::builder()
            .add_client_config(config1)
            .add_client_config(config2)
            .build()
            .unwrap();

        let manager = ConnectionManager::with_failover(failover);
        manager.connect_to(addr1).await.unwrap();

        let member = crate::listener::Member::new(
            uuid::Uuid::new_v4(),
            "127.0.0.1:5701".parse().unwrap(),
        );
        manager.handle_member_added(member).await;
        manager.set_partition_owner(0, uuid::Uuid::new_v4()).await;

        assert_eq!(manager.member_count().await, 1);
        assert_eq!(manager.partition_count(), 1);

        manager.try_alternative_cluster().await.unwrap();

        assert_eq!(manager.member_count().await, 0);
        assert_eq!(manager.partition_count(), 0);
    }

    #[tokio::test]
    async fn test_automatic_failover_cycles_all_clusters() {
        use crate::config::{ClientConfigBuilder, ClientFailoverConfig};

        let config1 = ClientConfigBuilder::new()
            .cluster_name("cluster1")
            .add_address("192.0.2.1:5701".parse().unwrap())
            .build()
            .unwrap();
        let config2 = ClientConfigBuilder::new()
            .cluster_name("cluster2")
            .add_address("192.0.2.2:5701".parse().unwrap())
            .build()
            .unwrap();
        let config3 = ClientConfigBuilder::new()
            .cluster_name("cluster3")
            .add_address("192.0.2.3:5701".parse().unwrap())
            .build()
            .unwrap();

        let failover = ClientFailoverConfig::builder()
            .add_client_config(config1)
            .add_client_config(config2)
            .add_client_config(config3)
            .try_count(1)
            .build()
            .unwrap();

        let manager = ConnectionManager::with_failover(failover);

        assert_eq!(manager.current_cluster_index(), 0);

        let _ = manager.try_alternative_cluster().await;
        assert_eq!(manager.current_cluster_index(), 1);

        let _ = manager.try_alternative_cluster().await;
        assert_eq!(manager.current_cluster_index(), 2);

        let _ = manager.try_alternative_cluster().await;
        assert_eq!(manager.current_cluster_index(), 0);
    }
}
