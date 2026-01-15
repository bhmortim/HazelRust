//! Connection pool management and lifecycle handling.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{broadcast, RwLock};
use tokio::time::{interval, timeout};
use uuid::Uuid;

use hazelcast_core::{HazelcastError, Result};

use super::connection::{Connection, ConnectionId};
use super::discovery::ClusterDiscovery;
use crate::config::ClientConfig;
use crate::listener::{Member, MemberEvent, MemberEventType};

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
    event_sender: broadcast::Sender<ConnectionEvent>,
    membership_sender: broadcast::Sender<MemberEvent>,
    shutdown: tokio::sync::watch::Sender<bool>,
}

impl ConnectionManager {
    /// Creates a new connection manager with the given configuration and discovery.
    pub fn new(config: ClientConfig, discovery: impl ClusterDiscovery + 'static) -> Self {
        let (event_sender, _) = broadcast::channel(64);
        let (membership_sender, _) = broadcast::channel(64);
        let (shutdown, _) = tokio::sync::watch::channel(false);

        Self {
            config: Arc::new(config),
            discovery: Arc::new(discovery),
            connections: Arc::new(RwLock::new(HashMap::new())),
            members: Arc::new(RwLock::new(HashMap::new())),
            event_sender,
            membership_sender,
            shutdown,
        }
    }

    /// Creates a connection manager using the network config addresses for discovery.
    pub fn from_config(config: ClientConfig) -> Self {
        let discovery = super::discovery::StaticAddressDiscovery::new(
            config.network().addresses().to_vec(),
        );
        Self::new(config, discovery)
    }

    /// Subscribes to connection lifecycle events.
    pub fn subscribe(&self) -> broadcast::Receiver<ConnectionEvent> {
        self.event_sender.subscribe()
    }

    /// Subscribes to cluster membership events.
    pub fn subscribe_membership(&self) -> broadcast::Receiver<MemberEvent> {
        self.membership_sender.subscribe()
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
    pub async fn start(&self) -> Result<()> {
        let addresses = self.discovery.discover().await?;

        if addresses.is_empty() {
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
            return Err(HazelcastError::Connection(
                "failed to establish any connections".to_string(),
            ));
        }

        self.spawn_heartbeat_task();

        Ok(())
    }

    /// Establishes a connection to the specified address.
    pub async fn connect_to(&self, address: SocketAddr) -> Result<ConnectionId> {
        let connect_timeout = self.config.network().connection_timeout();

        let connection = timeout(connect_timeout, self.create_connection(address))
            .await
            .map_err(|_| {
                HazelcastError::Timeout(format!(
                    "connection to {} timed out after {:?}",
                    address, connect_timeout
                ))
            })??;

        let id = connection.id();

        self.connections.write().await.insert(address, connection);

        let _ = self.event_sender.send(ConnectionEvent::Connected { id, address });
        tracing::info!(id = %id, address = %address, "connected to cluster member");

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
                address = %address,
                attempt = attempt,
                backoff = ?current_backoff,
                "attempting reconnection"
            );

            tokio::time::sleep(current_backoff).await;

            match self.connect_to(address).await {
                Ok(id) => return Ok(id),
                Err(e) => {
                    tracing::warn!(
                        address = %address,
                        attempt = attempt,
                        error = %e,
                        "reconnection attempt failed"
                    );
                }
            }

            current_backoff = std::cmp::min(
                Duration::from_secs_f64(
                    current_backoff.as_secs_f64() * retry_config.multiplier(),
                ),
                retry_config.max_backoff(),
            );
        }
    }

    /// Disconnects from the specified address.
    pub async fn disconnect(&self, address: SocketAddr) -> Result<()> {
        if let Some(connection) = self.connections.write().await.remove(&address) {
            let id = connection.id();

            let _ = self.event_sender.send(ConnectionEvent::Disconnected {
                id,
                address,
                error: None,
            });

            connection.close().await?;
            tracing::info!(id = %id, address = %address, "disconnected from cluster member");
        }

        Ok(())
    }

    /// Disconnects from all cluster members and shuts down.
    pub async fn shutdown(&self) -> Result<()> {
        let _ = self.shutdown.send(true);

        let addresses: Vec<SocketAddr> = self.connections.read().await.keys().copied().collect();

        for address in addresses {
            if let Err(e) = self.disconnect(address).await {
                tracing::warn!(address = %address, error = %e, "error during disconnect");
            }
        }

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

    /// Checks if connected to the specified address.
    pub async fn is_connected(&self, address: &SocketAddr) -> bool {
        self.connections.read().await.contains_key(address)
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
                                    error: Some(error),
                                });
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
}
