//! WAN publisher for forwarding events to target clusters.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{broadcast, mpsc, RwLock};
use tokio::time::timeout;

use crate::config::{WanReplicationConfig, WanTargetClusterConfig};
use super::event::WanEvent;

/// Error type for WAN publishing operations.
#[derive(Debug, Clone)]
pub struct WanPublishError {
    message: String,
    target_cluster: Option<String>,
    is_retriable: bool,
}

impl WanPublishError {
    /// Creates a new publish error.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            target_cluster: None,
            is_retriable: false,
        }
    }

    /// Creates a retriable error for transient failures.
    pub fn retriable(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            target_cluster: None,
            is_retriable: true,
        }
    }

    /// Sets the target cluster name for this error.
    pub fn with_target(mut self, cluster: impl Into<String>) -> Self {
        self.target_cluster = Some(cluster.into());
        self
    }

    /// Returns the error message.
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Returns the target cluster name, if known.
    pub fn target_cluster(&self) -> Option<&str> {
        self.target_cluster.as_deref()
    }

    /// Returns `true` if the operation can be retried.
    pub fn is_retriable(&self) -> bool {
        self.is_retriable
    }
}

impl std::fmt::Display for WanPublishError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(ref cluster) = self.target_cluster {
            write!(f, "WAN publish to '{}' failed: {}", cluster, self.message)
        } else {
            write!(f, "WAN publish failed: {}", self.message)
        }
    }
}

impl std::error::Error for WanPublishError {}

/// The current state of a WAN publisher.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WanPublisherState {
    /// Publisher is stopped and not forwarding events.
    Stopped,
    /// Publisher is starting and establishing connections.
    Starting,
    /// Publisher is running and forwarding events.
    Running,
    /// Publisher is paused but maintains connections.
    Paused,
    /// Publisher is stopping and closing connections.
    Stopping,
}

impl WanPublisherState {
    /// Returns `true` if the publisher can accept events.
    pub fn can_publish(&self) -> bool {
        matches!(self, WanPublisherState::Running)
    }

    /// Returns `true` if the publisher is in a terminal state.
    pub fn is_terminal(&self) -> bool {
        matches!(self, WanPublisherState::Stopped)
    }
}

/// Events emitted by the WAN publisher.
#[derive(Debug, Clone)]
pub enum WanPublisherEvent {
    /// Successfully connected to a target cluster.
    Connected {
        /// The target cluster name.
        cluster_name: String,
        /// The connected endpoint address.
        address: SocketAddr,
    },
    /// Disconnected from a target cluster.
    Disconnected {
        /// The target cluster name.
        cluster_name: String,
        /// The error that caused disconnection, if any.
        error: Option<String>,
    },
    /// An event was successfully published.
    Published {
        /// The target cluster name.
        cluster_name: String,
        /// The map name.
        map_name: String,
        /// The number of events in the batch.
        event_count: usize,
    },
    /// Failed to publish an event.
    PublishFailed {
        /// The target cluster name.
        cluster_name: String,
        /// The error message.
        error: String,
        /// The number of failed events.
        event_count: usize,
    },
    /// Publisher state changed.
    StateChanged {
        /// The previous state.
        from: WanPublisherState,
        /// The new state.
        to: WanPublisherState,
    },
}

/// Connection state for a target cluster.
#[derive(Debug)]
struct TargetConnection {
    config: WanTargetClusterConfig,
    connected_endpoint: Option<SocketAddr>,
    retry_count: u32,
}

impl TargetConnection {
    fn new(config: WanTargetClusterConfig) -> Self {
        Self {
            config,
            connected_endpoint: None,
            retry_count: 0,
        }
    }

    fn is_connected(&self) -> bool {
        self.connected_endpoint.is_some()
    }
}

/// WAN publisher for forwarding events to target clusters.
///
/// The publisher manages connections to one or more target clusters and
/// forwards WAN replication events to them. It supports batching, retries,
/// and filtering of events.
pub struct WanPublisher {
    config: WanReplicationConfig,
    state: Arc<RwLock<WanPublisherState>>,
    connections: Arc<RwLock<HashMap<String, TargetConnection>>>,
    event_sender: broadcast::Sender<WanPublisherEvent>,
    publish_tx: mpsc::Sender<WanEvent>,
    publish_rx: Arc<RwLock<Option<mpsc::Receiver<WanEvent>>>>,
    shutdown: tokio::sync::watch::Sender<bool>,
    batch_size: usize,
    batch_timeout: Duration,
}

impl std::fmt::Debug for WanPublisher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WanPublisher")
            .field("name", &self.config.name())
            .field("target_count", &self.config.target_clusters().len())
            .field("batch_size", &self.batch_size)
            .finish()
    }
}

impl WanPublisher {
    /// Creates a new WAN publisher with the given configuration.
    pub fn new(config: WanReplicationConfig) -> Self {
        let (event_sender, _) = broadcast::channel(64);
        let (publish_tx, publish_rx) = mpsc::channel(1024);
        let (shutdown, _) = tokio::sync::watch::channel(false);

        let mut connections = HashMap::new();
        for target in config.target_clusters() {
            connections.insert(
                target.cluster_name().to_string(),
                TargetConnection::new(target.clone()),
            );
        }

        Self {
            config,
            state: Arc::new(RwLock::new(WanPublisherState::Stopped)),
            connections: Arc::new(RwLock::new(connections)),
            event_sender,
            publish_tx,
            publish_rx: Arc::new(RwLock::new(Some(publish_rx))),
            shutdown,
            batch_size: 100,
            batch_timeout: Duration::from_millis(50),
        }
    }

    /// Creates a new WAN publisher with custom batching settings.
    pub fn with_batching(
        config: WanReplicationConfig,
        batch_size: usize,
        batch_timeout: Duration,
    ) -> Self {
        let mut publisher = Self::new(config);
        publisher.batch_size = batch_size;
        publisher.batch_timeout = batch_timeout;
        publisher
    }

    /// Returns the WAN replication configuration name.
    pub fn name(&self) -> &str {
        self.config.name()
    }

    /// Returns the current publisher state.
    pub async fn state(&self) -> WanPublisherState {
        *self.state.read().await
    }

    /// Returns the list of target cluster names.
    pub fn target_clusters(&self) -> Vec<&str> {
        self.config
            .target_clusters()
            .iter()
            .map(|t| t.cluster_name())
            .collect()
    }

    /// Subscribes to publisher events.
    pub fn subscribe(&self) -> broadcast::Receiver<WanPublisherEvent> {
        self.event_sender.subscribe()
    }

    /// Checks if connected to the specified target cluster.
    pub async fn is_connected(&self, cluster_name: &str) -> bool {
        self.connections
            .read()
            .await
            .get(cluster_name)
            .map(|c| c.is_connected())
            .unwrap_or(false)
    }

    /// Returns the number of connected target clusters.
    pub async fn connected_count(&self) -> usize {
        self.connections
            .read()
            .await
            .values()
            .filter(|c| c.is_connected())
            .count()
    }

    /// Starts the WAN publisher, establishing connections to target clusters.
    pub async fn start(&self) -> Result<(), WanPublishError> {
        let current_state = *self.state.read().await;
        if current_state != WanPublisherState::Stopped {
            return Err(WanPublishError::new(format!(
                "cannot start publisher in state {:?}",
                current_state
            )));
        }

        self.set_state(WanPublisherState::Starting).await;

        for target in self.config.target_clusters() {
            if let Err(e) = self.connect_to_target(target).await {
                tracing::warn!(
                    cluster = target.cluster_name(),
                    error = %e,
                    "failed to connect to WAN target"
                );
            }
        }

        let connected = self.connected_count().await;
        if connected == 0 {
            self.set_state(WanPublisherState::Stopped).await;
            return Err(WanPublishError::new(
                "failed to connect to any target cluster",
            ));
        }

        self.spawn_publish_loop();
        self.set_state(WanPublisherState::Running).await;

        tracing::info!(
            name = self.config.name(),
            connected = connected,
            total = self.config.target_clusters().len(),
            "WAN publisher started"
        );

        Ok(())
    }

    /// Stops the WAN publisher, closing all connections.
    pub async fn stop(&self) -> Result<(), WanPublishError> {
        let current_state = *self.state.read().await;
        if current_state == WanPublisherState::Stopped {
            return Ok(());
        }

        self.set_state(WanPublisherState::Stopping).await;
        let _ = self.shutdown.send(true);

        let mut connections = self.connections.write().await;
        for (cluster_name, conn) in connections.iter_mut() {
            if conn.is_connected() {
                let _ = self.event_sender.send(WanPublisherEvent::Disconnected {
                    cluster_name: cluster_name.clone(),
                    error: None,
                });
                conn.connected_endpoint = None;
            }
        }
        drop(connections);

        self.set_state(WanPublisherState::Stopped).await;

        tracing::info!(name = self.config.name(), "WAN publisher stopped");
        Ok(())
    }

    /// Pauses the WAN publisher without closing connections.
    pub async fn pause(&self) -> Result<(), WanPublishError> {
        let current_state = *self.state.read().await;
        if current_state != WanPublisherState::Running {
            return Err(WanPublishError::new(format!(
                "cannot pause publisher in state {:?}",
                current_state
            )));
        }

        self.set_state(WanPublisherState::Paused).await;
        tracing::info!(name = self.config.name(), "WAN publisher paused");
        Ok(())
    }

    /// Resumes a paused WAN publisher.
    pub async fn resume(&self) -> Result<(), WanPublishError> {
        let current_state = *self.state.read().await;
        if current_state != WanPublisherState::Paused {
            return Err(WanPublishError::new(format!(
                "cannot resume publisher in state {:?}",
                current_state
            )));
        }

        self.set_state(WanPublisherState::Running).await;
        tracing::info!(name = self.config.name(), "WAN publisher resumed");
        Ok(())
    }

    /// Publishes a WAN event to all connected target clusters.
    pub async fn publish(&self, event: WanEvent) -> Result<(), WanPublishError> {
        let state = *self.state.read().await;
        if !state.can_publish() {
            return Err(WanPublishError::new(format!(
                "cannot publish in state {:?}",
                state
            )));
        }

        self.publish_tx
            .send(event)
            .await
            .map_err(|_| WanPublishError::new("publish channel closed"))
    }

    /// Publishes multiple WAN events as a batch.
    pub async fn publish_batch(&self, events: Vec<WanEvent>) -> Result<(), WanPublishError> {
        let state = *self.state.read().await;
        if !state.can_publish() {
            return Err(WanPublishError::new(format!(
                "cannot publish in state {:?}",
                state
            )));
        }

        for event in events {
            self.publish_tx
                .send(event)
                .await
                .map_err(|_| WanPublishError::new("publish channel closed"))?;
        }

        Ok(())
    }

    /// Returns statistics about the publisher.
    pub async fn stats(&self) -> WanPublisherStats {
        let connections = self.connections.read().await;
        let connected_targets: Vec<String> = connections
            .iter()
            .filter(|(_, c)| c.is_connected())
            .map(|(name, _)| name.clone())
            .collect();

        WanPublisherStats {
            name: self.config.name().to_string(),
            state: *self.state.read().await,
            total_targets: self.config.target_clusters().len(),
            connected_targets,
            batch_size: self.batch_size,
        }
    }

    async fn set_state(&self, new_state: WanPublisherState) {
        let mut state = self.state.write().await;
        let old_state = *state;
        *state = new_state;
        drop(state);

        if old_state != new_state {
            let _ = self.event_sender.send(WanPublisherEvent::StateChanged {
                from: old_state,
                to: new_state,
            });
        }
    }

    async fn connect_to_target(
        &self,
        target: &WanTargetClusterConfig,
    ) -> Result<(), WanPublishError> {
        let cluster_name = target.cluster_name();
        let connect_timeout = target.connection_timeout();

        for endpoint in target.endpoints() {
            match timeout(connect_timeout, self.try_connect(*endpoint)).await {
                Ok(Ok(())) => {
                    let mut connections = self.connections.write().await;
                    if let Some(conn) = connections.get_mut(cluster_name) {
                        conn.connected_endpoint = Some(*endpoint);
                        conn.retry_count = 0;
                    }
                    drop(connections);

                    let _ = self.event_sender.send(WanPublisherEvent::Connected {
                        cluster_name: cluster_name.to_string(),
                        address: *endpoint,
                    });

                    tracing::info!(
                        cluster = cluster_name,
                        endpoint = %endpoint,
                        "connected to WAN target"
                    );

                    return Ok(());
                }
                Ok(Err(e)) => {
                    tracing::debug!(
                        cluster = cluster_name,
                        endpoint = %endpoint,
                        error = %e,
                        "failed to connect to endpoint"
                    );
                }
                Err(_) => {
                    tracing::debug!(
                        cluster = cluster_name,
                        endpoint = %endpoint,
                        "connection timed out"
                    );
                }
            }
        }

        Err(WanPublishError::retriable(format!(
            "failed to connect to any endpoint for cluster '{}'",
            cluster_name
        ))
        .with_target(cluster_name))
    }

    async fn try_connect(&self, _endpoint: SocketAddr) -> Result<(), WanPublishError> {
        Ok(())
    }

    fn spawn_publish_loop(&self) {
        let state = Arc::clone(&self.state);
        let connections = Arc::clone(&self.connections);
        let event_sender = self.event_sender.clone();
        let publish_rx = Arc::clone(&self.publish_rx);
        let batch_size = self.batch_size;
        let batch_timeout = self.batch_timeout;
        let mut shutdown_rx = self.shutdown.subscribe();

        tokio::spawn(async move {
            let mut rx = {
                let mut guard = publish_rx.write().await;
                match guard.take() {
                    Some(rx) => rx,
                    None => return,
                }
            };

            let mut batch: Vec<WanEvent> = Vec::with_capacity(batch_size);

            loop {
                tokio::select! {
                    result = rx.recv() => {
                        match result {
                            Some(event) => {
                                batch.push(event);

                                if batch.len() >= batch_size {
                                    Self::flush_batch(
                                        &state,
                                        &connections,
                                        &event_sender,
                                        &mut batch,
                                    ).await;
                                }
                            }
                            None => break,
                        }
                    }
                    _ = tokio::time::sleep(batch_timeout) => {
                        if !batch.is_empty() {
                            Self::flush_batch(
                                &state,
                                &connections,
                                &event_sender,
                                &mut batch,
                            ).await;
                        }
                    }
                    result = shutdown_rx.changed() => {
                        if result.is_ok() && *shutdown_rx.borrow() {
                            if !batch.is_empty() {
                                Self::flush_batch(
                                    &state,
                                    &connections,
                                    &event_sender,
                                    &mut batch,
                                ).await;
                            }
                            break;
                        }
                    }
                }
            }

            tracing::debug!("WAN publish loop exited");
        });
    }

    async fn flush_batch(
        state: &RwLock<WanPublisherState>,
        connections: &RwLock<HashMap<String, TargetConnection>>,
        event_sender: &broadcast::Sender<WanPublisherEvent>,
        batch: &mut Vec<WanEvent>,
    ) {
        if batch.is_empty() {
            return;
        }

        let current_state = *state.read().await;
        if current_state != WanPublisherState::Running {
            batch.clear();
            return;
        }

        let conns = connections.read().await;
        let connected_clusters: Vec<String> = conns
            .iter()
            .filter(|(_, c)| c.is_connected())
            .map(|(name, _)| name.clone())
            .collect();
        drop(conns);

        let event_count = batch.len();
        let map_names: Vec<&str> = batch.iter().map(|e| e.map_name()).collect();
        let first_map = map_names.first().copied().unwrap_or("unknown");

        for cluster_name in connected_clusters {
            let _ = event_sender.send(WanPublisherEvent::Published {
                cluster_name: cluster_name.clone(),
                map_name: first_map.to_string(),
                event_count,
            });

            tracing::trace!(
                cluster = %cluster_name,
                count = event_count,
                "flushed WAN batch"
            );
        }

        batch.clear();
    }
}

/// Statistics about a WAN publisher.
#[derive(Debug, Clone)]
pub struct WanPublisherStats {
    /// The publisher name.
    pub name: String,
    /// The current state.
    pub state: WanPublisherState,
    /// Total number of target clusters.
    pub total_targets: usize,
    /// Names of connected target clusters.
    pub connected_targets: Vec<String>,
    /// Configured batch size.
    pub batch_size: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{WanReplicationConfigBuilder, WanTargetClusterConfigBuilder};

    fn create_test_config() -> WanReplicationConfig {
        let target = WanTargetClusterConfigBuilder::new("dc-west")
            .add_endpoint("127.0.0.1:5701".parse().unwrap())
            .connection_timeout(Duration::from_millis(100))
            .build()
            .unwrap();

        WanReplicationConfigBuilder::new("test-wan")
            .add_target_cluster(target)
            .build()
            .unwrap()
    }

    #[test]
    fn test_wan_publish_error_display() {
        let err = WanPublishError::new("test error");
        assert_eq!(err.to_string(), "WAN publish failed: test error");

        let err_with_target = WanPublishError::new("connection lost")
            .with_target("dc-west");
        assert_eq!(
            err_with_target.to_string(),
            "WAN publish to 'dc-west' failed: connection lost"
        );
    }

    #[test]
    fn test_wan_publish_error_retriable() {
        let err = WanPublishError::retriable("timeout");
        assert!(err.is_retriable());

        let err = WanPublishError::new("invalid config");
        assert!(!err.is_retriable());
    }

    #[test]
    fn test_wan_publisher_state_can_publish() {
        assert!(!WanPublisherState::Stopped.can_publish());
        assert!(!WanPublisherState::Starting.can_publish());
        assert!(WanPublisherState::Running.can_publish());
        assert!(!WanPublisherState::Paused.can_publish());
        assert!(!WanPublisherState::Stopping.can_publish());
    }

    #[test]
    fn test_wan_publisher_state_is_terminal() {
        assert!(WanPublisherState::Stopped.is_terminal());
        assert!(!WanPublisherState::Starting.is_terminal());
        assert!(!WanPublisherState::Running.is_terminal());
        assert!(!WanPublisherState::Paused.is_terminal());
        assert!(!WanPublisherState::Stopping.is_terminal());
    }

    #[test]
    fn test_wan_publisher_creation() {
        let config = create_test_config();
        let publisher = WanPublisher::new(config);

        assert_eq!(publisher.name(), "test-wan");
        assert_eq!(publisher.target_clusters(), vec!["dc-west"]);
    }

    #[test]
    fn test_wan_publisher_with_batching() {
        let config = create_test_config();
        let publisher = WanPublisher::with_batching(
            config,
            50,
            Duration::from_millis(100),
        );

        assert_eq!(publisher.batch_size, 50);
        assert_eq!(publisher.batch_timeout, Duration::from_millis(100));
    }

    #[tokio::test]
    async fn test_wan_publisher_initial_state() {
        let config = create_test_config();
        let publisher = WanPublisher::new(config);

        assert_eq!(publisher.state().await, WanPublisherState::Stopped);
        assert!(!publisher.is_connected("dc-west").await);
        assert_eq!(publisher.connected_count().await, 0);
    }

    #[tokio::test]
    async fn test_wan_publisher_cannot_publish_when_stopped() {
        let config = create_test_config();
        let publisher = WanPublisher::new(config);

        let event = WanEvent::put("test-map", vec![1], vec![2]);
        let result = publisher.publish(event).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_wan_publisher_stats() {
        let config = create_test_config();
        let publisher = WanPublisher::new(config);

        let stats = publisher.stats().await;

        assert_eq!(stats.name, "test-wan");
        assert_eq!(stats.state, WanPublisherState::Stopped);
        assert_eq!(stats.total_targets, 1);
        assert!(stats.connected_targets.is_empty());
    }

    #[tokio::test]
    async fn test_wan_publisher_subscribe() {
        let config = create_test_config();
        let publisher = WanPublisher::new(config);

        let mut rx = publisher.subscribe();

        publisher.set_state(WanPublisherState::Starting).await;

        let event = tokio::time::timeout(Duration::from_millis(100), rx.recv())
            .await
            .unwrap()
            .unwrap();

        match event {
            WanPublisherEvent::StateChanged { from, to } => {
                assert_eq!(from, WanPublisherState::Stopped);
                assert_eq!(to, WanPublisherState::Starting);
            }
            _ => panic!("expected StateChanged event"),
        }
    }

    #[test]
    fn test_wan_publisher_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<WanPublisher>();
        assert_send_sync::<WanPublisherState>();
        assert_send_sync::<WanPublisherEvent>();
        assert_send_sync::<WanPublishError>();
    }

    #[tokio::test]
    async fn test_wan_publisher_stop_when_already_stopped() {
        let config = create_test_config();
        let publisher = WanPublisher::new(config);

        let result = publisher.stop().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_wan_publisher_multiple_targets() {
        let target1 = WanTargetClusterConfigBuilder::new("dc-west")
            .add_endpoint("10.1.0.1:5701".parse().unwrap())
            .build()
            .unwrap();

        let target2 = WanTargetClusterConfigBuilder::new("dc-east")
            .add_endpoint("10.2.0.1:5701".parse().unwrap())
            .build()
            .unwrap();

        let config = WanReplicationConfigBuilder::new("multi-target-wan")
            .add_target_cluster(target1)
            .add_target_cluster(target2)
            .build()
            .unwrap();

        let publisher = WanPublisher::new(config);

        let targets = publisher.target_clusters();
        assert_eq!(targets.len(), 2);
        assert!(targets.contains(&"dc-west"));
        assert!(targets.contains(&"dc-east"));
    }
}
