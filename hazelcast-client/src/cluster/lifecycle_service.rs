//! Client lifecycle management service.

use std::sync::Arc;

use tokio::sync::broadcast;
use uuid::Uuid;

use hazelcast_core::Result;

use crate::connection::ConnectionManager;
use crate::listener::LifecycleEvent;

/// Registration handle for a lifecycle event listener.
///
/// The listener will receive events until this registration is dropped
/// or explicitly removed via `LifecycleService::remove_lifecycle_listener()`.
#[derive(Debug)]
pub struct LifecycleListenerRegistration {
    id: Uuid,
    receiver: broadcast::Receiver<LifecycleEvent>,
}

impl LifecycleListenerRegistration {
    /// Returns the unique identifier for this registration.
    pub fn id(&self) -> Uuid {
        self.id
    }

    /// Returns a mutable reference to the event receiver.
    pub fn receiver(&mut self) -> &mut broadcast::Receiver<LifecycleEvent> {
        &mut self.receiver
    }

    /// Receives the next lifecycle event.
    ///
    /// Returns the event or an error if the channel is closed or lagged.
    pub async fn recv(&mut self) -> std::result::Result<LifecycleEvent, broadcast::error::RecvError> {
        self.receiver.recv().await
    }
}

/// Service for managing client lifecycle and listening to lifecycle events.
///
/// The lifecycle service provides methods to:
/// - Check if the client is running
/// - Gracefully shut down or forcefully terminate the client
/// - Register and unregister lifecycle event listeners
///
/// # Example
///
/// ```ignore
/// let lifecycle = client.lifecycle();
///
/// // Check if client is running
/// if lifecycle.is_running().await {
///     println!("Client is running");
/// }
///
/// // Add a lifecycle listener
/// let mut registration = lifecycle.add_lifecycle_listener();
/// tokio::spawn(async move {
///     while let Ok(event) = registration.recv().await {
///         println!("Lifecycle event: {}", event);
///     }
/// });
///
/// // Graceful shutdown
/// lifecycle.shutdown().await?;
/// ```
#[derive(Debug, Clone)]
pub struct LifecycleService {
    connection_manager: Arc<ConnectionManager>,
}

impl LifecycleService {
    /// Creates a new lifecycle service.
    pub fn new(connection_manager: Arc<ConnectionManager>) -> Self {
        Self { connection_manager }
    }

    /// Returns `true` if the client is currently running.
    ///
    /// The client is considered running if it has been started and
    /// shutdown has not been initiated.
    pub async fn is_running(&self) -> bool {
        !self.connection_manager.is_shutdown_requested()
            && self.connection_manager.connection_count().await > 0
    }

    /// Initiates a graceful shutdown of the client.
    ///
    /// This method:
    /// 1. Emits `ShuttingDown` lifecycle event
    /// 2. Closes all connections gracefully
    /// 3. Emits `ClientDisconnected` lifecycle event
    /// 4. Emits `Shutdown` lifecycle event
    ///
    /// After shutdown completes, `is_running()` will return `false`.
    pub async fn shutdown(&self) -> Result<()> {
        self.connection_manager.shutdown().await
    }

    /// Forcefully terminates the client immediately.
    ///
    /// Unlike `shutdown()`, this method attempts to close connections
    /// without waiting for pending operations to complete.
    ///
    /// Note: In the current implementation, this behaves the same as
    /// `shutdown()`. Future versions may implement more aggressive
    /// termination semantics.
    pub async fn terminate(&self) -> Result<()> {
        self.connection_manager.shutdown().await
    }

    /// Adds a lifecycle event listener.
    ///
    /// Returns a registration handle that receives lifecycle events.
    /// The listener will continue receiving events until the registration
    /// is dropped.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let mut registration = lifecycle.add_lifecycle_listener();
    /// while let Ok(event) = registration.recv().await {
    ///     match event {
    ///         LifecycleEvent::ShuttingDown => println!("Client shutting down"),
    ///         LifecycleEvent::Shutdown => break,
    ///         _ => {}
    ///     }
    /// }
    /// ```
    pub fn add_lifecycle_listener(&self) -> LifecycleListenerRegistration {
        LifecycleListenerRegistration {
            id: Uuid::new_v4(),
            receiver: self.connection_manager.subscribe_lifecycle(),
        }
    }

    /// Removes a lifecycle listener by its registration ID.
    ///
    /// Returns `true` if the removal was acknowledged.
    ///
    /// Note: With the broadcast-based implementation, listeners are
    /// automatically removed when their registration is dropped. This method
    /// is provided for API compatibility. To stop receiving events,
    /// simply drop the `LifecycleListenerRegistration`.
    pub fn remove_lifecycle_listener(&self, _registration_id: Uuid) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ClientConfigBuilder;
    use crate::connection::discovery::StaticAddressDiscovery;
    use std::time::Duration;
    use tokio::net::TcpListener;

    async fn create_mock_server() -> (TcpListener, std::net::SocketAddr) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        (listener, addr)
    }

    #[test]
    fn test_lifecycle_service_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<LifecycleService>();
    }

    #[test]
    fn test_lifecycle_listener_registration_is_send() {
        fn assert_send<T: Send>() {}
        assert_send::<LifecycleListenerRegistration>();
    }

    #[tokio::test]
    async fn test_is_running_false_when_no_connections() {
        let config = ClientConfigBuilder::new().build().unwrap();
        let discovery = StaticAddressDiscovery::default();
        let manager = Arc::new(ConnectionManager::new(config, discovery));

        let service = LifecycleService::new(manager);
        assert!(!service.is_running().await);
    }

    #[tokio::test]
    async fn test_is_running_true_with_connections() {
        let (listener, addr) = create_mock_server().await;

        tokio::spawn(async move {
            let _ = listener.accept().await;
        });

        let config = ClientConfigBuilder::new()
            .add_address(addr)
            .build()
            .unwrap();

        let manager = Arc::new(ConnectionManager::from_config(config));
        manager.start().await.unwrap();

        let service = LifecycleService::new(manager);
        assert!(service.is_running().await);
    }

    #[tokio::test]
    async fn test_is_running_false_after_shutdown() {
        let (listener, addr) = create_mock_server().await;

        tokio::spawn(async move {
            let _ = listener.accept().await;
        });

        let config = ClientConfigBuilder::new()
            .add_address(addr)
            .build()
            .unwrap();

        let manager = Arc::new(ConnectionManager::from_config(config));
        manager.start().await.unwrap();

        let service = LifecycleService::new(manager);
        assert!(service.is_running().await);

        service.shutdown().await.unwrap();
        assert!(!service.is_running().await);
    }

    #[tokio::test]
    async fn test_add_lifecycle_listener() {
        let config = ClientConfigBuilder::new().build().unwrap();
        let discovery = StaticAddressDiscovery::default();
        let manager = Arc::new(ConnectionManager::new(config, discovery));

        let service = LifecycleService::new(manager);
        let registration = service.add_lifecycle_listener();

        assert!(!registration.id().is_nil());
    }

    #[tokio::test]
    async fn test_lifecycle_listener_receives_events() {
        let (listener, addr) = create_mock_server().await;

        tokio::spawn(async move {
            let _ = listener.accept().await;
        });

        let config = ClientConfigBuilder::new()
            .add_address(addr)
            .build()
            .unwrap();

        let manager = Arc::new(ConnectionManager::from_config(config));
        let service = LifecycleService::new(Arc::clone(&manager));

        let mut registration = service.add_lifecycle_listener();

        manager.start().await.unwrap();

        let mut events = Vec::new();
        while let Ok(event) = tokio::time::timeout(
            Duration::from_millis(100),
            registration.recv(),
        ).await {
            if let Ok(e) = event {
                events.push(e);
            }
        }

        assert!(events.contains(&LifecycleEvent::Starting));
        assert!(events.contains(&LifecycleEvent::Started));
    }

    #[tokio::test]
    async fn test_remove_lifecycle_listener() {
        let config = ClientConfigBuilder::new().build().unwrap();
        let discovery = StaticAddressDiscovery::default();
        let manager = Arc::new(ConnectionManager::new(config, discovery));

        let service = LifecycleService::new(manager);
        let registration = service.add_lifecycle_listener();
        let id = registration.id();

        assert!(service.remove_lifecycle_listener(id));
    }

    #[tokio::test]
    async fn test_terminate() {
        let (listener, addr) = create_mock_server().await;

        tokio::spawn(async move {
            let _ = listener.accept().await;
        });

        let config = ClientConfigBuilder::new()
            .add_address(addr)
            .build()
            .unwrap();

        let manager = Arc::new(ConnectionManager::from_config(config));
        manager.start().await.unwrap();

        let service = LifecycleService::new(manager);
        assert!(service.is_running().await);

        service.terminate().await.unwrap();
        assert!(!service.is_running().await);
    }

    #[tokio::test]
    async fn test_multiple_listeners() {
        let config = ClientConfigBuilder::new().build().unwrap();
        let discovery = StaticAddressDiscovery::default();
        let manager = Arc::new(ConnectionManager::new(config, discovery));

        let service = LifecycleService::new(manager);

        let reg1 = service.add_lifecycle_listener();
        let reg2 = service.add_lifecycle_listener();
        let reg3 = service.add_lifecycle_listener();

        assert_ne!(reg1.id(), reg2.id());
        assert_ne!(reg2.id(), reg3.id());
        assert_ne!(reg1.id(), reg3.id());
    }

    #[test]
    fn test_lifecycle_service_clone() {
        let config = ClientConfigBuilder::new().build().unwrap();
        let discovery = StaticAddressDiscovery::default();
        let manager = Arc::new(ConnectionManager::new(config, discovery));

        let service = LifecycleService::new(manager);
        let cloned = service.clone();

        assert!(std::ptr::eq(
            Arc::as_ptr(&service.connection_manager),
            Arc::as_ptr(&cloned.connection_manager)
        ));
    }
}
