//! Client lifecycle event handling.

use std::fmt;

/// Listener for client state transitions.
///
/// Implement this trait to receive notifications when the client's
/// connection state changes. All methods have default empty implementations,
/// so you only need to override the ones you're interested in.
///
/// # Example
///
/// ```ignore
/// use hazelcast_client::listener::ClientStateListener;
///
/// struct MyStateListener;
///
/// impl ClientStateListener for MyStateListener {
///     fn client_connected(&self) {
///         println!("Connected to cluster!");
///     }
///
///     fn client_disconnected(&self) {
///         println!("Disconnected from cluster!");
///     }
/// }
///
/// let listener_id = client.add_client_state_listener(MyStateListener).await?;
/// ```
#[allow(dead_code)]
pub trait ClientStateListener: Send + Sync {
    /// Called when the client has connected to the cluster.
    fn client_connected(&self) {}

    /// Called when the client has disconnected from the cluster.
    fn client_disconnected(&self) {}

    /// Called when the client has started and is ready to accept operations.
    fn client_started(&self) {}

    /// Called when the client has completed shutdown.
    fn client_shutdown(&self) {}
}

impl std::fmt::Debug for dyn ClientStateListener {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ClientStateListener")
    }
}

/// Events emitted during client lifecycle transitions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LifecycleEvent {
    /// The client is starting and initializing resources.
    Starting,
    /// The client has started and is ready to accept operations.
    Started,
    /// The client is beginning the shutdown process.
    ShuttingDown,
    /// The client has completed shutdown.
    Shutdown,
    /// The client has connected to the cluster.
    ClientConnected,
    /// The client has disconnected from the cluster.
    ClientDisconnected,
    /// The client has changed cluster due to failover.
    ClientChangedCluster,
}

impl LifecycleEvent {
    /// Returns a human-readable name for this event.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Starting => "STARTING",
            Self::Started => "STARTED",
            Self::ShuttingDown => "SHUTTING_DOWN",
            Self::Shutdown => "SHUTDOWN",
            Self::ClientConnected => "CLIENT_CONNECTED",
            Self::ClientDisconnected => "CLIENT_DISCONNECTED",
            Self::ClientChangedCluster => "CLIENT_CHANGED_CLUSTER",
        }
    }
}

impl fmt::Display for LifecycleEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lifecycle_event_names() {
        assert_eq!(LifecycleEvent::Starting.name(), "STARTING");
        assert_eq!(LifecycleEvent::Started.name(), "STARTED");
        assert_eq!(LifecycleEvent::ShuttingDown.name(), "SHUTTING_DOWN");
        assert_eq!(LifecycleEvent::Shutdown.name(), "SHUTDOWN");
        assert_eq!(LifecycleEvent::ClientConnected.name(), "CLIENT_CONNECTED");
        assert_eq!(LifecycleEvent::ClientDisconnected.name(), "CLIENT_DISCONNECTED");
        assert_eq!(LifecycleEvent::ClientChangedCluster.name(), "CLIENT_CHANGED_CLUSTER");
    }

    #[test]
    fn test_lifecycle_event_display() {
        assert_eq!(LifecycleEvent::Starting.to_string(), "STARTING");
        assert_eq!(LifecycleEvent::Started.to_string(), "STARTED");
        assert_eq!(LifecycleEvent::ShuttingDown.to_string(), "SHUTTING_DOWN");
        assert_eq!(LifecycleEvent::Shutdown.to_string(), "SHUTDOWN");
        assert_eq!(LifecycleEvent::ClientConnected.to_string(), "CLIENT_CONNECTED");
        assert_eq!(LifecycleEvent::ClientDisconnected.to_string(), "CLIENT_DISCONNECTED");
        assert_eq!(LifecycleEvent::ClientChangedCluster.to_string(), "CLIENT_CHANGED_CLUSTER");
    }

    #[test]
    fn test_lifecycle_event_equality() {
        assert_eq!(LifecycleEvent::Starting, LifecycleEvent::Starting);
        assert_ne!(LifecycleEvent::Starting, LifecycleEvent::Started);
    }

    #[test]
    fn test_lifecycle_event_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<LifecycleEvent>();
    }

    #[test]
    fn test_lifecycle_event_is_copy() {
        fn assert_copy<T: Copy>() {}
        assert_copy::<LifecycleEvent>();
    }

    #[test]
    fn test_lifecycle_event_debug() {
        let event = LifecycleEvent::Starting;
        let debug_str = format!("{:?}", event);
        assert!(debug_str.contains("Starting"));
    }

    #[test]
    fn test_client_state_listener_trait_object_is_send_sync() {
        fn assert_send_sync<T: Send + Sync + ?Sized>() {}
        assert_send_sync::<dyn ClientStateListener>();
    }

    #[test]
    fn test_client_state_listener_default_implementations() {
        struct EmptyListener;
        impl ClientStateListener for EmptyListener {}

        let listener = EmptyListener;
        listener.client_connected();
        listener.client_disconnected();
        listener.client_started();
        listener.client_shutdown();
    }
}
