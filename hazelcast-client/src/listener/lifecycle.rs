//! Client lifecycle event handling.

use std::fmt;

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
    }

    #[test]
    fn test_lifecycle_event_display() {
        assert_eq!(LifecycleEvent::Starting.to_string(), "STARTING");
        assert_eq!(LifecycleEvent::Started.to_string(), "STARTED");
        assert_eq!(LifecycleEvent::ShuttingDown.to_string(), "SHUTTING_DOWN");
        assert_eq!(LifecycleEvent::Shutdown.to_string(), "SHUTDOWN");
        assert_eq!(LifecycleEvent::ClientConnected.to_string(), "CLIENT_CONNECTED");
        assert_eq!(LifecycleEvent::ClientDisconnected.to_string(), "CLIENT_DISCONNECTED");
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
}
