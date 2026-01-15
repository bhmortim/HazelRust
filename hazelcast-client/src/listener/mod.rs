//! Event listener infrastructure for Hazelcast distributed data structures.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use hazelcast_core::Result;
use tokio::sync::watch;
use uuid::Uuid;

/// Unique identifier for a listener registration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ListenerId(Uuid);

impl ListenerId {
    /// Creates a new unique listener ID.
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// Creates a listener ID from a UUID.
    pub fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }

    /// Returns the underlying UUID.
    pub fn as_uuid(&self) -> Uuid {
        self.0
    }
}

impl Default for ListenerId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for ListenerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "listener-{}", self.0)
    }
}

/// A registration handle for an event listener.
///
/// When dropped, the listener is automatically deregistered if `auto_deregister`
/// was enabled during creation.
#[derive(Debug)]
pub struct ListenerRegistration {
    id: ListenerId,
    active: Arc<AtomicBool>,
    shutdown_tx: Option<watch::Sender<bool>>,
}

impl ListenerRegistration {
    /// Creates a new listener registration.
    pub fn new(id: ListenerId) -> Self {
        let (shutdown_tx, _) = watch::channel(false);
        Self {
            id,
            active: Arc::new(AtomicBool::new(true)),
            shutdown_tx: Some(shutdown_tx),
        }
    }

    /// Returns the listener ID.
    pub fn id(&self) -> ListenerId {
        self.id
    }

    /// Returns `true` if the listener is still active.
    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::Acquire)
    }

    /// Deactivates the listener registration.
    pub fn deactivate(&self) {
        self.active.store(false, Ordering::Release);
        if let Some(ref tx) = self.shutdown_tx {
            let _ = tx.send(true);
        }
    }

    /// Returns a receiver that signals when the listener should shut down.
    pub fn shutdown_receiver(&self) -> Option<watch::Receiver<bool>> {
        self.shutdown_tx.as_ref().map(|tx| tx.subscribe())
    }

    /// Creates a clone of the active flag for sharing with listener tasks.
    pub fn active_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.active)
    }
}

impl Drop for ListenerRegistration {
    fn drop(&mut self) {
        self.deactivate();
    }
}

/// Statistics for listener operations.
#[derive(Debug, Default)]
pub struct ListenerStats {
    messages_received: AtomicU64,
    errors: AtomicU64,
}

impl ListenerStats {
    /// Creates new listener statistics.
    pub fn new() -> Self {
        Self::default()
    }

    /// Increments the messages received counter.
    pub fn record_message(&self) {
        self.messages_received.fetch_add(1, Ordering::Relaxed);
    }

    /// Increments the error counter.
    pub fn record_error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Returns the number of messages received.
    pub fn messages_received(&self) -> u64 {
        self.messages_received.load(Ordering::Relaxed)
    }

    /// Returns the number of errors encountered.
    pub fn errors(&self) -> u64 {
        self.errors.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_listener_id_uniqueness() {
        let id1 = ListenerId::new();
        let id2 = ListenerId::new();
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_listener_id_display() {
        let id = ListenerId::new();
        let display = id.to_string();
        assert!(display.starts_with("listener-"));
    }

    #[test]
    fn test_listener_registration_active() {
        let reg = ListenerRegistration::new(ListenerId::new());
        assert!(reg.is_active());

        reg.deactivate();
        assert!(!reg.is_active());
    }

    #[test]
    fn test_listener_registration_drop_deactivates() {
        let id = ListenerId::new();
        let active_flag;
        {
            let reg = ListenerRegistration::new(id);
            active_flag = reg.active_flag();
            assert!(active_flag.load(Ordering::Acquire));
        }
        assert!(!active_flag.load(Ordering::Acquire));
    }

    #[test]
    fn test_listener_stats() {
        let stats = ListenerStats::new();
        assert_eq!(stats.messages_received(), 0);
        assert_eq!(stats.errors(), 0);

        stats.record_message();
        stats.record_message();
        stats.record_error();

        assert_eq!(stats.messages_received(), 2);
        assert_eq!(stats.errors(), 1);
    }

    #[test]
    fn test_listener_registration_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<ListenerRegistration>();
    }

    #[test]
    fn test_listener_id_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<ListenerId>();
    }
}
