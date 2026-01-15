//! Event listener infrastructure for Hazelcast distributed data structures.

mod lifecycle;
mod membership;

pub use lifecycle::LifecycleEvent;
pub use membership::{Member, MemberEvent, MemberEventType, MembershipListener};

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use tokio::sync::watch;
use uuid::Uuid;

/// Type of entry event fired by map listeners.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i32)]
pub enum EntryEventType {
    /// Entry was added to the map.
    Added = 1,
    /// Entry was updated in the map.
    Updated = 2,
    /// Entry was removed from the map.
    Removed = 4,
    /// Entry was evicted from the map.
    Evicted = 8,
    /// Entry expired and was removed from the map.
    Expired = 16,
}

impl EntryEventType {
    /// Creates an event type from its wire format value.
    pub fn from_value(value: i32) -> Option<Self> {
        match value {
            1 => Some(Self::Added),
            2 => Some(Self::Updated),
            4 => Some(Self::Removed),
            8 => Some(Self::Evicted),
            16 => Some(Self::Expired),
            _ => None,
        }
    }

    /// Returns the wire format value for this event type.
    pub fn value(self) -> i32 {
        self as i32
    }
}

impl std::fmt::Display for EntryEventType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Added => write!(f, "ADDED"),
            Self::Updated => write!(f, "UPDATED"),
            Self::Removed => write!(f, "REMOVED"),
            Self::Evicted => write!(f, "EVICTED"),
            Self::Expired => write!(f, "EXPIRED"),
        }
    }
}

/// An event fired when a map entry is added, updated, removed, evicted, or expired.
#[derive(Debug, Clone)]
pub struct EntryEvent<K, V> {
    /// The key of the affected entry.
    pub key: K,
    /// The old value before the operation (None for ADDED events).
    pub old_value: Option<V>,
    /// The new value after the operation (None for REMOVED/EVICTED/EXPIRED events).
    pub new_value: Option<V>,
    /// The type of event that occurred.
    pub event_type: EntryEventType,
    /// UUID of the cluster member that originated the event.
    pub member_uuid: Uuid,
    /// Timestamp when the event occurred (milliseconds since epoch).
    pub timestamp: i64,
}

impl<K, V> EntryEvent<K, V> {
    /// Creates a new entry event.
    pub fn new(
        key: K,
        old_value: Option<V>,
        new_value: Option<V>,
        event_type: EntryEventType,
        member_uuid: Uuid,
        timestamp: i64,
    ) -> Self {
        Self {
            key,
            old_value,
            new_value,
            event_type,
            member_uuid,
            timestamp,
        }
    }
}

/// Configuration for entry listeners specifying which events to receive.
#[derive(Debug, Clone, Default)]
pub struct EntryListenerConfig {
    /// Whether to include entry values in events.
    pub include_value: bool,
    /// Whether to receive ADDED events.
    pub on_added: bool,
    /// Whether to receive UPDATED events.
    pub on_updated: bool,
    /// Whether to receive REMOVED events.
    pub on_removed: bool,
    /// Whether to receive EVICTED events.
    pub on_evicted: bool,
    /// Whether to receive EXPIRED events.
    pub on_expired: bool,
}

impl EntryListenerConfig {
    /// Creates a new entry listener config with all events disabled.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a config that listens to all event types.
    pub fn all() -> Self {
        Self {
            include_value: true,
            on_added: true,
            on_updated: true,
            on_removed: true,
            on_evicted: true,
            on_expired: true,
        }
    }

    /// Sets whether to include entry values in events.
    pub fn include_value(mut self, include: bool) -> Self {
        self.include_value = include;
        self
    }

    /// Enables ADDED event notifications.
    pub fn on_added(mut self) -> Self {
        self.on_added = true;
        self
    }

    /// Enables UPDATED event notifications.
    pub fn on_updated(mut self) -> Self {
        self.on_updated = true;
        self
    }

    /// Enables REMOVED event notifications.
    pub fn on_removed(mut self) -> Self {
        self.on_removed = true;
        self
    }

    /// Enables EVICTED event notifications.
    pub fn on_evicted(mut self) -> Self {
        self.on_evicted = true;
        self
    }

    /// Enables EXPIRED event notifications.
    pub fn on_expired(mut self) -> Self {
        self.on_expired = true;
        self
    }

    /// Returns the event type flags as a bitmask for the wire protocol.
    pub fn event_flags(&self) -> i32 {
        let mut flags = 0i32;
        if self.on_added {
            flags |= EntryEventType::Added.value();
        }
        if self.on_updated {
            flags |= EntryEventType::Updated.value();
        }
        if self.on_removed {
            flags |= EntryEventType::Removed.value();
        }
        if self.on_evicted {
            flags |= EntryEventType::Evicted.value();
        }
        if self.on_expired {
            flags |= EntryEventType::Expired.value();
        }
        flags
    }

    /// Returns true if the config accepts the given event type.
    pub fn accepts(&self, event_type: EntryEventType) -> bool {
        match event_type {
            EntryEventType::Added => self.on_added,
            EntryEventType::Updated => self.on_updated,
            EntryEventType::Removed => self.on_removed,
            EntryEventType::Evicted => self.on_evicted,
            EntryEventType::Expired => self.on_expired,
        }
    }
}

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

    #[test]
    fn test_entry_event_type_values() {
        assert_eq!(EntryEventType::Added.value(), 1);
        assert_eq!(EntryEventType::Updated.value(), 2);
        assert_eq!(EntryEventType::Removed.value(), 4);
        assert_eq!(EntryEventType::Evicted.value(), 8);
        assert_eq!(EntryEventType::Expired.value(), 16);
    }

    #[test]
    fn test_entry_event_type_from_value() {
        assert_eq!(EntryEventType::from_value(1), Some(EntryEventType::Added));
        assert_eq!(EntryEventType::from_value(2), Some(EntryEventType::Updated));
        assert_eq!(EntryEventType::from_value(4), Some(EntryEventType::Removed));
        assert_eq!(EntryEventType::from_value(8), Some(EntryEventType::Evicted));
        assert_eq!(EntryEventType::from_value(16), Some(EntryEventType::Expired));
        assert_eq!(EntryEventType::from_value(0), None);
        assert_eq!(EntryEventType::from_value(99), None);
    }

    #[test]
    fn test_entry_event_type_display() {
        assert_eq!(EntryEventType::Added.to_string(), "ADDED");
        assert_eq!(EntryEventType::Updated.to_string(), "UPDATED");
        assert_eq!(EntryEventType::Removed.to_string(), "REMOVED");
        assert_eq!(EntryEventType::Evicted.to_string(), "EVICTED");
        assert_eq!(EntryEventType::Expired.to_string(), "EXPIRED");
    }

    #[test]
    fn test_entry_event_creation() {
        let member = Uuid::new_v4();
        let event: EntryEvent<String, i32> = EntryEvent::new(
            "key1".to_string(),
            None,
            Some(42),
            EntryEventType::Added,
            member,
            1234567890,
        );

        assert_eq!(event.key, "key1");
        assert_eq!(event.old_value, None);
        assert_eq!(event.new_value, Some(42));
        assert_eq!(event.event_type, EntryEventType::Added);
        assert_eq!(event.member_uuid, member);
        assert_eq!(event.timestamp, 1234567890);
    }

    #[test]
    fn test_entry_listener_config_default() {
        let config = EntryListenerConfig::new();
        assert!(!config.include_value);
        assert!(!config.on_added);
        assert!(!config.on_updated);
        assert!(!config.on_removed);
        assert!(!config.on_evicted);
        assert!(!config.on_expired);
        assert_eq!(config.event_flags(), 0);
    }

    #[test]
    fn test_entry_listener_config_all() {
        let config = EntryListenerConfig::all();
        assert!(config.include_value);
        assert!(config.on_added);
        assert!(config.on_updated);
        assert!(config.on_removed);
        assert!(config.on_evicted);
        assert!(config.on_expired);
        assert_eq!(config.event_flags(), 1 | 2 | 4 | 8 | 16);
    }

    #[test]
    fn test_entry_listener_config_builder() {
        let config = EntryListenerConfig::new()
            .include_value(true)
            .on_added()
            .on_removed();

        assert!(config.include_value);
        assert!(config.on_added);
        assert!(!config.on_updated);
        assert!(config.on_removed);
        assert!(!config.on_evicted);
        assert!(!config.on_expired);
        assert_eq!(config.event_flags(), 1 | 4);
    }

    #[test]
    fn test_entry_listener_config_accepts() {
        let config = EntryListenerConfig::new()
            .on_added()
            .on_updated();

        assert!(config.accepts(EntryEventType::Added));
        assert!(config.accepts(EntryEventType::Updated));
        assert!(!config.accepts(EntryEventType::Removed));
        assert!(!config.accepts(EntryEventType::Evicted));
        assert!(!config.accepts(EntryEventType::Expired));
    }

    #[test]
    fn test_entry_event_type_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<EntryEventType>();
    }

    #[test]
    fn test_entry_listener_config_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<EntryListenerConfig>();
    }
}
