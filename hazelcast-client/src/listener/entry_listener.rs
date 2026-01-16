//! Entry listener trait for map data structures.

use std::sync::Arc;

use crate::listener::{EntryEvent, EntryEventType};

/// A listener for entry-level events on maps.
///
/// Implement this trait to receive notifications when entries are
/// added, removed, updated, evicted, or expired in a map.
///
/// # Type Parameters
///
/// - `K`: The key type of the map
/// - `V`: The value type of the map
///
/// # Example
///
/// ```ignore
/// struct MyListener;
///
/// impl EntryListener<String, i32> for MyListener {
///     fn entry_added(&self, event: EntryEvent<String, i32>) {
///         println!("Added: {} -> {:?}", event.key, event.new_value);
///     }
///
///     fn entry_removed(&self, event: EntryEvent<String, i32>) {
///         println!("Removed: {}", event.key);
///     }
/// }
/// ```
pub trait EntryListener<K, V>: Send + Sync {
    /// Called when a new entry is added to the map.
    fn entry_added(&self, event: EntryEvent<K, V>) {
        let _ = event;
    }

    /// Called when an entry is removed from the map.
    fn entry_removed(&self, event: EntryEvent<K, V>) {
        let _ = event;
    }

    /// Called when an existing entry is updated in the map.
    fn entry_updated(&self, event: EntryEvent<K, V>) {
        let _ = event;
    }

    /// Called when an entry is evicted from the map due to size constraints.
    fn entry_evicted(&self, event: EntryEvent<K, V>) {
        let _ = event;
    }

    /// Called when an entry expires and is removed from the map.
    fn entry_expired(&self, event: EntryEvent<K, V>) {
        let _ = event;
    }
}

/// A boxed entry listener for type-erased storage.
pub type BoxedEntryListener<K, V> = Arc<dyn EntryListener<K, V>>;

/// An entry listener implementation using closures.
///
/// Use [`FnEntryListener::builder`] to create a new instance.
pub struct FnEntryListener<K, V> {
    on_added: Option<Box<dyn Fn(EntryEvent<K, V>) + Send + Sync>>,
    on_removed: Option<Box<dyn Fn(EntryEvent<K, V>) + Send + Sync>>,
    on_updated: Option<Box<dyn Fn(EntryEvent<K, V>) + Send + Sync>>,
    on_evicted: Option<Box<dyn Fn(EntryEvent<K, V>) + Send + Sync>>,
    on_expired: Option<Box<dyn Fn(EntryEvent<K, V>) + Send + Sync>>,
}

impl<K, V> FnEntryListener<K, V> {
    /// Creates a new builder for constructing an `FnEntryListener`.
    pub fn builder() -> FnEntryListenerBuilder<K, V> {
        FnEntryListenerBuilder::new()
    }
}

impl<K, V> EntryListener<K, V> for FnEntryListener<K, V>
where
    K: Send + Sync,
    V: Send + Sync,
{
    fn entry_added(&self, event: EntryEvent<K, V>) {
        if let Some(ref f) = self.on_added {
            f(event);
        }
    }

    fn entry_removed(&self, event: EntryEvent<K, V>) {
        if let Some(ref f) = self.on_removed {
            f(event);
        }
    }

    fn entry_updated(&self, event: EntryEvent<K, V>) {
        if let Some(ref f) = self.on_updated {
            f(event);
        }
    }

    fn entry_evicted(&self, event: EntryEvent<K, V>) {
        if let Some(ref f) = self.on_evicted {
            f(event);
        }
    }

    fn entry_expired(&self, event: EntryEvent<K, V>) {
        if let Some(ref f) = self.on_expired {
            f(event);
        }
    }
}

impl<K, V> std::fmt::Debug for FnEntryListener<K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FnEntryListener")
            .field("on_added", &self.on_added.is_some())
            .field("on_removed", &self.on_removed.is_some())
            .field("on_updated", &self.on_updated.is_some())
            .field("on_evicted", &self.on_evicted.is_some())
            .field("on_expired", &self.on_expired.is_some())
            .finish()
    }
}

/// Builder for creating [`FnEntryListener`] instances.
pub struct FnEntryListenerBuilder<K, V> {
    on_added: Option<Box<dyn Fn(EntryEvent<K, V>) + Send + Sync>>,
    on_removed: Option<Box<dyn Fn(EntryEvent<K, V>) + Send + Sync>>,
    on_updated: Option<Box<dyn Fn(EntryEvent<K, V>) + Send + Sync>>,
    on_evicted: Option<Box<dyn Fn(EntryEvent<K, V>) + Send + Sync>>,
    on_expired: Option<Box<dyn Fn(EntryEvent<K, V>) + Send + Sync>>,
}

impl<K, V> FnEntryListenerBuilder<K, V> {
    fn new() -> Self {
        Self {
            on_added: None,
            on_removed: None,
            on_updated: None,
            on_evicted: None,
            on_expired: None,
        }
    }

    /// Sets the handler for entry added events.
    pub fn on_added<F>(mut self, f: F) -> Self
    where
        F: Fn(EntryEvent<K, V>) + Send + Sync + 'static,
    {
        self.on_added = Some(Box::new(f));
        self
    }

    /// Sets the handler for entry removed events.
    pub fn on_removed<F>(mut self, f: F) -> Self
    where
        F: Fn(EntryEvent<K, V>) + Send + Sync + 'static,
    {
        self.on_removed = Some(Box::new(f));
        self
    }

    /// Sets the handler for entry updated events.
    pub fn on_updated<F>(mut self, f: F) -> Self
    where
        F: Fn(EntryEvent<K, V>) + Send + Sync + 'static,
    {
        self.on_updated = Some(Box::new(f));
        self
    }

    /// Sets the handler for entry evicted events.
    pub fn on_evicted<F>(mut self, f: F) -> Self
    where
        F: Fn(EntryEvent<K, V>) + Send + Sync + 'static,
    {
        self.on_evicted = Some(Box::new(f));
        self
    }

    /// Sets the handler for entry expired events.
    pub fn on_expired<F>(mut self, f: F) -> Self
    where
        F: Fn(EntryEvent<K, V>) + Send + Sync + 'static,
    {
        self.on_expired = Some(Box::new(f));
        self
    }

    /// Builds the [`FnEntryListener`].
    pub fn build(self) -> FnEntryListener<K, V> {
        FnEntryListener {
            on_added: self.on_added,
            on_removed: self.on_removed,
            on_updated: self.on_updated,
            on_evicted: self.on_evicted,
            on_expired: self.on_expired,
        }
    }
}

impl<K, V> Default for FnEntryListenerBuilder<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> std::fmt::Debug for FnEntryListenerBuilder<K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FnEntryListenerBuilder").finish()
    }
}

/// Dispatches an entry event to the appropriate method on an entry listener.
pub fn dispatch_entry_event<K, V>(listener: &dyn EntryListener<K, V>, event: EntryEvent<K, V>) {
    match event.event_type {
        EntryEventType::Added => listener.entry_added(event),
        EntryEventType::Removed => listener.entry_removed(event),
        EntryEventType::Updated => listener.entry_updated(event),
        EntryEventType::Evicted => listener.entry_evicted(event),
        EntryEventType::Expired => listener.entry_expired(event),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn test_entry_listener_trait_is_object_safe() {
        fn assert_object_safe<K: Send + Sync, V: Send + Sync>(_: &dyn EntryListener<K, V>) {}

        struct TestListener;
        impl EntryListener<String, i32> for TestListener {}

        let listener = TestListener;
        assert_object_safe::<String, i32>(&listener);
    }

    #[test]
    fn test_fn_entry_listener_builder() {
        let listener: FnEntryListener<String, i32> = FnEntryListener::builder()
            .on_added(|_| {})
            .on_removed(|_| {})
            .build();

        let event = EntryEvent::new(
            "key".to_string(),
            None,
            Some(42),
            EntryEventType::Added,
            Uuid::new_v4(),
            0,
        );

        listener.entry_added(event);
    }

    #[test]
    fn test_fn_entry_listener_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<FnEntryListener<String, String>>();
    }

    #[test]
    fn test_default_implementations_do_nothing() {
        struct EmptyListener;
        impl EntryListener<String, i32> for EmptyListener {}

        let listener = EmptyListener;
        let event = EntryEvent::new(
            "key".to_string(),
            None,
            Some(42),
            EntryEventType::Added,
            Uuid::new_v4(),
            0,
        );

        listener.entry_added(event.clone());
        listener.entry_removed(event.clone());
        listener.entry_updated(event.clone());
        listener.entry_evicted(event.clone());
        listener.entry_expired(event);
    }

    #[test]
    fn test_dispatch_entry_event() {
        use std::sync::atomic::{AtomicU32, Ordering};

        struct CountingListener {
            added: AtomicU32,
            removed: AtomicU32,
            updated: AtomicU32,
            evicted: AtomicU32,
            expired: AtomicU32,
        }

        impl EntryListener<String, i32> for CountingListener {
            fn entry_added(&self, _: EntryEvent<String, i32>) {
                self.added.fetch_add(1, Ordering::Relaxed);
            }
            fn entry_removed(&self, _: EntryEvent<String, i32>) {
                self.removed.fetch_add(1, Ordering::Relaxed);
            }
            fn entry_updated(&self, _: EntryEvent<String, i32>) {
                self.updated.fetch_add(1, Ordering::Relaxed);
            }
            fn entry_evicted(&self, _: EntryEvent<String, i32>) {
                self.evicted.fetch_add(1, Ordering::Relaxed);
            }
            fn entry_expired(&self, _: EntryEvent<String, i32>) {
                self.expired.fetch_add(1, Ordering::Relaxed);
            }
        }

        let listener = CountingListener {
            added: AtomicU32::new(0),
            removed: AtomicU32::new(0),
            updated: AtomicU32::new(0),
            evicted: AtomicU32::new(0),
            expired: AtomicU32::new(0),
        };

        let make_event = |event_type| {
            EntryEvent::new("key".to_string(), None, Some(1), event_type, Uuid::new_v4(), 0)
        };

        dispatch_entry_event(&listener, make_event(EntryEventType::Added));
        dispatch_entry_event(&listener, make_event(EntryEventType::Removed));
        dispatch_entry_event(&listener, make_event(EntryEventType::Updated));
        dispatch_entry_event(&listener, make_event(EntryEventType::Evicted));
        dispatch_entry_event(&listener, make_event(EntryEventType::Expired));

        assert_eq!(listener.added.load(Ordering::Relaxed), 1);
        assert_eq!(listener.removed.load(Ordering::Relaxed), 1);
        assert_eq!(listener.updated.load(Ordering::Relaxed), 1);
        assert_eq!(listener.evicted.load(Ordering::Relaxed), 1);
        assert_eq!(listener.expired.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_fn_entry_listener_debug() {
        let listener: FnEntryListener<String, i32> = FnEntryListener::builder()
            .on_added(|_| {})
            .build();

        let debug_str = format!("{:?}", listener);
        assert!(debug_str.contains("FnEntryListener"));
        assert!(debug_str.contains("on_added: true"));
        assert!(debug_str.contains("on_removed: false"));
    }

    #[test]
    fn test_boxed_entry_listener_type() {
        struct TestListener;
        impl EntryListener<String, i32> for TestListener {}

        let _boxed: BoxedEntryListener<String, i32> = Arc::new(TestListener);
    }
}
