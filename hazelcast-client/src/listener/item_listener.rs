//! Item event listener infrastructure for collection data structures.

use std::fmt;
use uuid::Uuid;

/// Type of item event fired by collection listeners.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i32)]
pub enum ItemEventType {
    /// Item was added to the collection.
    Added = 1,
    /// Item was removed from the collection.
    Removed = 2,
}

impl ItemEventType {
    /// Creates an event type from its wire format value.
    pub fn from_value(value: i32) -> Option<Self> {
        match value {
            1 => Some(Self::Added),
            2 => Some(Self::Removed),
            _ => None,
        }
    }

    /// Returns the wire format value for this event type.
    pub fn value(self) -> i32 {
        self as i32
    }
}

impl fmt::Display for ItemEventType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Added => write!(f, "ADDED"),
            Self::Removed => write!(f, "REMOVED"),
        }
    }
}

/// An event fired when an item is added to or removed from a collection.
#[derive(Debug, Clone)]
pub struct ItemEvent<T> {
    /// The item that was added or removed.
    pub item: T,
    /// The type of event that occurred.
    pub event_type: ItemEventType,
    /// UUID of the cluster member that originated the event.
    pub member_uuid: Uuid,
    /// Name of the collection that fired the event.
    pub name: String,
}

impl<T> ItemEvent<T> {
    /// Creates a new item event.
    pub fn new(item: T, event_type: ItemEventType, member_uuid: Uuid, name: String) -> Self {
        Self {
            item,
            event_type,
            member_uuid,
            name,
        }
    }
}

/// Trait for listening to item events on collections (IList, ISet, IQueue).
///
/// Implement this trait to receive notifications when items are added to
/// or removed from a collection.
pub trait ItemListener<T>: Send + Sync {
    /// Called when an item is added to the collection.
    fn item_added(&self, event: ItemEvent<T>);

    /// Called when an item is removed from the collection.
    fn item_removed(&self, event: ItemEvent<T>);
}

/// A boxed item listener for dynamic dispatch.
pub type BoxedItemListener<T> = Box<dyn ItemListener<T>>;

/// Configuration for item listeners specifying behavior options.
#[derive(Debug, Clone, Default)]
pub struct ItemListenerConfig {
    /// Whether to include the item value in events.
    pub include_value: bool,
}

impl ItemListenerConfig {
    /// Creates a new item listener config with default settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets whether to include item values in events.
    pub fn include_value(mut self, include: bool) -> Self {
        self.include_value = include;
        self
    }
}

/// Implementation of ItemListener using closures.
pub struct FnItemListener<T, F1, F2>
where
    F1: Fn(ItemEvent<T>) + Send + Sync,
    F2: Fn(ItemEvent<T>) + Send + Sync,
{
    on_added: F1,
    on_removed: F2,
    _marker: std::marker::PhantomData<T>,
}

impl<T, F1, F2> FnItemListener<T, F1, F2>
where
    F1: Fn(ItemEvent<T>) + Send + Sync,
    F2: Fn(ItemEvent<T>) + Send + Sync,
{
    /// Creates a new function-based item listener.
    pub fn new(on_added: F1, on_removed: F2) -> Self {
        Self {
            on_added,
            on_removed,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<T, F1, F2> ItemListener<T> for FnItemListener<T, F1, F2>
where
    T: Send + Sync,
    F1: Fn(ItemEvent<T>) + Send + Sync,
    F2: Fn(ItemEvent<T>) + Send + Sync,
{
    fn item_added(&self, event: ItemEvent<T>) {
        (self.on_added)(event);
    }

    fn item_removed(&self, event: ItemEvent<T>) {
        (self.on_removed)(event);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_item_event_type_values() {
        assert_eq!(ItemEventType::Added.value(), 1);
        assert_eq!(ItemEventType::Removed.value(), 2);
    }

    #[test]
    fn test_item_event_type_from_value() {
        assert_eq!(ItemEventType::from_value(1), Some(ItemEventType::Added));
        assert_eq!(ItemEventType::from_value(2), Some(ItemEventType::Removed));
        assert_eq!(ItemEventType::from_value(0), None);
        assert_eq!(ItemEventType::from_value(99), None);
    }

    #[test]
    fn test_item_event_type_display() {
        assert_eq!(ItemEventType::Added.to_string(), "ADDED");
        assert_eq!(ItemEventType::Removed.to_string(), "REMOVED");
    }

    #[test]
    fn test_item_event_creation() {
        let member = Uuid::new_v4();
        let event: ItemEvent<String> = ItemEvent::new(
            "test-item".to_string(),
            ItemEventType::Added,
            member,
            "test-list".to_string(),
        );

        assert_eq!(event.item, "test-item");
        assert_eq!(event.event_type, ItemEventType::Added);
        assert_eq!(event.member_uuid, member);
        assert_eq!(event.name, "test-list");
    }

    #[test]
    fn test_item_listener_config_default() {
        let config = ItemListenerConfig::new();
        assert!(!config.include_value);
    }

    #[test]
    fn test_item_listener_config_builder() {
        let config = ItemListenerConfig::new().include_value(true);
        assert!(config.include_value);
    }

    #[test]
    fn test_item_event_type_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<ItemEventType>();
    }

    #[test]
    fn test_item_listener_config_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<ItemListenerConfig>();
    }
}
