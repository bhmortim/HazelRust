//! WAN replication events for cross-cluster forwarding.

use std::time::SystemTime;

/// The type of WAN replication event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WanEventType {
    /// A put/update operation.
    Put,
    /// A remove operation.
    Remove,
    /// A merge operation from conflict resolution.
    Merge,
    /// An eviction event.
    Evict,
    /// A clear operation on the data structure.
    Clear,
}

impl WanEventType {
    /// Returns `true` if this event type requires a value payload.
    pub fn requires_value(&self) -> bool {
        matches!(self, WanEventType::Put | WanEventType::Merge)
    }

    /// Returns the string representation used in the protocol.
    pub fn as_protocol_str(&self) -> &'static str {
        match self {
            WanEventType::Put => "PUT",
            WanEventType::Remove => "REMOVE",
            WanEventType::Merge => "MERGE",
            WanEventType::Evict => "EVICT",
            WanEventType::Clear => "CLEAR",
        }
    }
}

/// A WAN replication event representing a data change to be forwarded.
#[derive(Debug, Clone)]
pub struct WanEvent {
    event_type: WanEventType,
    map_name: String,
    key: Vec<u8>,
    value: Option<Vec<u8>>,
    old_value: Option<Vec<u8>>,
    merge_policy: Option<String>,
    timestamp: SystemTime,
    source_cluster: Option<String>,
    partition_id: Option<i32>,
}

impl WanEvent {
    /// Creates a new WAN event.
    pub fn new(
        event_type: WanEventType,
        map_name: impl Into<String>,
        key: Vec<u8>,
        value: Option<Vec<u8>>,
    ) -> Self {
        Self {
            event_type,
            map_name: map_name.into(),
            key,
            value,
            old_value: None,
            merge_policy: None,
            timestamp: SystemTime::now(),
            source_cluster: None,
            partition_id: None,
        }
    }

    /// Creates a PUT event.
    pub fn put(map_name: impl Into<String>, key: Vec<u8>, value: Vec<u8>) -> Self {
        Self::new(WanEventType::Put, map_name, key, Some(value))
    }

    /// Creates a REMOVE event.
    pub fn remove(map_name: impl Into<String>, key: Vec<u8>) -> Self {
        Self::new(WanEventType::Remove, map_name, key, None)
    }

    /// Creates a MERGE event with the specified merge policy.
    pub fn merge(
        map_name: impl Into<String>,
        key: Vec<u8>,
        value: Vec<u8>,
        merge_policy: impl Into<String>,
    ) -> Self {
        let mut event = Self::new(WanEventType::Merge, map_name, key, Some(value));
        event.merge_policy = Some(merge_policy.into());
        event
    }

    /// Creates a CLEAR event for the entire map.
    pub fn clear(map_name: impl Into<String>) -> Self {
        Self::new(WanEventType::Clear, map_name, Vec::new(), None)
    }

    /// Returns the event type.
    pub fn event_type(&self) -> WanEventType {
        self.event_type
    }

    /// Returns the map name this event applies to.
    pub fn map_name(&self) -> &str {
        &self.map_name
    }

    /// Returns the key data.
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    /// Returns the value data, if present.
    pub fn value(&self) -> Option<&[u8]> {
        self.value.as_deref()
    }

    /// Returns the old value data, if present.
    pub fn old_value(&self) -> Option<&[u8]> {
        self.old_value.as_deref()
    }

    /// Returns the merge policy class name, if set.
    pub fn merge_policy(&self) -> Option<&str> {
        self.merge_policy.as_deref()
    }

    /// Returns the event timestamp.
    pub fn timestamp(&self) -> SystemTime {
        self.timestamp
    }

    /// Returns the source cluster name, if set.
    pub fn source_cluster(&self) -> Option<&str> {
        self.source_cluster.as_deref()
    }

    /// Returns the partition ID, if set.
    pub fn partition_id(&self) -> Option<i32> {
        self.partition_id
    }

    /// Sets the old value for update tracking.
    pub fn with_old_value(mut self, old_value: Vec<u8>) -> Self {
        self.old_value = Some(old_value);
        self
    }

    /// Sets the source cluster name.
    pub fn with_source_cluster(mut self, cluster: impl Into<String>) -> Self {
        self.source_cluster = Some(cluster.into());
        self
    }

    /// Sets the partition ID.
    pub fn with_partition_id(mut self, partition_id: i32) -> Self {
        self.partition_id = Some(partition_id);
        self
    }

    /// Sets a custom timestamp.
    pub fn with_timestamp(mut self, timestamp: SystemTime) -> Self {
        self.timestamp = timestamp;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wan_event_type_requires_value() {
        assert!(WanEventType::Put.requires_value());
        assert!(WanEventType::Merge.requires_value());
        assert!(!WanEventType::Remove.requires_value());
        assert!(!WanEventType::Evict.requires_value());
        assert!(!WanEventType::Clear.requires_value());
    }

    #[test]
    fn test_wan_event_type_protocol_str() {
        assert_eq!(WanEventType::Put.as_protocol_str(), "PUT");
        assert_eq!(WanEventType::Remove.as_protocol_str(), "REMOVE");
        assert_eq!(WanEventType::Merge.as_protocol_str(), "MERGE");
        assert_eq!(WanEventType::Evict.as_protocol_str(), "EVICT");
        assert_eq!(WanEventType::Clear.as_protocol_str(), "CLEAR");
    }

    #[test]
    fn test_wan_event_put() {
        let event = WanEvent::put("my-map", vec![1, 2, 3], vec![4, 5, 6]);

        assert_eq!(event.event_type(), WanEventType::Put);
        assert_eq!(event.map_name(), "my-map");
        assert_eq!(event.key(), &[1, 2, 3]);
        assert_eq!(event.value(), Some(&[4, 5, 6][..]));
        assert!(event.old_value().is_none());
    }

    #[test]
    fn test_wan_event_remove() {
        let event = WanEvent::remove("my-map", vec![1, 2, 3]);

        assert_eq!(event.event_type(), WanEventType::Remove);
        assert_eq!(event.map_name(), "my-map");
        assert_eq!(event.key(), &[1, 2, 3]);
        assert!(event.value().is_none());
    }

    #[test]
    fn test_wan_event_merge() {
        let event = WanEvent::merge(
            "my-map",
            vec![1, 2, 3],
            vec![4, 5, 6],
            "com.example.MyMergePolicy",
        );

        assert_eq!(event.event_type(), WanEventType::Merge);
        assert_eq!(event.merge_policy(), Some("com.example.MyMergePolicy"));
    }

    #[test]
    fn test_wan_event_clear() {
        let event = WanEvent::clear("my-map");

        assert_eq!(event.event_type(), WanEventType::Clear);
        assert_eq!(event.map_name(), "my-map");
        assert!(event.key().is_empty());
        assert!(event.value().is_none());
    }

    #[test]
    fn test_wan_event_with_old_value() {
        let event = WanEvent::put("my-map", vec![1], vec![2])
            .with_old_value(vec![3]);

        assert_eq!(event.old_value(), Some(&[3][..]));
    }

    #[test]
    fn test_wan_event_with_source_cluster() {
        let event = WanEvent::put("my-map", vec![1], vec![2])
            .with_source_cluster("dc-east");

        assert_eq!(event.source_cluster(), Some("dc-east"));
    }

    #[test]
    fn test_wan_event_with_partition_id() {
        let event = WanEvent::put("my-map", vec![1], vec![2])
            .with_partition_id(42);

        assert_eq!(event.partition_id(), Some(42));
    }

    #[test]
    fn test_wan_event_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<WanEvent>();
        assert_send_sync::<WanEventType>();
    }

    #[test]
    fn test_wan_event_type_is_copy() {
        fn assert_copy<T: Copy>() {}
        assert_copy::<WanEventType>();
    }
}
