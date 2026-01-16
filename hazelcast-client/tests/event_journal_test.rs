//! Integration tests for Event Journal functionality.

use std::sync::Arc;

use hazelcast_client::config::{
    ClientConfigBuilder, PermissionAction, Permissions, QuorumConfig, QuorumType,
};
use hazelcast_client::connection::ConnectionManager;
use hazelcast_client::proxy::{
    EventJournalConfig, EventJournalEventType, EventJournalMapEvent, IMap,
};
use hazelcast_core::HazelcastError;

fn create_test_manager() -> Arc<ConnectionManager> {
    let config = ClientConfigBuilder::new().build().unwrap();
    Arc::new(ConnectionManager::from_config(config))
}

fn create_manager_with_permissions(perms: Permissions) -> Arc<ConnectionManager> {
    let config = ClientConfigBuilder::new()
        .security(|s| s.permissions(perms))
        .build()
        .unwrap();
    Arc::new(ConnectionManager::from_config(config))
}

#[test]
fn test_event_journal_event_type_from_value() {
    assert_eq!(
        EventJournalEventType::from_value(1),
        Some(EventJournalEventType::Added)
    );
    assert_eq!(
        EventJournalEventType::from_value(2),
        Some(EventJournalEventType::Removed)
    );
    assert_eq!(
        EventJournalEventType::from_value(4),
        Some(EventJournalEventType::Updated)
    );
    assert_eq!(
        EventJournalEventType::from_value(8),
        Some(EventJournalEventType::Evicted)
    );
    assert_eq!(
        EventJournalEventType::from_value(16),
        Some(EventJournalEventType::Expired)
    );
    assert_eq!(
        EventJournalEventType::from_value(32),
        Some(EventJournalEventType::Loaded)
    );
    assert_eq!(EventJournalEventType::from_value(0), None);
    assert_eq!(EventJournalEventType::from_value(64), None);
    assert_eq!(EventJournalEventType::from_value(-1), None);
}

#[test]
fn test_event_journal_event_type_value() {
    assert_eq!(EventJournalEventType::Added.value(), 1);
    assert_eq!(EventJournalEventType::Removed.value(), 2);
    assert_eq!(EventJournalEventType::Updated.value(), 4);
    assert_eq!(EventJournalEventType::Evicted.value(), 8);
    assert_eq!(EventJournalEventType::Expired.value(), 16);
    assert_eq!(EventJournalEventType::Loaded.value(), 32);
}

#[test]
fn test_event_journal_event_type_roundtrip() {
    for event_type in [
        EventJournalEventType::Added,
        EventJournalEventType::Removed,
        EventJournalEventType::Updated,
        EventJournalEventType::Evicted,
        EventJournalEventType::Expired,
        EventJournalEventType::Loaded,
    ] {
        let value = event_type.value();
        let parsed = EventJournalEventType::from_value(value);
        assert_eq!(parsed, Some(event_type));
    }
}

#[test]
fn test_event_journal_event_type_equality() {
    assert_eq!(EventJournalEventType::Added, EventJournalEventType::Added);
    assert_ne!(EventJournalEventType::Added, EventJournalEventType::Removed);
}

#[test]
fn test_event_journal_event_type_is_copy() {
    let t1 = EventJournalEventType::Updated;
    let t2 = t1;
    assert_eq!(t1, t2);
}

#[test]
fn test_event_journal_map_event_accessors() {
    let event: EventJournalMapEvent<String, i32> = EventJournalMapEvent {
        event_type: EventJournalEventType::Updated,
        key: "test-key".to_string(),
        old_value: Some(10),
        new_value: Some(20),
        sequence: 42,
    };

    assert_eq!(event.event_type(), EventJournalEventType::Updated);
    assert_eq!(event.key(), "test-key");
    assert_eq!(event.old_value(), Some(&10));
    assert_eq!(event.new_value(), Some(&20));
    assert_eq!(event.sequence(), 42);
}

#[test]
fn test_event_journal_map_event_with_none_values() {
    let event: EventJournalMapEvent<String, i32> = EventJournalMapEvent {
        event_type: EventJournalEventType::Removed,
        key: "deleted-key".to_string(),
        old_value: Some(100),
        new_value: None,
        sequence: 5,
    };

    assert_eq!(event.event_type(), EventJournalEventType::Removed);
    assert!(event.old_value().is_some());
    assert!(event.new_value().is_none());
}

#[test]
fn test_event_journal_map_event_added() {
    let event: EventJournalMapEvent<String, String> = EventJournalMapEvent {
        event_type: EventJournalEventType::Added,
        key: "new-key".to_string(),
        old_value: None,
        new_value: Some("new-value".to_string()),
        sequence: 1,
    };

    assert_eq!(event.event_type(), EventJournalEventType::Added);
    assert!(event.old_value().is_none());
    assert_eq!(event.new_value(), Some(&"new-value".to_string()));
}

#[test]
fn test_event_journal_map_event_clone() {
    let event: EventJournalMapEvent<String, i64> = EventJournalMapEvent {
        event_type: EventJournalEventType::Evicted,
        key: "evicted-key".to_string(),
        old_value: Some(999),
        new_value: None,
        sequence: 100,
    };

    let cloned = event.clone();
    assert_eq!(cloned.event_type, event.event_type);
    assert_eq!(cloned.key, event.key);
    assert_eq!(cloned.old_value, event.old_value);
    assert_eq!(cloned.new_value, event.new_value);
    assert_eq!(cloned.sequence, event.sequence);
}

#[test]
fn test_event_journal_map_event_debug() {
    let event: EventJournalMapEvent<String, i32> = EventJournalMapEvent {
        event_type: EventJournalEventType::Loaded,
        key: "k".to_string(),
        old_value: None,
        new_value: Some(1),
        sequence: 0,
    };

    let debug_str = format!("{:?}", event);
    assert!(debug_str.contains("EventJournalMapEvent"));
    assert!(debug_str.contains("Loaded"));
}

#[test]
fn test_event_journal_config_default() {
    let config = EventJournalConfig::default();
    assert_eq!(config.start_sequence, -1);
    assert_eq!(config.min_size, 1);
    assert_eq!(config.max_size, 100);
}

#[test]
fn test_event_journal_config_new() {
    let config = EventJournalConfig::new();
    assert_eq!(config.start_sequence, -1);
    assert_eq!(config.min_size, 1);
    assert_eq!(config.max_size, 100);
}

#[test]
fn test_event_journal_config_builder() {
    let config = EventJournalConfig::new()
        .start_sequence(500)
        .min_size(10)
        .max_size(1000);

    assert_eq!(config.start_sequence, 500);
    assert_eq!(config.min_size, 10);
    assert_eq!(config.max_size, 1000);
}

#[test]
fn test_event_journal_config_chained_builder() {
    let config = EventJournalConfig::new()
        .start_sequence(0)
        .min_size(5)
        .max_size(50)
        .start_sequence(100);

    assert_eq!(config.start_sequence, 100);
    assert_eq!(config.min_size, 5);
    assert_eq!(config.max_size, 50);
}

#[test]
fn test_event_journal_config_negative_start_sequence() {
    let config = EventJournalConfig::new().start_sequence(-1);
    assert_eq!(config.start_sequence, -1);
}

#[test]
fn test_event_journal_config_clone() {
    let config = EventJournalConfig::new()
        .start_sequence(42)
        .min_size(5)
        .max_size(200);

    let cloned = config.clone();
    assert_eq!(cloned.start_sequence, config.start_sequence);
    assert_eq!(cloned.min_size, config.min_size);
    assert_eq!(cloned.max_size, config.max_size);
}

#[tokio::test]
async fn test_read_from_event_journal_permission_denied() {
    let mut perms = Permissions::new();
    perms.grant(PermissionAction::Put);

    let cm = create_manager_with_permissions(perms);
    let map: IMap<String, String> = IMap::new("test-map".to_string(), cm);

    let config = EventJournalConfig::new();
    let result = map.read_from_event_journal(0, config).await;
    assert!(matches!(result, Err(HazelcastError::Authorization(_))));
}

#[tokio::test]
async fn test_read_from_event_journal_with_read_permission() {
    let mut perms = Permissions::new();
    perms.grant(PermissionAction::Read);

    let cm = create_manager_with_permissions(perms);
    let map: IMap<String, String> = IMap::new("journal-map".to_string(), cm);

    let config = EventJournalConfig::new()
        .start_sequence(-1)
        .min_size(1)
        .max_size(10);

    let result = map.read_from_event_journal(0, config).await;
    assert!(!matches!(result, Err(HazelcastError::Authorization(_))));
}

#[test]
fn test_event_journal_stream_is_send() {
    use hazelcast_client::proxy::EventJournalStream;

    fn assert_send<T: Send>() {}
    assert_send::<EventJournalStream<String, String>>();
    assert_send::<EventJournalStream<i64, Vec<u8>>>();
}

#[test]
fn test_event_journal_event_types_cover_all_mutations() {
    let all_types = [
        EventJournalEventType::Added,
        EventJournalEventType::Removed,
        EventJournalEventType::Updated,
        EventJournalEventType::Evicted,
        EventJournalEventType::Expired,
        EventJournalEventType::Loaded,
    ];

    assert_eq!(all_types.len(), 6);

    for event_type in all_types {
        assert!(event_type.value() > 0);
        assert!(event_type.value() <= 32);
    }
}

#[test]
fn test_event_journal_event_type_is_power_of_two() {
    let types = [
        EventJournalEventType::Added,
        EventJournalEventType::Removed,
        EventJournalEventType::Updated,
        EventJournalEventType::Evicted,
        EventJournalEventType::Expired,
        EventJournalEventType::Loaded,
    ];

    for event_type in types {
        let value = event_type.value();
        assert!(value > 0);
        assert!((value & (value - 1)) == 0, "value {} is not a power of 2", value);
    }
}

#[test]
fn test_event_journal_config_boundary_values() {
    let config = EventJournalConfig::new()
        .start_sequence(i64::MIN)
        .min_size(i32::MIN)
        .max_size(i32::MAX);

    assert_eq!(config.start_sequence, i64::MIN);
    assert_eq!(config.min_size, i32::MIN);
    assert_eq!(config.max_size, i32::MAX);
}

#[test]
fn test_event_journal_map_event_sequence_ordering() {
    let events: Vec<EventJournalMapEvent<String, i32>> = (0..5)
        .map(|i| EventJournalMapEvent {
            event_type: EventJournalEventType::Updated,
            key: format!("key-{}", i),
            old_value: Some(i),
            new_value: Some(i + 1),
            sequence: i as i64,
        })
        .collect();

    for (i, event) in events.iter().enumerate() {
        assert_eq!(event.sequence(), i as i64);
    }

    for i in 1..events.len() {
        assert!(events[i].sequence() > events[i - 1].sequence());
    }
}
