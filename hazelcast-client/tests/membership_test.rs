//! Integration tests for cluster membership tracking.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::broadcast;
use uuid::Uuid;

use hazelcast_client::listener::{Member, MemberEvent, MemberEventType, MembershipListener};

struct CountingListener {
    added_count: AtomicU32,
    removed_count: AtomicU32,
}

impl CountingListener {
    fn new() -> Self {
        Self {
            added_count: AtomicU32::new(0),
            removed_count: AtomicU32::new(0),
        }
    }

    fn added(&self) -> u32 {
        self.added_count.load(Ordering::Relaxed)
    }

    fn removed(&self) -> u32 {
        self.removed_count.load(Ordering::Relaxed)
    }
}

impl MembershipListener for CountingListener {
    fn member_added(&self, _event: &MemberEvent) {
        self.added_count.fetch_add(1, Ordering::Relaxed);
    }

    fn member_removed(&self, _event: &MemberEvent) {
        self.removed_count.fetch_add(1, Ordering::Relaxed);
    }
}

fn create_test_member(port: u16) -> Member {
    Member::new(
        Uuid::new_v4(),
        format!("127.0.0.1:{}", port).parse().unwrap(),
    )
}

fn create_member_with_attrs(port: u16, zone: &str) -> Member {
    let mut attrs = HashMap::new();
    attrs.insert("zone".to_string(), zone.to_string());
    Member::with_attributes(
        Uuid::new_v4(),
        format!("127.0.0.1:{}", port).parse().unwrap(),
        attrs,
        false,
    )
}

#[test]
fn test_member_event_creation() {
    let member = create_test_member(5701);
    let added = MemberEvent::member_added(member.clone());
    let removed = MemberEvent::member_removed(member.clone());

    assert_eq!(added.event_type, MemberEventType::Added);
    assert_eq!(removed.event_type, MemberEventType::Removed);
    assert_eq!(added.member.uuid(), member.uuid());
}

#[test]
fn test_membership_listener_receives_events() {
    let listener = Arc::new(CountingListener::new());
    let member = create_test_member(5701);

    let added_event = MemberEvent::member_added(member.clone());
    let removed_event = MemberEvent::member_removed(member);

    listener.member_added(&added_event);
    listener.member_added(&added_event);
    listener.member_removed(&removed_event);

    assert_eq!(listener.added(), 2);
    assert_eq!(listener.removed(), 1);
}

#[tokio::test]
async fn test_membership_broadcast_channel() {
    let (tx, mut rx1) = broadcast::channel::<MemberEvent>(16);
    let mut rx2 = tx.subscribe();

    let member = create_test_member(5701);
    let event = MemberEvent::member_added(member);

    tx.send(event.clone()).unwrap();

    let received1 = rx1.recv().await.unwrap();
    let received2 = rx2.recv().await.unwrap();

    assert_eq!(received1.event_type, MemberEventType::Added);
    assert_eq!(received2.event_type, MemberEventType::Added);
    assert_eq!(received1.member.uuid(), received2.member.uuid());
}

#[tokio::test]
async fn test_multiple_member_events() {
    let (tx, mut rx) = broadcast::channel::<MemberEvent>(16);

    let member1 = create_test_member(5701);
    let member2 = create_test_member(5702);
    let member3 = create_test_member(5703);

    tx.send(MemberEvent::member_added(member1.clone())).unwrap();
    tx.send(MemberEvent::member_added(member2.clone())).unwrap();
    tx.send(MemberEvent::member_removed(member1.clone())).unwrap();
    tx.send(MemberEvent::member_added(member3.clone())).unwrap();

    let events: Vec<_> = (0..4)
        .map(|_| rx.try_recv().unwrap())
        .collect();

    assert_eq!(events[0].event_type, MemberEventType::Added);
    assert_eq!(events[1].event_type, MemberEventType::Added);
    assert_eq!(events[2].event_type, MemberEventType::Removed);
    assert_eq!(events[3].event_type, MemberEventType::Added);
}

#[test]
fn test_member_with_attributes() {
    let member = create_member_with_attrs(5701, "us-east-1");

    assert_eq!(member.attributes().get("zone"), Some(&"us-east-1".to_string()));
    assert!(!member.is_lite_member());
}

#[test]
fn test_member_equality() {
    let uuid = Uuid::new_v4();
    let addr: SocketAddr = "127.0.0.1:5701".parse().unwrap();

    let member1 = Member::new(uuid, addr);
    let member2 = Member::new(uuid, addr);
    let member3 = Member::new(Uuid::new_v4(), addr);

    assert_eq!(member1, member2);
    assert_ne!(member1, member3);
}

#[tokio::test]
async fn test_late_subscriber_misses_old_events() {
    let (tx, mut rx1) = broadcast::channel::<MemberEvent>(16);

    let member1 = create_test_member(5701);
    tx.send(MemberEvent::member_added(member1)).unwrap();

    let mut rx2 = tx.subscribe();

    let member2 = create_test_member(5702);
    tx.send(MemberEvent::member_added(member2)).unwrap();

    let event1 = rx1.recv().await.unwrap();
    let event2 = rx1.recv().await.unwrap();
    assert_eq!(event1.member.address().port(), 5701);
    assert_eq!(event2.member.address().port(), 5702);

    let late_event = rx2.recv().await.unwrap();
    assert_eq!(late_event.member.address().port(), 5702);
}

#[test]
fn test_member_event_type_roundtrip() {
    for (value, expected) in [(1, MemberEventType::Added), (2, MemberEventType::Removed)] {
        let event_type = MemberEventType::from_value(value).unwrap();
        assert_eq!(event_type, expected);
        assert_eq!(event_type.value(), value);
    }
}

#[test]
fn test_member_display_format() {
    let uuid = Uuid::new_v4();
    let addr: SocketAddr = "192.168.1.100:5701".parse().unwrap();
    let member = Member::new(uuid, addr);

    let display = format!("{}", member);
    assert!(display.contains(&uuid.to_string()));
    assert!(display.contains("192.168.1.100:5701"));
}

#[test]
fn test_member_event_display_format() {
    let member = create_test_member(5701);
    let event = MemberEvent::member_added(member);

    let display = format!("{}", event);
    assert!(display.contains("MemberEvent"));
    assert!(display.contains("ADDED"));
}
