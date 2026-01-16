//! Membership listener infrastructure for cluster member events.

use std::collections::HashMap;
use std::net::SocketAddr;

use uuid::Uuid;

/// Type of membership event fired when cluster topology changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i32)]
pub enum MemberEventType {
    /// A new member joined the cluster.
    Added = 1,
    /// A member left the cluster.
    Removed = 2,
}

impl MemberEventType {
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

impl std::fmt::Display for MemberEventType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Added => write!(f, "ADDED"),
            Self::Removed => write!(f, "REMOVED"),
        }
    }
}

/// Represents a member of the Hazelcast cluster.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Member {
    /// Unique identifier of the member.
    pub uuid: Uuid,
    /// Network address of the member.
    pub address: SocketAddr,
    /// Custom attributes associated with the member.
    pub attributes: HashMap<String, String>,
    /// Whether this member runs a lite configuration.
    pub lite_member: bool,
}

impl Member {
    /// Creates a new cluster member.
    pub fn new(uuid: Uuid, address: SocketAddr) -> Self {
        Self {
            uuid,
            address,
            attributes: HashMap::new(),
            lite_member: false,
        }
    }

    /// Creates a new cluster member with attributes.
    pub fn with_attributes(
        uuid: Uuid,
        address: SocketAddr,
        attributes: HashMap<String, String>,
        lite_member: bool,
    ) -> Self {
        Self {
            uuid,
            address,
            attributes,
            lite_member,
        }
    }

    /// Returns the member's UUID.
    pub fn uuid(&self) -> Uuid {
        self.uuid
    }

    /// Returns the member's network address.
    pub fn address(&self) -> SocketAddr {
        self.address
    }

    /// Returns the member's attributes.
    pub fn attributes(&self) -> &HashMap<String, String> {
        &self.attributes
    }

    /// Returns whether this is a lite member.
    pub fn is_lite_member(&self) -> bool {
        self.lite_member
    }
}

impl std::fmt::Display for Member {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Member[uuid={}, address={}]", self.uuid, self.address)
    }
}

/// An event fired when an initial membership listener is registered.
///
/// This event contains the current snapshot of cluster members at the time
/// of listener registration.
#[derive(Debug, Clone)]
pub struct InitialMembershipEvent {
    /// The current members of the cluster at registration time.
    pub members: Vec<Member>,
}

impl InitialMembershipEvent {
    /// Creates a new initial membership event.
    pub fn new(members: Vec<Member>) -> Self {
        Self { members }
    }

    /// Returns the current cluster members.
    pub fn members(&self) -> &[Member] {
        &self.members
    }
}

impl std::fmt::Display for InitialMembershipEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "InitialMembershipEvent[members={}]", self.members.len())
    }
}

/// An event fired when a cluster member joins or leaves.
#[derive(Debug, Clone)]
pub struct MemberEvent {
    /// The member that triggered the event.
    pub member: Member,
    /// The type of membership change.
    pub event_type: MemberEventType,
}

impl MemberEvent {
    /// Creates a new membership event.
    pub fn new(member: Member, event_type: MemberEventType) -> Self {
        Self { member, event_type }
    }

    /// Creates an event for a member that joined the cluster.
    pub fn member_added(member: Member) -> Self {
        Self::new(member, MemberEventType::Added)
    }

    /// Creates an event for a member that left the cluster.
    pub fn member_removed(member: Member) -> Self {
        Self::new(member, MemberEventType::Removed)
    }
}

impl std::fmt::Display for MemberEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MemberEvent[{} {}]", self.member, self.event_type)
    }
}

/// Trait for listening to cluster membership changes.
pub trait MembershipListener: Send + Sync {
    /// Called when a new member joins the cluster.
    fn member_added(&self, event: &MemberEvent);

    /// Called when a member leaves the cluster.
    fn member_removed(&self, event: &MemberEvent);
}

/// Trait for listening to cluster membership changes with initial state.
///
/// This listener extends [`MembershipListener`] to receive the current
/// cluster membership state when first registered, before receiving
/// subsequent membership change events.
pub trait InitialMembershipListener: MembershipListener {
    /// Called when the listener is registered with the current cluster members.
    ///
    /// This method is invoked once immediately after registration with a
    /// snapshot of the current cluster membership. Subsequent membership
    /// changes are delivered through the [`MembershipListener`] methods.
    fn init(&self, event: &InitialMembershipEvent);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    #[test]
    fn test_member_event_type_values() {
        assert_eq!(MemberEventType::Added.value(), 1);
        assert_eq!(MemberEventType::Removed.value(), 2);
    }

    #[test]
    fn test_member_event_type_from_value() {
        assert_eq!(MemberEventType::from_value(1), Some(MemberEventType::Added));
        assert_eq!(MemberEventType::from_value(2), Some(MemberEventType::Removed));
        assert_eq!(MemberEventType::from_value(0), None);
        assert_eq!(MemberEventType::from_value(99), None);
    }

    #[test]
    fn test_member_event_type_display() {
        assert_eq!(MemberEventType::Added.to_string(), "ADDED");
        assert_eq!(MemberEventType::Removed.to_string(), "REMOVED");
    }

    #[test]
    fn test_member_creation() {
        let uuid = Uuid::new_v4();
        let addr: SocketAddr = "127.0.0.1:5701".parse().unwrap();
        let member = Member::new(uuid, addr);

        assert_eq!(member.uuid(), uuid);
        assert_eq!(member.address(), addr);
        assert!(member.attributes().is_empty());
        assert!(!member.is_lite_member());
    }

    #[test]
    fn test_member_with_attributes() {
        let uuid = Uuid::new_v4();
        let addr: SocketAddr = "192.168.1.100:5701".parse().unwrap();
        let mut attrs = HashMap::new();
        attrs.insert("zone".to_string(), "us-east-1".to_string());

        let member = Member::with_attributes(uuid, addr, attrs.clone(), true);

        assert_eq!(member.uuid(), uuid);
        assert_eq!(member.address(), addr);
        assert_eq!(member.attributes().get("zone"), Some(&"us-east-1".to_string()));
        assert!(member.is_lite_member());
    }

    #[test]
    fn test_member_display() {
        let uuid = Uuid::new_v4();
        let addr: SocketAddr = "127.0.0.1:5701".parse().unwrap();
        let member = Member::new(uuid, addr);

        let display = member.to_string();
        assert!(display.contains("Member["));
        assert!(display.contains(&uuid.to_string()));
        assert!(display.contains("127.0.0.1:5701"));
    }

    #[test]
    fn test_member_event_creation() {
        let member = Member::new(Uuid::new_v4(), "127.0.0.1:5701".parse().unwrap());
        let event = MemberEvent::new(member.clone(), MemberEventType::Added);

        assert_eq!(event.member.uuid(), member.uuid());
        assert_eq!(event.event_type, MemberEventType::Added);
    }

    #[test]
    fn test_member_event_convenience_constructors() {
        let member = Member::new(Uuid::new_v4(), "127.0.0.1:5701".parse().unwrap());

        let added = MemberEvent::member_added(member.clone());
        assert_eq!(added.event_type, MemberEventType::Added);

        let removed = MemberEvent::member_removed(member);
        assert_eq!(removed.event_type, MemberEventType::Removed);
    }

    #[test]
    fn test_member_event_display() {
        let member = Member::new(Uuid::new_v4(), "127.0.0.1:5701".parse().unwrap());
        let event = MemberEvent::member_added(member);

        let display = event.to_string();
        assert!(display.contains("MemberEvent["));
        assert!(display.contains("ADDED"));
    }

    struct TestListener {
        added_count: AtomicU32,
        removed_count: AtomicU32,
    }

    impl MembershipListener for TestListener {
        fn member_added(&self, _event: &MemberEvent) {
            self.added_count.fetch_add(1, Ordering::Relaxed);
        }

        fn member_removed(&self, _event: &MemberEvent) {
            self.removed_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn test_membership_listener_trait() {
        let listener = Arc::new(TestListener {
            added_count: AtomicU32::new(0),
            removed_count: AtomicU32::new(0),
        });

        let member = Member::new(Uuid::new_v4(), "127.0.0.1:5701".parse().unwrap());
        let added_event = MemberEvent::member_added(member.clone());
        let removed_event = MemberEvent::member_removed(member);

        listener.member_added(&added_event);
        listener.member_added(&added_event);
        listener.member_removed(&removed_event);

        assert_eq!(listener.added_count.load(Ordering::Relaxed), 2);
        assert_eq!(listener.removed_count.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_member_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Member>();
    }

    #[test]
    fn test_member_event_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<MemberEvent>();
    }

    #[test]
    fn test_member_event_type_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<MemberEventType>();
    }

    #[test]
    fn test_initial_membership_event_creation() {
        let members = vec![
            Member::new(Uuid::new_v4(), "127.0.0.1:5701".parse().unwrap()),
            Member::new(Uuid::new_v4(), "127.0.0.1:5702".parse().unwrap()),
        ];
        let event = InitialMembershipEvent::new(members.clone());

        assert_eq!(event.members().len(), 2);
        assert_eq!(event.members()[0].address(), members[0].address());
    }

    #[test]
    fn test_initial_membership_event_empty() {
        let event = InitialMembershipEvent::new(vec![]);
        assert!(event.members().is_empty());
    }

    #[test]
    fn test_initial_membership_event_display() {
        let members = vec![
            Member::new(Uuid::new_v4(), "127.0.0.1:5701".parse().unwrap()),
        ];
        let event = InitialMembershipEvent::new(members);

        let display = event.to_string();
        assert!(display.contains("InitialMembershipEvent["));
        assert!(display.contains("members=1"));
    }

    #[test]
    fn test_initial_membership_event_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<InitialMembershipEvent>();
    }

    struct TestInitialListener {
        init_count: AtomicU32,
        added_count: AtomicU32,
        removed_count: AtomicU32,
    }

    impl MembershipListener for TestInitialListener {
        fn member_added(&self, _event: &MemberEvent) {
            self.added_count.fetch_add(1, Ordering::Relaxed);
        }

        fn member_removed(&self, _event: &MemberEvent) {
            self.removed_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    impl InitialMembershipListener for TestInitialListener {
        fn init(&self, _event: &InitialMembershipEvent) {
            self.init_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn test_initial_membership_listener_trait() {
        let listener = Arc::new(TestInitialListener {
            init_count: AtomicU32::new(0),
            added_count: AtomicU32::new(0),
            removed_count: AtomicU32::new(0),
        });

        let members = vec![
            Member::new(Uuid::new_v4(), "127.0.0.1:5701".parse().unwrap()),
            Member::new(Uuid::new_v4(), "127.0.0.1:5702".parse().unwrap()),
        ];
        let init_event = InitialMembershipEvent::new(members);

        listener.init(&init_event);
        assert_eq!(listener.init_count.load(Ordering::Relaxed), 1);

        let member = Member::new(Uuid::new_v4(), "127.0.0.1:5703".parse().unwrap());
        let added_event = MemberEvent::member_added(member.clone());
        let removed_event = MemberEvent::member_removed(member);

        listener.member_added(&added_event);
        listener.member_removed(&removed_event);

        assert_eq!(listener.added_count.load(Ordering::Relaxed), 1);
        assert_eq!(listener.removed_count.load(Ordering::Relaxed), 1);
    }
}
