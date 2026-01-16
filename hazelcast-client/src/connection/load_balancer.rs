//! Load balancing strategies for distributing requests across cluster members.

use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::listener::Member;

/// A strategy for selecting cluster members for operations.
///
/// Load balancers distribute client requests across available cluster members
/// to balance load and improve availability.
pub trait LoadBalancer: Send + Sync {
    /// Selects a member from the given list of available members.
    ///
    /// Returns `None` if the member list is empty.
    fn select<'a>(&self, members: &'a [Member]) -> Option<&'a Member>;
}

impl std::fmt::Debug for dyn LoadBalancer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("LoadBalancer")
    }
}

/// A load balancer that cycles through members in round-robin fashion.
///
/// Each call to `select` returns the next member in sequence, wrapping
/// around to the first member after reaching the end of the list.
#[derive(Debug, Default)]
pub struct RoundRobinLoadBalancer {
    index: AtomicUsize,
}

impl RoundRobinLoadBalancer {
    /// Creates a new round-robin load balancer.
    pub fn new() -> Self {
        Self {
            index: AtomicUsize::new(0),
        }
    }
}

impl LoadBalancer for RoundRobinLoadBalancer {
    fn select<'a>(&self, members: &'a [Member]) -> Option<&'a Member> {
        if members.is_empty() {
            return None;
        }
        let idx = self.index.fetch_add(1, Ordering::Relaxed) % members.len();
        Some(&members[idx])
    }
}

/// A load balancer that randomly selects members.
///
/// Each call to `select` returns a randomly chosen member from the
/// available members list.
#[derive(Debug, Default)]
pub struct RandomLoadBalancer;

impl RandomLoadBalancer {
    /// Creates a new random load balancer.
    pub fn new() -> Self {
        Self
    }
}

impl LoadBalancer for RandomLoadBalancer {
    fn select<'a>(&self, members: &'a [Member]) -> Option<&'a Member> {
        if members.is_empty() {
            return None;
        }
        let random = RandomState::new().build_hasher().finish() as usize;
        let idx = random % members.len();
        Some(&members[idx])
    }
}

/// Creates a default load balancer (round-robin).
pub fn default_load_balancer() -> Arc<dyn LoadBalancer> {
    Arc::new(RoundRobinLoadBalancer::new())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    fn create_test_members(count: usize) -> Vec<Member> {
        (0..count)
            .map(|i| {
                let addr: SocketAddr = format!("127.0.0.1:{}", 5701 + i).parse().unwrap();
                Member::new(uuid::Uuid::new_v4(), addr)
            })
            .collect()
    }

    #[test]
    fn test_round_robin_empty_members() {
        let lb = RoundRobinLoadBalancer::new();
        assert!(lb.select(&[]).is_none());
    }

    #[test]
    fn test_round_robin_single_member() {
        let lb = RoundRobinLoadBalancer::new();
        let members = create_test_members(1);

        for _ in 0..5 {
            let selected = lb.select(&members).unwrap();
            assert_eq!(selected.address(), members[0].address());
        }
    }

    #[test]
    fn test_round_robin_cycles_through_members() {
        let lb = RoundRobinLoadBalancer::new();
        let members = create_test_members(3);

        for round in 0..3 {
            for (i, member) in members.iter().enumerate() {
                let selected = lb.select(&members).unwrap();
                assert_eq!(
                    selected.address(),
                    member.address(),
                    "Round {}, index {}: expected {:?}, got {:?}",
                    round,
                    i,
                    member.address(),
                    selected.address()
                );
            }
        }
    }

    #[test]
    fn test_random_empty_members() {
        let lb = RandomLoadBalancer::new();
        assert!(lb.select(&[]).is_none());
    }

    #[test]
    fn test_random_single_member() {
        let lb = RandomLoadBalancer::new();
        let members = create_test_members(1);

        let selected = lb.select(&members).unwrap();
        assert_eq!(selected.address(), members[0].address());
    }

    #[test]
    fn test_random_selects_from_members() {
        let lb = RandomLoadBalancer::new();
        let members = create_test_members(5);

        for _ in 0..20 {
            let selected = lb.select(&members).unwrap();
            assert!(members.iter().any(|m| m.address() == selected.address()));
        }
    }

    #[test]
    fn test_load_balancer_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<RoundRobinLoadBalancer>();
        assert_send_sync::<RandomLoadBalancer>();
    }

    #[test]
    fn test_default_load_balancer() {
        let lb = default_load_balancer();
        let members = create_test_members(2);
        assert!(lb.select(&members).is_some());
    }
}
