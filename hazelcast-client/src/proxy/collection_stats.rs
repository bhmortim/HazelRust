//! Local statistics for distributed collection data structures.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use super::local_stats::{LatencyStats, LatencyTracker};

/// Local statistics for `IQueue` operations.
#[derive(Debug)]
pub struct LocalQueueStats {
    creation_time: Instant,
    offer_count: AtomicU64,
    poll_count: AtomicU64,
    peek_count: AtomicU64,
    rejected_offer_count: AtomicU64,
    empty_poll_count: AtomicU64,
    other_operation_count: AtomicU64,
    offer_latency: LatencyTracker,
    poll_latency: LatencyTracker,
}

impl LocalQueueStats {
    pub(crate) fn new() -> Self {
        Self {
            creation_time: Instant::now(),
            offer_count: AtomicU64::new(0),
            poll_count: AtomicU64::new(0),
            peek_count: AtomicU64::new(0),
            rejected_offer_count: AtomicU64::new(0),
            empty_poll_count: AtomicU64::new(0),
            other_operation_count: AtomicU64::new(0),
            offer_latency: LatencyTracker::new(),
            poll_latency: LatencyTracker::new(),
        }
    }

    pub fn creation_time(&self) -> Instant {
        self.creation_time
    }

    pub fn offer_count(&self) -> u64 {
        self.offer_count.load(Ordering::Relaxed)
    }

    pub fn poll_count(&self) -> u64 {
        self.poll_count.load(Ordering::Relaxed)
    }

    pub fn peek_count(&self) -> u64 {
        self.peek_count.load(Ordering::Relaxed)
    }

    pub fn rejected_offer_count(&self) -> u64 {
        self.rejected_offer_count.load(Ordering::Relaxed)
    }

    pub fn empty_poll_count(&self) -> u64 {
        self.empty_poll_count.load(Ordering::Relaxed)
    }

    pub fn other_operation_count(&self) -> u64 {
        self.other_operation_count.load(Ordering::Relaxed)
    }

    pub(crate) fn increment_offer(&self) {
        self.offer_count.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn increment_poll(&self) {
        self.poll_count.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn increment_peek(&self) {
        self.peek_count.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn increment_rejected_offer(&self) {
        self.rejected_offer_count.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn increment_empty_poll(&self) {
        self.empty_poll_count.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn increment_other(&self) {
        self.other_operation_count.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_offer_latency(&self, duration: Duration) {
        self.offer_latency.record(duration);
    }

    pub(crate) fn record_poll_latency(&self, duration: Duration) {
        self.poll_latency.record(duration);
    }

    pub fn offer_latency_snapshot(&self) -> LatencyStats {
        self.offer_latency.snapshot()
    }

    pub fn poll_latency_snapshot(&self) -> LatencyStats {
        self.poll_latency.snapshot()
    }
}

/// Local statistics for `ISet` operations.
#[derive(Debug)]
pub struct LocalSetStats {
    creation_time: Instant,
    add_count: AtomicU64,
    remove_count: AtomicU64,
    contains_count: AtomicU64,
    other_operation_count: AtomicU64,
    add_latency: LatencyTracker,
    remove_latency: LatencyTracker,
}

impl LocalSetStats {
    pub(crate) fn new() -> Self {
        Self {
            creation_time: Instant::now(),
            add_count: AtomicU64::new(0),
            remove_count: AtomicU64::new(0),
            contains_count: AtomicU64::new(0),
            other_operation_count: AtomicU64::new(0),
            add_latency: LatencyTracker::new(),
            remove_latency: LatencyTracker::new(),
        }
    }

    pub fn creation_time(&self) -> Instant {
        self.creation_time
    }

    pub fn add_count(&self) -> u64 {
        self.add_count.load(Ordering::Relaxed)
    }

    pub fn remove_count(&self) -> u64 {
        self.remove_count.load(Ordering::Relaxed)
    }

    pub fn contains_count(&self) -> u64 {
        self.contains_count.load(Ordering::Relaxed)
    }

    pub fn other_operation_count(&self) -> u64 {
        self.other_operation_count.load(Ordering::Relaxed)
    }

    pub(crate) fn increment_add(&self) {
        self.add_count.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn increment_remove(&self) {
        self.remove_count.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn increment_contains(&self) {
        self.contains_count.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn increment_other(&self) {
        self.other_operation_count.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_add_latency(&self, duration: Duration) {
        self.add_latency.record(duration);
    }

    pub(crate) fn record_remove_latency(&self, duration: Duration) {
        self.remove_latency.record(duration);
    }

    pub fn add_latency_snapshot(&self) -> LatencyStats {
        self.add_latency.snapshot()
    }

    pub fn remove_latency_snapshot(&self) -> LatencyStats {
        self.remove_latency.snapshot()
    }
}

/// Local statistics for `IList` operations.
#[derive(Debug)]
pub struct LocalListStats {
    creation_time: Instant,
    add_count: AtomicU64,
    remove_count: AtomicU64,
    get_count: AtomicU64,
    set_count: AtomicU64,
    other_operation_count: AtomicU64,
    add_latency: LatencyTracker,
    get_latency: LatencyTracker,
}

impl LocalListStats {
    pub(crate) fn new() -> Self {
        Self {
            creation_time: Instant::now(),
            add_count: AtomicU64::new(0),
            remove_count: AtomicU64::new(0),
            get_count: AtomicU64::new(0),
            set_count: AtomicU64::new(0),
            other_operation_count: AtomicU64::new(0),
            add_latency: LatencyTracker::new(),
            get_latency: LatencyTracker::new(),
        }
    }

    pub fn creation_time(&self) -> Instant {
        self.creation_time
    }

    pub fn add_count(&self) -> u64 {
        self.add_count.load(Ordering::Relaxed)
    }

    pub fn remove_count(&self) -> u64 {
        self.remove_count.load(Ordering::Relaxed)
    }

    pub fn get_count(&self) -> u64 {
        self.get_count.load(Ordering::Relaxed)
    }

    pub fn set_count(&self) -> u64 {
        self.set_count.load(Ordering::Relaxed)
    }

    pub fn other_operation_count(&self) -> u64 {
        self.other_operation_count.load(Ordering::Relaxed)
    }

    pub(crate) fn increment_add(&self) {
        self.add_count.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn increment_remove(&self) {
        self.remove_count.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn increment_get(&self) {
        self.get_count.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn increment_set(&self) {
        self.set_count.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn increment_other(&self) {
        self.other_operation_count.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_add_latency(&self, duration: Duration) {
        self.add_latency.record(duration);
    }

    pub(crate) fn record_get_latency(&self, duration: Duration) {
        self.get_latency.record(duration);
    }

    pub fn add_latency_snapshot(&self) -> LatencyStats {
        self.add_latency.snapshot()
    }

    pub fn get_latency_snapshot(&self) -> LatencyStats {
        self.get_latency.snapshot()
    }
}
