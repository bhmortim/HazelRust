//! Local map statistics for client-side operation tracking.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use super::local_stats::{LatencyStats, LatencyTracker};

/// Client-side statistics for map operations.
///
/// These statistics are accumulated locally on the client and reflect
/// operations performed through this client instance. They do not include
/// operations performed by other clients or on the server side.
#[derive(Debug, Clone, Default)]
pub struct LocalMapStats {
    hits: u64,
    misses: u64,
    put_count: u64,
    get_count: u64,
    remove_count: u64,
    last_access_time: i64,
    last_update_time: i64,
    owned_entry_count: u64,
    backup_entry_count: u64,
    heap_cost: u64,
    get_latency: LatencyStats,
    put_latency: LatencyStats,
    remove_latency: LatencyStats,
}

impl LocalMapStats {
    /// Returns the number of cache hits (successful gets from near-cache).
    pub fn hits(&self) -> u64 {
        self.hits
    }

    /// Returns the number of cache misses (gets that required cluster access).
    pub fn misses(&self) -> u64 {
        self.misses
    }

    /// Returns the total number of put operations.
    pub fn put_count(&self) -> u64 {
        self.put_count
    }

    /// Returns the total number of get operations.
    pub fn get_count(&self) -> u64 {
        self.get_count
    }

    /// Returns the total number of remove operations.
    pub fn remove_count(&self) -> u64 {
        self.remove_count
    }

    /// Returns the last access time in milliseconds since epoch.
    ///
    /// Returns 0 if no access has occurred.
    pub fn last_access_time(&self) -> i64 {
        self.last_access_time
    }

    /// Returns the last update time in milliseconds since epoch.
    ///
    /// Returns 0 if no update has occurred.
    pub fn last_update_time(&self) -> i64 {
        self.last_update_time
    }

    /// Returns the number of entries owned by this client's near-cache.
    pub fn owned_entry_count(&self) -> u64 {
        self.owned_entry_count
    }

    /// Returns the number of backup entries (always 0 for client-side stats).
    pub fn backup_entry_count(&self) -> u64 {
        self.backup_entry_count
    }

    /// Returns the estimated heap cost in bytes for locally cached entries.
    pub fn heap_cost(&self) -> u64 {
        self.heap_cost
    }

    /// Returns the hit ratio (hits / total gets).
    ///
    /// Returns `0.0` if no get operations have been performed.
    pub fn hit_ratio(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64
        }
    }

    /// Returns the total number of operations (get + put + remove).
    pub fn total_operations(&self) -> u64 {
        self.get_count + self.put_count + self.remove_count
    }

    /// Returns the latency statistics for get operations.
    pub fn get_latency(&self) -> &LatencyStats {
        &self.get_latency
    }

    /// Returns the latency statistics for put operations.
    pub fn put_latency(&self) -> &LatencyStats {
        &self.put_latency
    }

    /// Returns the latency statistics for remove operations.
    pub fn remove_latency(&self) -> &LatencyStats {
        &self.remove_latency
    }
}

/// Internal tracker for accumulating map statistics atomically.
#[derive(Debug)]
pub(crate) struct MapStatsTracker {
    hits: AtomicU64,
    misses: AtomicU64,
    put_count: AtomicU64,
    get_count: AtomicU64,
    remove_count: AtomicU64,
    last_access_time: AtomicI64,
    last_update_time: AtomicI64,
    get_latency: LatencyTracker,
    put_latency: LatencyTracker,
    remove_latency: LatencyTracker,
}

impl Default for MapStatsTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl MapStatsTracker {
    /// Creates a new statistics tracker.
    pub fn new() -> Self {
        Self {
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            put_count: AtomicU64::new(0),
            get_count: AtomicU64::new(0),
            remove_count: AtomicU64::new(0),
            last_access_time: AtomicI64::new(0),
            last_update_time: AtomicI64::new(0),
            get_latency: LatencyTracker::new(),
            put_latency: LatencyTracker::new(),
            remove_latency: LatencyTracker::new(),
        }
    }

    /// Records a cache hit.
    pub fn record_hit(&self) {
        self.hits.fetch_add(1, Ordering::Relaxed);
        self.update_access_time();
    }

    /// Records a cache miss.
    pub fn record_miss(&self) {
        self.misses.fetch_add(1, Ordering::Relaxed);
        self.update_access_time();
    }

    /// Records a get operation.
    pub fn record_get(&self) {
        self.get_count.fetch_add(1, Ordering::Relaxed);
        self.update_access_time();
    }

    /// Records a put operation.
    pub fn record_put(&self) {
        self.put_count.fetch_add(1, Ordering::Relaxed);
        self.update_modification_time();
    }

    /// Records a remove operation.
    pub fn record_remove(&self) {
        self.remove_count.fetch_add(1, Ordering::Relaxed);
        self.update_modification_time();
    }

    /// Records a get operation latency.
    pub fn record_get_latency(&self, duration: Duration) {
        self.get_latency.record(duration);
    }

    /// Records a put operation latency.
    pub fn record_put_latency(&self, duration: Duration) {
        self.put_latency.record(duration);
    }

    /// Records a remove operation latency.
    pub fn record_remove_latency(&self, duration: Duration) {
        self.remove_latency.record(duration);
    }

    /// Returns a snapshot of the current statistics.
    pub fn snapshot(&self, owned_entry_count: u64, heap_cost: u64) -> LocalMapStats {
        LocalMapStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            put_count: self.put_count.load(Ordering::Relaxed),
            get_count: self.get_count.load(Ordering::Relaxed),
            remove_count: self.remove_count.load(Ordering::Relaxed),
            last_access_time: self.last_access_time.load(Ordering::Relaxed),
            last_update_time: self.last_update_time.load(Ordering::Relaxed),
            owned_entry_count,
            backup_entry_count: 0,
            heap_cost,
            get_latency: self.get_latency.snapshot(),
            put_latency: self.put_latency.snapshot(),
            remove_latency: self.remove_latency.snapshot(),
        }
    }

    fn update_access_time(&self) {
        let now = current_time_millis();
        self.last_access_time.store(now, Ordering::Relaxed);
    }

    fn update_modification_time(&self) {
        let now = current_time_millis();
        self.last_update_time.store(now, Ordering::Relaxed);
        self.last_access_time.store(now, Ordering::Relaxed);
    }
}

fn current_time_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_local_map_stats_default() {
        let stats = LocalMapStats::default();
        assert_eq!(stats.hits(), 0);
        assert_eq!(stats.misses(), 0);
        assert_eq!(stats.put_count(), 0);
        assert_eq!(stats.get_count(), 0);
        assert_eq!(stats.remove_count(), 0);
        assert_eq!(stats.last_access_time(), 0);
        assert_eq!(stats.last_update_time(), 0);
        assert_eq!(stats.owned_entry_count(), 0);
        assert_eq!(stats.backup_entry_count(), 0);
        assert_eq!(stats.heap_cost(), 0);
    }

    #[test]
    fn test_local_map_stats_hit_ratio() {
        let mut stats = LocalMapStats::default();
        assert_eq!(stats.hit_ratio(), 0.0);

        stats.hits = 3;
        stats.misses = 1;
        assert!((stats.hit_ratio() - 0.75).abs() < f64::EPSILON);
    }

    #[test]
    fn test_local_map_stats_total_operations() {
        let mut stats = LocalMapStats::default();
        stats.get_count = 10;
        stats.put_count = 5;
        stats.remove_count = 2;
        assert_eq!(stats.total_operations(), 17);
    }

    #[test]
    fn test_local_map_stats_clone() {
        let mut stats = LocalMapStats::default();
        stats.hits = 100;
        stats.put_count = 50;

        let cloned = stats.clone();
        assert_eq!(cloned.hits(), 100);
        assert_eq!(cloned.put_count(), 50);
    }

    #[test]
    fn test_map_stats_tracker_new() {
        let tracker = MapStatsTracker::new();
        let stats = tracker.snapshot(0, 0);
        assert_eq!(stats.hits(), 0);
        assert_eq!(stats.misses(), 0);
    }

    #[test]
    fn test_map_stats_tracker_record_hit() {
        let tracker = MapStatsTracker::new();
        tracker.record_hit();
        tracker.record_hit();
        tracker.record_hit();

        let stats = tracker.snapshot(0, 0);
        assert_eq!(stats.hits(), 3);
    }

    #[test]
    fn test_map_stats_tracker_record_miss() {
        let tracker = MapStatsTracker::new();
        tracker.record_miss();
        tracker.record_miss();

        let stats = tracker.snapshot(0, 0);
        assert_eq!(stats.misses(), 2);
    }

    #[test]
    fn test_map_stats_tracker_record_get() {
        let tracker = MapStatsTracker::new();
        tracker.record_get();
        tracker.record_get();
        tracker.record_get();
        tracker.record_get();

        let stats = tracker.snapshot(0, 0);
        assert_eq!(stats.get_count(), 4);
    }

    #[test]
    fn test_map_stats_tracker_record_put() {
        let tracker = MapStatsTracker::new();
        tracker.record_put();
        tracker.record_put();

        let stats = tracker.snapshot(0, 0);
        assert_eq!(stats.put_count(), 2);
    }

    #[test]
    fn test_map_stats_tracker_record_remove() {
        let tracker = MapStatsTracker::new();
        tracker.record_remove();

        let stats = tracker.snapshot(0, 0);
        assert_eq!(stats.remove_count(), 1);
    }

    #[test]
    fn test_map_stats_tracker_updates_access_time() {
        let tracker = MapStatsTracker::new();
        assert_eq!(tracker.last_access_time.load(Ordering::Relaxed), 0);

        tracker.record_get();
        assert!(tracker.last_access_time.load(Ordering::Relaxed) > 0);
    }

    #[test]
    fn test_map_stats_tracker_updates_modification_time() {
        let tracker = MapStatsTracker::new();
        assert_eq!(tracker.last_update_time.load(Ordering::Relaxed), 0);

        tracker.record_put();
        assert!(tracker.last_update_time.load(Ordering::Relaxed) > 0);
    }

    #[test]
    fn test_map_stats_tracker_snapshot_includes_owned_entries() {
        let tracker = MapStatsTracker::new();
        let stats = tracker.snapshot(100, 5000);

        assert_eq!(stats.owned_entry_count(), 100);
        assert_eq!(stats.heap_cost(), 5000);
    }

    #[test]
    fn test_map_stats_tracker_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<MapStatsTracker>();
    }

    #[test]
    fn test_local_map_stats_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<LocalMapStats>();
    }

    #[test]
    fn test_map_stats_tracker_default() {
        let tracker = MapStatsTracker::default();
        let stats = tracker.snapshot(0, 0);
        assert_eq!(stats.total_operations(), 0);
    }

    #[test]
    fn test_map_stats_tracker_latency() {
        let tracker = MapStatsTracker::new();
        tracker.record_get_latency(Duration::from_millis(5));
        tracker.record_put_latency(Duration::from_millis(10));
        tracker.record_remove_latency(Duration::from_millis(15));

        let stats = tracker.snapshot(0, 0);
        assert_eq!(stats.get_latency().count, 1);
        assert_eq!(stats.put_latency().count, 1);
        assert_eq!(stats.remove_latency().count, 1);
    }

    #[test]
    fn test_map_stats_tracker_latency_multiple() {
        let tracker = MapStatsTracker::new();
        tracker.record_get_latency(Duration::from_millis(10));
        tracker.record_get_latency(Duration::from_millis(20));
        tracker.record_get_latency(Duration::from_millis(30));

        let stats = tracker.snapshot(0, 0);
        assert_eq!(stats.get_latency().count, 3);
        assert_eq!(stats.get_latency().min_nanos, 10_000_000);
        assert_eq!(stats.get_latency().max_nanos, 30_000_000);
    }

    #[test]
    fn test_local_map_stats_latency_accessors() {
        let stats = LocalMapStats::default();
        assert_eq!(stats.get_latency().count, 0);
        assert_eq!(stats.put_latency().count, 0);
        assert_eq!(stats.remove_latency().count, 0);
    }
}
