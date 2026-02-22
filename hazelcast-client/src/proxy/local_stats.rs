//! Common latency tracking types for local statistics.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Statistics for operation latencies.
#[derive(Debug, Clone, Default)]
pub struct LatencyStats {
    /// Minimum latency in nanoseconds.
    pub min_nanos: u64,
    /// Maximum latency in nanoseconds.
    pub max_nanos: u64,
    /// Total latency in nanoseconds (for computing average).
    pub total_nanos: u64,
    /// Number of operations recorded.
    pub count: u64,
}

impl LatencyStats {
    /// Creates a new empty `LatencyStats`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the average latency in nanoseconds.
    ///
    /// Returns 0 if no operations have been recorded.
    pub fn average_nanos(&self) -> u64 {
        if self.count == 0 {
            0
        } else {
            self.total_nanos / self.count
        }
    }

    /// Returns the minimum latency in milliseconds.
    pub fn min_millis(&self) -> f64 {
        self.min_nanos as f64 / 1_000_000.0
    }

    /// Returns the maximum latency in milliseconds.
    pub fn max_millis(&self) -> f64 {
        self.max_nanos as f64 / 1_000_000.0
    }

    /// Returns the average latency in milliseconds.
    pub fn average_millis(&self) -> f64 {
        self.average_nanos() as f64 / 1_000_000.0
    }
}

/// Thread-safe tracker for recording operation latencies.
#[derive(Debug)]
pub struct LatencyTracker {
    min_nanos: AtomicU64,
    max_nanos: AtomicU64,
    total_nanos: AtomicU64,
    count: AtomicU64,
}

impl Default for LatencyTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl LatencyTracker {
    /// Creates a new latency tracker.
    pub fn new() -> Self {
        Self {
            min_nanos: AtomicU64::new(u64::MAX),
            max_nanos: AtomicU64::new(0),
            total_nanos: AtomicU64::new(0),
            count: AtomicU64::new(0),
        }
    }

    /// Records a latency measurement.
    pub fn record(&self, duration: Duration) {
        let nanos = duration.as_nanos() as u64;
        self.min_nanos.fetch_min(nanos, Ordering::Relaxed);
        self.max_nanos.fetch_max(nanos, Ordering::Relaxed);
        self.total_nanos.fetch_add(nanos, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    /// Returns a snapshot of the current latency statistics.
    pub fn snapshot(&self) -> LatencyStats {
        let count = self.count.load(Ordering::Relaxed);
        let min_nanos = if count == 0 {
            0
        } else {
            self.min_nanos.load(Ordering::Relaxed)
        };

        LatencyStats {
            min_nanos,
            max_nanos: self.max_nanos.load(Ordering::Relaxed),
            total_nanos: self.total_nanos.load(Ordering::Relaxed),
            count,
        }
    }
}

// Re-exports from other stats modules for convenience.
// These are intentional API re-exports even though not yet consumed internally.
#[allow(unused_imports)]
pub use super::collection_stats::{LocalListStats, LocalQueueStats, LocalSetStats};
#[allow(unused_imports)]
pub use super::map_stats::LocalMapStats;
#[allow(unused_imports)]
pub(crate) use super::map_stats::MapStatsTracker;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_latency_stats_default() {
        let stats = LatencyStats::default();
        assert_eq!(stats.count, 0);
        assert_eq!(stats.average_nanos(), 0);
        assert_eq!(stats.min_nanos, 0);
        assert_eq!(stats.max_nanos, 0);
    }

    #[test]
    fn test_latency_stats_new() {
        let stats = LatencyStats::new();
        assert_eq!(stats.count, 0);
    }

    #[test]
    fn test_latency_tracker_new() {
        let tracker = LatencyTracker::new();
        let stats = tracker.snapshot();
        assert_eq!(stats.count, 0);
        assert_eq!(stats.min_nanos, 0);
        assert_eq!(stats.max_nanos, 0);
    }

    #[test]
    fn test_latency_tracker_record() {
        let tracker = LatencyTracker::new();
        tracker.record(Duration::from_millis(10));
        tracker.record(Duration::from_millis(20));
        tracker.record(Duration::from_millis(30));

        let stats = tracker.snapshot();
        assert_eq!(stats.count, 3);
        assert_eq!(stats.min_nanos, 10_000_000);
        assert_eq!(stats.max_nanos, 30_000_000);
        assert_eq!(stats.average_nanos(), 20_000_000);
    }

    #[test]
    fn test_latency_tracker_single_record() {
        let tracker = LatencyTracker::new();
        tracker.record(Duration::from_nanos(5_000_000));

        let stats = tracker.snapshot();
        assert_eq!(stats.count, 1);
        assert_eq!(stats.min_nanos, 5_000_000);
        assert_eq!(stats.max_nanos, 5_000_000);
        assert_eq!(stats.average_nanos(), 5_000_000);
    }

    #[test]
    fn test_latency_tracker_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<LatencyTracker>();
    }

    #[test]
    fn test_latency_stats_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<LatencyStats>();
    }

    #[test]
    fn test_latency_stats_millis_conversion() {
        let stats = LatencyStats {
            min_nanos: 1_000_000,
            max_nanos: 5_000_000,
            total_nanos: 15_000_000,
            count: 5,
        };
        assert!((stats.min_millis() - 1.0).abs() < 0.001);
        assert!((stats.max_millis() - 5.0).abs() < 0.001);
        assert!((stats.average_millis() - 3.0).abs() < 0.001);
    }

    #[test]
    fn test_latency_stats_clone() {
        let stats = LatencyStats {
            min_nanos: 100,
            max_nanos: 200,
            total_nanos: 300,
            count: 2,
        };
        let cloned = stats.clone();
        assert_eq!(stats.min_nanos, cloned.min_nanos);
        assert_eq!(stats.max_nanos, cloned.max_nanos);
        assert_eq!(stats.total_nanos, cloned.total_nanos);
        assert_eq!(stats.count, cloned.count);
    }

    #[test]
    fn test_latency_tracker_default() {
        let tracker = LatencyTracker::default();
        let stats = tracker.snapshot();
        assert_eq!(stats.count, 0);
    }

    #[test]
    fn test_latency_stats_zero_count_average() {
        let stats = LatencyStats {
            min_nanos: 0,
            max_nanos: 0,
            total_nanos: 0,
            count: 0,
        };
        assert_eq!(stats.average_nanos(), 0);
        assert_eq!(stats.average_millis(), 0.0);
    }
}
