//! Slow operation detection and logging.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Detects and logs slow operations based on configurable thresholds.
///
/// The detector tracks operation durations and emits warnings via `tracing`
/// when operations exceed the configured threshold.
#[derive(Debug)]
pub struct SlowOperationDetector {
    threshold: Duration,
    total_operations: AtomicU64,
    slow_operations: AtomicU64,
}

impl SlowOperationDetector {
    /// Creates a new detector with the specified threshold.
    pub fn new(threshold: Duration) -> Self {
        Self {
            threshold,
            total_operations: AtomicU64::new(0),
            slow_operations: AtomicU64::new(0),
        }
    }

    /// Returns the configured slow operation threshold.
    pub fn threshold(&self) -> Duration {
        self.threshold
    }

    /// Starts tracking an operation.
    ///
    /// Returns a tracker that will automatically log a warning if the operation
    /// exceeds the threshold when dropped or finished.
    pub fn start_operation(&self, name: impl Into<String>) -> OperationTracker<'_> {
        self.total_operations.fetch_add(1, Ordering::Relaxed);
        OperationTracker {
            detector: self,
            name: name.into(),
            start: Instant::now(),
            finished: false,
        }
    }

    /// Records an operation's duration manually.
    ///
    /// Use this when you have already measured the duration and want to
    /// check it against the threshold.
    pub fn record(&self, name: &str, duration: Duration) {
        self.total_operations.fetch_add(1, Ordering::Relaxed);
        self.check_and_warn(name, duration);
    }

    /// Returns the total number of operations tracked.
    pub fn total_operations(&self) -> u64 {
        self.total_operations.load(Ordering::Relaxed)
    }

    /// Returns the number of slow operations detected.
    pub fn slow_operations(&self) -> u64 {
        self.slow_operations.load(Ordering::Relaxed)
    }

    /// Resets the operation counters.
    pub fn reset(&self) {
        self.total_operations.store(0, Ordering::Relaxed);
        self.slow_operations.store(0, Ordering::Relaxed);
    }

    fn check_and_warn(&self, name: &str, duration: Duration) {
        if duration > self.threshold {
            self.slow_operations.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(
                operation = %name,
                duration_ms = duration.as_millis(),
                threshold_ms = self.threshold.as_millis(),
                "Slow operation detected"
            );
        }
    }
}

/// Tracks the duration of an operation.
///
/// When dropped or finished, logs a warning if the operation exceeded the threshold.
#[derive(Debug)]
pub struct OperationTracker<'a> {
    detector: &'a SlowOperationDetector,
    name: String,
    start: Instant,
    finished: bool,
}

impl<'a> OperationTracker<'a> {
    /// Returns the elapsed duration since the operation started.
    pub fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }

    /// Returns the operation name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Finishes tracking and returns the duration.
    ///
    /// This consumes the tracker and checks the duration against the threshold.
    pub fn finish(mut self) -> Duration {
        self.finished = true;
        let duration = self.start.elapsed();
        self.detector.check_and_warn(&self.name, duration);
        duration
    }
}

impl Drop for OperationTracker<'_> {
    fn drop(&mut self) {
        if !self.finished {
            let duration = self.start.elapsed();
            self.detector.check_and_warn(&self.name, duration);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_new_detector() {
        let detector = SlowOperationDetector::new(Duration::from_millis(100));
        assert_eq!(detector.threshold(), Duration::from_millis(100));
        assert_eq!(detector.total_operations(), 0);
        assert_eq!(detector.slow_operations(), 0);
    }

    #[test]
    fn test_record_fast_operation() {
        let detector = SlowOperationDetector::new(Duration::from_millis(100));
        detector.record("fast_op", Duration::from_millis(10));

        assert_eq!(detector.total_operations(), 1);
        assert_eq!(detector.slow_operations(), 0);
    }

    #[test]
    fn test_record_slow_operation() {
        let detector = SlowOperationDetector::new(Duration::from_millis(100));
        detector.record("slow_op", Duration::from_millis(150));

        assert_eq!(detector.total_operations(), 1);
        assert_eq!(detector.slow_operations(), 1);
    }

    #[test]
    fn test_record_at_threshold_not_slow() {
        let detector = SlowOperationDetector::new(Duration::from_millis(100));
        detector.record("exact_op", Duration::from_millis(100));

        assert_eq!(detector.total_operations(), 1);
        assert_eq!(detector.slow_operations(), 0);
    }

    #[test]
    fn test_start_operation_tracker() {
        let detector = SlowOperationDetector::new(Duration::from_secs(10));
        let tracker = detector.start_operation("test_op");

        assert_eq!(tracker.name(), "test_op");
        assert!(tracker.elapsed() < Duration::from_millis(100));
        assert_eq!(detector.total_operations(), 1);

        drop(tracker);
        assert_eq!(detector.slow_operations(), 0);
    }

    #[test]
    fn test_tracker_finish() {
        let detector = SlowOperationDetector::new(Duration::from_secs(10));
        let tracker = detector.start_operation("finish_test");

        let duration = tracker.finish();
        assert!(duration < Duration::from_millis(100));
        assert_eq!(detector.total_operations(), 1);
        assert_eq!(detector.slow_operations(), 0);
    }

    #[test]
    fn test_tracker_detects_slow_on_drop() {
        let detector = SlowOperationDetector::new(Duration::from_millis(5));
        {
            let _tracker = detector.start_operation("slow_drop_op");
            thread::sleep(Duration::from_millis(10));
        }

        assert_eq!(detector.total_operations(), 1);
        assert_eq!(detector.slow_operations(), 1);
    }

    #[test]
    fn test_tracker_detects_slow_on_finish() {
        let detector = SlowOperationDetector::new(Duration::from_millis(5));
        let tracker = detector.start_operation("slow_finish_op");
        thread::sleep(Duration::from_millis(10));
        let duration = tracker.finish();

        assert!(duration >= Duration::from_millis(10));
        assert_eq!(detector.total_operations(), 1);
        assert_eq!(detector.slow_operations(), 1);
    }

    #[test]
    fn test_multiple_operations() {
        let detector = SlowOperationDetector::new(Duration::from_millis(50));

        detector.record("op1", Duration::from_millis(10));
        detector.record("op2", Duration::from_millis(100));
        detector.record("op3", Duration::from_millis(20));
        detector.record("op4", Duration::from_millis(200));

        assert_eq!(detector.total_operations(), 4);
        assert_eq!(detector.slow_operations(), 2);
    }

    #[test]
    fn test_reset() {
        let detector = SlowOperationDetector::new(Duration::from_millis(50));
        detector.record("op1", Duration::from_millis(10));
        detector.record("op2", Duration::from_millis(100));

        assert_eq!(detector.total_operations(), 2);
        assert_eq!(detector.slow_operations(), 1);

        detector.reset();

        assert_eq!(detector.total_operations(), 0);
        assert_eq!(detector.slow_operations(), 0);
    }

    #[test]
    fn test_detector_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<SlowOperationDetector>();
    }

    #[test]
    fn test_zero_threshold() {
        let detector = SlowOperationDetector::new(Duration::ZERO);
        detector.record("any_op", Duration::from_nanos(1));

        assert_eq!(detector.slow_operations(), 1);
    }

    #[test]
    fn test_operation_name_ownership() {
        let detector = SlowOperationDetector::new(Duration::from_secs(10));
        let name = String::from("owned_name");
        let tracker = detector.start_operation(name);

        assert_eq!(tracker.name(), "owned_name");
        tracker.finish();
    }

    #[test]
    fn test_tracker_elapsed_increases() {
        let detector = SlowOperationDetector::new(Duration::from_secs(10));
        let tracker = detector.start_operation("elapsed_test");

        let first = tracker.elapsed();
        thread::sleep(Duration::from_millis(5));
        let second = tracker.elapsed();

        assert!(second > first);
        tracker.finish();
    }
}
