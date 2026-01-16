//! Job status types for Jet streaming jobs.

use std::fmt;

/// Status of a Jet streaming job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum JobStatus {
    /// Job has not started yet.
    NotRunning,
    /// Job is starting.
    Starting,
    /// Job is currently running.
    Running,
    /// Job is suspended.
    Suspended,
    /// Job is suspended while exporting snapshot.
    SuspendedExportingSnapshot,
    /// Job is completing.
    Completing,
    /// Job has failed.
    Failed,
    /// Job completed successfully.
    Completed,
}

impl JobStatus {
    /// Returns true if this status represents a terminal state.
    ///
    /// Terminal states are `Failed` and `Completed`, indicating that
    /// the job will not transition to any other state.
    pub fn is_terminal(&self) -> bool {
        matches!(self, JobStatus::Failed | JobStatus::Completed)
    }

    /// Converts a wire format integer to a JobStatus.
    ///
    /// Returns `None` if the value doesn't correspond to a known status.
    pub fn from_wire_format(value: i32) -> Option<Self> {
        match value {
            0 => Some(JobStatus::NotRunning),
            1 => Some(JobStatus::Starting),
            2 => Some(JobStatus::Running),
            3 => Some(JobStatus::Suspended),
            4 => Some(JobStatus::SuspendedExportingSnapshot),
            5 => Some(JobStatus::Completing),
            6 => Some(JobStatus::Failed),
            7 => Some(JobStatus::Completed),
            _ => None,
        }
    }

    /// Converts this JobStatus to its wire format integer.
    pub fn to_wire_format(&self) -> i32 {
        match self {
            JobStatus::NotRunning => 0,
            JobStatus::Starting => 1,
            JobStatus::Running => 2,
            JobStatus::Suspended => 3,
            JobStatus::SuspendedExportingSnapshot => 4,
            JobStatus::Completing => 5,
            JobStatus::Failed => 6,
            JobStatus::Completed => 7,
        }
    }
}

/// Metrics for a Jet job.
#[derive(Debug, Clone, Default)]
pub struct JobMetrics {
    received_count: std::collections::HashMap<String, i64>,
    emitted_count: std::collections::HashMap<String, i64>,
    queue_sizes: std::collections::HashMap<String, i64>,
    queue_capacities: std::collections::HashMap<String, i64>,
    processing_latency_ns: i64,
    snapshot_time_ms: i64,
}

impl JobMetrics {
    /// Creates new empty job metrics.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the received count for a vertex.
    pub fn received_count(&self, vertex: &str) -> Option<i64> {
        self.received_count.get(vertex).copied()
    }

    /// Returns the emitted count for a vertex.
    pub fn emitted_count(&self, vertex: &str) -> Option<i64> {
        self.emitted_count.get(vertex).copied()
    }

    /// Returns the queue size for a vertex.
    pub fn queue_size(&self, vertex: &str) -> Option<i64> {
        self.queue_sizes.get(vertex).copied()
    }

    /// Returns the queue capacity for a vertex.
    pub fn queue_capacity(&self, vertex: &str) -> Option<i64> {
        self.queue_capacities.get(vertex).copied()
    }

    /// Returns the processing latency in nanoseconds.
    pub fn processing_latency_ns(&self) -> i64 {
        self.processing_latency_ns
    }

    /// Returns the snapshot time in milliseconds.
    pub fn snapshot_time_ms(&self) -> i64 {
        self.snapshot_time_ms
    }

    /// Returns all vertex names that have metrics.
    pub fn vertex_names(&self) -> Vec<&str> {
        let mut names: std::collections::HashSet<&str> = std::collections::HashSet::new();
        for key in self.received_count.keys() {
            names.insert(key.as_str());
        }
        for key in self.emitted_count.keys() {
            names.insert(key.as_str());
        }
        names.into_iter().collect()
    }

    /// Sets the received count for a vertex.
    pub fn set_received_count(&mut self, vertex: impl Into<String>, count: i64) {
        self.received_count.insert(vertex.into(), count);
    }

    /// Sets the emitted count for a vertex.
    pub fn set_emitted_count(&mut self, vertex: impl Into<String>, count: i64) {
        self.emitted_count.insert(vertex.into(), count);
    }

    /// Sets the queue size for a vertex.
    pub fn set_queue_size(&mut self, vertex: impl Into<String>, size: i64) {
        self.queue_sizes.insert(vertex.into(), size);
    }

    /// Sets the queue capacity for a vertex.
    pub fn set_queue_capacity(&mut self, vertex: impl Into<String>, capacity: i64) {
        self.queue_capacities.insert(vertex.into(), capacity);
    }

    /// Sets the processing latency.
    pub fn set_processing_latency_ns(&mut self, latency: i64) {
        self.processing_latency_ns = latency;
    }

    /// Sets the snapshot time.
    pub fn set_snapshot_time_ms(&mut self, time: i64) {
        self.snapshot_time_ms = time;
    }
}

impl fmt::Display for JobStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            JobStatus::NotRunning => "NOT_RUNNING",
            JobStatus::Starting => "STARTING",
            JobStatus::Running => "RUNNING",
            JobStatus::Suspended => "SUSPENDED",
            JobStatus::SuspendedExportingSnapshot => "SUSPENDED_EXPORTING_SNAPSHOT",
            JobStatus::Completing => "COMPLETING",
            JobStatus::Failed => "FAILED",
            JobStatus::Completed => "COMPLETED",
        };
        write!(f, "{}", s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_job_status_display() {
        assert_eq!(format!("{}", JobStatus::NotRunning), "NOT_RUNNING");
        assert_eq!(format!("{}", JobStatus::Starting), "STARTING");
        assert_eq!(format!("{}", JobStatus::Running), "RUNNING");
        assert_eq!(format!("{}", JobStatus::Suspended), "SUSPENDED");
        assert_eq!(
            format!("{}", JobStatus::SuspendedExportingSnapshot),
            "SUSPENDED_EXPORTING_SNAPSHOT"
        );
        assert_eq!(format!("{}", JobStatus::Completing), "COMPLETING");
        assert_eq!(format!("{}", JobStatus::Failed), "FAILED");
        assert_eq!(format!("{}", JobStatus::Completed), "COMPLETED");
    }

    #[test]
    fn test_job_status_is_terminal() {
        assert!(!JobStatus::NotRunning.is_terminal());
        assert!(!JobStatus::Starting.is_terminal());
        assert!(!JobStatus::Running.is_terminal());
        assert!(!JobStatus::Suspended.is_terminal());
        assert!(!JobStatus::SuspendedExportingSnapshot.is_terminal());
        assert!(!JobStatus::Completing.is_terminal());
        assert!(JobStatus::Failed.is_terminal());
        assert!(JobStatus::Completed.is_terminal());
    }

    #[test]
    fn test_job_status_is_copy() {
        fn assert_copy<T: Copy>() {}
        assert_copy::<JobStatus>();
    }

    #[test]
    fn test_job_status_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<JobStatus>();
    }

    #[test]
    fn test_job_status_wire_format_roundtrip() {
        let statuses = [
            JobStatus::NotRunning,
            JobStatus::Starting,
            JobStatus::Running,
            JobStatus::Suspended,
            JobStatus::SuspendedExportingSnapshot,
            JobStatus::Completing,
            JobStatus::Failed,
            JobStatus::Completed,
        ];

        for status in statuses {
            let wire = status.to_wire_format();
            let converted = JobStatus::from_wire_format(wire);
            assert_eq!(converted, Some(status));
        }
    }

    #[test]
    fn test_job_status_from_wire_format_invalid() {
        assert!(JobStatus::from_wire_format(-1).is_none());
        assert!(JobStatus::from_wire_format(100).is_none());
    }

    #[test]
    fn test_job_metrics_default() {
        let metrics = JobMetrics::new();
        assert_eq!(metrics.processing_latency_ns(), 0);
        assert_eq!(metrics.snapshot_time_ms(), 0);
        assert!(metrics.vertex_names().is_empty());
    }

    #[test]
    fn test_job_metrics_setters_and_getters() {
        let mut metrics = JobMetrics::new();
        metrics.set_received_count("source", 100);
        metrics.set_emitted_count("source", 95);
        metrics.set_queue_size("sink", 10);
        metrics.set_queue_capacity("sink", 1000);
        metrics.set_processing_latency_ns(500_000);
        metrics.set_snapshot_time_ms(1234567890);

        assert_eq!(metrics.received_count("source"), Some(100));
        assert_eq!(metrics.emitted_count("source"), Some(95));
        assert_eq!(metrics.queue_size("sink"), Some(10));
        assert_eq!(metrics.queue_capacity("sink"), Some(1000));
        assert_eq!(metrics.processing_latency_ns(), 500_000);
        assert_eq!(metrics.snapshot_time_ms(), 1234567890);
        assert!(metrics.vertex_names().contains(&"source"));
    }

    #[test]
    fn test_job_metrics_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<JobMetrics>();
    }
}
