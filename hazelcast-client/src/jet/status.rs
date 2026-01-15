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
}
