//! Job handle for managing Jet streaming jobs.

use std::sync::Arc;

use tokio::sync::RwLock;

use hazelcast_core::{HazelcastError, Result};

use crate::connection::ConnectionManager;
use crate::jet::JobStatus;

/// A handle to a submitted Jet streaming job.
///
/// The `Job` struct provides methods to monitor and control a running job,
/// including checking status, canceling, suspending, and resuming execution.
#[derive(Debug)]
pub struct Job {
    id: i64,
    name: Option<String>,
    connection_manager: Arc<ConnectionManager>,
    status: Arc<RwLock<JobStatus>>,
}

impl Job {
    /// Creates a new job handle.
    pub(crate) fn new(
        id: i64,
        name: Option<String>,
        connection_manager: Arc<ConnectionManager>,
    ) -> Self {
        Self {
            id,
            name,
            connection_manager,
            status: Arc::new(RwLock::new(JobStatus::NotRunning)),
        }
    }

    /// Returns the unique job ID.
    pub fn id(&self) -> i64 {
        self.id
    }

    /// Returns the optional job name.
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    /// Gets the current job status from the cluster.
    ///
    /// # Errors
    ///
    /// Returns an error if communication with the cluster fails.
    pub async fn get_status(&self) -> Result<JobStatus> {
        let _ = &self.connection_manager;
        Err(HazelcastError::Connection(
            "Jet operations not yet implemented".to_string(),
        ))
    }

    /// Cancels the job.
    ///
    /// A cancelled job will transition to the `Failed` state and cannot be resumed.
    ///
    /// # Errors
    ///
    /// Returns an error if the job is already in a terminal state or if
    /// communication with the cluster fails.
    pub async fn cancel(&self) -> Result<()> {
        let _ = &self.connection_manager;
        Err(HazelcastError::Connection(
            "Jet operations not yet implemented".to_string(),
        ))
    }

    /// Suspends the job.
    ///
    /// A suspended job can be later resumed with `resume()`. The job will
    /// create a snapshot before suspending to preserve its state.
    ///
    /// # Errors
    ///
    /// Returns an error if the job is not in a running state or if
    /// communication with the cluster fails.
    pub async fn suspend(&self) -> Result<()> {
        let _ = &self.connection_manager;
        Err(HazelcastError::Connection(
            "Jet operations not yet implemented".to_string(),
        ))
    }

    /// Resumes a suspended job.
    ///
    /// The job will resume from the snapshot taken when it was suspended.
    ///
    /// # Errors
    ///
    /// Returns an error if the job is not in a suspended state or if
    /// communication with the cluster fails.
    pub async fn resume(&self) -> Result<()> {
        let _ = &self.connection_manager;
        Err(HazelcastError::Connection(
            "Jet operations not yet implemented".to_string(),
        ))
    }

    /// Restarts the job.
    ///
    /// The job will be cancelled and resubmitted, potentially losing any
    /// in-flight state not captured in a snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error if the job is in a terminal state or if
    /// communication with the cluster fails.
    pub async fn restart(&self) -> Result<()> {
        let _ = &self.connection_manager;
        Err(HazelcastError::Connection(
            "Jet operations not yet implemented".to_string(),
        ))
    }

    /// Waits for the job to complete.
    ///
    /// This method blocks until the job reaches a terminal state
    /// (`Completed` or `Failed`).
    ///
    /// # Errors
    ///
    /// Returns an error if communication with the cluster fails.
    pub async fn join(&self) -> Result<()> {
        let _ = &self.connection_manager;
        Err(HazelcastError::Connection(
            "Jet operations not yet implemented".to_string(),
        ))
    }

    /// Exports a snapshot of the job state.
    ///
    /// The snapshot can be used to restart the job from a specific point
    /// or to migrate the job to a different cluster.
    ///
    /// # Arguments
    ///
    /// * `name` - The name to give the exported snapshot
    ///
    /// # Errors
    ///
    /// Returns an error if the job is not in a running state or if
    /// communication with the cluster fails.
    pub async fn export_snapshot(&self, name: &str) -> Result<()> {
        let _ = &self.connection_manager;
        let _ = name;
        Err(HazelcastError::Connection(
            "Jet operations not yet implemented".to_string(),
        ))
    }
}

unsafe impl Send for Job {}
unsafe impl Sync for Job {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_job_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Job>();
    }
}
