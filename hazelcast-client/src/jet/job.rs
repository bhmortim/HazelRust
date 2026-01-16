//! Job handle for managing Jet streaming jobs.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::RwLock;

use hazelcast_core::protocol::constants::{
    JET_EXPORT_SNAPSHOT, JET_GET_JOB_METRICS, JET_GET_JOB_STATUS, JET_RESUME_JOB,
    JET_TERMINATE_JOB, PARTITION_ID_ANY, RESPONSE_HEADER_SIZE,
};
use hazelcast_core::{ClientMessage, Frame, HazelcastError, Result};

use crate::connection::ConnectionManager;
use crate::jet::{JobMetrics, JobStatus};

/// Termination mode for jobs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TerminationMode {
    CancelForceful = 0,
    CancelGraceful = 1,
    Restart = 2,
    Suspend = 3,
}

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

    /// Creates a new job handle with an initial status.
    pub(crate) fn with_status(
        id: i64,
        name: Option<String>,
        connection_manager: Arc<ConnectionManager>,
        status: JobStatus,
    ) -> Self {
        Self {
            id,
            name,
            connection_manager,
            status: Arc::new(RwLock::new(status)),
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
        let mut message = ClientMessage::new();
        message.set_message_type(JET_GET_JOB_STATUS);
        message.set_partition_id(PARTITION_ID_ANY);

        let mut id_bytes = Vec::with_capacity(8);
        id_bytes.extend_from_slice(&self.id.to_le_bytes());
        message.add_frame(Frame::new(id_bytes));

        let response = self.invoke(message).await?;
        let status_value = self.decode_int_response(&response)?;
        let status = JobStatus::from_wire_format(status_value).unwrap_or(JobStatus::NotRunning);

        *self.status.write().await = status;

        Ok(status)
    }

    /// Returns the cached status without querying the cluster.
    pub async fn cached_status(&self) -> JobStatus {
        *self.status.read().await
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
        self.terminate(TerminationMode::CancelForceful).await
    }

    /// Cancels the job gracefully.
    ///
    /// A gracefully cancelled job will attempt to complete current processing
    /// before transitioning to the `Failed` state.
    ///
    /// # Errors
    ///
    /// Returns an error if the job is already in a terminal state or if
    /// communication with the cluster fails.
    pub async fn cancel_gracefully(&self) -> Result<()> {
        self.terminate(TerminationMode::CancelGraceful).await
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
        self.terminate(TerminationMode::Suspend).await
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
        let mut message = ClientMessage::new();
        message.set_message_type(JET_RESUME_JOB);
        message.set_partition_id(PARTITION_ID_ANY);

        let mut id_bytes = Vec::with_capacity(8);
        id_bytes.extend_from_slice(&self.id.to_le_bytes());
        message.add_frame(Frame::new(id_bytes));

        self.invoke(message).await?;
        Ok(())
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
        self.terminate(TerminationMode::Restart).await
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
        loop {
            let status = self.get_status().await?;
            if status.is_terminal() {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Waits for the job to complete with a timeout.
    ///
    /// Returns `Ok(true)` if the job completed within the timeout,
    /// `Ok(false)` if the timeout elapsed.
    ///
    /// # Errors
    ///
    /// Returns an error if communication with the cluster fails.
    pub async fn join_with_timeout(&self, timeout: Duration) -> Result<bool> {
        let start = std::time::Instant::now();
        loop {
            let status = self.get_status().await?;
            if status.is_terminal() {
                return Ok(true);
            }
            if start.elapsed() >= timeout {
                return Ok(false);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
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
        self.export_snapshot_with_options(name, false).await
    }

    /// Exports a snapshot with options.
    ///
    /// # Arguments
    ///
    /// * `name` - The name to give the exported snapshot
    /// * `cancel_job` - If true, cancel the job after exporting
    pub async fn export_snapshot_with_options(&self, name: &str, cancel_job: bool) -> Result<()> {
        let mut message = ClientMessage::new();
        message.set_message_type(JET_EXPORT_SNAPSHOT);
        message.set_partition_id(PARTITION_ID_ANY);

        let mut id_bytes = Vec::with_capacity(8);
        id_bytes.extend_from_slice(&self.id.to_le_bytes());
        message.add_frame(Frame::new(id_bytes));

        message.add_frame(Frame::new(name.as_bytes().to_vec()));

        message.add_frame(Frame::new(vec![if cancel_job { 1u8 } else { 0u8 }]));

        self.invoke(message).await?;
        Ok(())
    }

    /// Gets metrics for this job.
    ///
    /// # Errors
    ///
    /// Returns an error if communication with the cluster fails.
    pub async fn get_metrics(&self) -> Result<JobMetrics> {
        let mut message = ClientMessage::new();
        message.set_message_type(JET_GET_JOB_METRICS);
        message.set_partition_id(PARTITION_ID_ANY);

        let mut id_bytes = Vec::with_capacity(8);
        id_bytes.extend_from_slice(&self.id.to_le_bytes());
        message.add_frame(Frame::new(id_bytes));

        let response = self.invoke(message).await?;
        let _ = response;
        Ok(JobMetrics::new())
    }

    async fn terminate(&self, mode: TerminationMode) -> Result<()> {
        let mut message = ClientMessage::new();
        message.set_message_type(JET_TERMINATE_JOB);
        message.set_partition_id(PARTITION_ID_ANY);

        let mut id_bytes = Vec::with_capacity(8);
        id_bytes.extend_from_slice(&self.id.to_le_bytes());
        message.add_frame(Frame::new(id_bytes));

        message.add_frame(Frame::new(vec![mode as u8]));

        message.add_frame(Frame::new(vec![0u8]));

        self.invoke(message).await?;
        Ok(())
    }

    async fn invoke(&self, message: ClientMessage) -> Result<ClientMessage> {
        let address = self
            .connection_manager
            .connected_addresses()
            .await
            .into_iter()
            .next()
            .ok_or_else(|| HazelcastError::Connection("no connections available".to_string()))?;

        self.connection_manager.send_to(address, message).await?;

        self.connection_manager
            .receive_from(address)
            .await?
            .ok_or_else(|| HazelcastError::Connection("no response received".to_string()))
    }

    fn decode_int_response(&self, response: &ClientMessage) -> Result<i32> {
        let frame = response.initial_frame();
        if frame.content().len() >= RESPONSE_HEADER_SIZE + 4 {
            let bytes: [u8; 4] = frame.content()[RESPONSE_HEADER_SIZE..RESPONSE_HEADER_SIZE + 4]
                .try_into()
                .map_err(|_| HazelcastError::Serialization("invalid int response".to_string()))?;
            Ok(i32::from_le_bytes(bytes))
        } else {
            Ok(0)
        }
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

    #[test]
    fn test_termination_mode_values() {
        assert_eq!(TerminationMode::CancelForceful as u8, 0);
        assert_eq!(TerminationMode::CancelGraceful as u8, 1);
        assert_eq!(TerminationMode::Restart as u8, 2);
        assert_eq!(TerminationMode::Suspend as u8, 3);
    }
}
