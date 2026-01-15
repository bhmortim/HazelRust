//! Jet service for submitting and managing streaming jobs.

use std::sync::Arc;

use hazelcast_core::{HazelcastError, Result};

use crate::connection::ConnectionManager;
use crate::jet::{Job, JobConfig, JobStatus, Pipeline};

/// Service for submitting and managing Jet streaming jobs.
///
/// The `JetService` provides methods to submit new jobs, retrieve existing jobs,
/// and manage job lifecycle operations.
#[derive(Debug, Clone)]
pub struct JetService {
    connection_manager: Arc<ConnectionManager>,
}

impl JetService {
    /// Creates a new Jet service.
    pub fn new(connection_manager: Arc<ConnectionManager>) -> Self {
        Self { connection_manager }
    }

    /// Submits a new job with the given pipeline.
    ///
    /// # Arguments
    ///
    /// * `pipeline` - The pipeline definition to execute
    /// * `config` - Optional job configuration; if `None`, defaults are used
    ///
    /// # Returns
    ///
    /// Returns a `Job` handle that can be used to monitor and control the job.
    ///
    /// # Errors
    ///
    /// Returns an error if the pipeline is invalid or if communication with
    /// the cluster fails.
    pub async fn submit_job(
        &self,
        pipeline: &Pipeline,
        config: Option<JobConfig>,
    ) -> Result<Job> {
        let _ = pipeline;
        let _ = config;
        Err(HazelcastError::Connection(
            "Jet operations not yet implemented".to_string(),
        ))
    }

    /// Gets a job by its ID.
    ///
    /// # Arguments
    ///
    /// * `job_id` - The unique job identifier
    ///
    /// # Returns
    ///
    /// Returns `Some(Job)` if the job exists, `None` otherwise.
    ///
    /// # Errors
    ///
    /// Returns an error if communication with the cluster fails.
    pub async fn get_job(&self, job_id: i64) -> Result<Option<Job>> {
        let _ = job_id;
        Err(HazelcastError::Connection(
            "Jet operations not yet implemented".to_string(),
        ))
    }

    /// Gets a job by its name.
    ///
    /// # Arguments
    ///
    /// * `name` - The job name
    ///
    /// # Returns
    ///
    /// Returns `Some(Job)` if a job with the given name exists, `None` otherwise.
    /// If multiple jobs have the same name, returns the most recently submitted one.
    ///
    /// # Errors
    ///
    /// Returns an error if communication with the cluster fails.
    pub async fn get_job_by_name(&self, name: &str) -> Result<Option<Job>> {
        let _ = name;
        Err(HazelcastError::Connection(
            "Jet operations not yet implemented".to_string(),
        ))
    }

    /// Lists all jobs in the cluster.
    ///
    /// # Returns
    ///
    /// Returns a vector of all jobs, including completed and failed jobs.
    ///
    /// # Errors
    ///
    /// Returns an error if communication with the cluster fails.
    pub async fn get_jobs(&self) -> Result<Vec<Job>> {
        Err(HazelcastError::Connection(
            "Jet operations not yet implemented".to_string(),
        ))
    }

    /// Gets the status of a job by its ID.
    ///
    /// # Arguments
    ///
    /// * `job_id` - The unique job identifier
    ///
    /// # Returns
    ///
    /// Returns the current status of the job.
    ///
    /// # Errors
    ///
    /// Returns an error if the job does not exist or if communication
    /// with the cluster fails.
    pub async fn get_job_status(&self, job_id: i64) -> Result<JobStatus> {
        let _ = job_id;
        Err(HazelcastError::Connection(
            "Jet operations not yet implemented".to_string(),
        ))
    }

    /// Cancels a job by its ID.
    ///
    /// # Arguments
    ///
    /// * `job_id` - The unique job identifier
    ///
    /// # Errors
    ///
    /// Returns an error if the job does not exist, is already in a terminal
    /// state, or if communication with the cluster fails.
    pub async fn cancel_job(&self, job_id: i64) -> Result<()> {
        let _ = job_id;
        Err(HazelcastError::Connection(
            "Jet operations not yet implemented".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_jet_service_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<JetService>();
    }
}
