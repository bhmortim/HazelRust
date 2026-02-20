//! Jet service for submitting and managing streaming jobs.

use std::sync::Arc;

use hazelcast_core::protocol::constants::{
    JET_GET_JOB_IDS, JET_GET_JOB_STATUS, JET_GET_JOB_SUMMARY_LIST, JET_SUBMIT_JOB,
    JET_TERMINATE_JOB, RESPONSE_HEADER_SIZE,
};
use hazelcast_core::{ClientMessage, Frame, HazelcastError, Result};

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
    pub async fn submit_job(&self, pipeline: &Pipeline, config: Option<JobConfig>) -> Result<Job> {
        let config = config.unwrap_or_default();

        let mut message = ClientMessage::new_request(JET_SUBMIT_JOB);

        let job_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as i64)
            .unwrap_or(0);
        let id_bytes = job_id.to_le_bytes();
        message.add_frame(Frame::new_data_frame(&id_bytes));

        let pipeline_data = self.serialize_pipeline(pipeline)?;
        message.add_frame(Frame::new_data_frame(&pipeline_data));

        let config_data = self.serialize_config(&config)?;
        message.add_frame(Frame::new_data_frame(&config_data));

        if let Some(name) = config.name() {
            message.add_frame(Frame::new_string_frame(name));
        } else {
            message.add_frame(Frame::new_data_frame(&[]));
        }

        self.connection_manager.send(message).await?;

        Ok(Job::with_status(
            job_id,
            config.name().map(String::from),
            Arc::clone(&self.connection_manager),
            JobStatus::Starting,
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
        let status = self.get_job_status(job_id).await;

        match status {
            Ok(status) => Ok(Some(Job::with_status(
                job_id,
                None,
                Arc::clone(&self.connection_manager),
                status,
            ))),
            Err(HazelcastError::IllegalState(_)) => Ok(None),
            Err(e) => Err(e),
        }
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
        let jobs = self.get_jobs().await?;
        Ok(jobs.into_iter().find(|j| j.name() == Some(name)))
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
        let message = ClientMessage::new_request(JET_GET_JOB_SUMMARY_LIST);

        let response = self.connection_manager.send(message).await?;

        self.decode_job_list(&response)
    }

    /// Gets the IDs of all jobs in the cluster.
    ///
    /// # Returns
    ///
    /// Returns a vector of job IDs.
    ///
    /// # Errors
    ///
    /// Returns an error if communication with the cluster fails.
    pub async fn get_job_ids(&self) -> Result<Vec<i64>> {
        let mut message = ClientMessage::new_request(JET_GET_JOB_IDS);

        message.add_frame(Frame::new_data_frame(&[1u8]));

        let response = self.connection_manager.send(message).await?;

        self.decode_job_ids(&response)
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
        let mut message = ClientMessage::new_request(JET_GET_JOB_STATUS);

        let id_bytes = job_id.to_le_bytes();
        message.add_frame(Frame::new_data_frame(&id_bytes));

        let response = self.connection_manager.send(message).await?;
        let status_value = self.decode_int_response(&response)?;

        JobStatus::from_wire_format(status_value)
            .ok_or_else(|| HazelcastError::IllegalState(format!("unknown job status: {}", status_value)))
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
        let mut message = ClientMessage::new_request(JET_TERMINATE_JOB);

        let id_bytes = job_id.to_le_bytes();
        message.add_frame(Frame::new_data_frame(&id_bytes));

        message.add_frame(Frame::new_data_frame(&[0u8]));

        message.add_frame(Frame::new_data_frame(&[0u8]));

        self.connection_manager.send(message).await?;
        Ok(())
    }

    fn serialize_pipeline(&self, pipeline: &Pipeline) -> Result<Vec<u8>> {
        let mut data = Vec::new();

        let vertex_count = pipeline.vertex_count() as i32;
        data.extend_from_slice(&vertex_count.to_le_bytes());

        for vertex in pipeline.vertices() {
            let name_bytes = vertex.name().as_bytes();
            data.extend_from_slice(&(name_bytes.len() as i32).to_le_bytes());
            data.extend_from_slice(name_bytes);

            data.extend_from_slice(&vertex.local_parallelism().to_le_bytes());
        }

        let edge_count = pipeline.edge_count() as i32;
        data.extend_from_slice(&edge_count.to_le_bytes());

        for (from, to) in pipeline.edges() {
            data.extend_from_slice(&(*from as i32).to_le_bytes());
            data.extend_from_slice(&(*to as i32).to_le_bytes());
        }

        Ok(data)
    }

    fn serialize_config(&self, config: &JobConfig) -> Result<Vec<u8>> {
        let mut data = Vec::new();

        data.extend_from_slice(&(config.processing_guarantee() as i32).to_le_bytes());

        data.extend_from_slice(&config.snapshot_interval_millis().to_le_bytes());

        data.push(if config.auto_scaling() { 1 } else { 0 });

        data.push(if config.split_brain_protection() { 1 } else { 0 });

        data.push(if config.suspend_on_failure() { 1 } else { 0 });

        data.extend_from_slice(&config.max_concurrent_operations().to_le_bytes());

        Ok(data)
    }

    fn decode_int_response(&self, response: &ClientMessage) -> Result<i32> {
        let frame = response
            .initial_frame()
            .ok_or_else(|| HazelcastError::Serialization("missing initial frame".to_string()))?;
        if frame.content().len() >= RESPONSE_HEADER_SIZE + 4 {
            let bytes: [u8; 4] = frame.content()[RESPONSE_HEADER_SIZE..RESPONSE_HEADER_SIZE + 4]
                .try_into()
                .map_err(|_| HazelcastError::Serialization("invalid int response".to_string()))?;
            Ok(i32::from_le_bytes(bytes))
        } else {
            Ok(0)
        }
    }

    fn decode_job_ids(&self, response: &ClientMessage) -> Result<Vec<i64>> {
        let frame = response
            .initial_frame()
            .ok_or_else(|| HazelcastError::Serialization("missing initial frame".to_string()))?;
        let content = frame.content();
        let mut job_ids = Vec::new();

        if content.len() > RESPONSE_HEADER_SIZE {
            let mut offset = RESPONSE_HEADER_SIZE;

            if offset + 4 <= content.len() {
                let count_bytes: [u8; 4] = content[offset..offset + 4]
                    .try_into()
                    .map_err(|_| HazelcastError::Serialization("invalid count".to_string()))?;
                let count = i32::from_le_bytes(count_bytes) as usize;
                offset += 4;

                for _ in 0..count {
                    if offset + 8 <= content.len() {
                        let id_bytes: [u8; 8] = content[offset..offset + 8]
                            .try_into()
                            .map_err(|_| HazelcastError::Serialization("invalid job id".to_string()))?;
                        job_ids.push(i64::from_le_bytes(id_bytes));
                        offset += 8;
                    }
                }
            }
        }

        Ok(job_ids)
    }

    fn decode_job_list(&self, response: &ClientMessage) -> Result<Vec<Job>> {
        let job_ids = self.decode_job_ids(response)?;
        Ok(job_ids
            .into_iter()
            .map(|id| Job::new(id, None, Arc::clone(&self.connection_manager)))
            .collect())
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

    #[test]
    fn test_serialize_empty_pipeline() {
        use crate::config::ClientConfigBuilder;
        use crate::connection::discovery::StaticAddressDiscovery;

        let config = ClientConfigBuilder::new().build().unwrap();
        let discovery = StaticAddressDiscovery::default();
        let manager = Arc::new(crate::connection::ConnectionManager::new(config, discovery));
        let service = JetService::new(manager);

        let pipeline = Pipeline::builder().build();
        let data = service.serialize_pipeline(&pipeline).unwrap();

        assert_eq!(data.len(), 8);
        assert_eq!(&data[0..4], &0i32.to_le_bytes());
        assert_eq!(&data[4..8], &0i32.to_le_bytes());
    }

    #[test]
    fn test_serialize_config() {
        use crate::config::ClientConfigBuilder;
        use crate::connection::discovery::StaticAddressDiscovery;
        use crate::jet::ProcessingGuarantee;

        let config = ClientConfigBuilder::new().build().unwrap();
        let discovery = StaticAddressDiscovery::default();
        let manager = Arc::new(crate::connection::ConnectionManager::new(config, discovery));
        let service = JetService::new(manager);

        let job_config = JobConfig::builder()
            .processing_guarantee(ProcessingGuarantee::ExactlyOnce)
            .auto_scaling(false)
            .build();

        let data = service.serialize_config(&job_config).unwrap();
        assert!(!data.is_empty());
    }
}
