//! Integration tests for Jet streaming APIs.

use hazelcast_client::{
    jet::ProcessingGuarantee, ClientConfig, HazelcastClient, JobConfig, JobStatus, Pipeline,
};

/// Helper to create a test client.
async fn create_test_client() -> HazelcastClient {
    let config = ClientConfig::builder()
        .cluster_name("dev")
        .build()
        .unwrap();
    HazelcastClient::new(config).await.unwrap()
}

#[test]
fn test_job_status_display() {
    assert_eq!(format!("{}", JobStatus::NotRunning), "NOT_RUNNING");
    assert_eq!(format!("{}", JobStatus::Running), "RUNNING");
    assert_eq!(format!("{}", JobStatus::Completed), "COMPLETED");
    assert_eq!(format!("{}", JobStatus::Failed), "FAILED");
}

#[test]
fn test_job_status_is_terminal() {
    assert!(!JobStatus::NotRunning.is_terminal());
    assert!(!JobStatus::Starting.is_terminal());
    assert!(!JobStatus::Running.is_terminal());
    assert!(!JobStatus::Suspended.is_terminal());
    assert!(JobStatus::Completed.is_terminal());
    assert!(JobStatus::Failed.is_terminal());
}

#[test]
fn test_job_config_builder_defaults() {
    let config = JobConfig::builder().build();
    assert!(config.name().is_none());
    assert!(!config.split_brain_protection());
    assert!(config.auto_scaling());
    assert_eq!(config.snapshot_interval_millis(), 10_000);
}

#[test]
fn test_job_config_builder_custom() {
    let config = JobConfig::builder()
        .name("test-job")
        .split_brain_protection(true)
        .auto_scaling(false)
        .processing_guarantee(ProcessingGuarantee::ExactlyOnce)
        .snapshot_interval_millis(5000)
        .suspend_on_failure(true)
        .build();

    assert_eq!(config.name(), Some("test-job"));
    assert!(config.split_brain_protection());
    assert!(!config.auto_scaling());
    assert_eq!(config.processing_guarantee(), ProcessingGuarantee::ExactlyOnce);
    assert_eq!(config.snapshot_interval_millis(), 5000);
    assert!(config.suspend_on_failure());
}

#[test]
fn test_pipeline_builder() {
    let pipeline = Pipeline::builder()
        .read_from("source")
        .map("transform")
        .filter("filter")
        .write_to("sink")
        .build();

    assert_eq!(pipeline.vertices().len(), 4);
    assert_eq!(pipeline.edges().len(), 3);
}

#[test]
fn test_pipeline_empty() {
    let pipeline = Pipeline::builder().build();
    assert!(pipeline.vertices().is_empty());
    assert!(pipeline.edges().is_empty());
}

#[test]
fn test_processing_guarantee_display() {
    assert_eq!(format!("{}", ProcessingGuarantee::None), "NONE");
    assert_eq!(
        format!("{}", ProcessingGuarantee::AtLeastOnce),
        "AT_LEAST_ONCE"
    );
    assert_eq!(
        format!("{}", ProcessingGuarantee::ExactlyOnce),
        "EXACTLY_ONCE"
    );
}

#[test]
fn test_job_config_with_initial_snapshot() {
    let config = JobConfig::builder()
        .initial_snapshot_name("snapshot-1")
        .build();

    assert_eq!(config.initial_snapshot_name(), Some("snapshot-1"));
}

#[test]
fn test_jet_service_is_send_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<hazelcast_client::JetService>();
}

#[test]
fn test_job_is_send_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<hazelcast_client::Job>();
}

#[test]
fn test_job_status_is_copy() {
    fn assert_copy<T: Copy>() {}
    assert_copy::<JobStatus>();
}

#[tokio::test]
#[ignore = "requires Hazelcast cluster with Jet enabled"]
async fn test_jet_service_access() {
    let client = create_test_client().await;
    let _jet = client.jet();
    client.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires Hazelcast cluster with Jet enabled"]
async fn test_submit_simple_job() {
    let client = create_test_client().await;
    let jet = client.jet();

    let pipeline = Pipeline::builder()
        .read_from("test-source")
        .write_to("test-sink")
        .build();

    let result = jet.submit_job(&pipeline, None).await;
    assert!(result.is_err());

    client.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires Hazelcast cluster with Jet enabled"]
async fn test_get_jobs() {
    let client = create_test_client().await;
    let jet = client.jet();

    let result = jet.get_jobs().await;
    assert!(result.is_err());

    client.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires Hazelcast cluster with Jet enabled"]
async fn test_job_lifecycle() {
    let client = create_test_client().await;
    let jet = client.jet();

    let pipeline = Pipeline::builder()
        .read_from("source-map")
        .map("processor")
        .write_to("sink-map")
        .build();

    let config = JobConfig::builder()
        .name("test-lifecycle-job")
        .processing_guarantee(ProcessingGuarantee::AtLeastOnce)
        .build();

    let job_result = jet.submit_job(&pipeline, Some(config)).await;
    assert!(job_result.is_err());

    client.shutdown().await.unwrap();
}
