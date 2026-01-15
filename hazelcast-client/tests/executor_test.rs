//! Integration tests for distributed executor service.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use hazelcast_core::{Deserializable, HazelcastError, ObjectDataInput, ObjectDataOutput, Result, Serializable};

/// Example callable task for testing.
/// Note: Server-side implementation must be registered.
#[derive(Debug, Clone)]
pub struct EchoTask {
    pub message: String,
}

impl EchoTask {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl Serializable for EchoTask {
    fn serialize(&self, output: &mut dyn ObjectDataOutput) -> Result<()> {
        output.write_string(&self.message)
    }
}

impl hazelcast_client::executor::Callable<String> for EchoTask {
    fn factory_id(&self) -> i32 {
        1 // Must match server-side factory
    }

    fn class_id(&self) -> i32 {
        1 // Must match server-side class
    }
}

/// Test callback implementation for async result handling.
pub struct TestCallback {
    success_count: AtomicU32,
    failure_count: AtomicU32,
}

impl TestCallback {
    pub fn new() -> Self {
        Self {
            success_count: AtomicU32::new(0),
            failure_count: AtomicU32::new(0),
        }
    }

    pub fn success_count(&self) -> u32 {
        self.success_count.load(Ordering::SeqCst)
    }

    pub fn failure_count(&self) -> u32 {
        self.failure_count.load(Ordering::SeqCst)
    }
}

impl hazelcast_client::executor::ExecutionCallback<String> for TestCallback {
    fn on_response(&self, _result: String) {
        self.success_count.fetch_add(1, Ordering::SeqCst);
    }

    fn on_failure(&self, _error: HazelcastError) {
        self.failure_count.fetch_add(1, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn test_echo_task_serialization() {
        let task = EchoTask::new("hello");
        let mut output = Vec::new();
        task.serialize(&mut output).expect("serialization should succeed");
        assert!(!output.is_empty());
    }

    #[test]
    fn test_callback_counts() {
        let callback = TestCallback::new();
        assert_eq!(callback.success_count(), 0);
        assert_eq!(callback.failure_count(), 0);

        callback.on_response("test".to_string());
        assert_eq!(callback.success_count(), 1);

        callback.on_failure(HazelcastError::Timeout("test".to_string()));
        assert_eq!(callback.failure_count(), 1);
    }
}

/// Scheduled task for testing periodic execution.
#[derive(Debug, Clone)]
pub struct CounterTask {
    pub increment: i32,
}

impl CounterTask {
    pub fn new(increment: i32) -> Self {
        Self { increment }
    }
}

impl Serializable for CounterTask {
    fn serialize(&self, output: &mut dyn ObjectDataOutput) -> Result<()> {
        output.write_i32(self.increment)
    }
}

impl hazelcast_client::executor::Callable<i32> for CounterTask {
    fn factory_id(&self) -> i32 {
        1
    }

    fn class_id(&self) -> i32 {
        2
    }
}

/// Integration tests require a running Hazelcast cluster with the
/// EchoTask callable registered on the server side.
#[cfg(test)]
mod integration_tests {
    use super::*;

    /// Scaffold for submit test.
    /// Requires: Running Hazelcast cluster with EchoTask registered.
    #[tokio::test]
    #[ignore = "requires running Hazelcast cluster"]
    async fn test_executor_submit() {
        // let config = hazelcast_client::ClientConfig::default();
        // let client = hazelcast_client::HazelcastClient::connect(config).await.unwrap();
        // let executor = client.get_executor_service("test-executor").await;
        //
        // let task = EchoTask::new("hello");
        // let future = executor.submit(&task).await.unwrap();
        // let result = future.get_timeout(Duration::from_secs(5)).await.unwrap();
        // assert_eq!(result, "hello");
    }

    /// Scaffold for submit_to_member test.
    #[tokio::test]
    #[ignore = "requires running Hazelcast cluster"]
    async fn test_executor_submit_to_member() {
        // let config = hazelcast_client::ClientConfig::default();
        // let client = hazelcast_client::HazelcastClient::connect(config).await.unwrap();
        // let executor = client.get_executor_service("test-executor").await;
        //
        // let members = client.cluster_members().await;
        // let member = members.first().unwrap();
        //
        // let task = EchoTask::new("hello member");
        // let future = executor.submit_to_member(&task, member).await.unwrap();
        // let result = future.get().await.unwrap();
        // assert_eq!(result, "hello member");
    }

    /// Scaffold for submit_to_key_owner test.
    #[tokio::test]
    #[ignore = "requires running Hazelcast cluster"]
    async fn test_executor_submit_to_key_owner() {
        // let config = hazelcast_client::ClientConfig::default();
        // let client = hazelcast_client::HazelcastClient::connect(config).await.unwrap();
        // let executor = client.get_executor_service("test-executor").await;
        //
        // let task = EchoTask::new("hello key owner");
        // let key = "my-key";
        // let future = executor.submit_to_key_owner(&task, &key).await.unwrap();
        // let result = future.get().await.unwrap();
        // assert_eq!(result, "hello key owner");
    }

    /// Scaffold for submit_to_all_members test.
    #[tokio::test]
    #[ignore = "requires running Hazelcast cluster"]
    async fn test_executor_submit_to_all_members() {
        // let config = hazelcast_client::ClientConfig::default();
        // let client = hazelcast_client::HazelcastClient::connect(config).await.unwrap();
        // let executor = client.get_executor_service("test-executor").await;
        //
        // let task = EchoTask::new("broadcast");
        // let futures = executor.submit_to_all_members(&task).await.unwrap();
        //
        // for future in futures {
        //     let result = future.get().await.unwrap();
        //     assert_eq!(result, "broadcast");
        // }
    }

    /// Scaffold for callback-based submission test.
    #[tokio::test]
    #[ignore = "requires running Hazelcast cluster"]
    async fn test_executor_submit_with_callback() {
        // let config = hazelcast_client::ClientConfig::default();
        // let client = hazelcast_client::HazelcastClient::connect(config).await.unwrap();
        // let executor = client.get_executor_service("test-executor").await;
        //
        // let callback = Arc::new(TestCallback::new());
        // let task = EchoTask::new("callback test");
        //
        // executor.submit_with_callback(&task, Arc::clone(&callback)).await.unwrap();
        //
        // // Wait for callback
        // tokio::time::sleep(Duration::from_secs(2)).await;
        // assert_eq!(callback.success_count(), 1);
    }

    /// Scaffold for executor shutdown test.
    #[tokio::test]
    #[ignore = "requires running Hazelcast cluster"]
    async fn test_executor_shutdown() {
        // let config = hazelcast_client::ClientConfig::default();
        // let client = hazelcast_client::HazelcastClient::connect(config).await.unwrap();
        // let executor = client.get_executor_service("shutdown-test-executor").await;
        //
        // assert!(!executor.is_shutdown().await.unwrap());
        // executor.shutdown().await.unwrap();
        // assert!(executor.is_shutdown().await.unwrap());
    }
}

/// Scheduled executor integration tests.
#[cfg(test)]
mod scheduled_integration_tests {
    use super::*;

    /// Scaffold for scheduled executor schedule test.
    #[tokio::test]
    #[ignore = "requires running Hazelcast cluster"]
    async fn test_scheduled_executor_schedule() {
        // let config = hazelcast_client::ClientConfig::default();
        // let client = hazelcast_client::HazelcastClient::connect(config).await.unwrap();
        // let scheduler = client.get_scheduled_executor_service("test-scheduler").await;
        //
        // let task = EchoTask::new("delayed hello");
        // let future = scheduler.schedule(&task, Duration::from_secs(1)).await.unwrap();
        //
        // assert!(!future.is_cancelled());
        // let result = future.get().await.unwrap();
        // assert_eq!(result, "delayed hello");
    }

    /// Scaffold for schedule_at_fixed_rate test.
    #[tokio::test]
    #[ignore = "requires running Hazelcast cluster"]
    async fn test_scheduled_executor_at_fixed_rate() {
        // let config = hazelcast_client::ClientConfig::default();
        // let client = hazelcast_client::HazelcastClient::connect(config).await.unwrap();
        // let scheduler = client.get_scheduled_executor_service("test-scheduler").await;
        //
        // let task = CounterTask::new(1);
        // let future = scheduler
        //     .schedule_at_fixed_rate(&task, Duration::from_secs(0), Duration::from_secs(1))
        //     .await
        //     .unwrap();
        //
        // tokio::time::sleep(Duration::from_secs(3)).await;
        // assert!(!future.is_done().await.unwrap());
        // future.cancel(true).await.unwrap();
        // assert!(future.is_cancelled());
    }

    /// Scaffold for schedule_with_fixed_delay test.
    #[tokio::test]
    #[ignore = "requires running Hazelcast cluster"]
    async fn test_scheduled_executor_with_fixed_delay() {
        // let config = hazelcast_client::ClientConfig::default();
        // let client = hazelcast_client::HazelcastClient::connect(config).await.unwrap();
        // let scheduler = client.get_scheduled_executor_service("test-scheduler").await;
        //
        // let task = CounterTask::new(1);
        // let future = scheduler
        //     .schedule_with_fixed_delay(&task, Duration::from_secs(0), Duration::from_millis(500))
        //     .await
        //     .unwrap();
        //
        // tokio::time::sleep(Duration::from_secs(2)).await;
        // future.cancel(false).await.unwrap();
        // assert!(future.is_cancelled());
    }

    /// Scaffold for scheduled future cancel test.
    #[tokio::test]
    #[ignore = "requires running Hazelcast cluster"]
    async fn test_scheduled_future_cancel() {
        // let config = hazelcast_client::ClientConfig::default();
        // let client = hazelcast_client::HazelcastClient::connect(config).await.unwrap();
        // let scheduler = client.get_scheduled_executor_service("test-scheduler").await;
        //
        // let task = EchoTask::new("cancel me");
        // let future = scheduler.schedule(&task, Duration::from_secs(60)).await.unwrap();
        //
        // assert!(!future.is_cancelled());
        // let cancelled = future.cancel(true).await.unwrap();
        // assert!(cancelled);
        // assert!(future.is_cancelled());
    }

    /// Scaffold for scheduled future get_delay test.
    #[tokio::test]
    #[ignore = "requires running Hazelcast cluster"]
    async fn test_scheduled_future_get_delay() {
        // let config = hazelcast_client::ClientConfig::default();
        // let client = hazelcast_client::HazelcastClient::connect(config).await.unwrap();
        // let scheduler = client.get_scheduled_executor_service("test-scheduler").await;
        //
        // let task = EchoTask::new("delayed");
        // let future = scheduler.schedule(&task, Duration::from_secs(10)).await.unwrap();
        //
        // let delay = future.get_delay().await.unwrap();
        // assert!(delay.as_secs() > 8);
        // assert!(delay.as_secs() <= 10);
        //
        // future.cancel(true).await.unwrap();
    }

    /// Scaffold for schedule_on_member test.
    #[tokio::test]
    #[ignore = "requires running Hazelcast cluster"]
    async fn test_scheduled_executor_on_member() {
        // let config = hazelcast_client::ClientConfig::default();
        // let client = hazelcast_client::HazelcastClient::connect(config).await.unwrap();
        // let scheduler = client.get_scheduled_executor_service("test-scheduler").await;
        //
        // let members = client.cluster_members().await;
        // let member = members.first().unwrap();
        //
        // let task = EchoTask::new("member task");
        // let future = scheduler
        //     .schedule_on_member(&task, member, Duration::from_secs(1))
        //     .await
        //     .unwrap();
        //
        // let delay = future.get_delay().await.unwrap();
        // assert!(delay.as_millis() > 0);
    }

    /// Scaffold for scheduled executor shutdown test.
    #[tokio::test]
    #[ignore = "requires running Hazelcast cluster"]
    async fn test_scheduled_executor_shutdown() {
        // let config = hazelcast_client::ClientConfig::default();
        // let client = hazelcast_client::HazelcastClient::connect(config).await.unwrap();
        // let scheduler = client.get_scheduled_executor_service("shutdown-scheduler").await;
        //
        // assert!(!scheduler.is_shutdown().await.unwrap());
        // scheduler.shutdown().await.unwrap();
        // assert!(scheduler.is_shutdown().await.unwrap());
    }

    /// Scaffold for scheduled future dispose test.
    #[tokio::test]
    #[ignore = "requires running Hazelcast cluster"]
    async fn test_scheduled_future_dispose() {
        // let config = hazelcast_client::ClientConfig::default();
        // let client = hazelcast_client::HazelcastClient::connect(config).await.unwrap();
        // let scheduler = client.get_scheduled_executor_service("test-scheduler").await;
        //
        // let task = EchoTask::new("disposable");
        // let future = scheduler.schedule(&task, Duration::from_secs(1)).await.unwrap();
        //
        // // Wait for task to complete
        // tokio::time::sleep(Duration::from_secs(2)).await;
        //
        // // Dispose should succeed after completion
        // future.dispose().await.unwrap();
    }
}
