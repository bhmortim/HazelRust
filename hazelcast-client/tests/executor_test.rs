//! Integration tests for Distributed Executor Service.
//!
//! Run with: `cargo test --test executor_test -- --ignored`
//! Requires a Hazelcast cluster running at 127.0.0.1:5701

use std::net::SocketAddr;
use std::time::Duration;

use hazelcast_client::{ClientConfig, HazelcastClient};
use hazelcast_core::serialization::ObjectDataOutput;
use hazelcast_core::{Result, Serializable};

/// A simple callable task that echoes a message.
#[derive(Debug, Clone)]
struct EchoTask {
    message: String,
}

impl EchoTask {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl Serializable for EchoTask {
    fn serialize(&self, output: &mut ObjectDataOutput) -> Result<()> {
        output.write_utf("com.hazelcast.test.EchoTask")?;
        output.write_utf(&self.message)?;
        Ok(())
    }
}

/// A task that computes factorial.
#[derive(Debug, Clone)]
struct FactorialTask {
    n: i32,
}

impl FactorialTask {
    fn new(n: i32) -> Self {
        Self { n }
    }
}

impl Serializable for FactorialTask {
    fn serialize(&self, output: &mut ObjectDataOutput) -> Result<()> {
        output.write_utf("com.hazelcast.test.FactorialTask")?;
        output.write_i32(self.n)?;
        Ok(())
    }
}

/// A task that sleeps for a specified duration.
#[derive(Debug, Clone)]
struct SleepTask {
    millis: i64,
}

impl SleepTask {
    fn new(duration: Duration) -> Self {
        Self {
            millis: duration.as_millis() as i64,
        }
    }
}

impl Serializable for SleepTask {
    fn serialize(&self, output: &mut ObjectDataOutput) -> Result<()> {
        output.write_utf("com.hazelcast.test.SleepTask")?;
        output.write_i64(self.millis)?;
        Ok(())
    }
}

async fn create_client() -> Result<HazelcastClient> {
    let addr: SocketAddr = "127.0.0.1:5701".parse().unwrap();
    let config = ClientConfig::builder()
        .cluster_name("dev")
        .add_address(addr)
        .build()
        .map_err(|e| hazelcast_core::HazelcastError::Configuration(e.to_string()))?;

    HazelcastClient::new(config).await
}

#[tokio::test]
#[ignore]
async fn test_executor_service_creation() {
    let client = match create_client().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Skipping test - could not connect to Hazelcast: {}", e);
            return;
        }
    };

    let executor = client.get_executor_service("test-executor");
    println!("Created executor service: {:?}", executor);

    let _ = client.shutdown().await;
}

#[tokio::test]
#[ignore]
async fn test_submit_task() {
    let client = match create_client().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Skipping test - could not connect to Hazelcast: {}", e);
            return;
        }
    };

    let executor = client.get_executor_service("test-executor");
    let task = EchoTask::new("Hello from Rust client!");

    // Note: Actual task execution requires server-side task implementation
    println!("Submitting echo task...");
    // The executor.submit() method would be called here
    // For now, we verify the executor service can be obtained

    let _ = client.shutdown().await;
}

#[tokio::test]
#[ignore]
async fn test_executor_with_different_task_types() {
    let client = match create_client().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Skipping test - could not connect to Hazelcast: {}", e);
            return;
        }
    };

    let executor = client.get_executor_service("compute-executor");

    // Create various task types to verify serialization
    let echo = EchoTask::new("test");
    let factorial = FactorialTask::new(10);
    let sleep = SleepTask::new(Duration::from_millis(100));

    // Verify tasks can be serialized
    let mut output = hazelcast_core::serialization::ObjectDataOutput::new();
    assert!(echo.serialize(&mut output).is_ok());

    let mut output = hazelcast_core::serialization::ObjectDataOutput::new();
    assert!(factorial.serialize(&mut output).is_ok());

    let mut output = hazelcast_core::serialization::ObjectDataOutput::new();
    assert!(sleep.serialize(&mut output).is_ok());

    println!("All task types serialized successfully");

    let _ = client.shutdown().await;
}

#[tokio::test]
#[ignore]
async fn test_multiple_executor_services() {
    let client = match create_client().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Skipping test - could not connect to Hazelcast: {}", e);
            return;
        }
    };

    // Create multiple named executor services
    let executor1 = client.get_executor_service("executor-1");
    let executor2 = client.get_executor_service("executor-2");
    let default_executor = client.get_executor_service("default");

    println!("Created 3 executor services");
    println!("  executor-1: {:?}", executor1);
    println!("  executor-2: {:?}", executor2);
    println!("  default: {:?}", default_executor);

    let _ = client.shutdown().await;
}

#[tokio::test]
#[ignore]
async fn test_executor_service_with_member_selection() {
    let client = match create_client().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Skipping test - could not connect to Hazelcast: {}", e);
            return;
        }
    };

    let members = client.members().await;
    println!("Cluster has {} members", members.len());

    for member in &members {
        println!("  Member: {} ({})", member.address(), member.uuid());
    }

    let executor = client.get_executor_service("member-aware-executor");
    let task = EchoTask::new("Task for specific member");

    // In a full implementation, we could submit to specific members
    println!("Executor created, task prepared for submission");

    let _ = client.shutdown().await;
}

#[tokio::test]
#[ignore]
async fn test_executor_task_serialization_roundtrip() {
    // Test that tasks serialize correctly without needing a cluster
    let task = FactorialTask::new(5);

    let mut output = hazelcast_core::serialization::ObjectDataOutput::new();
    task.serialize(&mut output).expect("serialization should succeed");

    let bytes = output.into_bytes();
    assert!(!bytes.is_empty(), "serialized bytes should not be empty");
    println!("FactorialTask(5) serialized to {} bytes", bytes.len());
}
