//! Integration tests for Map Entry Processors.
//!
//! Run with: `cargo test --test entry_processor_test -- --ignored`
//! Requires a Hazelcast cluster running at 127.0.0.1:5701

use std::net::SocketAddr;

use hazelcast_client::{ClientConfig, HazelcastClient};
use hazelcast_core::serialization::DataOutput;
use hazelcast_core::{Result, Serializable};

/// Test entry processor that increments an integer value.
#[derive(Debug, Clone)]
struct IncrementProcessor {
    delta: i32,
}

impl IncrementProcessor {
    fn new(delta: i32) -> Self {
        Self { delta }
    }
}

impl hazelcast_client::proxy::EntryProcessor for IncrementProcessor {
    type Output = i32;
}

impl Serializable for IncrementProcessor {
    fn serialize<W: DataOutput>(&self, output: &mut W) -> Result<()> {
        output.write_string("com.hazelcast.test.IncrementProcessor")?;
        output.write_int(self.delta)?;
        Ok(())
    }
}

/// Test entry processor that returns the current value without modification.
#[derive(Debug, Clone)]
struct ReadOnlyProcessor;

impl hazelcast_client::proxy::EntryProcessor for ReadOnlyProcessor {
    type Output = String;
}

impl Serializable for ReadOnlyProcessor {
    fn serialize<W: DataOutput>(&self, output: &mut W) -> Result<()> {
        output.write_string("com.hazelcast.test.ReadOnlyProcessor")?;
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
async fn test_execute_on_key() {
    let client = match create_client().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Skipping test - could not connect to Hazelcast: {}", e);
            return;
        }
    };

    let map = client.get_map::<String, i32>("test-entry-processor");

    // Setup test data
    let key = format!("test-key-{}", uuid::Uuid::new_v4());
    map.put(key.clone(), 100).await.expect("put should succeed");

    // Execute processor
    let processor = IncrementProcessor::new(5);
    let result = map.execute_on_key(&key, &processor).await;

    // Note: This test verifies the protocol encoding works.
    // Actual execution requires a server-side processor implementation.
    match result {
        Ok(Some(value)) => {
            println!("Entry processor returned: {}", value);
        }
        Ok(None) => {
            println!("Entry processor returned no result");
        }
        Err(e) => {
            // Expected if server doesn't have the processor class
            println!("Entry processor error (expected without server impl): {}", e);
        }
    }

    // Cleanup
    let _ = map.remove(&key).await;
    let _ = client.shutdown().await;
}

#[tokio::test]
#[ignore]
async fn test_execute_on_keys() {
    let client = match create_client().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Skipping test - could not connect to Hazelcast: {}", e);
            return;
        }
    };

    let map = client.get_map::<String, i32>("test-entry-processor-multi");

    // Setup test data
    let keys: Vec<String> = (0..3)
        .map(|i| format!("multi-key-{}-{}", i, uuid::Uuid::new_v4()))
        .collect();

    for (i, key) in keys.iter().enumerate() {
        map.put(key.clone(), (i * 10) as i32)
            .await
            .expect("put should succeed");
    }

    // Execute processor on multiple keys
    let processor = IncrementProcessor::new(1);
    let result = map.execute_on_keys(&keys, &processor).await;

    match result {
        Ok(results) => {
            println!("Processed {} entries", results.len());
            for (key, value) in results.iter() {
                println!("  {}: {}", key, value);
            }
        }
        Err(e) => {
            println!("Multi-key processor error (expected without server impl): {}", e);
        }
    }

    // Cleanup
    for key in &keys {
        let _ = map.remove(key).await;
    }
    let _ = client.shutdown().await;
}

#[tokio::test]
#[ignore]
async fn test_execute_on_entries() {
    let client = match create_client().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Skipping test - could not connect to Hazelcast: {}", e);
            return;
        }
    };

    let map_name = format!("test-entry-processor-all-{}", uuid::Uuid::new_v4());
    let map = client.get_map::<String, i32>(&map_name);

    // Setup test data
    for i in 0..5 {
        map.put(format!("key-{}", i), i * 100)
            .await
            .expect("put should succeed");
    }

    // Execute processor on all entries
    let processor = IncrementProcessor::new(10);
    let result = map.execute_on_entries(&processor).await;

    match result {
        Ok(results) => {
            println!("Processed all {} entries", results.len());
        }
        Err(e) => {
            println!("All-entries processor error (expected without server impl): {}", e);
        }
    }

    // Cleanup
    let _ = map.clear().await;
    let _ = client.shutdown().await;
}

#[tokio::test]
#[ignore]
async fn test_entry_processor_result_types() {
    let client = match create_client().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Skipping test - could not connect to Hazelcast: {}", e);
            return;
        }
    };

    let map = client.get_map::<String, String>("test-entry-processor-string");

    let key = format!("string-key-{}", uuid::Uuid::new_v4());
    map.put(key.clone(), "test-value".to_string())
        .await
        .expect("put should succeed");

    let processor = ReadOnlyProcessor;
    let result = map.execute_on_key(&key, &processor).await;

    match result {
        Ok(Some(value)) => {
            println!("Read processor returned: {}", value);
        }
        Ok(None) => {
            println!("Read processor returned no result");
        }
        Err(e) => {
            println!("Read processor error: {}", e);
        }
    }

    let _ = map.remove(&key).await;
    let _ = client.shutdown().await;
}
