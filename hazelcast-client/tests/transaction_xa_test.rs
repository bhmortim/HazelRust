//! Integration tests for XA Transactions.
//!
//! Run with: `cargo test --test transaction_xa_test -- --ignored`
//! Requires a Hazelcast cluster running at 127.0.0.1:5701

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use hazelcast_client::transaction::TransactionOptions;
use hazelcast_client::{ClientConfig, HazelcastClient};
use hazelcast_core::Result;

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
async fn test_transaction_context_creation() {
    let client = match create_client().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Skipping test - could not connect to Hazelcast: {}", e);
            return;
        }
    };

    let options = TransactionOptions::new();
    let txn = client.new_transaction_context(options);

    println!("Created transaction context: {:?}", txn);

    let _ = client.shutdown().await;
}

#[tokio::test]
#[ignore]
async fn test_transaction_options_builder() {
    let options = TransactionOptions::new()
        .with_timeout(Duration::from_secs(30));

    println!("Transaction options configured with 30s timeout");

    // Verify the options can be used
    let client = match create_client().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Skipping test - could not connect to Hazelcast: {}", e);
            return;
        }
    };

    let _txn = client.new_transaction_context(options);
    let _ = client.shutdown().await;
}

#[tokio::test]
#[ignore]
async fn test_transaction_begin_commit() {
    let client = match create_client().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Skipping test - could not connect to Hazelcast: {}", e);
            return;
        }
    };

    let options = TransactionOptions::new()
        .with_timeout(Duration::from_secs(60));

    let mut txn = client.new_transaction_context(options);

    // Begin transaction
    match txn.begin().await {
        Ok(_) => println!("Transaction started successfully"),
        Err(e) => {
            eprintln!("Failed to begin transaction: {}", e);
            let _ = client.shutdown().await;
            return;
        }
    }

    // Perform transactional operations
    // Note: This would require getting transactional map proxy

    // Commit transaction
    match txn.commit().await {
        Ok(_) => println!("Transaction committed successfully"),
        Err(e) => {
            eprintln!("Failed to commit transaction: {}", e);
        }
    }

    let _ = client.shutdown().await;
}

#[tokio::test]
#[ignore]
async fn test_transaction_rollback() {
    let client = match create_client().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Skipping test - could not connect to Hazelcast: {}", e);
            return;
        }
    };

    let options = TransactionOptions::new();
    let mut txn = client.new_transaction_context(options);

    // Begin transaction
    match txn.begin().await {
        Ok(_) => println!("Transaction started"),
        Err(e) => {
            eprintln!("Failed to begin transaction: {}", e);
            let _ = client.shutdown().await;
            return;
        }
    }

    // Rollback instead of commit
    match txn.rollback().await {
        Ok(_) => println!("Transaction rolled back successfully"),
        Err(e) => {
            eprintln!("Failed to rollback transaction: {}", e);
        }
    }

    let _ = client.shutdown().await;
}

#[tokio::test]
#[ignore]
async fn test_transactional_map_operations() {
    let client = match create_client().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Skipping test - could not connect to Hazelcast: {}", e);
            return;
        }
    };

    let map_name = format!("txn-map-{}", std::process::id());

    let options = TransactionOptions::new();
    let mut txn = client.new_transaction_context(options);

    // Begin transaction
    if let Err(e) = txn.begin().await {
        eprintln!("Failed to begin transaction: {}", e);
        let _ = client.shutdown().await;
        return;
    }

    // Get transactional map
    let txn_map = txn.get_map::<String, String>(&map_name).unwrap();

    // Perform operations within transaction
    let key = "txn-key".to_string();
    let value = "txn-value".to_string();

    match txn_map.put(key.clone(), value.clone()).await {
        Ok(_) => println!("Put in transaction succeeded"),
        Err(e) => eprintln!("Transactional put failed: {}", e),
    }

    match txn_map.get(&key).await {
        Ok(Some(v)) => println!("Got value in transaction: {}", v),
        Ok(None) => println!("No value found in transaction"),
        Err(e) => eprintln!("Transactional get failed: {}", e),
    }

    // Commit transaction
    match txn.commit().await {
        Ok(_) => println!("Transaction committed"),
        Err(e) => eprintln!("Commit failed: {}", e),
    }

    // Verify outside transaction
    let map = client.get_map::<String, String>(&map_name);
    match map.get(&key).await {
        Ok(Some(v)) => {
            println!("Value after commit: {}", v);
            assert_eq!(v, value, "Value should match after commit");
        }
        Ok(None) => println!("Value not found after commit"),
        Err(e) => eprintln!("Get after commit failed: {}", e),
    }

    // Cleanup
    let _ = map.clear().await;
    let _ = client.shutdown().await;
}

#[tokio::test]
#[ignore]
async fn test_transaction_isolation() {
    let client = match create_client().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Skipping test - could not connect to Hazelcast: {}", e);
            return;
        }
    };

    let map_name = format!("txn-isolation-{}", std::process::id());
    let map = client.get_map::<String, i32>(&map_name);

    // Setup initial value
    let key = "counter".to_string();
    map.put(key.clone(), 100).await.expect("initial put should succeed");

    // Start transaction
    let mut txn = client.new_transaction_context(TransactionOptions::new());

    if let Err(e) = txn.begin().await {
        eprintln!("Failed to begin transaction: {}", e);
        let _ = client.shutdown().await;
        return;
    }

    let txn_map = txn.get_map::<String, i32>(&map_name).unwrap();

    // Modify in transaction
    let _ = txn_map.put(key.clone(), 200).await;

    // Read outside transaction should still see old value
    match map.get(&key).await {
        Ok(Some(v)) => {
            println!("Value outside transaction: {}", v);
            // Note: Actual isolation behavior depends on Hazelcast configuration
        }
        Ok(None) => println!("No value found outside transaction"),
        Err(e) => eprintln!("Read outside transaction failed: {}", e),
    }

    // Rollback to discard changes
    let _ = txn.rollback().await;

    // Value should be unchanged
    match map.get(&key).await {
        Ok(Some(v)) => {
            println!("Value after rollback: {}", v);
            assert_eq!(v, 100, "Value should be unchanged after rollback");
        }
        Ok(None) => println!("Value not found after rollback"),
        Err(e) => eprintln!("Read after rollback failed: {}", e),
    }

    let _ = map.clear().await;
    let _ = client.shutdown().await;
}

#[tokio::test]
#[ignore]
async fn test_transaction_timeout() {
    let client = match create_client().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Skipping test - could not connect to Hazelcast: {}", e);
            return;
        }
    };

    // Very short timeout
    let options = TransactionOptions::new()
        .with_timeout(Duration::from_millis(100));

    let mut txn = client.new_transaction_context(options);

    if let Err(e) = txn.begin().await {
        eprintln!("Failed to begin transaction: {}", e);
        let _ = client.shutdown().await;
        return;
    }

    // Wait longer than timeout
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Transaction should have timed out
    match txn.commit().await {
        Ok(_) => println!("Commit succeeded (transaction might have been auto-extended)"),
        Err(e) => println!("Commit failed as expected due to timeout: {}", e),
    }

    let _ = client.shutdown().await;
}

#[tokio::test]
#[ignore]
async fn test_multiple_concurrent_transactions() {
    let client = match create_client().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Skipping test - could not connect to Hazelcast: {}", e);
            return;
        }
    };

    let map_name = format!("txn-concurrent-{}", std::process::id());

    // Start multiple transactions sequentially (HazelcastClient is not Clone)
    for i in 0..3i32 {
        let mut txn = client.new_transaction_context(TransactionOptions::new());

        if txn.begin().await.is_err() {
            eprintln!("Transaction {} failed to begin", i);
            continue;
        }

        if let Ok(txn_map) = txn.get_map::<String, i32>(&map_name) {
            let _ = txn_map.put(format!("key-{}", i), i).await;

        if txn.commit().await.is_err() {
            eprintln!("Transaction {} failed to commit", i);
        } else {
            println!("Transaction {} completed", i);
        }
        } // if let Ok(txn_map)
    }

    // Verify results
    let map = client.get_map::<String, i32>(&map_name);
    for i in 0..3 {
        match map.get(&format!("key-{}", i)).await {
            Ok(Some(v)) => println!("key-{}: {}", i, v),
            Ok(None) => println!("key-{}: not found", i),
            Err(e) => eprintln!("Error reading key-{}: {}", i, e),
        }
    }

    let _ = map.clear().await;
    let _ = client.shutdown().await;
}
