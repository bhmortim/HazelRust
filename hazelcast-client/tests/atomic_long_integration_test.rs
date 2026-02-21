//! Integration tests for AtomicLong distributed counter.

mod common;

use std::sync::Arc;

use hazelcast_client::HazelcastClient;

use crate::common::{unique_name, wait_for_cluster_ready, skip_if_no_cluster};

#[tokio::test]
async fn test_atomic_long_get_and_set() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let counter_name = unique_name("test-counter");
    let counter = client.get_atomic_long(&counter_name);

    let initial = counter.get().await.unwrap();
    assert_eq!(initial, 0);

    counter.set(100).await.unwrap();
    let value = counter.get().await.unwrap();
    assert_eq!(value, 100);
}

#[tokio::test]
async fn test_atomic_long_get_and_set_returns_old_value() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let counter_name = unique_name("test-counter-gas");
    let counter = client.get_atomic_long(&counter_name);

    counter.set(50).await.unwrap();

    let old = counter.get_and_set(100).await.unwrap();
    assert_eq!(old, 50);

    let current = counter.get().await.unwrap();
    assert_eq!(current, 100);
}

#[tokio::test]
async fn test_atomic_long_increment_and_get() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let counter_name = unique_name("test-counter-inc");
    let counter = client.get_atomic_long(&counter_name);

    counter.set(0).await.unwrap();

    let value = counter.increment_and_get().await.unwrap();
    assert_eq!(value, 1);

    let value = counter.increment_and_get().await.unwrap();
    assert_eq!(value, 2);

    let value = counter.increment_and_get().await.unwrap();
    assert_eq!(value, 3);
}

#[tokio::test]
async fn test_atomic_long_decrement_and_get() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let counter_name = unique_name("test-counter-dec");
    let counter = client.get_atomic_long(&counter_name);

    counter.set(10).await.unwrap();

    let value = counter.decrement_and_get().await.unwrap();
    assert_eq!(value, 9);

    let value = counter.decrement_and_get().await.unwrap();
    assert_eq!(value, 8);
}

#[tokio::test]
async fn test_atomic_long_get_and_increment() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let counter_name = unique_name("test-counter-gai");
    let counter = client.get_atomic_long(&counter_name);

    counter.set(5).await.unwrap();

    let old = counter.get_and_increment().await.unwrap();
    assert_eq!(old, 5);

    let current = counter.get().await.unwrap();
    assert_eq!(current, 6);
}

#[tokio::test]
async fn test_atomic_long_get_and_decrement() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let counter_name = unique_name("test-counter-gad");
    let counter = client.get_atomic_long(&counter_name);

    counter.set(5).await.unwrap();

    let old = counter.get_and_decrement().await.unwrap();
    assert_eq!(old, 5);

    let current = counter.get().await.unwrap();
    assert_eq!(current, 4);
}

#[tokio::test]
async fn test_atomic_long_add_and_get() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let counter_name = unique_name("test-counter-aag");
    let counter = client.get_atomic_long(&counter_name);

    counter.set(10).await.unwrap();

    let value = counter.add_and_get(5).await.unwrap();
    assert_eq!(value, 15);

    let value = counter.add_and_get(-3).await.unwrap();
    assert_eq!(value, 12);
}

#[tokio::test]
async fn test_atomic_long_get_and_add() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let counter_name = unique_name("test-counter-gaa");
    let counter = client.get_atomic_long(&counter_name);

    counter.set(10).await.unwrap();

    let old = counter.get_and_add(5).await.unwrap();
    assert_eq!(old, 10);

    let current = counter.get().await.unwrap();
    assert_eq!(current, 15);
}

#[tokio::test]
async fn test_atomic_long_compare_and_set_success() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let counter_name = unique_name("test-counter-cas");
    let counter = client.get_atomic_long(&counter_name);

    counter.set(10).await.unwrap();

    let success = counter.compare_and_set(10, 20).await.unwrap();
    assert!(success);

    let value = counter.get().await.unwrap();
    assert_eq!(value, 20);
}

#[tokio::test]
async fn test_atomic_long_compare_and_set_failure() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let counter_name = unique_name("test-counter-cas-fail");
    let counter = client.get_atomic_long(&counter_name);

    counter.set(10).await.unwrap();

    let success = counter.compare_and_set(5, 20).await.unwrap();
    assert!(!success);

    let value = counter.get().await.unwrap();
    assert_eq!(value, 10);
}

#[tokio::test]
async fn test_atomic_long_negative_values() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let counter_name = unique_name("test-counter-neg");
    let counter = client.get_atomic_long(&counter_name);

    counter.set(-100).await.unwrap();
    assert_eq!(counter.get().await.unwrap(), -100);

    let value = counter.increment_and_get().await.unwrap();
    assert_eq!(value, -99);

    counter.set(0).await.unwrap();
    let value = counter.decrement_and_get().await.unwrap();
    assert_eq!(value, -1);
}

#[tokio::test]
async fn test_atomic_long_large_values() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let counter_name = unique_name("test-counter-large");
    let counter = client.get_atomic_long(&counter_name);

    counter.set(i64::MAX - 1).await.unwrap();
    let value = counter.increment_and_get().await.unwrap();
    assert_eq!(value, i64::MAX);

    counter.set(i64::MIN + 1).await.unwrap();
    let value = counter.decrement_and_get().await.unwrap();
    assert_eq!(value, i64::MIN);
}

#[tokio::test]
async fn test_atomic_long_concurrent_increments() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = Arc::new(
        HazelcastClient::new(common::default_config())
            .await
            .expect("failed to connect")
    );
    let counter_name = unique_name("test-counter-concurrent");

    {
        let counter = client.get_atomic_long(&counter_name);
        counter.set(0).await.unwrap();
    }

    let mut handles = Vec::new();

    for _ in 0..10 {
        let client_clone = Arc::clone(&client);
        let counter_name_clone = counter_name.clone();
        
        let handle = tokio::spawn(async move {
            let counter = client_clone
                .get_atomic_long(&counter_name_clone);
            
            for _ in 0..100 {
                counter.increment_and_get().await.unwrap();
            }
        });
        
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }

    let counter = client.get_atomic_long(&counter_name);
    let final_value = counter.get().await.unwrap();
    assert_eq!(final_value, 1000);
}

#[tokio::test]
async fn test_atomic_long_concurrent_cas() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = Arc::new(
        HazelcastClient::new(common::default_config())
            .await
            .expect("failed to connect")
    );
    let counter_name = unique_name("test-counter-cas-concurrent");

    {
        let counter = client.get_atomic_long(&counter_name);
        counter.set(0).await.unwrap();
    }

    let success_count = Arc::new(std::sync::atomic::AtomicU32::new(0));
    let mut handles = Vec::new();

    for _ in 0..10 {
        let client_clone = Arc::clone(&client);
        let counter_name_clone = counter_name.clone();
        let success_clone = Arc::clone(&success_count);
        
        let handle = tokio::spawn(async move {
            let counter = client_clone
                .get_atomic_long(&counter_name_clone);
            
            for _ in 0..10 {
                loop {
                    let current = counter.get().await.unwrap();
                    if counter.compare_and_set(current, current + 1).await.unwrap() {
                        success_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        break;
                    }
                }
            }
        });
        
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }

    let counter = client.get_atomic_long(&counter_name);
    let final_value = counter.get().await.unwrap();
    assert_eq!(final_value, 100);
    assert_eq!(success_count.load(std::sync::atomic::Ordering::SeqCst), 100);
}

#[tokio::test]
async fn test_atomic_long_name() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let counter_name = unique_name("test-counter-name");
    let counter = client.get_atomic_long(&counter_name);

    assert_eq!(counter.name(), counter_name);
}

#[tokio::test]
async fn test_atomic_long_clone() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let counter_name = unique_name("test-counter-clone");
    let counter1 = client.get_atomic_long(&counter_name);
    let counter2 = counter1.clone();

    counter1.set(42).await.unwrap();
    let value = counter2.get().await.unwrap();
    assert_eq!(value, 42);

    counter2.increment_and_get().await.unwrap();
    let value = counter1.get().await.unwrap();
    assert_eq!(value, 43);
}

#[tokio::test]
async fn test_atomic_long_multiple_counters() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    
    let counter1_name = unique_name("test-counter-multi-1");
    let counter2_name = unique_name("test-counter-multi-2");
    
    let counter1 = client.get_atomic_long(&counter1_name);
    let counter2 = client.get_atomic_long(&counter2_name);

    counter1.set(100).await.unwrap();
    counter2.set(200).await.unwrap();

    assert_eq!(counter1.get().await.unwrap(), 100);
    assert_eq!(counter2.get().await.unwrap(), 200);

    counter1.increment_and_get().await.unwrap();
    counter2.decrement_and_get().await.unwrap();

    assert_eq!(counter1.get().await.unwrap(), 101);
    assert_eq!(counter2.get().await.unwrap(), 199);
}
