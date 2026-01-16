//! Integration tests for ISet distributed set.

mod common;

use std::collections::HashSet;
use std::sync::Arc;

use hazelcast_client::HazelcastClient;

use crate::common::{unique_name, wait_for_cluster_ready, skip_if_no_cluster};

#[tokio::test]
async fn test_set_add_and_contains() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let set_name = unique_name("test-set");
    let set = client.get_set::<String>(&set_name).await.unwrap();

    assert!(!set.contains(&"item1".to_string()).await.unwrap());

    let added = set.add("item1".to_string()).await.unwrap();
    assert!(added);

    assert!(set.contains(&"item1".to_string()).await.unwrap());

    set.clear().await.unwrap();
}

#[tokio::test]
async fn test_set_add_duplicate() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let set_name = unique_name("test-set-dup");
    let set = client.get_set::<String>(&set_name).await.unwrap();

    let first_add = set.add("item1".to_string()).await.unwrap();
    assert!(first_add);

    let second_add = set.add("item1".to_string()).await.unwrap();
    assert!(!second_add);

    assert_eq!(set.size().await.unwrap(), 1);

    set.clear().await.unwrap();
}

#[tokio::test]
async fn test_set_remove() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let set_name = unique_name("test-set-remove");
    let set = client.get_set::<String>(&set_name).await.unwrap();

    set.add("item1".to_string()).await.unwrap();
    assert!(set.contains(&"item1".to_string()).await.unwrap());

    let removed = set.remove(&"item1".to_string()).await.unwrap();
    assert!(removed);

    assert!(!set.contains(&"item1".to_string()).await.unwrap());

    let removed_again = set.remove(&"item1".to_string()).await.unwrap();
    assert!(!removed_again);

    set.clear().await.unwrap();
}

#[tokio::test]
async fn test_set_size() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let set_name = unique_name("test-set-size");
    let set = client.get_set::<String>(&set_name).await.unwrap();

    assert_eq!(set.size().await.unwrap(), 0);

    set.add("item1".to_string()).await.unwrap();
    assert_eq!(set.size().await.unwrap(), 1);

    set.add("item2".to_string()).await.unwrap();
    assert_eq!(set.size().await.unwrap(), 2);

    set.add("item1".to_string()).await.unwrap();
    assert_eq!(set.size().await.unwrap(), 2);

    set.remove(&"item1".to_string()).await.unwrap();
    assert_eq!(set.size().await.unwrap(), 1);

    set.clear().await.unwrap();
}

#[tokio::test]
async fn test_set_is_empty() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let set_name = unique_name("test-set-empty");
    let set = client.get_set::<String>(&set_name).await.unwrap();

    assert!(set.is_empty().await.unwrap());

    set.add("item".to_string()).await.unwrap();
    assert!(!set.is_empty().await.unwrap());

    set.remove(&"item".to_string()).await.unwrap();
    assert!(set.is_empty().await.unwrap());
}

#[tokio::test]
async fn test_set_clear() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let set_name = unique_name("test-set-clear");
    let set = client.get_set::<String>(&set_name).await.unwrap();

    for i in 0..10 {
        set.add(format!("item-{}", i)).await.unwrap();
    }
    assert_eq!(set.size().await.unwrap(), 10);

    set.clear().await.unwrap();
    assert_eq!(set.size().await.unwrap(), 0);
    assert!(set.is_empty().await.unwrap());
}

#[tokio::test]
async fn test_set_with_integers() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let set_name = unique_name("test-set-int");
    let set = client.get_set::<i64>(&set_name).await.unwrap();

    set.add(42i64).await.unwrap();
    set.add(-100i64).await.unwrap();
    set.add(0i64).await.unwrap();

    assert!(set.contains(&42i64).await.unwrap());
    assert!(set.contains(&-100i64).await.unwrap());
    assert!(set.contains(&0i64).await.unwrap());
    assert!(!set.contains(&999i64).await.unwrap());

    assert_eq!(set.size().await.unwrap(), 3);

    set.clear().await.unwrap();
}

#[tokio::test]
async fn test_set_bulk_operations() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let set_name = unique_name("test-set-bulk");
    let set = client.get_set::<i32>(&set_name).await.unwrap();

    for i in 0..100 {
        set.add(i).await.unwrap();
    }

    assert_eq!(set.size().await.unwrap(), 100);

    for i in 0..100 {
        assert!(set.contains(&i).await.unwrap());
    }

    set.clear().await.unwrap();
}

#[tokio::test]
async fn test_set_concurrent_operations() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = Arc::new(
        HazelcastClient::new(common::default_config())
            .await
            .expect("failed to connect")
    );
    let set_name = unique_name("test-set-concurrent");

    let mut handles = Vec::new();

    for i in 0..10 {
        let client_clone = Arc::clone(&client);
        let set_name_clone = set_name.clone();
        
        let handle = tokio::spawn(async move {
            let set = client_clone
                .get_set::<i32>(&set_name_clone)
                .await
                .unwrap();
            
            for j in 0..10 {
                set.add(i * 10 + j).await.unwrap();
            }
        });
        
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }

    let set = client.get_set::<i32>(&set_name).await.unwrap();
    assert_eq!(set.size().await.unwrap(), 100);

    set.clear().await.unwrap();
}

#[tokio::test]
async fn test_set_special_string_values() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let set_name = unique_name("test-set-special");
    let set = client.get_set::<String>(&set_name).await.unwrap();

    let special_values = vec![
        String::new(),
        " ".to_string(),
        "\n\t\r".to_string(),
        "日本語".to_string(),
        "🚀🎉".to_string(),
        "a".repeat(1000),
    ];

    for value in &special_values {
        set.add(value.clone()).await.unwrap();
    }

    assert_eq!(set.size().await.unwrap(), special_values.len());

    for value in &special_values {
        assert!(set.contains(value).await.unwrap());
    }

    set.clear().await.unwrap();
}

#[tokio::test]
async fn test_set_clone_shares_state() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let set_name = unique_name("test-set-clone");
    let set1 = client.get_set::<String>(&set_name).await.unwrap();
    let set2 = set1.clone();

    set1.add("item1".to_string()).await.unwrap();

    assert!(set2.contains(&"item1".to_string()).await.unwrap());

    set2.add("item2".to_string()).await.unwrap();

    assert_eq!(set1.size().await.unwrap(), 2);

    set1.clear().await.unwrap();
}
