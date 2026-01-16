//! Integration tests for IQueue distributed queue.

mod common;

use std::sync::Arc;
use std::time::Duration;

use hazelcast_client::HazelcastClient;

use crate::common::{unique_name, wait_for_cluster_ready, skip_if_no_cluster};

#[tokio::test]
async fn test_queue_offer_and_poll() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let queue_name = unique_name("test-queue");
    let queue = client.get_queue::<String>(&queue_name).await.unwrap();

    let offered = queue.offer("item1".to_string()).await.unwrap();
    assert!(offered);

    let item = queue.poll().await.unwrap();
    assert_eq!(item, Some("item1".to_string()));

    let empty = queue.poll().await.unwrap();
    assert!(empty.is_none());
}

#[tokio::test]
async fn test_queue_fifo_order() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let queue_name = unique_name("test-queue-fifo");
    let queue = client.get_queue::<String>(&queue_name).await.unwrap();

    queue.offer("first".to_string()).await.unwrap();
    queue.offer("second".to_string()).await.unwrap();
    queue.offer("third".to_string()).await.unwrap();

    assert_eq!(queue.poll().await.unwrap(), Some("first".to_string()));
    assert_eq!(queue.poll().await.unwrap(), Some("second".to_string()));
    assert_eq!(queue.poll().await.unwrap(), Some("third".to_string()));
    assert_eq!(queue.poll().await.unwrap(), None);
}

#[tokio::test]
async fn test_queue_peek() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let queue_name = unique_name("test-queue-peek");
    let queue = client.get_queue::<String>(&queue_name).await.unwrap();

    let empty_peek = queue.peek().await.unwrap();
    assert!(empty_peek.is_none());

    queue.offer("item1".to_string()).await.unwrap();

    let peeked = queue.peek().await.unwrap();
    assert_eq!(peeked, Some("item1".to_string()));

    let peeked_again = queue.peek().await.unwrap();
    assert_eq!(peeked_again, Some("item1".to_string()));

    let polled = queue.poll().await.unwrap();
    assert_eq!(polled, Some("item1".to_string()));

    let empty_peek_after = queue.peek().await.unwrap();
    assert!(empty_peek_after.is_none());
}

#[tokio::test]
async fn test_queue_size() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let queue_name = unique_name("test-queue-size");
    let queue = client.get_queue::<String>(&queue_name).await.unwrap();

    assert_eq!(queue.size().await.unwrap(), 0);

    queue.offer("item1".to_string()).await.unwrap();
    assert_eq!(queue.size().await.unwrap(), 1);

    queue.offer("item2".to_string()).await.unwrap();
    assert_eq!(queue.size().await.unwrap(), 2);

    queue.poll().await.unwrap();
    assert_eq!(queue.size().await.unwrap(), 1);

    queue.poll().await.unwrap();
    assert_eq!(queue.size().await.unwrap(), 0);
}

#[tokio::test]
async fn test_queue_is_empty() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let queue_name = unique_name("test-queue-empty");
    let queue = client.get_queue::<String>(&queue_name).await.unwrap();

    assert!(queue.is_empty().await.unwrap());

    queue.offer("item".to_string()).await.unwrap();
    assert!(!queue.is_empty().await.unwrap());

    queue.poll().await.unwrap();
    assert!(queue.is_empty().await.unwrap());
}

#[tokio::test]
async fn test_queue_with_integers() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let queue_name = unique_name("test-queue-int");
    let queue = client.get_queue::<i64>(&queue_name).await.unwrap();

    queue.offer(42i64).await.unwrap();
    queue.offer(100i64).await.unwrap();
    queue.offer(-50i64).await.unwrap();

    assert_eq!(queue.poll().await.unwrap(), Some(42i64));
    assert_eq!(queue.poll().await.unwrap(), Some(100i64));
    assert_eq!(queue.poll().await.unwrap(), Some(-50i64));
}

#[tokio::test]
async fn test_queue_poll_timeout() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let queue_name = unique_name("test-queue-timeout");
    let queue = client.get_queue::<String>(&queue_name).await.unwrap();

    let start = std::time::Instant::now();
    let result = queue.poll_timeout(Duration::from_millis(500)).await.unwrap();
    let elapsed = start.elapsed();

    assert!(result.is_none());
    assert!(elapsed >= Duration::from_millis(400));
    assert!(elapsed < Duration::from_secs(2));
}

#[tokio::test]
async fn test_queue_bulk_operations() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let queue_name = unique_name("test-queue-bulk");
    let queue = client.get_queue::<i32>(&queue_name).await.unwrap();

    for i in 0..100 {
        queue.offer(i).await.unwrap();
    }

    assert_eq!(queue.size().await.unwrap(), 100);

    for i in 0..100 {
        let item = queue.poll().await.unwrap();
        assert_eq!(item, Some(i));
    }

    assert_eq!(queue.size().await.unwrap(), 0);
}

#[tokio::test]
async fn test_queue_concurrent_producers() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = Arc::new(
        HazelcastClient::new(common::default_config())
            .await
            .expect("failed to connect")
    );
    let queue_name = unique_name("test-queue-concurrent");

    let mut handles = Vec::new();

    for i in 0..5 {
        let client_clone = Arc::clone(&client);
        let queue_name_clone = queue_name.clone();
        
        let handle = tokio::spawn(async move {
            let queue = client_clone
                .get_queue::<String>(&queue_name_clone)
                .await
                .unwrap();
            
            for j in 0..20 {
                queue.offer(format!("producer-{}-item-{}", i, j)).await.unwrap();
            }
        });
        
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }

    let queue = client.get_queue::<String>(&queue_name).await.unwrap();
    assert_eq!(queue.size().await.unwrap(), 100);

    while queue.poll().await.unwrap().is_some() {}
}

#[tokio::test]
async fn test_queue_special_string_values() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let queue_name = unique_name("test-queue-special");
    let queue = client.get_queue::<String>(&queue_name).await.unwrap();

    let special_values = vec![
        String::new(),
        " ".to_string(),
        "\n\t\r".to_string(),
        "日本語".to_string(),
        "🚀🎉".to_string(),
        "a".repeat(10000),
    ];

    for value in &special_values {
        queue.offer(value.clone()).await.unwrap();
    }

    for expected in &special_values {
        let actual = queue.poll().await.unwrap();
        assert_eq!(actual.as_ref(), Some(expected));
    }
}

#[tokio::test]
async fn test_queue_clone_shares_state() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let queue_name = unique_name("test-queue-clone");
    let queue1 = client.get_queue::<String>(&queue_name).await.unwrap();
    let queue2 = queue1.clone();

    queue1.offer("item1".to_string()).await.unwrap();

    let item = queue2.poll().await.unwrap();
    assert_eq!(item, Some("item1".to_string()));

    queue2.offer("item2".to_string()).await.unwrap();

    assert_eq!(queue1.size().await.unwrap(), 1);
}
