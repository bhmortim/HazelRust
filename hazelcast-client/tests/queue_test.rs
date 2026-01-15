//! Integration tests for IQueue operations.

use std::time::Duration;

use hazelcast_client::{ClientConfig, ClientConfigBuilder, HazelcastClient, IQueue};

async fn create_test_client() -> Option<HazelcastClient> {
    let config = ClientConfigBuilder::new()
        .cluster_name("dev")
        .connection_timeout(Duration::from_secs(5))
        .build()
        .unwrap();

    HazelcastClient::new(config).await.ok()
}

#[tokio::test]
async fn test_queue_offer_and_poll() {
    let Some(client) = create_test_client().await else {
        eprintln!("Skipping test: no Hazelcast cluster available");
        return;
    };

    let queue: IQueue<String> = client.get_queue("test-offer-poll");

    let offered = queue.offer("item1".to_string()).await.unwrap();
    assert!(offered);

    let polled = queue.poll().await.unwrap();
    assert_eq!(polled, Some("item1".to_string()));

    let empty = queue.poll().await.unwrap();
    assert_eq!(empty, None);

    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_queue_peek() {
    let Some(client) = create_test_client().await else {
        eprintln!("Skipping test: no Hazelcast cluster available");
        return;
    };

    let queue: IQueue<String> = client.get_queue("test-peek");

    let empty_peek = queue.peek().await.unwrap();
    assert_eq!(empty_peek, None);

    queue.offer("peek-item".to_string()).await.unwrap();

    let peeked = queue.peek().await.unwrap();
    assert_eq!(peeked, Some("peek-item".to_string()));

    let still_there = queue.peek().await.unwrap();
    assert_eq!(still_there, Some("peek-item".to_string()));

    let polled = queue.poll().await.unwrap();
    assert_eq!(polled, Some("peek-item".to_string()));

    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_queue_size_and_is_empty() {
    let Some(client) = create_test_client().await else {
        eprintln!("Skipping test: no Hazelcast cluster available");
        return;
    };

    let queue: IQueue<i32> = client.get_queue("test-size");

    assert!(queue.is_empty().await.unwrap());
    assert_eq!(queue.size().await.unwrap(), 0);

    queue.offer(1).await.unwrap();
    queue.offer(2).await.unwrap();
    queue.offer(3).await.unwrap();

    assert!(!queue.is_empty().await.unwrap());
    assert_eq!(queue.size().await.unwrap(), 3);

    queue.poll().await.unwrap();
    assert_eq!(queue.size().await.unwrap(), 2);

    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_queue_poll_timeout_empty() {
    let Some(client) = create_test_client().await else {
        eprintln!("Skipping test: no Hazelcast cluster available");
        return;
    };

    let queue: IQueue<String> = client.get_queue("test-poll-timeout-empty");

    let start = std::time::Instant::now();
    let result = queue.poll_timeout(Duration::from_millis(500)).await.unwrap();
    let elapsed = start.elapsed();

    assert_eq!(result, None);
    assert!(elapsed >= Duration::from_millis(400));

    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_queue_poll_timeout_with_item() {
    let Some(client) = create_test_client().await else {
        eprintln!("Skipping test: no Hazelcast cluster available");
        return;
    };

    let queue: IQueue<String> = client.get_queue("test-poll-timeout-item");

    queue.offer("immediate".to_string()).await.unwrap();

    let start = std::time::Instant::now();
    let result = queue.poll_timeout(Duration::from_secs(5)).await.unwrap();
    let elapsed = start.elapsed();

    assert_eq!(result, Some("immediate".to_string()));
    assert!(elapsed < Duration::from_secs(1));

    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_queue_fifo_order() {
    let Some(client) = create_test_client().await else {
        eprintln!("Skipping test: no Hazelcast cluster available");
        return;
    };

    let queue: IQueue<i32> = client.get_queue("test-fifo");

    for i in 1..=5 {
        queue.offer(i).await.unwrap();
    }

    for expected in 1..=5 {
        let polled = queue.poll().await.unwrap();
        assert_eq!(polled, Some(expected));
    }

    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_queue_clone() {
    let Some(client) = create_test_client().await else {
        eprintln!("Skipping test: no Hazelcast cluster available");
        return;
    };

    let queue1: IQueue<String> = client.get_queue("test-clone");
    let queue2 = queue1.clone();

    queue1.offer("from-q1".to_string()).await.unwrap();

    let polled = queue2.poll().await.unwrap();
    assert_eq!(polled, Some("from-q1".to_string()));

    client.shutdown().await.unwrap();
}

#[test]
fn test_iqueue_is_send_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<IQueue<String>>();
    assert_send_sync::<IQueue<i32>>();
}
