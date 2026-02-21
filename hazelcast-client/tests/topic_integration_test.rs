//! Integration tests for ITopic distributed pub/sub.

mod common;

use std::sync::{Arc, atomic::{AtomicU32, Ordering}};
use std::time::Duration;

use hazelcast_client::HazelcastClient;

use crate::common::{unique_name, wait_for_cluster_ready, skip_if_no_cluster};

#[tokio::test]
async fn test_topic_publish() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let topic_name = unique_name("test-topic");
    let topic = client.get_topic::<String>(&topic_name);

    let result = topic.publish("Hello, World!".to_string()).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_topic_publish_multiple_messages() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let topic_name = unique_name("test-topic-multi");
    let topic = client.get_topic::<String>(&topic_name);

    for i in 0..10 {
        topic.publish(format!("Message {}", i)).await.unwrap();
    }
}

#[tokio::test]
async fn test_topic_publish_with_integers() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let topic_name = unique_name("test-topic-int");
    let topic = client.get_topic::<i64>(&topic_name);

    topic.publish(42i64).await.unwrap();
    topic.publish(-100i64).await.unwrap();
    topic.publish(i64::MAX).await.unwrap();
}

#[tokio::test]
async fn test_topic_add_message_listener() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let topic_name = unique_name("test-topic-listener");
    let topic = client.get_topic::<String>(&topic_name);

    let counter = Arc::new(AtomicU32::new(0));
    let counter_clone = Arc::clone(&counter);

    let registration = topic.add_message_listener(move |_msg| {
        counter_clone.fetch_add(1, Ordering::SeqCst);
    }).await.unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    for i in 0..5 {
        topic.publish(format!("Message {}", i)).await.unwrap();
    }

    tokio::time::sleep(Duration::from_secs(1)).await;

    registration.deactivate();
}

#[tokio::test]
async fn test_topic_listener_deactivation() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let topic_name = unique_name("test-topic-deactivate");
    let topic = client.get_topic::<String>(&topic_name);

    let counter = Arc::new(AtomicU32::new(0));
    let counter_clone = Arc::clone(&counter);

    let registration = topic.add_message_listener(move |_msg| {
        counter_clone.fetch_add(1, Ordering::SeqCst);
    }).await.unwrap();

    assert!(registration.is_active());

    registration.deactivate();

    assert!(!registration.is_active());
}

#[tokio::test]
async fn test_topic_stats() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let topic_name = unique_name("test-topic-stats");
    let topic = client.get_topic::<String>(&topic_name);

    let stats = topic.stats();
    let initial_messages = stats.messages_received();

    topic.publish("test message".to_string()).await.unwrap();

    let stats = topic.stats();
    assert!(stats.messages_received() >= initial_messages);
}

#[tokio::test]
async fn test_topic_name() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let topic_name = unique_name("test-topic-name");
    let topic = client.get_topic::<String>(&topic_name);

    assert_eq!(topic.name(), topic_name);
}

#[tokio::test]
async fn test_topic_clone() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let topic_name = unique_name("test-topic-clone");
    let topic1 = client.get_topic::<String>(&topic_name);
    let topic2 = topic1.clone();

    assert_eq!(topic1.name(), topic2.name());

    topic1.publish("from topic1".to_string()).await.unwrap();
    topic2.publish("from topic2".to_string()).await.unwrap();
}

#[tokio::test]
async fn test_topic_with_special_characters() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let topic_name = unique_name("test-topic-special");
    let topic = client.get_topic::<String>(&topic_name);

    let special_messages = vec![
        "Hello, World!",
        "日本語メッセージ",
        "🚀 Rocket Science 🎉",
        "Line1\nLine2\tTabbed",
        "",
    ];

    for msg in special_messages {
        topic.publish(msg.to_string()).await.unwrap();
    }
}

#[tokio::test]
async fn test_topic_concurrent_publishers() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = Arc::new(
        HazelcastClient::new(common::default_config())
            .await
            .expect("failed to connect")
    );
    let topic_name = unique_name("test-topic-concurrent");

    let mut handles = Vec::new();

    for i in 0..5 {
        let client_clone = Arc::clone(&client);
        let topic_name_clone = topic_name.clone();
        
        let handle = tokio::spawn(async move {
            let topic = client_clone
                .get_topic::<String>(&topic_name_clone);
            
            for j in 0..10 {
                topic.publish(format!("publisher-{}-msg-{}", i, j)).await.unwrap();
            }
        });
        
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }
}

#[tokio::test]
async fn test_topic_large_messages() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let topic_name = unique_name("test-topic-large");
    let topic = client.get_topic::<String>(&topic_name);

    let large_message = "x".repeat(100_000);
    topic.publish(large_message).await.unwrap();
}
