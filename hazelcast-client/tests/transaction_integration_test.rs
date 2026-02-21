//! Integration tests for transactions.

mod common;

use std::time::Duration;

use hazelcast_client::{
    HazelcastClient, TransactionOptions, TransactionType, TransactionState,
};

use crate::common::{unique_name, wait_for_cluster_ready, skip_if_no_cluster};

#[tokio::test]
async fn test_transaction_commit_map_operations() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let map_name = unique_name("test-txn-map");

    let options = TransactionOptions::new()
        .with_timeout(Duration::from_secs(30))
        .with_type(TransactionType::OnePhase);

    let mut txn = client.new_transaction_context(options);

    txn.begin().await.unwrap();
    assert!(txn.is_active());
    assert_eq!(txn.state(), TransactionState::Active);

    {
        let txn_map = txn.get_map::<String, String>(&map_name).unwrap();
        txn_map.put("key1".to_string(), "value1".to_string()).await.unwrap();
        txn_map.put("key2".to_string(), "value2".to_string()).await.unwrap();
    }

    txn.commit().await.unwrap();
    assert_eq!(txn.state(), TransactionState::Committed);

    let map = client.get_map::<String, String>(&map_name);
    assert_eq!(map.get(&"key1".to_string()).await.unwrap(), Some("value1".to_string()));
    assert_eq!(map.get(&"key2".to_string()).await.unwrap(), Some("value2".to_string()));

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_transaction_rollback_map_operations() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let map_name = unique_name("test-txn-rollback-map");

    let map = client.get_map::<String, String>(&map_name);
    map.put("key1".to_string(), "original".to_string()).await.unwrap();

    let options = TransactionOptions::new();
    let mut txn = client.new_transaction_context(options);

    txn.begin().await.unwrap();

    {
        let txn_map = txn.get_map::<String, String>(&map_name).unwrap();
        txn_map.put("key1".to_string(), "modified".to_string()).await.unwrap();
        txn_map.put("key2".to_string(), "new".to_string()).await.unwrap();
    }

    txn.rollback().await.unwrap();
    assert_eq!(txn.state(), TransactionState::RolledBack);

    assert_eq!(map.get(&"key1".to_string()).await.unwrap(), Some("original".to_string()));
    assert_eq!(map.get(&"key2".to_string()).await.unwrap(), None);

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_transaction_queue_operations() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let queue_name = unique_name("test-txn-queue");

    let options = TransactionOptions::new();
    let mut txn = client.new_transaction_context(options);

    txn.begin().await.unwrap();

    {
        let txn_queue = txn.get_queue::<String>(&queue_name).unwrap();
        txn_queue.offer("item1".to_string()).await.unwrap();
        txn_queue.offer("item2".to_string()).await.unwrap();

        assert_eq!(txn_queue.size().await.unwrap(), 2);
    }

    txn.commit().await.unwrap();

    let queue = client.get_queue::<String>(&queue_name);
    assert_eq!(queue.poll().await.unwrap(), Some("item1".to_string()));
    assert_eq!(queue.poll().await.unwrap(), Some("item2".to_string()));
}

#[tokio::test]
async fn test_transaction_set_operations() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let set_name = unique_name("test-txn-set");

    let options = TransactionOptions::new();
    let mut txn = client.new_transaction_context(options);

    txn.begin().await.unwrap();

    {
        let txn_set = txn.get_set::<String>(&set_name).unwrap();
        txn_set.add("item1".to_string()).await.unwrap();
        txn_set.add("item2".to_string()).await.unwrap();

        assert_eq!(txn_set.size().await.unwrap(), 2);
    }

    txn.commit().await.unwrap();

    let set = client.get_set::<String>(&set_name);
    assert!(set.contains(&"item1".to_string()).await.unwrap());
    assert!(set.contains(&"item2".to_string()).await.unwrap());

    set.clear().await.unwrap();
}

#[tokio::test]
async fn test_transaction_list_operations() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let list_name = unique_name("test-txn-list");

    let options = TransactionOptions::new();
    let mut txn = client.new_transaction_context(options);

    txn.begin().await.unwrap();

    {
        let txn_list = txn.get_list::<String>(&list_name).unwrap();
        txn_list.add("item1".to_string()).await.unwrap();
        txn_list.add("item2".to_string()).await.unwrap();

        assert_eq!(txn_list.size().await.unwrap(), 2);
    }

    txn.commit().await.unwrap();

    let list = client.get_list::<String>(&list_name);
    assert!(list.contains(&"item1".to_string()).await.unwrap());
    assert!(list.contains(&"item2".to_string()).await.unwrap());

    list.clear().await.unwrap();
}

#[tokio::test]
async fn test_transaction_two_phase() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let map_name = unique_name("test-txn-2pc");

    let options = TransactionOptions::new()
        .with_type(TransactionType::TwoPhase)
        .with_durability(1);

    let mut txn = client.new_transaction_context(options);

    txn.begin().await.unwrap();

    {
        let txn_map = txn.get_map::<String, String>(&map_name).unwrap();
        txn_map.put("2pc-key".to_string(), "2pc-value".to_string()).await.unwrap();
    }

    txn.commit().await.unwrap();

    let map = client.get_map::<String, String>(&map_name);
    assert_eq!(map.get(&"2pc-key".to_string()).await.unwrap(), Some("2pc-value".to_string()));

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_transaction_isolation() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let map_name = unique_name("test-txn-isolation");

    let map = client.get_map::<String, String>(&map_name);
    map.put("key".to_string(), "initial".to_string()).await.unwrap();

    let options = TransactionOptions::new();
    let mut txn = client.new_transaction_context(options);

    txn.begin().await.unwrap();

    {
        let txn_map = txn.get_map::<String, String>(&map_name).unwrap();
        txn_map.put("key".to_string(), "in-transaction".to_string()).await.unwrap();

        let txn_value = txn_map.get(&"key".to_string()).await.unwrap();
        assert_eq!(txn_value, Some("in-transaction".to_string()));
    }

    txn.rollback().await.unwrap();

    let final_value = map.get(&"key".to_string()).await.unwrap();
    assert_eq!(final_value, Some("initial".to_string()));

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_transaction_map_get_and_modify() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let map_name = unique_name("test-txn-get-modify");

    let map = client.get_map::<String, i64>(&map_name);
    map.put("counter".to_string(), 100).await.unwrap();

    let options = TransactionOptions::new();
    let mut txn = client.new_transaction_context(options);

    txn.begin().await.unwrap();

    {
        let txn_map = txn.get_map::<String, i64>(&map_name).unwrap();

        let current = txn_map.get(&"counter".to_string()).await.unwrap().unwrap();
        txn_map.put("counter".to_string(), current + 50).await.unwrap();
    }

    txn.commit().await.unwrap();

    let final_value = map.get(&"counter".to_string()).await.unwrap();
    assert_eq!(final_value, Some(150i64));

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_transaction_map_contains_key() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let map_name = unique_name("test-txn-contains");

    let options = TransactionOptions::new();
    let mut txn = client.new_transaction_context(options);

    txn.begin().await.unwrap();

    {
        let txn_map = txn.get_map::<String, String>(&map_name).unwrap();

        assert!(!txn_map.contains_key(&"key".to_string()).await.unwrap());

        txn_map.put("key".to_string(), "value".to_string()).await.unwrap();

        assert!(txn_map.contains_key(&"key".to_string()).await.unwrap());
    }

    txn.commit().await.unwrap();

    let map = client.get_map::<String, String>(&map_name);
    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_transaction_map_size() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let map_name = unique_name("test-txn-size");

    let options = TransactionOptions::new();
    let mut txn = client.new_transaction_context(options);

    txn.begin().await.unwrap();

    {
        let txn_map = txn.get_map::<String, String>(&map_name).unwrap();

        assert_eq!(txn_map.size().await.unwrap(), 0);

        txn_map.put("key1".to_string(), "value1".to_string()).await.unwrap();
        txn_map.put("key2".to_string(), "value2".to_string()).await.unwrap();

        assert_eq!(txn_map.size().await.unwrap(), 2);
    }

    txn.commit().await.unwrap();

    let map = client.get_map::<String, String>(&map_name);
    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_transaction_map_put_if_absent() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let map_name = unique_name("test-txn-put-if-absent");

    let options = TransactionOptions::new();
    let mut txn = client.new_transaction_context(options);

    txn.begin().await.unwrap();

    {
        let txn_map = txn.get_map::<String, String>(&map_name).unwrap();

        let result = txn_map.put_if_absent("key".to_string(), "value1".to_string()).await.unwrap();
        assert!(result.is_none());

        let result = txn_map.put_if_absent("key".to_string(), "value2".to_string()).await.unwrap();
        assert_eq!(result, Some("value1".to_string()));

        let current = txn_map.get(&"key".to_string()).await.unwrap();
        assert_eq!(current, Some("value1".to_string()));
    }

    txn.commit().await.unwrap();

    let map = client.get_map::<String, String>(&map_name);
    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_transaction_map_replace() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let map_name = unique_name("test-txn-replace");

    let map = client.get_map::<String, String>(&map_name);
    map.put("key".to_string(), "original".to_string()).await.unwrap();

    let options = TransactionOptions::new();
    let mut txn = client.new_transaction_context(options);

    txn.begin().await.unwrap();

    {
        let txn_map = txn.get_map::<String, String>(&map_name).unwrap();

        let old = txn_map.replace(&"key".to_string(), "replaced".to_string()).await.unwrap();
        assert_eq!(old, Some("original".to_string()));

        let old = txn_map.replace(&"nonexistent".to_string(), "value".to_string()).await.unwrap();
        assert!(old.is_none());
    }

    txn.commit().await.unwrap();

    let final_value = map.get(&"key".to_string()).await.unwrap();
    assert_eq!(final_value, Some("replaced".to_string()));

    map.clear().await.unwrap();
}

#[tokio::test]
async fn test_transaction_map_delete() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let map_name = unique_name("test-txn-delete");

    let map = client.get_map::<String, String>(&map_name);
    map.put("key".to_string(), "value".to_string()).await.unwrap();

    let options = TransactionOptions::new();
    let mut txn = client.new_transaction_context(options);

    txn.begin().await.unwrap();

    {
        let txn_map = txn.get_map::<String, String>(&map_name).unwrap();
        txn_map.delete(&"key".to_string()).await.unwrap();
    }

    txn.commit().await.unwrap();

    let exists = map.contains_key(&"key".to_string()).await.unwrap();
    assert!(!exists);
}

#[tokio::test]
async fn test_transaction_begin_twice_fails() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");

    let options = TransactionOptions::new();
    let mut txn = client.new_transaction_context(options);

    txn.begin().await.unwrap();

    let result = txn.begin().await;
    assert!(result.is_err());

    txn.rollback().await.unwrap();
}

#[tokio::test]
async fn test_transaction_commit_without_begin_fails() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");

    let options = TransactionOptions::new();
    let mut txn = client.new_transaction_context(options);

    let result = txn.commit().await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_transaction_rollback_without_begin_fails() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");

    let options = TransactionOptions::new();
    let mut txn = client.new_transaction_context(options);

    let result = txn.rollback().await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_transaction_options_builder() {
    let options = TransactionOptions::new()
        .with_timeout(Duration::from_secs(60))
        .with_durability(2)
        .with_type(TransactionType::TwoPhase);

    assert_eq!(options.timeout(), Duration::from_secs(60));
    assert_eq!(options.durability(), 2);
    assert_eq!(options.transaction_type(), TransactionType::TwoPhase);
}

#[tokio::test]
async fn test_transaction_queue_poll() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let queue_name = unique_name("test-txn-queue-poll");

    let queue = client.get_queue::<String>(&queue_name);
    queue.offer("item1".to_string()).await.unwrap();
    queue.offer("item2".to_string()).await.unwrap();

    let options = TransactionOptions::new();
    let mut txn = client.new_transaction_context(options);

    txn.begin().await.unwrap();

    {
        let txn_queue = txn.get_queue::<String>(&queue_name).unwrap();
        let item = txn_queue.poll().await.unwrap();
        assert_eq!(item, Some("item1".to_string()));
    }

    txn.commit().await.unwrap();

    assert_eq!(queue.size().await.unwrap(), 1);
    assert_eq!(queue.poll().await.unwrap(), Some("item2".to_string()));
}

#[tokio::test]
async fn test_transaction_set_remove() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let set_name = unique_name("test-txn-set-remove");

    let set = client.get_set::<String>(&set_name);
    set.add("item1".to_string()).await.unwrap();
    set.add("item2".to_string()).await.unwrap();

    let options = TransactionOptions::new();
    let mut txn = client.new_transaction_context(options);

    txn.begin().await.unwrap();

    {
        let txn_set = txn.get_set::<String>(&set_name).unwrap();
        let removed = txn_set.remove(&"item1".to_string()).await.unwrap();
        assert!(removed);
    }

    txn.commit().await.unwrap();

    assert!(!set.contains(&"item1".to_string()).await.unwrap());
    assert!(set.contains(&"item2".to_string()).await.unwrap());

    set.clear().await.unwrap();
}

#[tokio::test]
async fn test_transaction_list_remove() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let list_name = unique_name("test-txn-list-remove");

    let list = client.get_list::<String>(&list_name);
    list.add("item1".to_string()).await.unwrap();
    list.add("item2".to_string()).await.unwrap();

    let options = TransactionOptions::new();
    let mut txn = client.new_transaction_context(options);

    txn.begin().await.unwrap();

    {
        let txn_list = txn.get_list::<String>(&list_name).unwrap();
        let removed = txn_list.remove(&"item1".to_string()).await.unwrap();
        assert!(removed);
    }

    txn.commit().await.unwrap();

    assert_eq!(list.size().await.unwrap(), 1);
    assert!(list.contains(&"item2".to_string()).await.unwrap());

    list.clear().await.unwrap();
}
