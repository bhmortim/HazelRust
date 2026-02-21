//! Integration tests for IList distributed list.

mod common;

use std::sync::Arc;

use hazelcast_client::HazelcastClient;

use crate::common::{unique_name, wait_for_cluster_ready, skip_if_no_cluster};

#[tokio::test]
async fn test_list_add_and_get() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let list_name = unique_name("test-list");
    let list = client.get_list::<String>(&list_name);

    let added = list.add("item1".to_string()).await.unwrap();
    assert!(added);

    let item = list.get(0).await.unwrap();
    assert_eq!(item, Some("item1".to_string()));

    list.clear().await.unwrap();
}

#[tokio::test]
async fn test_list_preserves_order() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let list_name = unique_name("test-list-order");
    let list = client.get_list::<String>(&list_name);

    list.add("first".to_string()).await.unwrap();
    list.add("second".to_string()).await.unwrap();
    list.add("third".to_string()).await.unwrap();

    assert_eq!(list.get(0).await.unwrap(), Some("first".to_string()));
    assert_eq!(list.get(1).await.unwrap(), Some("second".to_string()));
    assert_eq!(list.get(2).await.unwrap(), Some("third".to_string()));

    list.clear().await.unwrap();
}

#[tokio::test]
async fn test_list_add_at_index() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let list_name = unique_name("test-list-add-at");
    let list = client.get_list::<String>(&list_name);

    list.add("first".to_string()).await.unwrap();
    list.add("third".to_string()).await.unwrap();

    list.add_at(1, "second".to_string()).await.unwrap();

    assert_eq!(list.get(0).await.unwrap(), Some("first".to_string()));
    assert_eq!(list.get(1).await.unwrap(), Some("second".to_string()));
    assert_eq!(list.get(2).await.unwrap(), Some("third".to_string()));

    list.clear().await.unwrap();
}

#[tokio::test]
async fn test_list_remove_at() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let list_name = unique_name("test-list-remove-at");
    let list = client.get_list::<String>(&list_name);

    list.add("first".to_string()).await.unwrap();
    list.add("second".to_string()).await.unwrap();
    list.add("third".to_string()).await.unwrap();

    let removed = list.remove_at(1).await.unwrap();
    assert_eq!(removed, Some("second".to_string()));

    assert_eq!(list.size().await.unwrap(), 2);
    assert_eq!(list.get(0).await.unwrap(), Some("first".to_string()));
    assert_eq!(list.get(1).await.unwrap(), Some("third".to_string()));

    list.clear().await.unwrap();
}

#[tokio::test]
async fn test_list_set() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let list_name = unique_name("test-list-set");
    let list = client.get_list::<String>(&list_name);

    list.add("old-value".to_string()).await.unwrap();

    let old = list.set(0, "new-value".to_string()).await.unwrap();
    assert_eq!(old, Some("old-value".to_string()));

    assert_eq!(list.get(0).await.unwrap(), Some("new-value".to_string()));

    list.clear().await.unwrap();
}

#[tokio::test]
async fn test_list_size() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let list_name = unique_name("test-list-size");
    let list = client.get_list::<String>(&list_name);

    assert_eq!(list.size().await.unwrap(), 0);

    list.add("item1".to_string()).await.unwrap();
    assert_eq!(list.size().await.unwrap(), 1);

    list.add("item2".to_string()).await.unwrap();
    assert_eq!(list.size().await.unwrap(), 2);

    list.remove_at(0).await.unwrap();
    assert_eq!(list.size().await.unwrap(), 1);

    list.clear().await.unwrap();
}

#[tokio::test]
async fn test_list_contains() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let list_name = unique_name("test-list-contains");
    let list = client.get_list::<String>(&list_name);

    assert!(!list.contains(&"item".to_string()).await.unwrap());

    list.add("item".to_string()).await.unwrap();
    assert!(list.contains(&"item".to_string()).await.unwrap());

    list.clear().await.unwrap();
}

#[tokio::test]
async fn test_list_is_empty() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let list_name = unique_name("test-list-empty");
    let list = client.get_list::<String>(&list_name);

    assert!(list.is_empty().await.unwrap());

    list.add("item".to_string()).await.unwrap();
    assert!(!list.is_empty().await.unwrap());

    list.remove_at(0).await.unwrap();
    assert!(list.is_empty().await.unwrap());
}

#[tokio::test]
async fn test_list_clear() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let list_name = unique_name("test-list-clear");
    let list = client.get_list::<String>(&list_name);

    for i in 0..10 {
        list.add(format!("item-{}", i)).await.unwrap();
    }
    assert_eq!(list.size().await.unwrap(), 10);

    list.clear().await.unwrap();
    assert_eq!(list.size().await.unwrap(), 0);
}

#[tokio::test]
async fn test_list_with_integers() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let list_name = unique_name("test-list-int");
    let list = client.get_list::<i32>(&list_name);

    list.add(10).await.unwrap();
    list.add(20).await.unwrap();
    list.add(30).await.unwrap();

    assert_eq!(list.get(0).await.unwrap(), Some(10));
    assert_eq!(list.get(1).await.unwrap(), Some(20));
    assert_eq!(list.get(2).await.unwrap(), Some(30));

    list.clear().await.unwrap();
}

#[tokio::test]
async fn test_list_allows_duplicates() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let list_name = unique_name("test-list-dup");
    let list = client.get_list::<String>(&list_name);

    list.add("same".to_string()).await.unwrap();
    list.add("same".to_string()).await.unwrap();
    list.add("same".to_string()).await.unwrap();

    assert_eq!(list.size().await.unwrap(), 3);

    assert_eq!(list.get(0).await.unwrap(), Some("same".to_string()));
    assert_eq!(list.get(1).await.unwrap(), Some("same".to_string()));
    assert_eq!(list.get(2).await.unwrap(), Some("same".to_string()));

    list.clear().await.unwrap();
}

#[tokio::test]
async fn test_list_bulk_operations() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let list_name = unique_name("test-list-bulk");
    let list = client.get_list::<i32>(&list_name);

    for i in 0..100 {
        list.add(i).await.unwrap();
    }

    assert_eq!(list.size().await.unwrap(), 100);

    for i in 0..100 {
        let value = list.get(i as usize).await.unwrap();
        assert_eq!(value, Some(i));
    }

    list.clear().await.unwrap();
}

#[tokio::test]
async fn test_list_concurrent_operations() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = Arc::new(
        HazelcastClient::new(common::default_config())
            .await
            .expect("failed to connect")
    );
    let list_name = unique_name("test-list-concurrent");

    let mut handles = Vec::new();

    for i in 0..10 {
        let client_clone = Arc::clone(&client);
        let list_name_clone = list_name.clone();
        
        let handle = tokio::spawn(async move {
            let list = client_clone
                .get_list::<i32>(&list_name_clone);
            
            for j in 0..10 {
                list.add(i * 10 + j).await.unwrap();
            }
        });
        
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }

    let list = client.get_list::<i32>(&list_name);
    assert_eq!(list.size().await.unwrap(), 100);

    list.clear().await.unwrap();
}

#[tokio::test]
async fn test_list_get_out_of_bounds() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let list_name = unique_name("test-list-oob");
    let list = client.get_list::<String>(&list_name);

    list.add("only-item".to_string()).await.unwrap();

    let result = list.get(100).await;
    assert!(result.is_ok());

    list.clear().await.unwrap();
}

#[tokio::test]
async fn test_list_clone_shares_state() {
    if skip_if_no_cluster() {
        return;
    }
    wait_for_cluster_ready().await;

    let client = HazelcastClient::new(common::default_config())
        .await
        .expect("failed to connect");
    let list_name = unique_name("test-list-clone");
    let list1 = client.get_list::<String>(&list_name);
    let list2 = list1.clone();

    list1.add("item1".to_string()).await.unwrap();

    assert_eq!(list2.size().await.unwrap(), 1);
    assert_eq!(list2.get(0).await.unwrap(), Some("item1".to_string()));

    list1.clear().await.unwrap();
}
