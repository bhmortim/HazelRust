//! Integration tests for client failover functionality.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use hazelcast_client::config::{ClientConfigBuilder, ClientFailoverConfig};
use hazelcast_client::connection::ConnectionManager;
use hazelcast_client::listener::LifecycleEvent;
use hazelcast_core::HazelcastError;
use tokio::net::TcpListener;

async fn create_mock_server() -> (TcpListener, SocketAddr) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    (listener, addr)
}

fn create_failover_config(
    addresses: Vec<(String, SocketAddr)>,
    try_count: u32,
) -> ClientFailoverConfig {
    let mut builder = ClientFailoverConfig::builder().try_count(try_count);

    for (name, addr) in addresses {
        let config = ClientConfigBuilder::new()
            .cluster_name(name)
            .add_address(addr)
            .build()
            .unwrap();
        builder = builder.add_client_config(config);
    }

    builder.build().unwrap()
}

#[test]
fn test_failover_config_builder() {
    let config1 = ClientConfigBuilder::new()
        .cluster_name("primary")
        .build()
        .unwrap();
    let config2 = ClientConfigBuilder::new()
        .cluster_name("backup")
        .build()
        .unwrap();

    let failover = ClientFailoverConfig::builder()
        .add_client_config(config1)
        .add_client_config(config2)
        .try_count(5)
        .build()
        .unwrap();

    assert_eq!(failover.cluster_count(), 2);
    assert_eq!(failover.try_count(), 5);
}

#[test]
fn test_failover_config_default_try_count() {
    let config = ClientConfigBuilder::new().build().unwrap();

    let failover = ClientFailoverConfig::builder()
        .add_client_config(config)
        .build()
        .unwrap();

    assert_eq!(failover.try_count(), 3);
}

#[test]
fn test_failover_config_no_configs_fails() {
    let result = ClientFailoverConfig::builder().build();

    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("at least one client configuration"));
}

#[test]
fn test_failover_config_zero_try_count_fails() {
    let config = ClientConfigBuilder::new().build().unwrap();

    let result = ClientFailoverConfig::builder()
        .add_client_config(config)
        .try_count(0)
        .build();

    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("try_count must be at least 1"));
}

#[test]
fn test_failover_config_get_config() {
    let config1 = ClientConfigBuilder::new()
        .cluster_name("cluster-1")
        .build()
        .unwrap();
    let config2 = ClientConfigBuilder::new()
        .cluster_name("cluster-2")
        .build()
        .unwrap();

    let failover = ClientFailoverConfig::builder()
        .add_client_config(config1)
        .add_client_config(config2)
        .build()
        .unwrap();

    assert_eq!(
        failover.get_config(0).unwrap().cluster_name(),
        "cluster-1"
    );
    assert_eq!(
        failover.get_config(1).unwrap().cluster_name(),
        "cluster-2"
    );
    assert!(failover.get_config(2).is_none());
}

#[test]
fn test_failover_config_client_configs_accessor() {
    let config1 = ClientConfigBuilder::new()
        .cluster_name("c1")
        .build()
        .unwrap();
    let config2 = ClientConfigBuilder::new()
        .cluster_name("c2")
        .build()
        .unwrap();

    let failover = ClientFailoverConfig::builder()
        .add_client_config(config1)
        .add_client_config(config2)
        .build()
        .unwrap();

    let configs = failover.client_configs();
    assert_eq!(configs.len(), 2);
    assert_eq!(configs[0].cluster_name(), "c1");
    assert_eq!(configs[1].cluster_name(), "c2");
}

#[test]
fn test_failover_config_clone() {
    let config = ClientConfigBuilder::new()
        .cluster_name("test")
        .build()
        .unwrap();

    let failover = ClientFailoverConfig::builder()
        .add_client_config(config)
        .try_count(7)
        .build()
        .unwrap();

    let cloned = failover.clone();
    assert_eq!(cloned.cluster_count(), failover.cluster_count());
    assert_eq!(cloned.try_count(), failover.try_count());
}

#[test]
fn test_failover_config_is_send_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<ClientFailoverConfig>();
}

#[test]
fn test_connection_manager_with_failover() {
    let config1 = ClientConfigBuilder::new()
        .cluster_name("primary")
        .build()
        .unwrap();
    let config2 = ClientConfigBuilder::new()
        .cluster_name("backup")
        .build()
        .unwrap();

    let failover = ClientFailoverConfig::builder()
        .add_client_config(config1)
        .add_client_config(config2)
        .try_count(2)
        .build()
        .unwrap();

    let manager = ConnectionManager::with_failover(failover);

    assert!(manager.has_failover());
    assert_eq!(manager.current_cluster_index(), 0);
    assert!(manager.failover_config().is_some());
}

#[test]
fn test_connection_manager_without_failover() {
    let config = ClientConfigBuilder::new().build().unwrap();
    let manager = ConnectionManager::from_config(config);

    assert!(!manager.has_failover());
    assert_eq!(manager.current_cluster_index(), 0);
    assert!(manager.failover_config().is_none());
}

#[tokio::test]
async fn test_trigger_failover_without_config() {
    let config = ClientConfigBuilder::new().build().unwrap();
    let manager = ConnectionManager::from_config(config);

    let result = manager.trigger_failover().await;

    assert!(result.is_err());
    match result.unwrap_err() {
        HazelcastError::Connection(msg) => {
            assert!(msg.contains("failover not configured"));
        }
        e => panic!("expected Connection error, got {:?}", e),
    }
}

#[tokio::test]
async fn test_failover_increments_cluster_index() {
    let (listener1, addr1) = create_mock_server().await;
    let (listener2, addr2) = create_mock_server().await;

    tokio::spawn(async move {
        let _ = listener1.accept().await;
    });
    tokio::spawn(async move {
        let _ = listener2.accept().await;
    });

    let failover = create_failover_config(
        vec![
            ("primary".to_string(), addr1),
            ("backup".to_string(), addr2),
        ],
        1,
    );

    let manager = ConnectionManager::with_failover(failover);
    manager.connect_to(addr1).await.unwrap();

    assert_eq!(manager.current_cluster_index(), 0);

    let _ = manager.trigger_failover().await;

    assert_eq!(manager.current_cluster_index(), 1);
}

#[tokio::test]
async fn test_failover_cycles_through_clusters() {
    let config1 = ClientConfigBuilder::new()
        .cluster_name("cluster1")
        .add_address("192.0.2.1:5701".parse().unwrap())
        .build()
        .unwrap();
    let config2 = ClientConfigBuilder::new()
        .cluster_name("cluster2")
        .add_address("192.0.2.2:5701".parse().unwrap())
        .build()
        .unwrap();
    let config3 = ClientConfigBuilder::new()
        .cluster_name("cluster3")
        .add_address("192.0.2.3:5701".parse().unwrap())
        .build()
        .unwrap();

    let failover = ClientFailoverConfig::builder()
        .add_client_config(config1)
        .add_client_config(config2)
        .add_client_config(config3)
        .try_count(1)
        .build()
        .unwrap();

    let manager = ConnectionManager::with_failover(failover);

    assert_eq!(manager.current_cluster_index(), 0);

    let _ = manager.trigger_failover().await;
    assert_eq!(manager.current_cluster_index(), 1);

    let _ = manager.trigger_failover().await;
    assert_eq!(manager.current_cluster_index(), 2);

    let _ = manager.trigger_failover().await;
    assert_eq!(manager.current_cluster_index(), 0);
}

#[tokio::test]
async fn test_failover_emits_lifecycle_event() {
    let (listener1, addr1) = create_mock_server().await;
    let (listener2, addr2) = create_mock_server().await;

    tokio::spawn(async move {
        let _ = listener1.accept().await;
    });
    tokio::spawn(async move {
        let _ = listener2.accept().await;
    });

    let failover = create_failover_config(
        vec![
            ("primary".to_string(), addr1),
            ("backup".to_string(), addr2),
        ],
        1,
    );

    let manager = ConnectionManager::with_failover(failover);
    let mut lifecycle_rx = manager.subscribe_lifecycle();

    manager.connect_to(addr1).await.unwrap();
    let _ = manager.trigger_failover().await;

    let mut found_event = false;
    while let Ok(event) =
        tokio::time::timeout(Duration::from_millis(200), lifecycle_rx.recv()).await
    {
        if let Ok(LifecycleEvent::ClientChangedCluster) = event {
            found_event = true;
            break;
        }
    }
    assert!(found_event, "expected ClientChangedCluster lifecycle event");
}

#[tokio::test]
async fn test_failover_clears_members() {
    let (listener1, addr1) = create_mock_server().await;
    let (listener2, addr2) = create_mock_server().await;

    tokio::spawn(async move {
        let _ = listener1.accept().await;
    });
    tokio::spawn(async move {
        let _ = listener2.accept().await;
    });

    let failover = create_failover_config(
        vec![
            ("primary".to_string(), addr1),
            ("backup".to_string(), addr2),
        ],
        1,
    );

    let manager = ConnectionManager::with_failover(failover);

    let member = hazelcast_client::listener::Member::new(
        uuid::Uuid::new_v4(),
        "127.0.0.1:5703".parse().unwrap(),
    );
    manager.handle_member_added(member).await;
    assert_eq!(manager.member_count().await, 1);

    manager.connect_to(addr1).await.unwrap();
    let _ = manager.trigger_failover().await;

    assert_eq!(manager.member_count().await, 0);
}

#[tokio::test]
async fn test_failover_clears_partition_table() {
    let (listener1, addr1) = create_mock_server().await;
    let (listener2, addr2) = create_mock_server().await;

    tokio::spawn(async move {
        let _ = listener1.accept().await;
    });
    tokio::spawn(async move {
        let _ = listener2.accept().await;
    });

    let failover = create_failover_config(
        vec![
            ("primary".to_string(), addr1),
            ("backup".to_string(), addr2),
        ],
        1,
    );

    let manager = ConnectionManager::with_failover(failover);

    let mut partitions = std::collections::HashMap::new();
    partitions.insert(0, uuid::Uuid::new_v4());
    partitions.insert(1, uuid::Uuid::new_v4());
    manager.update_partition_table(partitions).await;
    assert_eq!(manager.partition_count(), 2);

    manager.connect_to(addr1).await.unwrap();
    let _ = manager.trigger_failover().await;

    assert_eq!(manager.partition_count(), 0);
}

#[tokio::test]
async fn test_failover_try_count_retry() {
    let config = ClientConfigBuilder::new()
        .cluster_name("retry-test")
        .add_address("192.0.2.1:5701".parse().unwrap())
        .build()
        .unwrap();

    let failover = ClientFailoverConfig::builder()
        .add_client_config(config)
        .try_count(3)
        .build()
        .unwrap();

    let manager = ConnectionManager::with_failover(failover);

    assert_eq!(manager.current_cluster_index(), 0);

    let _ = manager.trigger_failover().await;
    assert_eq!(manager.current_cluster_index(), 0);

    let _ = manager.trigger_failover().await;
    assert_eq!(manager.current_cluster_index(), 0);

    let _ = manager.trigger_failover().await;
    assert_eq!(manager.current_cluster_index(), 0);
}

#[tokio::test]
async fn test_failover_config_replacement() {
    let config1 = ClientConfigBuilder::new()
        .cluster_name("first")
        .build()
        .unwrap();
    let config2 = ClientConfigBuilder::new()
        .cluster_name("second")
        .build()
        .unwrap();
    let config3 = ClientConfigBuilder::new()
        .cluster_name("third")
        .build()
        .unwrap();

    let failover = ClientFailoverConfig::builder()
        .add_client_config(config1)
        .client_configs([config2, config3])
        .build()
        .unwrap();

    assert_eq!(failover.cluster_count(), 2);
    assert_eq!(failover.get_config(0).unwrap().cluster_name(), "second");
    assert_eq!(failover.get_config(1).unwrap().cluster_name(), "third");
}

#[test]
fn test_failover_with_multiple_addresses_per_cluster() {
    let config = ClientConfigBuilder::new()
        .cluster_name("multi-addr")
        .add_address("192.168.1.1:5701".parse().unwrap())
        .add_address("192.168.1.2:5701".parse().unwrap())
        .add_address("192.168.1.3:5701".parse().unwrap())
        .build()
        .unwrap();

    let failover = ClientFailoverConfig::builder()
        .add_client_config(config)
        .build()
        .unwrap();

    let addresses = failover
        .get_config(0)
        .unwrap()
        .network()
        .addresses();

    assert_eq!(addresses.len(), 3);
}

#[tokio::test]
async fn test_failover_disconnects_from_old_cluster() {
    let (listener1, addr1) = create_mock_server().await;
    let (listener2, addr2) = create_mock_server().await;

    tokio::spawn(async move {
        let _ = listener1.accept().await;
    });
    tokio::spawn(async move {
        let _ = listener2.accept().await;
    });

    let failover = create_failover_config(
        vec![
            ("old-cluster".to_string(), addr1),
            ("new-cluster".to_string(), addr2),
        ],
        1,
    );

    let manager = ConnectionManager::with_failover(failover);
    manager.connect_to(addr1).await.unwrap();

    assert!(manager.is_connected(&addr1).await);

    let _ = manager.trigger_failover().await;

    assert!(!manager.is_connected(&addr1).await);
}

#[test]
fn test_lifecycle_event_client_changed_cluster() {
    let event = LifecycleEvent::ClientChangedCluster;
    assert_eq!(format!("{:?}", event), "ClientChangedCluster");
}

#[test]
fn test_lifecycle_events_are_send_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<LifecycleEvent>();
}
