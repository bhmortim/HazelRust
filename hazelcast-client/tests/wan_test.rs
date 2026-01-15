//! Integration tests for WAN replication client-side awareness.

use std::net::SocketAddr;
use std::time::Duration;

use hazelcast_client::config::{
    ClientConfigBuilder, WanReplicationConfigBuilder, WanTargetClusterConfigBuilder,
};
use hazelcast_client::wan::{WanEvent, WanEventType, WanPublisher, WanPublisherState};

fn create_wan_config() -> hazelcast_client::config::WanReplicationConfig {
    let target_west = WanTargetClusterConfigBuilder::new("dc-west")
        .add_endpoint("192.168.1.100:5701".parse().unwrap())
        .add_endpoint("192.168.1.101:5701".parse().unwrap())
        .connection_timeout(Duration::from_secs(5))
        .build()
        .unwrap();

    let target_east = WanTargetClusterConfigBuilder::new("dc-east")
        .add_endpoint("192.168.2.100:5701".parse().unwrap())
        .connection_timeout(Duration::from_secs(10))
        .build()
        .unwrap();

    WanReplicationConfigBuilder::new("geo-replication")
        .add_target_cluster(target_west)
        .add_target_cluster(target_east)
        .build()
        .unwrap()
}

#[test]
fn test_wan_target_cluster_config() {
    let target = WanTargetClusterConfigBuilder::new("remote-dc")
        .add_endpoint("10.0.0.1:5701".parse().unwrap())
        .add_endpoint("10.0.0.2:5701".parse().unwrap())
        .connection_timeout(Duration::from_secs(15))
        .build()
        .unwrap();

    assert_eq!(target.cluster_name(), "remote-dc");
    assert_eq!(target.endpoints().len(), 2);
    assert_eq!(target.connection_timeout(), Duration::from_secs(15));
}

#[test]
fn test_wan_target_cluster_config_validation() {
    let result = WanTargetClusterConfigBuilder::new("").build();
    assert!(result.is_err());

    let result = WanTargetClusterConfigBuilder::new("valid-name").build();
    assert!(result.is_err());
}

#[test]
fn test_wan_replication_config() {
    let config = create_wan_config();

    assert_eq!(config.name(), "geo-replication");
    assert_eq!(config.target_clusters().len(), 2);

    let west = config.find_target("dc-west");
    assert!(west.is_some());
    assert_eq!(west.unwrap().endpoints().len(), 2);

    let east = config.find_target("dc-east");
    assert!(east.is_some());
    assert_eq!(east.unwrap().endpoints().len(), 1);

    assert!(config.find_target("dc-north").is_none());
}

#[test]
fn test_wan_replication_config_validation() {
    let result = WanReplicationConfigBuilder::new("").build();
    assert!(result.is_err());

    let result = WanReplicationConfigBuilder::new("valid-name").build();
    assert!(result.is_err());
}

#[test]
fn test_client_config_with_wan() {
    let target = WanTargetClusterConfigBuilder::new("backup-dc")
        .add_endpoint("10.0.0.1:5701".parse().unwrap())
        .build()
        .unwrap();

    let wan_config = WanReplicationConfigBuilder::new("disaster-recovery")
        .add_target_cluster(target)
        .build()
        .unwrap();

    let config = ClientConfigBuilder::new()
        .cluster_name("primary-dc")
        .network(|n| n.add_wan_replication(wan_config))
        .build()
        .unwrap();

    assert_eq!(config.network().wan_replication().len(), 1);
    assert!(config.network().find_wan_replication("disaster-recovery").is_some());
}

#[test]
fn test_wan_event_creation() {
    let put_event = WanEvent::put("users", vec![1, 2, 3], vec![4, 5, 6]);
    assert_eq!(put_event.event_type(), WanEventType::Put);
    assert_eq!(put_event.map_name(), "users");
    assert_eq!(put_event.key(), &[1, 2, 3]);
    assert_eq!(put_event.value(), Some(&[4, 5, 6][..]));

    let remove_event = WanEvent::remove("users", vec![1, 2, 3]);
    assert_eq!(remove_event.event_type(), WanEventType::Remove);
    assert!(remove_event.value().is_none());

    let clear_event = WanEvent::clear("users");
    assert_eq!(clear_event.event_type(), WanEventType::Clear);
    assert!(clear_event.key().is_empty());
}

#[test]
fn test_wan_event_with_metadata() {
    let event = WanEvent::put("orders", vec![1], vec![2])
        .with_source_cluster("dc-primary")
        .with_partition_id(42)
        .with_old_value(vec![0]);

    assert_eq!(event.source_cluster(), Some("dc-primary"));
    assert_eq!(event.partition_id(), Some(42));
    assert_eq!(event.old_value(), Some(&[0][..]));
}

#[test]
fn test_wan_event_merge() {
    let event = WanEvent::merge(
        "inventory",
        vec![1, 2],
        vec![3, 4],
        "com.example.LatestUpdateMergePolicy",
    );

    assert_eq!(event.event_type(), WanEventType::Merge);
    assert_eq!(
        event.merge_policy(),
        Some("com.example.LatestUpdateMergePolicy")
    );
}

#[tokio::test]
async fn test_wan_publisher_creation() {
    let config = create_wan_config();
    let publisher = WanPublisher::new(config);

    assert_eq!(publisher.name(), "geo-replication");
    assert_eq!(publisher.state().await, WanPublisherState::Stopped);

    let targets = publisher.target_clusters();
    assert_eq!(targets.len(), 2);
    assert!(targets.contains(&"dc-west"));
    assert!(targets.contains(&"dc-east"));
}

#[tokio::test]
async fn test_wan_publisher_cannot_publish_when_stopped() {
    let config = create_wan_config();
    let publisher = WanPublisher::new(config);

    let event = WanEvent::put("test-map", vec![1], vec![2]);
    let result = publisher.publish(event).await;

    assert!(result.is_err());
}

#[tokio::test]
async fn test_wan_publisher_stats() {
    let config = create_wan_config();
    let publisher = WanPublisher::new(config);

    let stats = publisher.stats().await;

    assert_eq!(stats.name, "geo-replication");
    assert_eq!(stats.state, WanPublisherState::Stopped);
    assert_eq!(stats.total_targets, 2);
    assert!(stats.connected_targets.is_empty());
}

#[tokio::test]
async fn test_wan_publisher_stop_idempotent() {
    let config = create_wan_config();
    let publisher = WanPublisher::new(config);

    assert!(publisher.stop().await.is_ok());
    assert!(publisher.stop().await.is_ok());

    assert_eq!(publisher.state().await, WanPublisherState::Stopped);
}

#[tokio::test]
async fn test_wan_publisher_events() {
    let config = create_wan_config();
    let publisher = WanPublisher::new(config);

    let mut rx = publisher.subscribe();

    publisher.stop().await.unwrap();
}

#[test]
fn test_wan_publisher_with_custom_batching() {
    let config = create_wan_config();
    let publisher = WanPublisher::with_batching(
        config,
        500,
        Duration::from_millis(200),
    );

    assert_eq!(publisher.name(), "geo-replication");
}

#[test]
fn test_wan_event_type_properties() {
    assert!(WanEventType::Put.requires_value());
    assert!(WanEventType::Merge.requires_value());
    assert!(!WanEventType::Remove.requires_value());
    assert!(!WanEventType::Evict.requires_value());
    assert!(!WanEventType::Clear.requires_value());

    assert_eq!(WanEventType::Put.as_protocol_str(), "PUT");
    assert_eq!(WanEventType::Remove.as_protocol_str(), "REMOVE");
}

#[test]
fn test_multiple_wan_configs_in_network() {
    let target1 = WanTargetClusterConfigBuilder::new("dc-1")
        .add_endpoint("10.1.0.1:5701".parse().unwrap())
        .build()
        .unwrap();

    let target2 = WanTargetClusterConfigBuilder::new("dc-2")
        .add_endpoint("10.2.0.1:5701".parse().unwrap())
        .build()
        .unwrap();

    let wan1 = WanReplicationConfigBuilder::new("wan-scheme-1")
        .add_target_cluster(target1)
        .build()
        .unwrap();

    let wan2 = WanReplicationConfigBuilder::new("wan-scheme-2")
        .add_target_cluster(target2)
        .build()
        .unwrap();

    let config = ClientConfigBuilder::new()
        .network(|n| {
            n.add_wan_replication(wan1)
             .add_wan_replication(wan2)
        })
        .build()
        .unwrap();

    assert_eq!(config.network().wan_replication().len(), 2);
    assert!(config.network().find_wan_replication("wan-scheme-1").is_some());
    assert!(config.network().find_wan_replication("wan-scheme-2").is_some());
}

#[tokio::test]
async fn test_wan_publisher_connection_status() {
    let config = create_wan_config();
    let publisher = WanPublisher::new(config);

    assert!(!publisher.is_connected("dc-west").await);
    assert!(!publisher.is_connected("dc-east").await);
    assert!(!publisher.is_connected("nonexistent").await);

    assert_eq!(publisher.connected_count().await, 0);
}

#[test]
fn test_wan_replication_ref() {
    use hazelcast_client::config::WanReplicationRef;

    let wan_ref = WanReplicationRef::builder("my-wan-config")
        .merge_policy_class_name("com.example.CustomMergePolicy")
        .republishing_enabled(true)
        .add_filter("com.example.Filter1")
        .add_filter("com.example.Filter2")
        .build()
        .unwrap();

    assert_eq!(wan_ref.name(), "my-wan-config");
    assert_eq!(wan_ref.merge_policy_class_name(), "com.example.CustomMergePolicy");
    assert!(wan_ref.republishing_enabled());
    assert_eq!(wan_ref.filters().len(), 2);
}

#[test]
fn test_wan_replication_ref_defaults() {
    use hazelcast_client::config::WanReplicationRef;

    let wan_ref = WanReplicationRef::builder("default-config")
        .build()
        .unwrap();

    assert_eq!(wan_ref.name(), "default-config");
    assert_eq!(
        wan_ref.merge_policy_class_name(),
        "com.hazelcast.spi.merge.PassThroughMergePolicy"
    );
    assert!(!wan_ref.republishing_enabled());
    assert!(wan_ref.filters().is_empty());
}
