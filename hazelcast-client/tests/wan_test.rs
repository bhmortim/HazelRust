//! Integration tests for WAN replication configuration.
//!
//! Note: The WAN runtime types (WanPublisher, WanEvent, etc.) are internal
//! and not part of the public API. These tests verify the publicly available
//! WAN configuration types from the config module.

use std::time::Duration;

use hazelcast_client::config::{
    ClientConfigBuilder, WanReplicationConfigBuilder, WanReplicationRef,
    WanTargetClusterConfigBuilder,
};

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
    assert!(result.is_err()); // No endpoints added
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
    assert!(result.is_err()); // No target clusters
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

#[test]
fn test_wan_replication_ref() {
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
