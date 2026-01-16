//! Integration tests for AtomicReference operations.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use hazelcast_client::config::{
    ClientConfigBuilder, PermissionAction, Permissions, QuorumConfig, QuorumType,
};
use hazelcast_client::connection::ConnectionManager;
use hazelcast_client::proxy::AtomicReference;
use hazelcast_core::HazelcastError;

fn create_test_manager() -> Arc<ConnectionManager> {
    let config = ClientConfigBuilder::new().build().unwrap();
    Arc::new(ConnectionManager::from_config(config))
}

fn create_manager_with_permissions(perms: Permissions) -> Arc<ConnectionManager> {
    let config = ClientConfigBuilder::new()
        .security(|s| s.permissions(perms))
        .build()
        .unwrap();
    Arc::new(ConnectionManager::from_config(config))
}

fn create_manager_with_quorum(quorum: QuorumConfig) -> Arc<ConnectionManager> {
    let config = ClientConfigBuilder::new()
        .add_quorum_config(quorum)
        .build()
        .unwrap();
    Arc::new(ConnectionManager::from_config(config))
}

#[test]
fn test_atomic_reference_creation() {
    let cm = create_test_manager();
    let reference: AtomicReference<String> = AtomicReference::new("test-ref".to_string(), cm);

    assert_eq!(reference.name(), "test-ref");
}

#[test]
fn test_atomic_reference_is_send_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<AtomicReference<String>>();
    assert_send_sync::<AtomicReference<i64>>();
    assert_send_sync::<AtomicReference<Vec<u8>>>();
}

#[test]
fn test_atomic_reference_clone() {
    let cm = create_test_manager();
    let reference: AtomicReference<String> = AtomicReference::new("ref1".to_string(), cm);
    let cloned = reference.clone();

    assert_eq!(reference.name(), cloned.name());
}

#[tokio::test]
async fn test_atomic_reference_get_permission_denied() {
    let mut perms = Permissions::new();
    perms.grant(PermissionAction::Put);

    let cm = create_manager_with_permissions(perms);
    let reference: AtomicReference<String> = AtomicReference::new("test".to_string(), cm);

    let result = reference.get().await;
    assert!(matches!(result, Err(HazelcastError::Authorization(_))));
}

#[tokio::test]
async fn test_atomic_reference_set_permission_denied() {
    let mut perms = Permissions::new();
    perms.grant(PermissionAction::Read);

    let cm = create_manager_with_permissions(perms);
    let reference: AtomicReference<String> = AtomicReference::new("test".to_string(), cm);

    let result = reference.set(Some("value".to_string())).await;
    assert!(matches!(result, Err(HazelcastError::Authorization(_))));
}

#[tokio::test]
async fn test_atomic_reference_get_and_set_permission_denied() {
    let mut perms = Permissions::new();
    perms.grant(PermissionAction::Read);

    let cm = create_manager_with_permissions(perms);
    let reference: AtomicReference<String> = AtomicReference::new("test".to_string(), cm);

    let result = reference.get_and_set(Some("value".to_string())).await;
    assert!(matches!(result, Err(HazelcastError::Authorization(_))));
}

#[tokio::test]
async fn test_atomic_reference_compare_and_set_permission_denied() {
    let mut perms = Permissions::new();
    perms.grant(PermissionAction::Read);

    let cm = create_manager_with_permissions(perms);
    let reference: AtomicReference<String> = AtomicReference::new("test".to_string(), cm);

    let result = reference.compare_and_set(None, Some("value".to_string())).await;
    assert!(matches!(result, Err(HazelcastError::Authorization(_))));
}

#[tokio::test]
async fn test_atomic_reference_contains_permission_denied() {
    let mut perms = Permissions::new();
    perms.grant(PermissionAction::Put);

    let cm = create_manager_with_permissions(perms);
    let reference: AtomicReference<String> = AtomicReference::new("test".to_string(), cm);

    let result = reference.contains(&"value".to_string()).await;
    assert!(matches!(result, Err(HazelcastError::Authorization(_))));
}

#[tokio::test]
async fn test_atomic_reference_is_null_permission_denied() {
    let mut perms = Permissions::new();
    perms.grant(PermissionAction::Put);

    let cm = create_manager_with_permissions(perms);
    let reference: AtomicReference<String> = AtomicReference::new("test".to_string(), cm);

    let result = reference.is_null().await;
    assert!(matches!(result, Err(HazelcastError::Authorization(_))));
}

#[tokio::test]
async fn test_atomic_reference_quorum_blocks_read() {
    let quorum = QuorumConfig::builder("protected-*")
        .min_cluster_size(3)
        .quorum_type(QuorumType::Read)
        .build()
        .unwrap();

    let cm = create_manager_with_quorum(quorum);
    let reference: AtomicReference<String> =
        AtomicReference::new("protected-ref".to_string(), cm);

    let result = reference.get().await;
    assert!(matches!(result, Err(HazelcastError::QuorumNotPresent(_))));

    let result = reference.is_null().await;
    assert!(matches!(result, Err(HazelcastError::QuorumNotPresent(_))));

    let result = reference.contains(&"test".to_string()).await;
    assert!(matches!(result, Err(HazelcastError::QuorumNotPresent(_))));
}

#[tokio::test]
async fn test_atomic_reference_quorum_blocks_write() {
    let quorum = QuorumConfig::builder("protected-*")
        .min_cluster_size(3)
        .quorum_type(QuorumType::Write)
        .build()
        .unwrap();

    let cm = create_manager_with_quorum(quorum);
    let reference: AtomicReference<String> =
        AtomicReference::new("protected-ref".to_string(), cm);

    let result = reference.set(Some("value".to_string())).await;
    assert!(matches!(result, Err(HazelcastError::QuorumNotPresent(_))));

    let result = reference.get_and_set(Some("value".to_string())).await;
    assert!(matches!(result, Err(HazelcastError::QuorumNotPresent(_))));

    let result = reference
        .compare_and_set(None, Some("value".to_string()))
        .await;
    assert!(matches!(result, Err(HazelcastError::QuorumNotPresent(_))));
}

#[tokio::test]
async fn test_atomic_reference_quorum_read_write() {
    let quorum = QuorumConfig::builder("all-protected-*")
        .min_cluster_size(3)
        .quorum_type(QuorumType::ReadWrite)
        .build()
        .unwrap();

    let cm = create_manager_with_quorum(quorum);
    let reference: AtomicReference<String> =
        AtomicReference::new("all-protected-ref".to_string(), cm);

    let read_result = reference.get().await;
    assert!(matches!(
        read_result,
        Err(HazelcastError::QuorumNotPresent(_))
    ));

    let write_result = reference.set(Some("value".to_string())).await;
    assert!(matches!(
        write_result,
        Err(HazelcastError::QuorumNotPresent(_))
    ));
}

#[tokio::test]
async fn test_atomic_reference_quorum_pattern_matching() {
    let quorum = QuorumConfig::builder("user-*")
        .min_cluster_size(2)
        .quorum_type(QuorumType::ReadWrite)
        .build()
        .unwrap();

    let cm = create_manager_with_quorum(quorum);

    let protected: AtomicReference<String> =
        AtomicReference::new("user-sessions".to_string(), Arc::clone(&cm));
    let result = protected.get().await;
    assert!(matches!(result, Err(HazelcastError::QuorumNotPresent(_))));

    let unprotected: AtomicReference<String> =
        AtomicReference::new("other-ref".to_string(), Arc::clone(&cm));
    let result = unprotected.get().await;
    assert!(!matches!(result, Err(HazelcastError::QuorumNotPresent(_))));
}

#[test]
fn test_atomic_reference_with_different_types() {
    let cm = create_test_manager();

    let _string_ref: AtomicReference<String> =
        AtomicReference::new("str-ref".to_string(), Arc::clone(&cm));
    let _i64_ref: AtomicReference<i64> =
        AtomicReference::new("i64-ref".to_string(), Arc::clone(&cm));
    let _vec_ref: AtomicReference<Vec<u8>> =
        AtomicReference::new("vec-ref".to_string(), Arc::clone(&cm));
}

#[tokio::test]
async fn test_atomic_reference_clear_calls_set_none() {
    let mut perms = Permissions::new();
    perms.grant(PermissionAction::Read);

    let cm = create_manager_with_permissions(perms);
    let reference: AtomicReference<String> = AtomicReference::new("test".to_string(), cm);

    let result = reference.clear().await;
    assert!(matches!(result, Err(HazelcastError::Authorization(_))));
}

#[tokio::test]
async fn test_atomic_reference_permissions_with_all() {
    let cm = create_test_manager();
    let reference: AtomicReference<String> = AtomicReference::new("test".to_string(), cm);

    reference
        .check_permission(PermissionAction::Read)
        .expect("read should be permitted");
    reference
        .check_permission(PermissionAction::Put)
        .expect("put should be permitted");
}

#[test]
fn test_atomic_reference_name_preserved_on_clone() {
    let cm = create_test_manager();
    let original: AtomicReference<i32> = AtomicReference::new("counter".to_string(), cm);
    let cloned = original.clone();

    assert_eq!(original.name(), "counter");
    assert_eq!(cloned.name(), "counter");
}
