//! Integration tests for async map operations.

use std::sync::Arc;
use std::time::Duration;

use hazelcast_client::config::{
    ClientConfigBuilder, PermissionAction, Permissions, QuorumConfig, QuorumType,
};
use hazelcast_client::connection::ConnectionManager;
use hazelcast_client::proxy::IMap;
use hazelcast_core::{HazelcastError, Result};

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
fn test_async_methods_return_join_handles() {
    let cm = create_test_manager();
    let map: IMap<String, String> = IMap::new("test-map".to_string(), cm);

    let _get_handle: tokio::task::JoinHandle<Result<Option<String>>> =
        map.get_async("key".to_string());
    let _put_handle: tokio::task::JoinHandle<Result<Option<String>>> =
        map.put_async("key".to_string(), "value".to_string());
    let _remove_handle: tokio::task::JoinHandle<Result<Option<String>>> =
        map.remove_async("key".to_string());
    let _contains_handle: tokio::task::JoinHandle<Result<bool>> =
        map.contains_key_async("key".to_string());
}

#[tokio::test]
async fn test_get_async_spawns_task() {
    let cm = create_test_manager();
    let map: IMap<String, String> = IMap::new("async-map".to_string(), cm);

    let handle = map.get_async("nonexistent".to_string());

    assert!(!handle.is_finished());
}

#[tokio::test]
async fn test_put_async_spawns_task() {
    let cm = create_test_manager();
    let map: IMap<String, String> = IMap::new("async-map".to_string(), cm);

    let handle = map.put_async("key".to_string(), "value".to_string());

    assert!(!handle.is_finished());
}

#[tokio::test]
async fn test_remove_async_spawns_task() {
    let cm = create_test_manager();
    let map: IMap<String, String> = IMap::new("async-map".to_string(), cm);

    let handle = map.remove_async("key".to_string());

    assert!(!handle.is_finished());
}

#[tokio::test]
async fn test_contains_key_async_spawns_task() {
    let cm = create_test_manager();
    let map: IMap<String, String> = IMap::new("async-map".to_string(), cm);

    let handle = map.contains_key_async("key".to_string());

    assert!(!handle.is_finished());
}

#[tokio::test]
async fn test_multiple_async_operations_concurrent() {
    let cm = create_test_manager();
    let map: IMap<String, String> = IMap::new("concurrent-map".to_string(), cm);

    let handle1 = map.get_async("key1".to_string());
    let handle2 = map.get_async("key2".to_string());
    let handle3 = map.get_async("key3".to_string());

    assert!(!handle1.is_finished() || !handle2.is_finished() || !handle3.is_finished());
}

#[tokio::test]
async fn test_get_async_with_read_permission_denied() {
    let mut perms = Permissions::new();
    perms.grant(PermissionAction::Put);

    let cm = create_manager_with_permissions(perms);
    let map: IMap<String, String> = IMap::new("test-map".to_string(), cm);

    let handle = map.get_async("key".to_string());
    let result = handle.await;

    assert!(result.is_ok());
    let inner_result = result.unwrap();
    assert!(matches!(inner_result, Err(HazelcastError::Authorization(_))));
}

#[tokio::test]
async fn test_put_async_with_put_permission_denied() {
    let mut perms = Permissions::new();
    perms.grant(PermissionAction::Read);

    let cm = create_manager_with_permissions(perms);
    let map: IMap<String, String> = IMap::new("test-map".to_string(), cm);

    let handle = map.put_async("key".to_string(), "value".to_string());
    let result = handle.await;

    assert!(result.is_ok());
    let inner_result = result.unwrap();
    assert!(matches!(inner_result, Err(HazelcastError::Authorization(_))));
}

#[tokio::test]
async fn test_remove_async_with_remove_permission_denied() {
    let mut perms = Permissions::new();
    perms.grant(PermissionAction::Read);
    perms.grant(PermissionAction::Put);

    let cm = create_manager_with_permissions(perms);
    let map: IMap<String, String> = IMap::new("test-map".to_string(), cm);

    let handle = map.remove_async("key".to_string());
    let result = handle.await;

    assert!(result.is_ok());
    let inner_result = result.unwrap();
    assert!(matches!(inner_result, Err(HazelcastError::Authorization(_))));
}

#[tokio::test]
async fn test_contains_key_async_with_read_permission_denied() {
    let mut perms = Permissions::new();
    perms.grant(PermissionAction::Put);

    let cm = create_manager_with_permissions(perms);
    let map: IMap<String, String> = IMap::new("test-map".to_string(), cm);

    let handle = map.contains_key_async("key".to_string());
    let result = handle.await;

    assert!(result.is_ok());
    let inner_result = result.unwrap();
    assert!(matches!(inner_result, Err(HazelcastError::Authorization(_))));
}

#[tokio::test]
async fn test_async_operations_with_quorum() {
    let quorum = QuorumConfig::builder("protected-*")
        .min_cluster_size(3)
        .quorum_type(QuorumType::ReadWrite)
        .build()
        .unwrap();

    let cm = create_manager_with_quorum(quorum);
    let map: IMap<String, String> = IMap::new("protected-async-map".to_string(), cm);

    let get_handle = map.get_async("key".to_string());
    let get_result = get_handle.await.unwrap();
    assert!(matches!(
        get_result,
        Err(HazelcastError::QuorumNotPresent(_))
    ));

    let put_handle = map.put_async("key".to_string(), "value".to_string());
    let put_result = put_handle.await.unwrap();
    assert!(matches!(
        put_result,
        Err(HazelcastError::QuorumNotPresent(_))
    ));

    let remove_handle = map.remove_async("key".to_string());
    let remove_result = remove_handle.await.unwrap();
    assert!(matches!(
        remove_result,
        Err(HazelcastError::QuorumNotPresent(_))
    ));

    let contains_handle = map.contains_key_async("key".to_string());
    let contains_result = contains_handle.await.unwrap();
    assert!(matches!(
        contains_result,
        Err(HazelcastError::QuorumNotPresent(_))
    ));
}

#[test]
fn test_async_methods_from_cloned_map() {
    let cm = create_test_manager();
    let map: IMap<String, String> = IMap::new("clone-test".to_string(), cm);
    let cloned = map.clone();

    let _handle1 = map.get_async("key1".to_string());
    let _handle2 = cloned.get_async("key2".to_string());
}

#[tokio::test]
async fn test_async_operations_fire_and_forget() {
    let cm = create_test_manager();
    let map: IMap<String, i32> = IMap::new("fire-forget-map".to_string(), cm);

    let _h1 = map.put_async("counter1".to_string(), 1);
    let _h2 = map.put_async("counter2".to_string(), 2);
    let _h3 = map.put_async("counter3".to_string(), 3);
}

#[test]
fn test_join_handle_types() {
    fn assert_result_option<T>(_: tokio::task::JoinHandle<Result<Option<T>>>) {}
    fn assert_result_bool(_: tokio::task::JoinHandle<Result<bool>>) {}

    let cm = create_test_manager();
    let map: IMap<String, String> = IMap::new("type-check".to_string(), cm);

    assert_result_option(map.get_async("k".to_string()));
    assert_result_option(map.put_async("k".to_string(), "v".to_string()));
    assert_result_option(map.remove_async("k".to_string()));
    assert_result_bool(map.contains_key_async("k".to_string()));
}

#[tokio::test]
async fn test_async_batch_operations() {
    let cm = create_test_manager();
    let map: IMap<String, i64> = IMap::new("batch-async".to_string(), cm);

    let handles: Vec<_> = (0..10)
        .map(|i| map.put_async(format!("key-{}", i), i))
        .collect();

    assert_eq!(handles.len(), 10);
}

#[test]
fn test_async_methods_with_different_types() {
    let cm = create_test_manager();

    let string_map: IMap<String, String> = IMap::new("string-map".to_string(), Arc::clone(&cm));
    let _h = string_map.get_async("key".to_string());

    let i64_map: IMap<i64, i64> = IMap::new("i64-map".to_string(), Arc::clone(&cm));
    let _h = i64_map.get_async(42);

    let vec_map: IMap<String, Vec<u8>> = IMap::new("vec-map".to_string(), Arc::clone(&cm));
    let _h = vec_map.get_async("bytes".to_string());
}

#[tokio::test]
async fn test_async_operation_preserves_map_state() {
    let cm = create_test_manager();
    let map: IMap<String, String> = IMap::new("state-test".to_string(), cm);

    assert_eq!(map.name(), "state-test");

    let _h = map.get_async("key".to_string());

    assert_eq!(map.name(), "state-test");
}
