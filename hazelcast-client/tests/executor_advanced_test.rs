//! Advanced integration tests for ExecutorService.

use std::sync::Arc;
use std::time::Duration;

use hazelcast_client::config::ClientConfigBuilder;
use hazelcast_client::connection::ConnectionManager;
use hazelcast_client::executor::{
    Callable, CallableTask, ExecutionCallback, ExecutionTarget, ExecutorService,
    MemberSelector, Runnable, RunnableTask,
};
use hazelcast_client::listener::Member;
use hazelcast_core::serialization::ObjectDataOutput;
use hazelcast_core::{Deserializable, HazelcastError, Result, Serializable};
use uuid::Uuid;

fn create_test_manager() -> Arc<ConnectionManager> {
    let config = ClientConfigBuilder::new().build().unwrap();
    Arc::new(ConnectionManager::from_config(config))
}

struct SimpleCallable {
    value: i32,
}

impl Callable<i32> for SimpleCallable {}

impl Serializable for SimpleCallable {
    fn serialize(&self, output: &mut ObjectDataOutput) -> Result<()> {
        output.write_i32(self.value)?;
        Ok(())
    }
}

struct SimpleRunnable {
    message: String,
}

impl Runnable for SimpleRunnable {}

impl Serializable for SimpleRunnable {
    fn serialize(&self, output: &mut ObjectDataOutput) -> Result<()> {
        output.write_string(&self.message)?;
        Ok(())
    }
}

struct AllMemberSelector;

impl MemberSelector for AllMemberSelector {
    fn select(&self, _member: &Member) -> bool {
        true
    }
}

struct NoMemberSelector;

impl MemberSelector for NoMemberSelector {
    fn select(&self, _member: &Member) -> bool {
        false
    }
}

struct PortMemberSelector {
    port: u16,
}

impl MemberSelector for PortMemberSelector {
    fn select(&self, member: &Member) -> bool {
        member.address().port() == self.port
    }
}

#[derive(Debug)]
struct TestCallback {
    name: String,
}

impl ExecutionCallback<i32> for TestCallback {
    fn on_response(&self, result: i32) {
        println!("{}: received result {}", self.name, result);
    }

    fn on_failure(&self, error: HazelcastError) {
        println!("{}: received error {}", self.name, error);
    }
}

#[test]
fn test_executor_service_creation() {
    let cm = create_test_manager();
    let executor = ExecutorService::new("test-executor".to_string(), cm);

    assert_eq!(executor.name(), "test-executor");
}

#[test]
fn test_executor_service_is_send_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<ExecutorService>();
}

#[test]
fn test_execution_target_variants() {
    let any = ExecutionTarget::Any;
    let member = ExecutionTarget::Member(Uuid::new_v4());
    let key_owner = ExecutionTarget::KeyOwner(vec![1, 2, 3]);

    match any {
        ExecutionTarget::Any => {}
        _ => panic!("Expected Any"),
    }
    match member {
        ExecutionTarget::Member(_) => {}
        _ => panic!("Expected Member"),
    }
    match key_owner {
        ExecutionTarget::KeyOwner(_) => {}
        _ => panic!("Expected KeyOwner"),
    }
}

#[test]
fn test_callable_task_creation() {
    let callable = SimpleCallable { value: 42 };
    let task = CallableTask::<i32>::new(&callable);

    assert!(task.is_ok());
    let task = task.unwrap();
    assert!(!task.data().is_empty());
}

#[test]
fn test_runnable_task_creation() {
    let runnable = SimpleRunnable {
        message: "hello".to_string(),
    };
    let task = RunnableTask::new(&runnable);

    assert!(task.is_ok());
    let task = task.unwrap();
    assert!(!task.data().is_empty());
}

#[test]
fn test_member_selector_all() {
    let selector = AllMemberSelector;
    let member = Member::new(Uuid::new_v4(), "127.0.0.1:5701".parse().unwrap());

    assert!(selector.select(&member));
}

#[test]
fn test_member_selector_none() {
    let selector = NoMemberSelector;
    let member = Member::new(Uuid::new_v4(), "127.0.0.1:5701".parse().unwrap());

    assert!(!selector.select(&member));
}

#[test]
fn test_member_selector_by_port() {
    let selector = PortMemberSelector { port: 5701 };
    let member1 = Member::new(Uuid::new_v4(), "127.0.0.1:5701".parse().unwrap());
    let member2 = Member::new(Uuid::new_v4(), "127.0.0.1:5702".parse().unwrap());

    assert!(selector.select(&member1));
    assert!(!selector.select(&member2));
}

#[test]
fn test_compute_partition_id_empty() {
    let id = ExecutorService::compute_partition_id(&[]);
    assert_eq!(id, hazelcast_core::protocol::PARTITION_ID_ANY);
}

#[test]
fn test_compute_partition_id_deterministic() {
    let key = b"test-key";
    let id1 = ExecutorService::compute_partition_id(key);
    let id2 = ExecutorService::compute_partition_id(key);

    assert_eq!(id1, id2);
    assert!(id1 >= 0);
}

#[test]
fn test_compute_partition_id_different_keys() {
    let id1 = ExecutorService::compute_partition_id(b"key1");
    let id2 = ExecutorService::compute_partition_id(b"key2");

    assert!(id1 >= 0);
    assert!(id2 >= 0);
}

#[test]
fn test_compute_partition_id_range() {
    for i in 0..100 {
        let key = format!("test-key-{}", i);
        let id = ExecutorService::compute_partition_id(key.as_bytes());

        assert!(id >= 0);
        assert!(id < 271);
    }
}

#[tokio::test]
async fn test_submit_no_connections() {
    let cm = create_test_manager();
    let executor = ExecutorService::new("test-executor".to_string(), cm);

    let callable = SimpleCallable { value: 100 };
    let result = executor.submit::<_, i32>(&callable).await;

    assert!(result.is_err());
    match result.unwrap_err() {
        HazelcastError::Connection(_) => {}
        e => panic!("expected Connection error, got {:?}", e),
    }
}

#[tokio::test]
async fn test_execute_no_connections() {
    let cm = create_test_manager();
    let executor = ExecutorService::new("test-executor".to_string(), cm);

    let runnable = SimpleRunnable {
        message: "test".to_string(),
    };
    let result = executor.execute(&runnable).await;

    assert!(result.is_err());
    match result.unwrap_err() {
        HazelcastError::Connection(_) => {}
        e => panic!("expected Connection error, got {:?}", e),
    }
}

#[tokio::test]
async fn test_submit_to_all_members_no_members() {
    let cm = create_test_manager();
    let executor = ExecutorService::new("test-executor".to_string(), cm);

    let callable = SimpleCallable { value: 42 };
    let result = executor.submit_to_all_members::<_, i32>(&callable).await;

    assert!(result.is_err());
    match result.unwrap_err() {
        HazelcastError::Connection(msg) => {
            assert!(msg.contains("No cluster members"));
        }
        e => panic!("expected Connection error, got {:?}", e),
    }
}

#[tokio::test]
async fn test_execute_on_all_members_no_members() {
    let cm = create_test_manager();
    let executor = ExecutorService::new("test-executor".to_string(), cm);

    let runnable = SimpleRunnable {
        message: "broadcast".to_string(),
    };
    let result = executor.execute_on_all_members(&runnable).await;

    assert!(result.is_err());
    match result.unwrap_err() {
        HazelcastError::Connection(msg) => {
            assert!(msg.contains("No cluster members"));
        }
        e => panic!("expected Connection error, got {:?}", e),
    }
}

#[tokio::test]
async fn test_submit_to_members_no_matching() {
    let cm = create_test_manager();

    {
        let manager = &cm;
        let member = Member::new(Uuid::new_v4(), "127.0.0.1:5701".parse().unwrap());
        manager.handle_member_added(member).await;
    }

    let executor = ExecutorService::new("test-executor".to_string(), cm);

    let callable = SimpleCallable { value: 10 };
    let selector = NoMemberSelector;

    let result = executor.submit_to_members::<_, i32, _>(&callable, &selector).await;

    assert!(result.is_err());
    match result.unwrap_err() {
        HazelcastError::Connection(msg) => {
            assert!(msg.contains("No cluster members match"));
        }
        e => panic!("expected Connection error, got {:?}", e),
    }
}

#[tokio::test]
async fn test_execute_on_members_no_matching() {
    let cm = create_test_manager();

    {
        let manager = &cm;
        let member = Member::new(Uuid::new_v4(), "127.0.0.1:5701".parse().unwrap());
        manager.handle_member_added(member).await;
    }

    let executor = ExecutorService::new("test-executor".to_string(), cm);

    let runnable = SimpleRunnable {
        message: "selective".to_string(),
    };
    let selector = NoMemberSelector;

    let result = executor.execute_on_members(&runnable, &selector).await;

    assert!(result.is_err());
    match result.unwrap_err() {
        HazelcastError::Connection(msg) => {
            assert!(msg.contains("No cluster members match"));
        }
        e => panic!("expected Connection error, got {:?}", e),
    }
}

#[tokio::test]
async fn test_shutdown_no_connections() {
    let cm = create_test_manager();
    let executor = ExecutorService::new("test-executor".to_string(), cm);

    let result = executor.shutdown().await;

    assert!(result.is_err());
}

#[tokio::test]
async fn test_is_shutdown_no_connections() {
    let cm = create_test_manager();
    let executor = ExecutorService::new("test-executor".to_string(), cm);

    let result = executor.is_shutdown().await;

    assert!(result.is_err());
}

#[test]
fn test_executor_future_is_send() {
    use hazelcast_client::executor::ExecutorFuture;

    fn assert_send<T: Send>() {}
    assert_send::<ExecutorFuture<String>>();
    assert_send::<ExecutorFuture<i64>>();
    assert_send::<ExecutorFuture<Vec<u8>>>();
}

#[test]
fn test_callable_serialization() {
    let callable = SimpleCallable { value: 12345 };
    let task = CallableTask::<i32>::new(&callable).unwrap();
    let data = task.data();

    assert!(!data.is_empty());
    assert_eq!(data.len(), 4);
}

#[test]
fn test_runnable_serialization() {
    let runnable = SimpleRunnable {
        message: "test message".to_string(),
    };
    let task = RunnableTask::new(&runnable).unwrap();
    let data = task.data();

    assert!(!data.is_empty());
}

#[test]
fn test_execution_callback_trait() {
    let callback = TestCallback {
        name: "test".to_string(),
    };

    callback.on_response(42);
    callback.on_failure(HazelcastError::Connection("test error".to_string()));
}

#[test]
fn test_executor_service_clone() {
    fn assert_clone<T: Clone>() {}
    assert_clone::<ExecutorService>();
}

#[test]
fn test_executor_with_complex_callable() {
    struct ComplexCallable {
        items: Vec<String>,
        count: i32,
    }

    impl Callable<String> for ComplexCallable {}

    impl Serializable for ComplexCallable {
        fn serialize(&self, output: &mut ObjectDataOutput) -> Result<()> {
            output.write_i32(self.count)?;
            output.write_i32(self.items.len() as i32)?;
            for item in &self.items {
                output.write_string(item)?;
            }
            Ok(())
        }
    }

    let callable = ComplexCallable {
        items: vec!["a".to_string(), "b".to_string(), "c".to_string()],
        count: 3,
    };

    let task = CallableTask::<String>::new(&callable);
    assert!(task.is_ok());
}

#[tokio::test]
async fn test_submit_to_key_serializes_key() {
    let cm = create_test_manager();
    let executor = ExecutorService::new("test-executor".to_string(), cm);

    let callable = SimpleCallable { value: 1 };
    let key = "partition-key".to_string();

    let result = executor.submit_to_key::<_, i32, _>(&callable, &key).await;

    assert!(result.is_err());
}

#[tokio::test]
async fn test_submit_to_key_owner_alias() {
    let cm = create_test_manager();
    let executor = ExecutorService::new("test-executor".to_string(), cm);

    let callable = SimpleCallable { value: 2 };
    let key = 42i64;

    let result = executor.submit_to_key_owner::<_, i32, _>(&callable, &key).await;

    assert!(result.is_err());
}
