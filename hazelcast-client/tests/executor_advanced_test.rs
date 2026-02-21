//! Advanced integration tests for ExecutorService.
//!
//! These tests verify executor service behavior using the public API.
//! Tests that require a running cluster are marked with `#[ignore]`.

use std::net::SocketAddr;

use hazelcast_client::executor::{
    Callable, CallableTask, ExecutionCallback, ExecutionTarget, ExecutorService,
    MemberSelector, Runnable, RunnableTask,
};
use hazelcast_client::listener::Member;
use hazelcast_client::{ClientConfig, HazelcastClient};
use hazelcast_core::serialization::DataOutput;
use hazelcast_core::{HazelcastError, Result, Serializable};
use uuid::Uuid;

async fn create_test_client() -> HazelcastClient {
    let config = ClientConfig::builder()
        .cluster_name("dev")
        .add_address("127.0.0.1:5701".parse::<SocketAddr>().unwrap())
        .build()
        .expect("Failed to build config");
    HazelcastClient::new(config)
        .await
        .expect("Failed to connect to Hazelcast")
}

struct SimpleCallable {
    value: i32,
}

impl Callable<i32> for SimpleCallable {
    fn factory_id(&self) -> i32 { 1 }
    fn class_id(&self) -> i32 { 1 }
}

impl Serializable for SimpleCallable {
    fn serialize<W: DataOutput>(&self, output: &mut W) -> Result<()> {
        output.write_int(self.value)?;
        Ok(())
    }
}

struct SimpleRunnable {
    message: String,
}

impl Runnable for SimpleRunnable {
    fn factory_id(&self) -> i32 { 1 }
    fn class_id(&self) -> i32 { 2 }
}

impl Serializable for SimpleRunnable {
    fn serialize<W: DataOutput>(&self, output: &mut W) -> Result<()> {
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

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_submit_callable() {
    let client = create_test_client().await;
    let executor = client.get_executor_service("test-executor");

    let callable = SimpleCallable { value: 100 };
    let result = executor.submit::<_, i32>(&callable).await;

    // This may fail depending on cluster config - the test just verifies the API works
    println!("Submit result: {:?}", result);

    client.shutdown().await.expect("shutdown failed");
}

#[tokio::test]
#[ignore = "requires running Hazelcast cluster"]
async fn test_execute_runnable() {
    let client = create_test_client().await;
    let executor = client.get_executor_service("test-executor");

    let runnable = SimpleRunnable {
        message: "test".to_string(),
    };
    let result = executor.execute(&runnable).await;

    println!("Execute result: {:?}", result);

    client.shutdown().await.expect("shutdown failed");
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
fn test_executor_with_complex_callable() {
    struct ComplexCallable {
        items: Vec<String>,
        count: i32,
    }

    impl Callable<String> for ComplexCallable {
        fn factory_id(&self) -> i32 { 1 }
        fn class_id(&self) -> i32 { 3 }
    }

    impl Serializable for ComplexCallable {
        fn serialize<W: DataOutput>(&self, output: &mut W) -> Result<()> {
            output.write_int(self.count)?;
            output.write_int(self.items.len() as i32)?;
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
#[ignore = "requires running Hazelcast cluster"]
async fn test_submit_to_key() {
    let client = create_test_client().await;
    let executor = client.get_executor_service("test-executor");

    let callable = SimpleCallable { value: 1 };
    let key = "partition-key".to_string();

    let result = executor.submit_to_key::<_, i32, _>(&callable, &key).await;
    println!("Submit to key result: {:?}", result);

    client.shutdown().await.expect("shutdown failed");
}
