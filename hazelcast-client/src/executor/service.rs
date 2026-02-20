//! Executor service implementation for distributed task execution.

use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::oneshot;
use uuid::Uuid;

use hazelcast_core::protocol::{
    ClientMessage, Frame,
    EXECUTOR_SHUTDOWN, EXECUTOR_IS_SHUTDOWN,
    EXECUTOR_SUBMIT_TO_MEMBER, EXECUTOR_SUBMIT_TO_PARTITION,
    PARTITION_ID_ANY, RESPONSE_HEADER_SIZE,
};
use hazelcast_core::{Deserializable, HazelcastError, ObjectDataInput, Result, Serializable};

use crate::connection::ConnectionManager;
use crate::listener::Member;
use super::{Callable, CallableTask, ExecutionCallback, ExecutionTarget, MemberSelector, Runnable, RunnableTask};

/// A handle to a pending executor task result.
///
/// This future-like type allows waiting for the result of a submitted task.
#[derive(Debug)]
pub struct ExecutorFuture<T> {
    receiver: oneshot::Receiver<Result<T>>,
    _marker: PhantomData<T>,
}

impl<T> ExecutorFuture<T> {
    pub(crate) fn new(receiver: oneshot::Receiver<Result<T>>) -> Self {
        Self {
            receiver,
            _marker: PhantomData,
        }
    }

    /// Waits for the task result.
    ///
    /// Returns the task result when available, or an error if the task failed
    /// or the executor was shut down.
    pub async fn get(self) -> Result<T> {
        self.receiver.await.map_err(|_| {
            HazelcastError::Connection(
                "Executor task was cancelled or executor shut down".to_string(),
            )
        })?
    }

    /// Waits for the task result with a timeout.
    pub async fn get_timeout(self, timeout: Duration) -> Result<T> {
        tokio::time::timeout(timeout, self.get())
            .await
            .map_err(|_| HazelcastError::Timeout("Executor task timed out".to_string()))?
    }
}

impl super::ExecutorService {
    /// Submits a callable task to any available cluster member.
    ///
    /// The task will be executed on a member chosen by the cluster and the
    /// result will be available through the returned future.
    pub async fn submit<T, R>(&self, task: &T) -> Result<ExecutorFuture<R>>
    where
        T: Callable<R>,
        R: Deserializable + Send + 'static,
    {
        self.submit_to_target(task, ExecutionTarget::Any).await
    }

    /// Submits a callable task to a specific cluster member.
    pub async fn submit_to_member<T, R>(&self, task: &T, member: &Member) -> Result<ExecutorFuture<R>>
    where
        T: Callable<R>,
        R: Deserializable + Send + 'static,
    {
        self.submit_to_target(task, ExecutionTarget::Member(member.uuid()))
            .await
    }

    /// Submits a callable task to the member owning the specified key's partition.
    ///
    /// This ensures the task executes on the same member that owns the data
    /// for the given key, which can be useful for data locality.
    pub async fn submit_to_key<T, R, K>(&self, task: &T, key: &K) -> Result<ExecutorFuture<R>>
    where
        T: Callable<R>,
        R: Deserializable + Send + 'static,
        K: Serializable,
    {
        let key_data = key.to_bytes()?;
        self.submit_to_target(task, ExecutionTarget::KeyOwner(key_data))
            .await
    }

    /// Submits a callable task to the member owning the specified key's partition.
    ///
    /// Alias for [`submit_to_key`] for API consistency.
    pub async fn submit_to_key_owner<T, R, K>(&self, task: &T, key: &K) -> Result<ExecutorFuture<R>>
    where
        T: Callable<R>,
        R: Deserializable + Send + 'static,
        K: Serializable,
    {
        self.submit_to_key(task, key).await
    }

    /// Submits a callable task to all cluster members.
    ///
    /// Returns a vector of futures, one for each member's result.
    pub async fn submit_to_all_members<T, R>(&self, task: &T) -> Result<Vec<ExecutorFuture<R>>>
    where
        T: Callable<R>,
        R: Deserializable + Send + 'static,
    {
        let members = self.connection_manager.members().await;
        if members.is_empty() {
            return Err(HazelcastError::Connection(
                "No cluster members available".to_string(),
            ));
        }

        let mut futures = Vec::with_capacity(members.len());
        for member in &members {
            let future = self.submit_to_member(task, member).await?;
            futures.push(future);
        }
        Ok(futures)
    }

    /// Submits a callable task to cluster members matching the selector.
    ///
    /// Returns a vector of futures, one for each selected member's result.
    pub async fn submit_to_members<T, R, S>(
        &self,
        task: &T,
        selector: &S,
    ) -> Result<Vec<ExecutorFuture<R>>>
    where
        T: Callable<R>,
        R: Deserializable + Send + 'static,
        S: MemberSelector,
    {
        let members = self.connection_manager.members().await;
        let selected: Vec<_> = members.iter().filter(|m| selector.select(m)).collect();

        if selected.is_empty() {
            return Err(HazelcastError::Connection(
                "No cluster members match the selector".to_string(),
            ));
        }

        let mut futures = Vec::with_capacity(selected.len());
        for member in selected {
            let future = self.submit_to_member(task, member).await?;
            futures.push(future);
        }
        Ok(futures)
    }

    /// Executes a runnable task on any available cluster member.
    ///
    /// The task is submitted for execution without waiting for completion.
    pub async fn execute<R>(&self, task: &R) -> Result<()>
    where
        R: Runnable,
    {
        self.execute_on_target(task, ExecutionTarget::Any).await
    }

    /// Executes a runnable task on a specific cluster member.
    pub async fn execute_on_member<R>(&self, task: &R, member: &Member) -> Result<()>
    where
        R: Runnable,
    {
        self.execute_on_target(task, ExecutionTarget::Member(member.uuid()))
            .await
    }

    /// Executes a runnable task on all cluster members.
    ///
    /// The task is submitted to each member for execution.
    pub async fn execute_on_all_members<R>(&self, task: &R) -> Result<()>
    where
        R: Runnable,
    {
        let members = self.connection_manager.members().await;
        if members.is_empty() {
            return Err(HazelcastError::Connection(
                "No cluster members available".to_string(),
            ));
        }

        for member in &members {
            self.execute_on_member(task, member).await?;
        }
        Ok(())
    }

    /// Executes a runnable task on cluster members matching the selector.
    pub async fn execute_on_members<R, S>(&self, task: &R, selector: &S) -> Result<()>
    where
        R: Runnable,
        S: MemberSelector,
    {
        let members = self.connection_manager.members().await;
        let selected: Vec<_> = members.iter().filter(|m| selector.select(m)).collect();

        if selected.is_empty() {
            return Err(HazelcastError::Connection(
                "No cluster members match the selector".to_string(),
            ));
        }

        for member in selected {
            self.execute_on_member(task, member).await?;
        }
        Ok(())
    }

    async fn execute_on_target<R>(&self, task: &R, target: ExecutionTarget) -> Result<()>
    where
        R: Runnable,
    {
        let runnable_task = RunnableTask::new(task)?;
        let task_uuid = Uuid::new_v4();

        let message = match &target {
            ExecutionTarget::Any => {
                let mut msg =
                    ClientMessage::create_for_encode(EXECUTOR_SUBMIT_TO_PARTITION, PARTITION_ID_ANY);
                msg.add_frame(Self::string_frame(&self.name));
                msg.add_frame(Self::uuid_frame(task_uuid));
                msg.add_frame(Self::data_frame(runnable_task.data()));
                msg
            }
            ExecutionTarget::Member(member_uuid) => {
                let mut msg =
                    ClientMessage::create_for_encode(EXECUTOR_SUBMIT_TO_MEMBER, PARTITION_ID_ANY);
                msg.add_frame(Self::string_frame(&self.name));
                msg.add_frame(Self::uuid_frame(task_uuid));
                msg.add_frame(Self::data_frame(runnable_task.data()));
                msg.add_frame(Self::uuid_frame(*member_uuid));
                msg
            }
            ExecutionTarget::KeyOwner(key_data) => {
                let partition_id = Self::compute_partition_id(key_data);
                let mut msg =
                    ClientMessage::create_for_encode(EXECUTOR_SUBMIT_TO_PARTITION, partition_id);
                msg.add_frame(Self::string_frame(&self.name));
                msg.add_frame(Self::uuid_frame(task_uuid));
                msg.add_frame(Self::data_frame(runnable_task.data()));
                msg
            }
        };

        self.invoke(message).await?;
        Ok(())
    }

    /// Submits a callable task with a callback for result handling.
    ///
    /// The callback's `on_response` method is called with the result on success,
    /// or `on_failure` is called with the error on failure.
    pub async fn submit_with_callback<T, R, C>(
        &self,
        task: &T,
        callback: Arc<C>,
    ) -> Result<()>
    where
        T: Callable<R>,
        R: Deserializable + Send + 'static,
        C: ExecutionCallback<R> + 'static,
    {
        let future = self.submit(task).await?;
        Self::spawn_callback_handler(future, callback);
        Ok(())
    }

    /// Submits a callable task to a specific member with a callback.
    pub async fn submit_to_member_with_callback<T, R, C>(
        &self,
        task: &T,
        member: &Member,
        callback: Arc<C>,
    ) -> Result<()>
    where
        T: Callable<R>,
        R: Deserializable + Send + 'static,
        C: ExecutionCallback<R> + 'static,
    {
        let future = self.submit_to_member(task, member).await?;
        Self::spawn_callback_handler(future, callback);
        Ok(())
    }

    /// Submits a callable task to a key owner with a callback.
    pub async fn submit_to_key_owner_with_callback<T, R, K, C>(
        &self,
        task: &T,
        key: &K,
        callback: Arc<C>,
    ) -> Result<()>
    where
        T: Callable<R>,
        R: Deserializable + Send + 'static,
        K: Serializable,
        C: ExecutionCallback<R> + 'static,
    {
        let future = self.submit_to_key_owner(task, key).await?;
        Self::spawn_callback_handler(future, callback);
        Ok(())
    }

    /// Submits a callable task to all members with callbacks.
    ///
    /// Each member's result will trigger the callback independently.
    pub async fn submit_to_all_members_with_callback<T, R, C>(
        &self,
        task: &T,
        callback: Arc<C>,
    ) -> Result<()>
    where
        T: Callable<R>,
        R: Deserializable + Send + 'static,
        C: ExecutionCallback<R> + 'static,
    {
        let futures = self.submit_to_all_members(task).await?;
        for future in futures {
            Self::spawn_callback_handler(future, Arc::clone(&callback));
        }
        Ok(())
    }

    fn spawn_callback_handler<R, C>(future: ExecutorFuture<R>, callback: Arc<C>)
    where
        R: Send + 'static,
        C: ExecutionCallback<R> + 'static,
    {
        tokio::spawn(async move {
            match future.get().await {
                Ok(result) => callback.on_response(result),
                Err(error) => callback.on_failure(error),
            }
        });
    }

    /// Shuts down the executor service.
    ///
    /// After shutdown, no new tasks can be submitted.
    pub async fn shutdown(&self) -> Result<()> {
        let mut message = ClientMessage::create_for_encode(EXECUTOR_SHUTDOWN, PARTITION_ID_ANY);
        message.add_frame(Self::string_frame(&self.name));

        self.invoke(message).await?;
        Ok(())
    }

    /// Checks if the executor service is shut down.
    pub async fn is_shutdown(&self) -> Result<bool> {
        let mut message = ClientMessage::create_for_encode(EXECUTOR_IS_SHUTDOWN, PARTITION_ID_ANY);
        message.add_frame(Self::string_frame(&self.name));

        let response = self.invoke(message).await?;
        Self::decode_bool_response(&response)
    }

    async fn submit_to_target<T, R>(
        &self,
        task: &T,
        target: ExecutionTarget,
    ) -> Result<ExecutorFuture<R>>
    where
        T: Callable<R>,
        R: Deserializable + Send + 'static,
    {
        let callable_task = CallableTask::<R>::new(task)?;
        let task_uuid = Uuid::new_v4();

        let message = match &target {
            ExecutionTarget::Any => {
                let mut msg =
                    ClientMessage::create_for_encode(EXECUTOR_SUBMIT_TO_PARTITION, PARTITION_ID_ANY);
                msg.add_frame(Self::string_frame(&self.name));
                msg.add_frame(Self::uuid_frame(task_uuid));
                msg.add_frame(Self::data_frame(callable_task.data()));
                msg
            }
            ExecutionTarget::Member(member_uuid) => {
                let mut msg =
                    ClientMessage::create_for_encode(EXECUTOR_SUBMIT_TO_MEMBER, PARTITION_ID_ANY);
                msg.add_frame(Self::string_frame(&self.name));
                msg.add_frame(Self::uuid_frame(task_uuid));
                msg.add_frame(Self::data_frame(callable_task.data()));
                msg.add_frame(Self::uuid_frame(*member_uuid));
                msg
            }
            ExecutionTarget::KeyOwner(key_data) => {
                let partition_id = Self::compute_partition_id(key_data);
                let mut msg =
                    ClientMessage::create_for_encode(EXECUTOR_SUBMIT_TO_PARTITION, partition_id);
                msg.add_frame(Self::string_frame(&self.name));
                msg.add_frame(Self::uuid_frame(task_uuid));
                msg.add_frame(Self::data_frame(callable_task.data()));
                msg
            }
        };

        let (sender, receiver) = oneshot::channel();
        let connection_manager = Arc::clone(&self.connection_manager);

        tokio::spawn(async move {
            let result = async {
                let address = connection_manager
                    .connected_addresses()
                    .await
                    .into_iter()
                    .next()
                    .ok_or_else(|| {
                        HazelcastError::Connection("No connection available".to_string())
                    })?;

                connection_manager.send_to(address, message).await?;

                let response = connection_manager.receive_from(address).await?.ok_or_else(|| {
                    HazelcastError::Connection("Connection closed".to_string())
                })?;

                decode_result_response::<R>(&response)
            }
            .await;

            let _ = sender.send(result);
        });

        Ok(ExecutorFuture::new(receiver))
    }

    fn string_frame(s: &str) -> Frame {
        Frame::with_content(bytes::BytesMut::from(s.as_bytes()))
    }

    fn uuid_frame(uuid: Uuid) -> Frame {
        Frame::with_content(bytes::BytesMut::from(uuid.as_bytes().as_slice()))
    }

    fn data_frame(data: &[u8]) -> Frame {
        Frame::with_content(bytes::BytesMut::from(data))
    }

    fn compute_partition_id(key_data: &[u8]) -> i32 {
        if key_data.is_empty() {
            return PARTITION_ID_ANY;
        }
        let mut hash: u32 = 0;
        for byte in key_data {
            hash = hash.wrapping_mul(31).wrapping_add(*byte as u32);
        }
        (hash % 271) as i32
    }

    async fn invoke(&self, message: ClientMessage) -> Result<ClientMessage> {
        let address = self
            .connection_manager
            .connected_addresses()
            .await
            .into_iter()
            .next()
            .ok_or_else(|| HazelcastError::Connection("No connection available".to_string()))?;

        self.connection_manager.send_to(address, message).await?;

        self.connection_manager
            .receive_from(address)
            .await?
            .ok_or_else(|| HazelcastError::Connection("Connection closed".to_string()))
    }

    fn decode_bool_response(response: &ClientMessage) -> Result<bool> {
        let frames = response.frames();
        if frames.is_empty() {
            return Err(HazelcastError::Serialization("Empty response".to_string()));
        }
        let content = frames[0].content();
        if content.len() < RESPONSE_HEADER_SIZE + 1 {
            return Err(HazelcastError::Serialization("Invalid response".to_string()));
        }
        Ok(content[RESPONSE_HEADER_SIZE] != 0)
    }
}

fn decode_result_response<T: Deserializable>(response: &ClientMessage) -> Result<T> {
    let frames = response.frames();

    if frames.len() < 2 {
        return Err(HazelcastError::Serialization(
            "Invalid executor response".to_string(),
        ));
    }

    let data_frame = &frames[1];
    let content = data_frame.content();

    if content.is_empty() {
        return Err(HazelcastError::Serialization(
            "Empty result data".to_string(),
        ));
    }

    let mut input = ObjectDataInput::new(content);
    T::deserialize(&mut input)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_executor_future_is_send() {
        fn assert_send<T: Send>() {}
        assert_send::<ExecutorFuture<String>>();
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
    fn test_compute_partition_id_empty() {
        assert_eq!(
            super::super::ExecutorService::compute_partition_id(&[]),
            PARTITION_ID_ANY
        );
    }

    #[test]
    fn test_compute_partition_id_deterministic() {
        let key = b"test-key";
        let id1 = super::super::ExecutorService::compute_partition_id(key);
        let id2 = super::super::ExecutorService::compute_partition_id(key);
        assert_eq!(id1, id2);
        assert!(id1 >= 0);
    }
}
