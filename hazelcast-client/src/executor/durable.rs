//! Durable executor service for fault-tolerant distributed task execution.
//!
//! Unlike the standard executor, durable executor persists task results on the
//! cluster, allowing clients to retrieve results even after disconnection.

use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::oneshot;
use uuid::Uuid;

use hazelcast_core::protocol::{
    ClientMessage, Frame,
    DURABLE_EXECUTOR_SUBMIT_TO_PARTITION, DURABLE_EXECUTOR_RETRIEVE_RESULT,
    DURABLE_EXECUTOR_DISPOSE_RESULT, DURABLE_EXECUTOR_RETRIEVE_AND_DISPOSE_RESULT,
    DURABLE_EXECUTOR_SHUTDOWN, DURABLE_EXECUTOR_IS_SHUTDOWN,
    PARTITION_ID_ANY, RESPONSE_HEADER_SIZE,
};
use hazelcast_core::{Deserializable, HazelcastError, ObjectDataInput, Result, Serializable};

use crate::connection::ConnectionManager;
use super::{Callable, CallableTask};

/// A handle to a durable executor task result.
///
/// Contains the sequence ID assigned by the cluster, which can be used to
/// retrieve results after client reconnection.
#[derive(Debug)]
#[allow(dead_code)]
pub struct DurableFuture<T> {
    sequence: i64,
    partition_id: i32,
    scheduler_name: String,
    connection_manager: Arc<ConnectionManager>,
    receiver: Option<oneshot::Receiver<Result<T>>>,
    _marker: PhantomData<T>,
}

impl<T: Deserializable> DurableFuture<T> {
    pub(crate) fn new(
        sequence: i64,
        partition_id: i32,
        scheduler_name: String,
        connection_manager: Arc<ConnectionManager>,
        receiver: oneshot::Receiver<Result<T>>,
    ) -> Self {
        Self {
            sequence,
            partition_id,
            scheduler_name,
            connection_manager,
            receiver: Some(receiver),
            _marker: PhantomData,
        }
    }

    /// Returns the sequence ID assigned to this task.
    ///
    /// This ID can be stored and used to retrieve the result later,
    /// even after client restart, using [`DurableExecutorService::retrieve_result`].
    pub fn sequence(&self) -> i64 {
        self.sequence
    }

    /// Returns the partition ID where this task is executing.
    pub fn partition_id(&self) -> i32 {
        self.partition_id
    }

    /// Waits for the task result.
    ///
    /// Returns the task result when available, or an error if the task failed.
    pub async fn get(mut self) -> Result<T> {
        if let Some(receiver) = self.receiver.take() {
            receiver.await.map_err(|_| {
                HazelcastError::Connection(
                    "Durable executor task was cancelled".to_string(),
                )
            })?
        } else {
            Err(HazelcastError::IllegalState(
                "Result already consumed".to_string(),
            ))
        }
    }

    /// Waits for the task result with a timeout.
    pub async fn get_timeout(self, timeout: Duration) -> Result<T> {
        tokio::time::timeout(timeout, self.get())
            .await
            .map_err(|_| HazelcastError::Timeout("Durable executor task timed out".to_string()))?
    }
}

/// Distributed durable executor service for fault-tolerant task execution.
///
/// The durable executor persists task results on the cluster, allowing clients
/// to retrieve results even after disconnection or restart. Tasks are identified
/// by a sequence ID that can be stored and used for later result retrieval.
pub struct DurableExecutorService {
    name: String,
    connection_manager: Arc<ConnectionManager>,
}

#[allow(dead_code)]
impl DurableExecutorService {
    /// Creates a new durable executor service instance.
    pub(crate) fn new(name: String, connection_manager: Arc<ConnectionManager>) -> Self {
        Self {
            name,
            connection_manager,
        }
    }

    /// Returns the name of this durable executor service.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Submits a callable task for durable execution.
    ///
    /// Returns a [`DurableFuture`] containing a sequence ID that can be used
    /// to retrieve the result later, even after client restart.
    pub async fn submit<T, R>(&self, task: &T) -> Result<DurableFuture<R>>
    where
        T: Callable<R>,
        R: Deserializable + Send + 'static,
    {
        let callable_task = CallableTask::<R>::new(task)?;
        let partition_id = self.select_partition();

        let mut message = ClientMessage::create_for_encode(
            DURABLE_EXECUTOR_SUBMIT_TO_PARTITION,
            partition_id,
        );
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(callable_task.data()));

        let response = self.invoke(message).await?;
        let sequence = Self::decode_long_response(&response)?;

        let (sender, receiver) = oneshot::channel();
        let connection_manager = Arc::clone(&self.connection_manager);
        let name = self.name.clone();

        tokio::spawn(async move {
            let result = retrieve_result_internal::<R>(
                &connection_manager,
                &name,
                sequence,
                partition_id,
            )
            .await;
            let _ = sender.send(result);
        });

        Ok(DurableFuture::new(
            sequence,
            partition_id,
            self.name.clone(),
            Arc::clone(&self.connection_manager),
            receiver,
        ))
    }

    /// Submits a callable task to a specific partition.
    ///
    /// Useful when you want to ensure the task executes on the same partition
    /// as related data.
    pub async fn submit_to_partition<T, R>(
        &self,
        task: &T,
        partition_id: i32,
    ) -> Result<DurableFuture<R>>
    where
        T: Callable<R>,
        R: Deserializable + Send + 'static,
    {
        let callable_task = CallableTask::<R>::new(task)?;

        let mut message = ClientMessage::create_for_encode(
            DURABLE_EXECUTOR_SUBMIT_TO_PARTITION,
            partition_id,
        );
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(callable_task.data()));

        let response = self.invoke(message).await?;
        let sequence = Self::decode_long_response(&response)?;

        let (sender, receiver) = oneshot::channel();
        let connection_manager = Arc::clone(&self.connection_manager);
        let name = self.name.clone();

        tokio::spawn(async move {
            let result = retrieve_result_internal::<R>(
                &connection_manager,
                &name,
                sequence,
                partition_id,
            )
            .await;
            let _ = sender.send(result);
        });

        Ok(DurableFuture::new(
            sequence,
            partition_id,
            self.name.clone(),
            Arc::clone(&self.connection_manager),
            receiver,
        ))
    }

    /// Submits a callable task to the partition owning the specified key.
    pub async fn submit_to_key_owner<T, R, K>(
        &self,
        task: &T,
        key: &K,
    ) -> Result<DurableFuture<R>>
    where
        T: Callable<R>,
        R: Deserializable + Send + 'static,
        K: Serializable,
    {
        let key_data = key.to_bytes()?;
        let partition_id = Self::compute_partition_id(&key_data);
        self.submit_to_partition(task, partition_id).await
    }

    /// Retrieves a task result by sequence ID.
    ///
    /// This method can be used to retrieve results after client restart,
    /// as long as the result has not been disposed.
    pub async fn retrieve_result<R>(&self, sequence: i64, partition_id: i32) -> Result<R>
    where
        R: Deserializable,
    {
        retrieve_result_internal::<R>(
            &self.connection_manager,
            &self.name,
            sequence,
            partition_id,
        )
        .await
    }

    /// Disposes of a task result, releasing server-side resources.
    ///
    /// After disposal, the result can no longer be retrieved.
    pub async fn dispose_result(&self, sequence: i64, partition_id: i32) -> Result<()> {
        let mut message = ClientMessage::create_for_encode(
            DURABLE_EXECUTOR_DISPOSE_RESULT,
            partition_id,
        );
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::long_frame(sequence));

        self.invoke(message).await?;
        Ok(())
    }

    /// Retrieves and disposes of a task result in a single operation.
    ///
    /// This is more efficient than calling [`retrieve_result`] followed by
    /// [`dispose_result`] separately.
    pub async fn retrieve_and_dispose_result<R>(
        &self,
        sequence: i64,
        partition_id: i32,
    ) -> Result<R>
    where
        R: Deserializable,
    {
        let mut message = ClientMessage::create_for_encode(
            DURABLE_EXECUTOR_RETRIEVE_AND_DISPOSE_RESULT,
            partition_id,
        );
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::long_frame(sequence));

        let response = self.invoke(message).await?;
        Self::decode_result_response(&response)
    }

    /// Shuts down the durable executor service.
    ///
    /// After shutdown, no new tasks can be submitted.
    pub async fn shutdown(&self) -> Result<()> {
        let mut message = ClientMessage::create_for_encode(
            DURABLE_EXECUTOR_SHUTDOWN,
            PARTITION_ID_ANY,
        );
        message.add_frame(Self::string_frame(&self.name));

        self.invoke(message).await?;
        Ok(())
    }

    /// Checks if the durable executor service is shut down.
    pub async fn is_shutdown(&self) -> Result<bool> {
        let mut message = ClientMessage::create_for_encode(
            DURABLE_EXECUTOR_IS_SHUTDOWN,
            PARTITION_ID_ANY,
        );
        message.add_frame(Self::string_frame(&self.name));

        let response = self.invoke(message).await?;
        Self::decode_bool_response(&response)
    }

    fn select_partition(&self) -> i32 {
        let uuid = Uuid::new_v4();
        let bytes = uuid.as_bytes();
        let hash = bytes.iter().fold(0u32, |acc, &b| {
            acc.wrapping_mul(31).wrapping_add(b as u32)
        });
        (hash % 271) as i32
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

    fn string_frame(s: &str) -> Frame {
        Frame::with_content(bytes::BytesMut::from(s.as_bytes()))
    }

    fn data_frame(data: &[u8]) -> Frame {
        Frame::with_content(bytes::BytesMut::from(data))
    }

    fn long_frame(value: i64) -> Frame {
        Frame::with_content(bytes::BytesMut::from(&value.to_le_bytes()[..]))
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

    fn decode_long_response(response: &ClientMessage) -> Result<i64> {
        let frames = response.frames();
        if frames.is_empty() {
            return Err(HazelcastError::Serialization("Empty response".to_string()));
        }
        let content = frames[0].content();
        if content.len() < RESPONSE_HEADER_SIZE + 8 {
            return Err(HazelcastError::Serialization("Invalid response".to_string()));
        }
        let bytes: [u8; 8] = content[RESPONSE_HEADER_SIZE..RESPONSE_HEADER_SIZE + 8]
            .try_into()
            .map_err(|_| HazelcastError::Serialization("Invalid long value".to_string()))?;
        Ok(i64::from_le_bytes(bytes))
    }

    fn decode_result_response<R: Deserializable>(response: &ClientMessage) -> Result<R> {
        let frames = response.frames();

        if frames.len() < 2 {
            return Err(HazelcastError::Serialization(
                "Invalid durable executor response".to_string(),
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
        R::deserialize(&mut input)
    }
}

async fn retrieve_result_internal<R: Deserializable>(
    connection_manager: &ConnectionManager,
    name: &str,
    sequence: i64,
    partition_id: i32,
) -> Result<R> {
    let mut message = ClientMessage::create_for_encode(
        DURABLE_EXECUTOR_RETRIEVE_RESULT,
        partition_id,
    );
    message.add_frame(DurableExecutorService::string_frame(name));
    message.add_frame(DurableExecutorService::long_frame(sequence));

    let address = connection_manager
        .connected_addresses()
        .await
        .into_iter()
        .next()
        .ok_or_else(|| HazelcastError::Connection("No connection available".to_string()))?;

    connection_manager.send_to(address, message).await?;

    let response = connection_manager
        .receive_from(address)
        .await?
        .ok_or_else(|| HazelcastError::Connection("Connection closed".to_string()))?;

    DurableExecutorService::decode_result_response(&response)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_durable_future_is_send() {
        fn assert_send<T: Send>() {}
        assert_send::<DurableFuture<String>>();
    }

    #[test]
    fn test_compute_partition_id_empty() {
        assert_eq!(
            DurableExecutorService::compute_partition_id(&[]),
            PARTITION_ID_ANY
        );
    }

    #[test]
    fn test_compute_partition_id_deterministic() {
        let key = b"test-key";
        let id1 = DurableExecutorService::compute_partition_id(key);
        let id2 = DurableExecutorService::compute_partition_id(key);
        assert_eq!(id1, id2);
        assert!(id1 >= 0);
    }

    #[test]
    fn test_string_frame_encoding() {
        let frame = DurableExecutorService::string_frame("test");
        assert_eq!(frame.content(), b"test");
    }

    #[test]
    fn test_long_frame_encoding() {
        let frame = DurableExecutorService::long_frame(12345678901234i64);
        let bytes = frame.content();
        assert_eq!(bytes.len(), 8);
        let value = i64::from_le_bytes(bytes.try_into().unwrap());
        assert_eq!(value, 12345678901234i64);
    }

    #[test]
    fn test_data_frame_encoding() {
        let data = vec![1, 2, 3, 4, 5];
        let frame = DurableExecutorService::data_frame(&data);
        assert_eq!(frame.content(), &[1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_select_partition_in_range() {
        let config = crate::config::ClientConfigBuilder::new()
            .add_address("127.0.0.1:5701".parse().unwrap())
            .build()
            .unwrap();
        let cm = crate::connection::ConnectionManager::from_config(config);
        let service = DurableExecutorService {
            name: "test".to_string(),
            connection_manager: Arc::new(cm),
        };
        for _ in 0..100 {
            let partition = service.select_partition();
            assert!(partition >= 0 && partition < 271);
        }
    }
}
