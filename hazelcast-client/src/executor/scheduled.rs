//! Scheduled executor service for delayed and periodic task execution.

use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use uuid::Uuid;

use hazelcast_core::protocol::{
    ClientMessage, Frame,
    SCHEDULED_EXECUTOR_SUBMIT_TO_PARTITION, SCHEDULED_EXECUTOR_SUBMIT_TO_MEMBER,
    SCHEDULED_EXECUTOR_SHUTDOWN, SCHEDULED_EXECUTOR_IS_SHUTDOWN,
    SCHEDULED_EXECUTOR_CANCEL_FROM_PARTITION, SCHEDULED_EXECUTOR_CANCEL_FROM_MEMBER,
    SCHEDULED_EXECUTOR_IS_DONE_FROM_PARTITION, SCHEDULED_EXECUTOR_IS_DONE_FROM_MEMBER,
    SCHEDULED_EXECUTOR_GET_DELAY_FROM_PARTITION, SCHEDULED_EXECUTOR_GET_DELAY_FROM_MEMBER,
    SCHEDULED_EXECUTOR_GET_RESULT_FROM_PARTITION, SCHEDULED_EXECUTOR_DISPOSE,
    PARTITION_ID_ANY, RESPONSE_HEADER_SIZE,
};
use hazelcast_core::{Deserializable, HazelcastError, ObjectDataInput, Result, Serializable};

use crate::connection::ConnectionManager;
use crate::listener::Member;
use super::{Callable, CallableTask};

/// Unit of time for scheduled tasks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TimeUnit {
    Nanoseconds = 0,
    Microseconds = 1,
    Milliseconds = 2,
    Seconds = 3,
}

impl TimeUnit {
    fn to_nanos(&self, value: i64) -> i64 {
        match self {
            TimeUnit::Nanoseconds => value,
            TimeUnit::Microseconds => value * 1_000,
            TimeUnit::Milliseconds => value * 1_000_000,
            TimeUnit::Seconds => value * 1_000_000_000,
        }
    }
}

/// Type of scheduled task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ScheduleType {
    /// Single execution after delay.
    SingleRun = 0,
    /// Fixed rate periodic execution.
    AtFixedRate = 1,
    /// Fixed delay periodic execution.
    WithFixedDelay = 2,
}

/// Identifies where a scheduled task is running.
#[derive(Debug, Clone)]
pub(crate) enum ScheduledTaskTarget {
    Partition(i32),
    Member(Uuid),
}

/// A handle to a scheduled task result.
///
/// Provides methods to query task state, cancel execution, and retrieve results.
pub struct ScheduledFuture<T> {
    scheduler_name: String,
    task_name: String,
    target: ScheduledTaskTarget,
    connection_manager: Arc<ConnectionManager>,
    cancelled: AtomicBool,
    _marker: PhantomData<T>,
}

impl<T> std::fmt::Debug for ScheduledFuture<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScheduledFuture")
            .field("scheduler_name", &self.scheduler_name)
            .field("task_name", &self.task_name)
            .field("cancelled", &self.cancelled.load(Ordering::SeqCst))
            .finish()
    }
}

impl<T: Deserializable> ScheduledFuture<T> {
    pub(crate) fn new(
        scheduler_name: String,
        task_name: String,
        target: ScheduledTaskTarget,
        connection_manager: Arc<ConnectionManager>,
    ) -> Self {
        Self {
            scheduler_name,
            task_name,
            target,
            connection_manager,
            cancelled: AtomicBool::new(false),
            _marker: PhantomData,
        }
    }

    /// Attempts to cancel the scheduled task.
    ///
    /// Returns `true` if the task was successfully cancelled, `false` if
    /// the task has already completed or could not be cancelled.
    pub async fn cancel(&self, may_interrupt: bool) -> Result<bool> {
        if self.cancelled.load(Ordering::SeqCst) {
            return Ok(true);
        }

        let (message_type, partition_id) = match &self.target {
            ScheduledTaskTarget::Partition(pid) => {
                (SCHEDULED_EXECUTOR_CANCEL_FROM_PARTITION, *pid)
            }
            ScheduledTaskTarget::Member(_) => {
                (SCHEDULED_EXECUTOR_CANCEL_FROM_MEMBER, PARTITION_ID_ANY)
            }
        };

        let mut message = ClientMessage::create_for_encode(message_type, partition_id);
        message.add_frame(Self::string_frame(&self.scheduler_name));
        message.add_frame(Self::string_frame(&self.task_name));
        if let ScheduledTaskTarget::Member(uuid) = &self.target {
            message.add_frame(Self::uuid_frame(*uuid));
        }
        message.add_frame(Self::bool_frame(may_interrupt));

        let response = self.invoke(message).await?;
        let result = Self::decode_bool_response(&response)?;

        if result {
            self.cancelled.store(true, Ordering::SeqCst);
        }

        Ok(result)
    }

    /// Checks if the task was cancelled.
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }

    /// Checks if the task has completed execution.
    ///
    /// A task is done if it has completed normally, was cancelled, or
    /// terminated with an exception.
    pub async fn is_done(&self) -> Result<bool> {
        if self.cancelled.load(Ordering::SeqCst) {
            return Ok(true);
        }

        let (message_type, partition_id) = match &self.target {
            ScheduledTaskTarget::Partition(pid) => {
                (SCHEDULED_EXECUTOR_IS_DONE_FROM_PARTITION, *pid)
            }
            ScheduledTaskTarget::Member(_) => {
                (SCHEDULED_EXECUTOR_IS_DONE_FROM_MEMBER, PARTITION_ID_ANY)
            }
        };

        let mut message = ClientMessage::create_for_encode(message_type, partition_id);
        message.add_frame(Self::string_frame(&self.scheduler_name));
        message.add_frame(Self::string_frame(&self.task_name));
        if let ScheduledTaskTarget::Member(uuid) = &self.target {
            message.add_frame(Self::uuid_frame(*uuid));
        }

        let response = self.invoke(message).await?;
        Self::decode_bool_response(&response)
    }

    /// Returns the remaining delay before the task executes.
    ///
    /// For periodic tasks, returns the delay until the next execution.
    /// Returns zero or negative if the task is past its scheduled time.
    pub async fn get_delay(&self) -> Result<Duration> {
        let (message_type, partition_id) = match &self.target {
            ScheduledTaskTarget::Partition(pid) => {
                (SCHEDULED_EXECUTOR_GET_DELAY_FROM_PARTITION, *pid)
            }
            ScheduledTaskTarget::Member(_) => {
                (SCHEDULED_EXECUTOR_GET_DELAY_FROM_MEMBER, PARTITION_ID_ANY)
            }
        };

        let mut message = ClientMessage::create_for_encode(message_type, partition_id);
        message.add_frame(Self::string_frame(&self.scheduler_name));
        message.add_frame(Self::string_frame(&self.task_name));
        if let ScheduledTaskTarget::Member(uuid) = &self.target {
            message.add_frame(Self::uuid_frame(*uuid));
        }
        // Request delay in nanoseconds
        message.add_frame(Self::byte_frame(TimeUnit::Nanoseconds as u8));

        let response = self.invoke(message).await?;
        let nanos = Self::decode_long_response(&response)?;

        if nanos <= 0 {
            Ok(Duration::ZERO)
        } else {
            Ok(Duration::from_nanos(nanos as u64))
        }
    }

    /// Disposes of the scheduled task, releasing server-side resources.
    ///
    /// After disposal, the task cannot be queried or cancelled.
    pub async fn dispose(&self) -> Result<()> {
        let partition_id = match &self.target {
            ScheduledTaskTarget::Partition(pid) => *pid,
            ScheduledTaskTarget::Member(_) => PARTITION_ID_ANY,
        };

        let mut message = ClientMessage::create_for_encode(SCHEDULED_EXECUTOR_DISPOSE, partition_id);
        message.add_frame(Self::string_frame(&self.scheduler_name));
        message.add_frame(Self::string_frame(&self.task_name));
        if let ScheduledTaskTarget::Member(uuid) = &self.target {
            message.add_frame(Self::uuid_frame(*uuid));
        }

        self.invoke(message).await?;
        Ok(())
    }

    /// Waits for the task to complete and returns the result.
    ///
    /// For periodic tasks, this will return an error as they do not
    /// produce a single result.
    pub async fn get(self) -> Result<T> {
        let (message_type, partition_id) = match &self.target {
            ScheduledTaskTarget::Partition(pid) => {
                (SCHEDULED_EXECUTOR_GET_RESULT_FROM_PARTITION, *pid)
            }
            ScheduledTaskTarget::Member(_) => {
                return Err(HazelcastError::IllegalState(
                    "Get result from member not supported".to_string(),
                ));
            }
        };

        let mut message = ClientMessage::create_for_encode(message_type, partition_id);
        message.add_frame(Self::string_frame(&self.scheduler_name));
        message.add_frame(Self::string_frame(&self.task_name));

        let response = self.invoke(message).await?;
        Self::decode_result_response(&response)
    }

    async fn invoke(&self, message: ClientMessage) -> Result<ClientMessage> {
        let address = self
            .connection_manager
            .connected_addresses()
            .await
            .into_iter()
            .next()
            .ok_or_else(|| HazelcastError::Connection("No connection available".to_string()))?;

        self.connection_manager.send_to(&address, message).await?;

        self.connection_manager
            .receive_from(&address)
            .await
            .ok_or_else(|| HazelcastError::Connection("Connection closed".to_string()))
    }

    fn string_frame(s: &str) -> Frame {
        Frame::with_content(bytes::BytesMut::from(s.as_bytes()))
    }

    fn uuid_frame(uuid: Uuid) -> Frame {
        Frame::with_content(bytes::BytesMut::from(uuid.as_bytes().as_slice()))
    }

    fn bool_frame(value: bool) -> Frame {
        Frame::with_content(bytes::BytesMut::from(&[value as u8][..]))
    }

    fn byte_frame(value: u8) -> Frame {
        Frame::with_content(bytes::BytesMut::from(&[value][..]))
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

    fn decode_result_response(response: &ClientMessage) -> Result<T> {
        let frames = response.frames();

        if frames.len() < 2 {
            return Err(HazelcastError::Serialization(
                "Invalid scheduled executor response".to_string(),
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
}

/// Distributed scheduled executor service for delayed and periodic task execution.
///
/// Provides methods to schedule tasks for one-time execution after a delay,
/// or for periodic execution at fixed rates or with fixed delays.
pub struct ScheduledExecutorService {
    name: String,
    connection_manager: Arc<ConnectionManager>,
}

impl ScheduledExecutorService {
    /// Creates a new scheduled executor service instance.
    pub(crate) fn new(name: String, connection_manager: Arc<ConnectionManager>) -> Self {
        Self {
            name,
            connection_manager,
        }
    }

    /// Returns the name of this scheduled executor service.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Schedules a callable task for execution after the given delay.
    ///
    /// The task executes once on a partition determined by the cluster.
    pub async fn schedule<T, R>(&self, task: &T, delay: Duration) -> Result<ScheduledFuture<R>>
    where
        T: Callable<R>,
        R: Deserializable + Send + 'static,
    {
        let callable_task = CallableTask::<R>::new(task)?;
        let task_name = format!("task-{}", Uuid::new_v4());
        let delay_nanos = delay.as_nanos() as i64;

        let mut message =
            ClientMessage::create_for_encode(SCHEDULED_EXECUTOR_SUBMIT_TO_PARTITION, PARTITION_ID_ANY);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::byte_frame(ScheduleType::SingleRun as u8));
        message.add_frame(Self::string_frame(&task_name));
        message.add_frame(Self::data_frame(callable_task.data()));
        message.add_frame(Self::long_frame(delay_nanos));
        message.add_frame(Self::long_frame(0)); // period (not used for single run)

        self.invoke(message).await?;

        Ok(ScheduledFuture::new(
            self.name.clone(),
            task_name,
            ScheduledTaskTarget::Partition(PARTITION_ID_ANY),
            Arc::clone(&self.connection_manager),
        ))
    }

    /// Schedules a callable task to a specific member after the given delay.
    pub async fn schedule_on_member<T, R>(
        &self,
        task: &T,
        member: &Member,
        delay: Duration,
    ) -> Result<ScheduledFuture<R>>
    where
        T: Callable<R>,
        R: Deserializable + Send + 'static,
    {
        let callable_task = CallableTask::<R>::new(task)?;
        let task_name = format!("task-{}", Uuid::new_v4());
        let delay_nanos = delay.as_nanos() as i64;

        let mut message =
            ClientMessage::create_for_encode(SCHEDULED_EXECUTOR_SUBMIT_TO_MEMBER, PARTITION_ID_ANY);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::byte_frame(ScheduleType::SingleRun as u8));
        message.add_frame(Self::string_frame(&task_name));
        message.add_frame(Self::data_frame(callable_task.data()));
        message.add_frame(Self::long_frame(delay_nanos));
        message.add_frame(Self::long_frame(0)); // period
        message.add_frame(Self::uuid_frame(member.uuid()));

        self.invoke(message).await?;

        Ok(ScheduledFuture::new(
            self.name.clone(),
            task_name,
            ScheduledTaskTarget::Member(member.uuid()),
            Arc::clone(&self.connection_manager),
        ))
    }

    /// Schedules a callable task for periodic execution at a fixed rate.
    ///
    /// The task first executes after `initial_delay`, then repeatedly with
    /// the given `period` between the start of each execution.
    pub async fn schedule_at_fixed_rate<T, R>(
        &self,
        task: &T,
        initial_delay: Duration,
        period: Duration,
    ) -> Result<ScheduledFuture<R>>
    where
        T: Callable<R>,
        R: Deserializable + Send + 'static,
    {
        let callable_task = CallableTask::<R>::new(task)?;
        let task_name = format!("task-{}", Uuid::new_v4());
        let delay_nanos = initial_delay.as_nanos() as i64;
        let period_nanos = period.as_nanos() as i64;

        let mut message =
            ClientMessage::create_for_encode(SCHEDULED_EXECUTOR_SUBMIT_TO_PARTITION, PARTITION_ID_ANY);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::byte_frame(ScheduleType::AtFixedRate as u8));
        message.add_frame(Self::string_frame(&task_name));
        message.add_frame(Self::data_frame(callable_task.data()));
        message.add_frame(Self::long_frame(delay_nanos));
        message.add_frame(Self::long_frame(period_nanos));

        self.invoke(message).await?;

        Ok(ScheduledFuture::new(
            self.name.clone(),
            task_name,
            ScheduledTaskTarget::Partition(PARTITION_ID_ANY),
            Arc::clone(&self.connection_manager),
        ))
    }

    /// Schedules a callable task for periodic execution with a fixed delay.
    ///
    /// The task first executes after `initial_delay`, then repeatedly with
    /// the given `delay` between the end of one execution and the start of the next.
    pub async fn schedule_with_fixed_delay<T, R>(
        &self,
        task: &T,
        initial_delay: Duration,
        delay: Duration,
    ) -> Result<ScheduledFuture<R>>
    where
        T: Callable<R>,
        R: Deserializable + Send + 'static,
    {
        let callable_task = CallableTask::<R>::new(task)?;
        let task_name = format!("task-{}", Uuid::new_v4());
        let initial_nanos = initial_delay.as_nanos() as i64;
        let delay_nanos = delay.as_nanos() as i64;

        let mut message =
            ClientMessage::create_for_encode(SCHEDULED_EXECUTOR_SUBMIT_TO_PARTITION, PARTITION_ID_ANY);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::byte_frame(ScheduleType::WithFixedDelay as u8));
        message.add_frame(Self::string_frame(&task_name));
        message.add_frame(Self::data_frame(callable_task.data()));
        message.add_frame(Self::long_frame(initial_nanos));
        message.add_frame(Self::long_frame(delay_nanos));

        self.invoke(message).await?;

        Ok(ScheduledFuture::new(
            self.name.clone(),
            task_name,
            ScheduledTaskTarget::Partition(PARTITION_ID_ANY),
            Arc::clone(&self.connection_manager),
        ))
    }

    /// Shuts down the scheduled executor service.
    ///
    /// After shutdown, no new tasks can be scheduled.
    pub async fn shutdown(&self) -> Result<()> {
        let mut message =
            ClientMessage::create_for_encode(SCHEDULED_EXECUTOR_SHUTDOWN, PARTITION_ID_ANY);
        message.add_frame(Self::string_frame(&self.name));

        self.invoke(message).await?;
        Ok(())
    }

    /// Checks if the scheduled executor service is shut down.
    pub async fn is_shutdown(&self) -> Result<bool> {
        let mut message =
            ClientMessage::create_for_encode(SCHEDULED_EXECUTOR_IS_SHUTDOWN, PARTITION_ID_ANY);
        message.add_frame(Self::string_frame(&self.name));

        let response = self.invoke(message).await?;
        Self::decode_bool_response(&response)
    }

    async fn invoke(&self, message: ClientMessage) -> Result<ClientMessage> {
        let address = self
            .connection_manager
            .connected_addresses()
            .await
            .into_iter()
            .next()
            .ok_or_else(|| HazelcastError::Connection("No connection available".to_string()))?;

        self.connection_manager.send_to(&address, message).await?;

        self.connection_manager
            .receive_from(&address)
            .await
            .ok_or_else(|| HazelcastError::Connection("Connection closed".to_string()))
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

    fn byte_frame(value: u8) -> Frame {
        Frame::with_content(bytes::BytesMut::from(&[value][..]))
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_time_unit_to_nanos() {
        assert_eq!(TimeUnit::Nanoseconds.to_nanos(1), 1);
        assert_eq!(TimeUnit::Microseconds.to_nanos(1), 1_000);
        assert_eq!(TimeUnit::Milliseconds.to_nanos(1), 1_000_000);
        assert_eq!(TimeUnit::Seconds.to_nanos(1), 1_000_000_000);
    }

    #[test]
    fn test_time_unit_to_nanos_large_values() {
        assert_eq!(TimeUnit::Seconds.to_nanos(60), 60_000_000_000);
        assert_eq!(TimeUnit::Milliseconds.to_nanos(1000), 1_000_000_000);
        assert_eq!(TimeUnit::Microseconds.to_nanos(1_000_000), 1_000_000_000);
    }

    #[test]
    fn test_schedule_type_values() {
        assert_eq!(ScheduleType::SingleRun as u8, 0);
        assert_eq!(ScheduleType::AtFixedRate as u8, 1);
        assert_eq!(ScheduleType::WithFixedDelay as u8, 2);
    }

    #[test]
    fn test_schedule_type_debug() {
        assert_eq!(format!("{:?}", ScheduleType::SingleRun), "SingleRun");
        assert_eq!(format!("{:?}", ScheduleType::AtFixedRate), "AtFixedRate");
        assert_eq!(format!("{:?}", ScheduleType::WithFixedDelay), "WithFixedDelay");
    }

    #[test]
    fn test_time_unit_debug() {
        assert_eq!(format!("{:?}", TimeUnit::Nanoseconds), "Nanoseconds");
        assert_eq!(format!("{:?}", TimeUnit::Microseconds), "Microseconds");
        assert_eq!(format!("{:?}", TimeUnit::Milliseconds), "Milliseconds");
        assert_eq!(format!("{:?}", TimeUnit::Seconds), "Seconds");
    }

    #[test]
    fn test_time_unit_equality() {
        assert_eq!(TimeUnit::Nanoseconds, TimeUnit::Nanoseconds);
        assert_ne!(TimeUnit::Nanoseconds, TimeUnit::Milliseconds);
    }

    #[test]
    fn test_schedule_type_equality() {
        assert_eq!(ScheduleType::SingleRun, ScheduleType::SingleRun);
        assert_ne!(ScheduleType::SingleRun, ScheduleType::AtFixedRate);
    }

    #[test]
    fn test_scheduled_future_is_send() {
        fn assert_send<T: Send>() {}
        assert_send::<ScheduledFuture<String>>();
    }

    #[test]
    fn test_scheduled_future_is_sync() {
        fn assert_sync<T: Sync>() {}
        assert_sync::<ScheduledFuture<String>>();
    }

    #[test]
    fn test_scheduled_task_target_variants() {
        let partition = ScheduledTaskTarget::Partition(5);
        let member = ScheduledTaskTarget::Member(Uuid::new_v4());

        match partition {
            ScheduledTaskTarget::Partition(id) => assert_eq!(id, 5),
            _ => panic!("Expected Partition"),
        }
        match member {
            ScheduledTaskTarget::Member(_) => {}
            _ => panic!("Expected Member"),
        }
    }

    #[test]
    fn test_scheduled_task_target_debug() {
        let partition = ScheduledTaskTarget::Partition(42);
        let debug_str = format!("{:?}", partition);
        assert!(debug_str.contains("Partition"));
        assert!(debug_str.contains("42"));
    }

    #[test]
    fn test_string_frame_encoding() {
        let frame = ScheduledExecutorService::string_frame("test");
        assert_eq!(frame.content(), b"test");
    }

    #[test]
    fn test_byte_frame_encoding() {
        let frame = ScheduledExecutorService::byte_frame(42);
        assert_eq!(frame.content(), &[42]);
    }

    #[test]
    fn test_long_frame_encoding() {
        let frame = ScheduledExecutorService::long_frame(12345678901234i64);
        let bytes = frame.content();
        assert_eq!(bytes.len(), 8);
        let value = i64::from_le_bytes(bytes.try_into().unwrap());
        assert_eq!(value, 12345678901234i64);
    }

    #[test]
    fn test_uuid_frame_encoding() {
        let uuid = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let frame = ScheduledExecutorService::uuid_frame(uuid);
        assert_eq!(frame.content().len(), 16);
    }

    #[test]
    fn test_data_frame_encoding() {
        let data = vec![1, 2, 3, 4, 5];
        let frame = ScheduledExecutorService::data_frame(&data);
        assert_eq!(frame.content(), &[1, 2, 3, 4, 5]);
    }
}
