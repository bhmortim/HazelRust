//! Distributed topic proxy implementation for pub/sub messaging.

use std::marker::PhantomData;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use super::local_stats::{LatencyStats, LatencyTracker};

use bytes::BytesMut;
use hazelcast_core::protocol::constants::*;
use hazelcast_core::protocol::Frame;
use hazelcast_core::serialization::{ObjectDataInput, ObjectDataOutput};
use hazelcast_core::{ClientMessage, Deserializable, HazelcastError, Result, Serializable};
use tokio::task::JoinHandle;

use crate::config::PermissionAction;
use crate::connection::ConnectionManager;
use crate::listener::{ListenerId, ListenerRegistration, ListenerStats};

/// A message received from a distributed topic.
#[derive(Debug, Clone)]
pub struct TopicMessage<T> {
    /// The message payload.
    pub message: T,
    /// Unix timestamp in milliseconds when the message was published.
    pub publish_time: u64,
    /// The UUID of the member that published the message, if available.
    pub publishing_member: Option<String>,
    /// The sequence number of this message (for local ordering).
    pub sequence: i64,
}

impl<T> TopicMessage<T> {
    /// Creates a new topic message with the current timestamp.
    pub fn new(message: T) -> Self {
        let publish_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        Self {
            message,
            publish_time,
            publishing_member: None,
            sequence: -1,
        }
    }

    /// Creates a topic message with explicit metadata.
    pub fn with_metadata(message: T, publish_time: u64, publishing_member: Option<String>) -> Self {
        Self {
            message,
            publish_time,
            publishing_member,
            sequence: -1,
        }
    }

    /// Creates a topic message with full metadata including sequence.
    pub fn with_sequence(
        message: T,
        publish_time: u64,
        publishing_member: Option<String>,
        sequence: i64,
    ) -> Self {
        Self {
            message,
            publish_time,
            publishing_member,
            sequence,
        }
    }

    /// Returns the message payload.
    pub fn payload(&self) -> &T {
        &self.message
    }

    /// Consumes the topic message and returns the payload.
    pub fn into_payload(self) -> T {
        self.message
    }

    /// Returns the sequence number of this message.
    pub fn sequence(&self) -> i64 {
        self.sequence
    }
}

/// Local statistics for a topic.
#[derive(Debug, Clone, Default)]
pub struct LocalTopicStats {
    /// Total number of messages published by this client.
    pub publish_count: u64,
    /// Total number of messages received by listeners.
    pub receive_count: u64,
    /// Unix timestamp in milliseconds of the last publish operation.
    pub last_publish_time: Option<u64>,
    /// Unix timestamp in milliseconds of the last received message.
    pub last_receive_time: Option<u64>,
    /// Latency statistics for publish operations.
    pub publish_latency: LatencyStats,
    /// Latency statistics for receive operations.
    pub receive_latency: LatencyStats,
}

impl LocalTopicStats {
    /// Creates a new empty stats instance.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the total operation count (publish + receive).
    pub fn total_operations(&self) -> u64 {
        self.publish_count + self.receive_count
    }
}

/// Internal stats tracker for ITopic.
#[derive(Debug)]
struct TopicStatsTracker {
    publish_count: AtomicU64,
    receive_count: AtomicU64,
    last_publish_time: AtomicU64,
    last_receive_time: AtomicU64,
    sequence_counter: AtomicI64,
    publish_latency: LatencyTracker,
    receive_latency: LatencyTracker,
}

impl Default for TopicStatsTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl TopicStatsTracker {
    fn new() -> Self {
        Self {
            publish_count: AtomicU64::new(0),
            receive_count: AtomicU64::new(0),
            last_publish_time: AtomicU64::new(0),
            last_receive_time: AtomicU64::new(0),
            sequence_counter: AtomicI64::new(0),
            publish_latency: LatencyTracker::new(),
            receive_latency: LatencyTracker::new(),
        }
    }

    fn record_publish(&self) {
        self.publish_count.fetch_add(1, Ordering::Relaxed);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        self.last_publish_time.store(now, Ordering::Relaxed);
    }

    #[allow(dead_code)]
    fn record_publish_latency(&self, duration: Duration) {
        self.publish_latency.record(duration);
    }

    #[allow(dead_code)]
    fn record_receive_latency(&self, duration: Duration) {
        self.receive_latency.record(duration);
    }

    fn record_receive(&self) -> i64 {
        self.receive_count.fetch_add(1, Ordering::Relaxed);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        self.last_receive_time.store(now, Ordering::Relaxed);
        self.sequence_counter.fetch_add(1, Ordering::SeqCst)
    }

    #[allow(dead_code)]
    fn current_sequence(&self) -> i64 {
        self.sequence_counter.load(Ordering::SeqCst)
    }

    fn snapshot(&self) -> LocalTopicStats {
        let last_pub = self.last_publish_time.load(Ordering::Relaxed);
        let last_recv = self.last_receive_time.load(Ordering::Relaxed);
        LocalTopicStats {
            publish_count: self.publish_count.load(Ordering::Relaxed),
            receive_count: self.receive_count.load(Ordering::Relaxed),
            last_publish_time: if last_pub > 0 { Some(last_pub) } else { None },
            last_receive_time: if last_recv > 0 { Some(last_recv) } else { None },
            publish_latency: self.publish_latency.snapshot(),
            receive_latency: self.receive_latency.snapshot(),
        }
    }
}

/// A distributed topic proxy for pub/sub messaging on a Hazelcast cluster.
///
/// `ITopic` provides async publish/subscribe operations with automatic serialization.
#[derive(Debug)]
pub struct ITopic<T> {
    name: String,
    connection_manager: Arc<ConnectionManager>,
    stats: Arc<ListenerStats>,
    local_stats: Arc<TopicStatsTracker>,
    _phantom: PhantomData<fn() -> T>,
}

impl<T> ITopic<T> {
    /// Creates a new topic proxy.
    pub(crate) fn new(name: String, connection_manager: Arc<ConnectionManager>) -> Self {
        Self {
            name,
            connection_manager,
            stats: Arc::new(ListenerStats::new()),
            local_stats: Arc::new(TopicStatsTracker::new()),
            _phantom: PhantomData,
        }
    }

    /// Returns the name of this topic.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns statistics for this topic's listener operations.
    pub fn stats(&self) -> &ListenerStats {
        &self.stats
    }

    /// Returns local statistics for this topic including publish and receive counts.
    pub fn get_local_topic_stats(&self) -> LocalTopicStats {
        self.local_stats.snapshot()
    }

    fn check_permission(&self, action: PermissionAction) -> Result<()> {
        let permissions = self.connection_manager.effective_permissions();
        if !permissions.is_permitted(action) {
            return Err(HazelcastError::Authorization(format!(
                "topic '{}' operation denied: requires {:?} permission",
                self.name, action
            )));
        }
        Ok(())
    }
}

impl<T> ITopic<T>
where
    T: Serializable + Deserializable + Send + Sync + 'static,
{
    async fn check_quorum(&self, is_read: bool) -> Result<()> {
        self.connection_manager.check_quorum(&self.name, is_read).await
    }

    /// Publishes a message to all subscribers of this topic.
    ///
    /// The message is serialized and sent to the cluster, which then
    /// distributes it to all registered listeners.
    pub async fn publish(&self, message: T) -> Result<()> {
        self.check_permission(PermissionAction::Put)?;
        self.check_quorum(false).await?;
        let message_data = Self::serialize_value(&message)?;

        let mut msg = ClientMessage::create_for_encode_any_partition(TOPIC_PUBLISH);
        msg.add_frame(Self::string_frame(&self.name));
        msg.add_frame(Self::data_frame(&message_data));

        self.invoke(msg).await?;
        self.local_stats.record_publish();
        Ok(())
    }

    /// Publishes a message asynchronously, returning a handle to the completion future.
    ///
    /// This allows non-blocking publish operations where the caller can choose
    /// to await completion later or ignore the result entirely.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let topic = client.get_topic::<String>("my-topic");
    /// let handle = topic.publish_async("message".to_string());
    /// // Do other work...
    /// handle.await??; // Await completion when needed
    /// ```
    pub fn publish_async(&self, message: T) -> JoinHandle<Result<()>>
    where
        T: Clone,
    {
        let topic = self.clone();
        tokio::spawn(async move { topic.publish(message).await })
    }

    /// Publishes multiple messages to all subscribers of this topic.
    ///
    /// Messages are published in order. This is more efficient than calling
    /// `publish` repeatedly as it reuses the connection for all messages.
    ///
    /// # Arguments
    ///
    /// * `messages` - The messages to publish
    ///
    /// # Example
    ///
    /// ```ignore
    /// let topic = client.get_topic::<String>("my-topic");
    /// topic.publish_all(vec!["msg1".to_string(), "msg2".to_string()]).await?;
    /// ```
    pub async fn publish_all(&self, messages: Vec<T>) -> Result<()> {
        if messages.is_empty() {
            return Ok(());
        }

        self.check_permission(PermissionAction::Put)?;
        self.check_quorum(false).await?;

        let address = self.get_connection_address().await?;

        for message in messages {
            let message_data = Self::serialize_value(&message)?;

            let mut msg = ClientMessage::create_for_encode_any_partition(TOPIC_PUBLISH);
            msg.add_frame(Self::string_frame(&self.name));
            msg.add_frame(Self::data_frame(&message_data));

            self.connection_manager.send_to(address, msg).await?;
            self.connection_manager
                .receive_from(address)
                .await?
                .ok_or_else(|| {
                    HazelcastError::Connection("connection closed unexpectedly".to_string())
                })?;
        }

        Ok(())
    }

    /// Registers a message listener for this topic.
    ///
    /// The provided handler will be called for each message published to this topic.
    /// Returns a `ListenerRegistration` that can be used to manage the subscription.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let topic = client.get_topic::<String>("my-topic");
    /// let registration = topic.add_message_listener(|msg| {
    ///     println!("Received: {}", msg.message);
    /// }).await?;
    ///
    /// // Later, deactivate the listener
    /// registration.deactivate();
    /// ```
    pub async fn add_message_listener<F>(&self, handler: F) -> Result<ListenerRegistration>
    where
        F: Fn(TopicMessage<T>) + Send + Sync + 'static,
    {
        self.add_message_listener_with_initial_sequence(handler, -1).await
    }

    /// Registers a message listener starting from a specific sequence number.
    ///
    /// Messages with sequence numbers less than `initial_sequence` will be skipped.
    /// Use `-1` to receive all messages (equivalent to `add_message_listener`).
    ///
    /// This is useful for reliable replay scenarios where you want to resume
    /// processing from a known position.
    ///
    /// # Arguments
    ///
    /// * `handler` - The callback to invoke for each received message
    /// * `initial_sequence` - The minimum sequence number to process (-1 for all)
    ///
    /// # Example
    ///
    /// ```ignore
    /// let topic = client.get_topic::<String>("my-topic");
    /// // Resume from sequence 100
    /// let registration = topic.add_message_listener_with_initial_sequence(
    ///     |msg| println!("Seq {}: {}", msg.sequence(), msg.message),
    ///     100,
    /// ).await?;
    /// ```
    pub async fn add_message_listener_with_initial_sequence<F>(
        &self,
        handler: F,
        initial_sequence: i64,
    ) -> Result<ListenerRegistration>
    where
        F: Fn(TopicMessage<T>) + Send + Sync + 'static,
    {
        self.check_permission(PermissionAction::Listen)?;
        let mut msg = ClientMessage::create_for_encode_any_partition(TOPIC_ADD_MESSAGE_LISTENER);
        msg.add_frame(Self::string_frame(&self.name));
        msg.add_frame(Self::bool_frame(false)); // local_only = false

        let response = self.invoke(msg).await?;
        let listener_id = Self::decode_uuid_response(&response)?;

        let registration = ListenerRegistration::new(listener_id);
        let active_flag = registration.active_flag();
        let shutdown_rx = registration.shutdown_receiver();
        let stats = Arc::clone(&self.stats);
        let local_stats = Arc::clone(&self.local_stats);
        let connection_manager = Arc::clone(&self.connection_manager);
        let topic_name = self.name.clone();

        tokio::spawn(async move {
            let mut shutdown = shutdown_rx;
            loop {
                if !active_flag.load(Ordering::Acquire) {
                    break;
                }

                if let Some(ref mut rx) = shutdown {
                    if *rx.borrow() {
                        break;
                    }
                }

                let addresses = connection_manager.connected_addresses().await;
                if let Some(address) = addresses.first() {
                    match connection_manager.receive_from(*address).await {
                        Ok(Some(event_msg)) => {
                            if Self::is_topic_event(&event_msg) {
                                if let Ok(mut topic_msg) = Self::decode_topic_event(&event_msg) {
                                    let seq = local_stats.record_receive();
                                    topic_msg.sequence = seq;

                                    if initial_sequence < 0 || seq >= initial_sequence {
                                        stats.record_message();
                                        handler(topic_msg);
                                    }
                                }
                            }
                        }
                        Ok(None) => {
                            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                        }
                        Err(e) => {
                            stats.record_error();
                            tracing::warn!(
                                topic = %topic_name,
                                error = %e,
                                "error receiving topic event"
                            );
                            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                        }
                    }
                } else {
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
            }

            tracing::debug!(topic = %topic_name, "topic listener stopped");
        });

        Ok(registration)
    }

    fn is_topic_event(message: &ClientMessage) -> bool {
        let frames = message.frames();
        if frames.is_empty() {
            return false;
        }
        frames[0].flags & IS_EVENT_FLAG != 0
    }

    fn decode_topic_event(message: &ClientMessage) -> Result<TopicMessage<T>> {
        let frames = message.frames();
        if frames.len() < 2 {
            return Err(HazelcastError::Serialization(
                "invalid topic event format".to_string(),
            ));
        }

        let data_frame = &frames[1];
        let mut input = ObjectDataInput::new(&data_frame.content);
        let payload = T::deserialize(&mut input)?;

        let publish_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        Ok(TopicMessage::with_metadata(payload, publish_time, None))
    }

    fn serialize_value<V: Serializable>(value: &V) -> Result<Vec<u8>> {
        let mut output = ObjectDataOutput::new();
        value.serialize(&mut output)?;
        Ok(output.into_bytes())
    }

    fn string_frame(s: &str) -> Frame {
        Frame::with_content(BytesMut::from(s.as_bytes()))
    }

    fn data_frame(data: &[u8]) -> Frame {
        Frame::with_content(BytesMut::from(data))
    }

    fn bool_frame(value: bool) -> Frame {
        let mut buf = BytesMut::with_capacity(1);
        buf.extend_from_slice(&[if value { 1 } else { 0 }]);
        Frame::with_content(buf)
    }

    async fn invoke(&self, message: ClientMessage) -> Result<ClientMessage> {
        let address = self.get_connection_address().await?;

        self.connection_manager.send_to(address, message).await?;
        self.connection_manager
            .receive_from(address)
            .await?
            .ok_or_else(|| HazelcastError::Connection("connection closed unexpectedly".to_string()))
    }

    async fn get_connection_address(&self) -> Result<SocketAddr> {
        let addresses = self.connection_manager.connected_addresses().await;
        addresses.into_iter().next().ok_or_else(|| {
            HazelcastError::Connection("no connections available".to_string())
        })
    }

    fn decode_uuid_response(response: &ClientMessage) -> Result<ListenerId> {
        let frames = response.frames();
        if frames.is_empty() {
            return Err(HazelcastError::Serialization(
                "empty response".to_string(),
            ));
        }

        let initial_frame = &frames[0];
        if initial_frame.content.len() >= RESPONSE_HEADER_SIZE + 16 {
            let offset = RESPONSE_HEADER_SIZE;
            let mut uuid_bytes = [0u8; 16];
            uuid_bytes.copy_from_slice(&initial_frame.content[offset..offset + 16]);
            Ok(ListenerId::from_uuid(uuid::Uuid::from_bytes(uuid_bytes)))
        } else {
            Ok(ListenerId::new())
        }
    }
}

impl<T> Clone for ITopic<T> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            connection_manager: Arc::clone(&self.connection_manager),
            stats: Arc::clone(&self.stats),
            local_stats: Arc::clone(&self.local_stats),
            _phantom: PhantomData,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_itopic_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<ITopic<String>>();
    }

    #[tokio::test]
    async fn test_topic_permission_denied_publish() {
        use crate::config::{ClientConfigBuilder, Permissions, PermissionAction};
        use crate::connection::ConnectionManager;
        use std::sync::Arc;

        let mut perms = Permissions::new();
        perms.grant(PermissionAction::Listen);

        let config = ClientConfigBuilder::new()
            .security(|s| s.permissions(perms))
            .build()
            .unwrap();

        let cm = Arc::new(ConnectionManager::from_config(config));
        let topic: ITopic<String> = ITopic::new("test".to_string(), cm);

        let result = topic.publish("message".to_string()).await;
        assert!(matches!(result, Err(HazelcastError::Authorization(_))));
    }

    #[tokio::test]
    async fn test_topic_permission_denied_listen() {
        use crate::config::{ClientConfigBuilder, Permissions, PermissionAction};
        use crate::connection::ConnectionManager;
        use std::sync::Arc;

        let mut perms = Permissions::new();
        perms.grant(PermissionAction::Put);

        let config = ClientConfigBuilder::new()
            .security(|s| s.permissions(perms))
            .build()
            .unwrap();

        let cm = Arc::new(ConnectionManager::from_config(config));
        let topic: ITopic<String> = ITopic::new("test".to_string(), cm);

        let result = topic.add_message_listener(|_msg| {}).await;
        assert!(matches!(result, Err(HazelcastError::Authorization(_))));
    }

    #[test]
    fn test_itopic_clone() {
        fn assert_clone<T: Clone>() {}
        assert_clone::<ITopic<String>>();
    }

    #[test]
    fn test_topic_message_new() {
        let msg = TopicMessage::new("hello".to_string());
        assert_eq!(msg.payload(), "hello");
        assert!(msg.publish_time > 0);
        assert!(msg.publishing_member.is_none());
    }

    #[test]
    fn test_topic_message_with_metadata() {
        let msg = TopicMessage::with_metadata(
            "hello".to_string(),
            12345,
            Some("member-1".to_string()),
        );
        assert_eq!(msg.payload(), "hello");
        assert_eq!(msg.publish_time, 12345);
        assert_eq!(msg.publishing_member, Some("member-1".to_string()));
    }

    #[test]
    fn test_topic_message_into_payload() {
        let msg = TopicMessage::new("hello".to_string());
        let payload = msg.into_payload();
        assert_eq!(payload, "hello");
    }

    #[test]
    fn test_string_frame() {
        let frame = ITopic::<String>::string_frame("test-topic");
        assert_eq!(&frame.content[..], b"test-topic");
    }

    #[test]
    fn test_bool_frame_true() {
        let frame = ITopic::<String>::bool_frame(true);
        assert_eq!(frame.content.len(), 1);
        assert_eq!(frame.content[0], 1);
    }

    #[test]
    fn test_bool_frame_false() {
        let frame = ITopic::<String>::bool_frame(false);
        assert_eq!(frame.content.len(), 1);
        assert_eq!(frame.content[0], 0);
    }

    #[test]
    fn test_serialize_string() {
        let data = ITopic::<String>::serialize_value(&"hello".to_string()).unwrap();
        assert!(!data.is_empty());
    }

    #[tokio::test]
    async fn test_topic_publish_all_empty() {
        use crate::config::ClientConfigBuilder;
        use crate::connection::ConnectionManager;

        let config = ClientConfigBuilder::new().build().unwrap();
        let cm = Arc::new(ConnectionManager::from_config(config));
        let topic: ITopic<String> = ITopic::new("test".to_string(), cm);

        let result = topic.publish_all(vec![]).await;
        assert!(result.is_ok());
    }

    #[test]
    fn test_topic_message_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<TopicMessage<String>>();
    }

    #[test]
    fn test_topic_message_clone() {
        let msg = TopicMessage::new("hello".to_string());
        let cloned = msg.clone();
        assert_eq!(cloned.payload(), msg.payload());
        assert_eq!(cloned.publish_time, msg.publish_time);
    }

    #[test]
    fn test_topic_message_with_sequence() {
        let msg = TopicMessage::with_sequence(
            "hello".to_string(),
            12345,
            Some("member-1".to_string()),
            42,
        );
        assert_eq!(msg.payload(), "hello");
        assert_eq!(msg.publish_time, 12345);
        assert_eq!(msg.publishing_member, Some("member-1".to_string()));
        assert_eq!(msg.sequence(), 42);
    }

    #[test]
    fn test_topic_message_default_sequence() {
        let msg = TopicMessage::new("hello".to_string());
        assert_eq!(msg.sequence(), -1);
    }

    #[test]
    fn test_local_topic_stats_new() {
        let stats = LocalTopicStats::new();
        assert_eq!(stats.publish_count, 0);
        assert_eq!(stats.receive_count, 0);
        assert!(stats.last_publish_time.is_none());
        assert!(stats.last_receive_time.is_none());
    }

    #[test]
    fn test_local_topic_stats_total_operations() {
        let stats = LocalTopicStats {
            publish_count: 10,
            receive_count: 25,
            last_publish_time: None,
            last_receive_time: None,
            publish_latency: LatencyStats::default(),
            receive_latency: LatencyStats::default(),
        };
        assert_eq!(stats.total_operations(), 35);
    }

    #[test]
    fn test_topic_stats_tracker() {
        let tracker = TopicStatsTracker::new();
        assert_eq!(tracker.current_sequence(), 0);

        tracker.record_publish();
        let stats = tracker.snapshot();
        assert_eq!(stats.publish_count, 1);
        assert!(stats.last_publish_time.is_some());

        let seq = tracker.record_receive();
        assert_eq!(seq, 0);
        let seq2 = tracker.record_receive();
        assert_eq!(seq2, 1);

        let stats = tracker.snapshot();
        assert_eq!(stats.receive_count, 2);
        assert!(stats.last_receive_time.is_some());
    }

    #[test]
    fn test_get_local_topic_stats() {
        use crate::config::ClientConfigBuilder;
        use crate::connection::ConnectionManager;

        let config = ClientConfigBuilder::new().build().unwrap();
        let cm = Arc::new(ConnectionManager::from_config(config));
        let topic: ITopic<String> = ITopic::new("test".to_string(), cm);

        let stats = topic.get_local_topic_stats();
        assert_eq!(stats.publish_count, 0);
        assert_eq!(stats.receive_count, 0);
    }

    #[test]
    fn test_local_topic_stats_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<LocalTopicStats>();
    }

    #[test]
    fn test_local_topic_stats_clone() {
        let stats = LocalTopicStats {
            publish_count: 5,
            receive_count: 10,
            last_publish_time: Some(1000),
            last_receive_time: Some(2000),
            publish_latency: LatencyStats::default(),
            receive_latency: LatencyStats::default(),
        };
        let cloned = stats.clone();
        assert_eq!(cloned.publish_count, 5);
        assert_eq!(cloned.receive_count, 10);
        assert_eq!(cloned.last_publish_time, Some(1000));
        assert_eq!(cloned.last_receive_time, Some(2000));
    }

    #[test]
    fn test_topic_stats_tracker_latency() {
        let tracker = TopicStatsTracker::new();
        tracker.record_publish_latency(Duration::from_millis(10));
        tracker.record_receive_latency(Duration::from_millis(5));

        let stats = tracker.snapshot();
        assert_eq!(stats.publish_latency.count, 1);
        assert_eq!(stats.receive_latency.count, 1);
    }
}
