//! Reliable distributed topic implementation backed by Ringbuffer.

use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use hazelcast_core::{Deserializable, Result, Serializable};

use crate::connection::ConnectionManager;
use crate::listener::{ListenerId, ListenerRegistration, ListenerStats};
use super::{OverflowPolicy, Ringbuffer};

/// Configuration for reliable topic listeners.
#[derive(Debug, Clone)]
pub struct ReliableTopicConfig {
    /// Number of messages to read in each batch.
    pub read_batch_size: i32,
    /// Whether to start from the oldest available message or only new messages.
    pub start_from_oldest: bool,
    /// Timeout between read attempts when no messages available.
    pub poll_timeout: Duration,
}

impl Default for ReliableTopicConfig {
    fn default() -> Self {
        Self {
            read_batch_size: 10,
            start_from_oldest: false,
            poll_timeout: Duration::from_millis(100),
        }
    }
}

#[allow(missing_docs)]
impl ReliableTopicConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn read_batch_size(mut self, size: i32) -> Self {
        self.read_batch_size = size;
        self
    }

    pub fn start_from_oldest(mut self, oldest: bool) -> Self {
        self.start_from_oldest = oldest;
        self
    }

    pub fn poll_timeout(mut self, timeout: Duration) -> Self {
        self.poll_timeout = timeout;
        self
    }
}

/// A message received from a reliable topic.
#[derive(Debug, Clone)]
pub struct ReliableTopicMessage<T> {
    /// The message payload.
    pub message: T,
    /// The sequence number in the underlying ringbuffer.
    pub sequence: i64,
    /// Unix timestamp when the message was published (milliseconds).
    pub publish_time: u64,
}

#[allow(missing_docs)]
impl<T> ReliableTopicMessage<T> {
    pub fn new(message: T, sequence: i64) -> Self {
        use std::time::{SystemTime, UNIX_EPOCH};
        let publish_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        Self {
            message,
            sequence,
            publish_time,
        }
    }

    pub fn with_time(message: T, sequence: i64, publish_time: u64) -> Self {
        Self {
            message,
            sequence,
            publish_time,
        }
    }

    pub fn payload(&self) -> &T {
        &self.message
    }

    pub fn into_payload(self) -> T {
        self.message
    }
}

/// Statistics for reliable topic operations.
#[derive(Debug, Default)]
pub struct ReliableTopicStats {
    messages_published: AtomicU64,
    messages_received: AtomicU64,
    gaps_detected: AtomicU64,
    messages_lost: AtomicU64,
}

#[allow(missing_docs)]
impl ReliableTopicStats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_publish(&self) {
        self.messages_published.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_receive(&self) {
        self.messages_received.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_gap(&self, lost_count: u64) {
        self.gaps_detected.fetch_add(1, Ordering::Relaxed);
        self.messages_lost.fetch_add(lost_count, Ordering::Relaxed);
    }

    pub fn messages_published(&self) -> u64 {
        self.messages_published.load(Ordering::Relaxed)
    }

    pub fn messages_received(&self) -> u64 {
        self.messages_received.load(Ordering::Relaxed)
    }

    pub fn gaps_detected(&self) -> u64 {
        self.gaps_detected.load(Ordering::Relaxed)
    }

    pub fn messages_lost(&self) -> u64 {
        self.messages_lost.load(Ordering::Relaxed)
    }
}

/// A reliable distributed topic backed by Ringbuffer.
///
/// Unlike regular `ITopic`, `ReliableTopic` provides:
/// - Reliable delivery with sequence tracking
/// - Gap detection when messages are lost due to ringbuffer overflow
/// - Ability to replay messages from a specific sequence
///
/// # Example
///
/// ```ignore
/// let topic = client.get_reliable_topic::<String>("my-reliable-topic");
///
/// // Publish messages
/// topic.publish("Hello".to_string()).await?;
///
/// // Subscribe to messages
/// let config = ReliableTopicConfig::default().start_from_oldest(true);
/// let registration = topic.add_message_listener_with_config(config, |msg| {
///     println!("Received: {} at sequence {}", msg.message, msg.sequence);
/// }).await?;
/// ```
#[derive(Debug)]
pub struct ReliableTopic<T> {
    name: String,
    connection_manager: Arc<ConnectionManager>,
    stats: Arc<ReliableTopicStats>,
    listener_stats: Arc<ListenerStats>,
    _phantom: PhantomData<fn() -> T>,
}

impl<T> ReliableTopic<T>
where
    T: Serializable + Deserializable + Send + Sync + 'static,
{
    /// The prefix used for the underlying ringbuffer.
    const RINGBUFFER_PREFIX: &'static str = "_hz_rb_";

    pub(crate) fn new(name: String, connection_manager: Arc<ConnectionManager>) -> Self {
        Self {
            name,
            connection_manager,
            stats: Arc::new(ReliableTopicStats::new()),
            listener_stats: Arc::new(ListenerStats::new()),
            _phantom: PhantomData,
        }
    }

    /// Returns the name of this reliable topic.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the name of the underlying ringbuffer.
    pub fn ringbuffer_name(&self) -> String {
        format!("{}{}", Self::RINGBUFFER_PREFIX, self.name)
    }

    /// Returns statistics for this reliable topic.
    pub fn stats(&self) -> &ReliableTopicStats {
        &self.stats
    }

    /// Returns listener statistics.
    pub fn listener_stats(&self) -> &ListenerStats {
        &self.listener_stats
    }

    fn ringbuffer(&self) -> Ringbuffer<T> {
        Ringbuffer::new(self.ringbuffer_name(), Arc::clone(&self.connection_manager))
    }

    /// Publishes a message to this reliable topic.
    ///
    /// The message is added to the underlying ringbuffer and will be
    /// delivered to all registered listeners.
    pub async fn publish(&self, message: T) -> Result<i64> {
        let sequence = self.ringbuffer().add(message).await?;
        self.stats.record_publish();
        Ok(sequence)
    }

    /// Publishes a message with a specific overflow policy.
    pub async fn publish_with_policy(
        &self,
        message: T,
        policy: OverflowPolicy,
    ) -> Result<i64> {
        let sequence = self.ringbuffer().add_with_policy(message, policy).await?;
        self.stats.record_publish();
        Ok(sequence)
    }

    /// Publishes multiple messages to this reliable topic in a single batch.
    ///
    /// This is more efficient than publishing messages individually as it
    /// uses the underlying ringbuffer's batch add operation.
    ///
    /// Returns the sequence of the last written message.
    ///
    /// # Arguments
    ///
    /// * `messages` - The messages to publish
    ///
    /// # Example
    ///
    /// ```ignore
    /// let topic = client.get_reliable_topic::<String>("my-reliable-topic");
    /// let last_seq = topic.publish_all(vec!["msg1".to_string(), "msg2".to_string()]).await?;
    /// ```
    pub async fn publish_all(&self, messages: Vec<T>) -> Result<i64> {
        if messages.is_empty() {
            return self.tail_sequence().await;
        }

        let count = messages.len();
        let sequence = self.ringbuffer().add_all(messages, OverflowPolicy::Overwrite).await?;

        for _ in 0..count {
            self.stats.record_publish();
        }

        Ok(sequence)
    }

    /// Publishes multiple messages with a specific overflow policy.
    ///
    /// Returns the sequence of the last written message.
    pub async fn publish_all_with_policy(
        &self,
        messages: Vec<T>,
        policy: OverflowPolicy,
    ) -> Result<i64> {
        if messages.is_empty() {
            return self.tail_sequence().await;
        }

        let count = messages.len();
        let sequence = self.ringbuffer().add_all(messages, policy).await?;

        for _ in 0..count {
            self.stats.record_publish();
        }

        Ok(sequence)
    }

    /// Registers a message listener with default configuration.
    ///
    /// The listener will receive messages starting from the next published message.
    pub async fn add_message_listener<F>(&self, handler: F) -> Result<ListenerRegistration>
    where
        F: Fn(ReliableTopicMessage<T>) + Send + Sync + 'static,
    {
        self.add_message_listener_with_config(ReliableTopicConfig::default(), handler)
            .await
    }

    /// Registers a message listener with custom configuration.
    ///
    /// # Arguments
    ///
    /// * `config` - Configuration for the listener behavior
    /// * `handler` - Function called for each received message
    ///
    /// # Gap Detection
    ///
    /// If the ringbuffer overwrites messages before the listener can read them,
    /// a gap is detected. The listener will skip to the oldest available message
    /// and continue reading. Gap statistics are recorded in `stats()`.
    pub async fn add_message_listener_with_config<F>(
        &self,
        config: ReliableTopicConfig,
        handler: F,
    ) -> Result<ListenerRegistration>
    where
        F: Fn(ReliableTopicMessage<T>) + Send + Sync + 'static,
    {
        let rb = self.ringbuffer();
        let initial_sequence = if config.start_from_oldest {
            rb.head_sequence().await?
        } else {
            rb.tail_sequence().await?.saturating_add(1)
        };

        let listener_id = ListenerId::new();
        let registration = ListenerRegistration::new(listener_id);
        let active_flag = registration.active_flag();
        let shutdown_rx = registration.shutdown_receiver();

        let stats = Arc::clone(&self.stats);
        let listener_stats = Arc::clone(&self.listener_stats);
        let topic_name = self.name.clone();
        let ringbuffer_name = self.ringbuffer_name();
        let connection_manager = Arc::clone(&self.connection_manager);
        let handler = Arc::new(handler);

        let poll_timeout = config.poll_timeout;
        let batch_size = config.read_batch_size;

        tokio::spawn(async move {
            let rb: Ringbuffer<T> = Ringbuffer::new(ringbuffer_name, connection_manager);
            let mut current_sequence = initial_sequence;
            let mut shutdown = shutdown_rx;

            loop {
                if !active_flag.load(std::sync::atomic::Ordering::Acquire) {
                    break;
                }

                if let Some(ref mut rx) = shutdown {
                    if *rx.borrow() {
                        break;
                    }
                }

                match rb.head_sequence().await {
                    Ok(head) => {
                        if head > current_sequence {
                            let lost = (head - current_sequence) as u64;
                            stats.record_gap(lost);
                            tracing::warn!(
                                topic = %topic_name,
                                lost = lost,
                                "gap detected, skipping to head"
                            );
                            current_sequence = head;
                        }
                    }
                    Err(e) => {
                        listener_stats.record_error();
                        tracing::warn!(
                            topic = %topic_name,
                            error = %e,
                            "error checking head sequence"
                        );
                        tokio::time::sleep(poll_timeout).await;
                        continue;
                    }
                }

                match rb.read_many(current_sequence, 1, batch_size).await {
                    Ok((messages, next_seq)) => {
                        for (i, msg) in messages.into_iter().enumerate() {
                            let seq = current_sequence + i as i64;
                            listener_stats.record_message();
                            stats.record_receive();
                            handler(ReliableTopicMessage::new(msg, seq));
                        }

                        if next_seq > current_sequence {
                            current_sequence = next_seq;
                        } else {
                            tokio::time::sleep(poll_timeout).await;
                        }
                    }
                    Err(e) => {
                        listener_stats.record_error();
                        tracing::warn!(
                            topic = %topic_name,
                            error = %e,
                            "error reading messages"
                        );
                        tokio::time::sleep(poll_timeout).await;
                    }
                }
            }

            tracing::debug!(topic = %topic_name, "reliable topic listener stopped");
        });

        Ok(registration)
    }

    /// Returns the current head sequence of the underlying ringbuffer.
    pub async fn head_sequence(&self) -> Result<i64> {
        self.ringbuffer().head_sequence().await
    }

    /// Returns the current tail sequence of the underlying ringbuffer.
    pub async fn tail_sequence(&self) -> Result<i64> {
        self.ringbuffer().tail_sequence().await
    }

    /// Returns the capacity of the underlying ringbuffer.
    pub async fn capacity(&self) -> Result<i64> {
        self.ringbuffer().capacity().await
    }

    /// Returns the number of messages in the underlying ringbuffer.
    pub async fn size(&self) -> Result<i64> {
        self.ringbuffer().size().await
    }
}

impl<T> Clone for ReliableTopic<T>
where
    T: Serializable + Deserializable + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            connection_manager: Arc::clone(&self.connection_manager),
            stats: Arc::clone(&self.stats),
            listener_stats: Arc::clone(&self.listener_stats),
            _phantom: PhantomData,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reliable_topic_message_new() {
        let msg = ReliableTopicMessage::new("hello".to_string(), 42);
        assert_eq!(msg.payload(), "hello");
        assert_eq!(msg.sequence, 42);
        assert!(msg.publish_time > 0);
    }

    #[test]
    fn test_reliable_topic_message_with_time() {
        let msg = ReliableTopicMessage::with_time("hello".to_string(), 42, 12345);
        assert_eq!(msg.payload(), "hello");
        assert_eq!(msg.sequence, 42);
        assert_eq!(msg.publish_time, 12345);
    }

    #[test]
    fn test_reliable_topic_message_into_payload() {
        let msg = ReliableTopicMessage::new("hello".to_string(), 42);
        let payload = msg.into_payload();
        assert_eq!(payload, "hello");
    }

    #[test]
    fn test_reliable_topic_config_default() {
        let config = ReliableTopicConfig::default();
        assert_eq!(config.read_batch_size, 10);
        assert!(!config.start_from_oldest);
        assert_eq!(config.poll_timeout, Duration::from_millis(100));
    }

    #[test]
    fn test_reliable_topic_config_builder() {
        let config = ReliableTopicConfig::new()
            .read_batch_size(50)
            .start_from_oldest(true)
            .poll_timeout(Duration::from_secs(1));

        assert_eq!(config.read_batch_size, 50);
        assert!(config.start_from_oldest);
        assert_eq!(config.poll_timeout, Duration::from_secs(1));
    }

    #[test]
    fn test_reliable_topic_stats_new() {
        let stats = ReliableTopicStats::new();
        assert_eq!(stats.messages_published(), 0);
        assert_eq!(stats.messages_received(), 0);
        assert_eq!(stats.gaps_detected(), 0);
        assert_eq!(stats.messages_lost(), 0);
    }

    #[test]
    fn test_reliable_topic_stats_recording() {
        let stats = ReliableTopicStats::new();

        stats.record_publish();
        stats.record_publish();
        assert_eq!(stats.messages_published(), 2);

        stats.record_receive();
        assert_eq!(stats.messages_received(), 1);

        stats.record_gap(5);
        assert_eq!(stats.gaps_detected(), 1);
        assert_eq!(stats.messages_lost(), 5);

        stats.record_gap(3);
        assert_eq!(stats.gaps_detected(), 2);
        assert_eq!(stats.messages_lost(), 8);
    }

    #[test]
    fn test_reliable_topic_message_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<ReliableTopicMessage<String>>();
    }

    #[test]
    fn test_reliable_topic_config_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<ReliableTopicConfig>();
    }

    #[test]
    fn test_reliable_topic_stats_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<ReliableTopicStats>();
    }

    #[test]
    fn test_ringbuffer_prefix() {
        let name = "my-topic";
        let expected = "_hz_rb_my-topic";
        assert_eq!(format!("{}{}", "_hz_rb_", name), expected);
    }

    #[test]
    fn test_reliable_topic_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<ReliableTopic<String>>();
    }

    #[test]
    fn test_reliable_topic_stats_batch_recording() {
        let stats = ReliableTopicStats::new();

        for _ in 0..5 {
            stats.record_publish();
        }
        assert_eq!(stats.messages_published(), 5);

        for _ in 0..3 {
            stats.record_receive();
        }
        assert_eq!(stats.messages_received(), 3);
    }
}
