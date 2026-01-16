//! Distributed topic proxy implementation for pub/sub messaging.

use std::marker::PhantomData;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::BytesMut;
use hazelcast_core::protocol::constants::*;
use hazelcast_core::protocol::Frame;
use hazelcast_core::serialization::{ObjectDataInput, ObjectDataOutput};
use hazelcast_core::{ClientMessage, Deserializable, HazelcastError, Result, Serializable};

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
        }
    }

    /// Creates a topic message with explicit metadata.
    pub fn with_metadata(message: T, publish_time: u64, publishing_member: Option<String>) -> Self {
        Self {
            message,
            publish_time,
            publishing_member,
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
}

/// A distributed topic proxy for pub/sub messaging on a Hazelcast cluster.
///
/// `ITopic` provides async publish/subscribe operations with automatic serialization.
#[derive(Debug)]
pub struct ITopic<T> {
    name: String,
    connection_manager: Arc<ConnectionManager>,
    stats: Arc<ListenerStats>,
    _phantom: PhantomData<fn() -> T>,
}

impl<T> ITopic<T> {
    /// Creates a new topic proxy.
    pub(crate) fn new(name: String, connection_manager: Arc<ConnectionManager>) -> Self {
        Self {
            name,
            connection_manager,
            stats: Arc::new(ListenerStats::new()),
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
        Ok(())
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
        let connection_manager = Arc::clone(&self.connection_manager);
        let topic_name = self.name.clone();

        tokio::spawn(async move {
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

                let addresses = connection_manager.connected_addresses().await;
                if let Some(address) = addresses.first() {
                    match connection_manager.receive_from(*address).await {
                        Ok(Some(event_msg)) => {
                            if Self::is_topic_event(&event_msg) {
                                if let Ok(topic_msg) = Self::decode_topic_event(&event_msg) {
                                    stats.record_message();
                                    handler(topic_msg);
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
}
