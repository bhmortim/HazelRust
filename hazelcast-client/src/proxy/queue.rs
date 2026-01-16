//! Distributed queue proxy implementation.

use std::marker::PhantomData;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use hazelcast_core::protocol::constants::*;
use hazelcast_core::protocol::Frame;
use hazelcast_core::serialization::{ObjectDataInput, ObjectDataOutput};
use hazelcast_core::{ClientMessage, Deserializable, HazelcastError, Result, Serializable};

use crate::config::PermissionAction;
use crate::connection::ConnectionManager;
use crate::listener::ListenerId;

/// A distributed queue proxy for performing FIFO operations on a Hazelcast cluster.
///
/// `IQueue` provides async queue operations with automatic serialization.
#[derive(Debug)]
pub struct IQueue<T> {
    name: String,
    connection_manager: Arc<ConnectionManager>,
    _phantom: PhantomData<fn() -> T>,
}

impl<T> IQueue<T> {
    /// Creates a new queue proxy.
    pub(crate) fn new(name: String, connection_manager: Arc<ConnectionManager>) -> Self {
        Self {
            name,
            connection_manager,
            _phantom: PhantomData,
        }
    }

    /// Returns the name of this queue.
    pub fn name(&self) -> &str {
        &self.name
    }

    fn check_permission(&self, action: PermissionAction) -> Result<()> {
        let permissions = self.connection_manager.effective_permissions();
        if !permissions.is_permitted(action) {
            return Err(HazelcastError::Authorization(format!(
                "queue '{}' operation denied: requires {:?} permission",
                self.name, action
            )));
        }
        Ok(())
    }
}

impl<T> IQueue<T>
where
    T: Serializable + Deserializable + Send + Sync,
{
    async fn check_quorum(&self, is_read: bool) -> Result<()> {
        self.connection_manager.check_quorum(&self.name, is_read).await
    }

    /// Inserts the specified element into this queue.
    ///
    /// Returns `true` if the element was added successfully.
    pub async fn offer(&self, item: T) -> Result<bool> {
        self.check_permission(PermissionAction::Put)?;
        self.check_quorum(false).await?;
        let item_data = Self::serialize_value(&item)?;

        let mut message = ClientMessage::create_for_encode_any_partition(QUEUE_OFFER);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&item_data));
        message.add_frame(Self::long_frame(0)); // timeout: 0 for non-blocking

        let response = self.invoke(message).await?;
        Self::decode_bool_response(&response)
    }

    /// Retrieves and removes the head of this queue, or returns `None` if empty.
    pub async fn poll(&self) -> Result<Option<T>> {
        self.check_permission(PermissionAction::Remove)?;
        self.check_quorum(false).await?;
        let mut message = ClientMessage::create_for_encode_any_partition(QUEUE_POLL);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::long_frame(0)); // timeout: 0 for non-blocking

        let response = self.invoke(message).await?;
        Self::decode_nullable_response(&response)
    }

    /// Retrieves and removes the head of this queue, waiting up to the specified
    /// duration if necessary for an element to become available.
    ///
    /// Returns `None` if the timeout expires before an element is available.
    pub async fn poll_timeout(&self, timeout: Duration) -> Result<Option<T>> {
        self.check_permission(PermissionAction::Remove)?;
        self.check_quorum(false).await?;
        let timeout_ms = timeout.as_millis() as i64;

        let mut message = ClientMessage::create_for_encode_any_partition(QUEUE_POLL);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::long_frame(timeout_ms));

        let receive_timeout = timeout + Duration::from_secs(5);

        let address = self.get_connection_address().await?;
        self.connection_manager.send_to(address, message).await?;

        match tokio::time::timeout(
            receive_timeout,
            self.connection_manager.receive_from(address),
        )
        .await
        {
            Ok(Ok(Some(response))) => Self::decode_nullable_response(&response),
            Ok(Ok(None)) => Err(HazelcastError::Connection(
                "connection closed unexpectedly".to_string(),
            )),
            Ok(Err(e)) => Err(e),
            Err(_) => Ok(None),
        }
    }

    /// Retrieves, but does not remove, the head of this queue.
    ///
    /// Returns `None` if the queue is empty.
    pub async fn peek(&self) -> Result<Option<T>> {
        self.check_permission(PermissionAction::Read)?;
        self.check_quorum(true).await?;
        let mut message = ClientMessage::create_for_encode_any_partition(QUEUE_PEEK);
        message.add_frame(Self::string_frame(&self.name));

        let response = self.invoke(message).await?;
        Self::decode_nullable_response(&response)
    }

    /// Returns the number of elements in this queue.
    pub async fn size(&self) -> Result<usize> {
        self.check_permission(PermissionAction::Read)?;
        self.check_quorum(true).await?;
        let mut message = ClientMessage::create_for_encode_any_partition(QUEUE_SIZE);
        message.add_frame(Self::string_frame(&self.name));

        let response = self.invoke(message).await?;
        Self::decode_int_response(&response).map(|v| v as usize)
    }

    /// Returns `true` if this queue contains no elements.
    pub async fn is_empty(&self) -> Result<bool> {
        Ok(self.size().await? == 0)
    }

    /// Inserts the specified element into this queue, waiting if necessary
    /// for space to become available.
    pub async fn put(&self, item: T) -> Result<()> {
        self.check_permission(PermissionAction::Put)?;
        self.check_quorum(false).await?;
        let item_data = Self::serialize_value(&item)?;

        let mut message = ClientMessage::create_for_encode_any_partition(QUEUE_PUT);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&item_data));

        self.invoke(message).await?;
        Ok(())
    }

    /// Retrieves and removes the head of this queue, waiting if necessary
    /// until an element becomes available.
    pub async fn take(&self) -> Result<T> {
        self.check_permission(PermissionAction::Remove)?;
        self.check_quorum(false).await?;

        let mut message = ClientMessage::create_for_encode_any_partition(QUEUE_TAKE);
        message.add_frame(Self::string_frame(&self.name));

        let response = self.invoke(message).await?;
        Self::decode_nullable_response(&response)?
            .ok_or_else(|| HazelcastError::Serialization("expected non-null response".to_string()))
    }

    /// Removes a single instance of the specified element from this queue, if present.
    ///
    /// Returns `true` if the queue contained the specified element.
    pub async fn remove(&self, item: &T) -> Result<bool> {
        self.check_permission(PermissionAction::Remove)?;
        self.check_quorum(false).await?;
        let item_data = Self::serialize_value(item)?;

        let mut message = ClientMessage::create_for_encode_any_partition(QUEUE_REMOVE);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&item_data));

        let response = self.invoke(message).await?;
        Self::decode_bool_response(&response)
    }

    /// Returns `true` if this queue contains the specified element.
    pub async fn contains(&self, item: &T) -> Result<bool> {
        self.check_permission(PermissionAction::Read)?;
        self.check_quorum(true).await?;
        let item_data = Self::serialize_value(item)?;

        let mut message = ClientMessage::create_for_encode_any_partition(QUEUE_CONTAINS);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&item_data));

        let response = self.invoke(message).await?;
        Self::decode_bool_response(&response)
    }

    /// Adds all elements in the specified collection to this queue.
    ///
    /// Returns `true` if this queue changed as a result of the call.
    pub async fn add_all(&self, items: Vec<T>) -> Result<bool> {
        self.check_permission(PermissionAction::Put)?;
        self.check_quorum(false).await?;

        let mut message = ClientMessage::create_for_encode_any_partition(QUEUE_ADD_ALL);
        message.add_frame(Self::string_frame(&self.name));
        self.add_data_list_frames(&mut message, &items)?;

        let response = self.invoke(message).await?;
        Self::decode_bool_response(&response)
    }

    /// Removes from this queue all of its elements that are contained in the specified collection.
    ///
    /// Returns `true` if this queue changed as a result of the call.
    pub async fn remove_all(&self, items: &[T]) -> Result<bool> {
        self.check_permission(PermissionAction::Remove)?;
        self.check_quorum(false).await?;

        let mut message = ClientMessage::create_for_encode_any_partition(QUEUE_COMPARE_AND_REMOVE_ALL);
        message.add_frame(Self::string_frame(&self.name));
        self.add_data_list_frames(&mut message, items)?;

        let response = self.invoke(message).await?;
        Self::decode_bool_response(&response)
    }

    /// Returns `true` if this queue contains all of the elements in the specified collection.
    pub async fn contains_all(&self, items: &[T]) -> Result<bool> {
        self.check_permission(PermissionAction::Read)?;
        self.check_quorum(true).await?;

        let mut message = ClientMessage::create_for_encode_any_partition(QUEUE_CONTAINS_ALL);
        message.add_frame(Self::string_frame(&self.name));
        self.add_data_list_frames(&mut message, items)?;

        let response = self.invoke(message).await?;
        Self::decode_bool_response(&response)
    }

    /// Removes at most the given number of elements from this queue and returns them.
    ///
    /// This operation may be more efficient than repeatedly polling this queue.
    pub async fn drain_to(&self, max: usize) -> Result<Vec<T>> {
        self.check_permission(PermissionAction::Remove)?;
        self.check_quorum(false).await?;

        let mut message = ClientMessage::create_for_encode_any_partition(QUEUE_DRAIN_TO_MAX_SIZE);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::int_frame(max as i32));

        let response = self.invoke(message).await?;
        Self::decode_list_response(&response)
    }

    /// Registers an item listener to receive notifications when items are added or removed.
    ///
    /// Returns a `ListenerId` that can be used to remove the listener later.
    pub async fn add_item_listener(&self, include_value: bool) -> Result<ListenerId> {
        self.check_permission(PermissionAction::Read)?;

        let mut message = ClientMessage::create_for_encode_any_partition(QUEUE_ADD_LISTENER);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::bool_frame(include_value));
        message.add_frame(Self::bool_frame(false));

        let response = self.invoke(message).await?;
        Self::decode_uuid_response(&response).map(ListenerId::from_uuid)
    }

    /// Removes a previously registered item listener.
    ///
    /// Returns `true` if the listener was successfully removed.
    pub async fn remove_item_listener(&self, listener_id: ListenerId) -> Result<bool> {
        self.check_permission(PermissionAction::Read)?;

        let mut message = ClientMessage::create_for_encode_any_partition(QUEUE_REMOVE_LISTENER);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::uuid_frame(listener_id.as_uuid()));

        let response = self.invoke(message).await?;
        Self::decode_bool_response(&response)
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

    fn long_frame(value: i64) -> Frame {
        let mut buf = BytesMut::with_capacity(8);
        buf.extend_from_slice(&value.to_le_bytes());
        Frame::with_content(buf)
    }

    fn int_frame(value: i32) -> Frame {
        let mut buf = BytesMut::with_capacity(4);
        buf.extend_from_slice(&value.to_le_bytes());
        Frame::with_content(buf)
    }

    fn bool_frame(value: bool) -> Frame {
        let mut buf = BytesMut::with_capacity(1);
        buf.extend_from_slice(&[if value { 1u8 } else { 0u8 }]);
        Frame::with_content(buf)
    }

    fn uuid_frame(uuid: uuid::Uuid) -> Frame {
        let mut buf = BytesMut::with_capacity(16);
        buf.extend_from_slice(uuid.as_bytes());
        Frame::with_content(buf)
    }

    fn add_data_list_frames(&self, message: &mut ClientMessage, items: &[T]) -> Result<()> {
        let mut list_buf = BytesMut::new();
        list_buf.extend_from_slice(&(items.len() as i32).to_le_bytes());

        for item in items {
            let item_data = Self::serialize_value(item)?;
            list_buf.extend_from_slice(&(item_data.len() as i32).to_le_bytes());
            list_buf.extend_from_slice(&item_data);
        }

        message.add_frame(Frame::with_content(list_buf));
        Ok(())
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

    fn decode_nullable_response<V: Deserializable>(response: &ClientMessage) -> Result<Option<V>> {
        let frames = response.frames();
        if frames.len() < 2 {
            return Ok(None);
        }

        let data_frame = &frames[1];

        if data_frame.flags & IS_NULL_FLAG != 0 {
            return Ok(None);
        }

        if data_frame.content.is_empty() {
            return Ok(None);
        }

        let mut input = ObjectDataInput::new(&data_frame.content);
        V::deserialize(&mut input).map(Some)
    }

    fn decode_bool_response(response: &ClientMessage) -> Result<bool> {
        let frames = response.frames();
        if frames.is_empty() {
            return Err(HazelcastError::Serialization(
                "empty response".to_string(),
            ));
        }

        let initial_frame = &frames[0];
        if initial_frame.content.len() > RESPONSE_HEADER_SIZE {
            Ok(initial_frame.content[RESPONSE_HEADER_SIZE] != 0)
        } else {
            Ok(false)
        }
    }

    fn decode_int_response(response: &ClientMessage) -> Result<i32> {
        let frames = response.frames();
        if frames.is_empty() {
            return Err(HazelcastError::Serialization(
                "empty response".to_string(),
            ));
        }

        let initial_frame = &frames[0];
        if initial_frame.content.len() >= RESPONSE_HEADER_SIZE + 4 {
            let offset = RESPONSE_HEADER_SIZE;
            Ok(i32::from_le_bytes([
                initial_frame.content[offset],
                initial_frame.content[offset + 1],
                initial_frame.content[offset + 2],
                initial_frame.content[offset + 3],
            ]))
        } else {
            Ok(0)
        }
    }

    fn decode_list_response<V: Deserializable>(response: &ClientMessage) -> Result<Vec<V>> {
        let frames = response.frames();
        if frames.len() < 2 {
            return Ok(Vec::new());
        }

        let data_frame = &frames[1];
        if data_frame.content.is_empty() {
            return Ok(Vec::new());
        }

        let content = &data_frame.content;
        if content.len() < 4 {
            return Ok(Vec::new());
        }

        let count = i32::from_le_bytes([content[0], content[1], content[2], content[3]]) as usize;
        let mut results = Vec::with_capacity(count);
        let mut offset = 4;

        for _ in 0..count {
            if offset + 4 > content.len() {
                break;
            }

            let item_len = i32::from_le_bytes([
                content[offset],
                content[offset + 1],
                content[offset + 2],
                content[offset + 3],
            ]) as usize;
            offset += 4;

            if offset + item_len > content.len() {
                break;
            }

            let item_data = &content[offset..offset + item_len];
            let mut input = ObjectDataInput::new(item_data);
            results.push(V::deserialize(&mut input)?);
            offset += item_len;
        }

        Ok(results)
    }

    fn decode_uuid_response(response: &ClientMessage) -> Result<uuid::Uuid> {
        let frames = response.frames();
        if frames.len() < 2 {
            return Err(HazelcastError::Serialization(
                "missing uuid frame in response".to_string(),
            ));
        }

        let uuid_frame = &frames[1];
        if uuid_frame.content.len() < 16 {
            return Err(HazelcastError::Serialization(
                "invalid uuid frame length".to_string(),
            ));
        }

        let bytes: [u8; 16] = uuid_frame.content[..16].try_into().map_err(|_| {
            HazelcastError::Serialization("failed to convert uuid bytes".to_string())
        })?;
        Ok(uuid::Uuid::from_bytes(bytes))
    }
}

impl<T> Clone for IQueue<T> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            connection_manager: Arc::clone(&self.connection_manager),
            _phantom: PhantomData,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_iqueue_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<IQueue<String>>();
    }

    #[tokio::test]
    async fn test_queue_permission_denied_offer() {
        use crate::config::{ClientConfigBuilder, Permissions, PermissionAction};
        use crate::connection::ConnectionManager;
        use std::sync::Arc;

        let mut perms = Permissions::new();
        perms.grant(PermissionAction::Read);

        let config = ClientConfigBuilder::new()
            .security(|s| s.permissions(perms))
            .build()
            .unwrap();

        let cm = Arc::new(ConnectionManager::from_config(config));
        let queue: IQueue<String> = IQueue::new("test".to_string(), cm);

        let result = queue.offer("item".to_string()).await;
        assert!(matches!(result, Err(HazelcastError::Authorization(_))));
    }

    #[tokio::test]
    async fn test_queue_permission_denied_poll() {
        use crate::config::{ClientConfigBuilder, Permissions, PermissionAction};
        use crate::connection::ConnectionManager;
        use std::sync::Arc;

        let mut perms = Permissions::new();
        perms.grant(PermissionAction::Read);
        perms.grant(PermissionAction::Put);

        let config = ClientConfigBuilder::new()
            .security(|s| s.permissions(perms))
            .build()
            .unwrap();

        let cm = Arc::new(ConnectionManager::from_config(config));
        let queue: IQueue<String> = IQueue::new("test".to_string(), cm);

        let result = queue.poll().await;
        assert!(matches!(result, Err(HazelcastError::Authorization(_))));
    }

    #[tokio::test]
    async fn test_queue_quorum_not_present_blocks_operations() {
        use crate::config::{ClientConfigBuilder, QuorumConfig, QuorumType};
        use crate::connection::ConnectionManager;

        let quorum = QuorumConfig::builder("protected-*")
            .min_cluster_size(3)
            .quorum_type(QuorumType::ReadWrite)
            .build()
            .unwrap();

        let config = ClientConfigBuilder::new()
            .add_quorum_config(quorum)
            .build()
            .unwrap();

        let cm = Arc::new(ConnectionManager::from_config(config));
        let queue: IQueue<String> = IQueue::new("protected-queue".to_string(), cm);

        let result = queue.offer("item".to_string()).await;
        assert!(matches!(result, Err(HazelcastError::QuorumNotPresent(_))));

        let result = queue.poll().await;
        assert!(matches!(result, Err(HazelcastError::QuorumNotPresent(_))));

        let result = queue.peek().await;
        assert!(matches!(result, Err(HazelcastError::QuorumNotPresent(_))));

        let result = queue.size().await;
        assert!(matches!(result, Err(HazelcastError::QuorumNotPresent(_))));
    }

    #[test]
    fn test_iqueue_clone() {
        fn assert_clone<T: Clone>() {}
        assert_clone::<IQueue<String>>();
    }

    #[test]
    fn test_string_frame() {
        let frame = IQueue::<String>::string_frame("test-queue");
        assert_eq!(&frame.content[..], b"test-queue");
    }

    #[test]
    fn test_long_frame() {
        let frame = IQueue::<String>::long_frame(1000);
        assert_eq!(frame.content.len(), 8);
        assert_eq!(
            i64::from_le_bytes(frame.content[..8].try_into().unwrap()),
            1000
        );
    }

    #[test]
    fn test_serialize_string() {
        let data = IQueue::<String>::serialize_value(&"hello".to_string()).unwrap();
        assert!(!data.is_empty());
    }

    #[test]
    fn test_int_frame() {
        let frame = IQueue::<String>::int_frame(42);
        assert_eq!(frame.content.len(), 4);
        assert_eq!(
            i32::from_le_bytes(frame.content[..4].try_into().unwrap()),
            42
        );
    }

    #[tokio::test]
    async fn test_queue_permission_denied_take() {
        use crate::config::{ClientConfigBuilder, Permissions, PermissionAction};
        use crate::connection::ConnectionManager;
        use std::sync::Arc;

        let mut perms = Permissions::new();
        perms.grant(PermissionAction::Read);
        perms.grant(PermissionAction::Put);

        let config = ClientConfigBuilder::new()
            .security(|s| s.permissions(perms))
            .build()
            .unwrap();

        let cm = Arc::new(ConnectionManager::from_config(config));
        let queue: IQueue<String> = IQueue::new("test".to_string(), cm);

        let result = queue.take().await;
        assert!(matches!(result, Err(HazelcastError::Authorization(_))));
    }

    #[tokio::test]
    async fn test_queue_permission_denied_remove() {
        use crate::config::{ClientConfigBuilder, Permissions, PermissionAction};
        use crate::connection::ConnectionManager;
        use std::sync::Arc;

        let mut perms = Permissions::new();
        perms.grant(PermissionAction::Read);
        perms.grant(PermissionAction::Put);

        let config = ClientConfigBuilder::new()
            .security(|s| s.permissions(perms))
            .build()
            .unwrap();

        let cm = Arc::new(ConnectionManager::from_config(config));
        let queue: IQueue<String> = IQueue::new("test".to_string(), cm);

        let result = queue.remove(&"item".to_string()).await;
        assert!(matches!(result, Err(HazelcastError::Authorization(_))));
    }

    #[tokio::test]
    async fn test_queue_permission_denied_contains() {
        use crate::config::{ClientConfigBuilder, Permissions, PermissionAction};
        use crate::connection::ConnectionManager;
        use std::sync::Arc;

        let mut perms = Permissions::new();
        perms.grant(PermissionAction::Put);
        perms.grant(PermissionAction::Remove);

        let config = ClientConfigBuilder::new()
            .security(|s| s.permissions(perms))
            .build()
            .unwrap();

        let cm = Arc::new(ConnectionManager::from_config(config));
        let queue: IQueue<String> = IQueue::new("test".to_string(), cm);

        let result = queue.contains(&"item".to_string()).await;
        assert!(matches!(result, Err(HazelcastError::Authorization(_))));
    }
}
