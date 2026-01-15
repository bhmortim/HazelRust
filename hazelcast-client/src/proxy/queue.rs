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
    /// Inserts the specified element into this queue.
    ///
    /// Returns `true` if the element was added successfully.
    pub async fn offer(&self, item: T) -> Result<bool> {
        self.check_permission(PermissionAction::Put)?;
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
        let mut message = ClientMessage::create_for_encode_any_partition(QUEUE_PEEK);
        message.add_frame(Self::string_frame(&self.name));

        let response = self.invoke(message).await?;
        Self::decode_nullable_response(&response)
    }

    /// Returns the number of elements in this queue.
    pub async fn size(&self) -> Result<usize> {
        self.check_permission(PermissionAction::Read)?;
        let mut message = ClientMessage::create_for_encode_any_partition(QUEUE_SIZE);
        message.add_frame(Self::string_frame(&self.name));

        let response = self.invoke(message).await?;
        Self::decode_int_response(&response).map(|v| v as usize)
    }

    /// Returns `true` if this queue contains no elements.
    pub async fn is_empty(&self) -> Result<bool> {
        Ok(self.size().await? == 0)
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
}
