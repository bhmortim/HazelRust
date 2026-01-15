//! Distributed list proxy implementation.

use std::marker::PhantomData;
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::BytesMut;
use hazelcast_core::protocol::constants::*;
use hazelcast_core::protocol::Frame;
use hazelcast_core::serialization::{ObjectDataInput, ObjectDataOutput};
use hazelcast_core::{ClientMessage, Deserializable, HazelcastError, Result, Serializable};

use crate::config::PermissionAction;
use crate::connection::ConnectionManager;

/// A distributed list proxy for performing indexed operations on a Hazelcast cluster.
///
/// `IList` provides async list operations with automatic serialization.
#[derive(Debug)]
pub struct IList<T> {
    name: String,
    connection_manager: Arc<ConnectionManager>,
    _phantom: PhantomData<fn() -> T>,
}

impl<T> IList<T> {
    /// Creates a new list proxy.
    pub(crate) fn new(name: String, connection_manager: Arc<ConnectionManager>) -> Self {
        Self {
            name,
            connection_manager,
            _phantom: PhantomData,
        }
    }

    /// Returns the name of this list.
    pub fn name(&self) -> &str {
        &self.name
    }

    fn check_permission(&self, action: PermissionAction) -> Result<()> {
        let permissions = self.connection_manager.effective_permissions();
        if !permissions.is_permitted(action) {
            return Err(HazelcastError::Authorization(format!(
                "list '{}' operation denied: requires {:?} permission",
                self.name, action
            )));
        }
        Ok(())
    }
}

impl<T> IList<T>
where
    T: Serializable + Deserializable + Send + Sync,
{
    /// Appends the specified element to the end of this list.
    ///
    /// Returns `true` if the element was added successfully.
    pub async fn add(&self, item: T) -> Result<bool> {
        self.check_permission(PermissionAction::Put)?;
        let item_data = Self::serialize_value(&item)?;

        let mut message = ClientMessage::create_for_encode_any_partition(LIST_ADD);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&item_data));

        let response = self.invoke(message).await?;
        Self::decode_bool_response(&response)
    }

    /// Inserts the specified element at the specified position in this list.
    pub async fn add_at(&self, index: usize, item: T) -> Result<()> {
        self.check_permission(PermissionAction::Put)?;
        let item_data = Self::serialize_value(&item)?;

        let mut message = ClientMessage::create_for_encode_any_partition(LIST_ADD_AT);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::int_frame(index as i32));
        message.add_frame(Self::data_frame(&item_data));

        self.invoke(message).await?;
        Ok(())
    }

    /// Returns the element at the specified position in this list.
    ///
    /// Returns `None` if the index is out of bounds.
    pub async fn get(&self, index: usize) -> Result<Option<T>> {
        self.check_permission(PermissionAction::Read)?;
        let mut message = ClientMessage::create_for_encode_any_partition(LIST_GET);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::int_frame(index as i32));

        let response = self.invoke(message).await?;
        Self::decode_nullable_response(&response)
    }

    /// Removes the element at the specified position in this list.
    ///
    /// Returns the element that was removed, or `None` if the index is out of bounds.
    pub async fn remove_at(&self, index: usize) -> Result<Option<T>> {
        self.check_permission(PermissionAction::Remove)?;
        let mut message = ClientMessage::create_for_encode_any_partition(LIST_REMOVE_AT);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::int_frame(index as i32));

        let response = self.invoke(message).await?;
        Self::decode_nullable_response(&response)
    }

    /// Replaces the element at the specified position with the specified element.
    ///
    /// Returns the element previously at the specified position.
    pub async fn set(&self, index: usize, item: T) -> Result<Option<T>> {
        self.check_permission(PermissionAction::Put)?;
        let item_data = Self::serialize_value(&item)?;

        let mut message = ClientMessage::create_for_encode_any_partition(LIST_SET);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::int_frame(index as i32));
        message.add_frame(Self::data_frame(&item_data));

        let response = self.invoke(message).await?;
        Self::decode_nullable_response(&response)
    }

    /// Returns the number of elements in this list.
    pub async fn size(&self) -> Result<usize> {
        self.check_permission(PermissionAction::Read)?;
        let mut message = ClientMessage::create_for_encode_any_partition(LIST_SIZE);
        message.add_frame(Self::string_frame(&self.name));

        let response = self.invoke(message).await?;
        Self::decode_int_response(&response).map(|v| v as usize)
    }

    /// Returns `true` if this list contains the specified element.
    pub async fn contains(&self, item: &T) -> Result<bool> {
        self.check_permission(PermissionAction::Read)?;
        let item_data = Self::serialize_value(item)?;

        let mut message = ClientMessage::create_for_encode_any_partition(LIST_CONTAINS);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&item_data));

        let response = self.invoke(message).await?;
        Self::decode_bool_response(&response)
    }

    /// Removes all elements from this list.
    pub async fn clear(&self) -> Result<()> {
        self.check_permission(PermissionAction::Remove)?;
        let mut message = ClientMessage::create_for_encode_any_partition(LIST_CLEAR);
        message.add_frame(Self::string_frame(&self.name));

        self.invoke(message).await?;
        Ok(())
    }

    /// Returns `true` if this list contains no elements.
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

    fn int_frame(value: i32) -> Frame {
        let mut buf = BytesMut::with_capacity(4);
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

impl<T> Clone for IList<T> {
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
    fn test_ilist_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<IList<String>>();
    }

    #[tokio::test]
    async fn test_list_permission_denied_add() {
        use crate::config::{ClientConfigBuilder, Permissions, PermissionAction};
        use crate::connection::ConnectionManager;
        use std::sync::Arc;

        let perms = Permissions::new();

        let config = ClientConfigBuilder::new()
            .security(|s| s.permissions(perms))
            .build()
            .unwrap();

        let cm = Arc::new(ConnectionManager::from_config(config));
        let list: IList<String> = IList::new("test".to_string(), cm);

        let result = list.add("item".to_string()).await;
        assert!(matches!(result, Err(HazelcastError::Authorization(_))));
    }

    #[test]
    fn test_ilist_clone() {
        fn assert_clone<T: Clone>() {}
        assert_clone::<IList<String>>();
    }

    #[test]
    fn test_string_frame() {
        let frame = IList::<String>::string_frame("test-list");
        assert_eq!(&frame.content[..], b"test-list");
    }

    #[test]
    fn test_int_frame() {
        let frame = IList::<String>::int_frame(42);
        assert_eq!(frame.content.len(), 4);
        assert_eq!(
            i32::from_le_bytes(frame.content[..4].try_into().unwrap()),
            42
        );
    }

    #[test]
    fn test_serialize_string() {
        let data = IList::<String>::serialize_value(&"hello".to_string()).unwrap();
        assert!(!data.is_empty());
    }
}
