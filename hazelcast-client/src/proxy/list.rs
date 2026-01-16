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
use crate::listener::ListenerId;

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

    /// Appends all elements in the specified collection to the end of this list.
    ///
    /// Returns `true` if this list changed as a result of the call.
    pub async fn add_all(&self, items: Vec<T>) -> Result<bool> {
        self.check_permission(PermissionAction::Put)?;

        let mut message = ClientMessage::create_for_encode_any_partition(LIST_ADD_ALL);
        message.add_frame(Self::string_frame(&self.name));
        self.add_data_list_frames(&mut message, &items)?;

        let response = self.invoke(message).await?;
        Self::decode_bool_response(&response)
    }

    /// Inserts all elements in the specified collection at the specified position.
    ///
    /// Returns `true` if this list changed as a result of the call.
    pub async fn add_all_at(&self, index: usize, items: Vec<T>) -> Result<bool> {
        self.check_permission(PermissionAction::Put)?;

        let mut message = ClientMessage::create_for_encode_any_partition(LIST_ADD_ALL_AT);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::int_frame(index as i32));
        self.add_data_list_frames(&mut message, &items)?;

        let response = self.invoke(message).await?;
        Self::decode_bool_response(&response)
    }

    /// Removes from this list all of its elements that are contained in the specified collection.
    ///
    /// Returns `true` if this list changed as a result of the call.
    pub async fn remove_all(&self, items: &[T]) -> Result<bool> {
        self.check_permission(PermissionAction::Remove)?;

        let mut message = ClientMessage::create_for_encode_any_partition(LIST_REMOVE_ALL);
        message.add_frame(Self::string_frame(&self.name));
        self.add_data_list_frames(&mut message, items)?;

        let response = self.invoke(message).await?;
        Self::decode_bool_response(&response)
    }

    /// Retains only the elements in this list that are contained in the specified collection.
    ///
    /// Returns `true` if this list changed as a result of the call.
    pub async fn retain_all(&self, items: &[T]) -> Result<bool> {
        self.check_permission(PermissionAction::Remove)?;

        let mut message = ClientMessage::create_for_encode_any_partition(LIST_RETAIN_ALL);
        message.add_frame(Self::string_frame(&self.name));
        self.add_data_list_frames(&mut message, items)?;

        let response = self.invoke(message).await?;
        Self::decode_bool_response(&response)
    }

    /// Returns `true` if this list contains all of the elements in the specified collection.
    pub async fn contains_all(&self, items: &[T]) -> Result<bool> {
        self.check_permission(PermissionAction::Read)?;

        let mut message = ClientMessage::create_for_encode_any_partition(LIST_CONTAINS_ALL);
        message.add_frame(Self::string_frame(&self.name));
        self.add_data_list_frames(&mut message, items)?;

        let response = self.invoke(message).await?;
        Self::decode_bool_response(&response)
    }

    /// Returns the index of the first occurrence of the specified element in this list.
    ///
    /// Returns `None` if this list does not contain the element.
    pub async fn index_of(&self, item: &T) -> Result<Option<usize>> {
        self.check_permission(PermissionAction::Read)?;
        let item_data = Self::serialize_value(item)?;

        let mut message = ClientMessage::create_for_encode_any_partition(LIST_INDEX_OF);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&item_data));

        let response = self.invoke(message).await?;
        Self::decode_index_response(&response)
    }

    /// Returns the index of the last occurrence of the specified element in this list.
    ///
    /// Returns `None` if this list does not contain the element.
    pub async fn last_index_of(&self, item: &T) -> Result<Option<usize>> {
        self.check_permission(PermissionAction::Read)?;
        let item_data = Self::serialize_value(item)?;

        let mut message = ClientMessage::create_for_encode_any_partition(LIST_LAST_INDEX_OF);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&item_data));

        let response = self.invoke(message).await?;
        Self::decode_index_response(&response)
    }

    /// Registers an item listener to receive notifications when items are added or removed.
    ///
    /// Returns a `ListenerId` that can be used to remove the listener later.
    pub async fn add_item_listener(&self, include_value: bool) -> Result<ListenerId> {
        self.check_permission(PermissionAction::Read)?;

        let mut message = ClientMessage::create_for_encode_any_partition(LIST_ADD_LISTENER);
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

        let mut message = ClientMessage::create_for_encode_any_partition(LIST_REMOVE_LISTENER);
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

    fn decode_index_response(response: &ClientMessage) -> Result<Option<usize>> {
        let index = Self::decode_int_response(response)?;
        if index < 0 {
            Ok(None)
        } else {
            Ok(Some(index as usize))
        }
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
