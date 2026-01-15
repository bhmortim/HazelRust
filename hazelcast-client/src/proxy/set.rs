//! Distributed set proxy implementation.

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

/// A distributed set proxy for performing set operations on a Hazelcast cluster.
///
/// `ISet` provides async set operations with automatic serialization.
#[derive(Debug)]
pub struct ISet<T> {
    name: String,
    connection_manager: Arc<ConnectionManager>,
    _phantom: PhantomData<fn() -> T>,
}

impl<T> ISet<T> {
    /// Creates a new set proxy.
    pub(crate) fn new(name: String, connection_manager: Arc<ConnectionManager>) -> Self {
        Self {
            name,
            connection_manager,
            _phantom: PhantomData,
        }
    }

    /// Returns the name of this set.
    pub fn name(&self) -> &str {
        &self.name
    }

    fn check_permission(&self, action: PermissionAction) -> Result<()> {
        let permissions = self.connection_manager.effective_permissions();
        if !permissions.is_permitted(action) {
            return Err(HazelcastError::Authorization(format!(
                "set '{}' operation denied: requires {:?} permission",
                self.name, action
            )));
        }
        Ok(())
    }
}

impl<T> ISet<T>
where
    T: Serializable + Deserializable + Send + Sync,
{
    async fn check_quorum(&self, is_read: bool) -> Result<()> {
        self.connection_manager.check_quorum(&self.name, is_read).await
    }

    /// Adds the specified element to this set if it is not already present.
    ///
    /// Returns `true` if the set did not already contain the element.
    pub async fn add(&self, item: T) -> Result<bool> {
        self.check_permission(PermissionAction::Put)?;
        self.check_quorum(false).await?;
        let item_data = Self::serialize_value(&item)?;

        let mut message = ClientMessage::create_for_encode_any_partition(SET_ADD);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&item_data));

        let response = self.invoke(message).await?;
        Self::decode_bool_response(&response)
    }

    /// Removes the specified element from this set if it is present.
    ///
    /// Returns `true` if the set contained the element.
    pub async fn remove(&self, item: &T) -> Result<bool> {
        self.check_permission(PermissionAction::Remove)?;
        self.check_quorum(false).await?;
        let item_data = Self::serialize_value(item)?;

        let mut message = ClientMessage::create_for_encode_any_partition(SET_REMOVE);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&item_data));

        let response = self.invoke(message).await?;
        Self::decode_bool_response(&response)
    }

    /// Returns `true` if this set contains the specified element.
    pub async fn contains(&self, item: &T) -> Result<bool> {
        self.check_permission(PermissionAction::Read)?;
        self.check_quorum(true).await?;
        let item_data = Self::serialize_value(item)?;

        let mut message = ClientMessage::create_for_encode_any_partition(SET_CONTAINS);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&item_data));

        let response = self.invoke(message).await?;
        Self::decode_bool_response(&response)
    }

    /// Returns the number of elements in this set.
    pub async fn size(&self) -> Result<usize> {
        self.check_permission(PermissionAction::Read)?;
        self.check_quorum(true).await?;
        let mut message = ClientMessage::create_for_encode_any_partition(SET_SIZE);
        message.add_frame(Self::string_frame(&self.name));

        let response = self.invoke(message).await?;
        Self::decode_int_response(&response).map(|v| v as usize)
    }

    /// Returns `true` if this set contains no elements.
    pub async fn is_empty(&self) -> Result<bool> {
        Ok(self.size().await? == 0)
    }

    /// Removes all elements from this set.
    pub async fn clear(&self) -> Result<()> {
        self.check_permission(PermissionAction::Remove)?;
        self.check_quorum(false).await?;
        let mut message = ClientMessage::create_for_encode_any_partition(SET_CLEAR);
        message.add_frame(Self::string_frame(&self.name));

        self.invoke(message).await?;
        Ok(())
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

impl<T> Clone for ISet<T> {
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
    fn test_iset_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<ISet<String>>();
    }

    #[tokio::test]
    async fn test_set_permission_denied_add() {
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
        let set: ISet<String> = ISet::new("test".to_string(), cm);

        let result = set.add("item".to_string()).await;
        assert!(matches!(result, Err(HazelcastError::Authorization(_))));
    }

    #[test]
    fn test_iset_clone() {
        fn assert_clone<T: Clone>() {}
        assert_clone::<ISet<String>>();
    }

    #[test]
    fn test_string_frame() {
        let frame = ISet::<String>::string_frame("test-set");
        assert_eq!(&frame.content[..], b"test-set");
    }

    #[test]
    fn test_serialize_string() {
        let data = ISet::<String>::serialize_value(&"hello".to_string()).unwrap();
        assert!(!data.is_empty());
    }
}
