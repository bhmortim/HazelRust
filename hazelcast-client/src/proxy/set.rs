//! Distributed set proxy implementation.

use std::marker::PhantomData;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;

use bytes::BytesMut;
use hazelcast_core::protocol::constants::*;
use hazelcast_core::protocol::Frame;
use hazelcast_core::serialization::ObjectDataOutput;
use hazelcast_core::{ClientMessage, Deserializable, HazelcastError, Result, Serializable};

use crate::config::PermissionAction;
use crate::connection::ConnectionManager;
use crate::listener::ListenerId;

/// A distributed set proxy for performing set operations on a Hazelcast cluster.
///
/// `ISet` provides async set operations with automatic serialization.
#[derive(Debug)]
pub struct ISet<T> {
    name: String,
    connection_manager: Arc<ConnectionManager>,
    /// Cached partition id for this set's name (-1 = not yet computed). An ISet
    /// lives on a single partition derived from its immutable name, so this is
    /// computed once instead of re-serializing the name on every op.
    cached_partition: AtomicI32,
    _phantom: PhantomData<fn() -> T>,
}

impl<T> ISet<T> {
    /// Creates a new set proxy.
    pub(crate) fn new(name: String, connection_manager: Arc<ConnectionManager>) -> Self {
        Self {
            name,
            connection_manager,
            cached_partition: AtomicI32::new(-1),
            _phantom: PhantomData,
        }
    }

    /// Returns the name of this set.
    pub fn name(&self) -> &str {
        &self.name
    }

    fn check_permission(&self, action: PermissionAction) -> Result<()> {
        if !self.connection_manager.is_permitted(action) {
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
        self.connection_manager
            .check_quorum(&self.name, is_read)
            .await
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

    /// Returns all elements in this set.
    ///
    /// Uses the SET_GET_ALL protocol opcode to retrieve all members in one call.
    pub async fn get_all(&self) -> Result<Vec<T>> {
        self.check_permission(PermissionAction::Read)?;
        self.check_quorum(true).await?;
        let mut message = ClientMessage::create_for_encode_any_partition(SET_GET_ALL);
        message.add_frame(Self::string_frame(&self.name));

        let response = self.invoke(message).await?;
        Self::decode_data_list_response(&response)
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

    /// Adds all elements in the specified collection to this set.
    ///
    /// Returns `true` if this set changed as a result of the call.
    pub async fn add_all(&self, items: Vec<T>) -> Result<bool> {
        self.check_permission(PermissionAction::Put)?;
        self.check_quorum(false).await?;

        let mut message = ClientMessage::create_for_encode_any_partition(SET_ADD_ALL);
        message.add_frame(Self::string_frame(&self.name));
        self.add_data_list_frames(&mut message, &items)?;

        let response = self.invoke(message).await?;
        Self::decode_bool_response(&response)
    }

    /// Removes from this set all of its elements that are contained in the specified collection.
    ///
    /// Returns `true` if this set changed as a result of the call.
    pub async fn remove_all(&self, items: &[T]) -> Result<bool> {
        self.check_permission(PermissionAction::Remove)?;
        self.check_quorum(false).await?;

        let mut message = ClientMessage::create_for_encode_any_partition(SET_REMOVE_ALL);
        message.add_frame(Self::string_frame(&self.name));
        self.add_data_list_frames(&mut message, items)?;

        let response = self.invoke(message).await?;
        Self::decode_bool_response(&response)
    }

    /// Retains only the elements in this set that are contained in the specified collection.
    ///
    /// Returns `true` if this set changed as a result of the call.
    pub async fn retain_all(&self, items: &[T]) -> Result<bool> {
        self.check_permission(PermissionAction::Remove)?;
        self.check_quorum(false).await?;

        let mut message = ClientMessage::create_for_encode_any_partition(SET_RETAIN_ALL);
        message.add_frame(Self::string_frame(&self.name));
        self.add_data_list_frames(&mut message, items)?;

        let response = self.invoke(message).await?;
        Self::decode_bool_response(&response)
    }

    /// Returns `true` if this set contains all of the elements in the specified collection.
    pub async fn contains_all(&self, items: &[T]) -> Result<bool> {
        self.check_permission(PermissionAction::Read)?;
        self.check_quorum(true).await?;

        let mut message = ClientMessage::create_for_encode_any_partition(SET_CONTAINS_ALL);
        message.add_frame(Self::string_frame(&self.name));
        self.add_data_list_frames(&mut message, items)?;

        let response = self.invoke(message).await?;
        Self::decode_bool_response(&response)
    }

    /// Registers an item listener to receive notifications when items are added or removed.
    ///
    /// Returns a `ListenerId` that can be used to remove the listener later.
    pub async fn add_item_listener(&self, include_value: bool) -> Result<ListenerId> {
        self.check_permission(PermissionAction::Read)?;

        let mut message = ClientMessage::create_for_encode_any_partition(SET_ADD_LISTENER);
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

        let mut message = ClientMessage::create_for_encode_any_partition(SET_REMOVE_LISTENER);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::uuid_frame(listener_id.as_uuid()));

        let response = self.invoke(message).await?;
        Self::decode_bool_response(&response)
    }

    fn serialize_value<V: Serializable>(value: &V) -> Result<Vec<u8>> {
        use hazelcast_core::serialization::DataOutput;
        let mut output = ObjectDataOutput::new();
        output.write_int(0)?; // partition_hash placeholder
        output.write_int(value.type_id())?; // Hazelcast constant type id
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
        buf.extend_from_slice(&[if value { 1u8 } else { 0u8 }]);
        Frame::with_content(buf)
    }

    fn uuid_frame(uuid: uuid::Uuid) -> Frame {
        let mut buf = BytesMut::with_capacity(16);
        buf.extend_from_slice(uuid.as_bytes());
        Frame::with_content(buf)
    }

    fn decode_data_list_response(response: &ClientMessage) -> Result<Vec<T>> {
        use hazelcast_core::serialization::ObjectDataInput;
        let mut items = Vec::with_capacity(response.frames().len().saturating_sub(1));
        for frame in response.frames().iter().skip(1) {
            if frame.flags & (BEGIN_DATA_STRUCTURE_FLAG | END_DATA_STRUCTURE_FLAG) != 0 {
                continue;
            }
            if frame.flags & IS_NULL_FLAG != 0 || frame.content.is_empty() {
                continue;
            }
            let payload = if frame.content.len() > 8 {
                &frame.content[8..]
            } else {
                &frame.content[..]
            };
            let mut input = ObjectDataInput::new(payload);
            items.push(T::deserialize(&mut input)?);
        }
        Ok(items)
    }

    fn add_data_list_frames(&self, message: &mut ClientMessage, items: &[T]) -> Result<()> {
        message.add_frame(Frame::with_flags(BEGIN_DATA_STRUCTURE_FLAG));
        for item in items {
            let item_data = Self::serialize_value(item)?;
            message.add_frame(Frame::with_content(BytesMut::from(&item_data[..])));
        }
        message.add_frame(Frame::with_flags(END_DATA_STRUCTURE_FLAG));
        Ok(())
    }

    fn name_partition_id(&self) -> i32 {
        // Fast path: the partition never changes once known, so reuse the cached
        // value and skip the per-op name serialization + hash + allocations.
        let cached = self.cached_partition.load(Ordering::Relaxed);
        if cached >= 0 {
            return cached;
        }
        let raw_count = self.connection_manager.partition_count();
        let known = raw_count > 0;
        let count = if known { raw_count } else { 271 };
        let pid = match Self::serialize_value(&self.name) {
            Ok(data) => {
                let hash_input = if data.len() > 8 {
                    &data[8..]
                } else {
                    &data[..]
                };
                hazelcast_core::partition_id_for_hash(
                    hazelcast_core::compute_partition_hash(hash_input),
                    count,
                )
            }
            Err(_) => 0,
        };
        if known {
            self.cached_partition.store(pid, Ordering::Relaxed);
        }
        pid
    }

    async fn invoke(&self, mut message: ClientMessage) -> Result<ClientMessage> {
        let pid = self.name_partition_id();
        message.set_partition_id(pid);
        self.connection_manager
            .invoke_on_partition(pid, message)
            .await
    }

    fn decode_bool_response(response: &ClientMessage) -> Result<bool> {
        let frames = response.frames();
        if frames.is_empty() {
            return Err(HazelcastError::Serialization("empty response".to_string()));
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
            return Err(HazelcastError::Serialization("empty response".to_string()));
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

impl<T> Clone for ISet<T> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            connection_manager: Arc::clone(&self.connection_manager),
            cached_partition: AtomicI32::new(self.cached_partition.load(Ordering::Relaxed)),
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
        use crate::config::{ClientConfigBuilder, PermissionAction, Permissions};
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
