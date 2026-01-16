//! Distributed CP Map proxy implementation.

use std::marker::PhantomData;
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::BytesMut;
use hazelcast_core::protocol::constants::*;
use hazelcast_core::protocol::Frame;
use hazelcast_core::{ClientMessage, Deserializable, HazelcastError, Result, Serializable};

use crate::config::PermissionAction;
use crate::connection::ConnectionManager;

/// A distributed map backed by the CP subsystem.
///
/// `CPMap` provides a key-value store with strong consistency guarantees
/// through the CP (Consistent Partition) subsystem using the Raft consensus algorithm.
///
/// Unlike the regular `IMap`, `CPMap` guarantees linearizable operations,
/// making it suitable for scenarios requiring strong consistency such as
/// configuration management or leader election metadata.
///
/// Note: CPMap requires the CP subsystem to be enabled on the Hazelcast cluster.
///
/// # Type Parameters
///
/// - `K`: The key type, must implement `Serializable`, `Deserializable`, `Send`, and `Sync`
/// - `V`: The value type, must implement `Serializable`, `Deserializable`, `Send`, and `Sync`
#[derive(Debug)]
pub struct CPMap<K, V> {
    name: String,
    connection_manager: Arc<ConnectionManager>,
    _marker: PhantomData<(K, V)>,
}

impl<K, V> CPMap<K, V>
where
    K: Serializable + Deserializable + Send + Sync,
    V: Serializable + Deserializable + Send + Sync,
{
    /// Creates a new CPMap proxy.
    pub(crate) fn new(name: String, connection_manager: Arc<ConnectionManager>) -> Self {
        Self {
            name,
            connection_manager,
            _marker: PhantomData,
        }
    }

    /// Returns the name of this CPMap.
    pub fn name(&self) -> &str {
        &self.name
    }

    fn check_permission(&self, action: PermissionAction) -> Result<()> {
        let permissions = self.connection_manager.effective_permissions();
        if !permissions.is_permitted(action) {
            return Err(HazelcastError::Authorization(format!(
                "CPMap '{}' operation denied: requires {:?} permission",
                self.name, action
            )));
        }
        Ok(())
    }

    async fn check_quorum(&self, is_read: bool) -> Result<()> {
        self.connection_manager.check_quorum(&self.name, is_read).await
    }

    /// Gets the value associated with the specified key.
    ///
    /// # Arguments
    ///
    /// * `key` - The key whose associated value is to be returned
    ///
    /// # Returns
    ///
    /// The value associated with the key, or `None` if no mapping exists.
    ///
    /// # Errors
    ///
    /// Returns an error if a network error occurs or the CP subsystem is unavailable.
    pub async fn get(&self, key: &K) -> Result<Option<V>> {
        self.check_permission(PermissionAction::Read)?;
        self.check_quorum(true).await?;

        let key_data = Self::serialize_value(key)?;

        let mut message = ClientMessage::create_for_encode_any_partition(CP_MAP_GET);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&key_data));

        let response = self.invoke(message).await?;
        Self::decode_optional_value_response(&response)
    }

    /// Associates the specified value with the specified key.
    ///
    /// If the map previously contained a mapping for the key, the old value is
    /// replaced and returned.
    ///
    /// # Arguments
    ///
    /// * `key` - The key with which the specified value is to be associated
    /// * `value` - The value to be associated with the specified key
    ///
    /// # Returns
    ///
    /// The previous value associated with the key, or `None` if there was no mapping.
    ///
    /// # Errors
    ///
    /// Returns an error if a network error occurs or the CP subsystem is unavailable.
    pub async fn put(&self, key: K, value: V) -> Result<Option<V>> {
        self.check_permission(PermissionAction::Put)?;
        self.check_quorum(false).await?;

        let key_data = Self::serialize_value(&key)?;
        let value_data = Self::serialize_value(&value)?;

        let mut message = ClientMessage::create_for_encode_any_partition(CP_MAP_PUT);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&key_data));
        message.add_frame(Self::data_frame(&value_data));

        let response = self.invoke(message).await?;
        Self::decode_optional_value_response(&response)
    }

    /// Sets the value for the specified key without returning the old value.
    ///
    /// This operation is more efficient than `put()` when the old value is not needed.
    ///
    /// # Arguments
    ///
    /// * `key` - The key with which the specified value is to be associated
    /// * `value` - The value to be associated with the specified key
    ///
    /// # Errors
    ///
    /// Returns an error if a network error occurs or the CP subsystem is unavailable.
    pub async fn set(&self, key: K, value: V) -> Result<()> {
        self.check_permission(PermissionAction::Put)?;
        self.check_quorum(false).await?;

        let key_data = Self::serialize_value(&key)?;
        let value_data = Self::serialize_value(&value)?;

        let mut message = ClientMessage::create_for_encode_any_partition(CP_MAP_SET);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&key_data));
        message.add_frame(Self::data_frame(&value_data));

        self.invoke(message).await?;
        Ok(())
    }

    /// Removes the mapping for the specified key and returns the old value.
    ///
    /// # Arguments
    ///
    /// * `key` - The key whose mapping is to be removed
    ///
    /// # Returns
    ///
    /// The previous value associated with the key, or `None` if there was no mapping.
    ///
    /// # Errors
    ///
    /// Returns an error if a network error occurs or the CP subsystem is unavailable.
    pub async fn remove(&self, key: &K) -> Result<Option<V>> {
        self.check_permission(PermissionAction::Remove)?;
        self.check_quorum(false).await?;

        let key_data = Self::serialize_value(key)?;

        let mut message = ClientMessage::create_for_encode_any_partition(CP_MAP_REMOVE);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&key_data));

        let response = self.invoke(message).await?;
        Self::decode_optional_value_response(&response)
    }

    /// Removes the mapping for the specified key without returning the old value.
    ///
    /// This operation is more efficient than `remove()` when the old value is not needed.
    ///
    /// # Arguments
    ///
    /// * `key` - The key whose mapping is to be removed
    ///
    /// # Errors
    ///
    /// Returns an error if a network error occurs or the CP subsystem is unavailable.
    pub async fn delete(&self, key: &K) -> Result<()> {
        self.check_permission(PermissionAction::Remove)?;
        self.check_quorum(false).await?;

        let key_data = Self::serialize_value(key)?;

        let mut message = ClientMessage::create_for_encode_any_partition(CP_MAP_DELETE);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&key_data));

        self.invoke(message).await?;
        Ok(())
    }

    /// Atomically sets the value for the specified key if the current value equals the expected value.
    ///
    /// # Arguments
    ///
    /// * `key` - The key with which the specified value is to be associated
    /// * `expected` - The expected current value
    /// * `update` - The new value to be associated with the key
    ///
    /// # Returns
    ///
    /// `true` if the value was updated, `false` if the current value did not match the expected value.
    ///
    /// # Errors
    ///
    /// Returns an error if a network error occurs or the CP subsystem is unavailable.
    pub async fn compare_and_set(&self, key: &K, expected: &V, update: V) -> Result<bool> {
        self.check_permission(PermissionAction::Put)?;
        self.check_quorum(false).await?;

        let key_data = Self::serialize_value(key)?;
        let expected_data = Self::serialize_value(expected)?;
        let update_data = Self::serialize_value(&update)?;

        let mut message = ClientMessage::create_for_encode_any_partition(CP_MAP_COMPARE_AND_SET);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::data_frame(&key_data));
        message.add_frame(Self::data_frame(&expected_data));
        message.add_frame(Self::data_frame(&update_data));

        let response = self.invoke(message).await?;
        Self::decode_bool_response(&response)
    }

    fn serialize_value<T: Serializable>(value: &T) -> Result<BytesMut> {
        let mut buf = BytesMut::new();
        value.serialize(&mut buf)?;
        Ok(buf)
    }

    fn string_frame(s: &str) -> Frame {
        Frame::with_content(BytesMut::from(s.as_bytes()))
    }

    fn data_frame(data: &BytesMut) -> Frame {
        Frame::with_content(data.clone())
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

    fn decode_optional_value_response(response: &ClientMessage) -> Result<Option<V>> {
        let frames = response.frames();
        if frames.is_empty() {
            return Err(HazelcastError::Serialization("empty response".to_string()));
        }

        let initial_frame = &frames[0];
        if initial_frame.content.len() <= RESPONSE_HEADER_SIZE {
            return Ok(None);
        }

        if frames.len() < 2 {
            return Ok(None);
        }

        let data_frame = &frames[1];
        if data_frame.content.is_empty() || data_frame.is_null_frame() {
            return Ok(None);
        }

        let value = V::deserialize(&mut data_frame.content.clone())?;
        Ok(Some(value))
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
}

impl<K, V> Clone for CPMap<K, V> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            connection_manager: Arc::clone(&self.connection_manager),
            _marker: PhantomData,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cp_map_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<CPMap<String, String>>();
    }

    #[test]
    fn test_cp_map_clone() {
        fn assert_clone<T: Clone>() {}
        assert_clone::<CPMap<String, String>>();
    }

    #[test]
    fn test_string_frame() {
        let frame = CPMap::<String, String>::string_frame("test-cp-map");
        assert_eq!(&frame.content[..], b"test-cp-map");
    }

    #[test]
    fn test_data_frame() {
        let data = BytesMut::from(&b"test-data"[..]);
        let frame = CPMap::<String, String>::data_frame(&data);
        assert_eq!(&frame.content[..], b"test-data");
    }

    #[tokio::test]
    async fn test_cp_map_permission_denied_get() {
        use crate::config::{ClientConfigBuilder, PermissionAction, Permissions};
        use crate::connection::ConnectionManager;
        use std::sync::Arc;

        let mut perms = Permissions::new();
        perms.grant(PermissionAction::Put);

        let config = ClientConfigBuilder::new()
            .security(|s| s.permissions(perms))
            .build()
            .unwrap();

        let cm = Arc::new(ConnectionManager::from_config(config));
        let map: CPMap<String, String> = CPMap::new("test".to_string(), cm);

        let result = map.get(&"key".to_string()).await;
        assert!(matches!(result, Err(HazelcastError::Authorization(_))));
    }

    #[tokio::test]
    async fn test_cp_map_permission_denied_put() {
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
        let map: CPMap<String, String> = CPMap::new("test".to_string(), cm);

        let result = map.put("key".to_string(), "value".to_string()).await;
        assert!(matches!(result, Err(HazelcastError::Authorization(_))));
    }

    #[tokio::test]
    async fn test_cp_map_permission_denied_remove() {
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
        let map: CPMap<String, String> = CPMap::new("test".to_string(), cm);

        let result = map.remove(&"key".to_string()).await;
        assert!(matches!(result, Err(HazelcastError::Authorization(_))));
    }

    #[tokio::test]
    async fn test_cp_map_quorum_not_present_blocks_operations() {
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
        let map: CPMap<String, String> = CPMap::new("protected-map".to_string(), cm);

        let result = map.get(&"key".to_string()).await;
        assert!(matches!(result, Err(HazelcastError::QuorumNotPresent(_))));

        let result = map.put("key".to_string(), "value".to_string()).await;
        assert!(matches!(result, Err(HazelcastError::QuorumNotPresent(_))));

        let result = map.remove(&"key".to_string()).await;
        assert!(matches!(result, Err(HazelcastError::QuorumNotPresent(_))));

        let result = map.set("key".to_string(), "value".to_string()).await;
        assert!(matches!(result, Err(HazelcastError::QuorumNotPresent(_))));

        let result = map.delete(&"key".to_string()).await;
        assert!(matches!(result, Err(HazelcastError::QuorumNotPresent(_))));

        let result = map
            .compare_and_set(&"key".to_string(), &"old".to_string(), "new".to_string())
            .await;
        assert!(matches!(result, Err(HazelcastError::QuorumNotPresent(_))));
    }

    #[test]
    fn test_decode_bool_response_true() {
        let mut content = BytesMut::with_capacity(RESPONSE_HEADER_SIZE + 1);
        content.extend_from_slice(&[0u8; RESPONSE_HEADER_SIZE]);
        content.extend_from_slice(&[1u8]);

        let frame = Frame::with_content(content);
        let message = ClientMessage::from_frames(vec![frame]);

        let result = CPMap::<String, String>::decode_bool_response(&message);
        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    #[test]
    fn test_decode_bool_response_false() {
        let mut content = BytesMut::with_capacity(RESPONSE_HEADER_SIZE + 1);
        content.extend_from_slice(&[0u8; RESPONSE_HEADER_SIZE]);
        content.extend_from_slice(&[0u8]);

        let frame = Frame::with_content(content);
        let message = ClientMessage::from_frames(vec![frame]);

        let result = CPMap::<String, String>::decode_bool_response(&message);
        assert!(result.is_ok());
        assert!(!result.unwrap());
    }

    #[test]
    fn test_decode_bool_response_empty() {
        let message = ClientMessage::from_frames(vec![]);
        let result = CPMap::<String, String>::decode_bool_response(&message);
        assert!(result.is_err());
    }
}
