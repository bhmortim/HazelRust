//! Distributed CP Map proxy implementation.

use std::marker::PhantomData;
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::BytesMut;
use tokio::sync::OnceCell;

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
/// A resolved CP Raft group identifier (`seed`, `id`, `name`).
#[derive(Debug, Clone)]
struct RaftGroupId {
    seed: i64,
    id: i64,
    name: String,
}

#[derive(Debug)]
pub struct CPMap<K, V> {
    name: String,
    connection_manager: Arc<ConnectionManager>,
    /// Lazily-resolved Raft group id, shared across clones.
    group_id: Arc<OnceCell<RaftGroupId>>,
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
            group_id: Arc::new(OnceCell::new()),
            _marker: PhantomData,
        }
    }

    /// The object name without any `@group` suffix (what CP ops send as `name`).
    fn object_name(&self) -> &str {
        match self.name.split_once('@') {
            Some((obj, _)) => obj,
            None => &self.name,
        }
    }

    /// The CP group name (`default` unless an `@group` suffix is present).
    fn group_name(&self) -> &str {
        match self.name.split_once('@') {
            Some((_, grp)) if !grp.is_empty() => grp,
            _ => "default",
        }
    }

    /// Resolves (and caches) the Raft `groupId` for this CP map's group via
    /// `CPGroupCreateCPGroup` (0x1E0100), matching the AtomicLong CP path.
    async fn resolve_group(&self) -> Result<RaftGroupId> {
        let g = self
            .group_id
            .get_or_try_init(|| async {
                let mut request =
                    ClientMessage::create_for_encode_any_partition(CP_SUBSYSTEM_GET_GROUP_IDS);
                request.add_frame(Self::string_frame(self.group_name()));
                let response = self.connection_manager.invoke_on_random(request).await?;
                Self::decode_group_id(&response)
            })
            .await?;
        Ok(g.clone())
    }

    fn decode_group_id(response: &ClientMessage) -> Result<RaftGroupId> {
        let frames = response.frames();
        if frames.len() < 4 {
            return Err(HazelcastError::Protocol(
                "CP group id response too short (CP subsystem unavailable?)".to_string(),
            ));
        }
        let fixed = &frames[2].content;
        if fixed.len() < 16 {
            return Err(HazelcastError::Protocol(
                "CP group id fixed frame too short".to_string(),
            ));
        }
        let seed = i64::from_le_bytes(fixed[0..8].try_into().unwrap());
        let id = i64::from_le_bytes(fixed[8..16].try_into().unwrap());
        let name = String::from_utf8_lossy(&frames[3].content).to_string();
        Ok(RaftGroupId { seed, id, name })
    }

    /// Encodes a `RaftGroupId` data structure: BEGIN / [seed(8) id(8)] / name / END.
    fn encode_group_id(message: &mut ClientMessage, group: &RaftGroupId) {
        message.add_frame(Frame::with_flags(BEGIN_DATA_STRUCTURE_FLAG));
        let mut fixed = BytesMut::with_capacity(16);
        fixed.extend_from_slice(&group.seed.to_le_bytes());
        fixed.extend_from_slice(&group.id.to_le_bytes());
        message.add_frame(Frame::with_content(fixed));
        message.add_frame(Self::string_frame(&group.name));
        message.add_frame(Frame::with_flags(END_DATA_STRUCTURE_FLAG));
    }

    /// Builds a CP map request: initial frame (header), groupId data structure,
    /// object name, then the variable-size `Data` params in order.
    async fn build_request(&self, msg_type: i32, data_params: &[&[u8]]) -> Result<ClientMessage> {
        let group = self.resolve_group().await?;
        let mut message = ClientMessage::create_for_encode_any_partition(msg_type);
        Self::encode_group_id(&mut message, &group);
        message.add_frame(Self::string_frame(self.object_name()));
        for d in data_params {
            message.add_frame(Self::data_frame(d));
        }
        Ok(message)
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
        self.connection_manager
            .check_quorum(&self.name, is_read)
            .await
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
        let message = self
            .build_request(CP_MAP_GET, &[key_data.as_slice()])
            .await?;
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
        let message = self
            .build_request(CP_MAP_PUT, &[key_data.as_slice(), value_data.as_slice()])
            .await?;
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
        let message = self
            .build_request(CP_MAP_SET, &[key_data.as_slice(), value_data.as_slice()])
            .await?;
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
        let message = self
            .build_request(CP_MAP_REMOVE, &[key_data.as_slice()])
            .await?;
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
        let message = self
            .build_request(CP_MAP_DELETE, &[key_data.as_slice()])
            .await?;
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
        let message = self
            .build_request(
                CP_MAP_COMPARE_AND_SET,
                &[
                    key_data.as_slice(),
                    expected_data.as_slice(),
                    update_data.as_slice(),
                ],
            )
            .await?;
        let response = self.invoke(message).await?;
        Self::decode_bool_response(&response)
    }

    fn serialize_value<T: Serializable>(value: &T) -> Result<Vec<u8>> {
        // Emit a proper Hazelcast Data: [partition_hash: i32 BE][type_id: i32 BE][payload],
        // matching the IMap path. Previously this wrote a bare payload with no type-id
        // header, making CP-map entries non-standard Data (unreadable by other clients).
        use hazelcast_core::serialization::{DataOutput, ObjectDataOutput};
        let mut output = ObjectDataOutput::new();
        output.write_int(0)?; // partition_hash placeholder (CP maps are Raft-replicated, not partitioned)
        output.write_int(value.type_id())?;
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
        let _address = self.get_connection_address().await?;
        self.connection_manager.invoke_on_random(message).await
    }

    async fn get_connection_address(&self) -> Result<SocketAddr> {
        let addresses = self.connection_manager.connected_addresses().await;
        addresses
            .into_iter()
            .next()
            .ok_or_else(|| HazelcastError::Connection("no connections available".to_string()))
    }

    fn decode_optional_value_response(response: &ClientMessage) -> Result<Option<V>> {
        let frames = response.frames();
        if frames.is_empty() {
            return Err(HazelcastError::Serialization("empty response".to_string()));
        }

        // The value (nullable Data) is the first frame AFTER the response header frame.
        // Do NOT gate on the initial frame's length: for a Data-valued response the
        // initial frame is just the header and the value lives in frames[1].
        if frames.len() < 2 {
            return Ok(None);
        }

        let data_frame = &frames[1];
        if data_frame.content.is_empty() || data_frame.is_null_frame() {
            return Ok(None);
        }

        // Skip the 8-byte Data header (partition_hash + type_id) before deserializing,
        // matching the IMap value-decode path and the header now written by serialize_value.
        if data_frame.content.len() < 8 {
            return Err(HazelcastError::Serialization(
                "CP map value frame shorter than 8-byte Data header".to_string(),
            ));
        }
        let value = V::from_bytes(&data_frame.content[8..])?;
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
            group_id: Arc::clone(&self.group_id),
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
        let data = b"test-data";
        let frame = CPMap::<String, String>::data_frame(data);
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
