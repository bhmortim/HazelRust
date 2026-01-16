//! Distributed atomic reference proxy implementation.

use std::marker::PhantomData;
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::BytesMut;
use hazelcast_core::protocol::constants::*;
use hazelcast_core::protocol::Frame;
use hazelcast_core::{ClientMessage, Deserializable, HazelcastError, Result, Serializable};

use crate::config::PermissionAction;
use crate::connection::ConnectionManager;

/// A distributed atomic reference proxy for performing atomic operations on the Hazelcast CP subsystem.
///
/// `AtomicReference` provides async atomic reference operations with strong consistency guarantees
/// through the CP (Consistent Partition) subsystem.
#[derive(Debug)]
pub struct AtomicReference<T> {
    name: String,
    connection_manager: Arc<ConnectionManager>,
    _marker: PhantomData<T>,
}

impl<T> AtomicReference<T>
where
    T: Serializable + Deserializable + Send + Sync,
{
    /// Creates a new atomic reference proxy.
    pub(crate) fn new(name: String, connection_manager: Arc<ConnectionManager>) -> Self {
        Self {
            name,
            connection_manager,
            _marker: PhantomData,
        }
    }

    /// Returns the name of this atomic reference.
    pub fn name(&self) -> &str {
        &self.name
    }

    fn check_permission(&self, action: PermissionAction) -> Result<()> {
        let permissions = self.connection_manager.effective_permissions();
        if !permissions.is_permitted(action) {
            return Err(HazelcastError::Authorization(format!(
                "atomic reference '{}' operation denied: requires {:?} permission",
                self.name, action
            )));
        }
        Ok(())
    }

    async fn check_quorum(&self, is_read: bool) -> Result<()> {
        self.connection_manager.check_quorum(&self.name, is_read).await
    }

    /// Gets the current value.
    ///
    /// Returns `None` if the reference is null.
    pub async fn get(&self) -> Result<Option<T>> {
        self.check_permission(PermissionAction::Read)?;
        self.check_quorum(true).await?;
        let mut message = ClientMessage::create_for_encode_any_partition(CP_ATOMIC_REFERENCE_GET);
        message.add_frame(Self::string_frame(&self.name));

        let response = self.invoke(message).await?;
        self.decode_optional_value(&response)
    }

    /// Sets the value to the given value.
    ///
    /// Pass `None` to set the reference to null.
    pub async fn set(&self, value: Option<T>) -> Result<()> {
        self.check_permission(PermissionAction::Put)?;
        self.check_quorum(false).await?;
        let mut message = ClientMessage::create_for_encode_any_partition(CP_ATOMIC_REFERENCE_SET);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(self.serialize_optional_value(value.as_ref())?);

        self.invoke(message).await?;
        Ok(())
    }

    /// Atomically sets the value to the given value and returns the old value.
    ///
    /// Returns `None` if the previous value was null.
    pub async fn get_and_set(&self, value: Option<T>) -> Result<Option<T>> {
        self.check_permission(PermissionAction::Put)?;
        self.check_quorum(false).await?;
        let mut message = ClientMessage::create_for_encode_any_partition(CP_ATOMIC_REFERENCE_GET_AND_SET);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(self.serialize_optional_value(value.as_ref())?);

        let response = self.invoke(message).await?;
        self.decode_optional_value(&response)
    }

    /// Atomically sets the value to the given updated value if the current value equals the expected value.
    ///
    /// Returns `true` if successful, `false` if the actual value was not equal to the expected value.
    pub async fn compare_and_set(&self, expected: Option<&T>, update: Option<T>) -> Result<bool> {
        self.check_permission(PermissionAction::Put)?;
        self.check_quorum(false).await?;
        let mut message = ClientMessage::create_for_encode_any_partition(CP_ATOMIC_REFERENCE_COMPARE_AND_SET);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(self.serialize_optional_value(expected)?);
        message.add_frame(self.serialize_optional_value(update.as_ref())?);

        let response = self.invoke(message).await?;
        Self::decode_bool_response(&response)
    }

    /// Checks if the current value equals the given value.
    pub async fn contains(&self, value: &T) -> Result<bool> {
        self.check_permission(PermissionAction::Read)?;
        self.check_quorum(true).await?;
        let mut message = ClientMessage::create_for_encode_any_partition(CP_ATOMIC_REFERENCE_CONTAINS);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(self.serialize_value(value)?);

        let response = self.invoke(message).await?;
        Self::decode_bool_response(&response)
    }

    /// Checks if the current value is null.
    pub async fn is_null(&self) -> Result<bool> {
        self.check_permission(PermissionAction::Read)?;
        self.check_quorum(true).await?;
        let mut message = ClientMessage::create_for_encode_any_partition(CP_ATOMIC_REFERENCE_IS_NULL);
        message.add_frame(Self::string_frame(&self.name));

        let response = self.invoke(message).await?;
        Self::decode_bool_response(&response)
    }

    /// Clears the value (sets it to null).
    ///
    /// This is equivalent to calling `set(None)`.
    pub async fn clear(&self) -> Result<()> {
        self.set(None).await
    }

    fn string_frame(s: &str) -> Frame {
        Frame::with_content(BytesMut::from(s.as_bytes()))
    }

    fn serialize_value(&self, value: &T) -> Result<Frame> {
        let bytes = value.serialize()?;
        Ok(Frame::with_content(BytesMut::from(bytes.as_slice())))
    }

    fn serialize_optional_value(&self, value: Option<&T>) -> Result<Frame> {
        match value {
            Some(v) => self.serialize_value(v),
            None => Ok(Frame::with_content(BytesMut::new())),
        }
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

    fn decode_optional_value(&self, response: &ClientMessage) -> Result<Option<T>> {
        let frames = response.frames();
        if frames.is_empty() {
            return Ok(None);
        }

        let initial_frame = &frames[0];
        if initial_frame.content.len() <= RESPONSE_HEADER_SIZE {
            return Ok(None);
        }

        let data = &initial_frame.content[RESPONSE_HEADER_SIZE..];
        if data.is_empty() {
            return Ok(None);
        }

        let value = T::deserialize(data)?;
        Ok(Some(value))
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
}

impl<T> Clone for AtomicReference<T> {
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
    fn test_atomic_reference_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<AtomicReference<String>>();
    }

    #[tokio::test]
    async fn test_atomic_reference_permission_denied_get() {
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
        let reference: AtomicReference<String> = AtomicReference::new("test".to_string(), cm);

        let result = reference.get().await;
        assert!(matches!(result, Err(HazelcastError::Authorization(_))));
    }

    #[tokio::test]
    async fn test_atomic_reference_permission_denied_set() {
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
        let reference: AtomicReference<String> = AtomicReference::new("test".to_string(), cm);

        let result = reference.set(Some("value".to_string())).await;
        assert!(matches!(result, Err(HazelcastError::Authorization(_))));
    }

    #[tokio::test]
    async fn test_atomic_reference_quorum_not_present_blocks_operations() {
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
        let reference: AtomicReference<String> = AtomicReference::new("protected-ref".to_string(), cm);

        let result = reference.get().await;
        assert!(matches!(result, Err(HazelcastError::QuorumNotPresent(_))));

        let result = reference.set(Some("value".to_string())).await;
        assert!(matches!(result, Err(HazelcastError::QuorumNotPresent(_))));

        let result = reference.is_null().await;
        assert!(matches!(result, Err(HazelcastError::QuorumNotPresent(_))));

        let result = reference.compare_and_set(None, Some("value".to_string())).await;
        assert!(matches!(result, Err(HazelcastError::QuorumNotPresent(_))));
    }

    #[test]
    fn test_atomic_reference_clone() {
        fn assert_clone<T: Clone>() {}
        assert_clone::<AtomicReference<String>>();
    }

    #[test]
    fn test_string_frame() {
        let frame = AtomicReference::<String>::string_frame("test-reference");
        assert_eq!(&frame.content[..], b"test-reference");
    }

    #[test]
    fn test_serialize_deserialize_value() {
        use crate::config::ClientConfigBuilder;
        use crate::connection::ConnectionManager;
        use std::sync::Arc;

        let config = ClientConfigBuilder::new().build().unwrap();
        let cm = Arc::new(ConnectionManager::from_config(config));
        let reference: AtomicReference<String> = AtomicReference::new("test".to_string(), cm);

        let original = "hello world".to_string();
        let frame = reference.serialize_value(&original).unwrap();
        assert!(!frame.content.is_empty());

        let deserialized = String::deserialize(&frame.content).unwrap();
        assert_eq!(deserialized, original);
    }
}
