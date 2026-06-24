//! Distributed atomic reference proxy implementation.
//!
//! Like every CP data structure, each request must carry the Raft `groupId`,
//! encoded as a data structure (`BEGIN` / `[seed,id]` / name / `END`) — see issue
//! #12 and the `AtomicLong` fix. AtomicReference values travel as *nullable* `Data`
//! frames (a null value is a frame with `IS_NULL_FLAG`, not an empty frame), and the
//! response value lives in the frame *after* the response header frame. `set` and
//! `get_and_set` share the `Set` message, selected by the `returnOldValue` boolean
//! carried in the request's initial frame.

use std::marker::PhantomData;
use std::sync::Arc;

use bytes::BytesMut;
use hazelcast_core::protocol::constants::*;
use hazelcast_core::protocol::Frame;
use hazelcast_core::{ClientMessage, Deserializable, HazelcastError, Result, Serializable};
use tokio::sync::OnceCell;

use crate::config::PermissionAction;
use crate::connection::ConnectionManager;

// Correct Hazelcast AtomicRef message types. The hazelcast_core CP_ATOMIC_REFERENCE_*
// constants are mislabeled the same way the CP_ATOMIC_LONG_* ones are (declaration
// order vs. the real protocol order Apply/CompareAndSet/Contains/Get/Set), which is why
// every AtomicReference op silently no-op'd against a real CP subsystem. Verified
// against the Hazelcast client protocol codecs (issue #12). `set`/`get_and_set` both use
// `Set` and differ only by the `returnOldValue` flag.
const ATOMIC_REF_COMPARE_AND_SET: i32 = 0x0A0200;
const ATOMIC_REF_CONTAINS: i32 = 0x0A0300;
const ATOMIC_REF_GET: i32 = 0x0A0400;
const ATOMIC_REF_SET: i32 = 0x0A0500;

/// A resolved CP Raft group identifier (`seed`, `id`, `name`).
#[derive(Debug, Clone)]
struct RaftGroupId {
    seed: i64,
    id: i64,
    name: String,
}

/// A distributed atomic reference backed by the Hazelcast CP (Raft) subsystem.
///
/// Provides linearizable atomic operations on a single nullable value with strong
/// consistency guarantees.
#[derive(Debug)]
pub struct AtomicReference<T> {
    /// Full proxy name (may include an `@group` suffix).
    name: String,
    connection_manager: Arc<ConnectionManager>,
    /// Lazily-resolved Raft group id, shared across clones.
    group_id: Arc<OnceCell<RaftGroupId>>,
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
            group_id: Arc::new(OnceCell::new()),
            _marker: PhantomData,
        }
    }

    /// Returns the name of this atomic reference.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The object name without any `@group` suffix (what CP ops send as `name`).
    fn object_name(&self) -> &str {
        match self.name.split_once('@') {
            Some((obj, _)) => obj,
            None => &self.name,
        }
    }

    /// The CP group name owning this object (`default` unless an `@group` suffix is present).
    fn group_name(&self) -> &str {
        match self.name.split_once('@') {
            Some((_, grp)) if !grp.is_empty() => grp,
            _ => "default",
        }
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
        self.connection_manager
            .check_quorum(&self.name, is_read)
            .await
    }

    /// Resolves (and caches) the Raft `groupId` for this object's CP group via
    /// `CPGroupCreateCPGroup` (`0x1E0100`), which creates the group if needed.
    async fn resolve_group(&self) -> Result<&RaftGroupId> {
        self.group_id
            .get_or_try_init(|| async {
                let mut request =
                    ClientMessage::create_for_encode_any_partition(CP_GROUP_CREATE_CP_GROUP);
                request.add_frame(Self::string_frame(self.group_name()));
                let response = self.connection_manager.invoke_on_random(request).await?;
                Self::decode_group_id(&response)
            })
            .await
    }

    /// Decodes a `RaftGroupId` data structure: `[initial] BEGIN [seed(8) id(8)] name END`.
    fn decode_group_id(response: &ClientMessage) -> Result<RaftGroupId> {
        let frames = response.frames();
        if frames.len() < 4 {
            return Err(HazelcastError::Protocol(
                "CP group id response too short (CP subsystem unavailable?)".to_string(),
            ));
        }
        let fixed = frames[2].content();
        if fixed.len() < 16 {
            return Err(HazelcastError::Protocol(
                "CP group id fixed frame too short".to_string(),
            ));
        }
        let seed = i64::from_le_bytes(fixed[0..8].try_into().unwrap());
        let id = i64::from_le_bytes(fixed[8..16].try_into().unwrap());
        let name = String::from_utf8_lossy(frames[3].content()).to_string();
        Ok(RaftGroupId { seed, id, name })
    }

    /// Encodes a `RaftGroupId` as a data structure:
    /// `BEGIN` / `[seed(8) id(8)]` / name(String) / `END`.
    fn encode_group_id(message: &mut ClientMessage, group: &RaftGroupId) {
        message.add_frame(Frame::with_flags(BEGIN_DATA_STRUCTURE_FLAG));
        let mut fixed = BytesMut::with_capacity(16);
        fixed.extend_from_slice(&group.seed.to_le_bytes());
        fixed.extend_from_slice(&group.id.to_le_bytes());
        message.add_frame(Frame::with_content(fixed));
        message.add_frame(Self::string_frame(&group.name));
        message.add_frame(Frame::with_flags(END_DATA_STRUCTURE_FLAG));
    }

    /// Builds the common request prefix: initial frame (header + optional fixed
    /// `bool`), then the `groupId` data structure, then the object name. Callers
    /// append any nullable `Data` value frames afterwards.
    async fn cp_message(&self, msg_type: i32, init_bool: Option<bool>) -> Result<ClientMessage> {
        let group = self.resolve_group().await?.clone();
        let mut message = ClientMessage::create_for_encode_any_partition(msg_type);
        if let Some(b) = init_bool {
            if let Some(initial) = message.frames_mut().first_mut() {
                initial.content.extend_from_slice(&[u8::from(b)]);
            }
        }
        Self::encode_group_id(&mut message, &group);
        message.add_frame(Self::string_frame(self.object_name()));
        Ok(message)
    }

    /// Gets the current value, or `None` if the reference is null.
    pub async fn get(&self) -> Result<Option<T>> {
        self.check_permission(PermissionAction::Read)?;
        self.check_quorum(true).await?;
        let message = self.cp_message(ATOMIC_REF_GET, None).await?;
        let response = self.invoke(message).await?;
        self.decode_nullable_value(&response)
    }

    /// Sets the value (pass `None` to set the reference to null).
    pub async fn set(&self, value: Option<T>) -> Result<()> {
        self.check_permission(PermissionAction::Put)?;
        self.check_quorum(false).await?;
        let mut message = self.cp_message(ATOMIC_REF_SET, Some(false)).await?;
        message.add_frame(self.nullable_value_frame(value.as_ref())?);
        self.invoke(message).await?;
        Ok(())
    }

    /// Atomically sets the value and returns the old value (`None` if it was null).
    pub async fn get_and_set(&self, value: Option<T>) -> Result<Option<T>> {
        self.check_permission(PermissionAction::Put)?;
        self.check_quorum(false).await?;
        let mut message = self.cp_message(ATOMIC_REF_SET, Some(true)).await?;
        message.add_frame(self.nullable_value_frame(value.as_ref())?);
        let response = self.invoke(message).await?;
        self.decode_nullable_value(&response)
    }

    /// Atomically sets the value to `update` if the current value equals `expected`.
    ///
    /// Returns `true` on success, `false` if the actual value was not `expected`.
    pub async fn compare_and_set(&self, expected: Option<&T>, update: Option<T>) -> Result<bool> {
        self.check_permission(PermissionAction::Put)?;
        self.check_quorum(false).await?;
        let mut message = self.cp_message(ATOMIC_REF_COMPARE_AND_SET, None).await?;
        message.add_frame(self.nullable_value_frame(expected)?);
        message.add_frame(self.nullable_value_frame(update.as_ref())?);
        let response = self.invoke(message).await?;
        Self::decode_bool_response(&response)
    }

    /// Checks whether the current value equals `value`.
    pub async fn contains(&self, value: &T) -> Result<bool> {
        self.check_permission(PermissionAction::Read)?;
        self.check_quorum(true).await?;
        let mut message = self.cp_message(ATOMIC_REF_CONTAINS, None).await?;
        message.add_frame(self.serialize_value(value)?);
        let response = self.invoke(message).await?;
        Self::decode_bool_response(&response)
    }

    /// Checks whether the current value is null.
    ///
    /// The CP protocol has no dedicated `isNull` message; this reads the value and
    /// tests for null.
    pub async fn is_null(&self) -> Result<bool> {
        Ok(self.get().await?.is_none())
    }

    /// Clears the value (sets it to null). Equivalent to `set(None)`.
    pub async fn clear(&self) -> Result<()> {
        self.set(None).await
    }

    fn string_frame(s: &str) -> Frame {
        Frame::with_content(BytesMut::from(s.as_bytes()))
    }

    /// Serializes a value into a (non-null) `Data` frame.
    fn serialize_value(&self, value: &T) -> Result<Frame> {
        let bytes = value.to_bytes()?;
        Ok(Frame::with_content(BytesMut::from(bytes.as_slice())))
    }

    /// Serializes an optional value into a nullable `Data` frame: a real `Data` frame
    /// for `Some`, or a frame flagged `IS_NULL_FLAG` for `None`.
    fn nullable_value_frame(&self, value: Option<&T>) -> Result<Frame> {
        match value {
            Some(v) => self.serialize_value(v),
            None => Ok(Frame::with_flags(IS_NULL_FLAG)),
        }
    }

    async fn invoke(&self, message: ClientMessage) -> Result<ClientMessage> {
        self.connection_manager.invoke_on_random(message).await
    }

    /// Decodes a nullable `Data` response value, which lives in the frame after the
    /// response header frame (`null` if absent or `IS_NULL_FLAG`-flagged).
    fn decode_nullable_value(&self, response: &ClientMessage) -> Result<Option<T>> {
        let frames = response.frames();
        // frames[0] = response header frame; frames[1] = nullable Data value.
        let Some(value_frame) = frames.get(1) else {
            return Ok(None);
        };
        if value_frame.flags & IS_NULL_FLAG != 0 {
            return Ok(None);
        }
        let content = value_frame.content();
        if content.is_empty() {
            return Ok(None);
        }
        Ok(Some(T::from_bytes(content)?))
    }

    fn decode_bool_response(response: &ClientMessage) -> Result<bool> {
        let frames = response.frames();
        let initial_frame = frames
            .first()
            .ok_or_else(|| HazelcastError::Serialization("empty response".to_string()))?;
        if initial_frame.content.len() <= RESPONSE_HEADER_SIZE {
            return Err(HazelcastError::Protocol(
                "atomic reference response too short (expected a bool)".to_string(),
            ));
        }
        Ok(initial_frame.content[RESPONSE_HEADER_SIZE] != 0)
    }
}

impl<T> Clone for AtomicReference<T> {
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
    fn test_atomic_reference_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<AtomicReference<String>>();
    }

    #[test]
    fn test_name_parsing() {
        use crate::config::ClientConfigBuilder;
        let cm = Arc::new(crate::connection::ConnectionManager::from_config(
            ClientConfigBuilder::new().build().unwrap(),
        ));
        let a: AtomicReference<String> = AtomicReference::new("ref".to_string(), Arc::clone(&cm));
        assert_eq!(a.object_name(), "ref");
        assert_eq!(a.group_name(), "default");
        let b: AtomicReference<String> = AtomicReference::new("ref@payments".to_string(), cm);
        assert_eq!(b.object_name(), "ref");
        assert_eq!(b.group_name(), "payments");
    }

    #[test]
    fn test_encode_group_id_framing() {
        let mut msg = ClientMessage::create_for_encode_any_partition(ATOMIC_REF_GET);
        let g = RaftGroupId {
            seed: 9,
            id: 73,
            name: "default".to_string(),
        };
        AtomicReference::<String>::encode_group_id(&mut msg, &g);
        let frames = msg.frames();
        let begin = &frames[frames.len() - 4];
        let fixed = &frames[frames.len() - 3];
        let name = &frames[frames.len() - 2];
        let end = &frames[frames.len() - 1];
        assert!(begin.flags & BEGIN_DATA_STRUCTURE_FLAG != 0);
        assert!(end.flags & END_DATA_STRUCTURE_FLAG != 0);
        assert_eq!(fixed.content.len(), 16);
        assert_eq!(
            i64::from_le_bytes(fixed.content[0..8].try_into().unwrap()),
            9
        );
        assert_eq!(
            i64::from_le_bytes(fixed.content[8..16].try_into().unwrap()),
            73
        );
        assert_eq!(&name.content[..], b"default");
    }

    #[test]
    fn test_null_value_frame_uses_null_flag() {
        use crate::config::ClientConfigBuilder;
        let cm = Arc::new(crate::connection::ConnectionManager::from_config(
            ClientConfigBuilder::new().build().unwrap(),
        ));
        let r: AtomicReference<String> = AtomicReference::new("r".to_string(), cm);
        let null_frame = r.nullable_value_frame(None).unwrap();
        assert!(null_frame.flags & IS_NULL_FLAG != 0);
        let value = "x".to_string();
        let data_frame = r.nullable_value_frame(Some(&value)).unwrap();
        assert!(data_frame.flags & IS_NULL_FLAG == 0);
        assert!(!data_frame.content.is_empty());
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

        let config = ClientConfigBuilder::new().build().unwrap();
        let cm = Arc::new(ConnectionManager::from_config(config));
        let reference: AtomicReference<String> = AtomicReference::new("test".to_string(), cm);

        let original = "hello world".to_string();
        let frame = reference.serialize_value(&original).unwrap();
        assert!(!frame.content.is_empty());

        let deserialized = String::from_bytes(&frame.content).unwrap();
        assert_eq!(deserialized, original);
    }

    #[tokio::test]
    async fn test_atomic_reference_permission_denied_get() {
        use crate::config::{ClientConfigBuilder, PermissionAction, Permissions};
        use crate::connection::ConnectionManager;

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
        use crate::config::{ClientConfigBuilder, PermissionAction, Permissions};
        use crate::connection::ConnectionManager;

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
        let reference: AtomicReference<String> =
            AtomicReference::new("protected-ref".to_string(), cm);

        let result = reference.get().await;
        assert!(matches!(result, Err(HazelcastError::QuorumNotPresent(_))));

        let result = reference.set(Some("value".to_string())).await;
        assert!(matches!(result, Err(HazelcastError::QuorumNotPresent(_))));

        let result = reference.is_null().await;
        assert!(matches!(result, Err(HazelcastError::QuorumNotPresent(_))));

        let result = reference
            .compare_and_set(None, Some("value".to_string()))
            .await;
        assert!(matches!(result, Err(HazelcastError::QuorumNotPresent(_))));
    }

    #[test]
    fn test_atomic_reference_clone() {
        fn assert_clone<T: Clone>() {}
        assert_clone::<AtomicReference<String>>();
    }
}
