//! Distributed atomic long counter proxy implementation.
//!
//! CP data structures require the Raft `groupId` to be sent with every request,
//! encoded as a data structure (`BEGIN` / `[seed,id]` / name / `END`), and fixed
//! parameters (the `long` values) to live in the request's initial frame after the
//! header — per the Hazelcast Open Binary Protocol. See issue #12.

use std::sync::Arc;

use bytes::BytesMut;
use hazelcast_core::protocol::constants::*;
use hazelcast_core::protocol::Frame;
use hazelcast_core::{ClientMessage, HazelcastError, Result};
use tokio::sync::OnceCell;

use crate::config::PermissionAction;
use crate::connection::ConnectionManager;

// Correct Hazelcast AtomicLong message types. The hazelcast_core CP_ATOMIC_LONG_*
// constants are mislabeled (..._GET=0x090100 is actually Apply; ..._ADD_AND_GET=0x090500
// is actually Get), which is why mutating ops silently no-op'd. Verified against the
// Hazelcast client protocol (issue #12). AtomicLong has no plain Set; set() uses GetAndSet.
const ATOMIC_LONG_ADD_AND_GET: i32 = 0x090300;
const ATOMIC_LONG_COMPARE_AND_SET: i32 = 0x090400;
const ATOMIC_LONG_GET: i32 = 0x090500;
const ATOMIC_LONG_GET_AND_ADD: i32 = 0x090600;
const ATOMIC_LONG_GET_AND_SET: i32 = 0x090700;

/// A resolved CP Raft group identifier (`seed`, `id`, `name`).
#[derive(Debug, Clone)]
struct RaftGroupId {
    seed: i64,
    id: i64,
    name: String,
}

/// A distributed atomic long counter backed by the Hazelcast CP (Raft) subsystem.
///
/// Provides linearizable atomic counter operations with strong consistency.
#[derive(Debug)]
pub struct AtomicLong {
    /// Full proxy name (may include a `@group` suffix).
    name: String,
    connection_manager: Arc<ConnectionManager>,
    /// Lazily-resolved Raft group id, shared across clones.
    group_id: Arc<OnceCell<RaftGroupId>>,
}

impl AtomicLong {
    /// Creates a new atomic long proxy.
    pub(crate) fn new(name: String, connection_manager: Arc<ConnectionManager>) -> Self {
        Self {
            name,
            connection_manager,
            group_id: Arc::new(OnceCell::new()),
        }
    }

    /// Returns the name of this atomic long.
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
        if !self.connection_manager.is_permitted(action) {
            return Err(HazelcastError::Authorization(format!(
                "atomic long '{}' operation denied: requires {:?} permission",
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

    /// Resolves (and caches) the Raft `groupId` for this object's CP group.
    ///
    /// Uses `CPGroupCreateCPGroup` (0x1E0100), which creates the group if needed
    /// and returns its `RaftGroupId`.
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

    /// Decodes a `RaftGroupId` data structure from a response:
    /// `[response-initial] BEGIN [seed(8) id(8)] name END`.
    fn decode_group_id(response: &ClientMessage) -> Result<RaftGroupId> {
        let frames = response.frames();
        // frames[0] = response initial frame; [1] = BEGIN; [2] = seed+id; [3] = name; [4] = END
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

    /// Encodes a `RaftGroupId` into a request as a data structure:
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

    /// Builds a CP request: initial frame (header + the fixed `long` params), then
    /// the `groupId` data structure, then the object name.
    async fn build_request(&self, msg_type: i32, fixed_longs: &[i64]) -> Result<ClientMessage> {
        // Borrow the cached group (never changes) instead of cloning its String.
        let group = self.resolve_group().await?;
        let mut message = ClientMessage::create_for_encode_any_partition(msg_type);
        // Append fixed params to the initial frame (after the request header).
        if let Some(initial) = message.frames_mut().first_mut() {
            for v in fixed_longs {
                initial.content.extend_from_slice(&v.to_le_bytes());
            }
        }
        Self::encode_group_id(&mut message, group);
        message.add_frame(Self::string_frame(self.object_name()));
        Ok(message)
    }

    /// Gets the current value.
    pub async fn get(&self) -> Result<i64> {
        self.check_permission(PermissionAction::Read)?;
        self.check_quorum(true).await?;
        let message = self.build_request(ATOMIC_LONG_GET, &[]).await?;
        let response = self.invoke(message).await?;
        Self::decode_long_response(&response)
    }

    /// Sets the value to the given value.
    pub async fn set(&self, value: i64) -> Result<()> {
        self.check_permission(PermissionAction::Put)?;
        self.check_quorum(false).await?;
        let message = self
            .build_request(ATOMIC_LONG_GET_AND_SET, &[value])
            .await?;
        self.invoke(message).await?;
        Ok(())
    }

    /// Atomically sets the value and returns the old value.
    pub async fn get_and_set(&self, value: i64) -> Result<i64> {
        self.check_permission(PermissionAction::Put)?;
        self.check_quorum(false).await?;
        let message = self
            .build_request(ATOMIC_LONG_GET_AND_SET, &[value])
            .await?;
        let response = self.invoke(message).await?;
        Self::decode_long_response(&response)
    }

    /// Atomically sets the value to `update` if the current value equals `expected`.
    ///
    /// Returns `true` on success, `false` if the actual value was not `expected`.
    pub async fn compare_and_set(&self, expected: i64, update: i64) -> Result<bool> {
        self.check_permission(PermissionAction::Put)?;
        self.check_quorum(false).await?;
        let message = self
            .build_request(ATOMIC_LONG_COMPARE_AND_SET, &[expected, update])
            .await?;
        let response = self.invoke(message).await?;
        Self::decode_bool_response(&response)
    }

    /// Atomically increments the value by one and returns the updated value.
    pub async fn increment_and_get(&self) -> Result<i64> {
        self.add_and_get(1).await
    }

    /// Atomically decrements the value by one and returns the updated value.
    pub async fn decrement_and_get(&self) -> Result<i64> {
        self.add_and_get(-1).await
    }

    /// Atomically increments the value by one and returns the old value.
    pub async fn get_and_increment(&self) -> Result<i64> {
        self.get_and_add(1).await
    }

    /// Atomically decrements the value by one and returns the old value.
    pub async fn get_and_decrement(&self) -> Result<i64> {
        self.get_and_add(-1).await
    }

    /// Atomically adds `delta` and returns the updated value.
    pub async fn add_and_get(&self, delta: i64) -> Result<i64> {
        self.check_permission(PermissionAction::Put)?;
        self.check_quorum(false).await?;
        let message = self
            .build_request(ATOMIC_LONG_ADD_AND_GET, &[delta])
            .await?;
        let response = self.invoke(message).await?;
        Self::decode_long_response(&response)
    }

    /// Atomically adds `delta` and returns the old value.
    pub async fn get_and_add(&self, delta: i64) -> Result<i64> {
        self.check_permission(PermissionAction::Put)?;
        self.check_quorum(false).await?;
        let message = self
            .build_request(ATOMIC_LONG_GET_AND_ADD, &[delta])
            .await?;
        let response = self.invoke(message).await?;
        Self::decode_long_response(&response)
    }

    fn string_frame(s: &str) -> Frame {
        Frame::with_content(BytesMut::from(s.as_bytes()))
    }

    async fn invoke(&self, message: ClientMessage) -> Result<ClientMessage> {
        self.connection_manager.invoke_on_random(message).await
    }

    fn decode_long_response(response: &ClientMessage) -> Result<i64> {
        let frames = response.frames();
        let initial_frame = frames
            .first()
            .ok_or_else(|| HazelcastError::Serialization("empty response".to_string()))?;
        if initial_frame.content.len() < RESPONSE_HEADER_SIZE + 8 {
            return Err(HazelcastError::Protocol(
                "atomic long response too short (expected an i64)".to_string(),
            ));
        }
        let offset = RESPONSE_HEADER_SIZE;
        Ok(i64::from_le_bytes(
            initial_frame.content[offset..offset + 8]
                .try_into()
                .unwrap(),
        ))
    }

    fn decode_bool_response(response: &ClientMessage) -> Result<bool> {
        let frames = response.frames();
        let initial_frame = frames
            .first()
            .ok_or_else(|| HazelcastError::Serialization("empty response".to_string()))?;
        if initial_frame.content.len() <= RESPONSE_HEADER_SIZE {
            return Err(HazelcastError::Protocol(
                "atomic long response too short (expected a bool)".to_string(),
            ));
        }
        Ok(initial_frame.content[RESPONSE_HEADER_SIZE] != 0)
    }
}

impl Clone for AtomicLong {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            connection_manager: Arc::clone(&self.connection_manager),
            group_id: Arc::clone(&self.group_id),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_atomic_long_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<AtomicLong>();
    }

    #[test]
    fn test_atomic_long_clone() {
        fn assert_clone<T: Clone>() {}
        assert_clone::<AtomicLong>();
    }

    #[test]
    fn test_name_parsing() {
        let cm = Arc::new(crate::connection::ConnectionManager::from_config(
            crate::config::ClientConfigBuilder::new().build().unwrap(),
        ));
        let a = AtomicLong::new("counter".to_string(), Arc::clone(&cm));
        assert_eq!(a.object_name(), "counter");
        assert_eq!(a.group_name(), "default");
        let b = AtomicLong::new("counter@payments".to_string(), cm);
        assert_eq!(b.object_name(), "counter");
        assert_eq!(b.group_name(), "payments");
    }

    #[test]
    fn test_string_frame() {
        let frame = AtomicLong::string_frame("test-counter");
        assert_eq!(&frame.content[..], b"test-counter");
    }

    #[test]
    fn test_encode_group_id_framing() {
        let mut msg = ClientMessage::create_for_encode_any_partition(ATOMIC_LONG_GET);
        let g = RaftGroupId {
            seed: 7,
            id: 42,
            name: "default".to_string(),
        };
        AtomicLong::encode_group_id(&mut msg, &g);
        let frames = msg.frames();
        // initial, BEGIN, [seed+id], name, END
        let begin = &frames[frames.len() - 4];
        let fixed = &frames[frames.len() - 3];
        let name = &frames[frames.len() - 2];
        let end = &frames[frames.len() - 1];
        assert!(begin.flags & BEGIN_DATA_STRUCTURE_FLAG != 0);
        assert!(end.flags & END_DATA_STRUCTURE_FLAG != 0);
        assert_eq!(fixed.content.len(), 16);
        assert_eq!(
            i64::from_le_bytes(fixed.content[0..8].try_into().unwrap()),
            7
        );
        assert_eq!(
            i64::from_le_bytes(fixed.content[8..16].try_into().unwrap()),
            42
        );
        assert_eq!(&name.content[..], b"default");
    }

    #[tokio::test]
    async fn test_atomic_long_permission_denied_get() {
        use crate::config::{ClientConfigBuilder, PermissionAction, Permissions};
        let mut perms = Permissions::new();
        perms.grant(PermissionAction::Put);
        let config = ClientConfigBuilder::new()
            .security(|s| s.permissions(perms))
            .build()
            .unwrap();
        let cm = Arc::new(ConnectionManager::from_config(config));
        let counter = AtomicLong::new("test".to_string(), cm);
        let result = counter.get().await;
        assert!(matches!(result, Err(HazelcastError::Authorization(_))));
    }
}
