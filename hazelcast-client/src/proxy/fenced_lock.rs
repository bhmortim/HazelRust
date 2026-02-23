//! Distributed fenced lock proxy implementation.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use hazelcast_core::protocol::constants::*;
use hazelcast_core::protocol::Frame;
use hazelcast_core::{ClientMessage, HazelcastError, Result};

use crate::cluster::{CPSessionManager, NO_SESSION_ID};
use crate::connection::ConnectionManager;

static INVOCATION_COUNTER: AtomicU64 = AtomicU64::new(1);

/// A distributed fenced lock backed by the CP subsystem.
///
/// `FencedLock` provides a distributed mutual exclusion primitive with fencing tokens.
/// Each successful lock acquisition returns a monotonically increasing fence token
/// that can be used to detect stale lock holders.
///
/// Unlike traditional locks, FencedLock returns a fence token on lock acquisition.
/// This token should be passed to protected resources to prevent stale operations
/// from lock holders that have been preempted.
///
/// The lock uses CP sessions to track ownership. When a CP session expires
/// (e.g., due to client crash), the lock is automatically released, preventing
/// deadlocks from failed clients.
///
/// Note: FencedLock requires the CP subsystem to be enabled on the Hazelcast cluster.
#[derive(Debug)]
pub struct FencedLock {
    name: String,
    connection_manager: Arc<ConnectionManager>,
    session_manager: Option<Arc<CPSessionManager>>,
    session_id: i64,
    thread_id: i64,
}

impl FencedLock {
    /// Creates a new FencedLock proxy without session management (legacy mode).
    #[allow(dead_code)]
    pub(crate) fn new(name: String, connection_manager: Arc<ConnectionManager>) -> Self {
        Self {
            name,
            connection_manager,
            session_manager: None,
            session_id: NO_SESSION_ID,
            thread_id: std::process::id() as i64,
        }
    }

    /// Creates a new FencedLock proxy with CP session management.
    pub(crate) fn with_session_manager(
        name: String,
        connection_manager: Arc<ConnectionManager>,
        session_manager: Arc<CPSessionManager>,
    ) -> Self {
        Self {
            name,
            connection_manager,
            session_manager: Some(session_manager),
            session_id: NO_SESSION_ID,
            thread_id: std::process::id() as i64,
        }
    }

    /// Returns the name of this FencedLock.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Acquires the lock and returns a fence token.
    ///
    /// This method blocks until the lock is acquired. The returned fence token
    /// is monotonically increasing and can be used to detect stale lock holders.
    ///
    /// If a CP session manager is configured, a session is automatically
    /// acquired before the lock request and released on failure.
    ///
    /// # Returns
    ///
    /// The fence token associated with this lock acquisition.
    ///
    /// # Errors
    ///
    /// Returns an error if a network error occurs or the CP subsystem is unavailable.
    pub async fn lock(&mut self) -> Result<i64> {
        self.ensure_session().await?;

        let mut message = ClientMessage::create_for_encode_any_partition(CP_FENCED_LOCK_LOCK);
        self.encode_lock_request(&mut message, i64::MAX);

        match self.invoke(message).await {
            Ok(response) => Self::decode_fence_response(&response),
            Err(e) => {
                self.handle_session_error(&e).await;
                Err(e)
            }
        }
    }

    /// Tries to acquire the lock within the given timeout.
    ///
    /// # Arguments
    ///
    /// * `timeout` - The maximum time to wait for the lock
    ///
    /// # Returns
    ///
    /// - `Ok(Some(fence))` if the lock was acquired within the timeout
    /// - `Ok(None)` if the timeout expired before acquiring the lock
    ///
    /// # Errors
    ///
    /// Returns an error if a network error occurs or the CP subsystem is unavailable.
    pub async fn try_lock(&mut self, timeout: Duration) -> Result<Option<i64>> {
        self.ensure_session().await?;

        let timeout_ms = timeout.as_millis() as i64;
        let mut message = ClientMessage::create_for_encode_any_partition(CP_FENCED_LOCK_TRY_LOCK);
        self.encode_lock_request(&mut message, timeout_ms);

        match self.invoke(message).await {
            Ok(response) => Self::decode_try_lock_response(&response),
            Err(e) => {
                self.handle_session_error(&e).await;
                Err(e)
            }
        }
    }

    /// Releases the lock with the given fence token.
    ///
    /// # Arguments
    ///
    /// * `fence` - The fence token returned from `lock()` or `try_lock()`
    ///
    /// # Errors
    ///
    /// Returns an error if the fence token is invalid, the lock is not held,
    /// or a network error occurs.
    pub async fn unlock(&mut self, fence: i64) -> Result<()> {
        let mut message = ClientMessage::create_for_encode_any_partition(CP_FENCED_LOCK_UNLOCK);
        self.encode_unlock_request(&mut message, fence);

        match self.invoke(message).await {
            Ok(_response) => {
                self.release_session().await;
                Ok(())
            }
            Err(e) => {
                self.handle_session_error(&e).await;
                Err(e)
            }
        }
    }

    /// Returns whether the lock is currently held by any thread.
    ///
    /// # Returns
    ///
    /// `true` if the lock is held, `false` otherwise.
    pub async fn is_locked(&self) -> Result<bool> {
        let state = self.get_lock_ownership_state().await?;
        Ok(state.fence > 0)
    }

    /// Returns the reentrant lock count.
    ///
    /// If the lock is held, this returns the number of times `lock()` has been
    /// called without a corresponding `unlock()`. Returns 0 if not locked.
    pub async fn get_lock_count(&self) -> Result<i32> {
        let state = self.get_lock_ownership_state().await?;
        Ok(state.lock_count)
    }

    /// Returns the current fence token, or 0 if the lock is not held.
    ///
    /// The fence token can be used to detect stale lock holders in distributed
    /// systems where a client may have lost the lock due to session expiration.
    pub async fn get_fence(&self) -> Result<i64> {
        let state = self.get_lock_ownership_state().await?;
        Ok(state.fence)
    }

    async fn get_lock_ownership_state(&self) -> Result<LockOwnershipState> {
        let mut message = ClientMessage::create_for_encode_any_partition(
            CP_FENCED_LOCK_GET_LOCK_OWNERSHIP_STATE,
        );
        message.add_frame(Self::string_frame(&self.name));

        let response = self.invoke(message).await?;
        Self::decode_lock_ownership_state(&response)
    }

    // ── Session management helpers ──────────────────────────────────

    /// Ensures a CP session is active. If no session exists, acquires one.
    async fn ensure_session(&mut self) -> Result<()> {
        if let Some(ref sm) = self.session_manager {
            if self.session_id == NO_SESSION_ID {
                let group_id =
                    crate::cluster::CPGroupId::new(&self.name, 0, 0);
                self.session_id = sm.acquire_session(&group_id).await?;
            }
        }
        Ok(())
    }

    /// Releases the CP session reference after a successful unlock.
    async fn release_session(&mut self) {
        if let Some(ref sm) = self.session_manager {
            if self.session_id != NO_SESSION_ID {
                let group_id =
                    crate::cluster::CPGroupId::new(&self.name, 0, 0);
                sm.release_session(&group_id, self.session_id).await;
            }
        }
    }

    /// Handles session-related errors by invalidating the session if expired.
    async fn handle_session_error(&mut self, error: &HazelcastError) {
        if let Some(ref sm) = self.session_manager {
            if self.session_id != NO_SESSION_ID {
                if let HazelcastError::Server { code, .. } = error {
                    if code.value() == 76 {
                        // CpSessionNotFound
                        let group_id =
                            crate::cluster::CPGroupId::new(&self.name, 0, 0);
                        sm.invalidate_session(&group_id, self.session_id).await;
                        self.session_id = NO_SESSION_ID;
                    }
                }
            }
        }
    }

    // ── Encoding helpers ────────────────────────────────────────────

    fn encode_lock_request(&self, message: &mut ClientMessage, timeout_ms: i64) {
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::long_frame(self.session_id));
        message.add_frame(Self::long_frame(self.thread_id));
        message.add_frame(Self::invocation_uid_frame());
        message.add_frame(Self::long_frame(timeout_ms));
    }

    fn encode_unlock_request(&self, message: &mut ClientMessage, fence: i64) {
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::long_frame(self.session_id));
        message.add_frame(Self::long_frame(self.thread_id));
        message.add_frame(Self::invocation_uid_frame());
        message.add_frame(Self::long_frame(fence));
    }

    fn string_frame(s: &str) -> Frame {
        Frame::with_content(BytesMut::from(s.as_bytes()))
    }

    fn long_frame(value: i64) -> Frame {
        let mut buf = BytesMut::with_capacity(8);
        buf.extend_from_slice(&value.to_le_bytes());
        Frame::with_content(buf)
    }

    fn invocation_uid_frame() -> Frame {
        let counter = INVOCATION_COUNTER.fetch_add(1, Ordering::Relaxed);
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);

        let mut buf = BytesMut::with_capacity(16);
        buf.extend_from_slice(&timestamp.to_le_bytes());
        buf.extend_from_slice(&counter.to_le_bytes());
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

    fn decode_fence_response(response: &ClientMessage) -> Result<i64> {
        let frames = response.frames();
        if frames.is_empty() {
            return Err(HazelcastError::Serialization("empty response".to_string()));
        }

        let initial_frame = &frames[0];
        if initial_frame.content.len() >= RESPONSE_HEADER_SIZE + 8 {
            let offset = RESPONSE_HEADER_SIZE;
            let fence = i64::from_le_bytes([
                initial_frame.content[offset],
                initial_frame.content[offset + 1],
                initial_frame.content[offset + 2],
                initial_frame.content[offset + 3],
                initial_frame.content[offset + 4],
                initial_frame.content[offset + 5],
                initial_frame.content[offset + 6],
                initial_frame.content[offset + 7],
            ]);
            Ok(fence)
        } else {
            Err(HazelcastError::Serialization(
                "invalid fence response".to_string(),
            ))
        }
    }

    fn decode_try_lock_response(response: &ClientMessage) -> Result<Option<i64>> {
        let frames = response.frames();
        if frames.is_empty() {
            return Err(HazelcastError::Serialization("empty response".to_string()));
        }

        let initial_frame = &frames[0];
        if initial_frame.content.len() >= RESPONSE_HEADER_SIZE + 8 {
            let offset = RESPONSE_HEADER_SIZE;
            let fence = i64::from_le_bytes([
                initial_frame.content[offset],
                initial_frame.content[offset + 1],
                initial_frame.content[offset + 2],
                initial_frame.content[offset + 3],
                initial_frame.content[offset + 4],
                initial_frame.content[offset + 5],
                initial_frame.content[offset + 6],
                initial_frame.content[offset + 7],
            ]);
            if fence <= 0 {
                Ok(None)
            } else {
                Ok(Some(fence))
            }
        } else {
            Err(HazelcastError::Serialization(
                "invalid try_lock response".to_string(),
            ))
        }
    }

    fn decode_lock_ownership_state(response: &ClientMessage) -> Result<LockOwnershipState> {
        let frames = response.frames();
        if frames.is_empty() {
            return Err(HazelcastError::Serialization("empty response".to_string()));
        }

        let initial_frame = &frames[0];
        if initial_frame.content.len() >= RESPONSE_HEADER_SIZE + 12 {
            let offset = RESPONSE_HEADER_SIZE;
            let fence = i64::from_le_bytes([
                initial_frame.content[offset],
                initial_frame.content[offset + 1],
                initial_frame.content[offset + 2],
                initial_frame.content[offset + 3],
                initial_frame.content[offset + 4],
                initial_frame.content[offset + 5],
                initial_frame.content[offset + 6],
                initial_frame.content[offset + 7],
            ]);
            let lock_count = i32::from_le_bytes([
                initial_frame.content[offset + 8],
                initial_frame.content[offset + 9],
                initial_frame.content[offset + 10],
                initial_frame.content[offset + 11],
            ]);
            Ok(LockOwnershipState { fence, lock_count })
        } else {
            Err(HazelcastError::Serialization(
                "invalid lock ownership state response".to_string(),
            ))
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct LockOwnershipState {
    fence: i64,
    lock_count: i32,
}

impl Clone for FencedLock {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            connection_manager: Arc::clone(&self.connection_manager),
            session_manager: self.session_manager.clone(),
            session_id: self.session_id,
            thread_id: self.thread_id,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fenced_lock_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<FencedLock>();
    }

    #[test]
    fn test_fenced_lock_clone() {
        fn assert_clone<T: Clone>() {}
        assert_clone::<FencedLock>();
    }

    #[test]
    fn test_string_frame() {
        let frame = FencedLock::string_frame("test-lock");
        assert_eq!(&frame.content[..], b"test-lock");
    }

    #[test]
    fn test_long_frame() {
        let frame = FencedLock::long_frame(12345678901234i64);
        assert_eq!(frame.content.len(), 8);
        assert_eq!(
            i64::from_le_bytes(frame.content[..8].try_into().unwrap()),
            12345678901234i64
        );
    }

    #[test]
    fn test_invocation_uid_frame_unique() {
        let frame1 = FencedLock::invocation_uid_frame();
        let frame2 = FencedLock::invocation_uid_frame();
        assert_eq!(frame1.content.len(), 16);
        assert_eq!(frame2.content.len(), 16);
        assert_ne!(frame1.content[..], frame2.content[..]);
    }

    #[test]
    fn test_lock_ownership_state() {
        let state = LockOwnershipState {
            fence: 42,
            lock_count: 3,
        };
        assert_eq!(state.fence, 42);
        assert_eq!(state.lock_count, 3);
    }

    #[test]
    fn test_decode_fence_response_valid() {
        let mut content = BytesMut::with_capacity(RESPONSE_HEADER_SIZE + 8);
        content.extend_from_slice(&[0u8; RESPONSE_HEADER_SIZE]);
        content.extend_from_slice(&42i64.to_le_bytes());

        let frame = Frame::with_content(content);
        let message = ClientMessage::from_frames(vec![frame]);

        let result = FencedLock::decode_fence_response(&message);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);
    }

    #[test]
    fn test_decode_fence_response_empty() {
        let message = ClientMessage::from_frames(vec![]);
        let result = FencedLock::decode_fence_response(&message);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_try_lock_response_acquired() {
        let mut content = BytesMut::with_capacity(RESPONSE_HEADER_SIZE + 8);
        content.extend_from_slice(&[0u8; RESPONSE_HEADER_SIZE]);
        content.extend_from_slice(&100i64.to_le_bytes());

        let frame = Frame::with_content(content);
        let message = ClientMessage::from_frames(vec![frame]);

        let result = FencedLock::decode_try_lock_response(&message);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Some(100));
    }

    #[test]
    fn test_decode_try_lock_response_not_acquired() {
        let mut content = BytesMut::with_capacity(RESPONSE_HEADER_SIZE + 8);
        content.extend_from_slice(&[0u8; RESPONSE_HEADER_SIZE]);
        content.extend_from_slice(&0i64.to_le_bytes());

        let frame = Frame::with_content(content);
        let message = ClientMessage::from_frames(vec![frame]);

        let result = FencedLock::decode_try_lock_response(&message);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);
    }

    #[test]
    fn test_decode_lock_ownership_state_valid() {
        let mut content = BytesMut::with_capacity(RESPONSE_HEADER_SIZE + 12);
        content.extend_from_slice(&[0u8; RESPONSE_HEADER_SIZE]);
        content.extend_from_slice(&50i64.to_le_bytes());
        content.extend_from_slice(&2i32.to_le_bytes());

        let frame = Frame::with_content(content);
        let message = ClientMessage::from_frames(vec![frame]);

        let result = FencedLock::decode_lock_ownership_state(&message);
        assert!(result.is_ok());
        let state = result.unwrap();
        assert_eq!(state.fence, 50);
        assert_eq!(state.lock_count, 2);
    }

    #[test]
    fn test_fenced_lock_new_has_no_session() {
        let cm = ConnectionManager::from_config(
            crate::config::ClientConfig::builder()
                .cluster_name("test")
                .build()
                .unwrap(),
        );
        let lock = FencedLock::new("test-lock".to_string(), Arc::new(cm));
        assert_eq!(lock.session_id, NO_SESSION_ID);
        assert!(lock.session_manager.is_none());
    }

    #[test]
    fn test_fenced_lock_with_session_manager() {
        let cm = Arc::new(ConnectionManager::from_config(
            crate::config::ClientConfig::builder()
                .cluster_name("test")
                .build()
                .unwrap(),
        ));
        let sm = Arc::new(CPSessionManager::new(Arc::clone(&cm)));
        let lock = FencedLock::with_session_manager(
            "test-lock".to_string(),
            cm,
            sm,
        );
        assert_eq!(lock.session_id, NO_SESSION_ID);
        assert!(lock.session_manager.is_some());
    }
}
