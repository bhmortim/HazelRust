//! Distributed countdown latch proxy implementation.

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

/// A distributed countdown latch backed by the CP subsystem.
///
/// `CountDownLatch` is a synchronization primitive that allows one or more tasks
/// to wait until a set of operations being performed by other tasks completes.
///
/// A `CountDownLatch` is initialized with a given count. The `await_latch` method
/// blocks until the current count reaches zero due to invocations of the
/// `count_down` method, after which all waiting tasks are released.
///
/// Unlike `java.util.concurrent.CountDownLatch`, this distributed implementation
/// can be reset by calling `try_set_count` after the count has reached zero.
///
/// The latch uses CP sessions to track waiters. When a CP session expires
/// (e.g., due to client crash), the waiter is automatically removed.
///
/// Note: CountDownLatch requires the CP subsystem to be enabled on the Hazelcast cluster.
#[derive(Debug)]
pub struct CountDownLatch {
    name: String,
    connection_manager: Arc<ConnectionManager>,
    session_manager: Option<Arc<CPSessionManager>>,
    session_id: i64,
    thread_id: i64,
}

impl CountDownLatch {
    /// Creates a new CountDownLatch proxy without session management (legacy mode).
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

    /// Creates a new CountDownLatch proxy with CP session management.
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

    /// Returns the name of this CountDownLatch.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Sets the count to the given value if the current count is zero.
    ///
    /// This method can be used to reset the latch after it has counted down
    /// to zero. If the current count is not zero, this method has no effect
    /// and returns `false`.
    ///
    /// # Arguments
    ///
    /// * `count` - The new count value (must be positive)
    ///
    /// # Returns
    ///
    /// `true` if the count was set, `false` if the current count is not zero.
    ///
    /// # Errors
    ///
    /// Returns an error if a network error occurs or the CP subsystem is unavailable.
    pub async fn try_set_count(&self, count: i32) -> Result<bool> {
        let mut message =
            ClientMessage::create_for_encode_any_partition(CP_COUNTDOWN_LATCH_TRY_SET_COUNT);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::int_frame(count));

        let response = self.invoke(message).await?;
        Self::decode_bool_response(&response)
    }

    /// Decrements the count of the latch, releasing all waiting tasks if the
    /// count reaches zero.
    ///
    /// If the current count is greater than zero, it is decremented. If the
    /// new count is zero, all waiting tasks are released.
    ///
    /// If the current count is already zero, nothing happens.
    ///
    /// # Errors
    ///
    /// Returns an error if a network error occurs or the CP subsystem is unavailable.
    pub async fn count_down(&self) -> Result<()> {
        let mut message =
            ClientMessage::create_for_encode_any_partition(CP_COUNTDOWN_LATCH_COUNT_DOWN);
        self.encode_count_down_request(&mut message);

        let _response = self.invoke(message).await?;
        Ok(())
    }

    /// Returns the current count.
    ///
    /// # Returns
    ///
    /// The current count value.
    ///
    /// # Errors
    ///
    /// Returns an error if a network error occurs or the CP subsystem is unavailable.
    pub async fn get_count(&self) -> Result<i32> {
        let mut message =
            ClientMessage::create_for_encode_any_partition(CP_COUNTDOWN_LATCH_GET_COUNT);
        message.add_frame(Self::string_frame(&self.name));

        let response = self.invoke(message).await?;
        Self::decode_int_response(&response)
    }

    /// Causes the current task to wait until the latch has counted down to zero,
    /// or the specified timeout expires.
    ///
    /// If the current count is zero, this method returns immediately with `true`.
    ///
    /// If the current count is greater than zero, the current task waits until
    /// either:
    /// - The count reaches zero due to invocations of `count_down`, returning `true`
    /// - The specified timeout expires, returning `false`
    ///
    /// If a CP session manager is configured, a session is automatically
    /// acquired before the wait and released afterwards.
    ///
    /// # Arguments
    ///
    /// * `timeout` - The maximum time to wait for the count to reach zero
    ///
    /// # Returns
    ///
    /// `true` if the count reached zero, `false` if the timeout expired.
    ///
    /// # Errors
    ///
    /// Returns an error if a network error occurs or the CP subsystem is unavailable.
    pub async fn await_latch(&mut self, timeout: Duration) -> Result<bool> {
        self.ensure_session().await?;

        let timeout_ms = timeout.as_millis() as i64;
        let mut message = ClientMessage::create_for_encode_any_partition(CP_COUNTDOWN_LATCH_AWAIT);
        self.encode_await_request(&mut message, timeout_ms);

        match self.invoke(message).await {
            Ok(response) => {
                self.release_session().await;
                Self::decode_bool_response(&response)
            }
            Err(e) => {
                self.handle_session_error(&e).await;
                Err(e)
            }
        }
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

    /// Releases the CP session reference.
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

    fn encode_count_down_request(&self, message: &mut ClientMessage) {
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::invocation_uid_frame());
        message.add_frame(Self::int_frame(1));
    }

    fn encode_await_request(&self, message: &mut ClientMessage, timeout_ms: i64) {
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::invocation_uid_frame());
        message.add_frame(Self::long_frame(timeout_ms));
    }

    fn string_frame(s: &str) -> Frame {
        Frame::with_content(BytesMut::from(s.as_bytes()))
    }

    fn long_frame(value: i64) -> Frame {
        let mut buf = BytesMut::with_capacity(8);
        buf.extend_from_slice(&value.to_le_bytes());
        Frame::with_content(buf)
    }

    fn int_frame(value: i32) -> Frame {
        let mut buf = BytesMut::with_capacity(4);
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
        addresses
            .into_iter()
            .next()
            .ok_or_else(|| HazelcastError::Connection("no connections available".to_string()))
    }

    fn decode_bool_response(response: &ClientMessage) -> Result<bool> {
        let frames = response.frames();
        if frames.is_empty() {
            return Err(HazelcastError::Serialization("empty response".to_string()));
        }

        let initial_frame = &frames[0];
        if initial_frame.content.len() >= RESPONSE_HEADER_SIZE + 1 {
            let value = initial_frame.content[RESPONSE_HEADER_SIZE] != 0;
            Ok(value)
        } else {
            Err(HazelcastError::Serialization(
                "invalid bool response".to_string(),
            ))
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
            let value = i32::from_le_bytes([
                initial_frame.content[offset],
                initial_frame.content[offset + 1],
                initial_frame.content[offset + 2],
                initial_frame.content[offset + 3],
            ]);
            Ok(value)
        } else {
            Err(HazelcastError::Serialization(
                "invalid int response".to_string(),
            ))
        }
    }
}

impl Clone for CountDownLatch {
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
    fn test_countdown_latch_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<CountDownLatch>();
    }

    #[test]
    fn test_countdown_latch_clone() {
        fn assert_clone<T: Clone>() {}
        assert_clone::<CountDownLatch>();
    }

    #[test]
    fn test_string_frame() {
        let frame = CountDownLatch::string_frame("test-latch");
        assert_eq!(&frame.content[..], b"test-latch");
    }

    #[test]
    fn test_long_frame() {
        let frame = CountDownLatch::long_frame(5000i64);
        assert_eq!(frame.content.len(), 8);
        assert_eq!(
            i64::from_le_bytes(frame.content[..8].try_into().unwrap()),
            5000i64
        );
    }

    #[test]
    fn test_int_frame() {
        let frame = CountDownLatch::int_frame(3);
        assert_eq!(frame.content.len(), 4);
        assert_eq!(
            i32::from_le_bytes(frame.content[..4].try_into().unwrap()),
            3
        );
    }

    #[test]
    fn test_invocation_uid_frame_unique() {
        let frame1 = CountDownLatch::invocation_uid_frame();
        let frame2 = CountDownLatch::invocation_uid_frame();
        assert_eq!(frame1.content.len(), 16);
        assert_eq!(frame2.content.len(), 16);
        assert_ne!(frame1.content[..], frame2.content[..]);
    }

    #[test]
    fn test_decode_bool_response_true() {
        let mut content = BytesMut::with_capacity(RESPONSE_HEADER_SIZE + 1);
        content.extend_from_slice(&[0u8; RESPONSE_HEADER_SIZE]);
        content.extend_from_slice(&[1u8]);

        let frame = Frame::with_content(content);
        let message = ClientMessage::from_frames(vec![frame]);

        let result = CountDownLatch::decode_bool_response(&message);
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

        let result = CountDownLatch::decode_bool_response(&message);
        assert!(result.is_ok());
        assert!(!result.unwrap());
    }

    #[test]
    fn test_decode_int_response_valid() {
        let mut content = BytesMut::with_capacity(RESPONSE_HEADER_SIZE + 4);
        content.extend_from_slice(&[0u8; RESPONSE_HEADER_SIZE]);
        content.extend_from_slice(&5i32.to_le_bytes());

        let frame = Frame::with_content(content);
        let message = ClientMessage::from_frames(vec![frame]);

        let result = CountDownLatch::decode_int_response(&message);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 5);
    }

    #[test]
    fn test_decode_int_response_empty() {
        let message = ClientMessage::from_frames(vec![]);
        let result = CountDownLatch::decode_int_response(&message);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_int_response_zero() {
        let mut content = BytesMut::with_capacity(RESPONSE_HEADER_SIZE + 4);
        content.extend_from_slice(&[0u8; RESPONSE_HEADER_SIZE]);
        content.extend_from_slice(&0i32.to_le_bytes());

        let frame = Frame::with_content(content);
        let message = ClientMessage::from_frames(vec![frame]);

        let result = CountDownLatch::decode_int_response(&message);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 0);
    }

    #[test]
    fn test_countdown_latch_new_has_no_session() {
        let cm = ConnectionManager::from_config(
            crate::config::ClientConfig::builder()
                .cluster_name("test")
                .build()
                .unwrap(),
        );
        let latch = CountDownLatch::new("test-latch".to_string(), Arc::new(cm));
        assert_eq!(latch.session_id, NO_SESSION_ID);
        assert!(latch.session_manager.is_none());
    }

    #[test]
    fn test_countdown_latch_with_session_manager() {
        let cm = Arc::new(ConnectionManager::from_config(
            crate::config::ClientConfig::builder()
                .cluster_name("test")
                .build()
                .unwrap(),
        ));
        let sm = Arc::new(CPSessionManager::new(Arc::clone(&cm)));
        let latch = CountDownLatch::with_session_manager(
            "test-latch".to_string(),
            cm,
            sm,
        );
        assert_eq!(latch.session_id, NO_SESSION_ID);
        assert!(latch.session_manager.is_some());
    }
}
