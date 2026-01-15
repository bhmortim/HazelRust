//! Distributed semaphore proxy implementation.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use hazelcast_core::protocol::constants::*;
use hazelcast_core::protocol::Frame;
use hazelcast_core::{ClientMessage, HazelcastError, Result};

use crate::connection::ConnectionManager;

static INVOCATION_COUNTER: AtomicU64 = AtomicU64::new(1);

/// A distributed semaphore backed by the CP subsystem.
///
/// `Semaphore` provides a distributed counting semaphore that controls access
/// to a shared resource through permits. Unlike Java's `java.util.concurrent.Semaphore`,
/// this implementation is distributed and backed by the Raft consensus protocol.
///
/// Note: Semaphore requires the CP subsystem to be enabled on the Hazelcast cluster.
#[derive(Debug)]
pub struct Semaphore {
    name: String,
    connection_manager: Arc<ConnectionManager>,
    session_id: i64,
    thread_id: i64,
}

impl Semaphore {
    /// Creates a new Semaphore proxy.
    pub(crate) fn new(name: String, connection_manager: Arc<ConnectionManager>) -> Self {
        Self {
            name,
            connection_manager,
            session_id: -1,
            thread_id: std::process::id() as i64,
        }
    }

    /// Returns the name of this Semaphore.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Initializes the semaphore with the given number of permits.
    ///
    /// This method should be called once before using the semaphore.
    /// If the semaphore is already initialized, this method returns `false`.
    ///
    /// # Arguments
    ///
    /// * `permits` - The initial number of permits (must be non-negative)
    ///
    /// # Returns
    ///
    /// `true` if the semaphore was initialized, `false` if it was already initialized.
    ///
    /// # Errors
    ///
    /// Returns an error if a network error occurs or the CP subsystem is unavailable.
    pub async fn init(&self, permits: i32) -> Result<bool> {
        let mut message = ClientMessage::create_for_encode_any_partition(CP_SEMAPHORE_INIT);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::int_frame(permits));

        let response = self.invoke(message).await?;
        Self::decode_bool_response(&response)
    }

    /// Acquires the given number of permits, blocking until they are available.
    ///
    /// # Arguments
    ///
    /// * `permits` - The number of permits to acquire (must be positive)
    ///
    /// # Errors
    ///
    /// Returns an error if a network error occurs or the CP subsystem is unavailable.
    pub async fn acquire(&self, permits: i32) -> Result<()> {
        let mut message = ClientMessage::create_for_encode_any_partition(CP_SEMAPHORE_ACQUIRE);
        self.encode_acquire_request(&mut message, permits, i64::MAX);

        let _response = self.invoke(message).await?;
        Ok(())
    }

    /// Tries to acquire the given number of permits within the specified timeout.
    ///
    /// # Arguments
    ///
    /// * `permits` - The number of permits to acquire (must be positive)
    /// * `timeout` - The maximum time to wait for the permits
    ///
    /// # Returns
    ///
    /// `true` if the permits were acquired, `false` if the timeout expired.
    ///
    /// # Errors
    ///
    /// Returns an error if a network error occurs or the CP subsystem is unavailable.
    pub async fn try_acquire(&self, permits: i32, timeout: Duration) -> Result<bool> {
        let timeout_ms = timeout.as_millis() as i64;
        let mut message = ClientMessage::create_for_encode_any_partition(CP_SEMAPHORE_ACQUIRE);
        self.encode_acquire_request(&mut message, permits, timeout_ms);

        let response = self.invoke(message).await?;
        Self::decode_bool_response(&response)
    }

    /// Releases the given number of permits.
    ///
    /// # Arguments
    ///
    /// * `permits` - The number of permits to release (must be positive)
    ///
    /// # Errors
    ///
    /// Returns an error if a network error occurs or the CP subsystem is unavailable.
    pub async fn release(&self, permits: i32) -> Result<()> {
        let mut message = ClientMessage::create_for_encode_any_partition(CP_SEMAPHORE_RELEASE);
        self.encode_release_request(&mut message, permits);

        let _response = self.invoke(message).await?;
        Ok(())
    }

    /// Returns the current number of available permits.
    ///
    /// # Returns
    ///
    /// The number of permits currently available.
    ///
    /// # Errors
    ///
    /// Returns an error if a network error occurs or the CP subsystem is unavailable.
    pub async fn available_permits(&self) -> Result<i32> {
        let mut message =
            ClientMessage::create_for_encode_any_partition(CP_SEMAPHORE_AVAILABLE_PERMITS);
        message.add_frame(Self::string_frame(&self.name));

        let response = self.invoke(message).await?;
        Self::decode_int_response(&response)
    }

    /// Acquires and returns all permits that are immediately available.
    ///
    /// # Returns
    ///
    /// The number of permits acquired.
    ///
    /// # Errors
    ///
    /// Returns an error if a network error occurs or the CP subsystem is unavailable.
    pub async fn drain_permits(&self) -> Result<i32> {
        let mut message = ClientMessage::create_for_encode_any_partition(CP_SEMAPHORE_DRAIN);
        self.encode_drain_request(&mut message);

        let response = self.invoke(message).await?;
        Self::decode_int_response(&response)
    }

    /// Reduces the number of available permits by the indicated reduction.
    ///
    /// This method differs from `acquire` in that it does not block waiting for
    /// permits to become available.
    ///
    /// # Arguments
    ///
    /// * `reduction` - The number of permits to reduce (must be non-negative)
    ///
    /// # Errors
    ///
    /// Returns an error if a network error occurs or the CP subsystem is unavailable.
    pub async fn reduce_permits(&self, reduction: i32) -> Result<()> {
        let mut message = ClientMessage::create_for_encode_any_partition(CP_SEMAPHORE_CHANGE);
        self.encode_change_request(&mut message, -reduction);

        let _response = self.invoke(message).await?;
        Ok(())
    }

    /// Increases the number of available permits by the indicated amount.
    ///
    /// # Arguments
    ///
    /// * `increase` - The number of permits to add (must be non-negative)
    ///
    /// # Errors
    ///
    /// Returns an error if a network error occurs or the CP subsystem is unavailable.
    pub async fn increase_permits(&self, increase: i32) -> Result<()> {
        let mut message = ClientMessage::create_for_encode_any_partition(CP_SEMAPHORE_CHANGE);
        self.encode_change_request(&mut message, increase);

        let _response = self.invoke(message).await?;
        Ok(())
    }

    fn encode_acquire_request(&self, message: &mut ClientMessage, permits: i32, timeout_ms: i64) {
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::long_frame(self.session_id));
        message.add_frame(Self::long_frame(self.thread_id));
        message.add_frame(Self::invocation_uid_frame());
        message.add_frame(Self::int_frame(permits));
        message.add_frame(Self::long_frame(timeout_ms));
    }

    fn encode_release_request(&self, message: &mut ClientMessage, permits: i32) {
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::long_frame(self.session_id));
        message.add_frame(Self::long_frame(self.thread_id));
        message.add_frame(Self::invocation_uid_frame());
        message.add_frame(Self::int_frame(permits));
    }

    fn encode_drain_request(&self, message: &mut ClientMessage) {
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::long_frame(self.session_id));
        message.add_frame(Self::long_frame(self.thread_id));
        message.add_frame(Self::invocation_uid_frame());
    }

    fn encode_change_request(&self, message: &mut ClientMessage, delta: i32) {
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::long_frame(self.session_id));
        message.add_frame(Self::long_frame(self.thread_id));
        message.add_frame(Self::invocation_uid_frame());
        message.add_frame(Self::int_frame(delta));
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

impl Clone for Semaphore {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            connection_manager: Arc::clone(&self.connection_manager),
            session_id: self.session_id,
            thread_id: self.thread_id,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_semaphore_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Semaphore>();
    }

    #[test]
    fn test_semaphore_clone() {
        fn assert_clone<T: Clone>() {}
        assert_clone::<Semaphore>();
    }

    #[test]
    fn test_string_frame() {
        let frame = Semaphore::string_frame("test-semaphore");
        assert_eq!(&frame.content[..], b"test-semaphore");
    }

    #[test]
    fn test_long_frame() {
        let frame = Semaphore::long_frame(12345678901234i64);
        assert_eq!(frame.content.len(), 8);
        assert_eq!(
            i64::from_le_bytes(frame.content[..8].try_into().unwrap()),
            12345678901234i64
        );
    }

    #[test]
    fn test_int_frame() {
        let frame = Semaphore::int_frame(42);
        assert_eq!(frame.content.len(), 4);
        assert_eq!(
            i32::from_le_bytes(frame.content[..4].try_into().unwrap()),
            42
        );
    }

    #[test]
    fn test_invocation_uid_frame_unique() {
        let frame1 = Semaphore::invocation_uid_frame();
        let frame2 = Semaphore::invocation_uid_frame();
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

        let result = Semaphore::decode_bool_response(&message);
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

        let result = Semaphore::decode_bool_response(&message);
        assert!(result.is_ok());
        assert!(!result.unwrap());
    }

    #[test]
    fn test_decode_int_response_valid() {
        let mut content = BytesMut::with_capacity(RESPONSE_HEADER_SIZE + 4);
        content.extend_from_slice(&[0u8; RESPONSE_HEADER_SIZE]);
        content.extend_from_slice(&10i32.to_le_bytes());

        let frame = Frame::with_content(content);
        let message = ClientMessage::from_frames(vec![frame]);

        let result = Semaphore::decode_int_response(&message);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 10);
    }

    #[test]
    fn test_decode_int_response_empty() {
        let message = ClientMessage::from_frames(vec![]);
        let result = Semaphore::decode_int_response(&message);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_int_response_negative() {
        let mut content = BytesMut::with_capacity(RESPONSE_HEADER_SIZE + 4);
        content.extend_from_slice(&[0u8; RESPONSE_HEADER_SIZE]);
        content.extend_from_slice(&(-5i32).to_le_bytes());

        let frame = Frame::with_content(content);
        let message = ClientMessage::from_frames(vec![frame]);

        let result = Semaphore::decode_int_response(&message);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), -5);
    }
}
