//! Distributed atomic long counter proxy implementation.

use std::net::SocketAddr;
use std::sync::Arc;

use bytes::BytesMut;
use hazelcast_core::protocol::constants::*;
use hazelcast_core::protocol::Frame;
use hazelcast_core::{ClientMessage, HazelcastError, Result};

use crate::connection::ConnectionManager;

/// A distributed atomic long counter proxy for performing atomic operations on a Hazelcast CP subsystem.
///
/// `AtomicLong` provides async atomic counter operations with strong consistency guarantees
/// through the CP (Consistent Partition) subsystem.
#[derive(Debug)]
pub struct AtomicLong {
    name: String,
    connection_manager: Arc<ConnectionManager>,
}

impl AtomicLong {
    /// Creates a new atomic long proxy.
    pub(crate) fn new(name: String, connection_manager: Arc<ConnectionManager>) -> Self {
        Self {
            name,
            connection_manager,
        }
    }

    /// Returns the name of this atomic long.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Gets the current value.
    pub async fn get(&self) -> Result<i64> {
        let mut message = ClientMessage::create_for_encode_any_partition(CP_ATOMIC_LONG_GET);
        message.add_frame(Self::string_frame(&self.name));

        let response = self.invoke(message).await?;
        Self::decode_long_response(&response)
    }

    /// Sets the value to the given value.
    pub async fn set(&self, value: i64) -> Result<()> {
        let mut message = ClientMessage::create_for_encode_any_partition(CP_ATOMIC_LONG_SET);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::long_frame(value));

        self.invoke(message).await?;
        Ok(())
    }

    /// Atomically sets the value to the given value and returns the old value.
    pub async fn get_and_set(&self, value: i64) -> Result<i64> {
        let mut message = ClientMessage::create_for_encode_any_partition(CP_ATOMIC_LONG_GET_AND_SET);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::long_frame(value));

        let response = self.invoke(message).await?;
        Self::decode_long_response(&response)
    }

    /// Atomically sets the value to the given updated value if the current value equals the expected value.
    ///
    /// Returns `true` if successful, `false` if the actual value was not equal to the expected value.
    pub async fn compare_and_set(&self, expected: i64, update: i64) -> Result<bool> {
        let mut message = ClientMessage::create_for_encode_any_partition(CP_ATOMIC_LONG_COMPARE_AND_SET);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::long_frame(expected));
        message.add_frame(Self::long_frame(update));

        let response = self.invoke(message).await?;
        Self::decode_bool_response(&response)
    }

    /// Atomically increments the current value by one and returns the updated value.
    pub async fn increment_and_get(&self) -> Result<i64> {
        self.add_and_get(1).await
    }

    /// Atomically decrements the current value by one and returns the updated value.
    pub async fn decrement_and_get(&self) -> Result<i64> {
        self.add_and_get(-1).await
    }

    /// Atomically increments the current value by one and returns the old value.
    pub async fn get_and_increment(&self) -> Result<i64> {
        self.get_and_add(1).await
    }

    /// Atomically decrements the current value by one and returns the old value.
    pub async fn get_and_decrement(&self) -> Result<i64> {
        self.get_and_add(-1).await
    }

    /// Atomically adds the given delta to the current value and returns the updated value.
    pub async fn add_and_get(&self, delta: i64) -> Result<i64> {
        let mut message = ClientMessage::create_for_encode_any_partition(CP_ATOMIC_LONG_ADD_AND_GET);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::long_frame(delta));

        let response = self.invoke(message).await?;
        Self::decode_long_response(&response)
    }

    /// Atomically adds the given delta to the current value and returns the old value.
    pub async fn get_and_add(&self, delta: i64) -> Result<i64> {
        let mut message = ClientMessage::create_for_encode_any_partition(CP_ATOMIC_LONG_GET_AND_ADD);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::long_frame(delta));

        let response = self.invoke(message).await?;
        Self::decode_long_response(&response)
    }

    fn string_frame(s: &str) -> Frame {
        Frame::with_content(BytesMut::from(s.as_bytes()))
    }

    fn long_frame(value: i64) -> Frame {
        let mut buf = BytesMut::with_capacity(8);
        buf.extend_from_slice(&value.to_le_bytes());
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

    fn decode_long_response(response: &ClientMessage) -> Result<i64> {
        let frames = response.frames();
        if frames.is_empty() {
            return Err(HazelcastError::Serialization(
                "empty response".to_string(),
            ));
        }

        let initial_frame = &frames[0];
        if initial_frame.content.len() >= RESPONSE_HEADER_SIZE + 8 {
            let offset = RESPONSE_HEADER_SIZE;
            Ok(i64::from_le_bytes([
                initial_frame.content[offset],
                initial_frame.content[offset + 1],
                initial_frame.content[offset + 2],
                initial_frame.content[offset + 3],
                initial_frame.content[offset + 4],
                initial_frame.content[offset + 5],
                initial_frame.content[offset + 6],
                initial_frame.content[offset + 7],
            ]))
        } else {
            Ok(0)
        }
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

impl Clone for AtomicLong {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            connection_manager: Arc::clone(&self.connection_manager),
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
    fn test_string_frame() {
        let frame = AtomicLong::string_frame("test-counter");
        assert_eq!(&frame.content[..], b"test-counter");
    }

    #[test]
    fn test_long_frame() {
        let frame = AtomicLong::long_frame(42);
        assert_eq!(frame.content.len(), 8);
        assert_eq!(
            i64::from_le_bytes(frame.content[..8].try_into().unwrap()),
            42
        );
    }

    #[test]
    fn test_long_frame_negative() {
        let frame = AtomicLong::long_frame(-100);
        assert_eq!(frame.content.len(), 8);
        assert_eq!(
            i64::from_le_bytes(frame.content[..8].try_into().unwrap()),
            -100
        );
    }

    #[test]
    fn test_long_frame_max_value() {
        let frame = AtomicLong::long_frame(i64::MAX);
        assert_eq!(
            i64::from_le_bytes(frame.content[..8].try_into().unwrap()),
            i64::MAX
        );
    }

    #[test]
    fn test_long_frame_min_value() {
        let frame = AtomicLong::long_frame(i64::MIN);
        assert_eq!(
            i64::from_le_bytes(frame.content[..8].try_into().unwrap()),
            i64::MIN
        );
    }
}
