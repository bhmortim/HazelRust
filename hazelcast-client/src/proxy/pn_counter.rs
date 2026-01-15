//! Distributed PN (Positive-Negative) Counter proxy implementation.

use std::net::SocketAddr;
use std::sync::Arc;

use bytes::BytesMut;
use hazelcast_core::protocol::constants::*;
use hazelcast_core::protocol::Frame;
use hazelcast_core::{ClientMessage, HazelcastError, Result};

use crate::connection::ConnectionManager;

/// A distributed PN Counter (Positive-Negative Counter) proxy for performing conflict-free
/// counter operations on a Hazelcast cluster.
///
/// `PNCounter` is a CRDT (Conflict-free Replicated Data Type) that allows both increment
/// and decrement operations. It maintains eventual consistency across cluster members
/// without coordination, making it suitable for high-availability scenarios where
/// strong consistency is not required.
///
/// Unlike `AtomicLong`, which requires the CP subsystem, `PNCounter` works with the
/// standard AP (Available-Partition tolerant) data structures.
#[derive(Debug)]
pub struct PNCounter {
    name: String,
    connection_manager: Arc<ConnectionManager>,
}

impl PNCounter {
    /// Creates a new PN Counter proxy.
    pub(crate) fn new(name: String, connection_manager: Arc<ConnectionManager>) -> Self {
        Self {
            name,
            connection_manager,
        }
    }

    /// Returns the name of this PN Counter.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Gets the current value of the counter.
    ///
    /// The returned value is eventually consistent and may not reflect
    /// the most recent updates from other cluster members.
    pub async fn get(&self) -> Result<i64> {
        let mut message = ClientMessage::create_for_encode_any_partition(PN_COUNTER_GET);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::empty_replica_timestamps_frame());

        let response = self.invoke(message).await?;
        Self::decode_long_response(&response)
    }

    /// Adds the given delta to the counter and returns the new value.
    pub async fn add_and_get(&self, delta: i64) -> Result<i64> {
        let mut message = ClientMessage::create_for_encode_any_partition(PN_COUNTER_ADD);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::long_frame(delta));
        message.add_frame(Self::bool_frame(false)); // get_before_update = false
        message.add_frame(Self::empty_replica_timestamps_frame());

        let response = self.invoke(message).await?;
        Self::decode_long_response(&response)
    }

    /// Adds the given delta to the counter and returns the previous value.
    pub async fn get_and_add(&self, delta: i64) -> Result<i64> {
        let mut message = ClientMessage::create_for_encode_any_partition(PN_COUNTER_ADD);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::long_frame(delta));
        message.add_frame(Self::bool_frame(true)); // get_before_update = true
        message.add_frame(Self::empty_replica_timestamps_frame());

        let response = self.invoke(message).await?;
        Self::decode_long_response(&response)
    }

    /// Subtracts the given delta from the counter and returns the new value.
    pub async fn subtract_and_get(&self, delta: i64) -> Result<i64> {
        self.add_and_get(-delta).await
    }

    /// Subtracts the given delta from the counter and returns the previous value.
    pub async fn get_and_subtract(&self, delta: i64) -> Result<i64> {
        self.get_and_add(-delta).await
    }

    /// Increments the counter by one and returns the new value.
    pub async fn increment_and_get(&self) -> Result<i64> {
        self.add_and_get(1).await
    }

    /// Decrements the counter by one and returns the new value.
    pub async fn decrement_and_get(&self) -> Result<i64> {
        self.add_and_get(-1).await
    }

    /// Resets the counter to zero.
    ///
    /// Note: This operation is NOT atomic and relies on reading the current value
    /// and then subtracting it. Due to CRDT semantics, concurrent updates may
    /// result in a non-zero value after reset completes.
    pub async fn reset(&self) -> Result<()> {
        let current = self.get().await?;
        if current != 0 {
            self.add_and_get(-current).await?;
        }
        Ok(())
    }

    fn string_frame(s: &str) -> Frame {
        Frame::with_content(BytesMut::from(s.as_bytes()))
    }

    fn long_frame(value: i64) -> Frame {
        let mut buf = BytesMut::with_capacity(8);
        buf.extend_from_slice(&value.to_le_bytes());
        Frame::with_content(buf)
    }

    fn bool_frame(value: bool) -> Frame {
        let mut buf = BytesMut::with_capacity(1);
        buf.extend_from_slice(&[if value { 1u8 } else { 0u8 }]);
        Frame::with_content(buf)
    }

    fn empty_replica_timestamps_frame() -> Frame {
        Frame::with_content(BytesMut::new())
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
}

impl Clone for PNCounter {
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
    fn test_pn_counter_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<PNCounter>();
    }

    #[test]
    fn test_pn_counter_clone() {
        fn assert_clone<T: Clone>() {}
        assert_clone::<PNCounter>();
    }

    #[test]
    fn test_string_frame() {
        let frame = PNCounter::string_frame("test-counter");
        assert_eq!(&frame.content[..], b"test-counter");
    }

    #[test]
    fn test_long_frame() {
        let frame = PNCounter::long_frame(42);
        assert_eq!(frame.content.len(), 8);
        assert_eq!(
            i64::from_le_bytes(frame.content[..8].try_into().unwrap()),
            42
        );
    }

    #[test]
    fn test_long_frame_negative() {
        let frame = PNCounter::long_frame(-100);
        assert_eq!(frame.content.len(), 8);
        assert_eq!(
            i64::from_le_bytes(frame.content[..8].try_into().unwrap()),
            -100
        );
    }

    #[test]
    fn test_long_frame_max_value() {
        let frame = PNCounter::long_frame(i64::MAX);
        assert_eq!(
            i64::from_le_bytes(frame.content[..8].try_into().unwrap()),
            i64::MAX
        );
    }

    #[test]
    fn test_long_frame_min_value() {
        let frame = PNCounter::long_frame(i64::MIN);
        assert_eq!(
            i64::from_le_bytes(frame.content[..8].try_into().unwrap()),
            i64::MIN
        );
    }

    #[test]
    fn test_bool_frame_true() {
        let frame = PNCounter::bool_frame(true);
        assert_eq!(frame.content.len(), 1);
        assert_eq!(frame.content[0], 1);
    }

    #[test]
    fn test_bool_frame_false() {
        let frame = PNCounter::bool_frame(false);
        assert_eq!(frame.content.len(), 1);
        assert_eq!(frame.content[0], 0);
    }

    #[test]
    fn test_empty_replica_timestamps_frame() {
        let frame = PNCounter::empty_replica_timestamps_frame();
        assert!(frame.content.is_empty());
    }
}
