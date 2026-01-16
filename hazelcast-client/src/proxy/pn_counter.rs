//! Distributed PN (Positive-Negative) Counter proxy implementation.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use bytes::BytesMut;
use uuid::Uuid;
use hazelcast_core::protocol::constants::*;
use hazelcast_core::protocol::Frame;
use hazelcast_core::{ClientMessage, HazelcastError, Result};

use crate::connection::ConnectionManager;

/// A timestamp observed from a specific replica.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ReplicaTimestamp {
    /// UUID of the replica member.
    replica_id: Uuid,
    /// Logical timestamp from that replica.
    timestamp: i64,
}

/// Tracks observed timestamps from cluster replicas for monotonic read guarantees.
///
/// PN Counters use this to ensure clients don't read stale values by sending
/// their observed timestamps to the cluster, which routes requests to replicas
/// that have at least those timestamps.
#[derive(Debug, Clone, Default)]
struct ReplicaTimestampVector {
    timestamps: Vec<ReplicaTimestamp>,
}

impl ReplicaTimestampVector {
    /// Creates a new empty timestamp vector.
    fn new() -> Self {
        Self { timestamps: Vec::new() }
    }

    /// Merges observed timestamps from a response into this vector.
    /// For each replica, keeps the maximum observed timestamp.
    fn merge(&mut self, other: &[ReplicaTimestamp]) {
        for incoming in other {
            if let Some(existing) = self.timestamps.iter_mut()
                .find(|t| t.replica_id == incoming.replica_id)
            {
                if incoming.timestamp > existing.timestamp {
                    existing.timestamp = incoming.timestamp;
                }
            } else {
                self.timestamps.push(incoming.clone());
            }
        }
    }

    /// Encodes the timestamp vector into a frame for sending to the server.
    /// Format: [entry_count: i32][entries...] where each entry is:
    /// [uuid_most_sig_bits: i64][uuid_least_sig_bits: i64][timestamp: i64]
    fn to_frame(&self) -> Frame {
        let entry_count = self.timestamps.len() as i32;
        let capacity = 4 + self.timestamps.len() * 24;
        let mut buf = BytesMut::with_capacity(capacity);

        buf.extend_from_slice(&entry_count.to_le_bytes());
        for ts in &self.timestamps {
            let (most, least) = ts.replica_id.as_u64_pair();
            buf.extend_from_slice(&(most as i64).to_le_bytes());
            buf.extend_from_slice(&(least as i64).to_le_bytes());
            buf.extend_from_slice(&ts.timestamp.to_le_bytes());
        }

        Frame::with_content(buf)
    }
}

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
    /// Observed replica timestamps for monotonic read guarantees.
    observed_timestamps: Arc<Mutex<ReplicaTimestampVector>>,
}

impl PNCounter {
    /// Creates a new PN Counter proxy.
    pub(crate) fn new(name: String, connection_manager: Arc<ConnectionManager>) -> Self {
        Self {
            name,
            connection_manager,
            observed_timestamps: Arc::new(Mutex::new(ReplicaTimestampVector::new())),
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
        message.add_frame(self.replica_timestamps_frame());

        let response = self.invoke(message).await?;
        self.decode_response_and_update_timestamps(&response)
    }

    /// Adds the given delta to the counter and returns the new value.
    pub async fn add_and_get(&self, delta: i64) -> Result<i64> {
        let mut message = ClientMessage::create_for_encode_any_partition(PN_COUNTER_ADD);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::long_frame(delta));
        message.add_frame(Self::bool_frame(false)); // get_before_update = false
        message.add_frame(self.replica_timestamps_frame());

        let response = self.invoke(message).await?;
        self.decode_response_and_update_timestamps(&response)
    }

    /// Adds the given delta to the counter and returns the previous value.
    pub async fn get_and_add(&self, delta: i64) -> Result<i64> {
        let mut message = ClientMessage::create_for_encode_any_partition(PN_COUNTER_ADD);
        message.add_frame(Self::string_frame(&self.name));
        message.add_frame(Self::long_frame(delta));
        message.add_frame(Self::bool_frame(true)); // get_before_update = true
        message.add_frame(self.replica_timestamps_frame());

        let response = self.invoke(message).await?;
        self.decode_response_and_update_timestamps(&response)
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

    fn replica_timestamps_frame(&self) -> Frame {
        self.observed_timestamps
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .to_frame()
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

    /// Decodes a PNCounter response, extracts the value and updates observed timestamps.
    fn decode_response_and_update_timestamps(&self, response: &ClientMessage) -> Result<i64> {
        let frames = response.frames();
        if frames.is_empty() {
            return Err(HazelcastError::Serialization("empty response".to_string()));
        }

        let initial_frame = &frames[0];
        let value = if initial_frame.content.len() >= RESPONSE_HEADER_SIZE + 8 {
            let offset = RESPONSE_HEADER_SIZE;
            i64::from_le_bytes([
                initial_frame.content[offset],
                initial_frame.content[offset + 1],
                initial_frame.content[offset + 2],
                initial_frame.content[offset + 3],
                initial_frame.content[offset + 4],
                initial_frame.content[offset + 5],
                initial_frame.content[offset + 6],
                initial_frame.content[offset + 7],
            ])
        } else {
            return Err(HazelcastError::Serialization("response too short".to_string()));
        };

        if frames.len() > 1 {
            let ts_frame = &frames[1];
            if ts_frame.content.len() >= 4 {
                let entry_count = i32::from_le_bytes([
                    ts_frame.content[0],
                    ts_frame.content[1],
                    ts_frame.content[2],
                    ts_frame.content[3],
                ]) as usize;

                let mut incoming_timestamps = Vec::with_capacity(entry_count);
                let mut offset = 4;

                for _ in 0..entry_count {
                    if offset + 24 > ts_frame.content.len() {
                        break;
                    }

                    let most = i64::from_le_bytes([
                        ts_frame.content[offset],
                        ts_frame.content[offset + 1],
                        ts_frame.content[offset + 2],
                        ts_frame.content[offset + 3],
                        ts_frame.content[offset + 4],
                        ts_frame.content[offset + 5],
                        ts_frame.content[offset + 6],
                        ts_frame.content[offset + 7],
                    ]) as u64;
                    offset += 8;

                    let least = i64::from_le_bytes([
                        ts_frame.content[offset],
                        ts_frame.content[offset + 1],
                        ts_frame.content[offset + 2],
                        ts_frame.content[offset + 3],
                        ts_frame.content[offset + 4],
                        ts_frame.content[offset + 5],
                        ts_frame.content[offset + 6],
                        ts_frame.content[offset + 7],
                    ]) as u64;
                    offset += 8;

                    let timestamp = i64::from_le_bytes([
                        ts_frame.content[offset],
                        ts_frame.content[offset + 1],
                        ts_frame.content[offset + 2],
                        ts_frame.content[offset + 3],
                        ts_frame.content[offset + 4],
                        ts_frame.content[offset + 5],
                        ts_frame.content[offset + 6],
                        ts_frame.content[offset + 7],
                    ]);
                    offset += 8;

                    incoming_timestamps.push(ReplicaTimestamp {
                        replica_id: Uuid::from_u64_pair(most, least),
                        timestamp,
                    });
                }

                if !incoming_timestamps.is_empty() {
                    self.observed_timestamps
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .merge(&incoming_timestamps);
                }
            }
        }

        Ok(value)
    }
}

impl Clone for PNCounter {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            connection_manager: Arc::clone(&self.connection_manager),
            observed_timestamps: Arc::clone(&self.observed_timestamps),
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
    fn test_replica_timestamp_vector_new() {
        let vector = ReplicaTimestampVector::new();
        assert!(vector.timestamps.is_empty());
    }

    #[test]
    fn test_replica_timestamp_vector_merge_new_replica() {
        let mut vector = ReplicaTimestampVector::new();
        let uuid = Uuid::new_v4();
        let incoming = vec![ReplicaTimestamp { replica_id: uuid, timestamp: 100 }];

        vector.merge(&incoming);

        assert_eq!(vector.timestamps.len(), 1);
        assert_eq!(vector.timestamps[0].replica_id, uuid);
        assert_eq!(vector.timestamps[0].timestamp, 100);
    }

    #[test]
    fn test_replica_timestamp_vector_merge_keeps_max() {
        let mut vector = ReplicaTimestampVector::new();
        let uuid = Uuid::new_v4();

        vector.merge(&[ReplicaTimestamp { replica_id: uuid, timestamp: 100 }]);

        vector.merge(&[ReplicaTimestamp { replica_id: uuid, timestamp: 50 }]);
        assert_eq!(vector.timestamps[0].timestamp, 100);

        vector.merge(&[ReplicaTimestamp { replica_id: uuid, timestamp: 200 }]);
        assert_eq!(vector.timestamps[0].timestamp, 200);
    }

    #[test]
    fn test_replica_timestamp_vector_merge_multiple_replicas() {
        let mut vector = ReplicaTimestampVector::new();
        let uuid1 = Uuid::new_v4();
        let uuid2 = Uuid::new_v4();

        vector.merge(&[
            ReplicaTimestamp { replica_id: uuid1, timestamp: 100 },
            ReplicaTimestamp { replica_id: uuid2, timestamp: 200 },
        ]);

        assert_eq!(vector.timestamps.len(), 2);
    }

    #[test]
    fn test_replica_timestamp_vector_to_frame_empty() {
        let vector = ReplicaTimestampVector::new();
        let frame = vector.to_frame();

        assert_eq!(frame.content.len(), 4);
        assert_eq!(i32::from_le_bytes(frame.content[..4].try_into().unwrap()), 0);
    }

    #[test]
    fn test_replica_timestamp_vector_to_frame_with_entries() {
        let mut vector = ReplicaTimestampVector::new();
        let uuid = Uuid::from_u64_pair(0x1234567890ABCDEF, 0xFEDCBA0987654321);
        vector.merge(&[ReplicaTimestamp { replica_id: uuid, timestamp: 42 }]);

        let frame = vector.to_frame();

        assert_eq!(frame.content.len(), 28);

        assert_eq!(i32::from_le_bytes(frame.content[0..4].try_into().unwrap()), 1);

        let most = i64::from_le_bytes(frame.content[4..12].try_into().unwrap()) as u64;
        assert_eq!(most, 0x1234567890ABCDEF);

        let least = i64::from_le_bytes(frame.content[12..20].try_into().unwrap()) as u64;
        assert_eq!(least, 0xFEDCBA0987654321);

        let timestamp = i64::from_le_bytes(frame.content[20..28].try_into().unwrap());
        assert_eq!(timestamp, 42);
    }

    #[test]
    fn test_pn_counter_clone_shares_timestamps() {
        fn assert_clone<T: Clone>() {}
        assert_clone::<PNCounter>();
    }
}
