//! Distributed ringbuffer implementation.
//!
//! Ringbuffer is partition-routed by name. Fixed params (overflow policy,
//! sequence, read counts) live in the request initial frame; values are
//! Hazelcast `Data` (8-byte header + payload).

use std::marker::PhantomData;
use std::sync::Arc;

use bytes::BytesMut;
use hazelcast_core::protocol::constants::*;
use hazelcast_core::protocol::{ClientMessage, Frame};
use hazelcast_core::serialization::{DataOutput, ObjectDataInput, ObjectDataOutput};
use hazelcast_core::{Deserializable, HazelcastError, Result, Serializable};

use crate::connection::ConnectionManager;

/// Overflow policy for ringbuffer add operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverflowPolicy {
    /// Overwrite the oldest items when the ringbuffer is full.
    Overwrite = 0,
    /// Fail the operation when the ringbuffer is full.
    Fail = 1,
}

/// A filter function for ringbuffer read operations (server-side).
pub trait RingbufferFilter<T>: Send + Sync {
    /// Returns the factory ID for serialization.
    fn factory_id(&self) -> i32;
    /// Returns the class ID for serialization.
    fn class_id(&self) -> i32;
    /// Writes the filter data to the output buffer.
    fn write_data(&self, output: &mut Vec<u8>) -> Result<()>;
}

/// A filter that accepts all items.
#[derive(Debug, Clone, Copy, Default)]
pub struct TrueFilter;

impl<T> RingbufferFilter<T> for TrueFilter {
    fn factory_id(&self) -> i32 {
        -32
    }
    fn class_id(&self) -> i32 {
        7
    }
    fn write_data(&self, _output: &mut Vec<u8>) -> Result<()> {
        Ok(())
    }
}

/// A filter that rejects all items.
#[derive(Debug, Clone, Copy, Default)]
pub struct FalseFilter;

impl<T> RingbufferFilter<T> for FalseFilter {
    fn factory_id(&self) -> i32 {
        -32
    }
    fn class_id(&self) -> i32 {
        6
    }
    fn write_data(&self, _output: &mut Vec<u8>) -> Result<()> {
        Ok(())
    }
}

/// A distributed ringbuffer that stores a fixed-capacity sequence of items.
#[derive(Debug)]
pub struct Ringbuffer<T> {
    name: String,
    connection_manager: Arc<ConnectionManager>,
    _marker: PhantomData<T>,
}

impl<T> Ringbuffer<T>
where
    T: Serializable + Deserializable + Send + Sync,
{
    pub(crate) fn new(name: String, connection_manager: Arc<ConnectionManager>) -> Self {
        Self {
            name,
            connection_manager,
            _marker: PhantomData,
        }
    }

    /// Returns the name of this ringbuffer.
    pub fn name(&self) -> &str {
        &self.name
    }

    // ── framing helpers ──────────────────────────────────────────────

    fn name_partition(&self) -> i32 {
        let count = self.connection_manager.partition_count();
        let count = if count > 0 { count } else { 271 };
        match Self::value_data(&self.name) {
            Ok(d) => {
                let h = if d.len() > 8 { &d[8..] } else { &d[..] };
                hazelcast_core::compute_partition_hash(h).abs() % count
            }
            Err(_) => 0,
        }
    }

    /// Serializes a value into Hazelcast `Data`: [partition_hash:4][type_id:4][payload].
    fn value_data<V: Serializable>(value: &V) -> Result<Vec<u8>> {
        let mut out = ObjectDataOutput::new();
        out.write_int(0)?;
        out.write_int(value.type_id())?;
        value.serialize(&mut out)?;
        Ok(out.into_bytes())
    }

    fn string_frame(s: &str) -> Frame {
        Frame::with_content(BytesMut::from(s.as_bytes()))
    }

    fn data_frame(d: &[u8]) -> Frame {
        Frame::with_content(BytesMut::from(d))
    }

    fn put_int(m: &mut ClientMessage, v: i32) {
        if let Some(f) = m.frames_mut().first_mut() {
            f.content.extend_from_slice(&v.to_le_bytes());
        }
    }

    fn put_long(m: &mut ClientMessage, v: i64) {
        if let Some(f) = m.frames_mut().first_mut() {
            f.content.extend_from_slice(&v.to_le_bytes());
        }
    }

    async fn invoke(&self, mut m: ClientMessage) -> Result<ClientMessage> {
        let pid = self.name_partition();
        m.set_partition_id(pid);
        self.connection_manager.invoke_on_partition(pid, m).await
    }

    fn decode_i64(r: &ClientMessage) -> Result<i64> {
        let f = r
            .frames()
            .first()
            .ok_or_else(|| HazelcastError::Protocol("empty response".to_string()))?;
        if f.content.len() < RESPONSE_HEADER_SIZE + 8 {
            return Err(HazelcastError::Protocol("ringbuffer i64 too short".to_string()));
        }
        Ok(i64::from_le_bytes(
            f.content[RESPONSE_HEADER_SIZE..RESPONSE_HEADER_SIZE + 8]
                .try_into()
                .unwrap(),
        ))
    }

    fn decode_value_at(content: &[u8]) -> Result<T> {
        // content is a Data: skip the 8-byte header.
        let payload = if content.len() > 8 { &content[8..] } else { content };
        let mut input = ObjectDataInput::new(payload);
        T::deserialize(&mut input)
    }

    // ── operations ───────────────────────────────────────────────────

    /// Adds an item to the tail; returns the assigned sequence.
    pub async fn add(&self, item: T) -> Result<i64> {
        self.add_with_policy(item, OverflowPolicy::Overwrite).await
    }

    /// Adds an item with the given overflow policy; returns the sequence (-1 on fail).
    pub async fn add_with_policy(&self, item: T, overflow_policy: OverflowPolicy) -> Result<i64> {
        let data = Self::value_data(&item)?;
        let mut m = ClientMessage::create_for_encode_any_partition(RINGBUFFER_ADD);
        Self::put_int(&mut m, overflow_policy as i32);
        m.add_frame(Self::string_frame(&self.name));
        m.add_frame(Self::data_frame(&data));
        let r = self.invoke(m).await?;
        Self::decode_i64(&r)
    }

    /// Adds all items; returns the sequence of the last item.
    pub async fn add_all(&self, items: Vec<T>, overflow_policy: OverflowPolicy) -> Result<i64> {
        if items.is_empty() {
            return self.tail_sequence().await;
        }
        let mut m = ClientMessage::create_for_encode_any_partition(RINGBUFFER_ADD_ALL);
        Self::put_int(&mut m, overflow_policy as i32);
        m.add_frame(Self::string_frame(&self.name));
        m.add_frame(Frame::new_begin_frame_empty());
        for item in &items {
            m.add_frame(Self::data_frame(&Self::value_data(item)?));
        }
        m.add_frame(Frame::new_end_frame());
        let r = self.invoke(m).await?;
        Self::decode_i64(&r)
    }

    /// Reads a single item at the given sequence, or `None` if unavailable.
    pub async fn read_one(&self, sequence: i64) -> Result<Option<T>> {
        let mut m = ClientMessage::create_for_encode_any_partition(RINGBUFFER_READ_ONE);
        Self::put_long(&mut m, sequence);
        m.add_frame(Self::string_frame(&self.name));
        let r = self.invoke(m).await?;
        let frames = r.frames();
        match frames.get(1) {
            Some(f) if f.flags & IS_NULL_FLAG == 0 && !f.content.is_empty() => {
                Ok(Some(Self::decode_value_at(&f.content)?))
            }
            _ => Ok(None),
        }
    }

    /// Reads up to `max_count` items starting at `start_sequence`.
    /// Returns the items and the next sequence to read from.
    pub async fn read_many(
        &self,
        start_sequence: i64,
        min_count: i32,
        max_count: i32,
    ) -> Result<(Vec<T>, i64)> {
        let mut m = ClientMessage::create_for_encode_any_partition(RINGBUFFER_READ_MANY);
        Self::put_long(&mut m, start_sequence);
        Self::put_int(&mut m, min_count);
        Self::put_int(&mut m, max_count);
        m.add_frame(Self::string_frame(&self.name));
        m.add_frame(Frame::new_null_frame()); // filter = null
        let r = self.invoke(m).await?;
        let frames = r.frames();
        // initial frame: readCount(int) @ HDR, nextSeq(long) @ HDR+4 (+ readCount field layout)
        let init = &frames[0];
        let next_seq = if init.content.len() >= RESPONSE_HEADER_SIZE + 12 {
            i64::from_le_bytes(
                init.content[RESPONSE_HEADER_SIZE + 4..RESPONSE_HEADER_SIZE + 12]
                    .try_into()
                    .unwrap(),
            )
        } else {
            start_sequence
        };
        let mut items = Vec::new();
        for f in frames.iter().skip(1) {
            if f.flags & (BEGIN_DATA_STRUCTURE_FLAG | END_DATA_STRUCTURE_FLAG) != 0 {
                continue;
            }
            if f.flags & IS_NULL_FLAG != 0 || f.content.is_empty() {
                continue;
            }
            if let Ok(v) = Self::decode_value_at(&f.content) {
                items.push(v);
            }
        }
        Ok((items, next_seq))
    }

    /// Returns the number of items in the ringbuffer.
    pub async fn size(&self) -> Result<i64> {
        let mut m = ClientMessage::create_for_encode_any_partition(RINGBUFFER_SIZE);
        m.add_frame(Self::string_frame(&self.name));
        Self::decode_i64(&self.invoke(m).await?)
    }

    /// Returns the capacity of the ringbuffer.
    pub async fn capacity(&self) -> Result<i64> {
        let mut m = ClientMessage::create_for_encode_any_partition(RINGBUFFER_CAPACITY);
        m.add_frame(Self::string_frame(&self.name));
        Self::decode_i64(&self.invoke(m).await?)
    }

    /// Returns the sequence of the head (oldest item).
    pub async fn head_sequence(&self) -> Result<i64> {
        let mut m = ClientMessage::create_for_encode_any_partition(RINGBUFFER_HEAD_SEQUENCE);
        m.add_frame(Self::string_frame(&self.name));
        Self::decode_i64(&self.invoke(m).await?)
    }

    /// Returns the sequence of the tail (newest item), or -1 if empty.
    pub async fn tail_sequence(&self) -> Result<i64> {
        let mut m = ClientMessage::create_for_encode_any_partition(RINGBUFFER_TAIL_SEQUENCE);
        m.add_frame(Self::string_frame(&self.name));
        Self::decode_i64(&self.invoke(m).await?)
    }

    /// Returns the remaining capacity of the ringbuffer.
    pub async fn remaining_capacity(&self) -> Result<i64> {
        let mut m = ClientMessage::create_for_encode_any_partition(RINGBUFFER_REMAINING_CAPACITY);
        m.add_frame(Self::string_frame(&self.name));
        Self::decode_i64(&self.invoke(m).await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_overflow_policy_values() {
        assert_eq!(OverflowPolicy::Overwrite as i32, 0);
        assert_eq!(OverflowPolicy::Fail as i32, 1);
    }

    #[test]
    fn test_ringbuffer_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Ringbuffer<String>>();
    }

    #[test]
    fn test_true_filter_serialization() {
        let filter = TrueFilter;
        assert_eq!(
            <TrueFilter as RingbufferFilter<String>>::factory_id(&filter),
            -32
        );
        assert_eq!(<TrueFilter as RingbufferFilter<String>>::class_id(&filter), 7);
    }
}
