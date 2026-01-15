//! Distributed ringbuffer implementation.

use std::marker::PhantomData;
use std::sync::Arc;

use hazelcast_core::protocol::constants::*;
use hazelcast_core::protocol::{ClientMessage, Frame};
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

/// A distributed ringbuffer that stores a fixed-capacity sequence of items.
///
/// The ringbuffer provides a circular buffer with sequence-based access.
/// Each item has an associated sequence number for reading. When the
/// ringbuffer is full, the oldest items are overwritten (depending on
/// the overflow policy).
///
/// # Example
///
/// ```ignore
/// let rb = client.get_ringbuffer::<String>("my-ringbuffer");
///
/// let seq = rb.add("first".to_string()).await?;
/// let item = rb.read_one(seq).await?;
/// ```
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

    /// Adds an item to the tail of the ringbuffer.
    ///
    /// Returns the sequence number of the added item.
    /// If the ringbuffer is full, the oldest item is overwritten.
    pub async fn add(&self, item: T) -> Result<i64> {
        self.add_with_policy(item, OverflowPolicy::Overwrite).await
    }

    /// Adds an item to the tail with the specified overflow policy.
    ///
    /// Returns the sequence number of the added item, or -1 if the operation
    /// failed due to the overflow policy being `Fail` and the buffer being full.
    pub async fn add_with_policy(&self, item: T, overflow_policy: OverflowPolicy) -> Result<i64> {
        let item_data = item.serialize()?;

        let mut request = ClientMessage::create_for_encode(REQUEST_HEADER_SIZE + 8);
        request.set_message_type(RINGBUFFER_ADD);
        request.set_partition_id(PARTITION_ID_ANY);

        request.add_frame(Frame::new_string_frame(&self.name));

        let mut policy_frame = Frame::new(4);
        policy_frame.append_i32(overflow_policy as i32);
        request.add_frame(policy_frame);

        request.add_frame(Frame::new_data_frame(&item_data));
        request.finalize();

        let response = self.connection_manager.send(request).await?;
        let frames = response.frames();
        if frames.is_empty() {
            return Err(HazelcastError::Protocol("empty response".into()));
        }

        let initial_frame = &frames[0];
        let sequence = initial_frame.read_i64(RESPONSE_HEADER_SIZE);
        Ok(sequence)
    }

    /// Adds all items to the tail of the ringbuffer.
    ///
    /// Returns the sequence number of the last added item.
    pub async fn add_all(&self, items: Vec<T>, overflow_policy: OverflowPolicy) -> Result<i64> {
        if items.is_empty() {
            return self.tail_sequence().await;
        }

        let mut request = ClientMessage::create_for_encode(REQUEST_HEADER_SIZE + 8);
        request.set_message_type(RINGBUFFER_ADD_ALL);
        request.set_partition_id(PARTITION_ID_ANY);

        request.add_frame(Frame::new_string_frame(&self.name));

        request.add_frame(Frame::new_begin_frame());
        for item in items {
            let item_data = item.serialize()?;
            request.add_frame(Frame::new_data_frame(&item_data));
        }
        request.add_frame(Frame::new_end_frame());

        let mut policy_frame = Frame::new(4);
        policy_frame.append_i32(overflow_policy as i32);
        request.add_frame(policy_frame);

        request.finalize();

        let response = self.connection_manager.send(request).await?;
        let frames = response.frames();
        if frames.is_empty() {
            return Err(HazelcastError::Protocol("empty response".into()));
        }

        let initial_frame = &frames[0];
        let sequence = initial_frame.read_i64(RESPONSE_HEADER_SIZE);
        Ok(sequence)
    }

    /// Reads a single item from the ringbuffer at the given sequence.
    ///
    /// Returns `None` if the sequence is stale (already overwritten) or
    /// if the sequence has not been written yet.
    pub async fn read_one(&self, sequence: i64) -> Result<Option<T>> {
        let mut request = ClientMessage::create_for_encode(REQUEST_HEADER_SIZE + 8);
        request.set_message_type(RINGBUFFER_READ_ONE);
        request.set_partition_id(PARTITION_ID_ANY);

        request.add_frame(Frame::new_string_frame(&self.name));

        let mut seq_frame = Frame::new(8);
        seq_frame.append_i64(sequence);
        request.add_frame(seq_frame);
        request.finalize();

        let response = self.connection_manager.send(request).await?;
        let frames = response.frames();
        if frames.len() < 2 {
            return Ok(None);
        }

        let data_frame = &frames[1];
        if data_frame.is_null() {
            return Ok(None);
        }

        let item = T::deserialize(&data_frame.content())?;
        Ok(Some(item))
    }

    /// Reads multiple items from the ringbuffer starting at the given sequence.
    ///
    /// - `start_sequence`: The sequence to start reading from
    /// - `min_count`: Minimum number of items to read (blocks until available)
    /// - `max_count`: Maximum number of items to read
    ///
    /// Returns a tuple of (items, next_sequence_to_read).
    pub async fn read_many(
        &self,
        start_sequence: i64,
        min_count: i32,
        max_count: i32,
    ) -> Result<(Vec<T>, i64)> {
        let mut request = ClientMessage::create_for_encode(REQUEST_HEADER_SIZE + 24);
        request.set_message_type(RINGBUFFER_READ_MANY);
        request.set_partition_id(PARTITION_ID_ANY);

        request.add_frame(Frame::new_string_frame(&self.name));

        let mut params_frame = Frame::new(20);
        params_frame.append_i64(start_sequence);
        params_frame.append_i32(min_count);
        params_frame.append_i32(max_count);
        request.add_frame(params_frame);

        request.add_frame(Frame::new_null_frame());
        request.finalize();

        let response = self.connection_manager.send(request).await?;
        let frames = response.frames();
        if frames.is_empty() {
            return Err(HazelcastError::Protocol("empty response".into()));
        }

        let initial_frame = &frames[0];
        let read_count = initial_frame.read_i32(RESPONSE_HEADER_SIZE);
        let next_seq = initial_frame.read_i64(RESPONSE_HEADER_SIZE + 4);

        let mut items = Vec::with_capacity(read_count as usize);
        let mut i = 1;
        while i < frames.len() {
            let frame = &frames[i];
            if frame.is_end() {
                break;
            }
            if !frame.is_begin() && !frame.is_null() {
                let item = T::deserialize(&frame.content())?;
                items.push(item);
            }
            i += 1;
        }

        Ok((items, next_seq))
    }

    /// Returns the capacity of this ringbuffer.
    pub async fn capacity(&self) -> Result<i64> {
        let mut request = ClientMessage::create_for_encode(REQUEST_HEADER_SIZE);
        request.set_message_type(RINGBUFFER_CAPACITY);
        request.set_partition_id(PARTITION_ID_ANY);
        request.add_frame(Frame::new_string_frame(&self.name));
        request.finalize();

        let response = self.connection_manager.send(request).await?;
        let frames = response.frames();
        if frames.is_empty() {
            return Err(HazelcastError::Protocol("empty response".into()));
        }

        let initial_frame = &frames[0];
        Ok(initial_frame.read_i64(RESPONSE_HEADER_SIZE))
    }

    /// Returns the number of items in this ringbuffer.
    pub async fn size(&self) -> Result<i64> {
        let mut request = ClientMessage::create_for_encode(REQUEST_HEADER_SIZE);
        request.set_message_type(RINGBUFFER_SIZE);
        request.set_partition_id(PARTITION_ID_ANY);
        request.add_frame(Frame::new_string_frame(&self.name));
        request.finalize();

        let response = self.connection_manager.send(request).await?;
        let frames = response.frames();
        if frames.is_empty() {
            return Err(HazelcastError::Protocol("empty response".into()));
        }

        let initial_frame = &frames[0];
        Ok(initial_frame.read_i64(RESPONSE_HEADER_SIZE))
    }

    /// Returns the sequence of the head (oldest item).
    ///
    /// The head is the first item that can be read. If the ringbuffer is empty,
    /// returns the sequence of the next item to be added.
    pub async fn head_sequence(&self) -> Result<i64> {
        let mut request = ClientMessage::create_for_encode(REQUEST_HEADER_SIZE);
        request.set_message_type(RINGBUFFER_HEAD_SEQUENCE);
        request.set_partition_id(PARTITION_ID_ANY);
        request.add_frame(Frame::new_string_frame(&self.name));
        request.finalize();

        let response = self.connection_manager.send(request).await?;
        let frames = response.frames();
        if frames.is_empty() {
            return Err(HazelcastError::Protocol("empty response".into()));
        }

        let initial_frame = &frames[0];
        Ok(initial_frame.read_i64(RESPONSE_HEADER_SIZE))
    }

    /// Returns the sequence of the tail (newest item).
    ///
    /// The tail is the last item that was added. If the ringbuffer is empty,
    /// returns -1.
    pub async fn tail_sequence(&self) -> Result<i64> {
        let mut request = ClientMessage::create_for_encode(REQUEST_HEADER_SIZE);
        request.set_message_type(RINGBUFFER_TAIL_SEQUENCE);
        request.set_partition_id(PARTITION_ID_ANY);
        request.add_frame(Frame::new_string_frame(&self.name));
        request.finalize();

        let response = self.connection_manager.send(request).await?;
        let frames = response.frames();
        if frames.is_empty() {
            return Err(HazelcastError::Protocol("empty response".into()));
        }

        let initial_frame = &frames[0];
        Ok(initial_frame.read_i64(RESPONSE_HEADER_SIZE))
    }

    /// Returns the remaining capacity of this ringbuffer.
    ///
    /// This is the number of items that can be added before the oldest items
    /// start being overwritten.
    pub async fn remaining_capacity(&self) -> Result<i64> {
        let mut request = ClientMessage::create_for_encode(REQUEST_HEADER_SIZE);
        request.set_message_type(RINGBUFFER_REMAINING_CAPACITY);
        request.set_partition_id(PARTITION_ID_ANY);
        request.add_frame(Frame::new_string_frame(&self.name));
        request.finalize();

        let response = self.connection_manager.send(request).await?;
        let frames = response.frames();
        if frames.is_empty() {
            return Err(HazelcastError::Protocol("empty response".into()));
        }

        let initial_frame = &frames[0];
        Ok(initial_frame.read_i64(RESPONSE_HEADER_SIZE))
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
}
