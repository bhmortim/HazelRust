//! Distributed iterator for cluster-wide iteration over map data.

use std::collections::VecDeque;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::BytesMut;
use hazelcast_core::protocol::constants::{
    MAP_FETCH_ENTRIES, MAP_FETCH_KEYS, RESPONSE_HEADER_SIZE, IS_NULL_FLAG, END_FLAG,
};
use hazelcast_core::protocol::Frame;
use hazelcast_core::{ClientMessage, Deserializable, HazelcastError, Result};
use hazelcast_core::serialization::ObjectDataInput;

use crate::connection::ConnectionManager;

/// The type of iteration to perform.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IterationType {
    /// Iterate over keys only.
    Keys,
    /// Iterate over values only.
    Values,
    /// Iterate over key-value pairs.
    Entries,
}

/// Configuration for distributed iteration.
#[derive(Debug, Clone)]
pub struct IteratorConfig {
    /// Number of items to fetch per batch.
    pub batch_size: i32,
    /// Whether to prefetch the next batch.
    pub prefetch: bool,
}

impl Default for IteratorConfig {
    fn default() -> Self {
        Self {
            batch_size: 100,
            prefetch: true,
        }
    }
}

impl IteratorConfig {
    /// Creates a new configuration with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the batch size for fetching.
    pub fn batch_size(mut self, size: i32) -> Self {
        self.batch_size = size;
        self
    }

    /// Sets whether to prefetch the next batch.
    pub fn prefetch(mut self, prefetch: bool) -> Self {
        self.prefetch = prefetch;
        self
    }
}

/// Internal state for tracking iteration progress per partition.
#[derive(Debug, Clone)]
struct PartitionCursor {
    /// The partition ID.
    partition_id: i32,
    /// The table index for iteration (cursor position).
    table_index: i32,
    /// Whether this partition has more data.
    has_more: bool,
}

/// A distributed iterator that fetches data in batches across cluster partitions.
///
/// This iterator maintains cursors for each partition and fetches data lazily
/// as items are consumed. It supports iterating over keys, values, or entries.
///
/// # Example
///
/// ```ignore
/// // Get all keys from the map
/// let mut keys = map.key_set().await?;
/// while let Some(key) = keys.next().await? {
///     println!("Key: {:?}", key);
/// }
///
/// // Get all entries from the map
/// let mut entries = map.entry_set().await?;
/// while let Some((key, value)) = entries.next_entry().await? {
///     println!("{:?}: {:?}", key, value);
/// }
/// ```
pub struct DistributedIterator<T> {
    /// The map name.
    map_name: String,
    /// Connection manager for cluster communication.
    connection_manager: Arc<ConnectionManager>,
    /// Type of iteration.
    iteration_type: IterationType,
    /// Batch size for fetching.
    batch_size: i32,
    /// Buffer of fetched items.
    buffer: VecDeque<T>,
    /// Partition cursors tracking iteration progress.
    partition_cursors: Vec<PartitionCursor>,
    /// Index of the current partition being iterated.
    current_partition_index: usize,
    /// Whether iteration is complete.
    exhausted: bool,
    /// Phantom data for type safety.
    _phantom: PhantomData<T>,
}

impl<T> std::fmt::Debug for DistributedIterator<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DistributedIterator")
            .field("map_name", &self.map_name)
            .field("iteration_type", &self.iteration_type)
            .field("batch_size", &self.batch_size)
            .field("buffer_size", &self.buffer.len())
            .field("current_partition_index", &self.current_partition_index)
            .field("exhausted", &self.exhausted)
            .finish()
    }
}

impl<T> DistributedIterator<T> {
    /// Creates a new distributed iterator.
    pub(crate) fn new(
        map_name: String,
        connection_manager: Arc<ConnectionManager>,
        iteration_type: IterationType,
        partition_count: i32,
        config: IteratorConfig,
    ) -> Self {
        let partition_cursors: Vec<PartitionCursor> = (0..partition_count)
            .map(|id| PartitionCursor {
                partition_id: id,
                table_index: -1,
                has_more: true,
            })
            .collect();

        Self {
            map_name,
            connection_manager,
            iteration_type,
            batch_size: config.batch_size,
            buffer: VecDeque::new(),
            partition_cursors,
            current_partition_index: 0,
            exhausted: false,
            _phantom: PhantomData,
        }
    }

    /// Returns the map name being iterated.
    pub fn map_name(&self) -> &str {
        &self.map_name
    }

    /// Returns the iteration type.
    pub fn iteration_type(&self) -> IterationType {
        self.iteration_type
    }

    /// Returns `true` if the iterator has been exhausted.
    pub fn is_exhausted(&self) -> bool {
        self.exhausted
    }

    /// Returns the number of items currently buffered.
    pub fn buffered_count(&self) -> usize {
        self.buffer.len()
    }

    fn string_frame(s: &str) -> Frame {
        Frame::with_content(BytesMut::from(s.as_bytes()))
    }

    fn int_frame(value: i32) -> Frame {
        let mut buf = BytesMut::with_capacity(4);
        buf.extend_from_slice(&value.to_le_bytes());
        Frame::with_content(buf)
    }

    async fn get_connection_address(&self) -> Result<SocketAddr> {
        let addresses = self.connection_manager.connected_addresses().await;
        addresses.into_iter().next().ok_or_else(|| {
            HazelcastError::Connection("no connections available".to_string())
        })
    }

    async fn invoke(&self, message: ClientMessage) -> Result<ClientMessage> {
        let address = self.get_connection_address().await?;
        self.connection_manager.send_to(address, message).await?;
        self.connection_manager
            .receive_from(address)
            .await?
            .ok_or_else(|| HazelcastError::Connection("connection closed unexpectedly".to_string()))
    }
}

impl<K> DistributedIterator<K>
where
    K: Deserializable + Send,
{
    /// Fetches the next batch of keys from a partition.
    async fn fetch_keys_batch(&mut self, partition_id: i32, table_index: i32) -> Result<(Vec<K>, i32)> {
        let mut message = ClientMessage::create_for_encode(MAP_FETCH_KEYS, partition_id);
        message.add_frame(Self::string_frame(&self.map_name));
        message.add_frame(Self::int_frame(table_index));
        message.add_frame(Self::int_frame(self.batch_size));

        let response = self.invoke(message).await?;
        Self::decode_keys_response(&response)
    }

    fn decode_keys_response(response: &ClientMessage) -> Result<(Vec<K>, i32)> {
        let frames = response.frames();
        if frames.is_empty() {
            return Ok((Vec::new(), -1));
        }

        let initial_frame = &frames[0];
        let next_table_index = if initial_frame.content.len() >= RESPONSE_HEADER_SIZE + 4 {
            let offset = RESPONSE_HEADER_SIZE;
            i32::from_le_bytes([
                initial_frame.content[offset],
                initial_frame.content[offset + 1],
                initial_frame.content[offset + 2],
                initial_frame.content[offset + 3],
            ])
        } else {
            -1
        };

        let mut keys = Vec::new();
        for frame in frames.iter().skip(1) {
            if frame.flags & IS_NULL_FLAG != 0 {
                continue;
            }
            if frame.flags & END_FLAG != 0 && frame.content.is_empty() {
                break;
            }
            if frame.content.is_empty() {
                continue;
            }

            let mut input = ObjectDataInput::new(&frame.content);
            if let Ok(key) = K::deserialize(&mut input) {
                keys.push(key);
            }
        }

        Ok((keys, next_table_index))
    }

    /// Returns the next key, fetching more if needed.
    pub async fn next(&mut self) -> Result<Option<K>> {
        if let Some(item) = self.buffer.pop_front() {
            return Ok(Some(item));
        }

        if self.exhausted {
            return Ok(None);
        }

        while self.current_partition_index < self.partition_cursors.len() {
            let cursor = &self.partition_cursors[self.current_partition_index];
            
            if !cursor.has_more {
                self.current_partition_index += 1;
                continue;
            }

            let partition_id = cursor.partition_id;
            let table_index = cursor.table_index;

            let (keys, next_index) = self.fetch_keys_batch(partition_id, table_index).await?;

            let cursor = &mut self.partition_cursors[self.current_partition_index];
            cursor.table_index = next_index;
            cursor.has_more = next_index >= 0 && !keys.is_empty();

            if keys.is_empty() {
                self.current_partition_index += 1;
                continue;
            }

            for key in keys {
                self.buffer.push_back(key);
            }

            if let Some(item) = self.buffer.pop_front() {
                return Ok(Some(item));
            }
        }

        self.exhausted = true;
        Ok(None)
    }

    /// Collects all remaining items into a vector.
    pub async fn collect(mut self) -> Result<Vec<K>> {
        let mut results = Vec::new();
        while let Some(item) = self.next().await? {
            results.push(item);
        }
        Ok(results)
    }
}

impl<K, V> DistributedIterator<(K, V)>
where
    K: Deserializable + Send,
    V: Deserializable + Send,
{
    /// Fetches the next batch of entries from a partition.
    async fn fetch_entries_batch(&mut self, partition_id: i32, table_index: i32) -> Result<(Vec<(K, V)>, i32)> {
        let mut message = ClientMessage::create_for_encode(MAP_FETCH_ENTRIES, partition_id);
        message.add_frame(Self::string_frame(&self.map_name));
        message.add_frame(Self::int_frame(table_index));
        message.add_frame(Self::int_frame(self.batch_size));

        let response = self.invoke(message).await?;
        Self::decode_entries_response(&response)
    }

    fn decode_entries_response(response: &ClientMessage) -> Result<(Vec<(K, V)>, i32)> {
        let frames = response.frames();
        if frames.is_empty() {
            return Ok((Vec::new(), -1));
        }

        let initial_frame = &frames[0];
        let next_table_index = if initial_frame.content.len() >= RESPONSE_HEADER_SIZE + 4 {
            let offset = RESPONSE_HEADER_SIZE;
            i32::from_le_bytes([
                initial_frame.content[offset],
                initial_frame.content[offset + 1],
                initial_frame.content[offset + 2],
                initial_frame.content[offset + 3],
            ])
        } else {
            -1
        };

        let mut entries = Vec::new();
        let data_frames: Vec<_> = frames
            .iter()
            .skip(1)
            .filter(|f| f.flags & IS_NULL_FLAG == 0 && !f.content.is_empty())
            .collect();

        let mut i = 0;
        while i + 1 < data_frames.len() {
            let key_frame = data_frames[i];
            let value_frame = data_frames[i + 1];

            if key_frame.flags & END_FLAG != 0 && key_frame.content.is_empty() {
                break;
            }

            let mut key_input = ObjectDataInput::new(&key_frame.content);
            let mut value_input = ObjectDataInput::new(&value_frame.content);

            if let (Ok(key), Ok(value)) = (K::deserialize(&mut key_input), V::deserialize(&mut value_input)) {
                entries.push((key, value));
            }

            i += 2;
        }

        Ok((entries, next_table_index))
    }

    /// Returns the next entry, fetching more if needed.
    pub async fn next_entry(&mut self) -> Result<Option<(K, V)>> {
        if let Some(item) = self.buffer.pop_front() {
            return Ok(Some(item));
        }

        if self.exhausted {
            return Ok(None);
        }

        while self.current_partition_index < self.partition_cursors.len() {
            let cursor = &self.partition_cursors[self.current_partition_index];
            
            if !cursor.has_more {
                self.current_partition_index += 1;
                continue;
            }

            let partition_id = cursor.partition_id;
            let table_index = cursor.table_index;

            let (entries, next_index) = self.fetch_entries_batch(partition_id, table_index).await?;

            let cursor = &mut self.partition_cursors[self.current_partition_index];
            cursor.table_index = next_index;
            cursor.has_more = next_index >= 0 && !entries.is_empty();

            if entries.is_empty() {
                self.current_partition_index += 1;
                continue;
            }

            for entry in entries {
                self.buffer.push_back(entry);
            }

            if let Some(item) = self.buffer.pop_front() {
                return Ok(Some(item));
            }
        }

        self.exhausted = true;
        Ok(None)
    }

    /// Collects all remaining entries into a vector.
    pub async fn collect_entries(mut self) -> Result<Vec<(K, V)>> {
        let mut results = Vec::new();
        while let Some(item) = self.next_entry().await? {
            results.push(item);
        }
        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_iterator_config_default() {
        let config = IteratorConfig::default();
        assert_eq!(config.batch_size, 100);
        assert!(config.prefetch);
    }

    #[test]
    fn test_iterator_config_builder() {
        let config = IteratorConfig::new()
            .batch_size(50)
            .prefetch(false);
        
        assert_eq!(config.batch_size, 50);
        assert!(!config.prefetch);
    }

    #[test]
    fn test_iteration_type_equality() {
        assert_eq!(IterationType::Keys, IterationType::Keys);
        assert_ne!(IterationType::Keys, IterationType::Values);
        assert_ne!(IterationType::Values, IterationType::Entries);
    }

    #[test]
    fn test_iteration_type_copy() {
        let t1 = IterationType::Entries;
        let t2 = t1;
        assert_eq!(t1, t2);
    }

    #[test]
    fn test_distributed_iterator_is_send() {
        fn assert_send<T: Send>() {}
        assert_send::<DistributedIterator<String>>();
        assert_send::<DistributedIterator<(String, i32)>>();
    }
}
