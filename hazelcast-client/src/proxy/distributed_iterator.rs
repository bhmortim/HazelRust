//! Distributed iterator for cluster-wide iteration over map data.

use std::collections::VecDeque;
use std::marker::PhantomData;
use std::sync::Arc;

use bytes::BytesMut;
use hazelcast_core::protocol::constants::{
    MAP_FETCH_ENTRIES, MAP_FETCH_KEYS, PARTITION_ID_ANY, RESPONSE_HEADER_SIZE, IS_NULL_FLAG, END_FLAG,
    BEGIN_DATA_STRUCTURE_FLAG, END_DATA_STRUCTURE_FLAG,
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
                table_index: 0, // Start from slot 0
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

    async fn invoke(&self, partition_id: i32, message: ClientMessage) -> Result<ClientMessage> {
        // Route to partition owner for partition-specific fetch operations
        self.connection_manager.invoke_on_partition(partition_id, message).await
    }
}

impl<K> DistributedIterator<K>
where
    K: Deserializable + Send,
{
    /// Fetches the next batch of keys from a partition.
    async fn fetch_keys_batch(&mut self, partition_id: i32, table_index: i32) -> Result<(Vec<K>, i32)> {
        let mut message = ClientMessage::create_for_encode(MAP_FETCH_KEYS, partition_id);
        // Fixed-size param in initial frame: batch (int) ONLY
        if let Some(initial_frame) = message.frames_mut().first_mut() {
            use bytes::BufMut;
            initial_frame.content.put_i32_le(self.batch_size);
        }
        message.add_frame(Self::string_frame(&self.map_name));
        // iterationPointers: list of (tableIndex, tableSize) pairs
        // For initial iteration: tableIndex=table_index, tableSize=0
        // For subsequent: use values from response iterationPointers
        message.add_frame(Frame::with_flags(BEGIN_DATA_STRUCTURE_FLAG));
        {
            let mut buf = BytesMut::with_capacity(8);
            buf.extend_from_slice(&table_index.to_le_bytes()); // tableIndex (-1 = start)
            buf.extend_from_slice(&0i32.to_le_bytes()); // tableSize (0 = unknown)
            message.add_frame(Frame::with_content(buf));
        }
        message.add_frame(Frame::with_flags(END_DATA_STRUCTURE_FLAG));

        let response = self.invoke(partition_id, message).await?;
        Self::decode_keys_response(&response)
    }

    fn decode_keys_response(response: &ClientMessage) -> Result<(Vec<K>, i32)> {
        let frames = response.frames();
        if frames.is_empty() {
            return Ok((Vec::new(), -1));
        }

        // Extract next table index from iterationPointers section
        // The iterationPointers is an EntryListIntegerIntegerCodec:
        // BEGIN, [frame with 8 bytes: partition(i32) + tableIndex(i32)], END
        // We extract the tableIndex from the first entry.
        let mut next_table_index: i32 = -1;
        let mut begin_seen = false;
        for frame in frames.iter().skip(1) {
            if frame.flags & BEGIN_DATA_STRUCTURE_FLAG != 0 {
                if !begin_seen {
                    begin_seen = true;
                    continue;
                }
                break; // second BEGIN = start of keys section
            }
            if frame.flags & END_DATA_STRUCTURE_FLAG != 0 {
                break; // end of iterationPointers
            }
            // Each entry in iterationPointers: 8 bytes = partition(i32) + tableIndex(i32)
            if begin_seen && frame.content.len() >= 8 {
                next_table_index = i32::from_le_bytes([
                    frame.content[4], frame.content[5],
                    frame.content[6], frame.content[7],
                ]);
            }
        }

        // Response structure (MapFetchKeysCodec):
        // [0] Initial frame (response header, empty)
        // [1..] iterationPointers: EntryListIntegerIntegerCodec (BEGIN, int pairs, END)
        // [...] keys: ListMultiFrameCodec (BEGIN, Data key frames, END)
        //
        // We need to skip the iterationPointers section and only read the keys section.
        // Count BEGIN/END pairs: first pair is iterationPointers, second is keys.
        let mut keys = Vec::new();
        let mut begin_count = 0;
        let mut in_keys_section = false;

        for frame in frames.iter().skip(1) {
            if frame.flags & BEGIN_DATA_STRUCTURE_FLAG != 0 {
                begin_count += 1;
                if begin_count == 2 {
                    in_keys_section = true; // second BEGIN = keys list
                }
                continue;
            }
            if frame.flags & END_DATA_STRUCTURE_FLAG != 0 {
                if in_keys_section {
                    break; // end of keys list
                }
                continue;
            }
            if !in_keys_section {
                continue; // skip iterationPointers content
            }
            if frame.flags & IS_NULL_FLAG != 0 || frame.content.len() <= 8 {
                continue;
            }

            // Skip 8-byte Data header (partition_hash + type_id)
            let payload = &frame.content[8..];
            let mut input = ObjectDataInput::new(payload);
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
            // Continue fetching from this partition if we got data AND the
            // server indicated more data (next_index > table_index or >= 0).
            // If keys is empty, this partition has no (more) data.
            cursor.has_more = !keys.is_empty();

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
        // Fixed-size param: batch (int) ONLY
        if let Some(initial_frame) = message.frames_mut().first_mut() {
            use bytes::BufMut;
            initial_frame.content.put_i32_le(self.batch_size);
        }
        message.add_frame(Self::string_frame(&self.map_name));
        // iterationPointers: [partition_id -> table_index]
        message.add_frame(Frame::with_flags(BEGIN_DATA_STRUCTURE_FLAG));
        {
            let mut buf = BytesMut::with_capacity(8);
            buf.extend_from_slice(&partition_id.to_le_bytes());
            buf.extend_from_slice(&table_index.to_le_bytes());
            message.add_frame(Frame::with_content(buf));
        }
        message.add_frame(Frame::with_flags(END_DATA_STRUCTURE_FLAG));

        let response = self.invoke(partition_id, message).await?;
        Self::decode_entries_response(&response)
    }

    fn decode_entries_response(response: &ClientMessage) -> Result<(Vec<(K, V)>, i32)> {
        let frames = response.frames();
        if frames.is_empty() {
            return Ok((Vec::new(), -1));
        }

        // Extract next table index from iterationPointers section
        let mut next_table_index: i32 = -1;
        let mut ip_begin = false;
        for frame in frames.iter().skip(1) {
            if frame.flags & BEGIN_DATA_STRUCTURE_FLAG != 0 {
                if !ip_begin { ip_begin = true; continue; }
                break;
            }
            if frame.flags & END_DATA_STRUCTURE_FLAG != 0 { break; }
            if ip_begin && frame.content.len() >= 8 {
                next_table_index = i32::from_le_bytes([
                    frame.content[4], frame.content[5],
                    frame.content[6], frame.content[7],
                ]);
            }
        }

        // Response structure (MapFetchEntriesCodec):
        // [0] Initial frame (response header)
        // [...] iterationPointers: EntryListIntegerIntegerCodec (BEGIN, int pairs, END)
        // [...] entries: EntryListCodec (BEGIN, [key, val, key, val...], END)
        let mut entries = Vec::new();
        let mut begin_count = 0u32;
        let mut in_entries = false;
        let mut data_frames = Vec::new();
        for frame in frames.iter().skip(1) {
            if frame.flags & BEGIN_DATA_STRUCTURE_FLAG != 0 {
                begin_count += 1;
                if begin_count == 2 { in_entries = true; }
                continue;
            }
            if frame.flags & END_DATA_STRUCTURE_FLAG != 0 {
                if in_entries { break; }
                continue;
            }
            if !in_entries { continue; }
            if frame.flags & IS_NULL_FLAG == 0 && frame.content.len() > 8 {
                data_frames.push(frame);
            }
        }

        let mut i = 0;
        while i + 1 < data_frames.len() {
            let key_frame = data_frames[i];
            let value_frame = data_frames[i + 1];

            if key_frame.flags & END_FLAG != 0 && key_frame.content.is_empty() {
                break;
            }

            // Skip 8-byte Data header (partition_hash + type_id) on both key and value
            let key_payload = &key_frame.content[8..];
            let value_payload = &value_frame.content[8..];
            let mut key_input = ObjectDataInput::new(key_payload);
            let mut value_input = ObjectDataInput::new(value_payload);

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
            cursor.has_more = !entries.is_empty();

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
