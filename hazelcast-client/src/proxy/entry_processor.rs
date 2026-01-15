//! Entry processor support for distributed map operations.

use std::collections::HashMap;
use std::hash::Hash;

use hazelcast_core::serialization::{Deserializable, ObjectDataInput, ObjectDataOutput, Serializable};
use hazelcast_core::{HazelcastError, Result};

/// A processor that can be executed on map entries server-side.
///
/// Entry processors allow atomic read-modify-write operations on map entries
/// without requiring the data to be transferred to the client. The processor
/// is serialized and sent to the cluster where it executes on the entry.
///
/// # Type Parameters
///
/// - `Output`: The result type returned after processing an entry.
///
/// # Example
///
/// ```ignore
/// use hazelcast_client::proxy::EntryProcessor;
/// use hazelcast_core::serialization::{ObjectDataOutput, ObjectDataInput, Serializable, Deserializable};
/// use hazelcast_core::Result;
///
/// struct IncrementProcessor {
///     delta: i64,
/// }
///
/// impl EntryProcessor for IncrementProcessor {
///     type Output = i64;
/// }
///
/// impl Serializable for IncrementProcessor {
///     fn serialize(&self, output: &mut ObjectDataOutput) -> Result<()> {
///         output.write_i64(self.delta)?;
///         Ok(())
///     }
/// }
/// ```
pub trait EntryProcessor: Serializable + Send + Sync {
    /// The type of result returned when this processor is executed on an entry.
    type Output: Deserializable;
}

/// Result of executing an entry processor on multiple entries.
#[derive(Debug, Clone)]
pub struct EntryProcessorResult<K, R> {
    results: HashMap<K, R>,
}

impl<K, R> EntryProcessorResult<K, R>
where
    K: Eq + Hash,
{
    /// Creates a new entry processor result from a map of results.
    pub fn new(results: HashMap<K, R>) -> Self {
        Self { results }
    }

    /// Returns the result for a specific key, if present.
    pub fn get(&self, key: &K) -> Option<&R> {
        self.results.get(key)
    }

    /// Returns the number of entries that were processed.
    pub fn len(&self) -> usize {
        self.results.len()
    }

    /// Returns `true` if no entries were processed.
    pub fn is_empty(&self) -> bool {
        self.results.is_empty()
    }

    /// Consumes this result and returns the underlying map.
    pub fn into_inner(self) -> HashMap<K, R> {
        self.results
    }

    /// Returns an iterator over the key-result pairs.
    pub fn iter(&self) -> impl Iterator<Item = (&K, &R)> {
        self.results.iter()
    }
}

impl<K, R> IntoIterator for EntryProcessorResult<K, R> {
    type Item = (K, R);
    type IntoIter = std::collections::hash_map::IntoIter<K, R>;

    fn into_iter(self) -> Self::IntoIter {
        self.results.into_iter()
    }
}

impl<K, R> Default for EntryProcessorResult<K, R>
where
    K: Eq + Hash,
{
    fn default() -> Self {
        Self {
            results: HashMap::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestProcessor {
        value: i32,
    }

    impl EntryProcessor for TestProcessor {
        type Output = i32;
    }

    impl Serializable for TestProcessor {
        fn serialize(&self, output: &mut ObjectDataOutput) -> Result<()> {
            output.write_i32(self.value)?;
            Ok(())
        }
    }

    #[test]
    fn test_entry_processor_trait_bounds() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<TestProcessor>();
    }

    #[test]
    fn test_entry_processor_serialization() {
        let processor = TestProcessor { value: 42 };
        let mut output = ObjectDataOutput::new();
        processor.serialize(&mut output).unwrap();
        let bytes = output.into_bytes();
        assert!(!bytes.is_empty());
    }

    #[test]
    fn test_entry_processor_result_new() {
        let mut map = HashMap::new();
        map.insert("key1".to_string(), 10);
        map.insert("key2".to_string(), 20);

        let result = EntryProcessorResult::new(map);
        assert_eq!(result.len(), 2);
        assert!(!result.is_empty());
    }

    #[test]
    fn test_entry_processor_result_get() {
        let mut map = HashMap::new();
        map.insert("key1".to_string(), 10);

        let result = EntryProcessorResult::new(map);
        assert_eq!(result.get(&"key1".to_string()), Some(&10));
        assert_eq!(result.get(&"key2".to_string()), None);
    }

    #[test]
    fn test_entry_processor_result_into_inner() {
        let mut map = HashMap::new();
        map.insert("key1".to_string(), 10);

        let result = EntryProcessorResult::new(map);
        let inner = result.into_inner();
        assert_eq!(inner.get("key1"), Some(&10));
    }

    #[test]
    fn test_entry_processor_result_iter() {
        let mut map = HashMap::new();
        map.insert("key1".to_string(), 10);
        map.insert("key2".to_string(), 20);

        let result = EntryProcessorResult::new(map);
        let collected: Vec<_> = result.iter().collect();
        assert_eq!(collected.len(), 2);
    }

    #[test]
    fn test_entry_processor_result_into_iter() {
        let mut map = HashMap::new();
        map.insert("key1".to_string(), 10);

        let result = EntryProcessorResult::new(map);
        let collected: Vec<_> = result.into_iter().collect();
        assert_eq!(collected.len(), 1);
    }

    #[test]
    fn test_entry_processor_result_default() {
        let result: EntryProcessorResult<String, i32> = EntryProcessorResult::default();
        assert!(result.is_empty());
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_entry_processor_result_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<EntryProcessorResult<String, i32>>();
    }
}
