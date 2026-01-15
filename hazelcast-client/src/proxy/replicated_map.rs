//! Distributed ReplicatedMap implementation.

use std::marker::PhantomData;
use std::sync::Arc;

use hazelcast_core::protocol::constants::{
    REPLICATED_MAP_CLEAR, REPLICATED_MAP_CONTAINS_KEY, REPLICATED_MAP_CONTAINS_VALUE,
    REPLICATED_MAP_ENTRY_SET, REPLICATED_MAP_GET, REPLICATED_MAP_IS_EMPTY, REPLICATED_MAP_KEY_SET,
    REPLICATED_MAP_PUT, REPLICATED_MAP_REMOVE, REPLICATED_MAP_SIZE, REPLICATED_MAP_VALUES,
};
use hazelcast_core::{Deserializable, Result, Serializable};

use crate::connection::ConnectionManager;

/// A distributed, eventually-consistent map with full replication to all members.
///
/// Unlike `IMap`, data is replicated to all cluster members, providing
/// faster reads at the cost of higher memory usage and eventual consistency.
///
/// # Type Parameters
///
/// - `K`: The key type, must implement `Serializable`, `Deserializable`, `Send`, and `Sync`
/// - `V`: The value type, must implement `Serializable`, `Deserializable`, `Send`, and `Sync`
///
/// # Example
///
/// ```ignore
/// let replicated_map = client.get_replicated_map::<String, String>("my-replicated-map");
/// replicated_map.put("key".to_string(), "value".to_string()).await?;
/// let value = replicated_map.get(&"key".to_string()).await?;
/// ```
#[derive(Debug)]
pub struct ReplicatedMap<K, V> {
    name: String,
    connection_manager: Arc<ConnectionManager>,
    _marker: PhantomData<(K, V)>,
}

impl<K, V> ReplicatedMap<K, V>
where
    K: Serializable + Deserializable + Send + Sync,
    V: Serializable + Deserializable + Send + Sync,
{
    /// Creates a new ReplicatedMap proxy.
    pub(crate) fn new(name: String, connection_manager: Arc<ConnectionManager>) -> Self {
        Self {
            name,
            connection_manager,
            _marker: PhantomData,
        }
    }

    /// Returns the name of this replicated map.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Associates the specified value with the specified key in this map.
    ///
    /// Returns the previous value associated with the key, or `None` if there was no mapping.
    pub async fn put(&self, key: K, value: V) -> Result<Option<V>> {
        self.connection_manager
            .invoke_on_random(REPLICATED_MAP_PUT, &self.name, &(key, value))
            .await
    }

    /// Returns the value associated with the specified key, or `None` if no mapping exists.
    pub async fn get(&self, key: &K) -> Result<Option<V>> {
        self.connection_manager
            .invoke_on_random(REPLICATED_MAP_GET, &self.name, key)
            .await
    }

    /// Removes the mapping for a key from this map if present.
    ///
    /// Returns the previous value associated with the key, or `None` if there was no mapping.
    pub async fn remove(&self, key: &K) -> Result<Option<V>> {
        self.connection_manager
            .invoke_on_random(REPLICATED_MAP_REMOVE, &self.name, key)
            .await
    }

    /// Returns `true` if this map contains a mapping for the specified key.
    pub async fn contains_key(&self, key: &K) -> Result<bool> {
        self.connection_manager
            .invoke_on_random(REPLICATED_MAP_CONTAINS_KEY, &self.name, key)
            .await
    }

    /// Returns `true` if this map contains one or more mappings to the specified value.
    pub async fn contains_value(&self, value: &V) -> Result<bool> {
        self.connection_manager
            .invoke_on_random(REPLICATED_MAP_CONTAINS_VALUE, &self.name, value)
            .await
    }

    /// Returns the number of key-value mappings in this map.
    pub async fn size(&self) -> Result<i32> {
        self.connection_manager
            .invoke_on_random(REPLICATED_MAP_SIZE, &self.name, &())
            .await
    }

    /// Returns `true` if this map contains no key-value mappings.
    pub async fn is_empty(&self) -> Result<bool> {
        self.connection_manager
            .invoke_on_random(REPLICATED_MAP_IS_EMPTY, &self.name, &())
            .await
    }

    /// Removes all key-value mappings from this map.
    pub async fn clear(&self) -> Result<()> {
        self.connection_manager
            .invoke_on_random(REPLICATED_MAP_CLEAR, &self.name, &())
            .await
    }

    /// Returns a collection view of the keys contained in this map.
    pub async fn key_set(&self) -> Result<Vec<K>> {
        self.connection_manager
            .invoke_on_random(REPLICATED_MAP_KEY_SET, &self.name, &())
            .await
    }

    /// Returns a collection view of the values contained in this map.
    pub async fn values(&self) -> Result<Vec<V>> {
        self.connection_manager
            .invoke_on_random(REPLICATED_MAP_VALUES, &self.name, &())
            .await
    }

    /// Returns a collection view of the mappings contained in this map.
    pub async fn entry_set(&self) -> Result<Vec<(K, V)>> {
        self.connection_manager
            .invoke_on_random(REPLICATED_MAP_ENTRY_SET, &self.name, &())
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_replicated_map_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<ReplicatedMap<String, String>>();
    }

    #[test]
    fn test_replicated_map_with_various_types() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<ReplicatedMap<i32, i64>>();
        assert_send_sync::<ReplicatedMap<String, Vec<u8>>>();
        assert_send_sync::<ReplicatedMap<u64, bool>>();
    }
}
