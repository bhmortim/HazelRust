//! Hazelcast client entry point.

use std::sync::Arc;

use hazelcast_core::{Deserializable, Result, Serializable};

use crate::config::ClientConfig;
use crate::connection::ConnectionManager;
use crate::proxy::{IMap, IQueue};

/// The main entry point for connecting to a Hazelcast cluster.
///
/// `HazelcastClient` manages connections to cluster members and provides
/// access to distributed data structures.
///
/// # Example
///
/// ```ignore
/// use hazelcast_client::{ClientConfig, HazelcastClient};
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let config = ClientConfig::builder()
///         .cluster_name("dev")
///         .build()?;
///
///     let client = HazelcastClient::new(config).await?;
///     let map = client.get_map::<String, String>("my-map");
///
///     map.put("key".to_string(), "value".to_string()).await?;
///     let value = map.get(&"key".to_string()).await?;
///
///     client.shutdown().await?;
///     Ok(())
/// }
/// ```
#[derive(Debug)]
pub struct HazelcastClient {
    config: Arc<ClientConfig>,
    connection_manager: Arc<ConnectionManager>,
}

impl HazelcastClient {
    /// Creates a new client and connects to the Hazelcast cluster.
    ///
    /// This method establishes connections to cluster members based on the
    /// provided configuration. It will fail if no connections can be established.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - No cluster addresses are configured
    /// - All connection attempts fail
    /// - Network errors occur during connection
    pub async fn new(config: ClientConfig) -> Result<Self> {
        let connection_manager = ConnectionManager::from_config(config.clone());
        connection_manager.start().await?;

        tracing::info!(
            cluster = %config.cluster_name(),
            "connected to Hazelcast cluster"
        );

        Ok(Self {
            config: Arc::new(config),
            connection_manager: Arc::new(connection_manager),
        })
    }

    /// Returns a distributed map proxy for the given name.
    ///
    /// The map proxy allows performing key-value operations on the cluster.
    /// The actual map is created on the cluster lazily when first accessed.
    ///
    /// # Type Parameters
    ///
    /// - `K`: The key type, must implement `Serializable`, `Deserializable`, `Send`, and `Sync`
    /// - `V`: The value type, must implement `Serializable`, `Deserializable`, `Send`, and `Sync`
    pub fn get_map<K, V>(&self, name: &str) -> IMap<K, V>
    where
        K: Serializable + Deserializable + Send + Sync,
        V: Serializable + Deserializable + Send + Sync,
    {
        IMap::new(name.to_string(), Arc::clone(&self.connection_manager))
    }

    /// Returns a distributed queue proxy for the given name.
    ///
    /// The queue proxy allows performing FIFO operations on the cluster.
    /// The actual queue is created on the cluster lazily when first accessed.
    ///
    /// # Type Parameters
    ///
    /// - `T`: The element type, must implement `Serializable`, `Deserializable`, `Send`, and `Sync`
    pub fn get_queue<T>(&self, name: &str) -> IQueue<T>
    where
        T: Serializable + Deserializable + Send + Sync,
    {
        IQueue::new(name.to_string(), Arc::clone(&self.connection_manager))
    }

    /// Returns the cluster name this client is connected to.
    pub fn cluster_name(&self) -> &str {
        self.config.cluster_name()
    }

    /// Returns the client configuration.
    pub fn config(&self) -> &ClientConfig {
        &self.config
    }

    /// Returns the number of active connections to cluster members.
    pub async fn connection_count(&self) -> usize {
        self.connection_manager.connection_count().await
    }

    /// Shuts down the client and closes all connections.
    ///
    /// After shutdown, the client cannot be used for any operations.
    pub async fn shutdown(&self) -> Result<()> {
        tracing::info!(
            cluster = %self.config.cluster_name(),
            "shutting down Hazelcast client"
        );
        self.connection_manager.shutdown().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<HazelcastClient>();
    }
}
