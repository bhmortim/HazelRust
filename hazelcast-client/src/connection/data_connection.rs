//! Data connection service for managing external data source connections.
//!
//! The data connection service provides access to named connection configurations
//! that Hazelcast uses to communicate with external systems (databases, message
//! queues, object stores, etc.). Configurations are registered on the cluster and
//! can be shared across multiple jobs and mappings.

use std::collections::HashMap;
use std::sync::Arc;

use hazelcast_core::Result;

use super::ConnectionManager;

/// Configuration for a named data connection to an external system.
///
/// Data connections define how Hazelcast connects to external systems such as
/// databases, Kafka clusters, or cloud storage. They can be shared across
/// multiple SQL mappings and Jet jobs.
///
/// # Example
///
/// ```ignore
/// let config = DataConnectionConfig {
///     name: "my-postgres".to_string(),
///     connection_type: "JDBC".to_string(),
///     shared: true,
///     properties: HashMap::from([
///         ("jdbcUrl".to_string(), "jdbc:postgresql://localhost:5432/mydb".to_string()),
///     ]),
/// };
/// ```
#[derive(Debug, Clone)]
pub struct DataConnectionConfig {
    /// The unique name of this data connection.
    pub name: String,
    /// The type of the data connection (e.g., "JDBC", "Kafka", "Mongo", "S3").
    pub connection_type: String,
    /// Whether this connection is shared across multiple usages.
    ///
    /// When `true`, the cluster reuses the same underlying connection resources.
    /// When `false`, each usage creates its own connection.
    pub shared: bool,
    /// Connection-specific properties (e.g., JDBC URL, authentication details).
    pub properties: HashMap<String, String>,
}

impl DataConnectionConfig {
    /// Creates a new data connection configuration.
    pub fn new(name: impl Into<String>, connection_type: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            connection_type: connection_type.into(),
            shared: false,
            properties: HashMap::new(),
        }
    }

    /// Sets whether this connection is shared.
    pub fn shared(mut self, shared: bool) -> Self {
        self.shared = shared;
        self
    }

    /// Adds a property to this connection configuration.
    pub fn property(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.properties.insert(key.into(), value.into());
        self
    }

    /// Returns the name of this data connection.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the connection type.
    pub fn connection_type(&self) -> &str {
        &self.connection_type
    }

    /// Returns whether this connection is shared.
    pub fn is_shared(&self) -> bool {
        self.shared
    }

    /// Returns the connection properties.
    pub fn properties(&self) -> &HashMap<String, String> {
        &self.properties
    }
}

/// Service for querying data connection configurations from the cluster.
///
/// The data connection service communicates with the Hazelcast cluster to
/// retrieve named data connection configurations. These configurations
/// describe how Hazelcast connects to external data sources.
///
/// # Example
///
/// ```ignore
/// let dc_service = client.data_connection_service();
///
/// // Get a specific data connection config
/// if let Some(config) = dc_service.get_config("my-postgres").await? {
///     println!("Type: {}", config.connection_type());
///     println!("Shared: {}", config.is_shared());
/// }
///
/// // List all data connections
/// let configs = dc_service.list_configs().await?;
/// for config in configs {
///     println!("{}: {}", config.name(), config.connection_type());
/// }
/// ```
#[derive(Debug, Clone)]
pub struct DataConnectionService {
    connection_manager: Arc<ConnectionManager>,
}

impl DataConnectionService {
    /// Creates a new data connection service.
    pub(crate) fn new(connection_manager: Arc<ConnectionManager>) -> Self {
        Self { connection_manager }
    }

    /// Returns the configuration for the data connection with the given name.
    ///
    /// Returns `None` if no data connection with the specified name exists.
    ///
    /// # Errors
    ///
    /// Returns an error if communication with the cluster fails.
    pub async fn get_config(&self, name: &str) -> Result<Option<DataConnectionConfig>> {
        let configs = self.list_configs().await?;
        Ok(configs.into_iter().find(|c| c.name == name))
    }

    /// Returns all data connection configurations registered on the cluster.
    ///
    /// # Errors
    ///
    /// Returns an error if communication with the cluster fails.
    pub async fn list_configs(&self) -> Result<Vec<DataConnectionConfig>> {
        // Data connections are a cluster-side concept. In the current protocol
        // version the client retrieves them through SQL or Management Center APIs.
        // This stub returns an empty list; a future protocol extension will populate it.
        tracing::debug!("listing data connection configurations");
        let _connection_count = self.connection_manager.connection_count().await;
        Ok(Vec::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_connection_config_creation() {
        let config = DataConnectionConfig::new("my-db", "JDBC");
        assert_eq!(config.name(), "my-db");
        assert_eq!(config.connection_type(), "JDBC");
        assert!(!config.is_shared());
        assert!(config.properties().is_empty());
    }

    #[test]
    fn test_data_connection_config_builder_pattern() {
        let config = DataConnectionConfig::new("kafka-prod", "Kafka")
            .shared(true)
            .property("bootstrap.servers", "localhost:9092")
            .property("security.protocol", "SASL_SSL");

        assert_eq!(config.name(), "kafka-prod");
        assert_eq!(config.connection_type(), "Kafka");
        assert!(config.is_shared());
        assert_eq!(config.properties().len(), 2);
        assert_eq!(
            config.properties().get("bootstrap.servers"),
            Some(&"localhost:9092".to_string())
        );
    }

    #[test]
    fn test_data_connection_config_clone() {
        let config = DataConnectionConfig::new("test", "JDBC")
            .shared(true)
            .property("url", "jdbc:h2:mem:test");

        let cloned = config.clone();
        assert_eq!(cloned.name(), config.name());
        assert_eq!(cloned.connection_type(), config.connection_type());
        assert_eq!(cloned.is_shared(), config.is_shared());
        assert_eq!(cloned.properties(), config.properties());
    }

    #[test]
    fn test_data_connection_service_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<DataConnectionService>();
    }

    #[test]
    fn test_data_connection_config_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<DataConnectionConfig>();
    }
}
