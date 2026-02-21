//! Kafka connectors for Jet streaming pipelines.
//!
//! This module provides Kafka source and sink implementations for reading from
//! and writing to Apache Kafka topics in Jet pipelines.
//!
//! # Features
//!
//! - Topic subscription (single topic or pattern-based)
//! - Consumer group configuration for coordinated consumption
//! - At-least-once delivery semantics
//! - Configurable auto offset reset behavior
//! - Key and value serialization/deserialization
//!
//! # Example
//!
//! ```ignore
//! use hazelcast_client::jet::{Pipeline, kafka_source, kafka_sink};
//! use hazelcast_client::jet::connectors::kafka::{KafkaSourceConfig, KafkaSinkConfig};
//!
//! let source_config = KafkaSourceConfig::builder()
//!     .bootstrap_servers("localhost:9092")
//!     .topic("input-topic")
//!     .group_id("my-consumer-group")
//!     .build();
//!
//! let sink_config = KafkaSinkConfig::builder()
//!     .bootstrap_servers("localhost:9092")
//!     .topic("output-topic")
//!     .build();
//!
//! let pipeline = Pipeline::builder()
//!     .read_from(kafka_source(source_config))
//!     .map("process")
//!     .write_to(kafka_sink(sink_config))
//!     .build();
//! ```

use std::collections::HashMap;
use std::fmt;

use crate::jet::pipeline::{Sink, Source};

/// Auto offset reset behavior when no committed offset exists.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum AutoOffsetReset {
    /// Start from the earliest available offset.
    Earliest,
    /// Start from the latest offset (default).
    #[default]
    Latest,
    /// Fail if no offset is found.
    None,
}

impl fmt::Display for AutoOffsetReset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AutoOffsetReset::Earliest => write!(f, "earliest"),
            AutoOffsetReset::Latest => write!(f, "latest"),
            AutoOffsetReset::None => write!(f, "none"),
        }
    }
}

/// Isolation level for reading transactional messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum IsolationLevel {
    /// Read all messages including uncommitted transactional messages.
    ReadUncommitted,
    /// Only read committed messages (default for at-least-once semantics).
    #[default]
    ReadCommitted,
}

impl fmt::Display for IsolationLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IsolationLevel::ReadUncommitted => write!(f, "read_uncommitted"),
            IsolationLevel::ReadCommitted => write!(f, "read_committed"),
        }
    }
}

/// Acknowledgment level for producer messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum Acks {
    /// No acknowledgment (fire and forget).
    None,
    /// Leader acknowledgment only.
    Leader,
    /// All in-sync replicas must acknowledge (default for at-least-once).
    #[default]
    All,
}

impl fmt::Display for Acks {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Acks::None => write!(f, "0"),
            Acks::Leader => write!(f, "1"),
            Acks::All => write!(f, "all"),
        }
    }
}

/// Compression type for producer messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum CompressionType {
    /// No compression.
    #[default]
    None,
    /// GZIP compression.
    Gzip,
    /// Snappy compression.
    Snappy,
    /// LZ4 compression.
    Lz4,
    /// Zstandard compression.
    Zstd,
}

impl fmt::Display for CompressionType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CompressionType::None => write!(f, "none"),
            CompressionType::Gzip => write!(f, "gzip"),
            CompressionType::Snappy => write!(f, "snappy"),
            CompressionType::Lz4 => write!(f, "lz4"),
            CompressionType::Zstd => write!(f, "zstd"),
        }
    }
}

/// Configuration for a Kafka source connector.
#[derive(Debug, Clone)]
pub struct KafkaSourceConfig {
    bootstrap_servers: String,
    topics: Vec<String>,
    group_id: String,
    auto_offset_reset: AutoOffsetReset,
    enable_auto_commit: bool,
    isolation_level: IsolationLevel,
    max_poll_records: u32,
    session_timeout_ms: u32,
    heartbeat_interval_ms: u32,
    properties: HashMap<String, String>,
}

impl KafkaSourceConfig {
    /// Creates a new builder for KafkaSourceConfig.
    pub fn builder() -> KafkaSourceConfigBuilder {
        KafkaSourceConfigBuilder::new()
    }

    /// Returns the bootstrap servers.
    pub fn bootstrap_servers(&self) -> &str {
        &self.bootstrap_servers
    }

    /// Returns the topics to consume from.
    pub fn topics(&self) -> &[String] {
        &self.topics
    }

    /// Returns the consumer group ID.
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// Returns the auto offset reset behavior.
    pub fn auto_offset_reset(&self) -> AutoOffsetReset {
        self.auto_offset_reset
    }

    /// Returns whether auto commit is enabled.
    pub fn enable_auto_commit(&self) -> bool {
        self.enable_auto_commit
    }

    /// Returns the isolation level.
    pub fn isolation_level(&self) -> IsolationLevel {
        self.isolation_level
    }

    /// Returns the maximum number of records to poll at once.
    pub fn max_poll_records(&self) -> u32 {
        self.max_poll_records
    }

    /// Returns the session timeout in milliseconds.
    pub fn session_timeout_ms(&self) -> u32 {
        self.session_timeout_ms
    }

    /// Returns the heartbeat interval in milliseconds.
    pub fn heartbeat_interval_ms(&self) -> u32 {
        self.heartbeat_interval_ms
    }

    /// Returns additional Kafka properties.
    pub fn properties(&self) -> &HashMap<String, String> {
        &self.properties
    }
}

/// Builder for KafkaSourceConfig.
#[derive(Debug, Clone, Default)]
pub struct KafkaSourceConfigBuilder {
    bootstrap_servers: Option<String>,
    topics: Vec<String>,
    group_id: Option<String>,
    auto_offset_reset: AutoOffsetReset,
    enable_auto_commit: bool,
    isolation_level: IsolationLevel,
    max_poll_records: u32,
    session_timeout_ms: u32,
    heartbeat_interval_ms: u32,
    properties: HashMap<String, String>,
}

impl KafkaSourceConfigBuilder {
    /// Creates a new builder with default values.
    pub fn new() -> Self {
        Self {
            bootstrap_servers: None,
            topics: Vec::new(),
            group_id: None,
            auto_offset_reset: AutoOffsetReset::Latest,
            enable_auto_commit: false,
            isolation_level: IsolationLevel::ReadCommitted,
            max_poll_records: 500,
            session_timeout_ms: 30000,
            heartbeat_interval_ms: 3000,
            properties: HashMap::new(),
        }
    }

    /// Sets the bootstrap servers (comma-separated list).
    pub fn bootstrap_servers(mut self, servers: impl Into<String>) -> Self {
        self.bootstrap_servers = Some(servers.into());
        self
    }

    /// Sets a single topic to consume from.
    pub fn topic(mut self, topic: impl Into<String>) -> Self {
        self.topics = vec![topic.into()];
        self
    }

    /// Sets multiple topics to consume from.
    pub fn topics(mut self, topics: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.topics = topics.into_iter().map(|t| t.into()).collect();
        self
    }

    /// Sets the consumer group ID.
    pub fn group_id(mut self, group_id: impl Into<String>) -> Self {
        self.group_id = Some(group_id.into());
        self
    }

    /// Sets the auto offset reset behavior.
    pub fn auto_offset_reset(mut self, reset: AutoOffsetReset) -> Self {
        self.auto_offset_reset = reset;
        self
    }

    /// Enables or disables auto commit.
    /// Note: Auto commit is disabled by default for at-least-once semantics.
    pub fn enable_auto_commit(mut self, enable: bool) -> Self {
        self.enable_auto_commit = enable;
        self
    }

    /// Sets the isolation level for transactional messages.
    pub fn isolation_level(mut self, level: IsolationLevel) -> Self {
        self.isolation_level = level;
        self
    }

    /// Sets the maximum number of records to poll at once.
    pub fn max_poll_records(mut self, max: u32) -> Self {
        self.max_poll_records = max;
        self
    }

    /// Sets the session timeout in milliseconds.
    pub fn session_timeout_ms(mut self, timeout: u32) -> Self {
        self.session_timeout_ms = timeout;
        self
    }

    /// Sets the heartbeat interval in milliseconds.
    pub fn heartbeat_interval_ms(mut self, interval: u32) -> Self {
        self.heartbeat_interval_ms = interval;
        self
    }

    /// Adds a custom Kafka property.
    pub fn property(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.properties.insert(key.into(), value.into());
        self
    }

    /// Adds multiple custom Kafka properties.
    pub fn properties(
        mut self,
        props: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    ) -> Self {
        for (k, v) in props {
            self.properties.insert(k.into(), v.into());
        }
        self
    }

    /// Builds the KafkaSourceConfig.
    ///
    /// # Panics
    ///
    /// Panics if bootstrap_servers, topics, or group_id are not set.
    pub fn build(self) -> KafkaSourceConfig {
        KafkaSourceConfig {
            bootstrap_servers: self
                .bootstrap_servers
                .expect("bootstrap_servers is required"),
            topics: if self.topics.is_empty() {
                panic!("at least one topic is required")
            } else {
                self.topics
            },
            group_id: self.group_id.expect("group_id is required"),
            auto_offset_reset: self.auto_offset_reset,
            enable_auto_commit: self.enable_auto_commit,
            isolation_level: self.isolation_level,
            max_poll_records: self.max_poll_records,
            session_timeout_ms: self.session_timeout_ms,
            heartbeat_interval_ms: self.heartbeat_interval_ms,
            properties: self.properties,
        }
    }
}

/// Configuration for a Kafka sink connector.
#[derive(Debug, Clone)]
pub struct KafkaSinkConfig {
    bootstrap_servers: String,
    topic: String,
    acks: Acks,
    retries: u32,
    retry_backoff_ms: u32,
    batch_size: u32,
    linger_ms: u32,
    buffer_memory: u64,
    compression_type: CompressionType,
    idempotence: bool,
    transactional_id: Option<String>,
    properties: HashMap<String, String>,
}

impl KafkaSinkConfig {
    /// Creates a new builder for KafkaSinkConfig.
    pub fn builder() -> KafkaSinkConfigBuilder {
        KafkaSinkConfigBuilder::new()
    }

    /// Returns the bootstrap servers.
    pub fn bootstrap_servers(&self) -> &str {
        &self.bootstrap_servers
    }

    /// Returns the topic to produce to.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the acknowledgment level.
    pub fn acks(&self) -> Acks {
        self.acks
    }

    /// Returns the number of retries.
    pub fn retries(&self) -> u32 {
        self.retries
    }

    /// Returns the retry backoff in milliseconds.
    pub fn retry_backoff_ms(&self) -> u32 {
        self.retry_backoff_ms
    }

    /// Returns the batch size in bytes.
    pub fn batch_size(&self) -> u32 {
        self.batch_size
    }

    /// Returns the linger time in milliseconds.
    pub fn linger_ms(&self) -> u32 {
        self.linger_ms
    }

    /// Returns the buffer memory in bytes.
    pub fn buffer_memory(&self) -> u64 {
        self.buffer_memory
    }

    /// Returns the compression type.
    pub fn compression_type(&self) -> CompressionType {
        self.compression_type
    }

    /// Returns whether idempotence is enabled.
    pub fn idempotence(&self) -> bool {
        self.idempotence
    }

    /// Returns the transactional ID if set.
    pub fn transactional_id(&self) -> Option<&str> {
        self.transactional_id.as_deref()
    }

    /// Returns additional Kafka properties.
    pub fn properties(&self) -> &HashMap<String, String> {
        &self.properties
    }
}

/// Builder for KafkaSinkConfig.
#[derive(Debug, Clone, Default)]
pub struct KafkaSinkConfigBuilder {
    bootstrap_servers: Option<String>,
    topic: Option<String>,
    acks: Acks,
    retries: u32,
    retry_backoff_ms: u32,
    batch_size: u32,
    linger_ms: u32,
    buffer_memory: u64,
    compression_type: CompressionType,
    idempotence: bool,
    transactional_id: Option<String>,
    properties: HashMap<String, String>,
}

impl KafkaSinkConfigBuilder {
    /// Creates a new builder with default values.
    pub fn new() -> Self {
        Self {
            bootstrap_servers: None,
            topic: None,
            acks: Acks::All,
            retries: 3,
            retry_backoff_ms: 100,
            batch_size: 16384,
            linger_ms: 5,
            buffer_memory: 33554432,
            compression_type: CompressionType::None,
            idempotence: true,
            transactional_id: None,
            properties: HashMap::new(),
        }
    }

    /// Sets the bootstrap servers (comma-separated list).
    pub fn bootstrap_servers(mut self, servers: impl Into<String>) -> Self {
        self.bootstrap_servers = Some(servers.into());
        self
    }

    /// Sets the topic to produce to.
    pub fn topic(mut self, topic: impl Into<String>) -> Self {
        self.topic = Some(topic.into());
        self
    }

    /// Sets the acknowledgment level.
    pub fn acks(mut self, acks: Acks) -> Self {
        self.acks = acks;
        self
    }

    /// Sets the number of retries.
    pub fn retries(mut self, retries: u32) -> Self {
        self.retries = retries;
        self
    }

    /// Sets the retry backoff in milliseconds.
    pub fn retry_backoff_ms(mut self, backoff: u32) -> Self {
        self.retry_backoff_ms = backoff;
        self
    }

    /// Sets the batch size in bytes.
    pub fn batch_size(mut self, size: u32) -> Self {
        self.batch_size = size;
        self
    }

    /// Sets the linger time in milliseconds.
    pub fn linger_ms(mut self, linger: u32) -> Self {
        self.linger_ms = linger;
        self
    }

    /// Sets the buffer memory in bytes.
    pub fn buffer_memory(mut self, memory: u64) -> Self {
        self.buffer_memory = memory;
        self
    }

    /// Sets the compression type.
    pub fn compression_type(mut self, compression: CompressionType) -> Self {
        self.compression_type = compression;
        self
    }

    /// Enables or disables idempotent producer.
    pub fn idempotence(mut self, enable: bool) -> Self {
        self.idempotence = enable;
        self
    }

    /// Sets the transactional ID for exactly-once semantics.
    pub fn transactional_id(mut self, id: impl Into<String>) -> Self {
        self.transactional_id = Some(id.into());
        self
    }

    /// Adds a custom Kafka property.
    pub fn property(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.properties.insert(key.into(), value.into());
        self
    }

    /// Adds multiple custom Kafka properties.
    pub fn properties(
        mut self,
        props: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    ) -> Self {
        for (k, v) in props {
            self.properties.insert(k.into(), v.into());
        }
        self
    }

    /// Builds the KafkaSinkConfig.
    ///
    /// # Panics
    ///
    /// Panics if bootstrap_servers or topic are not set.
    pub fn build(self) -> KafkaSinkConfig {
        KafkaSinkConfig {
            bootstrap_servers: self
                .bootstrap_servers
                .expect("bootstrap_servers is required"),
            topic: self.topic.expect("topic is required"),
            acks: self.acks,
            retries: self.retries,
            retry_backoff_ms: self.retry_backoff_ms,
            batch_size: self.batch_size,
            linger_ms: self.linger_ms,
            buffer_memory: self.buffer_memory,
            compression_type: self.compression_type,
            idempotence: self.idempotence,
            transactional_id: self.transactional_id,
            properties: self.properties,
        }
    }
}

/// A Kafka source that consumes messages from Kafka topics.
#[derive(Debug, Clone)]
pub struct KafkaSource {
    config: KafkaSourceConfig,
}

impl KafkaSource {
    /// Creates a new Kafka source with the given configuration.
    pub fn new(config: KafkaSourceConfig) -> Self {
        Self { config }
    }

    /// Returns the configuration.
    pub fn config(&self) -> &KafkaSourceConfig {
        &self.config
    }
}

impl Source for KafkaSource {
    fn source_type(&self) -> &str {
        "kafka"
    }

    fn name(&self) -> &str {
        self.config
            .topics
            .first()
            .map(|s| s.as_str())
            .unwrap_or("kafka")
    }

    fn vertex_name(&self) -> String {
        let topics = self.config.topics.join(",");
        format!(
            "source:kafka:{}:{}:{}",
            self.config.bootstrap_servers, topics, self.config.group_id
        )
    }
}

/// A Kafka sink that produces messages to a Kafka topic.
#[derive(Debug, Clone)]
pub struct KafkaSink {
    config: KafkaSinkConfig,
}

impl KafkaSink {
    /// Creates a new Kafka sink with the given configuration.
    pub fn new(config: KafkaSinkConfig) -> Self {
        Self { config }
    }

    /// Returns the configuration.
    pub fn config(&self) -> &KafkaSinkConfig {
        &self.config
    }
}

impl Sink for KafkaSink {
    fn sink_type(&self) -> &str {
        "kafka"
    }

    fn name(&self) -> &str {
        &self.config.topic
    }

    fn vertex_name(&self) -> String {
        format!(
            "sink:kafka:{}:{}",
            self.config.bootstrap_servers, self.config.topic
        )
    }
}

/// Creates a Kafka source with the given configuration.
///
/// # Example
///
/// ```ignore
/// use hazelcast_client::jet::connectors::kafka::{kafka_source, KafkaSourceConfig};
///
/// let config = KafkaSourceConfig::builder()
///     .bootstrap_servers("localhost:9092")
///     .topic("my-topic")
///     .group_id("my-group")
///     .build();
///
/// let source = kafka_source(config);
/// ```
pub fn kafka_source(config: KafkaSourceConfig) -> KafkaSource {
    KafkaSource::new(config)
}

/// Creates a Kafka sink with the given configuration.
///
/// # Example
///
/// ```ignore
/// use hazelcast_client::jet::connectors::kafka::{kafka_sink, KafkaSinkConfig};
///
/// let config = KafkaSinkConfig::builder()
///     .bootstrap_servers("localhost:9092")
///     .topic("my-topic")
///     .build();
///
/// let sink = kafka_sink(config);
/// ```
pub fn kafka_sink(config: KafkaSinkConfig) -> KafkaSink {
    KafkaSink::new(config)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auto_offset_reset_default() {
        assert_eq!(AutoOffsetReset::default(), AutoOffsetReset::Latest);
    }

    #[test]
    fn test_auto_offset_reset_display() {
        assert_eq!(format!("{}", AutoOffsetReset::Earliest), "earliest");
        assert_eq!(format!("{}", AutoOffsetReset::Latest), "latest");
        assert_eq!(format!("{}", AutoOffsetReset::None), "none");
    }

    #[test]
    fn test_auto_offset_reset_clone() {
        let reset = AutoOffsetReset::Earliest;
        let cloned = reset.clone();
        assert_eq!(reset, cloned);
    }

    #[test]
    fn test_isolation_level_default() {
        assert_eq!(IsolationLevel::default(), IsolationLevel::ReadCommitted);
    }

    #[test]
    fn test_isolation_level_display() {
        assert_eq!(
            format!("{}", IsolationLevel::ReadUncommitted),
            "read_uncommitted"
        );
        assert_eq!(
            format!("{}", IsolationLevel::ReadCommitted),
            "read_committed"
        );
    }

    #[test]
    fn test_acks_default() {
        assert_eq!(Acks::default(), Acks::All);
    }

    #[test]
    fn test_acks_display() {
        assert_eq!(format!("{}", Acks::None), "0");
        assert_eq!(format!("{}", Acks::Leader), "1");
        assert_eq!(format!("{}", Acks::All), "all");
    }

    #[test]
    fn test_compression_type_default() {
        assert_eq!(CompressionType::default(), CompressionType::None);
    }

    #[test]
    fn test_compression_type_display() {
        assert_eq!(format!("{}", CompressionType::None), "none");
        assert_eq!(format!("{}", CompressionType::Gzip), "gzip");
        assert_eq!(format!("{}", CompressionType::Snappy), "snappy");
        assert_eq!(format!("{}", CompressionType::Lz4), "lz4");
        assert_eq!(format!("{}", CompressionType::Zstd), "zstd");
    }

    #[test]
    fn test_kafka_source_config_builder_minimal() {
        let config = KafkaSourceConfig::builder()
            .bootstrap_servers("localhost:9092")
            .topic("test-topic")
            .group_id("test-group")
            .build();

        assert_eq!(config.bootstrap_servers(), "localhost:9092");
        assert_eq!(config.topics(), &["test-topic"]);
        assert_eq!(config.group_id(), "test-group");
        assert_eq!(config.auto_offset_reset(), AutoOffsetReset::Latest);
        assert!(!config.enable_auto_commit());
        assert_eq!(config.isolation_level(), IsolationLevel::ReadCommitted);
    }

    #[test]
    fn test_kafka_source_config_builder_full() {
        let config = KafkaSourceConfig::builder()
            .bootstrap_servers("broker1:9092,broker2:9092")
            .topics(vec!["topic1", "topic2"])
            .group_id("my-group")
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .enable_auto_commit(true)
            .isolation_level(IsolationLevel::ReadUncommitted)
            .max_poll_records(1000)
            .session_timeout_ms(60000)
            .heartbeat_interval_ms(5000)
            .property("security.protocol", "SASL_SSL")
            .build();

        assert_eq!(config.bootstrap_servers(), "broker1:9092,broker2:9092");
        assert_eq!(config.topics(), &["topic1", "topic2"]);
        assert_eq!(config.group_id(), "my-group");
        assert_eq!(config.auto_offset_reset(), AutoOffsetReset::Earliest);
        assert!(config.enable_auto_commit());
        assert_eq!(config.isolation_level(), IsolationLevel::ReadUncommitted);
        assert_eq!(config.max_poll_records(), 1000);
        assert_eq!(config.session_timeout_ms(), 60000);
        assert_eq!(config.heartbeat_interval_ms(), 5000);
        assert_eq!(
            config.properties().get("security.protocol"),
            Some(&"SASL_SSL".to_string())
        );
    }

    #[test]
    fn test_kafka_source_config_multiple_properties() {
        let config = KafkaSourceConfig::builder()
            .bootstrap_servers("localhost:9092")
            .topic("test")
            .group_id("group")
            .properties([("sasl.mechanism", "PLAIN"), ("sasl.username", "user")])
            .build();

        assert_eq!(config.properties().len(), 2);
        assert_eq!(
            config.properties().get("sasl.mechanism"),
            Some(&"PLAIN".to_string())
        );
    }

    #[test]
    #[should_panic(expected = "bootstrap_servers is required")]
    fn test_kafka_source_config_missing_bootstrap_servers() {
        KafkaSourceConfig::builder()
            .topic("test")
            .group_id("group")
            .build();
    }

    #[test]
    #[should_panic(expected = "at least one topic is required")]
    fn test_kafka_source_config_missing_topic() {
        KafkaSourceConfig::builder()
            .bootstrap_servers("localhost:9092")
            .group_id("group")
            .build();
    }

    #[test]
    #[should_panic(expected = "group_id is required")]
    fn test_kafka_source_config_missing_group_id() {
        KafkaSourceConfig::builder()
            .bootstrap_servers("localhost:9092")
            .topic("test")
            .build();
    }

    #[test]
    fn test_kafka_sink_config_builder_minimal() {
        let config = KafkaSinkConfig::builder()
            .bootstrap_servers("localhost:9092")
            .topic("output-topic")
            .build();

        assert_eq!(config.bootstrap_servers(), "localhost:9092");
        assert_eq!(config.topic(), "output-topic");
        assert_eq!(config.acks(), Acks::All);
        assert_eq!(config.retries(), 3);
        assert!(config.idempotence());
        assert!(config.transactional_id().is_none());
    }

    #[test]
    fn test_kafka_sink_config_builder_full() {
        let config = KafkaSinkConfig::builder()
            .bootstrap_servers("broker1:9092")
            .topic("output")
            .acks(Acks::Leader)
            .retries(5)
            .retry_backoff_ms(200)
            .batch_size(32768)
            .linger_ms(10)
            .buffer_memory(67108864)
            .compression_type(CompressionType::Lz4)
            .idempotence(false)
            .transactional_id("my-txn-id")
            .property("max.in.flight.requests.per.connection", "5")
            .build();

        assert_eq!(config.bootstrap_servers(), "broker1:9092");
        assert_eq!(config.topic(), "output");
        assert_eq!(config.acks(), Acks::Leader);
        assert_eq!(config.retries(), 5);
        assert_eq!(config.retry_backoff_ms(), 200);
        assert_eq!(config.batch_size(), 32768);
        assert_eq!(config.linger_ms(), 10);
        assert_eq!(config.buffer_memory(), 67108864);
        assert_eq!(config.compression_type(), CompressionType::Lz4);
        assert!(!config.idempotence());
        assert_eq!(config.transactional_id(), Some("my-txn-id"));
    }

    #[test]
    #[should_panic(expected = "bootstrap_servers is required")]
    fn test_kafka_sink_config_missing_bootstrap_servers() {
        KafkaSinkConfig::builder().topic("test").build();
    }

    #[test]
    #[should_panic(expected = "topic is required")]
    fn test_kafka_sink_config_missing_topic() {
        KafkaSinkConfig::builder()
            .bootstrap_servers("localhost:9092")
            .build();
    }

    #[test]
    fn test_kafka_source_new() {
        let config = KafkaSourceConfig::builder()
            .bootstrap_servers("localhost:9092")
            .topic("test-topic")
            .group_id("test-group")
            .build();

        let source = KafkaSource::new(config);
        assert_eq!(source.source_type(), "kafka");
        assert_eq!(source.name(), "test-topic");
    }

    #[test]
    fn test_kafka_source_vertex_name() {
        let config = KafkaSourceConfig::builder()
            .bootstrap_servers("localhost:9092")
            .topics(vec!["topic1", "topic2"])
            .group_id("my-group")
            .build();

        let source = KafkaSource::new(config);
        assert_eq!(
            source.vertex_name(),
            "source:kafka:localhost:9092:topic1,topic2:my-group"
        );
    }

    #[test]
    fn test_kafka_source_config_accessor() {
        let config = KafkaSourceConfig::builder()
            .bootstrap_servers("localhost:9092")
            .topic("test")
            .group_id("group")
            .build();

        let source = KafkaSource::new(config);
        assert_eq!(source.config().bootstrap_servers(), "localhost:9092");
    }

    #[test]
    fn test_kafka_source_clone() {
        let config = KafkaSourceConfig::builder()
            .bootstrap_servers("localhost:9092")
            .topic("test")
            .group_id("group")
            .build();

        let source = KafkaSource::new(config);
        let cloned = source.clone();
        assert_eq!(source.source_type(), cloned.source_type());
        assert_eq!(source.name(), cloned.name());
    }

    #[test]
    fn test_kafka_sink_new() {
        let config = KafkaSinkConfig::builder()
            .bootstrap_servers("localhost:9092")
            .topic("output-topic")
            .build();

        let sink = KafkaSink::new(config);
        assert_eq!(sink.sink_type(), "kafka");
        assert_eq!(sink.name(), "output-topic");
    }

    #[test]
    fn test_kafka_sink_vertex_name() {
        let config = KafkaSinkConfig::builder()
            .bootstrap_servers("broker:9092")
            .topic("output")
            .build();

        let sink = KafkaSink::new(config);
        assert_eq!(sink.vertex_name(), "sink:kafka:broker:9092:output");
    }

    #[test]
    fn test_kafka_sink_config_accessor() {
        let config = KafkaSinkConfig::builder()
            .bootstrap_servers("localhost:9092")
            .topic("output")
            .build();

        let sink = KafkaSink::new(config);
        assert_eq!(sink.config().topic(), "output");
    }

    #[test]
    fn test_kafka_sink_clone() {
        let config = KafkaSinkConfig::builder()
            .bootstrap_servers("localhost:9092")
            .topic("output")
            .build();

        let sink = KafkaSink::new(config);
        let cloned = sink.clone();
        assert_eq!(sink.sink_type(), cloned.sink_type());
        assert_eq!(sink.name(), cloned.name());
    }

    #[test]
    fn test_kafka_source_factory() {
        let config = KafkaSourceConfig::builder()
            .bootstrap_servers("localhost:9092")
            .topic("test")
            .group_id("group")
            .build();

        let source = kafka_source(config);
        assert_eq!(source.source_type(), "kafka");
    }

    #[test]
    fn test_kafka_sink_factory() {
        let config = KafkaSinkConfig::builder()
            .bootstrap_servers("localhost:9092")
            .topic("output")
            .build();

        let sink = kafka_sink(config);
        assert_eq!(sink.sink_type(), "kafka");
    }

    #[test]
    fn test_kafka_source_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<KafkaSource>();
        assert_send_sync::<KafkaSourceConfig>();
    }

    #[test]
    fn test_kafka_sink_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<KafkaSink>();
        assert_send_sync::<KafkaSinkConfig>();
    }

    #[test]
    fn test_kafka_source_in_pipeline() {
        use crate::jet::pipeline::map_sink;
        use crate::jet::Pipeline;

        let source_config = KafkaSourceConfig::builder()
            .bootstrap_servers("localhost:9092")
            .topic("input-topic")
            .group_id("test-group")
            .build();

        let pipeline = Pipeline::builder()
            .read_from(kafka_source(source_config))
            .map("process")
            .write_to(map_sink("output-map"))
            .build();

        assert_eq!(pipeline.vertex_count(), 3);
        assert_eq!(pipeline.sources().len(), 1);
        assert_eq!(pipeline.sources()[0].source_type(), "kafka");
    }

    #[test]
    fn test_kafka_sink_in_pipeline() {
        use crate::jet::pipeline::map_source;
        use crate::jet::Pipeline;

        let sink_config = KafkaSinkConfig::builder()
            .bootstrap_servers("localhost:9092")
            .topic("output-topic")
            .build();

        let pipeline = Pipeline::builder()
            .read_from(map_source("input-map"))
            .map("process")
            .write_to(kafka_sink(sink_config))
            .build();

        assert_eq!(pipeline.vertex_count(), 3);
        assert_eq!(pipeline.sinks().len(), 1);
        assert_eq!(pipeline.sinks()[0].sink_type(), "kafka");
    }

    #[test]
    fn test_kafka_to_kafka_pipeline() {
        use crate::jet::Pipeline;

        let source_config = KafkaSourceConfig::builder()
            .bootstrap_servers("localhost:9092")
            .topic("input")
            .group_id("processor-group")
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .build();

        let sink_config = KafkaSinkConfig::builder()
            .bootstrap_servers("localhost:9092")
            .topic("output")
            .acks(Acks::All)
            .build();

        let pipeline = Pipeline::builder()
            .read_from(kafka_source(source_config))
            .filter("filter-invalid")
            .map("transform")
            .write_to(kafka_sink(sink_config))
            .build();

        assert_eq!(pipeline.vertex_count(), 4);
        assert!(pipeline.vertices()[0].name().contains("kafka"));
        assert!(pipeline.vertices()[3].name().contains("kafka"));
    }

    #[test]
    fn test_source_defaults_for_at_least_once() {
        let config = KafkaSourceConfig::builder()
            .bootstrap_servers("localhost:9092")
            .topic("test")
            .group_id("group")
            .build();

        assert!(!config.enable_auto_commit());
        assert_eq!(config.isolation_level(), IsolationLevel::ReadCommitted);
    }

    #[test]
    fn test_sink_defaults_for_at_least_once() {
        let config = KafkaSinkConfig::builder()
            .bootstrap_servers("localhost:9092")
            .topic("output")
            .build();

        assert_eq!(config.acks(), Acks::All);
        assert!(config.idempotence());
        assert!(config.retries() > 0);
    }
}
