//! Declarative configuration loading from YAML, TOML, and environment variables.
//!
//! This module provides file-based configuration support through mirror structs
//! that can be deserialized with serde. These structs are then converted into the
//! programmatic [`ClientConfig`](crate::config::ClientConfig) using the builder API.
//!
//! # Supported Formats
//!
//! - **YAML** (requires `config-file` feature): `ClientConfig::from_yaml("config.yaml")`
//! - **TOML** (requires `config-file` feature): `ClientConfig::from_toml("config.toml")`
//! - **Environment Variables** (always available): `ClientConfig::from_env()`
//!
//! # Example YAML
//!
//! ```yaml
//! cluster-name: production
//! instance-name: rust-client-1
//! labels:
//!   - env:production
//!   - region:us-west
//! network:
//!   addresses:
//!     - "10.0.0.1:5701"
//!     - "10.0.0.2:5701"
//!   connection-timeout-ms: 10000
//!   smart-routing: true
//!   reconnect-mode: async
//!   socket:
//!     tcp-nodelay: true
//!     keep-alive: true
//!     send-buffer-size: 65536
//! retry:
//!   initial-backoff-ms: 100
//!   max-backoff-ms: 30000
//!   multiplier: 2.0
//!   max-retries: 10
//! invocation-timeout-seconds: 120
//! max-concurrent-invocations: 512
//! redo-operation: false
//! invocation-retry-count: 3
//! invocation-retry-pause-ms: 1000
//! ```

use std::net::SocketAddr;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::config::{ClientConfig, ClientConfigBuilder, ConfigError};

/// Top-level file-based configuration.
///
/// This struct mirrors [`ClientConfig`](crate::config::ClientConfig) but uses
/// serde-friendly types (no trait objects, no `Arc`s). It can be deserialized
/// from YAML or TOML and then converted to `ClientConfig` via [`TryFrom`].
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case", default)]
pub struct FileConfig {
    /// Cluster name to connect to.
    pub cluster_name: Option<String>,
    /// Client instance name for identification.
    pub instance_name: Option<String>,
    /// Labels for Management Center identification.
    pub labels: Option<Vec<String>>,
    /// Network configuration.
    pub network: Option<FileNetworkConfig>,
    /// Retry/reconnection configuration.
    pub retry: Option<FileRetryConfig>,
    /// Invocation timeout in seconds.
    pub invocation_timeout_seconds: Option<u64>,
    /// Maximum concurrent invocations (0 = unlimited, default).
    /// Note: Java client defaults to 2^23; Rust defaults to 0 (unlimited/opt-in).
    pub max_concurrent_invocations: Option<usize>,
    /// Whether to retry non-idempotent operations on connection failure.
    pub redo_operation: Option<bool>,
    /// Number of retry attempts for retryable operations.
    pub invocation_retry_count: Option<u32>,
    /// Pause between retry attempts in milliseconds (default: 1000).
    pub invocation_retry_pause_ms: Option<u64>,
}

/// File-based network configuration.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case", default)]
pub struct FileNetworkConfig {
    /// Cluster member addresses.
    pub addresses: Option<Vec<String>>,
    /// Connection timeout in milliseconds.
    pub connection_timeout_ms: Option<u64>,
    /// Heartbeat interval in milliseconds.
    pub heartbeat_interval_ms: Option<u64>,
    /// Whether to use smart routing (route to partition owner).
    pub smart_routing: Option<bool>,
    /// Reconnection mode: "off", "on", or "async".
    pub reconnect_mode: Option<String>,
    /// Socket-level configuration.
    pub socket: Option<FileSocketConfig>,
}

/// File-based socket configuration.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case", default)]
pub struct FileSocketConfig {
    /// Enable TCP_NODELAY (Nagle's algorithm disabled).
    pub tcp_nodelay: Option<bool>,
    /// Enable TCP keep-alive.
    pub keep_alive: Option<bool>,
    /// Send buffer size in bytes.
    pub send_buffer_size: Option<u32>,
    /// Receive buffer size in bytes.
    pub recv_buffer_size: Option<u32>,
    /// Linger timeout in milliseconds.
    pub linger_ms: Option<u64>,
}

/// File-based retry configuration.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case", default)]
pub struct FileRetryConfig {
    /// Initial backoff duration in milliseconds.
    pub initial_backoff_ms: Option<u64>,
    /// Maximum backoff duration in milliseconds.
    pub max_backoff_ms: Option<u64>,
    /// Backoff multiplier.
    pub multiplier: Option<f64>,
    /// Maximum number of retry attempts.
    pub max_retries: Option<u32>,
    /// Jitter factor (0.0 to 1.0).
    pub jitter: Option<f64>,
}

impl TryFrom<FileConfig> for ClientConfig {
    type Error = ConfigError;

    fn try_from(file: FileConfig) -> Result<Self, Self::Error> {
        let mut builder = ClientConfigBuilder::new();

        if let Some(name) = file.cluster_name {
            builder = builder.cluster_name(name);
        }

        if let Some(name) = file.instance_name {
            builder = builder.instance_name(name);
        }

        if let Some(labels) = file.labels {
            builder = builder.with_labels(labels);
        }

        if let Some(net) = file.network {
            builder = builder.network(|mut n| {
                if let Some(addrs) = net.addresses {
                    for addr_str in addrs {
                        if let Ok(addr) = addr_str.parse::<SocketAddr>() {
                            n = n.add_address(addr);
                        } else {
                            // Try appending default port
                            if let Ok(addr) = format!("{addr_str}:5701").parse::<SocketAddr>() {
                                n = n.add_address(addr);
                            }
                        }
                    }
                }

                if let Some(timeout_ms) = net.connection_timeout_ms {
                    n = n.connection_timeout(Duration::from_millis(timeout_ms));
                }

                if let Some(interval_ms) = net.heartbeat_interval_ms {
                    n = n.heartbeat_interval(Duration::from_millis(interval_ms));
                }

                if let Some(smart) = net.smart_routing {
                    n = n.smart_routing(smart);
                }

                if let Some(mode) = net.reconnect_mode {
                    use crate::config::ReconnectMode;
                    let mode = match mode.to_lowercase().as_str() {
                        "off" => ReconnectMode::Off,
                        "on" => ReconnectMode::On,
                        "async" => ReconnectMode::Async,
                        _ => ReconnectMode::On,
                    };
                    n = n.reconnect_mode(mode);
                }

                if let Some(sock) = net.socket {
                    n = n.socket(|mut s| {
                        if let Some(v) = sock.tcp_nodelay {
                            s = s.tcp_nodelay(v);
                        }
                        if let Some(v) = sock.keep_alive {
                            s = s.keep_alive(v);
                        }
                        if let Some(v) = sock.send_buffer_size {
                            s = s.send_buffer_size(v);
                        }
                        if let Some(v) = sock.recv_buffer_size {
                            s = s.recv_buffer_size(v);
                        }
                        if let Some(ms) = sock.linger_ms {
                            s = s.linger(Duration::from_millis(ms));
                        }
                        s
                    });
                }

                n
            });
        }

        if let Some(retry) = file.retry {
            builder = builder.retry(|mut r| {
                if let Some(ms) = retry.initial_backoff_ms {
                    r = r.initial_backoff(Duration::from_millis(ms));
                }
                if let Some(ms) = retry.max_backoff_ms {
                    r = r.max_backoff(Duration::from_millis(ms));
                }
                if let Some(m) = retry.multiplier {
                    r = r.multiplier(m);
                }
                if let Some(n) = retry.max_retries {
                    r = r.max_retries(n);
                }
                if let Some(j) = retry.jitter {
                    r = r.jitter(j);
                }
                r
            });
        }

        if let Some(secs) = file.invocation_timeout_seconds {
            builder = builder.invocation_timeout(Duration::from_secs(secs));
        }

        if let Some(max) = file.max_concurrent_invocations {
            builder = builder.max_concurrent_invocations(max);
        }

        if let Some(redo) = file.redo_operation {
            builder = builder.redo_operation(redo);
        }

        if let Some(count) = file.invocation_retry_count {
            builder = builder.invocation_retry_count(count);
        }

        if let Some(ms) = file.invocation_retry_pause_ms {
            builder = builder.invocation_retry_pause(Duration::from_millis(ms));
        }

        builder.build()
    }
}

impl ClientConfig {
    /// Loads configuration from a YAML file.
    ///
    /// Requires the `config-file` feature.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let config = ClientConfig::from_yaml("hazelcast-client.yaml")?;
    /// let client = HazelcastClient::new(config).await?;
    /// ```
    #[cfg(feature = "config-file")]
    pub fn from_yaml<P: AsRef<std::path::Path>>(path: P) -> Result<Self, ConfigError> {
        let content = std::fs::read_to_string(path.as_ref()).map_err(|e| {
            ConfigError::new(format!("failed to read YAML config file: {e}"))
        })?;
        let file_config: FileConfig = serde_yaml::from_str(&content).map_err(|e| {
            ConfigError::new(format!("failed to parse YAML config: {e}"))
        })?;
        file_config.try_into()
    }

    /// Loads configuration from a TOML file.
    ///
    /// Requires the `config-file` feature.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let config = ClientConfig::from_toml("hazelcast-client.toml")?;
    /// let client = HazelcastClient::new(config).await?;
    /// ```
    #[cfg(feature = "config-file")]
    pub fn from_toml<P: AsRef<std::path::Path>>(path: P) -> Result<Self, ConfigError> {
        let content = std::fs::read_to_string(path.as_ref()).map_err(|e| {
            ConfigError::new(format!("failed to read TOML config file: {e}"))
        })?;
        let file_config: FileConfig = toml_crate::from_str(&content).map_err(|e| {
            ConfigError::new(format!("failed to parse TOML config: {e}"))
        })?;
        file_config.try_into()
    }

    /// Loads configuration from environment variables.
    ///
    /// This method is always available (no feature flag required).
    ///
    /// # Supported Environment Variables
    ///
    /// | Variable | Maps to |
    /// |----------|---------|
    /// | `HZ_CLUSTER_NAME` | `cluster_name` |
    /// | `HZ_INSTANCE_NAME` | `instance_name` |
    /// | `HZ_ADDRESSES` | Comma-separated addresses (e.g., `10.0.0.1:5701,10.0.0.2:5701`) |
    /// | `HZ_CONNECTION_TIMEOUT_MS` | Connection timeout in milliseconds |
    /// | `HZ_SMART_ROUTING` | `"true"` or `"false"` |
    /// | `HZ_INVOCATION_TIMEOUT_SECONDS` | Invocation timeout in seconds |
    /// | `HZ_MAX_CONCURRENT_INVOCATIONS` | Maximum concurrent invocations |
    /// | `HZ_REDO_OPERATION` | `"true"` or `"false"` |
    /// | `HZ_INVOCATION_RETRY_COUNT` | Number of retry attempts |
    /// | `HZ_INVOCATION_RETRY_PAUSE_MS` | Pause between retries in milliseconds |
    ///
    /// # Example
    ///
    /// ```ignore
    /// std::env::set_var("HZ_CLUSTER_NAME", "production");
    /// std::env::set_var("HZ_ADDRESSES", "10.0.0.1:5701,10.0.0.2:5701");
    /// let config = ClientConfig::from_env()?;
    /// ```
    pub fn from_env() -> Result<Self, ConfigError> {
        let mut file_config = FileConfig::default();

        if let Ok(val) = std::env::var("HZ_CLUSTER_NAME") {
            file_config.cluster_name = Some(val);
        }

        if let Ok(val) = std::env::var("HZ_INSTANCE_NAME") {
            file_config.instance_name = Some(val);
        }

        if let Ok(val) = std::env::var("HZ_ADDRESSES") {
            file_config.network.get_or_insert_with(Default::default).addresses =
                Some(val.split(',').map(|s| s.trim().to_string()).collect());
        }

        if let Ok(val) = std::env::var("HZ_CONNECTION_TIMEOUT_MS") {
            if let Ok(ms) = val.parse::<u64>() {
                file_config.network.get_or_insert_with(Default::default).connection_timeout_ms =
                    Some(ms);
            }
        }

        if let Ok(val) = std::env::var("HZ_SMART_ROUTING") {
            file_config
                .network
                .get_or_insert_with(Default::default)
                .smart_routing = Some(val.eq_ignore_ascii_case("true"));
        }

        if let Ok(val) = std::env::var("HZ_RECONNECT_MODE") {
            file_config
                .network
                .get_or_insert_with(Default::default)
                .reconnect_mode = Some(val);
        }

        if let Ok(val) = std::env::var("HZ_INVOCATION_TIMEOUT_SECONDS") {
            if let Ok(secs) = val.parse::<u64>() {
                file_config.invocation_timeout_seconds = Some(secs);
            }
        }

        if let Ok(val) = std::env::var("HZ_MAX_CONCURRENT_INVOCATIONS") {
            if let Ok(n) = val.parse::<usize>() {
                file_config.max_concurrent_invocations = Some(n);
            }
        }

        if let Ok(val) = std::env::var("HZ_REDO_OPERATION") {
            file_config.redo_operation = Some(val.eq_ignore_ascii_case("true"));
        }

        if let Ok(val) = std::env::var("HZ_INVOCATION_RETRY_COUNT") {
            if let Ok(n) = val.parse::<u32>() {
                file_config.invocation_retry_count = Some(n);
            }
        }

        if let Ok(val) = std::env::var("HZ_INVOCATION_RETRY_PAUSE_MS") {
            if let Ok(ms) = val.parse::<u64>() {
                file_config.invocation_retry_pause_ms = Some(ms);
            }
        }

        file_config.try_into()
    }
}

/// Convenience function to load a configuration file, auto-detecting format by extension.
///
/// Supports `.yaml`, `.yml`, and `.toml` extensions.
/// Requires the `config-file` feature.
///
/// # Example
///
/// ```ignore
/// let config = hazelcast_client::config_file::load_config("hazelcast-client.yaml")?;
/// ```
#[cfg(feature = "config-file")]
pub fn load_config<P: AsRef<std::path::Path>>(path: P) -> Result<ClientConfig, ConfigError> {
    let path = path.as_ref();
    match path.extension().and_then(|e| e.to_str()) {
        Some("yaml" | "yml") => ClientConfig::from_yaml(path),
        Some("toml") => ClientConfig::from_toml(path),
        Some(ext) => Err(ConfigError::new(format!(
            "unsupported config file extension: .{ext} (expected .yaml, .yml, or .toml)"
        ))),
        None => Err(ConfigError::new(
            "config file has no extension; expected .yaml, .yml, or .toml",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_file_config_defaults_produce_valid_client_config() {
        let file_config = FileConfig::default();
        let config: ClientConfig = file_config.try_into().unwrap();
        assert_eq!(config.cluster_name(), "dev");
    }

    #[test]
    fn test_file_config_with_cluster_name() {
        let file_config = FileConfig {
            cluster_name: Some("production".to_string()),
            ..Default::default()
        };
        let config: ClientConfig = file_config.try_into().unwrap();
        assert_eq!(config.cluster_name(), "production");
    }

    #[test]
    fn test_file_config_with_network() {
        let file_config = FileConfig {
            network: Some(FileNetworkConfig {
                addresses: Some(vec!["127.0.0.1:5701".to_string()]),
                connection_timeout_ms: Some(10_000),
                smart_routing: Some(false),
                ..Default::default()
            }),
            ..Default::default()
        };
        let config: ClientConfig = file_config.try_into().unwrap();
        assert_eq!(config.network().addresses().len(), 1);
        assert_eq!(
            config.network().connection_timeout(),
            Duration::from_secs(10)
        );
        assert!(!config.network().smart_routing());
    }

    #[test]
    fn test_file_config_with_retry() {
        let file_config = FileConfig {
            retry: Some(FileRetryConfig {
                initial_backoff_ms: Some(200),
                max_backoff_ms: Some(60_000),
                multiplier: Some(3.0),
                max_retries: Some(5),
                jitter: Some(0.2),
            }),
            ..Default::default()
        };
        let config: ClientConfig = file_config.try_into().unwrap();
        assert_eq!(config.retry().initial_backoff(), Duration::from_millis(200));
        assert_eq!(config.retry().max_backoff(), Duration::from_secs(60));
        assert_eq!(config.retry().multiplier(), 3.0);
        assert_eq!(config.retry().max_retries(), 5);
        assert_eq!(config.retry().jitter(), 0.2);
    }

    #[test]
    fn test_file_config_with_invocation_settings() {
        let file_config = FileConfig {
            invocation_timeout_seconds: Some(60),
            max_concurrent_invocations: Some(256),
            redo_operation: Some(true),
            invocation_retry_count: Some(5),
            invocation_retry_pause_ms: Some(2000),
            ..Default::default()
        };
        let config: ClientConfig = file_config.try_into().unwrap();
        assert_eq!(config.invocation_timeout(), Duration::from_secs(60));
        assert_eq!(config.max_concurrent_invocations(), 256);
        assert!(config.redo_operation());
        assert_eq!(config.invocation_retry_count(), 5);
        assert_eq!(config.invocation_retry_pause(), Duration::from_secs(2));
    }

    #[test]
    fn test_from_env_with_cluster_name() {
        // Use a unique env var prefix to avoid test interference
        std::env::set_var("HZ_CLUSTER_NAME", "test-cluster");
        let config = ClientConfig::from_env().unwrap();
        assert_eq!(config.cluster_name(), "test-cluster");
        std::env::remove_var("HZ_CLUSTER_NAME");
    }

    #[test]
    fn test_file_config_reconnect_mode() {
        for (mode_str, expected_async) in [("off", false), ("on", false), ("async", true)] {
            let file_config = FileConfig {
                network: Some(FileNetworkConfig {
                    reconnect_mode: Some(mode_str.to_string()),
                    ..Default::default()
                }),
                ..Default::default()
            };
            let config: ClientConfig = file_config.try_into().unwrap();
            assert_eq!(
                config.network().reconnect_mode().is_async(),
                expected_async,
                "mode_str={mode_str}"
            );
        }
    }

    #[cfg(feature = "config-file")]
    #[test]
    fn test_yaml_round_trip() {
        let file_config = FileConfig {
            cluster_name: Some("yaml-test".to_string()),
            invocation_timeout_seconds: Some(30),
            ..Default::default()
        };
        let yaml = serde_yaml::to_string(&file_config).unwrap();
        let parsed: FileConfig = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(parsed.cluster_name.as_deref(), Some("yaml-test"));
        assert_eq!(parsed.invocation_timeout_seconds, Some(30));
    }

    #[cfg(feature = "config-file")]
    #[test]
    fn test_toml_round_trip() {
        let file_config = FileConfig {
            cluster_name: Some("toml-test".to_string()),
            max_concurrent_invocations: Some(128),
            ..Default::default()
        };
        let toml_str = toml_crate::to_string(&file_config).unwrap();
        let parsed: FileConfig = toml_crate::from_str(&toml_str).unwrap();
        assert_eq!(parsed.cluster_name.as_deref(), Some("toml-test"));
        assert_eq!(parsed.max_concurrent_invocations, Some(128));
    }
}
