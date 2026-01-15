//! Hazelcast Cloud cluster discovery.
//!
//! This module provides automatic discovery of Hazelcast cluster members
//! running on Hazelcast Cloud using the coordinator API.

use std::net::SocketAddr;

use async_trait::async_trait;
use hazelcast_core::Result;

use super::ClusterDiscovery;

const DEFAULT_COORDINATOR_URL: &str = "https://coordinator.hazelcast.cloud";

/// Configuration for Hazelcast Cloud discovery.
#[derive(Debug, Clone)]
pub struct CloudDiscoveryConfig {
    /// Discovery token for authenticating with Hazelcast Cloud.
    discovery_token: String,
    /// Custom coordinator base URL (optional, for testing or private cloud).
    coordinator_url: Option<String>,
    /// Connection timeout in seconds (default: 10).
    timeout_secs: u64,
}

impl CloudDiscoveryConfig {
    /// Creates a new Cloud discovery configuration with the given token.
    pub fn new(discovery_token: impl Into<String>) -> Self {
        Self {
            discovery_token: discovery_token.into(),
            coordinator_url: None,
            timeout_secs: 10,
        }
    }

    /// Sets a custom coordinator URL (for private cloud or testing).
    pub fn with_coordinator_url(mut self, url: impl Into<String>) -> Self {
        self.coordinator_url = Some(url.into());
        self
    }

    /// Sets the connection timeout in seconds (default: 10).
    pub fn with_timeout(mut self, timeout_secs: u64) -> Self {
        self.timeout_secs = timeout_secs;
        self
    }

    /// Returns the discovery token.
    pub fn discovery_token(&self) -> &str {
        &self.discovery_token
    }

    /// Returns the coordinator URL.
    pub fn coordinator_url(&self) -> &str {
        self.coordinator_url.as_deref().unwrap_or(DEFAULT_COORDINATOR_URL)
    }

    /// Returns the timeout in seconds.
    pub fn timeout_secs(&self) -> u64 {
        self.timeout_secs
    }
}

/// Hazelcast Cloud cluster discovery.
///
/// Discovers Hazelcast cluster members by querying the Hazelcast Cloud
/// coordinator API using the configured discovery token.
#[derive(Debug, Clone)]
pub struct CloudDiscovery {
    config: CloudDiscoveryConfig,
    #[cfg(test)]
    mock_addresses: Option<Vec<SocketAddr>>,
}

impl CloudDiscovery {
    /// Creates a new Cloud discovery instance with the given configuration.
    pub fn new(config: CloudDiscoveryConfig) -> Self {
        Self {
            config,
            #[cfg(test)]
            mock_addresses: None,
        }
    }

    /// Creates a Cloud discovery with just a token using default settings.
    pub fn with_token(token: impl Into<String>) -> Self {
        Self::new(CloudDiscoveryConfig::new(token))
    }

    /// Returns the discovery configuration.
    pub fn config(&self) -> &CloudDiscoveryConfig {
        &self.config
    }

    #[cfg(test)]
    pub(crate) fn with_mock_addresses(config: CloudDiscoveryConfig, addresses: Vec<SocketAddr>) -> Self {
        Self {
            config,
            mock_addresses: Some(addresses),
        }
    }

    fn build_discovery_url(&self) -> String {
        format!(
            "{}/cluster/discovery?token={}",
            self.config.coordinator_url(),
            self.config.discovery_token()
        )
    }

    async fn fetch_addresses(&self) -> Result<Vec<SocketAddr>> {
        use hazelcast_core::HazelcastError;

        let url = self.build_discovery_url();

        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(self.config.timeout_secs))
            .build()
            .map_err(|e| HazelcastError::Connection(format!("Failed to create HTTP client: {}", e)))?;

        let response = client
            .get(&url)
            .header("Accept", "application/json")
            .send()
            .await
            .map_err(|e| HazelcastError::Connection(format!("Cloud coordinator request failed: {}", e)))?;

        if !response.status().is_success() {
            return Err(HazelcastError::Connection(format!(
                "Cloud coordinator returned status: {}",
                response.status()
            )));
        }

        let body = response
            .text()
            .await
            .map_err(|e| HazelcastError::Connection(format!("Failed to read response: {}", e)))?;

        self.parse_response(&body)
    }

    fn parse_response(&self, body: &str) -> Result<Vec<SocketAddr>> {
        use hazelcast_core::HazelcastError;

        let mut addresses = Vec::new();

        for line in body.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }

            // Expected format: "host:port" or JSON array
            if line.starts_with('[') {
                // JSON array format: ["host1:port1", "host2:port2"]
                let trimmed = line.trim_start_matches('[').trim_end_matches(']');
                for entry in trimmed.split(',') {
                    let entry = entry.trim().trim_matches('"');
                    if let Ok(addr) = entry.parse::<SocketAddr>() {
                        addresses.push(addr);
                    }
                }
            } else if let Ok(addr) = line.parse::<SocketAddr>() {
                addresses.push(addr);
            }
        }

        if addresses.is_empty() {
            tracing::warn!("Cloud discovery found no Hazelcast members");
        } else {
            tracing::info!("Cloud discovery found {} Hazelcast member(s)", addresses.len());
        }

        Ok(addresses)
    }
}

#[async_trait]
impl ClusterDiscovery for CloudDiscovery {
    async fn discover(&self) -> Result<Vec<SocketAddr>> {
        #[cfg(test)]
        if let Some(ref addresses) = self.mock_addresses {
            return Ok(addresses.clone());
        }

        self.fetch_addresses().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    #[test]
    fn test_config_new() {
        let config = CloudDiscoveryConfig::new("my-token-123");
        assert_eq!(config.discovery_token(), "my-token-123");
        assert_eq!(config.coordinator_url(), DEFAULT_COORDINATOR_URL);
        assert_eq!(config.timeout_secs(), 10);
    }

    #[test]
    fn test_config_builder() {
        let config = CloudDiscoveryConfig::new("token-xyz")
            .with_coordinator_url("https://custom.cloud.example.com")
            .with_timeout(30);

        assert_eq!(config.discovery_token(), "token-xyz");
        assert_eq!(config.coordinator_url(), "https://custom.cloud.example.com");
        assert_eq!(config.timeout_secs(), 30);
    }

    #[test]
    fn test_discovery_creation() {
        let config = CloudDiscoveryConfig::new("test-token");
        let discovery = CloudDiscovery::new(config);

        assert_eq!(discovery.config().discovery_token(), "test-token");
    }

    #[test]
    fn test_discovery_with_token() {
        let discovery = CloudDiscovery::with_token("simple-token");
        assert_eq!(discovery.config().discovery_token(), "simple-token");
    }

    #[test]
    fn test_build_discovery_url() {
        let config = CloudDiscoveryConfig::new("abc123");
        let discovery = CloudDiscovery::new(config);

        let url = discovery.build_discovery_url();
        assert_eq!(url, "https://coordinator.hazelcast.cloud/cluster/discovery?token=abc123");
    }

    #[test]
    fn test_build_discovery_url_custom_coordinator() {
        let config = CloudDiscoveryConfig::new("token")
            .with_coordinator_url("https://private.cloud.local");
        let discovery = CloudDiscovery::new(config);

        let url = discovery.build_discovery_url();
        assert_eq!(url, "https://private.cloud.local/cluster/discovery?token=token");
    }

    #[test]
    fn test_parse_response_line_format() {
        let config = CloudDiscoveryConfig::new("token");
        let discovery = CloudDiscovery::new(config);

        let body = "192.168.1.1:5701\n192.168.1.2:5701\n192.168.1.3:5701";
        let addresses = discovery.parse_response(body).unwrap();

        assert_eq!(addresses.len(), 3);
        assert!(addresses.contains(&"192.168.1.1:5701".parse().unwrap()));
        assert!(addresses.contains(&"192.168.1.2:5701".parse().unwrap()));
        assert!(addresses.contains(&"192.168.1.3:5701".parse().unwrap()));
    }

    #[test]
    fn test_parse_response_json_array() {
        let config = CloudDiscoveryConfig::new("token");
        let discovery = CloudDiscovery::new(config);

        let body = r#"["10.0.0.1:5701", "10.0.0.2:5701"]"#;
        let addresses = discovery.parse_response(body).unwrap();

        assert_eq!(addresses.len(), 2);
        assert!(addresses.contains(&"10.0.0.1:5701".parse().unwrap()));
        assert!(addresses.contains(&"10.0.0.2:5701".parse().unwrap()));
    }

    #[test]
    fn test_parse_response_empty() {
        let config = CloudDiscoveryConfig::new("token");
        let discovery = CloudDiscovery::new(config);

        let body = "";
        let addresses = discovery.parse_response(body).unwrap();
        assert!(addresses.is_empty());
    }

    #[test]
    fn test_parse_response_with_blank_lines() {
        let config = CloudDiscoveryConfig::new("token");
        let discovery = CloudDiscovery::new(config);

        let body = "\n192.168.1.1:5701\n\n192.168.1.2:5701\n  \n";
        let addresses = discovery.parse_response(body).unwrap();

        assert_eq!(addresses.len(), 2);
    }

    #[tokio::test]
    async fn test_mock_discovery() {
        let config = CloudDiscoveryConfig::new("mock-token");
        let mock_addresses = vec![
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 5701),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)), 5701),
        ];

        let discovery = CloudDiscovery::with_mock_addresses(config, mock_addresses.clone());
        let result = discovery.discover().await.unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(result, mock_addresses);
    }

    #[tokio::test]
    async fn test_mock_discovery_empty() {
        let config = CloudDiscoveryConfig::new("token");
        let discovery = CloudDiscovery::with_mock_addresses(config, vec![]);
        let result = discovery.discover().await.unwrap();

        assert!(result.is_empty());
    }

    #[test]
    fn test_config_clone() {
        let config = CloudDiscoveryConfig::new("original")
            .with_coordinator_url("https://test.com")
            .with_timeout(20);

        let cloned = config.clone();
        assert_eq!(cloned.discovery_token(), "original");
        assert_eq!(cloned.coordinator_url(), "https://test.com");
        assert_eq!(cloned.timeout_secs(), 20);
    }

    #[test]
    fn test_discovery_clone() {
        let discovery = CloudDiscovery::with_token("token");
        let cloned = discovery.clone();
        assert_eq!(cloned.config().discovery_token(), "token");
    }

    #[test]
    fn test_config_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<CloudDiscoveryConfig>();
    }

    #[test]
    fn test_discovery_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<CloudDiscovery>();
    }

    #[test]
    fn test_config_debug() {
        let config = CloudDiscoveryConfig::new("secret-token")
            .with_timeout(15);
        let debug_str = format!("{:?}", config);
        assert!(debug_str.contains("CloudDiscoveryConfig"));
        assert!(debug_str.contains("15"));
    }

    #[test]
    fn test_discovery_debug() {
        let discovery = CloudDiscovery::with_token("token");
        let debug_str = format!("{:?}", discovery);
        assert!(debug_str.contains("CloudDiscovery"));
    }
}
