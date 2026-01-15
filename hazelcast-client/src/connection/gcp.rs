//! GCP cloud discovery for Hazelcast clusters.
//!
//! Discovers cluster members using GCP Compute Engine API to find
//! VMs with specific labels.

use std::fmt;
use std::net::{IpAddr, SocketAddr};

use async_trait::async_trait;
use hazelcast_core::{HazelcastError, Result};
use reqwest::Client;
use serde_json::Value;
use tracing::debug;

use super::ClusterDiscovery;

const METADATA_SERVER_URL: &str = "http://metadata.google.internal";
const COMPUTE_API_URL: &str = "https://compute.googleapis.com/compute/v1";

/// Configuration for GCP VM discovery.
#[derive(Debug, Clone)]
pub struct GcpDiscoveryConfig {
    /// GCP project ID.
    pub project_id: String,
    /// Zone to search. If None, searches all zones via aggregated list.
    pub zone: Option<String>,
    /// Label key to filter Hazelcast VMs.
    pub label_key: String,
    /// Label value to filter Hazelcast VMs.
    pub label_value: String,
    /// Hazelcast port.
    pub port: u16,
    /// Use private IP addresses instead of public.
    pub use_private_ip: bool,
}

impl GcpDiscoveryConfig {
    /// Creates a new configuration with the specified project ID.
    pub fn new(project_id: impl Into<String>) -> Self {
        Self {
            project_id: project_id.into(),
            zone: None,
            label_key: "hazelcast".to_string(),
            label_value: "true".to_string(),
            port: 5701,
            use_private_ip: true,
        }
    }

    /// Sets the zone for discovery (optional, aggregates all zones if not set).
    pub fn with_zone(mut self, zone: impl Into<String>) -> Self {
        self.zone = Some(zone.into());
        self
    }

    /// Sets the label filter for VM discovery.
    pub fn with_label(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.label_key = key.into();
        self.label_value = value.into();
        self
    }

    /// Sets the Hazelcast port.
    pub fn with_port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    /// Sets whether to use private IP addresses.
    pub fn with_private_ip(mut self, use_private: bool) -> Self {
        self.use_private_ip = use_private;
        self
    }
}

impl Default for GcpDiscoveryConfig {
    fn default() -> Self {
        Self {
            project_id: String::new(),
            zone: None,
            label_key: "hazelcast".to_string(),
            label_value: "true".to_string(),
            port: 5701,
            use_private_ip: true,
        }
    }
}

/// Trait for GCP HTTP operations, enabling mock injection for tests.
#[async_trait]
trait GcpHttpClient: Send + Sync {
    async fn get_with_token(&self, url: &str, token: &str) -> Result<Value>;
    async fn get_metadata(&self, url: &str) -> Result<Value>;
}

/// Production HTTP client using reqwest.
struct ReqwestGcpClient {
    client: Client,
}

impl ReqwestGcpClient {
    fn new() -> Result<Self> {
        let client = Client::builder()
            .build()
            .map_err(|e| HazelcastError::Connection(e.to_string()))?;
        Ok(Self { client })
    }
}

#[async_trait]
impl GcpHttpClient for ReqwestGcpClient {
    async fn get_with_token(&self, url: &str, token: &str) -> Result<Value> {
        let response = self
            .client
            .get(url)
            .bearer_auth(token)
            .send()
            .await
            .map_err(|e| HazelcastError::Connection(e.to_string()))?;

        if !response.status().is_success() {
            return Err(HazelcastError::Connection(format!(
                "GCP API error: {} - {}",
                response.status(),
                response.text().await.unwrap_or_default()
            )));
        }

        response
            .json()
            .await
            .map_err(|e| HazelcastError::Connection(e.to_string()))
    }

    async fn get_metadata(&self, url: &str) -> Result<Value> {
        let response = self
            .client
            .get(url)
            .header("Metadata-Flavor", "Google")
            .send()
            .await
            .map_err(|e| HazelcastError::Connection(e.to_string()))?;

        if !response.status().is_success() {
            return Err(HazelcastError::Connection(format!(
                "GCP Metadata Server error: {}",
                response.status()
            )));
        }

        response
            .json()
            .await
            .map_err(|e| HazelcastError::Connection(e.to_string()))
    }
}

/// Trait for token retrieval, enabling mock injection for tests.
#[async_trait]
trait GcpTokenProvider: Send + Sync {
    async fn get_token(&self) -> Result<String>;
}

/// Production token provider using GCP Metadata Server.
struct MetadataTokenProvider {
    client: Client,
}

impl MetadataTokenProvider {
    fn new() -> Result<Self> {
        let client = Client::builder()
            .build()
            .map_err(|e| HazelcastError::Connection(e.to_string()))?;
        Ok(Self { client })
    }
}

#[async_trait]
impl GcpTokenProvider for MetadataTokenProvider {
    async fn get_token(&self) -> Result<String> {
        let url = format!(
            "{}/computeMetadata/v1/instance/service-accounts/default/token",
            METADATA_SERVER_URL
        );

        let response = self
            .client
            .get(&url)
            .header("Metadata-Flavor", "Google")
            .send()
            .await
            .map_err(|e| HazelcastError::Connection(format!("Failed to get GCP token: {}", e)))?;

        if !response.status().is_success() {
            return Err(HazelcastError::Connection(format!(
                "GCP Metadata Server returned status: {}",
                response.status()
            )));
        }

        let body: Value = response
            .json()
            .await
            .map_err(|e| HazelcastError::Connection(format!("Failed to parse token response: {}", e)))?;

        body.get("access_token")
            .and_then(|t| t.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| HazelcastError::Connection("No access_token in response".into()))
    }
}

/// GCP-based cluster discovery using Compute Engine API.
pub struct GcpDiscovery {
    config: GcpDiscoveryConfig,
    http_client: Box<dyn GcpHttpClient>,
    token_provider: Box<dyn GcpTokenProvider>,
}

impl fmt::Debug for GcpDiscovery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GcpDiscovery")
            .field("config", &self.config)
            .finish()
    }
}

impl GcpDiscovery {
    /// Creates a new GCP discovery instance with default credentials.
    pub fn new(config: GcpDiscoveryConfig) -> Result<Self> {
        Ok(Self {
            config,
            http_client: Box::new(ReqwestGcpClient::new()?),
            token_provider: Box::new(MetadataTokenProvider::new()?),
        })
    }

    #[cfg(test)]
    fn with_mocks(
        config: GcpDiscoveryConfig,
        http_client: Box<dyn GcpHttpClient>,
        token_provider: Box<dyn GcpTokenProvider>,
    ) -> Self {
        Self {
            config,
            http_client,
            token_provider,
        }
    }

    async fn discover_instances(&self) -> Result<Vec<SocketAddr>> {
        let token = self.token_provider.get_token().await?;

        let url = if let Some(ref zone) = self.config.zone {
            format!(
                "{}/projects/{}/zones/{}/instances",
                COMPUTE_API_URL, self.config.project_id, zone
            )
        } else {
            format!(
                "{}/projects/{}/aggregated/instances",
                COMPUTE_API_URL, self.config.project_id
            )
        };

        let body = self.http_client.get_with_token(&url, &token).await?;

        if self.config.zone.is_some() {
            self.parse_zone_instances(&body)
        } else {
            self.parse_aggregated_instances(&body)
        }
    }

    fn parse_zone_instances(&self, body: &Value) -> Result<Vec<SocketAddr>> {
        let mut addresses = Vec::new();

        let items = body
            .get("items")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        for instance in &items {
            if let Some(addr) = self.extract_instance_address(instance) {
                addresses.push(addr);
            }
        }

        Ok(addresses)
    }

    fn parse_aggregated_instances(&self, body: &Value) -> Result<Vec<SocketAddr>> {
        let mut addresses = Vec::new();

        let items = body
            .get("items")
            .and_then(|v| v.as_object())
            .cloned()
            .unwrap_or_default();

        for (_zone_key, zone_data) in items.iter() {
            let instances = zone_data
                .get("instances")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();

            for instance in &instances {
                if let Some(addr) = self.extract_instance_address(instance) {
                    addresses.push(addr);
                }
            }
        }

        Ok(addresses)
    }

    fn extract_instance_address(&self, instance: &Value) -> Option<SocketAddr> {
        let status = instance.get("status").and_then(|s| s.as_str())?;
        if status != "RUNNING" {
            return None;
        }

        let labels = instance.get("labels").and_then(|l| l.as_object())?;
        let label_value = labels.get(&self.config.label_key).and_then(|v| v.as_str())?;

        if label_value != self.config.label_value {
            return None;
        }

        let network_interfaces = instance
            .get("networkInterfaces")
            .and_then(|n| n.as_array())?;

        let first_interface = network_interfaces.first()?;

        let ip_str = if self.config.use_private_ip {
            first_interface.get("networkIP").and_then(|ip| ip.as_str())
        } else {
            first_interface
                .get("accessConfigs")
                .and_then(|ac| ac.as_array())
                .and_then(|configs| configs.first())
                .and_then(|config| config.get("natIP"))
                .and_then(|ip| ip.as_str())
        };

        let ip = ip_str?.parse::<IpAddr>().ok()?;
        let addr = SocketAddr::new(ip, self.config.port);
        debug!("Discovered GCP instance: {}", addr);
        Some(addr)
    }
}

#[async_trait]
impl ClusterDiscovery for GcpDiscovery {
    async fn discover(&self) -> Result<Vec<SocketAddr>> {
        self.discover_instances().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::Mutex;

    struct MockHttpClient {
        responses: Mutex<Vec<Value>>,
    }

    impl MockHttpClient {
        fn new(responses: Vec<Value>) -> Self {
            Self {
                responses: Mutex::new(responses),
            }
        }
    }

    #[async_trait]
    impl GcpHttpClient for MockHttpClient {
        async fn get_with_token(&self, _url: &str, _token: &str) -> Result<Value> {
            let mut responses = self.responses.lock().unwrap();
            if responses.is_empty() {
                Err(HazelcastError::Connection("No mock response".into()))
            } else {
                Ok(responses.remove(0))
            }
        }

        async fn get_metadata(&self, _url: &str) -> Result<Value> {
            Ok(json!({"access_token": "mock-token"}))
        }
    }

    struct MockTokenProvider;

    #[async_trait]
    impl GcpTokenProvider for MockTokenProvider {
        async fn get_token(&self) -> Result<String> {
            Ok("mock-token".to_string())
        }
    }

    fn create_mock_discovery(
        config: GcpDiscoveryConfig,
        responses: Vec<Value>,
    ) -> GcpDiscovery {
        GcpDiscovery::with_mocks(
            config,
            Box::new(MockHttpClient::new(responses)),
            Box::new(MockTokenProvider),
        )
    }

    #[tokio::test]
    async fn test_zone_discovery_parses_private_ips() {
        let config = GcpDiscoveryConfig::new("my-project")
            .with_zone("us-central1-a")
            .with_port(5701);

        let response = json!({
            "items": [
                {
                    "status": "RUNNING",
                    "labels": {
                        "hazelcast": "true"
                    },
                    "networkInterfaces": [
                        {
                            "networkIP": "10.128.0.2"
                        }
                    ]
                },
                {
                    "status": "RUNNING",
                    "labels": {
                        "hazelcast": "true"
                    },
                    "networkInterfaces": [
                        {
                            "networkIP": "10.128.0.3"
                        }
                    ]
                }
            ]
        });

        let discovery = create_mock_discovery(config, vec![response]);
        let addresses = discovery.discover().await.unwrap();

        assert_eq!(addresses.len(), 2);
        assert!(addresses.contains(&"10.128.0.2:5701".parse().unwrap()));
        assert!(addresses.contains(&"10.128.0.3:5701".parse().unwrap()));
    }

    #[tokio::test]
    async fn test_aggregated_discovery_parses_multiple_zones() {
        let config = GcpDiscoveryConfig::new("my-project")
            .with_label("hazelcast", "cluster-1");

        let response = json!({
            "items": {
                "zones/us-central1-a": {
                    "instances": [
                        {
                            "status": "RUNNING",
                            "labels": {
                                "hazelcast": "cluster-1"
                            },
                            "networkInterfaces": [
                                {
                                    "networkIP": "10.128.0.10"
                                }
                            ]
                        }
                    ]
                },
                "zones/us-east1-b": {
                    "instances": [
                        {
                            "status": "RUNNING",
                            "labels": {
                                "hazelcast": "cluster-1"
                            },
                            "networkInterfaces": [
                                {
                                    "networkIP": "10.142.0.20"
                                }
                            ]
                        }
                    ]
                }
            }
        });

        let discovery = create_mock_discovery(config, vec![response]);
        let addresses = discovery.discover().await.unwrap();

        assert_eq!(addresses.len(), 2);
        assert!(addresses.contains(&"10.128.0.10:5701".parse().unwrap()));
        assert!(addresses.contains(&"10.142.0.20:5701".parse().unwrap()));
    }

    #[tokio::test]
    async fn test_discovery_filters_by_label() {
        let config = GcpDiscoveryConfig::new("my-project")
            .with_zone("us-central1-a")
            .with_label("hazelcast", "prod");

        let response = json!({
            "items": [
                {
                    "status": "RUNNING",
                    "labels": {
                        "hazelcast": "prod"
                    },
                    "networkInterfaces": [
                        {
                            "networkIP": "10.128.0.5"
                        }
                    ]
                },
                {
                    "status": "RUNNING",
                    "labels": {
                        "hazelcast": "dev"
                    },
                    "networkInterfaces": [
                        {
                            "networkIP": "10.128.0.6"
                        }
                    ]
                }
            ]
        });

        let discovery = create_mock_discovery(config, vec![response]);
        let addresses = discovery.discover().await.unwrap();

        assert_eq!(addresses.len(), 1);
        assert_eq!(addresses[0], "10.128.0.5:5701".parse().unwrap());
    }

    #[tokio::test]
    async fn test_discovery_ignores_non_running_instances() {
        let config = GcpDiscoveryConfig::new("my-project")
            .with_zone("us-central1-a");

        let response = json!({
            "items": [
                {
                    "status": "RUNNING",
                    "labels": {
                        "hazelcast": "true"
                    },
                    "networkInterfaces": [
                        {
                            "networkIP": "10.128.0.7"
                        }
                    ]
                },
                {
                    "status": "TERMINATED",
                    "labels": {
                        "hazelcast": "true"
                    },
                    "networkInterfaces": [
                        {
                            "networkIP": "10.128.0.8"
                        }
                    ]
                }
            ]
        });

        let discovery = create_mock_discovery(config, vec![response]);
        let addresses = discovery.discover().await.unwrap();

        assert_eq!(addresses.len(), 1);
        assert_eq!(addresses[0], "10.128.0.7:5701".parse().unwrap());
    }

    #[tokio::test]
    async fn test_discovery_uses_public_ip_when_configured() {
        let config = GcpDiscoveryConfig::new("my-project")
            .with_zone("us-central1-a")
            .with_private_ip(false);

        let response = json!({
            "items": [
                {
                    "status": "RUNNING",
                    "labels": {
                        "hazelcast": "true"
                    },
                    "networkInterfaces": [
                        {
                            "networkIP": "10.128.0.9",
                            "accessConfigs": [
                                {
                                    "natIP": "35.192.0.100"
                                }
                            ]
                        }
                    ]
                }
            ]
        });

        let discovery = create_mock_discovery(config, vec![response]);
        let addresses = discovery.discover().await.unwrap();

        assert_eq!(addresses.len(), 1);
        assert_eq!(addresses[0], "35.192.0.100:5701".parse().unwrap());
    }

    #[tokio::test]
    async fn test_discovery_handles_empty_response() {
        let config = GcpDiscoveryConfig::new("my-project")
            .with_zone("us-central1-a");

        let response = json!({
            "items": []
        });

        let discovery = create_mock_discovery(config, vec![response]);
        let addresses = discovery.discover().await.unwrap();

        assert!(addresses.is_empty());
    }

    #[tokio::test]
    async fn test_discovery_skips_instances_without_labels() {
        let config = GcpDiscoveryConfig::new("my-project")
            .with_zone("us-central1-a");

        let response = json!({
            "items": [
                {
                    "status": "RUNNING",
                    "networkInterfaces": [
                        {
                            "networkIP": "10.128.0.11"
                        }
                    ]
                },
                {
                    "status": "RUNNING",
                    "labels": {
                        "hazelcast": "true"
                    },
                    "networkInterfaces": [
                        {
                            "networkIP": "10.128.0.12"
                        }
                    ]
                }
            ]
        });

        let discovery = create_mock_discovery(config, vec![response]);
        let addresses = discovery.discover().await.unwrap();

        assert_eq!(addresses.len(), 1);
        assert_eq!(addresses[0], "10.128.0.12:5701".parse().unwrap());
    }

    #[test]
    fn test_config_builder_pattern() {
        let config = GcpDiscoveryConfig::new("project-123")
            .with_zone("europe-west1-b")
            .with_label("env", "production")
            .with_port(5710)
            .with_private_ip(false);

        assert_eq!(config.project_id, "project-123");
        assert_eq!(config.zone, Some("europe-west1-b".to_string()));
        assert_eq!(config.label_key, "env");
        assert_eq!(config.label_value, "production");
        assert_eq!(config.port, 5710);
        assert!(!config.use_private_ip);
    }

    #[test]
    fn test_config_default() {
        let config = GcpDiscoveryConfig::default();

        assert!(config.project_id.is_empty());
        assert!(config.zone.is_none());
        assert_eq!(config.label_key, "hazelcast");
        assert_eq!(config.label_value, "true");
        assert_eq!(config.port, 5701);
        assert!(config.use_private_ip);
    }

    #[test]
    fn test_gcp_discovery_config_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<GcpDiscoveryConfig>();
    }

    #[test]
    fn test_gcp_discovery_config_debug() {
        let config = GcpDiscoveryConfig::new("test-project")
            .with_zone("us-west1-a");
        let debug_str = format!("{:?}", config);
        assert!(debug_str.contains("GcpDiscoveryConfig"));
        assert!(debug_str.contains("test-project"));
    }

    #[test]
    fn test_gcp_discovery_debug() {
        let config = GcpDiscoveryConfig::new("test-project");
        let discovery = create_mock_discovery(config, vec![]);
        let debug_str = format!("{:?}", discovery);
        assert!(debug_str.contains("GcpDiscovery"));
    }
}
