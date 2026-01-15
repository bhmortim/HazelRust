//! Azure cloud discovery for Hazelcast clusters.
//!
//! Discovers cluster members using Azure Resource Manager API to find
//! VMs with specific tags or instances within a VM Scale Set.

use std::fmt;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use async_trait::async_trait;
use azure_core::auth::TokenCredential;
use azure_identity::DefaultAzureCredential;
use hazelcast_core::{HazelcastError, Result};
use reqwest::Client;
use serde_json::Value;
use tracing::debug;

use super::ClusterDiscovery;

const AZURE_MANAGEMENT_SCOPE: &str = "https://management.azure.com/.default";
const API_VERSION_COMPUTE: &str = "2023-09-01";
const API_VERSION_NETWORK: &str = "2023-11-01";

/// Configuration for Azure VM discovery.
#[derive(Debug, Clone)]
pub struct AzureDiscoveryConfig {
    /// Azure subscription ID.
    pub subscription_id: String,
    /// Resource group containing the Hazelcast VMs.
    pub resource_group: String,
    /// Optional VM Scale Set name for VMSS-based deployments.
    pub scale_set_name: Option<String>,
    /// Tag key to filter Hazelcast VMs.
    pub tag_key: String,
    /// Tag value to filter Hazelcast VMs.
    pub tag_value: String,
    /// Hazelcast port.
    pub port: u16,
    /// Use private IP addresses instead of public.
    pub use_private_ip: bool,
}

impl AzureDiscoveryConfig {
    /// Creates a new configuration with required fields.
    pub fn new(subscription_id: impl Into<String>, resource_group: impl Into<String>) -> Self {
        Self {
            subscription_id: subscription_id.into(),
            resource_group: resource_group.into(),
            scale_set_name: None,
            tag_key: "hazelcast".to_string(),
            tag_value: "true".to_string(),
            port: 5701,
            use_private_ip: true,
        }
    }

    /// Sets the VM Scale Set name for VMSS-based discovery.
    pub fn with_scale_set(mut self, name: impl Into<String>) -> Self {
        self.scale_set_name = Some(name.into());
        self
    }

    /// Sets the tag filter for VM discovery.
    pub fn with_tag(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.tag_key = key.into();
        self.tag_value = value.into();
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

impl Default for AzureDiscoveryConfig {
    fn default() -> Self {
        Self {
            subscription_id: String::new(),
            resource_group: String::new(),
            scale_set_name: None,
            tag_key: "hazelcast".to_string(),
            tag_value: "true".to_string(),
            port: 5701,
            use_private_ip: true,
        }
    }
}

/// Trait for Azure HTTP operations, enabling mock injection for tests.
#[async_trait]
trait AzureHttpClient: Send + Sync {
    async fn get_with_token(&self, url: &str, token: &str) -> Result<Value>;
}

/// Production HTTP client using reqwest.
struct ReqwestAzureClient {
    client: Client,
}

impl ReqwestAzureClient {
    fn new() -> Result<Self> {
        let client = Client::builder()
            .build()
            .map_err(|e| HazelcastError::Connection(e.to_string()))?;
        Ok(Self { client })
    }
}

#[async_trait]
impl AzureHttpClient for ReqwestAzureClient {
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
                "Azure API error: {} - {}",
                response.status(),
                response.text().await.unwrap_or_default()
            )));
        }

        response
            .json()
            .await
            .map_err(|e| HazelcastError::Connection(e.to_string()))
    }
}

/// Trait for token credential operations, enabling mock injection for tests.
#[async_trait]
trait AzureTokenProvider: Send + Sync {
    async fn get_token(&self) -> Result<String>;
}

/// Production token provider using DefaultAzureCredential.
struct DefaultTokenProvider {
    credential: Arc<DefaultAzureCredential>,
}

impl DefaultTokenProvider {
    fn new() -> Self {
        Self {
            credential: Arc::new(DefaultAzureCredential::default()),
        }
    }
}

#[async_trait]
impl AzureTokenProvider for DefaultTokenProvider {
    async fn get_token(&self) -> Result<String> {
        let token_response = self
            .credential
            .get_token(&[AZURE_MANAGEMENT_SCOPE])
            .await
            .map_err(|e| HazelcastError::Connection(format!("Failed to get Azure token: {}", e)))?;

        Ok(token_response.token.secret().to_string())
    }
}

/// Azure-based cluster discovery using Azure Resource Manager API.
pub struct AzureDiscovery {
    config: AzureDiscoveryConfig,
    http_client: Box<dyn AzureHttpClient>,
    token_provider: Box<dyn AzureTokenProvider>,
}

impl fmt::Debug for AzureDiscovery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AzureDiscovery")
            .field("config", &self.config)
            .finish()
    }
}

impl AzureDiscovery {
    /// Creates a new Azure discovery instance with default credentials.
    pub fn new(config: AzureDiscoveryConfig) -> Result<Self> {
        Ok(Self {
            config,
            http_client: Box::new(ReqwestAzureClient::new()?),
            token_provider: Box::new(DefaultTokenProvider::new()),
        })
    }

    #[cfg(test)]
    fn with_mocks(
        config: AzureDiscoveryConfig,
        http_client: Box<dyn AzureHttpClient>,
        token_provider: Box<dyn AzureTokenProvider>,
    ) -> Self {
        Self {
            config,
            http_client,
            token_provider,
        }
    }

    async fn discover_from_vmss(&self) -> Result<Vec<SocketAddr>> {
        let scale_set_name = self.config.scale_set_name.as_ref().ok_or_else(|| {
            HazelcastError::Config("scale_set_name required for VMSS discovery".into())
        })?;

        let token = self.token_provider.get_token().await?;

        let url = format!(
            "https://management.azure.com/subscriptions/{}/resourceGroups/{}/providers/Microsoft.Compute/virtualMachineScaleSets/{}/networkInterfaces?api-version={}",
            self.config.subscription_id,
            self.config.resource_group,
            scale_set_name,
            API_VERSION_NETWORK
        );

        let body = self.http_client.get_with_token(&url, &token).await?;
        self.parse_network_interfaces(&body)
    }

    async fn discover_from_vms(&self) -> Result<Vec<SocketAddr>> {
        let token = self.token_provider.get_token().await?;

        let url = format!(
            "https://management.azure.com/subscriptions/{}/resourceGroups/{}/providers/Microsoft.Compute/virtualMachines?api-version={}",
            self.config.subscription_id,
            self.config.resource_group,
            API_VERSION_COMPUTE
        );

        let body = self.http_client.get_with_token(&url, &token).await?;
        self.parse_vm_list(&body, &token).await
    }

    fn parse_network_interfaces(&self, body: &Value) -> Result<Vec<SocketAddr>> {
        let mut addresses = Vec::new();

        let interfaces = body
            .get("value")
            .and_then(|v| v.as_array())
            .unwrap_or(&Vec::new())
            .clone();

        for interface in &interfaces {
            let ip_configs = interface
                .get("properties")
                .and_then(|p| p.get("ipConfigurations"))
                .and_then(|c| c.as_array());

            if let Some(configs) = ip_configs {
                for ip_config in configs {
                    let ip_str = if self.config.use_private_ip {
                        ip_config
                            .get("properties")
                            .and_then(|p| p.get("privateIPAddress"))
                            .and_then(|ip| ip.as_str())
                    } else {
                        ip_config
                            .get("properties")
                            .and_then(|p| p.get("publicIPAddress"))
                            .and_then(|pip| pip.get("properties"))
                            .and_then(|p| p.get("ipAddress"))
                            .and_then(|ip| ip.as_str())
                    };

                    if let Some(ip) = ip_str {
                        if let Ok(addr) = ip.parse::<IpAddr>() {
                            addresses.push(SocketAddr::new(addr, self.config.port));
                            debug!("Discovered Azure VMSS instance: {}", addr);
                        }
                    }
                }
            }
        }

        Ok(addresses)
    }

    async fn parse_vm_list(&self, body: &Value, token: &str) -> Result<Vec<SocketAddr>> {
        let mut addresses = Vec::new();

        let vms = body
            .get("value")
            .and_then(|v| v.as_array())
            .unwrap_or(&Vec::new())
            .clone();

        for vm in &vms {
            let tags = vm.get("tags").and_then(|t| t.as_object());
            let matches_tag = tags
                .and_then(|t| t.get(&self.config.tag_key))
                .and_then(|v| v.as_str())
                .map(|v| v == self.config.tag_value)
                .unwrap_or(false);

            if !matches_tag {
                continue;
            }

            let nic_id = vm
                .get("properties")
                .and_then(|p| p.get("networkProfile"))
                .and_then(|np| np.get("networkInterfaces"))
                .and_then(|nics| nics.as_array())
                .and_then(|nics| nics.first())
                .and_then(|nic| nic.get("id"))
                .and_then(|id| id.as_str());

            if let Some(id) = nic_id {
                if let Ok(addr) = self.get_ip_from_nic(id, token).await {
                    addresses.push(addr);
                }
            }
        }

        Ok(addresses)
    }

    async fn get_ip_from_nic(&self, nic_id: &str, token: &str) -> Result<SocketAddr> {
        let url = format!(
            "https://management.azure.com{}?api-version={}",
            nic_id, API_VERSION_NETWORK
        );

        let body = self.http_client.get_with_token(&url, token).await?;

        let ip_str = body
            .get("properties")
            .and_then(|p| p.get("ipConfigurations"))
            .and_then(|c| c.as_array())
            .and_then(|configs| configs.first())
            .and_then(|config| config.get("properties"))
            .and_then(|p| {
                if self.config.use_private_ip {
                    p.get("privateIPAddress")
                } else {
                    None
                }
            })
            .and_then(|ip| ip.as_str());

        let ip = ip_str
            .ok_or_else(|| HazelcastError::Connection("No IP address found for NIC".into()))?
            .parse::<IpAddr>()
            .map_err(|e| HazelcastError::Connection(e.to_string()))?;

        debug!("Discovered Azure VM: {}", ip);
        Ok(SocketAddr::new(ip, self.config.port))
    }
}

#[async_trait]
impl ClusterDiscovery for AzureDiscovery {
    async fn discover(&self) -> Result<Vec<SocketAddr>> {
        if self.config.scale_set_name.is_some() {
            self.discover_from_vmss().await
        } else {
            self.discover_from_vms().await
        }
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
    impl AzureHttpClient for MockHttpClient {
        async fn get_with_token(&self, _url: &str, _token: &str) -> Result<Value> {
            let mut responses = self.responses.lock().unwrap();
            if responses.is_empty() {
                Err(HazelcastError::Connection("No mock response".into()))
            } else {
                Ok(responses.remove(0))
            }
        }
    }

    struct MockTokenProvider;

    #[async_trait]
    impl AzureTokenProvider for MockTokenProvider {
        async fn get_token(&self) -> Result<String> {
            Ok("mock-token".to_string())
        }
    }

    fn create_mock_discovery(
        config: AzureDiscoveryConfig,
        responses: Vec<Value>,
    ) -> AzureDiscovery {
        AzureDiscovery::with_mocks(
            config,
            Box::new(MockHttpClient::new(responses)),
            Box::new(MockTokenProvider),
        )
    }

    #[tokio::test]
    async fn test_vmss_discovery_parses_private_ips() {
        let config = AzureDiscoveryConfig::new("sub-123", "rg-test")
            .with_scale_set("vmss-hazelcast")
            .with_port(5701);

        let response = json!({
            "value": [
                {
                    "properties": {
                        "ipConfigurations": [
                            {
                                "properties": {
                                    "privateIPAddress": "10.0.0.1"
                                }
                            }
                        ]
                    }
                },
                {
                    "properties": {
                        "ipConfigurations": [
                            {
                                "properties": {
                                    "privateIPAddress": "10.0.0.2"
                                }
                            }
                        ]
                    }
                }
            ]
        });

        let discovery = create_mock_discovery(config, vec![response]);
        let addresses = discovery.discover().await.unwrap();

        assert_eq!(addresses.len(), 2);
        assert!(addresses.contains(&"10.0.0.1:5701".parse().unwrap()));
        assert!(addresses.contains(&"10.0.0.2:5701".parse().unwrap()));
    }

    #[tokio::test]
    async fn test_vm_discovery_filters_by_tag() {
        let config = AzureDiscoveryConfig::new("sub-123", "rg-test")
            .with_tag("hazelcast", "cluster-1")
            .with_port(5702);

        let vm_list_response = json!({
            "value": [
                {
                    "tags": {
                        "hazelcast": "cluster-1"
                    },
                    "properties": {
                        "networkProfile": {
                            "networkInterfaces": [
                                {
                                    "id": "/subscriptions/sub-123/resourceGroups/rg-test/providers/Microsoft.Network/networkInterfaces/nic-1"
                                }
                            ]
                        }
                    }
                },
                {
                    "tags": {
                        "hazelcast": "cluster-2"
                    },
                    "properties": {
                        "networkProfile": {
                            "networkInterfaces": [
                                {
                                    "id": "/subscriptions/sub-123/resourceGroups/rg-test/providers/Microsoft.Network/networkInterfaces/nic-2"
                                }
                            ]
                        }
                    }
                }
            ]
        });

        let nic_response = json!({
            "properties": {
                "ipConfigurations": [
                    {
                        "properties": {
                            "privateIPAddress": "10.0.1.10"
                        }
                    }
                ]
            }
        });

        let discovery = create_mock_discovery(config, vec![vm_list_response, nic_response]);
        let addresses = discovery.discover().await.unwrap();

        assert_eq!(addresses.len(), 1);
        assert_eq!(addresses[0], "10.0.1.10:5702".parse().unwrap());
    }

    #[tokio::test]
    async fn test_discovery_handles_empty_response() {
        let config = AzureDiscoveryConfig::new("sub-123", "rg-test")
            .with_scale_set("vmss-empty");

        let response = json!({
            "value": []
        });

        let discovery = create_mock_discovery(config, vec![response]);
        let addresses = discovery.discover().await.unwrap();

        assert!(addresses.is_empty());
    }

    #[tokio::test]
    async fn test_discovery_skips_invalid_ips() {
        let config = AzureDiscoveryConfig::new("sub-123", "rg-test")
            .with_scale_set("vmss-test");

        let response = json!({
            "value": [
                {
                    "properties": {
                        "ipConfigurations": [
                            {
                                "properties": {
                                    "privateIPAddress": "not-an-ip"
                                }
                            }
                        ]
                    }
                },
                {
                    "properties": {
                        "ipConfigurations": [
                            {
                                "properties": {
                                    "privateIPAddress": "10.0.0.5"
                                }
                            }
                        ]
                    }
                }
            ]
        });

        let discovery = create_mock_discovery(config, vec![response]);
        let addresses = discovery.discover().await.unwrap();

        assert_eq!(addresses.len(), 1);
        assert_eq!(addresses[0], "10.0.0.5:5701".parse().unwrap());
    }

    #[tokio::test]
    async fn test_config_builder_pattern() {
        let config = AzureDiscoveryConfig::new("sub-id", "rg-name")
            .with_scale_set("my-vmss")
            .with_tag("env", "prod")
            .with_port(5710)
            .with_private_ip(false);

        assert_eq!(config.subscription_id, "sub-id");
        assert_eq!(config.resource_group, "rg-name");
        assert_eq!(config.scale_set_name, Some("my-vmss".to_string()));
        assert_eq!(config.tag_key, "env");
        assert_eq!(config.tag_value, "prod");
        assert_eq!(config.port, 5710);
        assert!(!config.use_private_ip);
    }

    #[test]
    fn test_azure_discovery_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<AzureDiscoveryConfig>();
    }

    #[test]
    fn test_azure_discovery_debug() {
        let config = AzureDiscoveryConfig::new("sub-123", "rg-test");
        let debug_str = format!("{:?}", config);
        assert!(debug_str.contains("AzureDiscoveryConfig"));
        assert!(debug_str.contains("sub-123"));
    }
}
