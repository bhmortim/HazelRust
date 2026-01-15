//! Kubernetes-based cluster discovery for Hazelcast client.

use std::net::SocketAddr;

use async_trait::async_trait;
use k8s_openapi::api::core::v1::{Endpoints, Pod};
use kube::api::ListParams;
use kube::{Api, Client};

use super::ClusterDiscovery;
use crate::HazelcastClientError;

/// Configuration for Kubernetes-based cluster discovery.
#[derive(Debug, Clone, Default)]
pub struct KubernetesDiscoveryConfig {
    /// Kubernetes namespace to search in. Defaults to "default".
    pub namespace: Option<String>,
    /// Service name to discover endpoints from.
    pub service_name: Option<String>,
    /// Label name to filter services.
    pub service_label_name: Option<String>,
    /// Label value to filter services.
    pub service_label_value: Option<String>,
    /// Label name to filter pods.
    pub pod_label_name: Option<String>,
    /// Label value to filter pods.
    pub pod_label_value: Option<String>,
    /// Hazelcast port. Defaults to 5701.
    pub port: Option<u16>,
}

impl KubernetesDiscoveryConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = Some(namespace.into());
        self
    }

    pub fn service_name(mut self, name: impl Into<String>) -> Self {
        self.service_name = Some(name.into());
        self
    }

    pub fn service_label(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.service_label_name = Some(name.into());
        self.service_label_value = Some(value.into());
        self
    }

    pub fn pod_label(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.pod_label_name = Some(name.into());
        self.pod_label_value = Some(value.into());
        self
    }

    pub fn port(mut self, port: u16) -> Self {
        self.port = Some(port);
        self
    }
}

/// Kubernetes-based cluster discovery.
///
/// Discovers Hazelcast cluster members by querying the Kubernetes API
/// for pods or service endpoints matching the configured selectors.
pub struct KubernetesDiscovery {
    config: KubernetesDiscoveryConfig,
    #[cfg(test)]
    mock_addresses: Option<Vec<SocketAddr>>,
}

impl KubernetesDiscovery {
    pub fn new(config: KubernetesDiscoveryConfig) -> Self {
        Self {
            config,
            #[cfg(test)]
            mock_addresses: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn with_mock_addresses(config: KubernetesDiscoveryConfig, addresses: Vec<SocketAddr>) -> Self {
        Self {
            config,
            mock_addresses: Some(addresses),
        }
    }

    fn namespace(&self) -> &str {
        self.config.namespace.as_deref().unwrap_or("default")
    }

    fn port(&self) -> u16 {
        self.config.port.unwrap_or(5701)
    }

    fn build_label_selector(&self) -> Option<String> {
        let mut parts = Vec::new();

        if let (Some(ref name), Some(ref value)) =
            (&self.config.pod_label_name, &self.config.pod_label_value)
        {
            parts.push(format!("{}={}", name, value));
        }

        if let (Some(ref name), Some(ref value)) =
            (&self.config.service_label_name, &self.config.service_label_value)
        {
            parts.push(format!("{}={}", name, value));
        }

        if parts.is_empty() {
            None
        } else {
            Some(parts.join(","))
        }
    }

    async fn discover_from_service(&self, client: Client) -> Result<Vec<SocketAddr>, HazelcastClientError> {
        let service_name = self.config.service_name.as_ref().ok_or_else(|| {
            HazelcastClientError::Connection("Service name not configured".to_string())
        })?;

        let endpoints: Api<Endpoints> = Api::namespaced(client, self.namespace());
        let ep = endpoints.get(service_name).await.map_err(|e| {
            HazelcastClientError::Connection(format!(
                "Failed to get endpoints for service '{}': {}",
                service_name, e
            ))
        })?;

        let port = self.port();
        let mut addresses = Vec::new();

        if let Some(subsets) = ep.subsets {
            for subset in subsets {
                if let Some(addrs) = subset.addresses {
                    for addr in addrs {
                        if let Ok(ip) = addr.ip.parse() {
                            addresses.push(SocketAddr::new(ip, port));
                        }
                    }
                }
            }
        }

        Ok(addresses)
    }

    async fn discover_from_pods(&self, client: Client) -> Result<Vec<SocketAddr>, HazelcastClientError> {
        let pods: Api<Pod> = Api::namespaced(client, self.namespace());
        let label_selector = self.build_label_selector();

        let list_params = if let Some(ref selector) = label_selector {
            ListParams::default().labels(selector)
        } else {
            ListParams::default()
        };

        let pod_list = pods.list(&list_params).await.map_err(|e| {
            HazelcastClientError::Connection(format!("Failed to list pods: {}", e))
        })?;

        let port = self.port();
        let mut addresses = Vec::new();

        for pod in pod_list {
            if let Some(status) = pod.status {
                if let Some(phase) = &status.phase {
                    if phase != "Running" {
                        continue;
                    }
                }
                if let Some(pod_ip) = status.pod_ip {
                    if let Ok(ip) = pod_ip.parse() {
                        addresses.push(SocketAddr::new(ip, port));
                    }
                }
            }
        }

        Ok(addresses)
    }
}

#[async_trait]
impl ClusterDiscovery for KubernetesDiscovery {
    async fn discover(&self) -> Result<Vec<SocketAddr>, HazelcastClientError> {
        #[cfg(test)]
        if let Some(ref addresses) = self.mock_addresses {
            return Ok(addresses.clone());
        }

        let client = Client::try_default().await.map_err(|e| {
            HazelcastClientError::Connection(format!("Failed to create Kubernetes client: {}", e))
        })?;

        if self.config.service_name.is_some() {
            self.discover_from_service(client).await
        } else {
            self.discover_from_pods(client).await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    #[test]
    fn test_config_builder() {
        let config = KubernetesDiscoveryConfig::new()
            .namespace("hazelcast")
            .service_name("hz-service")
            .pod_label("app", "hazelcast")
            .port(5702);

        assert_eq!(config.namespace.as_deref(), Some("hazelcast"));
        assert_eq!(config.service_name.as_deref(), Some("hz-service"));
        assert_eq!(config.pod_label_name.as_deref(), Some("app"));
        assert_eq!(config.pod_label_value.as_deref(), Some("hazelcast"));
        assert_eq!(config.port, Some(5702));
    }

    #[test]
    fn test_default_config() {
        let config = KubernetesDiscoveryConfig::default();
        assert!(config.namespace.is_none());
        assert!(config.service_name.is_none());
        assert!(config.pod_label_name.is_none());
        assert!(config.pod_label_value.is_none());
        assert!(config.service_label_name.is_none());
        assert!(config.service_label_value.is_none());
        assert!(config.port.is_none());
    }

    #[test]
    fn test_service_label_config() {
        let config = KubernetesDiscoveryConfig::new()
            .service_label("app.kubernetes.io/name", "hazelcast");

        assert_eq!(config.service_label_name.as_deref(), Some("app.kubernetes.io/name"));
        assert_eq!(config.service_label_value.as_deref(), Some("hazelcast"));
    }

    #[test]
    fn test_label_selector_pod_only() {
        let config = KubernetesDiscoveryConfig::new()
            .pod_label("app", "hazelcast");
        let discovery = KubernetesDiscovery::new(config);

        let selector = discovery.build_label_selector();
        assert_eq!(selector, Some("app=hazelcast".to_string()));
    }

    #[test]
    fn test_label_selector_service_only() {
        let config = KubernetesDiscoveryConfig::new()
            .service_label("tier", "backend");
        let discovery = KubernetesDiscovery::new(config);

        let selector = discovery.build_label_selector();
        assert_eq!(selector, Some("tier=backend".to_string()));
    }

    #[test]
    fn test_label_selector_multiple() {
        let config = KubernetesDiscoveryConfig::new()
            .pod_label("app", "hazelcast")
            .service_label("tier", "backend");
        let discovery = KubernetesDiscovery::new(config);

        let selector = discovery.build_label_selector();
        assert_eq!(selector, Some("app=hazelcast,tier=backend".to_string()));
    }

    #[test]
    fn test_label_selector_empty() {
        let config = KubernetesDiscoveryConfig::new();
        let discovery = KubernetesDiscovery::new(config);

        let selector = discovery.build_label_selector();
        assert!(selector.is_none());
    }

    #[test]
    fn test_default_namespace() {
        let config = KubernetesDiscoveryConfig::new();
        let discovery = KubernetesDiscovery::new(config);
        assert_eq!(discovery.namespace(), "default");
    }

    #[test]
    fn test_custom_namespace() {
        let config = KubernetesDiscoveryConfig::new().namespace("production");
        let discovery = KubernetesDiscovery::new(config);
        assert_eq!(discovery.namespace(), "production");
    }

    #[test]
    fn test_default_port() {
        let config = KubernetesDiscoveryConfig::new();
        let discovery = KubernetesDiscovery::new(config);
        assert_eq!(discovery.port(), 5701);
    }

    #[test]
    fn test_custom_port() {
        let config = KubernetesDiscoveryConfig::new().port(5702);
        let discovery = KubernetesDiscovery::new(config);
        assert_eq!(discovery.port(), 5702);
    }

    #[tokio::test]
    async fn test_mock_discovery() {
        let config = KubernetesDiscoveryConfig::new()
            .namespace("test")
            .pod_label("app", "hazelcast");

        let mock_addresses = vec![
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 5701),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)), 5701),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 3)), 5701),
        ];

        let discovery = KubernetesDiscovery::with_mock_addresses(config, mock_addresses.clone());
        let result = discovery.discover().await.unwrap();

        assert_eq!(result.len(), 3);
        assert_eq!(result, mock_addresses);
    }

    #[tokio::test]
    async fn test_mock_discovery_empty() {
        let config = KubernetesDiscoveryConfig::new();
        let discovery = KubernetesDiscovery::with_mock_addresses(config, vec![]);
        let result = discovery.discover().await.unwrap();

        assert!(result.is_empty());
    }

    #[test]
    fn test_partial_label_config_ignored() {
        let mut config = KubernetesDiscoveryConfig::new();
        config.pod_label_name = Some("app".to_string());

        let discovery = KubernetesDiscovery::new(config);
        let selector = discovery.build_label_selector();

        assert!(selector.is_none());
    }
}
