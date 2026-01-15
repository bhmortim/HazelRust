//! Kubernetes-based cluster discovery for Hazelcast client.

use std::fmt;
use std::net::SocketAddr;

use async_trait::async_trait;
use k8s_openapi::api::core::v1::{Endpoints, Pod};
use kube::api::ListParams;
use kube::{Api, Client};
use tracing::debug;

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

/// Discovered pod information from Kubernetes API.
#[derive(Debug, Clone)]
pub struct DiscoveredPod {
    /// Pod IP address.
    pub ip: String,
    /// Pod phase (Running, Pending, etc.).
    pub phase: String,
}

/// Discovered endpoint address from Kubernetes API.
#[derive(Debug, Clone)]
pub struct DiscoveredEndpoint {
    /// Endpoint IP address.
    pub ip: String,
}

/// Trait for Kubernetes client operations, enabling mock injection for tests.
#[async_trait]
pub trait K8sClientProvider: Send + Sync {
    /// Lists pods matching the given label selector in the specified namespace.
    async fn list_pods(
        &self,
        namespace: &str,
        label_selector: Option<&str>,
    ) -> Result<Vec<DiscoveredPod>, HazelcastClientError>;

    /// Gets endpoints for a service in the specified namespace.
    async fn get_service_endpoints(
        &self,
        namespace: &str,
        service_name: &str,
    ) -> Result<Vec<DiscoveredEndpoint>, HazelcastClientError>;
}

/// Production Kubernetes client using kube-rs.
pub struct KubeRsClient;

impl KubeRsClient {
    /// Creates a new kube-rs client.
    pub fn new() -> Self {
        Self
    }
}

impl Default for KubeRsClient {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl K8sClientProvider for KubeRsClient {
    async fn list_pods(
        &self,
        namespace: &str,
        label_selector: Option<&str>,
    ) -> Result<Vec<DiscoveredPod>, HazelcastClientError> {
        let client = Client::try_default().await.map_err(|e| {
            HazelcastClientError::Connection(format!("Failed to create Kubernetes client: {}", e))
        })?;

        let pods: Api<Pod> = Api::namespaced(client, namespace);

        let list_params = if let Some(selector) = label_selector {
            ListParams::default().labels(selector)
        } else {
            ListParams::default()
        };

        let pod_list = pods.list(&list_params).await.map_err(|e| {
            HazelcastClientError::Connection(format!("Failed to list pods: {}", e))
        })?;

        let mut discovered = Vec::new();

        for pod in pod_list {
            if let Some(status) = pod.status {
                let phase = status.phase.unwrap_or_else(|| "Unknown".to_string());
                if let Some(pod_ip) = status.pod_ip {
                    discovered.push(DiscoveredPod { ip: pod_ip, phase });
                }
            }
        }

        Ok(discovered)
    }

    async fn get_service_endpoints(
        &self,
        namespace: &str,
        service_name: &str,
    ) -> Result<Vec<DiscoveredEndpoint>, HazelcastClientError> {
        let client = Client::try_default().await.map_err(|e| {
            HazelcastClientError::Connection(format!("Failed to create Kubernetes client: {}", e))
        })?;

        let endpoints: Api<Endpoints> = Api::namespaced(client, namespace);
        let ep = endpoints.get(service_name).await.map_err(|e| {
            HazelcastClientError::Connection(format!(
                "Failed to get endpoints for service '{}': {}",
                service_name, e
            ))
        })?;

        let mut discovered = Vec::new();

        if let Some(subsets) = ep.subsets {
            for subset in subsets {
                if let Some(addrs) = subset.addresses {
                    for addr in addrs {
                        discovered.push(DiscoveredEndpoint { ip: addr.ip });
                    }
                }
            }
        }

        Ok(discovered)
    }
}

/// Kubernetes-based cluster discovery.
///
/// Discovers Hazelcast cluster members by querying the Kubernetes API
/// for pods or service endpoints matching the configured selectors.
pub struct KubernetesDiscovery {
    config: KubernetesDiscoveryConfig,
    k8s_client: Box<dyn K8sClientProvider>,
}

impl fmt::Debug for KubernetesDiscovery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KubernetesDiscovery")
            .field("config", &self.config)
            .finish()
    }
}

impl KubernetesDiscovery {
    /// Creates a new Kubernetes discovery instance with the given configuration.
    pub fn new(config: KubernetesDiscoveryConfig) -> Self {
        Self {
            config,
            k8s_client: Box::new(KubeRsClient::new()),
        }
    }

    #[cfg(test)]
    fn with_mock_client(
        config: KubernetesDiscoveryConfig,
        k8s_client: Box<dyn K8sClientProvider>,
    ) -> Self {
        Self { config, k8s_client }
    }

    /// Returns the discovery configuration.
    pub fn config(&self) -> &KubernetesDiscoveryConfig {
        &self.config
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

    async fn discover_from_service(&self) -> Result<Vec<SocketAddr>, HazelcastClientError> {
        let service_name = self.config.service_name.as_ref().ok_or_else(|| {
            HazelcastClientError::Connection("Service name not configured".to_string())
        })?;

        let endpoints = self
            .k8s_client
            .get_service_endpoints(self.namespace(), service_name)
            .await?;

        let port = self.port();
        let mut addresses = Vec::new();

        for endpoint in endpoints {
            if let Ok(ip) = endpoint.ip.parse() {
                let addr = SocketAddr::new(ip, port);
                addresses.push(addr);
                debug!("Discovered Hazelcast member from service endpoint: {}", addr);
            }
        }

        Ok(addresses)
    }

    async fn discover_from_pods(&self) -> Result<Vec<SocketAddr>, HazelcastClientError> {
        let label_selector = self.build_label_selector();
        let pods = self
            .k8s_client
            .list_pods(self.namespace(), label_selector.as_deref())
            .await?;

        let port = self.port();
        let mut addresses = Vec::new();

        for pod in pods {
            if pod.phase != "Running" {
                continue;
            }
            if let Ok(ip) = pod.ip.parse() {
                let addr = SocketAddr::new(ip, port);
                addresses.push(addr);
                debug!("Discovered Hazelcast member from pod: {}", addr);
            }
        }

        Ok(addresses)
    }
}

#[async_trait]
impl ClusterDiscovery for KubernetesDiscovery {
    async fn discover(&self) -> Result<Vec<SocketAddr>, HazelcastClientError> {
        let addresses = if self.config.service_name.is_some() {
            self.discover_from_service().await?
        } else {
            self.discover_from_pods().await?
        };

        if addresses.is_empty() {
            tracing::warn!("Kubernetes discovery found no Hazelcast members");
        } else {
            tracing::info!(
                "Kubernetes discovery found {} Hazelcast member(s)",
                addresses.len()
            );
        }

        Ok(addresses)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};
    use std::sync::Mutex;

    struct MockK8sClient {
        pods: Mutex<Vec<DiscoveredPod>>,
        endpoints: Mutex<Vec<DiscoveredEndpoint>>,
        should_error: bool,
    }

    impl MockK8sClient {
        fn new(pods: Vec<DiscoveredPod>, endpoints: Vec<DiscoveredEndpoint>) -> Self {
            Self {
                pods: Mutex::new(pods),
                endpoints: Mutex::new(endpoints),
                should_error: false,
            }
        }

        fn with_error() -> Self {
            Self {
                pods: Mutex::new(vec![]),
                endpoints: Mutex::new(vec![]),
                should_error: true,
            }
        }
    }

    #[async_trait]
    impl K8sClientProvider for MockK8sClient {
        async fn list_pods(
            &self,
            _namespace: &str,
            _label_selector: Option<&str>,
        ) -> Result<Vec<DiscoveredPod>, HazelcastClientError> {
            if self.should_error {
                return Err(HazelcastClientError::Connection("Mock K8s API error".into()));
            }
            Ok(self.pods.lock().unwrap().clone())
        }

        async fn get_service_endpoints(
            &self,
            _namespace: &str,
            _service_name: &str,
        ) -> Result<Vec<DiscoveredEndpoint>, HazelcastClientError> {
            if self.should_error {
                return Err(HazelcastClientError::Connection("Mock K8s API error".into()));
            }
            Ok(self.endpoints.lock().unwrap().clone())
        }
    }

    fn create_mock_discovery_pods(
        config: KubernetesDiscoveryConfig,
        pods: Vec<DiscoveredPod>,
    ) -> KubernetesDiscovery {
        KubernetesDiscovery::with_mock_client(config, Box::new(MockK8sClient::new(pods, vec![])))
    }

    fn create_mock_discovery_endpoints(
        config: KubernetesDiscoveryConfig,
        endpoints: Vec<DiscoveredEndpoint>,
    ) -> KubernetesDiscovery {
        KubernetesDiscovery::with_mock_client(config, Box::new(MockK8sClient::new(vec![], endpoints)))
    }

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

    #[test]
    fn test_partial_label_config_ignored() {
        let mut config = KubernetesDiscoveryConfig::new();
        config.pod_label_name = Some("app".to_string());

        let discovery = KubernetesDiscovery::new(config);
        let selector = discovery.build_label_selector();

        assert!(selector.is_none());
    }

    #[tokio::test]
    async fn test_pod_discovery_parses_running_pods() {
        let config = KubernetesDiscoveryConfig::new()
            .namespace("hazelcast")
            .pod_label("app", "hazelcast")
            .port(5701);

        let pods = vec![
            DiscoveredPod {
                ip: "10.0.0.1".to_string(),
                phase: "Running".to_string(),
            },
            DiscoveredPod {
                ip: "10.0.0.2".to_string(),
                phase: "Running".to_string(),
            },
            DiscoveredPod {
                ip: "10.0.0.3".to_string(),
                phase: "Running".to_string(),
            },
        ];

        let discovery = create_mock_discovery_pods(config, pods);
        let addresses = discovery.discover().await.unwrap();

        assert_eq!(addresses.len(), 3);
        assert!(addresses.contains(&SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 5701)));
        assert!(addresses.contains(&SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)), 5701)));
        assert!(addresses.contains(&SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 3)), 5701)));
    }

    #[tokio::test]
    async fn test_pod_discovery_filters_non_running_pods() {
        let config = KubernetesDiscoveryConfig::new()
            .namespace("test")
            .pod_label("app", "hazelcast");

        let pods = vec![
            DiscoveredPod {
                ip: "10.0.0.1".to_string(),
                phase: "Running".to_string(),
            },
            DiscoveredPod {
                ip: "10.0.0.2".to_string(),
                phase: "Pending".to_string(),
            },
            DiscoveredPod {
                ip: "10.0.0.3".to_string(),
                phase: "Terminated".to_string(),
            },
        ];

        let discovery = create_mock_discovery_pods(config, pods);
        let addresses = discovery.discover().await.unwrap();

        assert_eq!(addresses.len(), 1);
        assert_eq!(addresses[0], "10.0.0.1:5701".parse().unwrap());
    }

    #[tokio::test]
    async fn test_service_discovery_parses_endpoints() {
        let config = KubernetesDiscoveryConfig::new()
            .namespace("hazelcast")
            .service_name("hz-service")
            .port(5702);

        let endpoints = vec![
            DiscoveredEndpoint { ip: "10.1.0.1".to_string() },
            DiscoveredEndpoint { ip: "10.1.0.2".to_string() },
        ];

        let discovery = create_mock_discovery_endpoints(config, endpoints);
        let addresses = discovery.discover().await.unwrap();

        assert_eq!(addresses.len(), 2);
        assert!(addresses.contains(&"10.1.0.1:5702".parse().unwrap()));
        assert!(addresses.contains(&"10.1.0.2:5702".parse().unwrap()));
    }

    #[tokio::test]
    async fn test_discovery_handles_empty_response() {
        let config = KubernetesDiscoveryConfig::new()
            .namespace("empty");

        let discovery = create_mock_discovery_pods(config, vec![]);
        let addresses = discovery.discover().await.unwrap();

        assert!(addresses.is_empty());
    }

    #[tokio::test]
    async fn test_discovery_handles_api_error() {
        let config = KubernetesDiscoveryConfig::new()
            .namespace("test");

        let discovery =
            KubernetesDiscovery::with_mock_client(config, Box::new(MockK8sClient::with_error()));

        let result = discovery.discover().await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Mock K8s API error"));
    }

    #[tokio::test]
    async fn test_pod_discovery_skips_invalid_ips() {
        let config = KubernetesDiscoveryConfig::new()
            .namespace("test");

        let pods = vec![
            DiscoveredPod {
                ip: "not-an-ip".to_string(),
                phase: "Running".to_string(),
            },
            DiscoveredPod {
                ip: "10.0.0.5".to_string(),
                phase: "Running".to_string(),
            },
        ];

        let discovery = create_mock_discovery_pods(config, pods);
        let addresses = discovery.discover().await.unwrap();

        assert_eq!(addresses.len(), 1);
        assert_eq!(addresses[0], "10.0.0.5:5701".parse().unwrap());
    }

    #[tokio::test]
    async fn test_service_discovery_skips_invalid_ips() {
        let config = KubernetesDiscoveryConfig::new()
            .namespace("test")
            .service_name("hz-service");

        let endpoints = vec![
            DiscoveredEndpoint { ip: "invalid".to_string() },
            DiscoveredEndpoint { ip: "10.0.0.10".to_string() },
        ];

        let discovery = create_mock_discovery_endpoints(config, endpoints);
        let addresses = discovery.discover().await.unwrap();

        assert_eq!(addresses.len(), 1);
        assert_eq!(addresses[0], "10.0.0.10:5701".parse().unwrap());
    }

    #[tokio::test]
    async fn test_pod_discovery_with_custom_port() {
        let config = KubernetesDiscoveryConfig::new()
            .namespace("hazelcast")
            .port(5710);

        let pods = vec![DiscoveredPod {
            ip: "192.168.1.100".to_string(),
            phase: "Running".to_string(),
        }];

        let discovery = create_mock_discovery_pods(config, pods);
        let addresses = discovery.discover().await.unwrap();

        assert_eq!(addresses.len(), 1);
        assert_eq!(addresses[0].port(), 5710);
    }

    #[test]
    fn test_discovered_pod_clone() {
        let pod = DiscoveredPod {
            ip: "10.0.0.1".to_string(),
            phase: "Running".to_string(),
        };
        let cloned = pod.clone();
        assert_eq!(cloned.ip, pod.ip);
        assert_eq!(cloned.phase, pod.phase);
    }

    #[test]
    fn test_discovered_endpoint_clone() {
        let endpoint = DiscoveredEndpoint {
            ip: "10.0.0.1".to_string(),
        };
        let cloned = endpoint.clone();
        assert_eq!(cloned.ip, endpoint.ip);
    }

    #[test]
    fn test_kubernetes_discovery_debug() {
        let config = KubernetesDiscoveryConfig::new()
            .namespace("test-ns");
        let discovery = KubernetesDiscovery::new(config);
        let debug_str = format!("{:?}", discovery);
        assert!(debug_str.contains("KubernetesDiscovery"));
        assert!(debug_str.contains("test-ns"));
    }

    #[test]
    fn test_kubernetes_discovery_config_accessor() {
        let config = KubernetesDiscoveryConfig::new()
            .namespace("my-namespace")
            .service_name("my-service");
        let discovery = KubernetesDiscovery::new(config);

        let retrieved_config = discovery.config();
        assert_eq!(retrieved_config.namespace.as_deref(), Some("my-namespace"));
        assert_eq!(retrieved_config.service_name.as_deref(), Some("my-service"));
    }

    #[test]
    fn test_kube_rs_client_default() {
        let client = KubeRsClient::default();
        let _ = client;
    }
}
