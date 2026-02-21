//! Eureka-based cluster discovery for Hazelcast.
//!
//! This module provides automatic discovery of Hazelcast cluster members
//! registered with a Netflix Eureka service registry. It queries the
//! Eureka REST API to find instances of a specified application.

use std::fmt;
use std::net::{IpAddr, SocketAddr};

use async_trait::async_trait;
use hazelcast_core::{HazelcastError, Result};
use tracing::debug;

use super::ClusterDiscovery;

/// Default Eureka namespace.
const DEFAULT_NAMESPACE: &str = "hazelcast";

/// Default Eureka application name.
const DEFAULT_APP_NAME: &str = "hazelcast";

/// Default Hazelcast port.
const DEFAULT_HAZELCAST_PORT: u16 = 5701;

/// Configuration for Eureka-based cluster discovery.
#[derive(Debug, Clone)]
pub struct EurekaDiscoveryConfig {
    /// Eureka server URL (e.g., "http://eureka-server:8761/eureka").
    service_url: String,
    /// Eureka namespace for Hazelcast properties.
    namespace: String,
    /// Application name registered in Eureka.
    app_name: String,
    /// Whether to use metadata for the host address instead of the hostname.
    use_metadata_for_host: bool,
    /// Hazelcast port (default: 5701).
    port: u16,
}

impl EurekaDiscoveryConfig {
    /// Creates a new Eureka discovery configuration with the given service URL.
    pub fn new(service_url: impl Into<String>) -> Self {
        Self {
            service_url: service_url.into(),
            namespace: DEFAULT_NAMESPACE.to_string(),
            app_name: DEFAULT_APP_NAME.to_string(),
            use_metadata_for_host: false,
            port: DEFAULT_HAZELCAST_PORT,
        }
    }

    /// Sets the Eureka namespace.
    pub fn with_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = namespace.into();
        self
    }

    /// Sets the application name to look up in Eureka.
    pub fn with_app_name(mut self, app_name: impl Into<String>) -> Self {
        self.app_name = app_name.into();
        self
    }

    /// Sets whether to use metadata for host resolution.
    pub fn with_use_metadata_for_host(mut self, use_metadata: bool) -> Self {
        self.use_metadata_for_host = use_metadata;
        self
    }

    /// Sets the Hazelcast port (default: 5701).
    pub fn with_port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    /// Returns the Eureka service URL.
    pub fn service_url(&self) -> &str {
        &self.service_url
    }

    /// Returns the Eureka namespace.
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Returns the application name.
    pub fn app_name(&self) -> &str {
        &self.app_name
    }

    /// Returns whether metadata is used for host resolution.
    pub fn use_metadata_for_host(&self) -> bool {
        self.use_metadata_for_host
    }

    /// Returns the Hazelcast port.
    pub fn port(&self) -> u16 {
        self.port
    }
}

/// Discovered Eureka instance information.
#[derive(Debug, Clone)]
pub struct EurekaInstance {
    /// IP address of the instance.
    pub ip_addr: Option<String>,
    /// Hostname of the instance.
    pub hostname: Option<String>,
    /// Port of the instance.
    pub port: Option<u16>,
    /// Instance status (UP, DOWN, etc.).
    pub status: String,
    /// Metadata key-value pairs.
    pub metadata: Vec<(String, String)>,
}

/// Trait for Eureka HTTP operations, enabling mock injection for tests.
#[async_trait]
trait EurekaHttpClient: Send + Sync {
    /// Fetches instances for the given application from Eureka.
    async fn get_instances(&self, url: &str) -> Result<Vec<EurekaInstance>>;
}

/// Production HTTP client using reqwest.
struct ReqwestEurekaClient {
    client: reqwest::Client,
}

impl ReqwestEurekaClient {
    fn new() -> Result<Self> {
        let client = reqwest::Client::builder()
            .build()
            .map_err(|e| HazelcastError::Connection(format!("Failed to create HTTP client: {}", e)))?;
        Ok(Self { client })
    }
}

#[async_trait]
impl EurekaHttpClient for ReqwestEurekaClient {
    async fn get_instances(&self, url: &str) -> Result<Vec<EurekaInstance>> {
        let response = self
            .client
            .get(url)
            .header("Accept", "application/json")
            .send()
            .await
            .map_err(|e| HazelcastError::Connection(format!("Eureka request failed: {}", e)))?;

        if !response.status().is_success() {
            return Err(HazelcastError::Connection(format!(
                "Eureka API error: {} - {}",
                response.status(),
                response.text().await.unwrap_or_default()
            )));
        }

        let body = response
            .text()
            .await
            .map_err(|e| HazelcastError::Connection(format!("Failed to read Eureka response: {}", e)))?;

        parse_eureka_response(&body)
    }
}

/// Parses a Eureka JSON response into a list of discovered instances.
fn parse_eureka_response(body: &str) -> Result<Vec<EurekaInstance>> {
    // Eureka REST API returns JSON like:
    // { "application": { "instance": [ { "ipAddr": "...", "hostName": "...", ... } ] } }
    // We do minimal JSON parsing to avoid requiring serde_json as a mandatory dependency.
    let mut instances = Vec::new();

    // Find instances array in the response
    let instance_marker = "\"instance\"";
    if let Some(pos) = body.find(instance_marker) {
        let remainder = &body[pos + instance_marker.len()..];
        // Find the start of the array or single object
        if let Some(arr_start) = remainder.find('[') {
            let arr_content = &remainder[arr_start..];
            // Parse each instance object
            let mut depth = 0;
            let mut obj_start = None;

            for (i, ch) in arr_content.char_indices() {
                match ch {
                    '{' => {
                        if depth == 1 && obj_start.is_none() {
                            obj_start = Some(i);
                        }
                        depth += 1;
                    }
                    '}' => {
                        depth -= 1;
                        if depth == 1 {
                            if let Some(start) = obj_start.take() {
                                let obj_str = &arr_content[start..=i];
                                if let Some(instance) = parse_instance_object(obj_str) {
                                    instances.push(instance);
                                }
                            }
                        }
                    }
                    '[' if depth == 0 => {
                        depth = 1;
                    }
                    ']' if depth == 1 => {
                        break;
                    }
                    _ => {}
                }
            }
        } else if let Some(obj_start) = remainder.find('{') {
            // Single instance (not wrapped in array)
            let obj_content = &remainder[obj_start..];
            let mut depth = 0;
            for (i, ch) in obj_content.char_indices() {
                match ch {
                    '{' => depth += 1,
                    '}' => {
                        depth -= 1;
                        if depth == 0 {
                            let obj_str = &obj_content[..=i];
                            if let Some(instance) = parse_instance_object(obj_str) {
                                instances.push(instance);
                            }
                            break;
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    Ok(instances)
}

/// Extracts a simple JSON string value by key from a JSON object string.
fn extract_json_string(obj: &str, key: &str) -> Option<String> {
    let search = format!("\"{}\"", key);
    if let Some(pos) = obj.find(&search) {
        let after_key = &obj[pos + search.len()..];
        // Skip whitespace and colon
        let after_colon = after_key.trim_start().strip_prefix(':')?;
        let trimmed = after_colon.trim_start();
        if trimmed.starts_with('"') {
            let value_start = 1;
            let value_end = trimmed[value_start..].find('"')?;
            return Some(trimmed[value_start..value_start + value_end].to_string());
        }
    }
    None
}

/// Extracts a numeric value by key from a JSON object string.
fn extract_json_number(obj: &str, key: &str) -> Option<u16> {
    let search = format!("\"{}\"", key);
    if let Some(pos) = obj.find(&search) {
        let after_key = &obj[pos + search.len()..];
        let after_colon = after_key.trim_start().strip_prefix(':')?;
        let trimmed = after_colon.trim_start();
        // Handle both quoted and unquoted numbers, and {"$": 8761} format
        let value_str = if trimmed.starts_with('"') {
            let end = trimmed[1..].find('"')?;
            &trimmed[1..1 + end]
        } else {
            let end = trimmed.find(|c: char| !c.is_ascii_digit())?;
            &trimmed[..end]
        };
        return value_str.parse().ok();
    }
    None
}

/// Parses a single Eureka instance JSON object.
fn parse_instance_object(obj: &str) -> Option<EurekaInstance> {
    let status = extract_json_string(obj, "status").unwrap_or_else(|| "UNKNOWN".to_string());
    let ip_addr = extract_json_string(obj, "ipAddr");
    let hostname = extract_json_string(obj, "hostName");

    // Port can be in a nested object: "port": {"$": 5701, "@enabled": "true"}
    let port = extract_json_number(obj, "$")
        .or_else(|| extract_json_number(obj, "port"));

    Some(EurekaInstance {
        ip_addr,
        hostname,
        port,
        status,
        metadata: Vec::new(),
    })
}

/// Eureka-based cluster discovery.
///
/// Discovers Hazelcast cluster members by querying a Netflix Eureka
/// service registry for instances of the configured application name.
pub struct EurekaDiscovery {
    config: EurekaDiscoveryConfig,
    http_client: Box<dyn EurekaHttpClient>,
}

impl fmt::Debug for EurekaDiscovery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EurekaDiscovery")
            .field("config", &self.config)
            .finish()
    }
}

impl EurekaDiscovery {
    /// Creates a new Eureka discovery instance with the given configuration.
    pub fn new(config: EurekaDiscoveryConfig) -> Result<Self> {
        Ok(Self {
            config,
            http_client: Box::new(ReqwestEurekaClient::new()?),
        })
    }

    #[cfg(test)]
    fn with_mock_client(
        config: EurekaDiscoveryConfig,
        http_client: Box<dyn EurekaHttpClient>,
    ) -> Self {
        Self {
            config,
            http_client,
        }
    }

    /// Returns the discovery configuration.
    pub fn config(&self) -> &EurekaDiscoveryConfig {
        &self.config
    }

    /// Builds the Eureka REST API URL for querying application instances.
    fn build_eureka_url(&self) -> String {
        let base = self.config.service_url.trim_end_matches('/');
        format!("{}/apps/{}", base, self.config.app_name)
    }

    /// Extracts the host address from a discovered instance.
    fn extract_host(&self, instance: &EurekaInstance) -> Option<String> {
        if self.config.use_metadata_for_host {
            // Check metadata for a host override
            instance
                .metadata
                .iter()
                .find(|(k, _)| k == "hazelcast.host" || k == "host")
                .map(|(_, v)| v.clone())
                .or_else(|| instance.ip_addr.clone())
        } else {
            instance.ip_addr.clone().or_else(|| instance.hostname.clone())
        }
    }
}

#[async_trait]
impl ClusterDiscovery for EurekaDiscovery {
    async fn discover(&self) -> Result<Vec<SocketAddr>> {
        let url = self.build_eureka_url();
        let instances = self.http_client.get_instances(&url).await?;

        let mut addresses = Vec::new();

        for instance in &instances {
            if instance.status != "UP" {
                debug!("Skipping Eureka instance with status: {}", instance.status);
                continue;
            }

            if let Some(host) = self.extract_host(instance) {
                let port = instance.port.unwrap_or(self.config.port);
                if let Ok(ip) = host.parse::<IpAddr>() {
                    let addr = SocketAddr::new(ip, port);
                    addresses.push(addr);
                    debug!("Discovered Hazelcast member from Eureka: {}", addr);
                }
            }
        }

        if addresses.is_empty() {
            tracing::warn!("Eureka discovery found no Hazelcast members");
        } else {
            tracing::info!(
                "Eureka discovery found {} Hazelcast member(s)",
                addresses.len()
            );
        }

        Ok(addresses)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    struct MockEurekaClient {
        instances: Mutex<Vec<EurekaInstance>>,
        should_error: bool,
    }

    impl MockEurekaClient {
        fn new(instances: Vec<EurekaInstance>) -> Self {
            Self {
                instances: Mutex::new(instances),
                should_error: false,
            }
        }

        fn with_error() -> Self {
            Self {
                instances: Mutex::new(vec![]),
                should_error: true,
            }
        }
    }

    #[async_trait]
    impl EurekaHttpClient for MockEurekaClient {
        async fn get_instances(&self, _url: &str) -> Result<Vec<EurekaInstance>> {
            if self.should_error {
                return Err(HazelcastError::Connection("Mock Eureka API error".into()));
            }
            Ok(self.instances.lock().unwrap().clone())
        }
    }

    fn create_mock_discovery(
        config: EurekaDiscoveryConfig,
        instances: Vec<EurekaInstance>,
    ) -> EurekaDiscovery {
        EurekaDiscovery::with_mock_client(config, Box::new(MockEurekaClient::new(instances)))
    }

    #[test]
    fn test_config_new() {
        let config = EurekaDiscoveryConfig::new("http://eureka:8761/eureka");
        assert_eq!(config.service_url(), "http://eureka:8761/eureka");
        assert_eq!(config.namespace(), "hazelcast");
        assert_eq!(config.app_name(), "hazelcast");
        assert!(!config.use_metadata_for_host());
        assert_eq!(config.port(), 5701);
    }

    #[test]
    fn test_config_builder() {
        let config = EurekaDiscoveryConfig::new("http://eureka:8761/eureka")
            .with_namespace("custom-ns")
            .with_app_name("my-hazelcast")
            .with_use_metadata_for_host(true)
            .with_port(5702);

        assert_eq!(config.service_url(), "http://eureka:8761/eureka");
        assert_eq!(config.namespace(), "custom-ns");
        assert_eq!(config.app_name(), "my-hazelcast");
        assert!(config.use_metadata_for_host());
        assert_eq!(config.port(), 5702);
    }

    #[test]
    fn test_config_clone() {
        let config = EurekaDiscoveryConfig::new("http://eureka:8761/eureka")
            .with_app_name("test-app");
        let cloned = config.clone();
        assert_eq!(cloned.service_url(), "http://eureka:8761/eureka");
        assert_eq!(cloned.app_name(), "test-app");
    }

    #[test]
    fn test_config_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<EurekaDiscoveryConfig>();
    }

    #[test]
    fn test_config_debug() {
        let config = EurekaDiscoveryConfig::new("http://eureka:8761/eureka");
        let debug_str = format!("{:?}", config);
        assert!(debug_str.contains("EurekaDiscoveryConfig"));
        assert!(debug_str.contains("eureka"));
    }

    #[test]
    fn test_build_eureka_url() {
        let config = EurekaDiscoveryConfig::new("http://eureka:8761/eureka")
            .with_app_name("my-app");
        let discovery = create_mock_discovery(config, vec![]);
        let url = discovery.build_eureka_url();
        assert_eq!(url, "http://eureka:8761/eureka/apps/my-app");
    }

    #[test]
    fn test_build_eureka_url_trailing_slash() {
        let config = EurekaDiscoveryConfig::new("http://eureka:8761/eureka/")
            .with_app_name("my-app");
        let discovery = create_mock_discovery(config, vec![]);
        let url = discovery.build_eureka_url();
        assert_eq!(url, "http://eureka:8761/eureka/apps/my-app");
    }

    #[tokio::test]
    async fn test_discovery_returns_up_instances() {
        let config = EurekaDiscoveryConfig::new("http://eureka:8761/eureka")
            .with_port(5701);

        let instances = vec![
            EurekaInstance {
                ip_addr: Some("10.0.0.1".to_string()),
                hostname: Some("host1".to_string()),
                port: Some(5701),
                status: "UP".to_string(),
                metadata: Vec::new(),
            },
            EurekaInstance {
                ip_addr: Some("10.0.0.2".to_string()),
                hostname: Some("host2".to_string()),
                port: Some(5701),
                status: "UP".to_string(),
                metadata: Vec::new(),
            },
        ];

        let discovery = create_mock_discovery(config, instances);
        let addresses = discovery.discover().await.unwrap();

        assert_eq!(addresses.len(), 2);
        assert!(addresses.contains(&"10.0.0.1:5701".parse().unwrap()));
        assert!(addresses.contains(&"10.0.0.2:5701".parse().unwrap()));
    }

    #[tokio::test]
    async fn test_discovery_filters_down_instances() {
        let config = EurekaDiscoveryConfig::new("http://eureka:8761/eureka");

        let instances = vec![
            EurekaInstance {
                ip_addr: Some("10.0.0.1".to_string()),
                hostname: None,
                port: Some(5701),
                status: "UP".to_string(),
                metadata: Vec::new(),
            },
            EurekaInstance {
                ip_addr: Some("10.0.0.2".to_string()),
                hostname: None,
                port: Some(5701),
                status: "DOWN".to_string(),
                metadata: Vec::new(),
            },
            EurekaInstance {
                ip_addr: Some("10.0.0.3".to_string()),
                hostname: None,
                port: Some(5701),
                status: "OUT_OF_SERVICE".to_string(),
                metadata: Vec::new(),
            },
        ];

        let discovery = create_mock_discovery(config, instances);
        let addresses = discovery.discover().await.unwrap();

        assert_eq!(addresses.len(), 1);
        assert_eq!(addresses[0], "10.0.0.1:5701".parse().unwrap());
    }

    #[tokio::test]
    async fn test_discovery_uses_default_port() {
        let config = EurekaDiscoveryConfig::new("http://eureka:8761/eureka")
            .with_port(5702);

        let instances = vec![EurekaInstance {
            ip_addr: Some("10.0.0.1".to_string()),
            hostname: None,
            port: None,
            status: "UP".to_string(),
            metadata: Vec::new(),
        }];

        let discovery = create_mock_discovery(config, instances);
        let addresses = discovery.discover().await.unwrap();

        assert_eq!(addresses.len(), 1);
        assert_eq!(addresses[0], "10.0.0.1:5702".parse().unwrap());
    }

    #[tokio::test]
    async fn test_discovery_handles_empty_response() {
        let config = EurekaDiscoveryConfig::new("http://eureka:8761/eureka");
        let discovery = create_mock_discovery(config, vec![]);
        let addresses = discovery.discover().await.unwrap();

        assert!(addresses.is_empty());
    }

    #[tokio::test]
    async fn test_discovery_handles_api_error() {
        let config = EurekaDiscoveryConfig::new("http://eureka:8761/eureka");
        let discovery = EurekaDiscovery::with_mock_client(
            config,
            Box::new(MockEurekaClient::with_error()),
        );

        let result = discovery.discover().await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Mock Eureka API error"));
    }

    #[tokio::test]
    async fn test_discovery_uses_metadata_for_host() {
        let config = EurekaDiscoveryConfig::new("http://eureka:8761/eureka")
            .with_use_metadata_for_host(true);

        let instances = vec![EurekaInstance {
            ip_addr: Some("10.0.0.1".to_string()),
            hostname: Some("host1.example.com".to_string()),
            port: Some(5701),
            status: "UP".to_string(),
            metadata: vec![("hazelcast.host".to_string(), "172.16.0.1".to_string())],
        }];

        let discovery = create_mock_discovery(config, instances);
        let addresses = discovery.discover().await.unwrap();

        assert_eq!(addresses.len(), 1);
        assert_eq!(addresses[0], "172.16.0.1:5701".parse().unwrap());
    }

    #[tokio::test]
    async fn test_discovery_skips_invalid_ips() {
        let config = EurekaDiscoveryConfig::new("http://eureka:8761/eureka");

        let instances = vec![
            EurekaInstance {
                ip_addr: Some("not-an-ip".to_string()),
                hostname: None,
                port: Some(5701),
                status: "UP".to_string(),
                metadata: Vec::new(),
            },
            EurekaInstance {
                ip_addr: Some("10.0.0.5".to_string()),
                hostname: None,
                port: Some(5701),
                status: "UP".to_string(),
                metadata: Vec::new(),
            },
        ];

        let discovery = create_mock_discovery(config, instances);
        let addresses = discovery.discover().await.unwrap();

        assert_eq!(addresses.len(), 1);
        assert_eq!(addresses[0], "10.0.0.5:5701".parse().unwrap());
    }

    #[test]
    fn test_discovery_debug() {
        let config = EurekaDiscoveryConfig::new("http://eureka:8761/eureka");
        let discovery = create_mock_discovery(config, vec![]);
        let debug_str = format!("{:?}", discovery);
        assert!(debug_str.contains("EurekaDiscovery"));
    }

    #[test]
    fn test_discovery_config_accessor() {
        let config = EurekaDiscoveryConfig::new("http://eureka:8761/eureka")
            .with_app_name("test-app");
        let discovery = create_mock_discovery(config, vec![]);
        assert_eq!(discovery.config().app_name(), "test-app");
    }

    #[test]
    fn test_parse_eureka_response_with_instances() {
        let json = r#"{
            "application": {
                "name": "HAZELCAST",
                "instance": [
                    {
                        "ipAddr": "10.0.0.1",
                        "hostName": "host1",
                        "status": "UP",
                        "port": {"$": 5701, "@enabled": "true"}
                    },
                    {
                        "ipAddr": "10.0.0.2",
                        "hostName": "host2",
                        "status": "DOWN",
                        "port": {"$": 5701, "@enabled": "true"}
                    }
                ]
            }
        }"#;

        let instances = parse_eureka_response(json).unwrap();
        assert_eq!(instances.len(), 2);
        assert_eq!(instances[0].ip_addr, Some("10.0.0.1".to_string()));
        assert_eq!(instances[0].status, "UP");
        assert_eq!(instances[1].ip_addr, Some("10.0.0.2".to_string()));
        assert_eq!(instances[1].status, "DOWN");
    }

    #[test]
    fn test_parse_eureka_response_empty() {
        let json = r#"{"application": {"name": "HAZELCAST"}}"#;
        let instances = parse_eureka_response(json).unwrap();
        assert!(instances.is_empty());
    }

    #[test]
    fn test_eureka_instance_clone() {
        let instance = EurekaInstance {
            ip_addr: Some("10.0.0.1".to_string()),
            hostname: Some("host1".to_string()),
            port: Some(5701),
            status: "UP".to_string(),
            metadata: vec![("key".to_string(), "value".to_string())],
        };
        let cloned = instance.clone();
        assert_eq!(cloned.ip_addr, instance.ip_addr);
        assert_eq!(cloned.status, instance.status);
    }
}
