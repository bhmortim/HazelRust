//! AWS EC2/ECS cluster discovery for Hazelcast.
//!
//! This module provides automatic discovery of Hazelcast cluster members
//! running on AWS EC2 instances or ECS tasks using tag-based filtering.

use std::fmt;
use std::net::{IpAddr, SocketAddr};

use async_trait::async_trait;
use hazelcast_core::{HazelcastError, Result};
use tracing::debug;

use super::ClusterDiscovery;

/// Configuration for AWS-based cluster discovery.
#[derive(Debug, Clone)]
pub struct AwsDiscoveryConfig {
    /// AWS access key ID. If None, uses default credential chain.
    access_key: Option<String>,
    /// AWS secret access key. If None, uses default credential chain.
    secret_key: Option<String>,
    /// AWS region (e.g., "us-east-1").
    region: String,
    /// Security group ID to filter instances.
    security_group: Option<String>,
    /// Tag key to filter instances (e.g., "hazelcast:cluster").
    tag_key: Option<String>,
    /// Tag value to filter instances.
    tag_value: Option<String>,
    /// IAM role ARN for assuming role-based credentials.
    iam_role: Option<String>,
    /// Port to use for Hazelcast connections (default: 5701).
    port: u16,
    /// Whether to use private IP addresses (default: true).
    use_private_ip: bool,
}

impl AwsDiscoveryConfig {
    /// Creates a new AWS discovery configuration with the specified region.
    pub fn new(region: impl Into<String>) -> Self {
        Self {
            access_key: None,
            secret_key: None,
            region: region.into(),
            security_group: None,
            tag_key: None,
            tag_value: None,
            iam_role: None,
            port: 5701,
            use_private_ip: true,
        }
    }

    /// Sets AWS access credentials.
    pub fn with_credentials(mut self, access_key: impl Into<String>, secret_key: impl Into<String>) -> Self {
        self.access_key = Some(access_key.into());
        self.secret_key = Some(secret_key.into());
        self
    }

    /// Sets the security group filter.
    pub fn with_security_group(mut self, security_group: impl Into<String>) -> Self {
        self.security_group = Some(security_group.into());
        self
    }

    /// Sets tag-based filtering.
    pub fn with_tag(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.tag_key = Some(key.into());
        self.tag_value = Some(value.into());
        self
    }

    /// Sets the IAM role for assuming credentials.
    pub fn with_iam_role(mut self, role_arn: impl Into<String>) -> Self {
        self.iam_role = Some(role_arn.into());
        self
    }

    /// Sets the Hazelcast port (default: 5701).
    pub fn with_port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    /// Sets whether to use private IP addresses (default: true).
    pub fn with_use_private_ip(mut self, use_private: bool) -> Self {
        self.use_private_ip = use_private;
        self
    }

    /// Returns the configured region.
    pub fn region(&self) -> &str {
        &self.region
    }

    /// Returns the configured port.
    pub fn port(&self) -> u16 {
        self.port
    }

    /// Returns whether to use private IPs.
    pub fn use_private_ip(&self) -> bool {
        self.use_private_ip
    }

    /// Returns the security group filter if set.
    pub fn security_group(&self) -> Option<&str> {
        self.security_group.as_deref()
    }

    /// Returns the tag key filter if set.
    pub fn tag_key(&self) -> Option<&str> {
        self.tag_key.as_deref()
    }

    /// Returns the tag value filter if set.
    pub fn tag_value(&self) -> Option<&str> {
        self.tag_value.as_deref()
    }
}

impl Default for AwsDiscoveryConfig {
    fn default() -> Self {
        Self::new("us-east-1")
    }
}

/// Filter for EC2 instance queries.
#[derive(Debug, Clone)]
pub struct InstanceFilter {
    /// Filter name (e.g., "instance-state-name", "tag:Name").
    pub name: String,
    /// Filter values.
    pub values: Vec<String>,
}

impl InstanceFilter {
    /// Creates a new instance filter.
    pub fn new(name: impl Into<String>, values: Vec<String>) -> Self {
        Self {
            name: name.into(),
            values,
        }
    }
}

/// Discovered EC2 instance information.
#[derive(Debug, Clone)]
pub struct DiscoveredInstance {
    /// Private IP address if available.
    pub private_ip: Option<String>,
    /// Public IP address if available.
    pub public_ip: Option<String>,
}

/// Trait for EC2 client operations, enabling mock injection for tests.
#[async_trait]
pub trait Ec2ClientProvider: Send + Sync {
    /// Describes EC2 instances matching the given filters.
    async fn describe_instances(&self, filters: Vec<InstanceFilter>) -> Result<Vec<DiscoveredInstance>>;
}

/// Production EC2 client using AWS SDK.
pub struct AwsSdkEc2Client {
    config: AwsDiscoveryConfig,
}

impl AwsSdkEc2Client {
    /// Creates a new AWS SDK EC2 client with the given configuration.
    pub fn new(config: AwsDiscoveryConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl Ec2ClientProvider for AwsSdkEc2Client {
    async fn describe_instances(&self, filters: Vec<InstanceFilter>) -> Result<Vec<DiscoveredInstance>> {
        let mut config_loader = aws_config::from_env()
            .region(aws_sdk_ec2::config::Region::new(self.config.region.clone()));

        if let (Some(access_key), Some(secret_key)) = (&self.config.access_key, &self.config.secret_key) {
            let credentials = aws_sdk_ec2::config::Credentials::new(
                access_key.clone(),
                secret_key.clone(),
                None,
                None,
                "hazelcast-client",
            );
            config_loader = config_loader.credentials_provider(credentials);
        }

        let sdk_config = config_loader.load().await;
        let ec2_client = aws_sdk_ec2::Client::new(&sdk_config);

        let sdk_filters: Vec<aws_sdk_ec2::types::Filter> = filters
            .into_iter()
            .map(|f| {
                aws_sdk_ec2::types::Filter::builder()
                    .name(f.name)
                    .set_values(Some(f.values))
                    .build()
            })
            .collect();

        let response = ec2_client
            .describe_instances()
            .set_filters(Some(sdk_filters))
            .send()
            .await
            .map_err(|e| HazelcastError::Connection(format!("AWS EC2 API error: {}", e)))?;

        let mut instances = Vec::new();

        for reservation in response.reservations() {
            for instance in reservation.instances() {
                instances.push(DiscoveredInstance {
                    private_ip: instance.private_ip_address().map(|s| s.to_string()),
                    public_ip: instance.public_ip_address().map(|s| s.to_string()),
                });
            }
        }

        Ok(instances)
    }
}

/// AWS-based cluster discovery using EC2 instance metadata and tags.
pub struct AwsDiscovery {
    config: AwsDiscoveryConfig,
    ec2_client: Box<dyn Ec2ClientProvider>,
}

impl fmt::Debug for AwsDiscovery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AwsDiscovery")
            .field("config", &self.config)
            .finish()
    }
}

impl AwsDiscovery {
    /// Creates a new AWS discovery instance with the given configuration.
    pub fn new(config: AwsDiscoveryConfig) -> Self {
        let ec2_client = Box::new(AwsSdkEc2Client::new(config.clone()));
        Self { config, ec2_client }
    }

    #[cfg(test)]
    fn with_mock_client(config: AwsDiscoveryConfig, ec2_client: Box<dyn Ec2ClientProvider>) -> Self {
        Self { config, ec2_client }
    }

    /// Returns the discovery configuration.
    pub fn config(&self) -> &AwsDiscoveryConfig {
        &self.config
    }

    /// Builds EC2 filters based on configuration.
    fn build_filters(&self) -> Vec<InstanceFilter> {
        let mut filters = vec![InstanceFilter::new(
            "instance-state-name",
            vec!["running".to_string()],
        )];

        if let Some(sg) = &self.config.security_group {
            filters.push(InstanceFilter::new(
                "instance.group-id",
                vec![sg.clone()],
            ));
        }

        if let (Some(key), Some(value)) = (&self.config.tag_key, &self.config.tag_value) {
            filters.push(InstanceFilter::new(
                format!("tag:{}", key),
                vec![value.clone()],
            ));
        }

        filters
    }

    /// Extracts IP address from a discovered instance based on configuration.
    fn extract_ip(&self, instance: &DiscoveredInstance) -> Option<&str> {
        if self.config.use_private_ip {
            instance.private_ip.as_deref()
        } else {
            instance.public_ip.as_deref()
        }
    }
}

#[async_trait]
impl ClusterDiscovery for AwsDiscovery {
    async fn discover(&self) -> Result<Vec<SocketAddr>> {
        let filters = self.build_filters();
        let instances = self.ec2_client.describe_instances(filters).await?;

        let mut addresses = Vec::new();

        for instance in &instances {
            if let Some(ip_str) = self.extract_ip(instance) {
                if let Ok(ip) = ip_str.parse::<IpAddr>() {
                    let addr = SocketAddr::new(ip, self.config.port);
                    addresses.push(addr);
                    debug!("Discovered Hazelcast member at {}", addr);
                }
            }
        }

        if addresses.is_empty() {
            tracing::warn!("AWS discovery found no Hazelcast members");
        } else {
            tracing::info!("AWS discovery found {} Hazelcast member(s)", addresses.len());
        }

        Ok(addresses)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    struct MockEc2Client {
        instances: Mutex<Vec<DiscoveredInstance>>,
        should_error: bool,
    }

    impl MockEc2Client {
        fn new(instances: Vec<DiscoveredInstance>) -> Self {
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
    impl Ec2ClientProvider for MockEc2Client {
        async fn describe_instances(&self, _filters: Vec<InstanceFilter>) -> Result<Vec<DiscoveredInstance>> {
            if self.should_error {
                return Err(HazelcastError::Connection("Mock EC2 API error".into()));
            }
            Ok(self.instances.lock().unwrap().clone())
        }
    }

    fn create_mock_discovery(
        config: AwsDiscoveryConfig,
        instances: Vec<DiscoveredInstance>,
    ) -> AwsDiscovery {
        AwsDiscovery::with_mock_client(config, Box::new(MockEc2Client::new(instances)))
    }

    #[test]
    fn test_aws_config_default() {
        let config = AwsDiscoveryConfig::default();
        assert_eq!(config.region(), "us-east-1");
        assert_eq!(config.port(), 5701);
        assert!(config.use_private_ip());
        assert!(config.security_group().is_none());
        assert!(config.tag_key().is_none());
    }

    #[test]
    fn test_aws_config_builder() {
        let config = AwsDiscoveryConfig::new("eu-west-1")
            .with_credentials("access", "secret")
            .with_security_group("sg-12345")
            .with_tag("hazelcast:cluster", "production")
            .with_iam_role("arn:aws:iam::123456789:role/hazelcast")
            .with_port(5702)
            .with_use_private_ip(false);

        assert_eq!(config.region(), "eu-west-1");
        assert_eq!(config.port(), 5702);
        assert!(!config.use_private_ip());
        assert_eq!(config.security_group(), Some("sg-12345"));
        assert_eq!(config.tag_key(), Some("hazelcast:cluster"));
        assert_eq!(config.tag_value(), Some("production"));
    }

    #[test]
    fn test_aws_discovery_creation() {
        let config = AwsDiscoveryConfig::new("ap-southeast-1")
            .with_tag("Name", "hazelcast-node");

        let discovery = AwsDiscovery::new(config);
        assert_eq!(discovery.config().region(), "ap-southeast-1");
    }

    #[test]
    fn test_aws_discovery_filters() {
        let config = AwsDiscoveryConfig::new("us-west-2")
            .with_security_group("sg-abc123")
            .with_tag("env", "staging");

        let discovery = AwsDiscovery::new(config);
        let filters = discovery.build_filters();

        assert_eq!(filters.len(), 3);
        assert_eq!(filters[0].name, "instance-state-name");
        assert_eq!(filters[0].values, vec!["running"]);
        assert_eq!(filters[1].name, "instance.group-id");
        assert_eq!(filters[1].values, vec!["sg-abc123"]);
        assert_eq!(filters[2].name, "tag:env");
        assert_eq!(filters[2].values, vec!["staging"]);
    }

    #[test]
    fn test_aws_discovery_filters_minimal() {
        let config = AwsDiscoveryConfig::new("us-west-2");
        let discovery = AwsDiscovery::new(config);
        let filters = discovery.build_filters();

        assert_eq!(filters.len(), 1);
        assert_eq!(filters[0].name, "instance-state-name");
    }

    #[test]
    fn test_aws_config_clone() {
        let config = AwsDiscoveryConfig::new("eu-central-1")
            .with_tag("cluster", "test");

        let cloned = config.clone();
        assert_eq!(cloned.region(), "eu-central-1");
        assert_eq!(cloned.tag_key(), Some("cluster"));
    }

    #[test]
    fn test_aws_config_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<AwsDiscoveryConfig>();
    }

    #[test]
    fn test_aws_config_debug() {
        let config = AwsDiscoveryConfig::new("us-east-2")
            .with_security_group("sg-test");
        let debug_str = format!("{:?}", config);
        assert!(debug_str.contains("us-east-2"));
        assert!(debug_str.contains("sg-test"));
    }

    #[tokio::test]
    async fn test_aws_discovery_parses_private_ips() {
        let config = AwsDiscoveryConfig::new("us-west-2").with_port(5701);

        let instances = vec![
            DiscoveredInstance {
                private_ip: Some("10.0.0.1".to_string()),
                public_ip: Some("54.1.2.3".to_string()),
            },
            DiscoveredInstance {
                private_ip: Some("10.0.0.2".to_string()),
                public_ip: Some("54.1.2.4".to_string()),
            },
        ];

        let discovery = create_mock_discovery(config, instances);
        let addresses = discovery.discover().await.unwrap();

        assert_eq!(addresses.len(), 2);
        assert!(addresses.contains(&"10.0.0.1:5701".parse().unwrap()));
        assert!(addresses.contains(&"10.0.0.2:5701".parse().unwrap()));
    }

    #[tokio::test]
    async fn test_aws_discovery_uses_public_ips_when_configured() {
        let config = AwsDiscoveryConfig::new("us-west-2")
            .with_port(5702)
            .with_use_private_ip(false);

        let instances = vec![
            DiscoveredInstance {
                private_ip: Some("10.0.0.1".to_string()),
                public_ip: Some("54.1.2.3".to_string()),
            },
            DiscoveredInstance {
                private_ip: Some("10.0.0.2".to_string()),
                public_ip: Some("54.1.2.4".to_string()),
            },
        ];

        let discovery = create_mock_discovery(config, instances);
        let addresses = discovery.discover().await.unwrap();

        assert_eq!(addresses.len(), 2);
        assert!(addresses.contains(&"54.1.2.3:5702".parse().unwrap()));
        assert!(addresses.contains(&"54.1.2.4:5702".parse().unwrap()));
    }

    #[tokio::test]
    async fn test_aws_discovery_handles_empty_response() {
        let config = AwsDiscoveryConfig::new("us-west-2");
        let discovery = create_mock_discovery(config, vec![]);
        let addresses = discovery.discover().await.unwrap();

        assert!(addresses.is_empty());
    }

    #[tokio::test]
    async fn test_aws_discovery_skips_instances_without_ip() {
        let config = AwsDiscoveryConfig::new("us-west-2");

        let instances = vec![
            DiscoveredInstance {
                private_ip: None,
                public_ip: None,
            },
            DiscoveredInstance {
                private_ip: Some("10.0.0.5".to_string()),
                public_ip: None,
            },
        ];

        let discovery = create_mock_discovery(config, instances);
        let addresses = discovery.discover().await.unwrap();

        assert_eq!(addresses.len(), 1);
        assert_eq!(addresses[0], "10.0.0.5:5701".parse().unwrap());
    }

    #[tokio::test]
    async fn test_aws_discovery_handles_api_error() {
        let config = AwsDiscoveryConfig::new("us-west-2");
        let discovery = AwsDiscovery::with_mock_client(config, Box::new(MockEc2Client::with_error()));

        let result = discovery.discover().await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Mock EC2 API error"));
    }

    #[tokio::test]
    async fn test_aws_discovery_skips_invalid_ips() {
        let config = AwsDiscoveryConfig::new("us-west-2");

        let instances = vec![
            DiscoveredInstance {
                private_ip: Some("not-an-ip".to_string()),
                public_ip: None,
            },
            DiscoveredInstance {
                private_ip: Some("10.0.0.10".to_string()),
                public_ip: None,
            },
        ];

        let discovery = create_mock_discovery(config, instances);
        let addresses = discovery.discover().await.unwrap();

        assert_eq!(addresses.len(), 1);
        assert_eq!(addresses[0], "10.0.0.10:5701".parse().unwrap());
    }

    #[tokio::test]
    async fn test_aws_discovery_with_tag_filter() {
        let config = AwsDiscoveryConfig::new("eu-west-1")
            .with_tag("hazelcast:cluster", "production")
            .with_port(5703);

        let instances = vec![
            DiscoveredInstance {
                private_ip: Some("172.16.0.1".to_string()),
                public_ip: None,
            },
            DiscoveredInstance {
                private_ip: Some("172.16.0.2".to_string()),
                public_ip: None,
            },
            DiscoveredInstance {
                private_ip: Some("172.16.0.3".to_string()),
                public_ip: None,
            },
        ];

        let discovery = create_mock_discovery(config.clone(), instances);
        let filters = discovery.build_filters();

        assert!(filters.iter().any(|f| f.name == "tag:hazelcast:cluster" && f.values == vec!["production"]));

        let addresses = discovery.discover().await.unwrap();
        assert_eq!(addresses.len(), 3);
        assert!(addresses.iter().all(|a| a.port() == 5703));
    }

    #[tokio::test]
    async fn test_aws_discovery_with_security_group() {
        let config = AwsDiscoveryConfig::new("ap-northeast-1")
            .with_security_group("sg-hazelcast-cluster");

        let instances = vec![DiscoveredInstance {
            private_ip: Some("192.168.1.100".to_string()),
            public_ip: Some("13.114.0.50".to_string()),
        }];

        let discovery = create_mock_discovery(config, instances);
        let filters = discovery.build_filters();

        assert!(filters.iter().any(|f| f.name == "instance.group-id" && f.values == vec!["sg-hazelcast-cluster"]));

        let addresses = discovery.discover().await.unwrap();
        assert_eq!(addresses.len(), 1);
        assert_eq!(addresses[0], "192.168.1.100:5701".parse().unwrap());
    }

    #[test]
    fn test_instance_filter_new() {
        let filter = InstanceFilter::new("tag:Name", vec!["value1".to_string(), "value2".to_string()]);
        assert_eq!(filter.name, "tag:Name");
        assert_eq!(filter.values.len(), 2);
    }

    #[test]
    fn test_discovered_instance_clone() {
        let instance = DiscoveredInstance {
            private_ip: Some("10.0.0.1".to_string()),
            public_ip: Some("54.0.0.1".to_string()),
        };
        let cloned = instance.clone();
        assert_eq!(cloned.private_ip, instance.private_ip);
        assert_eq!(cloned.public_ip, instance.public_ip);
    }

    #[test]
    fn test_aws_discovery_debug() {
        let config = AwsDiscoveryConfig::new("us-west-2");
        let discovery = AwsDiscovery::new(config);
        let debug_str = format!("{:?}", discovery);
        assert!(debug_str.contains("AwsDiscovery"));
        assert!(debug_str.contains("us-west-2"));
    }
}
