//! AWS EC2/ECS cluster discovery for Hazelcast.
//!
//! This module provides automatic discovery of Hazelcast cluster members
//! running on AWS EC2 instances or ECS tasks using tag-based filtering.

use std::net::SocketAddr;

use async_trait::async_trait;
use hazelcast_core::Result;

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

/// AWS-based cluster discovery using EC2 instance metadata and tags.
#[derive(Debug, Clone)]
pub struct AwsDiscovery {
    config: AwsDiscoveryConfig,
}

impl AwsDiscovery {
    /// Creates a new AWS discovery instance with the given configuration.
    pub fn new(config: AwsDiscoveryConfig) -> Self {
        Self { config }
    }

    /// Returns the discovery configuration.
    pub fn config(&self) -> &AwsDiscoveryConfig {
        &self.config
    }

    /// Builds EC2 filters based on configuration.
    fn build_filters(&self) -> Vec<aws_sdk_ec2::types::Filter> {
        let mut filters = vec![
            aws_sdk_ec2::types::Filter::builder()
                .name("instance-state-name")
                .values("running")
                .build(),
        ];

        if let Some(sg) = &self.config.security_group {
            filters.push(
                aws_sdk_ec2::types::Filter::builder()
                    .name("instance.group-id")
                    .values(sg.clone())
                    .build(),
            );
        }

        if let (Some(key), Some(value)) = (&self.config.tag_key, &self.config.tag_value) {
            filters.push(
                aws_sdk_ec2::types::Filter::builder()
                    .name(format!("tag:{}", key))
                    .values(value.clone())
                    .build(),
            );
        }

        filters
    }

    /// Extracts IP address from an EC2 instance based on configuration.
    fn extract_ip(&self, instance: &aws_sdk_ec2::types::Instance) -> Option<String> {
        if self.config.use_private_ip {
            instance.private_ip_address().map(|s| s.to_string())
        } else {
            instance.public_ip_address().map(|s| s.to_string())
        }
    }
}

#[async_trait]
impl ClusterDiscovery for AwsDiscovery {
    async fn discover(&self) -> Result<Vec<SocketAddr>> {
        use hazelcast_core::HazelcastError;

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

        let filters = self.build_filters();
        let response = ec2_client
            .describe_instances()
            .set_filters(Some(filters))
            .send()
            .await
            .map_err(|e| HazelcastError::Connection(format!("AWS EC2 API error: {}", e)))?;

        let mut addresses = Vec::new();

        for reservation in response.reservations() {
            for instance in reservation.instances() {
                if let Some(ip) = self.extract_ip(instance) {
                    let addr_str = format!("{}:{}", ip, self.config.port);
                    if let Ok(addr) = addr_str.parse::<SocketAddr>() {
                        addresses.push(addr);
                        tracing::debug!("Discovered Hazelcast member at {}", addr);
                    }
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
    }

    #[test]
    fn test_aws_discovery_filters_minimal() {
        let config = AwsDiscoveryConfig::new("us-west-2");
        let discovery = AwsDiscovery::new(config);
        let filters = discovery.build_filters();

        assert_eq!(filters.len(), 1);
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
    fn test_aws_discovery_clone() {
        let discovery = AwsDiscovery::new(AwsDiscoveryConfig::default());
        let cloned = discovery.clone();
        assert_eq!(cloned.config().region(), "us-east-1");
    }

    #[test]
    fn test_aws_config_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<AwsDiscoveryConfig>();
    }

    #[test]
    fn test_aws_discovery_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<AwsDiscovery>();
    }

    #[test]
    fn test_aws_config_debug() {
        let config = AwsDiscoveryConfig::new("us-east-2")
            .with_security_group("sg-test");
        let debug_str = format!("{:?}", config);
        assert!(debug_str.contains("us-east-2"));
        assert!(debug_str.contains("sg-test"));
    }
}
