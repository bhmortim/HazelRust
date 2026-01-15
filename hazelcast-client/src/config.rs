//! Client configuration types and builders.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use crate::cache::NearCacheConfig;
use crate::deployment::UserCodeDeploymentConfig;
use crate::listener::Member;

/// Configuration for a WAN replication target cluster.
#[derive(Debug, Clone)]
pub struct WanTargetClusterConfig {
    cluster_name: String,
    endpoints: Vec<SocketAddr>,
    connection_timeout: Duration,
}

impl WanTargetClusterConfig {
    /// Returns the target cluster name.
    pub fn cluster_name(&self) -> &str {
        &self.cluster_name
    }

    /// Returns the target cluster endpoints.
    pub fn endpoints(&self) -> &[SocketAddr] {
        &self.endpoints
    }

    /// Returns the connection timeout for this target.
    pub fn connection_timeout(&self) -> Duration {
        self.connection_timeout
    }
}

/// Builder for `WanTargetClusterConfig`.
#[derive(Debug, Clone, Default)]
pub struct WanTargetClusterConfigBuilder {
    cluster_name: Option<String>,
    endpoints: Vec<SocketAddr>,
    connection_timeout: Option<Duration>,
}

impl WanTargetClusterConfigBuilder {
    /// Creates a new WAN target cluster configuration builder.
    pub fn new(cluster_name: impl Into<String>) -> Self {
        Self {
            cluster_name: Some(cluster_name.into()),
            ..Default::default()
        }
    }

    /// Adds an endpoint address for the target cluster.
    pub fn add_endpoint(mut self, endpoint: SocketAddr) -> Self {
        self.endpoints.push(endpoint);
        self
    }

    /// Sets the endpoint addresses for the target cluster.
    pub fn endpoints(mut self, endpoints: impl IntoIterator<Item = SocketAddr>) -> Self {
        self.endpoints = endpoints.into_iter().collect();
        self
    }

    /// Sets the connection timeout for the target cluster.
    pub fn connection_timeout(mut self, timeout: Duration) -> Self {
        self.connection_timeout = Some(timeout);
        self
    }

    /// Builds the WAN target cluster configuration.
    pub fn build(self) -> Result<WanTargetClusterConfig, ConfigError> {
        let cluster_name = self
            .cluster_name
            .ok_or_else(|| ConfigError::new("cluster_name is required for WAN target"))?;

        if cluster_name.is_empty() {
            return Err(ConfigError::new("cluster_name must not be empty"));
        }

        if self.endpoints.is_empty() {
            return Err(ConfigError::new(
                "at least one endpoint is required for WAN target",
            ));
        }

        Ok(WanTargetClusterConfig {
            cluster_name,
            endpoints: self.endpoints,
            connection_timeout: self.connection_timeout.unwrap_or(DEFAULT_CONNECTION_TIMEOUT),
        })
    }
}

/// Configuration for a WAN replication scheme.
#[derive(Debug, Clone)]
pub struct WanReplicationConfig {
    name: String,
    target_clusters: Vec<WanTargetClusterConfig>,
}

impl WanReplicationConfig {
    /// Returns the name of this WAN replication scheme.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the target cluster configurations.
    pub fn target_clusters(&self) -> &[WanTargetClusterConfig] {
        &self.target_clusters
    }

    /// Finds a target cluster by name.
    pub fn find_target(&self, cluster_name: &str) -> Option<&WanTargetClusterConfig> {
        self.target_clusters
            .iter()
            .find(|t| t.cluster_name() == cluster_name)
    }
}

/// Builder for `WanReplicationConfig`.
#[derive(Debug, Clone, Default)]
pub struct WanReplicationConfigBuilder {
    name: Option<String>,
    target_clusters: Vec<WanTargetClusterConfig>,
}

impl WanReplicationConfigBuilder {
    /// Creates a new WAN replication configuration builder.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: Some(name.into()),
            ..Default::default()
        }
    }

    /// Adds a target cluster configuration.
    pub fn add_target_cluster(mut self, target: WanTargetClusterConfig) -> Self {
        self.target_clusters.push(target);
        self
    }

    /// Builds the WAN replication configuration.
    pub fn build(self) -> Result<WanReplicationConfig, ConfigError> {
        let name = self
            .name
            .ok_or_else(|| ConfigError::new("name is required for WAN replication config"))?;

        if name.is_empty() {
            return Err(ConfigError::new(
                "WAN replication name must not be empty",
            ));
        }

        if self.target_clusters.is_empty() {
            return Err(ConfigError::new(
                "at least one target cluster is required for WAN replication",
            ));
        }

        Ok(WanReplicationConfig {
            name,
            target_clusters: self.target_clusters,
        })
    }
}

/// Reference to a WAN replication configuration for use in map settings.
#[derive(Debug, Clone)]
pub struct WanReplicationRef {
    name: String,
    merge_policy_class_name: String,
    republishing_enabled: bool,
    filters: Vec<String>,
}

impl WanReplicationRef {
    /// Creates a new builder for WAN replication reference.
    pub fn builder(name: impl Into<String>) -> WanReplicationRefBuilder {
        WanReplicationRefBuilder::new(name)
    }

    /// Returns the name of the WAN replication configuration to use.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the merge policy class name.
    pub fn merge_policy_class_name(&self) -> &str {
        &self.merge_policy_class_name
    }

    /// Returns whether republishing is enabled.
    pub fn republishing_enabled(&self) -> bool {
        self.republishing_enabled
    }

    /// Returns the filter class names.
    pub fn filters(&self) -> &[String] {
        &self.filters
    }
}

/// Default merge policy for WAN replication.
const DEFAULT_WAN_MERGE_POLICY: &str = "com.hazelcast.spi.merge.PassThroughMergePolicy";

/// Builder for `WanReplicationRef`.
#[derive(Debug, Clone, Default)]
pub struct WanReplicationRefBuilder {
    name: Option<String>,
    merge_policy_class_name: Option<String>,
    republishing_enabled: Option<bool>,
    filters: Vec<String>,
}

impl WanReplicationRefBuilder {
    /// Creates a new WAN replication reference builder.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: Some(name.into()),
            ..Default::default()
        }
    }

    /// Sets the merge policy class name.
    pub fn merge_policy_class_name(mut self, class_name: impl Into<String>) -> Self {
        self.merge_policy_class_name = Some(class_name.into());
        self
    }

    /// Enables or disables republishing of received WAN events.
    pub fn republishing_enabled(mut self, enabled: bool) -> Self {
        self.republishing_enabled = Some(enabled);
        self
    }

    /// Adds a filter class name for filtering WAN replication events.
    pub fn add_filter(mut self, filter_class_name: impl Into<String>) -> Self {
        self.filters.push(filter_class_name.into());
        self
    }

    /// Builds the WAN replication reference.
    pub fn build(self) -> Result<WanReplicationRef, ConfigError> {
        let name = self
            .name
            .ok_or_else(|| ConfigError::new("name is required for WAN replication ref"))?;

        if name.is_empty() {
            return Err(ConfigError::new(
                "WAN replication ref name must not be empty",
            ));
        }

        Ok(WanReplicationRef {
            name,
            merge_policy_class_name: self
                .merge_policy_class_name
                .unwrap_or_else(|| DEFAULT_WAN_MERGE_POLICY.to_string()),
            republishing_enabled: self.republishing_enabled.unwrap_or(false),
            filters: self.filters,
        })
    }
}

#[cfg(feature = "aws")]
use crate::connection::AwsDiscoveryConfig;

#[cfg(feature = "azure")]
use crate::connection::AzureDiscoveryConfig;

#[cfg(feature = "gcp")]
use crate::connection::GcpDiscoveryConfig;

#[cfg(feature = "kubernetes")]
use crate::connection::KubernetesDiscoveryConfig;

#[cfg(feature = "cloud")]
use crate::connection::CloudDiscoveryConfig;

/// Default cluster name.
const DEFAULT_CLUSTER_NAME: &str = "dev";
/// Default slow operation threshold.
const DEFAULT_SLOW_OPERATION_THRESHOLD: Duration = Duration::from_millis(100);
/// Default connection timeout.
const DEFAULT_CONNECTION_TIMEOUT: Duration = Duration::from_secs(5);
/// Default heartbeat interval.
const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
/// Default initial retry backoff.
const DEFAULT_INITIAL_BACKOFF: Duration = Duration::from_millis(100);
/// Default maximum retry backoff.
const DEFAULT_MAX_BACKOFF: Duration = Duration::from_secs(30);
/// Default retry multiplier.
const DEFAULT_RETRY_MULTIPLIER: f64 = 2.0;
/// Default maximum retry attempts.
const DEFAULT_MAX_RETRIES: u32 = 10;

/// Configuration error returned when validation fails.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigError {
    message: String,
}

impl ConfigError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "configuration error: {}", self.message)
    }
}

impl std::error::Error for ConfigError {}

/// Network configuration for cluster connections.
#[derive(Debug, Clone)]
pub struct NetworkConfig {
    addresses: Vec<SocketAddr>,
    connection_timeout: Duration,
    heartbeat_interval: Duration,
    tls: TlsConfig,
    ws_url: Option<String>,
    wan_replication: Vec<WanReplicationConfig>,
    #[cfg(feature = "aws")]
    aws_discovery: Option<AwsDiscoveryConfig>,
    #[cfg(feature = "azure")]
    azure_discovery: Option<AzureDiscoveryConfig>,
    #[cfg(feature = "gcp")]
    gcp_discovery: Option<GcpDiscoveryConfig>,
    #[cfg(feature = "kubernetes")]
    kubernetes_discovery: Option<KubernetesDiscoveryConfig>,
    #[cfg(feature = "cloud")]
    cloud_discovery: Option<CloudDiscoveryConfig>,
}

impl NetworkConfig {
    /// Returns the configured cluster member addresses.
    pub fn addresses(&self) -> &[SocketAddr] {
        &self.addresses
    }

    /// Returns the connection timeout duration.
    pub fn connection_timeout(&self) -> Duration {
        self.connection_timeout
    }

    /// Returns the heartbeat interval duration.
    pub fn heartbeat_interval(&self) -> Duration {
        self.heartbeat_interval
    }

    /// Returns the TLS configuration.
    pub fn tls(&self) -> &TlsConfig {
        &self.tls
    }

    /// Returns the WebSocket URL if configured.
    pub fn ws_url(&self) -> Option<&str> {
        self.ws_url.as_deref()
    }

    /// Returns the AWS discovery configuration if configured.
    #[cfg(feature = "aws")]
    pub fn aws_discovery(&self) -> Option<&AwsDiscoveryConfig> {
        self.aws_discovery.as_ref()
    }

    /// Returns the Azure discovery configuration if configured.
    #[cfg(feature = "azure")]
    pub fn azure_discovery(&self) -> Option<&AzureDiscoveryConfig> {
        self.azure_discovery.as_ref()
    }

    /// Returns the GCP discovery configuration if configured.
    #[cfg(feature = "gcp")]
    pub fn gcp_discovery(&self) -> Option<&GcpDiscoveryConfig> {
        self.gcp_discovery.as_ref()
    }

    /// Returns the Kubernetes discovery configuration if configured.
    #[cfg(feature = "kubernetes")]
    pub fn kubernetes_discovery(&self) -> Option<&KubernetesDiscoveryConfig> {
        self.kubernetes_discovery.as_ref()
    }

    /// Returns the Hazelcast Cloud discovery configuration if configured.
    #[cfg(feature = "cloud")]
    pub fn cloud_discovery(&self) -> Option<&CloudDiscoveryConfig> {
        self.cloud_discovery.as_ref()
    }

    /// Returns the WAN replication configurations.
    pub fn wan_replication(&self) -> &[WanReplicationConfig] {
        &self.wan_replication
    }

    /// Finds a WAN replication configuration by name.
    pub fn find_wan_replication(&self, name: &str) -> Option<&WanReplicationConfig> {
        self.wan_replication.iter().find(|w| w.name() == name)
    }
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            addresses: vec!["127.0.0.1:5701".parse().unwrap()],
            connection_timeout: DEFAULT_CONNECTION_TIMEOUT,
            heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL,
            tls: TlsConfig::default(),
            ws_url: None,
            wan_replication: Vec::new(),
            #[cfg(feature = "aws")]
            aws_discovery: None,
            #[cfg(feature = "azure")]
            azure_discovery: None,
            #[cfg(feature = "gcp")]
            gcp_discovery: None,
            #[cfg(feature = "kubernetes")]
            kubernetes_discovery: None,
            #[cfg(feature = "cloud")]
            cloud_discovery: None,
        }
    }
}

/// Builder for `NetworkConfig`.
#[derive(Debug, Clone, Default)]
pub struct NetworkConfigBuilder {
    addresses: Vec<SocketAddr>,
    connection_timeout: Option<Duration>,
    heartbeat_interval: Option<Duration>,
    tls: TlsConfigBuilder,
    ws_url: Option<String>,
    wan_replication: Vec<WanReplicationConfig>,
    #[cfg(feature = "aws")]
    aws_discovery: Option<AwsDiscoveryConfig>,
    #[cfg(feature = "azure")]
    azure_discovery: Option<AzureDiscoveryConfig>,
    #[cfg(feature = "gcp")]
    gcp_discovery: Option<GcpDiscoveryConfig>,
    #[cfg(feature = "kubernetes")]
    kubernetes_discovery: Option<KubernetesDiscoveryConfig>,
    #[cfg(feature = "cloud")]
    cloud_discovery: Option<CloudDiscoveryConfig>,
}

impl NetworkConfigBuilder {
    /// Creates a new network configuration builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a cluster member address.
    pub fn add_address(mut self, address: SocketAddr) -> Self {
        self.addresses.push(address);
        self
    }

    /// Sets the cluster member addresses, replacing any previously configured.
    pub fn addresses(mut self, addresses: impl IntoIterator<Item = SocketAddr>) -> Self {
        self.addresses = addresses.into_iter().collect();
        self
    }

    /// Sets the connection timeout duration.
    pub fn connection_timeout(mut self, timeout: Duration) -> Self {
        self.connection_timeout = Some(timeout);
        self
    }

    /// Sets the heartbeat interval duration.
    pub fn heartbeat_interval(mut self, interval: Duration) -> Self {
        self.heartbeat_interval = Some(interval);
        self
    }

    /// Configures TLS settings using a builder function.
    pub fn tls<F>(mut self, f: F) -> Self
    where
        F: FnOnce(TlsConfigBuilder) -> TlsConfigBuilder,
    {
        self.tls = f(self.tls);
        self
    }

    /// Enables TLS with default settings.
    pub fn enable_tls(mut self) -> Self {
        self.tls = self.tls.enabled(true);
        self
    }

    /// Sets the WebSocket URL for WebSocket transport.
    ///
    /// When set, the client will use WebSocket connections instead of raw TCP.
    /// The URL should be in the format `ws://host:port/path` or `wss://host:port/path`.
    pub fn ws_url(mut self, url: impl Into<String>) -> Self {
        self.ws_url = Some(url.into());
        self
    }

    /// Sets the AWS discovery configuration for EC2/ECS-based cluster discovery.
    ///
    /// When configured, the client will discover cluster members by querying
    /// AWS EC2 API for instances matching the configured filters.
    #[cfg(feature = "aws")]
    pub fn aws_discovery(mut self, config: AwsDiscoveryConfig) -> Self {
        self.aws_discovery = Some(config);
        self
    }

    /// Sets the Azure discovery configuration for VM-based cluster discovery.
    ///
    /// When configured, the client will discover cluster members by querying
    /// Azure Resource Manager API for VMs matching the configured tags.
    #[cfg(feature = "azure")]
    pub fn azure_discovery(mut self, config: AzureDiscoveryConfig) -> Self {
        self.azure_discovery = Some(config);
        self
    }

    /// Sets the GCP discovery configuration for Compute Engine-based cluster discovery.
    ///
    /// When configured, the client will discover cluster members by querying
    /// GCP Compute Engine API for instances matching the configured labels.
    #[cfg(feature = "gcp")]
    pub fn gcp_discovery(mut self, config: GcpDiscoveryConfig) -> Self {
        self.gcp_discovery = Some(config);
        self
    }

    /// Sets the Kubernetes discovery configuration for pod-based cluster discovery.
    ///
    /// When configured, the client will discover cluster members by querying
    /// the Kubernetes API for pods or service endpoints matching the configured selectors.
    #[cfg(feature = "kubernetes")]
    pub fn kubernetes_discovery(mut self, config: KubernetesDiscoveryConfig) -> Self {
        self.kubernetes_discovery = Some(config);
        self
    }

    /// Sets the Hazelcast Cloud discovery configuration.
    ///
    /// When configured, the client will discover cluster members by querying
    /// the Hazelcast Cloud coordinator API using the configured discovery token.
    #[cfg(feature = "cloud")]
    pub fn cloud_discovery(mut self, config: CloudDiscoveryConfig) -> Self {
        self.cloud_discovery = Some(config);
        self
    }

    /// Adds a WAN replication configuration.
    ///
    /// WAN replication allows data to be replicated across multiple clusters
    /// in different geographic locations or data centers.
    pub fn add_wan_replication(mut self, config: WanReplicationConfig) -> Self {
        self.wan_replication.push(config);
        self
    }

    /// Builds the network configuration.
    pub fn build(self) -> Result<NetworkConfig, ConfigError> {
        let addresses = if self.addresses.is_empty() {
            vec!["127.0.0.1:5701".parse().unwrap()]
        } else {
            self.addresses
        };

        let tls = self.tls.build()?;

        Ok(NetworkConfig {
            addresses,
            connection_timeout: self.connection_timeout.unwrap_or(DEFAULT_CONNECTION_TIMEOUT),
            heartbeat_interval: self.heartbeat_interval.unwrap_or(DEFAULT_HEARTBEAT_INTERVAL),
            tls,
            ws_url: self.ws_url,
            wan_replication: self.wan_replication,
            #[cfg(feature = "aws")]
            aws_discovery: self.aws_discovery,
            #[cfg(feature = "azure")]
            azure_discovery: self.azure_discovery,
            #[cfg(feature = "gcp")]
            gcp_discovery: self.gcp_discovery,
            #[cfg(feature = "kubernetes")]
            kubernetes_discovery: self.kubernetes_discovery,
            #[cfg(feature = "cloud")]
            cloud_discovery: self.cloud_discovery,
        })
    }
}

/// Retry configuration for connection attempts.
#[derive(Debug, Clone)]
pub struct RetryConfig {
    initial_backoff: Duration,
    max_backoff: Duration,
    multiplier: f64,
    max_retries: u32,
}

impl RetryConfig {
    /// Returns the initial backoff duration.
    pub fn initial_backoff(&self) -> Duration {
        self.initial_backoff
    }

    /// Returns the maximum backoff duration.
    pub fn max_backoff(&self) -> Duration {
        self.max_backoff
    }

    /// Returns the backoff multiplier.
    pub fn multiplier(&self) -> f64 {
        self.multiplier
    }

    /// Returns the maximum number of retry attempts.
    pub fn max_retries(&self) -> u32 {
        self.max_retries
    }
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            initial_backoff: DEFAULT_INITIAL_BACKOFF,
            max_backoff: DEFAULT_MAX_BACKOFF,
            multiplier: DEFAULT_RETRY_MULTIPLIER,
            max_retries: DEFAULT_MAX_RETRIES,
        }
    }
}

impl From<RetryConfig> for RetryConfigBuilder {
    fn from(config: RetryConfig) -> Self {
        Self {
            initial_backoff: Some(config.initial_backoff),
            max_backoff: Some(config.max_backoff),
            multiplier: Some(config.multiplier),
            max_retries: Some(config.max_retries),
        }
    }
}

/// Builder for `RetryConfig`.
#[derive(Debug, Clone, Default)]
pub struct RetryConfigBuilder {
    initial_backoff: Option<Duration>,
    max_backoff: Option<Duration>,
    multiplier: Option<f64>,
    max_retries: Option<u32>,
}

impl RetryConfigBuilder {
    /// Creates a new retry configuration builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the initial backoff duration.
    pub fn initial_backoff(mut self, backoff: Duration) -> Self {
        self.initial_backoff = Some(backoff);
        self
    }

    /// Sets the maximum backoff duration.
    pub fn max_backoff(mut self, backoff: Duration) -> Self {
        self.max_backoff = Some(backoff);
        self
    }

    /// Sets the backoff multiplier.
    pub fn multiplier(mut self, multiplier: f64) -> Self {
        self.multiplier = Some(multiplier);
        self
    }

    /// Sets the maximum number of retry attempts.
    pub fn max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = Some(max_retries);
        self
    }

    /// Builds the retry configuration, returning an error if validation fails.
    ///
    /// # Errors
    ///
    /// Returns `ConfigError` if:
    /// - `initial_backoff` exceeds `max_backoff`
    /// - `multiplier` is less than 1.0
    pub fn build(self) -> Result<RetryConfig, ConfigError> {
        let initial_backoff = self.initial_backoff.unwrap_or(DEFAULT_INITIAL_BACKOFF);
        let max_backoff = self.max_backoff.unwrap_or(DEFAULT_MAX_BACKOFF);
        let multiplier = self.multiplier.unwrap_or(DEFAULT_RETRY_MULTIPLIER);
        let max_retries = self.max_retries.unwrap_or(DEFAULT_MAX_RETRIES);

        if initial_backoff > max_backoff {
            return Err(ConfigError::new(
                "initial_backoff must not exceed max_backoff",
            ));
        }

        if multiplier < 1.0 {
            return Err(ConfigError::new("multiplier must be at least 1.0"));
        }

        Ok(RetryConfig {
            initial_backoff,
            max_backoff,
            multiplier,
            max_retries,
        })
    }
}

/// TLS configuration for secure connections.
#[derive(Debug, Clone)]
pub struct TlsConfig {
    enabled: bool,
    ca_cert_path: Option<PathBuf>,
    client_cert_path: Option<PathBuf>,
    client_key_path: Option<PathBuf>,
    verify_hostname: bool,
}

impl TlsConfig {
    /// Returns whether TLS is enabled.
    pub fn enabled(&self) -> bool {
        self.enabled
    }

    /// Returns the path to the CA certificate file.
    pub fn ca_cert_path(&self) -> Option<&PathBuf> {
        self.ca_cert_path.as_ref()
    }

    /// Returns the path to the client certificate file.
    pub fn client_cert_path(&self) -> Option<&PathBuf> {
        self.client_cert_path.as_ref()
    }

    /// Returns the path to the client private key file.
    pub fn client_key_path(&self) -> Option<&PathBuf> {
        self.client_key_path.as_ref()
    }

    /// Returns whether hostname verification is enabled.
    pub fn verify_hostname(&self) -> bool {
        self.verify_hostname
    }

    /// Returns true if client authentication is configured.
    pub fn has_client_auth(&self) -> bool {
        self.client_cert_path.is_some() && self.client_key_path.is_some()
    }
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            ca_cert_path: None,
            client_cert_path: None,
            client_key_path: None,
            verify_hostname: true,
        }
    }
}

/// Builder for `TlsConfig`.
#[derive(Debug, Clone, Default)]
pub struct TlsConfigBuilder {
    enabled: Option<bool>,
    ca_cert_path: Option<PathBuf>,
    client_cert_path: Option<PathBuf>,
    client_key_path: Option<PathBuf>,
    verify_hostname: Option<bool>,
}

impl TlsConfigBuilder {
    /// Creates a new TLS configuration builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Enables or disables TLS.
    pub fn enabled(mut self, enabled: bool) -> Self {
        self.enabled = Some(enabled);
        self
    }

    /// Sets the path to the CA certificate file for server verification.
    pub fn ca_cert_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.ca_cert_path = Some(path.into());
        self
    }

    /// Sets the path to the client certificate file for mutual TLS.
    pub fn client_cert_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.client_cert_path = Some(path.into());
        self
    }

    /// Sets the path to the client private key file for mutual TLS.
    pub fn client_key_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.client_key_path = Some(path.into());
        self
    }

    /// Sets client certificate and key paths for mutual TLS.
    pub fn client_auth(self, cert_path: impl Into<PathBuf>, key_path: impl Into<PathBuf>) -> Self {
        self.client_cert_path(cert_path).client_key_path(key_path)
    }

    /// Enables or disables hostname verification.
    pub fn verify_hostname(mut self, verify: bool) -> Self {
        self.verify_hostname = Some(verify);
        self
    }

    /// Builds the TLS configuration, returning an error if validation fails.
    ///
    /// # Errors
    ///
    /// Returns `ConfigError` if:
    /// - Only one of `client_cert_path` or `client_key_path` is set
    pub fn build(self) -> Result<TlsConfig, ConfigError> {
        let enabled = self.enabled.unwrap_or(false);

        if self.client_cert_path.is_some() != self.client_key_path.is_some() {
            return Err(ConfigError::new(
                "both client_cert_path and client_key_path must be provided together",
            ));
        }

        Ok(TlsConfig {
            enabled,
            ca_cert_path: self.ca_cert_path,
            client_cert_path: self.client_cert_path,
            client_key_path: self.client_key_path,
            verify_hostname: self.verify_hostname.unwrap_or(true),
        })
    }
}

/// The type of operations protected by a quorum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum QuorumType {
    /// Protects read operations only.
    Read,
    /// Protects write operations only.
    Write,
    /// Protects both read and write operations.
    ReadWrite,
}

impl QuorumType {
    /// Returns `true` if this quorum type protects read operations.
    pub fn protects_reads(&self) -> bool {
        matches!(self, QuorumType::Read | QuorumType::ReadWrite)
    }

    /// Returns `true` if this quorum type protects write operations.
    pub fn protects_writes(&self) -> bool {
        matches!(self, QuorumType::Write | QuorumType::ReadWrite)
    }
}

/// A function for custom quorum evaluation.
///
/// Implement this trait to provide custom logic for determining whether
/// a quorum is present based on the current cluster members.
pub trait QuorumFunction: Send + Sync {
    /// Returns `true` if the quorum is present given the current cluster members.
    fn is_present(&self, members: &[Member]) -> bool;
}

impl std::fmt::Debug for dyn QuorumFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("QuorumFunction")
    }
}

/// Configuration for split-brain protection (quorum).
///
/// Quorum configuration defines the minimum cluster size required for
/// operations to proceed. This helps prevent split-brain scenarios where
/// a partitioned cluster might accept conflicting updates.
#[derive(Clone)]
pub struct QuorumConfig {
    name: String,
    min_cluster_size: usize,
    quorum_type: QuorumType,
    quorum_function: Option<Arc<dyn QuorumFunction>>,
}

impl std::fmt::Debug for QuorumConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QuorumConfig")
            .field("name", &self.name)
            .field("min_cluster_size", &self.min_cluster_size)
            .field("quorum_type", &self.quorum_type)
            .field("quorum_function", &self.quorum_function.is_some())
            .finish()
    }
}

impl QuorumConfig {
    /// Creates a new builder for `QuorumConfig`.
    pub fn builder(name: impl Into<String>) -> QuorumConfigBuilder {
        QuorumConfigBuilder::new(name)
    }

    /// Returns the name pattern for this quorum configuration.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the minimum cluster size required for quorum.
    pub fn min_cluster_size(&self) -> usize {
        self.min_cluster_size
    }

    /// Returns the type of operations protected by this quorum.
    pub fn quorum_type(&self) -> QuorumType {
        self.quorum_type
    }

    /// Returns the custom quorum function, if configured.
    pub fn quorum_function(&self) -> Option<&Arc<dyn QuorumFunction>> {
        self.quorum_function.as_ref()
    }

    /// Returns `true` if the given data structure name matches this quorum configuration.
    ///
    /// Supports wildcard patterns:
    /// - `*` matches any sequence of characters
    /// - Exact match if no wildcards
    pub fn matches(&self, data_structure_name: &str) -> bool {
        if self.name.contains('*') {
            let pattern = &self.name;
            if pattern == "*" {
                return true;
            }
            if let Some(prefix) = pattern.strip_suffix('*') {
                return data_structure_name.starts_with(prefix);
            }
            if let Some(suffix) = pattern.strip_prefix('*') {
                return data_structure_name.ends_with(suffix);
            }
            false
        } else {
            self.name == data_structure_name
        }
    }

    /// Checks if the quorum is present given the current cluster members.
    ///
    /// Returns `true` if:
    /// - A custom quorum function is configured and returns `true`, or
    /// - The number of members is at least `min_cluster_size`
    pub fn check_quorum(&self, members: &[Member]) -> bool {
        if let Some(ref func) = self.quorum_function {
            func.is_present(members)
        } else {
            members.len() >= self.min_cluster_size
        }
    }

    /// Returns `true` if this quorum protects the given operation type.
    pub fn protects_operation(&self, is_read: bool) -> bool {
        if is_read {
            self.quorum_type.protects_reads()
        } else {
            self.quorum_type.protects_writes()
        }
    }
}

/// Builder for `QuorumConfig`.
#[derive(Clone)]
pub struct QuorumConfigBuilder {
    name: String,
    min_cluster_size: Option<usize>,
    quorum_type: Option<QuorumType>,
    quorum_function: Option<Arc<dyn QuorumFunction>>,
}

impl std::fmt::Debug for QuorumConfigBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QuorumConfigBuilder")
            .field("name", &self.name)
            .field("min_cluster_size", &self.min_cluster_size)
            .field("quorum_type", &self.quorum_type)
            .field("quorum_function", &self.quorum_function.is_some())
            .finish()
    }
}

impl QuorumConfigBuilder {
    /// Creates a new quorum configuration builder.
    ///
    /// The name can be a specific data structure name or a pattern with wildcards.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            min_cluster_size: None,
            quorum_type: None,
            quorum_function: None,
        }
    }

    /// Sets the minimum cluster size required for quorum.
    ///
    /// Operations will fail if the cluster has fewer members than this value.
    pub fn min_cluster_size(mut self, size: usize) -> Self {
        self.min_cluster_size = Some(size);
        self
    }

    /// Sets the type of operations protected by this quorum.
    ///
    /// Defaults to `QuorumType::ReadWrite` if not specified.
    pub fn quorum_type(mut self, quorum_type: QuorumType) -> Self {
        self.quorum_type = Some(quorum_type);
        self
    }

    /// Sets a custom quorum function for evaluating quorum presence.
    ///
    /// When set, this function is used instead of the simple member count check.
    pub fn quorum_function(mut self, func: Arc<dyn QuorumFunction>) -> Self {
        self.quorum_function = Some(func);
        self
    }

    /// Builds the quorum configuration.
    ///
    /// # Errors
    ///
    /// Returns `ConfigError` if:
    /// - The name is empty
    /// - `min_cluster_size` is 0 and no custom quorum function is set
    pub fn build(self) -> Result<QuorumConfig, ConfigError> {
        if self.name.is_empty() {
            return Err(ConfigError::new("quorum config name must not be empty"));
        }

        let min_cluster_size = self.min_cluster_size.unwrap_or(2);
        if min_cluster_size == 0 && self.quorum_function.is_none() {
            return Err(ConfigError::new(
                "min_cluster_size must be at least 1 when no quorum function is set",
            ));
        }

        Ok(QuorumConfig {
            name: self.name,
            min_cluster_size,
            quorum_type: self.quorum_type.unwrap_or(QuorumType::ReadWrite),
            quorum_function: self.quorum_function,
        })
    }
}

/// Permission actions that can be granted to a client.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PermissionAction {
    /// Permission to create distributed objects.
    Create,
    /// Permission to destroy distributed objects.
    Destroy,
    /// Permission to read from distributed objects.
    Read,
    /// Permission to put/write to distributed objects.
    Put,
    /// Permission to remove entries from distributed objects.
    Remove,
    /// Permission to listen to events.
    Listen,
    /// Permission to acquire locks.
    Lock,
    /// Permission to create indexes.
    Index,
    /// All permissions (admin).
    All,
}

/// Permissions granted to the authenticated client.
///
/// This struct tracks the operations the client is allowed to perform
/// based on RBAC configuration on the server.
#[derive(Debug, Clone, Default)]
pub struct Permissions {
    can_create: bool,
    can_destroy: bool,
    can_read: bool,
    can_put: bool,
    can_remove: bool,
    can_listen: bool,
    can_lock: bool,
    can_index: bool,
    is_admin: bool,
}

impl Permissions {
    /// Creates a new permissions set with all permissions denied.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a permissions set with all permissions granted (admin).
    pub fn all() -> Self {
        Self {
            can_create: true,
            can_destroy: true,
            can_read: true,
            can_put: true,
            can_remove: true,
            can_listen: true,
            can_lock: true,
            can_index: true,
            is_admin: true,
        }
    }

    /// Returns whether the client can create distributed objects.
    pub fn can_create(&self) -> bool {
        self.is_admin || self.can_create
    }

    /// Returns whether the client can destroy distributed objects.
    pub fn can_destroy(&self) -> bool {
        self.is_admin || self.can_destroy
    }

    /// Returns whether the client can read from distributed objects.
    pub fn can_read(&self) -> bool {
        self.is_admin || self.can_read
    }

    /// Returns whether the client can put/write to distributed objects.
    pub fn can_put(&self) -> bool {
        self.is_admin || self.can_put
    }

    /// Returns whether the client can remove entries from distributed objects.
    pub fn can_remove(&self) -> bool {
        self.is_admin || self.can_remove
    }

    /// Returns whether the client can listen to events.
    pub fn can_listen(&self) -> bool {
        self.is_admin || self.can_listen
    }

    /// Returns whether the client can acquire locks.
    pub fn can_lock(&self) -> bool {
        self.is_admin || self.can_lock
    }

    /// Returns whether the client can create indexes.
    pub fn can_index(&self) -> bool {
        self.is_admin || self.can_index
    }

    /// Returns whether the client has admin privileges.
    pub fn is_admin(&self) -> bool {
        self.is_admin
    }

    /// Grants the specified permission action.
    pub fn grant(&mut self, action: PermissionAction) {
        match action {
            PermissionAction::Create => self.can_create = true,
            PermissionAction::Destroy => self.can_destroy = true,
            PermissionAction::Read => self.can_read = true,
            PermissionAction::Put => self.can_put = true,
            PermissionAction::Remove => self.can_remove = true,
            PermissionAction::Listen => self.can_listen = true,
            PermissionAction::Lock => self.can_lock = true,
            PermissionAction::Index => self.can_index = true,
            PermissionAction::All => self.is_admin = true,
        }
    }

    /// Revokes the specified permission action.
    pub fn revoke(&mut self, action: PermissionAction) {
        match action {
            PermissionAction::Create => self.can_create = false,
            PermissionAction::Destroy => self.can_destroy = false,
            PermissionAction::Read => self.can_read = false,
            PermissionAction::Put => self.can_put = false,
            PermissionAction::Remove => self.can_remove = false,
            PermissionAction::Listen => self.can_listen = false,
            PermissionAction::Lock => self.can_lock = false,
            PermissionAction::Index => self.can_index = false,
            PermissionAction::All => self.is_admin = false,
        }
    }

    /// Returns whether the specified action is permitted.
    pub fn is_permitted(&self, action: PermissionAction) -> bool {
        if self.is_admin {
            return true;
        }
        match action {
            PermissionAction::Create => self.can_create,
            PermissionAction::Destroy => self.can_destroy,
            PermissionAction::Read => self.can_read,
            PermissionAction::Put => self.can_put,
            PermissionAction::Remove => self.can_remove,
            PermissionAction::Listen => self.can_listen,
            PermissionAction::Lock => self.can_lock,
            PermissionAction::Index => self.can_index,
            PermissionAction::All => self.is_admin,
        }
    }
}

/// Security configuration for authentication.
#[derive(Clone, Default)]
pub struct SecurityConfig {
    username: Option<String>,
    password: Option<String>,
    token: Option<String>,
    authenticator: Option<Arc<dyn crate::security::Authenticator>>,
    permissions: Option<Permissions>,
}

impl std::fmt::Debug for SecurityConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SecurityConfig")
            .field("username", &self.username)
            .field("password", &self.password)
            .field("token", &self.token)
            .field("authenticator", &self.authenticator.is_some())
            .finish()
    }
}

impl SecurityConfig {
    /// Returns the configured username.
    pub fn username(&self) -> Option<&str> {
        self.username.as_deref()
    }

    /// Returns the configured password.
    pub fn password(&self) -> Option<&str> {
        self.password.as_deref()
    }

    /// Returns the configured authentication token.
    pub fn token(&self) -> Option<&str> {
        self.token.as_deref()
    }

    /// Returns true if username/password credentials are configured.
    pub fn has_credentials(&self) -> bool {
        self.username.is_some() && self.password.is_some()
    }

    /// Returns true if token-based authentication is configured.
    pub fn has_token(&self) -> bool {
        self.token.is_some()
    }

    /// Returns true if any authentication method is configured.
    pub fn is_authenticated(&self) -> bool {
        self.has_credentials() || self.has_token() || self.authenticator.is_some()
    }

    /// Returns the custom authenticator if configured.
    pub fn authenticator(&self) -> Option<&Arc<dyn crate::security::Authenticator>> {
        self.authenticator.as_ref()
    }

    /// Returns the configured permissions if RBAC is enabled.
    pub fn permissions(&self) -> Option<&Permissions> {
        self.permissions.as_ref()
    }

    /// Returns the effective permissions for authorization checks.
    ///
    /// If no permissions are explicitly configured, returns `Permissions::all()`
    /// for backward compatibility.
    pub fn effective_permissions(&self) -> Permissions {
        self.permissions.clone().unwrap_or_else(Permissions::all)
    }
}

/// Builder for `SecurityConfig`.
#[derive(Clone, Default)]
pub struct SecurityConfigBuilder {
    username: Option<String>,
    password: Option<String>,
    token: Option<String>,
    authenticator: Option<Arc<dyn crate::security::Authenticator>>,
    permissions: Option<Permissions>,
}

impl std::fmt::Debug for SecurityConfigBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SecurityConfigBuilder")
            .field("username", &self.username)
            .field("password", &self.password)
            .field("token", &self.token)
            .field("authenticator", &self.authenticator.is_some())
            .finish()
    }
}

impl SecurityConfigBuilder {
    /// Creates a new security configuration builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the username for authentication.
    pub fn username(mut self, username: impl Into<String>) -> Self {
        self.username = Some(username.into());
        self
    }

    /// Sets the password for authentication.
    pub fn password(mut self, password: impl Into<String>) -> Self {
        self.password = Some(password.into());
        self
    }

    /// Sets both username and password for authentication.
    pub fn credentials(self, username: impl Into<String>, password: impl Into<String>) -> Self {
        self.username(username).password(password)
    }

    /// Sets the authentication token for token-based authentication.
    ///
    /// Token-based authentication is used for RBAC (Role-Based Access Control)
    /// where the token encodes the client's permissions.
    pub fn token(mut self, token: impl Into<String>) -> Self {
        self.token = Some(token.into());
        self
    }

    /// Sets a custom authenticator for authentication.
    ///
    /// Custom authenticators allow implementing non-standard authentication
    /// mechanisms such as LDAP, Kerberos, or custom token validation.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use hazelcast_client::security::DefaultAuthenticator;
    /// use std::sync::Arc;
    ///
    /// let config = SecurityConfigBuilder::new()
    ///     .custom_authenticator(Arc::new(DefaultAuthenticator))
    ///     .build()
    ///     .unwrap();
    /// ```
    pub fn custom_authenticator(
        mut self,
        authenticator: Arc<dyn crate::security::Authenticator>,
    ) -> Self {
        self.authenticator = Some(authenticator);
        self
    }

    /// Sets the permissions for RBAC enforcement.
    ///
    /// When permissions are set, operations will be checked against the
    /// granted permissions before being sent to the cluster.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use hazelcast_client::config::{Permissions, PermissionAction};
    ///
    /// let mut perms = Permissions::new();
    /// perms.grant(PermissionAction::Read);
    /// perms.grant(PermissionAction::Put);
    ///
    /// let config = SecurityConfigBuilder::new()
    ///     .permissions(perms)
    ///     .build()
    ///     .unwrap();
    /// ```
    pub fn permissions(mut self, permissions: Permissions) -> Self {
        self.permissions = Some(permissions);
        self
    }

    /// Builds the security configuration, returning an error if validation fails.
    ///
    /// # Errors
    ///
    /// Returns `ConfigError` if:
    /// - Only one of `username` or `password` is set (both must be provided together)
    /// - Both credentials and token are set (mutually exclusive)
    pub fn build(self) -> Result<SecurityConfig, ConfigError> {
        if self.username.is_some() != self.password.is_some() {
            return Err(ConfigError::new(
                "both username and password must be provided together",
            ));
        }

        if self.token.is_some() && (self.username.is_some() || self.password.is_some()) {
            return Err(ConfigError::new(
                "token and username/password authentication are mutually exclusive",
            ));
        }

        Ok(SecurityConfig {
            username: self.username,
            password: self.password,
            token: self.token,
            authenticator: self.authenticator,
            permissions: self.permissions,
        })
    }
}

/// Diagnostics configuration for monitoring and troubleshooting.
#[derive(Debug, Clone)]
pub struct DiagnosticsConfig {
    enabled: bool,
    slow_operation_threshold: Duration,
}

impl DiagnosticsConfig {
    /// Returns whether diagnostics are enabled.
    pub fn enabled(&self) -> bool {
        self.enabled
    }

    /// Returns the slow operation threshold.
    ///
    /// Operations exceeding this duration will trigger warnings.
    pub fn slow_operation_threshold(&self) -> Duration {
        self.slow_operation_threshold
    }
}

impl Default for DiagnosticsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            slow_operation_threshold: DEFAULT_SLOW_OPERATION_THRESHOLD,
        }
    }
}

/// Builder for `DiagnosticsConfig`.
#[derive(Debug, Clone, Default)]
pub struct DiagnosticsConfigBuilder {
    enabled: Option<bool>,
    slow_operation_threshold: Option<Duration>,
}

impl DiagnosticsConfigBuilder {
    /// Creates a new diagnostics configuration builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Enables or disables diagnostics.
    pub fn enabled(mut self, enabled: bool) -> Self {
        self.enabled = Some(enabled);
        self
    }

    /// Sets the slow operation threshold.
    ///
    /// Operations exceeding this duration will trigger warnings.
    pub fn slow_operation_threshold(mut self, threshold: Duration) -> Self {
        self.slow_operation_threshold = Some(threshold);
        self
    }

    /// Builds the diagnostics configuration.
    pub fn build(self) -> Result<DiagnosticsConfig, ConfigError> {
        let threshold = self
            .slow_operation_threshold
            .unwrap_or(DEFAULT_SLOW_OPERATION_THRESHOLD);

        if threshold.is_zero() {
            return Err(ConfigError::new(
                "slow_operation_threshold must be greater than zero",
            ));
        }

        Ok(DiagnosticsConfig {
            enabled: self.enabled.unwrap_or(true),
            slow_operation_threshold: threshold,
        })
    }
}

/// Main client configuration.
#[derive(Debug, Clone)]
pub struct ClientConfig {
    cluster_name: String,
    network: NetworkConfig,
    retry: RetryConfig,
    security: SecurityConfig,
    near_caches: Vec<NearCacheConfig>,
    quorum_configs: Vec<QuorumConfig>,
    diagnostics: DiagnosticsConfig,
    user_code_deployment: Option<UserCodeDeploymentConfig>,
}

impl ClientConfig {
    /// Creates a new client configuration builder.
    pub fn builder() -> ClientConfigBuilder {
        ClientConfigBuilder::new()
    }

    /// Returns the cluster name.
    pub fn cluster_name(&self) -> &str {
        &self.cluster_name
    }

    /// Returns the network configuration.
    pub fn network(&self) -> &NetworkConfig {
        &self.network
    }

    /// Returns the retry configuration.
    pub fn retry(&self) -> &RetryConfig {
        &self.retry
    }

    /// Returns the security configuration.
    pub fn security(&self) -> &SecurityConfig {
        &self.security
    }

    /// Returns the near-cache configurations.
    pub fn near_caches(&self) -> &[NearCacheConfig] {
        &self.near_caches
    }

    /// Finds a near-cache configuration matching the given map name.
    pub fn find_near_cache(&self, map_name: &str) -> Option<&NearCacheConfig> {
        self.near_caches.iter().find(|nc| nc.matches(map_name))
    }

    /// Returns the quorum configurations.
    pub fn quorum_configs(&self) -> &[QuorumConfig] {
        &self.quorum_configs
    }

    /// Finds a quorum configuration matching the given data structure name.
    pub fn find_quorum_config(&self, name: &str) -> Option<&QuorumConfig> {
        self.quorum_configs.iter().find(|qc| qc.matches(name))
    }

    /// Returns the diagnostics configuration.
    pub fn diagnostics(&self) -> &DiagnosticsConfig {
        &self.diagnostics
    }

    /// Returns the user code deployment configuration, if set.
    pub fn user_code_deployment(&self) -> Option<&UserCodeDeploymentConfig> {
        self.user_code_deployment.as_ref()
    }
}

impl Default for ClientConfig {
    fn default() -> Self {
        ClientConfigBuilder::new().build().unwrap()
    }
}

/// Builder for `ClientConfig`.
#[derive(Debug, Clone, Default)]
pub struct ClientConfigBuilder {
    cluster_name: Option<String>,
    network: NetworkConfigBuilder,
    retry: RetryConfigBuilder,
    security: SecurityConfigBuilder,
    near_caches: Vec<NearCacheConfig>,
    quorum_configs: Vec<QuorumConfig>,
    diagnostics: DiagnosticsConfigBuilder,
    user_code_deployment: Option<UserCodeDeploymentConfig>,
}

impl ClientConfigBuilder {
    /// Creates a new client configuration builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the cluster name.
    pub fn cluster_name(mut self, name: impl Into<String>) -> Self {
        self.cluster_name = Some(name.into());
        self
    }

    /// Configures network settings using a builder function.
    pub fn network<F>(mut self, f: F) -> Self
    where
        F: FnOnce(NetworkConfigBuilder) -> NetworkConfigBuilder,
    {
        self.network = f(self.network);
        self
    }

    /// Configures retry settings using a builder function.
    pub fn retry<F>(mut self, f: F) -> Self
    where
        F: FnOnce(RetryConfigBuilder) -> RetryConfigBuilder,
    {
        self.retry = f(self.retry);
        self
    }

    /// Configures security settings using a builder function.
    pub fn security<F>(mut self, f: F) -> Self
    where
        F: FnOnce(SecurityConfigBuilder) -> SecurityConfigBuilder,
    {
        self.security = f(self.security);
        self
    }

    /// Adds a cluster member address.
    pub fn add_address(mut self, address: SocketAddr) -> Self {
        self.network = self.network.add_address(address);
        self
    }

    /// Sets the cluster member addresses.
    pub fn addresses(mut self, addresses: impl IntoIterator<Item = SocketAddr>) -> Self {
        self.network = self.network.addresses(addresses);
        self
    }

    /// Sets the connection timeout.
    pub fn connection_timeout(mut self, timeout: Duration) -> Self {
        self.network = self.network.connection_timeout(timeout);
        self
    }

    /// Sets credentials for authentication.
    pub fn credentials(mut self, username: impl Into<String>, password: impl Into<String>) -> Self {
        self.security = self.security.credentials(username, password);
        self
    }

    /// Adds a near-cache configuration.
    ///
    /// Near-caches provide local caching of map entries to reduce network latency.
    /// Multiple configurations can be added, each matching different map name patterns.
    pub fn add_near_cache_config(mut self, config: NearCacheConfig) -> Self {
        self.near_caches.push(config);
        self
    }

    /// Adds a quorum configuration for split-brain protection.
    ///
    /// Quorum configurations define the minimum cluster size required for
    /// operations on matching data structures. Multiple configurations can
    /// be added with different name patterns.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use hazelcast_client::config::{QuorumConfig, QuorumType};
    ///
    /// let quorum = QuorumConfig::builder("important-*")
    ///     .min_cluster_size(3)
    ///     .quorum_type(QuorumType::ReadWrite)
    ///     .build()
    ///     .unwrap();
    ///
    /// let config = ClientConfigBuilder::new()
    ///     .add_quorum_config(quorum)
    ///     .build()
    ///     .unwrap();
    /// ```
    pub fn add_quorum_config(mut self, config: QuorumConfig) -> Self {
        self.quorum_configs.push(config);
        self
    }

    /// Configures diagnostics settings using a builder function.
    pub fn diagnostics<F>(mut self, f: F) -> Self
    where
        F: FnOnce(DiagnosticsConfigBuilder) -> DiagnosticsConfigBuilder,
    {
        self.diagnostics = f(self.diagnostics);
        self
    }

    /// Sets the slow operation threshold for diagnostics.
    pub fn slow_operation_threshold(mut self, threshold: Duration) -> Self {
        self.diagnostics = self.diagnostics.slow_operation_threshold(threshold);
        self
    }

    /// Sets the user code deployment configuration.
    ///
    /// User code deployment allows deploying Java classes to the cluster
    /// for server-side execution (entry processors, aggregators, etc.).
    ///
    /// # Example
    ///
    /// ```ignore
    /// use hazelcast_client::deployment::{UserCodeDeploymentConfig, ClassDefinition};
    ///
    /// let processor = ClassDefinition::new(
    ///     "com.example.IncrementProcessor",
    ///     bytecode,
    /// );
    ///
    /// let ucd = UserCodeDeploymentConfig::builder()
    ///     .enabled(true)
    ///     .add_class(processor)
    ///     .build()?;
    ///
    /// let config = ClientConfigBuilder::new()
    ///     .user_code_deployment(ucd)
    ///     .build()?;
    /// ```
    pub fn user_code_deployment(mut self, config: UserCodeDeploymentConfig) -> Self {
        self.user_code_deployment = Some(config);
        self
    }

    /// Builds the client configuration, returning an error if validation fails.
    pub fn build(self) -> Result<ClientConfig, ConfigError> {
        let cluster_name = self
            .cluster_name
            .unwrap_or_else(|| DEFAULT_CLUSTER_NAME.to_string());

        if cluster_name.is_empty() {
            return Err(ConfigError::new("cluster_name must not be empty"));
        }

        let network = self.network.build()?;
        let retry = self.retry.build()?;
        let security = self.security.build()?;
        let diagnostics = self.diagnostics.build()?;

        Ok(ClientConfig {
            cluster_name,
            network,
            retry,
            security,
            near_caches: self.near_caches,
            quorum_configs: self.quorum_configs,
            diagnostics,
            user_code_deployment: self.user_code_deployment,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_client_config() {
        let config = ClientConfig::default();
        assert_eq!(config.cluster_name(), "dev");
        assert_eq!(config.network().addresses().len(), 1);
        assert_eq!(
            config.network().addresses()[0],
            "127.0.0.1:5701".parse::<SocketAddr>().unwrap()
        );
    }

    #[test]
    fn test_builder_cluster_name() {
        let config = ClientConfig::builder()
            .cluster_name("production")
            .build()
            .unwrap();
        assert_eq!(config.cluster_name(), "production");
    }

    #[test]
    fn test_builder_empty_cluster_name_fails() {
        let result = ClientConfig::builder().cluster_name("").build();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("cluster_name must not be empty"));
    }

    #[test]
    fn test_builder_add_address() {
        let addr1: SocketAddr = "192.168.1.1:5701".parse().unwrap();
        let addr2: SocketAddr = "192.168.1.2:5701".parse().unwrap();

        let config = ClientConfig::builder()
            .add_address(addr1)
            .add_address(addr2)
            .build()
            .unwrap();

        assert_eq!(config.network().addresses().len(), 2);
        assert!(config.network().addresses().contains(&addr1));
        assert!(config.network().addresses().contains(&addr2));
    }

    #[test]
    fn test_builder_addresses_replaces() {
        let addr1: SocketAddr = "192.168.1.1:5701".parse().unwrap();
        let addr2: SocketAddr = "192.168.1.2:5701".parse().unwrap();

        let config = ClientConfig::builder()
            .add_address("10.0.0.1:5701".parse().unwrap())
            .addresses([addr1, addr2])
            .build()
            .unwrap();

        assert_eq!(config.network().addresses().len(), 2);
        assert!(!config
            .network()
            .addresses()
            .contains(&"10.0.0.1:5701".parse().unwrap()));
    }

    #[test]
    fn test_builder_connection_timeout() {
        let config = ClientConfig::builder()
            .connection_timeout(Duration::from_secs(10))
            .build()
            .unwrap();
        assert_eq!(config.network().connection_timeout(), Duration::from_secs(10));
    }

    #[test]
    fn test_builder_credentials() {
        let config = ClientConfig::builder()
            .credentials("admin", "secret123")
            .build()
            .unwrap();

        assert_eq!(config.security().username(), Some("admin"));
        assert_eq!(config.security().password(), Some("secret123"));
        assert!(config.security().has_credentials());
    }

    #[test]
    fn test_security_partial_credentials_fails() {
        let result = SecurityConfigBuilder::new().username("admin").build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("both username and password"));
    }

    #[test]
    fn test_security_no_credentials() {
        let config = SecurityConfigBuilder::new().build().unwrap();
        assert!(!config.has_credentials());
        assert!(config.username().is_none());
        assert!(config.password().is_none());
    }

    #[test]
    fn test_network_config_builder() {
        let addr: SocketAddr = "10.0.0.1:5701".parse().unwrap();
        let config = NetworkConfigBuilder::new()
            .add_address(addr)
            .connection_timeout(Duration::from_secs(15))
            .heartbeat_interval(Duration::from_secs(10))
            .build()
            .unwrap();

        assert_eq!(config.addresses(), &[addr]);
        assert_eq!(config.connection_timeout(), Duration::from_secs(15));
        assert_eq!(config.heartbeat_interval(), Duration::from_secs(10));
    }

    #[test]
    fn test_network_config_defaults() {
        let config = NetworkConfigBuilder::new().build().unwrap();
        assert_eq!(config.addresses().len(), 1);
        assert_eq!(config.connection_timeout(), DEFAULT_CONNECTION_TIMEOUT);
        assert_eq!(config.heartbeat_interval(), DEFAULT_HEARTBEAT_INTERVAL);
    }

    #[test]
    fn test_retry_config_builder() {
        let config = RetryConfigBuilder::new()
            .initial_backoff(Duration::from_millis(200))
            .max_backoff(Duration::from_secs(60))
            .multiplier(1.5)
            .max_retries(5)
            .build()
            .unwrap();

        assert_eq!(config.initial_backoff(), Duration::from_millis(200));
        assert_eq!(config.max_backoff(), Duration::from_secs(60));
        assert_eq!(config.multiplier(), 1.5);
        assert_eq!(config.max_retries(), 5);
    }

    #[test]
    fn test_retry_initial_exceeds_max_fails() {
        let result = RetryConfigBuilder::new()
            .initial_backoff(Duration::from_secs(60))
            .max_backoff(Duration::from_secs(10))
            .build();

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("initial_backoff must not exceed max_backoff"));
    }

    #[test]
    fn test_retry_multiplier_less_than_one_fails() {
        let result = RetryConfigBuilder::new().multiplier(0.5).build();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("multiplier must be at least 1.0"));
    }

    #[test]
    fn test_retry_config_defaults() {
        let config = RetryConfigBuilder::new().build().unwrap();
        assert_eq!(config.initial_backoff(), DEFAULT_INITIAL_BACKOFF);
        assert_eq!(config.max_backoff(), DEFAULT_MAX_BACKOFF);
        assert_eq!(config.multiplier(), DEFAULT_RETRY_MULTIPLIER);
        assert_eq!(config.max_retries(), DEFAULT_MAX_RETRIES);
    }

    #[test]
    fn test_fluent_sub_builder_api() {
        let config = ClientConfig::builder()
            .cluster_name("test-cluster")
            .network(|n| {
                n.add_address("192.168.1.1:5701".parse().unwrap())
                    .connection_timeout(Duration::from_secs(20))
            })
            .retry(|r| r.max_retries(3).multiplier(1.5))
            .security(|s| s.credentials("user", "pass"))
            .build()
            .unwrap();

        assert_eq!(config.cluster_name(), "test-cluster");
        assert_eq!(config.network().connection_timeout(), Duration::from_secs(20));
        assert_eq!(config.retry().max_retries(), 3);
        assert!(config.security().has_credentials());
    }

    #[test]
    fn test_config_error_display() {
        let err = ConfigError::new("test error");
        assert_eq!(err.to_string(), "configuration error: test error");
    }

    #[test]
    fn test_config_error_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<ConfigError>();
    }

    #[test]
    fn test_client_config_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<ClientConfig>();
    }

    #[test]
    fn test_client_config_clone() {
        let config = ClientConfig::builder()
            .cluster_name("clone-test")
            .credentials("user", "pass")
            .build()
            .unwrap();

        let cloned = config.clone();
        assert_eq!(cloned.cluster_name(), config.cluster_name());
        assert_eq!(
            cloned.security().username(),
            config.security().username()
        );
    }

    #[test]
    fn test_tls_config_defaults() {
        let config = TlsConfigBuilder::new().build().unwrap();
        assert!(!config.enabled());
        assert!(config.ca_cert_path().is_none());
        assert!(config.client_cert_path().is_none());
        assert!(config.client_key_path().is_none());
        assert!(config.verify_hostname());
        assert!(!config.has_client_auth());
    }

    #[test]
    fn test_tls_config_enabled() {
        let config = TlsConfigBuilder::new()
            .enabled(true)
            .verify_hostname(false)
            .build()
            .unwrap();

        assert!(config.enabled());
        assert!(!config.verify_hostname());
    }

    #[test]
    fn test_tls_config_with_ca_cert() {
        let config = TlsConfigBuilder::new()
            .enabled(true)
            .ca_cert_path("/path/to/ca.pem")
            .build()
            .unwrap();

        assert!(config.enabled());
        assert_eq!(
            config.ca_cert_path(),
            Some(&PathBuf::from("/path/to/ca.pem"))
        );
    }

    #[test]
    fn test_tls_config_client_auth() {
        let config = TlsConfigBuilder::new()
            .enabled(true)
            .client_auth("/path/to/cert.pem", "/path/to/key.pem")
            .build()
            .unwrap();

        assert!(config.has_client_auth());
        assert_eq!(
            config.client_cert_path(),
            Some(&PathBuf::from("/path/to/cert.pem"))
        );
        assert_eq!(
            config.client_key_path(),
            Some(&PathBuf::from("/path/to/key.pem"))
        );
    }

    #[test]
    fn test_tls_config_partial_client_auth_fails() {
        let result = TlsConfigBuilder::new()
            .enabled(true)
            .client_cert_path("/path/to/cert.pem")
            .build();

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("client_cert_path and client_key_path must be provided together"));
    }

    #[test]
    fn test_network_config_with_tls() {
        let config = NetworkConfigBuilder::new()
            .tls(|t| t.enabled(true).verify_hostname(false))
            .build()
            .unwrap();

        assert!(config.tls().enabled());
        assert!(!config.tls().verify_hostname());
    }

    #[test]
    fn test_network_config_enable_tls_shortcut() {
        let config = NetworkConfigBuilder::new()
            .enable_tls()
            .build()
            .unwrap();

        assert!(config.tls().enabled());
    }

    #[test]
    fn test_tls_config_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<TlsConfig>();
    }

    #[test]
    fn test_client_config_with_near_cache() {
        use crate::cache::{EvictionPolicy, InMemoryFormat, NearCacheConfig};

        let near_cache = NearCacheConfig::builder("user-*")
            .max_size(1000)
            .eviction_policy(EvictionPolicy::Lfu)
            .in_memory_format(InMemoryFormat::Object)
            .build()
            .unwrap();

        let config = ClientConfig::builder()
            .add_near_cache_config(near_cache)
            .build()
            .unwrap();

        assert_eq!(config.near_caches().len(), 1);
        assert_eq!(config.near_caches()[0].name(), "user-*");
    }

    #[test]
    fn test_client_config_find_near_cache() {
        use crate::cache::NearCacheConfig;

        let user_cache = NearCacheConfig::builder("user-*").build().unwrap();
        let product_cache = NearCacheConfig::builder("product-map").build().unwrap();

        let config = ClientConfig::builder()
            .add_near_cache_config(user_cache)
            .add_near_cache_config(product_cache)
            .build()
            .unwrap();

        assert!(config.find_near_cache("user-data").is_some());
        assert!(config.find_near_cache("user-sessions").is_some());
        assert!(config.find_near_cache("product-map").is_some());
        assert!(config.find_near_cache("product-other").is_none());
        assert!(config.find_near_cache("orders").is_none());
    }

    #[test]
    fn test_client_config_multiple_near_caches() {
        use crate::cache::NearCacheConfig;

        let config = ClientConfig::builder()
            .add_near_cache_config(NearCacheConfig::builder("cache1").build().unwrap())
            .add_near_cache_config(NearCacheConfig::builder("cache2").build().unwrap())
            .add_near_cache_config(NearCacheConfig::builder("cache3").build().unwrap())
            .build()
            .unwrap();

        assert_eq!(config.near_caches().len(), 3);
    }

    #[test]
    fn test_client_config_default_no_near_caches() {
        let config = ClientConfig::default();
        assert!(config.near_caches().is_empty());
    }

    #[test]
    fn test_security_token_authentication() {
        let config = SecurityConfigBuilder::new()
            .token("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.test")
            .build()
            .unwrap();

        assert!(config.has_token());
        assert!(!config.has_credentials());
        assert!(config.is_authenticated());
        assert_eq!(
            config.token(),
            Some("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.test")
        );
    }

    #[test]
    fn test_security_token_and_credentials_mutually_exclusive() {
        let result = SecurityConfigBuilder::new()
            .token("some-token")
            .credentials("admin", "secret")
            .build();

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("mutually exclusive"));
    }

    #[test]
    fn test_security_is_authenticated() {
        let no_auth = SecurityConfigBuilder::new().build().unwrap();
        assert!(!no_auth.is_authenticated());

        let with_creds = SecurityConfigBuilder::new()
            .credentials("user", "pass")
            .build()
            .unwrap();
        assert!(with_creds.is_authenticated());

        let with_token = SecurityConfigBuilder::new()
            .token("token")
            .build()
            .unwrap();
        assert!(with_token.is_authenticated());
    }

    #[test]
    fn test_client_config_with_token() {
        let config = ClientConfig::builder()
            .security(|s| s.token("my-auth-token"))
            .build()
            .unwrap();

        assert!(config.security().has_token());
        assert_eq!(config.security().token(), Some("my-auth-token"));
    }

    #[test]
    fn test_permissions_default_denied() {
        let perms = Permissions::new();
        assert!(!perms.can_create());
        assert!(!perms.can_destroy());
        assert!(!perms.can_read());
        assert!(!perms.can_put());
        assert!(!perms.can_remove());
        assert!(!perms.can_listen());
        assert!(!perms.can_lock());
        assert!(!perms.can_index());
        assert!(!perms.is_admin());
    }

    #[test]
    fn test_permissions_all() {
        let perms = Permissions::all();
        assert!(perms.can_create());
        assert!(perms.can_destroy());
        assert!(perms.can_read());
        assert!(perms.can_put());
        assert!(perms.can_remove());
        assert!(perms.can_listen());
        assert!(perms.can_lock());
        assert!(perms.can_index());
        assert!(perms.is_admin());
    }

    #[test]
    fn test_permissions_grant_and_revoke() {
        let mut perms = Permissions::new();

        perms.grant(PermissionAction::Read);
        perms.grant(PermissionAction::Put);
        assert!(perms.can_read());
        assert!(perms.can_put());
        assert!(!perms.can_remove());

        perms.revoke(PermissionAction::Read);
        assert!(!perms.can_read());
        assert!(perms.can_put());
    }

    #[test]
    fn test_permissions_admin_overrides() {
        let mut perms = Permissions::new();
        perms.grant(PermissionAction::All);

        assert!(perms.can_create());
        assert!(perms.can_read());
        assert!(perms.can_put());
        assert!(perms.can_remove());
    }

    #[test]
    fn test_permissions_is_permitted() {
        let mut perms = Permissions::new();
        perms.grant(PermissionAction::Read);
        perms.grant(PermissionAction::Listen);

        assert!(perms.is_permitted(PermissionAction::Read));
        assert!(perms.is_permitted(PermissionAction::Listen));
        assert!(!perms.is_permitted(PermissionAction::Put));
        assert!(!perms.is_permitted(PermissionAction::All));
    }

    #[test]
    fn test_permissions_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Permissions>();
    }

    #[test]
    fn test_security_config_with_permissions() {
        let mut perms = Permissions::new();
        perms.grant(PermissionAction::Read);
        perms.grant(PermissionAction::Put);

        let config = SecurityConfigBuilder::new()
            .permissions(perms)
            .build()
            .unwrap();

        assert!(config.permissions().is_some());
        let permissions = config.permissions().unwrap();
        assert!(permissions.can_read());
        assert!(permissions.can_put());
        assert!(!permissions.can_remove());
    }

    #[test]
    fn test_security_config_effective_permissions_with_none() {
        let config = SecurityConfigBuilder::new().build().unwrap();
        assert!(config.permissions().is_none());

        let effective = config.effective_permissions();
        assert!(effective.can_read());
        assert!(effective.can_put());
        assert!(effective.can_remove());
        assert!(effective.is_admin());
    }

    #[test]
    fn test_security_config_effective_permissions_with_some() {
        let mut perms = Permissions::new();
        perms.grant(PermissionAction::Read);

        let config = SecurityConfigBuilder::new()
            .permissions(perms)
            .build()
            .unwrap();

        let effective = config.effective_permissions();
        assert!(effective.can_read());
        assert!(!effective.can_put());
        assert!(!effective.can_remove());
    }

    #[test]
    fn test_permission_action_is_copy() {
        fn assert_copy<T: Copy>() {}
        assert_copy::<PermissionAction>();
    }

    #[test]
    fn test_network_config_ws_url() {
        let config = NetworkConfigBuilder::new()
            .ws_url("ws://127.0.0.1:5701/hazelcast")
            .build()
            .unwrap();

        assert_eq!(
            config.ws_url(),
            Some("ws://127.0.0.1:5701/hazelcast")
        );
    }

    #[test]
    fn test_network_config_ws_url_default_none() {
        let config = NetworkConfigBuilder::new().build().unwrap();
        assert!(config.ws_url().is_none());
    }

    #[test]
    fn test_security_custom_authenticator() {
        use crate::security::DefaultAuthenticator;

        let config = SecurityConfigBuilder::new()
            .custom_authenticator(Arc::new(DefaultAuthenticator))
            .build()
            .unwrap();

        assert!(config.authenticator().is_some());
    }

    #[test]
    fn test_security_custom_authenticator_is_authenticated() {
        use crate::security::DefaultAuthenticator;

        let config = SecurityConfigBuilder::new()
            .custom_authenticator(Arc::new(DefaultAuthenticator))
            .build()
            .unwrap();

        assert!(config.is_authenticated());
    }

    #[test]
    fn test_network_config_wss_url() {
        let config = NetworkConfigBuilder::new()
            .ws_url("wss://cluster.example.com:443")
            .build()
            .unwrap();

        assert_eq!(
            config.ws_url(),
            Some("wss://cluster.example.com:443")
        );
    }

    #[test]
    fn test_client_config_with_websocket() {
        let config = ClientConfig::builder()
            .network(|n| n.ws_url("ws://localhost:8080"))
            .build()
            .unwrap();

        assert_eq!(config.network().ws_url(), Some("ws://localhost:8080"));
    }

    #[test]
    fn test_diagnostics_config_defaults() {
        let config = DiagnosticsConfigBuilder::new().build().unwrap();
        assert!(config.enabled());
        assert_eq!(
            config.slow_operation_threshold(),
            DEFAULT_SLOW_OPERATION_THRESHOLD
        );
    }

    #[test]
    fn test_diagnostics_config_custom_threshold() {
        let config = DiagnosticsConfigBuilder::new()
            .slow_operation_threshold(Duration::from_millis(500))
            .build()
            .unwrap();

        assert_eq!(
            config.slow_operation_threshold(),
            Duration::from_millis(500)
        );
    }

    #[test]
    fn test_diagnostics_config_disabled() {
        let config = DiagnosticsConfigBuilder::new()
            .enabled(false)
            .build()
            .unwrap();

        assert!(!config.enabled());
    }

    #[test]
    fn test_diagnostics_config_zero_threshold_fails() {
        let result = DiagnosticsConfigBuilder::new()
            .slow_operation_threshold(Duration::ZERO)
            .build();

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("slow_operation_threshold must be greater than zero"));
    }

    #[test]
    fn test_client_config_with_diagnostics() {
        let config = ClientConfig::builder()
            .diagnostics(|d| d.slow_operation_threshold(Duration::from_secs(1)))
            .build()
            .unwrap();

        assert_eq!(
            config.diagnostics().slow_operation_threshold(),
            Duration::from_secs(1)
        );
    }

    #[test]
    fn test_client_config_slow_operation_threshold_shortcut() {
        let config = ClientConfig::builder()
            .slow_operation_threshold(Duration::from_millis(250))
            .build()
            .unwrap();

        assert_eq!(
            config.diagnostics().slow_operation_threshold(),
            Duration::from_millis(250)
        );
    }

    #[test]
    fn test_diagnostics_config_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<DiagnosticsConfig>();
    }

    #[test]
    fn test_client_config_default_diagnostics() {
        let config = ClientConfig::default();
        assert!(config.diagnostics().enabled());
        assert_eq!(
            config.diagnostics().slow_operation_threshold(),
            DEFAULT_SLOW_OPERATION_THRESHOLD
        );
    }

    #[cfg(feature = "aws")]
    #[test]
    fn test_network_config_aws_discovery() {
        use crate::connection::AwsDiscoveryConfig;

        let aws_config = AwsDiscoveryConfig::new("us-west-2")
            .with_tag("hazelcast:cluster", "production")
            .with_port(5702);

        let config = NetworkConfigBuilder::new()
            .aws_discovery(aws_config)
            .build()
            .unwrap();

        assert!(config.aws_discovery().is_some());
        let aws = config.aws_discovery().unwrap();
        assert_eq!(aws.region(), "us-west-2");
        assert_eq!(aws.port(), 5702);
    }

    #[cfg(feature = "azure")]
    #[test]
    fn test_network_config_azure_discovery() {
        use crate::connection::AzureDiscoveryConfig;

        let azure_config = AzureDiscoveryConfig::new("sub-123", "rg-hazelcast")
            .with_tag("env", "prod")
            .with_port(5703);

        let config = NetworkConfigBuilder::new()
            .azure_discovery(azure_config)
            .build()
            .unwrap();

        assert!(config.azure_discovery().is_some());
        let azure = config.azure_discovery().unwrap();
        assert_eq!(azure.subscription_id, "sub-123");
        assert_eq!(azure.resource_group, "rg-hazelcast");
        assert_eq!(azure.port, 5703);
    }

    #[cfg(feature = "gcp")]
    #[test]
    fn test_network_config_gcp_discovery() {
        use crate::connection::GcpDiscoveryConfig;

        let gcp_config = GcpDiscoveryConfig::new("my-project")
            .with_zone("us-central1-a")
            .with_label("hazelcast", "cluster-1")
            .with_port(5704);

        let config = NetworkConfigBuilder::new()
            .gcp_discovery(gcp_config)
            .build()
            .unwrap();

        assert!(config.gcp_discovery().is_some());
        let gcp = config.gcp_discovery().unwrap();
        assert_eq!(gcp.project_id, "my-project");
        assert_eq!(gcp.zone, Some("us-central1-a".to_string()));
        assert_eq!(gcp.port, 5704);
    }

    #[cfg(feature = "kubernetes")]
    #[test]
    fn test_network_config_kubernetes_discovery() {
        use crate::connection::KubernetesDiscoveryConfig;

        let k8s_config = KubernetesDiscoveryConfig::new()
            .namespace("hazelcast")
            .service_name("hz-service")
            .pod_label("app", "hazelcast")
            .port(5705);

        let config = NetworkConfigBuilder::new()
            .kubernetes_discovery(k8s_config)
            .build()
            .unwrap();

        assert!(config.kubernetes_discovery().is_some());
        let k8s = config.kubernetes_discovery().unwrap();
        assert_eq!(k8s.namespace, Some("hazelcast".to_string()));
        assert_eq!(k8s.service_name, Some("hz-service".to_string()));
        assert_eq!(k8s.port, Some(5705));
    }

    #[cfg(feature = "cloud")]
    #[test]
    fn test_network_config_cloud_discovery() {
        use crate::connection::CloudDiscoveryConfig;

        let cloud_config = CloudDiscoveryConfig::new("my-discovery-token")
            .with_timeout(30);

        let config = NetworkConfigBuilder::new()
            .cloud_discovery(cloud_config)
            .build()
            .unwrap();

        assert!(config.cloud_discovery().is_some());
        let cloud = config.cloud_discovery().unwrap();
        assert_eq!(cloud.discovery_token(), "my-discovery-token");
        assert_eq!(cloud.timeout_secs(), 30);
    }

    #[cfg(feature = "cloud")]
    #[test]
    fn test_client_config_with_cloud_discovery() {
        use crate::connection::CloudDiscoveryConfig;

        let config = ClientConfig::builder()
            .cluster_name("cloud-cluster")
            .network(|n| {
                n.cloud_discovery(
                    CloudDiscoveryConfig::new("token-abc123")
                        .with_coordinator_url("https://custom.hazelcast.cloud"),
                )
            })
            .build()
            .unwrap();

        assert!(config.network().cloud_discovery().is_some());
        let cloud = config.network().cloud_discovery().unwrap();
        assert_eq!(cloud.discovery_token(), "token-abc123");
        assert_eq!(cloud.coordinator_url(), "https://custom.hazelcast.cloud");
    }

    #[test]
    fn test_network_config_no_discovery_by_default() {
        let config = NetworkConfigBuilder::new().build().unwrap();

        #[cfg(feature = "aws")]
        assert!(config.aws_discovery().is_none());

        #[cfg(feature = "azure")]
        assert!(config.azure_discovery().is_none());

        #[cfg(feature = "gcp")]
        assert!(config.gcp_discovery().is_none());

        #[cfg(feature = "kubernetes")]
        assert!(config.kubernetes_discovery().is_none());

        #[cfg(feature = "cloud")]
        assert!(config.cloud_discovery().is_none());
    }

    #[test]
    fn test_wan_target_cluster_config_builder() {
        let target = WanTargetClusterConfigBuilder::new("target-cluster")
            .add_endpoint("192.168.1.100:5701".parse().unwrap())
            .add_endpoint("192.168.1.101:5701".parse().unwrap())
            .connection_timeout(Duration::from_secs(10))
            .build()
            .unwrap();

        assert_eq!(target.cluster_name(), "target-cluster");
        assert_eq!(target.endpoints().len(), 2);
        assert_eq!(target.connection_timeout(), Duration::from_secs(10));
    }

    #[test]
    fn test_wan_target_cluster_config_default_timeout() {
        let target = WanTargetClusterConfigBuilder::new("remote")
            .add_endpoint("10.0.0.1:5701".parse().unwrap())
            .build()
            .unwrap();

        assert_eq!(target.connection_timeout(), DEFAULT_CONNECTION_TIMEOUT);
    }

    #[test]
    fn test_wan_target_cluster_config_empty_name_fails() {
        let result = WanTargetClusterConfigBuilder::new("")
            .add_endpoint("10.0.0.1:5701".parse().unwrap())
            .build();

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("cluster_name must not be empty"));
    }

    #[test]
    fn test_wan_target_cluster_config_no_endpoints_fails() {
        let result = WanTargetClusterConfigBuilder::new("remote").build();

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("at least one endpoint is required"));
    }

    #[test]
    fn test_wan_target_cluster_config_endpoints_replace() {
        let target = WanTargetClusterConfigBuilder::new("remote")
            .add_endpoint("10.0.0.1:5701".parse().unwrap())
            .endpoints(["10.0.0.2:5701".parse().unwrap(), "10.0.0.3:5701".parse().unwrap()])
            .build()
            .unwrap();

        assert_eq!(target.endpoints().len(), 2);
        assert!(target.endpoints().contains(&"10.0.0.2:5701".parse().unwrap()));
        assert!(target.endpoints().contains(&"10.0.0.3:5701".parse().unwrap()));
    }

    #[test]
    fn test_wan_replication_config_builder() {
        let target1 = WanTargetClusterConfigBuilder::new("dc-west")
            .add_endpoint("10.1.0.1:5701".parse().unwrap())
            .build()
            .unwrap();

        let target2 = WanTargetClusterConfigBuilder::new("dc-east")
            .add_endpoint("10.2.0.1:5701".parse().unwrap())
            .build()
            .unwrap();

        let config = WanReplicationConfigBuilder::new("my-wan-replication")
            .add_target_cluster(target1)
            .add_target_cluster(target2)
            .build()
            .unwrap();

        assert_eq!(config.name(), "my-wan-replication");
        assert_eq!(config.target_clusters().len(), 2);
    }

    #[test]
    fn test_wan_replication_config_find_target() {
        let target = WanTargetClusterConfigBuilder::new("dc-west")
            .add_endpoint("10.1.0.1:5701".parse().unwrap())
            .build()
            .unwrap();

        let config = WanReplicationConfigBuilder::new("wan-config")
            .add_target_cluster(target)
            .build()
            .unwrap();

        assert!(config.find_target("dc-west").is_some());
        assert!(config.find_target("dc-east").is_none());
    }

    #[test]
    fn test_wan_replication_config_empty_name_fails() {
        let target = WanTargetClusterConfigBuilder::new("remote")
            .add_endpoint("10.0.0.1:5701".parse().unwrap())
            .build()
            .unwrap();

        let result = WanReplicationConfigBuilder::new("")
            .add_target_cluster(target)
            .build();

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("WAN replication name must not be empty"));
    }

    #[test]
    fn test_wan_replication_config_no_targets_fails() {
        let result = WanReplicationConfigBuilder::new("wan-config").build();

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("at least one target cluster is required"));
    }

    #[test]
    fn test_wan_replication_ref_builder() {
        let wan_ref = WanReplicationRefBuilder::new("my-wan-config")
            .merge_policy_class_name("com.example.MyMergePolicy")
            .republishing_enabled(true)
            .add_filter("com.example.MyFilter")
            .build()
            .unwrap();

        assert_eq!(wan_ref.name(), "my-wan-config");
        assert_eq!(
            wan_ref.merge_policy_class_name(),
            "com.example.MyMergePolicy"
        );
        assert!(wan_ref.republishing_enabled());
        assert_eq!(wan_ref.filters().len(), 1);
        assert_eq!(wan_ref.filters()[0], "com.example.MyFilter");
    }

    #[test]
    fn test_wan_replication_ref_defaults() {
        let wan_ref = WanReplicationRefBuilder::new("wan-config")
            .build()
            .unwrap();

        assert_eq!(
            wan_ref.merge_policy_class_name(),
            "com.hazelcast.spi.merge.PassThroughMergePolicy"
        );
        assert!(!wan_ref.republishing_enabled());
        assert!(wan_ref.filters().is_empty());
    }

    #[test]
    fn test_wan_replication_ref_empty_name_fails() {
        let result = WanReplicationRefBuilder::new("").build();

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("WAN replication ref name must not be empty"));
    }

    #[test]
    fn test_wan_replication_ref_multiple_filters() {
        let wan_ref = WanReplicationRefBuilder::new("wan-config")
            .add_filter("com.example.Filter1")
            .add_filter("com.example.Filter2")
            .add_filter("com.example.Filter3")
            .build()
            .unwrap();

        assert_eq!(wan_ref.filters().len(), 3);
    }

    #[test]
    fn test_wan_replication_ref_via_builder_method() {
        let wan_ref = WanReplicationRef::builder("test-wan")
            .republishing_enabled(true)
            .build()
            .unwrap();

        assert_eq!(wan_ref.name(), "test-wan");
        assert!(wan_ref.republishing_enabled());
    }

    #[test]
    fn test_network_config_with_wan_replication() {
        let target = WanTargetClusterConfigBuilder::new("dc-remote")
            .add_endpoint("10.0.0.1:5701".parse().unwrap())
            .build()
            .unwrap();

        let wan_config = WanReplicationConfigBuilder::new("wan-scheme")
            .add_target_cluster(target)
            .build()
            .unwrap();

        let config = NetworkConfigBuilder::new()
            .add_wan_replication(wan_config)
            .build()
            .unwrap();

        assert_eq!(config.wan_replication().len(), 1);
        assert_eq!(config.wan_replication()[0].name(), "wan-scheme");
    }

    #[test]
    fn test_network_config_find_wan_replication() {
        let target = WanTargetClusterConfigBuilder::new("remote")
            .add_endpoint("10.0.0.1:5701".parse().unwrap())
            .build()
            .unwrap();

        let wan1 = WanReplicationConfigBuilder::new("wan-primary")
            .add_target_cluster(target.clone())
            .build()
            .unwrap();

        let wan2 = WanReplicationConfigBuilder::new("wan-secondary")
            .add_target_cluster(target)
            .build()
            .unwrap();

        let config = NetworkConfigBuilder::new()
            .add_wan_replication(wan1)
            .add_wan_replication(wan2)
            .build()
            .unwrap();

        assert!(config.find_wan_replication("wan-primary").is_some());
        assert!(config.find_wan_replication("wan-secondary").is_some());
        assert!(config.find_wan_replication("wan-tertiary").is_none());
    }

    #[test]
    fn test_network_config_default_no_wan_replication() {
        let config = NetworkConfigBuilder::new().build().unwrap();
        assert!(config.wan_replication().is_empty());
    }

    #[test]
    fn test_client_config_with_wan_replication() {
        let target = WanTargetClusterConfigBuilder::new("backup-dc")
            .add_endpoint("192.168.100.1:5701".parse().unwrap())
            .build()
            .unwrap();

        let wan_config = WanReplicationConfigBuilder::new("geo-replication")
            .add_target_cluster(target)
            .build()
            .unwrap();

        let config = ClientConfig::builder()
            .cluster_name("primary")
            .network(|n| n.add_wan_replication(wan_config))
            .build()
            .unwrap();

        assert_eq!(config.network().wan_replication().len(), 1);
        assert!(config
            .network()
            .find_wan_replication("geo-replication")
            .is_some());
    }

    #[test]
    fn test_wan_target_cluster_config_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<WanTargetClusterConfig>();
    }

    #[test]
    fn test_wan_replication_config_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<WanReplicationConfig>();
    }

    #[test]
    fn test_wan_replication_ref_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<WanReplicationRef>();
    }

    #[test]
    fn test_quorum_type_protects_reads() {
        assert!(QuorumType::Read.protects_reads());
        assert!(!QuorumType::Write.protects_reads());
        assert!(QuorumType::ReadWrite.protects_reads());
    }

    #[test]
    fn test_quorum_type_protects_writes() {
        assert!(!QuorumType::Read.protects_writes());
        assert!(QuorumType::Write.protects_writes());
        assert!(QuorumType::ReadWrite.protects_writes());
    }

    #[test]
    fn test_quorum_config_builder_defaults() {
        let config = QuorumConfig::builder("test-*")
            .build()
            .unwrap();

        assert_eq!(config.name(), "test-*");
        assert_eq!(config.min_cluster_size(), 2);
        assert_eq!(config.quorum_type(), QuorumType::ReadWrite);
        assert!(config.quorum_function().is_none());
    }

    #[test]
    fn test_quorum_config_builder_custom() {
        let config = QuorumConfig::builder("important-map")
            .min_cluster_size(3)
            .quorum_type(QuorumType::Write)
            .build()
            .unwrap();

        assert_eq!(config.name(), "important-map");
        assert_eq!(config.min_cluster_size(), 3);
        assert_eq!(config.quorum_type(), QuorumType::Write);
    }

    #[test]
    fn test_quorum_config_empty_name_fails() {
        let result = QuorumConfig::builder("").build();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("name must not be empty"));
    }

    #[test]
    fn test_quorum_config_zero_size_fails() {
        let result = QuorumConfig::builder("test")
            .min_cluster_size(0)
            .build();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("min_cluster_size must be at least 1"));
    }

    #[test]
    fn test_quorum_config_matches_exact() {
        let config = QuorumConfig::builder("my-map")
            .build()
            .unwrap();

        assert!(config.matches("my-map"));
        assert!(!config.matches("other-map"));
        assert!(!config.matches("my-map-extra"));
    }

    #[test]
    fn test_quorum_config_matches_wildcard_suffix() {
        let config = QuorumConfig::builder("user-*")
            .build()
            .unwrap();

        assert!(config.matches("user-sessions"));
        assert!(config.matches("user-data"));
        assert!(config.matches("user-"));
        assert!(!config.matches("admin-user"));
        assert!(!config.matches("users"));
    }

    #[test]
    fn test_quorum_config_matches_wildcard_prefix() {
        let config = QuorumConfig::builder("*-cache")
            .build()
            .unwrap();

        assert!(config.matches("user-cache"));
        assert!(config.matches("product-cache"));
        assert!(config.matches("-cache"));
        assert!(!config.matches("cache"));
        assert!(!config.matches("cache-data"));
    }

    #[test]
    fn test_quorum_config_matches_all_wildcard() {
        let config = QuorumConfig::builder("*")
            .build()
            .unwrap();

        assert!(config.matches("anything"));
        assert!(config.matches(""));
        assert!(config.matches("some-map-name"));
    }

    #[test]
    fn test_quorum_config_check_quorum_simple() {
        let config = QuorumConfig::builder("test")
            .min_cluster_size(3)
            .build()
            .unwrap();

        let member1 = Member::new(uuid::Uuid::new_v4(), "127.0.0.1:5701".parse().unwrap());
        let member2 = Member::new(uuid::Uuid::new_v4(), "127.0.0.1:5702".parse().unwrap());
        let member3 = Member::new(uuid::Uuid::new_v4(), "127.0.0.1:5703".parse().unwrap());

        assert!(!config.check_quorum(&[]));
        assert!(!config.check_quorum(&[member1.clone()]));
        assert!(!config.check_quorum(&[member1.clone(), member2.clone()]));
        assert!(config.check_quorum(&[member1.clone(), member2.clone(), member3.clone()]));
    }

    #[test]
    fn test_quorum_config_protects_operation() {
        let read_config = QuorumConfig::builder("test")
            .quorum_type(QuorumType::Read)
            .build()
            .unwrap();

        let write_config = QuorumConfig::builder("test")
            .quorum_type(QuorumType::Write)
            .build()
            .unwrap();

        let rw_config = QuorumConfig::builder("test")
            .quorum_type(QuorumType::ReadWrite)
            .build()
            .unwrap();

        assert!(read_config.protects_operation(true));
        assert!(!read_config.protects_operation(false));

        assert!(!write_config.protects_operation(true));
        assert!(write_config.protects_operation(false));

        assert!(rw_config.protects_operation(true));
        assert!(rw_config.protects_operation(false));
    }

    #[test]
    fn test_quorum_config_with_custom_function() {
        struct AlwaysTrueQuorum;
        impl QuorumFunction for AlwaysTrueQuorum {
            fn is_present(&self, _members: &[Member]) -> bool {
                true
            }
        }

        let config = QuorumConfig::builder("test")
            .min_cluster_size(0)
            .quorum_function(Arc::new(AlwaysTrueQuorum))
            .build()
            .unwrap();

        assert!(config.check_quorum(&[]));
    }

    #[test]
    fn test_quorum_config_custom_function_overrides_size() {
        struct RequireSpecificMember {
            required_port: u16,
        }
        impl QuorumFunction for RequireSpecificMember {
            fn is_present(&self, members: &[Member]) -> bool {
                members.iter().any(|m| m.address().port() == self.required_port)
            }
        }

        let config = QuorumConfig::builder("test")
            .min_cluster_size(10)
            .quorum_function(Arc::new(RequireSpecificMember { required_port: 5701 }))
            .build()
            .unwrap();

        let member = Member::new(uuid::Uuid::new_v4(), "127.0.0.1:5701".parse().unwrap());
        assert!(config.check_quorum(&[member]));

        let other_member = Member::new(uuid::Uuid::new_v4(), "127.0.0.1:5702".parse().unwrap());
        assert!(!config.check_quorum(&[other_member]));
    }

    #[test]
    fn test_quorum_config_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<QuorumConfig>();
    }

    #[test]
    fn test_quorum_config_clone() {
        let config = QuorumConfig::builder("test-*")
            .min_cluster_size(3)
            .quorum_type(QuorumType::Write)
            .build()
            .unwrap();

        let cloned = config.clone();
        assert_eq!(cloned.name(), config.name());
        assert_eq!(cloned.min_cluster_size(), config.min_cluster_size());
        assert_eq!(cloned.quorum_type(), config.quorum_type());
    }

    #[test]
    fn test_client_config_with_quorum() {
        let quorum = QuorumConfig::builder("important-*")
            .min_cluster_size(3)
            .quorum_type(QuorumType::ReadWrite)
            .build()
            .unwrap();

        let config = ClientConfig::builder()
            .add_quorum_config(quorum)
            .build()
            .unwrap();

        assert_eq!(config.quorum_configs().len(), 1);
        assert!(config.find_quorum_config("important-data").is_some());
        assert!(config.find_quorum_config("other-data").is_none());
    }

    #[test]
    fn test_client_config_multiple_quorum_configs() {
        let quorum1 = QuorumConfig::builder("critical-*")
            .min_cluster_size(5)
            .build()
            .unwrap();

        let quorum2 = QuorumConfig::builder("standard-*")
            .min_cluster_size(2)
            .build()
            .unwrap();

        let config = ClientConfig::builder()
            .add_quorum_config(quorum1)
            .add_quorum_config(quorum2)
            .build()
            .unwrap();

        assert_eq!(config.quorum_configs().len(), 2);

        let critical = config.find_quorum_config("critical-map");
        assert!(critical.is_some());
        assert_eq!(critical.unwrap().min_cluster_size(), 5);

        let standard = config.find_quorum_config("standard-map");
        assert!(standard.is_some());
        assert_eq!(standard.unwrap().min_cluster_size(), 2);
    }

    #[test]
    fn test_client_config_default_no_quorum() {
        let config = ClientConfig::default();
        assert!(config.quorum_configs().is_empty());
    }

    #[test]
    fn test_quorum_type_is_copy() {
        fn assert_copy<T: Copy>() {}
        assert_copy::<QuorumType>();
    }

    #[test]
    fn test_client_config_with_user_code_deployment() {
        use crate::deployment::{ClassDefinition, UserCodeDeploymentConfig};

        // Create mock bytecode with valid Java class magic number
        let mut bytecode = vec![0xCA, 0xFE, 0xBA, 0xBE];
        bytecode.extend_from_slice(&[0x00, 0x00, 0x00, 0x34, 0x00, 0x01]);

        let class_def = ClassDefinition::new("com.example.MyProcessor", bytecode);
        let ucd_config = UserCodeDeploymentConfig::builder()
            .enabled(true)
            .add_class(class_def)
            .build()
            .unwrap();

        let config = ClientConfig::builder()
            .user_code_deployment(ucd_config)
            .build()
            .unwrap();

        assert!(config.user_code_deployment().is_some());
        let ucd = config.user_code_deployment().unwrap();
        assert!(ucd.enabled());
        assert_eq!(ucd.class_definitions().len(), 1);
    }

    #[test]
    fn test_client_config_default_no_user_code_deployment() {
        let config = ClientConfig::default();
        assert!(config.user_code_deployment().is_none());
    }

    #[test]
    fn test_wan_config_clone() {
        let target = WanTargetClusterConfigBuilder::new("remote")
            .add_endpoint("10.0.0.1:5701".parse().unwrap())
            .build()
            .unwrap();

        let config = WanReplicationConfigBuilder::new("wan")
            .add_target_cluster(target)
            .build()
            .unwrap();

        let cloned = config.clone();
        assert_eq!(cloned.name(), config.name());
        assert_eq!(
            cloned.target_clusters().len(),
            config.target_clusters().len()
        );
    }
}
