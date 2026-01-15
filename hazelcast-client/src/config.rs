//! Client configuration types and builders.

use std::net::SocketAddr;
use std::time::Duration;

/// Default cluster name.
const DEFAULT_CLUSTER_NAME: &str = "dev";
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
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            addresses: vec!["127.0.0.1:5701".parse().unwrap()],
            connection_timeout: DEFAULT_CONNECTION_TIMEOUT,
            heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL,
        }
    }
}

/// Builder for `NetworkConfig`.
#[derive(Debug, Clone, Default)]
pub struct NetworkConfigBuilder {
    addresses: Vec<SocketAddr>,
    connection_timeout: Option<Duration>,
    heartbeat_interval: Option<Duration>,
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

    /// Builds the network configuration.
    pub fn build(self) -> NetworkConfig {
        let addresses = if self.addresses.is_empty() {
            vec!["127.0.0.1:5701".parse().unwrap()]
        } else {
            self.addresses
        };

        NetworkConfig {
            addresses,
            connection_timeout: self.connection_timeout.unwrap_or(DEFAULT_CONNECTION_TIMEOUT),
            heartbeat_interval: self.heartbeat_interval.unwrap_or(DEFAULT_HEARTBEAT_INTERVAL),
        }
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

/// Security configuration for authentication.
#[derive(Debug, Clone, Default)]
pub struct SecurityConfig {
    username: Option<String>,
    password: Option<String>,
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

    /// Returns true if credentials are configured.
    pub fn has_credentials(&self) -> bool {
        self.username.is_some() && self.password.is_some()
    }
}

/// Builder for `SecurityConfig`.
#[derive(Debug, Clone, Default)]
pub struct SecurityConfigBuilder {
    username: Option<String>,
    password: Option<String>,
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

    /// Builds the security configuration, returning an error if validation fails.
    pub fn build(self) -> Result<SecurityConfig, ConfigError> {
        if self.username.is_some() != self.password.is_some() {
            return Err(ConfigError::new(
                "both username and password must be provided together",
            ));
        }

        Ok(SecurityConfig {
            username: self.username,
            password: self.password,
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

    /// Builds the client configuration, returning an error if validation fails.
    pub fn build(self) -> Result<ClientConfig, ConfigError> {
        let cluster_name = self
            .cluster_name
            .unwrap_or_else(|| DEFAULT_CLUSTER_NAME.to_string());

        if cluster_name.is_empty() {
            return Err(ConfigError::new("cluster_name must not be empty"));
        }

        let network = self.network.build();
        let retry = self.retry.build()?;
        let security = self.security.build()?;

        Ok(ClientConfig {
            cluster_name,
            network,
            retry,
            security,
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
            .build();

        assert_eq!(config.addresses(), &[addr]);
        assert_eq!(config.connection_timeout(), Duration::from_secs(15));
        assert_eq!(config.heartbeat_interval(), Duration::from_secs(10));
    }

    #[test]
    fn test_network_config_defaults() {
        let config = NetworkConfigBuilder::new().build();
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
}
