//! Client configuration types and builders.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use crate::cache::NearCacheConfig;

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
    tls: TlsConfig,
    ws_url: Option<String>,
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
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            addresses: vec!["127.0.0.1:5701".parse().unwrap()],
            connection_timeout: DEFAULT_CONNECTION_TIMEOUT,
            heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL,
            tls: TlsConfig::default(),
            ws_url: None,
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
#[derive(Debug, Clone, Default)]
pub struct SecurityConfig {
    username: Option<String>,
    password: Option<String>,
    token: Option<String>,
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
        self.has_credentials() || self.has_token()
    }
}

/// Builder for `SecurityConfig`.
#[derive(Debug, Clone, Default)]
pub struct SecurityConfigBuilder {
    username: Option<String>,
    password: Option<String>,
    token: Option<String>,
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

        Ok(ClientConfig {
            cluster_name,
            network,
            retry,
            security,
            near_caches: self.near_caches,
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
}
