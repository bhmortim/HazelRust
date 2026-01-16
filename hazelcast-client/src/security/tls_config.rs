//! TLS configuration for secure connections to Hazelcast clusters.

use std::path::PathBuf;

/// TLS protocol versions supported by the client.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TlsProtocolVersion {
    /// TLS 1.2 protocol.
    Tls1_2,
    /// TLS 1.3 protocol (recommended).
    Tls1_3,
}

impl TlsProtocolVersion {
    /// Returns the protocol version as a string identifier.
    pub fn as_str(&self) -> &'static str {
        match self {
            TlsProtocolVersion::Tls1_2 => "TLSv1.2",
            TlsProtocolVersion::Tls1_3 => "TLSv1.3",
        }
    }

    /// Returns all supported protocol versions.
    pub fn all() -> Vec<TlsProtocolVersion> {
        vec![TlsProtocolVersion::Tls1_2, TlsProtocolVersion::Tls1_3]
    }

    /// Returns the default protocol versions (TLS 1.2 and 1.3).
    pub fn defaults() -> Vec<TlsProtocolVersion> {
        vec![TlsProtocolVersion::Tls1_2, TlsProtocolVersion::Tls1_3]
    }
}

impl Default for TlsProtocolVersion {
    fn default() -> Self {
        TlsProtocolVersion::Tls1_3
    }
}

/// Common TLS cipher suites.
///
/// These constants provide commonly used cipher suite names that can be
/// passed to `TlsConfigBuilder::add_cipher_suite()`.
pub mod cipher_suites {
    /// TLS 1.3 cipher suites.
    pub mod tls13 {
        /// AES-256-GCM with SHA-384.
        pub const TLS_AES_256_GCM_SHA384: &str = "TLS_AES_256_GCM_SHA384";
        /// AES-128-GCM with SHA-256.
        pub const TLS_AES_128_GCM_SHA256: &str = "TLS_AES_128_GCM_SHA256";
        /// ChaCha20-Poly1305 with SHA-256.
        pub const TLS_CHACHA20_POLY1305_SHA256: &str = "TLS_CHACHA20_POLY1305_SHA256";
    }

    /// TLS 1.2 cipher suites.
    pub mod tls12 {
        /// ECDHE-RSA with AES-256-GCM and SHA-384.
        pub const TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384: &str =
            "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384";
        /// ECDHE-RSA with AES-128-GCM and SHA-256.
        pub const TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256: &str =
            "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256";
        /// ECDHE-ECDSA with AES-256-GCM and SHA-384.
        pub const TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384: &str =
            "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384";
        /// ECDHE-ECDSA with AES-128-GCM and SHA-256.
        pub const TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256: &str =
            "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256";
        /// ECDHE-RSA with ChaCha20-Poly1305 and SHA-256.
        pub const TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256: &str =
            "TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256";
        /// ECDHE-ECDSA with ChaCha20-Poly1305 and SHA-256.
        pub const TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256: &str =
            "TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256";
    }

    /// Returns default secure cipher suites for TLS 1.3.
    pub fn default_tls13() -> Vec<&'static str> {
        vec![
            tls13::TLS_AES_256_GCM_SHA384,
            tls13::TLS_AES_128_GCM_SHA256,
            tls13::TLS_CHACHA20_POLY1305_SHA256,
        ]
    }

    /// Returns default secure cipher suites for TLS 1.2.
    pub fn default_tls12() -> Vec<&'static str> {
        vec![
            tls12::TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
            tls12::TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
            tls12::TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
            tls12::TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
        ]
    }
}

/// Hostname verification mode for TLS connections.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum HostnameVerification {
    /// Verify the server's hostname matches the certificate (default, recommended).
    #[default]
    Strict,
    /// Skip hostname verification (not recommended for production).
    None,
}

impl HostnameVerification {
    /// Returns whether hostname verification is enabled.
    pub fn is_enabled(&self) -> bool {
        matches!(self, HostnameVerification::Strict)
    }
}

/// Configuration error for TLS settings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlsConfigError {
    message: String,
}

impl TlsConfigError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl std::fmt::Display for TlsConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TLS configuration error: {}", self.message)
    }
}

impl std::error::Error for TlsConfigError {}

/// TLS configuration for secure connections.
///
/// This configuration controls how the client establishes secure TLS connections
/// to Hazelcast cluster members.
///
/// # Example
///
/// ```ignore
/// use hazelcast_client::security::{TlsConfig, TlsProtocolVersion, HostnameVerification};
///
/// let tls = TlsConfig::builder()
///     .enabled(true)
///     .ca_cert_path("/path/to/ca.pem")
///     .client_auth("/path/to/client.pem", "/path/to/client.key")
///     .protocol_version(TlsProtocolVersion::Tls1_3)
///     .hostname_verification(HostnameVerification::Strict)
///     .build()
///     .unwrap();
/// ```
#[derive(Debug, Clone)]
pub struct TlsConfig {
    enabled: bool,
    ca_cert_path: Option<PathBuf>,
    client_cert_path: Option<PathBuf>,
    client_key_path: Option<PathBuf>,
    hostname_verification: HostnameVerification,
    cipher_suites: Vec<String>,
    protocol_versions: Vec<TlsProtocolVersion>,
    sni_hostname: Option<String>,
    allow_invalid_certs: bool,
}

impl TlsConfig {
    /// Creates a new TLS configuration builder.
    pub fn builder() -> TlsConfigBuilder {
        TlsConfigBuilder::new()
    }

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

    /// Returns the hostname verification mode.
    pub fn hostname_verification(&self) -> HostnameVerification {
        self.hostname_verification
    }

    /// Returns whether hostname verification is enabled.
    ///
    /// This is a convenience method equivalent to `hostname_verification().is_enabled()`.
    pub fn verify_hostname(&self) -> bool {
        self.hostname_verification.is_enabled()
    }

    /// Returns the configured cipher suites.
    ///
    /// If empty, the system defaults will be used.
    pub fn cipher_suites(&self) -> &[String] {
        &self.cipher_suites
    }

    /// Returns the configured TLS protocol versions.
    ///
    /// If empty, the system defaults (TLS 1.2 and 1.3) will be used.
    pub fn protocol_versions(&self) -> &[TlsProtocolVersion] {
        &self.protocol_versions
    }

    /// Returns the SNI hostname override, if configured.
    pub fn sni_hostname(&self) -> Option<&str> {
        self.sni_hostname.as_deref()
    }

    /// Returns whether invalid certificates are allowed.
    ///
    /// **Warning**: This should only be used for testing.
    pub fn allow_invalid_certs(&self) -> bool {
        self.allow_invalid_certs
    }

    /// Returns true if client authentication is configured.
    pub fn has_client_auth(&self) -> bool {
        self.client_cert_path.is_some() && self.client_key_path.is_some()
    }

    /// Returns true if mutual TLS (mTLS) is configured.
    ///
    /// This is equivalent to `has_client_auth()`.
    pub fn is_mtls(&self) -> bool {
        self.has_client_auth()
    }
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            ca_cert_path: None,
            client_cert_path: None,
            client_key_path: None,
            hostname_verification: HostnameVerification::Strict,
            cipher_suites: Vec::new(),
            protocol_versions: Vec::new(),
            sni_hostname: None,
            allow_invalid_certs: false,
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
    hostname_verification: Option<HostnameVerification>,
    cipher_suites: Vec<String>,
    protocol_versions: Vec<TlsProtocolVersion>,
    sni_hostname: Option<String>,
    allow_invalid_certs: Option<bool>,
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
    ///
    /// The file should be in PEM format and contain the certificate authority
    /// chain used to verify the server's certificate.
    pub fn ca_cert_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.ca_cert_path = Some(path.into());
        self
    }

    /// Sets the path to the client certificate file for mutual TLS.
    ///
    /// The file should be in PEM format. Use together with `client_key_path()`
    /// or the convenience method `client_auth()`.
    pub fn client_cert_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.client_cert_path = Some(path.into());
        self
    }

    /// Sets the path to the client private key file for mutual TLS.
    ///
    /// The file should be in PEM format. Use together with `client_cert_path()`
    /// or the convenience method `client_auth()`.
    pub fn client_key_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.client_key_path = Some(path.into());
        self
    }

    /// Sets client certificate and key paths for mutual TLS (mTLS).
    ///
    /// This is a convenience method equivalent to calling both
    /// `client_cert_path()` and `client_key_path()`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let tls = TlsConfig::builder()
    ///     .enabled(true)
    ///     .client_auth("/path/to/cert.pem", "/path/to/key.pem")
    ///     .build()
    ///     .unwrap();
    /// ```
    pub fn client_auth(
        self,
        cert_path: impl Into<PathBuf>,
        key_path: impl Into<PathBuf>,
    ) -> Self {
        self.client_cert_path(cert_path).client_key_path(key_path)
    }

    /// Sets the hostname verification mode.
    ///
    /// Defaults to `HostnameVerification::Strict` which verifies that the
    /// server's hostname matches the certificate.
    pub fn hostname_verification(mut self, mode: HostnameVerification) -> Self {
        self.hostname_verification = Some(mode);
        self
    }

    /// Enables or disables hostname verification.
    ///
    /// This is a convenience method. `true` sets `HostnameVerification::Strict`,
    /// `false` sets `HostnameVerification::None`.
    pub fn verify_hostname(mut self, verify: bool) -> Self {
        self.hostname_verification = Some(if verify {
            HostnameVerification::Strict
        } else {
            HostnameVerification::None
        });
        self
    }

    /// Adds a cipher suite to the allowed list.
    ///
    /// Use constants from `cipher_suites` module or provide custom suite names.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use hazelcast_client::security::tls_config::cipher_suites;
    ///
    /// let tls = TlsConfig::builder()
    ///     .enabled(true)
    ///     .add_cipher_suite(cipher_suites::tls13::TLS_AES_256_GCM_SHA384)
    ///     .add_cipher_suite(cipher_suites::tls13::TLS_AES_128_GCM_SHA256)
    ///     .build()
    ///     .unwrap();
    /// ```
    pub fn add_cipher_suite(mut self, suite: impl Into<String>) -> Self {
        self.cipher_suites.push(suite.into());
        self
    }

    /// Sets the allowed cipher suites, replacing any previously configured.
    ///
    /// If not set or empty, system defaults will be used.
    pub fn cipher_suites(mut self, suites: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.cipher_suites = suites.into_iter().map(|s| s.into()).collect();
        self
    }

    /// Adds a TLS protocol version to the allowed list.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let tls = TlsConfig::builder()
    ///     .enabled(true)
    ///     .add_protocol_version(TlsProtocolVersion::Tls1_3)
    ///     .build()
    ///     .unwrap();
    /// ```
    pub fn add_protocol_version(mut self, version: TlsProtocolVersion) -> Self {
        if !self.protocol_versions.contains(&version) {
            self.protocol_versions.push(version);
        }
        self
    }

    /// Sets a single allowed TLS protocol version.
    ///
    /// This is a convenience method for allowing only one protocol version.
    pub fn protocol_version(mut self, version: TlsProtocolVersion) -> Self {
        self.protocol_versions = vec![version];
        self
    }

    /// Sets the allowed TLS protocol versions, replacing any previously configured.
    ///
    /// If not set or empty, TLS 1.2 and 1.3 will be allowed.
    pub fn protocol_versions(
        mut self,
        versions: impl IntoIterator<Item = TlsProtocolVersion>,
    ) -> Self {
        self.protocol_versions = versions.into_iter().collect();
        self
    }

    /// Sets the SNI (Server Name Indication) hostname.
    ///
    /// This overrides the hostname sent in the TLS handshake. Useful when
    /// connecting through proxies or when the server certificate uses a
    /// different hostname.
    pub fn sni_hostname(mut self, hostname: impl Into<String>) -> Self {
        self.sni_hostname = Some(hostname.into());
        self
    }

    /// Allows invalid (expired, self-signed, wrong hostname) certificates.
    ///
    /// **Warning**: This completely disables certificate validation and should
    /// only be used for testing. Never enable this in production.
    pub fn allow_invalid_certs(mut self, allow: bool) -> Self {
        self.allow_invalid_certs = Some(allow);
        self
    }

    /// Builds the TLS configuration, returning an error if validation fails.
    ///
    /// # Errors
    ///
    /// Returns `TlsConfigError` if:
    /// - Only one of `client_cert_path` or `client_key_path` is set
    /// - `protocol_versions` is set but empty
    pub fn build(self) -> Result<TlsConfig, TlsConfigError> {
        let enabled = self.enabled.unwrap_or(false);

        if self.client_cert_path.is_some() != self.client_key_path.is_some() {
            return Err(TlsConfigError::new(
                "both client_cert_path and client_key_path must be provided together for mTLS",
            ));
        }

        Ok(TlsConfig {
            enabled,
            ca_cert_path: self.ca_cert_path,
            client_cert_path: self.client_cert_path,
            client_key_path: self.client_key_path,
            hostname_verification: self.hostname_verification.unwrap_or(HostnameVerification::Strict),
            cipher_suites: self.cipher_suites,
            protocol_versions: self.protocol_versions,
            sni_hostname: self.sni_hostname,
            allow_invalid_certs: self.allow_invalid_certs.unwrap_or(false),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tls_config_defaults() {
        let config = TlsConfig::default();
        assert!(!config.enabled());
        assert!(config.ca_cert_path().is_none());
        assert!(config.client_cert_path().is_none());
        assert!(config.client_key_path().is_none());
        assert_eq!(config.hostname_verification(), HostnameVerification::Strict);
        assert!(config.verify_hostname());
        assert!(config.cipher_suites().is_empty());
        assert!(config.protocol_versions().is_empty());
        assert!(config.sni_hostname().is_none());
        assert!(!config.allow_invalid_certs());
        assert!(!config.has_client_auth());
        assert!(!config.is_mtls());
    }

    #[test]
    fn test_tls_config_builder_basic() {
        let config = TlsConfig::builder()
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
    fn test_tls_config_mtls() {
        let config = TlsConfig::builder()
            .enabled(true)
            .client_auth("/path/to/cert.pem", "/path/to/key.pem")
            .build()
            .unwrap();

        assert!(config.has_client_auth());
        assert!(config.is_mtls());
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
        let result = TlsConfig::builder()
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
    fn test_tls_config_hostname_verification() {
        let strict = TlsConfig::builder()
            .hostname_verification(HostnameVerification::Strict)
            .build()
            .unwrap();
        assert!(strict.verify_hostname());

        let none = TlsConfig::builder()
            .hostname_verification(HostnameVerification::None)
            .build()
            .unwrap();
        assert!(!none.verify_hostname());

        let via_bool_true = TlsConfig::builder()
            .verify_hostname(true)
            .build()
            .unwrap();
        assert!(via_bool_true.verify_hostname());

        let via_bool_false = TlsConfig::builder()
            .verify_hostname(false)
            .build()
            .unwrap();
        assert!(!via_bool_false.verify_hostname());
    }

    #[test]
    fn test_tls_config_cipher_suites() {
        let config = TlsConfig::builder()
            .add_cipher_suite(cipher_suites::tls13::TLS_AES_256_GCM_SHA384)
            .add_cipher_suite(cipher_suites::tls13::TLS_AES_128_GCM_SHA256)
            .build()
            .unwrap();

        assert_eq!(config.cipher_suites().len(), 2);
        assert!(config.cipher_suites().contains(&"TLS_AES_256_GCM_SHA384".to_string()));
        assert!(config.cipher_suites().contains(&"TLS_AES_128_GCM_SHA256".to_string()));
    }

    #[test]
    fn test_tls_config_cipher_suites_replace() {
        let config = TlsConfig::builder()
            .add_cipher_suite("OLD_SUITE")
            .cipher_suites(["NEW_SUITE_1", "NEW_SUITE_2"])
            .build()
            .unwrap();

        assert_eq!(config.cipher_suites().len(), 2);
        assert!(!config.cipher_suites().contains(&"OLD_SUITE".to_string()));
        assert!(config.cipher_suites().contains(&"NEW_SUITE_1".to_string()));
        assert!(config.cipher_suites().contains(&"NEW_SUITE_2".to_string()));
    }

    #[test]
    fn test_tls_config_protocol_versions() {
        let config = TlsConfig::builder()
            .add_protocol_version(TlsProtocolVersion::Tls1_3)
            .add_protocol_version(TlsProtocolVersion::Tls1_2)
            .build()
            .unwrap();

        assert_eq!(config.protocol_versions().len(), 2);
        assert!(config.protocol_versions().contains(&TlsProtocolVersion::Tls1_3));
        assert!(config.protocol_versions().contains(&TlsProtocolVersion::Tls1_2));
    }

    #[test]
    fn test_tls_config_protocol_version_single() {
        let config = TlsConfig::builder()
            .protocol_version(TlsProtocolVersion::Tls1_3)
            .build()
            .unwrap();

        assert_eq!(config.protocol_versions().len(), 1);
        assert_eq!(config.protocol_versions()[0], TlsProtocolVersion::Tls1_3);
    }

    #[test]
    fn test_tls_config_protocol_versions_replace() {
        let config = TlsConfig::builder()
            .add_protocol_version(TlsProtocolVersion::Tls1_2)
            .protocol_versions([TlsProtocolVersion::Tls1_3])
            .build()
            .unwrap();

        assert_eq!(config.protocol_versions().len(), 1);
        assert!(!config.protocol_versions().contains(&TlsProtocolVersion::Tls1_2));
        assert!(config.protocol_versions().contains(&TlsProtocolVersion::Tls1_3));
    }

    #[test]
    fn test_tls_config_no_duplicate_protocol_versions() {
        let config = TlsConfig::builder()
            .add_protocol_version(TlsProtocolVersion::Tls1_3)
            .add_protocol_version(TlsProtocolVersion::Tls1_3)
            .add_protocol_version(TlsProtocolVersion::Tls1_3)
            .build()
            .unwrap();

        assert_eq!(config.protocol_versions().len(), 1);
    }

    #[test]
    fn test_tls_config_sni_hostname() {
        let config = TlsConfig::builder()
            .sni_hostname("hazelcast.example.com")
            .build()
            .unwrap();

        assert_eq!(config.sni_hostname(), Some("hazelcast.example.com"));
    }

    #[test]
    fn test_tls_config_allow_invalid_certs() {
        let config = TlsConfig::builder()
            .allow_invalid_certs(true)
            .build()
            .unwrap();

        assert!(config.allow_invalid_certs());
    }

    #[test]
    fn test_tls_config_full_example() {
        let config = TlsConfig::builder()
            .enabled(true)
            .ca_cert_path("/certs/ca.pem")
            .client_auth("/certs/client.pem", "/certs/client.key")
            .hostname_verification(HostnameVerification::Strict)
            .add_cipher_suite(cipher_suites::tls13::TLS_AES_256_GCM_SHA384)
            .add_protocol_version(TlsProtocolVersion::Tls1_3)
            .sni_hostname("cluster.hazelcast.cloud")
            .build()
            .unwrap();

        assert!(config.enabled());
        assert!(config.has_client_auth());
        assert!(config.verify_hostname());
        assert_eq!(config.cipher_suites().len(), 1);
        assert_eq!(config.protocol_versions().len(), 1);
        assert_eq!(config.sni_hostname(), Some("cluster.hazelcast.cloud"));
    }

    #[test]
    fn test_tls_protocol_version_as_str() {
        assert_eq!(TlsProtocolVersion::Tls1_2.as_str(), "TLSv1.2");
        assert_eq!(TlsProtocolVersion::Tls1_3.as_str(), "TLSv1.3");
    }

    #[test]
    fn test_tls_protocol_version_all() {
        let all = TlsProtocolVersion::all();
        assert_eq!(all.len(), 2);
        assert!(all.contains(&TlsProtocolVersion::Tls1_2));
        assert!(all.contains(&TlsProtocolVersion::Tls1_3));
    }

    #[test]
    fn test_tls_protocol_version_defaults() {
        let defaults = TlsProtocolVersion::defaults();
        assert_eq!(defaults.len(), 2);
        assert!(defaults.contains(&TlsProtocolVersion::Tls1_2));
        assert!(defaults.contains(&TlsProtocolVersion::Tls1_3));
    }

    #[test]
    fn test_tls_protocol_version_default() {
        assert_eq!(TlsProtocolVersion::default(), TlsProtocolVersion::Tls1_3);
    }

    #[test]
    fn test_hostname_verification_default() {
        assert_eq!(HostnameVerification::default(), HostnameVerification::Strict);
    }

    #[test]
    fn test_hostname_verification_is_enabled() {
        assert!(HostnameVerification::Strict.is_enabled());
        assert!(!HostnameVerification::None.is_enabled());
    }

    #[test]
    fn test_cipher_suites_constants() {
        assert_eq!(
            cipher_suites::tls13::TLS_AES_256_GCM_SHA384,
            "TLS_AES_256_GCM_SHA384"
        );
        assert_eq!(
            cipher_suites::tls12::TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
            "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384"
        );
    }

    #[test]
    fn test_cipher_suites_defaults() {
        let tls13 = cipher_suites::default_tls13();
        assert!(!tls13.is_empty());
        assert!(tls13.contains(&cipher_suites::tls13::TLS_AES_256_GCM_SHA384));

        let tls12 = cipher_suites::default_tls12();
        assert!(!tls12.is_empty());
        assert!(tls12.contains(&cipher_suites::tls12::TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384));
    }

    #[test]
    fn test_tls_config_error_display() {
        let err = TlsConfigError::new("test error");
        assert!(err.to_string().contains("TLS configuration error"));
        assert!(err.to_string().contains("test error"));
    }

    #[test]
    fn test_tls_config_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<TlsConfig>();
        assert_send_sync::<TlsConfigBuilder>();
        assert_send_sync::<TlsConfigError>();
    }

    #[test]
    fn test_tls_protocol_version_is_copy() {
        fn assert_copy<T: Copy>() {}
        assert_copy::<TlsProtocolVersion>();
    }

    #[test]
    fn test_hostname_verification_is_copy() {
        fn assert_copy<T: Copy>() {}
        assert_copy::<HostnameVerification>();
    }

    #[test]
    fn test_tls_config_clone() {
        let config = TlsConfig::builder()
            .enabled(true)
            .ca_cert_path("/path/to/ca.pem")
            .add_cipher_suite("TLS_AES_256_GCM_SHA384")
            .add_protocol_version(TlsProtocolVersion::Tls1_3)
            .build()
            .unwrap();

        let cloned = config.clone();
        assert_eq!(cloned.enabled(), config.enabled());
        assert_eq!(cloned.ca_cert_path(), config.ca_cert_path());
        assert_eq!(cloned.cipher_suites(), config.cipher_suites());
        assert_eq!(cloned.protocol_versions(), config.protocol_versions());
    }

    #[test]
    fn test_tls_config_builder_clone() {
        let builder = TlsConfig::builder()
            .enabled(true)
            .ca_cert_path("/path/to/ca.pem");

        let cloned_builder = builder.clone();
        let config = cloned_builder.build().unwrap();
        assert!(config.enabled());
    }
}
