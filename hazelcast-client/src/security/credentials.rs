//! Cloud credential providers for dynamic authentication.

use super::Credentials;

/// Errors that can occur when fetching credentials from a provider.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CredentialError {
    /// Credentials not found in expected location.
    NotFound(String),
    /// Failed to parse or decode credentials.
    ParseError(String),
    /// Network error fetching credentials from metadata service.
    NetworkError(String),
    /// Provider is not available in current environment.
    ProviderUnavailable(String),
    /// Credentials have expired.
    Expired(String),
    /// Generic error.
    Other(String),
}

impl std::fmt::Display for CredentialError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CredentialError::NotFound(msg) => write!(f, "credentials not found: {}", msg),
            CredentialError::ParseError(msg) => write!(f, "credential parse error: {}", msg),
            CredentialError::NetworkError(msg) => {
                write!(f, "network error fetching credentials: {}", msg)
            }
            CredentialError::ProviderUnavailable(msg) => {
                write!(f, "credential provider unavailable: {}", msg)
            }
            CredentialError::Expired(msg) => write!(f, "credentials expired: {}", msg),
            CredentialError::Other(msg) => write!(f, "credential error: {}", msg),
        }
    }
}

impl std::error::Error for CredentialError {}

/// Trait for dynamically fetching credentials from various sources.
///
/// Credential providers are used to obtain authentication credentials
/// at runtime from cloud metadata services, environment variables,
/// or other dynamic sources.
#[async_trait::async_trait]
pub trait CredentialProvider: Send + Sync {
    /// Fetches credentials from the provider.
    ///
    /// This method may make network calls to metadata services
    /// or read from environment/files.
    async fn get_credentials(&self) -> Result<Credentials, CredentialError>;

    /// Returns the name of this credential provider for logging.
    fn provider_name(&self) -> &str;

    /// Returns true if this provider is available in the current environment.
    ///
    /// For example, AWS provider checks for IAM role or environment variables,
    /// Kubernetes provider checks for mounted service account token.
    async fn is_available(&self) -> bool;
}

impl std::fmt::Debug for dyn CredentialProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CredentialProvider({})", self.provider_name())
    }
}

/// Simple credential provider that reads from environment variables.
///
/// Supports:
/// - `HAZELCAST_USERNAME` and `HAZELCAST_PASSWORD` for username/password auth
/// - `HAZELCAST_TOKEN` for token-based auth
#[derive(Debug, Clone)]
pub struct EnvironmentCredentialProvider {
    prefix: String,
}

impl EnvironmentCredentialProvider {
    /// Creates a new environment credential provider with the default prefix "HAZELCAST".
    pub fn new() -> Self {
        Self {
            prefix: "HAZELCAST".to_string(),
        }
    }

    /// Creates an environment credential provider with a custom prefix.
    ///
    /// The provider will look for `{PREFIX}_USERNAME`, `{PREFIX}_PASSWORD`,
    /// and `{PREFIX}_TOKEN` environment variables.
    pub fn with_prefix(prefix: impl Into<String>) -> Self {
        Self {
            prefix: prefix.into(),
        }
    }

    /// Returns the configured prefix.
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    fn username_var(&self) -> String {
        format!("{}_USERNAME", self.prefix)
    }

    fn password_var(&self) -> String {
        format!("{}_PASSWORD", self.prefix)
    }

    fn token_var(&self) -> String {
        format!("{}_TOKEN", self.prefix)
    }
}

impl Default for EnvironmentCredentialProvider {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl CredentialProvider for EnvironmentCredentialProvider {
    async fn get_credentials(&self) -> Result<Credentials, CredentialError> {
        if let Ok(token) = std::env::var(self.token_var()) {
            return Ok(Credentials::Token(token));
        }

        let username = std::env::var(self.username_var());
        let password = std::env::var(self.password_var());

        match (username, password) {
            (Ok(u), Ok(p)) => Ok(Credentials::UsernamePassword {
                username: u,
                password: p,
            }),
            (Ok(_), Err(_)) => Err(CredentialError::NotFound(format!(
                "{} is set but {} is missing",
                self.username_var(),
                self.password_var()
            ))),
            (Err(_), Ok(_)) => Err(CredentialError::NotFound(format!(
                "{} is set but {} is missing",
                self.password_var(),
                self.username_var()
            ))),
            (Err(_), Err(_)) => Err(CredentialError::NotFound(format!(
                "no credentials found in environment (checked {}, {}, {})",
                self.username_var(),
                self.password_var(),
                self.token_var()
            ))),
        }
    }

    fn provider_name(&self) -> &str {
        "environment"
    }

    async fn is_available(&self) -> bool {
        std::env::var(self.token_var()).is_ok()
            || (std::env::var(self.username_var()).is_ok()
                && std::env::var(self.password_var()).is_ok())
    }
}

/// AWS credential provider supporting:
/// - Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN)
/// - IAM instance profile (EC2 metadata service at 169.254.169.254)
/// - ECS task role (via AWS_CONTAINER_CREDENTIALS_RELATIVE_URI)
#[cfg(feature = "aws")]
#[derive(Debug, Clone)]
pub struct AwsCredentialProvider {
    region: Option<String>,
    metadata_endpoint: Option<String>,
}

#[cfg(feature = "aws")]
impl AwsCredentialProvider {
    /// Default EC2 metadata service endpoint.
    pub const DEFAULT_METADATA_ENDPOINT: &'static str = "http://169.254.169.254";

    /// Creates a new AWS credential provider.
    pub fn new() -> Self {
        Self {
            region: None,
            metadata_endpoint: None,
        }
    }

    /// Sets the AWS region for logging/identification.
    pub fn with_region(mut self, region: impl Into<String>) -> Self {
        self.region = Some(region.into());
        self
    }

    /// Sets a custom metadata endpoint (for testing).
    pub fn with_metadata_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.metadata_endpoint = Some(endpoint.into());
        self
    }

    /// Returns the configured region.
    pub fn region(&self) -> Option<&str> {
        self.region.as_deref()
    }

    /// Returns the metadata endpoint.
    pub fn metadata_endpoint(&self) -> &str {
        self.metadata_endpoint
            .as_deref()
            .unwrap_or(Self::DEFAULT_METADATA_ENDPOINT)
    }

    fn try_from_env(&self) -> Option<Credentials> {
        let access_key = std::env::var("AWS_ACCESS_KEY_ID").ok()?;
        let secret_key = std::env::var("AWS_SECRET_ACCESS_KEY").ok()?;
        let session_token = std::env::var("AWS_SESSION_TOKEN").ok();

        let mut attrs = super::CustomCredentials::new("aws");
        attrs = attrs.with_attribute("access_key_id", access_key.into_bytes());
        attrs = attrs.with_attribute("secret_access_key", secret_key.into_bytes());
        if let Some(token) = session_token {
            attrs = attrs.with_attribute("session_token", token.into_bytes());
        }
        if let Some(ref region) = self.region {
            attrs = attrs.with_attribute("region", region.clone().into_bytes());
        }

        Some(Credentials::Custom(attrs))
    }

    async fn try_from_metadata(&self) -> Result<Credentials, CredentialError> {
        Err(CredentialError::NetworkError(
            "HTTP client not configured for metadata service".to_string(),
        ))
    }
}

#[cfg(feature = "aws")]
impl Default for AwsCredentialProvider {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(feature = "aws")]
#[async_trait::async_trait]
impl CredentialProvider for AwsCredentialProvider {
    async fn get_credentials(&self) -> Result<Credentials, CredentialError> {
        if let Some(creds) = self.try_from_env() {
            return Ok(creds);
        }

        self.try_from_metadata().await
    }

    fn provider_name(&self) -> &str {
        "aws"
    }

    async fn is_available(&self) -> bool {
        std::env::var("AWS_ACCESS_KEY_ID").is_ok()
            && std::env::var("AWS_SECRET_ACCESS_KEY").is_ok()
    }
}

/// Azure credential provider supporting:
/// - Environment variables (AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_TENANT_ID)
/// - Managed Identity (IMDS endpoint at 169.254.169.254)
#[cfg(feature = "azure")]
#[derive(Debug, Clone)]
pub struct AzureCredentialProvider {
    client_id: Option<String>,
    resource: String,
    imds_endpoint: Option<String>,
}

#[cfg(feature = "azure")]
impl AzureCredentialProvider {
    /// Default Azure IMDS endpoint.
    pub const DEFAULT_IMDS_ENDPOINT: &'static str = "http://169.254.169.254";

    /// Default resource/audience for Azure tokens.
    pub const DEFAULT_RESOURCE: &'static str = "https://management.azure.com/";

    /// Creates a new Azure credential provider.
    pub fn new() -> Self {
        Self {
            client_id: None,
            resource: Self::DEFAULT_RESOURCE.to_string(),
            imds_endpoint: None,
        }
    }

    /// Sets the client ID for service principal authentication.
    pub fn with_client_id(mut self, client_id: impl Into<String>) -> Self {
        self.client_id = Some(client_id.into());
        self
    }

    /// Sets the resource/audience for the token.
    pub fn with_resource(mut self, resource: impl Into<String>) -> Self {
        self.resource = resource.into();
        self
    }

    /// Sets a custom IMDS endpoint (for testing).
    pub fn with_imds_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.imds_endpoint = Some(endpoint.into());
        self
    }

    /// Returns the configured client ID.
    pub fn client_id(&self) -> Option<&str> {
        self.client_id.as_deref()
    }

    /// Returns the resource/audience.
    pub fn resource(&self) -> &str {
        &self.resource
    }

    /// Returns the IMDS endpoint.
    pub fn imds_endpoint(&self) -> &str {
        self.imds_endpoint
            .as_deref()
            .unwrap_or(Self::DEFAULT_IMDS_ENDPOINT)
    }

    fn try_from_env(&self) -> Option<Credentials> {
        let client_id = std::env::var("AZURE_CLIENT_ID").ok()?;
        let client_secret = std::env::var("AZURE_CLIENT_SECRET").ok()?;
        let tenant_id = std::env::var("AZURE_TENANT_ID").ok()?;

        let mut attrs = super::CustomCredentials::new("azure");
        attrs = attrs.with_attribute("client_id", client_id.into_bytes());
        attrs = attrs.with_attribute("client_secret", client_secret.into_bytes());
        attrs = attrs.with_attribute("tenant_id", tenant_id.into_bytes());
        attrs = attrs.with_attribute("resource", self.resource.clone().into_bytes());

        Some(Credentials::Custom(attrs))
    }

    async fn try_from_imds(&self) -> Result<Credentials, CredentialError> {
        Err(CredentialError::NetworkError(
            "HTTP client not configured for IMDS".to_string(),
        ))
    }
}

#[cfg(feature = "azure")]
impl Default for AzureCredentialProvider {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(feature = "azure")]
#[async_trait::async_trait]
impl CredentialProvider for AzureCredentialProvider {
    async fn get_credentials(&self) -> Result<Credentials, CredentialError> {
        if let Some(creds) = self.try_from_env() {
            return Ok(creds);
        }

        self.try_from_imds().await
    }

    fn provider_name(&self) -> &str {
        "azure"
    }

    async fn is_available(&self) -> bool {
        std::env::var("AZURE_CLIENT_ID").is_ok()
            && std::env::var("AZURE_CLIENT_SECRET").is_ok()
            && std::env::var("AZURE_TENANT_ID").is_ok()
    }
}

/// GCP credential provider supporting:
/// - Environment variable (GOOGLE_APPLICATION_CREDENTIALS pointing to service account JSON)
/// - Metadata server (169.254.169.254 for GCE/GKE)
#[cfg(feature = "gcp")]
#[derive(Debug, Clone)]
pub struct GcpCredentialProvider {
    service_account: Option<String>,
    scopes: Vec<String>,
    metadata_endpoint: Option<String>,
}

#[cfg(feature = "gcp")]
impl GcpCredentialProvider {
    /// Default GCP metadata endpoint.
    pub const DEFAULT_METADATA_ENDPOINT: &'static str = "http://169.254.169.254";

    /// Default scope for GCP credentials.
    pub const DEFAULT_SCOPE: &'static str = "https://www.googleapis.com/auth/cloud-platform";

    /// Creates a new GCP credential provider.
    pub fn new() -> Self {
        Self {
            service_account: None,
            scopes: vec![Self::DEFAULT_SCOPE.to_string()],
            metadata_endpoint: None,
        }
    }

    /// Sets the service account email for identification.
    pub fn with_service_account(mut self, email: impl Into<String>) -> Self {
        self.service_account = Some(email.into());
        self
    }

    /// Sets the scopes to request.
    pub fn with_scopes(mut self, scopes: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.scopes = scopes.into_iter().map(|s| s.into()).collect();
        self
    }

    /// Adds a scope to request.
    pub fn add_scope(mut self, scope: impl Into<String>) -> Self {
        self.scopes.push(scope.into());
        self
    }

    /// Sets a custom metadata endpoint (for testing).
    pub fn with_metadata_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.metadata_endpoint = Some(endpoint.into());
        self
    }

    /// Returns the service account email.
    pub fn service_account(&self) -> Option<&str> {
        self.service_account.as_deref()
    }

    /// Returns the configured scopes.
    pub fn scopes(&self) -> &[String] {
        &self.scopes
    }

    /// Returns the metadata endpoint.
    pub fn metadata_endpoint(&self) -> &str {
        self.metadata_endpoint
            .as_deref()
            .unwrap_or(Self::DEFAULT_METADATA_ENDPOINT)
    }

    fn try_from_env(&self) -> Result<Option<Credentials>, CredentialError> {
        let creds_path = match std::env::var("GOOGLE_APPLICATION_CREDENTIALS") {
            Ok(path) => path,
            Err(_) => return Ok(None),
        };

        let contents = std::fs::read_to_string(&creds_path).map_err(|e| {
            CredentialError::NotFound(format!(
                "failed to read GOOGLE_APPLICATION_CREDENTIALS file '{}': {}",
                creds_path, e
            ))
        })?;

        let mut attrs = super::CustomCredentials::new("gcp-service-account");
        attrs = attrs.with_attribute("credentials_json", contents.into_bytes());
        attrs = attrs.with_attribute("scopes", self.scopes.join(",").into_bytes());
        if let Some(ref sa) = self.service_account {
            attrs = attrs.with_attribute("service_account", sa.clone().into_bytes());
        }

        Ok(Some(Credentials::Custom(attrs)))
    }

    async fn try_from_metadata(&self) -> Result<Credentials, CredentialError> {
        Err(CredentialError::NetworkError(
            "HTTP client not configured for metadata service".to_string(),
        ))
    }
}

#[cfg(feature = "gcp")]
impl Default for GcpCredentialProvider {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(feature = "gcp")]
#[async_trait::async_trait]
impl CredentialProvider for GcpCredentialProvider {
    async fn get_credentials(&self) -> Result<Credentials, CredentialError> {
        if let Some(creds) = self.try_from_env()? {
            return Ok(creds);
        }

        self.try_from_metadata().await
    }

    fn provider_name(&self) -> &str {
        "gcp"
    }

    async fn is_available(&self) -> bool {
        if let Ok(path) = std::env::var("GOOGLE_APPLICATION_CREDENTIALS") {
            return std::path::Path::new(&path).exists();
        }
        false
    }
}

/// Kubernetes credential provider using mounted service account tokens.
///
/// Reads the JWT token from the standard location:
/// `/var/run/secrets/kubernetes.io/serviceaccount/token`
#[cfg(feature = "kubernetes")]
#[derive(Debug, Clone)]
pub struct KubernetesCredentialProvider {
    token_path: String,
    namespace: Option<String>,
}

#[cfg(feature = "kubernetes")]
impl KubernetesCredentialProvider {
    /// Default path to the service account token file.
    pub const DEFAULT_TOKEN_PATH: &'static str =
        "/var/run/secrets/kubernetes.io/serviceaccount/token";

    /// Default path to the namespace file.
    pub const DEFAULT_NAMESPACE_PATH: &'static str =
        "/var/run/secrets/kubernetes.io/serviceaccount/namespace";

    /// Creates a new Kubernetes credential provider with default paths.
    pub fn new() -> Self {
        Self {
            token_path: Self::DEFAULT_TOKEN_PATH.to_string(),
            namespace: None,
        }
    }

    /// Sets a custom token path.
    pub fn with_token_path(path: impl Into<String>) -> Self {
        Self {
            token_path: path.into(),
            namespace: None,
        }
    }

    /// Sets the namespace explicitly.
    pub fn with_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = Some(namespace.into());
        self
    }

    /// Returns the token path.
    pub fn token_path(&self) -> &str {
        &self.token_path
    }

    /// Returns the namespace if configured or read from file.
    pub fn namespace(&self) -> Option<&str> {
        self.namespace.as_deref()
    }

    fn read_namespace(&self) -> Option<String> {
        if let Some(ref ns) = self.namespace {
            return Some(ns.clone());
        }
        std::fs::read_to_string(Self::DEFAULT_NAMESPACE_PATH)
            .ok()
            .map(|s| s.trim().to_string())
    }
}

#[cfg(feature = "kubernetes")]
impl Default for KubernetesCredentialProvider {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(feature = "kubernetes")]
#[async_trait::async_trait]
impl CredentialProvider for KubernetesCredentialProvider {
    async fn get_credentials(&self) -> Result<Credentials, CredentialError> {
        let token = std::fs::read_to_string(&self.token_path).map_err(|e| {
            CredentialError::NotFound(format!(
                "failed to read service account token from '{}': {}",
                self.token_path, e
            ))
        })?;

        let token = token.trim().to_string();

        if token.is_empty() {
            return Err(CredentialError::ParseError(
                "service account token is empty".to_string(),
            ));
        }

        Ok(Credentials::Token(token))
    }

    fn provider_name(&self) -> &str {
        "kubernetes"
    }

    async fn is_available(&self) -> bool {
        std::path::Path::new(&self.token_path).exists()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use std::io::Write;

    #[test]
    fn test_credential_error_display() {
        let not_found = CredentialError::NotFound("token".to_string());
        assert!(not_found.to_string().contains("not found"));
        assert!(not_found.to_string().contains("token"));

        let parse_err = CredentialError::ParseError("invalid format".to_string());
        assert!(parse_err.to_string().contains("parse error"));

        let network_err = CredentialError::NetworkError("connection refused".to_string());
        assert!(network_err.to_string().contains("network error"));

        let unavailable = CredentialError::ProviderUnavailable("aws".to_string());
        assert!(unavailable.to_string().contains("unavailable"));

        let expired = CredentialError::Expired("token expired".to_string());
        assert!(expired.to_string().contains("expired"));

        let other = CredentialError::Other("something went wrong".to_string());
        assert!(other.to_string().contains("credential error"));
    }

    #[test]
    fn test_credential_error_equality() {
        let err1 = CredentialError::NotFound("test".to_string());
        let err2 = CredentialError::NotFound("test".to_string());
        let err3 = CredentialError::NotFound("other".to_string());

        assert_eq!(err1, err2);
        assert_ne!(err1, err3);
    }

    #[test]
    fn test_credential_error_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<CredentialError>();
    }

    #[test]
    fn test_credential_provider_is_send_sync() {
        fn assert_send_sync<T: Send + Sync + ?Sized>() {}
        assert_send_sync::<dyn CredentialProvider>();
        assert_send_sync::<EnvironmentCredentialProvider>();
    }

    #[test]
    fn test_environment_provider_new() {
        let provider = EnvironmentCredentialProvider::new();
        assert_eq!(provider.prefix(), "HAZELCAST");
    }

    #[test]
    fn test_environment_provider_with_prefix() {
        let provider = EnvironmentCredentialProvider::with_prefix("MYAPP");
        assert_eq!(provider.prefix(), "MYAPP");
    }

    #[test]
    fn test_environment_provider_default() {
        let provider = EnvironmentCredentialProvider::default();
        assert_eq!(provider.prefix(), "HAZELCAST");
    }

    #[test]
    fn test_environment_provider_clone() {
        let provider = EnvironmentCredentialProvider::with_prefix("TEST");
        let cloned = provider.clone();
        assert_eq!(cloned.prefix(), "TEST");
    }

    #[test]
    fn test_environment_provider_name() {
        let provider = EnvironmentCredentialProvider::new();
        assert_eq!(provider.provider_name(), "environment");
    }

    struct EnvGuard {
        vars: Vec<(String, Option<String>)>,
    }

    impl EnvGuard {
        fn new(vars: &[&str]) -> Self {
            let vars = vars
                .iter()
                .map(|&v| (v.to_string(), env::var(v).ok()))
                .collect();
            Self { vars }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            for (var, orig) in &self.vars {
                match orig {
                    Some(v) => env::set_var(var, v),
                    None => env::remove_var(var),
                }
            }
        }
    }

    #[tokio::test]
    async fn test_environment_provider_with_username_password() {
        let _guard = EnvGuard::new(&[
            "HAZELCAST_USERNAME",
            "HAZELCAST_PASSWORD",
            "HAZELCAST_TOKEN",
        ]);

        env::remove_var("HAZELCAST_TOKEN");
        env::set_var("HAZELCAST_USERNAME", "testuser");
        env::set_var("HAZELCAST_PASSWORD", "testpass");

        let provider = EnvironmentCredentialProvider::new();
        assert!(provider.is_available().await);

        let creds = provider.get_credentials().await.unwrap();
        match creds {
            Credentials::UsernamePassword { username, password } => {
                assert_eq!(username, "testuser");
                assert_eq!(password, "testpass");
            }
            _ => panic!("expected UsernamePassword"),
        }
    }

    #[tokio::test]
    async fn test_environment_provider_with_token() {
        let _guard = EnvGuard::new(&[
            "HAZELCAST_USERNAME",
            "HAZELCAST_PASSWORD",
            "HAZELCAST_TOKEN",
        ]);

        env::remove_var("HAZELCAST_USERNAME");
        env::remove_var("HAZELCAST_PASSWORD");
        env::set_var("HAZELCAST_TOKEN", "my-test-token");

        let provider = EnvironmentCredentialProvider::new();
        assert!(provider.is_available().await);

        let creds = provider.get_credentials().await.unwrap();
        match creds {
            Credentials::Token(token) => {
                assert_eq!(token, "my-test-token");
            }
            _ => panic!("expected Token"),
        }
    }

    #[tokio::test]
    async fn test_environment_provider_token_takes_precedence() {
        let _guard = EnvGuard::new(&[
            "HAZELCAST_USERNAME",
            "HAZELCAST_PASSWORD",
            "HAZELCAST_TOKEN",
        ]);

        env::set_var("HAZELCAST_USERNAME", "testuser");
        env::set_var("HAZELCAST_PASSWORD", "testpass");
        env::set_var("HAZELCAST_TOKEN", "my-token");

        let provider = EnvironmentCredentialProvider::new();
        let creds = provider.get_credentials().await.unwrap();

        match creds {
            Credentials::Token(token) => {
                assert_eq!(token, "my-token");
            }
            _ => panic!("expected Token"),
        }
    }

    #[tokio::test]
    async fn test_environment_provider_not_available() {
        let _guard = EnvGuard::new(&[
            "HAZELCAST_USERNAME",
            "HAZELCAST_PASSWORD",
            "HAZELCAST_TOKEN",
        ]);

        env::remove_var("HAZELCAST_USERNAME");
        env::remove_var("HAZELCAST_PASSWORD");
        env::remove_var("HAZELCAST_TOKEN");

        let provider = EnvironmentCredentialProvider::new();
        assert!(!provider.is_available().await);

        let result = provider.get_credentials().await;
        assert!(result.is_err());
        match result {
            Err(CredentialError::NotFound(msg)) => {
                assert!(msg.contains("no credentials found"));
            }
            _ => panic!("expected NotFound error"),
        }
    }

    #[tokio::test]
    async fn test_environment_provider_partial_credentials_username_only() {
        let _guard = EnvGuard::new(&[
            "HAZELCAST_USERNAME",
            "HAZELCAST_PASSWORD",
            "HAZELCAST_TOKEN",
        ]);

        env::remove_var("HAZELCAST_TOKEN");
        env::set_var("HAZELCAST_USERNAME", "testuser");
        env::remove_var("HAZELCAST_PASSWORD");

        let provider = EnvironmentCredentialProvider::new();
        assert!(!provider.is_available().await);

        let result = provider.get_credentials().await;
        assert!(result.is_err());
        match result {
            Err(CredentialError::NotFound(msg)) => {
                assert!(msg.contains("HAZELCAST_PASSWORD"));
                assert!(msg.contains("missing"));
            }
            _ => panic!("expected NotFound error"),
        }
    }

    #[tokio::test]
    async fn test_environment_provider_partial_credentials_password_only() {
        let _guard = EnvGuard::new(&[
            "HAZELCAST_USERNAME",
            "HAZELCAST_PASSWORD",
            "HAZELCAST_TOKEN",
        ]);

        env::remove_var("HAZELCAST_TOKEN");
        env::remove_var("HAZELCAST_USERNAME");
        env::set_var("HAZELCAST_PASSWORD", "testpass");

        let provider = EnvironmentCredentialProvider::new();
        assert!(!provider.is_available().await);

        let result = provider.get_credentials().await;
        assert!(result.is_err());
        match result {
            Err(CredentialError::NotFound(msg)) => {
                assert!(msg.contains("HAZELCAST_USERNAME"));
                assert!(msg.contains("missing"));
            }
            _ => panic!("expected NotFound error"),
        }
    }

    #[tokio::test]
    async fn test_environment_provider_custom_prefix() {
        let _guard = EnvGuard::new(&["MYAPP_USERNAME", "MYAPP_PASSWORD", "MYAPP_TOKEN"]);

        env::remove_var("MYAPP_TOKEN");
        env::set_var("MYAPP_USERNAME", "customuser");
        env::set_var("MYAPP_PASSWORD", "custompass");

        let provider = EnvironmentCredentialProvider::with_prefix("MYAPP");
        assert!(provider.is_available().await);

        let creds = provider.get_credentials().await.unwrap();
        match creds {
            Credentials::UsernamePassword { username, password } => {
                assert_eq!(username, "customuser");
                assert_eq!(password, "custompass");
            }
            _ => panic!("expected UsernamePassword"),
        }
    }

    #[cfg(feature = "aws")]
    #[test]
    fn test_aws_provider_new() {
        let provider = AwsCredentialProvider::new();
        assert!(provider.region().is_none());
        assert_eq!(
            provider.metadata_endpoint(),
            AwsCredentialProvider::DEFAULT_METADATA_ENDPOINT
        );
    }

    #[cfg(feature = "aws")]
    #[test]
    fn test_aws_provider_builder() {
        let provider = AwsCredentialProvider::new()
            .with_region("us-west-2")
            .with_metadata_endpoint("http://localhost:1234");

        assert_eq!(provider.region(), Some("us-west-2"));
        assert_eq!(provider.metadata_endpoint(), "http://localhost:1234");
    }

    #[cfg(feature = "aws")]
    #[test]
    fn test_aws_provider_default() {
        let provider = AwsCredentialProvider::default();
        assert!(provider.region().is_none());
    }

    #[cfg(feature = "aws")]
    #[test]
    fn test_aws_provider_name() {
        let provider = AwsCredentialProvider::new();
        assert_eq!(provider.provider_name(), "aws");
    }

    #[cfg(feature = "aws")]
    #[tokio::test]
    async fn test_aws_provider_env_vars() {
        let _guard = EnvGuard::new(&[
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_SESSION_TOKEN",
        ]);

        env::set_var("AWS_ACCESS_KEY_ID", "AKIATEST123");
        env::set_var("AWS_SECRET_ACCESS_KEY", "secretkey123");
        env::set_var("AWS_SESSION_TOKEN", "sessiontoken123");

        let provider = AwsCredentialProvider::new().with_region("us-east-1");
        assert!(provider.is_available().await);

        let creds = provider.get_credentials().await.unwrap();
        match creds {
            Credentials::Custom(custom) => {
                assert_eq!(custom.credential_type(), "aws");
                assert!(custom.get_attribute("access_key_id").is_some());
                assert!(custom.get_attribute("secret_access_key").is_some());
                assert!(custom.get_attribute("session_token").is_some());
                assert!(custom.get_attribute("region").is_some());
            }
            _ => panic!("expected Custom credentials"),
        }
    }

    #[cfg(feature = "aws")]
    #[tokio::test]
    async fn test_aws_provider_env_vars_no_session_token() {
        let _guard = EnvGuard::new(&[
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_SESSION_TOKEN",
        ]);

        env::set_var("AWS_ACCESS_KEY_ID", "AKIATEST123");
        env::set_var("AWS_SECRET_ACCESS_KEY", "secretkey123");
        env::remove_var("AWS_SESSION_TOKEN");

        let provider = AwsCredentialProvider::new();
        assert!(provider.is_available().await);

        let creds = provider.get_credentials().await.unwrap();
        match creds {
            Credentials::Custom(custom) => {
                assert_eq!(custom.credential_type(), "aws");
                assert!(custom.get_attribute("access_key_id").is_some());
                assert!(custom.get_attribute("secret_access_key").is_some());
                assert!(custom.get_attribute("session_token").is_none());
            }
            _ => panic!("expected Custom credentials"),
        }
    }

    #[cfg(feature = "aws")]
    #[tokio::test]
    async fn test_aws_provider_not_available() {
        let _guard = EnvGuard::new(&[
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_SESSION_TOKEN",
        ]);

        env::remove_var("AWS_ACCESS_KEY_ID");
        env::remove_var("AWS_SECRET_ACCESS_KEY");
        env::remove_var("AWS_SESSION_TOKEN");

        let provider = AwsCredentialProvider::new();
        assert!(!provider.is_available().await);

        let result = provider.get_credentials().await;
        assert!(result.is_err());
        match result {
            Err(CredentialError::NetworkError(_)) => {}
            _ => panic!("expected NetworkError"),
        }
    }

    #[cfg(feature = "azure")]
    #[test]
    fn test_azure_provider_new() {
        let provider = AzureCredentialProvider::new();
        assert!(provider.client_id().is_none());
        assert_eq!(provider.resource(), AzureCredentialProvider::DEFAULT_RESOURCE);
        assert_eq!(
            provider.imds_endpoint(),
            AzureCredentialProvider::DEFAULT_IMDS_ENDPOINT
        );
    }

    #[cfg(feature = "azure")]
    #[test]
    fn test_azure_provider_builder() {
        let provider = AzureCredentialProvider::new()
            .with_client_id("my-client-id")
            .with_resource("https://custom.resource/")
            .with_imds_endpoint("http://localhost:5000");

        assert_eq!(provider.client_id(), Some("my-client-id"));
        assert_eq!(provider.resource(), "https://custom.resource/");
        assert_eq!(provider.imds_endpoint(), "http://localhost:5000");
    }

    #[cfg(feature = "azure")]
    #[test]
    fn test_azure_provider_name() {
        let provider = AzureCredentialProvider::new();
        assert_eq!(provider.provider_name(), "azure");
    }

    #[cfg(feature = "azure")]
    #[tokio::test]
    async fn test_azure_provider_env_vars() {
        let _guard = EnvGuard::new(&[
            "AZURE_CLIENT_ID",
            "AZURE_CLIENT_SECRET",
            "AZURE_TENANT_ID",
        ]);

        env::set_var("AZURE_CLIENT_ID", "client123");
        env::set_var("AZURE_CLIENT_SECRET", "secret456");
        env::set_var("AZURE_TENANT_ID", "tenant789");

        let provider = AzureCredentialProvider::new();
        assert!(provider.is_available().await);

        let creds = provider.get_credentials().await.unwrap();
        match creds {
            Credentials::Custom(custom) => {
                assert_eq!(custom.credential_type(), "azure");
                assert!(custom.get_attribute("client_id").is_some());
                assert!(custom.get_attribute("client_secret").is_some());
                assert!(custom.get_attribute("tenant_id").is_some());
                assert!(custom.get_attribute("resource").is_some());
            }
            _ => panic!("expected Custom credentials"),
        }
    }

    #[cfg(feature = "azure")]
    #[tokio::test]
    async fn test_azure_provider_not_available() {
        let _guard = EnvGuard::new(&[
            "AZURE_CLIENT_ID",
            "AZURE_CLIENT_SECRET",
            "AZURE_TENANT_ID",
        ]);

        env::remove_var("AZURE_CLIENT_ID");
        env::remove_var("AZURE_CLIENT_SECRET");
        env::remove_var("AZURE_TENANT_ID");

        let provider = AzureCredentialProvider::new();
        assert!(!provider.is_available().await);
    }

    #[cfg(feature = "gcp")]
    #[test]
    fn test_gcp_provider_new() {
        let provider = GcpCredentialProvider::new();
        assert!(provider.service_account().is_none());
        assert_eq!(provider.scopes(), &[GcpCredentialProvider::DEFAULT_SCOPE]);
        assert_eq!(
            provider.metadata_endpoint(),
            GcpCredentialProvider::DEFAULT_METADATA_ENDPOINT
        );
    }

    #[cfg(feature = "gcp")]
    #[test]
    fn test_gcp_provider_builder() {
        let provider = GcpCredentialProvider::new()
            .with_service_account("sa@project.iam.gserviceaccount.com")
            .with_scopes(["scope1", "scope2"])
            .add_scope("scope3")
            .with_metadata_endpoint("http://localhost:8080");

        assert_eq!(
            provider.service_account(),
            Some("sa@project.iam.gserviceaccount.com")
        );
        assert_eq!(provider.scopes(), &["scope1", "scope2", "scope3"]);
        assert_eq!(provider.metadata_endpoint(), "http://localhost:8080");
    }

    #[cfg(feature = "gcp")]
    #[test]
    fn test_gcp_provider_name() {
        let provider = GcpCredentialProvider::new();
        assert_eq!(provider.provider_name(), "gcp");
    }

    #[cfg(feature = "gcp")]
    #[tokio::test]
    async fn test_gcp_provider_not_available_no_env() {
        let _guard = EnvGuard::new(&["GOOGLE_APPLICATION_CREDENTIALS"]);
        env::remove_var("GOOGLE_APPLICATION_CREDENTIALS");

        let provider = GcpCredentialProvider::new();
        assert!(!provider.is_available().await);
    }

    #[cfg(feature = "gcp")]
    #[tokio::test]
    async fn test_gcp_provider_with_credentials_file() {
        let _guard = EnvGuard::new(&["GOOGLE_APPLICATION_CREDENTIALS"]);

        let mut temp_file = tempfile::NamedTempFile::new().unwrap();
        writeln!(
            temp_file,
            r#"{{"type": "service_account", "project_id": "test"}}"#
        )
        .unwrap();

        env::set_var(
            "GOOGLE_APPLICATION_CREDENTIALS",
            temp_file.path().to_str().unwrap(),
        );

        let provider = GcpCredentialProvider::new();
        assert!(provider.is_available().await);

        let creds = provider.get_credentials().await.unwrap();
        match creds {
            Credentials::Custom(custom) => {
                assert_eq!(custom.credential_type(), "gcp-service-account");
                assert!(custom.get_attribute("credentials_json").is_some());
            }
            _ => panic!("expected Custom credentials"),
        }
    }

    #[cfg(feature = "kubernetes")]
    #[test]
    fn test_kubernetes_provider_new() {
        let provider = KubernetesCredentialProvider::new();
        assert_eq!(
            provider.token_path(),
            KubernetesCredentialProvider::DEFAULT_TOKEN_PATH
        );
        assert!(provider.namespace().is_none());
    }

    #[cfg(feature = "kubernetes")]
    #[test]
    fn test_kubernetes_provider_with_token_path() {
        let provider = KubernetesCredentialProvider::with_token_path("/custom/path/token");
        assert_eq!(provider.token_path(), "/custom/path/token");
    }

    #[cfg(feature = "kubernetes")]
    #[test]
    fn test_kubernetes_provider_with_namespace() {
        let provider = KubernetesCredentialProvider::new().with_namespace("my-namespace");
        assert_eq!(provider.namespace(), Some("my-namespace"));
    }

    #[cfg(feature = "kubernetes")]
    #[test]
    fn test_kubernetes_provider_name() {
        let provider = KubernetesCredentialProvider::new();
        assert_eq!(provider.provider_name(), "kubernetes");
    }

    #[cfg(feature = "kubernetes")]
    #[tokio::test]
    async fn test_kubernetes_provider_not_available() {
        let provider =
            KubernetesCredentialProvider::with_token_path("/nonexistent/path/to/token");
        assert!(!provider.is_available().await);

        let result = provider.get_credentials().await;
        assert!(result.is_err());
        match result {
            Err(CredentialError::NotFound(msg)) => {
                assert!(msg.contains("failed to read"));
            }
            _ => panic!("expected NotFound error"),
        }
    }

    #[cfg(feature = "kubernetes")]
    #[tokio::test]
    async fn test_kubernetes_provider_reads_token_file() {
        let mut temp_file = tempfile::NamedTempFile::new().unwrap();
        writeln!(temp_file, "test-k8s-token-12345").unwrap();

        let provider =
            KubernetesCredentialProvider::with_token_path(temp_file.path().to_str().unwrap());

        assert!(provider.is_available().await);

        let creds = provider.get_credentials().await.unwrap();
        match creds {
            Credentials::Token(token) => {
                assert_eq!(token, "test-k8s-token-12345");
            }
            _ => panic!("expected Token"),
        }
    }

    #[cfg(feature = "kubernetes")]
    #[tokio::test]
    async fn test_kubernetes_provider_trims_token() {
        let mut temp_file = tempfile::NamedTempFile::new().unwrap();
        writeln!(temp_file, "  token-with-whitespace  \n").unwrap();

        let provider =
            KubernetesCredentialProvider::with_token_path(temp_file.path().to_str().unwrap());

        let creds = provider.get_credentials().await.unwrap();
        match creds {
            Credentials::Token(token) => {
                assert_eq!(token, "token-with-whitespace");
            }
            _ => panic!("expected Token"),
        }
    }

    #[cfg(feature = "kubernetes")]
    #[tokio::test]
    async fn test_kubernetes_provider_empty_token_error() {
        let mut temp_file = tempfile::NamedTempFile::new().unwrap();
        writeln!(temp_file, "   ").unwrap();

        let provider =
            KubernetesCredentialProvider::with_token_path(temp_file.path().to_str().unwrap());

        let result = provider.get_credentials().await;
        assert!(result.is_err());
        match result {
            Err(CredentialError::ParseError(msg)) => {
                assert!(msg.contains("empty"));
            }
            _ => panic!("expected ParseError"),
        }
    }
}
