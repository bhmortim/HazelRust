//! Security module for authentication and authorization.

pub mod authenticator;
pub mod authorization;
pub mod credentials;
pub mod tls_config;

#[cfg(feature = "kerberos")]
pub mod kerberos;

pub use authenticator::{
    AuthError, AuthResponse, Authenticator, Credentials, CustomCredentials, DefaultAuthenticator,
    JwtValidationResult, TokenAuthenticator, TokenCredentials, TokenFormat, validate_jwt_structure,
};

pub use authorization::{
    AuthorizationContext, Permission, PermissionDenied, PermissionGrant, ResourceType, Role,
};

pub use credentials::{CredentialError, CredentialProvider, EnvironmentCredentialProvider};

pub use tls_config::{
    cipher_suites, HostnameVerification, TlsConfig, TlsConfigBuilder, TlsConfigError,
    TlsProtocolVersion,
};

#[cfg(feature = "aws")]
pub use credentials::AwsCredentialProvider;

#[cfg(feature = "azure")]
pub use credentials::AzureCredentialProvider;

#[cfg(feature = "gcp")]
pub use credentials::GcpCredentialProvider;

#[cfg(feature = "kubernetes")]
pub use credentials::KubernetesCredentialProvider;

#[cfg(feature = "kerberos")]
pub use kerberos::{KerberosAuthenticator, KerberosCredentials};
