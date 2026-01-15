//! Custom authenticator implementations for Hazelcast authentication.

use std::collections::HashMap;

/// Credentials used for authentication.
#[derive(Debug, Clone)]
pub enum Credentials {
    /// Username and password credentials.
    UsernamePassword {
        /// The username.
        username: String,
        /// The password.
        password: String,
    },
    /// Token-based credentials (e.g., JWT).
    Token(String),
    /// Custom credentials with arbitrary key-value pairs.
    Custom(CustomCredentials),
}

/// Custom credentials with arbitrary attributes.
#[derive(Debug, Clone, Default)]
pub struct CustomCredentials {
    credential_type: String,
    attributes: HashMap<String, Vec<u8>>,
}

impl CustomCredentials {
    /// Creates new custom credentials with the given type identifier.
    pub fn new(credential_type: impl Into<String>) -> Self {
        Self {
            credential_type: credential_type.into(),
            attributes: HashMap::new(),
        }
    }

    /// Adds an attribute to the credentials.
    pub fn with_attribute(mut self, key: impl Into<String>, value: impl Into<Vec<u8>>) -> Self {
        self.attributes.insert(key.into(), value.into());
        self
    }

    /// Returns the credential type identifier.
    pub fn credential_type(&self) -> &str {
        &self.credential_type
    }

    /// Returns the attributes map.
    pub fn attributes(&self) -> &HashMap<String, Vec<u8>> {
        &self.attributes
    }

    /// Returns the value of a specific attribute.
    pub fn get_attribute(&self, key: &str) -> Option<&[u8]> {
        self.attributes.get(key).map(|v| v.as_slice())
    }
}

/// Response from a successful authentication.
#[derive(Debug, Clone)]
pub struct AuthResponse {
    /// Status code (0 = authenticated, 1 = credentials failed, 2 = serialization version mismatch).
    pub status: u8,
    /// Member UUID assigned to this client.
    pub member_uuid: Option<uuid::Uuid>,
    /// Cluster UUID.
    pub cluster_uuid: Option<uuid::Uuid>,
    /// Server-side serialization version.
    pub serialization_version: u8,
    /// Partition count in the cluster.
    pub partition_count: i32,
    /// Cluster ID.
    pub cluster_id: Option<uuid::Uuid>,
    /// Whether failover is supported.
    pub failover_supported: bool,
}

impl AuthResponse {
    /// Status code indicating successful authentication.
    pub const STATUS_AUTHENTICATED: u8 = 0;
    /// Status code indicating credentials failed.
    pub const STATUS_CREDENTIALS_FAILED: u8 = 1;
    /// Status code indicating serialization version mismatch.
    pub const STATUS_SERIALIZATION_MISMATCH: u8 = 2;

    /// Returns true if the authentication was successful.
    pub fn is_authenticated(&self) -> bool {
        self.status == Self::STATUS_AUTHENTICATED
    }

    /// Creates a new successful authentication response.
    pub fn new_authenticated(
        member_uuid: uuid::Uuid,
        cluster_uuid: uuid::Uuid,
        serialization_version: u8,
        partition_count: i32,
        cluster_id: uuid::Uuid,
        failover_supported: bool,
    ) -> Self {
        Self {
            status: Self::STATUS_AUTHENTICATED,
            member_uuid: Some(member_uuid),
            cluster_uuid: Some(cluster_uuid),
            serialization_version,
            partition_count,
            cluster_id: Some(cluster_id),
            failover_supported,
        }
    }

    /// Creates a new failed authentication response.
    pub fn new_failed(status: u8) -> Self {
        Self {
            status,
            member_uuid: None,
            cluster_uuid: None,
            serialization_version: 0,
            partition_count: 0,
            cluster_id: None,
            failover_supported: false,
        }
    }
}

/// Errors that can occur during authentication.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthError {
    /// Invalid credentials provided.
    InvalidCredentials(String),
    /// Authentication was rejected by the server.
    AuthenticationFailed {
        /// The status code from the server.
        status: u8,
        /// Error message.
        message: String,
    },
    /// Serialization error during credential encoding.
    SerializationError(String),
    /// Network or protocol error.
    ProtocolError(String),
    /// Authentication timeout.
    Timeout,
}

impl std::fmt::Display for AuthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AuthError::InvalidCredentials(msg) => write!(f, "invalid credentials: {}", msg),
            AuthError::AuthenticationFailed { status, message } => {
                write!(f, "authentication failed (status {}): {}", status, message)
            }
            AuthError::SerializationError(msg) => write!(f, "serialization error: {}", msg),
            AuthError::ProtocolError(msg) => write!(f, "protocol error: {}", msg),
            AuthError::Timeout => write!(f, "authentication timeout"),
        }
    }
}

impl std::error::Error for AuthError {}

/// Trait for implementing custom authentication mechanisms.
///
/// Implementors can provide custom credential serialization and
/// authentication logic for connecting to Hazelcast clusters.
#[async_trait::async_trait]
pub trait Authenticator: Send + Sync {
    /// Serializes credentials into bytes for the authentication protocol message.
    ///
    /// The returned bytes should be formatted according to the Hazelcast
    /// Open Binary Protocol authentication frame format.
    fn serialize_credentials(&self, credentials: &Credentials) -> Vec<u8>;

    /// Returns the authentication type identifier.
    ///
    /// Standard types:
    /// - "" (empty) for simple username/password
    /// - "token" for token-based auth
    fn auth_type(&self) -> &str;

    /// Called after receiving an authentication response to perform
    /// any post-authentication processing.
    async fn on_authenticated(&self, response: &AuthResponse) -> Result<(), AuthError>;
}

/// Default authenticator implementation supporting username/password and token auth.
#[derive(Debug, Clone, Default)]
pub struct DefaultAuthenticator;

impl DefaultAuthenticator {
    /// Creates a new default authenticator.
    pub fn new() -> Self {
        Self
    }
}

#[async_trait::async_trait]
impl Authenticator for DefaultAuthenticator {
    fn serialize_credentials(&self, credentials: &Credentials) -> Vec<u8> {
        let mut output = Vec::new();
        match credentials {
            Credentials::UsernamePassword { username, password } => {
                write_string(&mut output, username);
                write_string(&mut output, password);
            }
            Credentials::Token(token) => {
                output.push(1);
                write_string(&mut output, token);
            }
            Credentials::Custom(custom) => {
                write_string(&mut output, custom.credential_type());
                write_i32(&mut output, custom.attributes().len() as i32);
                for (key, value) in custom.attributes() {
                    write_string(&mut output, key);
                    write_bytes(&mut output, value);
                }
            }
        }
        output
    }

    fn auth_type(&self) -> &str {
        ""
    }

    async fn on_authenticated(&self, _response: &AuthResponse) -> Result<(), AuthError> {
        Ok(())
    }
}

/// Writes an i32 in big-endian format.
fn write_i32(output: &mut Vec<u8>, value: i32) {
    output.extend_from_slice(&value.to_be_bytes());
}

/// Writes a string with length prefix.
fn write_string(output: &mut Vec<u8>, s: &str) {
    write_i32(output, s.len() as i32);
    output.extend_from_slice(s.as_bytes());
}

/// Writes raw bytes with length prefix.
fn write_bytes(output: &mut Vec<u8>, bytes: &[u8]) {
    write_i32(output, bytes.len() as i32);
    output.extend_from_slice(bytes);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_credentials_username_password() {
        let creds = Credentials::UsernamePassword {
            username: "admin".to_string(),
            password: "secret".to_string(),
        };
        match creds {
            Credentials::UsernamePassword { username, password } => {
                assert_eq!(username, "admin");
                assert_eq!(password, "secret");
            }
            _ => panic!("expected UsernamePassword"),
        }
    }

    #[test]
    fn test_credentials_token() {
        let creds = Credentials::Token("my-jwt-token".to_string());
        match creds {
            Credentials::Token(token) => {
                assert_eq!(token, "my-jwt-token");
            }
            _ => panic!("expected Token"),
        }
    }

    #[test]
    fn test_custom_credentials_builder() {
        let creds = CustomCredentials::new("ldap")
            .with_attribute("dn", b"cn=admin,dc=example,dc=com".to_vec())
            .with_attribute("realm", b"EXAMPLE.COM".to_vec());

        assert_eq!(creds.credential_type(), "ldap");
        assert_eq!(creds.attributes().len(), 2);
    }

    #[test]
    fn test_custom_credentials_attributes() {
        let creds = CustomCredentials::new("custom")
            .with_attribute("key1", b"value1".to_vec())
            .with_attribute("key2", b"value2".to_vec());

        assert_eq!(creds.get_attribute("key1"), Some(b"value1".as_slice()));
        assert_eq!(creds.get_attribute("key2"), Some(b"value2".as_slice()));
        assert_eq!(creds.get_attribute("key3"), None);
    }

    #[test]
    fn test_auth_response_authenticated() {
        let member_uuid = uuid::Uuid::new_v4();
        let cluster_uuid = uuid::Uuid::new_v4();
        let cluster_id = uuid::Uuid::new_v4();

        let response = AuthResponse::new_authenticated(
            member_uuid,
            cluster_uuid,
            1,
            271,
            cluster_id,
            true,
        );

        assert_eq!(response.status, AuthResponse::STATUS_AUTHENTICATED);
        assert_eq!(response.member_uuid, Some(member_uuid));
        assert_eq!(response.cluster_uuid, Some(cluster_uuid));
        assert_eq!(response.serialization_version, 1);
        assert_eq!(response.partition_count, 271);
        assert_eq!(response.cluster_id, Some(cluster_id));
        assert!(response.failover_supported);
    }

    #[test]
    fn test_auth_response_failed() {
        let response = AuthResponse::new_failed(AuthResponse::STATUS_CREDENTIALS_FAILED);

        assert_eq!(response.status, AuthResponse::STATUS_CREDENTIALS_FAILED);
        assert!(response.member_uuid.is_none());
        assert!(response.cluster_uuid.is_none());
        assert_eq!(response.serialization_version, 0);
        assert_eq!(response.partition_count, 0);
        assert!(response.cluster_id.is_none());
        assert!(!response.failover_supported);
    }

    #[test]
    fn test_auth_response_is_authenticated() {
        let success = AuthResponse::new_authenticated(
            uuid::Uuid::new_v4(),
            uuid::Uuid::new_v4(),
            1,
            271,
            uuid::Uuid::new_v4(),
            false,
        );
        assert!(success.is_authenticated());

        let failed = AuthResponse::new_failed(AuthResponse::STATUS_CREDENTIALS_FAILED);
        assert!(!failed.is_authenticated());

        let mismatch = AuthResponse::new_failed(AuthResponse::STATUS_SERIALIZATION_MISMATCH);
        assert!(!mismatch.is_authenticated());
    }

    #[test]
    fn test_default_authenticator_serialize_username_password() {
        let auth = DefaultAuthenticator::new();
        let creds = Credentials::UsernamePassword {
            username: "user".to_string(),
            password: "pass".to_string(),
        };

        let bytes = auth.serialize_credentials(&creds);

        let mut expected = Vec::new();
        expected.extend_from_slice(&4_i32.to_be_bytes());
        expected.extend_from_slice(b"user");
        expected.extend_from_slice(&4_i32.to_be_bytes());
        expected.extend_from_slice(b"pass");

        assert_eq!(bytes, expected);
    }

    #[test]
    fn test_default_authenticator_serialize_token() {
        let auth = DefaultAuthenticator::new();
        let creds = Credentials::Token("mytoken".to_string());

        let bytes = auth.serialize_credentials(&creds);

        let mut expected = Vec::new();
        expected.push(1);
        expected.extend_from_slice(&7_i32.to_be_bytes());
        expected.extend_from_slice(b"mytoken");

        assert_eq!(bytes, expected);
    }

    #[test]
    fn test_default_authenticator_serialize_custom() {
        let auth = DefaultAuthenticator::new();
        let custom = CustomCredentials::new("test-type")
            .with_attribute("attr", b"value".to_vec());
        let creds = Credentials::Custom(custom);

        let bytes = auth.serialize_credentials(&creds);

        assert!(bytes.len() > 0);
        let type_len = i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        assert_eq!(type_len, 9);
    }

    #[test]
    fn test_default_authenticator_auth_type() {
        let auth = DefaultAuthenticator::new();
        assert_eq!(auth.auth_type(), "");
    }

    #[test]
    fn test_auth_error_display() {
        let invalid = AuthError::InvalidCredentials("bad password".to_string());
        assert!(invalid.to_string().contains("invalid credentials"));
        assert!(invalid.to_string().contains("bad password"));

        let failed = AuthError::AuthenticationFailed {
            status: 1,
            message: "rejected".to_string(),
        };
        assert!(failed.to_string().contains("authentication failed"));
        assert!(failed.to_string().contains("status 1"));

        let ser_err = AuthError::SerializationError("encoding failed".to_string());
        assert!(ser_err.to_string().contains("serialization error"));

        let proto_err = AuthError::ProtocolError("invalid frame".to_string());
        assert!(proto_err.to_string().contains("protocol error"));

        let timeout = AuthError::Timeout;
        assert!(timeout.to_string().contains("timeout"));
    }

    #[test]
    fn test_auth_error_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<AuthError>();
    }

    #[test]
    fn test_authenticator_is_send_sync() {
        fn assert_send_sync<T: Send + Sync + ?Sized>() {}
        assert_send_sync::<dyn Authenticator>();
    }

    #[test]
    fn test_credentials_clone() {
        let creds = Credentials::UsernamePassword {
            username: "user".to_string(),
            password: "pass".to_string(),
        };
        let cloned = creds.clone();
        match cloned {
            Credentials::UsernamePassword { username, password } => {
                assert_eq!(username, "user");
                assert_eq!(password, "pass");
            }
            _ => panic!("expected UsernamePassword"),
        }
    }

    #[test]
    fn test_custom_credentials_default() {
        let creds = CustomCredentials::default();
        assert_eq!(creds.credential_type(), "");
        assert!(creds.attributes().is_empty());
    }
}
