//! Custom authenticator implementations for Hazelcast authentication.

use std::collections::HashMap;

use base64::Engine;

/// Format of the authentication token.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TokenFormat {
    /// JWT (JSON Web Token) format with header.payload.signature structure.
    #[default]
    Jwt,
    /// Custom/opaque token format.
    Custom,
}

/// Token-based credentials for authentication.
#[derive(Debug, Clone)]
pub struct TokenCredentials {
    /// The raw token string.
    token: String,
    /// The format of the token.
    format: TokenFormat,
    /// Optional token name/identifier for logging.
    name: Option<String>,
}

impl TokenCredentials {
    /// Creates new token credentials with JWT format by default.
    pub fn new(token: impl Into<String>) -> Self {
        Self {
            token: token.into(),
            format: TokenFormat::Jwt,
            name: None,
        }
    }

    /// Sets the token format.
    pub fn with_format(mut self, format: TokenFormat) -> Self {
        self.format = format;
        self
    }

    /// Sets the token name/identifier for logging.
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Returns the raw token string.
    pub fn token(&self) -> &str {
        &self.token
    }

    /// Returns the token format.
    pub fn format(&self) -> TokenFormat {
        self.format
    }

    /// Returns the token name/identifier.
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }
}

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
    /// Token-based credentials with format and validation options.
    TokenCredentials(TokenCredentials),
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

/// Result of JWT token validation.
#[derive(Debug, Clone)]
pub struct JwtValidationResult {
    /// Whether the token structure is valid (3 base64url parts).
    pub valid_structure: bool,
    /// Decoded header as JSON string (if decodable).
    pub header: Option<String>,
    /// Decoded payload as JSON string (if decodable).
    pub payload: Option<String>,
    /// Expiration timestamp from payload (if present).
    pub expiration: Option<i64>,
    /// Whether the token is expired (if expiration is present).
    pub is_expired: bool,
}

/// Extracts the `exp` claim from a JSON payload string.
fn extract_exp_claim(payload: &str) -> (Option<i64>, bool) {
    if let Some(exp_pos) = payload.find("\"exp\"") {
        let after_exp = &payload[exp_pos + 5..];
        let after_colon = after_exp.trim_start().strip_prefix(':').unwrap_or(after_exp);
        let value_str = after_colon.trim_start();

        let end_pos = value_str
            .find(|c: char| !c.is_ascii_digit() && c != '-')
            .unwrap_or(value_str.len());
        if let Ok(exp) = value_str[..end_pos].parse::<i64>() {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0);
            return (Some(exp), now > exp);
        }
    }
    (None, false)
}

/// Validates a JWT token structure and extracts claims.
///
/// This performs structural validation only (no cryptographic signature verification).
/// For full JWT verification, use a dedicated JWT library on the server side.
pub fn validate_jwt_structure(token: &str) -> JwtValidationResult {
    let parts: Vec<&str> = token.split('.').collect();

    if parts.len() != 3 {
        return JwtValidationResult {
            valid_structure: false,
            header: None,
            payload: None,
            expiration: None,
            is_expired: false,
        };
    }

    let header = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(parts[0])
        .ok()
        .and_then(|bytes| String::from_utf8(bytes).ok());

    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(parts[1])
        .ok()
        .and_then(|bytes| String::from_utf8(bytes).ok());

    let (expiration, is_expired) = if let Some(ref payload_str) = payload {
        extract_exp_claim(payload_str)
    } else {
        (None, false)
    };

    JwtValidationResult {
        valid_structure: true,
        header,
        payload,
        expiration,
        is_expired,
    }
}

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
    /// - "kerberos" for Kerberos/GSSAPI auth
    fn auth_type(&self) -> &str;

    /// Called after receiving an authentication response to perform
    /// any post-authentication processing.
    async fn on_authenticated(&self, response: &AuthResponse) -> Result<(), AuthError>;

    /// Returns whether this authenticator supports challenge-response authentication.
    ///
    /// Authenticators that support multi-step authentication flows (e.g., Kerberos)
    /// should override this to return `true` and implement [`handle_challenge`](Self::handle_challenge).
    fn supports_challenge_response(&self) -> bool {
        false
    }

    /// Handles a challenge from the server during a multi-step authentication flow.
    ///
    /// This is called when the server sends a challenge token that the client
    /// must respond to. The default implementation returns an `UnsupportedOperation` error.
    ///
    /// Authenticators that support challenge-response (e.g., Kerberos, NTLM) should
    /// override both this method and [`supports_challenge_response`](Self::supports_challenge_response).
    async fn handle_challenge(&self, _challenge: &[u8]) -> Result<Vec<u8>, AuthError> {
        Err(AuthError::ProtocolError(
            "challenge-response authentication is not supported by this authenticator".to_string(),
        ))
    }
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
            Credentials::TokenCredentials(token_creds) => {
                output.push(1);
                write_string(&mut output, token_creds.token());
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
pub(crate) fn write_i32(output: &mut Vec<u8>, value: i32) {
    output.extend_from_slice(&value.to_be_bytes());
}

/// Writes a string with length prefix.
pub(crate) fn write_string(output: &mut Vec<u8>, s: &str) {
    write_i32(output, s.len() as i32);
    output.extend_from_slice(s.as_bytes());
}

/// Writes raw bytes with length prefix.
pub(crate) fn write_bytes(output: &mut Vec<u8>, bytes: &[u8]) {
    write_i32(output, bytes.len() as i32);
    output.extend_from_slice(bytes);
}

/// Authenticator for token-based authentication (JWT or custom tokens).
///
/// This authenticator supports both JWT tokens and custom/opaque tokens.
/// For JWT tokens, it can perform structural validation and expiration checking.
#[derive(Debug, Clone)]
pub struct TokenAuthenticator {
    /// Whether to validate JWT structure before sending.
    pub validate_jwt: bool,
    /// Whether to reject expired JWT tokens before sending.
    pub reject_expired: bool,
}

impl TokenAuthenticator {
    /// Creates a new token authenticator with validation enabled.
    pub fn new() -> Self {
        Self {
            validate_jwt: true,
            reject_expired: true,
        }
    }

    /// Sets whether to validate JWT structure before sending.
    pub fn with_validation(mut self, validate: bool) -> Self {
        self.validate_jwt = validate;
        self
    }

    /// Sets whether to reject expired JWT tokens before sending.
    pub fn with_reject_expired(mut self, reject: bool) -> Self {
        self.reject_expired = reject;
        self
    }

    /// Validates token credentials based on format.
    pub fn validate_token(&self, credentials: &TokenCredentials) -> Result<(), AuthError> {
        match credentials.format() {
            TokenFormat::Custom => Ok(()),
            TokenFormat::Jwt => {
                if !self.validate_jwt {
                    return Ok(());
                }

                let result = validate_jwt_structure(credentials.token());

                if !result.valid_structure {
                    return Err(AuthError::InvalidCredentials(
                        "Invalid JWT structure: token must have 3 parts".to_string(),
                    ));
                }

                if self.reject_expired && result.is_expired {
                    return Err(AuthError::InvalidCredentials(
                        "JWT token is expired".to_string(),
                    ));
                }

                Ok(())
            }
        }
    }
}

impl Default for TokenAuthenticator {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl Authenticator for TokenAuthenticator {
    fn serialize_credentials(&self, credentials: &Credentials) -> Vec<u8> {
        let mut output = Vec::new();
        match credentials {
            Credentials::TokenCredentials(token_creds) => {
                output.push(1);
                write_string(&mut output, token_creds.token());
            }
            Credentials::Token(token) => {
                output.push(1);
                write_string(&mut output, token);
            }
            Credentials::UsernamePassword { username, password } => {
                write_string(&mut output, username);
                write_string(&mut output, password);
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
        "token"
    }

    async fn on_authenticated(&self, _response: &AuthResponse) -> Result<(), AuthError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;

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

    #[test]
    fn test_token_format_default() {
        assert_eq!(TokenFormat::default(), TokenFormat::Jwt);
    }

    #[test]
    fn test_token_credentials_builder() {
        let creds = TokenCredentials::new("my-token")
            .with_format(TokenFormat::Custom)
            .with_name("api-key");

        assert_eq!(creds.token(), "my-token");
        assert_eq!(creds.format(), TokenFormat::Custom);
        assert_eq!(creds.name(), Some("api-key"));
    }

    #[test]
    fn test_token_credentials_jwt_default() {
        let creds = TokenCredentials::new("jwt.token.here");
        assert_eq!(creds.format(), TokenFormat::Jwt);
        assert!(creds.name().is_none());
    }

    #[test]
    fn test_validate_jwt_structure_valid() {
        let header = URL_SAFE_NO_PAD.encode(r#"{"alg":"HS256","typ":"JWT"}"#);
        let payload =
            URL_SAFE_NO_PAD.encode(r#"{"sub":"1234567890","name":"Test User","iat":1516239022}"#);
        let signature = URL_SAFE_NO_PAD.encode("fake-signature");

        let token = format!("{}.{}.{}", header, payload, signature);
        let result = validate_jwt_structure(&token);

        assert!(result.valid_structure);
        assert!(result.header.is_some());
        assert!(result.payload.is_some());
        assert!(!result.is_expired);
    }

    #[test]
    fn test_validate_jwt_structure_invalid_parts() {
        let result = validate_jwt_structure("not.a.valid.jwt.token");
        assert!(!result.valid_structure);

        let result = validate_jwt_structure("only-one-part");
        assert!(!result.valid_structure);

        let result = validate_jwt_structure("two.parts");
        assert!(!result.valid_structure);
    }

    #[test]
    fn test_validate_jwt_structure_expired() {
        let header = URL_SAFE_NO_PAD.encode(r#"{"alg":"HS256","typ":"JWT"}"#);
        let payload = URL_SAFE_NO_PAD.encode(r#"{"sub":"user","exp":1000000000}"#);
        let signature = URL_SAFE_NO_PAD.encode("sig");

        let token = format!("{}.{}.{}", header, payload, signature);
        let result = validate_jwt_structure(&token);

        assert!(result.valid_structure);
        assert!(result.is_expired);
        assert_eq!(result.expiration, Some(1000000000));
    }

    #[test]
    fn test_validate_jwt_structure_not_expired() {
        let header = URL_SAFE_NO_PAD.encode(r#"{"alg":"HS256","typ":"JWT"}"#);
        let future_exp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 3600;
        let payload = URL_SAFE_NO_PAD.encode(format!(r#"{{"sub":"user","exp":{}}}"#, future_exp));
        let signature = URL_SAFE_NO_PAD.encode("sig");

        let token = format!("{}.{}.{}", header, payload, signature);
        let result = validate_jwt_structure(&token);

        assert!(result.valid_structure);
        assert!(!result.is_expired);
        assert_eq!(result.expiration, Some(future_exp as i64));
    }

    #[test]
    fn test_token_authenticator_new() {
        let auth = TokenAuthenticator::new();
        assert!(auth.validate_jwt);
        assert!(auth.reject_expired);
    }

    #[test]
    fn test_token_authenticator_builder() {
        let auth = TokenAuthenticator::new()
            .with_validation(false)
            .with_reject_expired(false);

        assert!(!auth.validate_jwt);
        assert!(!auth.reject_expired);
    }

    #[test]
    fn test_token_authenticator_validate_custom_token() {
        let auth = TokenAuthenticator::new();
        let creds = TokenCredentials::new("any-opaque-token").with_format(TokenFormat::Custom);

        assert!(auth.validate_token(&creds).is_ok());
    }

    #[test]
    fn test_token_authenticator_validate_valid_jwt() {
        let auth = TokenAuthenticator::new();

        let header = URL_SAFE_NO_PAD.encode(r#"{"alg":"HS256","typ":"JWT"}"#);
        let future_exp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 3600;
        let payload = URL_SAFE_NO_PAD.encode(format!(r#"{{"sub":"user","exp":{}}}"#, future_exp));
        let signature = URL_SAFE_NO_PAD.encode("sig");

        let token = format!("{}.{}.{}", header, payload, signature);
        let creds = TokenCredentials::new(token);

        assert!(auth.validate_token(&creds).is_ok());
    }

    #[test]
    fn test_token_authenticator_reject_expired_jwt() {
        let auth = TokenAuthenticator::new();

        let header = URL_SAFE_NO_PAD.encode(r#"{"alg":"HS256","typ":"JWT"}"#);
        let payload = URL_SAFE_NO_PAD.encode(r#"{"sub":"user","exp":1000000000}"#);
        let signature = URL_SAFE_NO_PAD.encode("sig");

        let token = format!("{}.{}.{}", header, payload, signature);
        let creds = TokenCredentials::new(token);

        let result = auth.validate_token(&creds);
        assert!(result.is_err());
        match result {
            Err(AuthError::InvalidCredentials(msg)) => {
                assert!(msg.contains("expired"));
            }
            _ => panic!("expected InvalidCredentials error"),
        }
    }

    #[test]
    fn test_token_authenticator_allow_expired_when_disabled() {
        let auth = TokenAuthenticator::new().with_reject_expired(false);

        let header = URL_SAFE_NO_PAD.encode(r#"{"alg":"HS256","typ":"JWT"}"#);
        let payload = URL_SAFE_NO_PAD.encode(r#"{"sub":"user","exp":1000000000}"#);
        let signature = URL_SAFE_NO_PAD.encode("sig");

        let token = format!("{}.{}.{}", header, payload, signature);
        let creds = TokenCredentials::new(token);

        assert!(auth.validate_token(&creds).is_ok());
    }

    #[test]
    fn test_token_authenticator_reject_invalid_jwt_structure() {
        let auth = TokenAuthenticator::new();
        let creds = TokenCredentials::new("invalid-jwt-no-dots");

        let result = auth.validate_token(&creds);
        assert!(result.is_err());
        match result {
            Err(AuthError::InvalidCredentials(msg)) => {
                assert!(msg.contains("Invalid JWT"));
            }
            _ => panic!("expected InvalidCredentials error"),
        }
    }

    #[test]
    fn test_token_authenticator_skip_validation_when_disabled() {
        let auth = TokenAuthenticator::new().with_validation(false);
        let creds = TokenCredentials::new("invalid-jwt-no-dots");

        assert!(auth.validate_token(&creds).is_ok());
    }

    #[test]
    fn test_token_authenticator_auth_type() {
        let auth = TokenAuthenticator::new();
        assert_eq!(auth.auth_type(), "token");
    }

    #[test]
    fn test_token_authenticator_serialize_credentials() {
        let auth = TokenAuthenticator::new();
        let creds = Credentials::TokenCredentials(TokenCredentials::new("my-jwt-token"));

        let bytes = auth.serialize_credentials(&creds);

        assert!(!bytes.is_empty());
        assert_eq!(bytes[0], 1);
    }

    #[test]
    fn test_default_authenticator_handles_token_credentials() {
        let auth = DefaultAuthenticator::new();
        let creds = Credentials::TokenCredentials(TokenCredentials::new("test-token"));

        let bytes = auth.serialize_credentials(&creds);

        let expected_creds = Credentials::Token("test-token".to_string());
        let expected_bytes = auth.serialize_credentials(&expected_creds);

        assert_eq!(bytes, expected_bytes);
    }

    #[test]
    fn test_credentials_token_credentials_variant() {
        let token_creds = TokenCredentials::new("jwt.payload.sig")
            .with_format(TokenFormat::Jwt)
            .with_name("user-token");

        let creds = Credentials::TokenCredentials(token_creds);

        match creds {
            Credentials::TokenCredentials(tc) => {
                assert_eq!(tc.token(), "jwt.payload.sig");
                assert_eq!(tc.format(), TokenFormat::Jwt);
                assert_eq!(tc.name(), Some("user-token"));
            }
            _ => panic!("expected TokenCredentials"),
        }
    }

    #[test]
    fn test_token_credentials_clone() {
        let creds = TokenCredentials::new("token").with_name("test");
        let cloned = creds.clone();

        assert_eq!(cloned.token(), creds.token());
        assert_eq!(cloned.name(), creds.name());
    }

    #[test]
    fn test_jwt_validation_result_fields() {
        let result = JwtValidationResult {
            valid_structure: true,
            header: Some(r#"{"alg":"HS256"}"#.to_string()),
            payload: Some(r#"{"sub":"user"}"#.to_string()),
            expiration: Some(1234567890),
            is_expired: false,
        };

        assert!(result.valid_structure);
        assert!(result.header.is_some());
        assert!(result.payload.is_some());
        assert_eq!(result.expiration, Some(1234567890));
        assert!(!result.is_expired);
    }

    #[test]
    fn test_token_authenticator_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<TokenAuthenticator>();
    }

    #[test]
    fn test_token_format_is_copy() {
        fn assert_copy<T: Copy>() {}
        assert_copy::<TokenFormat>();
    }

    #[test]
    fn test_token_authenticator_default() {
        let auth = TokenAuthenticator::default();
        assert!(auth.validate_jwt);
        assert!(auth.reject_expired);
    }

    #[test]
    fn test_default_authenticator_does_not_support_challenge_response() {
        let auth = DefaultAuthenticator::new();
        assert!(!auth.supports_challenge_response());
    }

    #[tokio::test]
    async fn test_default_authenticator_handle_challenge_returns_error() {
        let auth = DefaultAuthenticator::new();
        let result = auth.handle_challenge(b"challenge-data").await;
        assert!(result.is_err());
        match result {
            Err(AuthError::ProtocolError(msg)) => {
                assert!(msg.contains("not supported"));
            }
            _ => panic!("expected ProtocolError"),
        }
    }

    #[test]
    fn test_token_authenticator_does_not_support_challenge_response() {
        let auth = TokenAuthenticator::new();
        assert!(!auth.supports_challenge_response());
    }

    #[tokio::test]
    async fn test_token_authenticator_handle_challenge_returns_error() {
        let auth = TokenAuthenticator::new();
        let result = auth.handle_challenge(b"challenge").await;
        assert!(result.is_err());
        match result {
            Err(AuthError::ProtocolError(msg)) => {
                assert!(msg.contains("not supported"));
            }
            _ => panic!("expected ProtocolError"),
        }
    }
}
