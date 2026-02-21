//! Kerberos authentication for Hazelcast.
//!
//! This module provides Kerberos-based authentication for connecting
//! to Hazelcast clusters that require Kerberos/GSSAPI security.

use super::authenticator::{
    write_bytes, write_i32, write_string, AuthError, AuthResponse, Authenticator, Credentials,
};

/// Kerberos credentials for authentication with a Hazelcast cluster.
#[derive(Debug, Clone)]
pub struct KerberosCredentials {
    /// The Kerberos service principal name (e.g., "hz/hazelcast.example.com").
    service_principal: String,
    /// The Kerberos realm (e.g., "EXAMPLE.COM").
    realm: Option<String>,
    /// Optional keytab file path for non-interactive authentication.
    keytab_path: Option<String>,
    /// Optional principal name for the client (e.g., "client@EXAMPLE.COM").
    client_principal: Option<String>,
}

impl KerberosCredentials {
    /// Creates new Kerberos credentials with the given service principal.
    pub fn new(service_principal: impl Into<String>) -> Self {
        Self {
            service_principal: service_principal.into(),
            realm: None,
            keytab_path: None,
            client_principal: None,
        }
    }

    /// Sets the Kerberos realm.
    pub fn with_realm(mut self, realm: impl Into<String>) -> Self {
        self.realm = Some(realm.into());
        self
    }

    /// Sets the keytab file path for non-interactive authentication.
    pub fn with_keytab(mut self, keytab_path: impl Into<String>) -> Self {
        self.keytab_path = Some(keytab_path.into());
        self
    }

    /// Sets the client principal name.
    pub fn with_client_principal(mut self, principal: impl Into<String>) -> Self {
        self.client_principal = Some(principal.into());
        self
    }

    /// Returns the service principal name.
    pub fn service_principal(&self) -> &str {
        &self.service_principal
    }

    /// Returns the Kerberos realm if set.
    pub fn realm(&self) -> Option<&str> {
        self.realm.as_deref()
    }

    /// Returns the keytab file path if set.
    pub fn keytab_path(&self) -> Option<&str> {
        self.keytab_path.as_deref()
    }

    /// Returns the client principal if set.
    pub fn client_principal(&self) -> Option<&str> {
        self.client_principal.as_deref()
    }

    /// Returns the fully qualified service principal (principal@REALM).
    pub fn full_service_principal(&self) -> String {
        match &self.realm {
            Some(realm) => format!("{}@{}", self.service_principal, realm),
            None => self.service_principal.clone(),
        }
    }
}

/// Kerberos authenticator for Hazelcast cluster connections.
///
/// This authenticator handles Kerberos/GSSAPI authentication by serializing
/// Kerberos credentials for the Hazelcast Open Binary Protocol. It supports
/// challenge-response authentication flows required by Kerberos.
#[derive(Debug, Clone)]
pub struct KerberosAuthenticator {
    /// Kerberos credentials.
    credentials: KerberosCredentials,
}

impl KerberosAuthenticator {
    /// Creates a new Kerberos authenticator with the given credentials.
    pub fn new(credentials: KerberosCredentials) -> Self {
        Self { credentials }
    }

    /// Returns the Kerberos credentials.
    pub fn credentials(&self) -> &KerberosCredentials {
        &self.credentials
    }
}

#[async_trait::async_trait]
impl Authenticator for KerberosAuthenticator {
    fn serialize_credentials(&self, credentials: &Credentials) -> Vec<u8> {
        let mut output = Vec::new();
        match credentials {
            Credentials::Custom(custom) => {
                // Serialize the Kerberos credential type and attributes
                write_string(&mut output, custom.credential_type());
                let attrs = custom.attributes();
                write_i32(&mut output, attrs.len() as i32);
                for (key, value) in attrs {
                    write_string(&mut output, key);
                    write_bytes(&mut output, value);
                }
            }
            Credentials::UsernamePassword { username, password } => {
                // Fallback: serialize as username/password
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
        }
        output
    }

    fn auth_type(&self) -> &str {
        "kerberos"
    }

    async fn on_authenticated(&self, response: &AuthResponse) -> Result<(), AuthError> {
        if response.is_authenticated() {
            tracing::info!(
                "Kerberos authentication successful for principal: {}",
                self.credentials.full_service_principal()
            );
            Ok(())
        } else {
            Err(AuthError::AuthenticationFailed {
                status: response.status,
                message: format!(
                    "Kerberos authentication failed for principal: {}",
                    self.credentials.full_service_principal()
                ),
            })
        }
    }

    fn supports_challenge_response(&self) -> bool {
        true
    }

    async fn handle_challenge(&self, challenge: &[u8]) -> Result<Vec<u8>, AuthError> {
        // In a full Kerberos implementation, this would process the server's
        // GSSAPI challenge token and produce a response using the Kerberos
        // ticket-granting ticket (TGT). This placeholder returns the challenge
        // as an acknowledgment, which allows the protocol flow to proceed.
        //
        // A production implementation would integrate with a GSSAPI library
        // (e.g., libgssapi or cross-krb5) to handle the actual token exchange.
        let mut response = Vec::new();
        write_string(&mut response, &self.credentials.full_service_principal());
        response.extend_from_slice(challenge);
        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::security::authenticator::CustomCredentials;

    #[test]
    fn test_kerberos_credentials_new() {
        let creds = KerberosCredentials::new("hz/hazelcast.example.com");
        assert_eq!(creds.service_principal(), "hz/hazelcast.example.com");
        assert!(creds.realm().is_none());
        assert!(creds.keytab_path().is_none());
        assert!(creds.client_principal().is_none());
    }

    #[test]
    fn test_kerberos_credentials_builder() {
        let creds = KerberosCredentials::new("hz/node1")
            .with_realm("EXAMPLE.COM")
            .with_keytab("/etc/hazelcast/hazelcast.keytab")
            .with_client_principal("client@EXAMPLE.COM");

        assert_eq!(creds.service_principal(), "hz/node1");
        assert_eq!(creds.realm(), Some("EXAMPLE.COM"));
        assert_eq!(creds.keytab_path(), Some("/etc/hazelcast/hazelcast.keytab"));
        assert_eq!(creds.client_principal(), Some("client@EXAMPLE.COM"));
    }

    #[test]
    fn test_kerberos_credentials_full_principal_with_realm() {
        let creds = KerberosCredentials::new("hz/hazelcast")
            .with_realm("EXAMPLE.COM");

        assert_eq!(creds.full_service_principal(), "hz/hazelcast@EXAMPLE.COM");
    }

    #[test]
    fn test_kerberos_credentials_full_principal_without_realm() {
        let creds = KerberosCredentials::new("hz/hazelcast");
        assert_eq!(creds.full_service_principal(), "hz/hazelcast");
    }

    #[test]
    fn test_kerberos_credentials_clone() {
        let creds = KerberosCredentials::new("hz/test")
            .with_realm("TEST.COM");
        let cloned = creds.clone();

        assert_eq!(cloned.service_principal(), creds.service_principal());
        assert_eq!(cloned.realm(), creds.realm());
    }

    #[test]
    fn test_kerberos_credentials_debug() {
        let creds = KerberosCredentials::new("hz/test");
        let debug_str = format!("{:?}", creds);
        assert!(debug_str.contains("KerberosCredentials"));
        assert!(debug_str.contains("hz/test"));
    }

    #[test]
    fn test_kerberos_credentials_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<KerberosCredentials>();
    }

    #[test]
    fn test_kerberos_authenticator_new() {
        let creds = KerberosCredentials::new("hz/node1").with_realm("EXAMPLE.COM");
        let auth = KerberosAuthenticator::new(creds);

        assert_eq!(auth.credentials().service_principal(), "hz/node1");
        assert_eq!(auth.credentials().realm(), Some("EXAMPLE.COM"));
    }

    #[test]
    fn test_kerberos_authenticator_auth_type() {
        let creds = KerberosCredentials::new("hz/test");
        let auth = KerberosAuthenticator::new(creds);

        assert_eq!(auth.auth_type(), "kerberos");
    }

    #[test]
    fn test_kerberos_authenticator_supports_challenge_response() {
        let creds = KerberosCredentials::new("hz/test");
        let auth = KerberosAuthenticator::new(creds);

        assert!(auth.supports_challenge_response());
    }

    #[test]
    fn test_kerberos_authenticator_serialize_custom_credentials() {
        let creds = KerberosCredentials::new("hz/test");
        let auth = KerberosAuthenticator::new(creds);

        let custom = CustomCredentials::new("kerberos")
            .with_attribute("token", b"some-token".to_vec());
        let credentials = Credentials::Custom(custom);

        let bytes = auth.serialize_credentials(&credentials);
        assert!(!bytes.is_empty());

        // Verify the credential type "kerberos" is in the output
        let type_len = i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        assert_eq!(type_len, 8); // "kerberos".len()
    }

    #[test]
    fn test_kerberos_authenticator_serialize_username_password() {
        let creds = KerberosCredentials::new("hz/test");
        let auth = KerberosAuthenticator::new(creds);

        let credentials = Credentials::UsernamePassword {
            username: "user".to_string(),
            password: "pass".to_string(),
        };

        let bytes = auth.serialize_credentials(&credentials);
        assert!(!bytes.is_empty());
    }

    #[test]
    fn test_kerberos_authenticator_clone() {
        let creds = KerberosCredentials::new("hz/test").with_realm("TEST.COM");
        let auth = KerberosAuthenticator::new(creds);
        let cloned = auth.clone();

        assert_eq!(cloned.auth_type(), "kerberos");
        assert_eq!(cloned.credentials().realm(), Some("TEST.COM"));
    }

    #[test]
    fn test_kerberos_authenticator_debug() {
        let creds = KerberosCredentials::new("hz/test");
        let auth = KerberosAuthenticator::new(creds);
        let debug_str = format!("{:?}", auth);
        assert!(debug_str.contains("KerberosAuthenticator"));
    }

    #[test]
    fn test_kerberos_authenticator_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<KerberosAuthenticator>();
    }

    #[tokio::test]
    async fn test_kerberos_authenticator_on_authenticated_success() {
        let creds = KerberosCredentials::new("hz/test").with_realm("TEST.COM");
        let auth = KerberosAuthenticator::new(creds);

        let response = AuthResponse::new_authenticated(
            uuid::Uuid::new_v4(),
            uuid::Uuid::new_v4(),
            1,
            271,
            uuid::Uuid::new_v4(),
            false,
        );

        assert!(auth.on_authenticated(&response).await.is_ok());
    }

    #[tokio::test]
    async fn test_kerberos_authenticator_on_authenticated_failure() {
        let creds = KerberosCredentials::new("hz/test");
        let auth = KerberosAuthenticator::new(creds);

        let response = AuthResponse::new_failed(AuthResponse::STATUS_CREDENTIALS_FAILED);
        let result = auth.on_authenticated(&response).await;

        assert!(result.is_err());
        match result {
            Err(AuthError::AuthenticationFailed { status, message }) => {
                assert_eq!(status, 1);
                assert!(message.contains("Kerberos authentication failed"));
            }
            _ => panic!("expected AuthenticationFailed error"),
        }
    }

    #[tokio::test]
    async fn test_kerberos_authenticator_handle_challenge() {
        let creds = KerberosCredentials::new("hz/test").with_realm("TEST.COM");
        let auth = KerberosAuthenticator::new(creds);

        let challenge = b"server-challenge-token";
        let response = auth.handle_challenge(challenge).await.unwrap();

        // Response should contain the service principal and the challenge
        assert!(!response.is_empty());
        // Verify the principal string is in the response
        let principal_len = i32::from_be_bytes([response[0], response[1], response[2], response[3]]);
        assert_eq!(principal_len as usize, "hz/test@TEST.COM".len());
    }
}
