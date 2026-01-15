//! Error types for Hazelcast operations.

use std::io;
use thiserror::Error;

/// The main error type for Hazelcast operations.
#[derive(Debug, Error)]
pub enum HazelcastError {
    /// Connection-related errors (network failures, disconnections).
    #[error("connection error: {0}")]
    Connection(String),

    /// Protocol-related errors (invalid messages, unsupported versions).
    #[error("protocol error: {0}")]
    Protocol(String),

    /// Serialization/deserialization errors.
    #[error("serialization error: {0}")]
    Serialization(String),

    /// Operation timeout errors.
    #[error("timeout error: {0}")]
    Timeout(String),

    /// Authentication errors (invalid credentials, failed login).
    #[error("authentication error: {0}")]
    Authentication(String),

    /// Authorization errors (insufficient permissions for operation).
    #[error("authorization error: {0}")]
    Authorization(String),

    /// Configuration errors (invalid settings).
    #[error("configuration error: {0}")]
    Configuration(String),

    /// I/O errors from the standard library.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
}

/// A specialized `Result` type for Hazelcast operations.
pub type Result<T> = std::result::Result<T, HazelcastError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connection_error_display() {
        let err = HazelcastError::Connection("failed to connect to server".to_string());
        assert_eq!(
            err.to_string(),
            "connection error: failed to connect to server"
        );
    }

    #[test]
    fn test_protocol_error_display() {
        let err = HazelcastError::Protocol("invalid message format".to_string());
        assert_eq!(err.to_string(), "protocol error: invalid message format");
    }

    #[test]
    fn test_serialization_error_display() {
        let err = HazelcastError::Serialization("failed to deserialize response".to_string());
        assert_eq!(
            err.to_string(),
            "serialization error: failed to deserialize response"
        );
    }

    #[test]
    fn test_timeout_error_display() {
        let err = HazelcastError::Timeout("operation timed out after 30s".to_string());
        assert_eq!(
            err.to_string(),
            "timeout error: operation timed out after 30s"
        );
    }

    #[test]
    fn test_authentication_error_display() {
        let err = HazelcastError::Authentication("invalid credentials".to_string());
        assert_eq!(
            err.to_string(),
            "authentication error: invalid credentials"
        );
    }

    #[test]
    fn test_authorization_error_display() {
        let err = HazelcastError::Authorization("missing read permission".to_string());
        assert_eq!(
            err.to_string(),
            "authorization error: missing read permission"
        );
    }

    #[test]
    fn test_configuration_error_display() {
        let err = HazelcastError::Configuration("invalid timeout value".to_string());
        assert_eq!(
            err.to_string(),
            "configuration error: invalid timeout value"
        );
    }

    #[test]
    fn test_io_error_conversion() {
        let io_err = io::Error::new(io::ErrorKind::ConnectionRefused, "connection refused");
        let err: HazelcastError = io_err.into();
        assert!(matches!(err, HazelcastError::Io(_)));
        assert!(err.to_string().contains("connection refused"));
    }

    #[test]
    fn test_io_error_display() {
        let io_err = io::Error::new(io::ErrorKind::NotFound, "file not found");
        let err = HazelcastError::Io(io_err);
        assert!(err.to_string().starts_with("I/O error:"));
    }

    #[test]
    fn test_error_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<HazelcastError>();
    }

    #[test]
    fn test_result_type_alias() {
        fn returns_ok() -> Result<i32> {
            Ok(42)
        }

        fn returns_err() -> Result<i32> {
            Err(HazelcastError::Timeout("test".to_string()))
        }

        assert!(returns_ok().is_ok());
        assert!(returns_err().is_err());
    }
}
