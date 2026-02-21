//! Error types for Hazelcast operations.

use std::fmt;
use std::io;
use std::sync::Arc;
use thiserror::Error;

/// Hazelcast server-side error codes.
///
/// These correspond to the error codes returned by the Hazelcast server
/// in protocol error responses. They enable clients to handle specific
/// server exceptions appropriately.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i32)]
pub enum ServerErrorCode {
    /// Undefined server error.
    Undefined = 0,
    /// java.lang.ArrayIndexOutOfBoundsException
    ArrayIndexOutOfBounds = 1,
    /// java.lang.ArrayStoreException
    ArrayStore = 2,
    /// java.lang.ClassCastException
    ClassCast = 4,
    /// java.lang.ClassNotFoundException
    ClassNotFound = 5,
    /// java.util.ConcurrentModificationException
    ConcurrentModification = 6,
    /// java.lang.IllegalArgumentException
    IllegalArgument = 9,
    /// java.lang.IllegalStateException
    IllegalState = 11,
    /// java.lang.IndexOutOfBoundsException
    IndexOutOfBounds = 13,
    /// java.io.IOException
    IoError = 14,
    /// java.lang.NullPointerException
    NullPointer = 17,
    /// java.lang.SecurityException
    Security = 21,
    /// java.net.SocketException
    Socket = 22,
    /// java.lang.UnsupportedOperationException
    UnsupportedOperation = 25,
    /// com.hazelcast.core.HazelcastException
    HazelcastException = 29,
    /// com.hazelcast.core.HazelcastInstanceNotActiveException
    HazelcastInstanceNotActive = 30,
    /// com.hazelcast.core.HazelcastOverloadException
    HazelcastOverload = 31,
    /// com.hazelcast.spi.exception.CallerNotMemberException
    CallerNotMember = 37,
    /// com.hazelcast.spi.exception.PartitionMigratingException
    PartitionMigrating = 38,
    /// com.hazelcast.spi.exception.RetryableHazelcastException
    RetryableHazelcast = 39,
    /// com.hazelcast.spi.exception.RetryableIOException
    RetryableIo = 40,
    /// com.hazelcast.spi.exception.TargetNotMemberException
    TargetNotMember = 41,
    /// com.hazelcast.spi.exception.WrongTargetException
    WrongTarget = 42,
    /// com.hazelcast.core.OperationTimeoutException
    OperationTimeout = 46,
    /// com.hazelcast.transaction.TransactionException
    Transaction = 48,
    /// com.hazelcast.transaction.TransactionNotActiveException
    TransactionNotActive = 49,
    /// com.hazelcast.transaction.TransactionTimedOutException
    TransactionTimedOut = 50,
    /// com.hazelcast.splitbrainprotection.SplitBrainProtectionException
    SplitBrainProtection = 52,
    /// com.hazelcast.core.IndeterminateOperationStateException
    IndeterminateOperationState = 53,
    /// com.hazelcast.spi.exception.TargetDisconnectedException
    TargetDisconnected = 62,
    /// com.hazelcast.core.MemberLeftException
    MemberLeft = 64,
    /// com.hazelcast.cp.exception.NotLeaderException
    NotLeader = 69,
    /// com.hazelcast.cp.exception.StaleAppendRequestException
    StaleAppendRequest = 71,
    /// com.hazelcast.cp.exception.CannotReplicateException
    CannotReplicate = 72,
    /// com.hazelcast.cp.exception.LeaderDemotedException
    LeaderDemoted = 73,
    /// com.hazelcast.cp.exception.CPGroupDestroyedException
    CpGroupDestroyed = 74,
    /// com.hazelcast.cp.lock.exception.LockOwnershipLostException
    LockOwnershipLost = 75,
    /// com.hazelcast.cp.session.CPSessionNotFoundException
    CpSessionNotFound = 76,
}

impl ServerErrorCode {
    /// Creates a `ServerErrorCode` from its wire protocol value.
    pub fn from_value(value: i32) -> Option<Self> {
        match value {
            0 => Some(Self::Undefined),
            1 => Some(Self::ArrayIndexOutOfBounds),
            2 => Some(Self::ArrayStore),
            4 => Some(Self::ClassCast),
            5 => Some(Self::ClassNotFound),
            6 => Some(Self::ConcurrentModification),
            9 => Some(Self::IllegalArgument),
            11 => Some(Self::IllegalState),
            13 => Some(Self::IndexOutOfBounds),
            14 => Some(Self::IoError),
            17 => Some(Self::NullPointer),
            21 => Some(Self::Security),
            22 => Some(Self::Socket),
            25 => Some(Self::UnsupportedOperation),
            29 => Some(Self::HazelcastException),
            30 => Some(Self::HazelcastInstanceNotActive),
            31 => Some(Self::HazelcastOverload),
            37 => Some(Self::CallerNotMember),
            38 => Some(Self::PartitionMigrating),
            39 => Some(Self::RetryableHazelcast),
            40 => Some(Self::RetryableIo),
            41 => Some(Self::TargetNotMember),
            42 => Some(Self::WrongTarget),
            46 => Some(Self::OperationTimeout),
            48 => Some(Self::Transaction),
            49 => Some(Self::TransactionNotActive),
            50 => Some(Self::TransactionTimedOut),
            52 => Some(Self::SplitBrainProtection),
            53 => Some(Self::IndeterminateOperationState),
            62 => Some(Self::TargetDisconnected),
            64 => Some(Self::MemberLeft),
            69 => Some(Self::NotLeader),
            71 => Some(Self::StaleAppendRequest),
            72 => Some(Self::CannotReplicate),
            73 => Some(Self::LeaderDemoted),
            74 => Some(Self::CpGroupDestroyed),
            75 => Some(Self::LockOwnershipLost),
            76 => Some(Self::CpSessionNotFound),
            _ => None,
        }
    }

    /// Returns the numeric wire protocol value.
    pub fn value(self) -> i32 {
        self as i32
    }

    /// Returns `true` if this error code indicates a retryable operation.
    pub fn is_retryable(self) -> bool {
        matches!(
            self,
            Self::RetryableHazelcast
                | Self::RetryableIo
                | Self::TargetNotMember
                | Self::WrongTarget
                | Self::PartitionMigrating
                | Self::CallerNotMember
                | Self::HazelcastInstanceNotActive
                | Self::TargetDisconnected
                | Self::MemberLeft
                | Self::NotLeader
                | Self::StaleAppendRequest
                | Self::CannotReplicate
                | Self::CpSessionNotFound
        )
    }
}

impl fmt::Display for ServerErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Undefined => write!(f, "UNDEFINED"),
            Self::ArrayIndexOutOfBounds => write!(f, "ARRAY_INDEX_OUT_OF_BOUNDS"),
            Self::ArrayStore => write!(f, "ARRAY_STORE"),
            Self::ClassCast => write!(f, "CLASS_CAST"),
            Self::ClassNotFound => write!(f, "CLASS_NOT_FOUND"),
            Self::ConcurrentModification => write!(f, "CONCURRENT_MODIFICATION"),
            Self::IllegalArgument => write!(f, "ILLEGAL_ARGUMENT"),
            Self::IllegalState => write!(f, "ILLEGAL_STATE"),
            Self::IndexOutOfBounds => write!(f, "INDEX_OUT_OF_BOUNDS"),
            Self::IoError => write!(f, "IO"),
            Self::NullPointer => write!(f, "NULL_POINTER"),
            Self::Security => write!(f, "SECURITY"),
            Self::Socket => write!(f, "SOCKET"),
            Self::UnsupportedOperation => write!(f, "UNSUPPORTED_OPERATION"),
            Self::HazelcastException => write!(f, "HAZELCAST"),
            Self::HazelcastInstanceNotActive => write!(f, "INSTANCE_NOT_ACTIVE"),
            Self::HazelcastOverload => write!(f, "OVERLOAD"),
            Self::CallerNotMember => write!(f, "CALLER_NOT_MEMBER"),
            Self::PartitionMigrating => write!(f, "PARTITION_MIGRATING"),
            Self::RetryableHazelcast => write!(f, "RETRYABLE_HAZELCAST"),
            Self::RetryableIo => write!(f, "RETRYABLE_IO"),
            Self::TargetNotMember => write!(f, "TARGET_NOT_MEMBER"),
            Self::WrongTarget => write!(f, "WRONG_TARGET"),
            Self::OperationTimeout => write!(f, "OPERATION_TIMEOUT"),
            Self::Transaction => write!(f, "TRANSACTION"),
            Self::TransactionNotActive => write!(f, "TRANSACTION_NOT_ACTIVE"),
            Self::TransactionTimedOut => write!(f, "TRANSACTION_TIMED_OUT"),
            Self::SplitBrainProtection => write!(f, "SPLIT_BRAIN_PROTECTION"),
            Self::IndeterminateOperationState => write!(f, "INDETERMINATE_OPERATION_STATE"),
            Self::TargetDisconnected => write!(f, "TARGET_DISCONNECTED"),
            Self::MemberLeft => write!(f, "MEMBER_LEFT"),
            Self::NotLeader => write!(f, "NOT_LEADER"),
            Self::StaleAppendRequest => write!(f, "STALE_APPEND_REQUEST"),
            Self::CannotReplicate => write!(f, "CANNOT_REPLICATE"),
            Self::LeaderDemoted => write!(f, "LEADER_DEMOTED"),
            Self::CpGroupDestroyed => write!(f, "CP_GROUP_DESTROYED"),
            Self::LockOwnershipLost => write!(f, "LOCK_OWNERSHIP_LOST"),
            Self::CpSessionNotFound => write!(f, "CP_SESSION_NOT_FOUND"),
        }
    }
}

/// The main error type for Hazelcast operations.
///
/// This enum covers both client-side errors (connection, configuration)
/// and server-side errors (returned via the Hazelcast protocol).
#[derive(Debug, Error, Clone)]
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

    /// Illegal state errors (operation not valid in current state).
    #[error("illegal state: {0}")]
    IllegalState(String),

    /// Quorum/split-brain protection errors.
    #[error("quorum not present: {0}")]
    QuorumNotPresent(String),

    /// The target member disconnected before the operation completed.
    #[error("target disconnected: {0}")]
    TargetDisconnected(String),

    /// A cluster member left while an operation was in progress.
    #[error("member left: {0}")]
    MemberLeft(String),

    /// The Hazelcast instance is not active (shutting down or not started).
    #[error("instance not active: {0}")]
    InstanceNotActive(String),

    /// A server-side error returned via the Hazelcast protocol.
    ///
    /// Contains the server error code and the server's error message.
    #[error("server error ({code}): {message}")]
    Server {
        /// The server error code identifying the exception type.
        code: ServerErrorCode,
        /// The error message from the server.
        message: String,
        /// Optional Java class name of the server exception.
        class_name: Option<String>,
    },

    /// I/O errors from the standard library (wrapped in `Arc` for `Clone`).
    #[error("I/O error: {0}")]
    Io(Arc<io::Error>),
}

impl From<io::Error> for HazelcastError {
    fn from(err: io::Error) -> Self {
        HazelcastError::Io(Arc::new(err))
    }
}

impl HazelcastError {
    /// Returns `true` if this error is retryable.
    ///
    /// Retryable errors are transient failures where repeating the operation
    /// may succeed. Examples include target member disconnections, partition
    /// migrations, and temporary connection issues.
    pub fn is_retryable(&self) -> bool {
        match self {
            // Server errors delegate to the error code's retryability
            Self::Server { code, .. } => code.is_retryable(),
            // Connection-related errors are generally retryable
            Self::Connection(_) => true,
            Self::TargetDisconnected(_) => true,
            Self::MemberLeft(_) => true,
            Self::InstanceNotActive(_) => true,
            // Timeouts may be retryable
            Self::Timeout(_) => true,
            // I/O errors are retryable (transient network issues)
            Self::Io(_) => true,
            // All other errors are not retryable
            _ => false,
        }
    }

    /// Creates a `HazelcastError::Server` from a server error code and message.
    pub fn from_server(code_value: i32, message: String, class_name: Option<String>) -> Self {
        let code = ServerErrorCode::from_value(code_value)
            .unwrap_or(ServerErrorCode::Undefined);
        Self::Server {
            code,
            message,
            class_name,
        }
    }

    /// Returns `true` if this error is transient (temporary, likely to resolve on its own).
    ///
    /// Transient errors are a subset of retryable errors — they indicate temporary
    /// conditions such as network blips, member restarts, or partition migrations
    /// that will typically resolve without intervention.
    pub fn is_transient(&self) -> bool {
        match self {
            Self::Connection(_) | Self::TargetDisconnected(_) | Self::MemberLeft(_) => true,
            Self::Timeout(_) => true,
            Self::Io(_) => true,
            Self::Server { code, .. } => matches!(
                code,
                ServerErrorCode::PartitionMigrating
                    | ServerErrorCode::TargetDisconnected
                    | ServerErrorCode::MemberLeft
                    | ServerErrorCode::RetryableIo
            ),
            _ => false,
        }
    }

    /// Returns the error category for structured error handling.
    pub fn category(&self) -> ErrorCategory {
        match self {
            Self::Connection(_) | Self::TargetDisconnected(_) | Self::MemberLeft(_)
            | Self::InstanceNotActive(_) | Self::Io(_) => ErrorCategory::Network,
            Self::Authentication(_) => ErrorCategory::Authentication,
            Self::Authorization(_) => ErrorCategory::Authorization,
            Self::Serialization(_) => ErrorCategory::Serialization,
            Self::Configuration(_) => ErrorCategory::Configuration,
            Self::Timeout(_) => ErrorCategory::Timeout,
            Self::IllegalState(_) | Self::QuorumNotPresent(_) => ErrorCategory::State,
            Self::Protocol(_) => ErrorCategory::Network,
            Self::Server { .. } => ErrorCategory::ServerSide,
        }
    }

    /// Returns the server error code if this is a server error.
    pub fn server_error_code(&self) -> Option<ServerErrorCode> {
        match self {
            Self::Server { code, .. } => Some(*code),
            _ => None,
        }
    }
}

/// Classification of error categories for structured error handling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorCategory {
    /// Network and connection errors.
    Network,
    /// Authentication failures.
    Authentication,
    /// Authorization / permission errors.
    Authorization,
    /// Serialization / deserialization errors.
    Serialization,
    /// Client configuration errors.
    Configuration,
    /// Server-side exceptions.
    ServerSide,
    /// Operation timeout.
    Timeout,
    /// Invalid state for the requested operation.
    State,
}

impl fmt::Display for ErrorCategory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Network => write!(f, "NETWORK"),
            Self::Authentication => write!(f, "AUTHENTICATION"),
            Self::Authorization => write!(f, "AUTHORIZATION"),
            Self::Serialization => write!(f, "SERIALIZATION"),
            Self::Configuration => write!(f, "CONFIGURATION"),
            Self::ServerSide => write!(f, "SERVER_SIDE"),
            Self::Timeout => write!(f, "TIMEOUT"),
            Self::State => write!(f, "STATE"),
        }
    }
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
        let err = HazelcastError::Io(Arc::new(io_err));
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

    #[test]
    fn test_error_is_clone() {
        let err = HazelcastError::Connection("test".to_string());
        let cloned = err.clone();
        assert_eq!(err.to_string(), cloned.to_string());
    }

    #[test]
    fn test_io_error_clone() {
        let io_err = io::Error::new(io::ErrorKind::NotFound, "file not found");
        let err = HazelcastError::Io(Arc::new(io_err));
        let cloned = err.clone();
        assert_eq!(err.to_string(), cloned.to_string());
    }

    #[test]
    fn test_server_error_display() {
        let err = HazelcastError::Server {
            code: ServerErrorCode::OperationTimeout,
            message: "timed out after 30s".to_string(),
            class_name: None,
        };
        assert_eq!(
            err.to_string(),
            "server error (OPERATION_TIMEOUT): timed out after 30s"
        );
    }

    #[test]
    fn test_server_error_from_value() {
        let err = HazelcastError::from_server(46, "timeout".to_string(), None);
        assert_eq!(
            err.server_error_code(),
            Some(ServerErrorCode::OperationTimeout)
        );
    }

    #[test]
    fn test_server_error_unknown_code() {
        let err = HazelcastError::from_server(9999, "unknown".to_string(), None);
        assert_eq!(
            err.server_error_code(),
            Some(ServerErrorCode::Undefined)
        );
    }

    #[test]
    fn test_retryable_errors() {
        assert!(HazelcastError::Connection("test".into()).is_retryable());
        assert!(HazelcastError::Timeout("test".into()).is_retryable());
        assert!(HazelcastError::TargetDisconnected("test".into()).is_retryable());
        assert!(HazelcastError::MemberLeft("test".into()).is_retryable());
        assert!(HazelcastError::InstanceNotActive("test".into()).is_retryable());

        assert!(!HazelcastError::Authentication("test".into()).is_retryable());
        assert!(!HazelcastError::Authorization("test".into()).is_retryable());
        assert!(!HazelcastError::Configuration("test".into()).is_retryable());
        assert!(!HazelcastError::Serialization("test".into()).is_retryable());
    }

    #[test]
    fn test_retryable_server_errors() {
        let retryable = HazelcastError::Server {
            code: ServerErrorCode::RetryableHazelcast,
            message: "retry".to_string(),
            class_name: None,
        };
        assert!(retryable.is_retryable());

        let non_retryable = HazelcastError::Server {
            code: ServerErrorCode::IllegalArgument,
            message: "bad arg".to_string(),
            class_name: None,
        };
        assert!(!non_retryable.is_retryable());
    }

    #[test]
    fn test_server_error_code_from_value() {
        assert_eq!(ServerErrorCode::from_value(46), Some(ServerErrorCode::OperationTimeout));
        assert_eq!(ServerErrorCode::from_value(64), Some(ServerErrorCode::MemberLeft));
        assert_eq!(ServerErrorCode::from_value(9999), None);
    }

    #[test]
    fn test_server_error_code_is_retryable() {
        assert!(ServerErrorCode::RetryableHazelcast.is_retryable());
        assert!(ServerErrorCode::RetryableIo.is_retryable());
        assert!(ServerErrorCode::TargetNotMember.is_retryable());
        assert!(ServerErrorCode::PartitionMigrating.is_retryable());

        assert!(!ServerErrorCode::IllegalArgument.is_retryable());
        assert!(!ServerErrorCode::Security.is_retryable());
        assert!(!ServerErrorCode::Transaction.is_retryable());
    }

    #[test]
    fn test_target_disconnected_display() {
        let err = HazelcastError::TargetDisconnected("member at 192.168.1.1".to_string());
        assert_eq!(
            err.to_string(),
            "target disconnected: member at 192.168.1.1"
        );
    }

    #[test]
    fn test_member_left_display() {
        let err = HazelcastError::MemberLeft("member-uuid-123".to_string());
        assert_eq!(err.to_string(), "member left: member-uuid-123");
    }

    #[test]
    fn test_instance_not_active_display() {
        let err = HazelcastError::InstanceNotActive("instance is shutting down".to_string());
        assert_eq!(
            err.to_string(),
            "instance not active: instance is shutting down"
        );
    }
}
