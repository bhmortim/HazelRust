//! CP Session management service for Hazelcast client.
//!
//! Provides management operations for CP sessions including
//! querying active sessions and force-closing sessions.

use std::sync::Arc;

use bytes::BytesMut;
use hazelcast_core::protocol::constants::PARTITION_ID_ANY;
use hazelcast_core::protocol::{ClientMessage, Frame};
use hazelcast_core::Result;

use super::cp_management::CPGroupId;
use crate::connection::ConnectionManager;

/// Identifier for a CP session.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CPSessionId {
    session_id: i64,
    group_id: CPGroupId,
}

impl CPSessionId {
    /// Creates a new CPSessionId.
    pub fn new(session_id: i64, group_id: CPGroupId) -> Self {
        Self { session_id, group_id }
    }

    /// Returns the session ID.
    pub fn session_id(&self) -> i64 {
        self.session_id
    }

    /// Returns the CP group ID this session belongs to.
    pub fn group_id(&self) -> &CPGroupId {
        &self.group_id
    }
}

/// Type of endpoint owning a CP session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CPSessionEndpointType {
    /// The session is owned by a client.
    Client,
    /// The session is owned by a server member.
    Server,
}

/// Information about a CP session.
#[derive(Debug, Clone)]
pub struct CPSession {
    id: CPSessionId,
    creation_time: i64,
    expiration_time: i64,
    endpoint_type: CPSessionEndpointType,
    endpoint_name: String,
}

impl CPSession {
    /// Creates a new CPSession.
    pub fn new(
        id: CPSessionId,
        creation_time: i64,
        expiration_time: i64,
        endpoint_type: CPSessionEndpointType,
        endpoint_name: impl Into<String>,
    ) -> Self {
        Self {
            id,
            creation_time,
            expiration_time,
            endpoint_type,
            endpoint_name: endpoint_name.into(),
        }
    }

    /// Returns the session identifier.
    pub fn id(&self) -> &CPSessionId {
        &self.id
    }

    /// Returns the creation time of this session in milliseconds since epoch.
    pub fn creation_time(&self) -> i64 {
        self.creation_time
    }

    /// Returns the expiration time of this session in milliseconds since epoch.
    pub fn expiration_time(&self) -> i64 {
        self.expiration_time
    }

    /// Returns the type of endpoint owning this session.
    pub fn endpoint_type(&self) -> CPSessionEndpointType {
        self.endpoint_type
    }

    /// Returns the name of the endpoint owning this session.
    pub fn endpoint_name(&self) -> &str {
        &self.endpoint_name
    }
}

/// Service for managing CP sessions.
///
/// The CP Session management service provides operations to:
/// - Query active CP sessions for a group
/// - Force close CP sessions (use with caution)
///
/// Note: These operations require the CP Subsystem to be enabled on the
/// Hazelcast cluster.
///
/// # Example
///
/// ```ignore
/// let session_mgmt = client.cp_session_management();
///
/// // List all sessions for a CP group
/// let sessions = session_mgmt.get_sessions("default").await?;
/// for session in &sessions {
///     println!("Session {} created at {}", session.id().session_id(), session.creation_time());
///     println!("  Endpoint: {} ({:?})", session.endpoint_name(), session.endpoint_type());
/// }
///
/// // Force close a stuck session (dangerous!)
/// session_mgmt.force_close_session("default", session_id).await?;
/// ```
#[derive(Debug, Clone)]
pub struct CPSessionManagementService {
    connection_manager: Arc<ConnectionManager>,
}

impl CPSessionManagementService {
    /// Creates a new CPSessionManagementService.
    pub fn new(connection_manager: Arc<ConnectionManager>) -> Self {
        Self { connection_manager }
    }

    /// Returns all active CP sessions for the specified group.
    ///
    /// # Arguments
    ///
    /// * `group_name` - The name of the CP group to query sessions for
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The CP Subsystem is not enabled on the cluster
    /// - The specified group does not exist
    /// - No connection to the cluster is available
    /// - A network error occurs
    pub async fn get_sessions(&self, group_name: &str) -> Result<Vec<CPSession>> {
        const CP_SESSION_GET_ALL: i32 = 0x1F0100;

        let mut request = ClientMessage::new_request(CP_SESSION_GET_ALL);
        request.set_partition_id(PARTITION_ID_ANY);

        let name_bytes = BytesMut::from(group_name.as_bytes());
        request.add_frame(Frame::with_content(name_bytes));

        let response = self.connection_manager.send(request).await?;

        let mut result = Vec::new();
        let frames = response.frames();

        let mut idx = 1;
        while idx + 4 < frames.len() {
            let session_id_frame = &frames[idx];
            if session_id_frame.is_end_frame() {
                break;
            }

            let session_id = if session_id_frame.content().len() >= 8 {
                i64::from_le_bytes(session_id_frame.content()[..8].try_into().unwrap_or([0; 8]))
            } else {
                0
            };

            let creation_frame = &frames[idx + 1];
            let creation_time = if creation_frame.content().len() >= 8 {
                i64::from_le_bytes(creation_frame.content()[..8].try_into().unwrap_or([0; 8]))
            } else {
                0
            };

            let expiration_frame = &frames[idx + 2];
            let expiration_time = if expiration_frame.content().len() >= 8 {
                i64::from_le_bytes(expiration_frame.content()[..8].try_into().unwrap_or([0; 8]))
            } else {
                0
            };

            let endpoint_type_frame = &frames[idx + 3];
            let endpoint_type = if !endpoint_type_frame.content().is_empty()
                && endpoint_type_frame.content()[0] == 1
            {
                CPSessionEndpointType::Server
            } else {
                CPSessionEndpointType::Client
            };

            let endpoint_name_frame = &frames[idx + 4];
            let endpoint_name =
                String::from_utf8_lossy(endpoint_name_frame.content()).to_string();

            let group_id = CPGroupId::new(group_name, 0, 0);
            let session = CPSession::new(
                CPSessionId::new(session_id, group_id),
                creation_time,
                expiration_time,
                endpoint_type,
                endpoint_name,
            );

            result.push(session);
            idx += 5;
        }

        tracing::debug!(group = %group_name, count = result.len(), "retrieved CP sessions");
        Ok(result)
    }

    /// Force closes a CP session.
    ///
    /// This operation is dangerous and should only be used when a session
    /// is stuck and cannot be closed normally. Any resources held by the
    /// session will be released.
    ///
    /// # Arguments
    ///
    /// * `group_name` - The name of the CP group containing the session
    /// * `session_id` - The ID of the session to close
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The CP Subsystem is not enabled on the cluster
    /// - The specified group or session does not exist
    /// - No connection to the cluster is available
    /// - A network error occurs
    pub async fn force_close_session(&self, group_name: &str, session_id: i64) -> Result<()> {
        const CP_SESSION_FORCE_CLOSE: i32 = 0x1F0200;

        let mut request = ClientMessage::new_request(CP_SESSION_FORCE_CLOSE);
        request.set_partition_id(PARTITION_ID_ANY);

        let name_bytes = BytesMut::from(group_name.as_bytes());
        request.add_frame(Frame::with_content(name_bytes));

        let session_bytes = BytesMut::from(session_id.to_le_bytes().as_slice());
        request.add_frame(Frame::with_content(session_bytes));

        let _response = self.connection_manager.send(request).await?;

        tracing::warn!(group = %group_name, session_id = %session_id, "force closed CP session");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cp_session_id() {
        let group_id = CPGroupId::new("test-group", 123, 456);
        let session_id = CPSessionId::new(789, group_id.clone());

        assert_eq!(session_id.session_id(), 789);
        assert_eq!(session_id.group_id(), &group_id);
    }

    #[test]
    fn test_cp_session_id_equality() {
        let group_id = CPGroupId::new("group", 1, 2);
        let id1 = CPSessionId::new(100, group_id.clone());
        let id2 = CPSessionId::new(100, group_id.clone());
        let id3 = CPSessionId::new(200, group_id);

        assert_eq!(id1, id2);
        assert_ne!(id1, id3);
    }

    #[test]
    fn test_cp_session() {
        let group_id = CPGroupId::new("test", 0, 1);
        let session_id = CPSessionId::new(42, group_id);
        let session = CPSession::new(
            session_id.clone(),
            1000,
            2000,
            CPSessionEndpointType::Client,
            "test-endpoint",
        );

        assert_eq!(session.id(), &session_id);
        assert_eq!(session.creation_time(), 1000);
        assert_eq!(session.expiration_time(), 2000);
        assert_eq!(session.endpoint_type(), CPSessionEndpointType::Client);
        assert_eq!(session.endpoint_name(), "test-endpoint");
    }

    #[test]
    fn test_cp_session_endpoint_type() {
        assert_ne!(CPSessionEndpointType::Client, CPSessionEndpointType::Server);

        let client_type = CPSessionEndpointType::Client;
        let server_type = CPSessionEndpointType::Server;
        assert_eq!(client_type, CPSessionEndpointType::Client);
        assert_eq!(server_type, CPSessionEndpointType::Server);
    }

    #[test]
    fn test_service_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<CPSessionManagementService>();
        assert_send_sync::<CPSessionId>();
        assert_send_sync::<CPSession>();
        assert_send_sync::<CPSessionEndpointType>();
    }
}
