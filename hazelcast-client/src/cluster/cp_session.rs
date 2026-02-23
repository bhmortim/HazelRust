//! CP Session management for Hazelcast client.
//!
//! This module provides both:
//! - **Runtime session lifecycle management** ([`CPSessionManager`]) — creates sessions,
//!   heartbeats them, generates thread IDs, and closes sessions on shutdown. Used
//!   internally by session-aware CP proxies (FencedLock, Semaphore, CountDownLatch).
//! - **Administrative operations** ([`CPSessionManagementService`]) — querying active
//!   sessions and force-closing stuck sessions.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use hazelcast_core::protocol::constants::{
    CP_SESSION_CLOSE_SESSION, CP_SESSION_CREATE_SESSION, CP_SESSION_GENERATE_THREAD_ID,
    CP_SESSION_HEARTBEAT, PARTITION_ID_ANY,
};
use hazelcast_core::protocol::{ClientMessage, Frame};
use hazelcast_core::{HazelcastError, Result};
use tokio::sync::RwLock;

use super::cp_management::CPGroupId;
use crate::connection::ConnectionManager;

/// Constant indicating no active CP session.
pub const NO_SESSION_ID: i64 = -1;

/// Default heartbeat interval for CP sessions (5 seconds).
const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);

// ── Session state per CP group ──────────────────────────────────────────

/// Internal state tracking a single CP session for a specific group.
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct SessionState {
    /// The server-assigned session ID.
    session_id: i64,
    /// Number of active acquire operations referencing this session.
    /// When this drops to 0, the session can be released.
    acquire_count: i64,
    /// Server-reported TTL in milliseconds for the session.
    ttl_ms: i64,
    /// Server-reported heartbeat interval in milliseconds.
    heartbeat_interval_ms: i64,
}

impl SessionState {
    fn new(session_id: i64, ttl_ms: i64, heartbeat_interval_ms: i64) -> Self {
        Self {
            session_id,
            acquire_count: 0,
            ttl_ms,
            heartbeat_interval_ms,
        }
    }
}

// ── CPSessionManager ────────────────────────────────────────────────────

/// Manages CP session lifecycle for the Hazelcast client.
///
/// `CPSessionManager` is responsible for:
/// - **Creating sessions** on demand when a CP proxy (FencedLock, Semaphore,
///   CountDownLatch) first needs one for a given CP group.
/// - **Heartbeating sessions** periodically to keep them alive on the server.
/// - **Generating unique thread IDs** via the CP protocol so that the server
///   can correctly track lock/semaphore ownership per client thread.
/// - **Closing sessions** gracefully on client shutdown.
///
/// Each CP group gets its own independent session. Sessions are created lazily
/// on first use and kept alive until the client shuts down or the session is
/// explicitly released.
///
/// # Thread Safety
///
/// `CPSessionManager` is `Send + Sync` and designed to be shared across
/// multiple proxy instances via `Arc`.
#[derive(Debug)]
pub struct CPSessionManager {
    connection_manager: Arc<ConnectionManager>,
    /// Map from CP group ID to session state.
    sessions: RwLock<HashMap<CPGroupId, SessionState>>,
    /// Monotonically increasing counter for generating locally-unique thread IDs
    /// when the server-side generate_thread_id call is unavailable.
    thread_id_counter: AtomicI64,
    /// Handle to the heartbeat background task so we can abort it on shutdown.
    heartbeat_handle: RwLock<Option<tokio::task::JoinHandle<()>>>,
}

impl CPSessionManager {
    /// Creates a new `CPSessionManager`.
    ///
    /// After creation, call [`start_heartbeat`] to begin the background
    /// heartbeat loop.
    pub fn new(connection_manager: Arc<ConnectionManager>) -> Self {
        Self {
            connection_manager,
            sessions: RwLock::new(HashMap::new()),
            thread_id_counter: AtomicI64::new(1),
            heartbeat_handle: RwLock::new(None),
        }
    }

    /// Starts the background heartbeat loop.
    ///
    /// The heartbeat task periodically sends `CP_SESSION_HEARTBEAT` messages
    /// for all active sessions to keep them alive on the server. This must
    /// be called once after construction; call [`shutdown`] to stop it.
    pub async fn start_heartbeat(self: &Arc<Self>) {
        let manager = Arc::clone(self);
        let handle = tokio::spawn(async move {
            manager.heartbeat_loop().await;
        });
        let mut guard = self.heartbeat_handle.write().await;
        *guard = Some(handle);
        tracing::debug!("CP session heartbeat loop started");
    }

    /// Acquires (or creates) a CP session for the given group.
    ///
    /// If a session already exists for the group, its acquire count is
    /// incremented and the existing session ID is returned. Otherwise a new
    /// session is created via `CP_SESSION_CREATE_SESSION`.
    ///
    /// # Arguments
    ///
    /// * `group_id` - The CP group to acquire a session for
    ///
    /// # Returns
    ///
    /// The session ID to use in subsequent CP operations.
    pub async fn acquire_session(&self, group_id: &CPGroupId) -> Result<i64> {
        // Fast path: session already exists
        {
            let mut sessions = self.sessions.write().await;
            if let Some(state) = sessions.get_mut(group_id) {
                state.acquire_count += 1;
                tracing::trace!(
                    group = %group_id.name(),
                    session_id = state.session_id,
                    acquire_count = state.acquire_count,
                    "reused existing CP session"
                );
                return Ok(state.session_id);
            }
        }

        // Slow path: create a new session
        let (session_id, ttl_ms, heartbeat_interval_ms) =
            self.create_session(group_id).await?;

        let mut sessions = self.sessions.write().await;
        // Double-check: another task might have raced us
        if let Some(state) = sessions.get_mut(group_id) {
            state.acquire_count += 1;
            return Ok(state.session_id);
        }

        let mut state = SessionState::new(session_id, ttl_ms, heartbeat_interval_ms);
        state.acquire_count = 1;
        sessions.insert(group_id.clone(), state);

        tracing::info!(
            group = %group_id.name(),
            session_id,
            ttl_ms,
            heartbeat_interval_ms,
            "created new CP session"
        );

        Ok(session_id)
    }

    /// Releases a reference to a CP session for the given group.
    ///
    /// Decrements the acquire count. The session remains alive (heartbeated)
    /// even when the count reaches zero — it will only be closed on
    /// client shutdown.
    pub async fn release_session(&self, group_id: &CPGroupId, session_id: i64) {
        let mut sessions = self.sessions.write().await;
        if let Some(state) = sessions.get_mut(group_id) {
            if state.session_id == session_id {
                state.acquire_count = (state.acquire_count - 1).max(0);
                tracing::trace!(
                    group = %group_id.name(),
                    session_id,
                    acquire_count = state.acquire_count,
                    "released CP session reference"
                );
            }
        }
    }

    /// Invalidates a session, removing it from the local cache.
    ///
    /// This is called when the server reports that a session has expired
    /// or is invalid (e.g., `SessionExpiredError`). The next acquire
    /// will create a fresh session.
    pub async fn invalidate_session(&self, group_id: &CPGroupId, session_id: i64) {
        let mut sessions = self.sessions.write().await;
        if let Some(state) = sessions.get(group_id) {
            if state.session_id == session_id {
                tracing::warn!(
                    group = %group_id.name(),
                    session_id,
                    "invalidated CP session"
                );
                sessions.remove(group_id);
            }
        }
    }

    /// Returns the current session ID for a group, or `NO_SESSION_ID` if none.
    pub async fn get_session_id(&self, group_id: &CPGroupId) -> i64 {
        let sessions = self.sessions.read().await;
        sessions
            .get(group_id)
            .map(|s| s.session_id)
            .unwrap_or(NO_SESSION_ID)
    }

    /// Generates a unique thread ID via the CP protocol.
    ///
    /// Sends a `CP_SESSION_GENERATE_THREAD_ID` request to the cluster to
    /// obtain a cluster-wide unique thread identifier. Falls back to a
    /// local counter if the request fails.
    ///
    /// # Arguments
    ///
    /// * `group_id` - The CP group to generate the thread ID for
    pub async fn generate_thread_id(&self, group_id: &CPGroupId) -> Result<i64> {
        match self.request_thread_id(group_id).await {
            Ok(id) => {
                tracing::debug!(
                    group = %group_id.name(),
                    thread_id = id,
                    "generated CP thread ID from server"
                );
                Ok(id)
            }
            Err(e) => {
                // Fallback to local counter
                let local_id = self.thread_id_counter.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    group = %group_id.name(),
                    error = %e,
                    fallback_thread_id = local_id,
                    "failed to generate CP thread ID from server, using local fallback"
                );
                Ok(local_id)
            }
        }
    }

    /// Shuts down the session manager, closing all active sessions and
    /// stopping the heartbeat loop.
    pub async fn shutdown(&self) {
        // Stop heartbeat task
        {
            let mut handle = self.heartbeat_handle.write().await;
            if let Some(h) = handle.take() {
                h.abort();
                tracing::debug!("CP session heartbeat loop stopped");
            }
        }

        // Close all active sessions
        let sessions: Vec<(CPGroupId, i64)> = {
            let sessions = self.sessions.read().await;
            sessions
                .iter()
                .map(|(gid, state)| (gid.clone(), state.session_id))
                .collect()
        };

        for (group_id, session_id) in &sessions {
            if let Err(e) = self.close_session(group_id, *session_id).await {
                tracing::warn!(
                    group = %group_id.name(),
                    session_id,
                    error = %e,
                    "failed to close CP session during shutdown"
                );
            }
        }

        // Clear local state
        self.sessions.write().await.clear();

        if !sessions.is_empty() {
            tracing::info!(
                count = sessions.len(),
                "closed CP sessions during shutdown"
            );
        }
    }

    /// Returns the number of active sessions.
    pub async fn session_count(&self) -> usize {
        self.sessions.read().await.len()
    }

    // ── Private protocol methods ────────────────────────────────────────

    /// Creates a new CP session on the server.
    ///
    /// Returns `(session_id, ttl_ms, heartbeat_interval_ms)`.
    async fn create_session(&self, group_id: &CPGroupId) -> Result<(i64, i64, i64)> {
        let mut request = ClientMessage::new_request(CP_SESSION_CREATE_SESSION);
        request.set_partition_id(PARTITION_ID_ANY);

        // Encode: group name, then group seed and id
        Self::encode_group_id(&mut request, group_id);

        // Encode: endpoint name (client name)
        let endpoint_name = format!("rust-client-{}", std::process::id());
        let name_bytes = BytesMut::from(endpoint_name.as_bytes());
        request.add_frame(Frame::with_content(name_bytes));

        let response = self.connection_manager.send(request).await?;

        // Decode response: initial frame contains session_id(i64), ttl(i64), heartbeat_interval(i64)
        let frames = response.frames();
        if frames.is_empty() {
            return Err(HazelcastError::Protocol(
                "empty create session response".to_string(),
            ));
        }

        let initial_frame = &frames[0];
        let content = initial_frame.content();
        let header_size = hazelcast_core::protocol::constants::RESPONSE_HEADER_SIZE;

        if content.len() < header_size + 24 {
            return Err(HazelcastError::Protocol(
                "create session response too short".to_string(),
            ));
        }

        let offset = header_size;
        let session_id = i64::from_le_bytes(
            content[offset..offset + 8]
                .try_into()
                .map_err(|_| HazelcastError::Serialization("invalid session_id".to_string()))?,
        );
        let ttl_ms = i64::from_le_bytes(
            content[offset + 8..offset + 16]
                .try_into()
                .map_err(|_| HazelcastError::Serialization("invalid ttl".to_string()))?,
        );
        let heartbeat_interval_ms = i64::from_le_bytes(
            content[offset + 16..offset + 24]
                .try_into()
                .map_err(|_| {
                    HazelcastError::Serialization("invalid heartbeat interval".to_string())
                })?,
        );

        Ok((session_id, ttl_ms, heartbeat_interval_ms))
    }

    /// Sends a heartbeat for a specific session.
    async fn heartbeat_session(&self, group_id: &CPGroupId, session_id: i64) -> Result<()> {
        let mut request = ClientMessage::new_request(CP_SESSION_HEARTBEAT);
        request.set_partition_id(PARTITION_ID_ANY);

        // Encode: group id
        Self::encode_group_id(&mut request, group_id);

        // Encode: session id
        let session_bytes = BytesMut::from(session_id.to_le_bytes().as_slice());
        request.add_frame(Frame::with_content(session_bytes));

        let _response = self.connection_manager.send(request).await?;

        tracing::trace!(
            group = %group_id.name(),
            session_id,
            "heartbeat sent for CP session"
        );
        Ok(())
    }

    /// Closes a CP session on the server.
    async fn close_session(&self, group_id: &CPGroupId, session_id: i64) -> Result<()> {
        let mut request = ClientMessage::new_request(CP_SESSION_CLOSE_SESSION);
        request.set_partition_id(PARTITION_ID_ANY);

        // Encode: group id
        Self::encode_group_id(&mut request, group_id);

        // Encode: session id
        let session_bytes = BytesMut::from(session_id.to_le_bytes().as_slice());
        request.add_frame(Frame::with_content(session_bytes));

        let _response = self.connection_manager.send(request).await?;

        tracing::debug!(
            group = %group_id.name(),
            session_id,
            "closed CP session"
        );
        Ok(())
    }

    /// Requests a unique thread ID from the server.
    async fn request_thread_id(&self, group_id: &CPGroupId) -> Result<i64> {
        let mut request = ClientMessage::new_request(CP_SESSION_GENERATE_THREAD_ID);
        request.set_partition_id(PARTITION_ID_ANY);

        // Encode: group id
        Self::encode_group_id(&mut request, group_id);

        let response = self.connection_manager.send(request).await?;

        let frames = response.frames();
        if frames.is_empty() {
            return Err(HazelcastError::Protocol(
                "empty generate_thread_id response".to_string(),
            ));
        }

        let initial_frame = &frames[0];
        let content = initial_frame.content();
        let header_size = hazelcast_core::protocol::constants::RESPONSE_HEADER_SIZE;

        if content.len() < header_size + 8 {
            return Err(HazelcastError::Protocol(
                "generate_thread_id response too short".to_string(),
            ));
        }

        let thread_id = i64::from_le_bytes(
            content[header_size..header_size + 8]
                .try_into()
                .map_err(|_| {
                    HazelcastError::Serialization("invalid thread_id".to_string())
                })?,
        );

        Ok(thread_id)
    }

    /// Encodes a CPGroupId into a request message.
    fn encode_group_id(message: &mut ClientMessage, group_id: &CPGroupId) {
        // Group name
        let name_bytes = BytesMut::from(group_id.name().as_bytes());
        message.add_frame(Frame::with_content(name_bytes));

        // Group seed
        let seed_bytes = BytesMut::from(group_id.seed().to_le_bytes().as_slice());
        message.add_frame(Frame::with_content(seed_bytes));

        // Group id
        let id_bytes = BytesMut::from(group_id.id().to_le_bytes().as_slice());
        message.add_frame(Frame::with_content(id_bytes));
    }

    /// The background heartbeat loop. Runs until aborted.
    async fn heartbeat_loop(&self) {
        let interval = DEFAULT_HEARTBEAT_INTERVAL;
        loop {
            tokio::time::sleep(interval).await;

            // Check if shutdown requested
            if self.connection_manager.is_shutdown_requested() {
                tracing::debug!("heartbeat loop exiting due to shutdown");
                break;
            }

            // Snapshot current sessions
            let session_snapshot: Vec<(CPGroupId, i64)> = {
                let sessions = self.sessions.read().await;
                sessions
                    .iter()
                    .map(|(gid, state)| (gid.clone(), state.session_id))
                    .collect()
            };

            if session_snapshot.is_empty() {
                continue;
            }

            // Heartbeat each session
            for (group_id, session_id) in &session_snapshot {
                if let Err(e) = self.heartbeat_session(group_id, *session_id).await {
                    tracing::warn!(
                        group = %group_id.name(),
                        session_id,
                        error = %e,
                        "CP session heartbeat failed"
                    );

                    // If the session is expired on the server, invalidate locally
                    if is_session_expired_error(&e) {
                        self.invalidate_session(group_id, *session_id).await;
                    }
                }
            }
        }
    }
}

/// Returns true if the error indicates that a CP session has expired.
fn is_session_expired_error(error: &HazelcastError) -> bool {
    match error {
        HazelcastError::Server { code, .. } => {
            // ServerErrorCode 76 = CpSessionNotFound
            code.value() == 76
        }
        HazelcastError::IllegalState(msg) => {
            msg.contains("session") && (msg.contains("expired") || msg.contains("not found"))
        }
        _ => false,
    }
}

// ── CPSessionId ─────────────────────────────────────────────────────────

/// Identifier for a CP session.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CPSessionId {
    session_id: i64,
    group_id: CPGroupId,
}

impl CPSessionId {
    /// Creates a new CPSessionId.
    pub fn new(session_id: i64, group_id: CPGroupId) -> Self {
        Self {
            session_id,
            group_id,
        }
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

// ── CPSessionEndpointType ───────────────────────────────────────────────

/// Type of endpoint owning a CP session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CPSessionEndpointType {
    /// The session is owned by a client.
    Client,
    /// The session is owned by a server member.
    Server,
}

// ── CPSession (info struct) ─────────────────────────────────────────────

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

// ── CPSessionManagementService (admin API) ──────────────────────────────

/// Service for managing CP sessions (administrative operations).
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
        // Note: This uses the same message type as get_all_sessions,
        // not the lifecycle CREATE_SESSION constant.
        const CP_SESSION_GET_ALL: i32 = 0x1F0500;

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

// ── Tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── CPSessionId tests ───────────────────────────────────────────

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

    // ── CPSession tests ─────────────────────────────────────────────

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

    // ── Send+Sync tests ─────────────────────────────────────────────

    #[test]
    fn test_service_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<CPSessionManagementService>();
        assert_send_sync::<CPSessionId>();
        assert_send_sync::<CPSession>();
        assert_send_sync::<CPSessionEndpointType>();
        assert_send_sync::<CPSessionManager>();
    }

    // ── CPSessionManager unit tests ─────────────────────────────────

    #[test]
    fn test_no_session_id_constant() {
        assert_eq!(NO_SESSION_ID, -1);
    }

    #[test]
    fn test_session_state_new() {
        let state = SessionState::new(42, 30000, 5000);
        assert_eq!(state.session_id, 42);
        assert_eq!(state.acquire_count, 0);
        assert_eq!(state.ttl_ms, 30000);
        assert_eq!(state.heartbeat_interval_ms, 5000);
    }

    #[test]
    fn test_is_session_expired_error_illegal_state() {
        let err = HazelcastError::IllegalState("session expired on server".to_string());
        assert!(is_session_expired_error(&err));

        let err = HazelcastError::IllegalState("session not found".to_string());
        assert!(is_session_expired_error(&err));

        let err = HazelcastError::IllegalState("something else".to_string());
        assert!(!is_session_expired_error(&err));
    }

    #[test]
    fn test_is_session_expired_error_other() {
        let err = HazelcastError::Connection("timeout".to_string());
        assert!(!is_session_expired_error(&err));

        let err = HazelcastError::Timeout("request timed out".to_string());
        assert!(!is_session_expired_error(&err));
    }

    #[test]
    fn test_encode_group_id() {
        let group_id = CPGroupId::new("my-group", 10, 20);
        let mut message = ClientMessage::new_request(0x1F0100);
        CPSessionManager::encode_group_id(&mut message, &group_id);

        let frames = message.frames();
        // Initial frame + 3 data frames (name, seed, id)
        assert!(frames.len() >= 4);

        // Verify name frame
        assert_eq!(&frames[1].content[..], b"my-group");

        // Verify seed frame
        assert_eq!(frames[2].content.len(), 8);
        assert_eq!(
            i64::from_le_bytes(frames[2].content[..8].try_into().unwrap()),
            10i64
        );

        // Verify id frame
        assert_eq!(frames[3].content.len(), 8);
        assert_eq!(
            i64::from_le_bytes(frames[3].content[..8].try_into().unwrap()),
            20i64
        );
    }

    #[tokio::test]
    async fn test_cp_session_manager_initial_state() {
        let cm = ConnectionManager::from_config(
            crate::config::ClientConfig::builder()
                .cluster_name("test")
                .build()
                .unwrap(),
        );
        let manager = CPSessionManager::new(Arc::new(cm));

        assert_eq!(manager.session_count().await, 0);

        let group_id = CPGroupId::new("default", 0, 0);
        assert_eq!(manager.get_session_id(&group_id).await, NO_SESSION_ID);
    }

    #[tokio::test]
    async fn test_cp_session_manager_invalidate_nonexistent() {
        let cm = ConnectionManager::from_config(
            crate::config::ClientConfig::builder()
                .cluster_name("test")
                .build()
                .unwrap(),
        );
        let manager = CPSessionManager::new(Arc::new(cm));

        let group_id = CPGroupId::new("default", 0, 0);
        // Should not panic
        manager.invalidate_session(&group_id, 42).await;
        assert_eq!(manager.session_count().await, 0);
    }

    #[tokio::test]
    async fn test_cp_session_manager_release_nonexistent() {
        let cm = ConnectionManager::from_config(
            crate::config::ClientConfig::builder()
                .cluster_name("test")
                .build()
                .unwrap(),
        );
        let manager = CPSessionManager::new(Arc::new(cm));

        let group_id = CPGroupId::new("default", 0, 0);
        // Should not panic
        manager.release_session(&group_id, 42).await;
    }

    #[tokio::test]
    async fn test_cp_session_manager_shutdown_empty() {
        let cm = ConnectionManager::from_config(
            crate::config::ClientConfig::builder()
                .cluster_name("test")
                .build()
                .unwrap(),
        );
        let manager = CPSessionManager::new(Arc::new(cm));

        // Should not panic when shutting down with no sessions
        manager.shutdown().await;
        assert_eq!(manager.session_count().await, 0);
    }
}
