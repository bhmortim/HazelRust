//! Distributed fenced lock proxy implementation (CP subsystem).
//!
//! Acquires a CP session for the lock's Raft group and a server-generated thread
//! id, then sends `lock`/`tryLock`/`unlock` with `sessionId` + `threadId` +
//! invocation UUID in the initial frame (see `cp_group`). `lock`/`tryLock` return
//! a monotonically increasing fence token.

use std::sync::Arc;
use std::time::Duration;

use hazelcast_core::protocol::constants::*;
use hazelcast_core::{ClientMessage, HazelcastError, Result};
use tokio::sync::OnceCell;

use crate::cluster::{CPGroupId, CPSessionManager, NO_SESSION_ID};
use crate::connection::ConnectionManager;
use crate::proxy::cp_group::{self, RaftGroupId};

/// A distributed fenced lock backed by the CP subsystem.
///
/// Requires the CP subsystem (an Enterprise feature in Hazelcast 5.7+).
#[derive(Debug)]
pub struct FencedLock {
    name: String,
    connection_manager: Arc<ConnectionManager>,
    session_manager: Option<Arc<CPSessionManager>>,
    group_id: Arc<OnceCell<RaftGroupId>>,
    session_id: i64,
    thread_id: i64,
}

#[derive(Debug, Clone, Copy)]
struct LockOwnershipState {
    fence: i64,
    lock_count: i32,
}

impl FencedLock {
    pub(crate) fn new(name: String, connection_manager: Arc<ConnectionManager>) -> Self {
        Self {
            name,
            connection_manager,
            session_manager: None,
            group_id: Arc::new(OnceCell::new()),
            session_id: NO_SESSION_ID,
            thread_id: 0,
        }
    }

    pub(crate) fn with_session_manager(
        name: String,
        connection_manager: Arc<ConnectionManager>,
        session_manager: Arc<CPSessionManager>,
    ) -> Self {
        Self {
            name,
            connection_manager,
            session_manager: Some(session_manager),
            group_id: Arc::new(OnceCell::new()),
            session_id: NO_SESSION_ID,
            thread_id: 0,
        }
    }

    /// Returns the name of this FencedLock.
    pub fn name(&self) -> &str {
        &self.name
    }

    async fn group(&self) -> Result<RaftGroupId> {
        let g = self
            .group_id
            .get_or_try_init(|| cp_group::resolve_group(&self.connection_manager, &self.name))
            .await?;
        Ok(g.clone())
    }

    /// Resolves the group and ensures a session + thread id are established.
    async fn ensure(&mut self) -> Result<(RaftGroupId, i64, i64)> {
        let g = self.group().await?;
        let sm = self.session_manager.clone().ok_or_else(|| {
            HazelcastError::Configuration("FencedLock requires the CP session manager".to_string())
        })?;
        let cpgid = CPGroupId::new(g.name.clone(), g.seed, g.id);
        if self.session_id == NO_SESSION_ID {
            self.session_id = sm.acquire_session(&cpgid).await?;
        }
        if self.thread_id <= 0 {
            self.thread_id = sm.generate_thread_id(&cpgid).await?;
        }
        Ok((g, self.session_id, self.thread_id))
    }

    /// Acquires the lock, blocking until held, and returns the fence token.
    pub async fn lock(&mut self) -> Result<i64> {
        let (g, sid, tid) = self.ensure().await?;
        let (msb, lsb) = cp_group::random_uuid();
        let mut m = ClientMessage::create_for_encode_any_partition(CP_FENCED_LOCK_LOCK);
        cp_group::put_long(&mut m, sid);
        cp_group::put_long(&mut m, tid);
        cp_group::put_uuid(&mut m, msb, lsb);
        cp_group::encode_group_id(&mut m, &g);
        m.add_frame(cp_group::string_frame(cp_group::object_name(&self.name)));
        let r = self.connection_manager.invoke_on_random(m).await?;
        cp_group::decode_long(&r)
    }

    /// Tries to acquire the lock within `timeout`. Returns the fence on success.
    pub async fn try_lock(&mut self, timeout: Duration) -> Result<Option<i64>> {
        let (g, sid, tid) = self.ensure().await?;
        let (msb, lsb) = cp_group::random_uuid();
        let mut m = ClientMessage::create_for_encode_any_partition(CP_FENCED_LOCK_TRY_LOCK);
        cp_group::put_long(&mut m, sid);
        cp_group::put_long(&mut m, tid);
        cp_group::put_uuid(&mut m, msb, lsb);
        cp_group::put_long(&mut m, timeout.as_millis() as i64);
        cp_group::encode_group_id(&mut m, &g);
        m.add_frame(cp_group::string_frame(cp_group::object_name(&self.name)));
        let r = self.connection_manager.invoke_on_random(m).await?;
        let fence = cp_group::decode_long(&r)?;
        Ok(if fence > 0 { Some(fence) } else { None })
    }

    /// Releases the lock. The `fence` argument is accepted for API symmetry; the
    /// server identifies the holder by session + thread.
    pub async fn unlock(&mut self, _fence: i64) -> Result<()> {
        let (g, sid, tid) = self.ensure().await?;
        let (msb, lsb) = cp_group::random_uuid();
        let mut m = ClientMessage::create_for_encode_any_partition(CP_FENCED_LOCK_UNLOCK);
        cp_group::put_long(&mut m, sid);
        cp_group::put_long(&mut m, tid);
        cp_group::put_uuid(&mut m, msb, lsb);
        cp_group::encode_group_id(&mut m, &g);
        m.add_frame(cp_group::string_frame(cp_group::object_name(&self.name)));
        let r = self.connection_manager.invoke_on_random(m).await?;
        let _ = cp_group::decode_bool(&r)?;
        if let Some(ref sm) = self.session_manager {
            let cpgid = CPGroupId::new(g.name.clone(), g.seed, g.id);
            sm.release_session(&cpgid, sid).await;
        }
        Ok(())
    }

    /// Returns whether the lock is currently held by any thread.
    pub async fn is_locked(&self) -> Result<bool> {
        Ok(self.ownership().await?.lock_count > 0)
    }

    /// Returns the reentrant lock count (0 if not held).
    pub async fn get_lock_count(&self) -> Result<i32> {
        Ok(self.ownership().await?.lock_count)
    }

    /// Returns the current fence token (0 if not held).
    pub async fn get_fence(&self) -> Result<i64> {
        Ok(self.ownership().await?.fence)
    }

    async fn ownership(&self) -> Result<LockOwnershipState> {
        let g = self.group().await?;
        let mut m =
            ClientMessage::create_for_encode_any_partition(CP_FENCED_LOCK_GET_LOCK_OWNERSHIP_STATE);
        cp_group::encode_group_id(&mut m, &g);
        m.add_frame(cp_group::string_frame(cp_group::object_name(&self.name)));
        let r = self.connection_manager.invoke_on_random(m).await?;
        // The RaftLockOwnershipState is returned as a data-structure frame:
        // [fence(8) sessionId(8) threadId(8) lockCount(4)] = 28 bytes. Find it
        // among the frames after the response header.
        let f = r
            .frames()
            .first()
            .ok_or_else(|| HazelcastError::Serialization("empty response".to_string()))?;
        let o = RESPONSE_HEADER_SIZE;
        if f.content.len() < o + 28 {
            return Err(HazelcastError::Protocol(
                "lock ownership state too short".to_string(),
            ));
        }
        let fence = i64::from_le_bytes(f.content[o..o + 8].try_into().unwrap());
        let lock_count = i32::from_le_bytes(f.content[o + 8..o + 12].try_into().unwrap());
        Ok(LockOwnershipState { fence, lock_count })
    }
}

impl Clone for FencedLock {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            connection_manager: Arc::clone(&self.connection_manager),
            session_manager: self.session_manager.clone(),
            group_id: Arc::clone(&self.group_id),
            session_id: self.session_id,
            thread_id: self.thread_id,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fenced_lock_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<FencedLock>();
    }

    #[test]
    fn test_fenced_lock_clone() {
        fn assert_clone<T: Clone>() {}
        assert_clone::<FencedLock>();
    }

    #[test]
    fn test_fenced_lock_name() {
        let cm = Arc::new(ConnectionManager::from_config(
            crate::config::ClientConfig::builder()
                .cluster_name("test")
                .build()
                .unwrap(),
        ));
        let lock = FencedLock::new("my-lock".to_string(), cm);
        assert_eq!(lock.name(), "my-lock");
    }
}
