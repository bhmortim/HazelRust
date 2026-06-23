//! Distributed semaphore proxy implementation (CP subsystem).
//!
//! Acquire/release/drain/change use a CP session + server thread id with the
//! session id, thread id and invocation UUID in the initial frame (see
//! `cp_group`). `init` and `available_permits` are sessionless.

use std::sync::Arc;
use std::time::Duration;

use hazelcast_core::protocol::constants::*;
use hazelcast_core::{ClientMessage, HazelcastError, Result};
use tokio::sync::OnceCell;

use crate::cluster::{CPGroupId, CPSessionManager, NO_SESSION_ID};
use crate::connection::ConnectionManager;
use crate::proxy::cp_group::{self, RaftGroupId};

/// A distributed counting semaphore backed by the CP subsystem.
///
/// Requires the CP subsystem (an Enterprise feature in Hazelcast 5.7+).
#[derive(Debug)]
pub struct Semaphore {
    name: String,
    connection_manager: Arc<ConnectionManager>,
    session_manager: Option<Arc<CPSessionManager>>,
    group_id: Arc<OnceCell<RaftGroupId>>,
    session_id: i64,
    thread_id: i64,
}

impl Semaphore {
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

    /// Returns the name of this Semaphore.
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

    async fn ensure(&mut self) -> Result<(RaftGroupId, i64, i64)> {
        let g = self.group().await?;
        let sm = self.session_manager.clone().ok_or_else(|| {
            HazelcastError::Configuration("Semaphore requires the CP session manager".to_string())
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

    /// Initializes the semaphore with `permits`. Returns `false` if already set.
    pub async fn init(&self, permits: i32) -> Result<bool> {
        let g = self.group().await?;
        let mut m = ClientMessage::create_for_encode_any_partition(CP_SEMAPHORE_INIT);
        cp_group::put_int(&mut m, permits);
        cp_group::encode_group_id(&mut m, &g);
        m.add_frame(cp_group::string_frame(cp_group::object_name(&self.name)));
        let r = self.connection_manager.invoke_on_random(m).await?;
        cp_group::decode_bool(&r)
    }

    /// Acquires `permits`, blocking until available.
    pub async fn acquire(&mut self, permits: i32) -> Result<()> {
        self.acquire_inner(permits, -1).await.map(|_| ())
    }

    /// Tries to acquire `permits` within `timeout`. Returns whether acquired.
    pub async fn try_acquire(&mut self, permits: i32, timeout: Duration) -> Result<bool> {
        self.acquire_inner(permits, timeout.as_millis() as i64).await
    }

    async fn acquire_inner(&mut self, permits: i32, timeout_ms: i64) -> Result<bool> {
        let (g, sid, tid) = self.ensure().await?;
        let (msb, lsb) = cp_group::random_uuid();
        let mut m = ClientMessage::create_for_encode_any_partition(CP_SEMAPHORE_ACQUIRE);
        cp_group::put_long(&mut m, sid);
        cp_group::put_long(&mut m, tid);
        cp_group::put_uuid(&mut m, msb, lsb);
        cp_group::put_int(&mut m, permits);
        cp_group::put_long(&mut m, timeout_ms);
        cp_group::encode_group_id(&mut m, &g);
        m.add_frame(cp_group::string_frame(cp_group::object_name(&self.name)));
        let r = self.connection_manager.invoke_on_random(m).await?;
        cp_group::decode_bool(&r)
    }

    /// Releases `permits`.
    pub async fn release(&mut self, permits: i32) -> Result<()> {
        let (g, sid, tid) = self.ensure().await?;
        let (msb, lsb) = cp_group::random_uuid();
        let mut m = ClientMessage::create_for_encode_any_partition(CP_SEMAPHORE_RELEASE);
        cp_group::put_long(&mut m, sid);
        cp_group::put_long(&mut m, tid);
        cp_group::put_uuid(&mut m, msb, lsb);
        cp_group::put_int(&mut m, permits);
        cp_group::encode_group_id(&mut m, &g);
        m.add_frame(cp_group::string_frame(cp_group::object_name(&self.name)));
        self.connection_manager.invoke_on_random(m).await?;
        Ok(())
    }

    /// Returns the number of available permits.
    pub async fn available_permits(&self) -> Result<i32> {
        let g = self.group().await?;
        let mut m = ClientMessage::create_for_encode_any_partition(CP_SEMAPHORE_AVAILABLE_PERMITS);
        cp_group::encode_group_id(&mut m, &g);
        m.add_frame(cp_group::string_frame(cp_group::object_name(&self.name)));
        let r = self.connection_manager.invoke_on_random(m).await?;
        cp_group::decode_int(&r)
    }

    /// Acquires and returns all immediately-available permits.
    pub async fn drain_permits(&mut self) -> Result<i32> {
        let (g, sid, tid) = self.ensure().await?;
        let (msb, lsb) = cp_group::random_uuid();
        let mut m = ClientMessage::create_for_encode_any_partition(CP_SEMAPHORE_DRAIN);
        cp_group::put_long(&mut m, sid);
        cp_group::put_long(&mut m, tid);
        cp_group::put_uuid(&mut m, msb, lsb);
        cp_group::encode_group_id(&mut m, &g);
        m.add_frame(cp_group::string_frame(cp_group::object_name(&self.name)));
        let r = self.connection_manager.invoke_on_random(m).await?;
        cp_group::decode_int(&r)
    }

    /// Reduces the available permits by `reduction`.
    pub async fn reduce_permits(&mut self, reduction: i32) -> Result<()> {
        self.change(-reduction).await
    }

    /// Increases the available permits by `increase`.
    pub async fn increase_permits(&mut self, increase: i32) -> Result<()> {
        self.change(increase).await
    }

    async fn change(&mut self, delta: i32) -> Result<()> {
        let (g, sid, tid) = self.ensure().await?;
        let (msb, lsb) = cp_group::random_uuid();
        let mut m = ClientMessage::create_for_encode_any_partition(CP_SEMAPHORE_CHANGE);
        cp_group::put_long(&mut m, sid);
        cp_group::put_long(&mut m, tid);
        cp_group::put_uuid(&mut m, msb, lsb);
        cp_group::put_int(&mut m, delta);
        cp_group::encode_group_id(&mut m, &g);
        m.add_frame(cp_group::string_frame(cp_group::object_name(&self.name)));
        self.connection_manager.invoke_on_random(m).await?;
        Ok(())
    }
}

impl Clone for Semaphore {
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
    fn test_semaphore_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Semaphore>();
    }

    #[test]
    fn test_semaphore_clone() {
        fn assert_clone<T: Clone>() {}
        assert_clone::<Semaphore>();
    }

    #[test]
    fn test_semaphore_name() {
        let cm = Arc::new(ConnectionManager::from_config(
            crate::config::ClientConfig::builder()
                .cluster_name("test")
                .build()
                .unwrap(),
        ));
        let sem = Semaphore::new("s".to_string(), cm);
        assert_eq!(sem.name(), "s");
    }
}
