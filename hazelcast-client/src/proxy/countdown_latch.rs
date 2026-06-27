//! Distributed countdown latch proxy implementation (CP subsystem).
//!
//! Every request carries the resolved Raft `groupId` (see `cp_group`); fixed
//! params (count, invocation UUID, expected round, timeout) live in the initial
//! frame. `count_down` first reads the current round via `getRound`, then issues
//! an idempotent `countDown(uuid, round)`.

use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use hazelcast_core::protocol::constants::*;
use hazelcast_core::protocol::Frame;
use hazelcast_core::{ClientMessage, Result};
use tokio::sync::OnceCell;

use crate::cluster::CPSessionManager;
use crate::connection::ConnectionManager;
use crate::proxy::cp_group::{self, RaftGroupId};

/// A distributed countdown latch backed by the CP subsystem.
///
/// Requires the CP subsystem (an Enterprise feature in Hazelcast 5.7+).
#[derive(Debug)]
pub struct CountDownLatch {
    name: String,
    connection_manager: Arc<ConnectionManager>,
    #[allow(dead_code)]
    session_manager: Option<Arc<CPSessionManager>>,
    group_id: Arc<OnceCell<RaftGroupId>>,
}

impl CountDownLatch {
    /// Creates a new CountDownLatch proxy.
    pub(crate) fn new(name: String, connection_manager: Arc<ConnectionManager>) -> Self {
        Self {
            name,
            connection_manager,
            session_manager: None,
            group_id: Arc::new(OnceCell::new()),
        }
    }

    /// Creates a new CountDownLatch proxy with a CP session manager.
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
        }
    }

    /// Returns the name of this CountDownLatch.
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

    /// Sets the count to the given value if the current count is zero.
    pub async fn try_set_count(&self, count: i32) -> Result<bool> {
        let g = self.group().await?;
        let mut m =
            ClientMessage::create_for_encode_any_partition(CP_COUNTDOWN_LATCH_TRY_SET_COUNT);
        cp_group::put_int(&mut m, count);
        cp_group::encode_group_id(&mut m, &g);
        m.add_frame(cp_group::string_frame(cp_group::object_name(&self.name)));
        let r = self.connection_manager.invoke_on_random(m).await?;
        cp_group::decode_bool(&r)
    }

    /// Returns the current count.
    pub async fn get_count(&self) -> Result<i32> {
        let g = self.group().await?;
        let mut m = ClientMessage::create_for_encode_any_partition(CP_COUNTDOWN_LATCH_GET_COUNT);
        cp_group::encode_group_id(&mut m, &g);
        m.add_frame(cp_group::string_frame(cp_group::object_name(&self.name)));
        let r = self.connection_manager.invoke_on_random(m).await?;
        cp_group::decode_int(&r)
    }

    /// Returns the current round (used internally to make `count_down` idempotent).
    pub async fn get_round(&self) -> Result<i32> {
        let g = self.group().await?;
        let mut m = ClientMessage::create_for_encode_any_partition(CP_COUNTDOWN_LATCH_GET_ROUND);
        cp_group::encode_group_id(&mut m, &g);
        m.add_frame(cp_group::string_frame(cp_group::object_name(&self.name)));
        let r = self.connection_manager.invoke_on_random(m).await?;
        cp_group::decode_int(&r)
    }

    /// Decrements the count of the latch, releasing waiters when it reaches zero.
    pub async fn count_down(&self) -> Result<()> {
        let round = self.get_round().await?;
        let (msb, lsb) = cp_group::random_uuid();
        let g = self.group().await?;
        let mut m = ClientMessage::create_for_encode_any_partition(CP_COUNTDOWN_LATCH_COUNT_DOWN);
        cp_group::put_uuid(&mut m, msb, lsb);
        cp_group::put_int(&mut m, round);
        cp_group::encode_group_id(&mut m, &g);
        m.add_frame(cp_group::string_frame(cp_group::object_name(&self.name)));
        self.connection_manager.invoke_on_random(m).await?;
        Ok(())
    }

    /// Waits until the latch counts down to zero, or the timeout expires.
    ///
    /// Returns `true` if the count reached zero, `false` if the timeout expired.
    pub async fn await_latch(&self, timeout: Duration) -> Result<bool> {
        let (msb, lsb) = cp_group::random_uuid();
        let g = self.group().await?;
        let mut m = ClientMessage::create_for_encode_any_partition(CP_COUNTDOWN_LATCH_AWAIT);
        cp_group::put_uuid(&mut m, msb, lsb);
        cp_group::put_long(&mut m, timeout.as_millis() as i64);
        cp_group::encode_group_id(&mut m, &g);
        m.add_frame(cp_group::string_frame(cp_group::object_name(&self.name)));
        let r = self.connection_manager.invoke_on_random(m).await?;
        cp_group::decode_bool(&r)
    }

    #[allow(dead_code)]
    fn string_frame(s: &str) -> Frame {
        Frame::with_content(BytesMut::from(s.as_bytes()))
    }
}

impl Clone for CountDownLatch {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            connection_manager: Arc::clone(&self.connection_manager),
            session_manager: self.session_manager.clone(),
            group_id: Arc::clone(&self.group_id),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_countdown_latch_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<CountDownLatch>();
    }

    #[test]
    fn test_countdown_latch_clone() {
        fn assert_clone<T: Clone>() {}
        assert_clone::<CountDownLatch>();
    }

    #[test]
    fn test_string_frame() {
        let frame = CountDownLatch::string_frame("test-latch");
        assert_eq!(&frame.content[..], b"test-latch");
    }

    #[test]
    fn test_name_and_group_parsing() {
        assert_eq!(cp_group::object_name("latch@grp"), "latch");
        assert_eq!(cp_group::group_name("latch@grp"), "grp");
        assert_eq!(cp_group::group_name("latch"), "default");
    }

    #[test]
    fn test_uuid_is_17_bytes_in_initial_frame() {
        let mut m = ClientMessage::create_for_encode_any_partition(CP_COUNTDOWN_LATCH_COUNT_DOWN);
        let before = m.frames()[0].content.len();
        cp_group::put_uuid(&mut m, 7, 42);
        let after = m.frames()[0].content.len();
        assert_eq!(after - before, 17);
    }

    #[test]
    fn test_countdown_latch_new_has_no_session() {
        let cm = ConnectionManager::from_config(
            crate::config::ClientConfig::builder()
                .cluster_name("test")
                .build()
                .unwrap(),
        );
        let latch = CountDownLatch::new("test-latch".to_string(), Arc::new(cm));
        assert!(latch.session_manager.is_none());
        assert_eq!(latch.name(), "test-latch");
    }

    #[test]
    fn test_countdown_latch_with_session_manager() {
        let cm = Arc::new(ConnectionManager::from_config(
            crate::config::ClientConfig::builder()
                .cluster_name("test")
                .build()
                .unwrap(),
        ));
        let sm = Arc::new(CPSessionManager::new(Arc::clone(&cm)));
        let latch = CountDownLatch::with_session_manager("test-latch".to_string(), cm, sm);
        assert!(latch.session_manager.is_some());
    }
}
