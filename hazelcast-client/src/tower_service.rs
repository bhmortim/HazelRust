//! Tower middleware integration for the Hazelcast client.
//!
//! When the `tower` feature is enabled, this module provides a [`tower::Service`]
//! implementation that wraps the connection manager, allowing Hazelcast client
//! invocations to be composed with Tower middleware layers (retry, rate-limiting,
//! timeout, load-balancing, etc.).
//!
//! # Example
//!
//! ```ignore
//! use hazelcast_client::tower_service::HazelcastService;
//! use tower::ServiceExt;
//!
//! let service = HazelcastService::new(connection_manager);
//!
//! // The service can be wrapped with Tower layers:
//! let service = tower::ServiceBuilder::new()
//!     .concurrency_limit(100)
//!     .service(service);
//!
//! let response = service.ready().await?.call(request_message).await?;
//! ```

use std::sync::Arc;

use hazelcast_core::{ClientMessage, HazelcastError};

use crate::connection::ConnectionManager;

/// A [`tower::Service`] implementation for invoking Hazelcast client operations.
///
/// `HazelcastService` wraps a [`ConnectionManager`] and exposes message
/// send/receive as a Tower `Service<ClientMessage>`. This lets users
/// compose Hazelcast calls with any Tower middleware (rate limiting,
/// retries, load shedding, observability, etc.).
///
/// Requires the `tower` feature.
#[derive(Debug, Clone)]
pub struct HazelcastService {
    connection_manager: Arc<ConnectionManager>,
}

impl HazelcastService {
    /// Creates a new Tower service backed by the given connection manager.
    pub fn new(connection_manager: Arc<ConnectionManager>) -> Self {
        Self { connection_manager }
    }

    /// Returns a reference to the underlying connection manager.
    pub fn connection_manager(&self) -> &Arc<ConnectionManager> {
        &self.connection_manager
    }
}

#[cfg(feature = "tower")]
impl tower::Service<ClientMessage> for HazelcastService {
    type Response = ClientMessage;
    type Error = HazelcastError;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
    >;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        // The service is always ready to accept requests; back-pressure
        // is handled internally by the connection pool.
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: ClientMessage) -> Self::Future {
        let connection_manager = Arc::clone(&self.connection_manager);
        Box::pin(async move { connection_manager.send(request).await })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hazelcast_service_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<HazelcastService>();
    }

    #[test]
    fn test_hazelcast_service_is_clone() {
        fn assert_clone<T: Clone>() {}
        assert_clone::<HazelcastService>();
    }
}
