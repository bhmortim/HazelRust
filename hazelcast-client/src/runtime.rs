//! Runtime abstraction for async executors.
//!
//! This module provides a [`Runtime`] trait that abstracts over the async
//! runtime used by the Hazelcast client. The default implementation,
//! [`TokioRuntime`], delegates to the Tokio runtime.
//!
//! By coding against the trait, downstream libraries can support alternative
//! async runtimes (e.g., `async-std`, `smol`, or test harnesses) without
//! modifying the core client logic.
//!
//! # Example
//!
//! ```rust
//! use hazelcast_client::runtime::{Runtime, TokioRuntime};
//! use std::time::Duration;
//!
//! let rt = TokioRuntime;
//! // rt.spawn(async { /* background work */ });
//! // rt.sleep(Duration::from_secs(1)).await;
//! ```

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

/// Abstraction over an async runtime.
///
/// Implementations must be `Send + Sync + 'static` so they can be shared
/// across tasks and stored in long-lived structures like the connection manager.
pub trait Runtime: Send + Sync + 'static {
    /// Spawns a future as a background task.
    ///
    /// The future will run to completion even if the returned handle is dropped.
    fn spawn<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static;

    /// Returns a future that completes after the given duration.
    fn sleep(&self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send>>;
}

/// The default [`Runtime`] implementation backed by Tokio.
///
/// This delegates `spawn` to [`tokio::spawn`] and `sleep` to
/// [`tokio::time::sleep`].
#[derive(Debug, Clone, Copy, Default)]
pub struct TokioRuntime;

impl Runtime for TokioRuntime {
    fn spawn<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        tokio::spawn(future);
    }

    fn sleep(&self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(tokio::time::sleep(duration))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tokio_runtime_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<TokioRuntime>();
    }

    #[test]
    fn test_tokio_runtime_is_runtime() {
        fn assert_runtime<T: Runtime>() {}
        assert_runtime::<TokioRuntime>();
    }

    #[test]
    fn test_tokio_runtime_default() {
        let _rt = TokioRuntime::default();
    }

    #[test]
    fn test_tokio_runtime_clone() {
        let rt = TokioRuntime;
        let _cloned = rt;
    }

    #[tokio::test]
    async fn test_tokio_runtime_sleep() {
        let rt = TokioRuntime;
        // Should complete without error
        rt.sleep(Duration::from_millis(1)).await;
    }

    #[tokio::test]
    async fn test_tokio_runtime_spawn() {
        let rt = TokioRuntime;
        let (tx, rx) = tokio::sync::oneshot::channel();
        rt.spawn(async move {
            let _ = tx.send(42);
        });
        let value = rx.await.unwrap();
        assert_eq!(value, 42);
    }
}
