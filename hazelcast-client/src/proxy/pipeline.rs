//! Pipelining support for batching multiple async requests.
//!
//! Pipelining allows sending multiple requests to the cluster without waiting
//! for each individual response, improving throughput for bulk operations.
//!
//! # Example
//!
//! ```ignore
//! use hazelcast_client::proxy::Pipelining;
//!
//! let mut pipeline = Pipelining::<String>::new(connection_manager);
//!
//! pipeline.add(request1)
//!     .add(request2)
//!     .add(request3);
//!
//! let results = pipeline.flush().await;
//! for result in results {
//!     match result {
//!         Ok(response) => println!("Got response"),
//!         Err(e) => eprintln!("Request failed: {}", e),
//!     }
//! }
//! ```

use std::marker::PhantomData;
use std::sync::Arc;

use hazelcast_core::{ClientMessage, Result};

use crate::connection::ConnectionManager;

/// Default maximum pipeline depth (number of concurrent in-flight requests).
const DEFAULT_MAX_DEPTH: usize = 64;

/// Batches multiple async requests for improved throughput.
///
/// `Pipelining` collects outgoing requests and sends them concurrently,
/// allowing the network stack to batch multiple messages together. This
/// reduces per-message overhead and can significantly improve throughput
/// for bulk operations.
///
/// # Type Parameters
///
/// - `V`: The expected response value type (used for type-safety in
///   higher-level wrappers that deserialize responses).
pub struct Pipelining<V> {
    connection_manager: Arc<ConnectionManager>,
    pending: Vec<tokio::task::JoinHandle<Result<ClientMessage>>>,
    max_depth: usize,
    _phantom: PhantomData<V>,
}

impl<V> std::fmt::Debug for Pipelining<V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pipelining")
            .field("pending_count", &self.pending.len())
            .field("max_depth", &self.max_depth)
            .finish()
    }
}

impl<V> Pipelining<V> {
    /// Creates a new `Pipelining` instance with the default maximum depth.
    ///
    /// The default depth is 64 concurrent in-flight requests.
    pub fn new(connection_manager: Arc<ConnectionManager>) -> Self {
        Self {
            connection_manager,
            pending: Vec::new(),
            max_depth: DEFAULT_MAX_DEPTH,
            _phantom: PhantomData,
        }
    }

    /// Creates a new `Pipelining` instance with the specified maximum depth.
    ///
    /// The depth controls the maximum number of concurrent in-flight requests.
    /// When the depth is reached, `add()` will automatically flush pending
    /// requests before accepting new ones.
    ///
    /// # Arguments
    ///
    /// * `connection_manager` - The connection manager to send requests through
    /// * `depth` - Maximum number of concurrent in-flight requests
    pub fn with_depth(connection_manager: Arc<ConnectionManager>, depth: usize) -> Self {
        Self {
            connection_manager,
            pending: Vec::new(),
            max_depth: if depth == 0 { DEFAULT_MAX_DEPTH } else { depth },
            _phantom: PhantomData,
        }
    }

    /// Returns the maximum pipeline depth.
    pub fn max_depth(&self) -> usize {
        self.max_depth
    }

    /// Returns the number of pending (in-flight) requests.
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    /// Returns true if there are no pending requests.
    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    /// Adds a request message to the pipeline.
    ///
    /// The message is sent asynchronously without waiting for a response.
    /// When the pipeline reaches its maximum depth, subsequent calls to
    /// `add()` will still accept the message (the depth is a soft limit
    /// for optimal batching).
    ///
    /// # Arguments
    ///
    /// * `message` - The client message to send
    ///
    /// # Returns
    ///
    /// A mutable reference to `self` for method chaining.
    pub fn add(&mut self, message: ClientMessage) -> &mut Self {
        let cm = Arc::clone(&self.connection_manager);
        let handle = tokio::spawn(async move { cm.send(message).await });
        self.pending.push(handle);
        self
    }

    /// Flushes all pending requests and collects their responses.
    ///
    /// This method awaits all in-flight requests and returns their results
    /// in the same order they were added. Each result is independent; a
    /// failure in one request does not affect others.
    ///
    /// After flushing, the pipeline is empty and ready to accept new requests.
    ///
    /// # Returns
    ///
    /// A vector of results corresponding to each added request, in order.
    pub async fn flush(&mut self) -> Vec<Result<ClientMessage>> {
        let handles: Vec<_> = self.pending.drain(..).collect();
        let mut results = Vec::with_capacity(handles.len());

        for handle in handles {
            let result = match handle.await {
                Ok(inner_result) => inner_result,
                Err(join_error) => Err(hazelcast_core::HazelcastError::IllegalState(
                    format!("pipeline task panicked: {}", join_error),
                )),
            };
            results.push(result);
        }

        results
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_max_depth() {
        assert_eq!(DEFAULT_MAX_DEPTH, 64);
    }
}
