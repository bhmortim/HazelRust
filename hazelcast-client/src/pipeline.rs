//! Invocation pipelining for batching concurrent operations.
//!
//! The [`InvocationPipeline`] struct provides a way to execute multiple
//! asynchronous operations concurrently with a configurable concurrency
//! limit (depth). This is useful for bulk loading, batch reads, or any
//! scenario where you want to fire many requests without overwhelming
//! the cluster.
//!
//! # Example
//!
//! ```ignore
//! use hazelcast_client::InvocationPipeline;
//!
//! let pipeline = InvocationPipeline::new(10); // max 10 concurrent ops
//!
//! let map = client.get_map::<String, String>("my-map");
//!
//! let puts: Vec<_> = (0..100).map(|i| {
//!     let map = map.clone();
//!     move || async move { map.put(format!("key-{i}"), format!("val-{i}")).await }
//! }).collect();
//!
//! let results = pipeline.execute(puts).await;
//! ```

use std::future::Future;
use std::sync::Arc;

use hazelcast_core::{HazelcastError, Result};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

/// A pipelining executor that limits the number of concurrent in-flight operations.
///
/// The `depth` parameter controls how many operations can be in-flight simultaneously.
/// Operations beyond this limit will wait for a permit before being spawned.
#[derive(Debug, Clone)]
pub struct InvocationPipeline {
    depth: usize,
}

impl InvocationPipeline {
    /// Creates a new pipeline with the specified concurrency depth.
    ///
    /// # Panics
    ///
    /// Panics if `depth` is 0.
    pub fn new(depth: usize) -> Self {
        assert!(depth > 0, "pipeline depth must be > 0");
        Self { depth }
    }

    /// Returns the configured concurrency depth.
    pub fn depth(&self) -> usize {
        self.depth
    }

    /// Executes all operations concurrently, respecting the pipeline depth limit.
    ///
    /// Each operation is spawned as a Tokio task. At most `depth` tasks will be
    /// running at any given time. Returns a `Vec` of results in completion order
    /// (not submission order).
    ///
    /// If a task panics, the result for that operation will be a
    /// `HazelcastError::Connection` with the panic message.
    pub async fn execute<T, F, Fut>(
        &self,
        operations: impl IntoIterator<Item = F>,
    ) -> Vec<Result<T>>
    where
        T: Send + 'static,
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<T>> + Send + 'static,
    {
        let semaphore = Arc::new(Semaphore::new(self.depth));
        let mut join_set = JoinSet::new();

        for op in operations {
            let sem = semaphore.clone();
            join_set.spawn(async move {
                let _permit = sem.acquire().await.expect("semaphore closed unexpectedly");
                op().await
            });
        }

        let mut results = Vec::new();
        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(r) => results.push(r),
                Err(e) => results.push(Err(HazelcastError::Connection(format!(
                    "pipeline task panicked: {e}"
                )))),
            }
        }
        results
    }

    /// Executes all operations concurrently, collecting results in submission order.
    ///
    /// Unlike [`execute`](Self::execute), this method preserves the order of the
    /// input operations in the result vector. This has slightly more overhead due
    /// to index tracking.
    pub async fn execute_ordered<T, F, Fut>(
        &self,
        operations: impl IntoIterator<Item = F>,
    ) -> Vec<Result<T>>
    where
        T: Send + 'static,
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<T>> + Send + 'static,
    {
        let semaphore = Arc::new(Semaphore::new(self.depth));
        let mut join_set = JoinSet::new();
        let mut count = 0usize;

        for (idx, op) in operations.into_iter().enumerate() {
            count = idx + 1;
            let sem = semaphore.clone();
            join_set.spawn(async move {
                let _permit = sem.acquire().await.expect("semaphore closed unexpectedly");
                let result = op().await;
                (idx, result)
            });
        }

        let mut indexed_results: Vec<Option<Result<T>>> = (0..count).map(|_| None).collect();

        while let Some(result) = join_set.join_next().await {
            match result {
                Ok((idx, r)) => {
                    indexed_results[idx] = Some(r);
                }
                Err(e) => {
                    // JoinError doesn't carry index info; push to first empty slot
                    if let Some(slot) = indexed_results.iter_mut().find(|s| s.is_none()) {
                        *slot = Some(Err(HazelcastError::Connection(format!(
                            "pipeline task panicked: {e}"
                        ))));
                    }
                }
            }
        }

        indexed_results
            .into_iter()
            .map(|r| {
                r.unwrap_or_else(|| {
                    Err(HazelcastError::Connection(
                        "pipeline task result missing".to_string(),
                    ))
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_pipeline_basic() {
        let pipeline = InvocationPipeline::new(3);

        let ops: Vec<_> = (0..10)
            .map(|i| {
                move || async move { Ok(i * 2) }
            })
            .collect();

        let results = pipeline.execute(ops).await;
        assert_eq!(results.len(), 10);

        let mut values: Vec<i32> = results.into_iter().map(|r| r.unwrap()).collect();
        values.sort();
        assert_eq!(values, vec![0, 2, 4, 6, 8, 10, 12, 14, 16, 18]);
    }

    #[tokio::test]
    async fn test_pipeline_ordered() {
        let pipeline = InvocationPipeline::new(2);

        let ops: Vec<_> = (0..5)
            .map(|i| {
                move || async move { Ok(i) }
            })
            .collect();

        let results = pipeline.execute_ordered(ops).await;
        let values: Vec<i32> = results.into_iter().map(|r| r.unwrap()).collect();
        assert_eq!(values, vec![0, 1, 2, 3, 4]);
    }

    #[tokio::test]
    async fn test_pipeline_depth() {
        let pipeline = InvocationPipeline::new(5);
        assert_eq!(pipeline.depth(), 5);
    }

    #[tokio::test]
    async fn test_pipeline_empty() {
        let pipeline = InvocationPipeline::new(1);
        let ops: Vec<Box<dyn FnOnce() -> std::pin::Pin<Box<dyn Future<Output = Result<i32>> + Send>> + Send>> =
            vec![];
        let results = pipeline.execute(ops).await;
        assert!(results.is_empty());
    }

    #[test]
    #[should_panic(expected = "pipeline depth must be > 0")]
    fn test_pipeline_zero_depth_panics() {
        let _ = InvocationPipeline::new(0);
    }

    #[tokio::test]
    async fn test_pipeline_with_errors() {
        let pipeline = InvocationPipeline::new(3);

        let ops: Vec<_> = (0..5)
            .map(|i| {
                move || async move {
                    if i == 2 {
                        Err(HazelcastError::Connection("test error".to_string()))
                    } else {
                        Ok(i)
                    }
                }
            })
            .collect();

        let results = pipeline.execute_ordered(ops).await;
        assert_eq!(results.len(), 5);
        assert!(results[0].is_ok());
        assert!(results[1].is_ok());
        assert!(results[2].is_err());
        assert!(results[3].is_ok());
        assert!(results[4].is_ok());
    }
}
