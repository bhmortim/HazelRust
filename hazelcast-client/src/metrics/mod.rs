//! Prometheus metrics for the Hazelcast client.
//!
//! This module provides observability into client behavior through Prometheus-compatible metrics.
//! Enable the `metrics` feature flag to use this functionality.
//!
//! # Example
//!
//! ```rust,ignore
//! use hazelcast_client::metrics::ClientMetrics;
//!
//! // Create metrics with a custom prefix
//! let metrics = ClientMetrics::new("myapp");
//!
//! // Record connection lifecycle events
//! metrics.record_connection_opened();
//! metrics.record_connection_closed();
//!
//! // Record request timing manually
//! metrics.record_request("map_get", std::time::Duration::from_millis(5));
//!
//! // Or use RAII-based timing
//! {
//!     let _timer = metrics.start_request("map_put");
//!     // ... perform operation ...
//! } // duration recorded automatically on drop
//!
//! // Record errors
//! metrics.record_error("connection_timeout");
//!
//! // Export metrics for Prometheus scraping
//! let families = metrics.gather();
//!
//! // Or encode directly to text format
//! let text = metrics.encode_text().unwrap();
//! ```
//!
//! # Integration with HTTP Endpoints
//!
//! The `gather()` method returns `Vec<MetricFamily>` which can be encoded using
//! any Prometheus encoder. For HTTP exposition:
//!
//! ```rust,ignore
//! use prometheus::Encoder;
//!
//! async fn metrics_handler(metrics: &ClientMetrics) -> Vec<u8> {
//!     let encoder = prometheus::TextEncoder::new();
//!     let mut buffer = Vec::new();
//!     encoder.encode(&metrics.gather(), &mut buffer).unwrap();
//!     buffer
//! }
//! ```

use prometheus::{
    Encoder, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge, Opts, Registry,
    TextEncoder,
};
use std::time::{Duration, Instant};

/// Prometheus metrics for tracking Hazelcast client behavior.
///
/// This struct provides metrics for monitoring:
/// - Connection pool state and lifecycle
/// - Request counts by operation type
/// - Request latencies
/// - Error counts by type
///
/// All metrics are thread-safe and can be shared across async tasks.
#[derive(Clone)]
pub struct ClientMetrics {
    registry: Registry,
    connections_active: IntGauge,
    connections_total: IntCounter,
    requests_total: IntCounterVec,
    request_duration_seconds: HistogramVec,
    errors_total: IntCounterVec,
}

impl ClientMetrics {
    /// Creates a new `ClientMetrics` instance with the given prefix.
    ///
    /// The prefix is prepended to all metric names to allow multiple clients
    /// to coexist in the same application.
    ///
    /// # Arguments
    ///
    /// * `prefix` - A string prefix for all metric names (e.g., "hazelcast", "myapp")
    ///
    /// # Panics
    ///
    /// Panics if metric registration fails, which should not occur under normal circumstances.
    pub fn new(prefix: &str) -> Self {
        let registry = Registry::new();

        let connections_active = IntGauge::with_opts(Opts::new(
            format!("{}_connections_active", prefix),
            "Number of currently active connections to the cluster",
        ))
        .expect("metric creation should not fail");

        let connections_total = IntCounter::with_opts(Opts::new(
            format!("{}_connections_total", prefix),
            "Total number of connections opened since client start",
        ))
        .expect("metric creation should not fail");

        let requests_total = IntCounterVec::new(
            Opts::new(
                format!("{}_requests_total", prefix),
                "Total number of requests by operation type",
            ),
            &["operation"],
        )
        .expect("metric creation should not fail");

        let request_duration_seconds = HistogramVec::new(
            HistogramOpts::new(
                format!("{}_request_duration_seconds", prefix),
                "Request duration in seconds by operation type",
            )
            .buckets(vec![
                0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
            ]),
            &["operation"],
        )
        .expect("metric creation should not fail");

        let errors_total = IntCounterVec::new(
            Opts::new(
                format!("{}_errors_total", prefix),
                "Total number of errors by error type",
            ),
            &["error_type"],
        )
        .expect("metric creation should not fail");

        registry
            .register(Box::new(connections_active.clone()))
            .expect("registration should not fail");
        registry
            .register(Box::new(connections_total.clone()))
            .expect("registration should not fail");
        registry
            .register(Box::new(requests_total.clone()))
            .expect("registration should not fail");
        registry
            .register(Box::new(request_duration_seconds.clone()))
            .expect("registration should not fail");
        registry
            .register(Box::new(errors_total.clone()))
            .expect("registration should not fail");

        Self {
            registry,
            connections_active,
            connections_total,
            requests_total,
            request_duration_seconds,
            errors_total,
        }
    }

    /// Records that a new connection was opened.
    ///
    /// Increments both `connections_total` and `connections_active`.
    pub fn record_connection_opened(&self) {
        self.connections_total.inc();
        self.connections_active.inc();
    }

    /// Records that a connection was closed.
    ///
    /// Decrements `connections_active`.
    pub fn record_connection_closed(&self) {
        self.connections_active.dec();
    }

    /// Records a completed request with its duration.
    ///
    /// # Arguments
    ///
    /// * `operation` - The operation type (e.g., "map_get", "map_put", "queue_offer")
    /// * `duration` - The time taken to complete the request
    pub fn record_request(&self, operation: &str, duration: Duration) {
        self.requests_total.with_label_values(&[operation]).inc();
        self.request_duration_seconds
            .with_label_values(&[operation])
            .observe(duration.as_secs_f64());
    }

    /// Records an error occurrence.
    ///
    /// # Arguments
    ///
    /// * `error_type` - The error category (e.g., "connection_timeout", "serialization", "protocol")
    pub fn record_error(&self, error_type: &str) {
        self.errors_total.with_label_values(&[error_type]).inc();
    }

    /// Starts a request timer that records duration on drop.
    ///
    /// This provides RAII-based timing for request operations.
    ///
    /// # Arguments
    ///
    /// * `operation` - The operation type label for the metric
    ///
    /// # Returns
    ///
    /// A `RequestTimer` guard that records the elapsed time when dropped.
    pub fn start_request(&self, operation: impl Into<String>) -> RequestTimer<'_> {
        RequestTimer::new(self, operation)
    }

    /// Returns the current number of active connections.
    pub fn active_connections(&self) -> i64 {
        self.connections_active.get()
    }

    /// Returns the total number of connections opened.
    pub fn total_connections(&self) -> u64 {
        self.connections_total.get()
    }

    /// Gathers all metric families for Prometheus exposition.
    ///
    /// # Returns
    ///
    /// A vector of `MetricFamily` that can be encoded using any Prometheus encoder.
    pub fn gather(&self) -> Vec<prometheus::proto::MetricFamily> {
        self.registry.gather()
    }

    /// Encodes all metrics as Prometheus text format.
    ///
    /// # Returns
    ///
    /// A string containing all metrics in Prometheus text exposition format,
    /// or an error if encoding fails.
    pub fn encode_text(&self) -> Result<String, MetricsError> {
        let encoder = TextEncoder::new();
        let metric_families = self.gather();
        let mut buffer = Vec::new();
        encoder
            .encode(&metric_families, &mut buffer)
            .map_err(|e| MetricsError::new(format!("encoding failed: {}", e)))?;
        String::from_utf8(buffer).map_err(|e| MetricsError::new(format!("invalid UTF-8: {}", e)))
    }

    /// Returns a reference to the underlying Prometheus registry.
    ///
    /// This allows users to register additional custom metrics.
    pub fn registry(&self) -> &Registry {
        &self.registry
    }
}

impl Default for ClientMetrics {
    fn default() -> Self {
        Self::new("hazelcast")
    }
}

impl std::fmt::Debug for ClientMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientMetrics")
            .field("connections_active", &self.connections_active.get())
            .field("connections_total", &self.connections_total.get())
            .finish_non_exhaustive()
    }
}

/// Guard that records request duration when dropped.
///
/// Use this to automatically record request timing using RAII.
/// Created via [`ClientMetrics::start_request`].
pub struct RequestTimer<'a> {
    metrics: &'a ClientMetrics,
    operation: String,
    start: Instant,
}

impl<'a> RequestTimer<'a> {
    /// Creates a new request timer for the given operation.
    fn new(metrics: &'a ClientMetrics, operation: impl Into<String>) -> Self {
        Self {
            metrics,
            operation: operation.into(),
            start: Instant::now(),
        }
    }

    /// Returns the operation name being timed.
    pub fn operation(&self) -> &str {
        &self.operation
    }

    /// Returns the elapsed time since the timer was started.
    pub fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }
}

impl Drop for RequestTimer<'_> {
    fn drop(&mut self) {
        self.metrics
            .record_request(&self.operation, self.start.elapsed());
    }
}

/// Error type for metrics operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricsError {
    message: String,
}

impl MetricsError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl std::fmt::Display for MetricsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "metrics error: {}", self.message)
    }
}

impl std::error::Error for MetricsError {}

/// Common operation type constants for consistent labeling.
pub mod operations {
    /// Map get operation.
    pub const MAP_GET: &str = "map_get";
    /// Map put operation.
    pub const MAP_PUT: &str = "map_put";
    /// Map remove operation.
    pub const MAP_REMOVE: &str = "map_remove";
    /// Map contains_key operation.
    pub const MAP_CONTAINS_KEY: &str = "map_contains_key";
    /// Map size operation.
    pub const MAP_SIZE: &str = "map_size";
    /// Map clear operation.
    pub const MAP_CLEAR: &str = "map_clear";
    /// Queue offer operation.
    pub const QUEUE_OFFER: &str = "queue_offer";
    /// Queue poll operation.
    pub const QUEUE_POLL: &str = "queue_poll";
    /// Queue peek operation.
    pub const QUEUE_PEEK: &str = "queue_peek";
    /// List add operation.
    pub const LIST_ADD: &str = "list_add";
    /// List get operation.
    pub const LIST_GET: &str = "list_get";
    /// List remove operation.
    pub const LIST_REMOVE: &str = "list_remove";
    /// Set add operation.
    pub const SET_ADD: &str = "set_add";
    /// Set contains operation.
    pub const SET_CONTAINS: &str = "set_contains";
    /// Set remove operation.
    pub const SET_REMOVE: &str = "set_remove";
    /// Topic publish operation.
    pub const TOPIC_PUBLISH: &str = "topic_publish";
    /// MultiMap put operation.
    pub const MULTIMAP_PUT: &str = "multimap_put";
    /// MultiMap get operation.
    pub const MULTIMAP_GET: &str = "multimap_get";
}

/// Common error type constants for consistent labeling.
pub mod error_types {
    /// Connection timeout error.
    pub const CONNECTION_TIMEOUT: &str = "connection_timeout";
    /// Connection closed error.
    pub const CONNECTION_CLOSED: &str = "connection_closed";
    /// Serialization error.
    pub const SERIALIZATION: &str = "serialization";
    /// Deserialization error.
    pub const DESERIALIZATION: &str = "deserialization";
    /// Protocol error.
    pub const PROTOCOL: &str = "protocol";
    /// Authentication error.
    pub const AUTHENTICATION: &str = "authentication";
    /// Target not found error.
    pub const TARGET_NOT_FOUND: &str = "target_not_found";
    /// Retry exhausted error.
    pub const RETRY_EXHAUSTED: &str = "retry_exhausted";
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_prefix() {
        let metrics = ClientMetrics::default();
        let families = metrics.gather();

        let names: Vec<_> = families.iter().map(|f| f.get_name()).collect();
        assert!(names
            .iter()
            .any(|n| n.contains("hazelcast_connections_active")));
        assert!(names
            .iter()
            .any(|n| n.contains("hazelcast_connections_total")));
        assert!(names
            .iter()
            .any(|n| n.contains("hazelcast_requests_total")));
        assert!(names
            .iter()
            .any(|n| n.contains("hazelcast_request_duration_seconds")));
        assert!(names
            .iter()
            .any(|n| n.contains("hazelcast_errors_total")));
    }

    #[test]
    fn test_custom_prefix() {
        let metrics = ClientMetrics::new("myapp");
        let families = metrics.gather();

        let names: Vec<_> = families.iter().map(|f| f.get_name()).collect();
        assert!(names
            .iter()
            .any(|n| n.contains("myapp_connections_active")));
        assert!(!names
            .iter()
            .any(|n| n.contains("hazelcast_connections_active")));
    }

    #[test]
    fn test_connection_tracking() {
        let metrics = ClientMetrics::new("test");

        assert_eq!(metrics.active_connections(), 0);
        assert_eq!(metrics.total_connections(), 0);

        metrics.record_connection_opened();
        assert_eq!(metrics.active_connections(), 1);
        assert_eq!(metrics.total_connections(), 1);

        metrics.record_connection_opened();
        assert_eq!(metrics.active_connections(), 2);
        assert_eq!(metrics.total_connections(), 2);

        metrics.record_connection_closed();
        assert_eq!(metrics.active_connections(), 1);
        assert_eq!(metrics.total_connections(), 2);

        metrics.record_connection_closed();
        metrics.record_connection_closed();
        assert_eq!(metrics.active_connections(), -1);
        assert_eq!(metrics.total_connections(), 2);
    }

    #[test]
    fn test_request_recording() {
        let metrics = ClientMetrics::new("test");

        metrics.record_request(operations::MAP_GET, Duration::from_millis(5));
        metrics.record_request(operations::MAP_GET, Duration::from_millis(10));
        metrics.record_request(operations::MAP_PUT, Duration::from_millis(15));

        let families = metrics.gather();
        let requests_family = families
            .iter()
            .find(|f| f.get_name() == "test_requests_total")
            .expect("requests_total metric should exist");

        let metrics_vec = requests_family.get_metric();
        assert!(!metrics_vec.is_empty());
    }

    #[test]
    fn test_error_recording() {
        let metrics = ClientMetrics::new("test");

        metrics.record_error(error_types::CONNECTION_TIMEOUT);
        metrics.record_error(error_types::CONNECTION_TIMEOUT);
        metrics.record_error(error_types::SERIALIZATION);

        let families = metrics.gather();
        let errors_family = families
            .iter()
            .find(|f| f.get_name() == "test_errors_total")
            .expect("errors_total metric should exist");

        assert!(!errors_family.get_metric().is_empty());
    }

    #[test]
    fn test_request_timer() {
        let metrics = ClientMetrics::new("test");

        {
            let timer = metrics.start_request("slow_operation");
            assert_eq!(timer.operation(), "slow_operation");
            std::thread::sleep(Duration::from_millis(10));
        }

        let families = metrics.gather();
        let duration_family = families
            .iter()
            .find(|f| f.get_name() == "test_request_duration_seconds")
            .expect("request_duration_seconds metric should exist");

        assert!(!duration_family.get_metric().is_empty());
    }

    #[test]
    fn test_request_timer_elapsed() {
        let metrics = ClientMetrics::new("test");
        let timer = metrics.start_request("operation");

        std::thread::sleep(Duration::from_millis(5));
        let elapsed = timer.elapsed();

        assert!(elapsed >= Duration::from_millis(5));
    }

    #[test]
    fn test_encode_text() {
        let metrics = ClientMetrics::new("test");
        metrics.record_connection_opened();
        metrics.record_request(operations::MAP_GET, Duration::from_millis(5));

        let text = metrics.encode_text().expect("encoding should succeed");

        assert!(text.contains("test_connections_active 1"));
        assert!(text.contains("test_connections_total 1"));
        assert!(text.contains("test_requests_total"));
        assert!(text.contains("operation=\"map_get\""));
    }

    #[test]
    fn test_clone_shares_state() {
        let metrics = ClientMetrics::new("test");
        metrics.record_connection_opened();

        let cloned = metrics.clone();
        assert_eq!(cloned.active_connections(), 1);

        metrics.record_connection_opened();
        assert_eq!(cloned.active_connections(), 2);
        assert_eq!(metrics.active_connections(), 2);
    }

    #[test]
    fn test_histogram_buckets() {
        let metrics = ClientMetrics::new("test");

        metrics.record_request("fast", Duration::from_micros(500));
        metrics.record_request("medium", Duration::from_millis(50));
        metrics.record_request("slow", Duration::from_secs(2));

        let families = metrics.gather();
        let duration_family = families
            .iter()
            .find(|f| f.get_name() == "test_request_duration_seconds")
            .expect("histogram should exist");

        for metric in duration_family.get_metric() {
            let histogram = metric.get_histogram();
            assert!(histogram.get_sample_count() > 0);
        }
    }

    #[test]
    fn test_debug_impl() {
        let metrics = ClientMetrics::new("test");
        metrics.record_connection_opened();

        let debug_str = format!("{:?}", metrics);
        assert!(debug_str.contains("ClientMetrics"));
        assert!(debug_str.contains("connections_active: 1"));
        assert!(debug_str.contains("connections_total: 1"));
    }

    #[test]
    fn test_registry_access() {
        let metrics = ClientMetrics::new("test");
        let registry = metrics.registry();

        let custom_counter = IntCounter::new("custom_metric", "A custom metric").unwrap();
        registry.register(Box::new(custom_counter)).unwrap();

        let families = metrics.gather();
        let names: Vec<_> = families.iter().map(|f| f.get_name()).collect();
        assert!(names.iter().any(|n| *n == "custom_metric"));
    }

    #[test]
    fn test_metrics_error_display() {
        let err = MetricsError::new("test error");
        assert_eq!(err.to_string(), "metrics error: test error");
    }

    #[test]
    fn test_metrics_error_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<MetricsError>();
    }

    #[test]
    fn test_client_metrics_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<ClientMetrics>();
    }

    #[test]
    fn test_operation_constants() {
        assert_eq!(operations::MAP_GET, "map_get");
        assert_eq!(operations::MAP_PUT, "map_put");
        assert_eq!(operations::QUEUE_OFFER, "queue_offer");
        assert_eq!(operations::TOPIC_PUBLISH, "topic_publish");
    }

    #[test]
    fn test_error_type_constants() {
        assert_eq!(error_types::CONNECTION_TIMEOUT, "connection_timeout");
        assert_eq!(error_types::SERIALIZATION, "serialization");
        assert_eq!(error_types::RETRY_EXHAUSTED, "retry_exhausted");
    }

    #[test]
    fn test_multiple_operations_same_label() {
        let metrics = ClientMetrics::new("test");

        for _ in 0..100 {
            metrics.record_request(operations::MAP_GET, Duration::from_micros(100));
        }

        let families = metrics.gather();
        let requests_family = families
            .iter()
            .find(|f| f.get_name() == "test_requests_total")
            .expect("requests_total should exist");

        let map_get_metric = requests_family
            .get_metric()
            .iter()
            .find(|m| {
                m.get_label()
                    .iter()
                    .any(|l| l.get_name() == "operation" && l.get_value() == "map_get")
            })
            .expect("map_get metric should exist");

        assert_eq!(map_get_metric.get_counter().get_value() as u64, 100);
    }
}
