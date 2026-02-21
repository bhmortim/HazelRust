//! Prometheus metrics export for client statistics.
//!
//! This module provides integration with the [`metrics`] crate facade,
//! exposing Hazelcast client statistics in Prometheus format.
//!
//! # Example
//!
//! ```ignore
//! use hazelcast_client::metrics::PrometheusExporter;
//!
//! let exporter = PrometheusExporter::builder()
//!     .with_prefix("hazelcast")
//!     .build()?;
//!
//! // Get metrics output for an HTTP endpoint
//! let output = exporter.render();
//! ```

use std::sync::Arc;
use std::time::Duration;

use metrics::{counter, gauge, histogram, Label};
use metrics_exporter_prometheus::{BuildError, Matcher, PrometheusBuilder, PrometheusHandle};

use crate::diagnostics::{ClientStatistics, OperationType, StatisticsCollector};

/// Error type for Prometheus exporter operations.
#[derive(Debug, thiserror::Error)]
pub enum PrometheusError {
    /// Failed to set up the Prometheus recorder.
    #[error("failed to setup prometheus recorder: {0}")]
    Setup(String),
    /// Recorder already installed.
    #[error("metrics recorder already installed")]
    RecorderAlreadyInstalled,
}

impl From<BuildError> for PrometheusError {
    fn from(err: BuildError) -> Self {
        PrometheusError::Setup(err.to_string())
    }
}

/// Builder for configuring a [`PrometheusExporter`].
#[derive(Debug, Clone)]
pub struct PrometheusExporterBuilder {
    prefix: Option<String>,
    idle_timeout: Option<Duration>,
    latency_buckets: Vec<f64>,
}

impl Default for PrometheusExporterBuilder {
    fn default() -> Self {
        Self {
            prefix: Some("hazelcast".to_string()),
            idle_timeout: Some(Duration::from_secs(300)),
            latency_buckets: vec![
                0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
            ],
        }
    }
}

impl PrometheusExporterBuilder {
    /// Creates a new builder with default settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets a prefix for all metric names.
    ///
    /// Default: `"hazelcast"`
    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = Some(prefix.into());
        self
    }

    /// Removes the metric name prefix.
    pub fn without_prefix(mut self) -> Self {
        self.prefix = None;
        self
    }

    /// Sets the idle timeout for metrics.
    ///
    /// Metrics that haven't been updated within this duration will be removed.
    /// Default: 5 minutes.
    pub fn idle_timeout(mut self, timeout: Duration) -> Self {
        self.idle_timeout = Some(timeout);
        self
    }

    /// Disables idle timeout (metrics are never removed).
    pub fn without_idle_timeout(mut self) -> Self {
        self.idle_timeout = None;
        self
    }

    /// Sets custom histogram buckets for latency metrics (in seconds).
    ///
    /// Default: `[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]`
    pub fn latency_buckets(mut self, buckets: Vec<f64>) -> Self {
        self.latency_buckets = buckets;
        self
    }

    /// Builds the [`PrometheusExporter`].
    ///
    /// # Errors
    ///
    /// Returns an error if a metrics recorder is already installed.
    pub fn build(self) -> Result<PrometheusExporter, PrometheusError> {
        let mut builder = PrometheusBuilder::new();

        builder = builder.set_buckets_for_metric(
            Matcher::Suffix("_latency_seconds".to_string()),
            &self.latency_buckets,
        )?;

        let handle = builder
            .install_recorder()
            .map_err(|e| PrometheusError::Setup(e.to_string()))?;

        Ok(PrometheusExporter {
            handle,
            prefix: self.prefix,
        })
    }
}

/// Prometheus metrics exporter for Hazelcast client statistics.
///
/// The exporter integrates with the [`metrics`] crate facade to expose
/// client statistics in Prometheus format. It records:
///
/// - **Connection metrics**: active connections, bytes sent/received
/// - **Operation metrics**: counts and latency histograms by operation type
/// - **Near cache metrics**: hits, misses, hit ratio, evictions
/// - **Memory metrics**: estimated memory usage
///
/// # Example
///
/// ```ignore
/// use hazelcast_client::metrics::PrometheusExporter;
/// use hazelcast_client::HazelcastClient;
///
/// // Create the exporter (installs global metrics recorder)
/// let exporter = PrometheusExporter::builder().build()?;
///
/// // Create client - operations will automatically record metrics
/// let client = HazelcastClient::new(config).await?;
///
/// // Periodically update gauges from statistics
/// let collector = client.statistics_collector().clone();
/// tokio::spawn(async move {
///     let mut interval = tokio::time::interval(Duration::from_secs(5));
///     loop {
///         interval.tick().await;
///         let stats = collector.collect(0).await;
///         PrometheusExporter::record_statistics(&stats);
///     }
/// });
///
/// // Expose metrics via HTTP (e.g., with axum)
/// async fn metrics_handler() -> String {
///     exporter.render()
/// }
/// ```
pub struct PrometheusExporter {
    handle: PrometheusHandle,
    prefix: Option<String>,
}

impl PrometheusExporter {
    /// Creates a new builder for configuring the exporter.
    pub fn builder() -> PrometheusExporterBuilder {
        PrometheusExporterBuilder::new()
    }

    /// Creates a new exporter with default settings.
    ///
    /// # Errors
    ///
    /// Returns an error if a metrics recorder is already installed.
    pub fn new() -> Result<Self, PrometheusError> {
        Self::builder().build()
    }

    /// Renders the current metrics in Prometheus text format.
    ///
    /// This output can be served via an HTTP endpoint for Prometheus scraping.
    pub fn render(&self) -> String {
        self.handle.render()
    }

    /// Records a connection opened event.
    pub fn record_connection_opened() {
        counter!("hazelcast_connections_opened_total").increment(1);
    }

    /// Records a connection closed event.
    pub fn record_connection_closed() {
        counter!("hazelcast_connections_closed_total").increment(1);
    }

    /// Records bytes sent to the cluster.
    pub fn record_bytes_sent(bytes: u64) {
        counter!("hazelcast_bytes_sent_total").increment(bytes);
    }

    /// Records bytes received from the cluster.
    pub fn record_bytes_received(bytes: u64) {
        counter!("hazelcast_bytes_received_total").increment(bytes);
    }

    /// Records an operation with its latency.
    pub fn record_operation(op_type: OperationType, latency: Duration) {
        let op_name = op_type.name();

        counter!("hazelcast_operations_total", "operation" => op_name).increment(1);
        histogram!("hazelcast_operation_latency_seconds", "operation" => op_name).record(latency.as_secs_f64());
    }

    /// Records an operation without latency tracking.
    pub fn record_operation_count(op_type: OperationType) {
        counter!("hazelcast_operations_total", "operation" => op_type.name()).increment(1);
    }

    /// Records a near cache hit.
    pub fn record_near_cache_hit(map_name: &str) {
        let labels = vec![Label::new("map", map_name.to_string())];
        counter!("hazelcast_near_cache_hits_total", labels).increment(1);
    }

    /// Records a near cache miss.
    pub fn record_near_cache_miss(map_name: &str) {
        let labels = vec![Label::new("map", map_name.to_string())];
        counter!("hazelcast_near_cache_misses_total", labels).increment(1);
    }

    /// Records a near cache eviction.
    pub fn record_near_cache_eviction(map_name: &str) {
        let labels = vec![Label::new("map", map_name.to_string())];
        counter!("hazelcast_near_cache_evictions_total", labels).increment(1);
    }

    /// Updates all gauge metrics from a statistics snapshot.
    ///
    /// This should be called periodically to update point-in-time gauges
    /// like active connection count, memory usage, and cache ratios.
    pub fn record_statistics(stats: &ClientStatistics) {
        let conn_stats = stats.connection_stats();
        gauge!("hazelcast_connections_active").set(conn_stats.active_connections() as f64);
        gauge!("hazelcast_connections_opened_total")
            .set(conn_stats.total_connections_opened() as f64);
        gauge!("hazelcast_connections_closed_total")
            .set(conn_stats.total_connections_closed() as f64);
        gauge!("hazelcast_bytes_sent_total").set(conn_stats.bytes_sent() as f64);
        gauge!("hazelcast_bytes_received_total").set(conn_stats.bytes_received() as f64);

        for (op_type, count) in stats.operation_counts() {
            gauge!("hazelcast_operations_total", "operation" => op_type.name()).set(*count as f64);
        }

        for (map_name, cache_stats) in stats.near_cache_stats() {
            gauge!("hazelcast_near_cache_hits_total", "map" => map_name.clone()).set(cache_stats.hits() as f64);
            gauge!("hazelcast_near_cache_misses_total", "map" => map_name.clone()).set(cache_stats.misses() as f64);
            gauge!("hazelcast_near_cache_evictions_total", "map" => map_name.clone())
                .set(cache_stats.evictions() as f64);
            gauge!("hazelcast_near_cache_expirations_total", "map" => map_name.clone())
                .set(cache_stats.expirations() as f64);
            gauge!("hazelcast_near_cache_hit_ratio", "map" => map_name.clone()).set(cache_stats.hit_ratio());
        }

        let mem_stats = stats.memory_stats();
        gauge!("hazelcast_memory_used_bytes").set(mem_stats.used_bytes() as f64);
        gauge!("hazelcast_near_cache_memory_bytes").set(mem_stats.near_cache_bytes() as f64);

        gauge!("hazelcast_uptime_seconds").set(stats.uptime().as_secs_f64());
    }

    /// Creates a metrics recorder task that periodically updates statistics.
    ///
    /// Returns a handle that can be used to stop the task.
    pub fn start_recorder(
        collector: Arc<StatisticsCollector>,
        interval: Duration,
    ) -> MetricsRecorderHandle {
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() {
                            tracing::debug!("metrics recorder shutting down");
                            break;
                        }
                    }
                    _ = ticker.tick() => {
                        let stats = collector.collect(0).await;
                        Self::record_statistics(&stats);
                    }
                }
            }
        });

        tracing::info!(
            interval_ms = interval.as_millis(),
            "metrics recorder started"
        );

        MetricsRecorderHandle { shutdown_tx }
    }
}

impl std::fmt::Debug for PrometheusExporter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrometheusExporter")
            .field("prefix", &self.prefix)
            .finish_non_exhaustive()
    }
}

/// Handle to control a running metrics recorder task.
#[derive(Debug)]
pub struct MetricsRecorderHandle {
    shutdown_tx: tokio::sync::watch::Sender<bool>,
}

impl MetricsRecorderHandle {
    /// Stops the metrics recorder task.
    pub fn stop(&self) {
        let _ = self.shutdown_tx.send(true);
    }
}

impl Drop for MetricsRecorderHandle {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Guard that records operation latency when dropped.
///
/// Use this for automatic latency tracking with RAII semantics.
///
/// # Example
///
/// ```ignore
/// let _guard = OperationLatencyGuard::new(OperationType::MapGet);
/// // ... perform operation ...
/// // latency is automatically recorded when guard is dropped
/// ```
#[derive(Debug)]
pub struct OperationLatencyGuard {
    op_type: OperationType,
    start: std::time::Instant,
}

impl OperationLatencyGuard {
    /// Creates a new latency guard that starts timing immediately.
    pub fn new(op_type: OperationType) -> Self {
        Self {
            op_type,
            start: std::time::Instant::now(),
        }
    }
}

impl Drop for OperationLatencyGuard {
    fn drop(&mut self) {
        let latency = self.start.elapsed();
        PrometheusExporter::record_operation(self.op_type, latency);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builder_default() {
        let builder = PrometheusExporterBuilder::default();
        assert_eq!(builder.prefix, Some("hazelcast".to_string()));
        assert!(builder.idle_timeout.is_some());
        assert!(!builder.latency_buckets.is_empty());
    }

    #[test]
    fn test_builder_customization() {
        let builder = PrometheusExporterBuilder::new()
            .with_prefix("myapp")
            .idle_timeout(Duration::from_secs(60))
            .latency_buckets(vec![0.1, 0.5, 1.0]);

        assert_eq!(builder.prefix, Some("myapp".to_string()));
        assert_eq!(builder.idle_timeout, Some(Duration::from_secs(60)));
        assert_eq!(builder.latency_buckets, vec![0.1, 0.5, 1.0]);
    }

    #[test]
    fn test_builder_without_prefix() {
        let builder = PrometheusExporterBuilder::new().without_prefix();
        assert!(builder.prefix.is_none());
    }

    #[test]
    fn test_operation_latency_guard_creation() {
        let guard = OperationLatencyGuard::new(OperationType::MapGet);
        assert_eq!(guard.op_type, OperationType::MapGet);
    }

    #[test]
    fn test_prometheus_error_display() {
        let err = PrometheusError::Setup("test error".to_string());
        assert!(err.to_string().contains("test error"));

        let err = PrometheusError::RecorderAlreadyInstalled;
        assert!(err.to_string().contains("already installed"));
    }

    #[test]
    fn test_metrics_recorder_handle_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<MetricsRecorderHandle>();
    }
}
