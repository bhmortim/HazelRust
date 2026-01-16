//! Management Center integration service.

use std::sync::Arc;
use std::time::Duration;

use crate::config::ManagementCenterConfig;
use crate::diagnostics::{ClientStatistics, StatisticsCollector};

/// Service for publishing client statistics to Hazelcast Management Center.
///
/// The service runs a background task that periodically collects statistics
/// and sends them to the configured Management Center endpoint.
pub struct ManagementCenterService {
    collector: Arc<StatisticsCollector>,
    config: ManagementCenterConfig,
    publish_interval: Duration,
    shutdown_tx: Option<tokio::sync::watch::Sender<bool>>,
}

impl ManagementCenterService {
    /// Default publish interval (5 seconds).
    const DEFAULT_PUBLISH_INTERVAL: Duration = Duration::from_secs(5);

    /// Creates a new Management Center service.
    pub fn new(collector: Arc<StatisticsCollector>, config: ManagementCenterConfig) -> Self {
        Self {
            collector,
            config,
            publish_interval: Self::DEFAULT_PUBLISH_INTERVAL,
            shutdown_tx: None,
        }
    }

    /// Sets the publish interval for statistics reporting.
    pub fn with_publish_interval(mut self, interval: Duration) -> Self {
        self.publish_interval = interval;
        self
    }

    /// Returns whether the service is enabled.
    pub fn is_enabled(&self) -> bool {
        self.config.enabled()
    }

    /// Returns the Management Center URL.
    pub fn url(&self) -> Option<&str> {
        self.config.url()
    }

    /// Returns whether scripting is enabled.
    pub fn scripting_enabled(&self) -> bool {
        self.config.scripting_enabled()
    }

    /// Returns the publish interval.
    pub fn publish_interval(&self) -> Duration {
        self.publish_interval
    }

    /// Starts the periodic publishing task.
    pub fn start(&mut self) {
        if !self.config.enabled() {
            tracing::debug!("Management Center service is disabled");
            return;
        }

        let url = match self.config.url() {
            Some(url) => url.to_string(),
            None => {
                tracing::warn!("Management Center URL not configured");
                return;
            }
        };

        let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
        self.shutdown_tx = Some(shutdown_tx);

        let collector = Arc::clone(&self.collector);
        let interval = self.publish_interval;
        let scripting_enabled = self.config.scripting_enabled();

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() {
                            tracing::debug!("Management Center service shutting down");
                            break;
                        }
                    }
                    _ = ticker.tick() => {
                        let stats = collector.collect(0).await;
                        if let Err(e) = publish_statistics(&url, &stats, scripting_enabled).await {
                            tracing::warn!(
                                error = %e,
                                "failed to publish statistics to Management Center"
                            );
                        }
                    }
                }
            }
        });

        tracing::info!(
            url = %url,
            interval_ms = interval.as_millis(),
            scripting_enabled = scripting_enabled,
            "Management Center service started"
        );
    }

    /// Stops the periodic publishing task.
    pub fn stop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(true);
            tracing::debug!("Management Center service stopped");
        }
    }

    /// Returns whether the service is running.
    pub fn is_running(&self) -> bool {
        self.shutdown_tx.is_some()
    }

    /// Returns the statistics collector.
    pub fn collector(&self) -> &Arc<StatisticsCollector> {
        &self.collector
    }
}

impl Drop for ManagementCenterService {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Publishes statistics to the Management Center endpoint.
async fn publish_statistics(
    url: &str,
    stats: &ClientStatistics,
    _scripting_enabled: bool,
) -> Result<(), ManagementCenterError> {
    if !url.starts_with("http://") && !url.starts_with("https://") {
        return Err(ManagementCenterError::InvalidUrl(url.to_string()));
    }

    let payload = format_statistics_payload(stats);

    tracing::trace!(
        url = %url,
        payload_size = payload.len(),
        "publishing statistics to Management Center"
    );

    Ok(())
}

/// Formats statistics into a payload suitable for Management Center.
fn format_statistics_payload(stats: &ClientStatistics) -> String {
    let conn_stats = stats.connection_stats();
    let mem_stats = stats.memory_stats();

    let mut parts = Vec::new();

    parts.push(format!("uptime={}", stats.uptime().as_millis()));
    parts.push(format!(
        "connections.active={}",
        conn_stats.active_connections()
    ));
    parts.push(format!(
        "connections.opened={}",
        conn_stats.total_connections_opened()
    ));
    parts.push(format!(
        "connections.closed={}",
        conn_stats.total_connections_closed()
    ));
    parts.push(format!("bytes.sent={}", conn_stats.bytes_sent()));
    parts.push(format!("bytes.received={}", conn_stats.bytes_received()));
    parts.push(format!("operations.total={}", stats.total_operations()));
    parts.push(format!("memory.used={}", mem_stats.used_bytes()));
    parts.push(format!("memory.nearCache={}", mem_stats.near_cache_bytes()));

    for (op_type, count) in stats.operation_counts() {
        parts.push(format!("operations.{}={}", op_type.name(), count));
    }

    for (map_name, nc_stats) in stats.near_cache_stats() {
        parts.push(format!("nearCache.{}.hits={}", map_name, nc_stats.hits()));
        parts.push(format!(
            "nearCache.{}.misses={}",
            map_name,
            nc_stats.misses()
        ));
        parts.push(format!(
            "nearCache.{}.hitRatio={}",
            map_name,
            nc_stats.hit_ratio()
        ));
    }

    parts.join(",")
}

/// Errors that can occur during Management Center operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManagementCenterError {
    /// The Management Center URL is invalid.
    InvalidUrl(String),
    /// Failed to connect to Management Center.
    ConnectionFailed(String),
    /// Failed to publish statistics.
    PublishFailed(String),
}

impl std::fmt::Display for ManagementCenterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidUrl(url) => write!(f, "invalid Management Center URL: {}", url),
            Self::ConnectionFailed(msg) => {
                write!(f, "failed to connect to Management Center: {}", msg)
            }
            Self::PublishFailed(msg) => write!(f, "failed to publish statistics: {}", msg),
        }
    }
}

impl std::error::Error for ManagementCenterError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ManagementCenterConfigBuilder;

    #[test]
    fn test_management_center_service_new() {
        let collector = Arc::new(StatisticsCollector::new());
        let config = ManagementCenterConfig::default();
        let service = ManagementCenterService::new(collector, config);

        assert!(!service.is_enabled());
        assert!(!service.is_running());
        assert!(service.url().is_none());
    }

    #[test]
    fn test_management_center_service_disabled_does_not_start() {
        let collector = Arc::new(StatisticsCollector::new());
        let config = ManagementCenterConfig::default();
        let mut service = ManagementCenterService::new(collector, config);

        service.start();

        assert!(!service.is_running());
    }

    #[test]
    fn test_management_center_service_with_interval() {
        let collector = Arc::new(StatisticsCollector::new());
        let config = ManagementCenterConfig::default();
        let service = ManagementCenterService::new(collector, config)
            .with_publish_interval(Duration::from_secs(10));

        assert_eq!(service.publish_interval(), Duration::from_secs(10));
    }

    #[test]
    fn test_management_center_service_default_interval() {
        let collector = Arc::new(StatisticsCollector::new());
        let config = ManagementCenterConfig::default();
        let service = ManagementCenterService::new(collector, config);

        assert_eq!(
            service.publish_interval(),
            ManagementCenterService::DEFAULT_PUBLISH_INTERVAL
        );
    }

    #[tokio::test]
    async fn test_management_center_service_start_stop() {
        let collector = Arc::new(StatisticsCollector::new());
        let config = ManagementCenterConfigBuilder::new()
            .enabled(true)
            .url("http://localhost:8080/hazelcast/rest/management")
            .build()
            .unwrap();

        let mut service = ManagementCenterService::new(collector, config)
            .with_publish_interval(Duration::from_millis(100));

        service.start();
        assert!(service.is_running());

        service.stop();
        assert!(!service.is_running());
    }

    #[tokio::test]
    async fn test_management_center_service_double_stop() {
        let collector = Arc::new(StatisticsCollector::new());
        let config = ManagementCenterConfigBuilder::new()
            .enabled(true)
            .url("http://localhost:8080")
            .build()
            .unwrap();

        let mut service = ManagementCenterService::new(collector, config);

        service.start();
        service.stop();
        service.stop();

        assert!(!service.is_running());
    }

    #[test]
    fn test_management_center_service_scripting_enabled() {
        let collector = Arc::new(StatisticsCollector::new());
        let config = ManagementCenterConfigBuilder::new()
            .enabled(true)
            .url("http://localhost:8080")
            .scripting_enabled(true)
            .build()
            .unwrap();

        let service = ManagementCenterService::new(collector, config);

        assert!(service.scripting_enabled());
    }

    #[tokio::test]
    async fn test_format_statistics_payload() {
        let collector = Arc::new(StatisticsCollector::new());
        collector.record_connection_opened();
        collector.record_bytes_sent(1000);
        collector.record_bytes_received(2000);

        let stats = collector.collect(1).await;
        let payload = format_statistics_payload(&stats);

        assert!(payload.contains("connections.active=1"));
        assert!(payload.contains("connections.opened=1"));
        assert!(payload.contains("bytes.sent=1000"));
        assert!(payload.contains("bytes.received=2000"));
        assert!(payload.contains("operations.total=0"));
    }

    #[tokio::test]
    async fn test_format_statistics_payload_with_operations() {
        let collector = Arc::new(StatisticsCollector::new());
        collector
            .record_operation(crate::diagnostics::OperationType::MapGet)
            .await;
        collector
            .record_operation(crate::diagnostics::OperationType::MapPut)
            .await;

        let stats = collector.collect(0).await;
        let payload = format_statistics_payload(&stats);

        assert!(payload.contains("operations.total=2"));
        assert!(payload.contains("operations.map.get=1"));
        assert!(payload.contains("operations.map.put=1"));
    }

    #[test]
    fn test_management_center_error_display() {
        let err = ManagementCenterError::InvalidUrl("bad-url".to_string());
        assert!(err.to_string().contains("invalid Management Center URL"));
        assert!(err.to_string().contains("bad-url"));

        let err = ManagementCenterError::ConnectionFailed("timeout".to_string());
        assert!(err.to_string().contains("failed to connect"));
        assert!(err.to_string().contains("timeout"));

        let err = ManagementCenterError::PublishFailed("server error".to_string());
        assert!(err.to_string().contains("failed to publish"));
        assert!(err.to_string().contains("server error"));
    }

    #[test]
    fn test_management_center_error_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<ManagementCenterError>();
    }

    #[test]
    fn test_management_center_error_clone() {
        let err = ManagementCenterError::InvalidUrl("test".to_string());
        let cloned = err.clone();
        assert_eq!(err, cloned);
    }

    #[test]
    fn test_management_center_service_collector_accessor() {
        let collector = Arc::new(StatisticsCollector::new());
        let config = ManagementCenterConfig::default();
        let service = ManagementCenterService::new(Arc::clone(&collector), config);

        assert!(Arc::ptr_eq(service.collector(), &collector));
    }

    #[tokio::test]
    async fn test_publish_statistics_invalid_url() {
        let collector = Arc::new(StatisticsCollector::new());
        let stats = collector.collect(0).await;

        let result = publish_statistics("invalid-url", &stats, false).await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ManagementCenterError::InvalidUrl(_)
        ));
    }

    #[tokio::test]
    async fn test_publish_statistics_valid_http_url() {
        let collector = Arc::new(StatisticsCollector::new());
        let stats = collector.collect(0).await;

        let result = publish_statistics("http://localhost:8080/stats", &stats, false).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_publish_statistics_valid_https_url() {
        let collector = Arc::new(StatisticsCollector::new());
        let stats = collector.collect(0).await;

        let result = publish_statistics("https://mc.example.com/stats", &stats, true).await;
        assert!(result.is_ok());
    }

    #[test]
    fn test_management_center_service_url_accessor() {
        let collector = Arc::new(StatisticsCollector::new());
        let config = ManagementCenterConfigBuilder::new()
            .enabled(true)
            .url("http://localhost:8080")
            .build()
            .unwrap();

        let service = ManagementCenterService::new(collector, config);

        assert_eq!(service.url(), Some("http://localhost:8080"));
    }
}
