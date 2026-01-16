//! Client statistics collection and reporting.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::RwLock;

use crate::cache::NearCacheStats;

/// Types of operations tracked for statistics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OperationType {
    /// Map get operation.
    MapGet,
    /// Map put operation.
    MapPut,
    /// Map remove operation.
    MapRemove,
    /// Map contains key operation.
    MapContainsKey,
    /// Map size operation.
    MapSize,
    /// Map clear operation.
    MapClear,
    /// Map query operation.
    MapQuery,
    /// Map aggregate operation.
    MapAggregate,
    /// Map entry processor operation.
    MapEntryProcessor,
    /// Queue offer operation.
    QueueOffer,
    /// Queue poll operation.
    QueuePoll,
    /// Topic publish operation.
    TopicPublish,
    /// SQL execute operation.
    SqlExecute,
    /// Transaction operation.
    Transaction,
    /// Other/unknown operation.
    Other,
}

impl OperationType {
    /// Returns the name of this operation type.
    pub fn name(&self) -> &'static str {
        match self {
            Self::MapGet => "map.get",
            Self::MapPut => "map.put",
            Self::MapRemove => "map.remove",
            Self::MapContainsKey => "map.containsKey",
            Self::MapSize => "map.size",
            Self::MapClear => "map.clear",
            Self::MapQuery => "map.query",
            Self::MapAggregate => "map.aggregate",
            Self::MapEntryProcessor => "map.entryProcessor",
            Self::QueueOffer => "queue.offer",
            Self::QueuePoll => "queue.poll",
            Self::TopicPublish => "topic.publish",
            Self::SqlExecute => "sql.execute",
            Self::Transaction => "transaction",
            Self::Other => "other",
        }
    }
}

/// Statistics for client connections.
#[derive(Debug, Clone, Default)]
pub struct ConnectionStats {
    active_connections: u64,
    total_connections_opened: u64,
    total_connections_closed: u64,
    bytes_sent: u64,
    bytes_received: u64,
}

impl ConnectionStats {
    /// Returns the number of currently active connections.
    pub fn active_connections(&self) -> u64 {
        self.active_connections
    }

    /// Returns the total number of connections opened since client start.
    pub fn total_connections_opened(&self) -> u64 {
        self.total_connections_opened
    }

    /// Returns the total number of connections closed since client start.
    pub fn total_connections_closed(&self) -> u64 {
        self.total_connections_closed
    }

    /// Returns the total bytes sent to the cluster.
    pub fn bytes_sent(&self) -> u64 {
        self.bytes_sent
    }

    /// Returns the total bytes received from the cluster.
    pub fn bytes_received(&self) -> u64 {
        self.bytes_received
    }
}

/// Near cache statistics with computed ratios.
#[derive(Debug, Clone, Default)]
pub struct NearCacheRatioStats {
    hits: u64,
    misses: u64,
    evictions: u64,
    expirations: u64,
    hit_ratio: f64,
}

impl NearCacheRatioStats {
    /// Creates ratio stats from base near cache stats.
    pub fn from_stats(stats: &NearCacheStats) -> Self {
        Self {
            hits: stats.hits(),
            misses: stats.misses(),
            evictions: stats.evictions(),
            expirations: stats.expirations(),
            hit_ratio: stats.hit_ratio(),
        }
    }

    /// Returns the number of cache hits.
    pub fn hits(&self) -> u64 {
        self.hits
    }

    /// Returns the number of cache misses.
    pub fn misses(&self) -> u64 {
        self.misses
    }

    /// Returns the number of evictions.
    pub fn evictions(&self) -> u64 {
        self.evictions
    }

    /// Returns the number of expirations.
    pub fn expirations(&self) -> u64 {
        self.expirations
    }

    /// Returns the hit ratio (0.0 to 1.0).
    pub fn hit_ratio(&self) -> f64 {
        self.hit_ratio
    }

    /// Returns the miss ratio (0.0 to 1.0).
    pub fn miss_ratio(&self) -> f64 {
        1.0 - self.hit_ratio
    }
}

/// Memory usage statistics.
#[derive(Debug, Clone, Default)]
pub struct MemoryStats {
    used_bytes: u64,
    near_cache_bytes: u64,
}

impl MemoryStats {
    /// Returns the estimated total memory used by the client.
    pub fn used_bytes(&self) -> u64 {
        self.used_bytes
    }

    /// Returns the estimated memory used by near caches.
    pub fn near_cache_bytes(&self) -> u64 {
        self.near_cache_bytes
    }
}

/// Aggregated client statistics snapshot.
///
/// This struct provides a point-in-time view of client statistics including
/// connection info, operation counts, near cache performance, and memory usage.
#[derive(Debug, Clone)]
pub struct ClientStatistics {
    connection_stats: ConnectionStats,
    operation_counts: HashMap<OperationType, u64>,
    operation_latencies: HashMap<OperationType, f64>,
    near_cache_stats: HashMap<String, NearCacheRatioStats>,
    memory_stats: MemoryStats,
    collected_at: Instant,
    uptime: Duration,
}

impl ClientStatistics {
    /// Returns connection statistics.
    pub fn connection_stats(&self) -> &ConnectionStats {
        &self.connection_stats
    }

    /// Returns operation counts by type.
    pub fn operation_counts(&self) -> &HashMap<OperationType, u64> {
        &self.operation_counts
    }

    /// Returns the count for a specific operation type.
    pub fn operation_count(&self, op_type: OperationType) -> u64 {
        self.operation_counts.get(&op_type).copied().unwrap_or(0)
    }

    /// Returns the total number of operations performed.
    pub fn total_operations(&self) -> u64 {
        self.operation_counts.values().sum()
    }

    /// Returns average operation latencies by type (in milliseconds).
    pub fn operation_latencies(&self) -> &HashMap<OperationType, f64> {
        &self.operation_latencies
    }

    /// Returns the average latency for a specific operation type in milliseconds.
    pub fn operation_latency(&self, op_type: OperationType) -> Option<f64> {
        self.operation_latencies.get(&op_type).copied()
    }

    /// Returns near cache statistics by map name.
    pub fn near_cache_stats(&self) -> &HashMap<String, NearCacheRatioStats> {
        &self.near_cache_stats
    }

    /// Returns near cache statistics for a specific map.
    pub fn near_cache_stats_for(&self, map_name: &str) -> Option<&NearCacheRatioStats> {
        self.near_cache_stats.get(map_name)
    }

    /// Returns the aggregate hit ratio across all near caches.
    ///
    /// Returns `0.0` if no near caches are configured or no lookups occurred.
    pub fn aggregate_near_cache_hit_ratio(&self) -> f64 {
        let (total_hits, total_misses) = self.near_cache_stats.values().fold(
            (0u64, 0u64),
            |(hits, misses), stats| (hits + stats.hits, misses + stats.misses),
        );

        let total = total_hits + total_misses;
        if total == 0 {
            0.0
        } else {
            total_hits as f64 / total as f64
        }
    }

    /// Returns memory usage statistics.
    pub fn memory_stats(&self) -> &MemoryStats {
        &self.memory_stats
    }

    /// Returns when these statistics were collected.
    pub fn collected_at(&self) -> Instant {
        self.collected_at
    }

    /// Returns the client uptime when these statistics were collected.
    pub fn uptime(&self) -> Duration {
        self.uptime
    }
}

/// Collector for tracking client statistics.
///
/// The collector uses atomic operations for thread-safe updates and provides
/// a snapshot method to capture current statistics.
#[derive(Debug)]
pub struct StatisticsCollector {
    started_at: Instant,
    connections_opened: AtomicU64,
    connections_closed: AtomicU64,
    bytes_sent: AtomicU64,
    bytes_received: AtomicU64,
    operation_counts: RwLock<HashMap<OperationType, u64>>,
    operation_latencies: RwLock<HashMap<OperationType, Vec<Duration>>>,
    near_cache_stats: RwLock<HashMap<String, NearCacheStats>>,
    near_cache_entry_count: RwLock<HashMap<String, u64>>,
}

impl StatisticsCollector {
    /// Creates a new statistics collector.
    pub fn new() -> Self {
        Self {
            started_at: Instant::now(),
            connections_opened: AtomicU64::new(0),
            connections_closed: AtomicU64::new(0),
            bytes_sent: AtomicU64::new(0),
            bytes_received: AtomicU64::new(0),
            operation_counts: RwLock::new(HashMap::new()),
            operation_latencies: RwLock::new(HashMap::new()),
            near_cache_stats: RwLock::new(HashMap::new()),
            near_cache_entry_count: RwLock::new(HashMap::new()),
        }
    }

    /// Records a connection being opened.
    pub fn record_connection_opened(&self) {
        self.connections_opened.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a connection being closed.
    pub fn record_connection_closed(&self) {
        self.connections_closed.fetch_add(1, Ordering::Relaxed);
    }

    /// Records bytes sent to the cluster.
    pub fn record_bytes_sent(&self, bytes: u64) {
        self.bytes_sent.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Records bytes received from the cluster.
    pub fn record_bytes_received(&self, bytes: u64) {
        self.bytes_received.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Records an operation of the given type.
    pub async fn record_operation(&self, op_type: OperationType) {
        let mut counts = self.operation_counts.write().await;
        *counts.entry(op_type).or_insert(0) += 1;
    }

    /// Records an operation synchronously (for non-async contexts).
    pub fn record_operation_sync(&self, op_type: OperationType) {
        if let Ok(mut counts) = self.operation_counts.try_write() {
            *counts.entry(op_type).or_insert(0) += 1;
        }
    }

    /// Records an operation with its latency duration.
    pub async fn record_operation_with_latency(&self, op_type: OperationType, latency: Duration) {
        let mut counts = self.operation_counts.write().await;
        *counts.entry(op_type).or_insert(0) += 1;
        drop(counts);

        let mut latencies = self.operation_latencies.write().await;
        let entry = latencies.entry(op_type).or_insert_with(Vec::new);
        // Keep last 1000 samples to limit memory
        if entry.len() >= 1000 {
            entry.remove(0);
        }
        entry.push(latency);
    }

    /// Returns average latency for an operation type in milliseconds.
    pub async fn average_latency(&self, op_type: OperationType) -> Option<f64> {
        let latencies = self.operation_latencies.read().await;
        latencies.get(&op_type).and_then(|samples| {
            if samples.is_empty() {
                None
            } else {
                let total: Duration = samples.iter().sum();
                Some(total.as_secs_f64() * 1000.0 / samples.len() as f64)
            }
        })
    }

    /// Returns all operation latencies as a map of operation type to average latency in milliseconds.
    pub async fn all_average_latencies(&self) -> HashMap<OperationType, f64> {
        let latencies = self.operation_latencies.read().await;
        latencies
            .iter()
            .filter_map(|(op_type, samples)| {
                if samples.is_empty() {
                    None
                } else {
                    let total: Duration = samples.iter().sum();
                    let avg = total.as_secs_f64() * 1000.0 / samples.len() as f64;
                    Some((*op_type, avg))
                }
            })
            .collect()
    }

    /// Updates near cache statistics for a map.
    pub async fn update_near_cache_stats(&self, map_name: &str, stats: NearCacheStats) {
        let mut near_cache_stats = self.near_cache_stats.write().await;
        near_cache_stats.insert(map_name.to_string(), stats);
    }

    /// Updates near cache entry count for a map.
    pub async fn update_near_cache_entry_count(&self, map_name: &str, count: u64) {
        let mut counts = self.near_cache_entry_count.write().await;
        counts.insert(map_name.to_string(), count);
    }

    /// Collects a snapshot of current statistics.
    pub async fn collect(&self, active_connections: u64) -> ClientStatistics {
        let now = Instant::now();

        let connection_stats = ConnectionStats {
            active_connections,
            total_connections_opened: self.connections_opened.load(Ordering::Relaxed),
            total_connections_closed: self.connections_closed.load(Ordering::Relaxed),
            bytes_sent: self.bytes_sent.load(Ordering::Relaxed),
            bytes_received: self.bytes_received.load(Ordering::Relaxed),
        };

        let operation_counts = self.operation_counts.read().await.clone();
        let operation_latencies = self.all_average_latencies().await;

        let near_cache_raw = self.near_cache_stats.read().await;
        let near_cache_stats: HashMap<String, NearCacheRatioStats> = near_cache_raw
            .iter()
            .map(|(name, stats)| (name.clone(), NearCacheRatioStats::from_stats(stats)))
            .collect();

        let near_cache_entry_counts = self.near_cache_entry_count.read().await;
        let near_cache_bytes: u64 = near_cache_entry_counts.values().sum::<u64>() * 256;

        let memory_stats = MemoryStats {
            used_bytes: near_cache_bytes + (operation_counts.len() as u64 * 64),
            near_cache_bytes,
        };

        ClientStatistics {
            connection_stats,
            operation_counts,
            operation_latencies,
            near_cache_stats,
            memory_stats,
            collected_at: now,
            uptime: now.duration_since(self.started_at),
        }
    }

    /// Returns the client uptime.
    pub fn uptime(&self) -> Duration {
        self.started_at.elapsed()
    }
}

impl Default for StatisticsCollector {
    fn default() -> Self {
        Self::new()
    }
}

/// Configuration for statistics reporting.
#[derive(Debug, Clone)]
pub struct StatisticsReporterConfig {
    enabled: bool,
    interval: Duration,
}

impl StatisticsReporterConfig {
    /// Creates a new reporter configuration.
    pub fn new(enabled: bool, interval: Duration) -> Self {
        Self { enabled, interval }
    }

    /// Returns whether reporting is enabled.
    pub fn enabled(&self) -> bool {
        self.enabled
    }

    /// Returns the reporting interval.
    pub fn interval(&self) -> Duration {
        self.interval
    }
}

impl Default for StatisticsReporterConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interval: Duration::from_secs(5),
        }
    }
}

/// Reporter for periodic statistics publishing to cluster.
///
/// The reporter runs a background task that periodically collects statistics
/// and sends them to the cluster for monitoring purposes.
pub struct StatisticsReporter {
    collector: Arc<StatisticsCollector>,
    config: StatisticsReporterConfig,
    shutdown_tx: Option<tokio::sync::watch::Sender<bool>>,
}

impl StatisticsReporter {
    /// Creates a new statistics reporter.
    pub fn new(collector: Arc<StatisticsCollector>, config: StatisticsReporterConfig) -> Self {
        Self {
            collector,
            config,
            shutdown_tx: None,
        }
    }

    /// Starts the periodic reporting task.
    ///
    /// The `report_fn` is called with collected statistics at each interval.
    pub fn start<F>(&mut self, mut report_fn: F)
    where
        F: FnMut(ClientStatistics) + Send + 'static,
    {
        if !self.config.enabled {
            return;
        }

        let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
        self.shutdown_tx = Some(shutdown_tx);

        let collector = Arc::clone(&self.collector);
        let interval = self.config.interval;

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() {
                            tracing::debug!("statistics reporter shutting down");
                            break;
                        }
                    }
                    _ = ticker.tick() => {
                        let stats = collector.collect(0).await;
                        report_fn(stats);
                    }
                }
            }
        });

        tracing::info!(
            interval_ms = interval.as_millis(),
            "statistics reporter started"
        );
    }

    /// Stops the periodic reporting task.
    pub fn stop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(true);
        }
    }

    /// Returns whether the reporter is running.
    pub fn is_running(&self) -> bool {
        self.shutdown_tx.is_some()
    }

    /// Returns the statistics collector.
    pub fn collector(&self) -> &Arc<StatisticsCollector> {
        &self.collector
    }
}

impl Drop for StatisticsReporter {
    fn drop(&mut self) {
        self.stop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_operation_type_name() {
        assert_eq!(OperationType::MapGet.name(), "map.get");
        assert_eq!(OperationType::MapPut.name(), "map.put");
        assert_eq!(OperationType::SqlExecute.name(), "sql.execute");
    }

    #[test]
    fn test_operation_type_is_copy() {
        fn assert_copy<T: Copy>() {}
        assert_copy::<OperationType>();
    }

    #[test]
    fn test_connection_stats_default() {
        let stats = ConnectionStats::default();
        assert_eq!(stats.active_connections(), 0);
        assert_eq!(stats.total_connections_opened(), 0);
        assert_eq!(stats.total_connections_closed(), 0);
        assert_eq!(stats.bytes_sent(), 0);
        assert_eq!(stats.bytes_received(), 0);
    }

    #[test]
    fn test_near_cache_ratio_stats() {
        let base = NearCacheStats::default();
        let ratio_stats = NearCacheRatioStats::from_stats(&base);
        assert_eq!(ratio_stats.hits(), 0);
        assert_eq!(ratio_stats.misses(), 0);
        assert_eq!(ratio_stats.hit_ratio(), 0.0);
        assert_eq!(ratio_stats.miss_ratio(), 1.0);
    }

    #[test]
    fn test_memory_stats_default() {
        let stats = MemoryStats::default();
        assert_eq!(stats.used_bytes(), 0);
        assert_eq!(stats.near_cache_bytes(), 0);
    }

    #[test]
    fn test_statistics_collector_new() {
        let collector = StatisticsCollector::new();
        assert!(collector.uptime() < Duration::from_secs(1));
    }

    #[test]
    fn test_statistics_collector_connection_tracking() {
        let collector = StatisticsCollector::new();

        collector.record_connection_opened();
        collector.record_connection_opened();
        collector.record_connection_closed();

        assert_eq!(collector.connections_opened.load(Ordering::Relaxed), 2);
        assert_eq!(collector.connections_closed.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_statistics_collector_bytes_tracking() {
        let collector = StatisticsCollector::new();

        collector.record_bytes_sent(100);
        collector.record_bytes_sent(50);
        collector.record_bytes_received(200);

        assert_eq!(collector.bytes_sent.load(Ordering::Relaxed), 150);
        assert_eq!(collector.bytes_received.load(Ordering::Relaxed), 200);
    }

    #[test]
    fn test_statistics_collector_operation_sync() {
        let collector = StatisticsCollector::new();

        collector.record_operation_sync(OperationType::MapGet);
        collector.record_operation_sync(OperationType::MapGet);
        collector.record_operation_sync(OperationType::MapPut);

        let counts = collector.operation_counts.try_read().unwrap();
        assert_eq!(counts.get(&OperationType::MapGet), Some(&2));
        assert_eq!(counts.get(&OperationType::MapPut), Some(&1));
    }

    #[tokio::test]
    async fn test_statistics_collector_operation_async() {
        let collector = StatisticsCollector::new();

        collector.record_operation(OperationType::MapGet).await;
        collector.record_operation(OperationType::MapGet).await;
        collector.record_operation(OperationType::MapRemove).await;

        let counts = collector.operation_counts.read().await;
        assert_eq!(counts.get(&OperationType::MapGet), Some(&2));
        assert_eq!(counts.get(&OperationType::MapRemove), Some(&1));
    }

    #[tokio::test]
    async fn test_statistics_collector_collect() {
        let collector = StatisticsCollector::new();

        collector.record_connection_opened();
        collector.record_bytes_sent(1000);
        collector.record_bytes_received(2000);
        collector.record_operation(OperationType::MapGet).await;
        collector.record_operation(OperationType::MapPut).await;

        let stats = collector.collect(1).await;

        assert_eq!(stats.connection_stats().active_connections(), 1);
        assert_eq!(stats.connection_stats().total_connections_opened(), 1);
        assert_eq!(stats.connection_stats().bytes_sent(), 1000);
        assert_eq!(stats.connection_stats().bytes_received(), 2000);
        assert_eq!(stats.operation_count(OperationType::MapGet), 1);
        assert_eq!(stats.operation_count(OperationType::MapPut), 1);
        assert_eq!(stats.total_operations(), 2);
    }

    #[tokio::test]
    async fn test_statistics_collector_near_cache_stats() {
        let collector = StatisticsCollector::new();

        let near_cache_stats = NearCacheStats::default();
        collector
            .update_near_cache_stats("my-map", near_cache_stats)
            .await;
        collector.update_near_cache_entry_count("my-map", 100).await;

        let stats = collector.collect(0).await;

        assert!(stats.near_cache_stats_for("my-map").is_some());
        assert!(stats.memory_stats().near_cache_bytes() > 0);
    }

    #[test]
    fn test_client_statistics_aggregate_hit_ratio_empty() {
        let stats = ClientStatistics {
            connection_stats: ConnectionStats::default(),
            operation_counts: HashMap::new(),
            operation_latencies: HashMap::new(),
            near_cache_stats: HashMap::new(),
            memory_stats: MemoryStats::default(),
            collected_at: Instant::now(),
            uptime: Duration::ZERO,
        };

        assert_eq!(stats.aggregate_near_cache_hit_ratio(), 0.0);
    }

    #[test]
    fn test_client_statistics_operation_count_missing() {
        let stats = ClientStatistics {
            connection_stats: ConnectionStats::default(),
            operation_counts: HashMap::new(),
            operation_latencies: HashMap::new(),
            near_cache_stats: HashMap::new(),
            memory_stats: MemoryStats::default(),
            collected_at: Instant::now(),
            uptime: Duration::ZERO,
        };

        assert_eq!(stats.operation_count(OperationType::MapGet), 0);
    }

    #[test]
    fn test_statistics_reporter_config_default() {
        let config = StatisticsReporterConfig::default();
        assert!(config.enabled());
        assert_eq!(config.interval(), Duration::from_secs(5));
    }

    #[test]
    fn test_statistics_reporter_config_custom() {
        let config = StatisticsReporterConfig::new(false, Duration::from_secs(10));
        assert!(!config.enabled());
        assert_eq!(config.interval(), Duration::from_secs(10));
    }

    #[test]
    fn test_statistics_reporter_not_running_initially() {
        let collector = Arc::new(StatisticsCollector::new());
        let config = StatisticsReporterConfig::default();
        let reporter = StatisticsReporter::new(collector, config);

        assert!(!reporter.is_running());
    }

    #[test]
    fn test_statistics_reporter_disabled_does_not_start() {
        let collector = Arc::new(StatisticsCollector::new());
        let config = StatisticsReporterConfig::new(false, Duration::from_secs(1));
        let mut reporter = StatisticsReporter::new(collector, config);

        reporter.start(|_| {});

        assert!(!reporter.is_running());
    }

    #[tokio::test]
    async fn test_statistics_reporter_start_stop() {
        let collector = Arc::new(StatisticsCollector::new());
        let config = StatisticsReporterConfig::new(true, Duration::from_millis(100));
        let mut reporter = StatisticsReporter::new(collector, config);

        reporter.start(|_| {});
        assert!(reporter.is_running());

        reporter.stop();
        assert!(!reporter.is_running());
    }

    #[test]
    fn test_statistics_collector_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<StatisticsCollector>();
    }

    #[test]
    fn test_client_statistics_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<ClientStatistics>();
    }

    #[test]
    fn test_statistics_reporter_collector_accessor() {
        let collector = Arc::new(StatisticsCollector::new());
        let config = StatisticsReporterConfig::default();
        let reporter = StatisticsReporter::new(Arc::clone(&collector), config);

        assert!(Arc::ptr_eq(reporter.collector(), &collector));
    }

    #[tokio::test]
    async fn test_statistics_collector_latency_tracking() {
        let collector = StatisticsCollector::new();

        collector
            .record_operation_with_latency(OperationType::MapGet, Duration::from_millis(10))
            .await;
        collector
            .record_operation_with_latency(OperationType::MapGet, Duration::from_millis(20))
            .await;
        collector
            .record_operation_with_latency(OperationType::MapGet, Duration::from_millis(30))
            .await;

        let avg = collector.average_latency(OperationType::MapGet).await;
        assert!(avg.is_some());
        assert!((avg.unwrap() - 20.0).abs() < 0.001);
    }

    #[tokio::test]
    async fn test_statistics_collector_latency_none_for_untracked() {
        let collector = StatisticsCollector::new();
        let avg = collector.average_latency(OperationType::MapGet).await;
        assert!(avg.is_none());
    }

    #[tokio::test]
    async fn test_statistics_collector_all_latencies() {
        let collector = StatisticsCollector::new();

        collector
            .record_operation_with_latency(OperationType::MapGet, Duration::from_millis(10))
            .await;
        collector
            .record_operation_with_latency(OperationType::MapPut, Duration::from_millis(20))
            .await;

        let all = collector.all_average_latencies().await;
        assert_eq!(all.len(), 2);
        assert!(all.contains_key(&OperationType::MapGet));
        assert!(all.contains_key(&OperationType::MapPut));
    }

    #[tokio::test]
    async fn test_client_statistics_latencies() {
        let collector = StatisticsCollector::new();
        collector
            .record_operation_with_latency(OperationType::MapGet, Duration::from_millis(15))
            .await;

        let stats = collector.collect(0).await;
        assert!(stats.operation_latency(OperationType::MapGet).is_some());
        assert!(stats.operation_latency(OperationType::MapRemove).is_none());
    }

    #[test]
    fn test_near_cache_ratio_stats_clone() {
        let stats = NearCacheRatioStats {
            hits: 10,
            misses: 5,
            evictions: 2,
            expirations: 1,
            hit_ratio: 0.67,
        };

        let cloned = stats.clone();
        assert_eq!(cloned.hits(), 10);
        assert_eq!(cloned.misses(), 5);
    }

    #[test]
    fn test_connection_stats_clone() {
        let stats = ConnectionStats {
            active_connections: 5,
            total_connections_opened: 10,
            total_connections_closed: 5,
            bytes_sent: 1000,
            bytes_received: 2000,
        };

        let cloned = stats.clone();
        assert_eq!(cloned.active_connections(), 5);
        assert_eq!(cloned.bytes_sent(), 1000);
    }

    #[test]
    fn test_memory_stats_clone() {
        let stats = MemoryStats {
            used_bytes: 1024,
            near_cache_bytes: 512,
        };

        let cloned = stats.clone();
        assert_eq!(cloned.used_bytes(), 1024);
        assert_eq!(cloned.near_cache_bytes(), 512);
    }
}
