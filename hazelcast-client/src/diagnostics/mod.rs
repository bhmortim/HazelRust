//! Diagnostics and monitoring utilities.

mod slow_ops;
mod statistics;

pub use slow_ops::{OperationTracker, SlowOperationDetector};
pub use statistics::{
    ClientStatistics, ConnectionStats, MemoryStats, NearCacheRatioStats, OperationType,
    StatisticsCollector, StatisticsReporter, StatisticsReporterConfig,
};
