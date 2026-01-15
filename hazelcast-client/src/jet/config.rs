//! Job configuration types for Jet streaming jobs.

use std::fmt;

/// Processing guarantee level for Jet jobs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum ProcessingGuarantee {
    /// No processing guarantee. Messages may be lost or duplicated.
    #[default]
    None,
    /// At-least-once processing. Messages are guaranteed to be processed
    /// at least once but may be duplicated in case of failures.
    AtLeastOnce,
    /// Exactly-once processing. Messages are guaranteed to be processed
    /// exactly once, even in case of failures.
    ExactlyOnce,
}

impl fmt::Display for ProcessingGuarantee {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            ProcessingGuarantee::None => "NONE",
            ProcessingGuarantee::AtLeastOnce => "AT_LEAST_ONCE",
            ProcessingGuarantee::ExactlyOnce => "EXACTLY_ONCE",
        };
        write!(f, "{}", s)
    }
}

/// Default snapshot interval in milliseconds.
const DEFAULT_SNAPSHOT_INTERVAL_MILLIS: u64 = 10_000;
/// Default max concurrent operations.
const DEFAULT_MAX_CONCURRENT_OPERATIONS: u32 = 256;

/// Configuration for a Jet streaming job.
#[derive(Debug, Clone)]
pub struct JobConfig {
    name: Option<String>,
    split_brain_protection: bool,
    auto_scaling: bool,
    processing_guarantee: ProcessingGuarantee,
    snapshot_interval_millis: u64,
    initial_snapshot_name: Option<String>,
    max_concurrent_operations: u32,
    suspend_on_failure: bool,
}

impl JobConfig {
    /// Creates a new job configuration builder.
    pub fn builder() -> JobConfigBuilder {
        JobConfigBuilder::new()
    }

    /// Returns the optional job name.
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    /// Returns whether split-brain protection is enabled.
    pub fn split_brain_protection(&self) -> bool {
        self.split_brain_protection
    }

    /// Returns whether auto-scaling is enabled.
    pub fn auto_scaling(&self) -> bool {
        self.auto_scaling
    }

    /// Returns the processing guarantee level.
    pub fn processing_guarantee(&self) -> ProcessingGuarantee {
        self.processing_guarantee
    }

    /// Returns the snapshot interval in milliseconds.
    pub fn snapshot_interval_millis(&self) -> u64 {
        self.snapshot_interval_millis
    }

    /// Returns the initial snapshot name to restore from.
    pub fn initial_snapshot_name(&self) -> Option<&str> {
        self.initial_snapshot_name.as_deref()
    }

    /// Returns the maximum number of concurrent async operations.
    pub fn max_concurrent_operations(&self) -> u32 {
        self.max_concurrent_operations
    }

    /// Returns whether the job should suspend on failure instead of failing.
    pub fn suspend_on_failure(&self) -> bool {
        self.suspend_on_failure
    }
}

impl Default for JobConfig {
    fn default() -> Self {
        Self {
            name: None,
            split_brain_protection: false,
            auto_scaling: true,
            processing_guarantee: ProcessingGuarantee::default(),
            snapshot_interval_millis: DEFAULT_SNAPSHOT_INTERVAL_MILLIS,
            initial_snapshot_name: None,
            max_concurrent_operations: DEFAULT_MAX_CONCURRENT_OPERATIONS,
            suspend_on_failure: false,
        }
    }
}

/// Builder for `JobConfig`.
#[derive(Debug, Clone, Default)]
pub struct JobConfigBuilder {
    name: Option<String>,
    split_brain_protection: Option<bool>,
    auto_scaling: Option<bool>,
    processing_guarantee: Option<ProcessingGuarantee>,
    snapshot_interval_millis: Option<u64>,
    initial_snapshot_name: Option<String>,
    max_concurrent_operations: Option<u32>,
    suspend_on_failure: Option<bool>,
}

impl JobConfigBuilder {
    /// Creates a new job configuration builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the job name.
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Enables or disables split-brain protection.
    pub fn split_brain_protection(mut self, enabled: bool) -> Self {
        self.split_brain_protection = Some(enabled);
        self
    }

    /// Enables or disables auto-scaling.
    pub fn auto_scaling(mut self, enabled: bool) -> Self {
        self.auto_scaling = Some(enabled);
        self
    }

    /// Sets the processing guarantee level.
    pub fn processing_guarantee(mut self, guarantee: ProcessingGuarantee) -> Self {
        self.processing_guarantee = Some(guarantee);
        self
    }

    /// Sets the snapshot interval in milliseconds.
    pub fn snapshot_interval_millis(mut self, millis: u64) -> Self {
        self.snapshot_interval_millis = Some(millis);
        self
    }

    /// Sets the initial snapshot name to restore from.
    pub fn initial_snapshot_name(mut self, name: impl Into<String>) -> Self {
        self.initial_snapshot_name = Some(name.into());
        self
    }

    /// Sets the maximum number of concurrent async operations.
    pub fn max_concurrent_operations(mut self, max: u32) -> Self {
        self.max_concurrent_operations = Some(max);
        self
    }

    /// Sets whether the job should suspend on failure instead of failing.
    pub fn suspend_on_failure(mut self, suspend: bool) -> Self {
        self.suspend_on_failure = Some(suspend);
        self
    }

    /// Builds the job configuration.
    pub fn build(self) -> JobConfig {
        JobConfig {
            name: self.name,
            split_brain_protection: self.split_brain_protection.unwrap_or(false),
            auto_scaling: self.auto_scaling.unwrap_or(true),
            processing_guarantee: self.processing_guarantee.unwrap_or_default(),
            snapshot_interval_millis: self
                .snapshot_interval_millis
                .unwrap_or(DEFAULT_SNAPSHOT_INTERVAL_MILLIS),
            initial_snapshot_name: self.initial_snapshot_name,
            max_concurrent_operations: self
                .max_concurrent_operations
                .unwrap_or(DEFAULT_MAX_CONCURRENT_OPERATIONS),
            suspend_on_failure: self.suspend_on_failure.unwrap_or(false),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_processing_guarantee_display() {
        assert_eq!(format!("{}", ProcessingGuarantee::None), "NONE");
        assert_eq!(format!("{}", ProcessingGuarantee::AtLeastOnce), "AT_LEAST_ONCE");
        assert_eq!(format!("{}", ProcessingGuarantee::ExactlyOnce), "EXACTLY_ONCE");
    }

    #[test]
    fn test_processing_guarantee_default() {
        assert_eq!(ProcessingGuarantee::default(), ProcessingGuarantee::None);
    }

    #[test]
    fn test_job_config_defaults() {
        let config = JobConfig::default();
        assert!(config.name().is_none());
        assert!(!config.split_brain_protection());
        assert!(config.auto_scaling());
        assert_eq!(config.processing_guarantee(), ProcessingGuarantee::None);
        assert_eq!(config.snapshot_interval_millis(), 10_000);
        assert!(config.initial_snapshot_name().is_none());
        assert_eq!(config.max_concurrent_operations(), 256);
        assert!(!config.suspend_on_failure());
    }

    #[test]
    fn test_job_config_builder_defaults() {
        let config = JobConfig::builder().build();
        assert!(config.name().is_none());
        assert!(!config.split_brain_protection());
        assert!(config.auto_scaling());
        assert_eq!(config.snapshot_interval_millis(), 10_000);
    }

    #[test]
    fn test_job_config_builder_custom() {
        let config = JobConfig::builder()
            .name("test-job")
            .split_brain_protection(true)
            .auto_scaling(false)
            .processing_guarantee(ProcessingGuarantee::ExactlyOnce)
            .snapshot_interval_millis(5000)
            .initial_snapshot_name("snapshot-1")
            .max_concurrent_operations(128)
            .suspend_on_failure(true)
            .build();

        assert_eq!(config.name(), Some("test-job"));
        assert!(config.split_brain_protection());
        assert!(!config.auto_scaling());
        assert_eq!(config.processing_guarantee(), ProcessingGuarantee::ExactlyOnce);
        assert_eq!(config.snapshot_interval_millis(), 5000);
        assert_eq!(config.initial_snapshot_name(), Some("snapshot-1"));
        assert_eq!(config.max_concurrent_operations(), 128);
        assert!(config.suspend_on_failure());
    }

    #[test]
    fn test_job_config_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<JobConfig>();
    }

    #[test]
    fn test_processing_guarantee_is_copy() {
        fn assert_copy<T: Copy>() {}
        assert_copy::<ProcessingGuarantee>();
    }
}
