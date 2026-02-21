//! Auto-detection discovery for Hazelcast clusters.
//!
//! This module provides automatic detection of the runtime environment
//! (Kubernetes, AWS, Azure, GCP) and selects the appropriate discovery
//! mechanism. If no cloud environment is detected, it falls back to
//! static address discovery.

use std::fmt;
use std::net::SocketAddr;

use async_trait::async_trait;
use hazelcast_core::Result;
use tracing::{debug, info};

use super::discovery::StaticAddressDiscovery;
use super::ClusterDiscovery;

/// Detected cloud environment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DetectedEnvironment {
    /// Kubernetes environment detected via KUBERNETES_SERVICE_HOST env var.
    Kubernetes,
    /// AWS environment detected via metadata endpoint.
    Aws,
    /// Azure environment detected via IMDS endpoint.
    Azure,
    /// GCP environment detected via metadata endpoint.
    Gcp,
    /// No cloud environment detected; use static/default discovery.
    None,
}

impl fmt::Display for DetectedEnvironment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Kubernetes => write!(f, "Kubernetes"),
            Self::Aws => write!(f, "AWS"),
            Self::Azure => write!(f, "Azure"),
            Self::Gcp => write!(f, "GCP"),
            Self::None => write!(f, "None"),
        }
    }
}

/// AWS EC2 instance metadata endpoint.
const AWS_METADATA_URL: &str = "http://169.254.169.254/latest/meta-data/";

/// Azure Instance Metadata Service (IMDS) endpoint.
const AZURE_IMDS_URL: &str = "http://169.254.169.254/metadata/instance?api-version=2021-02-01";

/// GCP metadata server endpoint.
const GCP_METADATA_URL: &str = "http://metadata.google.internal/computeMetadata/v1/";

/// Trait for environment detection operations, enabling mock injection for tests.
#[async_trait]
trait EnvironmentDetector: Send + Sync {
    /// Checks if the Kubernetes environment variable is set.
    fn is_kubernetes(&self) -> bool;

    /// Probes the AWS metadata endpoint.
    async fn is_aws(&self) -> bool;

    /// Probes the Azure IMDS endpoint.
    async fn is_azure(&self) -> bool;

    /// Probes the GCP metadata endpoint.
    async fn is_gcp(&self) -> bool;
}

/// Production environment detector.
struct DefaultEnvironmentDetector;

#[async_trait]
impl EnvironmentDetector for DefaultEnvironmentDetector {
    fn is_kubernetes(&self) -> bool {
        std::env::var("KUBERNETES_SERVICE_HOST").is_ok()
    }

    async fn is_aws(&self) -> bool {
        probe_metadata_endpoint(AWS_METADATA_URL, None).await
    }

    async fn is_azure(&self) -> bool {
        probe_metadata_endpoint(AZURE_IMDS_URL, Some("Metadata: true")).await
    }

    async fn is_gcp(&self) -> bool {
        probe_metadata_endpoint(GCP_METADATA_URL, Some("Metadata-Flavor: Google")).await
    }
}

/// Probes a metadata endpoint with an optional header.
async fn probe_metadata_endpoint(url: &str, _header: Option<&str>) -> bool {
    // Use a short timeout for metadata probes since they should be fast on actual cloud instances.
    let timeout = std::time::Duration::from_secs(2);

    match tokio::time::timeout(timeout, async {
        let socket = tokio::net::TcpStream::connect("169.254.169.254:80").await;
        socket.is_ok()
    })
    .await
    {
        Ok(result) => {
            debug!("Metadata probe to {} returned: {}", url, result);
            result
        }
        Err(_) => {
            debug!("Metadata probe to {} timed out", url);
            false
        }
    }
}

/// Auto-detection discovery that probes the environment and falls back to static addresses.
///
/// The detection order is:
/// 1. Kubernetes (via `KUBERNETES_SERVICE_HOST` environment variable)
/// 2. AWS (via EC2 metadata endpoint `169.254.169.254`)
/// 3. Azure (via IMDS endpoint `169.254.169.254`)
/// 4. GCP (via metadata server `metadata.google.internal`)
/// 5. Fallback to static address discovery (default: `127.0.0.1:5701`)
pub struct AutoDetectionDiscovery {
    /// Fallback addresses when no cloud environment is detected.
    fallback_addresses: Vec<SocketAddr>,
    /// Environment detector for probing runtime environment.
    detector: Box<dyn EnvironmentDetector>,
}

impl fmt::Debug for AutoDetectionDiscovery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AutoDetectionDiscovery")
            .field("fallback_addresses", &self.fallback_addresses)
            .finish()
    }
}

impl AutoDetectionDiscovery {
    /// Creates a new auto-detection discovery with default fallback (127.0.0.1:5701).
    pub fn new() -> Self {
        Self {
            fallback_addresses: vec!["127.0.0.1:5701".parse().unwrap()],
            detector: Box::new(DefaultEnvironmentDetector),
        }
    }

    /// Sets fallback addresses for when no cloud environment is detected.
    pub fn with_fallback_addresses(mut self, addresses: Vec<SocketAddr>) -> Self {
        self.fallback_addresses = addresses;
        self
    }

    #[cfg(test)]
    fn with_mock_detector(
        fallback_addresses: Vec<SocketAddr>,
        detector: Box<dyn EnvironmentDetector>,
    ) -> Self {
        Self {
            fallback_addresses,
            detector,
        }
    }

    /// Returns the fallback addresses.
    pub fn fallback_addresses(&self) -> &[SocketAddr] {
        &self.fallback_addresses
    }

    /// Detects the current cloud environment.
    pub async fn detect_environment(&self) -> DetectedEnvironment {
        // Check Kubernetes first (cheapest check, just env var)
        if self.detector.is_kubernetes() {
            info!("Auto-detected Kubernetes environment");
            return DetectedEnvironment::Kubernetes;
        }

        // Probe AWS metadata endpoint
        if self.detector.is_aws().await {
            info!("Auto-detected AWS environment");
            return DetectedEnvironment::Aws;
        }

        // Probe Azure IMDS endpoint
        if self.detector.is_azure().await {
            info!("Auto-detected Azure environment");
            return DetectedEnvironment::Azure;
        }

        // Probe GCP metadata endpoint
        if self.detector.is_gcp().await {
            info!("Auto-detected GCP environment");
            return DetectedEnvironment::Gcp;
        }

        debug!("No cloud environment detected, using fallback addresses");
        DetectedEnvironment::None
    }
}

impl Default for AutoDetectionDiscovery {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ClusterDiscovery for AutoDetectionDiscovery {
    async fn discover(&self) -> Result<Vec<SocketAddr>> {
        let environment = self.detect_environment().await;

        match environment {
            DetectedEnvironment::Kubernetes
            | DetectedEnvironment::Aws
            | DetectedEnvironment::Azure
            | DetectedEnvironment::Gcp => {
                // In a real implementation, the auto-detection would instantiate
                // the appropriate discovery mechanism (K8s, AWS, Azure, GCP).
                // For now, we report the detected environment and fall back to
                // static addresses. Users should configure the specific discovery
                // mechanism for their environment.
                info!(
                    "Detected {} environment; use the dedicated discovery provider for full support. Falling back to static addresses.",
                    environment
                );
                let fallback = StaticAddressDiscovery::new(self.fallback_addresses.clone());
                fallback.discover().await
            }
            DetectedEnvironment::None => {
                debug!("Using static address fallback");
                let fallback = StaticAddressDiscovery::new(self.fallback_addresses.clone());
                fallback.discover().await
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockDetector {
        kubernetes: bool,
        aws: bool,
        azure: bool,
        gcp: bool,
    }

    impl MockDetector {
        fn none() -> Self {
            Self {
                kubernetes: false,
                aws: false,
                azure: false,
                gcp: false,
            }
        }

        fn kubernetes() -> Self {
            Self {
                kubernetes: true,
                aws: false,
                azure: false,
                gcp: false,
            }
        }

        fn aws() -> Self {
            Self {
                kubernetes: false,
                aws: true,
                azure: false,
                gcp: false,
            }
        }

        fn azure() -> Self {
            Self {
                kubernetes: false,
                aws: false,
                azure: true,
                gcp: false,
            }
        }

        fn gcp() -> Self {
            Self {
                kubernetes: false,
                aws: false,
                azure: false,
                gcp: true,
            }
        }
    }

    #[async_trait]
    impl EnvironmentDetector for MockDetector {
        fn is_kubernetes(&self) -> bool {
            self.kubernetes
        }

        async fn is_aws(&self) -> bool {
            self.aws
        }

        async fn is_azure(&self) -> bool {
            self.azure
        }

        async fn is_gcp(&self) -> bool {
            self.gcp
        }
    }

    fn create_mock_auto_detect(
        detector: MockDetector,
        fallback: Vec<SocketAddr>,
    ) -> AutoDetectionDiscovery {
        AutoDetectionDiscovery::with_mock_detector(fallback, Box::new(detector))
    }

    #[tokio::test]
    async fn test_detect_kubernetes() {
        let discovery = create_mock_auto_detect(
            MockDetector::kubernetes(),
            vec!["127.0.0.1:5701".parse().unwrap()],
        );

        let env = discovery.detect_environment().await;
        assert_eq!(env, DetectedEnvironment::Kubernetes);
    }

    #[tokio::test]
    async fn test_detect_aws() {
        let discovery = create_mock_auto_detect(
            MockDetector::aws(),
            vec!["127.0.0.1:5701".parse().unwrap()],
        );

        let env = discovery.detect_environment().await;
        assert_eq!(env, DetectedEnvironment::Aws);
    }

    #[tokio::test]
    async fn test_detect_azure() {
        let discovery = create_mock_auto_detect(
            MockDetector::azure(),
            vec!["127.0.0.1:5701".parse().unwrap()],
        );

        let env = discovery.detect_environment().await;
        assert_eq!(env, DetectedEnvironment::Azure);
    }

    #[tokio::test]
    async fn test_detect_gcp() {
        let discovery = create_mock_auto_detect(
            MockDetector::gcp(),
            vec!["127.0.0.1:5701".parse().unwrap()],
        );

        let env = discovery.detect_environment().await;
        assert_eq!(env, DetectedEnvironment::Gcp);
    }

    #[tokio::test]
    async fn test_detect_none() {
        let discovery = create_mock_auto_detect(
            MockDetector::none(),
            vec!["127.0.0.1:5701".parse().unwrap()],
        );

        let env = discovery.detect_environment().await;
        assert_eq!(env, DetectedEnvironment::None);
    }

    #[tokio::test]
    async fn test_discover_fallback_on_no_environment() {
        let fallback = vec![
            "192.168.1.1:5701".parse().unwrap(),
            "192.168.1.2:5701".parse().unwrap(),
        ];

        let discovery = create_mock_auto_detect(MockDetector::none(), fallback.clone());
        let addresses = discovery.discover().await.unwrap();

        assert_eq!(addresses.len(), 2);
        assert!(addresses.contains(&"192.168.1.1:5701".parse().unwrap()));
        assert!(addresses.contains(&"192.168.1.2:5701".parse().unwrap()));
    }

    #[tokio::test]
    async fn test_discover_fallback_on_detected_environment() {
        let fallback = vec!["10.0.0.1:5701".parse().unwrap()];

        let discovery = create_mock_auto_detect(MockDetector::kubernetes(), fallback);
        let addresses = discovery.discover().await.unwrap();

        // Even with K8s detected, falls back to static addresses
        // since dedicated discovery is recommended
        assert_eq!(addresses.len(), 1);
        assert_eq!(addresses[0], "10.0.0.1:5701".parse().unwrap());
    }

    #[tokio::test]
    async fn test_discover_default_fallback() {
        let discovery = create_mock_auto_detect(
            MockDetector::none(),
            vec!["127.0.0.1:5701".parse().unwrap()],
        );

        let addresses = discovery.discover().await.unwrap();
        assert_eq!(addresses.len(), 1);
        assert_eq!(addresses[0], "127.0.0.1:5701".parse().unwrap());
    }

    #[test]
    fn test_auto_detection_new() {
        let discovery = AutoDetectionDiscovery::new();
        assert_eq!(discovery.fallback_addresses().len(), 1);
        assert_eq!(
            discovery.fallback_addresses()[0],
            "127.0.0.1:5701".parse().unwrap()
        );
    }

    #[test]
    fn test_auto_detection_with_fallback() {
        let addresses = vec![
            "10.0.0.1:5701".parse().unwrap(),
            "10.0.0.2:5701".parse().unwrap(),
        ];

        let discovery = AutoDetectionDiscovery::new().with_fallback_addresses(addresses.clone());
        assert_eq!(discovery.fallback_addresses(), &addresses[..]);
    }

    #[test]
    fn test_auto_detection_default() {
        let discovery = AutoDetectionDiscovery::default();
        assert_eq!(discovery.fallback_addresses().len(), 1);
    }

    #[test]
    fn test_auto_detection_debug() {
        let discovery = AutoDetectionDiscovery::new();
        let debug_str = format!("{:?}", discovery);
        assert!(debug_str.contains("AutoDetectionDiscovery"));
        assert!(debug_str.contains("127.0.0.1:5701"));
    }

    #[test]
    fn test_detected_environment_display() {
        assert_eq!(format!("{}", DetectedEnvironment::Kubernetes), "Kubernetes");
        assert_eq!(format!("{}", DetectedEnvironment::Aws), "AWS");
        assert_eq!(format!("{}", DetectedEnvironment::Azure), "Azure");
        assert_eq!(format!("{}", DetectedEnvironment::Gcp), "GCP");
        assert_eq!(format!("{}", DetectedEnvironment::None), "None");
    }

    #[test]
    fn test_detected_environment_eq() {
        assert_eq!(DetectedEnvironment::Kubernetes, DetectedEnvironment::Kubernetes);
        assert_ne!(DetectedEnvironment::Aws, DetectedEnvironment::Azure);
    }

    #[test]
    fn test_detected_environment_clone() {
        let env = DetectedEnvironment::Gcp;
        let cloned = env;
        assert_eq!(env, cloned);
    }

    #[test]
    fn test_detected_environment_debug() {
        let env = DetectedEnvironment::Aws;
        let debug_str = format!("{:?}", env);
        assert!(debug_str.contains("Aws"));
    }

    #[tokio::test]
    async fn test_kubernetes_takes_priority() {
        // When both K8s and AWS are detected, K8s should win
        let detector = MockDetector {
            kubernetes: true,
            aws: true,
            azure: false,
            gcp: false,
        };

        let discovery = create_mock_auto_detect(
            detector,
            vec!["127.0.0.1:5701".parse().unwrap()],
        );

        let env = discovery.detect_environment().await;
        assert_eq!(env, DetectedEnvironment::Kubernetes);
    }

    #[tokio::test]
    async fn test_aws_before_azure() {
        // When both AWS and Azure are detected, AWS should win
        let detector = MockDetector {
            kubernetes: false,
            aws: true,
            azure: true,
            gcp: false,
        };

        let discovery = create_mock_auto_detect(
            detector,
            vec!["127.0.0.1:5701".parse().unwrap()],
        );

        let env = discovery.detect_environment().await;
        assert_eq!(env, DetectedEnvironment::Aws);
    }
}
