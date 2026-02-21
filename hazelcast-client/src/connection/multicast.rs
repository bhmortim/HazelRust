//! Multicast-based cluster discovery for Hazelcast.
//!
//! This module provides automatic discovery of Hazelcast cluster members
//! using UDP multicast. Members announce their presence on a multicast group,
//! and this discovery mechanism listens for those announcements.

use std::fmt;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use async_trait::async_trait;
use hazelcast_core::{HazelcastError, Result};
use tokio::net::UdpSocket;
use tracing::debug;

use super::ClusterDiscovery;

/// Default multicast group address used by Hazelcast.
const DEFAULT_MULTICAST_GROUP: Ipv4Addr = Ipv4Addr::new(224, 2, 2, 3);

/// Default multicast port used by Hazelcast.
const DEFAULT_MULTICAST_PORT: u16 = 54327;

/// Default discovery timeout.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(3);

/// Hazelcast port for member connections.
const DEFAULT_HAZELCAST_PORT: u16 = 5701;

/// Configuration for multicast-based cluster discovery.
#[derive(Debug, Clone)]
pub struct MulticastDiscoveryConfig {
    /// Multicast group address (default: 224.2.2.3).
    group: Ipv4Addr,
    /// Multicast port (default: 54327).
    port: u16,
    /// Timeout for waiting for multicast responses.
    timeout: Duration,
    /// List of trusted interface patterns for filtering responses.
    trusted_interfaces: Vec<String>,
    /// Hazelcast member port (default: 5701).
    hazelcast_port: u16,
}

impl MulticastDiscoveryConfig {
    /// Creates a new multicast discovery configuration with defaults.
    pub fn new() -> Self {
        Self {
            group: DEFAULT_MULTICAST_GROUP,
            port: DEFAULT_MULTICAST_PORT,
            timeout: DEFAULT_TIMEOUT,
            trusted_interfaces: Vec::new(),
            hazelcast_port: DEFAULT_HAZELCAST_PORT,
        }
    }

    /// Sets the multicast group address.
    pub fn with_group(mut self, group: Ipv4Addr) -> Self {
        self.group = group;
        self
    }

    /// Sets the multicast port.
    pub fn with_port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    /// Sets the discovery timeout.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Sets the list of trusted interface patterns.
    ///
    /// Only responses from addresses matching these patterns will be accepted.
    /// If empty, all responses are accepted.
    pub fn with_trusted_interfaces(mut self, interfaces: Vec<String>) -> Self {
        self.trusted_interfaces = interfaces;
        self
    }

    /// Adds a trusted interface pattern.
    pub fn add_trusted_interface(mut self, interface: impl Into<String>) -> Self {
        self.trusted_interfaces.push(interface.into());
        self
    }

    /// Sets the Hazelcast member port (default: 5701).
    pub fn with_hazelcast_port(mut self, port: u16) -> Self {
        self.hazelcast_port = port;
        self
    }

    /// Returns the multicast group address.
    pub fn group(&self) -> Ipv4Addr {
        self.group
    }

    /// Returns the multicast port.
    pub fn port(&self) -> u16 {
        self.port
    }

    /// Returns the discovery timeout.
    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    /// Returns the trusted interface patterns.
    pub fn trusted_interfaces(&self) -> &[String] {
        &self.trusted_interfaces
    }

    /// Returns the Hazelcast member port.
    pub fn hazelcast_port(&self) -> u16 {
        self.hazelcast_port
    }
}

impl Default for MulticastDiscoveryConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// Trait for multicast socket operations, enabling mock injection for tests.
#[async_trait]
trait MulticastSocket: Send + Sync {
    /// Sends a discovery request to the multicast group.
    async fn send_discovery_request(&self) -> Result<()>;

    /// Receives responses until timeout, returning discovered addresses.
    async fn receive_responses(&self, timeout: Duration) -> Result<Vec<SocketAddr>>;
}

/// Production multicast socket using tokio UdpSocket.
struct TokioMulticastSocket {
    socket: UdpSocket,
    multicast_addr: SocketAddr,
    hazelcast_port: u16,
}

impl TokioMulticastSocket {
    async fn new(config: &MulticastDiscoveryConfig) -> Result<Self> {
        let bind_addr: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), config.port);
        let socket = UdpSocket::bind(bind_addr)
            .await
            .map_err(|e| HazelcastError::Connection(format!("Failed to bind multicast socket: {}", e)))?;

        socket
            .join_multicast_v4(config.group, Ipv4Addr::UNSPECIFIED)
            .map_err(|e| HazelcastError::Connection(format!("Failed to join multicast group: {}", e)))?;

        let multicast_addr = SocketAddr::new(IpAddr::V4(config.group), config.port);

        Ok(Self {
            socket,
            multicast_addr,
            hazelcast_port: config.hazelcast_port,
        })
    }
}

#[async_trait]
impl MulticastSocket for TokioMulticastSocket {
    async fn send_discovery_request(&self) -> Result<()> {
        // Send a Hazelcast multicast discovery request.
        // The discovery message is a simple "HZC" marker followed by the protocol version.
        let discovery_message = b"HZC\x00\x01";

        self.socket
            .send_to(discovery_message, self.multicast_addr)
            .await
            .map_err(|e| HazelcastError::Connection(format!("Failed to send multicast request: {}", e)))?;

        debug!("Sent multicast discovery request to {}", self.multicast_addr);
        Ok(())
    }

    async fn receive_responses(&self, timeout: Duration) -> Result<Vec<SocketAddr>> {
        let mut addresses = Vec::new();
        let mut buf = [0u8; 1024];

        let deadline = tokio::time::Instant::now() + timeout;

        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                break;
            }

            match tokio::time::timeout(remaining, self.socket.recv_from(&mut buf)).await {
                Ok(Ok((len, src_addr))) => {
                    if len > 0 {
                        let member_addr = SocketAddr::new(src_addr.ip(), self.hazelcast_port);
                        if !addresses.contains(&member_addr) {
                            debug!("Discovered Hazelcast member via multicast: {}", member_addr);
                            addresses.push(member_addr);
                        }
                    }
                }
                Ok(Err(e)) => {
                    debug!("Error receiving multicast response: {}", e);
                    break;
                }
                Err(_) => {
                    // Timeout reached
                    break;
                }
            }
        }

        Ok(addresses)
    }
}

/// Multicast-based cluster discovery.
///
/// Discovers Hazelcast cluster members by sending a multicast discovery
/// request and listening for member responses on the configured multicast group.
pub struct MulticastDiscovery {
    config: MulticastDiscoveryConfig,
    #[cfg(test)]
    mock_socket: Option<Box<dyn MulticastSocket>>,
}

impl fmt::Debug for MulticastDiscovery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MulticastDiscovery")
            .field("config", &self.config)
            .finish()
    }
}

impl MulticastDiscovery {
    /// Creates a new multicast discovery instance with the given configuration.
    pub fn new(config: MulticastDiscoveryConfig) -> Self {
        Self {
            config,
            #[cfg(test)]
            mock_socket: None,
        }
    }

    #[cfg(test)]
    fn with_mock_socket(config: MulticastDiscoveryConfig, socket: Box<dyn MulticastSocket>) -> Self {
        Self {
            config,
            mock_socket: Some(socket),
        }
    }

    /// Returns the discovery configuration.
    pub fn config(&self) -> &MulticastDiscoveryConfig {
        &self.config
    }

    /// Checks if a source address matches the trusted interface patterns.
    fn is_trusted(&self, addr: &SocketAddr) -> bool {
        if self.config.trusted_interfaces.is_empty() {
            return true;
        }

        let ip_str = addr.ip().to_string();
        self.config.trusted_interfaces.iter().any(|pattern| {
            if pattern.contains('*') {
                // Simple wildcard matching
                let parts: Vec<&str> = pattern.split('*').collect();
                if parts.len() == 2 {
                    ip_str.starts_with(parts[0]) && ip_str.ends_with(parts[1])
                } else {
                    ip_str == *pattern
                }
            } else {
                ip_str == *pattern
            }
        })
    }

    async fn discover_members(&self) -> Result<Vec<SocketAddr>> {
        #[cfg(test)]
        if let Some(ref socket) = self.mock_socket {
            socket.send_discovery_request().await?;
            let addresses = socket.receive_responses(self.config.timeout).await?;
            return Ok(addresses
                .into_iter()
                .filter(|addr| self.is_trusted(addr))
                .collect());
        }

        let socket = TokioMulticastSocket::new(&self.config).await?;
        socket.send_discovery_request().await?;
        let addresses = socket.receive_responses(self.config.timeout).await?;

        Ok(addresses
            .into_iter()
            .filter(|addr| self.is_trusted(addr))
            .collect())
    }
}

#[async_trait]
impl ClusterDiscovery for MulticastDiscovery {
    async fn discover(&self) -> Result<Vec<SocketAddr>> {
        let addresses = self.discover_members().await?;

        if addresses.is_empty() {
            tracing::warn!("Multicast discovery found no Hazelcast members");
        } else {
            tracing::info!(
                "Multicast discovery found {} Hazelcast member(s)",
                addresses.len()
            );
        }

        Ok(addresses)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    struct MockMulticastSocket {
        addresses: Mutex<Vec<SocketAddr>>,
        should_error: bool,
    }

    impl MockMulticastSocket {
        fn new(addresses: Vec<SocketAddr>) -> Self {
            Self {
                addresses: Mutex::new(addresses),
                should_error: false,
            }
        }

        fn with_error() -> Self {
            Self {
                addresses: Mutex::new(vec![]),
                should_error: true,
            }
        }
    }

    #[async_trait]
    impl MulticastSocket for MockMulticastSocket {
        async fn send_discovery_request(&self) -> Result<()> {
            if self.should_error {
                return Err(HazelcastError::Connection("Mock multicast send error".into()));
            }
            Ok(())
        }

        async fn receive_responses(&self, _timeout: Duration) -> Result<Vec<SocketAddr>> {
            if self.should_error {
                return Err(HazelcastError::Connection("Mock multicast receive error".into()));
            }
            Ok(self.addresses.lock().unwrap().clone())
        }
    }

    fn create_mock_discovery(
        config: MulticastDiscoveryConfig,
        addresses: Vec<SocketAddr>,
    ) -> MulticastDiscovery {
        MulticastDiscovery::with_mock_socket(config, Box::new(MockMulticastSocket::new(addresses)))
    }

    #[test]
    fn test_config_defaults() {
        let config = MulticastDiscoveryConfig::new();
        assert_eq!(config.group(), Ipv4Addr::new(224, 2, 2, 3));
        assert_eq!(config.port(), 54327);
        assert_eq!(config.timeout(), Duration::from_secs(3));
        assert!(config.trusted_interfaces().is_empty());
        assert_eq!(config.hazelcast_port(), 5701);
    }

    #[test]
    fn test_config_builder() {
        let config = MulticastDiscoveryConfig::new()
            .with_group(Ipv4Addr::new(239, 1, 1, 1))
            .with_port(12345)
            .with_timeout(Duration::from_secs(10))
            .with_trusted_interfaces(vec!["10.0.0.*".to_string()])
            .with_hazelcast_port(5702);

        assert_eq!(config.group(), Ipv4Addr::new(239, 1, 1, 1));
        assert_eq!(config.port(), 12345);
        assert_eq!(config.timeout(), Duration::from_secs(10));
        assert_eq!(config.trusted_interfaces(), &["10.0.0.*".to_string()]);
        assert_eq!(config.hazelcast_port(), 5702);
    }

    #[test]
    fn test_config_add_trusted_interface() {
        let config = MulticastDiscoveryConfig::new()
            .add_trusted_interface("192.168.1.*")
            .add_trusted_interface("10.0.0.*");

        assert_eq!(config.trusted_interfaces().len(), 2);
        assert_eq!(config.trusted_interfaces()[0], "192.168.1.*");
        assert_eq!(config.trusted_interfaces()[1], "10.0.0.*");
    }

    #[test]
    fn test_config_default_trait() {
        let config = MulticastDiscoveryConfig::default();
        assert_eq!(config.group(), DEFAULT_MULTICAST_GROUP);
        assert_eq!(config.port(), DEFAULT_MULTICAST_PORT);
    }

    #[test]
    fn test_config_clone() {
        let config = MulticastDiscoveryConfig::new()
            .with_group(Ipv4Addr::new(239, 0, 0, 1))
            .with_port(9999);

        let cloned = config.clone();
        assert_eq!(cloned.group(), Ipv4Addr::new(239, 0, 0, 1));
        assert_eq!(cloned.port(), 9999);
    }

    #[test]
    fn test_config_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<MulticastDiscoveryConfig>();
    }

    #[test]
    fn test_config_debug() {
        let config = MulticastDiscoveryConfig::new();
        let debug_str = format!("{:?}", config);
        assert!(debug_str.contains("MulticastDiscoveryConfig"));
        assert!(debug_str.contains("224.2.2.3"));
    }

    #[test]
    fn test_discovery_creation() {
        let config = MulticastDiscoveryConfig::new();
        let discovery = MulticastDiscovery::new(config);
        assert_eq!(discovery.config().group(), DEFAULT_MULTICAST_GROUP);
    }

    #[test]
    fn test_discovery_debug() {
        let config = MulticastDiscoveryConfig::new();
        let discovery = MulticastDiscovery::new(config);
        let debug_str = format!("{:?}", discovery);
        assert!(debug_str.contains("MulticastDiscovery"));
    }

    #[test]
    fn test_trusted_interface_empty_allows_all() {
        let config = MulticastDiscoveryConfig::new();
        let discovery = MulticastDiscovery::new(config);

        let addr: SocketAddr = "192.168.1.100:5701".parse().unwrap();
        assert!(discovery.is_trusted(&addr));
    }

    #[test]
    fn test_trusted_interface_exact_match() {
        let config = MulticastDiscoveryConfig::new()
            .with_trusted_interfaces(vec!["192.168.1.100".to_string()]);
        let discovery = MulticastDiscovery::new(config);

        let trusted: SocketAddr = "192.168.1.100:5701".parse().unwrap();
        let untrusted: SocketAddr = "192.168.1.101:5701".parse().unwrap();

        assert!(discovery.is_trusted(&trusted));
        assert!(!discovery.is_trusted(&untrusted));
    }

    #[test]
    fn test_trusted_interface_wildcard_match() {
        let config = MulticastDiscoveryConfig::new()
            .with_trusted_interfaces(vec!["10.0.0.*".to_string()]);
        let discovery = MulticastDiscovery::new(config);

        let trusted: SocketAddr = "10.0.0.5:5701".parse().unwrap();
        let untrusted: SocketAddr = "192.168.1.5:5701".parse().unwrap();

        assert!(discovery.is_trusted(&trusted));
        assert!(!discovery.is_trusted(&untrusted));
    }

    #[tokio::test]
    async fn test_discovery_returns_addresses() {
        let config = MulticastDiscoveryConfig::new();
        let addresses = vec![
            "10.0.0.1:5701".parse().unwrap(),
            "10.0.0.2:5701".parse().unwrap(),
        ];

        let discovery = create_mock_discovery(config, addresses);
        let result = discovery.discover().await.unwrap();

        assert_eq!(result.len(), 2);
        assert!(result.contains(&"10.0.0.1:5701".parse().unwrap()));
        assert!(result.contains(&"10.0.0.2:5701".parse().unwrap()));
    }

    #[tokio::test]
    async fn test_discovery_filters_untrusted() {
        let config = MulticastDiscoveryConfig::new()
            .with_trusted_interfaces(vec!["10.0.0.*".to_string()]);

        let addresses = vec![
            "10.0.0.1:5701".parse().unwrap(),
            "192.168.1.1:5701".parse().unwrap(),
            "10.0.0.2:5701".parse().unwrap(),
        ];

        let discovery = create_mock_discovery(config, addresses);
        let result = discovery.discover().await.unwrap();

        assert_eq!(result.len(), 2);
        assert!(result.contains(&"10.0.0.1:5701".parse().unwrap()));
        assert!(result.contains(&"10.0.0.2:5701".parse().unwrap()));
    }

    #[tokio::test]
    async fn test_discovery_handles_empty_response() {
        let config = MulticastDiscoveryConfig::new();
        let discovery = create_mock_discovery(config, vec![]);
        let result = discovery.discover().await.unwrap();

        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_discovery_handles_error() {
        let config = MulticastDiscoveryConfig::new();
        let discovery = MulticastDiscovery::with_mock_socket(
            config,
            Box::new(MockMulticastSocket::with_error()),
        );

        let result = discovery.discover().await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Mock multicast send error"));
    }

    #[test]
    fn test_discovery_config_accessor() {
        let config = MulticastDiscoveryConfig::new()
            .with_group(Ipv4Addr::new(239, 1, 1, 1));
        let discovery = MulticastDiscovery::new(config);

        assert_eq!(discovery.config().group(), Ipv4Addr::new(239, 1, 1, 1));
    }
}
