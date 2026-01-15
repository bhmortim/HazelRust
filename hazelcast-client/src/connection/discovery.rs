//! Cluster member discovery mechanisms.

use std::net::SocketAddr;

use async_trait::async_trait;
use hazelcast_core::Result;

/// Trait for discovering Hazelcast cluster member addresses.
#[async_trait]
pub trait ClusterDiscovery: Send + Sync + std::fmt::Debug {
    /// Discovers available cluster member addresses.
    async fn discover(&self) -> Result<Vec<SocketAddr>>;
}

/// Static address discovery using a pre-configured list of addresses.
#[derive(Debug, Clone)]
pub struct StaticAddressDiscovery {
    addresses: Vec<SocketAddr>,
}

impl StaticAddressDiscovery {
    /// Creates a new static discovery with the given addresses.
    pub fn new(addresses: Vec<SocketAddr>) -> Self {
        Self { addresses }
    }

    /// Creates a static discovery from a single address.
    pub fn from_address(address: SocketAddr) -> Self {
        Self::new(vec![address])
    }

    /// Returns the configured addresses.
    pub fn addresses(&self) -> &[SocketAddr] {
        &self.addresses
    }
}

impl Default for StaticAddressDiscovery {
    fn default() -> Self {
        Self::new(vec!["127.0.0.1:5701".parse().unwrap()])
    }
}

#[async_trait]
impl ClusterDiscovery for StaticAddressDiscovery {
    async fn discover(&self) -> Result<Vec<SocketAddr>> {
        Ok(self.addresses.clone())
    }
}

impl<T> From<T> for StaticAddressDiscovery
where
    T: IntoIterator<Item = SocketAddr>,
{
    fn from(addresses: T) -> Self {
        Self::new(addresses.into_iter().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_static_discovery_returns_addresses() {
        let addr1: SocketAddr = "192.168.1.1:5701".parse().unwrap();
        let addr2: SocketAddr = "192.168.1.2:5701".parse().unwrap();

        let discovery = StaticAddressDiscovery::new(vec![addr1, addr2]);
        let result = discovery.discover().await.unwrap();

        assert_eq!(result.len(), 2);
        assert!(result.contains(&addr1));
        assert!(result.contains(&addr2));
    }

    #[tokio::test]
    async fn test_static_discovery_default() {
        let discovery = StaticAddressDiscovery::default();
        let result = discovery.discover().await.unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0], "127.0.0.1:5701".parse::<SocketAddr>().unwrap());
    }

    #[tokio::test]
    async fn test_static_discovery_from_single_address() {
        let addr: SocketAddr = "10.0.0.1:5701".parse().unwrap();
        let discovery = StaticAddressDiscovery::from_address(addr);

        assert_eq!(discovery.addresses(), &[addr]);
    }

    #[test]
    fn test_static_discovery_from_iterator() {
        let addrs: Vec<SocketAddr> = vec![
            "192.168.1.1:5701".parse().unwrap(),
            "192.168.1.2:5701".parse().unwrap(),
        ];

        let discovery: StaticAddressDiscovery = addrs.clone().into();
        assert_eq!(discovery.addresses(), &addrs[..]);
    }

    #[test]
    fn test_static_discovery_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<StaticAddressDiscovery>();
    }

    #[test]
    fn test_static_discovery_debug() {
        let discovery = StaticAddressDiscovery::default();
        let debug_str = format!("{:?}", discovery);
        assert!(debug_str.contains("StaticAddressDiscovery"));
    }

    #[test]
    fn test_static_discovery_clone() {
        let original = StaticAddressDiscovery::new(vec![
            "192.168.1.1:5701".parse().unwrap(),
            "192.168.1.2:5701".parse().unwrap(),
        ]);

        let cloned = original.clone();

        assert_eq!(original.addresses(), cloned.addresses());
    }

    #[tokio::test]
    async fn test_static_discovery_empty_addresses() {
        let discovery = StaticAddressDiscovery::new(vec![]);
        let result = discovery.discover().await.unwrap();

        assert!(result.is_empty());
    }

    #[test]
    fn test_static_discovery_from_array() {
        let addrs: [SocketAddr; 2] = [
            "192.168.1.1:5701".parse().unwrap(),
            "192.168.1.2:5701".parse().unwrap(),
        ];

        let discovery: StaticAddressDiscovery = addrs.into();
        assert_eq!(discovery.addresses().len(), 2);
    }
}
