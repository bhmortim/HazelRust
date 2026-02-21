//! Socket interceptor for customizing TCP connections.
//!
//! The socket interceptor allows applications to customize TCP socket options
//! and perform actions on newly established connections before they are used
//! for Hazelcast protocol communication.
//!
//! # Example
//!
//! ```ignore
//! use hazelcast_client::connection::{SocketInterceptor, SocketOptions};
//! use tokio::net::TcpStream;
//! use async_trait::async_trait;
//!
//! struct MyInterceptor;
//!
//! #[async_trait]
//! impl SocketInterceptor for MyInterceptor {
//!     async fn on_connect(&self, stream: &mut TcpStream) -> hazelcast_core::Result<()> {
//!         // Custom handshake or authentication
//!         Ok(())
//!     }
//!
//!     fn socket_options(&self) -> SocketOptions {
//!         SocketOptions {
//!             tcp_nodelay: Some(true),
//!             send_buffer_size: Some(128 * 1024),
//!             receive_buffer_size: Some(128 * 1024),
//!             keep_alive: Some(true),
//!             linger: None,
//!         }
//!     }
//! }
//! ```

use std::time::Duration;

use async_trait::async_trait;
use tokio::net::TcpStream;

use hazelcast_core::Result;

/// Socket-level options applied to TCP connections.
///
/// Each field is optional; `None` means the system default is used.
#[derive(Debug, Clone, Default)]
pub struct SocketOptions {
    /// Whether to disable Nagle's algorithm (`TCP_NODELAY`).
    ///
    /// When `true`, small packets are sent immediately without waiting
    /// to fill a full TCP segment. This reduces latency at the cost of
    /// slightly higher bandwidth usage.
    pub tcp_nodelay: Option<bool>,

    /// The send buffer size in bytes (`SO_SNDBUF`).
    pub send_buffer_size: Option<usize>,

    /// The receive buffer size in bytes (`SO_RCVBUF`).
    pub receive_buffer_size: Option<usize>,

    /// Whether TCP keep-alive probes are enabled (`SO_KEEPALIVE`).
    ///
    /// When `true`, the operating system sends periodic keep-alive probes
    /// to detect dead connections.
    pub keep_alive: Option<bool>,

    /// The linger timeout (`SO_LINGER`).
    ///
    /// When set, the socket will linger for the specified duration after
    /// being closed, allowing any remaining data to be transmitted.
    /// `None` means the system default linger behavior.
    pub linger: Option<Duration>,
}

/// Interceptor for customizing TCP socket connections.
///
/// Implement this trait to perform custom actions when a new TCP connection
/// is established, such as:
///
/// - Setting custom socket options
/// - Performing a custom handshake or authentication
/// - Logging or auditing new connections
/// - Injecting monitoring or metrics hooks
///
/// The `on_connect` method is called after the TCP connection is established
/// but before any Hazelcast protocol communication begins.
#[async_trait]
pub trait SocketInterceptor: Send + Sync {
    /// Called when a new TCP connection is established.
    ///
    /// This method receives a mutable reference to the raw TCP stream,
    /// allowing any custom initialization before Hazelcast protocol
    /// negotiation begins.
    ///
    /// # Errors
    ///
    /// Return an error to reject the connection. The connection manager
    /// will close the socket and may retry depending on the retry policy.
    async fn on_connect(&self, stream: &mut TcpStream) -> Result<()>;

    /// Returns the socket options to apply to new connections.
    ///
    /// This method is called before `on_connect`. The returned options
    /// are applied to the TCP stream before any further processing.
    ///
    /// The default implementation returns `SocketOptions::default()`,
    /// which means all options use their system defaults.
    fn socket_options(&self) -> SocketOptions {
        SocketOptions::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_socket_options_default() {
        let opts = SocketOptions::default();
        assert!(opts.tcp_nodelay.is_none());
        assert!(opts.send_buffer_size.is_none());
        assert!(opts.receive_buffer_size.is_none());
        assert!(opts.keep_alive.is_none());
        assert!(opts.linger.is_none());
    }

    #[test]
    fn test_socket_options_custom() {
        let opts = SocketOptions {
            tcp_nodelay: Some(true),
            send_buffer_size: Some(65536),
            receive_buffer_size: Some(65536),
            keep_alive: Some(true),
            linger: Some(Duration::from_secs(5)),
        };
        assert_eq!(opts.tcp_nodelay, Some(true));
        assert_eq!(opts.send_buffer_size, Some(65536));
        assert_eq!(opts.receive_buffer_size, Some(65536));
        assert_eq!(opts.keep_alive, Some(true));
        assert_eq!(opts.linger, Some(Duration::from_secs(5)));
    }
}
