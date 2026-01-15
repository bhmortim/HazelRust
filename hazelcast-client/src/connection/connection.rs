//! Single connection to a Hazelcast cluster member.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use bytes::BytesMut;
use hazelcast_core::protocol::{ClientMessage, ClientMessageCodec, Frame};
use hazelcast_core::{HazelcastError, Result};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_util::codec::{Decoder, Encoder};

#[cfg(feature = "tls")]
use crate::config::TlsConfig;

/// Unique identifier for a connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ConnectionId(u64);

impl ConnectionId {
    /// Generates a new unique connection ID.
    pub fn new() -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(1);
        Self(COUNTER.fetch_add(1, Ordering::Relaxed))
    }

    /// Returns the raw ID value.
    pub fn value(&self) -> u64 {
        self.0
    }
}

impl Default for ConnectionId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for ConnectionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "conn-{}", self.0)
    }
}

/// Wrapper for connection streams supporting both plain and TLS.
enum ConnectionStream {
    Plain(TcpStream),
    #[cfg(feature = "tls")]
    Tls(tokio_rustls::client::TlsStream<TcpStream>),
}

impl std::fmt::Debug for ConnectionStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Plain(stream) => f.debug_tuple("Plain").field(stream).finish(),
            #[cfg(feature = "tls")]
            Self::Tls(_) => f.debug_tuple("Tls").field(&"<TlsStream>").finish(),
        }
    }
}

impl ConnectionStream {
    async fn read_buf(&mut self, buf: &mut BytesMut) -> std::io::Result<usize> {
        match self {
            Self::Plain(stream) => stream.read_buf(buf).await,
            #[cfg(feature = "tls")]
            Self::Tls(stream) => stream.read_buf(buf).await,
        }
    }

    async fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        match self {
            Self::Plain(stream) => stream.write_all(buf).await,
            #[cfg(feature = "tls")]
            Self::Tls(stream) => stream.write_all(buf).await,
        }
    }
}

/// A connection to a single Hazelcast cluster member.
#[derive(Debug)]
pub struct Connection {
    id: ConnectionId,
    address: SocketAddr,
    stream: ConnectionStream,
    codec: ClientMessageCodec,
    read_buffer: BytesMut,
    created_at: Instant,
    last_read_at: Instant,
    last_write_at: Instant,
}

impl Connection {
    /// Creates a new connection from an established TCP stream.
    pub fn new(stream: TcpStream, address: SocketAddr) -> Self {
        let now = Instant::now();
        Self {
            id: ConnectionId::new(),
            address,
            stream: ConnectionStream::Plain(stream),
            codec: ClientMessageCodec::new(),
            read_buffer: BytesMut::with_capacity(8192),
            created_at: now,
            last_read_at: now,
            last_write_at: now,
        }
    }

    #[cfg(feature = "tls")]
    fn new_tls(stream: tokio_rustls::client::TlsStream<TcpStream>, address: SocketAddr) -> Self {
        let now = Instant::now();
        Self {
            id: ConnectionId::new(),
            address,
            stream: ConnectionStream::Tls(stream),
            codec: ClientMessageCodec::new(),
            read_buffer: BytesMut::with_capacity(8192),
            created_at: now,
            last_read_at: now,
            last_write_at: now,
        }
    }

    /// Returns the connection's unique identifier.
    pub fn id(&self) -> ConnectionId {
        self.id
    }

    /// Returns the remote address of this connection.
    pub fn address(&self) -> SocketAddr {
        self.address
    }

    /// Returns when this connection was created.
    pub fn created_at(&self) -> Instant {
        self.created_at
    }

    /// Returns when data was last read from this connection.
    pub fn last_read_at(&self) -> Instant {
        self.last_read_at
    }

    /// Returns when data was last written to this connection.
    pub fn last_write_at(&self) -> Instant {
        self.last_write_at
    }

    /// Establishes a new connection to the given address.
    pub async fn connect(address: SocketAddr) -> Result<Self> {
        let stream = TcpStream::connect(address).await.map_err(|e| {
            HazelcastError::Connection(format!("failed to connect to {}: {}", address, e))
        })?;

        stream.set_nodelay(true).map_err(|e| {
            HazelcastError::Connection(format!("failed to set TCP_NODELAY: {}", e))
        })?;

        tracing::debug!(address = %address, "established connection");
        Ok(Self::new(stream, address))
    }

    /// Establishes a new TLS connection to the given address.
    #[cfg(feature = "tls")]
    pub async fn connect_tls(
        address: SocketAddr,
        tls_config: &TlsConfig,
        server_name: Option<&str>,
    ) -> Result<Self> {
        use std::io::BufReader;
        use std::sync::Arc;
        use tokio_rustls::rustls::{ClientConfig, RootCertStore};
        use tokio_rustls::TlsConnector;

        let stream = TcpStream::connect(address).await.map_err(|e| {
            HazelcastError::Connection(format!("failed to connect to {}: {}", address, e))
        })?;

        stream.set_nodelay(true).map_err(|e| {
            HazelcastError::Connection(format!("failed to set TCP_NODELAY: {}", e))
        })?;

        let tls_stream = Self::perform_tls_handshake(stream, address, tls_config, server_name).await?;

        tracing::debug!(address = %address, "established TLS connection");
        Ok(Self::new_tls(tls_stream, address))
    }

    #[cfg(feature = "tls")]
    async fn perform_tls_handshake(
        stream: TcpStream,
        address: SocketAddr,
        tls_config: &TlsConfig,
        server_name: Option<&str>,
    ) -> Result<tokio_rustls::client::TlsStream<TcpStream>> {
        use std::io::BufReader;
        use std::sync::Arc;
        use tokio_rustls::rustls::{ClientConfig, RootCertStore};
        use tokio_rustls::TlsConnector;

        let mut root_store = RootCertStore::empty();

        if let Some(ca_path) = tls_config.ca_cert_path() {
            let ca_file = std::fs::File::open(ca_path).map_err(|e| {
                HazelcastError::Connection(format!("failed to open CA certificate: {}", e))
            })?;
            let mut reader = BufReader::new(ca_file);
            let certs = rustls_pemfile::certs(&mut reader)
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|e| {
                    HazelcastError::Connection(format!("failed to parse CA certificate: {}", e))
                })?;

            for cert in certs {
                root_store.add(cert).map_err(|e| {
                    HazelcastError::Connection(format!("failed to add CA certificate: {}", e))
                })?;
            }
        } else {
            root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        }

        let config_builder = ClientConfig::builder().with_root_certificates(root_store);

        let client_config = if tls_config.has_client_auth() {
            let cert_path = tls_config.client_cert_path().unwrap();
            let key_path = tls_config.client_key_path().unwrap();

            let cert_file = std::fs::File::open(cert_path).map_err(|e| {
                HazelcastError::Connection(format!("failed to open client certificate: {}", e))
            })?;
            let mut cert_reader = BufReader::new(cert_file);
            let certs = rustls_pemfile::certs(&mut cert_reader)
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|e| {
                    HazelcastError::Connection(format!("failed to parse client certificate: {}", e))
                })?;

            let key_file = std::fs::File::open(key_path).map_err(|e| {
                HazelcastError::Connection(format!("failed to open client key: {}", e))
            })?;
            let mut key_reader = BufReader::new(key_file);
            let key = rustls_pemfile::private_key(&mut key_reader)
                .map_err(|e| {
                    HazelcastError::Connection(format!("failed to parse client key: {}", e))
                })?
                .ok_or_else(|| {
                    HazelcastError::Connection("no private key found in file".to_string())
                })?;

            config_builder.with_client_auth_cert(certs, key).map_err(|e| {
                HazelcastError::Connection(format!("failed to configure client auth: {}", e))
            })?
        } else {
            config_builder.with_no_client_auth()
        };

        let connector = TlsConnector::from(Arc::new(client_config));

        let name = server_name
            .map(|s| s.to_string())
            .unwrap_or_else(|| address.ip().to_string());

        let server_name = rustls_pki_types::ServerName::try_from(name.clone()).map_err(|e| {
            HazelcastError::Connection(format!("invalid server name '{}': {}", name, e))
        })?;

        let tls_stream = connector.connect(server_name, stream).await.map_err(|e| {
            HazelcastError::Connection(format!("TLS handshake failed with {}: {}", address, e))
        })?;

        Ok(tls_stream)
    }

    /// Sends a message over this connection.
    pub async fn send(&mut self, message: ClientMessage) -> Result<()> {
        let mut buf = BytesMut::new();
        self.codec.encode(message, &mut buf)?;

        self.stream.write_all(&buf).await.map_err(|e| {
            HazelcastError::Connection(format!("failed to write to {}: {}", self.address, e))
        })?;

        self.last_write_at = Instant::now();
        Ok(())
    }

    /// Receives a message from this connection.
    ///
    /// Returns `None` if the connection is closed cleanly.
    pub async fn receive(&mut self) -> Result<Option<ClientMessage>> {
        loop {
            if let Some(message) = self.codec.decode(&mut self.read_buffer)? {
                self.last_read_at = Instant::now();
                return Ok(Some(message));
            }

            let bytes_read = self.stream.read_buf(&mut self.read_buffer).await.map_err(|e| {
                HazelcastError::Connection(format!("failed to read from {}: {}", self.address, e))
            })?;

            if bytes_read == 0 {
                if self.read_buffer.is_empty() {
                    return Ok(None);
                }
                return Err(HazelcastError::Connection(format!(
                    "connection to {} closed unexpectedly",
                    self.address
                )));
            }
        }
    }

    /// Sends a heartbeat ping message.
    pub async fn send_heartbeat(&mut self) -> Result<()> {
        use hazelcast_core::protocol::constants::PARTITION_ID_ANY;

        const HEARTBEAT_MESSAGE_TYPE: i32 = 0x000200;

        let heartbeat = ClientMessage::create_for_encode(HEARTBEAT_MESSAGE_TYPE, PARTITION_ID_ANY);
        self.send(heartbeat).await
    }

    /// Closes this connection.
    pub async fn close(self) -> Result<()> {
        drop(self.stream);
        tracing::debug!(id = %self.id, address = %self.address, "connection closed");
        Ok(())
    }

    /// Checks if this connection is still alive based on the heartbeat interval.
    pub fn is_alive(&self, heartbeat_timeout: std::time::Duration) -> bool {
        self.last_read_at.elapsed() < heartbeat_timeout
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connection_id_uniqueness() {
        let id1 = ConnectionId::new();
        let id2 = ConnectionId::new();
        let id3 = ConnectionId::new();

        assert_ne!(id1, id2);
        assert_ne!(id2, id3);
        assert_ne!(id1, id3);
    }

    #[test]
    fn test_connection_id_display() {
        let id = ConnectionId(42);
        assert_eq!(id.to_string(), "conn-42");
    }

    #[test]
    fn test_connection_id_value() {
        let id = ConnectionId(123);
        assert_eq!(id.value(), 123);
    }
}
