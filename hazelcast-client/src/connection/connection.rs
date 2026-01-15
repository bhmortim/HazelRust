//! Single connection to a Hazelcast cluster member.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use bytes::BytesMut;
use hazelcast_core::protocol::{ClientMessage, ClientMessageCodec, Frame};
use hazelcast_core::{HazelcastError, Result};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_util::codec::{Decoder, Encoder};

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

/// A connection to a single Hazelcast cluster member.
#[derive(Debug)]
pub struct Connection {
    id: ConnectionId,
    address: SocketAddr,
    stream: TcpStream,
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
            stream,
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
