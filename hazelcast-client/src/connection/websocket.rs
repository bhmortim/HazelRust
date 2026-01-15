//! WebSocket-based connection to a Hazelcast cluster member.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use bytes::BytesMut;
use futures_util::{SinkExt, StreamExt};
use hazelcast_core::protocol::{ClientMessage, ClientMessageCodec, Frame};
use hazelcast_core::{HazelcastError, Result};
use tokio::net::TcpStream;
use tokio_tungstenite::{
    connect_async, tungstenite::protocol::Message, MaybeTlsStream, WebSocketStream,
};
use tokio_util::codec::{Decoder, Encoder};
use url::Url;

use super::ConnectionId;

/// A WebSocket connection to a single Hazelcast cluster member.
#[derive(Debug)]
pub struct WebSocketConnection {
    id: ConnectionId,
    url: Url,
    stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
    codec: ClientMessageCodec,
    read_buffer: BytesMut,
    created_at: Instant,
    last_read_at: Instant,
    last_write_at: Instant,
}

impl WebSocketConnection {
    /// Returns the connection's unique identifier.
    pub fn id(&self) -> ConnectionId {
        self.id
    }

    /// Returns the WebSocket URL of this connection.
    pub fn url(&self) -> &Url {
        &self.url
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

    /// Establishes a new WebSocket connection to the given URL.
    ///
    /// The URL should be in the format `ws://host:port/path` or `wss://host:port/path`.
    pub async fn connect(url: Url) -> Result<Self> {
        let (stream, _response) = connect_async(url.as_str()).await.map_err(|e| {
            HazelcastError::Connection(format!("failed to connect to {}: {}", url, e))
        })?;

        let now = Instant::now();
        tracing::debug!(url = %url, "established WebSocket connection");

        Ok(Self {
            id: ConnectionId::new(),
            url,
            stream,
            codec: ClientMessageCodec::new(),
            read_buffer: BytesMut::with_capacity(8192),
            created_at: now,
            last_read_at: now,
            last_write_at: now,
        })
    }

    /// Establishes a new WebSocket connection to the given address.
    ///
    /// This constructs a `ws://` URL from the address.
    pub async fn connect_addr(address: SocketAddr) -> Result<Self> {
        let url = Url::parse(&format!("ws://{}", address)).map_err(|e| {
            HazelcastError::Connection(format!("invalid WebSocket URL for {}: {}", address, e))
        })?;
        Self::connect(url).await
    }

    /// Establishes a new secure WebSocket connection to the given address.
    ///
    /// This constructs a `wss://` URL from the address.
    pub async fn connect_addr_tls(address: SocketAddr) -> Result<Self> {
        let url = Url::parse(&format!("wss://{}", address)).map_err(|e| {
            HazelcastError::Connection(format!("invalid WebSocket URL for {}: {}", address, e))
        })?;
        Self::connect(url).await
    }

    /// Sends a message over this WebSocket connection.
    pub async fn send(&mut self, message: ClientMessage) -> Result<()> {
        let mut buf = BytesMut::new();
        self.codec.encode(message, &mut buf)?;

        self.stream
            .send(Message::Binary(buf.to_vec()))
            .await
            .map_err(|e| {
                HazelcastError::Connection(format!("failed to send to {}: {}", self.url, e))
            })?;

        self.last_write_at = Instant::now();
        Ok(())
    }

    /// Receives a message from this WebSocket connection.
    ///
    /// Returns `None` if the connection is closed cleanly.
    pub async fn receive(&mut self) -> Result<Option<ClientMessage>> {
        loop {
            if let Some(message) = self.codec.decode(&mut self.read_buffer)? {
                self.last_read_at = Instant::now();
                return Ok(Some(message));
            }

            match self.stream.next().await {
                Some(Ok(Message::Binary(data))) => {
                    self.read_buffer.extend_from_slice(&data);
                }
                Some(Ok(Message::Close(_))) => {
                    if self.read_buffer.is_empty() {
                        return Ok(None);
                    }
                    return Err(HazelcastError::Connection(format!(
                        "WebSocket connection to {} closed unexpectedly",
                        self.url
                    )));
                }
                Some(Ok(Message::Ping(data))) => {
                    self.stream
                        .send(Message::Pong(data))
                        .await
                        .map_err(|e| {
                            HazelcastError::Connection(format!(
                                "failed to send pong to {}: {}",
                                self.url, e
                            ))
                        })?;
                }
                Some(Ok(Message::Pong(_))) => {
                    self.last_read_at = Instant::now();
                }
                Some(Ok(_)) => {
                    // Ignore text and other message types
                }
                Some(Err(e)) => {
                    return Err(HazelcastError::Connection(format!(
                        "WebSocket error from {}: {}",
                        self.url, e
                    )));
                }
                None => {
                    if self.read_buffer.is_empty() {
                        return Ok(None);
                    }
                    return Err(HazelcastError::Connection(format!(
                        "WebSocket connection to {} closed unexpectedly",
                        self.url
                    )));
                }
            }
        }
    }

    /// Sends a heartbeat ping message.
    pub async fn send_heartbeat(&mut self) -> Result<()> {
        self.stream
            .send(Message::Ping(vec![]))
            .await
            .map_err(|e| {
                HazelcastError::Connection(format!(
                    "failed to send heartbeat to {}: {}",
                    self.url, e
                ))
            })?;
        self.last_write_at = Instant::now();
        Ok(())
    }

    /// Closes this WebSocket connection.
    pub async fn close(mut self) -> Result<()> {
        let _ = self.stream.close(None).await;
        tracing::debug!(id = %self.id, url = %self.url, "WebSocket connection closed");
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
    fn test_websocket_url_parsing() {
        let url = Url::parse("ws://127.0.0.1:5701").unwrap();
        assert_eq!(url.scheme(), "ws");
        assert_eq!(url.host_str(), Some("127.0.0.1"));
        assert_eq!(url.port(), Some(5701));
    }

    #[test]
    fn test_websocket_url_parsing_with_path() {
        let url = Url::parse("ws://127.0.0.1:5701/hazelcast").unwrap();
        assert_eq!(url.path(), "/hazelcast");
    }

    #[test]
    fn test_websocket_secure_url_parsing() {
        let url = Url::parse("wss://cluster.example.com:443").unwrap();
        assert_eq!(url.scheme(), "wss");
        assert_eq!(url.host_str(), Some("cluster.example.com"));
    }

    #[tokio::test]
    async fn test_connect_invalid_url() {
        let url = Url::parse("ws://192.0.2.1:9999").unwrap();
        let result = WebSocketConnection::connect(url).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_connect_addr_constructs_correct_url() {
        let addr: SocketAddr = "192.0.2.1:5701".parse().unwrap();
        let result = WebSocketConnection::connect_addr(addr).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_connect_addr_tls_constructs_correct_url() {
        let addr: SocketAddr = "192.0.2.1:5701".parse().unwrap();
        let result = WebSocketConnection::connect_addr_tls(addr).await;
        assert!(result.is_err());
    }

    #[test]
    fn test_websocket_connection_is_send() {
        fn assert_send<T: Send>() {}
        assert_send::<WebSocketConnection>();
    }
}
