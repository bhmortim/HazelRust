//! Invocation service for concurrent Hazelcast operations.
//!
//! Manages pending operations with correlation-based response routing.
//! Each connection has a background reader task that routes incoming
//! messages to the correct waiter by correlation_id.
//!
//! Supports connection pooling: multiple data connections per member
//! to reduce write-Mutex contention under high concurrency.

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use dashmap::DashMap;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::{oneshot, Mutex, RwLock};

use hazelcast_core::protocol::constants::*;
use hazelcast_core::protocol::{ClientMessage, ClientMessageCodec, Frame};
use hazelcast_core::{HazelcastError, Result};
use tokio_util::codec::Decoder;

/// A pool of write connections to a single cluster member.
struct ConnectionPool {
    /// Write halves of the pooled connections.
    writers: Vec<Arc<Mutex<OwnedWriteHalf>>>,
    /// Round-robin counter for selecting the next connection.
    next: AtomicUsize,
}

impl ConnectionPool {
    /// Create a pool with a single connection.
    fn new(writer: Arc<Mutex<OwnedWriteHalf>>) -> Self {
        Self {
            writers: vec![writer],
            next: AtomicUsize::new(0),
        }
    }

    /// Add a connection to the pool.
    fn add(&mut self, writer: Arc<Mutex<OwnedWriteHalf>>) {
        self.writers.push(writer);
    }

    /// Select the next writer via round-robin.
    fn select(&self) -> &Arc<Mutex<OwnedWriteHalf>> {
        let idx = self.next.fetch_add(1, Ordering::Relaxed) % self.writers.len();
        &self.writers[idx]
    }

    /// Number of connections in the pool.
    fn size(&self) -> usize {
        self.writers.len()
    }
}

impl std::fmt::Debug for ConnectionPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionPool")
            .field("size", &self.writers.len())
            .finish()
    }
}

/// Manages concurrent invocations across Hazelcast connections.
///
/// Uses split TCP connections: write halves are protected by per-connection
/// mutexes, read halves run in background tasks that route responses to
/// pending operations by correlation_id.
///
/// With connection pooling enabled (pool_size > 1), multiple write
/// connections are opened per member, reducing Mutex contention under
/// concurrent load.
pub struct InvocationService {
    /// Per-member connection pools (each pool has 1..N write halves).
    /// Uses DashMap for lock-free reads on the hot path — `send_raw()`
    /// only needs a per-shard read lock instead of a global RwLock.
    pools: DashMap<SocketAddr, ConnectionPool>,
    /// Pending operation waiters keyed by correlation_id.
    pending_ops: Arc<DashMap<i64, oneshot::Sender<ClientMessage>>>,
    /// Set of map names for which CreateProxy has been sent.
    created_proxies: RwLock<HashSet<String>>,
    /// Invocation timeout.
    timeout: Duration,
    /// Reader task handles (kept alive).
    _readers: RwLock<Vec<tokio::task::JoinHandle<()>>>,
}

impl std::fmt::Debug for InvocationService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InvocationService")
            .field("pending_ops", &self.pending_ops.len())
            .finish()
    }
}

impl InvocationService {
    /// Create a new invocation service.
    pub fn new(timeout: Duration) -> Self {
        Self {
            pools: DashMap::new(),
            pending_ops: Arc::new(DashMap::new()),
            created_proxies: RwLock::new(HashSet::new()),
            timeout,
            _readers: RwLock::new(Vec::new()),
        }
    }

    /// Register a connection by splitting the TCP stream into read/write halves.
    /// The read half is moved to a background task that routes responses.
    ///
    /// If a pool already exists for this address, the connection is added to
    /// the existing pool. Otherwise a new pool is created.
    pub async fn register_connection(
        &self,
        address: SocketAddr,
        stream: tokio::net::TcpStream,
    ) {
        let (read_half, write_half) = stream.into_split();
        let write = Arc::new(Mutex::new(write_half));

        // Add writer to pool (create pool if first connection for this address)
        match self.pools.entry(address) {
            dashmap::mapref::entry::Entry::Occupied(mut entry) => {
                let pool = entry.get_mut();
                pool.add(write);
                tracing::info!(
                    address = %address,
                    pool_size = pool.size(),
                    "added connection to pool"
                );
            }
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                entry.insert(ConnectionPool::new(write));
            }
        }

        // Spawn background reader
        let pending = Arc::clone(&self.pending_ops);
        let handle = tokio::spawn(async move {
            Self::reader_loop(address, read_half, pending).await;
        });
        self._readers.write().await.push(handle);
    }

    /// Background reader loop that decodes messages and routes to pending ops.
    async fn reader_loop(
        address: SocketAddr,
        mut read: OwnedReadHalf,
        pending_ops: Arc<DashMap<i64, oneshot::Sender<ClientMessage>>>,
    ) {
        let mut codec = ClientMessageCodec::new();
        let mut read_buffer = BytesMut::with_capacity(16384);

        loop {
            // Try to decode from existing buffer
            match codec.decode(&mut read_buffer) {
                Ok(Some(msg)) => {
                    if let Some(corr_id) = msg.correlation_id() {
                        if let Some((_, tx)) = pending_ops.remove(&corr_id) {
                            let _ = tx.send(msg);
                        }
                        // Unmatched responses are silently discarded
                        // (cluster events, heartbeat acks, etc.)
                    }
                    continue; // Try to decode more from buffer
                }
                Ok(None) => {
                    // Need more data from socket
                }
                Err(e) => {
                    tracing::warn!(address = %address, error = %e, "reader decode error");
                    break;
                }
            }

            // Read from socket
            match read.read_buf(&mut read_buffer).await {
                Ok(0) => {
                    tracing::info!(address = %address, "connection closed by server");
                    break;
                }
                Ok(_) => {
                    // Data available, loop back to decode
                }
                Err(e) => {
                    tracing::warn!(address = %address, error = %e, "reader I/O error");
                    break;
                }
            }
        }
    }

    /// Send a message on a connection and wait for the correlated response.
    pub async fn invoke(
        &self,
        address: SocketAddr,
        mut message: ClientMessage,
    ) -> Result<ClientMessage> {
        let corr_id = message.correlation_id().unwrap_or(0);

        // Register pending operation BEFORE sending
        let (tx, rx) = oneshot::channel();
        self.pending_ops.insert(corr_id, tx);

        // Send the message
        if let Err(e) = self.send_raw(address, &mut message).await {
            self.pending_ops.remove(&corr_id);
            return Err(e);
        }

        // Wait for response with timeout
        match tokio::time::timeout(self.timeout, rx).await {
            Ok(Ok(response)) => Ok(response),
            Ok(Err(_)) => {
                self.pending_ops.remove(&corr_id);
                Err(HazelcastError::Connection(
                    "response channel dropped".to_string(),
                ))
            }
            Err(_) => {
                self.pending_ops.remove(&corr_id);
                Err(HazelcastError::Timeout(format!(
                    "invocation timed out after {:?}",
                    self.timeout
                )))
            }
        }
    }

    /// Send a message without waiting for a response.
    ///
    /// Uses round-robin selection across pooled connections to reduce
    /// contention on any single write Mutex.
    async fn send_raw(
        &self,
        address: SocketAddr,
        message: &mut ClientMessage,
    ) -> Result<()> {
        let pool_ref = self.pools.get(&address).ok_or_else(|| {
            HazelcastError::Connection(format!("no connection to {}", address))
        })?;

        let writer = pool_ref.select().clone();
        drop(pool_ref); // Release DashMap shard lock before awaiting

        let mut buf = BytesMut::with_capacity(256);
        message.write_to(&mut buf);

        let mut w = writer.lock().await;
        w.write_all(&buf).await.map_err(|e| {
            HazelcastError::Connection(format!("failed to write to {}: {}", address, e))
        })?;
        Ok(())
    }

    /// Ensure CreateProxy has been sent for a map name.
    pub async fn ensure_proxy(
        &self,
        address: SocketAddr,
        map_name: &str,
        service_name: &str,
    ) -> Result<()> {
        {
            let proxies = self.created_proxies.read().await;
            if proxies.contains(map_name) {
                return Ok(());
            }
        }

        let mut proxy_msg =
            ClientMessage::create_for_encode(CLIENT_CREATE_PROXY, PARTITION_ID_ANY);
        proxy_msg.add_frame(Frame::with_content(BytesMut::from(
            map_name.as_bytes(),
        )));
        let mut svc_frame =
            Frame::with_content(BytesMut::from(service_name.as_bytes()));
        svc_frame.flags |= IS_FINAL_FLAG;
        proxy_msg.add_frame(svc_frame);

        self.invoke(address, proxy_msg).await?;
        self.created_proxies
            .write()
            .await
            .insert(map_name.to_string());
        Ok(())
    }

    /// Get any available writer address.
    pub fn any_address(&self) -> Option<SocketAddr> {
        self.pools.iter().next().map(|entry| *entry.key())
    }

    /// Get all connected addresses.
    pub fn addresses(&self) -> Vec<SocketAddr> {
        self.pools.iter().map(|entry| *entry.key()).collect()
    }

    /// Check if a specific address has an active connection pool (O(1) DashMap lookup).
    pub fn has_address(&self, addr: &SocketAddr) -> bool {
        self.pools.contains_key(addr)
    }

    /// Check if a proxy has been created for the given name (fast byte-level check).
    /// Avoids String allocation on the hot path.
    pub fn is_proxy_created_bytes(&self, name_bytes: &[u8]) -> bool {
        if let Ok(name) = std::str::from_utf8(name_bytes) {
            if let Ok(proxies) = self.created_proxies.try_read() {
                return proxies.contains(name);
            }
        }
        false // Conservative: if lock contended, do the full ensure_proxy
    }
}
