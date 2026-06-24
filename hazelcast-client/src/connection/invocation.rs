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

    /// Returns the first (stable) writer in the pool. Used to pin a sequence of
    /// requests (e.g. a transaction) to a single server connection/endpoint.
    fn first(&self) -> &Arc<Mutex<OwnedWriteHalf>> {
        &self.writers[0]
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
    /// Long-lived event handlers keyed by the listener registration correlation_id.
    event_handlers: Arc<DashMap<i64, Arc<dyn Fn(ClientMessage) + Send + Sync>>>,
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
            event_handlers: Arc::new(DashMap::new()),
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
    pub async fn register_connection(&self, address: SocketAddr, stream: tokio::net::TcpStream) {
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
        let events = Arc::clone(&self.event_handlers);
        let handle = tokio::spawn(async move {
            Self::reader_loop(address, read_half, pending, events).await;
        });
        self._readers.write().await.push(handle);
    }

    /// Background reader loop that decodes messages and routes to pending ops.
    async fn reader_loop(
        address: SocketAddr,
        mut read: OwnedReadHalf,
        pending_ops: Arc<DashMap<i64, oneshot::Sender<ClientMessage>>>,
        event_handlers: Arc<DashMap<i64, Arc<dyn Fn(ClientMessage) + Send + Sync>>>,
    ) {
        let mut codec = ClientMessageCodec::new();
        let mut read_buffer = BytesMut::with_capacity(16384);

        loop {
            // Try to decode from existing buffer
            match codec.decode(&mut read_buffer) {
                Ok(Some(msg)) => {
                    if msg.is_event() {
                        // Listener event: route to the registered handler (which
                        // stays registered for the lifetime of the listener).
                        if let Some(corr_id) = msg.correlation_id() {
                            if let Some(h) = event_handlers.get(&corr_id) {
                                let handler = Arc::clone(h.value());
                                drop(h);
                                handler(msg);
                            }
                        }
                    } else if let Some(corr_id) = msg.correlation_id() {
                        if let Some((_, tx)) = pending_ops.remove(&corr_id) {
                            let _ = tx.send(msg);
                        }
                        // Unmatched non-event responses are discarded.
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

    /// Registers a long-lived event handler for a listener registration's
    /// correlation id. Events with this correlation id are routed to `handler`
    /// (in addition to the registration response completing the invocation).
    pub fn register_event_handler(
        &self,
        correlation_id: i64,
        handler: Arc<dyn Fn(ClientMessage) + Send + Sync>,
    ) {
        self.event_handlers.insert(correlation_id, handler);
    }

    /// Removes a previously registered event handler.
    pub fn deregister_event_handler(&self, correlation_id: i64) {
        self.event_handlers.remove(&correlation_id);
    }

    /// Registers an event handler for the message's correlation id, then invokes
    /// it and returns the (registration) response. Subsequent server events with
    /// the same correlation id are delivered to `handler`.
    pub async fn invoke_listener(
        &self,
        address: SocketAddr,
        message: ClientMessage,
        handler: Arc<dyn Fn(ClientMessage) + Send + Sync>,
    ) -> Result<ClientMessage> {
        let corr_id = message.correlation_id().unwrap_or(0);
        self.register_event_handler(corr_id, handler);
        match self.invoke(address, message).await {
            Ok(r) => Ok(r),
            Err(e) => {
                self.deregister_event_handler(corr_id);
                Err(e)
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
            Ok(Ok(response)) => check_response(response),
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
    async fn send_raw(&self, address: SocketAddr, message: &mut ClientMessage) -> Result<()> {
        let pool_ref = self
            .pools
            .get(&address)
            .ok_or_else(|| HazelcastError::Connection(format!("no connection to {}", address)))?;

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

    /// Invokes a request on the FIRST (pinned) connection for `address` rather
    /// than round-robining the pool. Required for transactions, which the server
    /// associates with a specific client endpoint (connection).
    pub async fn invoke_pinned(
        &self,
        address: SocketAddr,
        mut message: ClientMessage,
    ) -> Result<ClientMessage> {
        let corr_id = message.correlation_id().unwrap_or(0);
        let (tx, rx) = oneshot::channel();
        self.pending_ops.insert(corr_id, tx);
        if let Err(e) = self.send_raw_pinned(address, &mut message).await {
            self.pending_ops.remove(&corr_id);
            return Err(e);
        }
        match tokio::time::timeout(self.timeout, rx).await {
            Ok(Ok(response)) => check_response(response),
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

    async fn send_raw_pinned(
        &self,
        address: SocketAddr,
        message: &mut ClientMessage,
    ) -> Result<()> {
        let pool_ref = self
            .pools
            .get(&address)
            .ok_or_else(|| HazelcastError::Connection(format!("no connection to {}", address)))?;
        let writer = pool_ref.first().clone();
        drop(pool_ref);
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

        let mut proxy_msg = ClientMessage::create_for_encode(CLIENT_CREATE_PROXY, PARTITION_ID_ANY);
        proxy_msg.add_frame(Frame::with_content(BytesMut::from(map_name.as_bytes())));
        let mut svc_frame = Frame::with_content(BytesMut::from(service_name.as_bytes()));
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

/// If the response is a server error (message type 0), decode it into a
/// `HazelcastError`; otherwise pass the response through unchanged.
pub(crate) fn check_response(response: ClientMessage) -> Result<ClientMessage> {
    if response.message_type() == Some(0) {
        return Err(decode_error_response(&response));
    }
    Ok(response)
}

/// Decodes a Hazelcast error response (a list of `ErrorHolder`) into the first
/// error's code, Java class name, and message.
fn decode_error_response(response: &ClientMessage) -> HazelcastError {
    let mut code: i32 = -1;
    let mut strings: Vec<String> = Vec::new();
    for frame in response.frames().iter().skip(1) {
        if frame.flags & BEGIN_DATA_STRUCTURE_FLAG != 0 {
            if code < 0 && frame.content.len() >= 4 {
                code = i32::from_le_bytes([
                    frame.content[0],
                    frame.content[1],
                    frame.content[2],
                    frame.content[3],
                ]);
            }
            continue;
        }
        if frame.flags & (END_DATA_STRUCTURE_FLAG | IS_NULL_FLAG) != 0 || frame.content.is_empty() {
            continue;
        }
        if let Ok(text) = std::str::from_utf8(&frame.content) {
            if text.chars().all(|c| c == ' ' || !c.is_control()) {
                strings.push(text.to_string());
                if strings.len() >= 2 {
                    break;
                }
            }
        }
    }
    let class_name = strings.first().cloned();
    let message = strings
        .get(1)
        .cloned()
        .or_else(|| strings.first().cloned())
        .unwrap_or_else(|| "server exception".to_string());
    HazelcastError::from_server(code, message, class_name)
}
