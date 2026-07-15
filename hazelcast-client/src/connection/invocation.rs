//! Invocation service for concurrent Hazelcast operations.
//!
//! Manages pending operations with correlation-based response routing.
//! Each connection has a background reader task that routes incoming
//! messages to the correct waiter by correlation_id.
//!
//! Supports connection pooling: multiple data connections per member
//! to reduce write-Mutex contention under high concurrency.

use std::collections::HashSet;
use std::io::IoSlice;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use dashmap::DashMap;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot, RwLock};

use hazelcast_core::protocol::constants::*;
use hazelcast_core::protocol::{ClientMessage, ClientMessageCodec, Frame};
use hazelcast_core::{HazelcastError, Result};
use tokio_util::codec::Decoder;

/// Boxed write half of a pooled connection. Boxed (rather than a concrete
/// `OwnedWriteHalf`) so the same pool can hold both plaintext TCP halves and
/// `tokio_rustls` TLS halves — this is what lets data operations run over mTLS.
type BoxedWrite = Box<dyn AsyncWrite + Send + Unpin>;
/// Boxed read half of a pooled connection (see [`BoxedWrite`]).
type BoxedRead = Box<dyn AsyncRead + Send + Unpin>;
/// Correlation ids dispatched on ONE connection and still awaiting a response.
/// Tracked per-connection so a reader can fail exactly its own in-flight ops on
/// teardown (RR-21) — `pending_ops` is global across all members, so failing all
/// of it on a single connection's break would wrongly abort other members' ops.
type InFlight = Arc<DashMap<i64, ()>>;

/// `Client.ping` request message type — the same wire value the metadata
/// connections' heartbeat uses (`Connection::send_heartbeat`).
const CLIENT_PING_MESSAGE_TYPE: i32 = 0x000200;

/// Upper bound on waiting for a keepalive pong. Generous for a healthy member
/// (a ping round-trip is sub-millisecond on a LAN) yet far shorter than the
/// heartbeat interval, so a stuck member cannot pile up waiter tasks across
/// ticks or starve pings to healthy members.
const KEEPALIVE_PING_TIMEOUT: Duration = Duration::from_secs(2);

/// A message ready for the writer task: the (small) per-frame headers plus the
/// owned frames. The writer references each frame's content in place via
/// `IoSlice` for a zero-copy vectored write (no per-value `put_slice` copy);
/// over a transport without vectored support (TLS) it concatenates instead.
struct Outbound {
    headers: BytesMut,
    frames: Vec<Frame>,
}

/// A single pooled connection: a sender to its dedicated writer task plus the
/// set of correlation ids it has dispatched and is still awaiting responses for.
struct PooledConn {
    /// Requests are pushed here; one writer task per connection drains the
    /// channel and coalesces all immediately-available messages into a single
    /// vectored socket write (batched, zero-copy IO — like the Java client's IO
    /// thread), instead of each op locking a shared writer and issuing its own
    /// write syscall. This lets a single connection (e.g. a single-partition
    /// IQueue/ISet) pipeline efficiently under high concurrency, and avoids
    /// copying large values into a send buffer.
    outbound: mpsc::UnboundedSender<Outbound>,
    inflight: InFlight,
}

/// State for one in-flight invocation. Normally completed by the owner's
/// response (the `oneshot` fires). For backup-aware writes the owner replies
/// early with `acks_expected = backupAcks`, and backup members ack the client
/// directly ([`ClientMessage::is_backup_event`]); the op completes only once
/// the response AND all expected acks have arrived (in either order). `inflight`
/// is the OWNER connection's in-flight set — backup acks arrive on a *different*
/// connection, so whichever reader completes the op must clean the owner's set
/// (else it leaks). `response` retained so a lost-ack timeout can still return
/// the (already-applied) owner response instead of a false error.
struct PendingOp {
    tx: Option<oneshot::Sender<Result<ClientMessage>>>,
    response: Option<ClientMessage>,
    acks_received: u32,
    acks_expected: Option<u32>,
    inflight: Option<InFlight>,
}

impl PendingOp {
    fn new(tx: oneshot::Sender<Result<ClientMessage>>) -> Self {
        Self {
            tx: Some(tx),
            response: None,
            acks_received: 0,
            acks_expected: None,
            inflight: None,
        }
    }
}

/// A pool of write connections to a single cluster member.
struct ConnectionPool {
    /// Pooled connections (each a write half + its in-flight correlation set).
    conns: Vec<PooledConn>,
    /// Round-robin counter for selecting the next connection.
    next: AtomicUsize,
}

impl ConnectionPool {
    /// Create a pool with a single connection.
    fn new(conn: PooledConn) -> Self {
        Self {
            conns: vec![conn],
            next: AtomicUsize::new(0),
        }
    }

    /// Add a connection to the pool.
    fn add(&mut self, conn: PooledConn) {
        self.conns.push(conn);
    }

    /// Select the next connection via round-robin.
    fn select(&self) -> &PooledConn {
        let idx = self.next.fetch_add(1, Ordering::Relaxed) % self.conns.len();
        &self.conns[idx]
    }

    /// Returns the first (stable) connection in the pool. Used to pin a sequence
    /// of requests (e.g. a transaction) to a single server connection/endpoint.
    fn first(&self) -> &PooledConn {
        &self.conns[0]
    }

    /// Number of connections in the pool.
    fn size(&self) -> usize {
        self.conns.len()
    }
}

impl std::fmt::Debug for ConnectionPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionPool")
            .field("size", &self.conns.len())
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
    /// Pending operation waiters keyed by correlation_id. The value is a
    /// `Result` so a reader can deliver a typed failure (e.g. the connection was
    /// torn down with the op in flight) instead of dropping the sender silently.
    pending_ops: Arc<DashMap<i64, PendingOp>>,
    /// Long-lived event handlers keyed by the listener registration correlation_id.
    event_handlers: Arc<DashMap<i64, Arc<dyn Fn(ClientMessage) + Send + Sync>>>,
    /// Set of map names for which CreateProxy has been sent.
    created_proxies: RwLock<HashSet<String>>,
    /// Invocation timeout.
    timeout: Duration,
    /// Whether backup-ack-to-client is enabled. When false, the reader completes
    /// every op on its response (the historical behavior) and never waits for
    /// backup acks — so the feature-off path is byte-for-byte unchanged.
    backup_ack_enabled: bool,
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
    pub fn new(timeout: Duration, backup_ack_enabled: bool) -> Self {
        Self {
            pools: DashMap::new(),
            pending_ops: Arc::new(DashMap::new()),
            event_handlers: Arc::new(DashMap::new()),
            created_proxies: RwLock::new(HashSet::new()),
            timeout,
            backup_ack_enabled,
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
        read_half: BoxedRead,
        write_half: BoxedWrite,
    ) {
        let inflight: InFlight = Arc::new(DashMap::new());
        // Channel feeding this connection's dedicated writer task. Unbounded is
        // safe: outstanding depth is bounded by the client's concurrency (each
        // in-flight op has at most one queued message).
        let (outbound, write_rx) = mpsc::unbounded_channel::<Outbound>();

        // Add connection to pool (create pool if first connection for this address)
        match self.pools.entry(address) {
            dashmap::mapref::entry::Entry::Occupied(mut entry) => {
                let pool = entry.get_mut();
                pool.add(PooledConn {
                    outbound: outbound.clone(),
                    inflight: Arc::clone(&inflight),
                });
                tracing::info!(
                    address = %address,
                    pool_size = pool.size(),
                    "added connection to pool"
                );
            }
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                entry.insert(ConnectionPool::new(PooledConn {
                    outbound: outbound.clone(),
                    inflight: Arc::clone(&inflight),
                }));
            }
        }

        // Spawn the dedicated writer task (owns the write half + the receiver).
        // It exits when the pool entry — and thus the last `outbound` sender — is
        // dropped (reconnect/shutdown), or when the socket write fails.
        tokio::spawn(async move {
            Self::writer_loop(address, write_half, write_rx).await;
        });

        // Spawn background reader
        let pending = Arc::clone(&self.pending_ops);
        let events = Arc::clone(&self.event_handlers);
        let backup_ack = self.backup_ack_enabled;
        let handle = tokio::spawn(async move {
            Self::reader_loop(address, read_half, pending, events, inflight, backup_ack).await;
        });
        self._readers.write().await.push(handle);
    }

    /// Removes the entire connection pool for `address`.
    ///
    /// Used before a reconnect rebuilds the pool: it drops the (now-dead) write
    /// halves so `send_raw`'s round-robin can no longer select a stale, broken
    /// connection. The reader tasks for those connections exit on their own when
    /// their read halves break and fail any still-in-flight ops (RR-21), so no
    /// caller is left blocked. A no-op if no pool exists for the address (e.g. on
    /// the initial connect). See cbdc lead #3.
    pub fn remove_address(&self, address: SocketAddr) {
        if self.pools.remove(&address).is_some() {
            tracing::info!(
                address = %address,
                "evicted stale connection pool (reconnect or teardown)"
            );
        }
    }

    /// Sends a `Client.ping` on EVERY pooled invocation connection.
    ///
    /// The heartbeat task pings only the per-member metadata connections; the
    /// pooled data connections registered here carry no traffic while the
    /// client idles, so a member reaps them once its client-heartbeat timeout
    /// elapses — and every subsequent data op to that member fails with a
    /// broken pipe until the pool is rebuilt. Calling this from each heartbeat
    /// tick refreshes the members' idle timers on the exact sockets the data
    /// ops use.
    ///
    /// Fire-and-forget: each ping runs on its own task with a short timeout
    /// ([`KEEPALIVE_PING_TIMEOUT`]), so a stuck member can neither block the
    /// caller nor starve pings to healthy members. Failures are logged and
    /// otherwise ignored — the metadata heartbeat and the reader teardown path
    /// (RR-21) own connection-failure recovery.
    pub fn keepalive_all(&self) {
        for entry in self.pools.iter() {
            let address = *entry.key();
            for conn in entry.value().conns.iter() {
                let outbound = conn.outbound.clone();
                let inflight = Arc::clone(&conn.inflight);
                let pending_ops = Arc::clone(&self.pending_ops);
                tokio::spawn(async move {
                    Self::keepalive_ping(address, outbound, inflight, pending_ops).await;
                });
            }
        }
    }

    /// One keepalive ping on one pooled connection: registers a waiter, queues
    /// the ping on the connection's writer task, and awaits the pong with
    /// [`KEEPALIVE_PING_TIMEOUT`]. Cleans up its waiter on every exit path so
    /// an unanswered ping cannot leak a `pending_ops` entry.
    async fn keepalive_ping(
        address: SocketAddr,
        outbound: mpsc::UnboundedSender<Outbound>,
        inflight: InFlight,
        pending_ops: Arc<DashMap<i64, PendingOp>>,
    ) {
        let message = ClientMessage::create_for_encode(CLIENT_PING_MESSAGE_TYPE, PARTITION_ID_ANY);
        let corr_id = message.correlation_id().unwrap_or(0);

        // Same registration order as `send_raw`: waiter first, then the
        // in-flight mark, so the reader can complete OR fail this ping and
        // always finds a consistent pair.
        let (tx, rx) = oneshot::channel();
        pending_ops.insert(corr_id, PendingOp::new(tx));
        inflight.insert(corr_id, ());
        if let Some(mut op) = pending_ops.get_mut(&corr_id) {
            op.inflight = Some(Arc::clone(&inflight));
        }

        let (headers, frames) = message.into_segments();
        if outbound.send(Outbound { headers, frames }).is_err() {
            // Writer task gone: the pool was evicted mid-tick. Reconnect owns
            // recovery; just drop the waiter.
            pending_ops.remove(&corr_id);
            inflight.remove(&corr_id);
            tracing::debug!(address = %address, "keepalive ping skipped: connection closed");
            return;
        }

        match tokio::time::timeout(KEEPALIVE_PING_TIMEOUT, rx).await {
            Ok(Ok(Ok(_pong))) => {
                tracing::trace!(address = %address, "keepalive pong received");
            }
            Ok(Ok(Err(e))) => {
                // The reader failed the op (connection teardown, RR-21) and
                // already cleaned up both maps.
                tracing::debug!(address = %address, error = %e, "keepalive ping failed");
            }
            Ok(Err(_)) => {
                // Waiter dropped without a result — the pending op was removed
                // externally (e.g. shutdown). Nothing left to clean.
                tracing::debug!(address = %address, "keepalive waiter dropped");
            }
            Err(_) => {
                // No pong within the deadline. Clean up so unanswered pings
                // don't accumulate in pending_ops tick after tick.
                pending_ops.remove(&corr_id);
                inflight.remove(&corr_id);
                tracing::warn!(
                    address = %address,
                    timeout = ?KEEPALIVE_PING_TIMEOUT,
                    "keepalive ping timed out"
                );
            }
        }
    }

    /// Per-connection writer task: drains the outbound channel and coalesces all
    /// immediately-available request buffers into a single `write_all`, matching
    /// the Java client's batched IO thread. Under low concurrency each buffer is
    /// written on its own (no added latency); under high concurrency on a single
    /// hot connection (e.g. a single-partition IQueue/ISet/Topic) many requests
    /// collapse into one syscall, which is where the previous lock-and-write-per-op
    /// path serialized. Exits when all senders drop (pool removed) or a write fails;
    /// the reader loop fails this connection's in-flight ops on teardown.
    async fn writer_loop(
        address: SocketAddr,
        mut write: BoxedWrite,
        mut rx: mpsc::UnboundedReceiver<Outbound>,
    ) {
        // Cap messages coalesced per write so a burst can't build an unbounded
        // IoSlice list (kept well under IOV_MAX) or concat buffer.
        const MAX_BATCH_MSGS: usize = 128;
        // Vectored writes (writev) reference frame contents in place, avoiding a
        // copy of large values into a send buffer. Only the plaintext TCP half
        // (OwnedWriteHalf) reports vectored support; the TLS half does not, so we
        // fall back to concatenation there.
        let vectored = write.is_write_vectored();
        let mut concat = BytesMut::new();
        while let Some(first) = rx.recv().await {
            let mut pending: Vec<Outbound> = Vec::with_capacity(8);
            pending.push(first);
            while pending.len() < MAX_BATCH_MSGS {
                match rx.try_recv() {
                    Ok(o) => pending.push(o),
                    Err(_) => break,
                }
            }
            let res = if vectored {
                Self::write_pending_vectored(&mut write, &pending).await
            } else {
                concat.clear();
                for o in &pending {
                    for (i, f) in o.frames.iter().enumerate() {
                        let h = i * FRAME_HEADER_SIZE;
                        concat.extend_from_slice(&o.headers[h..h + FRAME_HEADER_SIZE]);
                        concat.extend_from_slice(&f.content);
                    }
                }
                write.write_all(&concat).await
            };
            if let Err(e) = res {
                tracing::warn!(address = %address, error = %e, "writer I/O error");
                break;
            }
        }
    }

    /// Gather-write every pending message (per-frame header + content) with a
    /// single `writev`, referencing the frame bytes in place — no per-value
    /// copy. Loops on partial writes, advancing past the bytes the kernel took.
    async fn write_pending_vectored(
        write: &mut BoxedWrite,
        pending: &[Outbound],
    ) -> std::io::Result<()> {
        let cap: usize = pending.iter().map(|o| o.frames.len() * 2).sum();
        let mut slices: Vec<IoSlice> = Vec::with_capacity(cap);
        for o in pending {
            for (i, f) in o.frames.iter().enumerate() {
                let h = i * FRAME_HEADER_SIZE;
                slices.push(IoSlice::new(&o.headers[h..h + FRAME_HEADER_SIZE]));
                slices.push(IoSlice::new(&f.content));
            }
        }
        let mut remaining: &mut [IoSlice] = &mut slices;
        while !remaining.is_empty() {
            let n = write.write_vectored(remaining).await?;
            if n == 0 {
                return Err(std::io::ErrorKind::WriteZero.into());
            }
            IoSlice::advance_slices(&mut remaining, n);
        }
        Ok(())
    }

    /// Background reader loop that decodes messages and routes to pending ops.
    ///
    /// On teardown (decode error / clean close / I/O error) it fails this
    /// connection's still-in-flight invocations with a typed error (RR-21), so
    /// callers fail fast instead of blocking until the invocation timeout.
    async fn reader_loop(
        address: SocketAddr,
        mut read: BoxedRead,
        pending_ops: Arc<DashMap<i64, PendingOp>>,
        event_handlers: Arc<DashMap<i64, Arc<dyn Fn(ClientMessage) + Send + Sync>>>,
        inflight: InFlight,
        _backup_ack_enabled: bool,
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
                        // Backup-ack EVENT_BACKUP messages are delivered here too —
                        // they arrive on the registered ClientLocalBackupListener
                        // correlation id and the handler routes them to the source
                        // op by the correlation id in their payload.
                        if let Some(corr_id) = msg.correlation_id() {
                            if let Some(h) = event_handlers.get(&corr_id) {
                                let handler = Arc::clone(h.value());
                                drop(h);
                                handler(msg);
                            }
                        }
                    } else if let Some(corr_id) = msg.correlation_id() {
                        // Operation response — complete the invocation on the owner
                        // response. With backup-ack-to-client, BACKUP_AWARE makes the
                        // owner reply EARLY (overlapping its sync backups) and the
                        // client completes on that reply without blocking on the
                        // backup acks (matching the Java client, whose
                        // ClientInvocation.shouldCompleteWithoutBackups() is always
                        // true). Backup acks still arrive on the registered listener
                        // and are counted for bookkeeping, but they do not gate
                        // completion. NOTE: this is the documented latency/durability
                        // trade-off — under owner failure between the early reply and
                        // the backup applying, the op may be lost (weaker RPO). Set
                        // backup_ack_to_client(false) for strong (wait-for-backup)
                        // durability.
                        // Single removal: take the PendingOp out in ONE hash + shard-lock
                        // (was get_mut + remove = two lookups per response). The owner
                        // response completes the op (shouldCompleteWithoutBackups), so
                        // removing here is correct; a late backup ack for this id then
                        // finds nothing in pending_ops and harmlessly no-ops.
                        if let Some((_, mut op)) = pending_ops.remove(&corr_id) {
                            if let Some(tx) = op.tx.take() {
                                if let Some(i) = op.inflight {
                                    i.remove(&corr_id);
                                } else {
                                    inflight.remove(&corr_id);
                                }
                                let _ = tx.send(Ok(msg));
                            }
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

        // The connection is gone. Fail THIS connection's still-in-flight ops with
        // a typed error so their callers return immediately instead of waiting out
        // the invocation timeout. Only this connection's correlation ids are
        // touched — other members' in-flight ops (also in the global pending_ops)
        // are left intact.
        let stranded: Vec<i64> = inflight.iter().map(|e| *e.key()).collect();
        if !stranded.is_empty() {
            tracing::warn!(
                address = %address,
                in_flight = stranded.len(),
                "failing in-flight invocations after connection teardown"
            );
        }
        for corr_id in stranded {
            inflight.remove(&corr_id);
            if let Some((_, mut op)) = pending_ops.remove(&corr_id) {
                if let Some(tx) = op.tx.take() {
                    let _ = tx.send(Err(HazelcastError::Connection(format!(
                        "connection to {} closed with operation in flight",
                        address
                    ))));
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

    /// Registers a `ClientLocalBackupListener` on `address` so that member sends
    /// backup-ack (`EVENT_BACKUP`) events for the backups it holds. Each event
    /// carries the source op's correlation id in its payload; the handler routes
    /// the ack to that pending op. Called once per member connection when
    /// backup-ack-to-client is enabled. Best-effort — if it fails, writes whose
    /// backup lands on this member simply fall back to the (slower) timeout
    /// completion path rather than overlapping the backup.
    pub async fn register_backup_listener(&self, address: SocketAddr) -> Result<()> {
        let pending = Arc::clone(&self.pending_ops);
        let handler: Arc<dyn Fn(ClientMessage) + Send + Sync> =
            Arc::new(move |msg: ClientMessage| {
                if let Some(f) = msg.initial_frame() {
                    let off = EVENT_BACKUP_SOURCE_CORRELATION_OFFSET;
                    if f.content.len() >= off + 8 {
                        let src = i64::from_le_bytes(f.content[off..off + 8].try_into().unwrap());
                        Self::deliver_backup_ack(&pending, src);
                    }
                }
            });
        let request =
            ClientMessage::create_for_encode(CLIENT_LOCAL_BACKUP_LISTENER, PARTITION_ID_ANY);
        self.invoke_listener(address, request, handler)
            .await
            .map(|_| ())
    }

    /// Records one backup ack for `src_corr`; completes the op iff the owner
    /// response is already in and all expected acks have now arrived (acks may
    /// arrive before or after the response).
    fn deliver_backup_ack(pending_ops: &DashMap<i64, PendingOp>, src_corr: i64) {
        let mut done = None;
        if let Some(mut e) = pending_ops.get_mut(&src_corr) {
            e.acks_received += 1;
            if let (Some(exp), true) = (e.acks_expected, e.response.is_some()) {
                if e.acks_received >= exp {
                    if let (Some(tx), Some(resp)) = (e.tx.take(), e.response.take()) {
                        done = Some((tx, resp, e.inflight.clone()));
                    }
                }
            }
        }
        if let Some((tx, resp, infl)) = done {
            pending_ops.remove(&src_corr);
            if let Some(i) = infl {
                i.remove(&src_corr);
            }
            let _ = tx.send(Ok(resp));
        }
    }

    /// Send a message on a connection and wait for the correlated response.
    pub async fn invoke(
        &self,
        address: SocketAddr,
        message: ClientMessage,
    ) -> Result<ClientMessage> {
        let corr_id = message.correlation_id().unwrap_or(0);

        // Register pending operation BEFORE sending
        let (tx, rx) = oneshot::channel();
        self.pending_ops.insert(corr_id, PendingOp::new(tx));

        // Send the message (corr_id already parsed — passed down, not re-read)
        if let Err(e) = self.send_raw(address, corr_id, message).await {
            self.pending_ops.remove(&corr_id);
            return Err(e);
        }

        // Wait for response with timeout
        match tokio::time::timeout(self.timeout, rx).await {
            // `result` is the reader's outcome: Ok(msg) for a real response, or
            // Err(..) if the connection was torn down with this op in flight.
            Ok(Ok(result)) => result.and_then(check_response),
            Ok(Err(_)) => {
                self.pending_ops.remove(&corr_id);
                Err(HazelcastError::Connection(
                    "response channel dropped".to_string(),
                ))
            }
            Err(_) => self.on_invoke_timeout(corr_id),
        }
    }

    /// Timeout handling shared by `invoke`/`invoke_pinned`. If the owner response
    /// already arrived but backup acks did not (a lost/late ack), the op DID
    /// apply on the owner — return that response rather than a false timeout
    /// error. Otherwise report the timeout.
    fn on_invoke_timeout(&self, corr_id: i64) -> Result<ClientMessage> {
        if let Some((_, mut op)) = self.pending_ops.remove(&corr_id) {
            if let Some(i) = &op.inflight {
                i.remove(&corr_id);
            }
            if let Some(resp) = op.response.take() {
                return check_response(resp);
            }
        }
        Err(HazelcastError::Timeout(format!(
            "invocation timed out after {:?}",
            self.timeout
        )))
    }

    /// Send a message without waiting for a response.
    ///
    /// Uses round-robin selection across pooled connections to reduce
    /// contention on any single write Mutex. The correlation id is recorded in
    /// the selected connection's in-flight set so its reader can fail it on
    /// teardown.
    async fn send_raw(
        &self,
        address: SocketAddr,
        corr_id: i64,
        message: ClientMessage,
    ) -> Result<()> {
        let (outbound, inflight) = {
            let pool_ref = self.pools.get(&address).ok_or_else(|| {
                HazelcastError::Connection(format!("no connection to {}", address))
            })?;
            let conn = pool_ref.select();
            (conn.outbound.clone(), conn.inflight.clone())
        }; // Release DashMap shard lock before awaiting

        inflight.insert(corr_id, ());
        // Record the OWNER connection's in-flight set on the pending op so the
        // reader that completes it (possibly a backup member's reader) cleans
        // the right set.
        if let Some(mut e) = self.pending_ops.get_mut(&corr_id) {
            e.inflight = Some(inflight.clone());
        }

        // Zero-copy: split into small per-frame headers + the owned frames. The
        // writer task references frame contents in place via IoSlice (no copy of
        // the value) and coalesces queued messages into one writev. Send only
        // fails if the writer task is gone (connection torn down).
        let (headers, frames) = message.into_segments();
        if outbound.send(Outbound { headers, frames }).is_err() {
            inflight.remove(&corr_id);
            return Err(HazelcastError::Connection(format!(
                "connection to {} closed before send",
                address
            )));
        }
        Ok(())
    }

    /// Invokes a request on the FIRST (pinned) connection for `address` rather
    /// than round-robining the pool. Required for transactions, which the server
    /// associates with a specific client endpoint (connection).
    pub async fn invoke_pinned(
        &self,
        address: SocketAddr,
        message: ClientMessage,
    ) -> Result<ClientMessage> {
        let corr_id = message.correlation_id().unwrap_or(0);
        let (tx, rx) = oneshot::channel();
        self.pending_ops.insert(corr_id, PendingOp::new(tx));
        if let Err(e) = self.send_raw_pinned(address, corr_id, message).await {
            self.pending_ops.remove(&corr_id);
            return Err(e);
        }
        match tokio::time::timeout(self.timeout, rx).await {
            Ok(Ok(result)) => result.and_then(check_response),
            Ok(Err(_)) => {
                self.pending_ops.remove(&corr_id);
                Err(HazelcastError::Connection(
                    "response channel dropped".to_string(),
                ))
            }
            Err(_) => self.on_invoke_timeout(corr_id),
        }
    }

    async fn send_raw_pinned(
        &self,
        address: SocketAddr,
        corr_id: i64,
        message: ClientMessage,
    ) -> Result<()> {
        let (outbound, inflight) = {
            let pool_ref = self.pools.get(&address).ok_or_else(|| {
                HazelcastError::Connection(format!("no connection to {}", address))
            })?;
            let conn = pool_ref.first();
            (conn.outbound.clone(), conn.inflight.clone())
        };
        inflight.insert(corr_id, ());
        if let Some(mut e) = self.pending_ops.get_mut(&corr_id) {
            e.inflight = Some(inflight.clone());
        }
        // Pinned ops (transactions) go to the first connection's writer task; the
        // channel preserves FIFO order, so the txn's request sequence is kept.
        let (headers, frames) = message.into_segments();
        if outbound.send(Outbound { headers, frames }).is_err() {
            inflight.remove(&corr_id);
            return Err(HazelcastError::Connection(format!(
                "connection to {} closed before send",
                address
            )));
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// Reads one complete `ClientMessage` off a test server end.
    async fn read_one_message(io: &mut tokio::io::DuplexStream) -> ClientMessage {
        let mut codec = ClientMessageCodec::new();
        let mut buf = BytesMut::new();
        loop {
            if let Some(msg) = codec.decode(&mut buf).expect("wire bytes must decode") {
                return msg;
            }
            if io.read_buf(&mut buf).await.expect("test read failed") == 0 {
                panic!("connection closed before a full message arrived");
            }
        }
    }

    /// keepalive_all must ping EVERY pooled connection — every connection in a
    /// multi-connection pool and every member's pool — because a member reaps
    /// each idle socket individually.
    #[tokio::test]
    async fn keepalive_all_pings_every_pooled_connection() {
        let svc = InvocationService::new(Duration::from_secs(30), true);
        let addr_a: SocketAddr = "127.0.0.1:65001".parse().unwrap();
        let addr_b: SocketAddr = "127.0.0.1:65002".parse().unwrap();

        // Two pooled connections to member A, one to member B.
        let mut server_ends = Vec::new();
        for addr in [addr_a, addr_a, addr_b] {
            let (client_io, server_io) = tokio::io::duplex(4096);
            let (read_half, write_half) = tokio::io::split(client_io);
            svc.register_connection(addr, Box::new(read_half), Box::new(write_half))
                .await;
            server_ends.push(server_io);
        }

        svc.keepalive_all();

        for mut server_io in server_ends {
            let msg =
                tokio::time::timeout(Duration::from_secs(5), read_one_message(&mut server_io))
                    .await
                    .expect("a ping must arrive on every pooled connection");
            assert_eq!(msg.message_type(), Some(CLIENT_PING_MESSAGE_TYPE));
        }
    }

    /// A pong routed back by the reader must complete the keepalive waiter and
    /// remove it from `pending_ops` (the normal, healthy-member path).
    #[tokio::test]
    async fn keepalive_pong_completes_and_cleans_waiter() {
        let svc = InvocationService::new(Duration::from_secs(30), true);
        let addr: SocketAddr = "127.0.0.1:65003".parse().unwrap();
        let (client_io, mut server_io) = tokio::io::duplex(4096);
        let (read_half, write_half) = tokio::io::split(client_io);
        svc.register_connection(addr, Box::new(read_half), Box::new(write_half))
            .await;

        svc.keepalive_all();

        let ping = tokio::time::timeout(Duration::from_secs(5), read_one_message(&mut server_io))
            .await
            .expect("ping must arrive");
        assert_eq!(ping.message_type(), Some(CLIENT_PING_MESSAGE_TYPE));
        let corr_id = ping
            .correlation_id()
            .expect("ping must carry a correlation id");
        assert!(
            svc.pending_ops.contains_key(&corr_id),
            "waiter must be registered until the pong arrives"
        );

        // Answer with a pong carrying the same correlation id (response
        // message type = request type + 1, as the server does).
        let mut pong =
            ClientMessage::create_for_encode(CLIENT_PING_MESSAGE_TYPE + 1, PARTITION_ID_ANY);
        pong.set_correlation_id(corr_id);
        let mut wire = BytesMut::new();
        for frame in pong.frames() {
            frame.write_to(&mut wire);
        }
        server_io.write_all(&wire).await.unwrap();

        // The reader routes the pong to the waiter, which cleans up after itself.
        let mut cleaned = false;
        for _ in 0..250 {
            if !svc.pending_ops.contains_key(&corr_id) {
                cleaned = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(
            cleaned,
            "pong must remove the keepalive waiter from pending_ops"
        );
    }

    /// An unanswered keepalive must clean up its own waiter once the ping
    /// deadline passes — otherwise every tick against a stuck member would
    /// leak a `pending_ops` entry forever.
    #[tokio::test(start_paused = true)]
    async fn unanswered_keepalive_cleans_up_after_timeout() {
        let svc = InvocationService::new(Duration::from_secs(30), true);
        let addr: SocketAddr = "127.0.0.1:65004".parse().unwrap();
        let (client_io, server_io) = tokio::io::duplex(4096);
        let (read_half, write_half) = tokio::io::split(client_io);
        svc.register_connection(addr, Box::new(read_half), Box::new(write_half))
            .await;

        svc.keepalive_all();

        // Let the spawned ping task register its waiter and queue the send.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            svc.pending_ops.len(),
            1,
            "keepalive must register exactly one waiter for one pooled connection"
        );

        // Never answer; jump past the ping deadline (virtual time).
        tokio::time::sleep(KEEPALIVE_PING_TIMEOUT + Duration::from_millis(100)).await;
        assert!(
            svc.pending_ops.is_empty(),
            "timed-out keepalive must remove its own waiter"
        );

        // Keep the server end alive until after the assertions so reader
        // teardown (RR-21) can't race the timeout path we're testing.
        drop(server_io);
    }

    /// RR-21: an invocation that is in flight when its connection is torn down
    /// must fail fast with a typed `Connection` error, not hang until the
    /// invocation timeout. Backed by an in-memory duplex so no cluster is needed.
    #[tokio::test]
    async fn in_flight_op_fails_fast_on_connection_teardown() {
        let svc = InvocationService::new(Duration::from_secs(30), true);
        let addr: SocketAddr = "127.0.0.1:65000".parse().unwrap();

        // `client_io` is the client end; `server_io` is the peer. Splitting the
        // client end gives the read/write halves the pool/reader use.
        let (client_io, server_io) = tokio::io::duplex(4096);
        let (read_half, write_half) = tokio::io::split(client_io);
        svc.register_connection(addr, Box::new(read_half), Box::new(write_half))
            .await;

        // Fire an invocation; it sends the request (buffered by the duplex) and
        // then awaits a response that will never come.
        let svc = Arc::new(svc);
        let svc2 = Arc::clone(&svc);
        let invoke = tokio::spawn(async move {
            let mut msg = ClientMessage::create_for_encode(0x010000, PARTITION_ID_ANY);
            msg.set_correlation_id(4242);
            svc2.invoke(addr, msg).await
        });

        // Let the request go out and the reader settle into its read.
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Tear the connection down: dropping the peer end EOFs the client's read
        // half, breaking the reader loop, which must fail the in-flight op.
        drop(server_io);

        // Must resolve quickly (well under the 30s invocation timeout).
        let result = tokio::time::timeout(Duration::from_secs(5), invoke)
            .await
            .expect("invoke must resolve fast after teardown, not hang")
            .expect("invoke task panicked");

        match result {
            Err(HazelcastError::Connection(_)) => {}
            other => panic!("expected Err(Connection(..)) on teardown, got {:?}", other),
        }
    }
}
