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
                        let src = i64::from_le_bytes(
                            f.content[off..off + 8].try_into().unwrap(),
                        );
                        Self::deliver_backup_ack(&pending, src);
                    }
                }
            });
        let request =
            ClientMessage::create_for_encode(CLIENT_LOCAL_BACKUP_LISTENER, PARTITION_ID_ANY);
        self.invoke_listener(address, request, handler).await.map(|_| ())
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
