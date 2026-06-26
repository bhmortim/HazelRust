# HazelRust Client — Performance & Efficiency Audit

_Method: a 12-path multi-agent audit (each finding adversarially verified against the
source) plus first-hand ground-truthing of the hottest paths by the reviewer. Where
the two disagreed, the source code was re-read and the discrepancy is called out
below (one false-negative was corrected). Auditing the current client source at
`cbdc/full-validation` (post backup-ack-to-client)._

## Verdict

**The client is already idiomatic and efficient on its hot paths.** Across IMap
read/write, serialization, inbound/outbound IO, invocation/correlation, partition
routing, CP subsystem, collections, the allocator, the async runtime, and the
connection pool: framing is `bytes`-based, the correlation table is a sharded
`DashMap`, inbound value bytes are **not** copied, there are no blocking calls on
async tasks, no large-payload clones, and no O(n²) traps. The findings below are
**refinements, not rescues** — one structural write-path opportunity plus a tail of
small, real, mechanical wins. None is a correctness or durability risk.

The three things actually worth doing, in order of value:

1. **Engage the connection pool (one constant).** `DEFAULT_CONNECTION_POOL_SIZE = 1`
   makes the existing round-robin inert, so every concurrent op to a member funnels
   through one writer `Mutex` + one socket. Best impact-per-effort in the audit.
2. **Kill the per-write triple-copy of the payload** (universal — IMap *and* CP, see
   the correction below). For large values this is real memory-bandwidth waste.
3. **(Bigger) A per-connection writer task with coalesced/vectored writes** — the one
   architecturally weighty change; amortizes syscalls and TLS records under load.

Everything else is low-risk plumbing that can land in small PRs.

---

## Correction to the automated synthesis (ground-truthed)

The automated report claimed the per-write payload double/triple-copy was
"CP-specific" and that "the IMap path threads ownership better." **That is incorrect.**
Re-reading the source: `IMap::serialize_value` (`proxy/map.rs:2841`) calls
`ObjectDataOutput::into_bytes()` which is `self.buffer.to_vec()` (`serialization/
data_output.rs:65` — a heap alloc + memcpy of bytes it already owns in a `BytesMut`),
then `IMap::data_frame` (`proxy/map.rs:2881`) does `BytesMut::from(data)` — **a second
copy** — and finally `send_raw` (`connection/invocation.rs:519`) copies every frame
again via `Frame::write_to`'s `put_slice`. The CP and collection paths use the
identical shared helpers. **The copy chain is universal, not CP-specific** — it is the
single highest-value memory optimization and is listed as finding **C1** below.
(The likely cause of the false negative: the verifier agents for the IMap
`serialize_value`/`data_frame`/`into_bytes` findings were lost to API rate-limiting, so
the synthesis under-counted them.)

---

## Prioritized findings

### Write path

**W1 — Default `pool_size = 1` neutralizes the round-robin pool.**
`config.rs:414` + `connection/invocation.rs` (`ConnectionPool::select`).
Impact **medium** · Effort **low** · Risk medium.
With `DEFAULT_CONNECTION_POOL_SIZE = 1`, `select()` always returns connection 0, so all
concurrent invocations to a member serialize through one `Mutex` + one socket; the
round-robin built to spread that load never fires. **Fix:** default to 2–4 (or
auto-size). Engages machinery already written. *Mitigation, not cure — within one slot
the path is still serial (W3).* Measure the startup/auth cost trade-off.

**C1 — Per-write payload is copied ~3× (UNIVERSAL: IMap, CP, collections).**
`serialization/data_output.rs:65` (`into_bytes` → `to_vec`), `proxy/map.rs:2881`
(`data_frame` → `BytesMut::from`), `connection/invocation.rs:519` (`send_raw` buffer +
`Frame::write_to` `put_slice`).
Impact **low–medium** (scales with value size) · Effort **medium** · Risk low.
For a `put`, key and value each: serialize into a `BytesMut`, `to_vec()` into a `Vec`
(copy 1), `BytesMut::from` into the frame (copy 2), `put_slice` into the send buffer
(copy 3). A 16 KB value is memcpy'd three times per write. **Fix (one ownership pass):**
`into_bytes(self) -> BytesMut { self.buffer }` (move, no copy); let `Frame::with_content`
accept `Bytes`; have `serialize_value` return `BytesMut`/`Bytes` so it moves into the
frame; serialize directly into a reused send buffer. Zero intermediate copies. Verify
wire bytes are byte-identical.

**W3 — Per-op `BytesMut::with_capacity(256)`; `wire_size()` ignored.**
`connection/invocation.rs:519` (and `send_raw_pinned`).
Impact **low** · Effort **low** · Risk low.
Every op mallocs a fresh 256 B buffer; for payloads > ~250 B (most real puts)
`write_to`'s `reserve` reallocs + recopies mid-serialization. `message.wire_size()`
(`client_message.rs:208`) is already implemented but unused. **Fix:**
`BytesMut::with_capacity(message.wire_size())`, and reuse a scratch buffer held behind
the writer `Mutex` you already own (`clear()` retains capacity). Folds into C1/W2.

**W2 — No coalescing / vectored write; writer `Mutex` held across `write_all().await`.** *(structural)*
`connection/invocation.rs:519-525` (and `send_raw_pinned`).
Impact **medium** · Effort **high** · Risk moderate.
The lock is held across the whole flush, so a backpressured peer blocks every other op
on that connection; and N simultaneously-ready ops each take the lock and issue their
own `write()` syscall (and TLS record over rustls) — never gathered into one `writev`.
**Fix:** a dedicated per-connection **writer task** fed by an `mpsc<Bytes>` channel
(`send_raw` pushes and returns — no lock across `.await`); the task `recv().await`s one
item, `try_recv()`-drains everything already queued, then does **one vectored write**
(`write_all_buf` over a chained `Buf`, or `poll_write_vectored` over a
`SmallVec<[IoSlice;N]>`). Must preserve ordering, RR-21 teardown semantics, per-message
error propagation, and bounded backpressure. **Drain only what's queued — never wait to
fill a batch** (that trades latency for throughput). Subsumes W3's reuse and the
lock-across-await issue.

### Read path

**R1 — Frame length parsed + bounds-checked twice per frame.**
`protocol/codec.rs:58` then `protocol/frame.rs:~200` (`Frame::read_from` re-reads the
same 4 bytes + re-checks). Impact **low** · Effort **low** · Risk very low.
**Fix:** pass the already-computed `frame_length` into a `read_from_len` helper (or
inline the `advance` + `split_to`); mark it `#[inline]`. Keep the malformed-frame guard.

**R2 — Collection-decode result `Vec` not pre-sized.**
`proxy/multimap.rs:669` (same in `queue.rs`, `set.rs`, `replicated_map.rs`).
Impact **low** · Effort **low** · Risk very low · Frequency per-batch.
`Vec::new()` + push forces the geometric realloc series; the frame count is a known
upper bound. **Fix:** `Vec::with_capacity(frames.len()-1)` (entries: `/2`).

### Process / shared paths

**P1 — Drop the dead `Vec<SocketAddr>` precheck in `CPMap::invoke`.**
`proxy/cp_map.rs:345`. Impact low · Effort low · Risk very low.
`let _address = self.get_connection_address().await?;` takes the connections `RwLock`
and `collect()`s **all** addresses into a `Vec`, then discards them — `invoke_on_random`
selects anyway. **Fix:** delete it; call `invoke_on_random` directly (as `AtomicLong`
does). Pure per-op `RwLock` + alloc removed.

**P2 — Pass `corr_id` down instead of re-parsing in `send_raw`.**
`connection/invocation.rs:502`. `invoke` computes it, `send_raw` re-reads the same 8
bytes. **Fix:** pass it as a param; `#[inline]` `correlation_id()`.

**P3 — Build `PendingOp` complete; drop the `pending_ops.get_mut` back-patch.**
`connection/invocation.rs:511`. `send_raw` does a third DashMap shard-lock + `Arc`
clone only to write the `inflight` field onto the entry `invoke` just inserted. **Fix:**
resolve the connection's `inflight` in `invoke` before `pending_ops.insert`, pass it to
`PendingOp::new`. (Deeper slab/array correlation table is a separate, higher-risk item.)

**P4 — Cache the collection/map name partition routing.**
`proxy/queue.rs:491` (+`list`/`set`/`replicated_map`). The name never changes yet is
fully re-serialized into a fresh `Vec` every op to compute a partition id. **Fix:**
cache the serialized name bytes / hash at construction; recompute only the cheap
`partition_id_for_hash(hash, partition_count())` per op (count can change at runtime, so
cache the hash, not the final id).

**P5 — Borrow `RaftGroupId` instead of cloning its `String` per CP op.**
`proxy/atomic_long.rs:149` (CPMap worse: `cp_map.rs:93`). `build_request` `.clone()`s
the group (heap-allocs `name: String`) though `encode_group_id` only reads it. **Fix:**
keep the borrow.

**P6 — Borrowing permission predicate (no `Permissions` clone).**
`effective_permissions()` returns an **owned `Permissions` by value on every op**
(`manager.rs:1227`, used by `IMap::check_permission` at `map.rs:643` and the CP proxies).
**Fix:** a `is_permitted(&self, action) -> bool` that borrows, default-allow when unset.

**P7 — Pre-size / reuse the per-op `ObjectDataOutput`.**
`proxy/map.rs:2841` and the collection `serialize_value`s. `ObjectDataOutput::new()`
allocates a `BytesMut(256)` with no payload-size hint. **Fix:** `with_capacity` for the
common small case, or a `thread_local!` reusable buffer (`split`/`freeze` then `clear`).
Folds into C1.

**P8 — Uncontended semaphore fast-path.** `manager.rs:1680`/`1727`. When
`max_concurrent_invocations > 0`, every invoke builds an `acquire().await` future even
when a permit is free. **Fix:** `try_acquire()` first, `.acquire().await` only when
contended; `&'static str` error. (Only trims uncontended overhead — the limiter is
opt-in and disabled by default, so this is genuinely zero-cost unless configured.)

---

## Top actions (impact-per-effort)

| # | Action | Where | Impact | Effort |
|---|---|---|--:|--:|
| 1 | Raise/auto-size default `pool_size` (engage round-robin) | `config.rs:414` | medium | low |
| 2 | `into_bytes` move + `Frame`/`serialize_value` own `BytesMut`/`Bytes` (kill triple-copy, **universal**) | `data_output.rs:65`, `map.rs:2881` | low–med | med |
| 3 | Delete dead `Vec<SocketAddr>` precheck in `CPMap::invoke` | `cp_map.rs:345` | low | low |
| 4 | Pre-size send buffer from `wire_size()` | `invocation.rs:519` | low | low |
| 5 | Pass `corr_id` down; build `PendingOp` complete | `invocation.rs:502,511` | low | low |
| 6 | Pre-size collection-decode `Vec` | `multimap.rs:669` (+3) | low | low |
| 7 | Borrow `RaftGroupId`; borrowing permission predicate | `atomic_long.rs:149`, `map.rs:643` | low | low |
| 8 | Cache collection name partition routing | `queue.rs:491` (+3) | low | low |
| 9 | Per-connection writer task + coalesced/vectored write *(structural)* | `invocation.rs:519` | medium | high |

## Already efficient / no change needed (honest negatives)

- **Inbound decode** borrows the payload — no per-element value copies (R1 is a double
  *length-parse*, not a data copy).
- **Partition routing math** is cheap arithmetic; IMap serializes the key **once** (the
  only waste is re-serializing the *name*, P4).
- **`check_quorum`** has a fast-path (Opt 3) that returns immediately when no quorum is
  configured — already optimal for the common case.
- **Async runtime:** no blocking on async tasks; the `Option<Semaphore>` is genuinely
  zero-cost when disabled (the default); the only lock-across-await is the writer Mutex
  (W2).
- **Correlation table:** sharded `DashMap`, no global hot-path lock; no large-payload
  clones outside the named cases.

## What to prototype + measure first (on the live EE 5.7 cluster)

1. **`pool_size` sweep (W1)** — one constant; A/B 1→2→4 under high-C put/get and the
   mixed workload. This directly targets the per-member write-Mutex serialization that
   showed up as the mixed-workload tail in the benchmark. Measure throughput **and** the
   startup/auth cost.
2. **The triple-copy removal (C1)** — measure with **large values (16 KB / 64 KB)** where
   the three memcpys dominate; expect CPU-per-op and large-value write throughput to
   improve. Low-risk; gate only on confirming wire bytes are byte-identical.
3. **Only if W1 shows the write path is the ceiling: the writer-task refactor (W2)** —
   its payoff (amortized syscalls + TLS records) only appears under genuine concurrent
   load over rustls, so it must be A/B'd live, not micro-benchmarked.

The rest of the Top-10 are low-risk enough to land directly; batch them into one
before/after benchmark run to confirm the aggregate per-op tax dropped.
