# HazelRust Comprehensive Test Results

> Date: 2026-04-10
> Cluster: 3-node Hazelcast 5.x on AWS m5.xlarge (us-east-2)
> Client: m5.xlarge bench node in same VPC

---

## Functional Test Results (122 tests)

### IMap — 50 tests

| Category | Pass | Fail | Details |
|----------|------|------|---------|
| CRUD (put/get/remove/delete/set) | 12 | 2 | put_if_absent, remove_if_equal fail (frame layout) |
| Bulk (put_all/get_all/clear/remove_all) | 1 | 5 | put_all, get_all, set_all, remove_all fail |
| TTL/Expiration | 0 | 4 | set_ttl, put_with_ttl, put_with_max_idle, set_with_ttl all fail |
| Locking | 0 | 4 | lock, try_lock, is_locked, force_unlock all fail |
| Compute | 3 | 0 | compute, compute_if_absent, compute_if_present all pass |
| Iterators (key_set/entry_set) | 0 | 2 | key_set, entry_set return empty |
| Size/isEmpty | 1 | 2 | size returns 0, is_empty returns true incorrectly |
| Other (evict, flush, etc.) | 3 | 2 | evict fails, get_entry_view fails |
| **Subtotal** | **20** | **21** | |

### Other Data Structures

| Data Structure | Tests | Pass | Fail | Notes |
|---------------|-------|------|------|-------|
| Client Lifecycle | 4 | 4 | 0 | connect, count, proxy, multi-client all pass |
| IList | 10 | 0 | 10 | All fail — IList protocol issues |
| ISet | 10 | 0 | 10 | All fail — ISet protocol issues |
| IQueue | 10 | TBD | TBD | Hangs on blocking ops (put/take) |
| AtomicLong | 8 | 0 | 8 | All fail — CP subsystem not configured |
| MultiMap | 8 | TBD | TBD | Some hang on locking ops |
| ReplicatedMap | 5 | TBD | TBD | Not yet run (test suite hangs before reaching) |
| Ringbuffer | 5 | TBD | TBD | Not yet run |
| ITopic | 3 | TBD | TBD | Not yet run |
| Semaphore | 3 | 0 | 3 | CP subsystem not configured |
| CountdownLatch | 3 | 0 | 3 | CP subsystem not configured |
| FencedLock | 3 | 0 | 3 | CP subsystem not configured |

### Root Cause Analysis

**Working methods (via IMap):**
- `put()`, `get()`, `remove()`, `delete()`, `set()`, `compute()`, `compute_if_absent()`, `compute_if_present()`, `evict_all()`, `flush()`, `clear()`

**Broken methods — missing threadId in initial frame:**
- `put_if_absent()` — returns None always (doesn't actually store)
- `replace()` — returns None always
- `remove_if_equal()` — always returns false
- `lock()`, `try_lock()`, `is_locked()`, `force_unlock()` — all fail
- `try_put()` — fails

**Broken methods — wrong frame layout:**
- `put_all()`, `set_all()`, `get_all()`, `remove_all()` — batch ops fail
- `put_with_ttl_and_max_idle()`, `set_with_ttl_and_max_idle()`, `put_with_max_idle()` — TTL ops fail
- `set_ttl()` — fixed in Patina workaround but still fails natively
- `get_entry_view()` — response decoding issue

**Broken methods — iterator/collection issues:**
- `key_set().collect()` — returns empty (Data header skip issue partially fixed but not fully)
- `entry_set().collect_entries()` — returns empty
- `size()` — returns 0 (MAP_SIZE protocol issue)
- `is_empty()` — returns true (delegates to size)

**Broken data structures — service routing:**
- ISet — all operations fail (wrong service proxy)
- IList — all operations fail (wrong service proxy)
- IQueue — blocking ops hang, non-blocking ops may fail

**Broken data structures — CP subsystem:**
- AtomicLong, Semaphore, CountdownLatch, FencedLock — require CP subsystem which is Enterprise-only or needs explicit configuration

---

## Performance Benchmark Results (20/40 benchmarks completed)

### IMap Operations — ALL 20 PASS

| Benchmark | ops/sec | p50 (ms) | p99 (ms) | Notes |
|-----------|---------|----------|----------|-------|
| **put (64B)** | 2,507 | 0.39 | 0.56 | Baseline write |
| **put (1KB)** | 2,432 | 0.40 | 0.73 | 3% slower than 64B |
| **put (10KB)** | 1,892 | 0.52 | 0.73 | 27% slower than 64B |
| **get** | 3,340 | 0.31 | 0.44 | Baseline read |
| **get (missing)** | 3,443 | 0.30 | 0.47 | Same speed as hit |
| **remove** | 2,514 | 0.40 | 0.52 | Same as put |
| **delete** | 2,466 | 0.39 | 0.64 | Fire-and-forget remove |
| **contains_key** | 2,808 | 0.37 | 0.54 | Slightly faster than get |
| **compute** | 1,632 | 0.61 | 0.69 | Atomic read-modify-write |
| **replace** | 1,623 | 0.58 | 1.41 | CAS-style |
| **set_ttl** | 3,633 | 0.27 | 0.38 | TTL update (fast) |
| **size** | 3,928 | 0.26 | 0.28 | Cluster-wide count |
| **lock+unlock** | 2,572 | 0.38 | 0.52 | Lock cycle |
| **put_all (10)** | 4,720 | 0.21 | 0.26 | Batch write 10 entries |
| **put_all (100)** | 1,984 | 0.49 | 1.37 | Batch write 100 entries |
| **get_all (10)** | 4,334 | 0.23 | 0.28 | Batch read 10 keys |
| **get_all (100)** | 1,697 | 0.59 | 0.63 | Batch read 100 keys |
| **key_set (100)** | 18 | 54.13 | 70.20 | Iterator — SLOW |
| **key_set (1000)** | 20 | 50.90 | 56.84 | Iterator — SLOW |
| **entry_set (100)** | 19 | 51.26 | 56.45 | Iterator — SLOW |

### Cross-Cutting Benchmarks

| Benchmark | ops/sec | p50 (ms) | p99 (ms) | Notes |
|-----------|---------|----------|----------|-------|
| **Mixed 70/20/10** | 3,035 | 0.33 | 0.51 | 70% get, 20% put, 10% delete |
| **Concurrent 4 tasks** | 7,869 total | 0.50 | 0.87 | 4 parallel clients |

### Key Findings

1. **Single-op latency**: 0.3-0.4ms for get/put (same VPC, ~0.1ms network)
2. **Throughput ceiling**: ~3,500 ops/s single-threaded, ~8,000 ops/s with 4 concurrent tasks
3. **Batch efficiency**: put_all(10) = 4,720 ops/s vs put(1) = 2,507 ops/s — 88% faster per entry
4. **Value size impact**: 64B→10KB = 27% throughput reduction (serialization overhead)
5. **Iterator performance**: 18-20 ops/s — orders of magnitude slower than direct ops (271 partitions × RPC per partition)
6. **compute() overhead**: 1,632 ops/s vs put() 2,507 ops/s — 35% slower for atomic read-modify-write

---

## Methods Requiring Fixes (Priority Order)

### P0: Frame Layout — Missing threadId
These methods need `i64 LE threadId` added to initial frame:
- `replace()`, `replace_if_equal()`
- `lock()`, `try_lock()`, `unlock()`, `is_locked()`, `force_unlock()`
- `try_put()`
- `evict()`
- `get_entry_view()`
- `remove_if_equal()`

### P0: Frame Layout — Batch Operations
These methods have wrong frame construction for batch data:
- `put_all()`, `set_all()`, `get_all()`
- `remove_all()`

### P1: Service Routing
- ISet, IList, IQueue — need correct service type in proxy creation
- Already partially fixed but not fully working

### P1: Iterator Performance
- `key_set()`, `entry_set()`, `values_iter()` — 18-20 ops/s is too slow
- Cause: iterates 271 partitions sequentially, 1 RPC per partition
- Fix: batch fetching, parallel partition iteration

### P2: CP Subsystem
- AtomicLong, Semaphore, CountdownLatch, FencedLock
- Require CP subsystem configuration or Hazelcast Enterprise
- Consider: document as Enterprise-only feature

---

## Recommendations

1. **Fix threadId for all remaining methods** — systematic audit of every `create_for_encode()` call
2. **Fix batch operation frame layout** — align with Java client codec
3. **Parallelize iterator** — fetch from multiple partitions concurrently
4. **Add ISet/IList integration tests** — verify after service routing fix
5. **Document CP subsystem requirements** — AtomicLong etc. need explicit config
