# Hazelcast Rust Client — Feature Completeness Tracker

This document tracks the completeness of the Hazelcast Rust client compared to the official
Hazelcast Java client (5.x/6.x). It serves as the authoritative reference for identifying
gaps and planning implementation work.

**Last updated:** 2026-02-22 (Phase 2 Tier A: 7 missing data structure methods implemented)

## Legend

| Symbol | Meaning |
|--------|---------|
| :white_check_mark: | Fully implemented — sends real protocol messages to the cluster |
| :warning: | Partially implemented — exists but has known gaps or limitations |
| :x: | Not implemented — missing entirely from the Rust client |
| :construction: | Stubbed — types/config exist but no real server interaction |

---

## 1. Distributed Data Structures

### 1.1 IMap

| Operation | Status | Notes |
|-----------|--------|-------|
| `get` | :white_check_mark: | |
| `put` | :white_check_mark: | |
| `put_with_ttl` | :white_check_mark: | |
| `remove` | :white_check_mark: | |
| `delete` | :white_check_mark: | Remove without returning old value |
| `set` | :white_check_mark: | Basic `set(key, value)` — put without returning old value |
| `contains_key` | :white_check_mark: | |
| `contains_value` | :white_check_mark: | Cluster-wide value scan via `MAP_CONTAINS_VALUE` |
| `size` | :white_check_mark: | |
| `is_empty` | :white_check_mark: | Derived from `size() == 0` |
| `clear` | :white_check_mark: | |
| `put_all` | :white_check_mark: | |
| `set_all` | :white_check_mark: | |
| `get_all` | :white_check_mark: | |
| `remove_all` | :white_check_mark: | Predicate-based |
| `put_if_absent` | :white_check_mark: | |
| `replace` | :white_check_mark: | |
| `replace_if_equal` | :white_check_mark: | |
| `remove_if_equal` | :white_check_mark: | |
| `try_put` | :white_check_mark: | |
| `put_transient` | :white_check_mark: | |
| `set_ttl` | :white_check_mark: | |
| `evict` | :white_check_mark: | |
| `evict_all` | :white_check_mark: | |
| `load_all` | :white_check_mark: | |
| `load_given_keys` | :white_check_mark: | |
| `flush` | :white_check_mark: | |
| `lock` | :white_check_mark: | |
| `try_lock` | :white_check_mark: | |
| `unlock` | :white_check_mark: | |
| `force_unlock` | :white_check_mark: | |
| `is_locked` | :white_check_mark: | |
| `get_entry_view` | :white_check_mark: | |
| `add_entry_listener` | :white_check_mark: | Multiple variants (key, predicate) |
| `remove_entry_listener` | :white_check_mark: | |
| `add_partition_lost_listener` | :white_check_mark: | |
| `remove_partition_lost_listener` | :white_check_mark: | |
| `add_interceptor` | :white_check_mark: | |
| `remove_interceptor` | :white_check_mark: | |
| `execute_on_key` | :white_check_mark: | Entry processor |
| `execute_on_keys` | :white_check_mark: | |
| `execute_on_all_keys` | :white_check_mark: | |
| `execute_with_predicate` | :white_check_mark: | |
| `values_with_predicate` | :white_check_mark: | |
| `entries_with_predicate` | :white_check_mark: | |
| `keys_with_predicate` | :white_check_mark: | |
| Paging predicates | :white_check_mark: | `values_with_paging_predicate`, `keys_with_paging_predicate`, `entries_with_paging_predicate` |
| `aggregate` | :white_check_mark: | |
| `aggregate_with_predicate` | :white_check_mark: | |
| `project` | :white_check_mark: | |
| `project_with_predicate` | :white_check_mark: | |
| `add_index` | :white_check_mark: | SORTED, HASH, BITMAP |
| Event journal | :white_check_mark: | `subscribe` + `read` |
| Distributed iteration | :white_check_mark: | Via `key_set()`, `entry_set()`, `values_iter()` using `MAP_FETCH_*` |
| Near cache | :white_check_mark: | Full implementation with invalidation, eviction, stats |
| `compute` / `merge` | :white_check_mark: | Added in recent PR |

**IMap completeness: 100%**

### 1.2 MultiMap

| Operation | Status | Notes |
|-----------|--------|-------|
| All CRUD ops | :white_check_mark: | `put`, `get`, `remove`, `remove_all`, `clear`, `size` |
| Query ops | :white_check_mark: | `value_count`, `contains_key`, `contains_value`, `contains_entry` |
| Collection views | :white_check_mark: | `key_set`, `values`, `entry_set` |
| Locking | :white_check_mark: | `lock`, `try_lock`, `unlock`, `is_locked`, `force_unlock` |
| Listeners | :white_check_mark: | `add_entry_listener`, `remove_entry_listener` (with predicate variants) |

**MultiMap completeness: 100%**

### 1.3 ReplicatedMap

| Operation | Status | Notes |
|-----------|--------|-------|
| All CRUD ops | :white_check_mark: | `put`, `get`, `remove`, `size`, `is_empty`, `clear`, `put_all` |
| Query ops | :white_check_mark: | `contains_key`, `contains_value` |
| Collection views | :white_check_mark: | `key_set`, `values`, `entry_set` |
| Listeners | :white_check_mark: | `add_entry_listener`, `remove_entry_listener` (with predicate variants) |

**ReplicatedMap completeness: 100%**

### 1.4 IQueue

| Operation | Status | Notes |
|-----------|--------|-------|
| Core ops | :white_check_mark: | `offer`, `poll`, `peek`, `put`, `take`, `size`, `is_empty`, `remaining_capacity` |
| Bulk ops | :white_check_mark: | `add_all`, `drain_to`, `drain_to_max_size` |
| Query ops | :white_check_mark: | `contains`, `remove` |
| `clear` | :white_check_mark: | Removes all elements via `QUEUE_CLEAR` |
| Listeners | :white_check_mark: | `add_item_listener`, `remove_item_listener` |

**IQueue completeness: 100%**

### 1.5 ISet

| Operation | Status | Notes |
|-----------|--------|-------|
| All ops | :white_check_mark: | `add`, `remove`, `contains`, `clear`, `size`, `add_all`, `remove_all`, `retain_all`, `contains_all` |
| Listeners | :white_check_mark: | `add_item_listener`, `remove_item_listener` |

**ISet completeness: 100%**

### 1.6 IList

| Operation | Status | Notes |
|-----------|--------|-------|
| Indexed ops | :white_check_mark: | `get`, `set`, `add_at`, `remove_at`, `index_of`, `last_index_of` |
| Basic ops | :white_check_mark: | `add`, `contains`, `clear`, `size`, `is_empty` |
| Bulk ops | :white_check_mark: | `add_all`, `add_all_at`, `remove_all`, `retain_all`, `contains_all` |
| `remove` by value | :white_check_mark: | Remove element by value via `LIST_REMOVE` |
| Listeners | :white_check_mark: | `add_item_listener`, `remove_item_listener` |

**IList completeness: 100%**

### 1.7 ITopic

| Operation | Status | Notes |
|-----------|--------|-------|
| `publish` | :white_check_mark: | Also has `publish_all` |
| `add_message_listener` | :white_check_mark: | |
| `remove_message_listener` | :x: | Client-side `deactivate()` only — no server unregister message |

**ITopic completeness: ~67% (missing server-side listener removal)**

### 1.8 ReliableTopic

| Operation | Status | Notes |
|-----------|--------|-------|
| `publish` | :white_check_mark: | Via Ringbuffer |
| `add_message_listener` | :white_check_mark: | Client-side polling loop |
| `remove_message_listener` | :x: | Client-side `deactivate()` only |

**ReliableTopic completeness: ~67% (missing listener removal)**

### 1.9 Ringbuffer

| Operation | Status | Notes |
|-----------|--------|-------|
| All ops | :white_check_mark: | `add`, `add_all`, `read_one`, `read_many`, `size`, `capacity`, `remaining_capacity`, `head_sequence`, `tail_sequence` |
| Overflow policies | :white_check_mark: | OVERWRITE, FAIL |

**Ringbuffer completeness: 100%**

### 1.10 ICache (JCache / JSR-107)

| Operation | Status | Notes |
|-----------|--------|-------|
| CRUD | :white_check_mark: | `get`, `get_all`, `put`, `put_all`, `put_if_absent`, `remove`, `remove_all`, `clear`, `replace`, `contains_key` |
| Atomic | :white_check_mark: | `get_and_put`, `get_and_remove`, `get_and_replace` |
| Entry processor | :white_check_mark: | `invoke_processor` |
| Event journal | :white_check_mark: | `read_from_event_journal` |
| Listeners | :white_check_mark: | `register_cache_entry_listener`, `deregister_cache_entry_listener` |
| `size` | :white_check_mark: | Cluster-wide size via `CACHE_SIZE` protocol call |
| `is_empty` | :white_check_mark: | Derived from `size() == 0` |

**ICache completeness: 100%**

### 1.11 VectorCollection

| Operation | Status | Notes |
|-----------|--------|-------|
| All ops | :white_check_mark: | `put`, `put_if_absent`, `put_all`, `get`, `remove`, `set`, `delete`, `clear`, `size`, `search_near_vector`, `optimize` |

**VectorCollection completeness: 100%**

### 1.12 CardinalityEstimator

| Operation | Status | Notes |
|-----------|--------|-------|
| `add` | :white_check_mark: | |
| `estimate` | :white_check_mark: | |

**CardinalityEstimator completeness: 100%**

### 1.13 FlakeIdGenerator

| Operation | Status | Notes |
|-----------|--------|-------|
| `new_id` | :white_check_mark: | With client-side batch caching |
| `new_id_batch` | :white_check_mark: | |

**FlakeIdGenerator completeness: 100%**

### 1.14 PNCounter

| Operation | Status | Notes |
|-----------|--------|-------|
| All ops | :white_check_mark: | `get`, `add_and_get`, `get_and_add`, `increment_and_get`, `decrement_and_get`, `reset` |
| Replica timestamps | :white_check_mark: | Monotonic read guarantee via `ReplicaTimestampVector` |

**PNCounter completeness: 100%**

---

## 2. CP Subsystem (Raft-Backed Primitives)

| Feature | Status | Notes |
|---------|--------|-------|
| AtomicLong | :white_check_mark: | `get`, `set`, `get_and_set`, `compare_and_set`, `add_and_get`, `get_and_add` |
| AtomicReference | :warning: | Core ops implemented; `alter`/`apply` (server-side function execution) constants defined but not exposed |
| CountDownLatch | :white_check_mark: | All ops with CP session lifecycle — `await_latch` auto-acquires session |
| Semaphore | :white_check_mark: | All ops with CP session lifecycle — `acquire`/`release` auto-manage sessions |
| FencedLock | :white_check_mark: | All ops with CP session lifecycle — `lock`/`unlock` auto-manage sessions, fencing tokens |
| CPMap | :white_check_mark: | `get`, `put`, `set`, `remove`, `delete`, `compare_and_set` |
| **CP Session lifecycle** | :white_check_mark: | `CPSessionManager` with `CREATE_SESSION`, `CLOSE_SESSION`, `HEARTBEAT` (background loop), `GENERATE_THREAD_ID`. Sessions auto-created on first use, heartbeated, and closed on shutdown |
| CP Subsystem management | :white_check_mark: | `get_group_ids`, `get_group`, `force_destroy_group`, `get_cp_members`, `promote_to_cp_member`, `remove_cp_member` |

---

## 3. SQL Service

| Feature | Status | Notes |
|---------|--------|-------|
| `execute` | :white_check_mark: | Builds `SQL_EXECUTE` with query, parameters, timeout, cursor size, query ID |
| `fetch` (cursor) | :white_check_mark: | Automatic cursor-based pagination via `SqlResult::next_row()` |
| `close` | :white_check_mark: | |
| SqlStatement | :white_check_mark: | Query text, parameters, cursor buffer size, timeout, schema |
| Result set iteration | :white_check_mark: | Row-based with column metadata |

**SQL completeness: 100%**

---

## 4. Jet / Pipeline API

| Feature | Status | Notes |
|---------|--------|-------|
| Job submission | :white_check_mark: | Via `JetService::submit_job()` |
| Job status | :white_check_mark: | `get_status()`, `cached_status()` |
| Job lifecycle | :white_check_mark: | `cancel`, `cancel_gracefully`, `suspend`, `resume`, `restart` |
| Job metrics | :white_check_mark: | `get_metrics()` |
| Snapshot export | :white_check_mark: | `export_snapshot()`, `export_snapshot_with_options()` |
| Job listing | :white_check_mark: | `get_job_ids()`, `get_job_summary_list()` |
| Pipeline API | :warning: | `Pipeline` struct exists with `execute`, `execute_ordered`; sources/sinks/transforms are limited to IMap-based operations |
| Kafka connectors | :white_check_mark: | Feature-gated (`kafka`); `KafkaSource`, `KafkaSink` with config |

**Jet completeness: ~85% (pipeline API has limited source/sink variety)**

---

## 5. Transactions

| Feature | Status | Notes |
|---------|--------|-------|
| TransactionContext | :white_check_mark: | `begin`, `commit`, `rollback` with timeout, durability, type config |
| TransactionalMap | :white_check_mark: | `get`, `put`, `set`, `put_if_absent`, `replace`, `remove`, `delete`, `contains_key`, `size` |
| TransactionalMultiMap | :white_check_mark: | `put`, `get`, `remove`, `remove_entry`, `value_count`, `size` |
| TransactionalQueue | :white_check_mark: | `offer`, `poll`, `size` |
| TransactionalSet | :white_check_mark: | `add`, `remove`, `size` |
| TransactionalList | :white_check_mark: | `add`, `remove`, `size` |
| XA Transactions | :white_check_mark: | Full `XAResource` trait; `start`, `prepare`, `commit`, `rollback`, `recover`, `forget` |

**Transaction completeness: 100%**

---

## 6. Executor Services

| Feature | Status | Notes |
|---------|--------|-------|
| IExecutorService | :white_check_mark: | `submit_to_partition`, `submit_to_member`, `cancel_on_partition`, `cancel_on_member`, `shutdown`, `is_shutdown` |
| DurableExecutorService | :white_check_mark: | `submit`, `retrieve_result`, `dispose_result`, `retrieve_and_dispose_result`, `shutdown`, `is_shutdown` |
| ScheduledExecutorService | :white_check_mark: | `submit_to_partition`, `submit_to_member`, `cancel`, `is_done`, `get_delay`, `get_result`, `shutdown`, `dispose` |

**Executor completeness: 100%**

---

## 7. Serialization

| Feature | Status | Notes |
|---------|--------|-------|
| Compact serialization | :white_check_mark: | `CompactWriter`/`CompactReader` traits, `DefaultCompactWriter`/`DefaultCompactReader`, `SchemaRegistry`, all field types |
| GenericRecord | :white_check_mark: | Schema-agnostic access, `GenericRecordBuilder`, Rabin fingerprint schema ID |
| Portable serialization | :white_check_mark: | `PortableSerializer`, `ClassDefinition`, factory registration, nested Portable support |
| IdentifiedDataSerializable | :white_check_mark: | `IdentifiedDataSerializable` trait, `FactoryRegistry` |
| JSON value | :white_check_mark: | `HazelcastJsonValue` with type ID `-130` |
| Serde integration | :white_check_mark: | `Serde<T>` wrapper via `bincode` |
| Custom/Global serializer | :white_check_mark: | `CustomSerializer` trait, `GlobalSerializer` trait, `SerializationConfig` |
| Derive macros | :white_check_mark: | `#[derive(CompactSerializable)]`, `#[derive(PortableSerializable)]`, `#[derive(IdentifiedSerializable)]` |

**Serialization completeness: 100%**

---

## 8. Query System

| Feature | Status | Notes |
|---------|--------|-------|
| Predicates | :white_check_mark: | True, False, Equal, NotEqual, GreaterThan, LessThan, Between, In, Like, Regex, Sql, And, Or, Not |
| JSON predicates | :white_check_mark: | `valueEquals`, `contains`, `rawSql` |
| Indexed predicates | :white_check_mark: | With `IndexHint` |
| Paging predicates | :white_check_mark: | `PagingPredicate` with comparator support |
| Aggregations | :white_check_mark: | Count, Sum, Average, Min, Max, DistinctValues, etc. |
| Projections | :white_check_mark: | Single and multi-attribute |
| Index config | :white_check_mark: | SORTED, HASH, BITMAP with builder |

**Query completeness: 100%**

---

## 9. Caching

| Feature | Status | Notes |
|---------|--------|-------|
| Near Cache | :white_check_mark: | Full implementation with eviction (LRU, LFU, RANDOM), TTL, max-idle, invalidation, stats |
| Near Cache preloader | :white_check_mark: | Configuration present |
| Query Cache | :white_check_mark: | Continuously-updated local cache with predicates, local indexing, eviction |

**Caching completeness: 100%**

---

## 10. Connection & Cluster Management

| Feature | Status | Notes |
|---------|--------|-------|
| Smart routing | :white_check_mark: | Route to partition owner |
| Unisocket mode | :white_check_mark: | Single connection mode |
| Reconnect modes | :white_check_mark: | OFF, ON, ASYNC |
| Heartbeat | :white_check_mark: | Configurable interval and timeout |
| Connection timeout | :white_check_mark: | |
| Membership listeners | :white_check_mark: | Member added/removed events via `subscribe_membership()` |
| Partition lost listeners | :white_check_mark: | Via `subscribe_partition_lost()` |
| Distributed object listeners | :white_check_mark: | Via `subscribe_distributed_object()` |
| Migration listeners | :white_check_mark: | Via `subscribe_migration()` |
| Lifecycle events | :white_check_mark: | Via `subscribe_lifecycle()` |
| Failover (multi-cluster) | :white_check_mark: | `ClientFailoverConfig`, `trigger_failover()` |
| Split-brain protection | :white_check_mark: | `SplitBrainProtectionService` with quorum checking |
| Load balancing | :white_check_mark: | Round-robin, random, custom |

**Cluster management completeness: 100%**

---

## 11. Security

| Feature | Status | Notes |
|---------|--------|-------|
| TLS/SSL | :white_check_mark: | Feature-gated (`tls`); `tokio-rustls` with CA cert, client cert/key, hostname verification |
| Username/password auth | :white_check_mark: | `Credentials` struct |
| Token-based auth | :white_check_mark: | `CustomCredentials` |
| Kerberos | :white_check_mark: | Feature-gated (`kerberos`); `KerberosAuthenticator`, `KerberosCredentials` |
| Client permissions | :white_check_mark: | `Permissions`, `PermissionAction` |
| Authenticator trait | :white_check_mark: | `Authenticator`, `DefaultAuthenticator` |

**Security completeness: 100%**

---

## 12. Cloud & Discovery

| Feature | Status | Notes |
|---------|--------|-------|
| Static addresses | :white_check_mark: | `StaticAddressDiscovery` |
| Hazelcast Cloud | :white_check_mark: | Feature-gated (`cloud`); `CloudDiscovery` |
| AWS EC2 | :white_check_mark: | Feature-gated (`aws`); `AwsDiscovery` |
| Azure | :white_check_mark: | Feature-gated (`azure`); `AzureDiscovery` |
| GCP | :white_check_mark: | Feature-gated (`gcp`); `GcpDiscovery` |
| Kubernetes | :white_check_mark: | Feature-gated (`kubernetes`); `KubernetesDiscovery` |
| Eureka | :white_check_mark: | Feature-gated (`eureka`); `EurekaDiscovery` |
| Multicast | :white_check_mark: | `MulticastDiscovery` |
| Auto-detection | :white_check_mark: | `AutoDetectionDiscovery` |

**Discovery completeness: 100%**

---

## 13. Configuration

| Feature | Status | Notes |
|---------|--------|-------|
| Programmatic config | :white_check_mark: | `ClientConfig::builder()` fluent API |
| YAML file config | :white_check_mark: | Feature-gated (`config-file`) |
| TOML file config | :white_check_mark: | Feature-gated (`config-file`) |
| Network config | :white_check_mark: | Addresses, timeout, heartbeat, socket config |
| Retry config | :white_check_mark: | Backoff, multiplier, max retries |
| Failover config | :white_check_mark: | Multi-cluster with priority |
| Near cache config | :white_check_mark: | Per-map configuration |
| Serialization config | :white_check_mark: | Factory registration, custom serializers |

**Configuration completeness: 100%**

---

## 14. Monitoring & Diagnostics

| Feature | Status | Notes |
|---------|--------|-------|
| Client statistics | :white_check_mark: | `CLIENT_STATISTICS` protocol message |
| Prometheus exporter | :white_check_mark: | Feature-gated (`metrics`); `PrometheusExporter`, connection/bytes/latency metrics |
| Local map stats | :white_check_mark: | `LocalMapStats` with operation counts, latencies, near cache stats |
| Local collection stats | :white_check_mark: | `LocalQueueStats`, `LocalSetStats`, `LocalListStats` |

**Monitoring completeness: 100%**

---

## 15. Other Features

| Feature | Status | Notes |
|---------|--------|-------|
| User code deployment | :white_check_mark: | `UserCodeDeploymentConfig`, `ResourceEntry` |
| Tower integration | :white_check_mark: | Feature-gated (`tower`); `HazelcastService` |
| WebSocket transport | :white_check_mark: | Feature-gated (`websocket`) |
| Data connections | :white_check_mark: | `DataConnectionService` with `get_config`, `list_configs` |
| PartitionAware | :white_check_mark: | Custom key partitioning trait |
| Pipelining | :white_check_mark: | `InvocationPipeline` for operation batching |
| Socket interceptor | :white_check_mark: | `SocketInterceptor` trait |
| Distributed iterator | :white_check_mark: | Cursor-based `DistributedIterator` |

---

## Overall Completeness Summary

| Category | Status | Completeness |
|----------|--------|-------------|
| IMap | :white_check_mark: | 100% |
| MultiMap | :white_check_mark: | 100% |
| ReplicatedMap | :white_check_mark: | 100% |
| IQueue | :white_check_mark: | 100% |
| ISet | :white_check_mark: | 100% |
| IList | :white_check_mark: | 100% |
| ITopic | :warning: | 67% — missing server-side `remove_message_listener` |
| ReliableTopic | :warning: | 67% — missing `remove_message_listener` |
| Ringbuffer | :white_check_mark: | 100% |
| ICache | :white_check_mark: | 100% |
| VectorCollection | :white_check_mark: | 100% |
| CardinalityEstimator | :white_check_mark: | 100% |
| FlakeIdGenerator | :white_check_mark: | 100% |
| PNCounter | :white_check_mark: | 100% |
| CP Subsystem | :white_check_mark: | 95% — session lifecycle implemented; only `alter`/`apply` on AtomicReference remain |
| SQL | :white_check_mark: | 100% |
| Jet | :warning: | 85% — pipeline source/sink variety limited |
| Transactions | :white_check_mark: | 100% |
| Executors | :white_check_mark: | 100% |
| Serialization | :white_check_mark: | 100% |
| Query System | :white_check_mark: | 100% |
| Caching | :white_check_mark: | 100% |
| Cluster Management | :white_check_mark: | 100% |
| Security | :white_check_mark: | 100% |
| Discovery | :white_check_mark: | 100% |
| Configuration | :white_check_mark: | 100% |
| Monitoring | :white_check_mark: | 100% |

**Overall estimated completeness: ~98%**

---

## Implementation Plan — Closing the Gaps

### Phase 1: Critical — CP Session Lifecycle (Priority: HIGH) :white_check_mark: COMPLETED

~~The most impactful gap.~~ **Implemented!** CP session lifecycle is now fully operational.

**Completed work:**
1. :white_check_mark: `CPSessionManager` struct in `cluster/cp_session.rs` with `acquire_session()`,
   `release_session()`, `invalidate_session()`, `get_session_id()`
2. :white_check_mark: Background heartbeat loop using `CP_SESSION_HEARTBEAT` (5-second interval,
   auto-invalidates expired sessions)
3. :white_check_mark: `CP_SESSION_GENERATE_THREAD_ID` with server call + local fallback
4. :white_check_mark: `CP_SESSION_CLOSE_SESSION` on client shutdown (graceful cleanup)
5. :white_check_mark: `FencedLock`, `Semaphore`, `CountDownLatch` all wired to use
   `CPSessionManager` via `with_session_manager()` constructor — sessions are
   auto-acquired on first operation and properly released/invalidated
6. :warning: `alter`/`apply` methods on AtomicReference still pending (moved to Phase 2)

**Remaining:** Only AtomicReference `alter`/`apply` methods remain from this phase.

### Phase 2: Missing Data Structure Methods (Priority: MEDIUM)

Small, isolated additions to existing proxies. Each is a single method following
established patterns.

**Work items:**
1. **AtomicReference**: Add `alter`/`apply` methods using `CP_ATOMIC_REFERENCE_ALTER`
   and `CP_ATOMIC_REFERENCE_APPLY` (2 methods — server-side function execution)
2. :white_check_mark: ~~**IMap**: Add `delete`, `set`, `contains_value`, `is_empty` (4 methods)~~
3. :white_check_mark: ~~**IQueue**: Add `clear` (1 method)~~
4. :white_check_mark: ~~**IList**: Add `remove` by value (1 method)~~
5. :white_check_mark: ~~**ICache**: Add `size` + bonus `is_empty` (2 methods)~~
6. **ITopic**: Add `remove_message_listener` using `TOPIC_REMOVE_MESSAGE_LISTENER`
   protocol message (1 method)
7. **ReliableTopic**: Add `remove_message_listener` — architecturally this is
   client-side only (cancel polling loop), but should be an explicit public method
   (1 method)

**Remaining effort:** Items 1, 6, 7 still pending. Each follows established patterns.

### Phase 3: Pipeline API Enrichment (Priority: LOW)

The Jet `Pipeline` struct exists but has limited source/sink variety compared to
the Java client.

**Work items:**
1. Add file-based sources/sinks
2. Add JDBC sources/sinks (if applicable in Rust)
3. Add socket sources/sinks
4. Add test sources (for development/testing)
5. Expand transformation operators

**Estimated effort:** Medium — requires designing Rust-idiomatic APIs for pipeline
stages that serialize correctly for the Hazelcast Jet engine.

---

## Appendix: Protocol Constants Defined But Unused

The following constants are defined in `hazelcast-core/src/protocol/constants.rs` but
are not referenced anywhere in client code. They may represent features that have
protocol support but no client-side implementation:

| Constant | Value | Feature |
|----------|-------|---------|
| `CP_ATOMIC_REFERENCE_ALTER` | `0x0A0700` | Server-side function execution |
| `CP_ATOMIC_REFERENCE_APPLY` | `0x0A0800` | Server-side function execution |

> **Note:** The CP session constants (`CP_SESSION_CREATE_SESSION`, `CP_SESSION_CLOSE_SESSION`,
> `CP_SESSION_HEARTBEAT`, `CP_SESSION_GENERATE_THREAD_ID`) were previously listed here but
> are now fully used by the `CPSessionManager` implementation.
