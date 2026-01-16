//! Protocol constants for the Hazelcast Open Binary Protocol.

/// Size of the frame length field in bytes.
pub const SIZE_OF_FRAME_LENGTH_FIELD: usize = 4;

/// Size of the frame flags field in bytes.
pub const SIZE_OF_FRAME_FLAGS_FIELD: usize = 2;

/// Total frame header size (length + flags).
pub const FRAME_HEADER_SIZE: usize = SIZE_OF_FRAME_LENGTH_FIELD + SIZE_OF_FRAME_FLAGS_FIELD;

/// Begin frame flag - marks the start of a client message.
pub const BEGIN_FLAG: u16 = 1 << 15;

/// End frame flag - marks the end of a client message.
pub const END_FLAG: u16 = 1 << 14;

/// Final flag - indicates the final response.
pub const IS_FINAL_FLAG: u16 = 1 << 13;

/// Event flag - indicates this is an event message.
pub const IS_EVENT_FLAG: u16 = 1 << 12;

/// Backup event flag.
pub const BACKUP_EVENT_FLAG: u16 = 1 << 11;

/// Null frame flag - indicates a null value.
pub const IS_NULL_FLAG: u16 = 1 << 10;

/// Default frame flags (no special flags set).
pub const DEFAULT_FLAGS: u16 = 0;

/// Flags for a begin frame (single frame that is both begin and end).
pub const BEGIN_END_FLAGS: u16 = BEGIN_FLAG | END_FLAG;

/// Offset of message type in initial frame content.
pub const TYPE_FIELD_OFFSET: usize = 0;

/// Offset of correlation ID in initial frame content.
pub const CORRELATION_ID_OFFSET: usize = TYPE_FIELD_OFFSET + 4;

/// Offset of partition ID in request initial frame.
pub const PARTITION_ID_OFFSET: usize = CORRELATION_ID_OFFSET + 8;

/// Size of the request initial frame header.
pub const REQUEST_HEADER_SIZE: usize = PARTITION_ID_OFFSET + 4;

/// Offset of backup acks count in response initial frame.
pub const RESPONSE_BACKUP_ACKS_OFFSET: usize = CORRELATION_ID_OFFSET + 8;

/// Size of the response initial frame header.
pub const RESPONSE_HEADER_SIZE: usize = RESPONSE_BACKUP_ACKS_OFFSET + 1;

/// Partition ID indicating no specific partition (-1).
pub const PARTITION_ID_ANY: i32 = -1;

// Message type constants for common operations.

/// Client authentication request.
pub const CLIENT_AUTHENTICATION: i32 = 0x000100;

/// Client authentication response.
pub const CLIENT_AUTHENTICATION_RESPONSE: i32 = 0x000101;

/// Map get request.
pub const MAP_GET: i32 = 0x010200;

/// Map put request.
pub const MAP_PUT: i32 = 0x010100;

/// Map remove request.
pub const MAP_REMOVE: i32 = 0x010300;

/// Map put all request.
pub const MAP_PUT_ALL: i32 = 0x010400;

/// Map set all request.
pub const MAP_SET_ALL: i32 = 0x012C00;

/// Map get all request.
pub const MAP_GET_ALL: i32 = 0x010B00;

/// Map put if absent request.
pub const MAP_PUT_IF_ABSENT: i32 = 0x010700;

/// Map contains key request.
pub const MAP_CONTAINS_KEY: i32 = 0x010900;

/// Map replace request.
pub const MAP_REPLACE: i32 = 0x010A00;

/// Map remove if same request.
pub const MAP_REMOVE_IF_SAME: i32 = 0x010C00;

/// Map replace if same request.
pub const MAP_REPLACE_IF_SAME: i32 = 0x011000;

/// Map size request.
pub const MAP_SIZE: i32 = 0x010500;

/// Map clear request.
pub const MAP_CLEAR: i32 = 0x010D00;

/// Queue offer request.
pub const QUEUE_OFFER: i32 = 0x030100;

/// Queue poll request.
pub const QUEUE_POLL: i32 = 0x030200;

/// Queue size request.
pub const QUEUE_SIZE: i32 = 0x030300;

/// Queue peek request.
pub const QUEUE_PEEK: i32 = 0x030400;

/// Queue put request (blocking).
pub const QUEUE_PUT: i32 = 0x030500;

/// Queue take request (blocking).
pub const QUEUE_TAKE: i32 = 0x030600;

/// Queue remove request.
pub const QUEUE_REMOVE: i32 = 0x030700;

/// Queue contains request.
pub const QUEUE_CONTAINS: i32 = 0x030800;

/// Queue add all request.
pub const QUEUE_ADD_ALL: i32 = 0x030900;

/// Queue compare and remove all request.
pub const QUEUE_COMPARE_AND_REMOVE_ALL: i32 = 0x030A00;

/// Queue drain to max size request.
pub const QUEUE_DRAIN_TO_MAX_SIZE: i32 = 0x030C00;

/// Queue contains all request.
pub const QUEUE_CONTAINS_ALL: i32 = 0x030D00;

/// Queue add item listener request.
pub const QUEUE_ADD_LISTENER: i32 = 0x030E00;

/// Queue remove item listener request.
pub const QUEUE_REMOVE_LISTENER: i32 = 0x030F00;

/// Queue remaining capacity request.
pub const QUEUE_REMAINING_CAPACITY: i32 = 0x031000;

/// Queue is empty request.
pub const QUEUE_IS_EMPTY: i32 = 0x031100;

/// Queue iterator (get all) request.
pub const QUEUE_ITERATOR: i32 = 0x031200;

/// Queue drain to (without max size) request.
pub const QUEUE_DRAIN_TO: i32 = 0x030B00;

/// Set add request.
pub const SET_ADD: i32 = 0x060100;

/// Set remove request.
pub const SET_REMOVE: i32 = 0x060200;

/// Set contains request.
pub const SET_CONTAINS: i32 = 0x060400;

/// Set clear request.
pub const SET_CLEAR: i32 = 0x060500;

/// Set size request.
pub const SET_SIZE: i32 = 0x060800;

/// Set contains all request.
pub const SET_CONTAINS_ALL: i32 = 0x060300;

/// Set add all request.
pub const SET_ADD_ALL: i32 = 0x060600;

/// Set remove all request.
pub const SET_REMOVE_ALL: i32 = 0x060700;

/// Set retain all request.
pub const SET_RETAIN_ALL: i32 = 0x060900;

/// Set add item listener request.
pub const SET_ADD_LISTENER: i32 = 0x060A00;

/// Set remove item listener request.
pub const SET_REMOVE_LISTENER: i32 = 0x060B00;

/// List size request.
pub const LIST_SIZE: i32 = 0x050100;

/// List contains request.
pub const LIST_CONTAINS: i32 = 0x050200;

/// List add request.
pub const LIST_ADD: i32 = 0x050500;

/// List add at index request.
pub const LIST_ADD_AT: i32 = 0x050600;

/// List set request.
pub const LIST_SET: i32 = 0x050700;

/// List get request.
pub const LIST_GET: i32 = 0x050800;

/// List remove at index request.
pub const LIST_REMOVE_AT: i32 = 0x050900;

/// List clear request.
pub const LIST_CLEAR: i32 = 0x050A00;

/// List contains all request.
pub const LIST_CONTAINS_ALL: i32 = 0x050300;

/// List add all request.
pub const LIST_ADD_ALL: i32 = 0x050400;

/// List add all at index request.
pub const LIST_ADD_ALL_AT: i32 = 0x050B00;

/// List remove all request.
pub const LIST_REMOVE_ALL: i32 = 0x050C00;

/// List retain all request.
pub const LIST_RETAIN_ALL: i32 = 0x050D00;

/// List index of request.
pub const LIST_INDEX_OF: i32 = 0x050E00;

/// List last index of request.
pub const LIST_LAST_INDEX_OF: i32 = 0x050F00;

/// List add item listener request.
pub const LIST_ADD_LISTENER: i32 = 0x051000;

/// List remove item listener request.
pub const LIST_REMOVE_LISTENER: i32 = 0x051100;

/// MultiMap put request.
pub const MULTI_MAP_PUT: i32 = 0x020100;

/// MultiMap get request.
pub const MULTI_MAP_GET: i32 = 0x020200;

/// MultiMap remove request.
pub const MULTI_MAP_REMOVE: i32 = 0x020300;

/// MultiMap clear request.
pub const MULTI_MAP_CLEAR: i32 = 0x020600;

/// MultiMap size request.
pub const MULTI_MAP_SIZE: i32 = 0x020700;

/// MultiMap value count request.
pub const MULTI_MAP_VALUE_COUNT: i32 = 0x020800;

/// MultiMap contains key request.
pub const MULTI_MAP_CONTAINS_KEY: i32 = 0x020900;

/// MultiMap contains value request.
pub const MULTI_MAP_CONTAINS_VALUE: i32 = 0x020A00;

/// MultiMap contains entry request.
pub const MULTI_MAP_CONTAINS_ENTRY: i32 = 0x020B00;

/// MultiMap remove entry request.
pub const MULTI_MAP_REMOVE_ENTRY: i32 = 0x020C00;

/// MultiMap key set request.
pub const MULTI_MAP_KEY_SET: i32 = 0x020400;

/// MultiMap values request.
pub const MULTI_MAP_VALUES: i32 = 0x020500;

/// MultiMap entry set request.
pub const MULTI_MAP_ENTRY_SET: i32 = 0x020D00;

/// MultiMap lock request.
pub const MULTI_MAP_LOCK: i32 = 0x020E00;

/// MultiMap try lock request.
pub const MULTI_MAP_TRY_LOCK: i32 = 0x020F00;

/// MultiMap unlock request.
pub const MULTI_MAP_UNLOCK: i32 = 0x021000;

/// MultiMap is locked request.
pub const MULTI_MAP_IS_LOCKED: i32 = 0x021100;

/// MultiMap force unlock request.
pub const MULTI_MAP_FORCE_UNLOCK: i32 = 0x021200;

/// MultiMap add entry listener request.
pub const MULTI_MAP_ADD_ENTRY_LISTENER: i32 = 0x021300;

/// MultiMap remove entry listener request.
pub const MULTI_MAP_REMOVE_ENTRY_LISTENER: i32 = 0x021400;

/// MultiMap add entry listener with predicate request.
pub const MULTI_MAP_ADD_ENTRY_LISTENER_WITH_PREDICATE: i32 = 0x021500;

/// Topic publish request.
pub const TOPIC_PUBLISH: i32 = 0x040100;

/// Topic add message listener request.
pub const TOPIC_ADD_MESSAGE_LISTENER: i32 = 0x040200;

/// Topic remove message listener request.
pub const TOPIC_REMOVE_MESSAGE_LISTENER: i32 = 0x040300;

/// CP AtomicLong get request.
pub const CP_ATOMIC_LONG_GET: i32 = 0x090100;

/// CP AtomicLong set request.
pub const CP_ATOMIC_LONG_SET: i32 = 0x090200;

/// CP AtomicLong get and set request.
pub const CP_ATOMIC_LONG_GET_AND_SET: i32 = 0x090300;

/// CP AtomicLong compare and set request.
pub const CP_ATOMIC_LONG_COMPARE_AND_SET: i32 = 0x090400;

/// CP AtomicLong add and get request.
pub const CP_ATOMIC_LONG_ADD_AND_GET: i32 = 0x090500;

/// CP AtomicLong get and add request.
pub const CP_ATOMIC_LONG_GET_AND_ADD: i32 = 0x090600;

/// CP AtomicReference get request.
pub const CP_ATOMIC_REFERENCE_GET: i32 = 0x0A0100;

/// CP AtomicReference set request.
pub const CP_ATOMIC_REFERENCE_SET: i32 = 0x0A0200;

/// CP AtomicReference get and set request.
pub const CP_ATOMIC_REFERENCE_GET_AND_SET: i32 = 0x0A0300;

/// CP AtomicReference compare and set request.
pub const CP_ATOMIC_REFERENCE_COMPARE_AND_SET: i32 = 0x0A0400;

/// CP AtomicReference contains request.
pub const CP_ATOMIC_REFERENCE_CONTAINS: i32 = 0x0A0500;

/// CP AtomicReference alter request.
pub const CP_ATOMIC_REFERENCE_ALTER: i32 = 0x0A0600;

/// CP AtomicReference apply request.
pub const CP_ATOMIC_REFERENCE_APPLY: i32 = 0x0A0700;

/// Map lock request.
pub const MAP_LOCK: i32 = 0x011200;

/// Map try lock request.
pub const MAP_TRY_LOCK: i32 = 0x011300;

/// Map add entry listener request.
pub const MAP_ADD_ENTRY_LISTENER: i32 = 0x011400;

/// Map remove entry listener request.
pub const MAP_REMOVE_ENTRY_LISTENER: i32 = 0x011500;

/// Map force unlock request.
pub const MAP_FORCE_UNLOCK: i32 = 0x011600;

/// Map unlock request.
pub const MAP_UNLOCK: i32 = 0x011700;

/// Map is locked request.
pub const MAP_IS_LOCKED: i32 = 0x011800;

/// Map add entry listener with predicate request.
pub const MAP_ADD_ENTRY_LISTENER_WITH_PREDICATE: i32 = 0x011900;

/// Map add entry listener for a specific key request.
pub const MAP_ADD_ENTRY_LISTENER_TO_KEY: i32 = 0x011C00;

/// Map add entry listener for a specific key with predicate request.
pub const MAP_ADD_ENTRY_LISTENER_TO_KEY_WITH_PREDICATE: i32 = 0x011D00;

/// Client add membership listener request.
pub const CLIENT_ADD_MEMBERSHIP_LISTENER: i32 = 0x000300;

/// Client remove membership listener request.
pub const CLIENT_REMOVE_MEMBERSHIP_LISTENER: i32 = 0x000301;

/// Client add partition lost listener request.
pub const CLIENT_ADD_PARTITION_LOST_LISTENER: i32 = 0x000600;

/// Client remove partition lost listener request.
pub const CLIENT_REMOVE_PARTITION_LOST_LISTENER: i32 = 0x000700;

/// Client add distributed object listener request.
pub const CLIENT_ADD_DISTRIBUTED_OBJECT_LISTENER: i32 = 0x000800;

/// Client remove distributed object listener request.
pub const CLIENT_REMOVE_DISTRIBUTED_OBJECT_LISTENER: i32 = 0x000900;

/// Client get distributed objects request.
pub const CLIENT_GET_DISTRIBUTED_OBJECTS: i32 = 0x000A00;

/// Client create proxy request.
pub const CLIENT_CREATE_PROXY: i32 = 0x000B00;

/// Client destroy proxy request.
pub const CLIENT_DESTROY_PROXY: i32 = 0x000C00;

/// ReplicatedMap put request.
pub const REPLICATED_MAP_PUT: i32 = 0x0D0100;

/// ReplicatedMap get request.
pub const REPLICATED_MAP_GET: i32 = 0x0D0200;

/// ReplicatedMap remove request.
pub const REPLICATED_MAP_REMOVE: i32 = 0x0D0300;

/// ReplicatedMap size request.
pub const REPLICATED_MAP_SIZE: i32 = 0x0D0400;

/// ReplicatedMap is empty request.
pub const REPLICATED_MAP_IS_EMPTY: i32 = 0x0D0500;

/// ReplicatedMap contains key request.
pub const REPLICATED_MAP_CONTAINS_KEY: i32 = 0x0D0600;

/// ReplicatedMap contains value request.
pub const REPLICATED_MAP_CONTAINS_VALUE: i32 = 0x0D0700;

/// ReplicatedMap clear request.
pub const REPLICATED_MAP_CLEAR: i32 = 0x0D0800;

/// ReplicatedMap key set request.
pub const REPLICATED_MAP_KEY_SET: i32 = 0x0D0900;

/// ReplicatedMap values request.
pub const REPLICATED_MAP_VALUES: i32 = 0x0D0A00;

/// ReplicatedMap entry set request.
pub const REPLICATED_MAP_ENTRY_SET: i32 = 0x0D0B00;

/// ReplicatedMap add entry listener request.
pub const REPLICATED_MAP_ADD_ENTRY_LISTENER: i32 = 0x0D0C00;

/// ReplicatedMap remove entry listener request.
pub const REPLICATED_MAP_REMOVE_ENTRY_LISTENER: i32 = 0x0D0D00;

/// ReplicatedMap add entry listener with predicate request.
pub const REPLICATED_MAP_ADD_ENTRY_LISTENER_WITH_PREDICATE: i32 = 0x0D0E00;

/// ReplicatedMap put all request.
pub const REPLICATED_MAP_PUT_ALL: i32 = 0x0D0F00;

/// ReplicatedMap put with TTL request.
pub const REPLICATED_MAP_PUT_WITH_TTL: i32 = 0x0D1000;

/// Ringbuffer size request.
pub const RINGBUFFER_SIZE: i32 = 0x190100;

/// Ringbuffer tail sequence request.
pub const RINGBUFFER_TAIL_SEQUENCE: i32 = 0x190200;

/// Ringbuffer head sequence request.
pub const RINGBUFFER_HEAD_SEQUENCE: i32 = 0x190300;

/// Ringbuffer capacity request.
pub const RINGBUFFER_CAPACITY: i32 = 0x190400;

/// Ringbuffer remaining capacity request.
pub const RINGBUFFER_REMAINING_CAPACITY: i32 = 0x190500;

/// Ringbuffer add request.
pub const RINGBUFFER_ADD: i32 = 0x190600;

/// Ringbuffer read one request.
pub const RINGBUFFER_READ_ONE: i32 = 0x190700;

/// Ringbuffer add all request.
pub const RINGBUFFER_ADD_ALL: i32 = 0x190800;

/// Ringbuffer read many request.
pub const RINGBUFFER_READ_MANY: i32 = 0x190900;

/// PN Counter get request.
pub const PN_COUNTER_GET: i32 = 0x200100;

/// PN Counter add request.
pub const PN_COUNTER_ADD: i32 = 0x200200;

/// FlakeIdGenerator new ID batch request.
pub const FLAKE_ID_GENERATOR_NEW_ID_BATCH: i32 = 0x1C0100;

/// CP FencedLock lock request.
pub const CP_FENCED_LOCK_LOCK: i32 = 0x070100;

/// CP FencedLock try lock request.
pub const CP_FENCED_LOCK_TRY_LOCK: i32 = 0x070200;

/// CP FencedLock unlock request.
pub const CP_FENCED_LOCK_UNLOCK: i32 = 0x070300;

/// CP FencedLock get lock ownership state request.
pub const CP_FENCED_LOCK_GET_LOCK_OWNERSHIP_STATE: i32 = 0x070400;

/// CP Semaphore init request.
pub const CP_SEMAPHORE_INIT: i32 = 0x0C0100;

/// CP Semaphore acquire request.
pub const CP_SEMAPHORE_ACQUIRE: i32 = 0x0C0200;

/// CP Semaphore release request.
pub const CP_SEMAPHORE_RELEASE: i32 = 0x0C0300;

/// CP Semaphore drain request.
pub const CP_SEMAPHORE_DRAIN: i32 = 0x0C0400;

/// CP Semaphore change (reduce/increase) request.
pub const CP_SEMAPHORE_CHANGE: i32 = 0x0C0500;

/// CP Semaphore available permits request.
pub const CP_SEMAPHORE_AVAILABLE_PERMITS: i32 = 0x0C0600;

/// CP CountDownLatch try set count request.
pub const CP_COUNTDOWN_LATCH_TRY_SET_COUNT: i32 = 0x0B0100;

/// CP CountDownLatch count down request.
pub const CP_COUNTDOWN_LATCH_COUNT_DOWN: i32 = 0x0B0200;

/// CP CountDownLatch await request.
pub const CP_COUNTDOWN_LATCH_AWAIT: i32 = 0x0B0300;

/// CP CountDownLatch get count request.
pub const CP_COUNTDOWN_LATCH_GET_COUNT: i32 = 0x0B0400;

/// CP Session create session request.
pub const CP_SESSION_CREATE_SESSION: i32 = 0x1F0100;

/// CP Session close session request.
pub const CP_SESSION_CLOSE_SESSION: i32 = 0x1F0200;

/// CP Session heartbeat request.
pub const CP_SESSION_HEARTBEAT: i32 = 0x1F0300;

/// CP Session generate thread ID request.
pub const CP_SESSION_GENERATE_THREAD_ID: i32 = 0x1F0400;

/// CP Map get request.
pub const CP_MAP_GET: i32 = 0x230100;

/// CP Map put request.
pub const CP_MAP_PUT: i32 = 0x230200;

/// CP Map set request.
pub const CP_MAP_SET: i32 = 0x230300;

/// CP Map remove request.
pub const CP_MAP_REMOVE: i32 = 0x230400;

/// CP Map delete request.
pub const CP_MAP_DELETE: i32 = 0x230500;

/// CP Map compare and set request.
pub const CP_MAP_COMPARE_AND_SET: i32 = 0x230600;

/// SQL execute request.
pub const SQL_EXECUTE: i32 = 0x210100;

/// SQL fetch request.
pub const SQL_FETCH: i32 = 0x210200;

/// SQL close request.
pub const SQL_CLOSE: i32 = 0x210300;

/// Map values with predicate request.
pub const MAP_VALUES_WITH_PREDICATE: i32 = 0x010E00;

/// Map entries with predicate request.
pub const MAP_ENTRIES_WITH_PREDICATE: i32 = 0x010F00;

/// Map key set with predicate request.
pub const MAP_KEYS_WITH_PREDICATE: i32 = 0x011100;

/// Map execute on key request.
pub const MAP_EXECUTE_ON_KEY: i32 = 0x012E00;

/// Map execute on keys request.
pub const MAP_EXECUTE_ON_KEYS: i32 = 0x012F00;

/// Map execute on all keys request.
pub const MAP_EXECUTE_ON_ALL_KEYS: i32 = 0x013000;

/// Map execute on entries with predicate request.
pub const MAP_EXECUTE_WITH_PREDICATE: i32 = 0x013700;

/// Map add index request.
pub const MAP_ADD_INDEX: i32 = 0x013100;

/// Map aggregate request.
pub const MAP_AGGREGATE: i32 = 0x013900;

/// Map aggregate with predicate request.
pub const MAP_AGGREGATE_WITH_PREDICATE: i32 = 0x013A00;

/// Map project request.
pub const MAP_PROJECT: i32 = 0x013B00;

/// Map project with predicate request.
pub const MAP_PROJECT_WITH_PREDICATE: i32 = 0x013C00;

/// Map values with paging predicate request.
pub const MAP_VALUES_WITH_PAGING_PREDICATE: i32 = 0x013D00;

/// Map keys with paging predicate request.
pub const MAP_KEYS_WITH_PAGING_PREDICATE: i32 = 0x013E00;

/// Map entries with paging predicate request.
pub const MAP_ENTRIES_WITH_PAGING_PREDICATE: i32 = 0x013F00;

/// Map try put request.
pub const MAP_TRY_PUT: i32 = 0x010600;

/// Map put transient request.
pub const MAP_PUT_TRANSIENT: i32 = 0x010800;

/// Map evict request.
pub const MAP_EVICT: i32 = 0x011A00;

/// Map evict all request.
pub const MAP_EVICT_ALL: i32 = 0x011B00;

/// Map get entry view request.
pub const MAP_GET_ENTRY_VIEW: i32 = 0x011E00;

/// Map flush request.
pub const MAP_FLUSH: i32 = 0x013200;

/// Map set TTL request.
pub const MAP_SET_TTL: i32 = 0x013500;

/// Executor submit to partition request.
pub const EXECUTOR_SUBMIT_TO_PARTITION: i32 = 0x0E0100;

/// Executor submit to member request.
pub const EXECUTOR_SUBMIT_TO_MEMBER: i32 = 0x0E0200;

/// Executor shutdown request.
pub const EXECUTOR_SHUTDOWN: i32 = 0x0E0300;

/// Executor is shutdown request.
pub const EXECUTOR_IS_SHUTDOWN: i32 = 0x0E0400;

/// Executor cancel on partition request.
pub const EXECUTOR_CANCEL_ON_PARTITION: i32 = 0x0E0500;

/// Executor cancel on member request.
pub const EXECUTOR_CANCEL_ON_MEMBER: i32 = 0x0E0600;

/// Scheduled executor submit to partition request.
pub const SCHEDULED_EXECUTOR_SUBMIT_TO_PARTITION: i32 = 0x1A0100;

/// Scheduled executor submit to member request.
pub const SCHEDULED_EXECUTOR_SUBMIT_TO_MEMBER: i32 = 0x1A0200;

/// Scheduled executor shutdown request.
pub const SCHEDULED_EXECUTOR_SHUTDOWN: i32 = 0x1A0400;

/// Scheduled executor dispose request.
pub const SCHEDULED_EXECUTOR_DISPOSE: i32 = 0x1A0500;

/// Scheduled executor cancel from partition request.
pub const SCHEDULED_EXECUTOR_CANCEL_FROM_PARTITION: i32 = 0x1A0600;

/// Scheduled executor cancel from member request.
pub const SCHEDULED_EXECUTOR_CANCEL_FROM_MEMBER: i32 = 0x1A0700;

/// Scheduled executor is done from partition request.
pub const SCHEDULED_EXECUTOR_IS_DONE_FROM_PARTITION: i32 = 0x1A0800;

/// Scheduled executor is done from member request.
pub const SCHEDULED_EXECUTOR_IS_DONE_FROM_MEMBER: i32 = 0x1A0900;

/// Scheduled executor get delay from partition request.
pub const SCHEDULED_EXECUTOR_GET_DELAY_FROM_PARTITION: i32 = 0x1A0A00;

/// Scheduled executor get delay from member request.
pub const SCHEDULED_EXECUTOR_GET_DELAY_FROM_MEMBER: i32 = 0x1A0B00;

/// Scheduled executor get result from partition request.
pub const SCHEDULED_EXECUTOR_GET_RESULT_FROM_PARTITION: i32 = 0x1A0C00;

/// Scheduled executor is shutdown request.
pub const SCHEDULED_EXECUTOR_IS_SHUTDOWN: i32 = 0x1A0E00;

/// Transaction create request.
pub const TXN_CREATE: i32 = 0x150100;

/// Transaction commit request.
pub const TXN_COMMIT: i32 = 0x150200;

/// Transaction rollback request.
pub const TXN_ROLLBACK: i32 = 0x150300;

/// Transactional map contains key request.
pub const TXN_MAP_CONTAINS_KEY: i32 = 0x0E0100;

/// Transactional map get request.
pub const TXN_MAP_GET: i32 = 0x0E0200;

/// Transactional map put request.
pub const TXN_MAP_PUT: i32 = 0x0E0300;

/// Transactional map set request.
pub const TXN_MAP_SET: i32 = 0x0E0400;

/// Transactional map put if absent request.
pub const TXN_MAP_PUT_IF_ABSENT: i32 = 0x0E0500;

/// Transactional map replace request.
pub const TXN_MAP_REPLACE: i32 = 0x0E0600;

/// Transactional map remove request.
pub const TXN_MAP_REMOVE: i32 = 0x0E0800;

/// Transactional map delete request.
pub const TXN_MAP_DELETE: i32 = 0x0E0900;

/// Transactional map size request.
pub const TXN_MAP_SIZE: i32 = 0x0E0B00;

/// Transactional queue offer request.
pub const TXN_QUEUE_OFFER: i32 = 0x100100;

/// Transactional queue poll request.
pub const TXN_QUEUE_POLL: i32 = 0x100300;

/// Transactional queue size request.
pub const TXN_QUEUE_SIZE: i32 = 0x100500;

/// Transactional set add request.
pub const TXN_SET_ADD: i32 = 0x110100;

/// Transactional set remove request.
pub const TXN_SET_REMOVE: i32 = 0x110200;

/// Transactional set size request.
pub const TXN_SET_SIZE: i32 = 0x110300;

/// Transactional list add request.
pub const TXN_LIST_ADD: i32 = 0x120100;

/// Transactional list remove request.
pub const TXN_LIST_REMOVE: i32 = 0x120200;

/// Transactional list size request.
pub const TXN_LIST_SIZE: i32 = 0x120300;

/// XA transaction create request.
pub const XA_TXN_CREATE: i32 = 0x150400;

/// XA transaction prepare request.
pub const XA_TXN_PREPARE: i32 = 0x150500;

/// XA transaction commit request.
pub const XA_TXN_COMMIT: i32 = 0x150600;

/// XA transaction rollback request.
pub const XA_TXN_ROLLBACK: i32 = 0x150700;

/// XA transaction clear remote request.
pub const XA_TXN_CLEAR_REMOTE: i32 = 0x150800;

/// XA transaction collect transactions request.
pub const XA_TXN_COLLECT_TRANSACTIONS: i32 = 0x150900;

/// XA transaction finalize request.
pub const XA_TXN_FINALIZE: i32 = 0x150A00;

/// Durable executor submit to partition request.
pub const DURABLE_EXECUTOR_SUBMIT_TO_PARTITION: i32 = 0x180100;

/// Durable executor retrieve result request.
pub const DURABLE_EXECUTOR_RETRIEVE_RESULT: i32 = 0x180200;

/// Durable executor dispose result request.
pub const DURABLE_EXECUTOR_DISPOSE_RESULT: i32 = 0x180300;

/// Durable executor retrieve and dispose result request.
pub const DURABLE_EXECUTOR_RETRIEVE_AND_DISPOSE_RESULT: i32 = 0x180400;

/// Durable executor shutdown request.
pub const DURABLE_EXECUTOR_SHUTDOWN: i32 = 0x180500;

/// Durable executor is shutdown request.
pub const DURABLE_EXECUTOR_IS_SHUTDOWN: i32 = 0x180600;

/// CardinalityEstimator add request.
pub const CARDINALITY_ESTIMATOR_ADD: i32 = 0x160100;

/// CardinalityEstimator estimate request.
pub const CARDINALITY_ESTIMATOR_ESTIMATE: i32 = 0x160200;

/// Map add interceptor request.
pub const MAP_ADD_INTERCEPTOR: i32 = 0x013300;

/// Map remove interceptor request.
pub const MAP_REMOVE_INTERCEPTOR: i32 = 0x013400;

/// Map load all keys request.
pub const MAP_LOAD_ALL: i32 = 0x012000;

/// Map load given keys request.
pub const MAP_LOAD_GIVEN_KEYS: i32 = 0x012100;

/// Map add partition lost listener request.
pub const MAP_ADD_PARTITION_LOST_LISTENER: i32 = 0x012200;

/// Map remove partition lost listener request.
pub const MAP_REMOVE_PARTITION_LOST_LISTENER: i32 = 0x012300;

/// Cache get request.
pub const CACHE_GET: i32 = 0x130100;

/// Cache get all request.
pub const CACHE_GET_ALL: i32 = 0x130200;

/// Cache put request.
pub const CACHE_PUT: i32 = 0x130300;

/// Cache put all request.
pub const CACHE_PUT_ALL: i32 = 0x130400;

/// Cache put if absent request.
pub const CACHE_PUT_IF_ABSENT: i32 = 0x130500;

/// Cache remove request.
pub const CACHE_REMOVE: i32 = 0x130600;

/// Cache remove all request.
pub const CACHE_REMOVE_ALL: i32 = 0x130700;

/// Cache clear request.
pub const CACHE_CLEAR: i32 = 0x130800;

/// Cache replace request.
pub const CACHE_REPLACE: i32 = 0x130900;

/// Cache replace if same request.
pub const CACHE_REPLACE_IF_SAME: i32 = 0x130A00;

/// Cache contains key request.
pub const CACHE_CONTAINS_KEY: i32 = 0x130B00;

/// Cache get and put request.
pub const CACHE_GET_AND_PUT: i32 = 0x130C00;

/// Cache get and remove request.
pub const CACHE_GET_AND_REMOVE: i32 = 0x130D00;

/// Cache get and replace request.
pub const CACHE_GET_AND_REPLACE: i32 = 0x130E00;

/// Cache size request.
pub const CACHE_SIZE: i32 = 0x130F00;

/// Cache event journal subscribe request.
pub const CACHE_EVENT_JOURNAL_SUBSCRIBE: i32 = 0x131500;

/// Cache event journal read request.
pub const CACHE_EVENT_JOURNAL_READ: i32 = 0x131600;

/// Cache add entry listener request.
pub const CACHE_ADD_ENTRY_LISTENER: i32 = 0x131700;

/// Cache remove entry listener request.
pub const CACHE_REMOVE_ENTRY_LISTENER: i32 = 0x131800;

// Jet operations

/// Jet submit job request.
pub const JET_SUBMIT_JOB: i32 = 0xFE0100;

/// Jet terminate job request.
pub const JET_TERMINATE_JOB: i32 = 0xFE0200;

/// Jet get job status request.
pub const JET_GET_JOB_STATUS: i32 = 0xFE0300;

/// Jet get job IDs request.
pub const JET_GET_JOB_IDS: i32 = 0xFE0400;

/// Jet get job submission time request.
pub const JET_GET_JOB_SUBMISSION_TIME: i32 = 0xFE0500;

/// Jet get job config request.
pub const JET_GET_JOB_CONFIG: i32 = 0xFE0600;

/// Jet resume job request.
pub const JET_RESUME_JOB: i32 = 0xFE0700;

/// Jet export snapshot request.
pub const JET_EXPORT_SNAPSHOT: i32 = 0xFE0800;

/// Jet get job metrics request.
pub const JET_GET_JOB_METRICS: i32 = 0xFE0900;

/// Jet get job summary list request.
pub const JET_GET_JOB_SUMMARY_LIST: i32 = 0xFE0A00;

/// Map event journal subscribe request.
pub const MAP_EVENT_JOURNAL_SUBSCRIBE: i32 = 0x014100;

/// Map event journal read request.
pub const MAP_EVENT_JOURNAL_READ: i32 = 0x014200;

/// Map fetch keys request (for iteration).
pub const MAP_FETCH_KEYS: i32 = 0x014300;

/// Map fetch entries request (for iteration).
pub const MAP_FETCH_ENTRIES: i32 = 0x014400;

/// Map fetch values request (for iteration).
pub const MAP_FETCH_VALUES: i32 = 0x014500;

/// Client statistics request.
pub const CLIENT_STATISTICS: i32 = 0x000C00;

// CP Subsystem Management operations

/// CP Subsystem get group IDs request.
pub const CP_SUBSYSTEM_GET_GROUP_IDS: i32 = 0x1E0100;

/// CP Subsystem get group request.
pub const CP_SUBSYSTEM_GET_GROUP: i32 = 0x1E0200;

/// CP Subsystem force destroy group request.
pub const CP_SUBSYSTEM_FORCE_DESTROY_GROUP: i32 = 0x1E0300;

/// CP Subsystem get CP members request.
pub const CP_SUBSYSTEM_GET_CP_MEMBERS: i32 = 0x1E0400;

/// CP Subsystem promote to CP member request.
pub const CP_SUBSYSTEM_PROMOTE_TO_CP_MEMBER: i32 = 0x1E0500;

/// CP Subsystem remove CP member request.
pub const CP_SUBSYSTEM_REMOVE_CP_MEMBER: i32 = 0x1E0600;
