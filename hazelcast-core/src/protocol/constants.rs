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

/// Map contains key request.
pub const MAP_CONTAINS_KEY: i32 = 0x010900;

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

/// Map add entry listener request.
pub const MAP_ADD_ENTRY_LISTENER: i32 = 0x011400;

/// Map remove entry listener request.
pub const MAP_REMOVE_ENTRY_LISTENER: i32 = 0x011500;

/// Client add membership listener request.
pub const CLIENT_ADD_MEMBERSHIP_LISTENER: i32 = 0x000300;

/// Client remove membership listener request.
pub const CLIENT_REMOVE_MEMBERSHIP_LISTENER: i32 = 0x000301;

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
