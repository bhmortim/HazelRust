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
