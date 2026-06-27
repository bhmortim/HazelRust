//! ClientMessage type for multi-frame Hazelcast protocol messages.

use bytes::{BufMut, BytesMut};
use std::sync::atomic::{AtomicI64, Ordering};

use super::constants::*;
use super::frame::Frame;

/// Global correlation ID counter.
static CORRELATION_ID_COUNTER: AtomicI64 = AtomicI64::new(1);

/// Generates a unique correlation ID for a request.
pub fn next_correlation_id() -> i64 {
    CORRELATION_ID_COUNTER.fetch_add(1, Ordering::Relaxed)
}

/// A client message composed of one or more frames.
///
/// The first frame is the "initial frame" containing the message header
/// (type, correlation ID, partition ID for requests). Additional frames
/// contain the message payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientMessage {
    frames: Vec<Frame>,
}

impl ClientMessage {
    /// Creates a new empty client message.
    pub fn new() -> Self {
        Self { frames: Vec::new() }
    }

    /// Creates a new request message with the given message type, targeting any partition.
    pub fn new_request(message_type: i32) -> Self {
        Self::create_for_encode(message_type, PARTITION_ID_ANY)
    }

    /// Creates a request message with the given type and partition ID.
    pub fn create_for_encode(message_type: i32, partition_id: i32) -> Self {
        let mut initial_frame = Frame::with_capacity(REQUEST_HEADER_SIZE, 0xC000); // UNFRAGMENTED (BEGIN|END) matching Java data ops
        initial_frame.content.put_i32_le(message_type);
        initial_frame.content.put_i64_le(next_correlation_id());
        initial_frame.content.put_i32_le(partition_id);

        Self {
            frames: vec![initial_frame],
        }
    }

    /// Creates a request message targeting any partition.
    pub fn create_for_encode_any_partition(message_type: i32) -> Self {
        Self::create_for_encode(message_type, PARTITION_ID_ANY)
    }

    /// Creates a request message with a pre-allocated initial frame of the given capacity.
    ///
    /// The message type and partition ID should be set separately via
    /// `set_message_type` and `set_partition_id`.
    pub fn create_for_encode_with_capacity(capacity: usize) -> Self {
        let initial_frame = Frame::with_capacity(capacity, BEGIN_FLAG);
        Self {
            frames: vec![initial_frame],
        }
    }

    /// Sets the message type in the initial frame.
    pub fn set_message_type(&mut self, message_type: i32) {
        if let Some(frame) = self.frames.first_mut() {
            // Ensure the frame content has enough space
            while frame.content.len() < TYPE_FIELD_OFFSET + 4 {
                frame.content.put_u8(0);
            }
            let bytes = message_type.to_le_bytes();
            frame.content[TYPE_FIELD_OFFSET..TYPE_FIELD_OFFSET + 4].copy_from_slice(&bytes);
        }
    }

    /// Finalizes the message by setting the END flag on the last frame.
    pub fn finalize(&mut self) {
        if let Some(last) = self.frames.last_mut() {
            last.flags |= END_FLAG;
        }
    }

    /// Creates a client message from received frames.
    pub fn from_frames(frames: Vec<Frame>) -> Self {
        Self { frames }
    }

    /// Returns the message type from the initial frame.
    ///
    /// Returns `None` if there is no initial frame or if the frame content
    /// is too short to contain a message type field.
    pub fn message_type(&self) -> Option<i32> {
        self.frames.first().and_then(|f| {
            if f.content.len() >= TYPE_FIELD_OFFSET + 4 {
                Some(i32::from_le_bytes([
                    f.content[TYPE_FIELD_OFFSET],
                    f.content[TYPE_FIELD_OFFSET + 1],
                    f.content[TYPE_FIELD_OFFSET + 2],
                    f.content[TYPE_FIELD_OFFSET + 3],
                ]))
            } else {
                None
            }
        })
    }

    /// Returns the correlation ID from the initial frame.
    ///
    /// Returns `None` if there is no initial frame or if the frame content
    /// is too short to contain a correlation ID field.
    pub fn correlation_id(&self) -> Option<i64> {
        self.frames.first().and_then(|f| {
            if f.content.len() >= CORRELATION_ID_OFFSET + 8 {
                Some(i64::from_le_bytes([
                    f.content[CORRELATION_ID_OFFSET],
                    f.content[CORRELATION_ID_OFFSET + 1],
                    f.content[CORRELATION_ID_OFFSET + 2],
                    f.content[CORRELATION_ID_OFFSET + 3],
                    f.content[CORRELATION_ID_OFFSET + 4],
                    f.content[CORRELATION_ID_OFFSET + 5],
                    f.content[CORRELATION_ID_OFFSET + 6],
                    f.content[CORRELATION_ID_OFFSET + 7],
                ]))
            } else {
                None
            }
        })
    }

    /// Sets the correlation ID in the initial frame.
    pub fn set_correlation_id(&mut self, correlation_id: i64) {
        if let Some(frame) = self.frames.first_mut() {
            if frame.content.len() >= CORRELATION_ID_OFFSET + 8 {
                let bytes = correlation_id.to_le_bytes();
                frame.content[CORRELATION_ID_OFFSET..CORRELATION_ID_OFFSET + 8]
                    .copy_from_slice(&bytes);
            }
        }
    }

    /// Returns the partition ID from the initial frame (for requests).
    ///
    /// Returns `None` if there is no initial frame or if the frame content
    /// is too short to contain a partition ID field.
    pub fn partition_id(&self) -> Option<i32> {
        self.frames.first().and_then(|f| {
            if f.content.len() >= PARTITION_ID_OFFSET + 4 {
                Some(i32::from_le_bytes([
                    f.content[PARTITION_ID_OFFSET],
                    f.content[PARTITION_ID_OFFSET + 1],
                    f.content[PARTITION_ID_OFFSET + 2],
                    f.content[PARTITION_ID_OFFSET + 3],
                ]))
            } else {
                None
            }
        })
    }

    /// Sets the partition ID in the initial frame.
    pub fn set_partition_id(&mut self, partition_id: i32) {
        if let Some(frame) = self.frames.first_mut() {
            if frame.content.len() >= PARTITION_ID_OFFSET + 4 {
                let bytes = partition_id.to_le_bytes();
                frame.content[PARTITION_ID_OFFSET..PARTITION_ID_OFFSET + 4].copy_from_slice(&bytes);
            }
        }
    }

    /// Adds a frame to the message.
    pub fn add_frame(&mut self, frame: Frame) {
        self.frames.push(frame);
    }

    /// Adds a frame containing the given data bytes.
    pub fn add_frame_with_data(&mut self, data: &[u8]) {
        self.frames.push(Frame::new_data_frame(data));
    }

    /// Returns a reference to the initial (first) frame, if present.
    pub fn initial_frame(&self) -> Option<&Frame> {
        self.frames.first()
    }

    /// Returns a reference to all frames.
    pub fn frames(&self) -> &[Frame] {
        &self.frames
    }

    /// Returns a mutable reference to all frames.
    pub fn frames_mut(&mut self) -> &mut Vec<Frame> {
        &mut self.frames
    }

    /// Returns the number of frames in the message.
    pub fn frame_count(&self) -> usize {
        self.frames.len()
    }

    /// Returns true if the message has no frames.
    pub fn is_empty(&self) -> bool {
        self.frames.is_empty()
    }

    /// Calculates the total size of the message on the wire.
    pub fn wire_size(&self) -> usize {
        self.frames.iter().map(|f| f.wire_size()).sum()
    }

    /// Writes all frames to the destination buffer.
    ///
    /// Sets the END flag on the last frame before writing.
    pub fn write_to(&mut self, dst: &mut BytesMut) {
        // Add IS_FINAL_FLAG to the last frame to signal end of message.
        // The Java server's ClientMessageReader reads frames until it
        // sees IS_FINAL_FLAG (1 << 13 = 0x2000), then processes the message.
        if let Some(last) = self.frames.last_mut() {
            last.flags |= IS_FINAL_FLAG;
        }
        for frame in &self.frames {
            frame.write_to(dst);
        }
    }

    /// Consumes the message into `(headers, frames)` for a zero-copy vectored
    /// write. Sets `IS_FINAL_FLAG` on the last frame exactly like [`write_to`].
    /// `headers[i*FRAME_HEADER_SIZE .. (i+1)*FRAME_HEADER_SIZE]` is frame `i`'s
    /// on-wire `[length_le(4), flags_le(2)]` header; the frame *contents* are
    /// returned untouched so the writer can reference them in place via
    /// `IoSlice` (avoiding the per-frame `put_slice` copy that `write_to` does).
    /// Concatenating `headers[i]` followed by `frames[i].content` for every `i`
    /// yields bytes identical to `write_to`.
    pub fn into_segments(mut self) -> (BytesMut, Vec<Frame>) {
        if let Some(last) = self.frames.last_mut() {
            last.flags |= IS_FINAL_FLAG;
        }
        let mut headers = BytesMut::with_capacity(self.frames.len() * FRAME_HEADER_SIZE);
        for f in &self.frames {
            headers.put_u32_le((FRAME_HEADER_SIZE + f.content.len()) as u32);
            headers.put_u16_le(f.flags);
        }
        (headers, self.frames)
    }

    /// Returns true if this message is a request (has partition ID field).
    pub fn is_request(&self) -> bool {
        self.frames
            .first()
            .map(|f| f.content.len() >= REQUEST_HEADER_SIZE)
            .unwrap_or(false)
    }

    /// Returns true if this message is flagged as an event.
    pub fn is_event(&self) -> bool {
        self.frames
            .first()
            .map(|f| f.is_event_frame())
            .unwrap_or(false)
    }

    /// Returns true if this message is a backup acknowledgement (sent by a
    /// backup member directly to the client under backup-ack-to-client).
    pub fn is_backup_event(&self) -> bool {
        self.frames
            .first()
            .map(|f| f.flags & BACKUP_EVENT_FLAG != 0)
            .unwrap_or(false)
    }

    /// Marks this request as backup-aware (opts into backup-ack-to-client).
    /// Sets [`BACKUP_AWARE_FLAG`] on the initial frame.
    pub fn set_backup_aware(&mut self) {
        if let Some(f) = self.frames.first_mut() {
            f.flags |= BACKUP_AWARE_FLAG;
        }
    }

    /// Number of backup acknowledgements the client should wait for, read from
    /// the response initial frame's `backupAcks` byte (offset 12). Returns 0 if
    /// absent (every read/non-backup-aware response).
    pub fn backup_ack_count(&self) -> u8 {
        self.frames
            .first()
            .and_then(|f| f.content.get(RESPONSE_BACKUP_ACKS_OFFSET).copied())
            .unwrap_or(0)
    }
}

impl Default for ClientMessage {
    fn default() -> Self {
        Self::new()
    }
}

/// Computes a partition hash for the given key data.
///
/// Uses MurmurHash3 algorithm compatible with Hazelcast.
pub fn compute_partition_hash(key: &[u8]) -> i32 {
    murmur_hash3_x86_32(key, 0x01000193)
}

/// Maps a partition hash to a partition id in `[0, partition_count)`, matching
/// Hazelcast's `HashUtil.hashToIndex`: `Integer.MIN_VALUE` maps to `0` (because
/// `Math.abs(MIN_VALUE)` overflows), otherwise `abs(hash) % partition_count`.
///
/// Using `i32::abs` directly would **panic in debug builds** (and wrap to a
/// negative value in release) when `hash == i32::MIN`, yielding a negative or
/// out-of-range partition id on the wire — this helper makes that impossible.
pub fn partition_id_for_hash(hash: i32, partition_count: i32) -> i32 {
    if partition_count <= 0 {
        return 0;
    }
    if hash == i32::MIN {
        0
    } else {
        hash.abs() % partition_count
    }
}

/// Computes the Hazelcast partition id for a serialized key `Data`.
///
/// A serialized key on the wire is `[partition_hash:i32][type_id:i32][payload]`;
/// Java's `HeapData.getPartitionHash()` hashes the bytes **after** that 8-byte
/// header. This skips the header, MurmurHash3-es the body, and maps the result
/// into `[0, partition_count)` via [`partition_id_for_hash`]. Centralizing this
/// keeps the IMap key path, the affinity (`*_with_partition_key`) path, and the
/// public `PartitionService::get_partition` API from drifting apart.
pub fn partition_id_for_key_data(key_data: &[u8], partition_count: i32) -> i32 {
    if partition_count <= 0 {
        return 0;
    }
    let body = if key_data.len() > 8 {
        &key_data[8..]
    } else {
        key_data
    };
    partition_id_for_hash(compute_partition_hash(body), partition_count)
}

/// MurmurHash3 x86 32-bit implementation.
fn murmur_hash3_x86_32(data: &[u8], seed: u32) -> i32 {
    const C1: u32 = 0xcc9e2d51;
    const C2: u32 = 0x1b873593;

    let len = data.len();
    let mut h1 = seed;
    let nblocks = len / 4;

    for i in 0..nblocks {
        let offset = i * 4;
        let k1 = u32::from_le_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]);

        let k1 = k1.wrapping_mul(C1);
        let k1 = k1.rotate_left(15);
        let k1 = k1.wrapping_mul(C2);

        h1 ^= k1;
        h1 = h1.rotate_left(13);
        h1 = h1.wrapping_mul(5).wrapping_add(0xe6546b64);
    }

    let tail = &data[nblocks * 4..];
    let mut k1: u32 = 0;

    match tail.len() {
        3 => {
            k1 ^= (tail[2] as u32) << 16;
            k1 ^= (tail[1] as u32) << 8;
            k1 ^= tail[0] as u32;
            k1 = k1.wrapping_mul(C1);
            k1 = k1.rotate_left(15);
            k1 = k1.wrapping_mul(C2);
            h1 ^= k1;
        }
        2 => {
            k1 ^= (tail[1] as u32) << 8;
            k1 ^= tail[0] as u32;
            k1 = k1.wrapping_mul(C1);
            k1 = k1.rotate_left(15);
            k1 = k1.wrapping_mul(C2);
            h1 ^= k1;
        }
        1 => {
            k1 ^= tail[0] as u32;
            k1 = k1.wrapping_mul(C1);
            k1 = k1.rotate_left(15);
            k1 = k1.wrapping_mul(C2);
            h1 ^= k1;
        }
        _ => {}
    }

    h1 ^= len as u32;
    h1 ^= h1 >> 16;
    h1 = h1.wrapping_mul(0x85ebca6b);
    h1 ^= h1 >> 13;
    h1 = h1.wrapping_mul(0xc2b2ae35);
    h1 ^= h1 >> 16;

    h1 as i32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_request_message() {
        let msg = ClientMessage::create_for_encode(CLIENT_AUTHENTICATION, PARTITION_ID_ANY);

        assert_eq!(msg.message_type(), Some(CLIENT_AUTHENTICATION));
        assert!(msg.correlation_id().unwrap() > 0);
        assert_eq!(msg.partition_id(), Some(PARTITION_ID_ANY));
        assert_eq!(msg.frame_count(), 1);
    }

    #[test]
    fn test_correlation_id_increments() {
        let msg1 = ClientMessage::create_for_encode(MAP_GET, 0);
        let msg2 = ClientMessage::create_for_encode(MAP_GET, 0);

        let id1 = msg1.correlation_id().unwrap();
        let id2 = msg2.correlation_id().unwrap();
        assert!(id2 > id1);
    }

    #[test]
    fn test_set_correlation_id() {
        let mut msg = ClientMessage::create_for_encode(MAP_GET, 0);
        msg.set_correlation_id(42);
        assert_eq!(msg.correlation_id(), Some(42));
    }

    #[test]
    fn test_set_partition_id() {
        let mut msg = ClientMessage::create_for_encode(MAP_GET, 0);
        msg.set_partition_id(123);
        assert_eq!(msg.partition_id(), Some(123));
    }

    #[test]
    fn test_add_frame() {
        let mut msg = ClientMessage::create_for_encode(MAP_PUT, 0);
        let key_frame = Frame::with_content(BytesMut::from(&b"key"[..]));
        let value_frame = Frame::with_content(BytesMut::from(&b"value"[..]));

        msg.add_frame(key_frame);
        msg.add_frame(value_frame);

        assert_eq!(msg.frame_count(), 3);
    }

    #[test]
    fn test_wire_size() {
        let mut msg = ClientMessage::create_for_encode(MAP_GET, 0);
        let initial_size = msg.wire_size();
        assert!(initial_size > 0);

        msg.add_frame(Frame::with_content(BytesMut::from(&[1, 2, 3, 4][..])));
        assert!(msg.wire_size() > initial_size);
    }

    #[test]
    fn test_write_to_sets_final_flag() {
        let mut msg = ClientMessage::create_for_encode(MAP_GET, 0);
        msg.add_frame(Frame::with_content(BytesMut::from(&b"data"[..])));

        let mut buf = BytesMut::new();
        msg.write_to(&mut buf);

        // `write_to` marks the last frame of a message with IS_FINAL_FLAG (the
        // protocol's end-of-message marker), which is distinct from END_FLAG
        // (end of a nested data structure). The previous assertion checked
        // `is_end_frame()` (END_FLAG) and so never matched what `write_to` does.
        assert!(msg.frames().last().unwrap().is_final_frame());
    }

    #[test]
    fn test_from_frames() {
        let frames = vec![
            Frame::with_flags(BEGIN_FLAG),
            Frame::default(),
            Frame::with_flags(END_FLAG),
        ];

        let msg = ClientMessage::from_frames(frames);
        assert_eq!(msg.frame_count(), 3);
    }

    #[test]
    fn test_is_request() {
        let request = ClientMessage::create_for_encode(MAP_GET, 0);
        assert!(request.is_request());

        let empty = ClientMessage::new();
        assert!(!empty.is_request());
    }

    #[test]
    fn test_partition_hash_deterministic() {
        let key = b"test-key";
        let hash1 = compute_partition_hash(key);
        let hash2 = compute_partition_hash(key);
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_partition_hash_different_keys() {
        let hash1 = compute_partition_hash(b"key1");
        let hash2 = compute_partition_hash(b"key2");
        assert_ne!(hash1, hash2);
    }

    #[test]
    fn test_murmur_hash_empty() {
        let hash = compute_partition_hash(b"");
        assert_ne!(hash, 0);
    }

    #[test]
    fn test_murmur_hash_various_lengths() {
        compute_partition_hash(b"a");
        compute_partition_hash(b"ab");
        compute_partition_hash(b"abc");
        compute_partition_hash(b"abcd");
        compute_partition_hash(b"abcde");
    }

    #[test]
    fn test_default_message() {
        let msg = ClientMessage::default();
        assert!(msg.is_empty());
        assert_eq!(msg.frame_count(), 0);
    }

    #[test]
    fn test_frames_mut() {
        let mut msg = ClientMessage::create_for_encode(MAP_GET, 0);
        let initial_count = msg.frame_count();

        msg.frames_mut()
            .push(Frame::with_content(BytesMut::from(&b"extra"[..])));

        assert_eq!(msg.frame_count(), initial_count + 1);
    }

    #[test]
    fn test_message_type_with_short_content() {
        let short_frame = Frame::with_content(BytesMut::from(&[0x01, 0x02][..]));
        let msg = ClientMessage::from_frames(vec![short_frame]);

        // Short content should return None instead of silently defaulting
        assert_eq!(msg.message_type(), None);
    }

    #[test]
    fn test_correlation_id_with_short_content() {
        let short_frame = Frame::with_content(BytesMut::from(&[0x01, 0x02, 0x03, 0x04][..]));
        let msg = ClientMessage::from_frames(vec![short_frame]);

        // Short content should return None instead of silently defaulting
        assert_eq!(msg.correlation_id(), None);
    }

    #[test]
    fn test_partition_id_with_short_content() {
        let short_frame = Frame::with_content(BytesMut::from(&[0x01; 10][..]));
        let msg = ClientMessage::from_frames(vec![short_frame]);

        // Short content should return None instead of silently defaulting
        assert_eq!(msg.partition_id(), None);
    }

    #[test]
    fn test_set_correlation_id_with_short_content() {
        let short_frame = Frame::with_content(BytesMut::from(&[0x01, 0x02][..]));
        let mut msg = ClientMessage::from_frames(vec![short_frame]);

        // Setting on a short frame is a no-op; reading back returns None
        msg.set_correlation_id(999);
        assert_eq!(msg.correlation_id(), None);
    }

    #[test]
    fn test_set_partition_id_with_short_content() {
        let short_frame = Frame::with_content(BytesMut::from(&[0x01; 10][..]));
        let mut msg = ClientMessage::from_frames(vec![short_frame]);

        // Setting on a short frame is a no-op; reading back returns None
        msg.set_partition_id(42);
        assert_eq!(msg.partition_id(), None);
    }

    #[test]
    fn test_is_event() {
        let event_frame = Frame::with_flags(IS_EVENT_FLAG | BEGIN_FLAG);
        let msg = ClientMessage::from_frames(vec![event_frame]);

        assert!(msg.is_event());
    }

    #[test]
    fn test_is_not_event() {
        let normal_frame = Frame::with_flags(BEGIN_FLAG);
        let msg = ClientMessage::from_frames(vec![normal_frame]);

        assert!(!msg.is_event());
    }

    #[test]
    fn test_is_event_empty_message() {
        let msg = ClientMessage::new();
        assert!(!msg.is_event());
    }

    #[test]
    fn test_message_type_empty_message() {
        let msg = ClientMessage::new();
        assert!(msg.message_type().is_none());
    }

    #[test]
    fn test_correlation_id_empty_message() {
        let msg = ClientMessage::new();
        assert!(msg.correlation_id().is_none());
    }

    #[test]
    fn test_partition_id_empty_message() {
        let msg = ClientMessage::new();
        assert!(msg.partition_id().is_none());
    }

    #[test]
    fn test_wire_size_empty_message() {
        let msg = ClientMessage::new();
        assert_eq!(msg.wire_size(), 0);
    }

    #[test]
    fn test_write_to_empty_message() {
        let mut msg = ClientMessage::new();
        let mut buf = BytesMut::new();

        msg.write_to(&mut buf);

        assert!(buf.is_empty());
    }

    #[test]
    fn test_murmur_hash_alignment_edge_cases() {
        assert_ne!(compute_partition_hash(b"1234"), 0);
        assert_ne!(compute_partition_hash(b"12345"), 0);
        assert_ne!(compute_partition_hash(b"123456"), 0);
        assert_ne!(compute_partition_hash(b"1234567"), 0);
        assert_ne!(compute_partition_hash(b"12345678"), 0);
    }

    #[test]
    fn test_create_for_encode_any_partition() {
        let msg = ClientMessage::create_for_encode_any_partition(MAP_GET);

        assert_eq!(msg.message_type(), Some(MAP_GET));
        assert_eq!(msg.partition_id(), Some(PARTITION_ID_ANY));
    }

    #[test]
    fn test_into_segments_matches_write_to_byte_for_byte() {
        // Build a representative multi-frame request (header + name + key + value),
        // like IMap.put, with a large value to exercise the zero-copy path.
        // Build ONE message (create_for_encode auto-increments the global
        // correlation id, so the two encodings must come from the same instance).
        let mut msg = ClientMessage::create_for_encode(0x0102, 7);
        msg.add_frame(Frame::with_content(BytesMut::from(&b"bench-map"[..])));
        msg.add_frame(Frame::with_content(BytesMut::from(
            &[0u8, 1, 2, 3, 4, 5, 6, 7][..],
        )));
        msg.add_frame(Frame::with_content(BytesMut::from(
            &vec![0xABu8; 16384][..],
        )));

        // Reference: the existing concatenating encoder (on a clone).
        let mut reference = BytesMut::new();
        msg.clone().write_to(&mut reference);

        // Zero-copy: headers + frame contents, reassembled in order, must match.
        let (headers, frames) = msg.into_segments();
        let mut assembled = BytesMut::new();
        for (i, f) in frames.iter().enumerate() {
            let h = i * FRAME_HEADER_SIZE;
            assembled.extend_from_slice(&headers[h..h + FRAME_HEADER_SIZE]);
            assembled.extend_from_slice(&f.content);
        }
        assert_eq!(
            &assembled[..],
            &reference[..],
            "into_segments wire bytes differ from write_to"
        );

        // Single-frame message (e.g. a poll: just the header frame).
        let single = ClientMessage::create_for_encode(0x0103, 1);
        let mut r2 = BytesMut::new();
        single.clone().write_to(&mut r2);
        let (h2, f2) = single.into_segments();
        let mut a2 = BytesMut::new();
        for (i, f) in f2.iter().enumerate() {
            let h = i * FRAME_HEADER_SIZE;
            a2.extend_from_slice(&h2[h..h + FRAME_HEADER_SIZE]);
            a2.extend_from_slice(&f.content);
        }
        assert_eq!(&a2[..], &r2[..]);
    }
}
