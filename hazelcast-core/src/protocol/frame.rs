//! Frame type for the Hazelcast Open Binary Protocol.

use bytes::{Buf, BufMut, BytesMut};

use super::constants::*;

/// A single frame in the Hazelcast protocol.
///
/// Each frame consists of:
/// - A 4-byte length field (little-endian)
/// - A 2-byte flags field (little-endian)
/// - Variable-length content
#[derive(Debug, Clone)]
pub struct Frame {
    /// The frame content (payload after flags).
    pub content: BytesMut,
    /// Frame flags indicating frame type and properties.
    pub flags: u16,
}

impl Frame {
    /// Creates a new frame with the given content and flags.
    pub fn new(content: BytesMut, flags: u16) -> Self {
        Self { content, flags }
    }

    /// Creates a new frame with content and default flags.
    pub fn with_content(content: BytesMut) -> Self {
        Self::new(content, DEFAULT_FLAGS)
    }

    /// Creates a new empty frame with the given flags.
    pub fn with_flags(flags: u16) -> Self {
        Self::new(BytesMut::new(), flags)
    }

    /// Creates a new frame with the given capacity and flags.
    pub fn with_capacity(capacity: usize, flags: u16) -> Self {
        Self::new(BytesMut::with_capacity(capacity), flags)
    }

    /// Creates a begin frame (marks start of a client message).
    pub fn new_begin_frame(content: BytesMut) -> Self {
        Self::new(content, BEGIN_FLAG)
    }

    /// Creates an end frame (marks end of a client message).
    pub fn new_end_frame() -> Self {
        Self::with_flags(END_FLAG)
    }

    /// Creates a null frame (represents a null value).
    pub fn new_null_frame() -> Self {
        Self::with_flags(IS_NULL_FLAG)
    }

    /// Returns true if this frame has the BEGIN flag set.
    pub fn is_begin_frame(&self) -> bool {
        self.flags & BEGIN_FLAG != 0
    }

    /// Returns true if this frame has the END flag set.
    pub fn is_end_frame(&self) -> bool {
        self.flags & END_FLAG != 0
    }

    /// Returns true if this frame has the NULL flag set.
    pub fn is_null_frame(&self) -> bool {
        self.flags & IS_NULL_FLAG != 0
    }

    /// Returns true if this frame has the FINAL flag set.
    pub fn is_final_frame(&self) -> bool {
        self.flags & IS_FINAL_FLAG != 0
    }

    /// Returns true if this frame has the EVENT flag set.
    pub fn is_event_frame(&self) -> bool {
        self.flags & IS_EVENT_FLAG != 0
    }

    /// Returns true if this frame has the BACKUP_EVENT flag set.
    pub fn is_backup_event_frame(&self) -> bool {
        self.flags & BACKUP_EVENT_FLAG != 0
    }

    /// Returns the size of this frame on the wire.
    ///
    /// This includes the 4-byte length field, 2-byte flags, and content.
    pub fn wire_size(&self) -> usize {
        SIZE_OF_FRAME_LENGTH_FIELD + SIZE_OF_FRAME_FLAGS_FIELD + self.content.len()
    }

    /// Returns the frame length value (flags + content length).
    ///
    /// This is the value written in the length field.
    pub fn frame_length(&self) -> usize {
        SIZE_OF_FRAME_FLAGS_FIELD + self.content.len()
    }

    /// Writes this frame to the given buffer.
    pub fn write_to(&self, dst: &mut BytesMut) {
        dst.reserve(self.wire_size());
        dst.put_u32_le(self.frame_length() as u32);
        dst.put_u16_le(self.flags);
        dst.put_slice(&self.content);
    }

    /// Reads a frame from the given buffer.
    ///
    /// Returns `None` if there isn't enough data to read a complete frame.
    pub fn read_from(src: &mut BytesMut) -> Option<Self> {
        if src.len() < SIZE_OF_FRAME_LENGTH_FIELD {
            return None;
        }

        let frame_length = u32::from_le_bytes([src[0], src[1], src[2], src[3]]) as usize;
        let total_frame_size = SIZE_OF_FRAME_LENGTH_FIELD + frame_length;

        if src.len() < total_frame_size {
            return None;
        }

        src.advance(SIZE_OF_FRAME_LENGTH_FIELD);
        let flags = src.get_u16_le();
        let content_length = frame_length - SIZE_OF_FRAME_FLAGS_FIELD;
        let content = src.split_to(content_length);

        Some(Self::new(content, flags))
    }
}

impl Default for Frame {
    fn default() -> Self {
        Self::with_flags(DEFAULT_FLAGS)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_frame() {
        let content = BytesMut::from(&[1, 2, 3][..]);
        let frame = Frame::new(content.clone(), BEGIN_FLAG);
        assert_eq!(frame.content, content);
        assert_eq!(frame.flags, BEGIN_FLAG);
    }

    #[test]
    fn test_frame_flags() {
        let begin = Frame::with_flags(BEGIN_FLAG);
        assert!(begin.is_begin_frame());
        assert!(!begin.is_end_frame());

        let end = Frame::new_end_frame();
        assert!(!end.is_begin_frame());
        assert!(end.is_end_frame());

        let null = Frame::new_null_frame();
        assert!(null.is_null_frame());

        let final_frame = Frame::with_flags(IS_FINAL_FLAG);
        assert!(final_frame.is_final_frame());

        let event = Frame::with_flags(IS_EVENT_FLAG);
        assert!(event.is_event_frame());

        let backup = Frame::with_flags(BACKUP_EVENT_FLAG);
        assert!(backup.is_backup_event_frame());
    }

    #[test]
    fn test_wire_size() {
        let empty = Frame::default();
        assert_eq!(empty.wire_size(), 6);

        let with_content = Frame::with_content(BytesMut::from(&[1, 2, 3, 4, 5][..]));
        assert_eq!(with_content.wire_size(), 11);
    }

    #[test]
    fn test_frame_length() {
        let empty = Frame::default();
        assert_eq!(empty.frame_length(), 2);

        let with_content = Frame::with_content(BytesMut::from(&[1, 2, 3][..]));
        assert_eq!(with_content.frame_length(), 5);
    }

    #[test]
    fn test_write_and_read_frame() {
        let original = Frame::new(BytesMut::from(&[0xDE, 0xAD, 0xBE, 0xEF][..]), BEGIN_FLAG);
        let mut buf = BytesMut::new();
        original.write_to(&mut buf);

        assert_eq!(buf.len(), original.wire_size());

        let decoded = Frame::read_from(&mut buf).unwrap();
        assert_eq!(decoded.flags, original.flags);
        assert_eq!(decoded.content, original.content);
        assert!(buf.is_empty());
    }

    #[test]
    fn test_read_incomplete_length() {
        let mut buf = BytesMut::from(&[0x01, 0x02][..]);
        assert!(Frame::read_from(&mut buf).is_none());
        assert_eq!(buf.len(), 2);
    }

    #[test]
    fn test_read_incomplete_content() {
        let mut buf = BytesMut::from(&[
            0x06, 0x00, 0x00, 0x00, // length = 6 (flags + 4 bytes content)
            0x00, 0x80, // flags
            0x01, 0x02, // only 2 bytes of content
        ][..]);
        assert!(Frame::read_from(&mut buf).is_none());
    }

    #[test]
    fn test_read_empty_frame() {
        let mut buf = BytesMut::from(&[
            0x02, 0x00, 0x00, 0x00, // length = 2 (just flags)
            0x00, 0x40, // END_FLAG
        ][..]);

        let frame = Frame::read_from(&mut buf).unwrap();
        assert!(frame.is_end_frame());
        assert!(frame.content.is_empty());
    }

    #[test]
    fn test_multiple_flags() {
        let frame = Frame::with_flags(BEGIN_FLAG | END_FLAG | IS_FINAL_FLAG);
        assert!(frame.is_begin_frame());
        assert!(frame.is_end_frame());
        assert!(frame.is_final_frame());
    }

    #[test]
    fn test_begin_end_flags_constant() {
        let frame = Frame::with_flags(BEGIN_END_FLAGS);
        assert!(frame.is_begin_frame());
        assert!(frame.is_end_frame());
    }

    #[test]
    fn test_new_begin_frame() {
        let content = BytesMut::from(&[0xAB, 0xCD][..]);
        let frame = Frame::new_begin_frame(content.clone());

        assert!(frame.is_begin_frame());
        assert!(!frame.is_end_frame());
        assert_eq!(frame.content, content);
        assert_eq!(frame.flags, BEGIN_FLAG);
    }

    #[test]
    fn test_with_capacity() {
        let frame = Frame::with_capacity(1024, BEGIN_FLAG | END_FLAG);

        assert!(frame.is_begin_frame());
        assert!(frame.is_end_frame());
        assert!(frame.content.is_empty());
        assert!(frame.content.capacity() >= 1024);
    }

    #[test]
    fn test_frame_roundtrip_with_all_flags() {
        let flags = BEGIN_FLAG | END_FLAG | IS_FINAL_FLAG | IS_EVENT_FLAG;
        let content = BytesMut::from(&[1, 2, 3, 4, 5, 6, 7, 8][..]);
        let original = Frame::new(content.clone(), flags);

        let mut buf = BytesMut::new();
        original.write_to(&mut buf);

        let decoded = Frame::read_from(&mut buf).unwrap();

        assert_eq!(decoded.flags, flags);
        assert_eq!(decoded.content, content);
        assert!(decoded.is_begin_frame());
        assert!(decoded.is_end_frame());
        assert!(decoded.is_final_frame());
        assert!(decoded.is_event_frame());
    }

    #[test]
    fn test_read_large_frame() {
        let content: Vec<u8> = (0..1000).map(|i| (i % 256) as u8).collect();
        let original = Frame::new(BytesMut::from(&content[..]), DEFAULT_FLAGS);

        let mut buf = BytesMut::new();
        original.write_to(&mut buf);

        let decoded = Frame::read_from(&mut buf).unwrap();
        assert_eq!(decoded.content.len(), 1000);
        assert_eq!(&decoded.content[..], &content[..]);
    }

    #[test]
    fn test_write_to_reserves_capacity() {
        let frame = Frame::with_content(BytesMut::from(&[1, 2, 3][..]));
        let mut buf = BytesMut::new();

        frame.write_to(&mut buf);

        assert_eq!(buf.len(), frame.wire_size());
    }

    #[test]
    fn test_default_frame_properties() {
        let frame = Frame::default();

        assert!(!frame.is_begin_frame());
        assert!(!frame.is_end_frame());
        assert!(!frame.is_null_frame());
        assert!(!frame.is_final_frame());
        assert!(!frame.is_event_frame());
        assert!(!frame.is_backup_event_frame());
        assert!(frame.content.is_empty());
    }
}
