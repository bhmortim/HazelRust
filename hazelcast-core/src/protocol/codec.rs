//! Codec implementation for encoding/decoding Hazelcast protocol messages.

use bytes::BytesMut;
use tokio_util::codec::{Decoder, Encoder};

use super::constants::*;
use super::frame::Frame;
use super::ClientMessage;
use crate::error::{HazelcastError, Result};

/// Codec for encoding and decoding Hazelcast client messages.
///
/// Implements the `tokio_util::codec::{Encoder, Decoder}` traits for use
/// with tokio's framed I/O.
#[derive(Debug, Default)]
pub struct ClientMessageCodec {
    /// Frames accumulated while decoding a multi-frame message.
    pending_frames: Vec<Frame>,
    /// Whether we're currently accumulating frames for a message.
    in_message: bool,
}

impl ClientMessageCodec {
    /// Creates a new codec instance.
    pub fn new() -> Self {
        Self {
            pending_frames: Vec::new(),
            in_message: false,
        }
    }
}

impl Encoder<ClientMessage> for ClientMessageCodec {
    type Error = HazelcastError;

    fn encode(&mut self, mut item: ClientMessage, dst: &mut BytesMut) -> Result<()> {
        if item.is_empty() {
            return Err(HazelcastError::Protocol(
                "cannot encode empty message".to_string(),
            ));
        }

        item.write_to(dst);
        Ok(())
    }
}

impl Decoder for ClientMessageCodec {
    type Item = ClientMessage;
    type Error = HazelcastError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>> {
        loop {
            if src.len() < SIZE_OF_FRAME_LENGTH_FIELD {
                return Ok(None);
            }

            let frame_length =
                u32::from_le_bytes([src[0], src[1], src[2], src[3]]) as usize;
            let total_frame_size = SIZE_OF_FRAME_LENGTH_FIELD + frame_length;

            if src.len() < total_frame_size {
                return Ok(None);
            }

            let frame = Frame::read_from(src)
                .ok_or_else(|| HazelcastError::Protocol("failed to read frame".to_string()))?;

            let is_begin = frame.is_begin_frame();
            let is_end = frame.is_end_frame();

            if is_begin {
                self.pending_frames.clear();
                self.in_message = true;
            }

            if self.in_message {
                self.pending_frames.push(frame);
            }

            if is_end {
                self.in_message = false;
                let frames = std::mem::take(&mut self.pending_frames);
                return Ok(Some(ClientMessage::from_frames(frames)));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_simple_message(message_type: i32) -> ClientMessage {
        ClientMessage::create_for_encode(message_type, PARTITION_ID_ANY)
    }

    #[test]
    fn test_encode_decode_roundtrip() {
        let mut codec = ClientMessageCodec::new();
        let original = create_simple_message(CLIENT_AUTHENTICATION);
        let original_type = original.message_type();
        let original_partition = original.partition_id();

        let mut buf = BytesMut::new();
        codec.encode(original, &mut buf).unwrap();

        assert!(!buf.is_empty());

        let decoded = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded.message_type(), original_type);
        assert_eq!(decoded.partition_id(), original_partition);
    }

    #[test]
    fn test_encode_empty_message_fails() {
        let mut codec = ClientMessageCodec::new();
        let empty = ClientMessage::new();
        let mut buf = BytesMut::new();

        let result = codec.encode(empty, &mut buf);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_incomplete_length() {
        let mut codec = ClientMessageCodec::new();
        let mut buf = BytesMut::from(&[0x01, 0x02][..]);

        let result = codec.decode(&mut buf).unwrap();
        assert!(result.is_none());
        assert_eq!(buf.len(), 2);
    }

    #[test]
    fn test_decode_incomplete_frame() {
        let mut codec = ClientMessageCodec::new();
        let mut buf = BytesMut::from(&[
            0x10, 0x00, 0x00, 0x00, // length = 16
            0x00, 0x80, // BEGIN flag
            0x01, 0x02, // only 2 bytes of content (need 14 more)
        ][..]);

        let result = codec.decode(&mut buf).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_decode_multi_frame_message() {
        let mut codec = ClientMessageCodec::new();

        let mut msg = ClientMessage::create_for_encode(MAP_PUT, 42);
        msg.add_frame(Frame::with_content(BytesMut::from(&b"key"[..])));
        msg.add_frame(Frame::with_content(BytesMut::from(&b"value"[..])));

        let mut buf = BytesMut::new();
        codec.encode(msg, &mut buf).unwrap();

        let decoded = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded.frame_count(), 3);
        assert_eq!(decoded.message_type(), Some(MAP_PUT));
        assert_eq!(decoded.partition_id(), Some(42));
    }

    #[test]
    fn test_decode_multiple_messages() {
        let mut codec = ClientMessageCodec::new();

        let msg1 = create_simple_message(MAP_GET);
        let msg2 = create_simple_message(MAP_PUT);

        let mut buf = BytesMut::new();
        codec.encode(msg1, &mut buf).unwrap();
        codec.encode(msg2, &mut buf).unwrap();

        let decoded1 = codec.decode(&mut buf).unwrap().unwrap();
        let decoded2 = codec.decode(&mut buf).unwrap().unwrap();

        assert_eq!(decoded1.message_type(), Some(MAP_GET));
        assert_eq!(decoded2.message_type(), Some(MAP_PUT));
        assert!(buf.is_empty());
    }

    #[test]
    fn test_encode_authentication_request() {
        let mut codec = ClientMessageCodec::new();
        let mut msg = ClientMessage::create_for_encode(CLIENT_AUTHENTICATION, PARTITION_ID_ANY);

        let cluster_name = Frame::with_content(BytesMut::from(&b"dev"[..]));
        let username = Frame::with_content(BytesMut::from(&b"admin"[..]));
        let password = Frame::with_content(BytesMut::from(&b"password"[..]));

        msg.add_frame(cluster_name);
        msg.add_frame(username);
        msg.add_frame(password);

        let mut buf = BytesMut::new();
        codec.encode(msg, &mut buf).unwrap();

        let decoded = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded.message_type(), Some(CLIENT_AUTHENTICATION));
        assert_eq!(decoded.frame_count(), 4);
    }

    #[test]
    fn test_encode_map_get_request() {
        let mut codec = ClientMessageCodec::new();
        let partition_id = 7;
        let mut msg = ClientMessage::create_for_encode(MAP_GET, partition_id);

        let map_name = Frame::with_content(BytesMut::from(&b"my-map"[..]));
        let key = Frame::with_content(BytesMut::from(&b"my-key"[..]));

        msg.add_frame(map_name);
        msg.add_frame(key);

        let mut buf = BytesMut::new();
        codec.encode(msg, &mut buf).unwrap();

        let decoded = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded.message_type(), Some(MAP_GET));
        assert_eq!(decoded.partition_id(), Some(partition_id));
        assert_eq!(decoded.frame_count(), 3);
    }

    #[test]
    fn test_codec_is_reusable() {
        let mut codec = ClientMessageCodec::new();

        for i in 0..10 {
            let msg = ClientMessage::create_for_encode(MAP_GET, i);
            let mut buf = BytesMut::new();
            codec.encode(msg, &mut buf).unwrap();

            let decoded = codec.decode(&mut buf).unwrap().unwrap();
            assert_eq!(decoded.partition_id(), Some(i));
        }
    }

    #[test]
    fn test_partial_then_complete_decode() {
        let mut codec = ClientMessageCodec::new();
        let msg = create_simple_message(MAP_SIZE);

        let mut full_buf = BytesMut::new();
        codec.encode(msg, &mut full_buf).unwrap();

        let full_len = full_buf.len();
        let split_point = full_len / 2;

        let mut partial_buf = full_buf.split_to(split_point);
        assert!(codec.decode(&mut partial_buf).unwrap().is_none());

        partial_buf.unsplit(full_buf);
        let decoded = codec.decode(&mut partial_buf).unwrap().unwrap();
        assert_eq!(decoded.message_type(), Some(MAP_SIZE));
    }
}
