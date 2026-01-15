#![no_main]

use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;

use hazelcast_core::protocol::{ClientMessage, ClientMessageCodec};
use tokio_util::codec::Decoder;

fuzz_target!(|data: &[u8]| {
    let mut codec = ClientMessageCodec::new();
    let mut buf = BytesMut::from(data);

    loop {
        match codec.decode(&mut buf) {
            Ok(Some(msg)) => {
                let _ = msg.message_type();
                let _ = msg.correlation_id();
                let _ = msg.partition_id();
                let _ = msg.frame_count();
                let _ = msg.is_request();
                let _ = msg.is_event();
                let _ = msg.wire_size();
            }
            Ok(None) => break,
            Err(_) => break,
        }
    }

    let msg = ClientMessage::from_frames(vec![]);
    let _ = msg.is_empty();
});
