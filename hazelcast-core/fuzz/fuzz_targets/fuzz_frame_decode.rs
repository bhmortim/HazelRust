#![no_main]

use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;

use hazelcast_core::protocol::Frame;

fuzz_target!(|data: &[u8]| {
    let mut buf = BytesMut::from(data);
    
    while !buf.is_empty() {
        match Frame::read_from(&mut buf) {
            Some(frame) => {
                let _ = frame.is_begin_frame();
                let _ = frame.is_end_frame();
                let _ = frame.is_null_frame();
                let _ = frame.is_final_frame();
                let _ = frame.is_event_frame();
                let _ = frame.wire_size();
                let _ = frame.frame_length();
            }
            None => break,
        }
    }
});
