/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#![no_main]

use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;

use hazelcast_client_core::protocol::Frame;

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
