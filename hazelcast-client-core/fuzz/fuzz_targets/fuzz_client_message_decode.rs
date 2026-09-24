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

use hazelcast_client_core::protocol::{ClientMessage, ClientMessageCodec};
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
