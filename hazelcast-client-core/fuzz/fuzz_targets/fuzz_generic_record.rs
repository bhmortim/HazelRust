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

use libfuzzer_sys::fuzz_target;
use hazelcast_client_core::GenericRecord;

fuzz_target!(|data: &[u8]| {
    if let Ok(record) = GenericRecord::from_compact_bytes(data) {
        let _ = record.type_name();
        let _ = record.schema_id();
        let _ = record.field_count();

        for name in record.field_names() {
            let _ = record.get_field_kind(name);
            let _ = record.get_boolean(name);
            let _ = record.get_int8(name);
            let _ = record.get_int16(name);
            let _ = record.get_int32(name);
            let _ = record.get_int64(name);
            let _ = record.get_float32(name);
            let _ = record.get_float64(name);
            let _ = record.get_string(name);
            let _ = record.get_nullable_boolean(name);
            let _ = record.get_nullable_int32(name);
            let _ = record.get_array_of_int32(name);
            let _ = record.get_array_of_string(name);
            let _ = record.get_generic_record(name);
            let _ = record.get_array_of_generic_record(name);
        }
    }
});
