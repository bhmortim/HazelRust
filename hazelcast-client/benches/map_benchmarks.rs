//! Benchmarks for IMap operations.
//!
//! These benchmarks measure the client-side overhead of map operations,
//! including message encoding and response decoding.

use bytes::BytesMut;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use hazelcast_core::protocol::constants::{MAP_GET, MAP_PUT, RESPONSE_HEADER_SIZE};
use hazelcast_core::protocol::Frame;
use hazelcast_core::serialization::{ObjectDataInput, ObjectDataOutput, Serializable};
use hazelcast_core::{compute_partition_hash, ClientMessage};

fn string_frame(s: &str) -> Frame {
    Frame::with_content(BytesMut::from(s.as_bytes()))
}

fn data_frame(data: &[u8]) -> Frame {
    Frame::with_content(BytesMut::from(data))
}

fn long_frame(value: i64) -> Frame {
    let mut buf = BytesMut::with_capacity(8);
    buf.extend_from_slice(&value.to_le_bytes());
    Frame::with_content(buf)
}

fn serialize_value<T: Serializable>(value: &T) -> Vec<u8> {
    let mut output = ObjectDataOutput::new();
    value.serialize(&mut output).unwrap();
    output.into_bytes()
}

fn benchmark_map_put_message_creation(c: &mut Criterion) {
    let mut group = c.benchmark_group("map_put_encode");
    group.throughput(Throughput::Elements(1));

    let map_name = "benchmark-map";

    for value_size in [16, 256, 4096] {
        let key = "benchmark-key".to_string();
        let value: String = "x".repeat(value_size);

        group.bench_with_input(
            BenchmarkId::new("value_size", value_size),
            &(key, value),
            |b, (k, v)| {
                b.iter(|| {
                    let key_data = serialize_value(k);
                    let value_data = serialize_value(v);
                    let partition_id = compute_partition_hash(&key_data);

                    let mut message = ClientMessage::create_for_encode(MAP_PUT, partition_id);
                    message.add_frame(string_frame(map_name));
                    message.add_frame(data_frame(&key_data));
                    message.add_frame(data_frame(&value_data));
                    message.add_frame(long_frame(-1));
                    message.add_frame(long_frame(-1));

                    black_box(message)
                })
            },
        );
    }

    group.finish();
}

fn benchmark_map_get_message_creation(c: &mut Criterion) {
    let mut group = c.benchmark_group("map_get_encode");
    group.throughput(Throughput::Elements(1));

    let map_name = "benchmark-map";

    for key_size in [16, 64, 256] {
        let key: String = "k".repeat(key_size);

        group.bench_with_input(BenchmarkId::new("key_size", key_size), &key, |b, k| {
            b.iter(|| {
                let key_data = serialize_value(k);
                let partition_id = compute_partition_hash(&key_data);

                let mut message = ClientMessage::create_for_encode(MAP_GET, partition_id);
                message.add_frame(string_frame(map_name));
                message.add_frame(data_frame(&key_data));

                black_box(message)
            })
        });
    }

    group.finish();
}

fn benchmark_partition_hash(c: &mut Criterion) {
    let mut group = c.benchmark_group("partition_hash");
    group.throughput(Throughput::Elements(1));

    for key_size in [16, 64, 256, 1024] {
        let key: String = "k".repeat(key_size);
        let key_data = serialize_value(&key);

        group.bench_with_input(
            BenchmarkId::new("key_size", key_size),
            &key_data,
            |b, data| {
                b.iter(|| black_box(compute_partition_hash(data)))
            },
        );
    }

    group.finish();
}

fn benchmark_response_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("map_response_decode");
    group.throughput(Throughput::Elements(1));

    // Simulate a nullable response with value
    for value_size in [16, 256, 4096] {
        let value: String = "v".repeat(value_size);
        let value_data = serialize_value(&value);

        let mut response = ClientMessage::create_for_encode(0, -1);
        response.add_frame(data_frame(&value_data));

        group.bench_with_input(
            BenchmarkId::new("value_size", value_size),
            &response,
            |b, resp| {
                b.iter(|| {
                    let frames = resp.frames();
                    if frames.len() >= 1 {
                        let data_frame = &frames[0];
                        if !data_frame.content.is_empty() {
                            let mut input = ObjectDataInput::new(&data_frame.content);
                            black_box(String::deserialize(&mut input).ok())
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                })
            },
        );
    }

    group.finish();
}

fn benchmark_bool_response_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("bool_response_decode");
    group.throughput(Throughput::Elements(1));

    // Create initial frame with bool at response header offset
    let mut content = BytesMut::with_capacity(RESPONSE_HEADER_SIZE + 1);
    content.extend_from_slice(&vec![0u8; RESPONSE_HEADER_SIZE]);
    content.extend_from_slice(&[1u8]); // true

    let mut response = ClientMessage::create_for_encode(0, -1);
    response.add_frame(Frame::with_content(content));

    group.bench_function("decode", |b| {
        b.iter(|| {
            let frames = response.frames();
            if !frames.is_empty() {
                let initial_frame = &frames[0];
                if initial_frame.content.len() > RESPONSE_HEADER_SIZE {
                    black_box(initial_frame.content[RESPONSE_HEADER_SIZE] != 0)
                } else {
                    false
                }
            } else {
                false
            }
        })
    });

    group.finish();
}

fn benchmark_int_response_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("int_response_decode");
    group.throughput(Throughput::Elements(1));

    // Create initial frame with int at response header offset
    let mut content = BytesMut::with_capacity(RESPONSE_HEADER_SIZE + 4);
    content.extend_from_slice(&vec![0u8; RESPONSE_HEADER_SIZE]);
    content.extend_from_slice(&42i32.to_le_bytes());

    let mut response = ClientMessage::create_for_encode(0, -1);
    response.add_frame(Frame::with_content(content));

    group.bench_function("decode", |b| {
        b.iter(|| {
            let frames = response.frames();
            if !frames.is_empty() {
                let initial_frame = &frames[0];
                if initial_frame.content.len() >= RESPONSE_HEADER_SIZE + 4 {
                    let offset = RESPONSE_HEADER_SIZE;
                    black_box(i32::from_le_bytes([
                        initial_frame.content[offset],
                        initial_frame.content[offset + 1],
                        initial_frame.content[offset + 2],
                        initial_frame.content[offset + 3],
                    ]))
                } else {
                    0
                }
            } else {
                0
            }
        })
    });

    group.finish();
}

fn benchmark_key_serialization_overhead(c: &mut Criterion) {
    let mut group = c.benchmark_group("key_serialization");

    for key_type in ["string", "i64", "uuid"] {
        match key_type {
            "string" => {
                let key = "user-12345".to_string();
                group.bench_function("string", |b| {
                    b.iter(|| black_box(serialize_value(&key)))
                });
            }
            "i64" => {
                let key = 12345i64;
                group.bench_function("i64", |b| {
                    b.iter(|| black_box(serialize_value(&key)))
                });
            }
            "uuid" => {
                let key = uuid::Uuid::new_v4().to_string();
                group.bench_function("uuid_string", |b| {
                    b.iter(|| black_box(serialize_value(&key)))
                });
            }
            _ => {}
        }
    }

    group.finish();
}

criterion_group!(
    benches,
    benchmark_map_put_message_creation,
    benchmark_map_get_message_creation,
    benchmark_partition_hash,
    benchmark_response_decode,
    benchmark_bool_response_decode,
    benchmark_int_response_decode,
    benchmark_key_serialization_overhead,
);

criterion_main!(benches);
