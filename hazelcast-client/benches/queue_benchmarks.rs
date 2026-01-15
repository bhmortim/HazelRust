//! Benchmarks for IQueue operations.
//!
//! These benchmarks measure the client-side overhead of queue operations,
//! including message encoding and response decoding.

use bytes::BytesMut;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use hazelcast_core::protocol::constants::{IS_NULL_FLAG, QUEUE_OFFER, QUEUE_PEEK, QUEUE_POLL, QUEUE_SIZE, RESPONSE_HEADER_SIZE};
use hazelcast_core::protocol::Frame;
use hazelcast_core::serialization::{Deserializable, ObjectDataInput, ObjectDataOutput, Serializable};
use hazelcast_core::ClientMessage;

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

fn benchmark_queue_offer_message_creation(c: &mut Criterion) {
    let mut group = c.benchmark_group("queue_offer_encode");
    group.throughput(Throughput::Elements(1));

    let queue_name = "benchmark-queue";

    for item_size in [16, 256, 4096] {
        let item: String = "x".repeat(item_size);

        group.bench_with_input(
            BenchmarkId::new("item_size", item_size),
            &item,
            |b, item| {
                b.iter(|| {
                    let item_data = serialize_value(item);

                    let mut message =
                        ClientMessage::create_for_encode_any_partition(QUEUE_OFFER);
                    message.add_frame(string_frame(queue_name));
                    message.add_frame(data_frame(&item_data));
                    message.add_frame(long_frame(0));

                    black_box(message)
                })
            },
        );
    }

    group.finish();
}

fn benchmark_queue_poll_message_creation(c: &mut Criterion) {
    let mut group = c.benchmark_group("queue_poll_encode");
    group.throughput(Throughput::Elements(1));

    let queue_name = "benchmark-queue";

    group.bench_function("non_blocking", |b| {
        b.iter(|| {
            let mut message = ClientMessage::create_for_encode_any_partition(QUEUE_POLL);
            message.add_frame(string_frame(queue_name));
            message.add_frame(long_frame(0));
            black_box(message)
        })
    });

    group.bench_function("with_timeout_5s", |b| {
        b.iter(|| {
            let mut message = ClientMessage::create_for_encode_any_partition(QUEUE_POLL);
            message.add_frame(string_frame(queue_name));
            message.add_frame(long_frame(5000));
            black_box(message)
        })
    });

    group.finish();
}

fn benchmark_queue_peek_message_creation(c: &mut Criterion) {
    let mut group = c.benchmark_group("queue_peek_encode");
    group.throughput(Throughput::Elements(1));

    let queue_name = "benchmark-queue";

    group.bench_function("encode", |b| {
        b.iter(|| {
            let mut message = ClientMessage::create_for_encode_any_partition(QUEUE_PEEK);
            message.add_frame(string_frame(queue_name));
            black_box(message)
        })
    });

    group.finish();
}

fn benchmark_queue_size_message_creation(c: &mut Criterion) {
    let mut group = c.benchmark_group("queue_size_encode");
    group.throughput(Throughput::Elements(1));

    let queue_name = "benchmark-queue";

    group.bench_function("encode", |b| {
        b.iter(|| {
            let mut message = ClientMessage::create_for_encode_any_partition(QUEUE_SIZE);
            message.add_frame(string_frame(queue_name));
            black_box(message)
        })
    });

    group.finish();
}

fn benchmark_queue_nullable_response_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("queue_nullable_decode");
    group.throughput(Throughput::Elements(1));

    // Response with value
    for item_size in [16, 256, 4096] {
        let item: String = "v".repeat(item_size);
        let item_data = serialize_value(&item);

        let mut response = ClientMessage::create_for_encode(0, -1);
        // Add initial frame (header)
        let header = BytesMut::from(&vec![0u8; RESPONSE_HEADER_SIZE][..]);
        response.add_frame(Frame::with_content(header));
        // Add data frame
        response.add_frame(data_frame(&item_data));

        group.bench_with_input(
            BenchmarkId::new("with_value", item_size),
            &response,
            |b, resp| {
                b.iter(|| {
                    let frames = resp.frames();
                    if frames.len() >= 2 {
                        let data_frame = &frames[1];
                        if data_frame.flags & IS_NULL_FLAG == 0 && !data_frame.content.is_empty() {
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

    // Response with null (empty queue)
    let mut null_response = ClientMessage::create_for_encode(0, -1);
    let header = BytesMut::from(&vec![0u8; RESPONSE_HEADER_SIZE][..]);
    null_response.add_frame(Frame::with_content(header));
    let mut null_frame = Frame::with_content(BytesMut::new());
    null_frame.flags |= IS_NULL_FLAG;
    null_response.add_frame(null_frame);

    group.bench_function("null_value", |b| {
        b.iter(|| {
            let frames = null_response.frames();
            if frames.len() >= 2 {
                let data_frame = &frames[1];
                if data_frame.flags & IS_NULL_FLAG != 0 {
                    black_box(Option::<String>::None)
                } else if data_frame.content.is_empty() {
                    black_box(Option::<String>::None)
                } else {
                    let mut input = ObjectDataInput::new(&data_frame.content);
                    black_box(String::deserialize(&mut input).ok())
                }
            } else {
                black_box(Option::<String>::None)
            }
        })
    });

    group.finish();
}

fn benchmark_queue_bool_response_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("queue_bool_decode");
    group.throughput(Throughput::Elements(1));

    // Create response with bool=true (offer succeeded)
    let mut content = BytesMut::with_capacity(RESPONSE_HEADER_SIZE + 1);
    content.extend_from_slice(&vec![0u8; RESPONSE_HEADER_SIZE]);
    content.extend_from_slice(&[1u8]);

    let mut response = ClientMessage::create_for_encode(0, -1);
    response.add_frame(Frame::with_content(content));

    group.bench_function("decode_true", |b| {
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

    // Create response with bool=false (offer failed/queue full)
    let mut content_false = BytesMut::with_capacity(RESPONSE_HEADER_SIZE + 1);
    content_false.extend_from_slice(&vec![0u8; RESPONSE_HEADER_SIZE]);
    content_false.extend_from_slice(&[0u8]);

    let mut response_false = ClientMessage::create_for_encode(0, -1);
    response_false.add_frame(Frame::with_content(content_false));

    group.bench_function("decode_false", |b| {
        b.iter(|| {
            let frames = response_false.frames();
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

fn benchmark_queue_int_response_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("queue_int_decode");
    group.throughput(Throughput::Elements(1));

    // Create response with size=1000
    let mut content = BytesMut::with_capacity(RESPONSE_HEADER_SIZE + 4);
    content.extend_from_slice(&vec![0u8; RESPONSE_HEADER_SIZE]);
    content.extend_from_slice(&1000i32.to_le_bytes());

    let mut response = ClientMessage::create_for_encode(0, -1);
    response.add_frame(Frame::with_content(content));

    group.bench_function("decode_size", |b| {
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

fn benchmark_queue_item_serialization(c: &mut Criterion) {
    let mut group = c.benchmark_group("queue_item_serialize");

    // String items of various sizes
    for size in [32, 128, 512, 2048] {
        let item: String = "q".repeat(size);
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::new("string", size), &item, |b, item| {
            b.iter(|| black_box(serialize_value(item)))
        });
    }

    // Vec<i32> items
    for count in [10, 100, 1000] {
        let item: Vec<i32> = (0..count).collect();
        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(BenchmarkId::new("vec_i32", count), &item, |b, item| {
            b.iter(|| black_box(serialize_value(item)))
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    benchmark_queue_offer_message_creation,
    benchmark_queue_poll_message_creation,
    benchmark_queue_peek_message_creation,
    benchmark_queue_size_message_creation,
    benchmark_queue_nullable_response_decode,
    benchmark_queue_bool_response_decode,
    benchmark_queue_int_response_decode,
    benchmark_queue_item_serialization,
);

criterion_main!(benches);
