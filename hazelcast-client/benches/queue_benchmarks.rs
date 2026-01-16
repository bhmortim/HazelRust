//! IQueue client-side serialization benchmarks.

use criterion::{
    black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput,
};
use hazelcast_core::serialization::{
    Deserializable, ObjectDataInput, ObjectDataOutput, Serializable,
};

fn simulate_queue_offer_serialization<T: Serializable>(item: &T) -> Vec<u8> {
    let mut output = ObjectDataOutput::new();
    item.serialize(&mut output).unwrap();
    output.into_bytes()
}

fn simulate_queue_poll_deserialization<T: Deserializable>(item_bytes: &[u8]) -> T {
    let mut input = ObjectDataInput::new(item_bytes);
    T::deserialize(&mut input).unwrap()
}

fn bench_queue_offer_string(c: &mut Criterion) {
    let mut group = c.benchmark_group("queue_offer_string");

    for size in [10, 100, 1000].iter() {
        let item = "x".repeat(*size);

        group.throughput(Throughput::Bytes(*size as u64));

        group.bench_with_input(BenchmarkId::new("serialize", size), &item, |b, data| {
            b.iter(|| black_box(simulate_queue_offer_serialization(data)))
        });
    }

    group.finish();
}

fn bench_queue_offer_bytes(c: &mut Criterion) {
    let mut group = c.benchmark_group("queue_offer_bytes");

    for size in [64, 256, 1024, 4096].iter() {
        let item: Vec<u8> = (0..*size).map(|i| (i % 256) as u8).collect();

        group.throughput(Throughput::Bytes(*size as u64));

        group.bench_with_input(BenchmarkId::new("serialize", size), &item, |b, data| {
            b.iter(|| black_box(simulate_queue_offer_serialization(data)))
        });
    }

    group.finish();
}

fn bench_queue_poll_string(c: &mut Criterion) {
    let mut group = c.benchmark_group("queue_poll_string");

    for size in [10, 100, 1000].iter() {
        let item = "x".repeat(*size);
        let serialized = simulate_queue_offer_serialization(&item);

        group.throughput(Throughput::Bytes(*size as u64));

        group.bench_with_input(
            BenchmarkId::new("deserialize", size),
            &serialized,
            |b, data| {
                b.iter(|| black_box(simulate_queue_poll_deserialization::<String>(data)))
            },
        );
    }

    group.finish();
}

fn bench_queue_poll_bytes(c: &mut Criterion) {
    let mut group = c.benchmark_group("queue_poll_bytes");

    for size in [64, 256, 1024, 4096].iter() {
        let item: Vec<u8> = (0..*size).map(|i| (i % 256) as u8).collect();
        let serialized = simulate_queue_offer_serialization(&item);

        group.throughput(Throughput::Bytes(*size as u64));

        group.bench_with_input(
            BenchmarkId::new("deserialize", size),
            &serialized,
            |b, data| {
                b.iter(|| black_box(simulate_queue_poll_deserialization::<Vec<u8>>(data)))
            },
        );
    }

    group.finish();
}

fn bench_queue_round_trip(c: &mut Criterion) {
    let mut group = c.benchmark_group("queue_round_trip");

    group.bench_function("string_100", |b| {
        let item = "x".repeat(100);

        b.iter(|| {
            let serialized = simulate_queue_offer_serialization(&item);
            let result: String = simulate_queue_poll_deserialization(&serialized);
            black_box(result)
        })
    });

    group.bench_function("bytes_1024", |b| {
        let item: Vec<u8> = (0..1024).map(|i| (i % 256) as u8).collect();

        b.iter(|| {
            let serialized = simulate_queue_offer_serialization(&item);
            let result: Vec<u8> = simulate_queue_poll_deserialization(&serialized);
            black_box(result)
        })
    });

    group.bench_function("i64", |b| {
        let item = 123456789i64;

        b.iter(|| {
            let serialized = simulate_queue_offer_serialization(&item);
            let result: i64 = simulate_queue_poll_deserialization(&serialized);
            black_box(result)
        })
    });

    group.finish();
}

fn bench_queue_batch_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("queue_batch_operations");

    for batch_size in [10, 50, 100].iter() {
        let items: Vec<String> = (0..*batch_size)
            .map(|i| format!("queue_item_{}", i))
            .collect();

        group.throughput(Throughput::Elements(*batch_size as u64));

        group.bench_with_input(
            BenchmarkId::new("offer_batch_serialize", batch_size),
            &items,
            |b, data| {
                b.iter(|| {
                    let serialized: Vec<_> = data
                        .iter()
                        .map(|item| simulate_queue_offer_serialization(item))
                        .collect();
                    black_box(serialized)
                })
            },
        );
    }

    group.finish();
}

fn bench_queue_primitives(c: &mut Criterion) {
    let mut group = c.benchmark_group("queue_primitives");

    group.bench_function("i32_offer_poll", |b| {
        let item = 42i32;
        b.iter(|| {
            let serialized = simulate_queue_offer_serialization(&item);
            let result: i32 = simulate_queue_poll_deserialization(&serialized);
            black_box(result)
        })
    });

    group.bench_function("i64_offer_poll", |b| {
        let item = 123456789i64;
        b.iter(|| {
            let serialized = simulate_queue_offer_serialization(&item);
            let result: i64 = simulate_queue_poll_deserialization(&serialized);
            black_box(result)
        })
    });

    group.bench_function("f64_offer_poll", |b| {
        let item = 3.14159265359f64;
        b.iter(|| {
            let serialized = simulate_queue_offer_serialization(&item);
            let result: f64 = simulate_queue_poll_deserialization(&serialized);
            black_box(result)
        })
    });

    group.bench_function("bool_offer_poll", |b| {
        let item = true;
        b.iter(|| {
            let serialized = simulate_queue_offer_serialization(&item);
            let result: bool = simulate_queue_poll_deserialization(&serialized);
            black_box(result)
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_queue_offer_string,
    bench_queue_offer_bytes,
    bench_queue_poll_string,
    bench_queue_poll_bytes,
    bench_queue_round_trip,
    bench_queue_batch_operations,
    bench_queue_primitives,
);

criterion_main!(benches);
