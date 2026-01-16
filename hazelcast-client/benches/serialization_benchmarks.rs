//! Serialization/deserialization throughput benchmarks.

use criterion::{
    black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput,
};
use hazelcast_core::serialization::{
    Deserializable, ObjectDataInput, ObjectDataOutput, Serializable,
};

fn bench_primitive_serialization(c: &mut Criterion) {
    let mut group = c.benchmark_group("primitive_serialization");

    group.bench_function("i32_serialize", |b| {
        b.iter(|| {
            let mut output = ObjectDataOutput::new();
            black_box(42i32).serialize(&mut output).unwrap();
            black_box(output.as_bytes())
        })
    });

    group.bench_function("i64_serialize", |b| {
        b.iter(|| {
            let mut output = ObjectDataOutput::new();
            black_box(123456789i64).serialize(&mut output).unwrap();
            black_box(output.as_bytes())
        })
    });

    group.bench_function("f64_serialize", |b| {
        b.iter(|| {
            let mut output = ObjectDataOutput::new();
            black_box(3.14159265359f64).serialize(&mut output).unwrap();
            black_box(output.as_bytes())
        })
    });

    group.bench_function("bool_serialize", |b| {
        b.iter(|| {
            let mut output = ObjectDataOutput::new();
            black_box(true).serialize(&mut output).unwrap();
            black_box(output.as_bytes())
        })
    });

    group.finish();
}

fn bench_primitive_deserialization(c: &mut Criterion) {
    let mut group = c.benchmark_group("primitive_deserialization");

    let i32_bytes = {
        let mut output = ObjectDataOutput::new();
        42i32.serialize(&mut output).unwrap();
        output.into_bytes()
    };

    let i64_bytes = {
        let mut output = ObjectDataOutput::new();
        123456789i64.serialize(&mut output).unwrap();
        output.into_bytes()
    };

    let f64_bytes = {
        let mut output = ObjectDataOutput::new();
        3.14159265359f64.serialize(&mut output).unwrap();
        output.into_bytes()
    };

    group.bench_function("i32_deserialize", |b| {
        b.iter(|| {
            let mut input = ObjectDataInput::new(&i32_bytes);
            black_box(i32::deserialize(&mut input).unwrap())
        })
    });

    group.bench_function("i64_deserialize", |b| {
        b.iter(|| {
            let mut input = ObjectDataInput::new(&i64_bytes);
            black_box(i64::deserialize(&mut input).unwrap())
        })
    });

    group.bench_function("f64_deserialize", |b| {
        b.iter(|| {
            let mut input = ObjectDataInput::new(&f64_bytes);
            black_box(f64::deserialize(&mut input).unwrap())
        })
    });

    group.finish();
}

fn bench_string_serialization(c: &mut Criterion) {
    let mut group = c.benchmark_group("string_serialization");

    for size in [10, 100, 1000, 10000].iter() {
        let test_string = "x".repeat(*size);
        group.throughput(Throughput::Bytes(*size as u64));

        group.bench_with_input(BenchmarkId::new("serialize", size), &test_string, |b, s| {
            b.iter(|| {
                let mut output = ObjectDataOutput::new();
                s.serialize(&mut output).unwrap();
                black_box(output.as_bytes())
            })
        });
    }

    group.finish();
}

fn bench_string_deserialization(c: &mut Criterion) {
    let mut group = c.benchmark_group("string_deserialization");

    for size in [10, 100, 1000, 10000].iter() {
        let test_string = "x".repeat(*size);
        let serialized = {
            let mut output = ObjectDataOutput::new();
            test_string.serialize(&mut output).unwrap();
            output.into_bytes()
        };

        group.throughput(Throughput::Bytes(*size as u64));

        group.bench_with_input(
            BenchmarkId::new("deserialize", size),
            &serialized,
            |b, data| {
                b.iter(|| {
                    let mut input = ObjectDataInput::new(data);
                    black_box(String::deserialize(&mut input).unwrap())
                })
            },
        );
    }

    group.finish();
}

fn bench_bytes_serialization(c: &mut Criterion) {
    let mut group = c.benchmark_group("bytes_serialization");

    for size in [64, 256, 1024, 4096, 16384].iter() {
        let test_bytes: Vec<u8> = (0..*size).map(|i| (i % 256) as u8).collect();
        group.throughput(Throughput::Bytes(*size as u64));

        group.bench_with_input(
            BenchmarkId::new("serialize", size),
            &test_bytes,
            |b, data| {
                b.iter(|| {
                    let mut output = ObjectDataOutput::new();
                    data.serialize(&mut output).unwrap();
                    black_box(output.as_bytes())
                })
            },
        );
    }

    group.finish();
}

fn bench_bytes_deserialization(c: &mut Criterion) {
    let mut group = c.benchmark_group("bytes_deserialization");

    for size in [64, 256, 1024, 4096, 16384].iter() {
        let test_bytes: Vec<u8> = (0..*size).map(|i| (i % 256) as u8).collect();
        let serialized = {
            let mut output = ObjectDataOutput::new();
            test_bytes.serialize(&mut output).unwrap();
            output.into_bytes()
        };

        group.throughput(Throughput::Bytes(*size as u64));

        group.bench_with_input(
            BenchmarkId::new("deserialize", size),
            &serialized,
            |b, data| {
                b.iter(|| {
                    let mut input = ObjectDataInput::new(data);
                    black_box(Vec::<u8>::deserialize(&mut input).unwrap())
                })
            },
        );
    }

    group.finish();
}

fn bench_mixed_serialization(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed_serialization");

    group.bench_function("key_value_pair_serialize", |b| {
        let key = "user:12345".to_string();
        let value = 9999i64;

        b.iter(|| {
            let mut output = ObjectDataOutput::new();
            key.serialize(&mut output).unwrap();
            value.serialize(&mut output).unwrap();
            black_box(output.as_bytes())
        })
    });

    group.bench_function("multiple_fields_serialize", |b| {
        let id = 42i32;
        let name = "test_item".to_string();
        let price = 19.99f64;
        let active = true;

        b.iter(|| {
            let mut output = ObjectDataOutput::new();
            id.serialize(&mut output).unwrap();
            name.serialize(&mut output).unwrap();
            price.serialize(&mut output).unwrap();
            active.serialize(&mut output).unwrap();
            black_box(output.as_bytes())
        })
    });

    group.finish();
}

fn bench_round_trip(c: &mut Criterion) {
    let mut group = c.benchmark_group("round_trip");

    group.bench_function("i64_round_trip", |b| {
        b.iter(|| {
            let mut output = ObjectDataOutput::new();
            black_box(123456789i64).serialize(&mut output).unwrap();
            let bytes = output.into_bytes();
            let mut input = ObjectDataInput::new(&bytes);
            black_box(i64::deserialize(&mut input).unwrap())
        })
    });

    group.bench_function("string_100_round_trip", |b| {
        let test_string = "x".repeat(100);
        b.iter(|| {
            let mut output = ObjectDataOutput::new();
            test_string.serialize(&mut output).unwrap();
            let bytes = output.into_bytes();
            let mut input = ObjectDataInput::new(&bytes);
            black_box(String::deserialize(&mut input).unwrap())
        })
    });

    group.bench_function("bytes_1024_round_trip", |b| {
        let test_bytes: Vec<u8> = (0..1024).map(|i| (i % 256) as u8).collect();
        b.iter(|| {
            let mut output = ObjectDataOutput::new();
            test_bytes.serialize(&mut output).unwrap();
            let bytes = output.into_bytes();
            let mut input = ObjectDataInput::new(&bytes);
            black_box(Vec::<u8>::deserialize(&mut input).unwrap())
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_primitive_serialization,
    bench_primitive_deserialization,
    bench_string_serialization,
    bench_string_deserialization,
    bench_bytes_serialization,
    bench_bytes_deserialization,
    bench_mixed_serialization,
    bench_round_trip,
);

criterion_main!(benches);
