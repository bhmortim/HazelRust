//! Benchmarks for serialization performance.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use hazelcast_core::serialization::{Deserializable, ObjectDataInput, ObjectDataOutput, Serializable};

fn benchmark_primitive_serialization(c: &mut Criterion) {
    let mut group = c.benchmark_group("primitive_serialize");
    group.throughput(Throughput::Elements(1));

    group.bench_function("i32", |b| {
        b.iter(|| {
            let mut output = ObjectDataOutput::new();
            black_box(42i32).serialize(&mut output).unwrap();
            black_box(output.into_bytes())
        })
    });

    group.bench_function("i64", |b| {
        b.iter(|| {
            let mut output = ObjectDataOutput::new();
            black_box(123456789i64).serialize(&mut output).unwrap();
            black_box(output.into_bytes())
        })
    });

    group.bench_function("f64", |b| {
        b.iter(|| {
            let mut output = ObjectDataOutput::new();
            black_box(3.14159265359f64).serialize(&mut output).unwrap();
            black_box(output.into_bytes())
        })
    });

    group.bench_function("bool", |b| {
        b.iter(|| {
            let mut output = ObjectDataOutput::new();
            black_box(true).serialize(&mut output).unwrap();
            black_box(output.into_bytes())
        })
    });

    group.finish();
}

fn benchmark_primitive_deserialization(c: &mut Criterion) {
    let mut group = c.benchmark_group("primitive_deserialize");
    group.throughput(Throughput::Elements(1));

    let i32_data = {
        let mut output = ObjectDataOutput::new();
        42i32.serialize(&mut output).unwrap();
        output.into_bytes()
    };
    group.bench_function("i32", |b| {
        b.iter(|| {
            let mut input = ObjectDataInput::new(&i32_data);
            black_box(i32::deserialize(&mut input).unwrap())
        })
    });

    let i64_data = {
        let mut output = ObjectDataOutput::new();
        123456789i64.serialize(&mut output).unwrap();
        output.into_bytes()
    };
    group.bench_function("i64", |b| {
        b.iter(|| {
            let mut input = ObjectDataInput::new(&i64_data);
            black_box(i64::deserialize(&mut input).unwrap())
        })
    });

    let f64_data = {
        let mut output = ObjectDataOutput::new();
        3.14159265359f64.serialize(&mut output).unwrap();
        output.into_bytes()
    };
    group.bench_function("f64", |b| {
        b.iter(|| {
            let mut input = ObjectDataInput::new(&f64_data);
            black_box(f64::deserialize(&mut input).unwrap())
        })
    });

    group.finish();
}

fn benchmark_string_serialization(c: &mut Criterion) {
    let mut group = c.benchmark_group("string_serialize");

    for size in [16, 64, 256, 1024, 4096] {
        let test_string: String = "x".repeat(size);
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &test_string, |b, s| {
            b.iter(|| {
                let mut output = ObjectDataOutput::new();
                s.serialize(&mut output).unwrap();
                black_box(output.into_bytes())
            })
        });
    }

    group.finish();
}

fn benchmark_string_deserialization(c: &mut Criterion) {
    let mut group = c.benchmark_group("string_deserialize");

    for size in [16, 64, 256, 1024, 4096] {
        let test_string: String = "x".repeat(size);
        let serialized = {
            let mut output = ObjectDataOutput::new();
            test_string.serialize(&mut output).unwrap();
            output.into_bytes()
        };

        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &serialized, |b, data| {
            b.iter(|| {
                let mut input = ObjectDataInput::new(data);
                black_box(String::deserialize(&mut input).unwrap())
            })
        });
    }

    group.finish();
}

fn benchmark_vec_serialization(c: &mut Criterion) {
    let mut group = c.benchmark_group("vec_i32_serialize");

    for count in [10, 100, 1000, 10000] {
        let test_vec: Vec<i32> = (0..count).collect();
        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(BenchmarkId::from_parameter(count), &test_vec, |b, v| {
            b.iter(|| {
                let mut output = ObjectDataOutput::new();
                v.serialize(&mut output).unwrap();
                black_box(output.into_bytes())
            })
        });
    }

    group.finish();
}

fn benchmark_vec_deserialization(c: &mut Criterion) {
    let mut group = c.benchmark_group("vec_i32_deserialize");

    for count in [10, 100, 1000, 10000] {
        let test_vec: Vec<i32> = (0..count).collect();
        let serialized = {
            let mut output = ObjectDataOutput::new();
            test_vec.serialize(&mut output).unwrap();
            output.into_bytes()
        };

        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(BenchmarkId::from_parameter(count), &serialized, |b, data| {
            b.iter(|| {
                let mut input = ObjectDataInput::new(data);
                black_box(Vec::<i32>::deserialize(&mut input).unwrap())
            })
        });
    }

    group.finish();
}

fn benchmark_roundtrip(c: &mut Criterion) {
    let mut group = c.benchmark_group("roundtrip");
    group.throughput(Throughput::Elements(1));

    group.bench_function("i32", |b| {
        b.iter(|| {
            let mut output = ObjectDataOutput::new();
            42i32.serialize(&mut output).unwrap();
            let bytes = output.into_bytes();
            let mut input = ObjectDataInput::new(&bytes);
            black_box(i32::deserialize(&mut input).unwrap())
        })
    });

    let test_string = "Hello, Hazelcast!".to_string();
    group.bench_function("string_short", |b| {
        b.iter(|| {
            let mut output = ObjectDataOutput::new();
            test_string.serialize(&mut output).unwrap();
            let bytes = output.into_bytes();
            let mut input = ObjectDataInput::new(&bytes);
            black_box(String::deserialize(&mut input).unwrap())
        })
    });

    let test_vec: Vec<i32> = (0..100).collect();
    group.bench_function("vec_100_i32", |b| {
        b.iter(|| {
            let mut output = ObjectDataOutput::new();
            test_vec.serialize(&mut output).unwrap();
            let bytes = output.into_bytes();
            let mut input = ObjectDataInput::new(&bytes);
            black_box(Vec::<i32>::deserialize(&mut input).unwrap())
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    benchmark_primitive_serialization,
    benchmark_primitive_deserialization,
    benchmark_string_serialization,
    benchmark_string_deserialization,
    benchmark_vec_serialization,
    benchmark_vec_deserialization,
    benchmark_roundtrip,
);

criterion_main!(benches);
