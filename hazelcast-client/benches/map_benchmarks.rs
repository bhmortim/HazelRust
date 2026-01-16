//! IMap client-side serialization benchmarks.

use criterion::{
    black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput,
};
use hazelcast_core::serialization::{
    Deserializable, ObjectDataInput, ObjectDataOutput, Serializable,
};

fn simulate_map_put_serialization<K: Serializable, V: Serializable>(
    key: &K,
    value: &V,
) -> (Vec<u8>, Vec<u8>) {
    let mut key_output = ObjectDataOutput::new();
    key.serialize(&mut key_output).unwrap();

    let mut value_output = ObjectDataOutput::new();
    value.serialize(&mut value_output).unwrap();

    (key_output.into_bytes(), value_output.into_bytes())
}

fn simulate_map_get_deserialization<V: Deserializable>(value_bytes: &[u8]) -> V {
    let mut input = ObjectDataInput::new(value_bytes);
    V::deserialize(&mut input).unwrap()
}

fn bench_map_put_string_key(c: &mut Criterion) {
    let mut group = c.benchmark_group("map_put_string_key");

    for value_size in [10, 100, 1000].iter() {
        let key = "user:12345".to_string();
        let value = "v".repeat(*value_size);

        group.throughput(Throughput::Bytes(*value_size as u64));

        group.bench_with_input(
            BenchmarkId::new("serialize", value_size),
            &(key.clone(), value),
            |b, (k, v)| {
                b.iter(|| black_box(simulate_map_put_serialization(k, v)))
            },
        );
    }

    group.finish();
}

fn bench_map_put_int_key(c: &mut Criterion) {
    let mut group = c.benchmark_group("map_put_int_key");

    for value_size in [10, 100, 1000].iter() {
        let key = 12345i64;
        let value = "v".repeat(*value_size);

        group.throughput(Throughput::Bytes(*value_size as u64));

        group.bench_with_input(
            BenchmarkId::new("serialize", value_size),
            &(key, value),
            |b, (k, v)| {
                b.iter(|| black_box(simulate_map_put_serialization(k, v)))
            },
        );
    }

    group.finish();
}

fn bench_map_get_deserialization(c: &mut Criterion) {
    let mut group = c.benchmark_group("map_get_deserialization");

    for value_size in [10, 100, 1000].iter() {
        let value = "v".repeat(*value_size);
        let mut output = ObjectDataOutput::new();
        value.serialize(&mut output).unwrap();
        let value_bytes = output.into_bytes();

        group.throughput(Throughput::Bytes(*value_size as u64));

        group.bench_with_input(
            BenchmarkId::new("deserialize", value_size),
            &value_bytes,
            |b, bytes| {
                b.iter(|| black_box(simulate_map_get_deserialization::<String>(bytes)))
            },
        );
    }

    group.finish();
}

fn bench_map_round_trip(c: &mut Criterion) {
    let mut group = c.benchmark_group("map_round_trip");

    group.bench_function("string_key_string_value_100", |b| {
        let key = "user:12345".to_string();
        let value = "v".repeat(100);

        b.iter(|| {
            let (key_bytes, value_bytes) = simulate_map_put_serialization(&key, &value);
            black_box(&key_bytes);
            let result: String = simulate_map_get_deserialization(&value_bytes);
            black_box(result)
        })
    });

    group.bench_function("int_key_bytes_value_1024", |b| {
        let key = 12345i64;
        let value: Vec<u8> = (0..1024).map(|i| (i % 256) as u8).collect();

        b.iter(|| {
            let (key_bytes, value_bytes) = simulate_map_put_serialization(&key, &value);
            black_box(&key_bytes);
            let result: Vec<u8> = simulate_map_get_deserialization(&value_bytes);
            black_box(result)
        })
    });

    group.finish();
}

fn bench_map_batch_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("map_batch_operations");

    for batch_size in [10, 50, 100].iter() {
        let entries: Vec<(String, String)> = (0..*batch_size)
            .map(|i| (format!("key:{}", i), format!("value:{}", i)))
            .collect();

        group.throughput(Throughput::Elements(*batch_size as u64));

        group.bench_with_input(
            BenchmarkId::new("put_all_serialize", batch_size),
            &entries,
            |b, data| {
                b.iter(|| {
                    let serialized: Vec<_> = data
                        .iter()
                        .map(|(k, v)| simulate_map_put_serialization(k, v))
                        .collect();
                    black_box(serialized)
                })
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_map_put_string_key,
    bench_map_put_int_key,
    bench_map_get_deserialization,
    bench_map_round_trip,
    bench_map_batch_operations,
);

criterion_main!(benches);
