# hazelcast-client

Rust client library for Hazelcast.

## Features

- Distributed data structures: `IMap`, `IQueue`, `ISet`, `IList`, `MultiMap`, `ReplicatedMap`
- Pub/sub messaging: `ITopic`, `ReliableTopic`
- Distributed primitives: `AtomicLong`, `PNCounter`, `FencedLock`, `Semaphore`, `CountDownLatch`
- ID generation: `FlakeIdGenerator`, `Ringbuffer`
- Query: SQL service, Predicate API
- Transactions: ACID transactions across data structures
- Executors: `ExecutorService`, `ScheduledExecutorService`

### Optional Features

| Feature | Description |
|---------|-------------|
| `tls` | TLS/SSL encrypted connections |
| `metrics` | Prometheus metrics integration |
| `aws` | AWS EC2 cluster discovery |
| `kubernetes` | Kubernetes cluster discovery |
| `cloud` | Hazelcast Cloud discovery |
| `websocket` | WebSocket transport |

## Running Benchmarks

The client includes a comprehensive benchmark suite using [Criterion.rs](https://github.com/bheisler/criterion.rs).

### Run All Benchmarks

```bash
cargo bench
```

### Run Specific Benchmark Group

```bash
# Map operation benchmarks
cargo bench --bench map_benchmarks

# Queue operation benchmarks
cargo bench --bench queue_benchmarks

# Serialization benchmarks
cargo bench --bench serialization_benchmarks
```

### Run Specific Benchmark Function

```bash
# Run only partition hash benchmarks
cargo bench --bench map_benchmarks -- partition_hash

# Run only string serialization benchmarks
cargo bench --bench serialization_benchmarks -- string_serialize
```

### Benchmark Output

Benchmark results are saved to `target/criterion/`. Open `target/criterion/report/index.html` in a browser to view detailed HTML reports with:

- Throughput measurements (ops/sec, bytes/sec)
- Latency percentiles (min, median, max)
- Historical comparison graphs
- Statistical analysis

### Benchmark Categories

| Benchmark | Description |
|-----------|-------------|
| `serialization_benchmarks` | Primitive types, strings, vectors encoding/decoding |
| `map_benchmarks` | IMap message creation, partition hashing, response decoding |
| `queue_benchmarks` | IQueue offer/poll/peek message encoding, response parsing |

### Tips

- Run benchmarks on a quiet system for consistent results
- Use `--save-baseline <name>` to save results for comparison
- Use `--baseline <name>` to compare against a saved baseline
- Add `-- --verbose` for detailed output during benchmark runs

```bash
# Save current performance as baseline
cargo bench --bench serialization_benchmarks -- --save-baseline main

# Compare against baseline after changes
cargo bench --bench serialization_benchmarks -- --baseline main
```
