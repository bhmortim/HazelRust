# Integration Tests

This directory contains integration tests for the Hazelcast Rust client.

## Prerequisites

Integration tests require a running Hazelcast cluster. You can start one using Docker:

```bash
# Start a single-node cluster
docker run -d --name hazelcast-test -p 5701:5701 hazelcast/hazelcast:5.3

# Or use the provided docker-compose for a 2-node cluster
docker-compose -f tests/ci_docker_compose.yml up -d
```

## Running Tests

Integration tests are marked with `#[ignore]` and won't run by default:

```bash
# Run all unit tests (default)
cargo test

# Run integration tests only
cargo test --test entry_processor_test -- --ignored
cargo test --test aggregation_test -- --ignored
cargo test --test executor_test -- --ignored
cargo test --test transaction_xa_test -- --ignored

# Run all ignored tests
cargo test -- --ignored
```

## Test Categories

| Test File | Description |
|-----------|-------------|
| `entry_processor_test.rs` | Tests for `IMap` entry processors |
| `aggregation_test.rs` | Tests for `IMap` aggregations |
| `executor_test.rs` | Tests for distributed executor service |
| `transaction_xa_test.rs` | Tests for XA transactions |

## CI Pipeline

In CI, the integration tests run after starting the Docker containers:

```bash
# Start Hazelcast
docker-compose -f tests/ci_docker_compose.yml up -d

# Wait for cluster to be ready
sleep 30

# Run integration tests
cargo test -- --ignored

# Cleanup
docker-compose -f tests/ci_docker_compose.yml down
```

## Notes

- Entry processor and executor tests require server-side Java implementations
  of the task/processor classes. Without them, the tests verify protocol 
  encoding but actual execution will fail.
- Transaction tests verify the transaction lifecycle (begin/commit/rollback)
  against a real cluster.
- Aggregation tests work with primitive values stored directly in maps.
