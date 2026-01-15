# Contributing to Hazelcast Rust Client

Thank you for your interest in contributing! This document provides guidelines for contributing to the Hazelcast Rust client.

## Getting Started

### Prerequisites

- Rust 1.70 or later
- A running Hazelcast cluster for integration tests
- Docker (optional, for running Hazelcast locally)

### Development Setup

This project is organized as a Cargo workspace with two crates:

| Crate | Description |
|-------|-------------|
| `hazelcast-core` | Core types, protocols, and serialization |
| `hazelcast-client` | Client implementation and connection management |

```bash
# Clone the repository
git clone https://github.com/hazelcast/hazelcast-rust-client.git
cd hazelcast-rust-client

# Build the entire workspace
cargo build

# Run tests for all crates (requires a Hazelcast cluster on localhost:5701)
cargo test

# Run with all features
cargo test --all-features

# Build/test a specific crate
cargo build -p hazelcast-core
cargo test -p hazelcast-client
```

### Running Hazelcast Locally

```bash
docker run -d --name hazelcast \
  -p 5701:5701 \
  hazelcast/hazelcast:5.3
```

## Code Style

### Formatting

All code must be formatted with `rustfmt`:

```bash
cargo fmt --all
```

### Linting

Code must pass `clippy` without warnings:

```bash
cargo clippy --all-targets --all-features -- -D warnings
```

### Style Guidelines

- Use `snake_case` for functions and variables
- Use `PascalCase` for types and traits
- Prefer `&str` over `String` in function parameters where possible
- Use `Result<T, HazelcastError>` for fallible operations
- Document all public APIs with `///` doc comments
- Include examples in doc comments where helpful

### Error Handling

- Use the `?` operator for error propagation
- Provide context in error messages
- Avoid `unwrap()` and `expect()` in library code

## Testing

### Unit Tests

Place unit tests in the same file as the code they test:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_feature() {
        // ...
    }
}
```

### Integration Tests

Integration tests go in `tests/` directories and require a running Hazelcast cluster:

```rust
#[tokio::test]
async fn test_map_operations() {
    let client = create_test_client().await;
    // ...
}
```

Run integration tests:

```bash
cargo test --test '*'
```

### Benchmarks

Benchmarks use Criterion and are in `benches/`:

```bash
cargo bench
```

## Pull Request Process

1. **Fork** the repository and create your branch from `main`
2. **Write tests** for any new functionality
3. **Update documentation** if you're changing APIs
4. **Run the full test suite** locally
5. **Submit a pull request** with a clear description

### PR Checklist

- [ ] Code compiles without warnings
- [ ] All tests pass
- [ ] Code is formatted with `rustfmt`
- [ ] No `clippy` warnings
- [ ] Documentation updated (if applicable)
- [ ] CHANGELOG.md updated (for user-facing changes)

### Commit Messages

Use conventional commit format:

```
type(scope): description

[optional body]

[optional footer]
```

Types:
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation only
- `refactor`: Code change that neither fixes a bug nor adds a feature
- `test`: Adding or updating tests
- `bench`: Benchmark changes
- `chore`: Build process or auxiliary tool changes

Examples:
```
feat(map): add TTL support for put operations
fix(connection): handle reconnection on cluster restart
docs(readme): add near cache configuration example
```

## Reporting Issues

### Bug Reports

Include:
- Rust version (`rustc --version`)
- Hazelcast server version
- Operating system
- Minimal reproduction code
- Expected vs actual behavior
- Full error messages and stack traces

### Feature Requests

Include:
- Use case description
- Proposed API (if applicable)
- References to related Hazelcast features

## Architecture Overview

```
hazelcast-rust-client/
├── Cargo.toml              # Workspace manifest
├── hazelcast-core/         # Core crate
│   ├── Cargo.toml
│   └── src/
│       ├── lib.rs          # Public API exports
│       ├── protocol/       # Hazelcast protocol implementation
│       ├── serialization/  # Data serialization
│       └── types/          # Common types and errors
├── hazelcast-client/       # Client crate
│   ├── Cargo.toml
│   └── src/
│       ├── lib.rs          # Public API exports
│       ├── client.rs       # Client entry point
│       ├── config.rs       # Configuration builders
│       ├── proxy/          # Data structure proxies (IMap, IQueue, etc.)
│       ├── connection/     # Network layer (TCP, TLS, WebSocket)
│       ├── listener/       # Event listener infrastructure
│       ├── cache/          # Near cache implementation
│       ├── sql/            # SQL service
│       ├── query/          # Predicate API
│       ├── executor/       # Executor services
│       └── transaction/    # Transaction support
│   ├── tests/              # Integration tests
│   ├── benches/            # Benchmarks
│   └── examples/           # Example programs
```

## License

By contributing, you agree that your contributions will be licensed under the Apache License 2.0.
