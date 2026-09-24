# AGENTS.md

Guidance for AI coding agents working in this repository. Human contributors should
start with [CONTRIBUTING.md](CONTRIBUTING.md).

## What this is

The Hazelcast Rust client (experimental): an async, Tokio-based client for Hazelcast
5.x clusters that speaks the Hazelcast Open Binary Client Protocol.

## Workspace

| Crate | Path | Role |
|-------|------|------|
| `hazelcast-client-core` | `hazelcast-client-core/` | Wire protocol (`ClientMessage`/`Frame`), serialization (Compact, Portable, IdentifiedDataSerializable, JSON, serde), error types |
| `hazelcast-client` | `hazelcast-client/` | Connections, cluster and partition management, data-structure proxies, CP, SQL, transactions, Jet, near cache, config. Re-exports the core crate as `hazelcast_client::core` |
| `hazelcast-client-derive` | `hazelcast-client-derive/` | Derive macros for the serialization traits |
| `hazelcast-client-bench` | `hazelcast-client-bench/` | Benchmark harness against the Java client (`publish = false`); see `bench/README.md` |

Fuzz targets live in `hazelcast-client-core/fuzz/`, a standalone cargo-fuzz workspace
that needs a nightly toolchain.

## Build and test

CI runs these on every push and pull request; build, test, fmt, and deny are blocking:

```sh
cargo build --workspace --locked
cargo test --workspace --locked
cargo fmt --all --check
cargo clippy --all-targets --locked   # observe-only in CI; don't add new warnings
cargo deny check
```

Integration tests that need a live cluster are `#[ignore]`d. Point them at a cluster
with `CLUSTER_ADDRESS`:

```sh
CLUSTER_ADDRESS=127.0.0.1:5701 cargo test -p hazelcast-client -- --ignored
```

## Conventions

- Every source file starts with the Hazelcast Apache 2.0 license header; copy it from
  an existing file.
- Return `hazelcast_client_core::Result` / `HazelcastError` from fallible code; avoid
  `unwrap()` and `expect()` in library code.
- Protocol encoders must match the Java client's wire format; check message types and
  frame layout against the Hazelcast client protocol definitions, not just local
  constants.
- Use conventional commit messages (`feat`, `fix`, `docs`, `refactor`, `test`, `bench`,
  `chore`).
