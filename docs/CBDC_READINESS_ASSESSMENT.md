# HazelRust — Production-Readiness Assessment for Central-Bank / CBDC Use

**Subject:** `github.com/bhmortim/HazelRust` — Rust smart client for Hazelcast
**Question posed:** Is this client ready to manage critical digital currency inside central-bank infrastructure, and if not, what testing and quality process would get it there?
**Reviewer stance:** Senior diligence review. Findings below are grounded in a direct read of the repository at the current `main`, not on the README's claims.
**Date:** 2026-06-22

---

## 1. The one-line answer

**No. It is not ready, and it could not be made ready by fixing a list of bugs.** What is missing is not primarily code — it is the entire verification, governance, and operational-assurance apparatus that a central bank requires before any component touches settlement of real money. HazelRust is a genuinely substantial piece of engineering (≈86,500 lines across three crates, a real Hazelcast binary-protocol implementation, fuzz targets, benchmarks, and 38 integration-test files). But on the evidence in the repository it is a **pre-1.0, single-author, ~3-month-old, AI-assisted library with no released version, no external review, no CI test gate on its main crate, and no distributed-systems correctness testing.** That is an early-stage open-source client. The distance between that and "central banks move sovereign digital currency through it" is measured in years of work and an organization, not a sprint.

This is not a criticism of the ambition or the craft. It is a statement about the bar. CBDC infrastructure sits in the same assurance tier as flight-control and nuclear-safety software. A library earns its way into that tier by *proving* properties under adversarial conditions — and that proof does not yet exist for HazelRust.

---

## 2. What "central-bank CBDC" actually demands (the bar)

Before judging the client, it's worth stating the standard it would be held to. Money-movement infrastructure at a central bank is typically required to demonstrate:

- **Correctness under all orderings and failures** — not "passes tests," but *no* lost, duplicated, or reordered state transition under arbitrary network partitions, member crashes, GC pauses, and clock skew. Settlement finality is non-negotiable.
- **Availability targets of 99.99%+** with defined RTO/RPO, tested failover, and graceful degradation rather than panic.
- **Deterministic, reproducible behavior** — the same inputs must produce the same outputs, and failures must be reproducible for forensic audit.
- **Security and supply-chain integrity** — mandatory transport security, authenticated peers, a threat model, third-party penetration testing, signed/reproducible builds, and a vetted dependency tree with no known CVEs.
- **Governance** — independent code review, segregation of duties, change management, an SLA-backed maintenance commitment, vendor support, and a credible answer to "what happens when the one person who understands this is unavailable."
- **Regulatory alignment** — operational-resilience regimes (e.g. the principles behind BIS/CPMI–IOSCO for financial market infrastructures, and national operational-resilience rules) expect documented, audited, independently-validated resilience for any system on the critical path.

Hold any client to that bar and the question stops being "is the code good?" and becomes "where is the evidence?" For HazelRust, most of that evidence does not exist yet.

---

## 3. What the repository actually shows

### 3.1 Maturity and provenance
- **Version `0.1.0`**, Apache-2.0, edition 2021, MSRV 1.70. No git tags, no releases, no `CHANGELOG`. It has never been cut as a versioned artifact.
- **Single author.** All ~300 commits are by one person. **175 of them land on the first day** (2026-01-15) — a bulk initial generation — and the history runs only to **2026-04-13**, i.e. nothing in roughly the last two months. Bus factor is one.
- **AI-assisted scaffolding, lightly reviewed.** The repo's own `AGENTS.md` "coding style guide" describes a **game engine / GUI framework** — it talks about `on_render`, GPU contexts, window handles, single-threaded `RefCell` interior mutability, and "Window creation." None of that applies to a Tokio-based async network client; the advice to prefer single-threaded `RefCell` is the opposite of what this codebase needs. A contributor guide this mismatched is strong evidence that generated material was committed without careful human review.
- **Positioning overclaim.** The README bills HazelRust as *"The official Rust client for Hazelcast."* It is a personal repository; calling it "official" is inaccurate and is itself the kind of thing a procurement/diligence process flags immediately.

### 3.2 CI is not a quality gate
There are exactly three GitHub workflows: `benchmarks.yml`, `coverage.yml`, `fuzz.yml`. Consequently:
- **There is no build/test workflow.** Nothing runs `cargo test` for the main `hazelcast-client` crate, the 73,799-line crate where all the proxy, connection-management, transaction, CP-subsystem, and near-cache logic lives. Its ~2,000+ unit tests are **never executed in CI**. The crate is only *compiled* in CI as a side-effect of the benchmark job.
- **No lint or format gate** — no `cargo clippy -D warnings`, no `cargo fmt --check`, despite the project's stated standards.
- **No supply-chain scanning** — no `cargo audit`, no `cargo deny`, no SBOM, no dependency-CVE gate.
- **Coverage is measured on the wrong 14%.** The coverage job runs only `--package hazelcast-core` (12,089 lines) and runs it with `--ignore-run-fail` and `fail_ci_if_error: false`, so even failing tests don't fail the build. Coverage on the 74k-line client crate is zero-by-construction.
- **The fuzz workflow is broken.** It references `dtolnay/rust-action@nightly`, which is not a real action (the canonical one is `dtolnay/rust-toolchain`), so the job fails at toolchain setup. Even if fixed, it only runs **30 seconds per target** and only on PRs that touch protocol/serialization paths. Thirty seconds is not fuzzing; meaningful fuzzing is continuous and measured in CPU-days.
- **Benchmarks run on shared GitHub runners**, which are noisy multi-tenant VMs — unsuitable as a source of truth for latency/throughput claims.

### 3.3 Test reality
- 38 integration-test files exist, but **165 tests are `#[ignore]`'d** because they "require a running Hazelcast cluster." Nothing in CI starts a cluster, so the failover, TLS, CP-subsystem, transaction, and end-to-end functional tests **do not run in automation** — they can only be run by hand, on a developer's machine, when someone remembers to. For a system whose entire value is correct behavior against a real cluster under failure, the tests that would prove that are exactly the ones that don't run.
- No evidence of property-based testing harnesses driving the protocol invariants, no Jepsen-style linearizability suite, no deterministic simulation, no chaos/fault-injection harness, no long-running soak tests.

### 3.4 Code-safety signals (assessed fairly)
I separated production code from embedded `#[cfg(test)]` code before counting, because the raw numbers are misleading:
- **Panics:** ~149 `.unwrap()`, 15 `.expect(...)`, and 8 `panic!/unreachable!/unimplemented!` in **production** paths (the other ~1,460 unwraps are in test modules and don't matter). Of the 149, **89 are lock-guard acquisitions** (`.lock()/.read()/.write().unwrap()`) — the conventional Rust idiom, not reckless parsing of network bytes. So the code is more disciplined than a naive grep suggests. **However**, for a "must never crash the host process" library this is still a real exposure: `std` mutexes *poison* on panic, so any one panic while holding a lock turns into a cascade of panics on every subsequent `.lock().unwrap()`. And using blocking `std` mutexes in async hot paths (e.g. `proxy/map.rs`) is itself a latency/deadlock hazard. A high-assurance build eliminates panicking unwraps from production paths entirely (deny-by-lint), and uses non-poisoning or async-aware locks.
- **`unsafe`:** 7 occurrences. Six are **manual `unsafe impl Send/Sync`** on `Job`, `PagingPredicate`, and `QueryCache` — these override the compiler's thread-safety analysis and each needs a written, reviewed soundness argument; if any is wrong, it's a latent data race. One `unsafe { std::mem::zeroed::<K>() }` exists but is inside a `#[test]` type-trick, not a production path (still a bad pattern to keep, but not a runtime hazard).
- **TLS is optional/feature-gated** (`tokio-rustls` behind a feature). The default build can speak plaintext. For CBDC, transport security and peer authentication must be mandatory and on by default.
- **No `SECURITY.md`**, no disclosure policy, no threat model.

None of these individually is unusual for a young library. Collectively, against the CBDC bar, they describe a codebase that has not yet been *hardened* — only *written*.

---

## 4. How I would test it — the assurance regime to reach the bar

This is the substance of the question: *what testing, exactly, and how do you ensure quality?* The work falls into twelve layers. Each has a concrete exit criterion in §5. The ordering is roughly the order I'd build them, because later layers depend on earlier ones.

### Layer 0 — Make CI a real gate (days, not months; do this first)
Nothing else is trustworthy until the basics run on every commit:
- `cargo build`, `cargo test --workspace` (all crates), `cargo clippy --all-targets --all-features -D warnings`, `cargo fmt --check`.
- `cargo audit` + `cargo deny` (licenses, bans, advisories) with the build failing on any advisory.
- Coverage on **all** crates, with `fail_ci_if_error: true` and a minimum threshold that ratchets upward.
- Pin the toolchain, vendor/lock dependencies, and produce an SBOM (CycloneDX) per build.

### Layer 1 — Static assurance & code hygiene
- **Zero-panic discipline** in production paths: lint against `unwrap/expect/panic/unreachable/indexing` in non-test code (`clippy::unwrap_used`, `disallowed-methods`), convert all to typed errors. Lock acquisition must not be able to crash the process.
- **MIRI** over the unit tests to catch undefined behavior; **a documented soundness proof for every `unsafe impl`**, ideally removing them by restructuring so the compiler can prove `Send/Sync`.
- Mandatory transport security: TLS + optional mTLS on by default, certificate validation tests, no plaintext fallback in the supported configuration.

### Layer 2 — Unit + property-based testing
- Run the existing unit tests in CI (they currently don't run for the client crate).
- **Property tests** (`proptest`/`quickcheck`) for every serialization codec — Compact, Portable, Identified, the frame/`ClientMessage` layer — asserting round-trip identity (`decode(encode(x)) == x`), and decode-never-panics on arbitrary bytes. This is where subtle wire-format bugs hide, and it's the cheapest high-yield testing available here.

### Layer 3 — Differential / conformance testing against the reference client
- The repo already has a `java_parity_test.rs`. Turn that into a **systematic differential harness**: drive the same operation sequences through the official Java (or .NET/Go) client and through HazelRust against the same cluster, and assert byte-identical protocol frames and identical observable results. The Hazelcast binary protocol is the contract; conformance to the reference implementation is the strongest correctness signal short of formal proof.

### Layer 4 — Continuous fuzzing
- Fix the broken fuzz workflow, then run the five existing targets **continuously** (OSS-Fuzz-style, CPU-days not 30 seconds), with structure-aware/coverage-guided fuzzing and a persistent corpus. Fuzz every parser that touches untrusted bytes (server responses are untrusted from the client's perspective). Track crash-free CPU-hours as a release metric.

### Layer 5 — Integration testing against real clusters, in CI
- Stand up real Hazelcast clusters in CI with `testcontainers`, and **run all 165 currently-ignored tests** on every PR.
- Test a **matrix** of Hazelcast server versions (the versions the bank runs, plus N-1/N+1), RESP/protocol versions, TLS on/off, and cluster sizes (single member through 5+).

### Layer 6 — Distributed-systems correctness (the heart of it)
This is the layer that separates "a client that works on my laptop" from "a client a central bank trusts," and it's the layer that is entirely absent today:
- **Jepsen-style testing**: drive concurrent workloads while injecting partitions, and check the histories for **linearizability** (for the CP/Raft-backed structures — `AtomicLong`, `AtomicReference`, CP maps, locks) and the documented consistency model for AP structures. Verify the client's claimed guarantees for retries: are operations **idempotent**, and is "at-least-once" vs "exactly-once" semantics actually what happens when a request is retried across a failover? For money, a silently duplicated operation is a catastrophe.
- **Deterministic Simulation Testing (DST)**, in the style of FoundationDB/TigerBeetle: run the client against a simulated network and cluster with a seeded RNG so that partitions, message reordering, delays, and crashes are *reproducible*. This lets you explore millions of failure interleavings and replay any failure exactly. For a money-path component this is, in my view, the single highest-value investment.
- Explicitly test: leader failover mid-operation, split-brain and merge, member add/remove during traffic, partition-table migration under load, connection storms, and slow/stuck members.

### Layer 7 — Fault injection & chaos
- Inject latency, jitter, packet loss, connection resets, half-open connections, TLS renegotiation, DNS failures, GC-pause simulation, and clock skew. Verify timeouts, backoff, reconnection, and heartbeat logic do the right thing and never wedge or panic. Run these continuously against a staging cluster, not just once.

### Layer 8 — Performance, load & endurance ("highest standards on earth")
Performance for a money system is about **tail latency and stability over time**, not peak throughput:
- Measure **p50/p99/p999/p9999 and max** latency under sustained, realistic load on **dedicated, isolated hardware** (never shared CI runners). Define explicit SLOs and fail the release if tails regress.
- **Soak/endurance tests of 72 hours+** to surface memory leaks, FD leaks, unbounded queue growth, and slow degradation. Track RSS and heap over time (`heaptrack`/`valgrind massif`); require flat memory.
- **Backpressure and overload behavior**: the client must degrade gracefully (bounded buffers, shed load) rather than OOM or melt down under a connection/traffic spike.
- Establish a **performance-regression gate** with statistically sound comparisons (the current `critcmp` approach is reasonable, but must run on stable hardware).

### Layer 9 — Concurrency correctness
- **`loom`** to exhaustively model-check the lock/atomic/channel logic in the connection manager and caches under all thread interleavings.
- **ThreadSanitizer** and **AddressSanitizer** runs over the test suite.
- Prove or remove every `unsafe impl Send/Sync` (Layer 1 ties in here).

### Layer 10 — Security assurance
- A written **threat model** and **independent third-party penetration test / security audit** of the client and its TLS/auth paths.
- Secrets handling review (no credentials in logs, zeroization of key material), fuzzing of all untrusted inputs (Layer 4), and a **supply-chain story**: pinned dependencies, reproducible builds, signed artifacts, and ideally SLSA-level provenance.
- A `SECURITY.md` with a disclosure process and a committed patch SLA.

### Layer 11 — Operational readiness
- Observability that an SRE team can run on: structured logs, metrics (the Prometheus example is a start), and traces for every operation, with documented dashboards and alerts.
- **Runbooks** for every failure mode, tested **upgrade/rollback** procedures, and **disaster-recovery drills** with measured RTO/RPO.
- Capacity planning and documented limits.

### Layer 12 — Governance & process (the part that isn't code)
- Independent code review with **at least two reviewers** and segregation of duties; no single author on the money path.
- A funded, SLA-backed **long-term maintenance commitment** (or commercial support), because bus-factor-one is itself an unacceptable operational risk.
- A formal, audited **sign-off** mapping evidence to the bank's model-risk-management and operational-resilience frameworks before go-live, plus change-management for every subsequent release.

---

## 5. How I'd define "ready" — exit criteria

I would not let it near production money until **all** of these are demonstrably, continuously true:

1. CI gates every commit: build + full test (all crates) + clippy(-D warnings) + fmt + `cargo audit`/`deny`, all green, with SBOM produced.
2. Zero `unwrap/expect/panic` reachable from production paths; locks cannot crash the process; every `unsafe` has a reviewed soundness proof or is gone.
3. ≥90% line/branch coverage on **both** crates, with the integration suite (all 165 currently-ignored tests) running in CI against a real-cluster matrix.
4. Property tests prove serialization round-trip and decode-never-panics; a differential harness shows protocol parity with the reference client.
5. Continuous fuzzing with a maintained corpus and a tracked crash-free-CPU-hours metric trending up.
6. A Jepsen suite passes for the claimed consistency/linearizability guarantees, **and** a deterministic-simulation harness can reproduce and survive injected partition/crash/reorder schedules; retry idempotency is proven.
7. 72h+ soak passes with flat memory and no FD leaks; p999/p9999 latency SLOs met on dedicated hardware with a regression gate.
8. Independent security audit and penetration test passed; TLS/mTLS mandatory; signed, reproducible builds.
9. Runbooks, observability, and tested upgrade/rollback + DR drills exist.
10. Independent review, a real maintenance/support commitment, and a formal risk sign-off are on file.

A practical sequencing: **Phase A (weeks)** — Layer 0–2 and the security basics; this is cheap and removes most of the embarrassing risk. **Phase B (a few months)** — Layers 3–5 and 8–9; real correctness and performance evidence. **Phase C (the long pole, 6–18 months with a team)** — Layer 6 distributed-correctness, Layer 10 audit, Layers 11–12 operational and governance. Phase C is where most of the calendar time and almost all of the assurance value live, and it cannot be rushed.

---

## 6. Scope caveat — the client is one link

Even a perfect client does not make a CBDC platform. The same scrutiny has to land on Hazelcast itself (an in-memory data grid as a system-of-record for sovereign money raises hard questions about durability, settlement finality, and immutable audit trail that belong in an architecture review), on the Patina proxy layer, on the network, and on the operating organization. A central bank would typically also demand **defense in depth**: reconciliation, an independent ledger of record, and the ability to detect and recover from a component silently doing the wrong thing — precisely because no single component's testing is ever assumed to be perfect.

---

## 7. Bottom line

HazelRust is impressive for what it is: a broad, working, AI-accelerated Hazelcast client that one person built in about three months. If someone asked me "is this a promising open-source client worth maturing?" I'd say yes. But the question asked is whether it can carry critical digital currency for a central bank, and the honest answer is **no — not today, and not by patching a few issues.** The blocker is the absence of the proof: no CI test gate on the main crate, no distributed-systems correctness testing, no continuous fuzzing, no soak/tail-latency evidence on real hardware, no security audit, no released version, no independent review, and bus-factor-one governance. Quality at this tier is not asserted; it is *demonstrated*, continuously, under adversarial conditions — and that demonstration is the multi-phase program in §4. Until that program exists and stays green, the correct recommendation to a central bank is: do not put it on the money path.
