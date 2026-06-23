# HazelRust — Production-Hardening Execution Plan

This is the working tracker for taking HazelRust from an early-stage client toward
the assurance bar required for high-stakes (up to central-bank / CBDC) use. It is
derived from `docs/CBDC_READINESS_ASSESSMENT.md`, which holds the full rationale.

**How to read this:** work is grouped into three phases. Phase A is cheap, high-value,
and shippable now. Phase B builds real correctness and performance evidence. Phase C —
distributed-systems correctness, external audit, and governance — is the long pole and
holds almost all of the assurance value. Check items off as their exit criteria are met.

Status legend: `[ ]` not started · `[~]` in progress · `[x]` done
Owner: fill in a name; **no item on the money path may be reviewed by only its author.**

---

## Phase A — Make quality measurable (days–weeks)

### Layer 0 — CI is a real gate
- [x] Add `ci.yml`: `build` + `test` blocking on stable for every push/PR — *done in this branch*
- [x] Wire `fmt`, `clippy`, `cargo-audit`, `cargo-deny` in observe mode — *done in this branch*
- [x] Add `deny.toml` (advisories / licenses / bans / sources) — *done in this branch*
- [x] Fix broken fuzz workflow (`dtolnay/rust-action` → `rust-toolchain`) + nightly schedule — *done in this branch*
- [ ] First CI run green; triage the observe-mode output (fmt/clippy/audit/deny baseline) — *Owner: ___*
- [ ] **Ratchet:** flip `fmt` to blocking once `cargo fmt --all` is clean — *Owner: ___*
- [ ] **Ratchet:** flip `clippy` to `-D warnings` once the warning backlog is cleared — *Owner: ___*
- [ ] **Ratchet:** flip `audit` + `deny` to blocking once the tree is clean — *Owner: ___*
- [ ] Add coverage for **both** crates (today only `hazelcast-core`), remove `--ignore-run-fail`, set `fail_ci_if_error: true`, add a ratcheting threshold — *Owner: ___*
- [ ] Produce an SBOM (CycloneDX) per build; pin toolchain via `rust-toolchain.toml` — *Owner: ___*

**Exit:** every commit is gated by build + test + fmt + clippy(-D warnings) + audit + deny, all green; coverage measured on both crates.

### Layer 1 — Static assurance & no-panic discipline
- [ ] Inventory the ~149 production `.unwrap()` / `.expect()` / `panic!` sites; convert to typed errors — *Owner: ___*
- [ ] Replace poisoning `std::sync::Mutex` lock-unwraps in async hot paths (esp. `proxy/map.rs`) with non-poisoning / async-aware locks — *Owner: ___*
- [ ] Add `#![deny(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::indexing_slicing)]` to production modules (test code exempt) — *Owner: ___*
- [ ] Write a documented soundness argument for every `unsafe impl Send/Sync` (`Job`, `PagingPredicate`, `QueryCache`); remove where restructuring lets the compiler prove it — *Owner: ___*
- [ ] Run **MIRI** over the unit tests; fix any UB (incl. the `mem::zeroed::<K>()` test trick) — *Owner: ___*
- [ ] Make TLS + peer auth mandatory/on-by-default in the supported config; no plaintext fallback — *Owner: ___*

**Exit:** zero panicking unwraps reachable from production paths; every `unsafe` justified; MIRI clean; TLS mandatory.

### Layer 2 — Unit + property tests
- [ ] Run the `hazelcast-client` unit suite in CI (today it never runs) — *Owner: ___*
- [ ] Property tests (`proptest`) for every serializer — Compact, Portable, Identified, frame/`ClientMessage` — asserting `decode(encode(x)) == x` and decode-never-panics on arbitrary bytes — *Owner: ___*

**Exit:** round-trip + never-panic properties green in CI for all codecs.

---

## Phase B — Real correctness & performance evidence (months)

### Layer 3 — Differential / conformance testing
- [ ] Turn `java_parity_test.rs` into a systematic differential harness vs. the official Java (or .NET/Go) client against the same cluster; assert identical protocol frames and observable results — *Owner: ___*

### Layer 4 — Continuous fuzzing
- [ ] Stand up OSS-Fuzz-style continuous fuzzing (CPU-days, not minutes) with a persistent corpus; track crash-free CPU-hours as a release metric — *Owner: ___*

### Layer 5 — Integration tests against real clusters, in CI
- [ ] Run all 165 currently-`#[ignore]`'d tests in CI via `testcontainers` — *Owner: ___*
- [ ] Matrix: Hazelcast server versions (N-1/N/N+1), TLS on/off, cluster sizes 1→5+ — *Owner: ___*

### Layer 8 — Performance, load & endurance
- [ ] Move perf measurement off shared CI runners onto dedicated, isolated hardware — *Owner: ___*
- [ ] Define and gate p50/p99/p999/p9999/max latency SLOs with statistically sound regression checks — *Owner: ___*
- [ ] 72h+ soak with flat memory (heaptrack/massif), no FD/queue growth — *Owner: ___*
- [ ] Backpressure / overload tests: graceful degradation, no OOM under connection/traffic storms — *Owner: ___*

### Layer 9 — Concurrency correctness
- [ ] `loom` model-check the connection-manager / cache lock+atomic+channel logic — *Owner: ___*
- [ ] ThreadSanitizer + AddressSanitizer runs over the suite — *Owner: ___*

---

## Phase C — Distributed correctness, security & governance (the long pole, 6–18 months)

### Layer 6 — Distributed-systems correctness
- [ ] Jepsen-style suite: linearizability for CP/Raft structures (`AtomicLong`, `AtomicReference`, CP maps, locks) and the documented model for AP structures, under injected partitions — *Owner: ___*
- [ ] Prove retry semantics: idempotency / exactly-once vs. at-least-once across failover (a duplicated money op is catastrophic) — *Owner: ___*
- [ ] Deterministic Simulation Testing (FoundationDB/TigerBeetle-style): seeded, reproducible partition/crash/reorder schedules; replay any failure exactly — *Owner: ___*
- [ ] Targeted failures: leader failover mid-op, split-brain + merge, member add/remove under load, partition-table migration, slow/stuck members — *Owner: ___*

### Layer 7 — Fault injection & chaos
- [ ] Continuous injection of latency, loss, resets, half-open conns, TLS renegotiation, DNS failures, GC pauses, clock skew against staging — *Owner: ___*

### Layer 10 — Security assurance
- [ ] Written threat model; independent third-party pen test / security audit of client + TLS/auth — *Owner: ___*
- [ ] Secrets handling review (no creds in logs, key zeroization) — *Owner: ___*
- [ ] Supply chain: pinned deps, reproducible + signed builds, SLSA provenance — *Owner: ___*
- [ ] Add `SECURITY.md` with a disclosure process and patch SLA — *Owner: ___*

### Layer 11 — Operational readiness
- [ ] Observability: structured logs, metrics, traces per op; documented dashboards + alerts — *Owner: ___*
- [ ] Runbooks for every failure mode; tested upgrade/rollback; DR drills with measured RTO/RPO — *Owner: ___*

### Layer 12 — Governance & process
- [ ] Independent review with ≥2 reviewers; segregation of duties; no single author on the money path — *Owner: ___*
- [ ] Funded, SLA-backed long-term maintenance/support commitment (resolve bus-factor-1) — *Owner: ___*
- [ ] Formal, audited risk sign-off mapping evidence to the institution's model-risk & operational-resilience frameworks before any go-live — *Owner: ___*
- [ ] Cut a versioned, tagged `0.x`/`1.0` release with a `CHANGELOG`; correct the README "official client" claim — *Owner: ___*

---

## Definition of "ready" (exit criteria — all must be continuously true)

1. CI gates every commit: build + full test (all crates) + clippy(-D warnings) + fmt + audit + deny, green; SBOM produced.
2. Zero panicking unwraps in production paths; locks can't crash the process; every `unsafe` justified.
3. ≥90% line/branch coverage on both crates; the full integration suite runs in CI against a real-cluster matrix.
4. Property tests prove serialization round-trip + decode-never-panic; differential harness shows protocol parity.
5. Continuous fuzzing with a maintained corpus; crash-free-CPU-hours trending up.
6. Jepsen passes for the claimed guarantees; deterministic simulation reproduces and survives injected failures; retry idempotency proven.
7. 72h+ soak passes with flat memory; p999/p9999 SLOs met on dedicated hardware with a regression gate.
8. Independent security audit + pen test passed; TLS/mTLS mandatory; signed, reproducible builds.
9. Runbooks, observability, tested upgrade/rollback + DR drills exist.
10. Independent review, a real maintenance/support commitment, and a formal risk sign-off are on file.

**Until all ten hold and stay green, the recommendation for a central-bank money path is: do not deploy.**
