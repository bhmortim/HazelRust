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
- [x] **Poisoning lock-unwraps eliminated** — all 88 production `.lock()/.read()/.write().unwrap()` across the proxy + cache layers now use `.unwrap_or_else(PoisonError::into_inner)`, so a poisoned `std` lock recovers instead of cascading client-wide panics (commit `5980cdc`).
- [x] **`unsafe impl Send/Sync` eliminated** — all three (`Job`, `PagingPredicate`, `QueryCache`) were unnecessary (`Predicate`/`AttributeExtractor` are `: Send + Sync`, the listener alias is `+ Send + Sync`, `PhantomData<fn() -> (K,V)>` is unconditionally `Send + Sync`). Removed so the compiler verifies; production code now has **zero hand-written `unsafe`** (commit `d9cf527`).
- [x] **Remaining non-lock `unwrap`/`expect`/`panic!` audited** (~66 `unwrap`, 11 `expect`, 2 `panic!`): overwhelmingly *not* reachable panics — the SQL response parser (`sql/service.rs`) bounds-checks (`if data.len() < offset + N { return Err }`) before slicing, config parses are on hardcoded constants (`"127.0.0.1:5701".parse()`), and many counted sites are doc-comment examples or test fixtures. The 2 `panic!`s are builder fail-fast on misconfiguration at startup; `IndexConfigBuilder` already exposes a non-panicking `try_build() -> Result`. Auth + invocation-timeout "gaps" were also investigated and are correctly wired (commit `d9cf527`).
- [ ] Minor follow-ups: add `try_build()` to the Kafka builders; route `IndexConfigBuilder::build` through `try_build`; clean the stray non-UTF8 byte + 1 test lock-unwrap in `security/credentials.rs` — *Owner: ___*
- [ ] Add `#![deny(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::indexing_slicing)]` to production modules once residual sites are annotated — *Owner: ___*
- [ ] Run **MIRI** over the unit tests; make TLS + peer auth mandatory/on-by-default — *Owner: ___*

**Exit:** no panicking unwraps reachable from untrusted-input paths (met for locks + audited for the rest); every `unsafe` removed/justified (met); MIRI clean; TLS mandatory.

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

**First real-cluster run (2026-06, AWS) — findings.** A 3-node **Hazelcast 5.7
Enterprise** cluster (CP subsystem enabled, `CPMemberCount=3`, license loaded) was
stood up on AWS (`c5.2xlarge`, us-east-2) and the `#[ignore]`'d integration suite was
run against it. Results so far:
- [x] **IMap / Hash / Set / String / key-mgmt paths:** exercised against the live
  cluster; behavior matches expectations.
- [x] **CP `AtomicLong`: bug found, root-caused, fixed, and VERIFIED on a real cluster
  → [issue #12](https://github.com/bhmortim/HazelRust/issues/12).**
  Against a real CP subsystem, every op returned `0` / silently no-op'd. **Three**
  defects, all in `proxy/atomic_long.rs`, fixed in commit `41316dd`: (1) CP requests
  omitted the Raft `groupId`; (2) the `RaftGroupId` was mis-framed as plain frames
  instead of a `BEGIN/[seed,id]/name/END` data structure; (3) the `hazelcast_core`
  `CP_ATOMIC_LONG_*` message-type constants are mislabeled (`…_ADD_AND_GET = 0x090500`
  is really *Get*), so `add_and_get` was invoking *Get* and never mutating. Fix resolves
  the group via `CPGroupCreateCPGroup` (`0x1E0100`) and uses correct local message types.
  **Verified 2026-06-23 against a 3-node Hazelcast 5.7 *Enterprise* CP cluster (license
  loaded, CP subsystem initialized with 3 CP members): `atomic_long_integration_test`
  17/17 and `atomic_long_test` 11/11 pass — 28/28 — including `concurrent_increments`
  (1000 increments → exactly 1000) and `concurrent_cas` (100 CAS), which prove the ops
  now mutate cluster state atomically.** Note: in Hazelcast 5.7 the CP subsystem is an
  *Enterprise* feature (OSS members refuse to start it), so an EE license is mandatory
  for any CP testing.
- [x] **CP `AtomicReference` had the same `groupId` bug (masked by smoke tests) — now
  fixed and VERIFIED.** `proxy/atomic_reference.rs` built every CP request the pre-fix way
  (object name as a plain frame, **no `RaftGroupId`**, mislabeled `CP_ATOMIC_REFERENCE_*`
  message types, no `Set` `returnOldValue` flag, `null` encoded as an empty frame instead
  of an `IS_NULL_FLAG` frame, and the response value decoded from the wrong frame). Its 11
  old tests passed only because they were smoke tests (`println!`, no value assertions).
  Rewrote the proxy with the same `RaftGroupId` resolve/encode as AtomicLong + the correct
  AtomicRef message types (`Get 0x0A0400`, `Set 0x0A0500` with `returnOldValue`,
  `CompareAndSet 0x0A0200`, `Contains 0x0A0300`, all verified against the Hazelcast client
  protocol codecs) and nullable-`Data` framing, and **rewrote the tests to assert
  round-trip values**. **Verified 2026-06-23 against the same 3-node 5.7 EE CP cluster:
  `atomic_reference_test` 13/13 pass** (set/get, get_and_set returns old, CAS
  success/failure/from-null, contains, is_null, clear, clone-shares-state, i64 values).
- [ ] **Follow-up:** `cp_session.rs` `encode_group_id` also writes plain frames rather
  than the data-structure framing — audit whether any live CP path depends on it, and
  correct the mislabeled `CP_ATOMIC_LONG_*` / `CP_SUBSYSTEM_*` constants at the source in
  `hazelcast-core/src/protocol/constants.rs` (currently only shadowed locally). — *Owner: ___*
- [ ] **Follow-up:** the mislabeled `CP_ATOMIC_LONG_*` / `CP_SUBSYSTEM_*` constants in
  `hazelcast-core/src/protocol/constants.rs` should be corrected at the source (not just
  shadowed locally) and cross-checked against the generated protocol definitions. — *Owner: ___*
- [ ] **Follow-up:** separate live-cluster hangs/failures observed (IMap lock test hang;
  ~15 `java_parity` failures) — triage independently. — *Owner: ___*

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
