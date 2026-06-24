# HazelRust — Production-Readiness Assessment (CBDC / Central-Bank Use)

**Verdict: NO-GO**

| | |
|---|---|
| **Subject** | HazelRust Rust smart-client for Hazelcast (Open Binary Client Protocol) |
| **Commit audited** | `origin/main` = `e6386d2a563f0cace203053ba108220817ab56ff` |
| **Intended role** | Backend of the Patina Redis-compatible proxy, holding/serving CBDC ledger & cache data against a Hazelcast Enterprise cluster |
| **Assessment date** | 2026-06-24 |
| **Cluster under test** | 3× Hazelcast **Enterprise 5.7.0** (Docker), one cluster `dev`, `127.0.0.1:5701/2/3`, on AWS (16 vCPU / 31 GB) |
| **Assessor** | Independent production-readiness validation (adversarial; every prior claim treated as unverified) |

---

## 1. Executive summary

HazelRust is a broad, feature-rich, well-organized client that **compiles cleanly, formats cleanly, comfortably meets the latency/throughput SLA, and survives a benign single-member loss with zero data loss**. Its primary request/response path now correctly surfaces server exceptions as typed errors, and its non-transactional and basic transactional IMap paths work against a live Enterprise cluster.

It is nonetheless **not fit for a central-bank/CBDC money path**, and not by a small margin. Independent verification — adversarial static audit (107 sub-agents, every finding double-verified), source reading with my own eyes, and live tests against the Enterprise cluster — found **multiple independent, individually-disqualifying defect classes** for a ledger:

1. **Broken distributed-transaction atomicity.** The `XAResource` implementation discards the server response on `commit`/`rollback` and **unconditionally reports success**; `prepare` decodes an error response's header bytes as the XA vote (defaulting to `XA_OK`). A failed two-phase commit is reported as committed → divergent ledger state with no error. *(Confirmed at source.)*
2. **Retry can double-apply a money mutation.** `redo_operation` retries non-idempotent IMap mutations with **no idempotency/dedup token**, and client-side timeout / connection-lost (the ambiguous "request sent, response lost" case) is classified retryable. A transient blip during a `put`/`remove`/`getAndAdd` can silently apply it 2–4×, returning success. *(Confirmed at source.)*
3. **Silent data corruption / loss.** The `Serializable::type_id()` default is **STRING (-11)**, so `byte[]`, `u8/u16/u32/u64`, and `HazelcastJsonValue` are sent mislabeled (cross-client corruption, broken SQL/predicate queries). `values()`/`key_set()` **silently drop** any element that fails to deserialize (a balance scan returns a short, wrong-but-plausible total with no error). *(Confirmed at source.)*
4. **The mTLS security gate — the one formal gate you set — is not actually enforced.** Hostname verification is performed against the member **IP literal, not its DNS identity** (`server_name` is always `None`); the `HostnameVerification`/`verify_hostname` knobs are **dead config never read in the handshake**; and `SecurityConfig`'s `Debug` prints **password and token in cleartext**. *(Confirmed at source.)*
5. **Scrambled / colliding protocol constants on reachable paths.** The `CP_SUBSYSTEM_*` family is bound to the wrong service id (`0x1E` CPGroup, not `0x22` CPSubsystem), so CP-management calls invoke the wrong operations; four live `MAP_*` constants collide with unrelated operations (interceptors silently no-op; partition-lost — i.e. data-loss — listeners are never installed). *(Confirmed against upstream codecs.)*

Layered on top: error-swallowing on the XA/executor/auth secondary invocation paths; unbounded attacker-controlled allocations in the Compact/Portable decoders (process-abort DoS); and a **test suite that systematically overstates assurance** (cluster-absent runs report green, money-path tests are 100% `#[ignore]`d, XA tests assert client-side enums but never the data effect, no wire-format golden vectors).

The project's own `EXECUTION_PLAN.md` concludes "**do not deploy**" for a central-bank money path. This independent assessment **corroborates that conclusion with specific, reproducible evidence.**

**Verdict: NO-GO.** A CONDITIONAL-GO is not appropriate at this maturity: the defects are foundational (atomicity, idempotency, type fidelity, transport authentication), span multiple subsystems, and several are masked by tests that pass. Remediation is a multi-month effort (see §8) followed by re-validation — consistent with the project's own Phase-B/C roadmap.

---

## 2. Acceptance bar (confirmed with stakeholder, Step 0)

| Dimension | Bar |
|---|---|
| **In scope** | IMap K-V core; full CP subsystem (AtomicLong/Ref, FencedLock, CountDownLatch, Semaphore, CPMap); Transactions (Txn + XA); Cache/query/listeners/pub-sub |
| **Consistency** | CP (linearizable) for ledger; AP IMap (sync backups) for cache only. Client must never silently lose/corrupt a committed write. |
| **Failover** | Single-member loss (RPO=0, RTO≤s); CP leader failover (exactly-once); network partition/split-brain (no stale/corrupt/double-apply; minority fails safe); AZ/multi-member loss |
| **Security gate (formal)** | mTLS with full cert chain + hostname validation. *(Due-diligence, not hard gate: cargo audit/deny, auth-required, no secret leakage.)* |
| **SLA** | p99 ≤ 10 ms, p999 ≤ 50 ms, ≥ 10k TPS sustained (single-client, single-key, client-side) |
| **Chaos auth** | Destructive allowed; restore to healthy 3-node when done |
| **Soak** | 30–60 min |

---

## 3. Methodology & environment

- **Provenance.** Verified the canonical local clone and the AWS instance clone are both exactly at `origin/main` (`e6386d2`); instance working tree pristine (zero diff). A stale second local clone (`f902b86`) was disregarded. After testing, the instance tree was **restored to pristine `e6386d2`** (scratch test files removed; the two tracked files touched by a benchmark test / my nextest profile reverted via `git checkout`).
- **Build & static gates** run on the instance (no local Rust toolchain): `cargo fmt`, `cargo build`, `cargo clippy`, `cargo audit`, `cargo deny`.
- **Test suite** run with a strict nextest profile: **retries=0** (so flakiness/non-idempotency is never masked), **fail-fast=off** (see every failure), **hang-termination** (so one hang can't stall the run). Unit/non-ignored tests run parallel; cluster integration tests run **serially** to remove cross-test interference as a confound.
- **Adversarial static audit** — a multi-agent workflow (9 dimensions × find→verify, 107 agents) read the source and cross-checked against the upstream Hazelcast protocol; **every finding was independently re-verified by ≥1 skeptic agent** (Critical/High by two, with distinct correctness- and reachability-lenses). Full digest: `AUDIT_DIGEST.md`.
- **Independent confirmation.** The decisive Criticals were re-read by hand against the actual source (not taken on the workflow's word), and the most impactful behaviors were tested **live** against the Enterprise cluster with assertion-bearing tests the existing suite lacks.
- **Secret hygiene.** The EE license and GitHub token were never printed, logged, or committed. (Separately, a client **defect** that leaks *cluster* credentials via `Debug` was found — §6, RR-12.)

**Evidence trail:** `EVIDENCE_LOG.md` (chronology), `AUDIT_DIGEST.md` (all 63 audit findings), this report's §7 (commands + outputs).

---

## 4. Criteria assessment

Legend: **PROVEN-LIVE** (demonstrated against the cluster) · **CONFIRMED-AT-SOURCE** (read in code) · **AUDIT-VERIFIED** (adversarial static, double-checked) · **PARTIAL** · **GAP** (not exercised).

| # | Criterion | Status | Result |
|---|---|---|---|
| 1 | Protocol correctness & wire fidelity | **FAIL** (AUDIT-VERIFIED) | Scrambled CP_SUBSYSTEM service id; 4 colliding MAP_* constants; default type-id STRING; affinity-key routing wrong; PartitionService uses SipHash not MurmurHash3. Mislabeled CP_ATOMIC_* source constants are dead (shadowed) — latent. Full Java byte-diff = **GAP**. |
| 2 | Data integrity (no silent loss/corruption) | **FAIL** (CONFIRMED-AT-SOURCE) | Wrong type-ids for byte[]/unsigned/JSON; `values()`/`key_set()` silently drop undeserializable elements; CPMap value decode misses the 8-byte Data header. Happy-path homogeneous round-trips up to 64 MB / 5000 entries are exact (PROVEN-LIVE) — the corruption is conditional (mixed types, cross-client, partial-deserialize), not happy-path. |
| 3 | Consistency & durability | **FAIL** (CONFIRMED-AT-SOURCE) | XA commit/rollback report false success; XA prepare reads error frame as vote. Non-XA `TransactionContext` commit/rollback **works** (PROVEN-LIVE, Run B). CP `AtomicLong` concurrent-increment correctness passed (Run B). CP linearizability under partition = **GAP**. |
| 4 | Concurrency & atomicity under contention | **FAIL** (CONFIRMED-AT-SOURCE) | Retry double-apply with no idempotency token; correlation-id reuse can cross-match a stale response to a retry. CAS happy-path passed live. Contention/lost-update stress = **GAP**. |
| 5 | Resilience & failover | **PARTIAL / FAIL** | Single-member loss: **RPO=0, no corruption, fast recovery (PROVEN-LIVE)**. But ambiguous-failure double-apply (the dangerous case) is a confirmed code defect, unproven live; CP-leader failover, split-brain/partition, AZ loss = **GAP**. |
| 6 | Memory & resource safety | **PARTIAL** | Unbounded `Vec::with_capacity(attacker_len)` in Compact/Portable decoders → process abort/OOM on one malformed frame (CONFIRMED-AT-SOURCE). No production hand-written `unsafe` (the one `mem::zeroed` is dead test code). Live fuzzing & soak = **GAP** (cargo-fuzz needs nightly; not run). |
| 7 | Security (mTLS gate) | **FAIL** (CONFIRMED-AT-SOURCE) | Hostname verified against IP not DNS; verification knobs dead; default trusts public WebPKI roots (no private-PKI pinning); credentials leak via `Debug`. TLS is **not a default feature** (`default=[]`) → default build is plaintext-only. Live mTLS handshake test = **GAP** (no TLS listener stood up). |
| 8 | Performance & capacity | **PASS** (PROVEN-LIVE) | PUT p99 175 µs / p999 216 µs; GET p99 112 µs; 18,409 ops/s @ concurrency 64. All bars met with large margin. Loopback (no network RTT); saturation/degradation stress = partial. |
| 9 | Observability & operability | **PARTIAL** | `tracing` present; metrics module exists. But errors are swallowed on secondary paths; auth failures only logged; credential leakage in logs. Health/readiness signals & runbooks = GAP. |
| 10 | Test quality & "infra-blocked" disposition | **FAIL** | Cluster-absent runs report green (skip = pass); money-path test files 100% `#[ignore]`d; XA tests assert no data effect; no golden vectors; a benchmark test mutates a tracked file. See §6 disposition of the 10 failures. |

---

## 5. What works (verified positives — reported honestly)

- **Builds & formats clean.** `cargo build --workspace` exit 0 (production lib: only `missing_docs` warnings). `cargo fmt --all --check` clean.
- **Primary invocation path surfaces server errors.** `invoke`/`invoke_pinned` → `check_response()` turns message-type-0 (EXCEPTION) into a typed `HazelcastError` for IMap get/put/remove/replace/CAS, CP AtomicLong/CPMap, and non-XA transactions. The "swallowed server exception" defect class is **fixed on the primary path** (it survives only on the XA/executor/auth secondary paths).
- **Non-XA transactions work end-to-end (live).** `TransactionContext` commit persists data; rollback discards — both asserted and passing (Run B).
- **CP AtomicLong correctness (live).** `concurrent_increments` (1000→exactly 1000) and CAS tests pass against the real Enterprise CP subsystem.
- **Single-member-loss resilience (live).** 2000 committed keys all readable (miss=0, corrupt=0) after `hz1` killed; degraded writes succeed; cluster heals to 3 on restore.
- **Performance meets the SLA (live)** — see §4 #8.
- **Supply chain — licenses & sources clean.** `cargo deny` licenses/sources/bans OK; the 4 `cargo audit` vulnerabilities are **all in dev-only deps** (`testcontainers`→`bollard`); the production `tls` feature pulls patched `rustls-webpki 0.103.13`, not the vulnerable `0.101.7`.
- **Large-value & large-collection round-trips exact up to 64 MB / 5000 entries (live)** — the "no fragmentation reassembly" finding did **not** reproduce as corruption at any tested size.

---

## 6. Risk register

Severity reflects CBDC impact. **D = individually disqualifying for a money path.** Full 63-finding list with evidence and adversarial verdicts: `AUDIT_DIGEST.md`. IDs cross-reference that digest.

### Critical (deploy-blocking)

| ID | Finding | Evidence | Status |
|---|---|---|---|
| **RR-1 (D)** | **XA commit/rollback report false success**; response discarded, state set unconditionally; `invoke` bypasses `check_response` | `transaction/xa.rs:556-563,645-672` | CONFIRMED-AT-SOURCE |
| **RR-2 (D)** | **XA prepare decodes an error frame's bytes as the vote** (defaults to `Ok(0)=XA_OK`) → coordinator commits an unprepared branch | `transaction/xa.rs:573-591,627-643` | CONFIRMED-AT-SOURCE |
| **RR-3 (D)** | **Retry double-apply**: `redo_operation` retries non-idempotent mutations, no dedup; ambiguous timeout/conn-loss treated retryable | `connection/manager.rs:1490-1539`, `core/error.rs:295-311`, `connection/invocation.rs:256-266` | CONFIRMED-AT-SOURCE |
| **RR-4 (D)** | **Correlation-id reuse across retries** can deliver a stale response to the wrong attempt → wrong return value | `connection/invocation.rs:180-185,256-285` | AUDIT-VERIFIED |
| **RR-5 (D)** | **byte[] serialized with type-id STRING(-11)** not BYTE_ARRAY(-12); binary blobs corrupted cross-client | `core/serialization/traits.rs:13-15,170-189` | CONFIRMED-AT-SOURCE |
| **RR-6 (D)** | **`values()`/`key_set()` silently drop** undeserializable elements (`if let Ok` with no else) → short, wrong-but-plausible collections | `proxy/map.rs:3411-3414,3420-3422` | CONFIRMED-AT-SOURCE |
| **RR-7 (D)** | **CPMap value decode omits the 8-byte Data header skip** → every CPMap read corrupted/wrong | `proxy/cp_map.rs:271-293` | AUDIT-VERIFIED |
| **RR-8** | **`CP_SUBSYSTEM_*` on wrong service** (0x1E vs 0x22): `get_cp_group_ids()` actually calls `createCPGroup`, etc. | `core/protocol/constants.rs:988-1003`, `cluster/cp_management.rs` | AUDIT-VERIFIED (upstream cross-check) |
| **RR-9** | **`MAP_ADD_INTERCEPTOR`/related constants collide** with other ops → interceptors silently no-op | `core/protocol/constants.rs:834-849` | AUDIT-VERIFIED |
| **RR-10 (D)** | **Affinity routing `*_with_partition_key` uses raw MurmurHash** (no 8-byte skip / abs / modulo) as partition id → co-located data mis-placed/unreadable | `proxy/map.rs:1164,1200,1243,1274` | AUDIT-VERIFIED |
| **RR-11** | Mislabeled `CP_ATOMIC_REFERENCE_*` **source** constants (latent; live proxies use correct shadows) | `core/protocol/constants.rs:351-372` | AUDIT-VERIFIED |

### High

| ID | Finding | Evidence | Status |
|---|---|---|---|
| **RR-12** | **`SecurityConfig`/Builder `Debug` print password & token in cleartext** → secret leakage in any `{:?}` log/error/panic | `config.rs:1714-1723,1796-1805` | CONFIRMED-AT-SOURCE |
| **RR-13** | **TLS hostname verified against member IP, not DNS** (`server_name` always `None`) → hostname half of mTLS defeated | `connection/connection.rs:381-387` | AUDIT-VERIFIED |
| **RR-14** | **`HostnameVerification`/`verify_hostname` are dead config**, never read in handshake | `connection/connection.rs:308-394` | AUDIT-VERIFIED |
| **RR-15** | **No private-PKI pinning** — with no CA configured, trusts public WebPKI roots | `connection/connection.rs:338-340` | AUDIT-VERIFIED |
| **RR-16** | **u16/u32/u64/u8 and HazelcastJsonValue serialized as STRING** → ids/amounts/JSON mislabeled cross-client; SQL over JSON wrong | `core/serialization/traits.rs:195-241`, `serialization/json.rs` | CONFIRMED-AT-SOURCE |
| **RR-17** | **`PartitionService::get_partition` uses SipHash over the key object**, not MurmurHash3 over bytes → wrong partition/owner from public API | `cluster/partition_service.rs:339-359` | AUDIT-VERIFIED |
| **RR-18** | **Executor/scheduled/durable + event-journal reads bypass `check_response`** → server exceptions returned as garbage results | `executor/service.rs:428-483`, `proxy/map.rs:5617-5625` | AUDIT-VERIFIED |
| **RR-19** | **ReplicatedMap `values()` + IMap predicate/project decoders drop elements** silently | `proxy/replicated_map.rs:817-836`, `proxy/map.rs:4479,4596` | AUDIT-VERIFIED |
| **RR-20** | **Auth response errors only logged, not surfaced** → half-authenticated connection registered | `connection/manager.rs:492-501` | AUDIT-VERIFIED |
| **RR-21** | **Reader-loop decode/IO error breaks loop without failing in-flight ops** with a typed error → masked as retryable | `connection/invocation.rs:157-212` | AUDIT-VERIFIED |
| **RR-22** | **Compact/Portable array readers `Vec::with_capacity(attacker_len)`** (no sign/bound check) → process abort/OOM on one malformed frame (DoS) | `serialization/compact/mod.rs:1521-1715`, `serialization/portable/reader_writer.rs:576-753` | CONFIRMED-AT-SOURCE |
| **RR-23** | **`MAP_*_PARTITION_LOST_LISTENER` constants wrong** → partition-loss (data-loss) events never delivered/removed | `core/protocol/constants.rs:846-849` | AUDIT-VERIFIED |
| **RR-24** | **Test skip = green**: `skip_if_no_cluster()` returns (passes) with no cluster; `require_cluster!` dead | `tests/common/mod.rs:109-126` | CONFIRMED-AT-SOURCE |
| **RR-25** | **Money-path test files 100% `#[ignore]`d** (XA, entry-proc, aggregation, executor, replicated-map, java-parity CRUD) → no default coverage | multiple `tests/*.rs` | CONFIRMED |
| **RR-26** | **XA tests assert client-side enums only, never the commit/rollback data effect** → a no-op commit passes green | `tests/transaction_xa_test.rs:29-105` | AUDIT-VERIFIED |
| **RR-27** | **No wire-format golden vectors**; serialization tests are self-referential → endianness/offset/framing drift undetected | `core/protocol/client_message.rs`, proptests | AUDIT-VERIFIED |

### Medium / Low (selected — full list in `AUDIT_DIGEST.md`)

| ID | Finding | Status |
|---|---|---|
| RR-28 | **`cargo audit` red** (4 vulns, 6 warnings) — all in **dev-only** deps; production unaffected. But the gate is red, and `cargo deny` **passes** on the same tree (gate inconsistency: webpki 0.101.7 / rand 0.7.3 advisories not in deny's ignore list yet deny reports OK — needs reconciliation) | PROVEN |
| RR-29 | **No response fragmentation reassembly** in the decoder (code gap) — but **not reproducible as corruption up to 64 MB / 5000 entries** live; likely dormant for 5.7 server responses. Latent. | CONFIRMED-AT-SOURCE + PROVEN-LIVE (no impact) |
| RR-30 | `BACKUP_EVENT_FLAG` wrong value (1<<11) collides with `END_DATA_STRUCTURE_FLAG`; `BACKUP_AWARE_FLAG` missing — dormant (no backup-ack path wired) | AUDIT-VERIFIED |
| RR-31 | **clippy `-D warnings` gate red** — 15 `approx_constant` errors, all benign test-code float literals; trivial fix | PROVEN |
| RR-32 | A benchmark test (`run_full_benchmark_suite`) **mutates a tracked file** (`benchmarks/results/rust_results.json`) → non-hermetic test | PROVEN |
| RR-33 | `unsafe { mem::zeroed::<K>() }` at `proxy/map.rs:8668` — dead test-only code, never executed (latent footgun) | CONFIRMED-AT-SOURCE |

**2 audit findings were adversarially *refuted*** (e.g., the prior claim that `cp_session::encode_group_id` uses plain framing — it correctly uses data-structure framing in this revision). Recorded in `AUDIT_DIGEST.md` to show what was checked and dismissed.

---

## 7. Coverage & gap analysis

### 7.1 Test reconciliation (reproduced, not taken on faith)

- **Non-ignored suite (`cargo nextest run --workspace`):** **2182 passed, 0 failed, 158 skipped** (skips = the `#[ignore]`'d integration tests). Includes live cluster work (`comprehensive_functional_test`, real timings).
- **Ignored integration suite (`--run-ignored ignored-only`, serial):** **148 passed, 10 failed** of 158. All 10 failures are in `java_parity_test`.

### 7.2 Disposition of the 10 failures (the "infra-blocked" claim, investigated)

The prior run reported ~10 tests "blocked by test infrastructure." Investigated by **where the server fails** (clean post-decode error = infra; codec error at decode = client defect):

| Test | Server response | Disposition |
|---|---|---|
| `execute_on_entries_with_predicate` | `ClassCastException: String→EntryProcessor` (post-decode) | **Infra** — no registered EntryProcessor class; client error-surfacing **correct** |
| `load_all` | `IllegalArgumentException: configure a map store` | **Infra** — no MapStore configured |
| `load_all_keys` | `NullPointerException` | **Infra** (MapStore) |
| `project` | `QueryException: no accessor 'name' on String` | **Infra/test-design** — needs typed domain object |
| `get_entry_view` | `assert hits()>=2` | **Infra** — per-entry stats not enabled (or decode); needs config to settle |
| `entry_set_with_predicate` | `assert left==right` (:494) | **Unresolved** — predicate/decoding; needs review |
| `try_lock_with_timeout` | assertion (:676) | **Unresolved** — lock semantics; needs review |
| `execute_on_key` | **`ArrayIndexOutOfBoundsException` at decode** | **Likely CLIENT DEFECT** — malformed request |
| `execute_on_keys` | **`NPE: peekNext()/isEndFrame()` null at decode** | **Likely CLIENT DEFECT** — missing frame (corroborates framing findings) |
| `submit_to_key_async` | **`ArrayIndexOutOfBoundsException` at decode** | **Likely CLIENT DEFECT** — malformed request |

**Key positive:** none of the 10 silently returned wrong data — every one surfaced as a typed error or assertion. The error-surfacing on these IMap paths works. **To fully close infra-vs-defect:** stand up a server-side EntryProcessor class + MapStore + per-entry-stats config and re-run; the three decode-time codec errors are expected to persist (= confirmed client defect) while the others should pass. *(Not completed — recommended remediation step.)*

### 7.3 Explicit gaps (untested = unproven, recorded as such)

| Area | Gap | Why |
|---|---|---|
| Protocol | Full byte-level diff vs stock **Java client** | Not run; static cross-check against upstream codecs done instead |
| Failover | CP-leader failover, split-brain/partition, AZ/multi-member loss; **ambiguous-failure double-apply repro** | Only benign single-member loss exercised live |
| Memory | **Live fuzzing** of codec (the 5 `fuzz_targets/` harnesses) and **soak** | cargo-fuzz needs nightly toolchain; not installed. Static panic findings stand (RR-22) |
| Security | **Live mTLS handshake** (cert-chain + hostname enforcement; bypass attempts) | No TLS listener stood up; static findings stand (RR-13/14/15) |
| Consistency | CP **linearizability under partition** (Jepsen-style) | Out of scope for this window; flagged by project's own plan |

---

## 8. Remediation plan (prioritized; gates to re-validation)

**P0 — money-path correctness (must fix before any re-assessment):**
1. **XA**: route `invoke` through `check_response`; on `commit`/`rollback` **assert server success** before setting state; in `prepare`, only accept a vote from a verified success response (RR-1, RR-2).
2. **Retry safety**: make the protocol carry a dedup/idempotency token the server honors; never retry non-idempotent mutations on ambiguous (request-sent) failures; stop treating client-side timeout/conn-loss as blanket-retryable; eliminate correlation-id reuse across attempts (RR-3, RR-4, RR-21).
3. **Type fidelity**: give `byte[]`, `u8/u16/u32/u64`, and JSON their correct Hazelcast type-ids; add **cross-client golden-vector tests** (RR-5, RR-16, RR-27).
4. **No silent drops**: `values()`/`key_set()`/ReplicatedMap/predicate decoders must **fail loudly** on a deserialize error, not skip (RR-6, RR-19).
5. **CPMap** Data-header skip; **affinity-key** and **PartitionService** routing must use MurmurHash3-over-bytes with the 8-byte skip + modulo (RR-7, RR-10, RR-17).
6. **Protocol constants**: fix `CP_SUBSYSTEM_*` service id and the colliding `MAP_*` constants at source; cross-check **all** message-type constants against generated upstream definitions (RR-8, RR-9, RR-11, RR-23).

**P1 — security gate & robustness:**
7. Enforce real **hostname/SAN verification against DNS identity**; wire the verification knobs; require explicit CA/pinning for private PKI; make `tls`/mTLS first-class (RR-13/14/15).
8. **Redact** password/token in all `Debug`/log paths; key zeroization review (RR-12).
9. Bound all length-driven allocations in the decoders; fuzz to confirm no panic/OOM (RR-22).
10. Surface errors on **every** invocation path (executor, auth, reader-loop) (RR-18, RR-20).

**P2 — assurance infrastructure (so green means green):**
11. Make cluster-absence **fail or formally skip**, not pass (RR-24); un-ignore money-path tests and run them in CI against a real cluster; make XA/txn tests assert the **data effect** (RR-25, RR-26).
12. Reconcile `cargo audit` vs `cargo deny`; flip clippy/fmt/audit/deny to blocking; hermetic tests (RR-28, RR-31, RR-32).
13. Stand up the server-side config (EntryProcessor class, MapStore, per-entry stats) to resolve the §7.2 infra-vs-defect items definitively.

**P3 — distributed-correctness (the long pole):** Jepsen-style linearizability under partition; CP-leader/split-brain/AZ chaos with exactly-once verification; 72h+ soak; independent security audit + pen test. (Matches the project's Phase-C.)

**Re-validation:** NO-GO stands until P0+P1 are fixed *and* P2 makes the suite trustworthy, then a full re-run of this assessment (including the §7.3 gaps) passes.

---

## 9. Evidence appendix (reproducible; secrets masked)

Environment: `ssh ec2-user@18.225.173.180` (key `~/.ssh/hzcp.pem`, CRLF-fixed); cluster `dev` on `127.0.0.1:5701/2/3`. All `cargo` on the instance.

```
# Provenance
git rev-parse origin/main                     # e6386d2a563f0cace203053ba108220817ab56ff
git status --porcelain                        # (clean, before and after testing)

# Static gates
cargo fmt --all -- --check                    # exit 0 (clean)
cargo build --workspace                       # exit 0 (lib: missing_docs warnings only)
cargo clippy --workspace --all-targets        # exit 101 — 15 approx_constant in TEST code (benign)
cargo audit                                   # exit 1 — 4 vulns (ALL dev-only: testcontainers/bollard), 6 warnings
cargo deny check                              # advisories/bans/licenses/sources OK (exit 0)
cargo tree -i rustls-webpki -e normal         # (empty) — vulnerable webpki not on production path

# Test suite (profile: retries=0, fail-fast=off, hang-terminate)
cargo nextest run --workspace --profile cbdc                          # 2182 passed, 0 failed, 158 skipped
CLUSTER_ADDRESS=127.0.0.1:5701 cargo nextest run -p hazelcast-client \
  --profile cbdc --run-ignored ignored-only --test-threads 1         # 148 passed, 10 failed (all java_parity)

# Performance (custom assertion tests, single client)
PUT  n=20000 mean=144us p50=144us p99=175us p999=216us max=799us     # bar p99<=10ms, p999<=50ms  -> PASS
GET  n=20000 mean=89us  p50=88us  p99=112us p999=149us max=262us
THROUGHPUT 192000 PUTs in 10.43s = 18409 ops/sec (concurrency=64)    # bar >=10k TPS -> PASS

# Large-value / collection integrity (custom)
1KB,256KB,1MB,4MB,16MB,32MB,64MB values: round-trip byte-exact       # fragmentation not reproduced
size/values/key_set over 5000 entries == 5000; i64 ledger sum exact

# Live single-member failover (custom; authorized + restored)
seed 2000 keys -> docker stop hz1 -> Members{size:2} ->
  VERIFY: ok=2000 miss=0 corrupt=0 err=0; degraded-write ok in 3ms   # RPO=0
  -> docker start hz1 -> Members{size:3}  (cluster restored healthy)

# Cleanup: instance tree restored to pristine e6386d2 (git status clean)
```

Selected failure outputs (from `--run-ignored`):
```
execute_on_keys:  Server NPE "ClientMessage$ForwardFrameIterator.peekNext() is null"  (decode-time -> client framing defect)
execute_on_key:   Server ArrayIndexOutOfBoundsException                               (decode-time -> client defect)
load_all:         Server IllegalArgumentException "First you should configure a map store" (infra)
project:          Server QueryException "no suitable accessor for 'name' on java.lang.String" (infra/test-design)
```

Source confirmations (read by hand):
```
core/serialization/traits.rs:13-15   fn type_id(&self) -> i32 { -11 }     # default STRING
core/serialization/traits.rs:170-241 Vec<u8>/u8/u16/u32/u64 omit type_id  # -> STRING
proxy/map.rs:3411-3414                if let Ok(value)=T::deserialize(..){values.push(value)}  # silent drop
transaction/xa.rs:654-657            let _response = self.invoke(..)?; self.state=Committed; Ok(())  # false success
config.rs:1717-1719                  .field("password",&self.password).field("token",&self.token)    # secret leak
```

**Companion artifacts:** `EVIDENCE_LOG.md` (full chronology), `AUDIT_DIGEST.md` (all 63 adversarially-verified findings with verdicts).
