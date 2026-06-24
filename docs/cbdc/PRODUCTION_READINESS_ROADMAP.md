# HazelRust — Production-Readiness Roadmap (to GO for a CBDC money path)

Forward-looking work list to take the client from its current state (independent verdict: **NO-GO**, partially remediated on `fix/cbdc-remediation`) to a defensible **GO**. Ordered by priority. Each item states *what*, *why*, and the *exit criterion* that proves it done. Effort is a rough order of magnitude (S ≤ days, M ≤ weeks, L ≤ months). Cross-refs: `INDEPENDENT_PRODUCTION_READINESS_ASSESSMENT.md` (risk register RR-*), `CBDC_REMEDIATION_PLAN.md` (R*), `CONSTANTS_VERIFICATION.md`, `AUDIT_DIGEST.md`.

> **Rule:** no item on the money path may be reviewed only by its author. Every behavioral fix ships with an assertion-bearing test that fails before and passes after, plus a live-cluster check.

---

## P0 — Money-path correctness (deploy-blocking; must finish what remediation started)

### A1. Fix XA two-phase-commit request framing — *M*
- **What:** the XA `create/prepare/commit/rollback` request encoders are mis-framed; the server throws `ArrayIndexOutOfBoundsException`/`NullPointerException` during decode. The false-success bug is fixed (errors now surface), but XA is **non-functional**. Re-derive the XA codec framing against the Hazelcast 5.x `XATransaction` codec; verify each op decodes server-side.
- **Why:** atomic multi-resource ledger postings (RR-1/RR-2). Until fixed, XA cannot be used at all.
- **Exit:** the 8 `xa_transaction_test` cases pass **and assert the data effect** (committed writes are visible via a separate client; rollback leaves none; a server-rejected commit returns `Err`, never `Ok`).

### A2. Fix CPMap request framing — *M*
- **What:** CPMap `put`/`get` requests are mis-framed (server `NullPointerException` on a missing frame). The value Data-header fix (R6) is in place but unverifiable until the request frames are correct.
- **Why:** strongly-consistent CP map for ledger metadata/config.
- **Exit:** live CPMap round-trip (i64 + String) passes; cross-client read (Java) matches.

### A3. Fix EntryProcessor invocation framing — *M*
- **What:** `execute_on_key`, `execute_on_keys`, `submit_to_key` produce server codec errors (`AIOOBE`, `peekNext()==null`) at **decode time** — the requests are mis-framed (distinct from the infra-blocked cases that fail post-decode). Correct the EntryProcessor request codecs.
- **Why:** server-side compute on ledger entries (in-scope cache/query).
- **Exit:** with a registered server-side EntryProcessor class, these cases pass and return the processor result; framing-only fixes verified even where a class is absent (clean typed error, not a codec crash).

### A4. Complete protocol-constant correctness & hygiene — *M*
- **What:** of 219 audited constants, 18 corrected; ~11 remain wrong/unverifiable (`CONSTANTS_VERIFICATION.md`). Add a correctly-named `CP_GROUP_CREATE_CP_GROUP = 0x1E0100`, repoint the CP proxies to it, then give `CP_SUBSYSTEM_*` their real `0x22` values; fix `ContinuousQuery` service (`0x14`→`0x16`); resolve `MAP_SET_ALL`/`MAP_FETCH_VALUES` semantics; re-check AtomicLong/AtomicReference `n/a` ops (no upstream method → re-implement via the correct codec).
- **Why:** a wrong message-type silently invokes the wrong server operation (RR-8/9/11/23).
- **Exit:** every in-scope constant matches a generated upstream definition; a **golden-vector test** pins each message type; ideally constants are *generated* from the protocol YAML, not hand-maintained.

### A5. Retry idempotency / exactly-once under failover — *L*
- **What:** the retry framework is safe-by-default (mutations non-idempotent, `redo_operation` off), but there is **no op-level dedup token** the server honors, correlation-ids can be reused across attempts (RR-4), and the reader loop drops in-flight ops on a decode/IO error without a typed failure (RR-21). Add unique per-invocation call-ids the member dedupes; ensure a retry never reuses a live correlation-id; fail in-flight ops loudly on connection teardown.
- **Why:** a duplicated money mutation is catastrophic; enables safe `redo_operation`/at-least-once.
- **Exit:** a fault-injection test (kill the owner mid-`put`/`getAndAdd`) shows **exactly-once** application across reconnect; no stale response is ever delivered to the wrong attempt.

### A6. Surface server errors on every invocation path (R8) — *S/M*
- **What:** executor/scheduled/durable-executor + event-journal reads, the auth path (`manager.rs`), and the `send_to`/`receive_from` path do not route through `check_response`; server exceptions become garbage results or are only logged (RR-18/20).
- **Why:** silent wrong-result / half-authenticated connections.
- **Exit:** an induced server error on each path returns a typed `Err`; a unit/integration test asserts it.

---

## P1 — Security gate & supply chain

### B1. Enforce mTLS with real identity verification — *M*
- **What:** verify the server cert against its **DNS identity** (set `server_name` from the configured host, not the IP); read and honor `HostnameVerification`/`verify_hostname`; require an explicit CA / certificate pinning for private PKI (don't silently trust public WebPKI roots); wire mutual-TLS client certs; make TLS a first-class, prod-default-on path (RR-13/14/15).
- **Exit:** against a TLS-enabled cluster, the client (a) succeeds with a valid chain + matching hostname + client cert, and (b) **rejects** an expired/wrong-hostname/untrusted cert; `allow_invalid_certs` cannot be reached implicitly.

### B2. Stand up a TLS/mTLS integration environment — *S*
- **What:** add a TLS-configured Hazelcast EE listener (certs, truststore) to the test cluster; un-ignore `tls_integration_test.rs`.
- **Exit:** the mTLS suite runs in CI against the TLS listener, including negative (bypass-attempt) cases.

### B3. Secret handling hardening — *S*
- **What:** zeroize credentials/license/token in memory (`zeroize`); audit every `tracing`/`log`/error/panic path for leakage (beyond the `Debug` redaction already done); add `SECURITY.md` + disclosure process + patch SLA.
- **Exit:** a leak-scanning test passes; secrets are zeroized on drop; SECURITY.md merged.

### B4. Supply-chain integrity — *S/M*
- **What:** reconcile `cargo audit` vs `cargo deny` (they currently disagree); remove/replace the dev-only vulnerable deps (`testcontainers`→`bollard`→`tokio-tar`, old `rustls`/`rand`) or formally justify each ignore; produce a CycloneDX **SBOM** per build; pin the toolchain (`rust-toolchain.toml`); reproducible + signed builds with SLSA provenance.
- **Exit:** `cargo audit` and `cargo deny` both green (or every exception justified + tracked); SBOM artifact attached to each build; builds are reproducible and signed.

---

## P2 — Test, assurance & distributed-correctness (the long pole)

### C1. Make the test suite trustworthy — *M*
- Un-ignore the money-path files (XA, entry-proc, aggregation, executor, replicated-map, java-parity CRUD) and run the **full** integration suite in CI against a real-cluster matrix (HZ N-1/N/N+1, TLS on/off, cluster sizes 1→5+) via `testcontainers`, with `HZ_REQUIRE_CLUSTER=1` so a missing cluster fails.
- Every test asserts the **data effect**, not just client-side enums.
- Hermetic tests (no mutation of tracked files); flip `fmt`/`clippy -D warnings`/`audit`/`deny` to **blocking**; coverage on **both** crates with a ratcheting threshold.
- **Exit:** CI is a real gate; coverage ≥ target; no test can pass without exercising wire behavior.

### C2. Wire-format conformance — *M*
- **Differential harness vs the stock Java client:** drive identical ops from both clients against the same cluster and **diff the bytes**; assert identical frames + observable results. This is the gold standard the current `java_parity_test` only approximates.
- Golden-vector tests for partition hashing and every codec; property tests (`proptest`) asserting `decode(encode(x)) == x` and decode-never-panics on arbitrary bytes for Compact/Portable/Identified/frame/`ClientMessage`.
- **Exit:** byte-level parity with the Java client for all in-scope ops; round-trip + never-panic properties green.

### C3. Continuous fuzzing — *M*
- Run the 5 existing `cargo-fuzz` targets (nightly) continuously with a persistent corpus (OSS-Fuzz-style); track crash-free CPU-hours as a release metric. (Static bounds added in R9 cover the known `with_capacity` DoS; fuzzing finds the unknowns.)
- **Exit:** crash-free CPU-hours trending up; no reachable panic/OOM on malformed input.

### D1. Distributed-systems correctness — *L*
- **Jepsen-style** linearizability for CP structures (AtomicLong/Ref, FencedLock, CPMap) and the documented AP model for IMap, under injected partitions.
- **Failover chaos beyond single-member** (only benign single-member loss verified so far): CP leader failover, split-brain + merge, AZ loss, member add/remove under load, partition-table migration, slow/stuck members — verify exactly-once + the agreed RTO/RPO and that the minority side fails safe.
- **Deterministic simulation testing** (seeded, reproducible crash/partition/reorder schedules; replay any failure).
- **Exit:** Jepsen passes for the claimed guarantees; injected-failure schedules survive without loss/corruption/double-apply; retry exactly-once proven (depends on A5).

### E1. Performance, capacity & concurrency — *M*
- Re-measure on **dedicated hardware with real network RTT** (current numbers are loopback); gate p50/p99/p999/p9999/max SLOs with statistically sound regression checks.
- **72h+ soak** with flat memory (heaptrack/massif), no FD/queue growth.
- **Backpressure/overload** tests: graceful degradation, no OOM under connection/traffic storms.
- `loom` model-check the connection-manager/cache lock+atomic+channel logic; TSan/ASan over the suite; MIRI over unit tests.
- **Exit:** SLOs met on representative hardware/topology with a regression gate; soak flat; overload degrades gracefully.

---

## P3 — Operability & governance (required for sign-off)

### F1. Observability & operations — *M*
- Structured logs, metrics, and traces per operation; documented dashboards + alerts; health/readiness signals the proxy can consume.
- Runbooks for every failure mode; tested upgrade/rollback; DR drills with measured RTO/RPO.

### G1. Governance, audit & release — *M*
- Independent review (≥2 reviewers; segregation of duties; no single author on the money path).
- Independent third-party **security audit + penetration test** of the client + TLS/auth.
- Funded, SLA-backed long-term **maintenance/support** commitment (resolve bus-factor-1).
- Formal, audited **risk sign-off** mapping this evidence to the institution's model-risk & operational-resilience frameworks before go-live.
- Cut a versioned, tagged **1.0** release with a `CHANGELOG`; correct the README "official client" claim.

---

## Definition of "ready" (all must be continuously true before GO)

1. CI gates every commit: build + full test (both crates, real-cluster matrix) + `clippy -D warnings` + `fmt` + `audit` + `deny`, all green; SBOM produced; builds signed.
2. Zero silent data loss/corruption: type fidelity proven cross-client; no decoder drops elements; byte-level parity with the Java client.
3. Transactions atomic: XA + `TransactionContext` commit/rollback proven by data-effect tests, including mid-failure.
4. Exactly-once under failover: dedup tokens proven; no double-apply, no lost/stale response (Jepsen + fault injection).
5. mTLS mandatory and enforced: chain + hostname validated; bypass attempts rejected; no secret leakage; secrets zeroized.
6. Continuous fuzzing crash-free; property + golden-vector tests green; deterministic simulation survives injected failures.
7. SLOs (p999/p9999) met on dedicated hardware with a regression gate; 72h soak flat; graceful overload.
8. Runbooks, observability, tested upgrade/rollback + DR drills exist.
9. Independent review, independent security audit/pen test, a real maintenance commitment, and a formal risk sign-off are on file.

**Until all nine hold and stay green, the recommendation for a central-bank money path remains: do not deploy.**
