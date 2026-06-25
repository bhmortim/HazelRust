# HazelRust — Production-Readiness Roadmap (to GO for a CBDC money path)

Forward-looking work list to take the client from its current state (independent verdict: **NO-GO**, partially remediated on `fix/cbdc-remediation`) to a defensible **GO**. Ordered by priority. Each item states *what*, *why*, and the *exit criterion* that proves it done. Effort is a rough order of magnitude (S ≤ days, M ≤ weeks, L ≤ months). Cross-refs: `INDEPENDENT_PRODUCTION_READINESS_ASSESSMENT.md` (risk register RR-*), `CBDC_REMEDIATION_PLAN.md` (R*), `CONSTANTS_VERIFICATION.md`, `AUDIT_DIGEST.md`.

> **Rule:** no item on the money path may be reviewed only by its author. Every behavioral fix ships with an assertion-bearing test that fails before and passes after, plus a live-cluster check.

---

## P0 — Money-path correctness (deploy-blocking; must finish what remediation started)

> **Category-A update (pass 4, see `REMEDIATION_RESULTS.md`):** the four technically-completable client items are now **done & verified live** — A1 (XA, 12/12 + data-effect), A6 (auth-failure surfacing / reader-loop in-flight fail), B1 (mTLS data ops), and the 9 infra-blocked `java_parity` tests (now 161/1 ignored, only the non-defect `try_lock_with_timeout`). The overall verdict is **unchanged (NO-GO)** pending the external P3 items.

> **Pass 5 update (see `REMEDIATION_RESULTS.md` → "Remediation pass 5"):** five more money-path defect classes found and **fixed + verified live** (each with a before/after regression test): (1) `partition_count` parsed two bytes early → silently 0 [lead #1]; (2) heartbeat reconnect never re-authenticated/rebuilt the invocation pool → data ops dead after a member blip [lead #3]; (3) four collection decoders silently dropped undeserializable elements [silent-element-drop]; (4) **XA `recover()` was non-functional** — it decoded the in-doubt `List<Xid>` with the wrong format and always returned empty, so in-doubt transactions were invisible to recovery (**new finding**); plus lead #5 (2PC across two maps, data-effect). **Still open**: the differential-vs-Java wire harness (C2) to confirm/refute the residual decoder findings, `ringbuffer::read_many` reframe, lead #2 (lenient auth arms), lead #4 (flaky/vacuous failover tests), and tracks C3/D1/E1. Verdict **unchanged (NO-GO)**.

> **Pass 8 update (see `REMEDIATION_RESULTS.md` → "Remediation pass 8"):** unblocked previously "blocked" items by building the missing verification infra (a Java cross-client harness + a Java paging reference + tcpdump). **Fixed + verified live:** AtomicReference Data-header — proven CROSS-CLIENT (a Java-written value decoded as `Some("")` in Rust before, correct both directions after); ringbuffer `read_many` ReadResultSet reframe (before/after: a trailing `itemSeqs` long[] was injected as a 6th i64 item); **paging fully functional** — implemented the `PagingPredicateHolder` request codec + the 5.x response decoders (values/keys/entries, the entries `EntryList` is interleaved `[k,v]`). **New finding:** ICache is broken deeper than the value header — its entire `CACHE_*` message-type constant block is wrong (sequential placeholders, not the real 5.x values), so it invokes the wrong server ops; needs a full codec re-derivation (and a `constants.rs` audit, since CP `ATOMIC_*` were likewise mislabeled). **Still open:** ICache full codec, event-journal decoders, mTLS default-hostname, message fragmentation. Verdict **unchanged (NO-GO)**.

> **Pass 7 update (see `REMEDIATION_RESULTS.md` → "Remediation pass 7"):** continued working the decoder/security/test ledger. **Fixed + verified live:** IMap entry-listener decode was broadly mis-framed AND `EntryEventType` had `Updated`/`Removed` **swapped** (so `.on_updated()` subscribed to REMOVED — UPDATE events were silently never delivered) — both caught by a new entry-listener data-effect test (the old listener tests never asserted the callback fired); credential `Debug` now redacts password/token/secret-attributes + secrets zeroize on drop; `BACKUP_EVENT_FLAG` value corrected; the vacuous `failover_integration_test` "reconnect" tests renamed to honest smoke tests. **Code-parity fix:** paging Data-header skip (identical to the verified `decode_values_response`; live-exercise blocked by a separate paging-REQUEST framing bug = `PagingPredicateHolder` codec). **Still open (precise specs in pass 7):** paging request framing; map/cache event-journal decoders (same deep pattern as entry-listener, needs event-journal-enabled cluster); ICache/AtomicReference Data-header on encode (needs Java cross-client verification); mTLS default-hostname binding (mechanism already proven in pass 3); ringbuffer reframe; message fragmentation. Verdict **unchanged (NO-GO)**.

> **Pass 6 update (see `REMEDIATION_RESULTS.md` → "Remediation pass 6"):** **smart (partition-aware) routing — previously non-functional in production (the member map and partition table had only `#[cfg(test)]` writers, so every op fell back to `addresses[0]`) — is now implemented and verified live**: the client registers `ClientAddClusterViewListener` (`0x000300`) and decodes its members-view/partitions-view events (wire-confirmed by hexdump) to populate both maps, and proactively connects to all members so ops route to their partition owners (proven by `HZ_DEBUG_ROUTING` decisions — 0 fallbacks — and a `tcpdump` PSH-packet differential across all 3 member ports). Also fixed 3 partition-id correctness bugs (`i32::MIN` panic via `hashToIndex`; `get_partition` missing the 8-byte Data-header skip; `*_with_partition_key` raw-hash partition id) and **hardened invocation-pool (data-carrying) connection auth to check the status byte**. **Still open** (all verified-in-code with precise fixes recorded, not yet live-fixed): the decoder silent-corruption class in *listener/paging/event-journal/cache/CP-reference* paths (esp. `decode_entry_event`, fully re-derived against the EE jar), ICache/AtomicReference Data-header on encode, `ringbuffer::read_many` reframe, message-fragmentation reassembly, mTLS DNS-identity verification, credential zeroization, and the vacuous `failover_integration_test` reconnect tests. Verdict **unchanged (NO-GO)**.

### A1. Fix XA two-phase-commit request framing — *M*
- **What:** the XA `create/prepare/commit/rollback` request encoders are mis-framed; the server throws `ArrayIndexOutOfBoundsException`/`NullPointerException` during decode. The false-success bug is fixed (errors now surface), but XA is **non-functional**. Re-derive the XA codec framing against the Hazelcast 5.x `XATransaction` codec; verify each op decodes server-side.
- **Why:** atomic multi-resource ledger postings (RR-1/RR-2). Until fixed, XA cannot be used at all.
- **Exit:** the 8 `xa_transaction_test` cases pass **and assert the data effect** (committed writes are visible via a separate client; rollback leaves none; a server-rejected commit returns `Err`, never `Ok`).
- **STATUS (pass 4 — DONE):** XA is **12/12 live**. The residual failure was `multiple_xa_transactions` — a *response-misrouting race* on the shared metadata connection (a stock Java client ran the identical sequence fine, and the commit frame was byte-identical between passing/failing runs), not a framing bug. Fixed by routing XA through the correlation-matched `invoke_pinned` and pinning each branch to one member endpoint. Added `XATransaction::get_map` + two **data-effect** tests: a committed XA write is visible via a separate client; a rolled-back (post-prepare) write is not. (Commit `4ebde9e`.)

### A2. Fix CPMap request framing — *M*
- **What:** CPMap `put`/`get` requests are mis-framed (server `NullPointerException` on a missing frame). The value Data-header fix (R6) is in place but unverifiable until the request frames are correct.
- **Why:** strongly-consistent CP map for ledger metadata/config.
- **Exit:** live CPMap round-trip (i64 + String) passes; cross-client read (Java) matches.
- **STATUS (remediation pass 2 — DONE):** the requests were missing the entire `RaftGroupId` data structure; added the group machinery (resolve via `CPGroupCreateCPGroup`, cache, encode BEGIN/[seed,id]/name/END) and fixed the value decode (8-byte Data-header skip + removed a wrong response short-circuit). **CPMap put/get round-trip verified live** for i64 and String. (Cross-client Java read still to confirm.)

### A3. Fix EntryProcessor invocation framing — *M*
- **What:** `execute_on_key`, `execute_on_keys`, `submit_to_key` produce server codec errors (`AIOOBE`, `peekNext()==null`) at **decode time** — the requests are mis-framed (distinct from the infra-blocked cases that fail post-decode). Correct the EntryProcessor request codecs.
- **Why:** server-side compute on ledger entries (in-scope cache/query).
- **Exit:** with a registered server-side EntryProcessor class, these cases pass and return the processor result; framing-only fixes verified even where a class is absent (clean typed error, not a codec crash).
- **STATUS (remediation pass 2 — framing DONE):** `execute_on_key` was missing the `threadId` fixed param; `execute_on_keys` sent an int-count + raw frames instead of a `List<Data>`. Both re-framed; `submit_to_key` delegates to `execute_on_key`. `entry_processor_test::{test_execute_on_key,test_execute_on_keys}` now **PASS live**; the `java_parity` variants now fail with a clean post-decode `ClassCastException` (custom processor class not deployed) instead of a codec crash — framing confirmed correct, residual failure is the server-class infra gap.

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
