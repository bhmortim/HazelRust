# HazelRust — CBDC Remediation Plan

Addresses every finding from the independent Production-Readiness Assessment (verdict: NO-GO at `e6386d2`). Work lands on branch `fix/cbdc-remediation`. Each item lists the fix and how it is verified. RR-ids map to the assessment's risk register.

**Status key:** ☐ todo · ◐ in progress · ☑ done+verified · ⚠ partial/needs-follow-up · ⊘ infra/process (not code-completable in this pass)

## Verification doctrine
- Build clean on the instance after every batch (`cargo build --workspace`, `cargo clippy`).
- For each behavioral fix, add an **assertion-bearing test** (the suite's weakness was tests that don't assert the data effect).
- Confirm the **headline defects** against the **live Enterprise cluster** (the gold standard), not just unit tests.
- Protocol-constant fixes are verified against **authoritative upstream definitions**, not the prior audit's sample.

---

## P0 — Money-path correctness (deploy-blocking)

### R1 — Protocol message-type constants (RR-8, RR-9, RR-11, RR-23, RR-30)
- **Fix:** correct `CP_SUBSYSTEM_*` (service `0x22`, not `0x1E`), the colliding `MAP_ADD/REMOVE_INTERCEPTOR` and `MAP_*_PARTITION_LOST_LISTENER` constants, the mislabeled `CP_ATOMIC_*` source constants, and `BACKUP_EVENT_FLAG`/`BACKUP_AWARE_FLAG`. **Verify the entire in-scope constant table** against upstream `hazelcast-client-protocol` definitions — not just the audited sample.
- **Verify:** automated diff vs upstream YAML (CONSTANTS_VERIFICATION.md); golden-vector unit test pinning a sample of message types; live interceptor + partition-lost-listener round-trip.

### R2 — Serialization type-ids (RR-5, RR-16)
- **Fix:** `Vec<u8>`/`[u8]` → BYTE_ARRAY (`-12`); `u8`→`-3`, `u16`→`-6`, `u32`→`-7`, `u64`→`-8`; `HazelcastJsonValue` → `-130`. (`String` correctly defaults to `-11`.) Flag the `type_id()` default of `-11` as a footgun; recommend making it non-defaulted (breaking) in a follow-up.
- **Verify:** golden-vector unit tests pinning the Data header (type-id + payload) for each type; live round-trip; cross-client (Java) read = documented gap.

### R3 — Silent element-drop (RR-6, RR-19)
- **Fix:** `decode_values_response`/`decode_keys_response` (map.rs), ReplicatedMap `values()`, and predicate/project decoders must **propagate** a deserialize error (`?`) instead of `if let Ok { push }`. Also fail on short (<8-byte) Data frames rather than `continue`.
- **Verify:** unit test feeding a poison element asserts `Err`; live large-collection integrity (already passing) must still pass.

### R4 — XA atomicity (RR-1, RR-2)
- **Fix:** route XA `invoke` through `check_response` (so server EXCEPTION → typed `Err`); `commit`/`rollback` must **only** set terminal state after a verified-success response; `prepare` must decode the vote only from a verified-success response and never default a short/error frame to `XA_OK`.
- **Verify:** live test — XA commit data-effect; XA commit on a server-rejected state returns `Err` (not false success).

### R5 — Retry idempotency & safety (RR-3, RR-4, RR-21)
- **Fix:** never auto-retry non-idempotent mutations on **ambiguous** (request-sent, response-lost) failures; stop classifying client-side `Timeout`/`Connection` as blanket-retryable for mutations; ensure each retry uses a **fresh correlation id** and the old one can't cross-match a late response; default `redo_operation`/retry policy to safe-by-default for mutations.
- **Verify:** unit tests on `is_retryable` + retry policy; reasoning/audit re-read (live double-apply repro = documented gap requiring fault injection).

### R6 — Partition routing & CPMap decode (RR-7, RR-10, RR-17)
- **Fix:** CPMap value decode must skip the 8-byte Data header; `*_with_partition_key` affinity routing and `PartitionService::get_partition` must use MurmurHash3-over-serialized-bytes with the 8-byte skip, `abs`, and `% partitionCount` (matching the working IMap key path).
- **Verify:** unit test that affinity routing and `PartitionService` agree with the verified IMap key-path hashing; live CPMap round-trip.

## P1 — Security gate & robustness

### R7 — mTLS enforcement + secret redaction (RR-12, RR-13, RR-14, RR-15)
- **Fix:** verify the server cert against its **DNS identity** (set `server_name` from the configured host, not the IP); read and honor `HostnameVerification`/`verify_hostname`; require an explicit CA / pinning for private PKI (don't silently fall back to public WebPKI roots when a CA is expected); **redact** `password`/`token` in `SecurityConfig`/builder `Debug`.
- **Verify:** unit test that `Debug` output contains no secret; static review of the handshake wiring; live mTLS handshake against a TLS listener = documented gap (no TLS listener stood up this pass).

### R8 — Error surfacing on all invocation paths (RR-18, RR-20, RR-21)
- **Fix:** executor/scheduled/durable + event-journal reads, the auth path, and the reader loop must convert server EXCEPTION / decode failures into typed `Err` and fail in-flight ops, not log-and-continue or return garbage.
- **Verify:** unit/integration tests asserting a server error surfaces; reader-loop error path review.

### R9 — Decoder allocation bounds + fragmentation (RR-22, RR-29)
- **Fix:** bound every attacker-controlled `Vec::with_capacity(len)` in Compact/Portable readers (reject negative/oversized lengths before allocating); add response fragmentation reassembly (BEGIN/END fragment frames + fragmentation id) to the decoder.
- **Verify:** unit tests feeding malformed/negative lengths assert `Err` (no panic/OOM); fuzz = documented gap (cargo-fuzz needs nightly).

## P2 — Assurance infrastructure (so "green" means green)

### R10 — Test validity + lint/hermetic (RR-24, RR-26, RR-27, RR-31, RR-32, RR-33)
- **Fix:** cluster-absence must **fail or formally skip** (not pass green); XA/txn tests must assert the **data effect**; add wire-format **golden-vector** tests; fix the 15 `approx_constant` clippy errors (use `std::f*::consts` or `#[allow]` with rationale); make the benchmark test hermetic (don't mutate a tracked file); remove the dead `mem::zeroed` test footgun.
- **Verify:** clippy `--all-targets` clean; tests assert effects; benchmark leaves tree clean.

### R-supply — audit/deny reconciliation (RR-28)
- **Fix:** reconcile `cargo audit` vs `cargo deny` (add the dev-only webpki/rand advisories to deny's ignore list **with justification**, or update the dev deps); document that production is unaffected.
- **Verify:** both gates agree; `cargo deny check` green with justified ignores.

## P3 — Distributed correctness & process (the long pole — not code-completable here) ⊘
- Jepsen-style linearizability under partition; CP-leader/split-brain/AZ chaos with exactly-once verification; live mTLS handshake suite; continuous fuzzing (nightly); 72h+ soak; independent security audit + pen test; SBOM/signed builds; runbooks/DR drills. These require sustained engineering and external parties; tracked here and in EXECUTION_PLAN.md Phase C.

---

## Execution status
(updated as batches land — see git history on `fix/cbdc-remediation`)

| Item | Status | Notes |
|---|---|---|
| R1 constants | ☑/⚠ | Full table verified vs upstream (29 wrong of 219). Applied 18 live-impacting corrections (Map interceptors/partition-lost/set_all/fetch_values, Client listeners, MultiMap listener, XA service 0x14). **Reverted** `CP_SUBSYSTEM_GET_GROUP_IDS` (value `0x1E0100` is load-bearing as createCPGroup — caused a CP regression). CP_ATOMIC_* source constants documented as dead (proxies use shadows). ContinuousQuery wrong-service flagged. |
| R2 type-ids | ☑ | byte[]→-12, u8/u16/u32/u64→correct, JSON→-130. Golden-vector test + live byte-array round-trip. |
| R3 element-drop | ☑ | 7 fail-loud fixes in map.rs + replicated_map.rs; get_all surfaces per-key errors. Live values() integrity passes. |
| R4 XA | ☑/⚠ | `invoke` now routes through `check_response`; commit/rollback no longer false-succeed; prepare won't default a bad frame to XA_OK. **Exposes** that XA never worked end-to-end (server AIOOBE → framing). False-success bug FIXED; full XA functionality requires framing debugging (remaining). |
| R5 retry | ☑ (verified-safe) | Framework verified safe-by-default: mutations tagged `idempotent=false`, `redo_operation` defaults false → no auto-retry of non-idempotent ops. Residual at-least-once under explicit `redo_operation=true` needs server-honored dedup tokens (remaining). |
| R6 routing/CPMap | ☑/⚠ | PartitionService now uses MurmurHash3 (was SipHash). CPMap coordinated write-header + read-skip-8 fix. Live CPMap round-trip verification pending. |
| R7 mTLS/secrets | ☑ secret / ☐ TLS | Password/token redacted in Debug (verified). TLS DNS-hostname verification + pinning (RR-13/14/15) NOT done — needs rustls connector changes + a live TLS listener. **Remaining.** |
| R8 error surfacing | ☐ | Executor/scheduled/durable + auth paths still bypass check_response. **Remaining** (same pattern as the XA fix). |
| R9 decoder bounds/frag | ☑ bounds / ☐ frag | 19 Compact/Portable `with_capacity(attacker_len)` sites hardened (reject negative, cap pre-alloc). Fragmentation reassembly NOT added (RR-29 downgraded: no reproduced impact up to 64MB). |
| R10 test validity/lint | ☑/⚠ | Cluster-absence now fails under `HZ_REQUIRE_CLUSTER` (RR-24). clippy `approx_constant` gate now GREEN. XA data-effect assertions / golden vectors / hermetic benchmark: partial — XA tests now fail honestly; broader test-validity work remaining. |
| R-supply audit/deny | ☐ | Not changed this pass; documented (vulns dev-only, production unaffected). |
| P3 distributed/process | ⊘ | Tracked: Jepsen, CP-leader/split-brain/AZ chaos, live mTLS suite, fuzzing, soak, pen test, SBOM/signed builds, runbooks. |
