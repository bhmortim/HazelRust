# HazelRust CBDC Remediation — Results

**Merged to `main`** (from `origin/main` `e6386d2`). **Date:** 2026-06-24.
**Commits:** `dff7058` (correctness/type-id/DoS/secret/lint), `bcfd057` (docs), `fa27761` (CPMap + executor error-surfacing), `231ee6f` (XA framing), `13cdf58` (EntryProcessor framing), `2fba538` (docs pass-2), `3b48ff6` (A4 CP-constant hygiene). All pushed to `origin/main`.
**Plan:** `CBDC_REMEDIATION_PLAN.md` · **Roadmap:** `PRODUCTION_READINESS_ROADMAP.md` (both in-repo).

### Disposition of the remaining ignored-suite failures (investigated, not guessed)
- **9 java_parity = was server-side infra; now CLOSED (pass 4 / A-4).** Deployed the MapStore / EntryProcessor+factory / per-entry-stats / typed (JSON, numeric) values and fixed two latent client decode bugs (missing 8-byte Data-header skip in `decode_entry_processor_results` and `decode_projection_response`). All 9 now pass live. See pass 4 below and `java_parity_infra/`.
- **`try_lock_with_timeout` = not a client defect** (the only remaining ignored failure). Both `lock` and `try_lock` use `threadId=0`, so re-locking the client's own key is **correctly reentrant**; the test assumes Java multi-threading the single-`threadId` client doesn't model.
- **`multiple_xa_transactions` = CLOSED (pass 4 / A-2).** Was a response-misrouting race on the shared metadata connection, not a framing bug; fixed by routing XA through the correlation-matched `invoke_pinned`.

### Remediation pass 3 — mTLS (verified against a live TLS EE member)
Stood up a mutual-auth Hazelcast EE 5.7 TLS member (cluster `tls`, port 5801, CA + server cert with SAN `hzcp.test`/`127.0.0.1` + client cert) and exercised the client's `tls` feature end-to-end. Found + fixed **three** TLS defects: (1) **missing rustls CryptoProvider** — every TLS connection *panicked* (TLS was 100% broken); (2) **missing `CP2` protocol preamble** on the TLS path — server rejected connections (`Unknown protocol`); (3) **R7** — server cert verified against the IP, not the configured DNS identity. **Verified live:** mTLS handshake + client-cert auth succeeds (`authenticated=true`); an **untrusted-CA** server cert is **rejected** (chain validation); a **wrong hostname** is **rejected** (R7/SAN validation). The mTLS *security* properties of the gate are proven.

### Remediation pass 4 — category-A client engineering (all four items DONE & verified live)
The four technically-completable client items from `NEXT_SESSION_HANDOFF.md`, each fixed, verified live against EE 5.7, and pushed to `main`. (This does **not** change the overall CBDC verdict, which remains NO-GO pending the external P3 items.)

- **A-1 — mTLS data operations: DONE & verified live** (commit `93127d3`). Root cause was NOT a TLS reset: the server log's `connectionType=RST` is the client *type label* the client sends in its auth frame (`b"RST"`), not a TCP reset. The real bug was that the invocation pool was never populated for a TLS member — pool connections were handed to the `InvocationService` via `Connection::into_tcp_stream()`, which returns `Some` only for the plaintext variant and `None` for TLS, so every authenticated TLS pool connection was silently dropped and `send_raw` then failed with "no connection". Fix: `Connection::into_split_halves()` splits the stream into boxed `AsyncRead`/`AsyncWrite` halves for both transports (lock-free `TcpStream::into_split` for plaintext; `tokio::io::split` for the `tokio_rustls` stream); the pool now stores boxed write halves and registers every connection unconditionally. **Verified live** against the mutual-TLS EE member (cluster `tls`, :5801): mTLS put/get round-trip succeeds (repeatably), the untrusted-CA and wrong-hostname negative cases still reject, and plaintext suites are unaffected (unit **2183/0**; ignored integration no regression). Committed regression test `test_mtls_data_operation_round_trip` (env-gated) + the scratch `cbdc_mtls_verify` harness.

- **A-2 — XA two-phase commit + concurrent XA: DONE & verified live (10/10 → 12/12 with data-effect asserts)** (commit `4ebde9e`). The handoff's premise was **stale**: commit-after-prepare (`full_lifecycle`, `rollback_after_prepare`) already passed; the only failing case was `multiple_xa_transactions`, which failed **non-deterministically** with a server `ArrayIndexOutOfBoundsException`. Diagnosis (gold-standard): a stock **Java EE client** running the identical sequence (start1/start2/end1/end2/commit1/rollback2) **succeeds**, so it is not invalid usage; and a wire hexdump showed the failing and passing runs send a **byte-identical, correctly-framed** commit (type `0x140400`, `[16-byte header][17-byte UUID][1-byte onePhase]`, correct transactionId) — so it is **not a framing bug**. Root cause: XA `invoke()` used `send_to`/`receive_from` on the shared metadata connection with **no correlation-id matching**, so unsolicited cluster events / heartbeat responses were mis-read as XA responses, corrupting the decoded transactionId and triggering the server AIOOBE on a later commit/rollback (explains "fails alone, passes in-suite once cluster events settle"). Fix: route XA ops through the correlation-matched `invoke_pinned` and **pin each branch to one member endpoint** for its lifecycle. Also added `XATransaction::get_map` so data ops run inside an XA branch. **Verified live:** XA **12/12** (`multiple_xa` now 6/6 in isolation), plus two new data-effect tests — a committed XA write **is visible to a separate client**, and a rolled-back (post-prepare) XA write **is not**. No regressions.

- **A-3 — auth-failure surfacing (R8) + reader-loop in-flight fail (RR-21): DONE & verified live** (commit `f3cd5ad`). (1) `connect_to` now reads the authentication response **status byte** at `content[RESPONSE_HEADER_SIZE]` (offset 13): a non-zero status (1 credentials-failed / 2 serialization-mismatch / 3 not-allowed-in-cluster) returns a typed `Err(Authentication)` and the connection is **not registered**. Verified live by forcing rejection with a **wrong cluster name** against the EE member — the member answers `status=1` (confirmed by hexdump) and `HazelcastClient::new` returns `Err` (previously it could return `Ok` with a half-authenticated connection); the correct cluster name still authenticates and `put`/`get` work (proves the offset). The connection-closed / read-error / timeout arms remain **best-effort log-only**: a real member always answers auth with a status byte, these arise only from transport faults, the resulting connection is dead (its first op fails) and RR-21 fails its in-flight ops fast — and hard-failing them would also reject the in-process mock servers in the unit suite, which don't speak the auth handshake. (2) The invocation `reader_loop` now fails the connection's **in-flight invocations** with a typed `Connection` error on a decode/IO break or clean close, instead of letting them hang until the invocation timeout. Because `pending_ops` is global across members, each connection tracks its own dispatched correlation ids (`InFlight` set) so a teardown fails **only that connection's** ops. Verified by a deterministic unit test (`in_flight_op_fails_fast_on_connection_teardown`, in-memory duplex) that completes in ~0.1 s vs the 30 s timeout. Committed `auth_failure_test.rs` (env-gated). No regressions.

- **A-4 — close the 9 infra-blocked `java_parity` tests: DONE & verified live (9/9)** (commit `6cf2edd`). Deployed the missing server-side artifacts to the dev cluster (Docker EE members; classes jar mounted into `/opt/hazelcast/lib`, config via `start_cluster_a4.sh`; both checked in under `docs/cbdc/java_parity_infra/`) and fixed two genuine client decode bugs the proper setup exposed:
  - `get_entry_view` — added `per-entry-stats-enabled`+`statistics-enabled` (config only). ✅
  - `load_all`/`load_all_keys` — added a `MapStore` (`JavaParityMapStore`) + `map-store` config on the two load maps. ✅
  - `execute_on_key`/`execute_on_keys`/`submit_to_key_async` — deployed `IncrementEntryProcessor` (factoryId 1/classId 1) + `JavaParityFactory`; the client now serializes a processor as `IdentifiedDataSerializable` when it declares `factory_id`/`class_id` (new `EntryProcessor` methods, default `None` so existing processors are unaffected). **Client bug fixed:** `decode_entry_processor_results` did not skip the 8-byte Data header, so every key decoded as `""` and the results collapsed to one entry. ✅
  - `execute_on_entries_with_predicate` + `entry_set_with_predicate` — these compared `this >/>= 100` over **String** values, which the member compares *lexically* (so "50" matches); reworked to numeric i64 values (+ a Long processor, classId 2). The tests, not the client, were at fault. ✅
  - `project` — projecting `name` over plain String values returns nothing; reworked to store `HazelcastJsonValue` (queryable JSON, type -130). **Client bug fixed:** `decode_projection_response` did not skip the 8-byte Data header, so every projected value decoded empty. ✅

  **Verified live:** ignored integration **152/10 → 161/1** (the lone remaining failure is `try_lock_with_timeout`, the documented non-defect). `entry_processor_test` still passes (the trait additions are backward-compatible). clippy 0 errors (default + tls).

### Still open
- **`try_lock_with_timeout`** — not a client defect (documented above); the single-`threadId` client is correctly reentrant.
- **P2/P3** (Jepsen, chaos beyond single-member, dedicated-hardware perf + 72h soak, continuous fuzzing, third-party pen test, governance/sign-off) — external infra and parties.

> **Test-suite note:** under `cargo nextest --test-threads 8`, a few *non-ignored* cluster-bound integration tests (queue/topic/list/transaction) occasionally hit a transient connection error and fail; the failing test varies per run and they pass in isolation and on `--retries 2` (unit then **2184/2184**). This is environmental (cluster under high client-parallelism), not a category-A regression — those structures are untouched by A-1..A-4.

## Verdict after remediation: still **NO-GO**, now with a clear conditional path

The remediation **eliminated or reduced several disqualifying defect classes** (silent type-id corruption, silent element-drop, XA false-success, secret leakage in logs, decoder DoS, wrong public partition routing, ~18 wrong protocol constants), **made three non-functional in-scope features work** (CPMap, XA, EntryProcessor — pass 2), and **hardened the gates** (clippy green, cluster-absence can no longer false-pass). The system is **not yet production-ready**: TLS hostname verification, secondary-path error surfacing, the XA two-phase commit-after-prepare path, server-honored dedup tokens, and all of distributed-correctness/soak/pen-test remain. **Re-validation required after those land.**

### Remediation pass 2 — protocol request framing (CPMap / XA / EntryProcessor)

The three in-scope features that were *non-functional* (server codec crashes) were re-framed against the authoritative upstream codecs and verified live:

| Feature | Before | After |
|---|---|---|
| **CPMap** | server `NullPointerException` (request missing the entire `RaftGroupId` group) — unusable | **put/get round-trip verified live** (i64 + String); group resolved + encoded; value decode fixed |
| **XA** | every op `AIOOBE`/`NPE`; false-success masked it — unusable | **7/10 cases pass live** (create, one-phase commit, prepare, recover, rollback-from-active, suspend/resume, timeout, auto-xid, via-context); transactionId now captured from create |
| **EntryProcessor** | `execute_on_key/keys`, `submit_to_key` crashed the server codec at decode | **framing fixed** — `entry_processor_test` cases pass; `java_parity` variants now fail only with a clean post-decode `ClassCastException` (custom class not deployed = infra) |

**Live test deltas (ignored integration suite):** 148 pass / 10 fail (baseline, with XA *vacuously* passing via the false-success bug) → 140 / 18 (after pass 1 made XA fail *honestly*) → **147 / 11** (after pass 2 made the features actually work). Non-ignored suite: **2183 / 0** throughout (zero regressions). The 11 remaining ignored failures are now all (a) genuine server-side **infra** gaps — MapStore (`load_all`/`load_all_keys`), typed domain objects (`project`/`entry_set_with_predicate`), per-entry stats (`get_entry_view`), registered server-side EntryProcessor classes (`execute_on_key/keys/entries`, `submit_to_key_async` — framing now proven correct) — or (b) two residual code items: the XA two-phase commit-after-prepare path (state-dependent server `AIOOBE`) and `try_lock_with_timeout`. **None are the silent-corruption or false-success class.**

## What was implemented and how it was verified

| Batch | Change | Verification |
|---|---|---|
| **R2 type-ids** | `byte[]/[u8]`→BYTE_ARRAY(-12); `u8/u16/u32/u64`→correct ids; JSON→-130 | Golden-vector unit test (`test_constant_type_ids_match_hazelcast`) + **live** non-UTF8 byte-array round-trip ✅ |
| **R3 element-drop** | 7 decoders (values/key_set/get_all/projection/paging/entry-proc + ReplicatedMap) now `?`-propagate; get_all surfaces per-key errors | **Live** values()/ledger-sum integrity (1000 elems) ✅; 2183/2183 non-ignored pass |
| **R4 XA atomicity** | XA `invoke`→`check_response`; commit/rollback no longer false-succeed; prepare won't default to XA_OK | Suite now shows XA failing **honestly** (server framing error surfaced) instead of vacuous pass ✅ |
| **R1 constants** | 18 corrected vs upstream (Map interceptor/partition-lost/set_all/fetch_values, Client listeners, MultiMap listener, XA service 0x14); full 219-constant audit (`CONSTANTS_VERIFICATION.md`) | Build/suite green; **caught + reverted** a regression (CP_SUBSYSTEM value is load-bearing) — the live suite detected it ✅ |
| **R6 routing/CPMap** | PartitionService SipHash→MurmurHash3; CPMap Data-header write+read | PartitionService correct-by-construction (matches verified IMap helper); CPMap fix applied but **blocked by pre-existing request-framing defect** (see below) |
| **R7 secret** | `SecurityConfig`/Builder `Debug` redact password+token | Code-verified; no secret in `{:?}` ✅ |
| **R9 DoS** | 19 Compact/Portable `with_capacity(attacker_len)` sites: reject negative, cap pre-alloc | Build green; logic-verified ✅ |
| **R10 test/lint** | cluster-absence fails under `HZ_REQUIRE_CLUSTER`; clippy `approx_constant` gate green | `cargo clippy --workspace --all-targets` → **0 errors** ✅; fmt clean |
| **R5 retry** | (verification, not change) framework is safe-by-default | Confirmed: mutations tagged `idempotent=false`, `redo_operation` defaults false ✅ |

## Test evidence (reproduced on the live EE cluster)

| | Baseline (`e6386d2`) | After remediation |
|---|---|---|
| Build / fmt | ok / clean | ok / clean |
| **clippy `--all-targets`** | **15 errors** (approx_constant) | **0 errors** ✅ |
| Non-ignored tests | 2182 pass / 0 fail | **2183 pass / 0 fail** (+1 new golden-vector test; zero regressions) |
| Ignored integration | 148 pass / 10 fail | 140 pass / **18 fail** |

The "drop" from 148→140 is **entirely the 8 XA tests now failing honestly** (they previously passed only because `commit` returned `Ok(())` unconditionally). AtomicLong/AtomicReference were briefly regressed by a constant change and **restored** after reverting it (the suite caught it). The 10 java_parity failures are unchanged (pre-existing infra + framing).

## Honest course-corrections (prime directive applied to my own audit)

Three audit "Critical/High" items did **not** hold up under live/source scrutiny and were re-dispositioned rather than blindly "fixed":
- **Fragmentation reassembly (RR-29):** no data corruption reproduced up to 64 MB → latent, downgraded.
- **CPMap "every read corrupted" (RR-7):** write was *also* headerless, so same-client round-trips worked; the real issue is cross-client + a **separate request-framing defect** that blocks CPMap entirely (server NPE on a missing frame — independent of my value fix).
- **Retry double-apply (RR-3):** the retry framework is already safe-by-default; double-apply needs explicit `redo_operation=true`.

## Remaining (tracked; required before GO + re-validation)

- **R7-TLS:** real DNS-hostname/SAN verification + private-PKI pinning (needs rustls connector changes + a live TLS listener to verify).
- **R8:** executor/scheduled/durable + auth + reader-loop must surface server errors (same pattern as the XA fix).
- **Full XA & CPMap request framing:** both error server-side with framing NPEs/AIOOBE — the request encoders are mis-framed (same class as java_parity `execute_on_keys`). XA/CPMap are non-functional until fixed.
- **Idempotency/dedup tokens:** server-honored op dedup for exactly-once under retry/`redo_operation`.
- **Constant naming hygiene:** add `CP_GROUP_CREATE_CP_GROUP=0x1E0100`, repoint CP proxies, then give `CP_SUBSYSTEM_*` real values; fix ContinuousQuery service (0x16); audit the remaining ~11 wrong/unverifiable constants in `CONSTANTS_VERIFICATION.md`.
- **P3:** Jepsen linearizability, CP-leader/split-brain/AZ chaos, live mTLS suite, continuous fuzzing, 72h soak, independent pen test, SBOM/signed builds, runbooks/DR.

## Reproduce

```
git checkout fix/cbdc-remediation            # commit dff7058
cargo fmt --all -- --check                    # clean
cargo clippy --workspace --all-targets        # 0 errors
cargo build --workspace                       # ok
cargo nextest run --workspace                 # 2183 passed, 0 failed
CLUSTER_ADDRESS=127.0.0.1:5701 cargo nextest run -p hazelcast-client \
  --run-ignored ignored-only --test-threads 1 --no-fail-fast   # 140 passed, 18 failed (8 XA honest, 10 java_parity infra/framing)
```
