# HazelRust CBDC Remediation — Results

**Merged to `main`** (from `origin/main` `e6386d2`). **Date:** 2026-06-24.
**Commits:** `dff7058` (correctness/type-id/DoS/secret/lint), `bcfd057` (docs), `fa27761` (CPMap + executor error-surfacing), `231ee6f` (XA framing), `13cdf58` (EntryProcessor framing), `2fba538` (docs pass-2), `3b48ff6` (A4 CP-constant hygiene). All pushed to `origin/main`.
**Plan:** `CBDC_REMEDIATION_PLAN.md` · **Roadmap:** `PRODUCTION_READINESS_ROADMAP.md` (both in-repo).

---

## Remediation pass 5 — full technical-validation session (branch `cbdc/full-validation`)

**Date:** 2026-06-24. **Base:** `origin/main` `f23dc4e`. Five money-path defect classes
found and **fixed + verified live against EE 5.7** with before/after evidence (each fix
has a committed regression test that fails before and passes after). The overall verdict
is **unchanged — still NO-GO** (the external P3 items and the remaining engineering tracks
below are not satisfied).

### Fixed & verified live this pass

- **Lead #1 — `partition_count` parsed two bytes early** (`471dd2d`). `connect_to` read
  the auth-response `partitionCount` at content offset **30**; the real layout after
  `RESPONSE_HEADER_SIZE=13` is `status@13(1)`, `memberUuid@14(17 — a 1-byte not-null flag
  + 16-byte value)`, `serialVersion@31(1)`, `partitionCount@32(4)`. Reading `[30..34]`
  yielded a large garbage value that the `>0 && <100000` check silently rejected, leaving
  `partition_count` at **0** (masked by a hard-coded 271 fallback; `get_partition` also
  collapsed every key onto partition 0). **Verified live by hexdumping a real auth
  response** — `partitionCount@32 = 0x0000010F = 271` — and a new regression test asserts
  the client now reports 271 (it read 0 before). NB: the handoff's suggested offset **31**
  was *also* wrong (same 16-vs-17-byte UUID error); the correct offset is **32**, confirmed
  by the live bytes. *Secondary finding (open):* production never populates the partition
  table (`update_partition_table`/`set_partition_owner` have only `#[cfg(test)]` callers; no
  partition-listener/fetch path), so smart routing is non-functional and falls back to
  `addresses[0]` regardless of this fix.

- **Lead #3 — heartbeat-driven reconnect never rebuilt the invocation pool** (`fe373dd`).
  The static `attempt_reconnect` opened a raw, **unauthenticated** socket, inserted it into
  the metadata `connections` map, and never touched the `InvocationService` pool;
  `register_connection` also only ever *added* to a pool (no eviction). So after a member
  connection dropped + reconnected, the pool kept only dead write-halves and `send_raw`'s
  round-robin failed/timed-out every data op until the client was restarted — a severe
  availability defect. Fixed by routing the heartbeat reconnect through `reconnect ->
  connect_to` (re-auth + partition parse + fresh pool) via `Arc<Self>`, and adding
  `InvocationService::remove_address` (called at the top of `connect_to`) to evict the stale
  pool. **Verified live by fault injection** on a dedicated single-member cluster: `ss -K`
  kills the client's socket; before the fix data ops never resume (test FAILS at ~26 s),
  after the fix they resume in ~4.5 s ("resumed at attempt 2"). The existing
  `failover_integration_test.rs` "reconnect" tests were found to be **vacuous** (they never
  induce a disconnect).

- **Silent-element-drop — 4 collection decoders** (`941dfb1`). `decode_entries_response`
  (map `entries`/`get_all`/`entries_with_predicate`), the `DistributedIterator` key/entry
  fetch (`key_set`/`entry_set`/`values_iter`), `multimap` `decode_collection_response` +
  `decode_entry_set_response`, and `replicated_map` `decode_entries_response` used
  `if let Ok(x) = deserialize(..) { push(x) }` and **silently dropped** any element that
  failed to deserialize (marker/null frames are filtered out first, so the swallow only hid
  real decode failures). For a ledger a vanished entry is catastrophic and undetectable.
  Fixed to `?`-propagate. **Verified live by a poison data-effect test**: an `i64` written
  under a map the reader views as `<String,String>` cannot deserialize as a String; before
  the fix `entries()`/`entry_set()` silently returned the shortened collection (test FAILS),
  after the fix they return `Err` (test PASSES). Existing multi-element integration tests
  still pass (markers still handled). **`ringbuffer::read_many` was reverted + documented**:
  its lenient decode is *load-bearing* (the `ReadResultSet` response carries trailing
  item-sequence frames it skips by letting decode fail) — a naive `?` regressed
  `test_ringbuffer_add_multiple_read_many`; a correct fix needs a ReadResultSet reframe
  (tracked open).

- **XA `recover()` was non-functional — in-doubt transactions invisible** (`2a6806d`,
  **new finding, missed by the original audit/leads**). `recover()` decoded the
  `XATransactionCollectTransactions` response by calling `Xid::from_bytes` on each
  *individual* frame (the client's concatenated `[formatId][gtridLen][gtrid][bqualLen][bqual]`
  layout), but the server returns a protocol `List<Xid>` where each Xid spans frames:
  `BEGIN, [formatId:int(4)], [globalTransactionId:byte[]], [branchQualifier:byte[]], END`.
  So `from_bytes` matched nothing and `recover()` **silently returned an empty list even
  when prepared-but-uncommitted (in-doubt) transactions existed** — they could never be
  discovered or resolved. The prior test only asserted "empty or valid", so it never caught
  this. Fixed by grouping the non-marker data frames in threes `(formatId, gtrid, bqual)`
  (erroring on a non-multiple-of-three count — no silent drop). The framing was captured by
  **hexdumping a real recover response**. **Verified live**
  (`test_recover_returns_prepared_in_doubt_transaction`): prepare a branch without
  committing, then `recover()` — before the fix the XID is missing (FAILS), after it is
  returned (PASSES). Also adds **lead #5** coverage
  (`test_xa_two_phase_commit_across_two_maps_data_effect`): full `prepare -> commit(onePhase=false)`
  across two maps; both committed-after-prepare writes are visible to a separate client.

### Method note — prior claims treated as leads, not facts

A read-only multi-agent investigation surfaced **57 "confirmed" findings** (3 critical / 35
high / 11 medium / 8 low). These were treated as *leads requiring live verification*, not
ground truth: the investigation's own Lead-#1 analysis concluded offset **31**, which the
live hexdump **disproved** (it is 32). Cross-referencing against the 166 passing data-effect
integration tests, most **`data-header-skip`** findings appear to be false positives (the
decoders are exercised by passing data-effect tests; e.g. `decode_paging_entries_response`
was flagged but already uses `?`). **Definitive confirmation/refutation of the residual
decoder findings requires the differential-vs-Java wire harness (Track 2), which was not
built this session.**

### Open / not done this session (honest)

- **Track 2 — differential-vs-Java byte-diff harness + golden vectors** (the gold standard to
  resolve the residual decoder findings) — not built.
- **`ringbuffer::read_many`** ReadResultSet reframe (item-list vs trailing seq array).
- **Lead #2 — lenient auth-failure arms**: the connection-closed/read-error/timeout arms in
  `connect_to` are still best-effort (log-only). The A-3 status-byte check + RR-21 already
  reject a *rejected* auth and fail a dead connection's in-flight ops, so the residual risk
  is a transport-fault corner, not a live-exploitable path; hardening it requires teaching
  the in-process unit mocks to send a valid auth-OK frame first. Recommended, not done.
- **Lead #4 — flaky non-ignored cluster-bound tests** confirmed (and the "failover" tests are
  vacuous); recommend `#[ignore]` + rewrite. Not changed.
- **Tracks 3/5/7/8** — continuous fuzz; chaos beyond single-member + exactly-once dedup
  tokens; full live mTLS negative suite + zeroize + leak-scan; dedicated-hardware perf/soak —
  not done this session.

### Baselines after pass 5 (no regressions)

- `cargo nextest run --workspace --test-threads 8 --retries 2` → **2184 / 2184** (a few
  non-ignored cluster-bound tests flake transiently, pass on retry — lead #4).
- `CLUSTER_ADDRESS=127.0.0.1:5701 cargo nextest run -p hazelcast-client --run-ignored
  ignored-only --test-threads 1 --no-fail-fast` → **166 pass / 1 fail** (the 1 is
  `try_lock_with_timeout`, the documented non-defect; the new pass-5 regression tests pass).
- `cargo clippy --workspace --all-targets` and `--features tls` → **0 errors**;
  `cargo fmt --all -- --check` clean.

### Reproduce pass 5 (on the AWS instance, `~/HazelRust`, `source ~/.cargo/env`)

```sh
# dev (3 members + A-4 infra, ~/start_cluster_a4.sh) and the TLS member are standard.
# The L3 reconnect test additionally needs a single-member cluster "solo" on :5710:
#   ~/hz/hz-solo.yaml = { cluster-name: solo, network.port 5710 (auto-increment off),
#   join.tcp-ip member-list [127.0.0.1:5710], license-key $(cat ~/hz/license.key) }
sudo docker run -d --name hzsolo --network host \
  -v ~/hz/hz-solo.yaml:/opt/hazelcast/hz.yaml \
  -e JAVA_OPTS="-Dhazelcast.config=/opt/hazelcast/hz.yaml -Xms256m -Xmx512m" \
  hazelcast/hazelcast-enterprise:5.7.0      # wait for "Members {size:1" in docker logs hzsolo

# Baselines
cargo nextest run --workspace --test-threads 8 --retries 2                    # 2184/2184
CLUSTER_ADDRESS=127.0.0.1:5701 cargo nextest run -p hazelcast-client \
  --run-ignored ignored-only --test-threads 1 --no-fail-fast                  # 166 pass / 1 fail (try_lock non-defect)

# Pass-5 live data-effect tests (offset/silent-drop/XA recovery/2PC)
CLUSTER_ADDRESS=127.0.0.1:5701 cargo nextest run -p hazelcast-client --run-ignored all --test-threads 1 \
  -E 'test(test_partition_count_parsed_from_auth_response) + test(test_entries_propagates_undeserializable_element) + test(test_entry_set_iterator_propagates_undeserializable_element) + test(test_recover_returns_prepared_in_doubt_transaction) + test(test_xa_two_phase_commit_across_two_maps_data_effect)'

# L3 fault injection: kills the client's TCP socket to the solo member, asserts ops resume
HZ_FAULT_INJECTION=1 CLUSTER_ADDRESS=127.0.0.1:5710 cargo nextest run -p hazelcast-client \
  --run-ignored all --test-threads 1 -E 'test(test_data_ops_resume_after_member_reconnect)'
```

---

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
