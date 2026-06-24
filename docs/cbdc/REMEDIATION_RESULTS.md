# HazelRust CBDC Remediation — Results

**Branch:** `fix/cbdc-remediation` (commit `dff7058`, 18 files, +446/−100), from `origin/main` `e6386d2`.
**Plan:** `docs/CBDC_REMEDIATION_PLAN.md` (in-repo). **Date:** 2026-06-24.

## Verdict after remediation: still **NO-GO**, now with a clear conditional path

The remediation **eliminated or reduced several disqualifying defect classes** (silent type-id corruption, silent element-drop, XA false-success, secret leakage in logs, decoder DoS, wrong public partition routing, ~18 wrong protocol constants) and **hardened the gates** (clippy green, cluster-absence can no longer false-pass). It also **exposed** latent breakage that the old false-success behavior was hiding (XA, CPMap framing). The system is **not yet production-ready**: TLS hostname verification, secondary-path error surfacing, full XA/CPMap request framing, server-honored dedup tokens, and all of distributed-correctness/soak/pen-test remain. **Re-validation required after those land.**

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
