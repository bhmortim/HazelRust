# HazelRust CBDC Production-Readiness — Evidence Log

Independent validation. Every claim reproduced. Secrets (EE license, GH token) never printed.

- **Target commit:** `origin/main` = `e6386d2a563f0cace203053ba108220817ab56ff`
- **Validation date:** 2026-06-24
- **Auditor environment:** local sandbox (no Rust toolchain) + AWS instance over SSH (`hzcp` → ec2-user@18.225.173.180)
- **Cluster:** 3× Hazelcast Enterprise 5.7.0 (Docker), one cluster `dev`, `127.0.0.1:5701/2/3`. Membership `{size:3, ver:3}` confirmed via member logs. Instance: 16 vCPU / 31 GB / 28 GB free.

## Confirmed acceptance bar (set by stakeholder, Step 0)
- **Scope (all in):** IMap K-V core; CP subsystem (AtomicLong/Ref, FencedLock, CountDownLatch, Semaphore, CPMap); Transactions (Txn + XA); Cache/query/listeners/pub-sub (NearCache, query-cache, predicates/aggregations, EntryProcessors, Topic/ReliableTopic).
- **Consistency:** CP (linearizable) for ledger; AP IMap (sync backups) for cache only. Client must never silently lose/corrupt a committed write.
- **Failover (all in):** single-member loss (RPO=0, RTO≤s); CP leader failover (exactly-once); network partition/split-brain (no stale/corrupt/double-apply; minority fails safe); AZ/multi-member loss (recover within RTO).
- **Security gate (formal):** mTLS with full cert chain + hostname validation. Due-diligence (not hard gate): cargo audit/deny, auth-required, no secret leakage.
- **SLA:** p99 ≤ 10 ms, p999 ≤ 50 ms, ≥ 10k TPS sustained (single-client, single-key, client-side measured).
- **Chaos auth:** destructive allowed; restore to healthy 3-node when done.
- **Soak:** 30–60 min.

## Evidence chronology

### E1 — Repo/commit provenance (PASS)
- Canonical clone `C:\Users\stream\HazelRust` and instance clone `~/HazelRust` both at `e6386d2` = `origin/main`; instance tree clean (zero diff). Stale clone at `C:\Users\stream\projects\...` (`f902b86`) disregarded.

### E2 — fmt gate (PASS)
- `cargo fmt --all -- --check` → exit 0, 0 diff lines. Instance, commit e6386d2.

### E3 — Tooling
- rustc/cargo 1.96.0; cargo-nextest 0.9.138.
- `cargo-audit` and `cargo-deny` were NOT installed → installing from source in background (`/tmp/hzr_toolinstall.log`).
- Strict nextest profile `cbdc` written: retries=0, fail-fast=off, slow-timeout terminate-after=2, test-threads=4, junit on.

### E4 — `mem::zeroed::<K>()` at proxy/map.rs:8668 — NOT a production bug (Low)
- Located inside `#[test] fn test_get_async_returns_join_handle` → nested generic `fn check_return_type<K,V>` that is **never called**; the `unsafe { mem::zeroed::<K>() }` lives in a closure that is **never invoked** and the fn is never monomorphized with a concrete K. Test-only, never executed → no runtime UB. Plan's "zero *production* unsafe" claim is consistent (this is test code). Severity: **Low** (latent footgun / bad pattern), not Critical.

### E5 — Test-harness validity landmine (Medium, methodological)
- `tests/common/mod.rs` `require_cluster!`: if `skip_if_no_cluster()` (TCP connect to 5701 fails within 1s), the test does `return` → **counts as PASSED**. A green pass-count does not prove the test executed against the cluster. Mitigation: cluster confirmed up; will verify tests do real work and watch for mid-run cluster flaps.
- `unique_name()` uses pid+counter (good isolation) but only where tests use it; hardcoded structure names risk cross-test contamination.

(continued…)
