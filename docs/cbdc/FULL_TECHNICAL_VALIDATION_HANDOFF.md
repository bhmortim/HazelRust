# Full Technical-Validation Handoff — find, fix, and verify-live EVERY client bug

This is a self-contained brief for a fresh session whose mission is the **complete
technical validation of the HazelRust client**: exhaustively hunt for *all*
correctness, protocol, concurrency, connection, serialization, and security
bugs/failure-modes, **fix them, and prove each fix live** against a real
Hazelcast 5.7 Enterprise cluster. This is the engineering half of the
`PRODUCTION_READINESS_ROADMAP.md` (tracks C1/C2/C3/D1/E1 + the residual P0/P1
hardening). It does **not** cover the external/governance P3 items (independent
pen test, third-party Jepsen, dedicated-hardware soak, formal risk sign-off) —
those are not producible by a coding agent, and the overall CBDC verdict
**remains NO-GO** until they are also satisfied.

> Be rigorous and honest. "Tested" means *executed live against EE 5.7 and the
> assertion checks the data effect / wire bytes*, not a green client-side enum.
> Verify behavior yourself; treat prior claims (including this doc) as leads to
> confirm. Do not overstate. Report exactly what is fixed-and-verified vs. open.

---

## 1. Current state (verified, on `origin/main`)

- Head **`0565eda`**. All four "category-A" client items are done & verified live
  (see `REMEDIATION_RESULTS.md` pass 4): A-1 mTLS data ops, A-2 XA (12/12 +
  data-effect), A-3 auth-failure surfacing + reader-loop in-flight fail, A-4 the
  9 infra-blocked `java_parity` tests.
- **Baselines that must not regress:**
  - `cargo nextest run --workspace --test-threads 8` → **2184 / 2184**
    (use `--retries 2`: a few *non-ignored* cluster-bound integration tests flake
    transiently under high parallelism — see lead #4 below).
  - `CLUSTER_ADDRESS=127.0.0.1:5701 cargo nextest run -p hazelcast-client --run-ignored ignored-only --test-threads 1 --no-fail-fast` → **161 / 1**
    (the 1 is `try_lock_with_timeout`, a documented non-defect).
  - `cargo clippy --workspace --all-targets` and `--features tls` → **0 errors**; `cargo fmt --all -- --check` clean.
- Working in-scope & proven: IMap CRUD/bulk/values/keyset/TTL/entry-processor/
  projection/aggregation/paging, CP AtomicLong/Ref/CPMap, `TransactionContext` +
  XA (with data-effect), single-member failover (RPO 0), perf (loopback p99≈175µs,
  ≈18k TPS), mTLS security gate + mTLS data ops.

## 2. Environment & access (everything you need to start)

**Edit locally, build/test on the AWS instance** (no Rust toolchain locally).

- **Local canonical clone (edit here):** `C:\Users\stream\HazelRust` (Windows, git-bash).
- **AWS instance (build + test here):** `ec2-user@18.225.173.180`.
  - SSH key: `~/.ssh/hzcp.pem`. It may have CRLF that breaks OpenSSH — fix once:
    ```sh
    tr -d '\r' < ~/.ssh/hzcp.pem > ~/.ssh/hzcp.fixed.pem && chmod 600 ~/.ssh/hzcp.fixed.pem
    ```
    then connect with:
    ```sh
    ssh -i ~/.ssh/hzcp.fixed.pem -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new \
        -o UserKnownHostsFile=~/.ssh/known_hosts_hzcp -o ServerAliveInterval=30 ec2-user@18.225.173.180
    ```
    The dedicated SSH MCP tool fails on a host-key mismatch — SSH directly with the key.
  - Instance repo: `~/HazelRust`. Toolchain: `source ~/.cargo/env` (rustc 1.96, cargo-nextest 0.9.138). Java 21 (Corretto).
- **Clusters (Hazelcast EE 5.7.0, Docker, `--network host`; verify with `sudo docker ps`):**
  - **dev** — 3 members, plaintext, `127.0.0.1:5701/2/3`, CP subsystem enabled (containers `hz1/hz2/hz3`). Currently launched by **`~/start_cluster_a4.sh`** (adds the A-4 server infra: per-entry-stats, MapStore, `DataSerializableFactory`, and mounts `~/hz/a4classes.jar` into each member's `/opt/hazelcast/lib`). `~/start_cluster_ee.sh` is the stock (no-A-4) launcher. After any restart, wait for `Members {size:3` in `sudo docker logs hz1`.
  - **tls** — 1 member, **mutual-TLS REQUIRED**, `127.0.0.1:5801` (container `hztls`, cluster name `tls`). Certs in `~/certs` (`ca.crt`, `client.crt`, `client.key`; server SAN `hzcp.test`+`127.0.0.1`; keystore pw `hztest123`). Config `~/hz/hz-tls.yaml`.
- **EE jars:** `~/hzlib` (`hazelcast-enterprise-5.7.0.jar`, `cache-api-1.1.1.jar`, …) — use these to compile server-side Java classes and to build a **stock Java reference client** for differential testing.
- **License:** staged at `~/hz/license.key` on the instance — read it there (`LIC=$(cat ~/hz/license.key)`), **never print, log, or commit it**; mask anything license-shaped in output and `docker inspect`.
- **A-4 server artifacts** (reproducible): sources + README in-repo at `docs/cbdc/java_parity_infra/`; compiled jar lives at `~/hz/a4classes.jar`. A stock-Java XA differential capture template is at `~/xacap/XaMultiCapture.java` (compile with the `~/hzlib` jars; it ran live this session).
- **AWS SSO:** profile `PowerUserAccess-539247491847` (acct 539247491847). Re-run `aws sso login --profile PowerUserAccess-539247491847` if AWS calls fail. (Usually not needed — SSH + the running clusters suffice.)

## 3. Build / test / sync workflow

Edit locally → sync to the instance → build + test on the instance.
- Sync (tar-pipe): `bash C:\Users\stream\hazelrust-validation\sync.sh` (tars `hazelcast-*/src|tests|benches`, `Cargo.toml`s, `deny.toml`, `docs` → `~/HazelRust`). `tar` extracts, does **not** delete; scratch test files placed only on the instance won't pollute local git.
- On the instance (`source ~/.cargo/env`): `cargo build --workspace [--features tls]` · `cargo clippy --workspace --all-targets [--features tls]` · `cargo fmt --all` · `cargo nextest run …`.
- **Always run `cargo fmt --all` on the instance and tar the changed files back before committing** (commit from the local clone). Commit + push to `main` per verified fix.
- Long runs: run the SSH command in the background (no `nohup`) so you're notified on completion; or poll a log. The dev cluster accumulates heap/state over long runs — **restart it clean (`~/start_cluster_a4.sh`) before a definitive regression run** (stale heap causes transient flakes; see lead #4).

## 4. Validation tracks (do these; each ends with a live-verified exit criterion)

1. **Trustworthy harness.** Make a missing/unhealthy cluster fail loudly (`HZ_REQUIRE_CLUSTER=1`); make every money-path test assert the **data effect**, not a client enum; fix or correctly `#[ignore]` the non-ignored cluster-bound integration tests that flake under parallelism (lead #4). Exit: a clean, deterministic full run; coverage on both crates with a ratcheting floor.
2. **Wire-format conformance (gold standard).** Build a **differential harness vs the stock Java EE client** (`~/hzlib`): drive identical ops from both clients against the same cluster, capture frames (tiny logging TCP proxy on a spare port, or `sudo tcpdump -i lo`), and **byte-diff** request + response for *every* in-scope op (IMap, CP, XA, executors, listeners, predicates/aggregations/projections, multimap, replicated-map, queue/set/list/topic/ringbuffer). Add **golden-vector** tests pinning partition-hash (MurmurHash3) and every codec/message-type. Exit: byte-level parity for all in-scope ops; golden vectors green.
3. **Property + fuzz.** `proptest`: `decode(encode(x)) == x` and **decode-never-panics on arbitrary bytes** for Compact/Portable/Identified/Frame/ClientMessage. Run the 5 `cargo-fuzz` targets in `hazelcast-core/fuzz/fuzz_targets/` continuously with a persistent corpus; fix every crash/OOM. Exit: round-trip + never-panic green; crash-free fuzz hours trending up.
4. **Decoder/serialization audit.** Systematically audit **every** decoder for: the 8-byte Data-header skip (two such bugs were found+fixed this session in `decode_entry_processor_results` and `decode_projection_response` — there may be more), silent element-drop, nullable/`IS_NULL` handling, `EntryList`/`List<Data>` framing, and fragmentation reassembly (>64MB). Cross-client type fidelity (byte[]/unsigned/JSON/Compact/Portable). Exit: no silent drop/corruption proven cross-client.
5. **Distributed correctness / fault injection.** Beyond single-member loss: CP-leader failover, split-brain + merge, AZ/member add-remove under load, partition-table migration, slow/stuck members. **Exactly-once under retry** (server-honored dedup token; no double-apply of a money mutation; no stale response delivered to the wrong attempt). Jepsen-style linearizability for CP structures (AtomicLong/Ref/FencedLock/CPMap) + the documented AP model for IMap. Exit: injected-failure schedules survive without loss/corruption/double-apply.
6. **Connection / concurrency robustness.** Verify reconnect **repopulates the InvocationService pool** (lead #3); `loom`-model the connection-manager lock+atomic+channel logic; TSan/ASan/MIRI passes; backpressure/overload degrades gracefully (no OOM). Exit: reconnect restores data ops; concurrency models clean.
7. **Security gate.** Un-ignore and run the full **mTLS suite** against the live `tls` member (valid chain + hostname + client cert succeeds; expired/wrong-host/untrusted/bypass-attempt rejected); zeroize credentials/license/token in memory; leak-scan every log/error/panic path; harden the lenient auth arms (lead #2). Exit: mTLS mandatory + enforced; no secret leakage.
8. **Perf/soak (caveated).** Re-measure with real network RTT if dedicated hardware is available; gate p50/p99/p999/p9999; 72h+ soak with flat memory/FDs. (Loopback-only numbers are not representative — note this honestly.)

## 5. Known leads / weak spots observed this session (confirm + fix)

1. **partition_count parse is likely off-by-one.** `connection/manager.rs` (`connect_to`, ~line 503) reads `partition_count` at auth-response content **offset 30**, but with `RESPONSE_HEADER_SIZE=13` the layout is status@13, memberUuid@14..30, serializationVersion@30, partitionCount@**31**..35. The wrong value passes a loose sanity check (`>0 && <100000`) so it's latent. Confirm via a hexdump of a real auth response and fix; check whether smart-routing depends on it.
2. **Lenient auth failure arms.** `connect_to`'s connection-closed / read-error / timeout arms are log-only (a dead connection still gets registered). Kept lenient because the in-process unit mocks don't speak the auth handshake. Harden by making those mocks send a valid auth-OK response (a single frame, `BEGIN_FLAG|IS_FINAL_FLAG`, status byte 0 at `content[13]`, ≥34 bytes) and then hard-failing all non-AUTHENTICATED outcomes.
3. **Reconnect does not repopulate the invocation pool.** `reconnect`/`attempt_reconnect` re-insert into `self.connections` (metadata) but do **not** rebuild the `InvocationService` pool — after a heartbeat-driven drop + reconnect, data ops may fail with "no connection". Reproduce (kill a member's connection mid-load) and fix.
4. **Flaky non-ignored cluster-bound integration tests.** Under `--test-threads 8`, queue/topic/list/transaction integration tests occasionally fail transiently (different test each run; pass in isolation / on `--retries 2`). Decide: `#[ignore]` them (they need a cluster) or make them robust; check whether the new RR-21 in-flight-fail surfaces transients that previously self-healed.
5. **XA two-phase data-effect.** A-2 added one-phase-commit and rollback-after-prepare data-effect tests; add a full **prepare→commit** data-effect across two resources/maps, and confirm the committed-after-prepare write is visible via a separate client.
6. **EntryProcessor IDS ids are hardcoded in the test.** `factory_id()/class_id()` default `None`; the java_parity processors hardcode `(1,1)`/`(1,2)` to match the deployed server classes. A production design should let real processors declare their own ids and register matching server factories.

## 6. Constraints (non-negotiable)

- **Never** print, log, or commit the EE license or any token; read the license from `~/hz/license.key`. Don't commit the SSH private key — reference its path only.
- Use **Hazelcast 5.7 EE** for all testing.
- Work on a branch; build + test on the instance against the live cluster; **verify live before claiming done**; commit and push to `main` per verified fix; **restore clusters to healthy** when done.
- Keep `REMEDIATION_RESULTS.md` + `PRODUCTION_READINESS_ROADMAP.md` updated; report the resulting suite counts.
- Don't regress the baselines in §1. The overall CBDC verdict **stays NO-GO** until the external P3 items land — say so plainly.

## 7. Definition of done

Each track either closed with live-verified evidence (data-effect / byte-diff / fuzz-clean / fault-injection-survives) or documented as genuinely external with evidence. No silent data loss/corruption; byte-level parity with the Java client for in-scope ops; transactions atomic by data-effect; exactly-once under failover proven; mTLS enforced with no secret leakage; property + golden-vector + fuzz green; reconnect restores data ops. Report the final non-ignored and ignored suite counts and an honest fixed-vs-open ledger.
