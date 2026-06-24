# Next-Session Handoff — finish the remaining client engineering (category "a")

This is a self-contained brief for a fresh session to **finish the technically-completable
client gaps** identified by the CBDC production-readiness validation. It is the result of a
multi-pass remediation already on `main` (see git log `dff7058..5dc9b79`, and
`REMEDIATION_RESULTS.md` / `PRODUCTION_READINESS_ROADMAP.md` in this folder).

> Scope of THIS handoff = the four engineering items below ("category a"). It does **not**
> cover the external/governance items (independent pen test, Jepsen, 72h soak on dedicated
> hardware, formal risk sign-off) — those are not producible by a coding agent.

## Current state (verified)
- `origin/main` head `5dc9b79`. `cargo build --workspace` clean; `clippy --workspace --all-targets` (and `--features tls`) **0 errors**; `fmt` clean.
- Non-ignored test suite: **2183 passed / 0 failed**. Ignored integration: **147 / 11** (the 11 are the items below + already-dispositioned infra/test cases).
- Working in-scope: IMap (get/put/remove/bulk/values/keyset/get_all/TTL), CP AtomicLong/AtomicReference, **CPMap** (live put/get), non-XA `TransactionContext` (commit/rollback with data effect), predicates/aggregations/paging, single-member failover (RPO=0), perf (p99≈175µs, ≈18k TPS). mTLS *security* (handshake/auth/chain/hostname) verified.

## Environment & access
- **Local clone (edit here):** `C:\Users\stream\HazelRust` (Windows; no Rust toolchain locally).
- **AWS test instance (build + test here):** `ssh ec2-user@18.225.173.180`, key `~/.ssh/hzcp.pem`.
  - The key file may have CRLF that breaks OpenSSH; fix once: `tr -d '\r' < ~/.ssh/hzcp.pem > ~/.ssh/hzcp.fixed.pem && chmod 600 ~/.ssh/hzcp.fixed.pem`, then `ssh -i ~/.ssh/hzcp.fixed.pem -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new -o UserKnownHostsFile=~/.ssh/known_hosts_hzcp ec2-user@18.225.173.180`. The dedicated SSH MCP tool fails on a host-key mismatch — SSH directly with the key.
  - Instance clone: `~/HazelRust`. Java 21 (Corretto) + Hazelcast EE 5.7 jars in `~/hzlib`.
- **Clusters (Hazelcast EE 5.7.0, Docker, `--network host`):**
  - `dev` — 3 members, plaintext, `127.0.0.1:5701/2/3`, CP subsystem enabled (containers `hz1/hz2/hz3`).
  - `tls` — 1 member, **mutual-TLS REQUIRED**, `127.0.0.1:5801` (container `hztls`). Certs in `~/certs` (PEM: `ca.crt`, `client.crt`, `client.key`; keystores `server.p12`/`truststore.p12`, password `hztest123`; server SAN = `hzcp.test` + `127.0.0.1`). Config `~/hz/hz-tls.yaml`.
  - Cluster config: `~/hz/hz*.yaml`; launch script `~/start_cluster_ee.sh` (reads license from `~/hz/license.key`).
- **AWS SSO:** profile `PowerUserAccess-539247491847` (account 539247491847). The session is time-limited — if AWS calls fail, ask the user to re-run `aws sso login --profile PowerUserAccess-539247491847`. (AWS is generally not needed for the work below; SSH + the running clusters suffice.)

## Build / test / sync workflow
Edit locally → sync to the instance → build + test on the instance.
- Sync (tar-pipe, edit-then-push): from git-bash, tar the source dirs and pipe over ssh, e.g.
  `tar czf - hazelcast-core/src hazelcast-client/src hazelcast-client/tests hazelcast-client/benches | ssh <opts> ec2-user@18.225.173.180 'tar xzf - -C ~/HazelRust'`. (A `sync.sh` doing exactly this is at `C:\Users\stream\hazelrust-validation\sync.sh`.) Run `cargo fmt --all` on the instance and tar changed files back before committing, so committed code is formatted.
- Commands (on the instance, `source ~/.cargo/env`):
  - `cargo build --workspace` · `cargo clippy --workspace --all-targets` (and `--features tls`) · `cargo fmt --all -- --check`
  - Unit/non-ignored: `cargo nextest run --workspace --test-threads 8`  → expect 2183/0.
  - Ignored integration: `CLUSTER_ADDRESS=127.0.0.1:5701 cargo nextest run -p hazelcast-client --run-ignored ignored-only --test-threads 1 --no-fail-fast`.
  - TLS tests: add `--features tls`.

## Constraints (non-negotiable)
- **Never** print, log, or commit the EE license or any token. The license is already staged at `~/hz/license.key` on the instance — reference it there (`LIC=$(cat ~/hz/license.key)`), never echo it. Mask anything license-shaped in output and `docker inspect`.
- Use **Hazelcast 5.7 EE** for all testing.
- Work on a branch; build + test on the instance against the live cluster; **commit and push to `main`** when an item is verified. Restore clusters to healthy when done.
- Verify live before claiming done; document gaps honestly. Keep `REMEDIATION_RESULTS.md` + `PRODUCTION_READINESS_ROADMAP.md` updated.

---

## Work item A-1 — mTLS *operations* (the security gate is proven; ops don't complete)
**Status:** mTLS handshake + client-cert auth, server-cert chain validation, and DNS/SAN hostname verification are all verified live (commit `2c8df13`). Three TLS bugs were fixed there: rustls `CryptoProvider` install, the `CP2` protocol preamble on the TLS path, and `sni_hostname`→`server_name` (R7), all in `hazelcast-client/src/connection/connection.rs`.
**Symptom:** data operations over TLS fail — `put` returns `Connection("no connection to 127.0.0.1:5801")`. Server (`hztls`) log shows the client **authenticates** (`ClientEndpoint{... authenticated=true, clientName=hazelrust, clientVersion=5.6.0}`) but the endpoints are then **`Destroying ... connectionType=RST`**, and some pool connections intermittently hit `javax.net.ssl.SSLHandshakeException: TLS handshake failed`. So the auth/metadata connection authenticates over mTLS, but the **invocation connection pool** doesn't settle (connections reset before use). Plaintext (`dev`) is unaffected.
**Where to look:** `connection/manager.rs` — pool creation `create_connection` (~560) and `create_connection_static` (~1868) both route through `Connection::connect_tls`; `authenticate_connection_inline` (~514) and InvocationService connection registration; the connection read-loop / `Stream` enum read path in `connection.rs`. Hypotheses to test: (a) pool connections race the TLS handshake / shared init; (b) the read-loop on a `tokio_rustls` stream mis-handles partial reads or the post-preamble first read, causing the client to close; (c) the data connections aren't being registered with the InvocationService after TLS auth. Add tracing on the pool-connection lifecycle; compare plaintext vs TLS paths.
**Verify with** the mTLS harness (recreate `hazelcast-client/tests/cbdc_mtls_verify.rs` — full content saved at `C:\Users\stream\hazelrust-validation\cbdc_mtls_verify.rs`): `cargo nextest run -p hazelcast-client --features tls -E 'binary(cbdc_mtls_verify)'`. Goal: `mtls_connect_and_op_succeeds` passes (put/get round-trip over mTLS) while the two negative tests keep passing. Also fold the three TLS bug fixes into proper unit/integration coverage.

## Work item A-2 — XA two-phase commit-after-prepare
**Status:** XA is 7/10 live (commit `231ee6f`): create, one-phase commit, prepare, recover, rollback-from-active, suspend/resume, timeout, auto-xid, via-context all work. The 3 failures (`full_lifecycle`, `rollback_after_prepare`, `multiple_xa_transactions`) are the **two-phase commit/rollback AFTER prepare** path.
**Symptom:** server returns `ArrayIndexOutOfBoundsException: Index 16 out of bounds for length 16` on `commit(one_phase=false)` after a successful `prepare`. The Rust commit bytes look spec-correct (messageType `0x140400`, correlationId, partitionId `-1`, nullable UUID 17B = `isNull(1)+msb(8)+lsb(8)`, then `onePhase` bool). The server does **not** log a stack trace (it returns the exception to the client).
**Approach (gold standard):** differential byte capture vs the stock Java client. Java 21 + `hazelcast-enterprise-5.7.0.jar` are in `~/hzlib`. Write a small Java program using the EE client's `XAResource` (`start/end/prepare/commit(xid,false)`) against the `dev` cluster; capture its prepare+commit frames (a tiny logging TCP proxy on a spare port forwarding to 5701, or `sudo tcpdump -i lo -w cap.pcap tcp port 5701`); diff against the Rust client's frames (instrument `xa.rs` to hexdump the initial frame). The likely culprit is the **Xid group encoding** (`encode_xid` in `xa.rs`) — what `create`/`prepare` persist server-side and `commit(false)` re-reads — or a subtle prepared-state framing detail; confirm via the diff, not by guessing.
**Files:** `hazelcast-client/src/transaction/xa.rs` (`encode_xid`, `append_uuid`, `decode_uuid_response`, `prepare`/`commit`/`rollback`). **Verify:** `xa_transaction_test` 10/10, and add a data-effect assertion (committed XA writes visible via a separate client; a server-rejected commit returns `Err`).

## Work item A-3 — auth-failure surfacing (R8) + reader-loop (RR-20/RR-21)
**Status:** open (the primary invoke path already surfaces server exceptions via `check_response`; executor paths were fixed in `fa27761`).
**Symptom/fix:** `connection/manager.rs` (~455–505) does not check the authentication response **status byte** — a failed/rejected/timed-out auth is only logged, then the connection is registered (half-authenticated). Check the status (first response param at `RESPONSE_HEADER_SIZE`), return a typed `Err`, and do **not** register a failed connection. Also (RR-21) in `connection/invocation.rs` (~157–212) the reader loop should fail all in-flight invocations with a typed error on a decode/IO break, instead of silently dropping them.
**Verify the failure path** by making auth fail safely: stand up a small credential-required EE member (set `security:`/`client-authentication` or just use a wrong `cluster-name` to force auth rejection) and assert the client returns an error and registers no connection; confirm the success path (existing clusters) still works (2183/0 + ignored suite unchanged).

## Work item A-4 — close the 9 "infra-blocked" java_parity tests
**Status:** the client framing/decode for these is proven correct (failures are clean post-decode server errors, not codec crashes). Closing them needs **server-side config/classes** + (for entry processors) matching client serialization.
- **get_entry_view** (easiest): add `per-entry-stats-enabled: true` (+ `statistics-enabled: true`) to a wildcard map config in `~/hz/hz*.yaml`, restart the `dev` cluster (`~/start_cluster_ee.sh`), re-run — `hits()` should accumulate. Client decode already verified.
- **load_all / load_all_keys:** configure a `map-store` (a simple Java `MapLoader`/`MapStore` class) — deploy the class jar into `~/hzlib` (it's on the member classpath) and add `map-store:` to the map config; restart.
- **execute_on_key / execute_on_keys / execute_on_entries / submit_to_key_async:** need a registered server-side `EntryProcessor` class (+ `DataSerializableFactory`) deployed to `~/hzlib`, AND the Rust client must serialize the processor as a matching `IdentifiedDataSerializable` (factoryId+classId) — co-design. Update the `EntryProcessor` serialization in the client + the test's processor.
- **project / entry_set_with_predicate:** the tests project field `name` over `String` values; they need a typed value object exposing `name` (Portable or Compact with a `name` field) — client-side test rework + matching server class config.
Restart the `dev` cluster cleanly afterward and re-run the full ignored suite; update the disposition table in `REMEDIATION_RESULTS.md`.

---

## Definition of done for this handoff
All four items either fixed-and-verified-live or, where a specific sub-item proves to need something genuinely external, documented as such with evidence. Each fix: on a branch, built + clippy-clean + tested on the instance against EE 5.7, then committed and pushed to `main`, with `REMEDIATION_RESULTS.md` updated. Re-confirm non-ignored 2183/0 (no regressions) and report the new ignored-suite count. The overall CBDC **verdict remains NO-GO** until the external P3 items (pen test, Jepsen, soak, governance sign-off) are also satisfied — say so plainly; do not overstate.
