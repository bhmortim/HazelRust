# Kickoff prompt — HazelRust vs. Hazelcast Java client comparative benchmark

Paste the block below into a new session to start the benchmarking work.

---

You are running a rigorous, head-to-head performance benchmark of the **HazelRust**
client (this repo, `github.com/bhmortim/HazelRust`, branch `cbdc/full-validation`,
which is also fast-forwarded to `main`) against the **official Hazelcast Java
client**, both executing identical operations on identical hardware against the
identical live Hazelcast Enterprise 5.7 cluster.

**Read first (do not skip):**
- `docs/cbdc/BENCHMARK_METHODOLOGY.md` in this repo — the full methodology. Follow
  it. It defines the fairness contract, the operation matrix, warmup/steady-state,
  the open-loop coordinated-omission-correct latency model, the metric set, the
  harness architecture, and the statistical reporting. Treat it as the spec.
- Your auto-memory `cbdc-readiness-verdict.md` and `hazelcast-ee-cluster-access.md`
  — how to reach the AWS instance (SSH key/CRLF workaround), build/test on the
  instance, and the live EE 5.7 clusters (`dev` 3-node :5701-3, plus `solo` :5710).
  **Never print, log, or commit the EE license key or any credential/SSH key** —
  they live in read-only configs on the instance (e.g. `~/hz/hz1.yaml`); avoid broad
  greps that would surface them.

**Mission.** Build the benchmark harnesses and produce a defensible comparison
report. Honesty contract: report variance and 95% CIs, never bare averages; flag any
result that is within noise instead of declaring a winner; document every fairness
caveat; publish the raw HdrHistograms so results are auditable; "measured" means
executed live on the controlled rig, not asserted.

**What to build (per the methodology):**
1. A **shared `manifest.json`** that fully specifies every benchmark cell (op,
   key/value kind+size, working-set, concurrency C, load model {closed | open@rate},
   warmup/measure spec, distribution, batch size). Both harnesses consume it; neither
   hard-codes a workload.
2. A **Rust harness** (`--release` workspace bin) on the `hazelcast-client` crate +
   `hdrhistogram` + `tokio`.
3. A **Java harness** (Gradle shaded JAR) on the latest Hazelcast Java client that is
   wire-compatible with the EE 5.7 cluster (pin the latest `5.7.x`; record the exact
   version). Identical manifest, args, concurrency model, and output schema as the
   Rust harness — `HdrHistogram` + GC log + JFR.
4. An **orchestrator** that drives cell × client × fork × trial with A/B/B/A
   interleaving, core pinning (members vs client on disjoint cores), the
   `performance` governor, resource samplers (`pidstat`/`/proc`), Java GC/JFR
   capture, and server-side metric scrape, plus the contamination guard.
5. An **analyzer** that merges histograms, computes per-cell Rust/Java ratios with
   bootstrap CIs, flags within-noise cells, and emits `BENCHMARK_REPORT.md` with
   overlaid latency CDFs, throughput-vs-C and latency-vs-throughput curves, resource
   efficiency, server-side impact, and a full provenance/caveats appendix.

**Hard fairness requirements (from the methodology — enforce them):**
- Identical work via the shared manifest; same cluster; same data (shared RNG seed);
  `byte[]`/primitive/`String` values only (no Compact/Portable) so per-op wire bytes
  match; smart routing ON, statistics OFF, near-cache OFF except the near-cache
  suite; matched outstanding-op count `C`.
- Warm up to steady state before every measurement (Java JIT especially); ≥5 forks
  (fresh processes) × ≥3 interleaved trials per cell; ≥1e6 ops / ≥1000 samples above
  p99.9 for tail-latency cells.
- Run **both** load models: closed-loop concurrency sweep for max throughput, and
  **open-loop** at {25,50,75,90}% of max with HdrHistogram expected-interval
  correction for the authoritative latency numbers.
- Record the full metric set: latency histograms (p50…p99.99/max), throughput +
  per-second stability, errors, client CPU/mem/threads/ctx-switches, Java GC/JIT,
  bytes-per-op wire efficiency, server-side member CPU/heap/GC/op-latency, cold-start
  and failover-recovery, and complete provenance on every record.

**Environment notes:** the AWS instance co-locates the cluster and the client — pin
them to disjoint core sets and record the co-location caveat; if a second machine is
available, run the client there and note it. The HazelRust client targets EE 5.7, so
keep the cluster at 5.7 and the Java client at the matching `5.7.x` for protocol
parity (a separate run with the absolute-latest Java client is allowed only as a
clearly-labeled secondary data point).

**Locked decisions (from the methodology — do not re-litigate):**
- **Java client = latest `5.7.x`** (matching the cluster) as the headline
  comparison; the absolute-latest 5.x only as a labeled secondary point.
- **Co-located rig** (cluster + client on the one AWS instance) with **disjoint core
  pinning** (members vs client on separate core sets); state the co-location caveat
  prominently in the report.
- **Scope = T0 smoke → T2 full**: validate on T0, then run the entire T2 matrix
  (no intermediate review gate). Keep T1 as a fast spot-check/repro target.

**Order of work:**
1. Confirm cluster health + pre-warm; set the `performance` governor + core pinning;
   record full provenance (hardware, MHz, cores, versions, commit).
2. Write the manifest, beginning with the **T0 smoke** tier (IMap get/put/set +
   AtomicLong increment at C∈{1,64}, 100 B, 1 trial).
3. Build both harnesses; get T0 producing valid, schema-matched records from both
   clients; build the analyzer; validate it end-to-end on T0.
4. **Go straight to T2 full** (overnight) with all samplers + GC/JFR + server-side
   scrape and the A/B/B/A interleave + contamination guard.
5. Analyze; write `BENCHMARK_REPORT.md`; archive raw results; spot-re-run the T1
   subset to confirm reproducibility before publishing conclusions.

Build and run the harnesses on the AWS instance (the only place with the Rust
toolchain, the Java toolchain, and the cluster). Commit per milestone to
`cbdc/full-validation` (and fast-forward `main`) with honest messages. When you
report numbers, lead with the realistic mixed workload (Suite J) and the CP
money-path suite (Suite B), and always pair every headline number with its CI and
its caveats.
