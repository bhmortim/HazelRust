# HazelRust vs. Hazelcast Java Client — Comparative Benchmarking Methodology

Status: methodology spec (v1). Companion kickoff brief: `BENCHMARK_KICKOFF_PROMPT.md`.

## 0. Goal & non-goals

**Goal.** Produce a defensible, reproducible, head-to-head performance comparison of
the **HazelRust** client and the **official Hazelcast Java client**, executing the
*exact same operations* on the *exact same hardware* against the *exact same
cluster*, with warmup, coordinated-omission-correct latency, and extremely detailed
per-client metrics.

**Non-goals.** Tuning either client to win; benchmarking the *cluster* (the cluster
is a fixed control); micro-benchmarking serialization in isolation (covered only as
a wire-efficiency side metric). We measure **end-to-end client operation
performance** as an application would observe it.

**The honesty contract (carried from this project).** Report variance and
confidence intervals, never bare averages; flag any cell where the Rust/Java
difference is within noise; document every fairness caveat; report operations that
are not comparable rather than forcing them; publish raw histograms so results are
auditable.

---

## 1. Fairness principles (the contract both harnesses must honor)

1. **Identical work.** Every measured unit ("cell") is defined once in a shared
   manifest and executed bit-for-bit identically by both clients: same op, same key
   set (shared RNG seed), same value bytes, same batch sizes, same concurrency,
   same load model, same warmup/measure durations, same trial count.
2. **Same cluster, same data.** One fixed cluster. Data is seeded deterministically
   and reset identically before each cell. Only one client drives load at a time.
3. **Same wire format.** Both clients store Hazelcast `Data` with identical
   `[partition_hash][type_id][payload]` framing. Use primitive/`byte[]`/`String`
   values only (no Compact/Portable) so per-op wire bytes are ~identical — any
   throughput delta is the client, not the payload.
4. **Same client configuration.** Smart routing ON for both; statistics OFF;
   near-cache OFF (except the explicit near-cache suite, configured identically);
   identical connection counts / IO concurrency where configurable; identical
   invocation timeouts; backup-ack-to-client matched.
5. **Idiomatic-but-matched concurrency.** Each client uses its idiomatic
   high-performance path (Java blocking-threads or async `CompletionStage`; Rust
   async tokio tasks), but the **number of outstanding in-flight operations `C`** is
   matched per cell. Report both a closed-loop max-throughput sweep and an
   open-loop latency-under-rate sweep (§6).
6. **Steady state only.** Measurement begins only after warmup has reached steady
   state (§7); the cluster is pre-warmed before the first measured client.
7. **Interleave to cancel drift.** Cluster behavior drifts (member GC, compaction,
   migrations). Cells are run A/B back-to-back and trials are interleaved so neither
   client systematically samples a "good" or "bad" cluster phase (§8).
8. **Provenance.** Every result record carries full environment provenance (§9.7).

---

## 2. Environment & provenance

- **Hardware.** A single, fixed host. *Preferred:* client on a **separate machine**
  from the cluster over a low-latency LAN, so client CPU never contends with member
  CPU. *Acceptable fallback (this project's AWS instance):* co-located, with strict
  **core pinning** — pin the 3 members to one core set and the client process to a
  disjoint core set (`taskset`/`cpuset`), leaving 1–2 cores for the OS. The
  co-location caveat **must** be recorded in the report.
- **CPU stability.** Set the governor to `performance`
  (`cpupower frequency-set -g performance`), disable turbo if it cannot be pinned,
  and record the achieved frequency. Run `nice -n -5` (or a dedicated `chrt` policy)
  and document it identically for both clients.
- **Cluster.** A **dedicated, quiesced** Hazelcast EE 5.7 cluster (no other test
  load). 3 members is the realistic default; also capture a **single-member** run to
  isolate pure client overhead from partition spread. Same member heap/flags for all
  runs. (This project: the `dev` 3-node Docker cluster when idle, or a fresh
  bench cluster.)
- **Java client.** The **latest released Hazelcast Java client that is wire-compatible
  with the EE 5.7 cluster** — pin to the latest `5.7.x` patch for protocol parity and
  record the exact version; optionally also run the absolute-latest 5.x as a
  secondary data point (note the cluster-version mismatch caveat). JVM: a single
  pinned LTS (e.g., Temurin 21), flags recorded verbatim, G1 default unless a GC
  sweep is requested.
- **Rust client.** HazelRust at a recorded git commit, built `--release`; record
  `rustc` version and the exact cargo profile (note if LTO / `codegen-units=1`;
  optionally a tuned profile as a second data point).
- **NEVER** print, log, or commit the EE license key or any credential/SSH key
  (see project memory). Configs that embed the license are read-only inputs.

---

## 3. Cluster & data setup

- **Maps/structures pre-created** with identical configs (backup-count, in-memory
  format BINARY, no eviction during the run, no TTL unless the TTL cell). Seed once
  per cell from a shared seed so both clients read/write the same keys.
- **Working-set sizes:** small (1k keys, fits everywhere), medium (100k), large (1M)
  — to exercise cold reads, partition spread, and (where enabled) near-cache hit/miss.
- **Reset discipline.** Read-only cells share a pre-seeded immutable dataset.
  Mutating cells either (a) operate on a per-(client,trial) namespace so they never
  collide, or (b) reset the structure between A and B. Prefer (a) for throughput,
  (b) where absolute key state matters.
- **Cluster pre-warm.** Before the first measured client, run ~3–5 min of mixed
  load (puts/gets across all partitions) so the **members** (also JVMs) JIT-warm,
  the partition table is stable, and heaps reach steady state.

---

## 4. The operation matrix (the "vast array")

Each row is run across the workload dimensions in §5. `EP` = needs a server-side
entry-processor/task class deployed (the project already deploys `a4classes.jar`).

### Suite A — IMap (primary workhorse)
- **Single-key reads:** `get` (hit), `get` (miss), `containsKey` (hit/miss),
  `getEntryView`.
- **Single-key writes:** `put` (insert), `put` (update), `set`, `putWithTtl`,
  `getAndPut`, `remove` (exists), `delete`, `getAndRemove`.
- **Conditional/atomic:** `putIfAbsent` (absent/exists), `replace`,
  `replaceIfSame` (success/fail).
- **Bulk:** `getAll`, `putAll`, `setAll` at batch sizes {10, 100, 1000}.
- **Whole-collection:** `keySet`, `values`, `entrySet`, `size`, `isEmpty` over maps
  of {1k, 10k, 100k} entries.
- **Queries:** `values`/`keySet`/`entrySet` with predicates {equal, between (range),
  like, in, and/or composite, SQL-string}; **paging predicate** (values/keys/entries)
  at page sizes {10, 100}.
- **Aggregations:** count, longSum, doubleAvg, min, max, distinct.
- **Projections:** single-attribute, multi-attribute.
- **Entry processors (EP):** `executeOnKey` (no-op + increment), `executeOnKeys`
  (batch 100), `executeOnEntries` (all + with-predicate).
- **Locks:** `lock`/`unlock` (uncontended), `tryLock` (available/held).
- **Listeners (event path):** entry-added listener — measure events/sec delivered
  and put→callback end-to-end latency.

### Suite B — CP subsystem (money path)
- `AtomicLong`: get, set, incrementAndGet, addAndGet, getAndAdd, compareAndSet
  (succeed/fail).
- `AtomicReference`: get, set, compareAndSet.
- `CPMap`: get, put, set, compareAndSet, remove.
- `FencedLock`: lock+unlock (uncontended), tryLock.
- `Semaphore`: acquire+release (1 permit), tryAcquire.
- `CountDownLatch`: countDown, getCount.

### Suite C — Collections
- `IQueue`: offer, poll, peek, size, drainTo(100).
- `IList`: add, get, set, remove, contains, size, addAll(100).
- `ISet`: add, contains, remove, size.
- `MultiMap`: put, get, remove, valueCount, size.
- `ReplicatedMap`: put, get, containsKey, values, size.
- `Ringbuffer`: add, readOne, readMany(100), addAll(100), tailSequence.

### Suite D — Messaging
- `ITopic`: publish throughput; publish→receive **end-to-end** latency
  (1 publisher → K∈{1,8} subscribers).
- `ReliableTopic`: publish throughput; end-to-end latency.

### Suite E — Transactions
- Non-XA: 1-op txn (begin+put+commit), 5-op txn, rollback.
- XA: begin+put+prepare+commit, recover().

### Suite F — ICache (JCache; now functional in HazelRust)
- get (hit/miss), put, getAndPut, putIfAbsent, replace, remove.

### Suite G — Counters / IDs
- `PNCounter`: addAndGet, get.
- `FlakeIdGenerator`: newId (batched, client-side), newId (forced server fetch).

### Suite H — Executors (optional; EP)
- `IExecutorService`: submitToKeyOwner (no-op), submit (callable).
- `DurableExecutor`, `ScheduledExecutor`: submit.

### Suite I — Lifecycle
- Cold start: process start → connected → first op complete.
- Failover recovery: time to resume successful ops after a member bounce
  (`docker stop`/`start`), measured as the gap in a steady op stream.

### Suite J — Realistic mixed workloads (YCSB-style)
- **A** 50/50 read/update, **B** 95/5 read/update, **C** read-only,
  **D** read-latest (insert + read recent), **F** read-modify-write.
- Key distribution: **uniform** and **Zipfian (θ=0.99)** (hot keys), over the
  100k and 1M working sets.

---

## 5. Workload dimensions (applied across the matrix)

- **Value sizes** (`byte[]` for clean wire parity): 8 B, 100 B, 1 KB, 4 KB, 16 KB,
  64 KB, 256 KB. Plus typed values: `i64` (8 B numeric) and `String` (16, 64 chars)
  for the typed suites.
- **Key types:** `i64` and `String(16/64)`.
- **Concurrency `C`** (outstanding in-flight ops): {1, 2, 4, 8, 16, 32, 64, 128, 256}.
- **Read/write mix** (for the mixed suite): 100/0, 95/5, 50/50, 5/95, 0/100.
- **Key distribution:** uniform, Zipfian (θ=0.99), sequential.

Not every cross-product is run (combinatorial blowup). Define **tiers**:
- **T0 smoke** (CI-able, ~minutes): core IMap get/put/set, AtomicLong inc, at C∈{1,64},
  value 100 B, 1 trial — sanity + harness validation.
- **T1 core** (~1–2 h): Suites A,B,F + mixed J, value sizes {100 B,1 KB,16 KB},
  C∈{1,16,64,256}, 5 forks × 3 trials.
- **T2 full** (overnight): the entire matrix × all dimensions × 5 forks × 5 trials.

---

## 6. Load-generation models

Run **both** models; they answer different questions.

- **Closed-loop (max throughput / saturation).** `C` workers each issue an op, await
  the response, immediately issue the next. Sweep `C` upward until throughput
  plateaus (saturation). Reports **max throughput** and **service time**. *Warning:*
  under saturation this **under-reports tail latency** (coordinated omission); use it
  for throughput, not for latency claims.
- **Open-loop (latency under controlled rate).** A scheduler issues requests on a
  fixed-rate clock (Poisson or uniform arrivals) **regardless of completion**, at
  target rates of {25, 50, 75, 90}% of the measured closed-loop max. Record
  **response time = completion − scheduled-start** and feed HdrHistogram with the
  expected interval so it **synthesizes the missing samples that coordinated
  omission would otherwise hide**. This is the authoritative latency measurement.

A run "fails" (and is flagged) if the open-loop generator cannot keep up with its
target rate (back-pressure) — that target rate is above the client's capacity and
its latency numbers are reported as "saturated".

---

## 7. Warmup & steady-state protocol (critical for Java)

- **Connection warmup** (both): establish all member connections, fetch the
  partition table, (near-cache suites) pre-populate the near-cache, and run a brief
  burst so routing caches are hot — *before* the warmup window.
- **JIT/steady-state warmup.** Per cell, run a **warmup window** at the cell's load
  until steady state. Default warmup = **max(30 s, until the 5-second-windowed p50
  and throughput change < 2% across 3 consecutive windows)**. For Java additionally
  confirm JIT has quiesced (compilation count flat via `-Xlog:jit+compilation` or
  JFR). Rust warmup uses the same window protocol for symmetry (allocator, branch
  predictor, connection pool, and the *members* all warm).
- **Forks.** Because JIT inlining/profile varies across JVM runs, each cell is
  executed in **≥5 fresh processes ("forks")** per client (each fork = warmup +
  measure). Rust is also re-forked the same number of times for symmetry (captures
  allocator/OS variance). Cross-fork variance is part of the reported result.

---

## 8. Measurement protocol

For each cell × client:
1. Seed/reset data (deterministic).
2. **Fork loop** (≥5): fresh process → connection warmup → steady-state warmup (§7)
   → **measurement** of `N` ops or `M` seconds (whichever yields ≥1e6 ops for
   tail-latency cells; ≥1e3 samples must land above p99.9) → emit a result record +
   the full HdrHistogram.
3. **Trials & interleave.** Repeat the whole thing for ≥3 trials. Order is
   interleaved at the cell level: `…, A_cellX, B_cellX, B_cellY, A_cellY, …`
   (A/B/B/A) so cluster drift is shared. Randomize cell order across trials.
4. **Contamination guard.** Monitor member GC/migration during each window; if a
   long member GC pause or a partition migration overlaps a measurement window, mark
   that fork "contaminated" and re-run it.

Aggregate across forks×trials with the **median** and a **95% bootstrap CI**; never
average histograms — merge them (HdrHistogram add) for pooled percentiles, but keep
per-fork percentiles for the variance estimate.

---

## 9. Metrics (record *everything*, per cell × client × fork)

### 9.1 Latency (the headline)
Full **HdrHistogram** (1 µs–60 s range, 3 significant digits), exported as a
compressed `.hgrm`. Derived: min, p25, p50, p75, p90, p95, p99, **p99.9, p99.99**,
max, mean, stddev. Open-loop histograms are CO-corrected.

### 9.2 Throughput
Overall ops/sec; **per-1-second** throughput series → mean, stddev, coefficient of
variation (stability), min/max window.

### 9.3 Errors
Count, rate, and a type breakdown (timeouts, server exceptions, connection errors).
Any nonzero error rate invalidates the latency/throughput of that fork unless
expected by the cell.

### 9.4 Client resource usage (sampled @1 s via `pidstat`/`/proc`)
- CPU: %user, %sys, total, per-core; **CPU-seconds per million ops** (efficiency).
- Memory: RSS peak & mean; Java also heap used/committed, non-heap, metaspace.
- Threads: count over time. Context switches: voluntary/involuntary (`pidstat -w`).
- File descriptors / sockets open.

### 9.5 Java-specific
GC pause count, total pause ms, **max pause ms**, by collector phase
(young/mixed/full); allocation rate (MB/s); promotion rate; JIT compilation time;
deoptimization count. Source: `-Xlog:gc*,safepoint` + **JFR** recording.

### 9.6 Rust-specific
Peak RSS; allocation count/bytes from a **separate instrumented run** with a
counting global allocator (it perturbs perf, so never use it for the timed run);
tokio runtime metrics (task count, queue depth) if exposed.

### 9.7 Network / wire efficiency
Bytes sent/received **per op** (instrument the harness socket counters, or
`pidstat -d` / `nstat` deltas); TCP retransmits & RTT (`ss -ti`). This exposes
protocol/framing overhead differences. Expect near-parity (same Data framing); a
delta is a real client-framing finding.

### 9.8 Server-side (shared cluster, captured during each client's run)
Per member: CPU, heap used, GC pauses, **operation latency** & pending-invocation
count (Hazelcast **Diagnostics** plugin + Prometheus/JMX/Management-Center), slow
operations, partition migrations. Shows whether one client stresses the cluster more
for the same logical work.

### 9.9 Lifecycle
Cold-start: start→connected→first-op (ms). Failover: recovery gap (ms) + ops lost.

### 9.10 Provenance (every record)
HazelRust git commit; Java client version; cluster version + topology; JVM version
+ flags; rustc version + cargo profile; OS/kernel; CPU model + pinned cores +
governor + achieved MHz; co-located vs separated; wall-clock timestamp; manifest
cell id; fork/trial index; load model + target rate.

---

## 10. Harness architecture

Three components plus a shared manifest.

- **Shared manifest** (`bench/manifest.json`): the single source of truth — an array
  of fully-specified cells (id, suite, op, key_type, value_kind+size, working_set,
  concurrency C, load_model {closed | open@rate}, warmup_spec, measure_spec,
  distribution, batch_size, …). Both harnesses consume it; neither hard-codes a
  workload.
- **Rust harness** (`hazelrust-bench`, a `--release` bin in the workspace): uses the
  `hazelcast-client` crate + `hdrhistogram` + `tokio` + `serde`/`clap`. Args:
  `--manifest --cell <id> --client rust --fork <n> --trial <n> --out <dir>
  --cluster <addrs> --seed <s>`. Concurrency via a `C`-permit semaphore (closed) or a
  rate-clock scheduler (open). Per-task `Histogram`, merged at the end.
  `Instant::now()` timing.
- **Java harness** (`bench-java/`, Gradle, shaded JAR): official `hazelcast` client
  (pinned version) + `HdrHistogram` + Jackson + picocli. **Identical** manifest,
  args, concurrency model, and output schema. `System.nanoTime()` timing. GC log +
  JFR enabled via flags the orchestrator injects.
- **Orchestrator** (`bench/run.py` or `run.sh`): drives the cell × client × fork ×
  trial loop with the §8 interleaving; pins cores; sets the governor; starts/stops
  the resource samplers and (Java) GC/JFR capture and the server-side metric scrape;
  seeds/resets data; collects result records; enforces the contamination guard.
- **Analyzer** (`bench/analyze.py`): loads all records + `.hgrm`s, aligns cells,
  merges histograms, computes per-cell Rust/Java **ratios** (throughput, p50, p99,
  p99.9, CPU-per-Mops, bytes-per-op) with bootstrap CIs, flags within-noise cells,
  and emits: a markdown **report**, CSVs, and plots (overlaid latency CDFs;
  throughput-vs-C curves; latency-vs-throughput "hockey-stick" curves;
  resource-usage bars). Executive summary: where each client wins, by how much, with
  confidence, and the caveats.

**Output schema** (one JSON per cell×client×fork×trial) — example fields:
`{client, version, commit, cell_id, suite, op, value_size, C, load_model,
target_rate, fork, trial, ops, duration_s, throughput, errors{...},
latency{p50,p90,p99,p999,p9999,max,mean,stddev,hgrm_b64}, cpu{...}, mem{...},
gc{...}, net{bytes_per_op,...}, server{...}, provenance{...}}`.

---

## 11. Statistical analysis & reporting

- Report **median across forks×trials** + **95% bootstrap CI**; show distribution,
  not a point.
- Per metric, compute the **ratio** Rust/Java and its CI. A cell is "**within
  noise**" if the ratio CI spans 1.0 — say so explicitly; do **not** claim a winner.
- For "is X faster than Y" use a **bootstrap** over fork-level results (or
  Mann-Whitney U); report effect size, not just p-values.
- Tail latency requires ≥1000 samples above the reported percentile — verify and
  state the sample count behind each p99.9/p99.99.
- The report leads with the **mixed realistic suite (J)** and the **money-path CP
  suite (B)**, then the per-op micro-results, then resource efficiency.

---

## 12. Threats to validity & required controls

| Threat | Control |
|---|---|
| Java JIT cold | Per-cell warmup to steady state + ≥5 forks (§7). |
| Coordinated omission | Open-loop + HdrHistogram expected-interval (§6). |
| Cluster drift (GC/migration) | A/B/B/A interleave, contamination guard, pre-warm (§3,§8). |
| Co-location CPU contention | Pin members vs client to disjoint cores; prefer separate hosts (§2). |
| Frequency scaling / turbo | `performance` governor, record MHz (§2). |
| Unequal payloads | `byte[]`/primitive values, same bytes, same Data framing (§1.3). |
| Unequal concurrency model | Match outstanding-op count `C`; report both closed & open (§1.5,§6). |
| Unequal client config | Smart routing on, stats off, near-cache off, timeouts matched (§1.4). |
| Cross-run noise | ≥3 trials, randomized cell order, median+CI (§8,§11). |
| Version mismatch | Java client pinned to cluster minor (5.7.x); record exactly (§2). |
| Feature gaps | Mark non-comparable cells explicitly; never substitute (§0). |
| Measurement overhead | Histogram record is O(1) lock-free per-thread; verify the timing harness adds < ~1% (calibrate with a no-op cell). |

---

## 13. Deliverables

1. `manifest.json` (the workload spec) + the two harnesses + orchestrator + analyzer.
2. Raw per-cell result JSON + `.hgrm` histograms (auditable archive).
3. `BENCHMARK_REPORT.md`: executive summary, per-suite tables (Rust vs Java with
   CIs and ratios), latency CDFs, throughput/latency curves, resource efficiency,
   server-side impact, and a **full provenance + caveats** appendix.
4. A reproducibility script that re-runs a named tier end-to-end.

---

## 14. Execution runbook (happy path)

1. Provision/confirm hardware, governor, core pinning; record provenance.
2. Stand up the dedicated EE 5.7 cluster (3-node + a single-node variant); confirm
   health; pre-warm.
3. Build the Rust harness (`--release`) and the Java harness (pinned client);
   smoke-test both connect + run the **T0** tier and produce valid records.
4. Validate the analyzer on T0 output (schema, histogram merge, plots).
5. Run **T1 core**; sanity-check ratios and variance; fix any harness asymmetry.
6. Run **T2 full** (overnight), with samplers + server-side scrape + GC/JFR.
7. Analyze; write `BENCHMARK_REPORT.md`; archive raw results.
8. Re-run a spot subset to confirm reproducibility before publishing conclusions.
