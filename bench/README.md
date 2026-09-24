# Hazelcast Rust client vs. Hazelcast Java client — comparative benchmark harness

A reproducible, head-to-head benchmark of the **Hazelcast Rust** client and the
**official Hazelcast Enterprise 5.7.0 Java client** running *identical* operations
on *identical* hardware against the *identical* live EE 5.7 cluster. Implements
[`docs/cbdc/BENCHMARK_METHODOLOGY.md`](../docs/cbdc/BENCHMARK_METHODOLOGY.md).

## Components

| File | Role |
|---|---|
| `gen_manifest.py` | Emits the shared manifest (`manifest.<tier>.json`) — the single source of truth. Every cell fully specifies op, key/value kind+size, working-set, concurrency `C`, load model, warmup/measure, distribution, batch. Both harnesses consume it; neither hard-codes a workload. |
| `../hazelcast-client-bench/` | Rust harness (`--release` workspace bin): `hazelcast-client` + `hdrhistogram` + `tokio`. |
| `../bench-java/` | Java harness (Gradle shaded JAR): Hazelcast **Enterprise** 5.7.0 client + `HdrHistogram` + Jackson + picocli. |
| `run.py` | Orchestrator: core pinning, interleave, forks×trials, samplers, GC/JFR, server scrape, open-loop rate resolution, contamination guard. |
| `analyze.py` | Analyzer: merges histograms, median + 95% bootstrap CIs, Rust/Java ratios, within-noise flags, `BENCHMARK_REPORT.md` + SVG plots. |
| `hdr.py` | Pure-stdlib HdrHistogram V2(+zlib) decoder/merger (cross-language: Rust + Java). |

## Why the Enterprise Java client

The OSS `com.hazelcast:hazelcast` client throws `UnsupportedOperationException:
"CP subsystem is an Enterprise feature"` for any CP structure (AtomicLong, CPMap,
…) against an EE cluster. Suite B (the CP money path) is a headline suite, so the
**EE 5.7.0 client** — the correct official client for an EE cluster — is used for
all suites. It is version-identical to the cluster (exact protocol parity). The EE
client requires a license (also gates CP); the orchestrator reads it from the
read-only member config at runtime and passes it only via the Java subprocess
`HZ_LICENSE` env. **The license is never printed, logged, or committed.**

## Data-generation contract (bit-identical across clients)

Both harnesses (`hazelcast-client-bench/src/data.rs`, `bench-java/.../Data.java`) implement
the *same* pure formulas so the only variable is the client:

- **PRNG:** SplitMix64, per-worker seed = `global_seed ^ (worker·φ) ^ fnv1a(cell_id)`.
- **Keys:** i64 key = working-set index; string keys are `"key"` + zero-padded.
- **Values:** `byte[]` of size `N`: `byte[j] = (vidx·1000003 + j·31 + seed) mod 256`.
  Numeric suites use `long`. (No Compact/Portable — identical Data framing →
  identical per-op wire bytes.)
- **Distribution:** `uniform` (`u64 mod n`), `sequential`, `zipfian` (YCSB θ=0.99;
  identical `zetan` from the same summation order).
- **Data discipline:** read/update cells share one pre-seeded immutable map per
  *shape* (structure, value_size, working_set); working-set sizes are scaled so a
  map (+1 backup) fits the 3×1g member heaps.

## Fairness controls (enforced)

Smart routing ON, client statistics OFF, near-cache OFF (both); matched
outstanding-op count `C` (Rust = `C` tokio tasks / Java = `C` platform threads in
closed loop; `C`-permit cap in open loop); identical manifest/seed; one client
drives load at a time; A/B/B/A interleave + randomized cell order; disjoint core
pinning (members `0-3,8-11`, client `4-6,12-14`, OS `7,15`).

## Load models

- **Closed-loop** — `C` workers issue→await→reissue. Max throughput + service time
  (under-reports tail under saturation: coordinated omission).
- **Open-loop** — a dedicated scheduler thread paces a fixed arrival rate with µs
  accuracy (sleep the bulk, spin the last ~1 ms) and dispatches up to `C`
  outstanding. Latency = `completion − scheduled-start` (CO-correct). Rates are
  {25,50,75,90}% of the measured closed-loop max; `saturated` is flagged if the
  generator cannot keep up.

## Tiers

- `t0` — smoke (8 cells, C∈{1,64}, 100 B, 1 trial): harness/analyzer validation.
- `headline` — curated executed matrix (44 cells: Suites J+B+A, C-sweep, value
  sweep, zipfian, open-loop), forks=3 × trials=2.
- `t1` / `t2` — the broader / full design matrices (T2 is the overnight ceiling;
  hundreds of cells × 5 forks × 5 trials).

## Run it

```bash
# 1. generate manifests
python3 bench/gen_manifest.py --all
python3 bench/gen_manifest.py --tier headline --out bench/manifest.headline.json

# 2. build both harnesses (on the instance)
cargo build --release -p hazelcast-client-bench
(cd bench-java && ./gradlew shadowJar)

# 3. orchestrate (closed then open), with pinning + GC/JFR + samplers
python3 bench/run.py --manifest bench/manifest.headline.json --out benchmarks/headline \
    --clients rust,java --commit "$(git rev-parse --short HEAD)" \
    --warmup-s 15 --measure-s 15 --pin --jfr --quiesce --open

# 4. analyze -> report + plots
python3 bench/analyze.py --in benchmarks/headline
```

See `reproduce.sh` for a one-shot named-tier run. Raw per-run records + embedded
HdrHistograms are preserved in the run directory (auditable).
