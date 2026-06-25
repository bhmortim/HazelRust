#!/usr/bin/env python3
"""Shared benchmark manifest generator — the single source of truth for the
HazelRust-vs-Java comparison (see docs/cbdc/BENCHMARK_METHODOLOGY.md).

Both harnesses (Rust `hazelrust-bench`, Java `bench-java`) consume the emitted
manifest and execute every cell *bit-for-bit identically*. Neither harness
hard-codes a workload. This generator emits one manifest per tier:

  T0 smoke  — core IMap get/put/set + AtomicLong inc, C in {1,64}, 100B, 1 trial.
  T1 core   — Suites A/B + mixed J, value {100,1024,16384}, C in {1,16,64,256}.
  T2 full   — the entire defined matrix x dimensions x forks x trials.

Run:  python3 bench/gen_manifest.py --tier t0 --out bench/manifest.t0.json
      python3 bench/gen_manifest.py --all

Data-generation contract (implemented identically in both harnesses; see
bench/README.md): keys and values are produced by pure deterministic formulas
seeded from the global seed so both clients touch the same key set with the same
bytes and the same access distribution.
"""
import argparse
import json
import sys

SCHEMA_VERSION = 3
GLOBAL_SEED = 0x5EED_C0DE_1234_ABCD  # shared across both clients

# ---------------------------------------------------------------------------
# Tier configuration. Each tier fixes the statistical-rigor knobs (forks,
# trials, warmup/measure windows, min ops) that the methodology mandates.
# ---------------------------------------------------------------------------
TIERS = {
    "t0": dict(forks=1, trials=1, warmup_s=5, measure_s=10, min_ops=50_000,
               open_loop=False, c_set=[1, 64], value_sizes=[100],
               distributions=["uniform"]),
    "t1": dict(forks=5, trials=3, warmup_s=20, measure_s=30, min_ops=1_000_000,
               open_loop=True, c_set=[1, 16, 64, 256],
               value_sizes=[100, 1024, 16384], distributions=["uniform", "zipfian"]),
    "t2": dict(forks=5, trials=5, warmup_s=30, measure_s=30, min_ops=1_000_000,
               open_loop=True, c_set=[1, 2, 4, 8, 16, 32, 64, 128, 256],
               value_sizes=[8, 100, 1024, 4096, 16384, 65536, 262144],
               distributions=["uniform", "zipfian"]),
}

# Open-loop target rates as a fraction of the measured closed-loop max
# (resolved by the orchestrator after the closed sweep).
OPEN_RATE_FRACS = [0.25, 0.50, 0.75, 0.90]

ZIPF_THETA = 0.99
WORKING_SET = 100_000   # distinct keys for the steady-state suites
WORKING_SET_SMALL = 1_000  # T0 / quick suites
# Bound the logical data of any one shared map so it (plus 1 backup) fits the
# 3x1g member heaps. ~400 MB logical -> ~800 MB with backups, well within 3 GB.
MAP_DATA_BUDGET = 400_000_000


def ws_for(value_size):
    """Working-set size that keeps a single shared map within the heap budget."""
    return max(WORKING_SET_SMALL, min(WORKING_SET, MAP_DATA_BUDGET // max(1, value_size)))


def cell(**kw):
    """Build one fully-specified cell with schema defaults."""
    base = dict(
        id=None, suite=None, structure=None, op=None, variant="default",
        key_type="i64", value_kind="bytes", value_size=100,
        working_set=WORKING_SET, concurrency=1, load_model="closed",
        rate_group=None, rate_frac=None, target_rate=None,
        batch_size=None, distribution="uniform", zipf_theta=ZIPF_THETA,
        warmup_s=30, measure_s=30, forks=5, trials=5, min_ops=1_000_000,
        mix=None,
    )
    base.update(kw)
    if base["id"] is None:
        base["id"] = make_id(base)
    if base["rate_group"] is None:
        base["rate_group"] = "{structure}.{op}.{variant}.k{key_type}.v{value_size}.{distribution}".format(**base)
    return base


def make_id(c):
    parts = [c["suite"], c["structure"], c["op"], c["variant"],
             "k" + str(c["key_type"]), "v" + str(c["value_size"]),
             "ws" + str(c["working_set"]), "C" + str(c["concurrency"]),
             c["distribution"], c["load_model"]]
    if c["load_model"] == "open" and c["rate_frac"] is not None:
        parts.append("r" + str(int(c["rate_frac"] * 100)))
    if c["batch_size"]:
        parts.append("b" + str(c["batch_size"]))
    return ".".join(str(p) for p in parts)


# Operation catalogue: (suite, structure, op, variant, key_type, value_kind).
# This is the dispatch contract — both harnesses must implement every op listed
# for the tier being run, or fail loud (never silently skip).
SUITE_A_IMAP = [
    ("A", "imap", "get", "hit", "i64", "bytes"),
    ("A", "imap", "get", "miss", "i64", "bytes"),
    ("A", "imap", "put", "update", "i64", "bytes"),
    ("A", "imap", "set", "default", "i64", "bytes"),
    ("A", "imap", "contains_key", "hit", "i64", "bytes"),
    ("A", "imap", "put_if_absent", "exists", "i64", "bytes"),
    ("A", "imap", "get_and_put", "update", "i64", "bytes"),
    ("A", "imap", "remove", "hit", "i64", "bytes"),
]
SUITE_A_BULK = [
    ("A", "imap", "get_all", "hit", "i64", "bytes"),
    ("A", "imap", "put_all", "default", "i64", "bytes"),
]
SUITE_B_CP = [
    ("B", "atomiclong", "get", "default", "i64", "i64"),
    ("B", "atomiclong", "set", "default", "i64", "i64"),
    ("B", "atomiclong", "increment_and_get", "default", "i64", "i64"),
    ("B", "atomiclong", "add_and_get", "default", "i64", "i64"),
    ("B", "atomiclong", "compare_and_set", "success", "i64", "i64"),
    ("B", "cpmap", "get", "hit", "i64", "i64"),
    ("B", "cpmap", "put", "update", "i64", "i64"),
    ("B", "cpmap", "set", "default", "i64", "i64"),
    ("B", "cpmap", "compare_and_set", "success", "i64", "i64"),
]
SUITE_C_COLL = [
    ("C", "iqueue", "offer_poll", "default", "i64", "bytes"),
    ("C", "ilist", "add_get", "default", "i64", "bytes"),
    ("C", "iset", "add_contains", "default", "i64", "bytes"),
    ("C", "multimap", "put_get", "default", "i64", "bytes"),
    ("C", "replicatedmap", "put", "update", "i64", "bytes"),
    ("C", "replicatedmap", "get", "hit", "i64", "bytes"),
]
# Suite J — realistic mixed read/update workloads (YCSB-style). The mix dict
# gives per-op probabilities; the harness draws an op per request from it.
SUITE_J_MIX = [
    ("J", "imap", "mixed", "ycsb_a", dict(get=0.50, put=0.50)),       # 50/50
    ("J", "imap", "mixed", "ycsb_b", dict(get=0.95, put=0.05)),       # 95/5
    ("J", "imap", "mixed", "ycsb_c", dict(get=1.0)),                  # read-only
    ("J", "imap", "mixed", "ycsb_f", dict(rmw=1.0)),                  # read-modify-write
]


def gen_tier(tier):
    cfg = TIERS[tier]
    cells = []
    tdefaults = dict(warmup_s=cfg["warmup_s"], measure_s=cfg["measure_s"],
                     forks=cfg["forks"], trials=cfg["trials"], min_ops=cfg["min_ops"])

    if tier == "t0":
        ws = WORKING_SET_SMALL
        ops = [("A", "imap", "get", "hit", "i64", "bytes"),
               ("A", "imap", "put", "update", "i64", "bytes"),
               ("A", "imap", "set", "default", "i64", "bytes"),
               ("B", "atomiclong", "increment_and_get", "default", "i64", "i64")]
        for (suite, struct, op, variant, kt, vk) in ops:
            for c in cfg["c_set"]:
                cells.append(cell(suite=suite, structure=struct, op=op, variant=variant,
                                  key_type=kt, value_kind=vk, value_size=100,
                                  working_set=ws, concurrency=c, load_model="closed",
                                  distribution="uniform", **tdefaults))
        return cells

    # T1 / T2: build the closed-loop matrix, then derive open-loop cells.
    catalogue = list(SUITE_A_IMAP) + list(SUITE_B_CP)
    if tier == "t2":
        catalogue += list(SUITE_C_COLL)

    for (suite, struct, op, variant, kt, vk) in catalogue:
        # CP / numeric structures use i64 values (fixed 8B); byte-value sweeps
        # only apply to value_kind == bytes.
        vsizes = cfg["value_sizes"] if vk == "bytes" else [8]
        # large value sizes only meaningful for IMap single-key put/get
        for vs in vsizes:
            if vk == "bytes" and vs >= 65536 and not (struct == "imap" and op in ("get", "put", "set")):
                continue
            for dist in cfg["distributions"]:
                # sequential/zipfian only sensible for keyed structures
                if struct in ("iqueue",) and dist == "zipfian":
                    continue
                if struct == "imap":
                    ws = ws_for(vs)
                elif struct in ("cpmap",):
                    ws = WORKING_SET_SMALL  # CP maps are coordination state, kept small
                elif struct in ("replicatedmap", "multimap"):
                    ws = min(WORKING_SET, ws_for(vs))
                else:
                    ws = WORKING_SET_SMALL
                for c in cfg["c_set"]:
                    cells.append(cell(suite=suite, structure=struct, op=op, variant=variant,
                                      key_type=kt, value_kind=vk, value_size=vs,
                                      working_set=ws, concurrency=c, load_model="closed",
                                      distribution=dist, **tdefaults))

    # Bulk ops (batch sizes) — IMap only
    if tier == "t2":
        for (suite, struct, op, variant, kt, vk) in SUITE_A_BULK:
            for vs in [100, 1024]:
                for bs in [10, 100, 1000]:
                    for c in [1, 16, 64]:
                        cells.append(cell(suite=suite, structure=struct, op=op, variant=variant,
                                          key_type=kt, value_kind=vk, value_size=vs,
                                          working_set=ws_for(vs), concurrency=c, load_model="closed",
                                          distribution="uniform", batch_size=bs, **tdefaults))

    # Suite J — mixed realistic workloads
    j_vsizes = [100, 1024] if tier == "t2" else [100]
    for (suite, struct, op, variant, mix) in SUITE_J_MIX:
        for vs in j_vsizes:
            for dist in cfg["distributions"]:
                for c in cfg["c_set"]:
                    cells.append(cell(suite=suite, structure=struct, op=op, variant=variant,
                                      key_type="i64", value_kind="bytes", value_size=vs,
                                      working_set=ws_for(vs), concurrency=c, load_model="closed",
                                      distribution=dist, mix=mix, **tdefaults))

    # Open-loop cells: one per closed cell's rate_group, at each rate fraction,
    # at a representative mid concurrency (the open generator does not depend on
    # C the same way; we fix C high enough to absorb the target rate). The
    # orchestrator resolves target_rate from the closed-loop max for the group.
    if cfg["open_loop"]:
        seen_groups = {}
        for c in list(cells):
            if c["load_model"] != "closed":
                continue
            g = c["rate_group"]
            if g in seen_groups:
                continue
            seen_groups[g] = c
        for g, base in seen_groups.items():
            for frac in OPEN_RATE_FRACS:
                oc = cell(suite=base["suite"], structure=base["structure"], op=base["op"],
                          variant=base["variant"], key_type=base["key_type"],
                          value_kind=base["value_kind"], value_size=base["value_size"],
                          working_set=base["working_set"], concurrency=256,
                          load_model="open", rate_group=g, rate_frac=frac,
                          distribution=base["distribution"], batch_size=base["batch_size"],
                          mix=base["mix"], **tdefaults)
                cells.append(oc)

    return cells


def build(tier):
    cells = gen_tier(tier)
    return dict(
        meta=dict(schema_version=SCHEMA_VERSION, tier=tier, seed=GLOBAL_SEED,
                  zipf_theta=ZIPF_THETA, open_rate_fracs=OPEN_RATE_FRACS,
                  cell_count=len(cells),
                  notes="Generated by bench/gen_manifest.py — do not hand-edit."),
        cells=cells,
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tier", choices=list(TIERS))
    ap.add_argument("--out")
    ap.add_argument("--all", action="store_true")
    args = ap.parse_args()
    if args.all:
        for t in TIERS:
            m = build(t)
            path = "bench/manifest.%s.json" % t
            with open(path, "w", newline="\n") as f:
                json.dump(m, f, indent=2)
                f.write("\n")
            print("wrote %s (%d cells)" % (path, m["meta"]["cell_count"]))
        return
    if not args.tier:
        ap.error("--tier or --all required")
    m = build(args.tier)
    out = args.out or ("bench/manifest.%s.json" % args.tier)
    with open(out, "w", newline="\n") as f:
        json.dump(m, f, indent=2)
        f.write("\n")
    print("wrote %s (%d cells)" % (out, m["meta"]["cell_count"]))


if __name__ == "__main__":
    main()
