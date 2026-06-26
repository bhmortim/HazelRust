#!/usr/bin/env python3
"""Generate publication-quality benchmark charts (PNG) from chartdata.json.

Run locally (matplotlib). Usage:
  python bench/mkcharts.py .benchwork/chartdata.json docs/cbdc/bench_charts
"""
import json
import os
import sys
import math

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

RUST = "#d62728"      # red
JAVA = "#1f77b4"      # blue
RUST_OFF = "#7f7f7f"  # gray
plt.rcParams.update({"font.size": 11, "axes.grid": True, "grid.alpha": 0.3,
                     "figure.dpi": 150, "savefig.dpi": 150, "axes.axisbelow": True})


def kfmt(v, _=None):
    if v >= 1e6:
        return "%.1fM" % (v / 1e6)
    if v >= 1e3:
        return "%.0fk" % (v / 1e3)
    return "%.0f" % v


def cell(run, cid):
    return run.get(cid)


def get(run, cid, client, field):
    e = run.get(cid)
    if not e:
        return None
    c = e["clients"].get(client)
    return c.get(field) if c else None


def save(fig, outdir, name):
    path = os.path.join(outdir, name)
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)
    print("  wrote", path)
    return path


def chart_write_optimization(headline, postopt, outdir):
    """Centerpiece: IMap put throughput, Rust-OFF (pre) vs Rust-ON (post) vs Java."""
    Cs = [1, 64, 256]
    def cid(c): return "A.imap.put.update.ki64.v100.ws100000.C%d.uniform.closed" % c
    rust_off = [get(headline, cid(c), "rust", "thr") for c in Cs]
    rust_on = [get(postopt, cid(c), "rust", "thr") for c in Cs]
    java = [get(postopt, cid(c), "java", "thr") or get(headline, cid(c), "java", "thr") for c in Cs]
    x = range(len(Cs)); w = 0.27
    fig, ax = plt.subplots(figsize=(8, 4.5))
    ax.bar([i - w for i in x], rust_off, w, label="HazelRust (before / backup-ack OFF)", color=RUST_OFF)
    ax.bar([i for i in x], rust_on, w, label="HazelRust (after / backup-ack ON)", color=RUST)
    ax.bar([i + w for i in x], java, w, label="Hazelcast Java (backup-ack ON)", color=JAVA)
    for i in x:
        for off, v, col in [(-w, rust_off[i], RUST_OFF), (0, rust_on[i], RUST), (w, java[i], JAVA)]:
            if v:
                ax.text(i + off, v, kfmt(v), ha="center", va="bottom", fontsize=8)
    ax.set_xticks(list(x)); ax.set_xticklabels(["C=%d" % c for c in Cs])
    ax.set_ylabel("throughput (ops/s)"); ax.yaxis.set_major_formatter(FuncFormatter(kfmt))
    ax.set_title("IMap put throughput — backup-ack-to-client optimization\n(higher is better; 0 errors)")
    ax.legend(fontsize=9)
    return save(fig, outdir, "01_write_optimization.png")


def chart_thr_vs_c(headline, postopt, outdir):
    fig, axes = plt.subplots(1, 2, figsize=(11, 4.3))
    # put (with before/after)
    Cs = [1, 16, 64, 256]
    def pcid(c): return "A.imap.put.update.ki64.v100.ws100000.C%d.uniform.closed" % c
    ax = axes[0]
    roff = [(c, get(headline, pcid(c), "rust", "thr")) for c in Cs]
    ron = [(c, get(postopt, pcid(c), "rust", "thr")) for c in [1, 64, 256]]
    jav = [(c, get(postopt, pcid(c), "java", "thr") or get(headline, pcid(c), "java", "thr")) for c in [1, 64, 256]]
    _line(ax, roff, RUST_OFF, "Rust OFF (before)", "o--")
    _line(ax, ron, RUST, "Rust ON (after)", "o-")
    _line(ax, jav, JAVA, "Java", "s-")
    ax.set_title("IMap put: throughput vs concurrency"); ax.set_xlabel("concurrency C (log2)")
    ax.set_ylabel("throughput (ops/s)"); ax.set_xscale("log", base=2)
    ax.yaxis.set_major_formatter(FuncFormatter(kfmt)); ax.legend(fontsize=8)
    # get (read; unaffected)
    def gcid(c): return "A.imap.get.hit.ki64.v100.ws100000.C%d.uniform.closed" % c
    ax = axes[1]
    rg = [(c, get(headline, gcid(c), "rust", "thr")) for c in Cs]
    jg = [(c, get(headline, gcid(c), "java", "thr")) for c in Cs]
    _line(ax, rg, RUST, "Rust", "o-")
    _line(ax, jg, JAVA, "Java", "s-")
    ax.set_title("IMap get: throughput vs concurrency"); ax.set_xlabel("concurrency C (log2)")
    ax.set_ylabel("throughput (ops/s)"); ax.set_xscale("log", base=2)
    ax.yaxis.set_major_formatter(FuncFormatter(kfmt)); ax.legend(fontsize=8)
    fig.suptitle("Throughput vs concurrency (closed-loop)")
    return save(fig, outdir, "02_throughput_vs_c.png")


def _line(ax, pairs, color, label, style):
    pairs = [(c, v) for c, v in pairs if v]
    if pairs:
        ax.plot([c for c, _ in pairs], [v for _, v in pairs], style, color=color, label=label, lw=2, ms=6)


def chart_resources(headline, outdir):
    # representative cells across suites
    cells = [
        ("A.imap.get.hit.ki64.v100.ws100000.C64.uniform.closed", "IMap get C64"),
        ("A.imap.put.update.ki64.v100.ws100000.C64.uniform.closed", "IMap put C64"),
        ("J.imap.mixed.ycsb_a.ki64.v100.ws100000.C64.uniform.closed", "Mixed 50/50 C64"),
        ("B.atomiclong.increment_and_get.default.ki64.v8.ws100000.C64.uniform.closed", "AtomicLong inc C64"),
        ("A.imap.get.hit.ki64.v100.ws100000.C1.uniform.closed", "IMap get C1"),
        ("A.imap.put.update.ki64.v100.ws100000.C1.uniform.closed", "IMap put C1"),
    ]
    fig, axes = plt.subplots(1, 2, figsize=(11, 4.5))
    labels = [lbl for _, lbl in cells]
    y = range(len(cells)); h = 0.38
    # RSS
    ax = axes[0]
    rr = [get(headline, c, "rust", "rss_mb") or 0 for c, _ in cells]
    jr = [get(headline, c, "java", "rss_mb") or 0 for c, _ in cells]
    ax.barh([i + h/2 for i in y], rr, h, label="Rust", color=RUST)
    ax.barh([i - h/2 for i in y], jr, h, label="Java", color=JAVA)
    ax.set_yticks(list(y)); ax.set_yticklabels(labels, fontsize=8)
    ax.set_xscale("log"); ax.set_xlabel("peak RSS (MB, log scale)")
    ax.set_title("Client memory footprint"); ax.legend(fontsize=9)
    for i in y:
        if rr[i] and jr[i]:
            ax.text(jr[i], i - h/2, " %.0f× " % (jr[i] / rr[i]), va="center", fontsize=8, color=JAVA)
    # CPU per Mops
    ax = axes[1]
    rc = [get(headline, c, "rust", "cpu_mops") or 0 for c, _ in cells]
    jc = [get(headline, c, "java", "cpu_mops") or 0 for c, _ in cells]
    ax.barh([i + h/2 for i in y], rc, h, label="Rust", color=RUST)
    ax.barh([i - h/2 for i in y], jc, h, label="Java", color=JAVA)
    ax.set_yticks(list(y)); ax.set_yticklabels([])
    ax.set_xlabel("CPU-seconds per million ops (lower is better)")
    ax.set_title("Client CPU efficiency"); ax.legend(fontsize=9)
    fig.suptitle("Resource efficiency — HazelRust uses ~10–30× less memory & less CPU/op")
    return save(fig, outdir, "03_resource_efficiency.png")


def chart_cp_moneypath(headline, outdir):
    cells = [
        ("B.atomiclong.get.default.ki64.v8.ws100000.C64.uniform.closed", "AL get"),
        ("B.atomiclong.increment_and_get.default.ki64.v8.ws100000.C64.uniform.closed", "AL inc"),
        ("B.atomiclong.compare_and_set.success.ki64.v8.ws100000.C64.uniform.closed", "AL CAS"),
        ("B.cpmap.get.hit.ki64.v8.ws100000.C64.uniform.closed", "CPMap get"),
        ("B.cpmap.put.update.ki64.v8.ws100000.C64.uniform.closed", "CPMap put"),
    ]
    labels = [l for _, l in cells]
    x = range(len(cells)); w = 0.38
    fig, ax = plt.subplots(figsize=(8.5, 4.3))
    rr = [get(headline, c, "rust", "thr") or 0 for c, _ in cells]
    jj = [get(headline, c, "java", "thr") or 0 for c, _ in cells]
    ax.bar([i - w/2 for i in x], rr, w, label="Rust", color=RUST)
    ax.bar([i + w/2 for i in x], jj, w, label="Java", color=JAVA)
    ax.set_xticks(list(x)); ax.set_xticklabels(labels)
    ax.set_ylabel("throughput (ops/s)"); ax.yaxis.set_major_formatter(FuncFormatter(kfmt))
    ax.set_title("Suite B — CP subsystem money-path throughput (C=64)\nRust leads CP writes")
    ax.legend()
    return save(fig, outdir, "04_cp_moneypath.png")


def chart_cdfs(cdfs, outdir):
    paths = []
    titles = {
        "J.imap.mixed.ycsb_a.ki64.v100.ws100000.C256.uniform.closed": "Mixed 50/50 @ C=256",
        "A.imap.get.hit.ki64.v100.ws100000.C64.uniform.closed": "IMap get @ C=64",
        "A.imap.put.update.ki64.v100.ws100000.C64.uniform.closed": "IMap put @ C=64",
        "B.atomiclong.increment_and_get.default.ki64.v8.ws100000.C64.uniform.closed": "AtomicLong inc @ C=64",
    }
    items = [(cid, d) for cid, d in cdfs.items() if d["series"]]
    if not items:
        return paths
    n = len(items)
    fig, axes = plt.subplots(1, n, figsize=(4.2 * n, 4))
    if n == 1:
        axes = [axes]
    for ax, (cid, d) in zip(axes, items):
        for cl, color in (("rust", RUST), ("java", JAVA)):
            pts = d["series"].get(cl)
            if pts:
                ax.plot([p[0] for p in pts], [p[1] for p in pts], color=color, label=cl, lw=2)
        ax.set_xscale("log"); ax.set_xlabel("latency (µs, log)"); ax.set_ylabel("P(latency ≤ x)")
        ax.set_title(titles.get(cid, cid.split(".")[1]), fontsize=10); ax.legend(fontsize=9)
        ax.set_ylim(0, 1)
    fig.suptitle("Latency CDFs (pooled across forks)")
    paths.append(save(fig, outdir, "05_latency_cdfs.png"))
    return paths


def chart_mixed(headline, outdir):
    variants = [("ycsb_a", "50/50"), ("ycsb_b", "95/5"), ("ycsb_c", "read-only"), ("ycsb_f", "RMW")]
    def cid(v): return "J.imap.mixed.%s.ki64.v100.ws100000.C64.uniform.closed" % v
    labels = [l for _, l in variants]
    x = range(len(variants)); w = 0.38
    fig, ax = plt.subplots(figsize=(8, 4.2))
    rr = [get(headline, cid(v), "rust", "thr") or 0 for v, _ in variants]
    jj = [get(headline, cid(v), "java", "thr") or 0 for v, _ in variants]
    ax.bar([i - w/2 for i in x], rr, w, label="Rust (pre-opt)", color=RUST_OFF)
    ax.bar([i + w/2 for i in x], jj, w, label="Java", color=JAVA)
    ax.set_xticks(list(x)); ax.set_xticklabels(labels)
    ax.set_ylabel("throughput (ops/s)"); ax.yaxis.set_major_formatter(FuncFormatter(kfmt))
    ax.set_title("Suite J — realistic mixed workloads @ C=64 (pre-optimization)")
    ax.legend()
    return save(fig, outdir, "06_mixed_workloads.png")


def main():
    data = json.load(open(sys.argv[1]))
    outdir = sys.argv[2]
    os.makedirs(outdir, exist_ok=True)
    runs = data["runs"]
    headline = runs.get("headline", {})
    postopt = runs.get("postopt", headline)
    print("charts ->", outdir)
    chart_write_optimization(headline, postopt, outdir)
    chart_thr_vs_c(headline, postopt, outdir)
    chart_resources(headline, outdir)
    chart_cp_moneypath(headline, outdir)
    chart_cdfs(data.get("cdfs", {}), outdir)
    chart_mixed(headline, outdir)
    print("done")


if __name__ == "__main__":
    main()
