#!/usr/bin/env python3
"""Benchmark analyzer + report generator (pure stdlib).

Loads all per-run records + env sidecars from a run directory, merges
HdrHistograms across forks (bench/hdr.py), computes per-cell Rust/Java ratios
(throughput, p50, p99, p99.9, CPU-per-Mops, bytes-per-op) with 95% bootstrap
CIs, flags within-noise cells (ratio CI spans 1.0), checks tail-sample validity,
and emits BENCHMARK_REPORT.md with overlaid latency CDFs, throughput-vs-C and
latency-vs-throughput curves (hand-rendered SVG), resource efficiency, server
impact, and a full provenance/caveats appendix.

Honesty contract: median + 95% CI (never bare averages); within-noise cells say
so; tail percentiles report their sample count; raw histograms are preserved.
"""
import argparse
import json
import math
import os
import random
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import hdr  # noqa: E402

RNG = random.Random(0xB0073571)
SUITE_ORDER = ["J", "B", "A", "C", "F"]  # report leads with J (mixed) and B (CP)
SUITE_NAMES = {"J": "Suite J — Realistic mixed workloads (YCSB-style)",
               "B": "Suite B — CP subsystem (money path)",
               "A": "Suite A — IMap (primary workhorse)",
               "C": "Suite C — Collections", "F": "Suite F — ICache"}


# ---------- loading ----------
def load_run(out_dir):
    recs = []
    for fn in sorted(os.listdir(out_dir)):
        if fn == "provenance.json" or fn.endswith(".env.json") or not fn.endswith(".json"):
            continue
        try:
            r = json.load(open(os.path.join(out_dir, fn)))
        except Exception as e:
            print("skip %s: %s" % (fn, e), file=sys.stderr)
            continue
        env_path = os.path.join(out_dir, fn[:-5] + ".env.json")
        r["_env"] = json.load(open(env_path)) if os.path.exists(env_path) else {}
        recs.append(r)
    prov = {}
    pp = os.path.join(out_dir, "provenance.json")
    if os.path.exists(pp):
        prov = json.load(open(pp))
    return recs, prov


# ---------- stats ----------
def median(xs):
    xs = sorted(xs)
    n = len(xs)
    if n == 0:
        return float("nan")
    return xs[n // 2] if n % 2 else (xs[n // 2 - 1] + xs[n // 2]) / 2.0


def bootstrap_ci(xs, stat=median, B=2000, lo=2.5, hi=97.5):
    xs = [x for x in xs if x is not None and not (isinstance(x, float) and math.isnan(x))]
    if len(xs) == 0:
        return (float("nan"), float("nan"), float("nan"))
    point = stat(xs)
    if len(xs) == 1:
        return (point, point, point)
    boots = []
    n = len(xs)
    for _ in range(B):
        sample = [xs[RNG.randrange(n)] for _ in range(n)]
        boots.append(stat(sample))
    boots.sort()
    return (point, boots[int(lo / 100 * B)], boots[min(B - 1, int(hi / 100 * B))])


def ratio_ci(rust_xs, java_xs, B=2000):
    """Bootstrap CI for median(rust)/median(java)."""
    r = [x for x in rust_xs if x is not None]
    j = [x for x in java_xs if x is not None]
    if not r or not j:
        return (float("nan"), float("nan"), float("nan"))
    point = median(r) / median(j) if median(j) else float("nan")
    if len(r) == 1 and len(j) == 1:
        return (point, point, point)
    boots = []
    for _ in range(B):
        rr = median([r[RNG.randrange(len(r))] for _ in range(len(r))])
        jj = median([j[RNG.randrange(len(j))] for _ in range(len(j))])
        if jj:
            boots.append(rr / jj)
    boots.sort()
    if not boots:
        return (point, float("nan"), float("nan"))
    return (point, boots[int(0.025 * len(boots))], boots[min(len(boots) - 1, int(0.975 * len(boots)))])


# ---------- grouping ----------
def group_cells(recs):
    """cell_id -> client -> {metric -> [per fork/trial values], 'recs':[...]}"""
    cells = {}
    for r in recs:
        cid, cl = r["cell_id"], r["client"]
        c = cells.setdefault(cid, {})
        g = c.setdefault(cl, {"throughput": [], "p50": [], "p99": [], "p999": [],
                              "p9999": [], "mean": [], "max": [], "errors": [],
                              "cpu_mops": [], "bytes_op": [], "samples_above_p999": [],
                              "saturated": [], "recs": []})
        lat = r["latency"]
        g["throughput"].append(r["throughput"])
        for k in ("p50", "p99", "p999", "p9999", "mean", "max"):
            g[k].append(lat.get(k))
        g["samples_above_p999"].append(lat.get("samples_above_p999", 0))
        g["errors"].append(r["errors"]["total"])
        g["saturated"].append(bool(r.get("saturated")))
        env = r.get("_env", {})
        cpu = env.get("sampler", {}).get("client_cpu_pct", {})
        # CPU-seconds per million ops over the measure window (approx: mean cpu% * duration)
        if cpu.get("mean") is not None and r.get("ops"):
            cpu_s = (cpu["mean"] / 100.0) * r.get("duration_s", 0)
            if r["ops"] > 0:
                g["cpu_mops"].append(cpu_s / r["ops"] * 1e6)
        if env.get("net_bytes_per_op_loopback") is not None:
            g["bytes_op"].append(env["net_bytes_per_op_loopback"])
        g["recs"].append(r)
    return cells


def agg(cells):
    """For each cell/client, compute medians+CI; for each cell, ratios."""
    out = {}
    for cid, byclient in cells.items():
        entry = {"clients": {}, "ratio": {}}
        meta = None
        for cl, g in byclient.items():
            meta = meta or g["recs"][0]
            entry["clients"][cl] = {
                "n": len(g["throughput"]),
                "throughput": bootstrap_ci(g["throughput"]),
                "p50": bootstrap_ci(g["p50"]),
                "p99": bootstrap_ci(g["p99"]),
                "p999": bootstrap_ci(g["p999"]),
                "p9999": bootstrap_ci(g["p9999"]),
                "max": bootstrap_ci(g["max"]),
                "errors_total": sum(g["errors"]),
                "saturated_any": any(g["saturated"]),
                "samples_above_p999_min": min(g["samples_above_p999"]) if g["samples_above_p999"] else 0,
                "cpu_mops": bootstrap_ci(g["cpu_mops"]) if g["cpu_mops"] else None,
                "bytes_op": bootstrap_ci(g["bytes_op"]) if g["bytes_op"] else None,
            }
        if "rust" in byclient and "java" in byclient:
            R, J = byclient["rust"], byclient["java"]
            # throughput ratio rust/java (>1 = rust faster)
            entry["ratio"]["throughput"] = ratio_ci(R["throughput"], J["throughput"])
            # latency ratios rust/java (>1 = rust slower)
            for k in ("p50", "p99", "p999"):
                entry["ratio"][k] = ratio_ci(R[k], J[k])
            if R["cpu_mops"] and J["cpu_mops"]:
                entry["ratio"]["cpu_mops"] = ratio_ci(R["cpu_mops"], J["cpu_mops"])
        entry["meta"] = {k: meta.get(k) for k in
                         ("suite", "structure", "op", "variant", "value_size", "concurrency",
                          "load_model", "distribution", "key_type", "batch_size", "target_rate")} if meta else {}
        out[cid] = entry
    return out


def within_noise(ci):
    if ci is None:
        return True
    _, lo, hi = ci
    if any(math.isnan(x) for x in (lo, hi)):
        return True
    return lo <= 1.0 <= hi


# ---------- merged histograms ----------
def merged_hist(recs):
    h = None
    for r in recs:
        try:
            hh = hdr.decode_compressed(r["latency"]["hgrm_b64"])
        except Exception:
            continue
        if h is None:
            h = hh
        else:
            h.merge(hh)
    return h


# ---------- SVG ----------
def _scale(v, vmin, vmax, lo, hi, logx=False):
    if logx:
        v = math.log10(max(v, 1e-9)); vmin = math.log10(max(vmin, 1e-9)); vmax = math.log10(max(vmax, 1e-9))
    if vmax == vmin:
        return (lo + hi) / 2
    return lo + (v - vmin) / (vmax - vmin) * (hi - lo)


def svg_lines(series, xlabel, ylabel, title, logx=False, logy=False, width=720, height=420):
    """series: list of (name, color, [(x,y),...])."""
    ml, mr, mt, mb = 70, 160, 40, 55
    pw, ph = width - ml - mr, height - mt - mb
    allx = [p[0] for _, _, pts in series for p in pts]
    ally = [p[1] for _, _, pts in series for p in pts]
    if not allx or not ally:
        return "<svg xmlns='http://www.w3.org/2000/svg' width='%d' height='40'><text x='10' y='25'>%s: no data</text></svg>" % (width, title)
    xmin, xmax = min(allx), max(allx)
    ymin, ymax = (min(ally), max(ally))
    if logy:
        ymin = max(min([y for y in ally if y > 0], default=1), 1e-6)
    else:
        ymin = min(ymin, 0)
    out = ["<svg xmlns='http://www.w3.org/2000/svg' width='%d' height='%d' font-family='sans-serif' font-size='12'>" % (width, height)]
    out.append("<rect width='100%%' height='100%%' fill='white'/>")
    out.append("<text x='%d' y='20' font-weight='bold'>%s</text>" % (ml, title))
    # axes
    out.append("<line x1='%d' y1='%d' x2='%d' y2='%d' stroke='black'/>" % (ml, mt, ml, mt + ph))
    out.append("<line x1='%d' y1='%d' x2='%d' y2='%d' stroke='black'/>" % (ml, mt + ph, ml + pw, mt + ph))
    # gridlines / ticks (5 each)
    for i in range(6):
        gy = mt + ph - i / 5 * ph
        yval = ymin + i / 5 * (ymax - ymin)
        if logy:
            yval = 10 ** (math.log10(ymin) + i / 5 * (math.log10(max(ymax, ymin * 10)) - math.log10(ymin)))
        out.append("<line x1='%d' y1='%.1f' x2='%d' y2='%.1f' stroke='#eee'/>" % (ml, gy, ml + pw, gy))
        out.append("<text x='%d' y='%.1f' text-anchor='end'>%s</text>" % (ml - 5, gy + 4, _fmt(yval)))
        gx = ml + i / 5 * pw
        xval = xmin + i / 5 * (xmax - xmin)
        if logx:
            xval = 10 ** (math.log10(max(xmin, 1e-9)) + i / 5 * (math.log10(max(xmax, 1e-9)) - math.log10(max(xmin, 1e-9))))
        out.append("<text x='%.1f' y='%d' text-anchor='middle'>%s</text>" % (gx, mt + ph + 18, _fmt(xval)))
    out.append("<text x='%d' y='%d' text-anchor='middle'>%s</text>" % (ml + pw / 2, height - 8, xlabel))
    out.append("<text x='14' y='%d' transform='rotate(-90 14 %d)' text-anchor='middle'>%s</text>" % (mt + ph / 2, mt + ph / 2, ylabel))

    def sy(y):
        if logy:
            return mt + ph - _scale(max(y, ymin), ymin, max(ymax, ymin * 10), 0, ph, logx=True)
        return mt + ph - _scale(y, ymin, ymax, 0, ph)

    def sx(x):
        return ml + _scale(x, xmin, xmax, 0, pw, logx=logx)

    for i, (name, color, pts) in enumerate(series):
        if not pts:
            continue
        d = " ".join("%.1f,%.1f" % (sx(x), sy(y)) for x, y in pts)
        out.append("<polyline fill='none' stroke='%s' stroke-width='2' points='%s'/>" % (color, d))
        ly = mt + 10 + i * 18
        out.append("<line x1='%d' y1='%d' x2='%d' y2='%d' stroke='%s' stroke-width='2'/>" % (ml + pw + 12, ly, ml + pw + 32, ly, color))
        out.append("<text x='%d' y='%d'>%s</text>" % (ml + pw + 36, ly + 4, name))
    out.append("</svg>")
    return "".join(out)


def _fmt(v):
    if v >= 1000:
        return "%.0fk" % (v / 1000) if v < 1e6 else "%.1fM" % (v / 1e6)
    if v >= 10:
        return "%.0f" % v
    if v >= 1:
        return "%.1f" % v
    return "%.2f" % v


# ---------- report ----------
def fmt_ci(ci, unit="", scale=1.0):
    if ci is None:
        return "n/a"
    p, lo, hi = ci
    if any(math.isnan(x) for x in (p, lo, hi)):
        return "n/a"
    return "%.1f%s [%.1f, %.1f]" % (p * scale, unit, lo * scale, hi * scale)


def verdict(ratio_thr, ratio_p99):
    """Plain-language verdict from throughput ratio (rust/java) and p99 ratio."""
    if ratio_thr is None:
        return "single-client"
    if within_noise(ratio_thr):
        return "**within noise**"
    p = ratio_thr[0]
    if p > 1:
        return "Rust faster x%.2f" % p
    return "Java faster x%.2f" % (1 / p)


def write_report(out_dir, report_path, plots_dir):
    recs, prov = load_run(out_dir)
    if not recs:
        print("no records in", out_dir); return
    cells = group_cells(recs)
    A = agg(cells)
    os.makedirs(plots_dir, exist_ok=True)

    by_suite = {}
    for cid, e in A.items():
        by_suite.setdefault(e["meta"].get("suite", "?"), []).append((cid, e))

    L = []
    L.append("# HazelRust vs. Official Hazelcast Java Client — Benchmark Report\n")
    L.append("_Generated by bench/analyze.py. Methodology: docs/cbdc/BENCHMARK_METHODOLOGY.md._\n")
    tier = recs[0]["provenance"].get("manifest_tier", "?")
    n_runs = len(recs)
    L.append("**Tier:** `%s`  **Records:** %d  **Cells:** %d\n" % (tier, n_runs, len(A)))

    # ---- executive summary ----
    L.append("\n## Executive summary\n")
    wins_r = wins_j = noise = 0
    for cid, e in A.items():
        rt = e["ratio"].get("throughput")
        if rt is None:
            continue
        if within_noise(rt):
            noise += 1
        elif rt[0] > 1:
            wins_r += 1
        else:
            wins_j += 1
    L.append("Across %d head-to-head cells (closed-loop throughput): "
             "**Rust faster in %d**, **Java faster in %d**, **within noise in %d**.\n"
             % (wins_r + wins_j + noise, wins_r, wins_j, noise))
    L.append("\n> Honesty contract: every figure is a **median across forks×trials with a 95%% "
             "bootstrap CI**. Cells whose Rust/Java ratio CI spans 1.0 are flagged **within noise** "
             "and no winner is claimed. Tail percentiles report their sample count. "
             "The rig is **co-located** (see Caveats).\n")

    # ---- per-suite tables ----
    for suite in SUITE_ORDER + [s for s in by_suite if s not in SUITE_ORDER]:
        if suite not in by_suite:
            continue
        L.append("\n## %s\n" % SUITE_NAMES.get(suite, "Suite " + suite))
        L.append("\n| Cell | C | val | dist | Rust thr (med ops/s) | Java thr | thr ratio (R/J) | Rust p99 (µs) | Java p99 | p99 ratio | tail n | verdict |")
        L.append("|---|--:|--:|---|--:|--:|--:|--:|--:|--:|--:|---|")
        rows = sorted(by_suite[suite], key=lambda kv: (kv[1]["meta"].get("op", ""),
                      kv[1]["meta"].get("value_size", 0), kv[1]["meta"].get("concurrency", 0)))
        for cid, e in rows:
            if e["meta"].get("load_model") != "closed":
                continue
            m = e["meta"]
            rc = e["clients"].get("rust"); jc = e["clients"].get("java")
            rt = e["ratio"].get("throughput"); rp = e["ratio"].get("p99")
            shortid = "%s.%s.%s" % (m.get("structure"), m.get("op"), m.get("variant"))
            tailn = min([c["samples_above_p999_min"] for c in e["clients"].values()] or [0])
            L.append("| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %d | %s |" % (
                shortid, m.get("concurrency"), m.get("value_size"), (m.get("distribution") or "")[:4],
                _num(rc, "throughput"), _num(jc, "throughput"),
                _rat(rt), _num(rc, "p99"), _num(jc, "p99"), _rat(rp), tailn, verdict(rt, rp)))

        # open-loop latency-under-rate table
        opencells = [(cid, e) for cid, e in by_suite[suite] if e["meta"].get("load_model") == "open"]
        if opencells:
            L.append("\n**Open-loop latency under controlled rate (CO-corrected):**\n")
            L.append("\n| Cell | C | target ops/s | Rust p99 (µs) | Java p99 | p99.9 R | p99.9 J | saturated |")
            L.append("|---|--:|--:|--:|--:|--:|--:|:--:|")
            for cid, e in sorted(opencells, key=lambda kv: kv[1]["meta"].get("target_rate") or 0):
                m = e["meta"]; rc = e["clients"].get("rust"); jc = e["clients"].get("java")
                sat = any(c["saturated_any"] for c in e["clients"].values())
                L.append("| %s | %s | %s | %s | %s | %s | %s | %s |" % (
                    "%s.%s" % (m.get("structure"), m.get("op")), m.get("concurrency"),
                    _fmt(m.get("target_rate") or 0), _num(rc, "p99"), _num(jc, "p99"),
                    _num(rc, "p999"), _num(jc, "p999"), "⚠️" if sat else "no"))

    # ---- plots ----
    L.append("\n## Plots\n")
    plot_files = make_plots(A, cells, plots_dir)
    for title, fn in plot_files:
        rel = os.path.relpath(fn, os.path.dirname(report_path))
        L.append("\n**%s**\n\n![%s](%s)\n" % (title, title, rel.replace("\\", "/")))

    # ---- resource efficiency ----
    L.append("\n## Resource efficiency & server-side impact\n")
    L.append("\n| Cell | Rust CPU-s/Mops | Java CPU-s/Mops | Rust RSS (MB) | Java RSS (MB) | server CPU% (mean of 3 members) |")
    L.append("|---|--:|--:|--:|--:|--:|")
    for cid, e in sorted(A.items()):
        if e["meta"].get("load_model") != "closed":
            continue
        rc = e["clients"].get("rust"); jc = e["clients"].get("java")
        rrss = _rss(cells, cid, "rust"); jrss = _rss(cells, cid, "java")
        srv = _server_cpu(cells, cid)
        L.append("| %s | %s | %s | %s | %s | %s |" % (
            cid.split(".closed")[0][-46:], _num(rc, "cpu_mops", "%.2f"), _num(jc, "cpu_mops", "%.2f"),
            rrss, jrss, srv))

    # ---- errors / validity ----
    total_err = sum(e["clients"][cl]["errors_total"] for cid, e in A.items() for cl in e["clients"])
    L.append("\n## Validity\n")
    L.append("- Total errors across all runs: **%d**.\n" % total_err)
    contaminated = [r["cell_id"] + "/" + r["client"] for r in recs if r.get("_env", {}).get("contamination")]
    L.append("- Contamination flags (member migration/slow-op/Full-GC during a window): **%d**%s\n"
             % (len(contaminated), (" — " + ", ".join(contaminated[:6]) + ("…" if len(contaminated) > 6 else "")) if contaminated else ""))
    low_tail = [cid for cid, e in A.items() if any(c["samples_above_p999_min"] < 1000 for c in e["clients"].values())
                and recs[0]["provenance"].get("manifest_tier") not in ("t0",)]
    L.append("- Cells with <1000 samples above p99.9 (tail estimate weaker): **%d**.\n" % len(low_tail))

    # ---- provenance + caveats ----
    L.append("\n## Provenance\n```json\n%s\n```\n" % json.dumps(prov, indent=2))
    L.append("\n## Caveats (fairness deviations, stated plainly)\n")
    L.append("- **Co-located rig.** Cluster + client share one host with disjoint core pinning "
             "(members `%s`, client `%s`). They still share L3/memory-bandwidth/loopback; "
             "absolute throughput is higher than a real LAN deployment but BOTH clients see "
             "identical conditions, so the comparison is fair.\n" % (MEMBER_CORES_DOC, CLIENT_CORES_DOC))
    L.append("- **CPU governor** is not settable in the virtualized guest (no cpufreq); frequency is "
             "hypervisor-managed. Achieved MHz is recorded in provenance.\n")
    L.append("- **Java client = 5.7.0** (latest 5.7.x on Maven Central), version-identical to the EE "
             "5.7.0 cluster — exact protocol parity. JVM: Corretto 21. JFR uses low-overhead `default` "
             "settings; GC logging adds negligible overhead.\n")
    L.append("- **Wire parity:** byte[]/long/String values only (no Compact/Portable), identical Data "
             "framing, so per-op wire bytes match; smart routing ON, statistics OFF, near-cache OFF "
             "for both.\n")
    L.append("- **net bytes/op** is measured on loopback (counts both directions); treat as a relative "
             "framing indicator, not an absolute on-wire byte count.\n")
    L.append("- Raw per-run records + embedded HdrHistograms are preserved in the run directory "
             "(auditable).\n")

    open(report_path, "w").write("\n".join(L) + "\n")
    print("wrote", report_path)


MEMBER_CORES_DOC = "0-3,8-11"
CLIENT_CORES_DOC = "4-6,12-14"


def _num(client_entry, key, fmt="%.0f"):
    if not client_entry or client_entry.get(key) is None:
        return "—"
    ci = client_entry[key]
    if ci is None or (isinstance(ci, tuple) and math.isnan(ci[0])):
        return "—"
    return fmt % ci[0]


def _rat(ci):
    if ci is None or math.isnan(ci[0]):
        return "—"
    flag = " *(noise)*" if within_noise(ci) else ""
    return "%.2f [%.2f,%.2f]%s" % (ci[0], ci[1], ci[2], flag)


def _rss(cells, cid, client):
    g = cells.get(cid, {}).get(client)
    if not g:
        return "—"
    rss = [r.get("mem", {}).get("rss_peak_kb", 0) for r in g["recs"]]
    rss = [x for x in rss if x]
    return "%.0f" % (max(rss) / 1024.0) if rss else "—"


def _server_cpu(cells, cid):
    vals = []
    for client in ("rust", "java"):
        g = cells.get(cid, {}).get(client)
        if not g:
            continue
        for r in g["recs"]:
            sc = r.get("_env", {}).get("sampler", {}).get("server_cpu_pct", {})
            ms = [sc[m].get("mean") for m in sc if sc[m].get("mean") is not None]
            if ms:
                vals.append(sum(ms))
    return "%.0f" % (median(vals)) if vals else "—"


def make_plots(A, cells, plots_dir):
    files = []
    # 1) Latency CDF overlay for a few headline cells (one per suite, mid-C)
    headline = pick_headline(A)
    for cid in headline:
        series = []
        for client, color in (("rust", "#d62728"), ("java", "#1f77b4")):
            g = cells.get(cid, {}).get(client)
            if not g:
                continue
            h = merged_hist(g["recs"])
            if h is None:
                continue
            pts = [(v / 1000.0, q) for v, q in h.cdf_points(300)]  # ns->us
            series.append((client, color, pts))
        if series:
            svg = svg_lines(series, "latency (µs, log)", "cumulative probability",
                            "Latency CDF — " + _short(A[cid]["meta"]), logx=True)
            fn = os.path.join(plots_dir, "cdf_%s.svg" % _safe(cid))
            open(fn, "w").write(svg)
            files.append(("Latency CDF — " + _short(A[cid]["meta"]), fn))
    # 2) Throughput vs C, per (structure,op,value,dist) group
    groups = {}
    for cid, e in A.items():
        m = e["meta"]
        if m.get("load_model") != "closed":
            continue
        key = (m["structure"], m["op"], m.get("variant"), m.get("value_size"), m.get("distribution"))
        groups.setdefault(key, []).append((m.get("concurrency", 1), cid, e))
    # plot the few groups with the most C points
    top = sorted(groups.items(), key=lambda kv: -len(kv[1]))[:4]
    for key, items in top:
        items.sort()
        rust_pts = [(c, e["clients"]["rust"]["throughput"][0]) for c, cid, e in items if "rust" in e["clients"]]
        java_pts = [(c, e["clients"]["java"]["throughput"][0]) for c, cid, e in items if "java" in e["clients"]]
        series = [("rust", "#d62728", rust_pts), ("java", "#1f77b4", java_pts)]
        title = "Throughput vs C — %s.%s v%s %s" % (key[0], key[1], key[3], key[4])
        svg = svg_lines(series, "concurrency C (log2)", "throughput (ops/s)", title, logx=True)
        fn = os.path.join(plots_dir, "thrC_%s.svg" % _safe("%s_%s_%s_%s" % (key[0], key[1], key[3], key[4])))
        open(fn, "w").write(svg)
        files.append((title, fn))
    return files


def pick_headline(A):
    chosen = []
    for suite in ("J", "B", "A"):
        cands = [(cid, e) for cid, e in A.items()
                 if e["meta"].get("suite") == suite and e["meta"].get("load_model") == "closed"
                 and "rust" in e["clients"] and "java" in e["clients"]]
        cands.sort(key=lambda kv: -(kv[1]["meta"].get("concurrency") or 0))
        if cands:
            chosen.append(cands[0][0])
    return chosen


def _short(m):
    return "%s.%s.%s C%s v%s %s" % (m.get("structure"), m.get("op"), m.get("variant"),
                                    m.get("concurrency"), m.get("value_size"), m.get("distribution"))


def _safe(s):
    return "".join(ch if ch.isalnum() else "_" for ch in s)[:80]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="in_dir", required=True)
    ap.add_argument("--report", default=None)
    ap.add_argument("--plots", default=None)
    args = ap.parse_args()
    report = args.report or os.path.join(args.in_dir, "BENCHMARK_REPORT.md")
    plots = args.plots or os.path.join(args.in_dir, "plots")
    write_report(args.in_dir, report, plots)


if __name__ == "__main__":
    main()
