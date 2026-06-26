#!/usr/bin/env python3
"""Extract compact chart-ready JSON from one or more benchmark run dirs.

Reuses analyze.py (grouping, bootstrap medians) and hdr.py (histogram merge for
CDFs). Emits bench/chartdata.json consumed by the local chart/DOCX generator.

Usage: python3 bench/chartdata.py <out.json> <label1>:<dir1> [<label2>:<dir2> ...]
"""
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import analyze  # noqa: E402


def extract(out_dir):
    recs, prov = analyze.load_run(out_dir)
    cells = analyze.group_cells(recs)
    A = analyze.agg(cells)
    data = {}
    for cid, e in A.items():
        entry = {"meta": e["meta"], "clients": {}, "ratio": {}}
        for cl, c in e["clients"].items():
            entry["clients"][cl] = {
                "n": c["n"],
                "thr": _pt(c["throughput"]),
                "thr_ci": _ci(c["throughput"]),
                "p50": _pt(c["p50"]), "p99": _pt(c["p99"]), "p999": _pt(c["p999"]),
                "max": _pt(c["max"]),
                "rss_mb": analyze._rss_val(cells, cid, cl),
                "cpu_mops": _pt(c["cpu_mops"]) if c["cpu_mops"] else None,
                "saturated": c["saturated_any"],
                "errors": c["errors_total"],
            }
        for k in ("throughput", "p50", "p99", "p999", "cpu_mops"):
            r = e["ratio"].get(k)
            if r:
                entry["ratio"][k] = {"p": _pt(r), "lo": r[1], "hi": r[2],
                                     "noise": analyze.within_noise(r)}
        data[cid] = entry
    return data, prov, cells


def cdf(cells, cid, client, n=240):
    g = cells.get(cid, {}).get(client)
    if not g:
        return []
    h = analyze.merged_hist(g["recs"])
    if not h:
        return []
    return [[round(v / 1000.0, 3), round(q, 5)] for v, q in h.cdf_points(n)]


def _pt(ci):
    import math
    if ci is None or (isinstance(ci, tuple) and (math.isnan(ci[0]))):
        return None
    return ci[0]


def _ci(ci):
    import math
    if ci is None or math.isnan(ci[1]):
        return None
    return [ci[1], ci[2]]


def main():
    out = sys.argv[1]
    runs = {}
    cells_by_label = {}
    prov = {}
    for spec in sys.argv[2:]:
        label, d = spec.split(":", 1)
        data, p, cells = extract(d)
        runs[label] = data
        cells_by_label[label] = cells
        prov[label] = p

    # CDF overlays for a few headline cells (Rust vs Java), from the first label
    # that contains them.
    cdf_cells = [
        "J.imap.mixed.ycsb_a.ki64.v100.ws100000.C256.uniform.closed",
        "A.imap.get.hit.ki64.v100.ws100000.C64.uniform.closed",
        "A.imap.put.update.ki64.v100.ws100000.C64.uniform.closed",
        "B.atomiclong.increment_and_get.default.ki64.v8.ws100000.C64.uniform.closed",
    ]
    cdfs = {}
    for cid in cdf_cells:
        for label, cells in cells_by_label.items():
            if cid in cells:
                series = {}
                for cl in ("rust", "java"):
                    pts = cdf(cells, cid, cl)
                    if pts:
                        series[cl] = pts
                if series:
                    cdfs[cid] = {"label": label, "series": series}
                break

    json.dump({"runs": runs, "cdfs": cdfs, "provenance": prov}, open(out, "w"))
    print("wrote %s : runs=%s, cdf_cells=%d" % (out, list(runs), len(cdfs)))


if __name__ == "__main__":
    main()
