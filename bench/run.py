#!/usr/bin/env python3
"""Benchmark orchestrator (runs ON the AWS instance).

Drives cell x client x fork x trial with A/B/B/A interleaving + randomized cell
order, core pinning (members vs client on disjoint cpusets), best-effort
governor, resource samplers (/proc + docker stats), Java GC log + JFR capture,
server-side member scrape, open-loop rate resolution from the closed-loop max,
and a contamination guard. Emits, per run: the harness record (<stem>.json), an
environment sidecar (<stem>.env.json), and for Java a GC log + JFR.

See docs/cbdc/BENCHMARK_METHODOLOGY.md sections 6-9. Honesty contract: every
fairness deviation (co-location, governor) is recorded in provenance.json.
"""
import argparse
import json
import os
import re
import random
import subprocess
import sys
import threading
import time

# ---- Rig configuration (disjoint core pinning; §2) ------------------------
MEMBER_CORES = "0-3,8-11"   # 4 physical cores (8 logical) for the 3 EE members
CLIENT_CORES = "4-6,12-14"  # 3 physical cores (6 logical) for the client harness
OS_CORES = "7,15"           # left for OS / docker daemon / orchestrator+samplers
MEMBERS = ["hz1", "hz2", "hz3"]
OTHER_CONTAINERS = ["hzsec", "hzsolo", "hztls"]  # quiesced during the run


def read_license(path=os.path.expanduser("~/hz/hz1.yaml")):
    """Read the EE license from the read-only member config. Returns the key (or
    None). NEVER printed/logged/committed — passed only to the Java subprocess
    via the HZ_LICENSE env var. The EE Java client requires it (also for CP)."""
    try:
        for ln in open(path):
            m = re.match(r"\s*license-key\s*:\s*(\S+)", ln)
            if m:
                return m.group(1).strip().strip('"').strip("'")
    except Exception:
        pass
    return None


def sh(cmd, timeout=60, check=False):
    try:
        p = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)
        if check and p.returncode != 0:
            print("WARN cmd failed (%d): %s\n%s" % (p.returncode, cmd, p.stderr[:400]), file=sys.stderr)
        return p.returncode, p.stdout, p.stderr
    except Exception as e:
        print("WARN cmd exc: %s : %s" % (cmd, e), file=sys.stderr)
        return 1, "", str(e)


# ---- Provenance ------------------------------------------------------------
def cpu_mhz():
    try:
        vals = []
        for line in open("/proc/cpuinfo"):
            if line.lower().startswith("cpu mhz"):
                vals.append(float(line.split(":")[1]))
        return {"min": min(vals), "max": max(vals), "mean": sum(vals) / len(vals)} if vals else {}
    except Exception:
        return {}


def governor_state():
    path = "/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor"
    if os.path.exists(path):
        return open(path).read().strip()
    return "unavailable (virtualized guest; frequency managed by hypervisor)"


def collect_provenance(args, hz_client_version):
    _, kernel, _ = sh("uname -a")
    _, rustc, _ = sh("rustc --version")
    _, javav, _ = sh("java -version 2>&1 | head -1")
    _, model, _ = sh("lscpu | grep -i 'model name' | head -1")
    _, nproc, _ = sh("nproc --all")  # --all: true machine count, not the pinned affinity
    member_cpuset = {}
    for m in MEMBERS:
        _, cs, _ = sh("sudo docker inspect --format '{{.HostConfig.CpusetCpus}}' %s" % m)
        member_cpuset[m] = cs.strip()
    return {
        "benchmark": "HazelRust vs official Hazelcast Java client",
        "commit": args.commit,
        "cluster_name": args.cluster_name,
        "cluster": args.cluster,
        "cluster_topology": "3-node EE 5.7.0 (dev), -Xms512m -Xmx1g per member",
        "hz_java_client_version": hz_client_version,
        "rustc": rustc.strip(),
        "java": javav.strip(),
        "cargo_profile": "release (opt-level=3, lto=false, codegen-units=16)",
        "cpu_model": model.split(":")[-1].strip() if ":" in model else model.strip(),
        "logical_cpus": nproc.strip(),
        "cpu_mhz": cpu_mhz(),
        "governor": governor_state(),
        "kernel": kernel.strip(),
        "pinning": {"members": MEMBER_CORES, "client": CLIENT_CORES, "os": OS_CORES,
                    "applied": args.pin, "member_cpuset_observed": member_cpuset},
        "colocation_caveat": ("Cluster and client are CO-LOCATED on one AWS instance. "
                              "Members and client are pinned to DISJOINT core sets, but "
                              "they share L3, memory bandwidth, and the NIC/loopback. "
                              "Absolute numbers may differ from a 2-machine LAN setup; "
                              "the Rust/Java COMPARISON is fair (identical conditions)."),
        "jfr": args.jfr,
        "ts_unix_ms": int(time.time() * 1000),
    }


# ---- Rig setup -------------------------------------------------------------
def setup_rig(args):
    if args.quiesce:
        for c in OTHER_CONTAINERS:
            sh("sudo docker stop %s" % c, timeout=60)
        print("quiesced: %s" % ", ".join(OTHER_CONTAINERS))
    if args.pin:
        for m in MEMBERS:
            sh("sudo docker update --cpuset-cpus '%s' %s" % (MEMBER_CORES, m), check=True)
        print("pinned members -> %s" % MEMBER_CORES)
        # pin this orchestrator + children that aren't taskset-wrapped to OS cores
        sh("taskset -cp %s %d" % (OS_CORES, os.getpid()))
    if args.governor:
        rc, _, _ = sh("sudo cpupower frequency-set -g performance")
        print("governor set rc=%d (best-effort; may be unavailable on EC2)" % rc)


def restore_rig(args):
    if args.quiesce and args.restore:
        for c in OTHER_CONTAINERS:
            sh("sudo docker start %s" % c, timeout=60)
        print("restored: %s" % ", ".join(OTHER_CONTAINERS))


# ---- Samplers --------------------------------------------------------------
class Sampler(threading.Thread):
    """Samples client /proc and server docker stats at ~1s during a run."""
    def __init__(self, pid):
        super().__init__(daemon=True)
        self.pid = pid
        self.stop = threading.Event()
        self.cpu_pct = []      # client process CPU% per sample
        self.threads = []      # client thread count
        self.server = {m: [] for m in MEMBERS}  # member cpu% samples
        self.server_mem = {m: [] for m in MEMBERS}

    def _proc_stat(self):
        try:
            f = open("/proc/%d/stat" % self.pid).read()
            after = f[f.rfind(")") + 2:].split()
            utime, stime = int(after[11]), int(after[12])
            nthreads = int(after[17])
            return utime + stime, nthreads
        except Exception:
            return None, None

    def run(self):
        clk = os.sysconf("SC_CLK_TCK")
        last_cpu, last_t = self._proc_stat(), time.time()
        while not self.stop.is_set():
            time.sleep(1.0)
            cur_cpu, nthreads = self._proc_stat()
            now = time.time()
            if cur_cpu is not None and last_cpu[0] is not None and now > last_t:
                dj = cur_cpu - last_cpu[0]
                self.cpu_pct.append(100.0 * (dj / clk) / (now - last_t))
                self.threads.append(nthreads)
            last_cpu, last_t = (cur_cpu, nthreads), now
            # server-side member CPU/mem (one shot)
            rc, out, _ = sh("sudo docker stats --no-stream --format '{{.Name}};{{.CPUPerc}};{{.MemUsage}}' %s"
                            % " ".join(MEMBERS), timeout=15)
            for line in out.splitlines():
                parts = line.split(";")
                if len(parts) >= 3 and parts[0] in self.server:
                    try:
                        self.server[parts[0]].append(float(parts[1].replace("%", "")))
                    except Exception:
                        pass
                    self.server_mem[parts[0]].append(parts[2].split("/")[0].strip())

    def summary(self):
        def stats(xs):
            xs = [x for x in xs if x is not None]
            if not xs:
                return {}
            return {"mean": sum(xs) / len(xs), "max": max(xs), "min": min(xs), "n": len(xs)}
        return {
            "client_cpu_pct": stats(self.cpu_pct),
            "client_threads_max": max(self.threads) if self.threads else 0,
            "server_cpu_pct": {m: stats(self.server[m]) for m in MEMBERS},
            "server_mem_last": {m: (self.server_mem[m][-1] if self.server_mem[m] else None) for m in MEMBERS},
        }


def net_lo_bytes():
    try:
        for line in open("/proc/net/dev"):
            if line.strip().startswith("lo:"):
                f = line.split(":")[1].split()
                return int(f[0]) + int(f[8])  # rx + tx bytes
    except Exception:
        pass
    return 0


def contamination_check(start_ms):
    """Scan member logs for migration / slow-op / long-GC during the window."""
    since = "%ds" % max(1, int((time.time() * 1000 - start_ms) / 1000) + 2)
    flags = []
    for m in MEMBERS:
        rc, out, _ = sh("sudo docker logs --since %s %s 2>&1 | grep -iE 'migration|repartition|slow operation|Pause Full' | head -3"
                        % (since, m), timeout=20)
        if out.strip():
            flags.append({"member": m, "events": out.strip().splitlines()[:3]})
    return flags


# ---- Run a single cell x client x fork x trial -----------------------------
def run_one(args, cell, client, fork, trial, out_dir, target_rate=None):
    stem = "%s.%s.f%d.t%d" % (cell["id"], client, fork, trial)
    rec_path = os.path.join(out_dir, "%s.json" % stem)
    common = ["--manifest", args.manifest, "--cell", cell["id"],
              "--fork", str(fork), "--trial", str(trial), "--out", out_dir,
              "--cluster", args.cluster, "--cluster-name", args.cluster_name,
              "--commit", args.commit]
    if args.warmup_s is not None:
        common += ["--warmup-s", str(args.warmup_s)]
    if args.measure_s is not None:
        common += ["--measure-s", str(args.measure_s)]
    if target_rate is not None:
        common += ["--target-rate", "%.3f" % target_rate]

    taskset = ["taskset", "-c", CLIENT_CORES] if args.pin else []
    if client == "rust":
        cmd = taskset + [args.rust_bin] + common
    else:
        jvm = ["java"]
        if args.jfr:
            jvm += ["-XX:+FlightRecorder",
                    "-XX:StartFlightRecording=filename=%s.jfr,dumponexit=true,settings=default" % os.path.join(out_dir, stem)]
        jvm += ["-Xlog:gc*:file=%s.gc.log:time,uptime,level,tags" % os.path.join(out_dir, stem)]
        jvm += ["-jar", args.java_jar, "--client-version", args.hz_client_version]
        cmd = taskset + jvm + common

    start_ms = int(time.time() * 1000)
    lo0 = net_lo_bytes()
    env = {"argv": cmd, "client_cores": CLIENT_CORES if args.pin else "unpinned"}
    proc_env = dict(os.environ)
    if client == "java" and getattr(args, "license", None):
        proc_env["HZ_LICENSE"] = args.license  # EE client license (never logged)
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=proc_env)
    sampler = Sampler(proc.pid)
    sampler.start()
    out, err = proc.communicate()
    sampler.stop.set()
    sampler.join(timeout=5)
    lo1 = net_lo_bytes()
    env["sampler"] = sampler.summary()
    env["net_lo_bytes_total"] = lo1 - lo0
    env["contamination"] = contamination_check(start_ms)
    env["harness_stderr_tail"] = (err or "").strip().splitlines()[-3:]
    env["exit_code"] = proc.returncode
    if proc.returncode != 0 or not os.path.exists(rec_path):
        print("  !! %s FAILED rc=%d: %s" % (stem, proc.returncode, (err or "")[-300:]), file=sys.stderr)
        env["ok"] = False
    else:
        env["ok"] = True
        # net bytes per op (loopback counts both directions; divide by 2 for one-way estimate)
        try:
            rec = json.load(open(rec_path))
            ops = rec.get("ops", 0)
            if ops > 0:
                env["net_bytes_per_op_loopback"] = (lo1 - lo0) / ops
        except Exception:
            pass
        line = (err or "").strip().splitlines()
        if line:
            print("  " + line[-1])
    json.dump(env, open(os.path.join(out_dir, "%s.env.json" % stem), "w"), indent=1)
    return env


# ---- Manifest selection + interleave ---------------------------------------
def select_cells(manifest, args):
    cells = manifest["cells"]
    if args.cells:
        wanted = args.cells.split(",")
        cells = [c for c in cells if any(w == c["id"] or w in c["id"] for w in wanted)]
    if args.suites:
        sset = set(args.suites.split(","))
        cells = [c for c in cells if c["suite"] in sset]
    if args.structures:
        st = set(args.structures.split(","))
        cells = [c for c in cells if c["structure"] in st]
    if args.max_c is not None:
        cells = [c for c in cells if c["concurrency"] <= args.max_c]
    if args.open_only:
        return [c for c in cells if c["load_model"] == "open"]
    if not args.open:
        cells = [c for c in cells if c["load_model"] != "open"]
    return cells


def preseed(args, cells, out_dir):
    """Seed each distinct shared-data shape once (with the Rust harness)."""
    shapes = {}
    for c in cells:
        if c["structure"] in ("imap", "cpmap"):
            key = (c["structure"], c["value_size"], c["working_set"], c["op"], c["variant"])
            # only need one seeding cell per (structure,value_size,ws)
            shapes.setdefault((c["structure"], c["value_size"], c["working_set"]), c)
    print("pre-seeding %d data shapes ..." % len(shapes))
    for (struct, vs, ws), c in shapes.items():
        cmd = [args.rust_bin, "--manifest", args.manifest, "--cell", c["id"],
               "--cluster", args.cluster, "--cluster-name", args.cluster_name,
               "--out", out_dir, "--seed-only"]
        rc, o, e = sh(" ".join("'%s'" % x for x in cmd), timeout=900)
        print("  seed %s v%s ws%s rc=%d" % (struct, vs, ws, rc))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--manifest", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--clients", default="rust,java")
    ap.add_argument("--cluster", default="127.0.0.1:5701,127.0.0.1:5702,127.0.0.1:5703")
    ap.add_argument("--cluster-name", default="dev")
    ap.add_argument("--commit", default="unknown")
    ap.add_argument("--rust-bin", default="target/release/hazelrust-bench")
    ap.add_argument("--java-jar", default="bench-java/build/libs/hazeljava-bench.jar")
    ap.add_argument("--hz-client-version", default="5.7.0")
    ap.add_argument("--forks", type=int)
    ap.add_argument("--trials", type=int)
    ap.add_argument("--warmup-s", type=int)
    ap.add_argument("--measure-s", type=int)
    ap.add_argument("--cells")
    ap.add_argument("--suites")
    ap.add_argument("--structures")
    ap.add_argument("--max-c", type=int)
    ap.add_argument("--open", action="store_true", help="also run open-loop cells")
    ap.add_argument("--open-only", action="store_true",
                    help="run ONLY open-loop cells, resolving rates from closed records already in --out")
    ap.add_argument("--pin", action="store_true")
    ap.add_argument("--governor", action="store_true")
    ap.add_argument("--quiesce", action="store_true", help="stop non-dev containers")
    ap.add_argument("--restore", action="store_true", help="restart non-dev containers at end")
    ap.add_argument("--jfr", action="store_true")
    ap.add_argument("--no-preseed", action="store_true")
    ap.add_argument("--seed", type=int, default=12345)
    args = ap.parse_args()

    os.makedirs(args.out, exist_ok=True)
    manifest = json.load(open(args.manifest))
    clients = args.clients.split(",")
    cells = select_cells(manifest, args)
    closed = [c for c in cells if c["load_model"] == "closed"]
    opencells = [c for c in cells if c["load_model"] == "open"]
    forks = args.forks if args.forks is not None else (cells[0]["forks"] if cells else 1)
    trials = args.trials if args.trials is not None else (cells[0]["trials"] if cells else 1)

    print("=== orchestrator ===")
    print("manifest=%s tier=%s clients=%s cells=%d (closed=%d open=%d) forks=%d trials=%d"
          % (args.manifest, manifest["meta"]["tier"], clients, len(cells), len(closed), len(opencells), forks, trials))

    setup_rig(args)
    args.license = read_license()
    print("EE license for Java client: %s" % ("loaded (hidden)" if args.license else "NOT FOUND — Java CP will fail"))
    prov = collect_provenance(args, args.hz_client_version)
    prov["java_client_edition"] = "enterprise 5.7.0 (instance ~/hzlib jar; required for CP)"
    prov["license_source"] = "instance ~/hz/hz1.yaml (not logged)"
    json.dump(prov, open(os.path.join(args.out, "provenance.json"), "w"), indent=2)

    if not args.no_preseed:
        preseed(args, cells, args.out)

    rng = random.Random(args.seed)
    records = []

    def do_phase(phase_cells, rate_for):
        for trial in range(trials):
            order = list(phase_cells)
            rng.shuffle(order)  # randomize cell order per trial (§8)
            for ci, cell in enumerate(order):
                for fork in range(forks):
                    # A/B/B/A: alternate which client goes first
                    order_clients = clients if ((ci + fork) % 2 == 0) else list(reversed(clients))
                    tr = rate_for(cell) if rate_for else None
                    if cell["load_model"] == "open" and tr is None:
                        print("  skip open cell (no rate): %s" % cell["id"]); continue
                    for client in order_clients:
                        env = run_one(args, cell, client, fork, trial, args.out, target_rate=tr)
                        if env.get("ok"):
                            records.append((cell["id"], client, fork, trial))

    # Phase 1: closed-loop
    print("--- closed-loop phase (%d cells) ---" % len(closed))
    do_phase(closed, None)

    # Phase 2: resolve open-loop target rates from the closed-loop max throughput
    if opencells:
        group_max = compute_group_max(args.out)
        def rate_for(cell):
            gmax = group_max.get(cell.get("rate_group"))
            if not gmax or not cell.get("rate_frac"):
                return None
            return gmax * cell["rate_frac"]
        print("--- open-loop phase (%d cells; rates from closed max) ---" % len(opencells))
        do_phase(opencells, rate_for)

    restore_rig(args)
    print("=== done: %d successful runs -> %s ===" % (len(records), args.out))


def compute_group_max(out_dir):
    """Per rate_group, the max over C of median closed-loop throughput."""
    from collections import defaultdict
    byg = defaultdict(lambda: defaultdict(list))  # group -> C -> [throughput]
    for fn in os.listdir(out_dir):
        if fn.endswith(".env.json") or not fn.endswith(".json") or fn == "provenance.json":
            continue
        try:
            r = json.load(open(os.path.join(out_dir, fn)))
        except Exception:
            continue
        if r.get("load_model") != "closed":
            continue
        # Reconstruct the manifest's rate_group format exactly from record fields:
        #   {structure}.{op}.{variant}.k{key_type}.v{value_size}.{distribution}
        g = "%s.%s.%s.k%s.v%s.%s" % (
            r["structure"], r["op"], r.get("variant", "default"),
            r["key_type"], r["value_size"], r["distribution"])
        byg[g][r["concurrency"]].append(r["throughput"])
    out = {}
    for g, cmap in byg.items():
        best = 0.0
        for c, thrs in cmap.items():
            thrs = sorted(thrs)
            med = thrs[len(thrs) // 2]
            best = max(best, med)
        out[g] = best
    return out


if __name__ == "__main__":
    main()
