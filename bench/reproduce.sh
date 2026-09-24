#!/usr/bin/env bash
# Reproducibility driver: re-run a named tier end-to-end (gen -> build -> run ->
# analyze) on the AWS instance. Usage:
#   bench/reproduce.sh <tier> [out_dir] [extra run.py args...]
# Examples:
#   bench/reproduce.sh t0 benchmarks/t0_repro
#   bench/reproduce.sh headline benchmarks/headline --open
set -euo pipefail
cd "$(dirname "$0")/.."

TIER="${1:?usage: reproduce.sh <tier> [out_dir] [run.py args...]}"
OUT="${2:-benchmarks/${TIER}}"
shift || true; shift || true

MANIFEST="bench/manifest.${TIER}.json"
echo "[1/4] generate manifest -> ${MANIFEST}"
python3 bench/gen_manifest.py --tier "${TIER}" --out "${MANIFEST}"

echo "[2/4] build Rust harness (--release)"
RUSTC_VERSION="$(rustc --version)" cargo build --release -p hazelcast-client-bench

echo "[3/4] build Java harness (EE client shaded JAR)"
( cd bench-java && ./gradlew --no-daemon --console=plain shadowJar )

echo "[4/4] orchestrate + analyze"
COMMIT="$(git rev-parse --short HEAD)"
python3 bench/run.py --manifest "${MANIFEST}" --out "${OUT}" \
    --clients rust,java --commit "${COMMIT}" --pin --quiesce "$@"
python3 bench/analyze.py --in "${OUT}"
echo "done -> ${OUT}/BENCHMARK_REPORT.md"
