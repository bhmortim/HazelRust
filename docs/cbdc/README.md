# CBDC Production-Readiness — Independent Validation & Remediation

This directory is the version-controlled audit trail for the independent
production-readiness validation of the HazelRust client for a central-bank /
CBDC money path (behind the Patina proxy), and the remediation that followed.

**Validation baseline:** `origin/main` `e6386d2`. **Remediation:** branch `fix/cbdc-remediation`.
**Verdict:** NO-GO (partially remediated; see roadmap for the path to GO).

| Document | What it is |
|---|---|
| [INDEPENDENT_PRODUCTION_READINESS_ASSESSMENT.md](INDEPENDENT_PRODUCTION_READINESS_ASSESSMENT.md) | The verdict report: GO/NO-GO against the agreed bar, criteria, risk register, coverage/gap analysis, evidence appendix. |
| [AUDIT_DIGEST.md](AUDIT_DIGEST.md) | All 63 adversarially-verified static findings (11 Critical, 24 High, …) with file:line, impact, and verdict. |
| [CONSTANTS_VERIFICATION.md](CONSTANTS_VERIFICATION.md) | Full 219-constant message-type table vs upstream Hazelcast (29 wrong). |
| [CBDC_REMEDIATION_PLAN.md](CBDC_REMEDIATION_PLAN.md) | The remediation plan with per-item (R1–R10) status. |
| [REMEDIATION_RESULTS.md](REMEDIATION_RESULTS.md) | What was fixed and live-verified; before/after test evidence; honest course-corrections. |
| [PRODUCTION_READINESS_ROADMAP.md](PRODUCTION_READINESS_ROADMAP.md) | **The forward work list** to reach GO, prioritized P0→P3 with exit criteria. |
| [EVIDENCE_LOG.md](EVIDENCE_LOG.md) | Chronological evidence (commands, environment, raw outputs). |

> Note: `../CBDC_READINESS_ASSESSMENT.md` is the project's own pre-existing
> assessment; the documents here are the **independent** validation and are kept separate.

## Reproduce (live EE cluster required)
```
git checkout fix/cbdc-remediation
cargo fmt --all -- --check          # clean
cargo clippy --workspace --all-targets   # 0 errors
cargo build --workspace             # ok
cargo nextest run --workspace       # 2183 passed, 0 failed
CLUSTER_ADDRESS=127.0.0.1:5701 cargo nextest run -p hazelcast-client \
  --run-ignored ignored-only --test-threads 1 --no-fail-fast   # 140 passed, 18 failed (8 XA honest, 10 java_parity framing/infra)
```
