# Benchmark addendum — backup-ack-to-client optimization (post-optimization results)

_Companion to `BENCHMARK_REPORT.md` (the initial run) and the full Word report
`HazelRust_vs_Java_Benchmark_Report.docx`. Charts: `bench_charts/`._

## What changed

The initial headline run showed HazelRust ~20–30% slower than the Java client on
IMap writes (put/set/mixed) at every concurrency, while reads were
competitive-to-winning. A controlled experiment (disabling the Java client's
`backupAckToClientEnabled`) proved the **entire** write deficit was one missing
feature: **backup-ack-to-client**. It was implemented (commit `5a85ea2`):

- Set `BACKUP_AWARE` (flag `1<<8`) on mutating partition requests → the partition
  owner replies **early**, overlapping its sync backups.
- Register a `ClientLocalBackupListener` (req 3840) per member connection, as the
  Java client does.
- Complete the invocation on the owner's early reply — matching the Java client,
  whose `ClientInvocation.shouldCompleteWithoutBackups()` returns `true`
  unconditionally (verified by decompiling the EE 5.7 jar). Acks are bookkeeping;
  they do not gate completion.

## Post-optimization, matched (both clients backup-ack ON, 3 forks × 2 trials, 0 errors)

| Operation | C | Rust ops/s | Java ops/s | Rust/Java | Result |
|---|--:|--:|--:|--:|---|
| IMap get (read) | 64 | 272,124 | 279,042 | 0.98 | even (noise) |
| IMap put | 1 | 16,359 | 10,723 | **1.53** | Rust ×1.53 |
| IMap put | 64 | 151,700 | 118,470 | **1.28** | Rust ×1.28 |
| IMap put | 256 | 236,060 | 188,530 | **1.25** | Rust ×1.25 |
| IMap set | 64 | 150,605 | 119,236 | **1.26** | Rust ×1.26 |
| Mixed 50/50 | 1 | 18,240 | 13,422 | **1.36** | Rust ×1.36 |
| Mixed 50/50 | 64 | 184,873 | 160,022 | **1.16** | Rust ×1.16 |
| Mixed 50/50 | 256 | 269,723 | 242,785 | **1.11** | Rust ×1.11 |
| Mixed RMW | 64 | 94,032 | 82,534 | **1.14** | Rust ×1.14 |

**HazelRust now beats the Java client on every write/mixed cell**, with reads even.
The win comes from the early-reply mechanism plus HazelRust being the leaner client
(it already matched Java with the feature off).

## Durability trade-off (why the library default stays OFF)

Completing on the early reply returns success **before the backup is confirmed**, so
an owner crash in that window can lose the write (weaker RPO). Because this client
targets a CBDC money path that relies on **RPO-0**, the HazelRust library default for
`backup_ack_to_client_enabled` is **OFF** (owner waits for backups = strong
durability, byte-for-byte the prior verified behavior) — an opt-in, and a
safer-by-default posture than the Java client (default ON). The benchmark enables it
on **both** clients for a fair comparison.

## Fairness correction

The original headline run inadvertently compared Java-default-ON against
HazelRust-OFF — the methodology's "backup-ack-to-client matched" was not enforced.
The matched (both-ON) numbers above correct for it. Unit tests stayed green
(hazelcast-client 1489/0, hazelcast-core 306/0).
