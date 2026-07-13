# Milestone-1 Plan: S3 → S3, Production-Ready

## Scope

Make the **S3 → S3** copy pipeline genuinely production-ready: distributed,
atomic, verified, idempotent, and observable, gated by end-to-end integration
tests. S3 → HDFS/Isilon is explicitly **milestone-2** (see
`production-readiness.md` §10 and `spike-s3-to-isilon.md`).

This plan is the roadmap; each PR gets its own detailed design/grill session
before implementation. Deliverable-level details (temp-orphan cleanup, retry
backoff shape, manifest format/location, partitioner internals) are intentionally
deferred to those sessions.

## Definition of Done (release gate)

- **Correctness (hard gate, non-negotiable):** zero silent data loss. Every file
  either lands atomically (temp → verify length → rename) or is reported failed
  in the manifest. Any unrecovered failure → **non-zero exit**. (Milestone-1 is
  strict-only; the `ignoreFailures` best-effort opt-in is milestone-2.)
- **Representative dataset:** Iceberg-table-shaped — mixed file sizes (many small
  metadata/manifest files + a few large data files), on the order of **hundreds
  of thousands of objects, up to a few TB**. This scale is comfortably within
  driver-side listing, which is why parallel listing is PR8, not core.
- **Throughput:** no absolute MB/s target. The operational gate is **anti-straggler**
  — no single large file dominates wall-clock (partitioning demonstrably
  balances).
- **Green integration suite (per-PR MinIO tests, as the gate):** full copy;
  idempotent re-run (`update` skips); induced mid-copy failure leaves **no
  corrupt target**; retry exhaustion → reported failure + non-zero exit; manifest
  correctness.

## Deliverables (one PR each, in order)

Each PR is independently reviewable and leaves the tool in a better state. From
PR3 onward, each carries its own integration tests against the harness from PR2 —
there is no monolithic "add all the tests" PR at the end.

### PR1 — Critical safety — **DONE**
- Remove hardcoded `master("local")` from `SparkSessionProvider`; master comes
  from the Spark submit environment. **Done:** uses `SparkSession.builder().orCreate`.
- Restore non-zero exit on failure in `App.main` so a failed run never reports
  success to the scheduler. **Done:** `App.main` rethrows and
  `check(summary.failureCount == 0)` fails the run; the JVM exits non-zero via the
  propagated exception (no `exitProcess` needed).
- Verified by existing unit tests (this PR does not require the harness).

### PR2 — Integration harness
- Stand up MinIO + Testcontainers + a local Spark session in `src/test`, wiring
  the existing `integrationTest` Gradle task.
- Seed one happy-path full-copy test against the **current** copier, establishing
  the living gate that every subsequent PR extends.

### PR3 — Atomic copy + verification
- Port SparkDistCP's `performCopy` into `FileCopier`: write to a temp path keyed
  by task-attempt id → verify written length equals source length → rename into
  place. Preserve the existing retry loop.
- Resolve here (deliverable-level): temp-file orphan cleanup policy, and retry
  backoff (exponential + jitter vs the current fixed delay).
- Tests: induced mid-copy failure leaves no corrupt target; retry exhaustion →
  reported failure + non-zero exit.

### PR4 — Byte-balanced partitioning
- Replace Spark-default `parallelize(filePaths)` in `CopyJobRunner` with batching
  by `maxBytesPerTask` / `maxFilesPerTask` plus a custom `CopyPartitioner`.
  (`maxMaps` was already removed in the pre-POC cleanup, so no config migration is
  needed here — this PR only introduces the new tuning knobs.)
- The per-element copy contract stays intact, so `FileCopier`'s signature is
  untouched.
- Test: balanced partitions / no straggler under skewed file sizes.

### PR5 — FileSystem caching
- Refactor `FileCopier` to `mapPartitions` with a per-task FileSystem cache keyed
  by URI authority, eliminating the per-file FS/Configuration churn in
  `FileCopier.copySingleFile` (a fresh FileSystem + Configuration is currently
  built per file and per retry attempt).
- Test: FileSystem instances reused within a task.

### PR6 — Idempotent `update`
- Add skip-if-identical inside the atomic copy path. For S3 → S3 (same scheme),
  decide by **length** (safe, simple); explicitly do not compare cross-scheme
  checksums (that policy matters in milestone-2).
- Test: a second run skips already-identical files.

### PR7 — Result manifest
- Emit a machine-readable JSON manifest at a known destination location, listing
  every file with outcome, bytes, attempts, and error — enabling reconciliation
  and alerting.
- Document the **point-in-time consistency caveat** in the README ("source must
  be quiescent, or the backup may be inconsistent").
- Resolve here (deliverable-level): resume semantics — for milestone-1, a re-run
  relies on `update`-skip, not manifest-driven resume (state this explicitly).
- Test: manifest correctness against a known copy.

### PR8 — Parallel/distributed listing *(later, un-bundled)*
- Thread the source listing (`numListStatusThreads`) in `FileLister`, separate
  from the manifest. Lower priority; land after the core is green. Kept in the
  plan by explicit decision even though the milestone-1 scale ceiling does not
  strictly require it.

## Order rationale

Correctness (PR3) lands before performance (PR4/PR5) before idempotency (PR6).
The two `FileCopier`-internal PRs (PR3, PR6) are separated by the two structural
PRs (PR4, PR5) so sequential work does not collide on the same lines.

## Resolved decisions (locked for milestone-1)

- **Two milestones.** Milestone-1 = S3 → S3; milestone-2 = S3 → HDFS/Isilon.
- **PR1 already landed** and the config surface was trimmed to `source`/`target`
  only (the `copy` tuning block was removed) during the pre-POC cleanup. Retry is
  internal with defaults; partitioning derives from Spark default parallelism
  until PR4.
- **`${VAR}` substitution** is handled **platform-side** at spark-operator CRD
  deploy time. No substitution code is needed; the README is accurate.
  (Closes `production-readiness.md` §3.1 / §11 open question.)
- **Integration tests** are woven per-PR against an early harness (PR2); there is
  no monolithic end-of-milestone test suite.
- **Strict-only** failure handling; `ignoreFailures` deferred to milestone-2.
- **Parallel listing** is a real deliverable (PR8) but decoupled from the manifest
  and lower priority.
- **Empty directories** are a non-issue for S3 → S3 (no real directories); the
  decision is deferred to milestone-2 (HDFS target).

## Deferred backlog (post-milestone-1)

S3 → HDFS/Isilon (milestone-2), preserve-metadata, bandwidth throttling,
delete-at-destination, snapshots/diff, HDFS HA/Kerberos, job-level atomic commit,
Iceberg-awareness. See `production-readiness.md` §8.

## Parallelization with the Isilon spike

The `spike/s3-to-isilon` workstream (see `spike-s3-to-isilon.md`) touches the
config/sealed-class branches (`Config.kt`, `HadoopConfigBuilder`, `PathResolver`,
`ConfigValidator`, `ConfigUtils`). Milestone-1 touches the copy engine
(`FileCopier`, `CopyJobRunner`, `App`, `SparkSessionProvider`). Overlap is near
zero. Because the spike branch **never merges to `main`**, milestone-1 stays
S3-only and inherits no HDFS `when` branches. The two can proceed in parallel.

## Progress checklist

- [x] PR1 — Critical safety
- [ ] PR2 — Integration harness
- [ ] PR3 — Atomic copy + verification
- [ ] PR4 — Byte-balanced partitioning
- [ ] PR5 — FileSystem caching
- [ ] PR6 — Idempotent `update`
- [ ] PR7 — Result manifest
- [ ] PR8 — Parallel/distributed listing *(later)*
