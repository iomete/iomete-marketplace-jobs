# To-do

## Done on this branch (foundation hygiene, pre-POC)
- **Config surface trimmed** to `source` / `target` only. Removed the `copy`
  tuning block (`maxMaps`, `maxAttempts`, `retryDelayMs`); retry is now internal
  with defaults and partitioning is derived from Spark's default parallelism.
  Avoids a future config migration when byte-balanced partitioning lands.
- **Removed cruft:** commented-out HDFS/incremental/bandwidth stubs, inline
  design-question TODOs, and the large commented-out future-test blocks.
  Simplified config parse-error messages.
- **Logging standard applied:** bounded output (no per-file INFO line at scale —
  failures only, at WARN), no decorative banners, `SparkSessionProvider` is the
  sole owner of session lifecycle logs.
- **README** rewritten to the actual POC scope (S3 → S3), with the point-in-time
  consistency caveat documented.
- Confirmed the two "critical" defects in `production-readiness.md` §3.4/§3.5
  (hardcoded `master("local")`, swallowed failures) are **already fixed** in the
  code — the docs were stale.

## Before HDFS/Isilon support
- Run the `spike/s3-to-isilon` validation (see `docs/spike-s3-to-isilon.md`) off
  a clean `main` after this branch merges.

## Can be done later
- Integration test gap: current tests never start Spark. `App`,
  `SparkSessionProvider`, `CopyJobRunner` (the distributed copy) are untested;
  `spark-sql` is on the classpath only for Hadoop helpers.
  - Add `CopyJobRunner` test against a real `local[*]` SparkSession + MinIO
    testcontainer (S3A path).
  - Add real-image test: `spark-submit` the built jar inside
    `iomete/spark:<ver>-<rev>` against MinIO — the only true prod-parity check.
  - Wire these under the existing `integrationTest` task via `@Tag("integration")`
    (task + testcontainers deps already declared, currently unused).
- GitLab pipeline to run unit and integration tests.
- Milestone-1 hardening (atomic copy, verification, byte-balanced partitioning,
  FileSystem caching, idempotent `update`, result manifest). See
  `docs/milestone-1-plan.md`.
