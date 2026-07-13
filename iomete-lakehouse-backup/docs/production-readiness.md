# IOMETE Lakehouse Backup — Architecture, Gap Analysis, and Production Roadmap

## 1. Purpose of this document

This document establishes a shared understanding of the `iomete-lakehouse-backup`
job in its current form, evaluates it honestly against a mature reference
implementation (Cox Automotive's [SparkDistCP](https://github.com/CoxAutomotiveDataSolutions/spark-distcp)),
and defines the smallest credible path to a production-ready "DistCP-on-Spark"
copy engine.

The end goal is a production-grade distributed copy tool — functionally a
counterpart to Hadoop DistCP, but executing on Apache Spark rather than
MapReduce — that will later serve as the foundation for Iceberg-aware backup and
restore. Two pipelines are in scope for the first production milestone:

- **S3 → S3** (object store to object store), and
- **S3 → HDFS** where the HDFS target is **Dell Isilon (OneFS)** using simple
  authentication.

This document deliberately scopes that first milestone to a **minimal, correct
feature set** across both pipelines, and records the richer feature backlog
separately so that we can grow into it without blocking the initial release. The
HDFS/Isilon target backend design is detailed in §10.

The guiding principle throughout is **correctness first**: a backup tool that
silently loses or corrupts data is worse than no tool at all.

---

## 2. What the tool does today

The job is a Kotlin/Gradle Spark application that copies every file under a
source object-store prefix to a target object-store prefix. The code as it
stands implements S3-to-S3 only; the HDFS backend is stubbed (see §10 for the
required implementation). The end-to-end flow, driven by `App.kt`, is:

1. **Parse** the JSON configuration file (path from `args[0]`, defaulting to
   `/etc/configs/application.json`).
2. **Validate** the configuration and **log a secret-redacted** copy of it.
3. **Initialise** a Spark session.
4. **Enumerate** every source file recursively on the driver.
5. **Distribute** the copy across executors: file paths are parallelised into an
   RDD and each file is copied independently.
6. **Aggregate** per-file results into a summary (success/failure counts, bytes
   copied) and log them.

### Package map

| Package | Responsibility |
|---|---|
| `config/` | Config model (`Config.kt`), parsing (`ConfigParser`), validation (`ConfigValidator`), secret redaction (`ConfigUtils`) |
| `fs/` | Recursive source enumeration (`FileLister`) producing serialisable `FileEntry` records |
| `copy/` | The copy engine: `CopyJobRunner` (orchestration), `FileCopier` (per-file copy on executors), `HadoopConfigBuilder` (S3A config), `PathResolver` (source→target path mapping), result types |
| root | `App` (entry point), `SparkSessionProvider` (Spark session lifecycle) |

### Configuration shape

The tool is configured declaratively via JSON, which suits a scheduled Spark job
mounted by the IOMETE platform:

```json
{
  "source": { "type": "s3", "bucket": "...", "prefix": "...", "endpoint": "...",
              "pathStyleAccess": true, "accessKey": "...", "secretKey": "..." },
  "target": { "type": "s3", "bucket": "...", "prefix": "...", "endpoint": "...",
              "pathStyleAccess": true, "accessKey": "...", "secretKey": "..." },
  "copy":   { "options": { "maxMaps": 20, "maxAttempts": 3, "retryDelayMs": 1000 } }
}
```

The `StorageConfig` type is a sealed class with Jackson polymorphism on `"type"`,
so additional backends (HDFS is stubbed) can be added without disturbing the
existing model.

---

## 3. What we have — component-by-component assessment

This section records, for each part of the current codebase, what is sound and
what is wrong or missing. "Right" means it should be kept; "Wrong" means it is a
defect that must be fixed before production.

### 3.1 Configuration layer — **mostly right**

- **Right:** JSON config with a sealed-class storage model, tolerant parsing
  (unknown fields ignored), explicit validation returning structured errors, and
  secret redaction before logging. This is a genuinely good foundation and is
  *better* than SparkDistCP, which is CLI-argument driven and has no credential
  model.
- **Right:** **Separate source and target credentials.** `CopyJobRunner` builds
  independent Hadoop config maps for each end. This is essential for real backup
  scenarios (production account → backup account) and is a capability SparkDistCP
  lacks entirely.
- **Resolved:** The README documents `${VAR_NAME}` placeholder substitution for
  secrets. This is performed **platform-side** by the spark-operator at CRD deploy
  time, so no substitution logic is needed in the parser (see
  `milestone-1-plan.md`).
- **Update (pre-POC cleanup):** the `copy` tuning block (`maxMaps`,
  `maxAttempts`, `retryDelayMs`) was removed from the user config surface. Retry
  is internal with defaults; the config now takes `source`/`target` only.

### 3.2 Source enumeration (`FileLister`) — **works, but limited**

- **Right:** Uses Hadoop's recursive `listFiles`, exposed as a lazy sequence of
  serialisable `FileEntry` records.
- **Wrong:** Listing runs **single-threaded on the driver**. For large
  namespaces (millions of objects) this is slow and memory-bound. There is no
  parallelism control (SparkDistCP exposes `numListStatusThreads`).

### 3.3 Copy engine (`FileCopier`, `CopyJobRunner`) — **the weakest area**

- **Right:** Per-file **retry** with configurable `maxAttempts` and
  `retryDelayMs`. This is a real strength — SparkDistCP has *no* per-file retry.
- **Right:** Correct target-path derivation (`PathResolver`) and parent-directory
  creation.
- **Wrong (critical): no write atomicity.** `FileCopier` calls
  `FileUtil.copy(..., overwrite = true, ...)` directly onto the **final target
  path**. If an executor dies mid-copy, the destination is left with a
  truncated file that *looks present*. There is no temp-file-then-rename staging.
- **Wrong (critical): no post-copy verification.** The reported `bytesCopied`
  comes from the *source* `FileStatus`; the tool never confirms the target was
  written completely.
- **Wrong (high): naive partitioning.** `jsc.parallelize(filePaths, maxMaps)`
  distributes by file *count*, not bytes. A single large file colocated with many
  small ones creates severe skew — one task does most of the work.
- **Wrong (medium): FileSystem churn.** In `FileCopier.copySingleFile`, a new
  source *and* target `FileSystem` — and a new Hadoop `Configuration` — is built
  **per file** (and per retry attempt). SparkDistCP reuses FileSystem objects per
  task via a cache. Fixed in milestone-1 PR5.
- **Wrong (medium): no idempotency.** `overwrite = true` unconditionally, so
  every run re-copies the entire dataset. There is no skip-if-identical
  (`update`) semantics.

### 3.4 Spark session (`SparkSessionProvider`) — **fixed**

- **Fixed (was critical): `master("local")` is no longer hardcoded.**
  `SparkSessionProvider` uses `SparkSession.builder().orCreate`, so the master
  comes from the Spark submit environment and work distributes to executors.

### 3.5 Entry point (`App`) — **mostly fixed**

- **Fixed (was critical): failures propagate.** `App.main` rethrows on error and
  `check(summary.failureCount == 0)` fails the run, so the JVM exits non-zero via
  the propagated exception. A failed backup is no longer reported as success.
- **Wrong (medium): no machine-readable result output.** Results are logged only.
  There is no manifest artifact for reconciliation, alerting, or resume.

### 3.6 Test posture — **thin, and the critical path is untested**

- Unit tests exist for config parsing/validation/redaction, path resolution,
  Hadoop config building, file listing, and the copier — but the copier tests run
  against the **local filesystem with mocked statics**, not real object storage.
- Despite Testcontainers/MinIO being declared as dependencies and an
  `integrationTest` Gradle task existing, **there are no integration tests** that
  exercise the S3 path end-to-end. There are no tests for `CopyJobRunner`,
  `App`, or `SparkSessionProvider`.
- Net: the distributed copy path — the part most likely to lose data — has no
  automated proof that it works.

### Summary scorecard

| Area | Status | Severity if wrong |
|---|---|---|
| Separate src/dst credentials | ✅ right | — |
| JSON config + validation + redaction | ✅ right | — |
| Per-file retry | ✅ right | — |
| Distributed execution (`master`) | ✅ fixed | Critical |
| Write atomicity | ❌ wrong | Critical |
| Post-copy verification | ❌ wrong | Critical |
| Failure propagation / exit code | ✅ fixed | Critical |
| Partition balancing | ❌ wrong | High |
| Idempotent `update` | ❌ missing | High |
| Parallel / distributed listing | ❌ missing | Medium |
| FileSystem reuse | ❌ wrong | Medium |
| Result manifest | ❌ missing | Medium |
| Integration tests on S3 path | ❌ missing | High |

---

## 4. Reference implementation: what to take from SparkDistCP

SparkDistCP is a Scala/sbt reimplementation of Hadoop DistCP on Spark. It is more
feature-complete than our current code but is **unmaintained** (last release
January 2022, built against Spark 3.2) and its README explicitly warns it may
cause accidental data loss. We therefore treat it as an **executable
specification and a source of tested algorithms**, not as a base to fork or a
dependency to adopt (see §6 for the rationale).

The parts worth taking, and porting faithfully into our Kotlin engine, are:

### 4.1 Atomic per-file copy (`CopyUtils.performCopy`)

SparkDistCP copies to a temporary path
`.sparkdistcp.<taskAttemptId>.<filename>`, then:

1. verifies the written length equals the source length,
2. optionally removes an existing destination file (for overwrite/update),
3. renames the temp file into place, failing if the destination already exists.

This directly fixes our two critical copy defects (no atomicity, no
verification). The temp-file naming keyed on task-attempt id also prevents
collisions between speculative/retried Spark task attempts.

### 4.2 Byte-balanced partitioning (`batchAndPartitionFiles` + `CopyPartitioner`)

Instead of partitioning by file count, SparkDistCP batches files within
partitions until either `maxFilesPerTask` (default 1000) or `maxBytesPerTask`
(default 1 GiB) is reached, then uses a custom `CopyPartitioner` to keep each
batch in its own Spark partition. This eliminates large-file skew and replaces
our `maxMaps`-based scheme. We should adopt `maxBytesPerTask` / `maxFilesPerTask`
as the tuning knobs.

### 4.3 FileSystem caching (`FileSystemObjectCacher`)

A per-partition cache keyed by filesystem URI authority, so source and
destination FileSystem objects are created once per task rather than once per
file. This fixes our FileSystem-churn defect.

### 4.4 Update/skip semantics (`CopyUtils.filesAreIdentical`)

Compares length and (when available) checksum to skip identical files. **Take the
structure, but not the bug:** SparkDistCP's fallback treats a missing checksum as
"identical" (`getOrElse(true)`), which across heterogeneous filesystems (e.g.
HDFS vs S3, whose checksums are incompatible/absent) causes it to **silently skip
different files of equal length**. Because **S3 → HDFS (Isilon) is an in-scope
pipeline**, this is not a theoretical edge case — it is on our primary path. Our
port must make the cross-filesystem checksum policy explicit and safe (see §5 and
§10).

### 4.5 What we intentionally do *not* take (for the first milestone)

Snapshot/`-diff` sync, `-preserve` (permissions/owner/ACL/xattr), bandwidth
throttling, `-delete` (which in SparkDistCP bypasses trash — dangerous),
`-append`, and job-level atomic-commit. These are deferred to §8.

---

## 5. What "production ready" means for the first milestone

We define production readiness for a **feature-limited** first release as the
intersection of correctness, safety, and operability — not feature parity with
Hadoop DistCP. The required capabilities are:

**Correctness & safety (must-have)**

- Executes as a genuinely distributed Spark job (no hardcoded `local` master).
- Supports both target pipelines: **S3 → S3** and **S3 → HDFS (Isilon, simple
  auth)** — see §10 for the backend design.
- Atomic per-file copy: write to temp, verify length, rename into place. (Rename
  is atomic and cheap on HDFS/Isilon, so this fits the HDFS target especially
  well.)
- Explicit checksum/verification policy, safe across heterogeneous filesystems —
  **never compare checksums across schemes** (S3A and HDFS checksums are
  incompatible), and never silently skip a file whose content may differ. Verify
  by length across schemes; checksums may be used only within the same scheme.
- Per-file retry (already present) preserved through the new copy path.
- Failures propagate: non-zero exit on any unrecovered failure, honouring an
  explicit "ignore errors / best effort" flag when the operator opts in.

**Operability (must-have)**

- Byte-balanced partitioning so large files do not create stragglers.
- A machine-readable result manifest written to a known location, listing every
  file with outcome, bytes, attempts, and error — enabling reconciliation,
  alerting, and (future) resume.
- Idempotent re-runs: `update` mode skips files already identical at the target.
- Separate source/target credentials (already present).

**Quality gates (must-have)**

- Integration tests against MinIO/S3 (Testcontainers) covering: full copy,
  re-run/idempotency, mid-copy failure leaving no corrupt target, retry
  exhaustion, and manifest correctness.

**Explicitly out of scope for the first milestone**

- HDFS **HA and Kerberos** authentication (simple auth only for the first
  release), snapshots/incremental-by-diff, preserve-metadata, bandwidth limiting,
  delete-at-destination, append, Iceberg-awareness. All are recorded in §8 and
  can be added later without reworking the core.

---

## 6. Recommendation: port, do not fork

**Do not extend or depend on SparkDistCP. Port its proven copy core into our
Kotlin repository and keep our own configuration/credential foundation.**

Rationale:

1. **Language and build fit.** SparkDistCP is Scala + sbt; our stack is Kotlin +
   Gradle. Extending it means either maintaining Scala the team does not
   primarily write, or straddling two languages for every change — including the
   real end goal, Iceberg-awareness.
2. **Adopting it means maintaining an abandoned upstream.** It has not been
   released since 2022 and targets Spark 3.2; we run 3.5. "Reusing" it would
   still require us to upgrade and re-test it — with none of the benefit of a
   living dependency.
3. **The dependency cannot reach the end goal.** Iceberg-aware backup/restore is
   a layer *above* raw copy and is net-new code regardless of the copier. We do
   not want our product's core value living as a wrapper around a frozen jar with
   a single-credential model we have already outgrown.
4. **The valuable part is small and portable.** SparkDistCP's worth is ~3–4 files
   of tested copy semantics, not its CLI scaffolding (which we would discard).
   Those algorithms — and, crucially, **their tests** — translate cleanly to
   Kotlin. Porting the tests alongside the logic is what makes reimplementation
   safe rather than a fresh source of data-loss bugs.

The single scenario that would justify forking SparkDistCP instead is a goal of
*full* Hadoop DistCP parity (snapshots, preserve, all path-behaviour edge cases)
combined with a willingness to commit to Scala long-term. We are pursuing
neither, so porting the minimal core is the better choice.

### Porting map

| Take from SparkDistCP | Port into our code | Fixes |
|---|---|---|
| `CopyUtils.performCopy` (temp → verify → rename) | `copy/FileCopier.kt` | atomicity, verification |
| `batchAndPartitionFiles` + `CopyPartitioner` | `copy/CopyJobRunner.kt` (+ new partitioner) | large-file skew |
| `FileSystemObjectCacher` | new `copy/FileSystemCache.kt`, used in `FileCopier` | FileSystem churn |
| `filesAreIdentical` (with safe checksum policy) | `copy/FileCopier.kt` | idempotent `update` |
| `TestCopyUtils`, `TestCopyPartitioner`, `TestFileListUtils` | corresponding Kotlin tests | proof of correctness |

Note SparkDistCP acquires FileSystems with the 2-argument `FileSystem.get(uri,
conf)` and a single Hadoop configuration for both ends. Our HDFS/Isilon target
needs a **user identity** and **separate per-end configuration**, so we do not
port its FS-acquisition path verbatim — see §10.

Keep unchanged: `config/*`, separate-credential handling, secret redaction, retry
loop.

---

## 7. Delivery plan (incremental)

Ordered so each step is independently reviewable and leaves the tool in a
better state. Each is a separate commit/PR.

1. **Fix critical safety defects.** Remove hardcoded `master("local")`; restore
   non-zero exit on failure in `App`. Smallest, highest-value change.
2. **Atomic copy + verification.** Port `performCopy` (temp-file, length check,
   rename) into `FileCopier`, preserving the existing retry loop. Add integration
   tests proving no corrupt target on induced mid-copy failure.
3. **Byte-balanced partitioning.** Replace the `maxMaps` count-based RDD with
   `maxBytesPerTask`/`maxFilesPerTask` batching and a custom partitioner.
4. **FileSystem caching.** Introduce a per-task FS cache; stop creating
   FileSystems per file.
5. **HDFS/Isilon target backend.** Implement `HdfsConfig`, its validation, config
   builder, and path resolver; thread a `user` through FileSystem acquisition for
   simple auth (§10). Add integration coverage against a Hadoop/HDFS container.
6. **Idempotent `update`.** Add length+checksum skip with an explicit, safe
   cross-filesystem policy (length-only across schemes).
7. **Result manifest + parallel listing.** Emit a JSON manifest of per-file
   outcomes at the destination; thread the source listing.
8. **Integration test suite.** MinIO-backed (S3) and HDFS-container end-to-end
   coverage as a gate, including the S3 → HDFS cross-scheme path.

Steps 1 and the safety portions of 2 are the minimum to stop the tool from
silently losing or corrupting data. Step 5 unblocks the second in-scope pipeline
(S3 → Isilon).

---

## 8. Deferred feature backlog (post-milestone)

Recorded so the core design leaves room for them; none are required for the first
production release.

- **HDFS backend** — the storage model and path/config builders are already
  structured for it (stubbed with TODOs).
- **Incremental by snapshot/diff** — cheap delta sync instead of full re-list.
  For the Iceberg end goal this likely becomes *manifest-driven* incremental
  (copy only files referenced by new snapshots) rather than HDFS snapshots.
- **Preserve metadata** — permissions, owner/group, ACLs, xattrs, timestamps.
- **Bandwidth throttling** — protect shared clusters and source systems.
- **Delete-at-destination** — mirror deletions; must move to trash, not hard
  delete (unlike SparkDistCP).
- **Job-level atomic commit** — stage the whole run and swap atomically.
- **Iceberg-aware backup/restore** — the actual product goal: snapshot-consistent
  table backup, metadata rewrite/repoint on restore, catalog registration. This
  is net-new code layered on top of the copy engine above.

---

## 9. Summary

The current code is not yet production-ready — three defects (non-distributed
execution, non-atomic copy, and swallowed failures) make it unsafe as a backup
engine today. However, its configuration model, separate-credential handling,
secret redaction, and per-file retry are a *better foundation* for a backup
product than SparkDistCP offers. The right path is therefore to **keep our
skeleton and transplant SparkDistCP's proven copy core (and its tests)** rather
than fork an abandoned Scala codebase. Scoping the first release to a minimal,
correct feature set — distributed, atomic, verified, idempotent, observable
copy — across the two in-scope pipelines (**S3 → S3** and **S3 → HDFS/Isilon**)
puts a genuinely production-ready DistCP-on-Spark tool within a handful of
focused, reviewable changes, while leaving a clear runway to the richer feature
set and the Iceberg-aware end goal.

---

## 10. Target backend design: HDFS / Isilon (Dell OneFS)

The second in-scope pipeline copies from S3 to an HDFS target backed by **Dell
Isilon (OneFS)**. Isilon exposes a standard HDFS interface, so no special client
is required — every Hadoop `FileSystem` operation the copy engine already uses
(`listFiles`, `mkdirs`, `create`, `getFileStatus`, `rename`, `delete`) works
against it unchanged. A minimal working client (provided as reference) connects
with simple authentication as follows:

```java
Configuration conf = new Configuration();
conf.set("hadoop.security.authentication", "simple");
conf.set("fs.defaultFS", "hdfs://durdsonddl03h-d0.onefs.dell.com:8020");
FileSystem fs = FileSystem.get(new URI(hdfsUri), conf, user); // 3-arg: carries the user identity
```

Four design consequences follow, and they drive the backend implementation.

### 10.1 FileSystem acquisition must carry a user

This is the one genuine departure from the S3 path. S3A authenticates with an
access/secret key pair and needs no user identity, so the current code uses the
2-argument `FileSystem.newInstance(uri, conf)`. Isilon simple auth instead
derives identity from the **connecting user**, which determines file ownership
and permission checks, so the target FileSystem must be acquired with the
3-argument form `FileSystem.newInstance(uri, conf, user)` (or equivalently
`UserGroupInformation.createRemoteUser(user).doAs { … }`).

The copy engine must therefore thread an optional `user` alongside the per-end
Hadoop configuration map that is shipped to executors. Concretely, `FileCopier`
(and the FileSystem cache it will use) gains an optional `sourceUser` /
`targetUser`; for S3 ends these are null and behaviour is unchanged.

### 10.2 Configuration model

Add an `HdfsConfig` variant to the sealed `StorageConfig` and register it in the
Jackson `@JsonSubTypes` under the name `hdfs`. For the first release (simple auth
only) the fields are:

```kotlin
data class HdfsConfig(
    val namenode: String,             // e.g. "durdsonddl03h-d0.onefs.dell.com:8020"
    val path: String = "",
    val authentication: String = "simple",  // "kerberos" deferred (§8)
    val user: String? = null,         // simple-auth user; defaults to the process user
) : StorageConfig()
```

HA (nameservice + multiple namenodes) and Kerberos (principal + keytab) fields
are deliberately omitted now and added later on this same type (§8).

### 10.3 Config builder and path resolver

- **`HadoopConfigBuilder.buildHdfsConfigMap`** produces exactly the two keys the
  reference client sets: `fs.defaultFS = hdfs://<namenode>` and
  `hadoop.security.authentication = <authentication>`. (HA adds `dfs.nameservices`
  and related keys later.)
- **`PathResolver.resolveHdfsRoot`** yields `hdfs://<namenode>/<path>` with the
  path trimmed of stray slashes, mirroring the existing S3 root resolution.
- **`ConfigValidator.validateHdfsConfig`** requires a non-blank `namenode`,
  a supported `authentication` value, and (for simple auth) tolerates a null
  `user` by falling back to the process user.

### 10.4 Cross-scheme correctness (S3 → HDFS)

This pipeline is heterogeneous, which makes the verification policy of §5
non-negotiable rather than a nicety:

- **Length check** works across schemes and is the primary integrity guard after
  each copy.
- **Checksums must not be compared across schemes.** S3A and HDFS use different,
  incompatible checksum algorithms; comparing them (or treating an absent one as
  "identical") is exactly the SparkDistCP silent-skip bug. For S3 → HDFS,
  idempotent `update` therefore decides by length only, and copy verification
  relies on the temp-file length equalling the source length before the rename.

### 10.5 Why the atomic-copy design fits Isilon well

The temp-file → verify → rename commit we are porting from SparkDistCP is a
*better* fit for the HDFS target than for S3: on HDFS/Isilon, `rename` is a real
atomic metadata operation (cheap, no data movement), whereas on S3 a rename is a
non-atomic copy-plus-delete. So the same commit path gives genuine atomicity on
the Isilon side and best-effort atomicity on the S3 side, with no special-casing.

### 10.6 Operational notes

- **Network reachability.** Because the copy runs distributed, **every Spark
  executor** (not just the driver) must reach the Isilon namenode on its RPC port
  (`:8020` in the reference) and the OneFS data services. This is a deployment
  prerequisite to validate early.
- **Datanode hostname resolution.** Connecting to Isilon HDFS from executors that
  live outside the OneFS network commonly requires
  `dfs.client.use.datanode.hostname = true` (so clients address datanodes by
  hostname rather than internal IP). The two keys in §10.3 are the minimum; this
  one is frequently also needed in practice and must be verified early, since the
  symptom is confusing connect/read timeouts to datanodes rather than a clear
  auth error. If required, `buildHdfsConfigMap` should set it (or expose it as a
  passthrough config).
- **Ownership.** Under simple auth, target files are owned by the configured
  `user`; we do not preserve source ownership in the first release (see
  preserve-metadata, §8).
- **Testing.** Add an HDFS-container integration test (a Hadoop image or
  equivalent) covering the S3 → HDFS cross-scheme copy, re-run idempotency by
  length, and atomic-rename behaviour on induced mid-copy failure.

---

## 11. Known gaps and open questions

These are recognised limitations and unresolved questions that are **not**
addressed by the design above. They are recorded so a fresh session inherits them
rather than rediscovering them. Each is tagged with when it must be resolved.

### Correctness

- **Point-in-time consistency (before first release).** Raw file copy has no
  consistency guarantee: copying a dataset that is being mutated produces an
  inconsistent backup. The Iceberg-aware layer will solve this via snapshot
  pinning, but the interim S3 → S3 / S3 → HDFS releases must at minimum document
  the caveat (“source must be quiescent, or backups may be inconsistent”) and
  ideally record the source snapshot/version copied.
- **Empty directories are dropped (design decision needed).** `FileLister` uses
  `listFiles(recursive = true)`, which returns files only; `FileCopier` recreates
  only the parent directories of files. Truly-empty source directories are not
  recreated on an HDFS/Isilon target. Impact is low for S3 sources (no real
  directories) but is a behavioural difference from Hadoop DistCP and SparkDistCP
  (which copy directory definitions explicitly). Decide whether to replicate
  empty directories or accept the difference and document it.
- **Temp-file orphans (define in step 2).** The temp → rename commit can leave
  temporary files behind when a task dies after writing but before renaming.
  Uniqueness by task-attempt id prevents collisions, but there is no cleanup
  story. Define intended behaviour: either a pre/post-run sweep of stale temp
  files under the target prefix, or documented manual cleanup.

### Robustness

- **Retry backoff (in step 2).** The existing retry uses a fixed delay. Object
  stores throttle (e.g. S3 `503 SlowDown`), so retries should use exponential
  backoff with jitter rather than a constant `retryDelayMs`.
- **No intra-file parallelism (known limitation).** Byte-batching balances work
  across *many* files, but a single very large file is still copied by one task.
  Acceptable for the first release; record as a known limitation for datasets
  dominated by a few huge objects.

### Open questions to resolve early in the next session

- **`${VAR}` secret substitution (§3.1).** The README documents `${VAR_NAME}`
  placeholders, but no substitution exists in `ConfigParser`. Confirm whether the
  IOMETE platform substitutes at config-mount time; if not, implement it.
- **Isilon user source.** Decide whether `HdfsConfig.user` should be **required**
  in config (deterministic ownership, recommended) rather than defaulting to the
  executor process user.
- **Resume semantics.** Clarify whether a re-run resumes from the result manifest
  or simply relies on `update`-skip. The latter is acceptable for the first
  release, but state it explicitly.

### Not gaps (explicitly accepted for the first release)

- No preserve-metadata, no bandwidth limiting, no delete-at-destination, no
  snapshots/diff, no HDFS HA/Kerberos, no Iceberg-awareness. See §8.

### Definition of done (to agree before starting)

The milestone lacks explicit acceptance criteria. Before implementation, agree
targets for: representative dataset size and file-count, minimum sustained
throughput, acceptable failure budget per run, and the required green integration
suite (S3 → S3 and S3 → HDFS). Treat these as the release gate.
