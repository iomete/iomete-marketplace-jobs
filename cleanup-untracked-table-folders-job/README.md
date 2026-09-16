# Cleanup Untracked Table Folders Job

This job detects and optionally deletes **whole table folders in object storage** that are no longer claimed by the configured Spark/Iceberg catalog.

It is intended for controlled cleanup scenarios where a table was dropped without purging its underlying storage folder, leaving behind unused object-storage data.

> Start with `dry_run=true`. Review the logs and audit table before enabling deletion.

---

## Purpose

When users drop tables without purge, the catalog may stop referencing the table while the physical storage folder remains in object storage.

This job helps identify folders like that:

```text
Catalog says:
  active_table -> s3a://bucket/data/example_db/active_table

Object storage contains:
  s3a://bucket/data/example_db/active_table/
  s3a://bucket/data/example_db/dropped_without_purge/

Candidate:
  s3a://bucket/data/example_db/dropped_without_purge/
```

The job compares:

1. **Catalog-discovered active table locations**
2. **Immediate child folders under the resolved object-storage scan root**

Folders that exist in storage but are not claimed by the catalog can become cleanup candidates.

---

## What this job does

For each configured database, the job:

1. Loads the database and active table metadata through Spark SQL.
2. Resolves the object-storage folder to scan.
3. Lists immediate child folders under that scan root.
4. Excludes protected paths.
5. Applies age and candidate-count safety checks.
6. Reports candidates in dry-run mode, or deletes them when deletion mode is explicitly enabled.
7. Writes one audit row per configured database.

The job separates discovery, candidate detection, deletion, and audit writing. This makes the destructive part easier to reason about: deletion only happens after catalog discovery, storage discovery, filtering, safety checks, and pre-delete revalidation.

```text
Load config
  |
  v
Ensure audit table exists
  |
  v
For each configured database
  |
  v
Discover active table locations through Spark catalog
  |
  v
Resolve object-storage scan root
  |
  v
List immediate child folders in object storage
  |
  v
Compare storage folders against active catalog table locations
  |
  v
Detect untracked candidate folders
  |
  +--> dry_run=true
  |       write audit row, no deletion
  |
  +--> dry_run=false + delete_enabled=true
          revalidate active table locations
          delete only folders that are still untracked
          write audit row
```

---

## What this job does not do

This job does **not** clean individual orphan files inside an active Iceberg table location.

Example:

```text
s3a://bucket/data/example_db/sales/
  metadata/
  data/live_file.parquet
  data/old_unreferenced_file.parquet
```

If `sales` is still an active table location, this job protects the whole folder. It does not inspect Iceberg manifests or snapshots to decide which individual files are reachable.

That case belongs to Iceberg-aware table maintenance, such as orphan-file cleanup or snapshot expiration.

```text
This job:
  deletes whole untracked table folders

Iceberg table maintenance:
  deletes unreferenced files inside active table folders
```

---

## Configuration

The job reads `application.json`.

Example:

```json
{
  "catalog": "spark_catalog",
  "databases": ["example_database"],
  "exclude_paths": [],
  "exclude_database_folders": [],
  "older_than_hours": 24,
  "dry_run": true,
  "delete_enabled": false,
  "max_candidate_folders_per_database": 10,
  "collect_size_statistics": true
}
```

---

## Configuration fields

### `catalog`

The Spark/Iceberg catalog to inspect.

```json
"catalog": "spark_catalog"
```

The job uses Spark SQL against this catalog to discover databases, tables, and table locations.

Object-storage access uses the same catalog. For S3 and S3-compatible catalogs, the job reads
the available storage properties from the catalog's runtime configuration and uses them for
direct filesystem access. See [Object-storage configuration](#object-storage-configuration).
You do not configure storage credentials separately for this job.

---

### `databases`

List of database/schema names to inspect.

```json
"databases": ["analytics", "sales"]
```

These values must be **database/schema names**, not table names.

For a table like:

```text
spark_catalog.analytics.customer_events
```

use:

```json
"databases": ["analytics"]
```

Do not use:

```json
"databases": ["customer_events"]
```

because `customer_events` is a table, not a database/schema.

If a configured database does not exist, that database is skipped and the job writes a `SKIPPED` audit row with `status_reason=database_not_found`. Other configured databases continue to run.

---

### `exclude_paths`

Exact physical object-storage folder paths that should be protected from cleanup.

```json
"exclude_paths": [
  "s3a://bucket/data/analytics/customer_events"
]
```

`exclude_paths` is intentionally path-based. It does **not** accept table names, catalog identifiers, partial paths, or substring matches.

For a shorter database-scoped exclusion format, use `exclude_database_folders`.

| Input | Supported? | Notes |
|---|---:|---|
| `s3a://bucket/data/analytics/customer_events` | Yes | Exact physical storage path |
| `s3a://bucket/data/analytics/customer_events/` | Yes | Trailing slash is normalized |
| `customer_events` | No | Table/folder name alone is ambiguous across databases |
| `analytics.customer_events` | No for `exclude_paths` | Use `exclude_database_folders` for this database-scoped folder format |
| `customer` as a substring | No | Substring matching is intentionally not supported |

Full paths are the most explicit option and avoid ambiguity when multiple configured databases contain folders with the same table or folder name.

---

### `exclude_database_folders`

Database-scoped immediate child folders that should be protected from cleanup.

```json
"exclude_database_folders": [
  "analytics.customer_events"
]
```

Each entry uses this format:

```text
<database>.<folder>
```

For example, if the resolved scan root for database `analytics` is:

```text
s3a://bucket/data/analytics
```

then this config:

```json
"exclude_database_folders": ["analytics.customer_events"]
```

is internally resolved to this protected physical path:

```text
s3a://bucket/data/analytics/customer_events
```

This is useful when operators know the database and folder name but do not want to provide the full object-storage path.

Rules:

- The database part must be listed in `databases`.
- The folder part must be an immediate child folder name.
- The folder part must not contain `/`.
- Catalog prefixes are not supported in this field.
- The entry is converted to a full physical path internally and then handled together with `exclude_paths`.

| Input | Supported? | Notes |
|---|---:|---|
| `analytics.customer_events` | Yes | Protects the `customer_events` folder under the resolved scan root for database `analytics` |
| `spark_catalog.analytics.customer_events` | No | Catalog prefix is not supported; `catalog` is already configured separately |
| `customer_events` | No | Database name is required to avoid ambiguity |
| `analytics.customer_events/nested` | No | Only immediate child folders are supported |

---

### `older_than_hours`

Only folders with a modification time at or before the cutoff can become candidates.

```json
"older_than_hours": 24
```

This gives a safety buffer so recently created or recently modified folders are not selected immediately.

---

### `dry_run`

When `true`, the job detects and reports candidates but does not delete anything.

Recommended first run:

```json
"dry_run": true
```

---

### `delete_enabled`

Deletion is allowed only when both deletion gates are explicitly set:

```json
"dry_run": false,
"delete_enabled": true
```

This two-flag design is intentional. `dry_run` controls whether the job is only reporting candidates, while `delete_enabled` is an explicit confirmation that destructive cleanup is allowed.

In other words, turning off dry-run is not enough to delete data. Deletion happens only when the operator sets both:

```json
"dry_run": false,
"delete_enabled": true
```

This may feel redundant at first, but it is a deliberate fail-safe for a job that can delete whole object-storage folders.

| `dry_run` | `delete_enabled` | Result |
|---:|---:|---|
| `true` | `false` | Discovery only; no deletion |
| `true` | `true` | Discovery only; no deletion |
| `false` | `false` | Fails safe before deletion |
| `false` | `true` | Deletion is allowed after all safety checks pass |

---

### `max_candidate_folders_per_database`

Maximum number of candidate folders allowed per database before the job refuses to continue for that database.

```json
"max_candidate_folders_per_database": 10
```

This setting limits **candidate folders**, not individual files.

It is a **blast-radius guardrail**. It does not prove deletion is safe. It forces manual review when the number of candidates is larger than expected.

If the job finds more candidates than this limit, it skips cleanup for that database and writes a `SKIPPED` audit row with `status_reason=too_many_candidate_folders`.

For example, with this setting:

```json
"max_candidate_folders_per_database": 10
```

| Candidate folders found | Result |
|---:|---|
| `0` | Continue normally |
| `5` | Continue normally |
| `10` | Continue normally |
| `11` | Skip that database; no folders are deleted |

Use a conservative value first. Increase it only after reviewing dry-run output and confirming the candidate folders are expected.

---
### `collect_size_statistics`

Whether to estimate object count and total size for candidate folders.

When enabled, the job recursively lists objects under the final candidate folders and sums object count and size. This happens only after candidate detection and safety checks, so the job does not recursively scan the whole database.

This can take time for folders with many objects. If size estimation adds too much overhead, set:

```json
"collect_size_statistics": false
```

Audit rows store raw byte values:

- `candidate_object_count`
- `candidate_total_size_bytes`
- `deleted_object_count`
- `deleted_total_size_bytes`

Logs display the same values in a human-readable format such as MB, GB, or TB.

## Safety model

The job has several independent fail-safe layers. These are intentionally redundant because the job can delete whole object-storage folders.

- Dry-run mode can report candidates without deleting anything.
- Deletion requires both `dry_run=false` and `delete_enabled=true`; changing only one flag cannot delete data.
- Only immediate child folders under the resolved scan root are considered.
- Active catalog table locations are protected.
- Configured `exclude_paths` and `exclude_database_folders` are protected.
- Candidate folders must satisfy `older_than_hours`.
- Candidate count is capped by `max_candidate_folders_per_database`.
- Candidate folders are revalidated against active catalog table locations before deletion.
- Catalog or storage uncertainty fails closed instead of being treated as an empty result.
- Results are written to an audit table.


The main safety principle is **fail closed**: if the job cannot prove that a folder is inside the expected scan boundary and not currently claimed by the catalog, it does not delete it.

### Fail-safe layers at a glance

| Layer | What it protects against |
|---|---|
| `dry_run=true` by default | Accidental deletion during first inspection |
| `delete_enabled=true` required separately | Accidental deletion from changing only `dry_run` |
| Immediate-child-folder scan only | Recursive scanning of arbitrary nested files |
| Scan-root boundary validation | Scanning outside the intended database storage area |
| Active table location protection | Deleting folders still claimed by the catalog |
| Configured exclusions | Deleting folders explicitly protected through `exclude_paths` or `exclude_database_folders` |
| `older_than_hours` cutoff | Selecting very recent folders too quickly |
| `max_candidate_folders_per_database` | Broad accidental deletion when too many candidates appear |
| Pre-delete catalog revalidation | Deleting a folder that became active after initial discovery |
| Audit row per database | Clear outcome tracking when a run scans multiple databases |
| Empty-database guard | Deleting every folder in a database whose catalog has no active table locations (likely misconfiguration) |
| Catalog-scoped storage access | Reading or deleting in the platform's storage instead of the catalog's, and vice versa |
| Framework sentinel folder protection | Deleting `_temporary`, `.spark-staging-*`, `.hive-staging_*`, `__magic`, and similar working folders left by in-flight Hadoop, Spark, or Hive writes |

---

## Fail-closed behavior

Catalog and object-storage discovery failures are not treated as empty results.

For example, if the job cannot list storage folders or cannot discover catalog metadata, it does not assume there are zero candidates. Unknown state is not considered safe for destructive cleanup.

The same rule covers storage configuration. A catalog whose storage configuration cannot be
resolved fails the database instead of falling back to the platform's storage. Size statistics are
reporting only, so a single unreadable candidate folder is tolerated and recorded as an unknown
size, but a configuration failure, or every candidate folder failing, fails the database rather
than producing a run that looks successful with no sizes.

Outcome meanings:

- `SUCCESS` means the database was processed normally.
- `SKIPPED` means a guardrail intentionally stopped cleanup for that database.
- `FAILED` means an unexpected error occurred and should be investigated.

This distinction matters when a single job scans multiple databases. Each configured database gets its own audit row, and the shared run ID groups those rows together.

---

## Object-storage configuration

The job never asks for storage credentials, and never overrides Spark's global Hadoop
configuration.

### Where the settings come from

The job reads the configured catalog's `spark.sql.catalog.<name>.*` properties from the Spark
application's `sparkConf`. For S3 and S3-compatible catalogs, it maps the available catalog
storage properties into an isolated Hadoop S3A configuration used for direct filesystem access.

| Catalog property | Hadoop setting |
|---|---|
| `s3.endpoint` | `fs.s3a.endpoint` |
| `s3.access-key-id` | `fs.s3a.access.key` |
| `s3.secret-access-key` | `fs.s3a.secret.key` |
| `s3.path-style-access` | `fs.s3a.path.style.access` |
| `s3.region`, `s3.client.region` or `client.region` | `fs.s3a.endpoint.region` |
| `s3.connection-ssl-enabled` | `fs.s3a.connection.ssl.enabled` |

The mapping follows the catalog-to-Hadoop S3A property conventions used by the IOMETE Spark catalog extension.

Defaults when the catalog does not state them:

- When a catalog declares an endpoint and does not specify path-style access, the job defaults
  `fs.s3a.path.style.access` to `true`.
- `fs.s3a.connection.ssl.enabled` follows the endpoint scheme: `false` for `http://`, otherwise
  `true`.
- When the catalog does not declare its own endpoint and carries no access key, no credentials
  are set and S3A falls through to its own credential chain. This preserves Hadoop’s normal credential-chain behavior for catalogs that do not declare their own endpoint. A catalog that declares its own endpoint must
  supply its own credentials; see [Failure behavior](#failure-behavior).

The `s3://` and `s3n://` schemes are bound to `S3AFileSystem`, because catalog locations are
stored with whatever scheme the catalog was created with and Hadoop 3 has no built-in binding for
those two.

### Why it is done per catalog

One run holds two storage systems at once: the audited catalog's bucket, and the platform bucket
that carries the audit table and the Spark event log. Setting a catalog endpoint globally, through
`spark.hadoop.fs.s3a.endpoint` or by writing to Spark's Hadoop configuration, sends the platform's
own traffic to the catalog's endpoint and breaks the run.

So each storage operation builds a **copy** of Spark's Hadoop configuration, applies that
catalog's settings to the copy, and creates the filesystem with `FileSystem.newInstance`. Spark's
configuration object is read, never written. The Hadoop filesystem cache is bypassed, so an
instance built for one endpoint can never be handed back for another. Each filesystem is closed
when the operation finishes.

A catalog that declares its own endpoint is never accessed with platform or runtime credentials.
It must supply its own static credentials, and the job fails closed otherwise.

### Failure behavior

The job stops for the database and records `FAILED` when the catalog's storage configuration
cannot be resolved safely:

- The catalog declares `s3.endpoint` but does not supply a complete `s3.access-key-id` and
  `s3.secret-access-key` pair. Platform and runtime credentials are never used against a
  catalog-owned endpoint, so the job refuses to run rather than fall back to them.
- Only one of `s3.access-key-id` and `s3.secret-access-key` is set.
- The catalog sets `s3.session-token`. Temporary session credentials are not supported by this
  job. The token is rejected rather than silently ignored, because dropping it would leave an
  incomplete credential set. Configure static credentials instead.

If the configured catalog is not registered in the Spark session, the job stops for that database
and records `FAILED`. It does not fall back to Spark's global configuration, because that
configuration belongs to the platform's storage and silently scanning or deleting there is exactly
what the per-catalog design prevents.

The usual cause is that the catalog does not exist, or that the job's domain has no permission for
it. Check the catalog in the console rather than adding storage settings to the job.

---

## Object-storage scan root

The catalog database location and the actual table folder parent may differ.

Example:

```text
Discovered database location:
  s3a://bucket/data/example_db.db

Active table location:
  s3a://bucket/data/example_db/active_table

Object-storage scan root:
  s3a://bucket/data/example_db
```

The job resolves the scan root from active table locations when possible, then validates that the resolved scan root stays inside the allowed database boundary.

---

## Audit table

The job writes one audit row per configured database.

A single job run can scan multiple databases. Each database can independently succeed, skip, or fail. The shared run ID groups those audit rows together.

Audit table:

```text
spark_catalog.iomete_system_db.cleanup_untracked_table_folder_runs
```

Audit rows include:

- Run ID generated by this cleanup job
- Spark application ID
- Initiating Spark user
- Runtime compute/application ID
- Runtime compute namespace
- Runtime domain
- Runtime user
- Catalog name
- Database name
- Operation
- Dry-run and delete flags
- Configured age threshold
- Calculated cutoff time
- Configured candidate-folder limit
- Excluded paths
- Status and status reason
- Error message, if any
- Discovered database location
- Storage scan location
- Active table count
- Storage folder count
- Candidate folder count
- Candidate object count and estimated size in bytes
- Deleted folder count
- Deleted object count and size in bytes
- Candidate folders
- Deleted folders
- Diagnostic details
- Start and end times

### Audit field notes

| Field | Meaning |
|---|---|
| `run_id` | Cleanup-job-generated UUID. Groups all database audit rows from one cleanup run. |
| `spark_app_id` | Spark application ID from the runtime Spark context. This is different from the platform job ID shown in the UI. |
| `external_job_id` | Stable platform Job ID shown in the UI for the job definition. |
| `platform_started_by` | Platform user who started this specific job run, when exposed by the platform runtime. |
| `runtime_compute_id` | Runtime compute/application ID exposed to the driver container through `IOMETE_COMPUTE_ID`. This maps to the application/activity ID visible in the platform UI. |
| `runtime_compute_namespace` | Runtime Kubernetes namespace exposed through `IOMETE_COMPUTE_NAMESPACE`, for example `spark-resources-1`. |
| `runtime_domain` | Runtime IOMETE domain exposed through `IOMETE_DOMAIN`, for example `fde`. |
| `runtime_user` | Runtime Spark user exposed through `SPARK_USER`. This is useful for comparing the audit row with the run-as user shown in the platform UI. Read from the env var rather than `SparkContext.sparkUser()` so the value remains the run-as identity even when Spark is configured with proxy-user impersonation. |
| `status` | High-level outcome: `SUCCESS`, `SKIPPED`, or `FAILED`. |
| `status_reason` | More specific reason for the outcome, such as `database_not_found`, `database_location_missing`, `too_many_candidate_folders`, or `unexpected_error`. |
| `older_than_hours` | Configured age threshold used for candidate detection. |
| `cutoff_time` | Calculated timestamp used to decide whether a folder is old enough to be considered. |
| `max_candidate_folders_per_database` | Configured per-database candidate-folder safety limit. |
| `candidate_object_count` | Number of objects found under candidate folders when `collect_size_statistics=true`; otherwise `0`. |
| `candidate_total_size_bytes` | Total size in bytes under candidate folders when size statistics are collected. |
| `deleted_object_count` | Number of objects under folders that were actually deleted. |
| `deleted_total_size_bytes` | Total size in bytes under folders that were actually deleted. |
| `diagnostic_details` | Job-specific diagnostic/debug details such as path samples and truncation flags. This is not Spark metrics and not Iceberg metrics. |

### Diagnostic details field

The `diagnostic_details` field is a flexible audit/debug details map populated by this job.

It is not Spark metrics and it is not Iceberg table metrics.

Stable audit values such as `status_reason`, `older_than_hours`, `cutoff_time`, and `max_candidate_folders_per_database` are stored as explicit audit columns. `diagnostic_details` is reserved for extra diagnostic context that is useful during investigation but should not be treated as the primary schema.

It may include values such as:

```text
active_table_locations_sample
storage_folder_paths_sample
candidate_folder_paths_sample
non_candidate_storage_folder_paths_sample
*_truncated
```

Path lists are sampled to avoid writing very large audit rows.

---

## Status outcomes

| Scenario | Status | Notes |
|---|---|---|
| Database processed successfully | `SUCCESS` | Includes dry-run runs with zero candidates |
| Database location is missing | `SKIPPED` | Storage discovery is skipped |
| Too many candidate folders | `SKIPPED` | Candidate count exceeded configured limit |
| Configured database does not exist | `SKIPPED` | Logged as a warning with `status_reason=database_not_found` |
| Unexpected catalog, storage, or deletion failure | `FAILED` | Real failure path remains an error |

---

## Missing database behavior

If one configured database does not exist, that database is skipped while the remaining databases continue.

Example:

```json
{
  "catalog": "spark_catalog",
  "databases": ["valid_database", "this_does_not_exist"],
  "exclude_paths": [],
  "exclude_database_folders": [],
  "older_than_hours": 24,
  "dry_run": true,
  "delete_enabled": false,
  "max_candidate_folders_per_database": 10,
  "collect_size_statistics": true
}
```

Expected audit outcome:

```text
valid_database       -> SUCCESS, if processed normally
this_does_not_exist  -> SKIPPED, status_reason=database_not_found
```

The missing database is logged as a warning, not as a cleanup failure. Unexpected errors still use the `FAILED` path.

---

## Recommended workflow

### 1. Start with dry-run

```json
{
  "catalog": "spark_catalog",
  "databases": ["analytics"],
  "exclude_paths": [],
  "exclude_database_folders": [],
  "older_than_hours": 24,
  "dry_run": true,
  "delete_enabled": false,
  "max_candidate_folders_per_database": 10,
  "collect_size_statistics": true
}
```

Review:

- Summary logs
- Candidate folders
- Estimated candidate object count and size, when `collect_size_statistics=true`
- Audit rows
- Storage scan root
- Active table locations
- Excluded paths

### 2. Confirm candidates are safe

Before enabling deletion, verify that each candidate folder:

- Is no longer claimed by the catalog.
- Is older than the configured cutoff.
- Is not in `exclude_paths`.
- Is not protected by `exclude_database_folders`.
- Is expected to be removed.
- Is not an active table folder recreated at the same location.

### 3. Enable deletion explicitly

```json
{
  "catalog": "spark_catalog",
  "databases": ["analytics"],
  "exclude_paths": [],
  "exclude_database_folders": [],
  "older_than_hours": 24,
  "dry_run": false,
  "delete_enabled": true,
  "max_candidate_folders_per_database": 10,
  "collect_size_statistics": true
}
```

The job revalidates active table locations before deleting candidates.

---

## Recommended validation checklist

Before enabling deletion mode, run the job in dry-run mode and validate the output. This checklist is intended for operator validation, not as a replacement for automated tests.

| ID | Scenario | Expected result |
|---|---|---|
| T01 | Valid DB, no untracked folders, dry-run | `SUCCESS`, zero candidates, zero deleted |
| T02 | Valid DB with one manually created untracked folder, dry-run | `SUCCESS`, one candidate, zero deleted |
| T03 | Same as T02 with deletion mode enabled | `SUCCESS`, one deleted folder |
| T04 | Post-delete dry-run | `SUCCESS`, zero candidates |
| T05 | Config contains valid DB and missing DB | Valid DB `SUCCESS`, missing DB `SKIPPED` |
| T06 | Config contains only missing DB | `SKIPPED`, `status_reason=database_not_found` |
| T07 | `dry_run=false`, `delete_enabled=false` | Fails safe, no deletion |
| T08 | Candidate count exceeds max limit | `SKIPPED`, no deletion |
| T09 | Candidate is listed in `exclude_paths` by full path | Candidate is protected |
| T09b | Candidate is listed in `exclude_database_folders` as `database.folder` | Candidate is protected |
| T10 | Loose file directly under scan root | Ignored, not deleted |
| T11 | Table is dropped then recreated at same location | Folder is protected because it is active again |
| T12 | Table is dropped then recreated at different location | Old untracked folder can become a candidate |
| T13 | Candidate becomes active between detection and deletion | Deletion is skipped for that folder |
| T14 | Unexpected catalog/storage error | `FAILED` |
| T15 | `collect_size_statistics=true` with candidate folders | Candidate object count and size are collected and logged |
| T16 | `collect_size_statistics=false` with candidate folders | Candidate and deleted size audit fields remain `0` and size collection is skipped |

---

## Examples

### Valid database with no candidates

```text
Catalog active table locations:
  s3a://bucket/data/analytics/active_table

Storage folders:
  s3a://bucket/data/analytics/active_table

Candidate folders:
  none
```

Result:

```text
status = SUCCESS
candidate_folder_count = 0
deleted_folder_count = 0
```

---

### Dropped table folder left behind

```text
Catalog active table locations:
  s3a://bucket/data/analytics/active_table

Storage folders:
  s3a://bucket/data/analytics/active_table
  s3a://bucket/data/analytics/dropped_without_purge
```

Candidate:

```text
s3a://bucket/data/analytics/dropped_without_purge
```

Dry-run result:

```text
status = SUCCESS
candidate_folder_count = 1
deleted_folder_count = 0
```

Deletion-mode result:

```text
status = SUCCESS
candidate_folder_count = 1
deleted_folder_count = 1
```

---

### Table recreated at the same location

```text
Old table:
  s3a://bucket/data/analytics/sales

Dropped without purge:
  old files remain

New table:
  s3a://bucket/data/analytics/sales
```

Because the folder is now claimed by an active catalog table, this job protects the folder.

Result:

```text
s3a://bucket/data/analytics/sales is not deleted
```

Any old unreferenced files inside that active table folder should be handled by Iceberg-aware table maintenance.

---

## Known limitations

- `exclude_paths` supports full physical storage paths only.
- `exclude_database_folders` supports database-scoped immediate child folder names in `database.folder` format.
- Table names or folder names alone are not supported because they can be ambiguous across databases.
- The platform UI Job ID is not currently stored in the audit table because it is not exposed to the driver process as an environment variable. The runtime compute/application ID is stored as `runtime_compute_id`.
- The job deletes whole untracked table folders, not individual orphan files inside active Iceberg table folders.
- Candidate discovery is table-folder-level: the job only selects immediate child folders under the resolved scan root as cleanup candidates.
- When a selected candidate folder is deleted, the full object-storage prefix under that folder is deleted, including nested data and metadata objects.
- The job relies on Spark catalog discovery for active table locations.
- Object-storage access supports S3 and S3-compatible storage. A non-S3 catalog location falls back to Spark's own Hadoop configuration, which carries no catalog-specific endpoint or credentials.
- Catalogs using vended or remotely signed credentials are not supported for storage access, because those credentials are issued per Iceberg request and never appear in `sparkConf`.
- Folder names are compared case-sensitively, because `ORDERS` and `orders` are different object-storage prefixes.
- The job is intended for controlled cleanup workflows, not blind automatic deletion.
- Size statistics require recursively listing objects under final candidate folders. This can add overhead for folders with many objects and can be disabled with `collect_size_statistics=false`.
