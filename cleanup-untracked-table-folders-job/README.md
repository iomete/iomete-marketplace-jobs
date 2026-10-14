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
  "max_candidate_folders_per_database": 10
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

Physical object-storage paths that should be protected from cleanup.

```json
"exclude_paths": [
  "s3a://bucket/data/analytics/customer_events"
]
```

`exclude_paths` is intentionally path-based. It does **not** accept table names, catalog identifiers, partial paths, or substring matches.

A candidate folder is skipped when it equals an excluded path, contains one, or sits inside one. Paths are compared by whole path segments, so `customer_events` does not protect `customer_events_v2`.

Candidate folders are deleted recursively, so an excluded path nested inside a candidate protects the whole candidate folder. The job never deletes around an exclusion.

For a shorter database-scoped exclusion format, use `exclude_database_folders`.

| Input | Supported? | Notes |
|---|---:|---|
| `s3a://bucket/data/analytics/customer_events` | Yes | Full physical storage path |
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
- The same folder name is also excluded under every [additional storage root](#additional-storage-roots-report-only) the job lists for reporting.

| Input | Supported? | Notes |
|---|---:|---|
| `analytics.customer_events` | Yes | Protects the `customer_events` folder under the resolved scan root for database `analytics` |
| `spark_catalog.analytics.customer_events` | No | Catalog prefix is not supported; `catalog` is already configured separately |
| `customer_events` | No | Database name is required to avoid ambiguity |
| `analytics.customer_events/nested` | No | Only immediate child folders are supported |

---

### `older_than_hours`

A folder's age is the modification time of the newest object anywhere under it. Only folders whose newest object is at or before the cutoff can become candidates.

```json
"older_than_hours": 24
```

This gives a safety buffer so folders with recently written objects are not selected immediately.

Object storage does not keep a reliable modification time for folders themselves, so the job never uses folder timestamps. A folder whose age cannot be established is skipped: one that is empty or holds only directory markers, one that cannot be listed completely, or one whose objects report an invalid modification time.

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

Deprecated and ignored. The setting is still accepted so existing configurations keep working.

The job always lists the objects under each candidate folder, because that same listing establishes folder age. Object count and total size come from the same listing at no extra cost. The listing runs only after candidate detection and the `max_candidate_folders_per_database` check, so the job never recursively lists the whole database.

If the setting is `false`, the job logs one warning at startup and collects statistics anyway.

Audit rows store raw byte values:

- `candidate_object_count`
- `candidate_total_size_bytes`
- `deleted_object_count`
- `deleted_total_size_bytes`

Logs display the same values in a human-readable format such as MB, GB, or TB.

### `report_only_storage_roots`

Optional. Storage locations, per database, that the job may list one level deep for review. Nothing in
them is ever deleted.

```json
"report_only_storage_roots": {
  "sales": ["s3a://other-bucket/external/sales_events"]
}
```

Use it for table locations outside the database's own storage, which the job does not list on its own.
Approval is evidence that the location is relevant, not proof that the database owns everything in it.

**Approve only locations that belong to the database they are listed under.** The job does not compare
approved locations with the storage of other databases. If an approved location holds another
database's tables, the report lists their folders as not matched to a visible table.

- Each key must be listed in `databases`.
- A location is listed only if at least one table of that database that the job can see is stored under
  it, including a location inside the database's own storage, and it passes the same checks as every
  other root. See [Additional storage roots](#additional-storage-roots-report-only).
- Table parents outside the database's storage are reported as `not_scanned` with
  `outside_database_boundary`. Review them before adding any of them here.

## Safety model

The job has several independent fail-safe layers. These are intentionally redundant because the job can delete whole object-storage folders.

- Dry-run mode can report candidates without deleting anything.
- Deletion requires both `dry_run=false` and `delete_enabled=true`; changing only one flag cannot delete data.
- Only immediate child folders under the resolved scan root are considered.
- Active catalog table locations are protected. A folder that equals, contains, or sits inside one is never a candidate.
- Configured `exclude_paths` and `exclude_database_folders` are protected the same way.
- A database location that is a bucket root, or that equals or contains the catalog warehouse root, is never scanned.
- The newest object in a candidate folder must be older than `older_than_hours`. A folder whose age cannot be established is skipped.
- Candidate count is capped by `max_candidate_folders_per_database`.
- Candidate folders are revalidated against active catalog table locations and exclusions before deletion.
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
| Bucket-root database location rejection | Treating a whole bucket as one database's storage |
| Warehouse-root rejection | Scanning a shared warehouse root where other databases live |
| Active table location protection | Deleting folders still claimed by the catalog, including folders inside a table location |
| Configured exclusions | Deleting folders explicitly protected through `exclude_paths` or `exclude_database_folders`, including an excluded path nested inside a candidate |
| `older_than_hours` cutoff | Selecting folders that hold recently written objects; folders of unknown age are skipped |
| `max_candidate_folders_per_database` | Broad accidental deletion when too many candidates appear |
| Pre-delete revalidation | Deleting a folder that became active after initial discovery, or that overlaps an exclusion |
| Audit row per database | Clear outcome tracking when a run scans multiple databases |
| Empty-database guard | Deleting every folder in a database whose catalog has no active table locations (likely misconfiguration) |
| Catalog-scoped storage access | Reading or deleting in the platform's storage instead of the catalog's, and vice versa |
| Framework sentinel folder protection | Deleting `_temporary`, `.spark-staging-*`, `.hive-staging_*`, `__magic`, and similar working folders left by in-flight Hadoop, Spark, or Hive writes |

---

## Fail-closed behavior

Catalog and object-storage discovery failures are not treated as empty results.

For example, if the job cannot list storage folders or cannot discover catalog metadata, it does not assume there are zero candidates. Unknown state is not considered safe for destructive cleanup.

The same rule covers storage configuration. A catalog whose storage configuration cannot be
resolved fails the database instead of falling back to the platform's storage. A candidate folder
that cannot be listed completely has an unknown age, so it is skipped and never deleted. If every
candidate folder fails to list, the database fails rather than producing a run that looks
successful.

If a recursive delete reports that an existing folder was not deleted, the folder's state is
unknown. The job stops deleting for that database and records `FAILED`.

Outcome meanings:

- `SUCCESS` means the database was processed normally.
- `SKIPPED` means a guardrail intentionally stopped cleanup for that database.
- `BLOCKED` means reconciliation completed far enough to inspect catalog and storage state, but a
  safety condition prevented cleanup. The reconciliation result is still reported.
- `FAILED` means an unexpected error occurred and should be investigated. When deletion itself
  fails partway, some folders may already be gone, so a `FAILED` row can have
  `deleted_folder_count` greater than `0`.

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
  are set and S3A falls through to its own credential chain. A catalog that declares its own
  endpoint must supply its own credentials; see [Failure behavior](#failure-behavior).

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

Before choosing a scan root, the job checks the database location itself:

- A database location that is a bucket root, such as `s3a://bucket`, is rejected even when the tables sit deeper. A whole bucket is not a narrow enough ownership boundary.
- A database location that equals or contains the catalog warehouse root is rejected. Databases created without their own location live under the warehouse root, so scanning it could reach other databases. This applies, for example, to a `default` database whose location is the warehouse root.
- A table stored directly at a bucket root does not contribute a scan root.

The warehouse root is read from `spark.sql.catalog.<catalog>.warehouse`. For `spark_catalog`, Spark's `spark.sql.warehouse.dir` is checked as well. If the catalog warehouse is not configured, the job cannot run this check and skips the database.

A rejected database is recorded as `SKIPPED` with one of these reasons. The rejected location itself is never listed:

| `status_reason` | Meaning |
|---|---|
| `database_location_is_bucket_root` | The database location is a bucket root |
| `database_location_contains_warehouse_root` | The database location equals or contains the catalog warehouse root |
| `catalog_warehouse_unknown` | The catalog warehouse location is not configured |
| `scan_root_outside_database_boundary` | The resolved scan root falls outside the database location |
| `invalid_storage_location` | A database, table, or warehouse location cannot be parsed |

The row keeps this status and reason. For `scan_root_outside_database_boundary`,
`database_location_is_bucket_root` and `database_location_contains_warehouse_root`, the job may still list
[report-only roots](#additional-storage-roots-report-only) whose own checks pass, and adds them to
`diagnostic_details`. A rejected bucket-root or warehouse-root database location is never one of them.
Nothing is deleted.

When tables sit under more than one parent location, the job keeps the conservative fallback and scans only the database location. Table locations outside the scanned root are not checked in that run, and the job reports this instead of hiding it:

- The summary log lists them under `TABLE LOCATIONS OUTSIDE THE SCANNED ROOT`.
- A `SUCCESS` audit row gets `status_reason=partial_scan_coverage`.
- A row with a stronger outcome, such as `BLOCKED`, keeps its own reason.
- `diagnostic_details` holds `unscanned_table_location_count` and a bounded sample.
- A `FAILED` row always has `unscanned_table_location_count` once coverage was checked. A value of
  `0` means the scan covered every table location. No count means the job failed before it got
  that far.

The scan root above is the **cleanup root**. It is the only place where folders can be deleted. Deletion
is also blocked when no table the job can see is stored under the cleanup root. See
[Cleanup root with no visible table](#cleanup-root-with-no-visible-table).

### Additional storage roots (report only)

A database often has tables under more than one parent folder. The job also lists these other roots, one
level deep, and reports what it finds. It never deletes from them.

A root is listed only when it sits inside the database's own storage:

- the database location, or the same path without the `.db` suffix;
- for `spark_catalog` and other IOMETE-managed REST catalogs, `<catalog warehouse>/<database>`, where the
  catalog stores the database's Iceberg tables. A catalog counts as IOMETE-managed when its `uri` ends with
  `/internal/catalogs/<catalog>`, or with `/catalogs/<catalog>` on the in-cluster `iom-rest-catalog`
  service. For any other Iceberg REST catalog, the audit row has `rest_namespace_root=unrecognized_uri`
  and the log explains it;
- a location approved for the database in `report_only_storage_roots`, as long as at least one visible
  table of the database is stored under it.

Every other table parent is reported as `not_scanned` with a reason, and its table locations count as
uncovered:

| Reason | Meaning |
|---|---|
| `outside_database_boundary` | The parent is outside the database's own storage, in the same or another bucket |
| `bucket_root` | The parent is a bucket root |
| `contains_warehouse_root` | The parent equals or contains the catalog warehouse root |
| `inside_table_location` | The parent sits inside another table's folder |
| `contains_database_storage` | The location would contain the database's own location or REST root |
| `no_visible_table` | An approved location holds no table of the database that the job can see |
| `invalid_location` | The location cannot be parsed |
| `root_limit_reached` | More than 25 additional roots were found; the rest are not listed |

A listed root is `scanned`, `absent` (it does not exist yet) or `listing_failed`. A failed listing never
changes what is deleted. Folders in these roots that do not belong to a table the job can see are listed
under `FOLDERS IN REPORT-ONLY ROOTS NOT MATCHED TO A VISIBLE TABLE`. A table the job cannot see may
still own them, so treat them as items to review, not as orphans.

Approval never overrides these checks, and a root listed because of approval is shown with the role
`report_only_approved`. The log prints at most 25 not-scanned roots. The audit counts are always complete.

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
- Unresolved table count
- Storage folder count
- Candidate folder count
- Potentially untracked folder count
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
| `status` | High-level outcome: `SUCCESS`, `SKIPPED`, `BLOCKED`, or `FAILED`. |
| `status_reason` | More specific reason for the outcome, such as `database_not_found`, `database_location_missing`, `too_many_candidate_folders`, `unresolved_catalog_ownership`, `database_location_contains_warehouse_root`, `partial_scan_coverage`, `deletion_failed`, `audit_integrity_lost`, or `unexpected_error`. See [Object-storage scan root](#object-storage-scan-root) for the scan-root reasons. |
| `older_than_hours` | Configured age threshold used for candidate detection. |
| `cutoff_time` | Calculated timestamp used to decide whether a folder is old enough to be considered. |
| `max_candidate_folders_per_database` | Configured per-database candidate-folder safety limit. |
| `unresolved_table_count` | Number of catalog tables in this database whose storage location could not be read during discovery. |
| `candidate_folder_count` | Number of folders confirmed as untracked and eligible for deletion. Reserved for confirmed deletion-eligible candidates, so it stays `0` when table storage ownership could not be fully verified. |
| `potentially_untracked_folder_count` | Number of storage folders that matched no verified table location while at least one table's storage location was unresolved. These folders are reported for review only. They are not confirmed untracked and are never deletion candidates, which is why they are counted separately from `candidate_folder_count`. |
| `candidate_object_count` | Number of objects found under candidate folders. |
| `candidate_total_size_bytes` | Total size in bytes under candidate folders. |
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
potentially_untracked_folder_paths_sample
unresolved_tables_sample
non_candidate_storage_folder_paths_sample
unscanned_table_location_count
unscanned_table_locations_sample
ownership_basis
scan_plan_status
scan_plan_error
scan_root_count
scan_root_not_scanned_count
scan_roots_sample
scan_plan_coverage
scan_plan_uncovered_table_location_count
scan_plan_uncovered_table_locations_sample
report_only_folder_count
report_only_folders_sample
rest_namespace_root
*_truncated
```

Storage root keys follow `scan_plan_status`:

- No `scan_plan_status` means planning was not attempted, for example a database skipped before its
  storage was checked.
- `scan_plan_status=failed` means planning threw. `scan_plan_error` holds the error, and no other storage
  root key is written. The row keeps the status it would have had anyway.
- `scan_plan_status=planned` comes with the keys below.
- `scan_roots_sample` has one line per root: `<location>=<role>:<status>:visible_tables=<n>`, where
  role is `cleanup`, `report_only`, `report_only_approved` or `not_scanned`. The cleanup root comes first,
  then listed roots, then not-scanned roots with the most visible tables first.
- `scan_plan_coverage` is `complete` when every table the job can see sits under a listed root and every
  listing worked. It says nothing about tables the job cannot see.
- `ownership_basis=visible_catalog_tables` records that ownership was checked only against the tables the
  job can see in the catalog.

Path lists are sampled to avoid writing very large audit rows.

When deletion stops partway, `deletion_failed_folder_path` names the folder whose delete failed.
Its state is unknown: it may be fully present, partly deleted, or gone.

---

## Status outcomes

| Scenario | Status | Notes |
|---|---|---|
| Database processed successfully | `SUCCESS` | Includes dry-run runs with zero candidates |
| Database processed, but some table locations are outside the scanned root | `SUCCESS` | `status_reason=partial_scan_coverage` |
| Database location is missing | `SKIPPED` | Storage discovery is skipped |
| Too many candidate folders | `SKIPPED` | Candidate count exceeded configured limit |
| Configured database does not exist | `SKIPPED` | Logged as a warning with `status_reason=database_not_found` |
| Database location or scan root is unsafe | `SKIPPED` | Storage is never listed; see [Object-storage scan root](#object-storage-scan-root) for the reasons |
| One or more table storage locations could not be resolved | `BLOCKED` | Reconciliation is still reported, nothing is deleted, `status_reason=unresolved_catalog_ownership` |
| Unexpected catalog, storage, or deletion failure | `FAILED` | Real failure path remains an error |
| Deletion failed partway through the candidates | `FAILED` | `status_reason=deletion_failed`; see [Partial deletion](#partial-deletion) |
| An earlier audit write in the same run failed | `BLOCKED` | `status_reason=audit_integrity_lost`; see [Audit write failures](#audit-write-failures) |
| No visible table is stored under the cleanup root | `BLOCKED` | `status_reason=cleanup_root_has_no_visible_table`; candidates are reported, nothing is deleted |

---

## Partial deletion

Candidates are deleted one at a time. The job stops at the first folder it cannot delete and does
not try the remaining candidates in that database.

- The audit row is written with `status=FAILED` and `status_reason=deletion_failed`.
- `deleted_folder_count` and `deleted_folders` list only the folders confirmed deleted before the
  failure, so they can be greater than zero on a `FAILED` row.
- `diagnostic_details.deletion_failed_folder_path` names the folder that failed. Check it in
  object storage before the next run. It is absent when the failure came after the last confirmed
  deletion, for example while closing the storage connection.
- A folder that was already gone when the job reached it is not counted as deleted.
- A delete that reports no change for a folder that still exists is treated as a failure, because
  the job cannot tell what happened to it.

Later databases in the same run continue, as long as this audit row was written.

---

## Audit write failures

The audit table must be available before any database is processed. If it cannot be created or
prepared, the job fails before scanning or deleting anything.

If an audit row cannot be written during the run, the job no longer has a reliable record of what
it did. From that point on:

- No more folders are deleted in this run.
- Later databases are still discovered and reconciled, and their report is logged.
- A database that would otherwise have been eligible for deletion is recorded as `BLOCKED` with
  `status_reason=audit_integrity_lost`. This also applies in dry-run. Other outcomes, such as
  `SKIPPED` or `FAILED`, keep their own status and reason.
- The job fails at the end of the run, and the log contains the original audit error.

Each later row is written once on a best-effort basis. A database whose own audit row failed has
no row in the audit table, so use the job log for that database.

---

## Cleanup root with no visible table

When tables sit under more than one parent location, the cleanup root falls back to the database
location. If no table the job can see is stored under that location, nothing in it can be checked
against a known table. The database is then recorded as `BLOCKED` with
`status_reason=cleanup_root_has_no_visible_table`, and nothing is deleted. This also applies in dry-run
and when there are no candidates. The candidates are still listed in the audit row and the log, and the
log's `DELETION` section gives the reason.

---

## Unresolved table storage ownership

A table is unresolved when it exists in the catalog but the job cannot read its Iceberg metadata or
storage location during the run. This says nothing about whether the table is wanted or healthy. It
only means the job does not currently know which storage folder that table owns.

Storage reconciliation still runs. The job lists the database's storage folders and compares them
against the table locations it could verify, so the operator still gets the full picture of what is
in storage.

What changes is what the job is allowed to conclude. If even one table in the database has an
unresolved storage location, that table could own any of the unmatched folders. No unmatched folder
in that database can be proven untracked. So for that database:

- Every unmatched folder is reported as potentially untracked, not as a candidate.
- `potentially_untracked_folder_count` carries those folders.
- `candidate_folder_count` stays `0`, because it is reserved for confirmed deletion-eligible candidates.
- Nothing is deleted, in dry-run mode or in deletion mode.
- The audit row is written with `status=BLOCKED` and `status_reason=unresolved_catalog_ownership`.

Ownership certainty is a property of the whole database, not of an individual folder. The job never
mixes confirmed candidates and unverified folders in the same database.

The per-database folder limit is still checked first. If the number of unmatched folders exceeds
`max_candidate_folders_per_database`, the database is reported as `SKIPPED` with
`status_reason=too_many_candidate_folders` instead. Both outcomes stop deletion for that database.

Other configured databases are unaffected and continue to be processed normally.

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
  "max_candidate_folders_per_database": 10
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
  "max_candidate_folders_per_database": 10
}
```

Review:

- Summary logs
- Candidate folders
- Candidate object count and size
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
  "max_candidate_folders_per_database": 10
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
| T09c | An excluded path sits inside a candidate folder | The whole candidate folder is protected |
| T10 | Loose file directly under scan root | Ignored, not deleted |
| T11 | Table is dropped then recreated at same location | Folder is protected because it is active again |
| T12 | Table is dropped then recreated at different location | Old untracked folder can become a candidate |
| T13 | Candidate becomes active between detection and deletion | Deletion is skipped for that folder |
| T14 | Unexpected catalog/storage error | `FAILED` |
| T15 | Candidate folders present | Candidate object count and size are collected and logged |
| T16 | `collect_size_statistics=false` with candidate folders | One startup warning; statistics are still collected |
| T17 | Database location is a bucket root or the catalog warehouse root | `SKIPPED`, storage is not listed |
| T18 | A candidate folder holds a recently written object | The folder is skipped as too new |
| T19 | Deletion fails partway through the candidates | `FAILED`, `status_reason=deletion_failed`, `deleted_folders` lists only folders deleted before the failure |
| T20 | Audit write fails for one database in a multi-database run | Later databases are not deleted, eligible ones are `BLOCKED` with `status_reason=audit_integrity_lost`, the job fails |
| T21 | Tables under several parent folders | `scan_roots_sample` lists each root with its role and status; folders are deleted only under the cleanup root |
| T22 | A table parent outside the database's storage | Reported as `not_scanned:outside_database_boundary`, never listed, its table counted as uncovered |
| T23 | The same parent added to `report_only_storage_roots` | Listed as `report_only_approved`; its folders are reported and never deleted |
| T24 | A database rejected with `scan_root_outside_database_boundary` | Still `SKIPPED` with the same reason; report-only roots appear in `diagnostic_details`; nothing is deleted |

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
- `report_only_storage_roots` approves listing for review only. It cannot make a location eligible for
  deletion. Older job versions reject configs that contain this setting, so upgrade the job before adding it.
- Object-storage access supports S3 and S3-compatible storage. A non-S3 catalog location falls back to Spark's own Hadoop configuration, which carries no catalog-specific endpoint or credentials.
- Catalogs using vended or remotely signed credentials are not supported for storage access, because those credentials are issued per Iceberg request and never appear in `sparkConf`.
- Folder names are compared case-sensitively, because `ORDERS` and `orders` are different object-storage prefixes.
- A database whose location equals or contains the catalog warehouse root cannot be cleaned by this job.
- Folders are deleted only under the cleanup root. Additional roots are reported only, and table parents
  outside the database's own storage are not listed at all.
- Ownership is checked against the tables the job can see in the catalog. A table the job cannot see does
  not protect its folder.
- The job is intended for controlled cleanup workflows, not blind automatic deletion.
- Folder age and size require recursively listing objects under each candidate folder, for at most `max_candidate_folders_per_database` folders per database. This can take time for folders with many objects and cannot be turned off, because it is needed to establish folder age.
