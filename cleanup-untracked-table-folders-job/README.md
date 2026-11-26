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
2. Resolves the object-storage folder to scan, and a [second folder](#second-cleanup-root) when the database's tables sit in both of its folders.
3. Lists immediate child folders under each scan root.
4. Excludes protected paths.
5. Applies the candidate-count limit, then the age check.
6. Reports candidates in dry-run mode, or deletes them when deletion mode is explicitly enabled.
7. Writes one audit row per configured database.

The job separates discovery, candidate detection, deletion, and audit writing. This makes the destructive part easier to reason about: deletion only happens after catalog discovery, storage discovery, filtering, safety checks, and a [check of each folder right before it is deleted](#deletion-fence).

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
          read ownership again
          for each folder: check it again, list it again,
            and delete exactly the listed objects
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

A catalog or database name that contains a backtick (`` ` ``) is not supported. The job never puts such a
name in SQL text. That database gets a `FAILED` row with `status_reason=unexpected_error`, and
`error_message` says the name contains a backtick. The other configured databases still run.

Names are used as written. A name such as `sales_${env}` is looked up as `sales_${env}`. Older versions
of this job let Spark replace `${...}` in the SQL text first, so such a name could resolve to another
name or to nothing.

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

The job deletes a candidate folder with everything below it, so an excluded path nested inside a candidate protects the whole candidate folder. The job never deletes around an exclusion.

For a shorter database-scoped exclusion format, use `exclude_database_folders`.

| Input | Supported? | Notes |
|---|---:|---|
| `s3a://bucket/data/analytics/customer_events` | Yes | Full physical storage path |
| `s3a://bucket/data/analytics/customer_events/` | Yes | Trailing slash is normalized |
| `customer_events` | No | Table/folder name alone is ambiguous across databases |
| `analytics.customer_events` | No for `exclude_paths` | Use `exclude_database_folders` for this database-scoped folder format |
| `/data/analytics/customer_events` | No | A path without a scheme and bucket fails validation, because it cannot be compared with storage paths |
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
- The same folder name is also excluded under a [second cleanup root](#second-cleanup-root) and under every [additional storage root](#additional-storage-roots-report-only) the job lists for reporting.

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

#### 72-hour minimum in deletion mode

When `dry_run` is `false`, the job never uses less than 72 hours. If `older_than_hours` is lower,
including the default of 24, the run uses 72 instead. It logs one warning at the start that names both
values. A higher value, such as 100, is used as it is.

This minimum is fixed in the code. No configuration key, environment variable or system property can
lower it. The job reads only its JSON configuration file, and that file accepts only the keys on this
page.

| `older_than_hours` | Dry run uses | Deletion mode uses |
|---:|---:|---:|
| `0` | 0 | 72 |
| unset (24) | 24 | 72 |
| `71` | 71 | 72 |
| `72` | 72 | 72 |
| `100` | 100 | 100 |

A dry run keeps the configured value, `0` included, so you can still test with new folders. A dry run
below 72 hours can therefore list folders that deletion mode would keep for now. The audit row counts
them in `would_be_held_by_deletion_floor_count` and names them in
`would_be_held_by_deletion_floor_paths_sample`. The log report lists them under the untracked folders.

The audit column `older_than_hours` holds the age the run used. The configured value is in
`diagnostic_details` as `configured_older_than_hours`, next to `deletion_min_age_hours=72`. Rows
written before this minimum existed have neither key. In those rows the column is both the configured
and the used value. So this query gives the configured value for old and new rows:

```sql
coalesce(diagnostic_details['configured_older_than_hours'], CAST(older_than_hours AS STRING))
```

To test deletion mode, create the test folders at least 72 hours before the run. Object storage sets
the modification time when an object is written, and it cannot be set back.

The age check compares object times with the clock of the Spark driver. If that clock runs ahead of the
storage clock, the minimum is shorter by the same amount.

Object storage does not keep a reliable modification time for folders themselves, so the job never uses folder timestamps. A folder is skipped and never deleted when it is missing at inspection, cannot be read, is empty, holds too many objects, or its age cannot be established. The job records why:

| Reason | Meaning |
|---|---|
| `folder_not_found` | The folder was listed under the cleanup root, but listing its contents and a second lookup both found nothing. It no longer exists, so there is nothing to delete. |
| `statistics_unavailable` | The folder's contents could not be listed, for example because access was denied or the request failed. |
| `empty_folder` | The folder exists but holds no objects, only directory markers or nothing. Empty folders, including their directory markers, are not deleted. |
| `unknown_age` | The folder holds objects, but at least one reports an invalid modification time. |
| `too_many_objects` | The folder holds more than 10,000 objects. The job deletes object by object, so it keeps larger folders. The listing stops after 10,001 objects. The limit is provisional. See [Object limit](#object-limit). |

The job only examines immediate child folders of the cleanup root, so a folder nested inside a candidate is never classified on its own.

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

Maximum number of folders the job may delete in one database in one run. If more are due, it deletes none of them.

```json
"max_candidate_folders_per_database": 10
```

This setting limits **candidate folders**, not individual files.

It is a **blast-radius guardrail**. It does not prove deletion is safe. It forces manual review when the number of candidates is larger than expected.

The limit counts only the folders that passed every check and are due for deletion: ownership, age, the
[storage holds](#storage-holds) and the confirmation. If more folders are due than the limit, the job
deletes none of them and writes a `SKIPPED` audit row with `status_reason=too_many_candidate_folders`. No
[deletion intent](#deletion-intent) row is written. When a database has a
[second cleanup root](#second-cleanup-root), folders from both roots count toward one limit. The log report
and `due_for_deletion_paths_sample` list the due folders, so they can be reviewed before the limit is raised.
The row keeps the folder fingerprints and `prev_run_id`, so after the limit is raised the next run can
delete the same folders without a new 72-hour wait.

For example, with this setting:

```json
"max_candidate_folders_per_database": 10
```

| Folders due for deletion | Result |
|---:|---|
| `0` | Continue normally |
| `5` | Continue normally |
| `10` | Continue normally |
| `11` | Skip that database; no folders are deleted |

Held folders never count. For example, a database with 3 old folders, 9 folders written in the last
48 hours and a limit of `3` deletes the 3 old folders. The 9 are held as `too_new`. The row has the held
folder counts described in [Held folders](#held-folders).

A separate inspection limit protects the job from listing too many folders. If a database has more
folders with no verified catalog owner than the larger of `100` and `max_candidate_folders_per_database`,
the job lists none of them. It writes a `SKIPPED` row with `status_reason=too_many_folders_to_inspect`,
and nothing is deleted. This row has no held folder counts, because no folder was inspected. To raise the
inspection limit, set `max_candidate_folders_per_database` above `100`, or protect folders with
`exclude_paths`.

Use a conservative value first. Increase it only after reviewing dry-run output and confirming the candidate folders are expected.

---
### `collect_size_statistics`

Deprecated and ignored. The setting is still accepted so existing configurations keep working.

The job always lists the objects under each candidate folder, because that same listing establishes folder age. Object count and total size come from the same listing at no extra cost. The listing runs only after candidate detection and the inspection limit check (see [`max_candidate_folders_per_database`](#max_candidate_folders_per_database)), so the job never recursively lists the whole database.

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
- Only immediate child folders under the cleanup roots are considered.
- Active catalog table locations are protected. A folder that equals, contains, or sits inside one is never a candidate.
- Configured `exclude_paths` and `exclude_database_folders` are protected the same way.
- A database location that is a bucket root, or that equals or contains the catalog warehouse root, is never scanned.
- The newest object in a candidate folder must be older than `older_than_hours`, and in deletion mode at least 72 hours old. A folder whose age cannot be established is skipped.
- The folders due for deletion are capped by `max_candidate_folders_per_database`. Held folders do not count. A
  database with more unmatched folders than the larger of 100 and that cap is not inspected at all.
- Right before deleting each folder, the job checks it against its last ownership read, which is at most 5
  minutes old. It also looks up a table with the folder's name, lists the folder again and checks its age. An
  owner registered after the last ownership read can be missed. See [Deletion fence](#deletion-fence) and
  [Races the job cannot close](#races-the-job-cannot-close).
- A folder is deleted object by object. The job lists it again, deletes exactly the listed objects, and checks each one
  before its delete. It never sends a recursive delete. A folder with more than 10,000 objects is kept.
- The job waits for each check and each delete only until its last ownership read is 5 minutes old. It
  starts no delete in the last 30 seconds of that time. A check that does not finish in time stops deletion
  for that database. A delete still running at the 5 minutes is not counted as done and may land later, with
  no bound on when. See [Time limit](#time-limit).
- A folder that passes every ownership and age check is still kept unless its contents show it is safe:
  only commit scratch, or table metadata of a table at this folder. Every folder is also kept until earlier
  runs of the same job saw it unchanged for 72 hours. See [Storage holds](#storage-holds).
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
| Other database protection | Deleting the storage of another database the job can see that sits inside the cleanup root |
| Other table protection | Deleting a folder used by a table the job can see in another database, or in another catalog of its Spark session |
| Iceberg write path protection | Deleting folders named by a visible Iceberg table's `write.*.path` properties |
| Configured exclusions | Deleting folders explicitly protected through `exclude_paths` or `exclude_database_folders`, including an excluded path nested inside a candidate |
| `older_than_hours` cutoff | Selecting folders that hold recently written objects; folders of unknown age are skipped |
| 72-hour minimum in deletion mode | Deleting a folder written in the last 72 hours, even when `older_than_hours` is lower |
| `max_candidate_folders_per_database` | Broad accidental deletion when more folders are due than expected |
| [Storage holds](#storage-holds) | Deleting a view's folder, a folder whose metadata belongs elsewhere, data that a table or job may read by path, or a folder that changed or appeared in the last 72 hours of this job's runs |
| [Deletion fence](#deletion-fence), per folder | Deleting a folder that a table, another database or an exclusion claimed before the last ownership read, a folder that changed since inspection, or a folder that got a new object before its last listing |
| Exact-object deletion | Deleting objects written after the last listing, and objects that changed before their last check. Not covered: an object rewritten while its own delete runs, and an object written at a directory's exact name while that directory is deleted (see [Known limitations](#known-limitations)) |
| 5-minute limit on the ownership read | Starting a delete with an ownership read that is more than 4.5 minutes old, or waiting without end for a read or a delete that does not return |
| Object limit per folder | Deletions that run for hours, or a key list too large to hold; a folder above 10,000 objects is kept as `too_many_objects`. The limit is provisional |
| Audit row per database | Clear outcome tracking when a run scans multiple databases |
| Empty-database guard | Deleting every folder in a database whose catalog has no active table locations (likely misconfiguration) |
| Catalog-scoped storage access | Reading or deleting in the platform's storage instead of the catalog's, and vice versa |
| Framework sentinel folder protection | Deleting `_temporary`, `.spark-staging-*`, `.hive-staging_*`, `__magic`, and similar working folders left by in-flight Hadoop, Spark, or Hive writes |

---

## What deletion mode can and cannot prove

Before a folder is deleted, the job checks it against what it can see. A folder is deleted only when
none of these claims it:

- a table of this database that the job can see, including its Iceberg write paths;
- the storage of another database that the job can see, as described in
  [Storage of other databases and Iceberg write paths](#storage-of-other-databases-and-iceberg-write-paths);
- a table of another database that the job can see, or the storage of another catalog of its Spark
  session, as described in
  [Tables of other databases and catalogs](#tables-of-other-databases-and-catalogs);
- `exclude_paths` and `exclude_database_folders`.

In deletion mode, the job reads ownership again right before it deletes the first folder of a
database, and again once that read is more than 4 minutes old, before a later folder or inside one. Each folder also
gets its own checks right before it is deleted. See [Deletion fence](#deletion-fence). A table created
in a candidate folder after the last ownership read is not seen, unless it is a table of this database
with the folder's name. For an object the job counted as deleted, that window ends at most 5 minutes
after the read started. See [Races the job cannot close](#races-the-job-cannot-close).

A deleted folder therefore means "no table or database visible to this job, in the places checked,
claims this folder". It does not prove that nothing else uses the folder. The job does not check:

| Not checked | Example | What to do |
|---|---|---|
| Catalogs outside the job's Spark session | A catalog of another data plane or tool with a table whose `LOCATION` is in this database's folder | Review the dry-run candidates. Protect the folder with `exclude_paths`. |
| Tables the job cannot see | Tables the job's run-as user is not allowed to see. A catalog that may hide tables from its credential holds every folder as `catalog_visibility_unverified` instead | Run the job as a user who can see every table in the catalog. |
| Tables of nested namespaces | A table in a namespace inside another namespace | Protect its folder with `exclude_paths`. |
| Files a table uses outside its own folder | After `ALTER TABLE ... SET LOCATION`, the existing files stay in the old folder and the table still reads them. Files added with `add_files`, or used by a table made with `snapshot` or `migrate`, stay where they were. | Protect the old or source folder with `exclude_paths` while any table still uses it. |

Deletion mode is only as safe as these preconditions:

- The job's run-as user can see every database and table in the catalog.
- No other catalog, data plane or tool writes tables into this database's folders.
- Tables keep their files inside their own folder or their declared Iceberg write paths.

If any of these may not hold, run in dry-run, review the candidates, and protect what must stay with
`exclude_paths`.

Every audit row states which of these checks ran for its database, in `diagnostic_details`.
`ownership_checks` lists the checks that ran, and `ownership_not_checked` lists the rest. A check that
was skipped or failed is listed as not checked, so a row never claims a check that did not run. When the
storage of other databases is unknown, `ownership_unreadable_sample` says why.

| In `ownership_checks` | Listed when |
|---|---|
| `visible_tables_of_this_database`, `write_paths_of_visible_tables_of_this_database`, `configured_exclusions` | Always. They are part of every folder comparison. A row that stops before comparing folders has no candidates. |
| `default_folders_of_visible_other_databases` | The locations of other databases were read. They are read only when the cleanup root has at least one folder. |
| `visible_tables_of_other_databases`, `visible_storage_of_other_session_catalogs` | The tables of other databases and catalogs were read. They are read only when a folder has no owner after the checks above. |
| `final_recheck_before_deletion` | Other storage was read again by the [deletion fence](#deletion-fence) before the first folder was deleted. This happens only in deletion mode. |

When one of the last three rows is missing, `ownership_not_checked` names it instead, as
`default_folders_of_other_databases`, `other_database_tables` and `other_session_catalogs`, or
`final_recheck_before_deletion`. `ownership_not_checked` always has
`catalogs_outside_session`, `hidden_tables`, `nested_namespace_tables` and `files_outside_table_folders`.

Each `CLEANUP REPORT` in the log starts with an `OWNERSHIP CHECKS` section that says the same for its
database.

---

## Storage holds

A folder that no visible table, database or exclusion claims is not deleted on that alone. What is stored in
it must show that it is safe, and earlier runs of the same job must have seen it unchanged. A folder that
fails a check is kept and named in the audit row under its reason. Dry runs make the same checks, so their
rows show what deletion mode would keep.

### What the folder holds

The job sorts each folder by the listing it already made:

- A folder with any Iceberg metadata file (`*.metadata.json`, `*.gz.metadata.json` or
  `*.metadata.json.gz`, at any depth) is a metadata folder. The job reads the start of every one of these
  files, at most 1 MiB of each. The folder is deleted only if every file is a table's, with a table UUID
  and this folder as its `location`.
- A folder with a HadoopTables `version-hint.text` but no metadata file is kept as `metadata_unreadable`.
- Any other folder is data-only. It is deleted only if it holds nothing but commit scratch: `_temporary/`,
  `__magic*/`, `.spark-staging-*/` or `.hive-staging*/` at the top of the folder, and a top-level
  `_SUCCESS` or `*.crc`. A path through `_delta_log`, `_spark_metadata`, `.hoodie`, `_metadata`,
  `_common_metadata` or a partition directory (a name with `=`) is never scratch.

| Reason | Kept because |
|---|---|
| `metadata_view` | A metadata file describes a view. A view can read any path. |
| `metadata_no_uuid` | A metadata file has no table UUID. |
| `metadata_location_mismatch` | A metadata file names another location, so its table may live elsewhere and read this folder. |
| `metadata_unreadable` | A metadata file could not be read or parsed, its start is over 1 MiB, or only a version hint is left. |
| `data_only_unknown_provenance` | Plain Spark output (`part-NNNNN*`, `_SUCCESS`, `*.crc`, partitions allowed) with no metadata. A path-based table or job may still read it. |
| `data_only_not_plain_output` | Data-only files that are neither scratch nor plain Spark output, or another format's state such as `_delta_log`. |
| `inspection_incomplete` | The folder's listing or fingerprint is missing. |

These folders stay kept on every later run. To delete one, remove it by hand after review, or protect it
with `exclude_paths` to stop it being reported.

### Confirmation over 72 hours

Every row written after the folders were inspected records the fingerprint of each inspected folder in
`candidate_fingerprints`, before the age check, and the `run_id` of the previous row of the same job,
catalog and database in `prev_run_id`. A folder is deleted only when:

- an earlier `SUCCESS` row of the same `external_job_id`, catalog and database ended at least 72 hours
  before this run listed the folder, and saw it with the same fingerprint;
- every row of that job and database since then saw it with the same fingerprint, did not delete it, and
  names the row before it in `prev_run_id`;
- no deletion intent row since then names it.

A dry-run row confirms as well as a deletion-mode row, so a job run in dry-run mode for more than three
days can delete on its first deletion-mode run. The 72 hours run from the end of the earlier run to the
listing of this one, so a daily job confirms on its fourth or fifth run, not its third. A row that inspected nothing, such as a `too_many_folders_to_inspect`
or `unexpected_error` row, breaks the chain, and the 72 hours start again. A `too_many_candidate_folders`
row inspected its folders, so it keeps the chain. Rows older than 30 days are
not read.

| Reason | Kept because |
|---|---|
| `awaiting_confirmation` | Earlier runs have not yet seen the folder unchanged for 72 hours. |
| `confirmation_no_job_id` | `IOMETE_EXTERNAL_JOB_ID` is not set, so no earlier run can be matched to this job. |
| `confirmation_history_unreadable` | Earlier rows could not be read, or the audit table cannot be trusted to hold every row: it is not an Iceberg table, a history column is missing or of another type, or write-audit-publish (`write.wap.enabled`, `spark.wap.id`, `spark.wap.branch`) is on. |
| `confirmation_path_not_exact` | The path changes when it is written to the audit table, for example a very long path or one with a control character, so it cannot be matched. |

History never adds a folder. It can only confirm a folder this run listed and checked.

### Deletion intent

In deletion mode, before the first delete of a database, the job writes a row with `status=INTENT` and
`status_reason=deletion_intent`. Its `candidate_folders` lists the folders about to be deleted. The job
then reads the row back. If the write or the read fails, nothing is deleted in that database and its row
is `BLOCKED` with `status_reason=deletion_intent_not_recorded`. An intent row is not an outcome. The
database's outcome is the other row of the same `run_id`. If that row is missing, the run ended during
deletion, and later runs do not confirm the folders the intent names.

### Limits

These checks guard against mistakes, not against someone who can write to the audit table. A deleted or
rewritten newest row cannot be detected. The metadata check costs one read of up to 1 MiB per metadata
file in each folder that passed the other checks.

---

## Fail-closed behavior

Catalog and object-storage discovery failures are not treated as empty results.

For example, if the job cannot list storage folders or cannot discover catalog metadata, it does not assume there are zero candidates. Unknown state is not considered safe for destructive cleanup.

The same rule covers storage configuration. A catalog whose storage configuration cannot be
resolved fails the database instead of falling back to the platform's storage. A candidate folder
that cannot be listed completely is skipped as `statistics_unavailable` and never deleted. If every
candidate folder that still exists fails to list, the database fails rather than producing a run
that looks successful.

A folder is treated as absent (`folder_not_found`) only when Hadoop reports it as not found and a
second lookup confirms it. Any other error, including access denied, stays a listing failure. If
the second lookup finds the folder, it also stays a failure. Absent folders do not count toward
the "every candidate folder failed" check, so a database whose candidate folders were all deleted
since they were listed does not fail; with no other block, it records `SUCCESS` with no candidates.

In deletion mode, each folder is checked again right before it is deleted, against an ownership read
that is at most 5 minutes old, and only the objects listed in that check are deleted. A check that cannot be done, or does not finish in time, stops deletion for
the database. See [Deletion fence](#deletion-fence).

Outcome meanings:

- `SUCCESS` means the database was processed normally.
- `SKIPPED` means a guardrail intentionally stopped cleanup for that database.
- `BLOCKED` means reconciliation completed far enough to inspect catalog and storage state, but a
  safety condition prevented cleanup. The reconciliation result is still reported.
- `FAILED` means an unexpected error occurred and should be investigated. When deletion itself
  fails partway, some folders may already be gone, so a `FAILED` row can have
  `deleted_folder_count` greater than `0`.
- `INTENT` is not an outcome. It is written in deletion mode before the first delete of a database. See
  [Deletion intent](#deletion-intent).

This distinction matters when a single job scans multiple databases. Each configured database gets its own audit row, and the shared run ID groups those rows together.

---

## Deletion fence

In deletion mode, the job checks each candidate folder again right before it deletes it. The first
ownership read can be minutes old by then, and storage can change while the job runs.

### Ownership read

Before the first folder of a database, the job reads ownership again. It reads the storage of other
databases, catalogs and tables, as at the first read, and the tables of this database. Nothing is
reused from the first read. What any read protects stays protected. So a folder is checked against
everything that any of these reads found.

The job reads ownership again when the last read started more than 4 minutes ago. That is the 5-minute
limit minus twice the 30-second start margin, so the status read before the next delete still fits.
It checks this before each later folder, and inside a folder before each status read of an object or
directory. A read inside a folder checks that folder again, as steps 2 and 3 below do. If the folder
is now protected and nothing in it was deleted yet, it is kept with the same reason as before its
listing, and the job goes on with the next folder. If some of its objects were already deleted,
deletion stops with `claimed_during_deletion`. Those objects stay deleted, and the rest stay in
`remaining`. A read that takes more than 2.5 minutes, half the limit, stops deletion with
`recheck_too_slow`, and the job waits for a read no longer than that. A longer read would leave little
time to delete, and the next read would come soon after. The job measures this age on a clock that a change of the system time cannot move. A
table created in a candidate folder after the last read is not seen before deletion when it is in
another database, or has another name than the folder. See [Time limit](#time-limit) for how long that
window lasts.

### Checks for each folder

For each folder, in path order, the job:

1. Reads ownership again, if the last read started more than 4 minutes ago.
2. Looks up a table of this database with the folder's name, and reads its storage as the full read
   does. If its location or one of its Iceberg write paths is in the folder, the folder is kept. If its
   storage cannot be established, for example because `gc.enabled` is not true or a location provider
   is set, deletion stops with `name_probe_failed`.
3. Keeps the folder if it overlaps anything an ownership read protects.
4. Lists the folder again, up to 10,000 objects. A folder with more is kept.
5. Keeps the folder if this listing differs from the one taken at inspection, if an object has no
   usable modification time, or if an object is newer than the cutoff of this run.
6. Reads every listed object back by its own name. If one is not found, or two listed objects have the
   same name, the folder is kept and nothing in it is deleted.
7. Deletes the listed objects one at a time. Before each status read here, and in steps 6 and 8, it
   reads ownership again when the last read started more than 4 minutes ago, as above. Before each
   delete, it reads the object's status again.
   The size and ETag must match the listing, and the modification time must match to the second. Right
   before the delete, it checks that at least 30 seconds of the ownership read's 5 minutes are left. It
   waits for the delete only until the ownership read is 5 minutes old.
8. When every listed object is gone, removes the folder's empty directories from the bottom up, one at
   a time. It lists each directory first. It looks at no more than 10,000 entries in all, and counts each
   entry it lists in this step once. A directory whose delete is refused, or that is not empty, is
   looked at again. If it is still there and holds no object, it is kept, and the folder still counts as
   deleted. The row names it in `deleted_folder_kept_directory_*`. If the second look finds an object, the
   folder is partly deleted, and the row names the object as unlisted. The second look also lists every
   directory it finds there, at any depth, because each one appeared after the job removed or passed it.
   A new empty directory is named as an unlisted object too. A kept directory also keeps every directory
   above it, and those are not looked at again. An object at the folder's own name keeps the folder with no
   delete. So when the folder is still there at the end and nothing new was found, the job lists it once
   more, at any depth, and names every object below it. Kept directories are not named or counted again.
   If an object now has the folder's own name, this listing stops the step, as the first listing does. If
   this step cannot finish, the folder is partly deleted and deletion stops. That happens when:
   - a listing, status read or delete of a directory fails or does not finish in time;
   - a read would start after the ownership read is 5 minutes old, or a delete in its last 30 seconds;
   - the listing of a directory returns an object with the directory's own name, so the job cannot see
     what is below it;
   - the 10,000-entry limit is reached.

   A directory delete that threw an error or was not waited for is in an unknown state, because it may
   still complete. It is never sent again.

The job never sends a recursive delete. It deletes only objects it listed, with two exceptions. The
first is a directory marker, the object whose key ends in `/` that S3 keeps for a directory. The job
finds markers in step 8, not in its listing, and does not compare their age with the cutoff. A marker
with content is deleted too, because S3A treats any key that ends in `/` as a directory. The second is
an object written at a directory's exact name while that directory is deleted. See
[Known limitations](#known-limitations). The cutoff in step 5 is the one on the row:
`older_than_hours`, and in deletion mode at least 72 hours.

A folder that is gone when deletion starts, or whose new listing finds no object, is recorded as
`folder_not_found_at_deletion` and deletion continues. Its directory markers stay, as for
`empty_folder`. These folders stay counted as candidates, not as deleted.

### Kept folders

A kept folder is not deleted, and nothing in it is touched. It stays counted as a candidate, not as
deleted, and the job goes on with the next folder. With no other problem, the row is `SUCCESS`. Each
reason has its own `folder_kept_<reason>_count`, `_paths_sample` and `_paths_truncated` keys in
`diagnostic_details`, and its own line in the log report under `DELETION`.

| Reason | Meaning |
|---|---|
| `folder_kept_at_final_check` | An ownership read found storage of another database, catalog or table in the folder. |
| `folder_kept_table_named_like_folder` | A table of this database with the folder's name is stored in the folder. |
| `folder_kept_changed_since_inspection` | The listing before deletion differs from the listing taken at inspection. |
| `folder_kept_too_many_objects` | The listing before deletion found more than 10,000 objects. |
| `folder_kept_too_new` | An object in the listing before deletion is newer than the cutoff of this run. |
| `folder_kept_unknown_age` | An object in the listing before deletion has no usable modification time. |
| `folder_kept_key_not_addressable` | Before the first delete, a listed object was not found by its own name, or two listed objects had the same name. |

### Failures

The job stops deleting for that database and records `FAILED` with `status_reason=deletion_failed`
when:

- the listing before deletion failed;
- a listed object changed before its delete;
- a listed object disappeared after the job read it back in step 6;
- a status read of an object failed;
- an object delete failed or was refused;
- a delete threw an error or did not finish within the time limit. That object is reported in an
  unknown state, because the delete may still complete;
- an object that was not in the listing turned up in the folder after the listed objects were deleted;
- removing the empty directories in step 8 failed: a listing or status read of a directory failed, a
  directory delete threw an error or did not finish within the time limit, or more than 10,000 entries
  were found. Such a directory delete is reported in an unknown state, because it may still complete.

A folder stopped this way is not counted as deleted, even when some of its objects are gone. See
[Partial deletion](#partial-deletion) for what the row says about it. A delete that throws an error is
never treated as a missing object.

### Stops

The job also stops when a check that the next delete needs cannot be done. The row names the cause in
`deletion_stop_cause`:

| Cause | Meaning |
|---|---|
| `other_storage_incomplete` | An ownership read of other databases, catalogs or tables failed, or found storage it could not establish, such as an unresolved table. |
| `own_database_recheck_failed` | The tables of this database could not be read again. |
| `own_tables_unresolved` | A table of this database could not be resolved to a storage location at an ownership read. |
| `recheck_too_slow` | An ownership read took longer than 2.5 minutes, half the 5-minute limit. A read still running at 2.5 minutes is not waited for, and its result is ignored. |
| `claimed_during_deletion` | Ownership read again in the middle of a folder protects that folder after some of its objects were deleted. Those stay deleted, and the rest stay in `remaining`. Before the first delete, the folder is kept instead. |
| `name_probe_failed` | The lookup of a table named like the folder failed, found a location or write path with no scheme and bucket, or found a table whose storage cannot be established. |
| `snapshot_expired_during_deletion` | The ownership read became more than 5 minutes old while a folder was being deleted, including while its empty directories were removed, or less than 30 seconds of the 5 minutes were left before the next delete. |
| `read_timed_out` | A storage or catalog read needed before the next delete was still running when the ownership read reached its 5-minute limit, or a delete or read was not started because no time was left. The job stopped waiting and ignored the result. |

What the row says depends on whether anything was deleted:

- If a folder or an object was deleted, or a delete may still complete, the row is `FAILED` with
  `status_reason=deletion_stopped_at_recheck`. `deleted_folders` lists the folders deleted before the
  stop. The row also has `deletion_stop_cause`, `ownership_snapshot_max_age_seconds=300`, the folders
  not tried in `folder_not_attempted_*`, and the partly deleted folder, if there is one. See
  [Partial deletion](#partial-deletion).
- If nothing was deleted, the row is the same as when the first read finds the same problem. An
  ownership read that finished but left the storage of other databases unknown gives `BLOCKED` with
  `other_database_storage_unknown` or `other_table_unresolved`, and this row also has
  `deletion_stop_cause`. A database that is gone gives `SKIPPED` with `database_not_found`. Any other
  stop gives `FAILED` with `status_reason=unexpected_error`, and `error_message` says what stopped.

### Time limit

The job waits for each catalog or storage read in these checks, and for each delete, only until the
ownership read is 5 minutes old. It starts no read after that. It starts no delete in the last 30
seconds before that, so a delete it sends usually returns in time. If a read does not finish in time,
the job stops deleting for that database and ignores the late result. When the 30 seconds are reached,
the folder stops partly deleted with `snapshot_expired_during_deletion`. The object that was next stays
in `remaining`.

A delete call is more than one request. Inside it, the storage client first reads the object's status,
then sends the delete request, and then checks the parent directory. It may retry each of them. So the
request can go out at any time until the call returns. The 30 seconds are not a bound: a call can take
longer, for example while the client retries. A delete that returned in time counts as deleted, so it
returned before the ownership read was 5 minutes old. A delete still running at that point is not
waited for. Its object is reported in an unknown state, the row is `deletion_failed`, and deletion
stops for that database.

The job only stops waiting. The call itself may keep running in the background, and such a delete may
still complete later, even after the job ends. The job has no hard time limit on a delete, and there
is no bound on when such a delete lands. Turn on bucket versioning so such an object can be restored.
See [Versioned buckets](#versioned-buckets).

These waits are bounded this way:

- the ownership read, and the read of this database's tables, for at most 2.5 minutes from the start of that read;
- the lookup of a table named like the folder;
- the listing before deletion, and every status read of an object;
- every object delete;
- the listing, status reads and deletes of empty directories. Once the ownership read is more than 5
  minutes old, none of these starts, and no directory delete starts in its last 30 seconds. The folder
  is then partly deleted, and its empty directories stay.

These are not bounded by the job: opening and closing the storage connection, the audit write, and
everything before deletion starts, such as the first ownership read and the inspection listings. The
job sets no timeout or retry setting of the storage client or the catalog client. Their own limits
apply inside each call.

The 5-minute limit is fixed in the code. It and the 30-second margin are provisional: neither has been
measured on a real deployment yet. A full ownership read has to finish within 2.5 minutes, half the
limit. If it does not, every deletion run stops at the first read with `recheck_too_slow`, and nothing
is deleted. After a read, deleting goes on until the read is 4 minutes old, and the job then reads
again. So a read of R minutes leaves about 4 minus R minutes of deleting per read. To see
how long a read takes, look at the log line "Checked N table(s) of other databases ... in X ms". It
leaves out the reads of database locations. `ownership_refresh_count` on the row says how many times
ownership was read again during deletion.

### Object limit

The limit of 10,000 objects per folder is provisional. It is fixed in the code, and it may change once
deletion has been timed on real storage. No setting raises it.

A folder above the limit is held, not deleted, for two reasons:

- Time. The job deletes object by object, with about five storage requests per object. If each request
  takes 5 to 50 ms, 10,000 objects take about 4 to 42 minutes. The job starts no delete in the last 30
  seconds of its 5-minute ownership read, so a slow folder stops partly deleted, which is worse than
  kept. At 100,000 objects, almost every such folder would stop partly deleted. The removal of empty
  directories after the objects has to finish in the same time. It costs a few requests per directory,
  so a folder with many directories can stop partly deleted even below 10,000 objects.
- Memory. The job keeps every listed key of the folder while it deletes it: about 9 MiB for 10,000
  objects, and about 82 MiB for 100,000.

A held folder is reported as `too_many_objects` at inspection, or as `folder_kept_too_many_objects` at
the listing before deletion. Nothing in it is touched. To clear one, check that no table uses it, then
delete it with a storage tool, or protect it with `exclude_paths` so it is no longer reported.

### Races the job cannot close

The fence makes the time in which a new owner can be missed short. It cannot make it zero. No catalog
read says which table uses a given path, so each check can only compare the folder with the last full
read.

| Race | Window | What the job does | Status |
|---|---|---|---|
| A table or database registered in a candidate folder after the last ownership read | From the start of that read until each delete request lands. For a delete the job counted as done, that is at most 5 minutes. A delete the job stopped waiting for can land later, with no bound. | A table of this database with the folder's name is kept if its location or a write path is in the folder when the job looks the name up. One whose storage cannot be established stops deletion. A new object written before the listing keeps the folder as changed. A new object written after the listing is never deleted, but the listed objects are. When the removal of empty directories finds the new object, the folder is reported partly deleted and the row names it as unlisted. One written after the job last looked at that place is not found, and the folder is reported deleted. When that step does not finish, the row has `partially_deleted_folder_directory_cleanup=not_finished` and may not name it. Any other new owner is not seen, and the folder is deleted. | Residual. Fixed for the name lookup and a new object before the listing. Fails closed for a new object after it. |
| A table that takes over the files already in a folder and writes nothing new: `CREATE TABLE ... LOCATION`, `register_table`, `ALTER TABLE ... SET LOCATION` | The same window. Before the last read, the folder is kept. | No new object shows the change, so the folder is deleted. | Residual |
| `add_files`, or a table that reads files outside its own folder | The whole run | Never seen. Protect such folders with `exclude_paths`. | Residual |
| A catalog outside the job's Spark session, a table the job cannot see, or another engine that writes to the bucket | The whole run | Never seen. See [What deletion mode can and cannot prove](#what-deletion-mode-can-and-cannot-prove). | Residual |

Residual means the job cannot close the race. A read that fails, times out, is incomplete or cannot be
compared always stops the remaining folders, so those cases fail closed and are not in this table.

Reading everything again before each folder would not close the first two races. A full read takes
minutes, and an owner can still be registered after its database was read. Run deletion when no one
creates or moves tables in these folders, and turn on bucket versioning so a deleted object can be
restored. See [Versioned buckets](#versioned-buckets).

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
| `invalid_storage_location` | A database, table, or warehouse location cannot be parsed, a database or warehouse location has no scheme or bucket, or a database is on a storage scheme the job does not support, such as HDFS |

The row keeps this status and reason. For `scan_root_outside_database_boundary`,
`database_location_is_bucket_root` and `database_location_contains_warehouse_root`, the job may still list
[report-only roots](#additional-storage-roots-report-only) whose own checks pass, and adds them to
`diagnostic_details`. A rejected bucket-root or warehouse-root database location is never one of them.
Nothing is deleted.

When tables sit under more than one parent location, the job falls back to the database location. It adds
the same folder without `.db` as a [second cleanup root](#second-cleanup-root) only when that folder is
proven to be this database's. Table locations outside the scanned roots are not checked in that run, and the
job reports this instead of hiding it:

- The summary log lists them under `TABLE LOCATIONS OUTSIDE THE SCANNED ROOT`.
- A `SUCCESS` audit row gets `status_reason=partial_scan_coverage`.
- A row with a stronger outcome, such as `BLOCKED`, keeps its own reason.
- `diagnostic_details` holds `unscanned_table_location_count` and a bounded sample.
- A `FAILED` row always has `unscanned_table_location_count` once coverage was checked. A value of
  `0` means the scan covered every table location. No count means the job failed before it got
  that far.

The scan root above is the **cleanup root**. Together with a second cleanup root, it is the only place
where folders can be deleted. Deletion is also blocked when no table the job can see is stored under a
cleanup root. See [Cleanup root with no visible table](#cleanup-root-with-no-visible-table).

### Second cleanup root

A database can keep tables in two folders: its location `<warehouse>/<database>.db`, and the REST
namespace folder `<warehouse>/<database>`. When the tables sit under more than one parent location, the
job adds `<warehouse>/<database>` as a second cleanup root only when all of these hold:

- the database location ends in `.db`, and the other folder is the same path without `.db`;
- the catalog derives `<warehouse>/<database>` as this database's REST namespace folder
  (`rest_namespace_root=derived`);
- at least one table the job can see has its verified location inside that folder, and no table
  location is that folder or above it. A folder named only for an unresolved table never counts.

If the database location holds no visible table, the second folder is the only cleanup root. Otherwise
both are. The bucket-root and warehouse-root checks above apply to both folders. No setting turns this on
or off.

With two cleanup roots:

- Each folder is listed one level deep. A folder that does not exist has no candidates. A folder counts as
  existing when any object sits under it, so a folder without a directory marker object is still found.
- If one folder cannot be listed, nothing in it is deleted and the other folder is still cleaned. Coverage
  is `partial`, and the folder shows `listing_failed`. If neither folder can be listed, the row is `FAILED`.
- Folders due in both count toward one `max_candidate_folders_per_database` limit, and unmatched folders of
  both count toward one inspection limit.
- Exclusions, the [checks of other databases and catalogs](#tables-of-other-databases-and-catalogs), their
  final read and the pre-delete recheck apply to both folders. An unresolved table, or other storage that
  cannot be read under either folder, blocks deletion in both.
- `storage_scan_location` is the first folder that was listed. `diagnostic_details` lists both.

When the second folder does not qualify, it stays a [report-only root](#additional-storage-roots-report-only),
and `managed_root_status` gives the reason.

Upgrading from 0.1.5: a database with tables in both folders now also deletes orphans in
`<warehouse>/<database>`, and the candidate limit counts both folders. Run dry-run first and review the
candidates.

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
changes what is deleted. In the log report, a root that exists but holds no child folders, such as a
database folder kept only as a marker object, is shown as `scanned, no folders`. The audit status stays
`scanned`.

The log report also shows `visible tables at any depth` for each listed root. It counts this database's
visible tables stored anywhere under that root, not only its direct children. Roots that are not scanned
show no count, because a broad root such as a bucket root would count every table of the database. Folders in these roots that do not belong to a table the job can see are listed
under `FOLDERS IN REPORT-ONLY ROOTS NOT MATCHED TO A VISIBLE TABLE`. A table the job cannot see may
still own them, so treat them as items to review, not as orphans.

Approval never overrides these checks, and a root listed because of approval is shown with the role
`report_only_approved`. The log prints at most 25 not-scanned roots. The audit counts are always complete.

### JDBC tables

A table that uses Spark's JDBC data source keeps its rows in another database. The metastore still gives
it a default location, usually `<database location>/<table>`, but no files are stored there. The job treats
a table as a JDBC table when its provider is one of these, in any letter case:

- `jdbc`
- `org.apache.spark.sql.jdbc` or `org.apache.spark.sql.jdbc.DefaultSource`
- `org.apache.spark.sql.execution.datasources.jdbc`, or the same name followed by `.DefaultSource` or
  `.JdbcRelationProvider`
- `com.microsoft.sqlserver.jdbc.spark`, the SQL Server connector

Every other provider, including one the job does not recognize, counts as a table that stores files.

A JDBC table does not choose the scan root, the cleanup roots or the report-only roots. It is not the
visible table a cleanup root needs, and it does not count toward coverage. Its location stays protected: a
folder there is never a candidate, is not listed under report-only roots, and is checked again right before
deletion. A JDBC table of another database or catalog protects its location the same way.
`active_table_count` and `active_table_locations_sample` still include JDBC tables.

Upgrading from 0.1.5 can change the result for a database with JDBC tables, in both directions:

- Folders that were not cleaned before can become candidates. When the tables that store files all sit
  in one folder, that folder becomes the cleanup root even if a JDBC table sits elsewhere, and its child
  folders can be deleted. Check that `exclude_database_folders` still names the folders you want to keep.
- A database can be skipped or blocked, or clean less. A database with only JDBC tables is `SKIPPED` with
  `no_active_tables_in_database`. A cleanup root that holds only JDBC tables gives `BLOCKED` with
  `cleanup_root_has_no_visible_table`. If the other tables sit outside the database's storage, the database
  is `SKIPPED` with `scan_root_outside_database_boundary`. A `<database>.db` folder that was a cleanup root
  only because of a JDBC table becomes report-only.

Run the job with `dry_run=true` after upgrading, and review the candidates and audit rows before deleting.

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
- Fingerprints of the inspected folders and the previous row's run ID

### How audit values are written

The job writes each row with an `INSERT` statement. Values are stored exactly as the job had them:

- A quote or a backslash in a text value is escaped with a backslash. Older versions of this job
  dropped single quotes from stored text.
- A `$`, a `*` and any control, format or line separator character is written as `\uXXXX` in the
  statement text. Spark reads it back as the same character, and the statement text never holds it raw.
- Times are written with an explicit UTC offset, for example `TIMESTAMP '2026-05-21 10:00:00.123456Z'`.
  The stored instant does not depend on the driver's time zone or on `spark.sql.session.timeZone`.
  Older versions wrote the driver's local time without a zone.
- Spark stores text as UTF-8. A lone UTF-16 surrogate, which UTF-8 cannot hold, is stored as `?`.

The job sets `spark.sql.variable.substitute=false` and `spark.sql.parser.escapedStringLiterals=false`
in its Spark session, and checks that both hold each time before it sends SQL. If a setting does not
hold, no SQL is sent and the run ends with exit code `1`. With substitution on, Spark would replace
`${...}` in names and values before it reads the statement.

Tests read the job's `INSERT` text with Spark 3.5's own SQL parser and these two settings, and check that
every value comes back exactly. No extension parser runs in the tests: not the platform's Iceberg fork,
not Nessie and not Ranger. A session extension can put its own parser in front of Spark's. The platform
turns on the Iceberg and Nessie extensions, and the Ranger extension when Ranger is on. From reading
IOMETE's own extension source at v3.5.7, it adds no parser that changes how text values are read. That
was not tested. The Iceberg and Nessie parsers are expected to handle only their own commands and pass an
`INSERT` on unchanged. The statement holds no raw `$`, so a substitution step in front of Spark's parser
finds nothing to replace. Check T51 is there to confirm the whole chain on the target image before
release. It has not been run yet.

Reading the statement takes time, and odd names can make it slow. The Iceberg extension runs a comment
pattern over the whole statement, and that pattern is slow on a `/*` with no `*/` after it. The job
writes `*` as `\u002A`, so the text holds no `/*`. Spark's own parser reads to the end of a value at
each escape in it, so a long value with many escapes takes time that grows with both. Ordinary names
need few escapes. A row whose samples hold many long names full of quotes, backslashes or hidden
characters can take seconds or more to read. The row is written after the database's deletes, so a
slow write delays the run but does not sit between a check and a delete.

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
| `status` | High-level outcome: `SUCCESS`, `SKIPPED`, `BLOCKED`, or `FAILED`. `INTENT` marks a [deletion intent](#deletion-intent) row, which is not an outcome. |
| `status_reason` | More specific reason for the outcome, such as `database_not_found`, `database_location_missing`, `too_many_candidate_folders`, `too_many_folders_to_inspect`, `unresolved_catalog_ownership`, `database_location_contains_warehouse_root`, `partial_scan_coverage`, `deletion_failed`, `deletion_stopped_at_recheck`, `audit_integrity_lost`, `deletion_intent_not_recorded`, `deletion_intent`, or `unexpected_error`. See [Object-storage scan root](#object-storage-scan-root) for the scan-root reasons. |
| `error_message` | The error that ended processing of this database, cleaned and cut to at most 1000 characters. It names the classes of the causes but not their messages. See [Error messages and logs](#error-messages-and-logs). |
| `older_than_hours` | The age threshold the run used for candidate detection. In deletion mode this is at least 72, even when the configured value is lower; the configured value is in `diagnostic_details` as `configured_older_than_hours`. See [72-hour minimum in deletion mode](#72-hour-minimum-in-deletion-mode). |
| `cutoff_time` | Calculated timestamp used to decide whether a folder is old enough to be considered. Null when no age check ran. |
| `max_candidate_folders_per_database` | Configured limit on the folders due for deletion in one database. |
| `unresolved_table_count` | Number of catalog tables in this database whose storage location could not be read during discovery. Reported whatever the row's `status_reason` is, so filter on `unresolved_table_count > 0` to find affected databases. It is `0` when the count is unknown: when discovery itself failed, or when the pre-delete recheck found new unresolved tables, in which case the number is in `error_message`. Both are `FAILED` rows, so check `FAILED` rows as well. |
| `candidate_folder_count` | Number of folders confirmed as untracked and eligible for deletion. Reserved for confirmed deletion-eligible candidates, so it stays `0` when table storage ownership could not be fully verified. |
| `potentially_untracked_folder_count` | Number of storage folders that matched no verified table location while at least one table's storage location was unresolved. These folders are reported for review only. They are not confirmed untracked and are never deletion candidates, which is why they are counted separately from `candidate_folder_count`. On a `too_many_folders_to_inspect` row this is the count that exceeded the inspection limit, before any age check. |
| `candidate_object_count` | Number of objects found under candidate folders. |
| `candidate_total_size_bytes` | Total size in bytes under candidate folders. |
| `deleted_object_count` | Number of objects under folders that were actually deleted. |
| `deleted_total_size_bytes` | Total size in bytes under folders that were actually deleted. |
| `diagnostic_details` | Job-specific diagnostic/debug details such as path samples and truncation flags. This is not Spark metrics and not Iceberg metrics. |
| `candidate_fingerprints` | Fingerprint of each folder inspected by this row, by path, taken before the age check. Null on rows that inspected no folder. Read by later runs for [confirmation](#confirmation-over-72-hours). |
| `prev_run_id` | `run_id` of the newest earlier row of the same `external_job_id`, catalog and database, as this run read it. Null when there was none or it could not be read. |

### Diagnostic details field

The `diagnostic_details` field is a flexible audit/debug details map populated by this job.

It is not Spark metrics and it is not Iceberg table metrics.

Stable audit values such as `status_reason`, `older_than_hours`, `cutoff_time`, and `max_candidate_folders_per_database` are stored as explicit audit columns. `diagnostic_details` is reserved for extra diagnostic context that is useful during investigation but should not be treated as the primary schema.

It may include values such as:

```text
ownership_checks
ownership_not_checked
configured_older_than_hours
deletion_min_age_hours
ownership_unreadable_sample
ownership_unreadable_truncated
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
folder_not_found_count
folder_not_found_paths_sample
statistics_unavailable_count
statistics_unavailable_paths_sample
empty_folder_count
empty_folder_paths_sample
unknown_age_count
unknown_age_paths_sample
too_many_objects_count
too_many_objects_paths_sample
too_new_count
too_new_paths_sample
<storage_hold>_count
<storage_hold>_paths_sample
held_folder_count
held_folder_counts
folder_not_found_at_deletion_count
folder_not_found_at_deletion_paths_sample
folder_kept_<reason>_count
folder_kept_<reason>_paths_sample
folder_not_attempted_count
folder_not_attempted_paths_sample
deletion_failed_folder_path
partially_deleted_folder_path
partially_deleted_folder_listed_object_count
partially_deleted_folder_<set>_object_count
partially_deleted_folder_<set>_keys_sample
partially_deleted_folder_directory_cleanup
partially_deleted_folder_kept_directory_count
partially_deleted_folder_kept_directory_paths_sample
partially_deleted_folder_unknown_state_directory_count
partially_deleted_folder_unknown_state_directory_paths_sample
deleted_folder_kept_directory_count
deleted_folder_kept_directory_paths_sample
deletion_stop_cause
ownership_snapshot_max_age_seconds
ownership_refresh_count
would_be_held_by_deletion_floor_count
would_be_held_by_deletion_floor_paths_sample
due_for_deletion_count
due_for_deletion_paths_sample
*_truncated
```

`<reason>` is one of the [kept folder reasons](#kept-folders) without the `folder_kept_` prefix, such as
`at_final_check` or `key_not_addressable`. `<storage_hold>` is one of the [storage hold](#storage-holds)
reasons, such as `metadata_view` or `awaiting_confirmation`. `<set>` is `deleted`, `remaining`, `unknown_state`,
`vanished` or `unlisted`. See [Partial deletion](#partial-deletion). `due_for_deletion_*` is only on a
`too_many_candidate_folders` row and names the folders that were due.

`configured_older_than_hours` and `deletion_min_age_hours` are on every row. They give the configured
age and the 72-hour minimum of deletion mode.

The `would_be_held_by_deletion_floor` keys appear only on dry-run rows, and only when at least one
listed folder is younger than 72 hours. These folders pass the configured age, but deletion mode would
keep them for now. They are still listed as candidates or as potentially untracked folders. They appear
on `SUCCESS` and `BLOCKED` rows.

The `folder_not_found`, `statistics_unavailable`, `empty_folder`, `unknown_age`, `too_many_objects` and
`too_new` keys appear only when at least one folder was skipped for that reason. `too_new` names the
folders whose newest object is newer than the cutoff of this run. They are written on `SUCCESS`,
`BLOCKED`, `deletion_failed` and `deletion_stopped_at_recheck` rows. Folders skipped for these reasons
are never listed as candidates or as potentially untracked folders. A `too_many_candidate_folders` row has
them too. A `too_many_folders_to_inspect` row has none of them, because no folder was inspected.

The `held_folder_count` and `held_folder_counts` keys count these folders and the
[kept folders](#kept-folders) together. See [Held folders](#held-folders).

The `folder_not_found_at_deletion` keys are separate. They name candidate folders that passed
inspection but were gone, or held no object, when deletion reached them. They appear on `SUCCESS`,
`deletion_failed` and `deletion_stopped_at_recheck` rows.

The `folder_kept_<reason>` keys name candidate folders that the [deletion fence](#deletion-fence) kept.
These folders are not deleted and stay counted as candidates. `folder_kept_at_final_check` names the
folders that another database, catalog or table claimed when the job read ownership again before
deleting. See [Tables of other databases and catalogs](#tables-of-other-databases-and-catalogs).

The `folder_not_attempted`, `deletion_stop_cause` and `ownership_snapshot_max_age_seconds` keys appear
when the fence stopped deletion for the database. `deletion_stop_cause` is one of the
[stop causes](#stops), `ownership_snapshot_max_age_seconds` is the 5-minute limit in seconds, and
`folder_not_attempted` names the candidates the job did not reach. `ownership_refresh_count` appears
when ownership was read again during deletion, after the first read of the fence.

Storage root keys follow `scan_plan_status`:

- No `scan_plan_status` means planning was not attempted, for example a database skipped before its
  storage was checked.
- `scan_plan_status=failed` means planning threw. `scan_plan_error` holds the error, and no other storage
  root key is written except `cleanup_roots` and `managed_root_status`. The row keeps the status it would
  have had anyway.
- `scan_plan_status=planned` comes with the keys below.
- `scan_roots_sample` has one line per root: `<location>=<role>:<status>:visible_tables=<n>`, where
  role is `cleanup`, `report_only`, `report_only_approved` or `not_scanned`. Cleanup roots come first,
  then listed roots, then not-scanned roots with the most visible tables first.
- `scan_plan_coverage` is `complete` when every table the job can see is stored under a cleanup root and
  every listing worked. Otherwise it is `partial`. A table under a report-only root still makes it
  `partial`, because folders there are never deleted. An unresolved table also makes it `partial`,
  because its location is unknown, even when a known folder is shown for it. So a `SUCCESS` row with
  `status_reason=partial_scan_coverage` always has `scan_plan_coverage=partial`. Job versions up to 0.1.5
  also counted tables under report-only roots as covered, and did not count unresolved tables.
- Coverage counts only tables the job can see. It is not an ownership check. See
  [What deletion mode can and cannot prove](#what-deletion-mode-can-and-cannot-prove).
- `ownership_basis=visible_catalog_tables` records that ownership was checked only against the tables the
  job can see in the catalog.
- `cleanup_roots` lists the cleanup roots that were listed, one per line. The first one is also
  `storage_scan_location`. `cleanup_root_folder_counts` has one `<location>=<child folders>` line for
  each of them that exists.
- `managed_root_status` says whether `<warehouse>/<database>` is a
  [second cleanup root](#second-cleanup-root): `not_applicable` (every visible table is under the first
  cleanup root, or the database location has no `.db` suffix), `used`, or `refused:<reason>`. The reasons
  are `rest_root_not_derived`, `rest_root_differs`, `no_visible_table`, `table_at_or_above_root` and
  `listing_failed`.

Path lists are sampled to avoid writing very large audit rows.

On a `deletion_failed` row, `deletion_failed_folder_path` names the folder whose deletion failed. The
`partially_deleted_folder_*` keys say what happened to its objects and directories, when any of them was
touched. See [Partial deletion](#partial-deletion).

`deleted_folder_kept_directory_*` names empty directories that are still there in folders that were
deleted, because their delete was refused or they were not empty when the job looked again. Their
listed objects are all gone.

### Held folders

A folder with no verified catalog owner can still be kept by this run: by the age check, by
inspection, by the [storage holds](#storage-holds), or by the deletion fence. The row counts all of them
by reason:

- `held_folder_count` is the number of held folders.
- `held_folder_counts` has one `<reason>=<count>` line per reason, in this order: `statistics_unavailable`,
  `empty_folder`, `too_new`, `unknown_age`, `too_many_objects`, then the [storage holds](#storage-holds)
  (`inspection_incomplete`, `metadata_view`, `metadata_unreadable`, `metadata_no_uuid`,
  `metadata_location_mismatch`, `data_only_unknown_provenance`, `data_only_not_plain_output`,
  `confirmation_no_job_id`, `confirmation_history_unreadable`, `confirmation_path_not_exact`,
  `awaiting_confirmation`), then the `folder_kept_<reason>` reasons.
  Each reason's own keys list its folders.

A folder that was gone is not held, so `folder_not_found` and `folder_not_found_at_deletion` are not
counted here. In a dry run, the folders deletion mode would keep for the 72-hour minimum are still
candidates, so `would_be_held_by_deletion_floor` is not counted either. Both keys appear only when at
least one folder is held.

The log report has a `FOLDERS HELD, BY REASON` section with the same counts. When every folder with no
verified owner is held or gone, so nothing is left to delete, the job also logs one warning:

```text
None of the 4 folder(s) with no verified catalog owner is eligible for deletion for catalog=spark_catalog,
database=sales. Held by reason: empty_folder=1, too_new=1, folder_kept_table_named_like_folder=1.
Already gone: 1.
```

It is one line in the log. It is not logged for a database with an unresolved table
(`unresolved_catalog_ownership`), which gets its own warning, or when deletion failed or stopped.

---

## Status outcomes

| Scenario | Status | Notes |
|---|---|---|
| Database processed successfully | `SUCCESS` | Includes dry-run runs with zero candidates |
| Database processed, but some table locations are outside the scanned root | `SUCCESS` | `status_reason=partial_scan_coverage` |
| Database location is missing | `SKIPPED` | Storage discovery is skipped |
| No table stores files at a known location, for example the database has no tables, or only views or [JDBC tables](#jdbc-tables) | `SKIPPED` | `status_reason=no_active_tables_in_database`; storage is not listed. `active_table_count` still counts the JDBC tables |
| More folders due for deletion than `max_candidate_folders_per_database` | `SKIPPED` | `status_reason=too_many_candidate_folders`; folders were inspected and held folders do not count; nothing is deleted and no intent row is written. A block reason takes precedence |
| Too many unmatched folders to inspect | `SKIPPED` | `status_reason=too_many_folders_to_inspect`; more than the larger of 100 and `max_candidate_folders_per_database`, counted before the age check; nothing is listed. Unresolved tables are reported on the same row |
| Configured database does not exist | `SKIPPED` | Logged as a warning with `status_reason=database_not_found` |
| Database location or scan root is unsafe | `SKIPPED` | Storage is never listed; see [Object-storage scan root](#object-storage-scan-root) for the reasons |
| One or more table storage locations could not be resolved | `BLOCKED` | Reconciliation is still reported when at least one table has a verified location, nothing is deleted, `status_reason=unresolved_catalog_ownership` |
| Unexpected catalog, storage, or deletion failure | `FAILED` | Real failure path remains an error |
| Deletion failed partway through the candidates | `FAILED` | `status_reason=deletion_failed`; see [Partial deletion](#partial-deletion) |
| The deletion fence stopped after a folder or an object was deleted | `FAILED` | `status_reason=deletion_stopped_at_recheck`; see [Stops](#stops) and [Partial deletion](#partial-deletion) |
| The deletion fence kept some folders | `SUCCESS` | Each kept folder is named in `folder_kept_<reason>_paths_sample`; other folders are still deleted; see [Kept folders](#kept-folders) |
| Every folder with no verified owner was held | `SUCCESS` | Nothing deleted; one warning names each reason; see [Held folders](#held-folders) |
| An earlier audit write in the same run failed | `BLOCKED` | `status_reason=audit_integrity_lost`; see [Audit write failures](#audit-write-failures) |
| The deletion intent row could not be written or read back | `BLOCKED` | `status_reason=deletion_intent_not_recorded`; nothing is deleted; see [Deletion intent](#deletion-intent) |
| No visible table is stored under the cleanup root | `BLOCKED` | `status_reason=cleanup_root_has_no_visible_table`; candidates are reported, nothing is deleted |
| The storage of other databases could not be established | `BLOCKED` | `status_reason=other_database_storage_unknown`; candidates are reported, nothing is deleted; see [Storage of other databases and Iceberg write paths](#storage-of-other-databases-and-iceberg-write-paths) |
| A table of another database or catalog is unresolved | `BLOCKED` | `status_reason=other_table_unresolved`; candidates are reported, nothing is deleted; see [Tables of other databases and catalogs](#tables-of-other-databases-and-catalogs) |

A `FAILED` row does not change the job's exit code. The job exits with `1` only when the run itself
stops with an error. See [Error messages and logs](#error-messages-and-logs).

---

## Partial deletion

Candidates are deleted one at a time. The job stops at the first folder it cannot delete and does
not try the remaining candidates in that database.

- A failure is recorded with `status=FAILED` and `status_reason=deletion_failed`. A stop of the
  [deletion fence](#deletion-fence) after something was deleted is recorded with `status=FAILED` and
  `status_reason=deletion_stopped_at_recheck`.
- `deleted_folder_count` and `deleted_folders` list only the folders confirmed deleted before the
  failure or stop, so they can be greater than zero on a `FAILED` row.
- On a `deletion_failed` row, `diagnostic_details.deletion_failed_folder_path` names the folder that
  failed. Check it in object storage before the next run. It is absent when the failure came after the
  last confirmed deletion, for example while closing the storage connection.
- A folder that was already gone when the job reached it is not counted as deleted.
- The failed or stopped folder may be partly deleted. It is never in `deleted_folders`. The error
  message gives its counts: listed objects deleted, kept, in an unknown state, already gone, and
  objects found that were not listed. The objects that were kept are untouched. The job log names each
  deleted object at DEBUG level.
- A delete that reports no change for an object that still exists is treated as a failure, because
  the job cannot tell what happened to it.
- A delete that threw an error or did not finish within the time limit is never sent again. Its object
  is in an unknown state, because the delete may still complete, even after the job ends.
- A folder whose listed objects are all gone is still partly deleted when the removal of its empty
  directories did not finish. A directory delete that threw an error or was not waited for is in an
  unknown state, as for an object.

The same sets are in `diagnostic_details`, when at least one object of the folder was touched:

| Key | Meaning |
|---|---|
| `partially_deleted_folder_path` | The folder. |
| `partially_deleted_folder_listed_object_count` | The number of objects in its listing before deletion. |
| `partially_deleted_folder_deleted_*` | Listed objects confirmed deleted. |
| `partially_deleted_folder_remaining_*` | Listed objects not tried, or still there after their delete returned. |
| `partially_deleted_folder_unknown_state_*` | Listed objects whose delete may still complete. |
| `partially_deleted_folder_vanished_*` | Listed objects that were gone before the job deleted them. |
| `partially_deleted_folder_unlisted_*` | Objects found in the folder that were not in the listing, and new empty directories found below it. |
| `partially_deleted_folder_directory_cleanup` | `not_run` when not every listed object was deleted, so no directory was looked at; `finished`; or `not_finished` when that step stopped. Unless it is `finished`, an object written into the folder after the listing may be missing from `unlisted`. Even when it is, one written after the job last looked at that place is not in it. |
| `partially_deleted_folder_kept_directory_*` | Directories that are still there: not empty, refused, the directory where the step stopped, and its parents. Directories after that one were not looked at and are not listed. |
| `partially_deleted_folder_unknown_state_directory_*` | Directory deletes that threw an error or were not waited for, so each may still complete. |

Each set has an `_object_count`. When it is not empty, it also has a `_keys_sample` of up to 100 keys,
relative to the folder, one per line, and a `_keys_truncated` flag. Each listed object is in exactly
one of `deleted`, `remaining`, `unknown_state` and `vanished`. Each directory key has a `_count`, and
when it is not empty, a `_paths_sample` of full paths and a `_paths_truncated` flag.

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

## Error messages and logs

Error text can hold connection details, so the job cleans it before it writes it to the log or the
audit table.

- Values that look like credentials become `***`. This covers values after keys such as `password`,
  `secret`, `token`, `access-key`, `session` and `signature`, `Bearer` tokens, AWS access key IDs,
  the user and password part of a URL, URL query strings, and an Azure SAS `sig=` value outside a URL.
- Control characters, text-direction characters and other invisible format characters, such as
  U+200B, U+FEFF, U+00AD and tag characters, are written as `\uXXXX`. A folder name with a newline
  stays on one line in the log and in one entry of a `diagnostic_details` sample.
- In the audit table, folder paths and names keep their exact text, apart from these escapes and a URL
  user and password. A backslash in a path or name is written as `\\`. So a folder named `token=1` is
  stored as is.
- In log and report lines, paths are cleaned like any other text. A part of a path that looks like
  `key=value` is hidden: a folder named `token=1` is logged as `token=***`, and
  `.../api_key=abc/part-0.parquet` as `.../api_key=***`, because the value runs over the `/`. The
  audit row is the exact record.
- Long messages are cut and end with `...[truncated]`.
- `error_message` holds the error message and the class names of its causes, for example
  `...; caused by java.io.IOException`. The messages of the causes are only in the job log.
- WARN and ERROR log lines show the error and its causes on one line. The stack trace is logged at
  DEBUG, with its messages cleaned the same way.

To read a path or name back from the audit table, decode `\\` as one backslash and `\uXXXX` as one
UTF-16 unit. The value is then the exact text, with two exceptions: a URL user and password were
removed, or the value ends with `...[truncated]`. An S3, GCS or Azure object key is short enough that
it is never cut. `error_message`, `ownership_unreadable_sample`, `scan_plan_error` and log lines are
display text. They cannot be decoded back, because a backslash in them is left as is.

The cleaning works on patterns, so it has limits:

- A secret with no key name in front of it, and no known shape, is not found.
- A value without quotes ends at `,` `(` `"` `;` or `&`. So `password=ab,cd` shows `,cd`.
- A key followed by a space and a secret with no digit, such as `fs.s3a.secret.key` and 40 letters, is
  not found.
- A key name that ends in `error`, `exception`, `throwable`, `provider` or `providers`, such as
  `s3.secret-key-error=x`, is not treated as a secret key.
- A key name with more than 128 characters after its last keyword is not found. Real keys have the
  keyword near the end.
- A URL password part longer than 1,024 characters before an `@`, and a path longer than 2,048
  characters before a `?`, are not cleaned as such.
- With a user and password in a URL, everything up to the last `@` before a space is hidden. A path
  with an `@` after such a URL loses its start. That hides too much, not too little.
- Some harmless values that sit after a key such as `session` are hidden too.
- Unicode look-alike separators, YAML block values, JSON keys written with escapes, a `user:password@host`
  with no scheme, and URLs with spaces or unusual schemes are not cleaned.
- Logs written by Spark and Hadoop themselves are not cleaned by this job.

The job exits with code `1` when the run stops with an error, for example when an audit row could
not be written. It exits with `0` otherwise. A database that fails gets its own `FAILED` row and the
run goes on with the next database, so a `FAILED` row alone does not change the exit code.

---

## Cleanup root with no visible table

When tables sit under more than one parent location, the cleanup root falls back to the database
location, unless the folder without `.db` replaces it as described in
[Second cleanup root](#second-cleanup-root). If no table the job can see is stored under the cleanup root,
nothing in it can be checked against a known table. The database is then recorded as `BLOCKED` with
`status_reason=cleanup_root_has_no_visible_table`, and nothing is deleted. This also applies in dry-run
and when there are no candidates. The candidates are still listed in the audit row and the log, and the
log's `DELETION` section gives the reason.

---

## Storage of other databases and Iceberg write paths

Before choosing candidates, the job reads the location of every database it can see in the catalog,
once per run, with `SHOW NAMESPACES` and `DESCRIBE DATABASE`.
For `spark_catalog` and IOMETE-managed REST catalogs, a database's storage also includes its REST
namespace folder, `<catalog warehouse>/<database>`, where its Iceberg tables live.

- Storage of another database at or inside the cleanup root is protected. A folder that is, holds, or
  sits inside it is never a candidate.
- A database location that holds the cleanup root, such as `default` at the warehouse root, places its
  Hive tables at `<location>/<table>`. The job looks up the one table that this rule would put on the
  cleanup root's path, for example `default.sales` for a cleanup root at `<warehouse>/sales`. If that
  table exists, its location is protected.
- A REST namespace folder of another database that holds the cleanup root cannot be checked this way,
  because the catalog adds a unique suffix to table folders.
- The job finds its own database in that list by its exact name. If the catalog normalizes names, one
  entry that differs only in letter case is its own. If two entries differ from the configured name
  only in letter case, the job cannot tell which one is its own.
- In that case, or if the database list, a database location, or the table lookup cannot be read, or a
  location has no scheme and bucket, the database is `BLOCKED` with
  `status_reason=other_database_storage_unknown`, and nothing is deleted.

For an Iceberg table the job can see, its `write.data.path`, `write.metadata.path`,
`write.object-storage.path` and `write.folder-storage.path` are protected like its location, during
candidate selection and again at every ownership read of the [deletion fence](#deletion-fence). A table with `gc.enabled` set to anything but
`true` or with `write.location-provider.impl` set, a blank write path or one without a scheme and bucket,
or properties that cannot be read is treated as an unresolved table, which blocks deletion for its database.

### Tables of other databases and catalogs

A table keeps its location when it is renamed into another database, and a table can be registered or
created with an explicit location anywhere. So when a folder is still unmatched after the checks above,
the job also reads every table it can see in the other databases of the catalog, the same way it reads
its own tables. It does the same for every other catalog in the job's Spark session, whatever bucket its
`warehouse` is in: a table of a catalog in another bucket can still have its `LOCATION` in this
database's folder, and a REST catalog can get its warehouse from the server. JDBC catalogs (`JDBCTableCatalog`) keep no table folders, so they are not read. The catalogs come from the session's
configuration, so a catalog added while the job runs, for example by catalog sync, is included.
`spark_catalog` always counts as one of them. Its Hive tables go to Spark's `spark.sql.warehouse.dir`, so
that is also one of its warehouses. An IOMETE-managed REST catalog is read with the job's own client. Any
other Iceberg REST catalog, for example an external one with its own token, is read through the client Spark
built from the session's settings, so it uses the same authentication, with Spark's table cache bypassed. If
that catalog does not offer the view endpoints, its views can't be listed, so its reads fail and the database
is `BLOCKED`.

Such a catalog only shows what its credential may see, so a read that succeeds does not show that it listed
every table. Its tables still protect their folders, but a table it hides looks like a dropped one. So while
any catalog of the session other than `spark_catalog`, an IOMETE-managed REST catalog or a JDBC catalog is in
the session, every folder that passes the other checks is held as `catalog_visibility_unverified`, whatever it
holds: own table metadata, commit scratch or anything else. Nothing is deleted under any cleanup root of the
database, and the dry run still reads every catalog and reports every other hold. For those catalogs:

- a warehouse at or inside the cleanup root is protected whole;
- the storage of their databases is protected with the rules above;
- their tables are read like the tables of other databases.

Each of these tables' location and write paths is protected when it is, holds, or sits inside a folder
of the cleanup root. If one of these tables is unresolved, its files could be in any folder, so the
database is `BLOCKED` with `status_reason=other_table_unresolved` and the log names the table. If the
databases or tables of one of these catalogs cannot be read, the database is `BLOCKED` with
`status_reason=other_database_storage_unknown`. The same happens for an IOMETE-managed REST catalog with
no `warehouse`: it stores each database under `<warehouse>/<database>`, and that folder is unknown.

The first read is shared by every configured database. A read that fails, or that could not read a
database location or found an unresolved table, is not kept, so the next database reads again. Reading costs about one catalog lookup per table. The tables of up to 8
databases are read at a time, and the namespaces of an Iceberg REST catalog are listed 8 at a time; the
metastore serves one call at a time. Results are used in the same order as a one-by-one read. After the
first failed read, databases not yet started are not read. The log reports each store's database count
and time, progress every 30 seconds on long reads, and how many tables were checked and how long it took.
When every folder already has an owner, nothing is read.

Within one database, the database and table lists are read once and reused under both cleanup roots.
Once the storage under one root is unknown, the other root is not read, because the database is blocked
either way. So a catalog that cannot be reached costs one wait per database, not one per root. A catalog whose server
does not answer can take minutes to fail, depending on its connection timeout. Before reading, the log
lists each other catalog and why it is read: it has no warehouse setting, its warehouse is not a storage
path, or its warehouse is in the same or another bucket.

When the storage of other databases is unknown, the row's `diagnostic_details` says why in
`ownership_unreadable_sample`, for example `catalog=sales_rest: the database list could not be read
(ConnectException)`. It names the first thing that could not be read or used, because reading stops
there, so a second problem shows up only once the first is fixed. Only the class of an error is recorded
there. The WARN log has the details. The report shows the same entry as `Unreadable` under
`OWNERSHIP CHECKS`.

Another database can claim a folder while the run is going. So right before a database deletes its
first folder, the job reads all of this again, with nothing reused from the first read. It reads again
once that read is more than 4 minutes old, before a later folder or inside one. See [Deletion fence](#deletion-fence).
Iceberg catalogs cache the tables they load, so before every table read the job drops that table's
cached copy, and the read comes from the catalog. This also applies to the read of the database's own
tables in the fence. If the copy cannot be dropped, the read fails. A candidate folder that is now
claimed is kept and recorded as `folder_kept_at_final_check`, and the log names the new storage. If
such a read fails before anything is deleted, the database is `BLOCKED` with
`status_reason=other_database_storage_unknown`. If it finds an unresolved table, the database is
`BLOCKED` with `status_reason=other_table_unresolved`. After a folder was deleted, either one stops
deletion with `status_reason=deletion_stopped_at_recheck`. A dry run deletes nothing, so it does not
read again.

These checks use only what the job can see. They do not protect a database or table hidden from the
job, tables of a catalog outside the job's Spark session, tables
of nested namespaces, a table placed in a candidate folder after the last ownership read (unless it is
a table of this database with the folder's name), or files that a table reaches
without a write path property, for example through `add_files`,
`migrate`, `snapshot`, or an earlier location.

---

## Unresolved table storage ownership

A table is unresolved when it exists in the catalog but the job cannot read its Iceberg metadata or
storage location during the run, its location is missing or blank and it is not a view, its location
has no scheme and bucket, or its name contains a backtick, which the job never puts in SQL text. This says nothing about
whether the table is wanted or healthy. It only means the job does not currently know which storage
folder that table owns.

When at least one table has a verified location, storage reconciliation still runs. The job lists the
database's storage folders and compares them against the table locations it could verify, so the
operator still gets the full picture of what is in storage.

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

Some tables become unresolved after their location was read, for example when `gc.enabled` is not
`true`. The job keeps that location only to leave the table's own folder out of the unmatched list.
It is used only when it is inside the scan root, so it can leave out at most one listed folder.
It is not a verified location: it never selects the scan root and never makes a folder deletable,
and the database stays `BLOCKED` even when no unmatched folder remains.

A table whose metadata file is missing is handled the same way when the catalog error names that file.
For a missing `<folder>/metadata/<file>.metadata.json`, the job keeps `<folder>` as the table's known
folder. This is evidence only. The table may keep metadata or data elsewhere, so the folder is never
treated as its verified location. Any other error leaves the table with no known folder.

The log report lists each unresolved table with the reason it could not be resolved and, when there is
one, its known folder.

The inspection limit is still checked first. If the number of unmatched folders exceeds the larger of
`100` and `max_candidate_folders_per_database`, the database is reported as `SKIPPED` with
`status_reason=too_many_folders_to_inspect` instead. Both outcomes stop deletion for that database.
This row still reports both conditions: `unresolved_table_count` carries the unresolved tables,
`potentially_untracked_folder_count` carries the folders that exceeded the limit, and
`candidate_folder_count` stays `0`. Folder contents are not inspected. Raising the limit will not
make these folders deletable, because the database then becomes `BLOCKED`. Below the inspection limit,
`max_candidate_folders_per_database` does not apply to such a database, because no folder is due.

If no table that stores files has a verified location, for example when the other tables are
[JDBC tables](#jdbc-tables), there is nothing to compare folders against, so the job does not access storage and does not check the folder limit. It logs a short report and writes
a `BLOCKED` row with `status_reason=unresolved_catalog_ownership`. On this row `storage_scan_location`
is empty, `cutoff_time` is null, and the folder counts are `0` because no folder was listed.

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

## Versioned buckets

On a bucket with versioning enabled or suspended, the job deletes folders the same way as on any other
bucket. It does not pass object version IDs. The effect depends on the bucket:

- **Versioning enabled:** each deleted object gets a delete marker. Its earlier versions stay as
  noncurrent versions and still use storage.
- **Versioning suspended:** the current `null` version is removed and a delete marker is added. Versions
  written while versioning was enabled stay and still use storage.

So a deleted folder disappears from listings, but storage use may not go down. To free the space, add
bucket lifecycle rules:

- expire noncurrent versions after a set number of days (`NoncurrentVersionExpiration`);
- remove delete markers that have no versions left (`ExpiredObjectDeleteMarker`).

With versioning enabled, the noncurrent-version window is also the recovery window for anything the
job deletes. Before it expires, a deleted folder can be restored from its earlier versions. With
versioning suspended, objects written while it was suspended have no earlier version, so they cannot be
restored this way. The job does not delete old versions itself.

Versioning is also the way back for a delete the job stopped waiting for. Such a delete may still
complete later, even after the job ends, and its object is listed under
`partially_deleted_folder_unknown_state_keys_sample`. With versioning enabled, that object keeps its
last version behind a delete marker, so it can be restored. Without versioning it cannot.

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

- Summary logs. Each database's report is written as one log entry.
- Candidate folders
- Candidate object count and size
- Audit rows
- Storage scan root
- Active table locations
- Excluded paths

### 2. Confirm candidates are safe

Before enabling deletion, verify that each candidate folder:

- Is no longer claimed by the catalog.
- Is older than the cutoff deletion mode will use: `older_than_hours`, but never less than 72 hours.
  Folders in `would_be_held_by_deletion_floor_paths_sample` are younger than that, so deletion mode
  keeps them for now.
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
  "older_than_hours": 72,
  "dry_run": false,
  "delete_enabled": true,
  "max_candidate_folders_per_database": 10
}
```

Deletion mode never uses less than 72 hours. With a lower `older_than_hours`, the run uses 72 and logs
a warning. See [72-hour minimum in deletion mode](#72-hour-minimum-in-deletion-mode).

Before deleting each folder, the job checks it again. See [Deletion fence](#deletion-fence).

---

## Recommended validation checklist

Before enabling deletion mode, run the job in dry-run mode and validate the output. This checklist is intended for operator validation, not as a replacement for automated tests.

| ID | Scenario | Expected result |
|---|---|---|
| T01 | Valid DB, no untracked folders, dry-run | `SUCCESS`, zero candidates, zero deleted |
| T02 | Valid DB with one manually created untracked folder, dry-run | `SUCCESS`, one candidate, zero deleted |
| T03 | Same as T02 with deletion mode enabled, once the folder's newest object is at least 72 hours old | `SUCCESS`, one deleted folder |
| T04 | Post-delete dry-run | `SUCCESS`, zero candidates |
| T05 | Config contains valid DB and missing DB | Valid DB `SUCCESS`, missing DB `SKIPPED` |
| T06 | Config contains only missing DB | `SKIPPED`, `status_reason=database_not_found` |
| T07 | `dry_run=false`, `delete_enabled=false` | Fails safe, no deletion |
| T08 | Candidate count exceeds max limit | `SKIPPED`, no deletion; folders too new for the age check count toward the limit too |
| T09 | Candidate is listed in `exclude_paths` by full path | Candidate is protected |
| T09b | Candidate is listed in `exclude_database_folders` as `database.folder` | Candidate is protected |
| T09c | An excluded path sits inside a candidate folder | The whole candidate folder is protected |
| T10 | Loose file directly under scan root | Ignored, not deleted |
| T11 | Table is dropped then recreated at same location | Folder is protected because it is active again |
| T12 | Table is dropped then recreated at different location | Old untracked folder can become a candidate |
| T13 | Candidate becomes active between detection and deletion | That folder is kept as `folder_kept_at_final_check` or `folder_kept_table_named_like_folder`; other folders are still deleted |
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
| T25 | Another database is stored inside the cleanup root | That database's folder is never a candidate; unrelated orphans are still deleted |
| T26 | An Iceberg table sets `write.data.path` to a folder under the cleanup root | That folder is never a candidate |
| T27 | A candidate folder is deleted between listing and inspection | Skipped as `folder_not_found`; if every candidate was deleted, the database does not fail; with no other block, it records `SUCCESS` with no candidates |
| T28 | A candidate folder holds only directory markers | Skipped as `empty_folder` on every run; never deleted |
| T29 | Listing a candidate folder is denied | Skipped as `statistics_unavailable`; if every existing candidate is denied, `FAILED` |
| T30 | A candidate folder is deleted by someone else after inspection, before the job deletes it | Recorded as `folder_not_found_at_deletion`; still a candidate, not deleted; other folders are still deleted |
| T31 | A table is renamed from the cleaned database into another database | Its folder stays and is never a candidate; unrelated orphans are still deleted |
| T32 | Another catalog of the job session, in the same bucket, has a table stored under the cleanup root | That folder is never a candidate |
| T33 | Another database has a table whose metadata is missing | `BLOCKED`, `status_reason=other_table_unresolved`, in dry-run and deletion mode |
| T34 | Another database places a table in a candidate folder after the first read, before deletion | That folder is kept and recorded as `folder_kept_at_final_check`; unrelated orphans are still deleted |
| T35 | Reading other databases fails, or finds an unresolved table, only at the final read | `BLOCKED` with `other_database_storage_unknown` or `other_table_unresolved`; nothing deleted |
| T36 | Reading other databases fails briefly, or cannot read a database location or a table, while the first configured database is processed | That database is `BLOCKED`; the next database reads again and is not blocked by the earlier failure |
| T37 | Another database's existing Iceberg table changes its `write.data.path` to a candidate folder after the first read, before deletion | That folder is kept and recorded as `folder_kept_at_final_check`; unrelated orphans are still deleted |
| T38 | Another catalog of the job session has no `warehouse` setting, or a blank one, and a table stored under the cleanup root | The catalog is read, at the first read and the final read, and that folder is never deleted. An IOMETE-managed REST catalog with no `warehouse`, or a catalog that cannot be read, gives `BLOCKED` with `other_database_storage_unknown` |
| T39 | A JDBC catalog is registered in the job session | It is not read and does not block deletion |
| T40 | A database has two cleanup roots, and another catalog of the job session has a table in `<warehouse>/<database>`, or cannot be read there | That folder is never deleted; a read that fails under either root gives `BLOCKED` with `other_database_storage_unknown`, and nothing is deleted in either root |
| T41 | A database has a JDBC table in `<database>.db` and its other tables in `<warehouse>/<database>` | The JDBC table's folder is never a candidate and does not make `<database>.db` a cleanup root; with only JDBC tables, `SKIPPED` with `no_active_tables_in_database` |
| T42 | Deletion mode with `older_than_hours=0`, an orphan 48 hours old and one 73 hours old | One warning names both values; the 48-hour folder is kept, the 73-hour folder is deleted; `older_than_hours=72` and `configured_older_than_hours=0` on the row |
| T43 | Dry run with `older_than_hours=0` and the same two folders | Both are candidates, nothing is deleted; `would_be_held_by_deletion_floor_count=1` names the 48-hour folder |
| T44 | A candidate folder holds more than 10,000 objects | Skipped as `too_many_objects` in dry-run and deletion mode; never deleted; other folders are still deleted. A folder that passes more than 10,000 objects only after inspection is kept as `folder_kept_too_many_objects`, and the row stays `SUCCESS` |
| T45 | Deletion mode, and an object is added to a candidate folder after inspection, before the job lists it again | The folder is kept as `folder_kept_changed_since_inspection`; nothing in it is deleted; other folders are still deleted; `SUCCESS` |
| T45b | Deletion mode, and an object is added to a candidate folder after the job listed it again, while it deletes it | The new object stays; `FAILED` with `deletion_failed`; `partially_deleted_folder_unlisted_object_count=1` |
| T46 | Before the first deletion run, on the target storage | A listing and a HEAD of the same object agree on size and ETag, and on modification time to the second. If not, deletion stops at the first object with `deletion_failed` and nothing is deleted |
| T47 | Deletion mode, 3 confirmed folders older than 72 hours, 9 folders 48 hours old, `max_candidate_folders_per_database=3` | The 3 old folders are deleted and `too_new_count=9`. With a limit of `2`, `SKIPPED` with `too_many_candidate_folders`, `due_for_deletion_count=3`, nothing deleted |
| T47b | 101 folders with no verified owner, `max_candidate_folders_per_database=10` | `SKIPPED` with `too_many_folders_to_inspect`; no folder is listed. With a limit of `101`, all are inspected |
| T48 | Every folder with no verified owner is held, for example because each is too new or empty | `SUCCESS`, nothing deleted; one warning names each reason and its count; `held_folder_counts` has the same counts |
| T49 | Deletion mode, and the first ownership read of the fence takes more than 2.5 minutes | `FAILED` with `unexpected_error`; nothing deleted; `error_message` says the ownership recheck took more than the 150 s a read may use of the 300 s limit |
| T50 | Deletion mode, and the ownership read fails after one folder was deleted | `FAILED` with `deletion_stopped_at_recheck`; `deleted_folders` lists that folder; `deletion_stop_cause` and `folder_not_attempted_paths_sample` are on the row |
| T51 | Dry run on the target image, with a candidate folder whose name holds a quote and `${x}` | `candidate_folders` on the audit row holds the folder path exactly as it is in storage |

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

Each bullet starts with its class. **Fixed** means closed in this version, with a test. **Fails closed**
means the job keeps the folder or stops deleting, with a test. **Residual** means the job cannot close it;
the bullet says why and what covers it instead. **Scope** means something the job does not try to do. No
bullet is a release blocker.

- **Scope.** `exclude_paths` supports full physical storage paths only.
- **Scope.** `exclude_database_folders` supports database-scoped immediate child folder names in `database.folder` format.
- **Scope.** Table names or folder names alone are not supported because they can be ambiguous across databases.
- **Scope.** The platform UI Job ID is not currently stored in the audit table because it is not exposed to the driver process as an environment variable. The runtime compute/application ID is stored as `runtime_compute_id`.
- **Scope.** The job deletes whole untracked table folders, not individual orphan files inside active Iceberg table folders.
- **Scope.** Candidate discovery is table-folder-level: the job only selects immediate child folders under the resolved scan root as cleanup candidates.
- **Scope.** When a selected candidate folder is deleted, every object listed under that folder is deleted, including nested data and metadata objects.
- **Residual.** An object rewritten after the job last reads its status, and before its delete finishes, is
  deleted with its new content. Object storage has no conditional delete here. The storage client also retries
  a failed delete by itself, and each retry deletes whatever is at that key at that moment. On a slow store
  this window can last seconds to minutes. Bucket versioning is the way back. An object written under a new
  key after the last listing is never deleted.
- **Residual.** The check before each delete compares modification times to the second, because a HEAD
  reports seconds. A rewrite with the same size and ETag in the same second is not seen. Different content
  changes the ETag.
- **Fails closed.** An object key with an empty, `.` or `..` segment, such as `a//b`, cannot be addressed
  exactly through Hadoop paths. Such a key is listed under a different name. Before the first delete, the job
  reads every listed key back by its own name, so a folder that holds one is kept as
  `folder_kept_key_not_addressable` and nothing in it is deleted.
- **Fails closed.** Deleting object by object sends about five storage requests per object, one after
  another. A folder near the 10,000-object limit can take several minutes, longer than one ownership read
  may be used. The job then reads ownership again in the middle of the folder and goes on. If that read
  fails, is too slow or protects the folder, the folder is left partly deleted, and deletion stops with
  `deletion_stopped_at_recheck` and the cause. The job starts no delete in the last 30 seconds of a read's
  5 minutes, which stops the folder the same way with `snapshot_expired_during_deletion`. The row lists
  exactly which objects were deleted and which remain. One delete call can take longer than those 30 seconds, for example while the storage client
  retries. The folder then ends as `deletion_failed` instead. That object is in `unknown_state`, and its
  delete may still land after the limit, as the next bullet says. The limit is provisional. See
  [Object limit](#object-limit).
- **Fails closed, and residual for a delete still running.** Time limit. The job measures the age of its
  ownership read on a clock that a change of the system time cannot move. It waits for each catalog or storage
  read and each delete only until that read is 5 minutes old, starts no read after that, and starts no delete
  in the last 30 seconds before it. If a read does not finish in time, the job stops deleting for that database
  and ignores the late result.
  Inside a delete call, the storage client reads the object's status and may retry before and after it sends
  the request. A delete that returned in time counts as deleted. A delete still running at the limit is
  reported in an unknown state and stops the database. The job only stops waiting. That delete may still
  complete later, even after the job ends, with no bound on when. Turn on bucket versioning so such an object
  can be restored. See [Time limit](#time-limit).
- **Residual.** After a folder's objects are deleted, the job removes its empty directories one at a time.
  Inside the delete of a directory, the storage client first checks whether an object has that exact name,
  and deletes that object instead of the directory marker. Through Hadoop paths the job cannot ask for the
  marker alone. So an object written at a directory's exact name after the job's own status read, and before
  the client's check, is deleted. For a directory inside the folder, that object is in the folder. For the
  folder itself, it sits beside the folder (`db/t` next to `db/t/`), outside it. Inside the folder, the
  client leaves the directory's marker behind, which keeps the folder. The removal of empty directories names
  that directory as unlisted, so the folder is reported partly deleted. Beside the folder nothing is left to
  show it, and the folder is reported deleted. The window runs from the
  job's status read to the client's check inside the delete, usually milliseconds. For a directory delete
  the job stopped waiting for, it lasts until that delete lands. The job could close this only by sending no
  directory delete at all. Every deleted folder would then keep its markers and come back as a candidate in
  the next run. It would count toward the inspection limit before the age check, so the
  leftovers could fill that limit over time. Keeping only the folder's own marker would
  close the case beside the folder, not the one inside it. Older versions deleted each folder with one
  recursive delete. That deleted such an object inside the folder whenever it was written, and an object at
  the folder's own name whenever one was there. Bucket versioning is the way back. No directory delete
  starts in the last 30 seconds of the ownership read's 5 minutes, and the folder is then reported partly
  deleted.
- **Fails closed.** On S3A, the listing of a directory returns an object of the same name only when nothing is
  listed below the directory. Other stores may return it in other cases. So when the removal of empty
  directories gets such an object back, it stops there. The folder is then reported partly deleted, with
  `partially_deleted_folder_directory_cleanup=not_finished`. Below the folder, the object is new and is named
  as unlisted. For the folder itself, this also happens for an object beside the folder (`db/t` next to
  `db/t/`) that was there before the run, or that was written while its directories were removed. The folder
  is then partly deleted even though every listed object is gone.
- **Residual.** A metastore call that never returns can still hold up the job at its next Spark SQL call,
  for example the audit write. The deletion checks return in time and stop deletion, but the row may be
  written late or, if the job is killed, not at all.
- **Residual.** A table created in a candidate folder after the last ownership read is not seen before
  deletion, when it is in another database or has another name than the folder. The window lasts until the
  folder's deletes land. See [Races the job cannot close](#races-the-job-cannot-close). Bucket versioning is
  the way back.
- **Residual.** A `CREATE TABLE ... LOCATION` on a candidate folder after the last ownership read is not
  seen when it writes no object into the folder, for example a Hive table with no data yet, and the table is
  in another database or has another name than the folder. The new listing and the age check find nothing
  new, so the folder can be deleted. When the table writes an object there before the job lists the folder
  again, the folder is kept as `folder_kept_changed_since_inspection`.
- **Residual.** A multipart upload that is still in progress has no object yet, so it is not listed and not
  checked, and its parts are not deleted. If it completes after the job listed the folder, the new object
  stays. The folder is then reported partly deleted, with the object as unlisted when the removal of empty
  directories finds it. When the upload completes after the folder was done, the folder is reported deleted
  with the new object in it.
- **Residual, not verified.** S3 gives the object of a multipart upload the time the upload started as its
  modification time. This comes from AWS documentation and was not checked here. An upload started more than
  72 hours ago and completed shortly before the job inspects the folder can pass the age check, and its
  object is then deleted with the folder.
- **Fails closed.** The listing before deletion and the deletes are not one step. Seconds to minutes pass
  between the listing and the end of the removal of empty directories. An object written under a new key
  in that time is never deleted. That step lists each directory, looks again after a refused delete, and
  lists a kept folder once more at the end. When it finds the object, it records `deletion_failed` with an
  unlisted object. When that step does not finish, the folder is still partly deleted, and the row has
  `partially_deleted_folder_directory_cleanup=not_finished` but may not name the object. An object written
  after the job last looked at that place is not found, and the folder is reported deleted with the object
  in it, as for a multipart upload that completes late.
- **Residual.** The age check compares object times with the clock of the Spark driver. If that clock runs
  ahead of the storage clock, the 72-hour minimum is shorter by the same amount. Keep both on NTP.
- **Residual.** The job relies on Spark catalog discovery for active table locations.
- **Scope.** `report_only_storage_roots` approves listing for review only. It cannot make a location eligible
  for deletion. Older job versions reject configs that contain this setting, so upgrade the job before
  adding it.
- **Scope.** Object-storage access supports S3 and S3-compatible storage. A non-S3 catalog location falls back to Spark's own Hadoop configuration, which carries no catalog-specific endpoint or credentials.
- **Scope.** Only databases on S3-compatible storage, Google Cloud Storage, or Azure storage are cleaned. A database on another filesystem, such as HDFS, is refused with `invalid_storage_location`.
- **Scope.** Catalogs using vended or remotely signed credentials are not supported for storage access, because those credentials are issued per Iceberg request and never appear in `sparkConf`.
- **Scope.** Folder names are compared case-sensitively, because `ORDERS` and `orders` are different object-storage prefixes.
- **Fails closed.** Catalog, database and table names with a backtick are not supported. Such a database
  fails, such a table is unresolved, and another database with such a name has unknown storage, so nothing is
  deleted on its account. A candidate folder whose name holds a backtick stops deletion at the lookup of a
  table with that name.
- **Scope.** A database whose location equals or contains the catalog warehouse root cannot be cleaned by this job.
- **Scope.** Folders are deleted only under the cleanup root. Additional roots are reported only, and table
  parents outside the database's own storage are not listed at all.
- **Residual.** Ownership is checked against the tables and databases the job can see in the catalog, and in
  the other catalogs of its Spark session. A table or
  database the job cannot see does not protect its folder, and neither does a Hive partition stored outside
  its table's folder. Catalogs outside the Spark session, and files
  a table uses outside its own folder, are not checked. See
  [What deletion mode can and cannot prove](#what-deletion-mode-can-and-cannot-prove). Protect such folders
  with `exclude_paths`.
- **Scope.** On a versioned bucket, deleting a folder may not free storage. See [Versioned buckets](#versioned-buckets).
- **Scope.** The job is intended for controlled cleanup workflows, not blind automatic deletion.
- **Scope.** Folder age and size require recursively listing objects under each candidate folder, for at most the larger of 100 and `max_candidate_folders_per_database` folders per database. This can take time for folders with many objects and cannot be turned off, because it is needed to establish folder age. The listing stops after 10,001 objects, and such a folder is kept as `too_many_objects`.
