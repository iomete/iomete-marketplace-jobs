# Backup run history

Every run of the backup job is recorded in two Iceberg tables: what it copied,
how it ended, and how long each stage took. Driver logs are removed with the
pod, so these tables are usually the only record of a run that is still
available days later.

Recording is enabled by default and needs no configuration. To turn it off, move
the tables to another database, or change how many failure rows a run records,
see the `stats` block in the [README](../README.md#run-history).

## Tables

Both tables are created on first use in `spark_catalog.iomete_system_db`, or in
the database set by `stats.database`. Both are partitioned by day on
`started_at`.

| Table | Contents |
|---|---|
| `lakehouse_backup_runs` | One row per run: status, counts, byte totals, stage timings and the settings the run used. |
| `lakehouse_backup_run_file_failures` | One row per entry the run failed to copy, joined to the run by `run_id`. |

The run row is written when the run starts, with `status = 'RUNNING'`, and
updated when the run ends with the final status, counts and timings. Only the
driver updates this row. A run still marked `RUNNING` long after `started_at`
therefore means the driver stopped before it could record the outcome, for
example because the pod was killed, ran out of memory, or lost its node.

Every column has a comment, so `DESCRIBE TABLE EXTENDED
spark_catalog.iomete_system_db.lakehouse_backup_runs` returns the same
descriptions listed below.

## Column reference

### Identity

| Column | Description |
|---|---|
| `run_id` | Run ID shown in the IOMETE console. |
| `job_id` | Identifies the job; the same value for every run of that job. |
| `started_by` | User who triggered the run. Null for scheduled runs. |
| `source_type`, `target_type` | `s3` or `hdfs`. |
| `source_uri`, `target_uri` | Root URIs the run read from and wrote to. |

### Outcome

| Column | Description |
|---|---|
| `status` | `RUNNING`, `SUCCEEDED` or `FAILED`. |
| `error_message` | Exception class and message. Null unless the status is `FAILED`. |
| `started_at` | When the run started, before source enumeration. |
| `ended_at` | When the run finished. Null while `RUNNING`. |

### Counts

| Column | Description |
|---|---|
| `files_listed` | Files found by source enumeration. |
| `dirs_listed` | Empty directories found. HDFS sources only. |
| `files_copied` | Files written to the target. |
| `files_skipped` | Files already identical at the target. |
| `files_failed` | Number of entries that failed. Always the true count, even when the failures table holds fewer rows. |
| `dirs_created` | Empty directories recreated at the target. |
| `retries_used` | Copy attempts beyond the first, summed across all entries. |
| `failures_truncated` | True when the failures table holds fewer rows than `files_failed`. |
| `bytes_source` | Total size of everything enumerated at the source. |
| `bytes_copied` | Bytes written to the target. |
| `bytes_skipped` | Bytes in files that were skipped. |

### Stage timings

The five stages run one after another on the driver, so they add up to close to
the run's wall time. The remainder is mostly the row written when the run
starts, plus table creation on the first run.

| Column | Description |
|---|---|
| `source_listing_ms` | Enumerating the source tree. |
| `target_listing_ms` | Enumerating the target tree. `0` when `copy.skipIdentical` is `false`. |
| `planning_ms` | Deciding what to copy and what to skip. |
| `copy_ms` | Wall time of the distributed copy. The only stage that runs on the cluster. |
| `dir_create_ms` | Recreating empty directories. |

### Executor timings

These cover the copy stage only and are summed across all tasks, so they are
much larger than `copy_ms`. Compare them with each other rather than with wall
time.

| Column | Description |
|---|---|
| `copy_task_ms` | Time spent copying files, summed across tasks. |
| `fs_init_ms` | Building a filesystem client for each file. |
| `source_read_ms` | Reading from the source. |
| `target_write_ms` | Writing to the target, including the final upload. |
| `throttle_wait_ms` | Blocked by the bandwidth limit. |
| `verify_ms` | Length check after each write. |
| `commit_ms` | Delete and rename that publish each file. |
| `retry_sleep_ms` | Waiting between copy attempts. |

Spark can retry or speculate a task, so these timings may count the same work
more than once. Use them to compare where time went, and use `files_copied` and
`bytes_copied` for exact totals.

### Settings and shape

| Column | Description |
|---|---|
| `bytes_per_task`, `files_per_task`, `skip_identical`, `max_bandwidth_mb_per_sec` | The `copy` settings this run used. `max_bandwidth_mb_per_sec` is null when no limit was set. |
| `task_count` | Number of Spark tasks the copy was split into. |
| `largest_file_bytes` | Size of the largest file the run had to copy. A single file is never split across tasks, so no run finishes faster than its largest file. |

### Failure rows

| Column | Description |
|---|---|
| `run_id` | Joins to `lakehouse_backup_runs.run_id`. |
| `started_at` | Copied from the run row so both tables share a partition key. |
| `source_path` | Entry that failed to copy. |
| `target_path` | Where it would have been written. |
| `attempts_used` | Copy attempts made before the job gave up. |
| `error` | Exception class and message from the last attempt. |

## Example queries

### Check recent runs

```sql
SELECT run_id, status, started_at, ended_at,
       files_copied, files_skipped, files_failed,
       round(bytes_copied / 1024 / 1024 / 1024, 2) AS gb_copied,
       error_message
FROM spark_catalog.iomete_system_db.lakehouse_backup_runs
WHERE started_at > current_timestamp() - INTERVAL 2 DAYS
ORDER BY started_at DESC;
```

A successful run has `status = 'SUCCEEDED'`, `files_failed = 0` and an
`ended_at`. A failed run has `status = 'FAILED'` and the exception in
`error_message`; the job also exits with a non-zero code, so the platform
reports the failure as well.

### Find runs that never finished

```sql
SELECT run_id, started_at, source_uri, target_uri
FROM spark_catalog.iomete_system_db.lakehouse_backup_runs
WHERE status = 'RUNNING'
  AND started_at < current_timestamp() - INTERVAL 12 HOURS;
```

These runs lost their driver before they could record an outcome. Open the run
in the IOMETE console to find out why.

### List the files a run failed to copy

```sql
SELECT f.source_path, f.attempts_used, f.error
FROM spark_catalog.iomete_system_db.lakehouse_backup_run_file_failures f
WHERE f.run_id = '<run-id>'
ORDER BY f.source_path;
```

The number of rows per run is capped by `stats.maxFailureRows`, 1000 by default.
When the cap applies, `failures_truncated` is true on the run row and
`files_failed` still holds the complete count.

### See where a run spent its time

```sql
SELECT run_id,
       unix_millis(ended_at) - unix_millis(started_at) AS run_wall_ms,
       source_listing_ms, target_listing_ms, planning_ms, copy_ms, dir_create_ms,
       (unix_millis(ended_at) - unix_millis(started_at))
         - (source_listing_ms + target_listing_ms + planning_ms + copy_ms + dir_create_ms)
         AS unaccounted_ms
FROM spark_catalog.iomete_system_db.lakehouse_backup_runs
WHERE run_id = '<run-id>';
```

A run dominated by `source_listing_ms` is limited by how long it takes to walk
the source tree, not by how fast data moves.

### Check copy throughput and cluster use

```sql
SELECT run_id,
       round(bytes_copied / 1024 / 1024 / (copy_ms / 1000), 1) AS mb_per_sec,
       round(copy_task_ms / copy_ms, 1)                        AS avg_concurrency,
       task_count,
       largest_file_bytes
FROM spark_catalog.iomete_system_db.lakehouse_backup_runs
WHERE run_id = '<run-id>';
```

`copy_task_ms` divided by `copy_ms` gives the average number of task slots busy
during the copy, without needing to know how many executors the run had. Two
causes explain a low value, and `task_count` tells them apart: a small
`task_count` means the work was split into too few pieces, so lower
`bytesPerTask`; a healthy `task_count` with a `largest_file_bytes` that accounts
for most of `copy_ms` means one large file kept the run open after the rest had
finished.

### Compare runs of the same job

```sql
SELECT started_at,
       round(bytes_copied / 1024 / 1024 / 1024, 2)             AS gb_copied,
       round(bytes_copied / 1024 / 1024 / (copy_ms / 1000), 1) AS mb_per_sec,
       round(copy_task_ms / copy_ms, 1)                        AS avg_concurrency,
       source_listing_ms, copy_ms
FROM spark_catalog.iomete_system_db.lakehouse_backup_runs
WHERE job_id = '<job-id>' AND status = 'SUCCEEDED'
ORDER BY started_at DESC
LIMIT 30;
```

Because each row also stores the settings the run used, you can match a change
in throughput against a configuration change without looking up how the job was
defined at the time.

### Compare the two sides of the copy

```sql
SELECT run_id, copy_task_ms,
       fs_init_ms, source_read_ms, target_write_ms,
       throttle_wait_ms, verify_ms, commit_ms, retry_sleep_ms,
       copy_task_ms - (fs_init_ms + source_read_ms + throttle_wait_ms
                       + target_write_ms + verify_ms + commit_ms + retry_sleep_ms)
         AS uninstrumented_ms
FROM spark_catalog.iomete_system_db.lakehouse_backup_runs
WHERE run_id = '<run-id>';
```

## Troubleshooting

| Signal | Likely cause | What to do |
|---|---|---|
| `RUNNING` long after `started_at` | The driver was killed or the run was cancelled | Check the run in the console; re-run the backup |
| `source_listing_ms` is a large share of the run | Source enumeration dominates, and the driver does this alone | Narrow the source `prefix` if possible |
| Low `avg_concurrency` with a small `task_count` | Work split into too few tasks | Lower `copy.bytesPerTask` |
| Low `avg_concurrency` with `largest_file_bytes` close to `copy_ms` | One large file finished last | Expected; a single file is never split across tasks |
| `fs_init_ms` is a large share of `copy_task_ms` | Filesystem clients rebuilt per file, common with many small files | Lower `copy.filesPerTask` so tasks are shorter |
| `throttle_wait_ms` is large | The bandwidth limit is being reached | Expected when `copy.maxBandwidthMbPerSec` is set; raise it if there is spare capacity |
| `source_read_ms` far above `target_write_ms` | Source storage is the bottleneck | Investigate the source system |
| `target_write_ms` far above `source_read_ms` | Target storage is the bottleneck | Investigate the target system |
| `verify_ms` plus `commit_ms` is large | Metadata operations dominate, common with many small files | Expected for small-file workloads |
| `retry_sleep_ms` is large | An endpoint is returning errors rather than running slowly | Check the storage endpoint for throttling or outages |
