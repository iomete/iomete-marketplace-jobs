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
| `skip_identical`, `max_bandwidth_mb_per_sec`, `tasks_per_slot` | The corresponding `copy` settings used for this run. `max_bandwidth_mb_per_sec` is null when no limit was set. |
| `executor_count` | Configured executors, from `spark.executor.instances` or `spark.dynamicAllocation.maxExecutors`. |
| `vcpu_per_executor` | CPU limit of one executor pod, from `spark.kubernetes.executor.limit.cores`. |
| `slots_per_executor` | Configured copy slots per executor. The job calculates this as `ceil(vcpu_per_executor * slotsPerVcpu)`. |
| `task_count` | Spark tasks created for the copy. |
| `max_files_in_task` | Largest number of files assigned to one task. If it is much higher than `files_copied / task_count`, the planner may be underestimating the fixed cost per file. |
| `largest_file_bytes` | Largest file selected for copying. Files are never split across tasks, so the largest file sets a lower bound on the copy time. |

Multiply `executor_count` by `slots_per_executor` to get the maximum number of
copies the run could have in flight. The planner aims for that number multiplied
by `tasks_per_slot`, although the number of files and `maxBytesPerTask` can
change the final task count.

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
       executor_count * slots_per_executor                       AS slots,
       round(bytes_copied / 1024 / 1024 / nullif(copy_ms / 1000, 0), 1)
                                                                  AS mb_per_sec,
       round(copy_task_ms / nullif(copy_ms, 0), 1)                AS avg_concurrency,
       files_copied,
       task_count,
       largest_file_bytes
FROM spark_catalog.iomete_system_db.lakehouse_backup_runs
WHERE run_id = '<run-id>';
```

`avg_concurrency` is the average number of copy slots in use. Compare it with
`slots`, the run's maximum concurrency. If it is low, check whether
`task_count` is also low or every file already has its own task. Raising
`tasksPerSlot` only helps in the first case. One unusually large file can also
leave most slots idle near the end because files are never split.

### See where a run spent its time, as a share of the whole

Driver stages already use wall time, but executor timings are summed across
parallel tasks. The query scales executor timings by `copy_ms / copy_task_ms`
so every row uses the same clock and the rows add up to the run's wall time.

```sql
WITH r AS (
  SELECT *,
         unix_millis(ended_at) - unix_millis(started_at) AS wall_ms,
         copy_ms / nullif(copy_task_ms, 0)               AS scale
  FROM spark_catalog.iomete_system_db.lakehouse_backup_runs
  WHERE run_id = '<run-id>'
)
SELECT phase,
       round(ms)                    AS ms,
       round(100 * ms / wall_ms, 1) AS pct_of_run
FROM r
LATERAL VIEW stack(13,
  'source listing',        source_listing_ms,
  'target listing',        target_listing_ms,
  'planning',              planning_ms,
  'copy: filesystem init', scale * fs_init_ms,
  'copy: source read',     scale * source_read_ms,
  'copy: target write',    scale * target_write_ms,
  'copy: throttle wait',   scale * throttle_wait_ms,
  'copy: verify',          scale * verify_ms,
  'copy: commit',          scale * commit_ms,
  'copy: retry sleep',     scale * retry_sleep_ms,
  'copy: uninstrumented',  scale * (copy_task_ms - (fs_init_ms + source_read_ms + target_write_ms
                                    + throttle_wait_ms + verify_ms + commit_ms + retry_sleep_ms)),
  'directory create',      dir_create_ms,
  'driver: other',         wall_ms - (source_listing_ms + target_listing_ms + planning_ms
                                      + copy_ms + dir_create_ms)
) t AS phase, ms
ORDER BY ms DESC;
```

The copy rows show how the distributed copy time was spent. If `copy: target
write` is largest, the target is the main constraint. If `source listing` is
largest, focus on source enumeration before tuning executor concurrency.

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

The run row records `tasks_per_slot`, the bandwidth limit, and the effective
executor concurrency, so you can match changes in those settings with changes
in throughput.

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

## Tuning from the run history

Compare successful runs of the same job because a different file mix can hide
or exaggerate the effect of a setting. Change one value at a time, run the same
backup again, and keep the change only if it improves `copy_ms` or
`mb_per_sec`.

This query shows the main signals for the ten most recent runs:

```sql
SELECT run_id,
       started_at,
       executor_count * slots_per_executor                       AS slots,
       tasks_per_slot,
       task_count,
       round(copy_task_ms / nullif(copy_ms, 0), 1)                AS avg_concurrency,
       round(bytes_copied / 1024 / 1024 / nullif(copy_ms / 1000, 0), 1)
                                                                  AS mb_per_sec,
       round(copy_task_ms / nullif(files_copied, 0))              AS ms_per_file,
       round(files_copied / nullif(task_count, 0))                 AS avg_files_in_task,
       max_files_in_task,
       round(100 * largest_file_bytes / nullif(bytes_copied, 0), 1)
                                                                  AS largest_file_pct,
       round(100 * throttle_wait_ms / nullif(copy_task_ms, 0), 1) AS pct_throttled
FROM spark_catalog.iomete_system_db.lakehouse_backup_runs
WHERE job_id = '<job-id>' AND status = 'SUCCEEDED'
ORDER BY started_at DESC
LIMIT 10;
```

### `slotsPerVcpu`

`slotsPerVcpu` controls how many copies can run at once per executor vCPU.
Copies spend much of their time waiting on network I/O, so running more copies
than vCPUs is normal.

Raise it when `avg_concurrency` stays close to `slots` and previous increases
also improved `mb_per_sec`. It is already high enough when another increase
raises `ms_per_file` but leaves throughput unchanged.

Double the value and run a comparable backup. Keep the higher value only if
throughput improves beyond normal run-to-run variation; otherwise, revert it.

### `tasksPerSlot`

`tasksPerSlot` sets the target number of tasks for each available copy slot.
More tasks give a slot another piece of work when it finishes early, which
reduces idle time near the end of a run.

Raise it when `avg_concurrency` is below `slots` and `task_count` is close to the
current target, `slots * tasks_per_slot`. Do not raise it when `task_count`
already equals `files_copied`, because every file already has its own task.

Double the value and keep it only while `copy_ms` continues to fall.

### `perFileOverheadBytes`

The planner adds this estimated fixed cost to every file when it balances tasks.
It changes task planning only; the job does not copy any extra bytes.

Raise it when `max_files_in_task` is several times higher than
`avg_files_in_task`. That pattern means one task may be collecting too many
small files and keeping the run open after other tasks finish.

Change the value by a factor of two or four, then compare `max_files_in_task`
and `copy_ms` with the next run. The default is 25 MiB.

### `maxBytesPerTask`

`maxBytesPerTask` limits the estimated work assigned to a normal task. It is
not a hard file-size limit: a file above the value still goes into one task
because files are never split.

The limit is active when `task_count` is well above `slots * tasks_per_slot`;
the copy-plan log also reports when it applied. This is not an error. Keep the
default unless you specifically need shorter tasks for a very large backup.
Prefer `tasksPerSlot` for routine load balancing.

### `maxBandwidthMbPerSec`

`maxBandwidthMbPerSec` caps the copy throughput of the whole job. It is active
when `pct_throttled` is more than a few percent.

Raise the limit if the network has spare capacity. If the limit protects other
traffic, keep it and accept the longer run. Leave it unset when the backup
should copy as fast as the storage and network allow.

## Troubleshooting

| Signal | Likely cause | What to do |
|---|---|---|
| `RUNNING` long after `started_at` | The driver was killed or the run was cancelled | Check the run in the console; re-run the backup |
| `source_listing_ms` is a large share of the run | Source enumeration dominates, and the driver does this alone | Narrow the source `prefix` if possible |
| Low `avg_concurrency` with `task_count` near `slots * tasks_per_slot` | The planner created too few tasks to smooth out the tail | Raise `copy.tasksPerSlot`, unless every file already has its own task |
| `avg_concurrency` near `slots` with throughput still rising after each change | The executors have network capacity to spare | Raise `copy.slotsPerVcpu` |
| Low `avg_concurrency` with a high `largest_file_pct` | One large file may have finished after the other tasks | Expected; a single file is never split across tasks |
| `max_files_in_task` far above `avg_files_in_task` | The planner underestimated the fixed cost of small files | Raise `copy.perFileOverheadBytes` |
| `fs_init_ms` is a large share of `copy_task_ms` | Filesystem clients are rebuilt per file, which is expensive for small-file workloads | Expected; use `max_files_in_task` to check whether the files are also distributed unevenly |
| `throttle_wait_ms` is large | The bandwidth limit is being reached | Expected when `copy.maxBandwidthMbPerSec` is set; raise it if there is spare capacity |
| `source_read_ms` far above `target_write_ms` | Source storage is the bottleneck | Investigate the source system |
| `target_write_ms` far above `source_read_ms` | Target storage is the bottleneck | Investigate the target system |
| `verify_ms` plus `commit_ms` is large | Metadata operations dominate, common with many small files | Expected for small-file workloads |
| `retry_sleep_ms` is large | An endpoint is returning errors rather than running slowly | Check the storage endpoint for throttling or outages |
