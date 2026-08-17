# Run an end-to-end backup load test

Use this runbook to measure backup performance in your own IOMETE environment.
You will copy the same TPC-DS dataset with controlled configuration changes, then
compare the results from each run.

## What you will measure

- **End-to-end throughput:** The source byte total divided by the full run time,
  including listing, planning, and copying. This is the primary benchmark.
- **Copy throughput:** The transfer rate during the distributed copy stage. Use
  it to separate copy performance from listing and planning overhead.
- **Stage timings:** Time spent listing the source and target, planning the work,
  copying files, and creating directories. These timings show which stage limits
  the run.
- **Average concurrency:** The average number of copy slots busy during the copy
  stage. A slot runs one Spark task at a time; a task contains one or more files
  that the slot copies one after another.

## Before you start

You need:

- An IOMETE workspace and access to the console.
- Permission to create and run Spark jobs.
- Permission to run queries on stats tables with IOMETE SQL editor.
- The TPC-DS Iceberg generator from the IOMETE marketplace, unless you already
  have a dataset with a known size and file count.
- A source location for the test dataset and a separate target location for each
  full copy.
- Source credentials with read access and target credentials with write access,
  stored as IOMETE Secrets or environment variables.
- Dedicated source and target storage capacity, with enough network capacity
  reserved for the test. Both storage ends should be elastic; otherwise, the
  result measures the slower end.

**Caution:** This test performs full copies. Every run consumes source and
target network bandwidth, and every fresh target prefix consumes storage until
you remove it. Confirm that the target has enough free space for the complete
sweep and use dedicated capacity so the test does not affect production
workloads.

The procedure uses the IOMETE console and SQL editor as the primary path.
Optional AWS CLI checks are labelled separately.

## Choose a dataset

Choose the dataset before you configure the generator. TPC-DS in Iceberg gives
you partitioned tables, mixed file sizes, and many small Iceberg metadata files.
The backup copies the metadata files too, so they belong in the measurement.

TPC-DS scale factor describes raw text size, not stored size. Parquet
compression makes the stored dataset smaller. Across five schemas in our
workspace, we measured:

| Scale factor | Stored | Files | Stored GB per scale-factor unit |
|---|---:|---:|---:|
| 1 | 0.40 GB | 11,935 | 0.40 |
| 10 | 0.49 GB | 10,146 | 0.05 |
| 100 | 27.2 GB | 12,054 | 0.27 |
| 1000 | 255.8 GB | 13,141 | 0.26 |
| 5000 | 1.21 TB | 26,407 | 0.24 |

From SF100 upward, allow approximately 0.25 GB stored per scale-factor unit. At
lower scale factors, Iceberg metadata and per-table minimums dominate, which is
why SF1 and SF10 store roughly the same bytes and about 11,000 files each.

Use SF400, approximately 100 GB stored, for quick configuration sweeps. Use
SF4000, approximately 1 TB stored, for a sustained benchmark.

## Generate the dataset

The following generator configuration creates SF400. If you chose SF4000, change
`scale-factor` to `4000` and change the database name to `tpcds.tpcds_sf4000`
before you run it.

1. In the IOMETE console, open **Job Templates → New Job Template** and create a job.
2. Set **Docker Image** to
   `iomete.azurecr.io/iomete/tpcds-iceberg-generator:3.5.5`.
3. Set **Main Application File** to `spark-internal` and **Main Class** to
   `org.apache.spark.sql.execution.benchmark.TPCDSDatagen`.
4. Under **Configuration**, add `application.conf`, mount it at `/etc/configs`,
   and paste this configuration:

```hocon
{
    scale-factor = 400,
    partition-tables = true,
    use-double-for-decimal = false,
    use-string-for-char = true,
    num-partitions = 512,
    table-filter = [],
    database = "tpcds.tpcds_sf400"
}
```

5. Create and run the generator job.
6. Record the complete generator configuration with your results because
   `scale-factor`, partitioning, and `num-partitions` determine the file count
   and file-size mix.

To test the same approximate byte volume with more files, raise `num-partitions`
and leave `partition-tables` enabled. Keep the original value for every other
sweep.

**Expected result:** The generator run completes and the selected schema appears
in the IOMETE catalog.

**For more info:** https://github.com/iomete/spark-tpcds-datagen

## Confirm the dataset size

Use the source byte total for every throughput calculation, so confirm the
dataset before the first backup.

1. Open the generated schema on the catalog page in the IOMETE console.
2. Record its stored size and file count with the generator configuration.

**Expected result:** The catalog reports a size close to the estimate you
selected. The exact size and file count become the fixed dataset values for the
sweep.

**Optional:** If you have a configured AWS CLI, check the storage total
independently:

```bash
aws s3 ls --recursive --summarize "s3://SOURCE_BUCKET/DATASET_PREFIX/" | tail -2
```

Replace `SOURCE_BUCKET` and `DATASET_PREFIX` with the generated dataset
location. The AWS CLI total and catalog total should agree within the size of
metadata files written since the last catalog refresh. After the first backup,
use `bytes_source` from the run row because it is the byte total the job
actually enumerated.

## Configure the backup job

Create the backup job by following [Set up as a job in
IOMETE](../README.md#set-up-as-a-job-in-iomete). Use this `application.json` as
the test configuration:

```json
{
  "source": {
    "type": "s3",
    "bucket": "<SOURCE_BUCKET>",
    "prefix": "<DATASET_PREFIX>",
    "accessKey": "${SOURCE_ACCESS_KEY}",
    "secretKey": "${SOURCE_SECRET_KEY}"
  },
  "target": {
    "type": "s3",
    "bucket": "<TARGET_BUCKET>",
    "prefix": "<TARGET_PREFIX>",
    "accessKey": "${TARGET_ACCESS_KEY}",
    "secretKey": "${TARGET_SECRET_KEY}"
  },
  "copy": {
    "skipIdentical": false,
    "slotsPerVcpu": 2,
    "tasksPerSlot": 20
  }
}
```

Replace:

- `SOURCE_BUCKET`: bucket containing the generated dataset.
- `DATASET_PREFIX`: prefix containing only the schema you measured.
- `SOURCE_ACCESS_KEY` and `SOURCE_SECRET_KEY`: IOMETE Secret or environment
  variable names containing credentials that can read the source.
- `TARGET_BUCKET`: bucket reserved for test copies.
- `TARGET_PREFIX`: a new, empty prefix for this run, such as `loadtest/run-01`.
- `TARGET_ACCESS_KEY` and `TARGET_SECRET_KEY`: IOMETE Secret or environment
  variable names containing credentials that can write the target.

Hold these conditions fixed unless the current sweep changes one of them:

- Use a fresh target prefix for every run. This keeps each test a cold full copy
  and prevents earlier data from changing the result.
- Keep `skipIdentical` set to `false`. If you accidentally reuse a prefix, the
  job still copies every byte instead of measuring a skip.
- Turn off dynamic allocation and set `spark.executor.instances` manually.
  Executor count is a controlled variable.
- Set `spark.kubernetes.executor.limit.cores` explicitly and keep it fixed. The
  run fails at startup without it, and the job multiplies it by `slotsPerVcpu`
  to decide how many copies each executor runs at once.
- Leave `maxBandwidthMbPerSec` unset so the test has no configured bandwidth
  cap.

**Expected result:** The backup job appears in **Job Templates → <job-name>** and is ready to
run with a unique target prefix.

## Run the baseline

1. Choose the baseline compute size and fixed `spark.executor.instances` value.
   Record both with the dataset and backup settings.
2. Confirm that the configured target prefix is new and empty.
3. Trigger the backup from the job detail page and monitor it under **Runs →
   Logs**.
4. When the run ends, record the `run_id` shown in the IOMETE console.

**Expected result:** The run reaches a terminal state and has a `run_id` that
you can query.

## Collect the result

Every run writes its outcome, byte totals, stage timings, and settings to
`lakehouse_backup_runs`. Query that row in the IOMETE SQL editor instead of
using a stopwatch, driver log, or Spark UI. The row remains available after the
cluster stops; see [Backup run history](run-stats.md) for the full column
reference.

Replace `RUN_ID` in this query with the value from the console:

```sql
SELECT run_id,
       status,
       files_failed,
       files_skipped,
       round(bytes_source / 1024 / 1024
             / ((unix_millis(ended_at) - unix_millis(started_at)) / 1000), 1) AS end_to_end_mib_per_sec,
       round(bytes_copied / 1024 / 1024 / (copy_ms / 1000), 1)                AS copy_mib_per_sec,
       round(copy_task_ms / copy_ms, 1)                                       AS avg_concurrency,
       executor_count,
       vcpu_per_executor,
       slots_per_executor,
       task_count,
       max_files_in_task,
       largest_file_bytes,
       source_listing_ms,
       target_listing_ms,
       planning_ms,
       copy_ms,
       dir_create_ms
FROM spark_catalog.iomete_system_db.lakehouse_backup_runs
WHERE run_id = '<RUN_ID>';
```

A valid run has `status = 'SUCCEEDED'`, `files_failed = 0`, and `files_skipped =
0`. Discard any run that fails one of these checks: a partial failure has no
clean total to divide by, and skipped files mean the target was not fresh. Fix
the cause, choose another new target prefix, and repeat the run.

Report end-to-end throughput as the primary result and use copy throughput to
examine the stage split. Compare `avg_concurrency` with `executor_count ×
slots_per_executor`; a much lower value means the available copy capacity was
not busy throughout the stage. Use `task_count`, `largest_file_bytes`, and the
stage timings to find where the time went.

**Expected result:** The query returns one valid row with both throughput
values, average concurrency, the tested cluster settings, and all five stage
timings.

## Compare configurations

Start each sweep from the baseline, change only the listed variable, and use a
fresh target prefix for every run. Run the backup and collect a valid result
after each change.

| Sweep | Change only | What it shows |
|---|---|---|
| Executors | `spark.executor.instances`: 1, 2, 4, 8 | Whether throughput rises with cluster size or flattens when the driver's single-threaded listing becomes the limit |
| Slots | `slotsPerVcpu`: 1, 2, 4, 8, 16 | How much executor network capacity you use; more slots than vCPUs is normal because copy slots spend most of their time waiting on the network |
| Task granularity | `tasksPerSlot`: 5, 20, 50 | Whether work is split finely enough to keep slots busy |
| File shape | The same scale factor with higher `num-partitions` | The cost of copying the same approximate bytes as many small files |

Record the generator configuration, backup configuration, compute size, target
prefix, and query result together for each run. Do not compare an invalid run
with the sweep.

**Expected result:** You have one valid result row for each tested
configuration, with only one controlled variable changed between comparable
runs.

## Clean up the test data

Remove every target prefix created by the baseline and sweeps. Remove the
generated TPC-DS schema if you no longer need it, but keep the recorded
configuration and `lakehouse_backup_runs` rows for comparison.

If you expect to repeat the test, add a storage lifecycle rule for the load-test
target prefixes so full copies do not accumulate.

**Expected result:** The generated copies no longer consume target storage, and
your run records remain available in IOMETE.

## Our benchmark results

We measured AWS S3 to AWS S3 within one region with TPC-DS Iceberg datasets
generated from the configuration above. Both storage ends were elastic.

### Throughput

_Pending: fill this table from the run rows._

| Dataset | Stored | Executors | vCPU each | Slots each | End-to-end MiB/s | Copy MiB/s | Concurrency |
|---|---|---|---|---|---|---|---|
| | | | | | | | |

We already recorded two results on the same 1 TB dataset and cluster, changing
only the slots per executor: 184.6 MiB/s at 3 slots and 690.7 MiB/s at 20 slots.
In these runs, slot count had a larger effect than cluster size.

### Per-file cost

Across five runs on three datasets, copying one file from S3 to S3 within one
region cost 0.90 seconds plus the file size at 29 MiB/s. This model predicted
all five runs within 5%:

```
seconds ≈ (files × 0.90 + bytes / 30_400_000) / (executors × slots_per_executor)
```

At 29 MiB/s, 0.90 seconds is about 26 MiB of transfer. This is the basis for the
25 MiB default of `perFileOverheadBytes`. These figures apply only to S3 to S3
within one region. HDFS should have a much lower per-file cost because a rename
is a metadata operation rather than a server-side copy.

### Compare storage systems carefully

A copy from a datacentre to a cloud region is limited by the connection between
them, not the cluster. Adding executors measures that link rather than the job,
so keep cross-system results separate from the cluster scaling curve.
